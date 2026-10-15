package checkout

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/scm/encoding"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// scm is a Source over the SCM's Browse and Store services.
type scm struct {
	browse scmpb.BrowseClient
	store  scmpb.StoreClient
	// The repositories' dictionaries read so far, as decoders, by repository and ID.
	mu    sync.Mutex
	dicts map[dictKey]*zstd.Decoder
}

type dictKey struct {
	repo int64
	id   uint32
}

// SCM returns a Source over the SCM's Browse and Store services.
func SCM(browse scmpb.BrowseClient, store scmpb.StoreClient) Source {
	return &scm{browse: browse, store: store, dicts: map[dictKey]*zstd.Decoder{}}
}

// dictionary is a decoder holding a dictionary of the repository's, read from the SCM once.
func (s *scm) dictionary(ctx context.Context, repo int64, id uint32) (*zstd.Decoder, error) {
	k := dictKey{repo, id}
	s.mu.Lock()
	dec, ok := s.dicts[k]
	s.mu.Unlock()
	if ok {
		return dec, nil
	}
	d, err := s.store.ReadDictionary(ctx, &scmpb.ReadDictionaryRequest{RepositoryId: repo, Id: id})
	if err != nil {
		return nil, fmt.Errorf("reading dictionary %d: %w", id, interrupted(err))
	}
	dec, err = zstd.NewReader(nil, zstd.WithDecoderDicts(d.GetBody()), zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(maxChunkLength))
	if err != nil {
		return nil, fmt.Errorf("dictionary %d: %w", id, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if have, ok := s.dicts[k]; ok {
		dec.Close()
		return have, nil
	}
	s.dicts[k] = dec
	return dec, nil
}

func (s *scm) Entries(ctx context.Context, repo int64, commit []byte) ([]Entry, error) {
	stream, err := s.browse.Export(ctx, &scmpb.ExportRequest{RepositoryId: repo, CommitId: commit, EntriesOnly: true})
	if err != nil {
		return nil, fmt.Errorf("listing the commit: %w", interrupted(err))
	}
	var out []Entry
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			// The SCM refuses a commit it cannot present as one tree: it has conflicts.
			if status.Code(err) == codes.InvalidArgument {
				return nil, &CommitError{Message: status.Convert(err).Message()}
			}
			return nil, fmt.Errorf("listing the commit: %w", interrupted(err))
		}
		e := frame.GetEntry()
		if e == nil {
			return nil, fmt.Errorf("listing the commit: the SCM sent file contents, not asked for")
		}
		v := e.GetValue()
		entry := Entry{Path: e.GetPath(), Executable: v.GetExecutable(), Size: int64(v.GetSize()), Target: v.GetSymlinkTarget()}
		switch v.GetType() {
		case scmpb.EntryType_ENTRY_TYPE_FILE:
			entry.Kind = File
			if len(v.GetId()) != len(entry.ID) {
				return nil, fmt.Errorf("listing the commit: %s has a %d-byte ID", e.GetPath(), len(v.GetId()))
			}
			copy(entry.ID[:], v.GetId())
		case scmpb.EntryType_ENTRY_TYPE_SYMLINK:
			entry.Kind = Symlink
		default:
			return nil, fmt.Errorf("listing the commit: %s is a %s", e.GetPath(), v.GetType())
		}
		out = append(out, entry)
	}
}

// interrupted marks an error as ErrInterrupted when the SCM broke the call off: it was
// unreachable, or its stream ended abruptly — as one does when the SCM restarts.
func interrupted(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.Unavailable, codes.Internal, codes.Aborted:
		return fmt.Errorf("%w: %w", ErrInterrupted, err)
	}
	return err
}

func (s *scm) ReadFiles(ctx context.Context, repo int64, ids []ID, write func(ID, []byte, bool) error) error {
	req := &scmpb.ReadFilesRequest{RepositoryId: repo, Stored: true, Dictionaries: true}
	for _, id := range ids {
		req.Files = append(req.Files, &scmpb.FileRange{FileId: id[:]})
	}
	stream, err := s.store.ReadFiles(ctx, req)
	if err != nil {
		return fmt.Errorf("reading files: %w", interrupted(err))
	}
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading files: %w", interrupted(err))
		}
		var id ID
		if len(frame.GetFileId()) != len(id) {
			return fmt.Errorf("reading files: a %d-byte file ID", len(frame.GetFileId()))
		}
		copy(id[:], frame.GetFileId())
		if frame.GetNotFound() {
			return fmt.Errorf("reading files: the SCM does not have file %x", id[:])
		}
		data, err := chunkData(frame, func(id uint32) (*zstd.Decoder, error) { return s.dictionary(ctx, repo, id) })
		if err != nil {
			return fmt.Errorf("reading files: file %x: %w", id[:], err)
		}
		if err := write(id, data, frame.GetLast()); err != nil {
			return err
		}
	}
}

// maxChunkLength bounds the raw length of a chunk the SCM sends stored: a frame claiming more is
// refused before it is decompressed. A chunk is at most 256 KiB (docs/scm.md).
const maxChunkLength = 16 << 20

// zstdDecoder decompresses the chunks the SCM sends zstd-compressed, as their packs store them.
var zstdDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(maxChunkLength))

// chunkData is the raw bytes of the chunk a frame carries, decompressed as its codec says — with
// dict's decoder for a dictionary the frame names — and checked against the chunk's ID; a frame
// without a chunk carries none.
func chunkData(f *scmpb.FileData, dict func(id uint32) (*zstd.Decoder, error)) ([]byte, error) {
	data := f.GetData()
	if len(data) == 0 {
		return nil, nil
	}
	raw := data
	switch codec := f.GetCodec(); codec {
	case scmpb.ChunkCodec_CHUNK_CODEC_UNSPECIFIED, scmpb.ChunkCodec_CHUNK_CODEC_RAW:
	case scmpb.ChunkCodec_CHUNK_CODEC_ZSTD:
		if f.GetRawLength() > maxChunkLength {
			return nil, fmt.Errorf("a chunk of %d bytes, over %d", f.GetRawLength(), maxChunkLength)
		}
		var err error
		if raw, err = zstdDecoder.DecodeAll(data, make([]byte, 0, f.GetRawLength())); err != nil {
			return nil, fmt.Errorf("chunk %x: %w", f.GetChunkId(), err)
		}
	case scmpb.ChunkCodec_CHUNK_CODEC_ZSTD_DICT:
		if f.GetRawLength() > maxChunkLength {
			return nil, fmt.Errorf("a chunk of %d bytes, over %d", f.GetRawLength(), maxChunkLength)
		}
		var h zstd.Header
		if err := h.Decode(data); err != nil || h.DictionaryID == 0 {
			return nil, fmt.Errorf("chunk %x: a dictionary's frame names no dictionary", f.GetChunkId())
		}
		if dict == nil {
			return nil, fmt.Errorf("chunk %x: compressed with dictionary %d, and none is read", f.GetChunkId(), h.DictionaryID)
		}
		dec, err := dict(h.DictionaryID)
		if err != nil {
			return nil, fmt.Errorf("chunk %x: %w", f.GetChunkId(), err)
		}
		if raw, err = dec.DecodeAll(data, make([]byte, 0, f.GetRawLength())); err != nil {
			return nil, fmt.Errorf("chunk %x: %w", f.GetChunkId(), err)
		}
	default:
		return nil, fmt.Errorf("chunk %x: unknown codec %v", f.GetChunkId(), codec)
	}
	if f.GetCodec() != scmpb.ChunkCodec_CHUNK_CODEC_UNSPECIFIED && uint64(len(raw)) != uint64(f.GetRawLength()) {
		return nil, fmt.Errorf("chunk %x: %d bytes, the SCM says %d", f.GetChunkId(), len(raw), f.GetRawLength())
	}
	h := encoding.NewHasher(encoding.KindChunk)
	h.Write(raw)
	if id := h.Sum(); !bytes.Equal(id[:], f.GetChunkId()) {
		return nil, fmt.Errorf("chunk %x from the SCM does not hash to its ID", f.GetChunkId())
	}
	return raw, nil
}
