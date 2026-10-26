package checkout

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/scm/encoding"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// scm is a Source over the SCM's Store service.
type scm struct {
	store scmpb.StoreClient
	// The repositories' dictionaries read so far, as decoders, by repository and ID.
	mu    sync.Mutex
	dicts map[dictKey]*zstd.Decoder
	// layers are the checkout layers read so far, and full the trees whose checkouts they hold
	// whole (layers.go).
	layers layerCache
	full   holding
}

type dictKey struct {
	repo int64
	id   uint32
}

// SCM returns a Source over the SCM's Store service.
func SCM(store scmpb.StoreClient) Source {
	return &scm{store: store, dicts: map[dictKey]*zstd.Decoder{}}
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
