package checkout

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/scm/encoding"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// The SCM unreachable, or its stream ended abruptly — "server closed the stream without sending
// trailers" and a stream reset are Internal — is an interruption; a refusal is not.
func TestInterrupted(t *testing.T) {
	for err, want := range map[error]bool{
		status.Error(codes.Unavailable, "connection refused"):                                           true,
		status.Error(codes.Internal, "server closed the stream without sending trailers"):               true,
		status.Error(codes.Internal, "stream terminated by RST_STREAM with error code: INTERNAL_ERROR"): true,
		status.Error(codes.Aborted, "aborted"):                                                          true,
		status.Error(codes.InvalidArgument, "the commit has conflicts"):                                 false,
		status.Error(codes.NotFound, "no such repository"):                                              false,
		status.Error(codes.PermissionDenied, "no"):                                                      false,
		errors.New("the SCM sent file x twice"):                                                         false,
	} {
		if got := errors.Is(interrupted(err), ErrInterrupted); got != want {
			t.Errorf("%v: interrupted %v, want %v", err, got, want)
		}
	}
}

// A frame's chunk is decompressed as its codec says and verified against its ID; one that does
// not decode, or decodes to other bytes or another length than the SCM says, is refused.
func TestChunkData(t *testing.T) {
	raw := bytes.Repeat([]byte("checkout chunk\n"), 4000)
	h := encoding.NewHasher(encoding.KindChunk)
	h.Write(raw)
	id := h.Sum()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	packed := enc.EncodeAll(raw, nil)
	n := uint32(len(raw))
	zstdCodec, rawCodec := scmpb.ChunkCodec_CHUNK_CODEC_ZSTD, scmpb.ChunkCodec_CHUNK_CODEC_RAW
	for name, f := range map[string]*scmpb.FileData{
		"raw, from an SCM that sends no codec": {ChunkId: id[:], Data: raw},
		"raw, as stored":                       {ChunkId: id[:], Data: raw, Codec: rawCodec, RawLength: n},
		"zstd":                                 {ChunkId: id[:], Data: packed, Codec: zstdCodec, RawLength: n},
	} {
		got, err := chunkData(f, nil)
		if err != nil || !bytes.Equal(got, raw) {
			t.Errorf("%s: %d bytes, %v", name, len(got), err)
		}
	}
	if got, err := chunkData(&scmpb.FileData{}, nil); err != nil || got != nil {
		t.Errorf("a frame without a chunk: %v, %v", got, err)
	}
	other := encoding.ID{1}
	for name, c := range map[string]struct {
		f    *scmpb.FileData
		want string
	}{
		"another chunk's ID":   {&scmpb.FileData{ChunkId: other[:], Data: packed, Codec: zstdCodec, RawLength: n}, "does not hash"},
		"another raw length":   {&scmpb.FileData{ChunkId: id[:], Data: packed, Codec: zstdCodec, RawLength: n - 1}, "the SCM says"},
		"not zstd":             {&scmpb.FileData{ChunkId: id[:], Data: raw, Codec: zstdCodec, RawLength: n}, "chunk"},
		"a raw length too big": {&scmpb.FileData{ChunkId: id[:], Data: packed, Codec: zstdCodec, RawLength: maxChunkLength + 1}, "over"},
		"an unknown codec":     {&scmpb.FileData{ChunkId: id[:], Data: raw, Codec: 7, RawLength: n}, "unknown codec"},
		"raw, another ID":      {&scmpb.FileData{ChunkId: other[:], Data: raw}, "does not hash"},
	} {
		if _, err := chunkData(c.f, nil); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error saying %q", name, err, c.want)
		}
	}
}

// A chunk compressed with a repository's dictionary decodes with the one its frame names, and
// only with it.
func TestChunkDataWithADictionary(t *testing.T) {
	raw := []byte("package checkout\n\nfunc F() {}\n")
	h := encoding.NewHasher(encoding.KindChunk)
	h.Write(raw)
	id := h.Sum()
	content := bytes.Repeat([]byte("package checkout\n\nfunc G() {}\n"), 8)
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderDictRaw(40001, content))
	if err != nil {
		t.Fatal(err)
	}
	f := &scmpb.FileData{ChunkId: id[:], Data: enc.EncodeAll(raw, nil), Codec: scmpb.ChunkCodec_CHUNK_CODEC_ZSTD_DICT, RawLength: uint32(len(raw))}
	asked := map[uint32]int{}
	dict := func(want uint32) func(uint32) (*zstd.Decoder, error) {
		return func(id uint32) (*zstd.Decoder, error) {
			asked[id]++
			return zstd.NewReader(nil, zstd.WithDecoderDictRaw(want, content))
		}
	}
	got, err := chunkData(f, dict(40001))
	if err != nil || !bytes.Equal(got, raw) || asked[40001] != 1 {
		t.Fatalf("decoded %q, %v; asked %v", got, err, asked)
	}
	if _, err := chunkData(f, nil); err == nil {
		t.Fatal("decoded without a dictionary")
	}
	if _, err := chunkData(f, dict(40002)); err == nil {
		t.Fatal("decoded with another dictionary")
	}
	plain := &scmpb.FileData{ChunkId: id[:], Data: zstdDecoderTestFrame(t, raw), Codec: scmpb.ChunkCodec_CHUNK_CODEC_ZSTD_DICT, RawLength: uint32(len(raw))}
	if _, err := chunkData(plain, dict(40001)); err == nil || !strings.Contains(err.Error(), "names no dictionary") {
		t.Fatalf("a frame naming no dictionary: %v", err)
	}
}

func zstdDecoderTestFrame(t *testing.T, raw []byte) []byte {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	return enc.EncodeAll(raw, nil)
}
