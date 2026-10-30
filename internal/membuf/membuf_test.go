package membuf

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"
)

// Bytes written in pieces of every size, across many blocks, read back whole and at any offset as
// they were written; a read past the end reads what there is and io.EOF.
func TestBuffer(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	want := make([]byte, 5<<20+12345)
	for i := range want {
		want[i] = byte(rng.Uint32())
	}
	var b Buffer
	for rest := want; len(rest) > 0; {
		n := min(len(rest), rng.IntN(300<<10))
		if w, err := b.Write(rest[:n]); w != n || err != nil {
			t.Fatalf("Write = %d, %v", w, err)
		}
		rest = rest[n:]
	}
	if b.Len() != int64(len(want)) {
		t.Fatalf("Len = %d, want %d", b.Len(), len(want))
	}
	got, err := io.ReadAll(b.Reader())
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("the whole buffer: %d bytes, %v", len(got), err)
	}
	for range 2000 {
		off := rng.Int64N(int64(len(want)))
		p := make([]byte, rng.IntN(3<<20))
		n, err := b.ReadAt(p, off)
		end := min(off+int64(len(p)), int64(len(want)))
		if n != int(end-off) || !bytes.Equal(p[:n], want[off:end]) {
			t.Fatalf("ReadAt(%d bytes, %d) = %d", len(p), off, n)
		}
		if past := off+int64(len(p)) > int64(len(want)); past != (err == io.EOF) || (!past && err != nil) {
			t.Fatalf("ReadAt(%d bytes, %d): %v", len(p), off, err)
		}
	}
	if n, err := b.ReadAt(make([]byte, 1), int64(len(want))); n != 0 || err != io.EOF {
		t.Errorf("a read at the end: %d, %v", n, err)
	}
	// A small buffer takes a small block.
	var small Buffer
	small.Write([]byte("abc"))
	if c := cap(small.blocks[0]); c != firstBlock {
		t.Errorf("three bytes took a block of %d", c)
	}
	b.Reset()
	if b.Len() != 0 || len(b.blocks) != 0 {
		t.Errorf("after Reset: %d bytes, %d blocks", b.Len(), len(b.blocks))
	}
}
