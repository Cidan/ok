package encoding

import (
	"encoding/binary"
	"fmt"
)

// TreeSizes is what a tree's entries weigh: one size per entry, in the tree's order — a file's
// length in bytes, a symlink's target's, and 0 for a subtree (docs/scm.md, "Tree sizes";
// Mononoke calls the like "fsnodes"). It is derived from a tree and what the tree names, and kept
// and sent under the tree's ID; it is not a jj object, and never hashed into one.
type TreeSizes struct {
	Sizes []uint64
}

// KindTreeSizes is a tree sizes record's first byte. It names no object: Decode refuses it.
const KindTreeSizes Kind = 0x20

// TreeSizesVersion is the record format EncodeTreeSizes writes and DecodeTreeSizes reads.
const TreeSizesVersion = 1

// EncodeTreeSizes returns s's record: KindTreeSizes, the version, the entry count, and each size,
// the count and sizes as minimal unsigned LEB128 varints.
func EncodeTreeSizes(s *TreeSizes) []byte {
	b := make([]byte, 0, 3+len(s.Sizes)*2)
	b = append(b, byte(KindTreeSizes), TreeSizesVersion)
	b = binary.AppendUvarint(b, uint64(len(s.Sizes)))
	for _, n := range s.Sizes {
		b = binary.AppendUvarint(b, n)
	}
	return b
}

// DecodeTreeSizes reads a record EncodeTreeSizes wrote, refusing any other bytes: a varint that is
// not minimal (so a record has one encoding), a count larger than the bytes left could hold, and
// bytes after the last size.
func DecodeTreeSizes(b []byte) (*TreeSizes, error) {
	fail := func(off int, code Code, format string, args ...any) (*TreeSizes, error) {
		return nil, &Error{Code: code, Offset: off, Detail: fmt.Sprintf(format, args...)}
	}
	if len(b) < 1 {
		return fail(0, CodeTruncated, "no kind byte")
	}
	if Kind(b[0]) != KindTreeSizes {
		return fail(0, CodeKind, "%s, not tree sizes", Kind(b[0]))
	}
	if len(b) < 2 {
		return fail(1, CodeTruncated, "no version")
	}
	if b[1] != TreeSizesVersion {
		return fail(1, CodeVersion, "version %d", b[1])
	}
	off := 2
	uvarint := func() (uint64, bool, *TreeSizes, error) {
		v, n := binary.Uvarint(b[off:])
		switch {
		case n == 0:
			s, err := fail(off, CodeTruncated, "a varint ends at the end of the input")
			return 0, false, s, err
		case n < 0:
			s, err := fail(off, CodeVarint, "a varint exceeds 64 bits")
			return 0, false, s, err
		case n > 1 && b[off+n-1] == 0:
			s, err := fail(off, CodeVarint, "a varint is not minimal")
			return 0, false, s, err
		}
		off += n
		return v, true, nil, nil
	}
	count, ok, s, err := uvarint()
	if !ok {
		return s, err
	}
	if count > uint64(len(b)-off) {
		return fail(off, CodeTruncated, "%d entries, but %d bytes left", count, len(b)-off)
	}
	out := &TreeSizes{Sizes: make([]uint64, count)}
	for i := range out.Sizes {
		v, ok, s, err := uvarint()
		if !ok {
			return s, err
		}
		out.Sizes[i] = v
	}
	if off != len(b) {
		return fail(off, CodeTrailing, "%d bytes after the last size", len(b)-off)
	}
	return out, nil
}
