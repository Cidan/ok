package encoding

import (
	"encoding/binary"
	"fmt"
)

// Large trees (docs/scm.md, "Large trees"): a tree whose canonical body is over TreeChunkThreshold —
// a directory of thousands of entries — is held and sent as chunks of its body, as a large file is,
// so an edit of one entry stores and sends a chunk or two, not the whole tree. Its ID is unchanged:
// the BLAKE3 of its whole body. The chunks are cut where its entries say, so the same entries cut
// the same way on every machine and an edit moves only the cuts around it:
//
//   - the first chunk is the body's entry count (8 bytes), which an added or removed entry changes;
//   - each other is a run of whole entries, at least treeLeafMin bytes, ended after the entry whose
//     name's BLAKE3 — its first little-endian 32 bits — falls below a share of 2³² that is the
//     entry's length over treeLeafAvg less treeLeafMin, so runs average about treeLeafAvg; and an
//     entry that would carry a run past treeLeafMax begins the next, so none is longer than a chunk
//     may be. A body with an entry over treeLeafMax is not cut: the tree is held whole.
//
// Each run is a leaf of a prolly tree of the tree's entries (a B-tree cut by its content, as Dolt's):
// its first entry's name and its chunk name it in the tree's index (internal/scm/store's
// lookupLarge), which finds an entry, or what changed between two of a tree's versions, by reading
// only the leaves that hold them.

const (
	// TreeChunkThreshold is the largest tree body held whole.
	TreeChunkThreshold = 128 << 10
	treeLeafMin        = 16 << 10
	treeLeafAvg        = 64 << 10
	treeLeafMax        = 256 << 10
	// treeCountLen is the length of a tree body's entry count.
	treeCountLen = 8
)

// ChunkTree cuts a tree's canonical body, over TreeChunkThreshold, into its chunks: the entry
// count, then runs of whole entries. They alias body, and concatenate to it.
func ChunkTree(body []byte) ([][]byte, error) {
	d := &decoder{b: body}
	n := d.count()
	if d.failed() {
		return nil, d.err
	}
	out := [][]byte{body[:treeCountLen]}
	start := d.off
	for i := 0; i < n; i++ {
		entry := d.off
		name := d.str()
		d.treeValue()
		if d.failed() {
			return nil, d.err
		}
		if !validName(name) {
			return nil, fmt.Errorf("tree entry name %q", name)
		}
		if d.off-entry > treeLeafMax {
			return nil, fmt.Errorf("tree entry %q is %d bytes: a large tree's entries are at most %d", name, d.off-entry, treeLeafMax)
		}
		// An entry that would carry a run past treeLeafMax begins the next.
		if d.off-start > treeLeafMax {
			out = append(out, body[start:entry])
			start = entry
		}
		if (d.off-start >= treeLeafMin && cutAfter(name, d.off-entry)) || i == n-1 {
			out = append(out, body[start:d.off])
			start = d.off
		}
	}
	if d.remaining() != 0 {
		return nil, fmt.Errorf("tree body: %d bytes after its %d entries", d.remaining(), n)
	}
	return out, nil
}

// cutAfter reports whether a run ends after an entry of name, entry bytes long.
func cutAfter(name string, entry int) bool {
	h := Hash([]byte(name))
	return uint64(binary.LittleEndian.Uint32(h[:4])) < uint64(entry)<<32/(treeLeafAvg-treeLeafMin)
}

// TreeLeafEntries decodes a leaf of a large tree: a run of its entries, without the count.
func TreeLeafEntries(leaf []byte) ([]TreeEntry, error) {
	d := &decoder{b: leaf}
	var out []TreeEntry
	for d.remaining() > 0 && !d.failed() {
		start := d.off
		name := d.str()
		v := d.treeValue()
		if d.failed() {
			break
		}
		if !validName(name) {
			d.off = start
			d.fail(CodeName, "tree entry name %q", name)
			break
		}
		if len(out) > 0 && name <= out[len(out)-1].Name {
			d.off = start
			d.fail(CodeUnsorted, "tree entry %q follows %q", name, out[len(out)-1].Name)
			break
		}
		out = append(out, TreeEntry{Name: name, Value: v})
	}
	if d.failed() {
		return nil, d.err
	}
	return out, nil
}

// TreeLeafFirst is the name of a large tree's leaf's first entry: what names the leaf in the tree's
// index.
func TreeLeafFirst(leaf []byte) (string, error) {
	d := &decoder{b: leaf}
	name := d.str()
	if d.failed() {
		return "", d.err
	}
	if !validName(name) {
		return "", fmt.Errorf("tree entry name %q", name)
	}
	return name, nil
}

// TreeCount is a large tree's entry count, its first chunk.
func TreeCount(first []byte) (uint64, error) {
	if len(first) != treeCountLen {
		return 0, fmt.Errorf("a large tree's first chunk is %d bytes, not its entry count", len(first))
	}
	return binary.LittleEndian.Uint64(first), nil
}
