// Package codesearch is a repository's code search (docs/hosted.md, "ok control at scale, as built"):
// a trigram index of a commit's text files — each file's path and content ID, then each trigram's
// sorted list of the files holding it — and queries, a literal or a regular expression, answered
// by intersecting the lists of the trigrams a match must hold, then reading the files they name and
// matching each a line at a time.
//
// Trigrams are of bytes, ASCII letters lowercased, so a case-sensitive query's candidates are a
// superset of its matches; every candidate is matched in full.
package codesearch

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
)

// MaxFile is the largest file indexed; larger ones, and binary ones, are not searched.
const MaxFile = 1 << 20

// magic starts an encoded index.
var magic = []byte("okcs\x01")

// File is a file of an indexed commit.
type File struct {
	Path string
	ID   [32]byte
	Size uint64
}

// Index is a commit's trigram index.
type Index struct {
	Commit [32]byte
	Files  []File
	// postings are each trigram's files, by index into Files, ascending.
	postings map[uint32][]uint32
}

// trigram packs three bytes, lowercased.
func trigram(a, b, c byte) uint32 {
	return uint32(lower(a))<<16 | uint32(lower(b))<<8 | uint32(lower(c))
}

func lower(b byte) byte {
	if 'A' <= b && b <= 'Z' {
		return b + 'a' - 'A'
	}
	return b
}

// Builder builds an index a file at a time.
type Builder struct {
	ix   *Index
	seen map[uint32]bool
}

// NewBuilder starts an index of a commit.
func NewBuilder(commit [32]byte) *Builder {
	return &Builder{ix: &Index{Commit: commit, postings: map[uint32][]uint32{}}, seen: map[uint32]bool{}}
}

// Text reports whether a file's contents are text: no NUL in its first 8 KiB, and not too large.
func Text(data []byte) bool {
	if len(data) > MaxFile {
		return false
	}
	return bytes.IndexByte(data[:min(len(data), 8<<10)], 0) < 0
}

// Add indexes a file, if it is text.
func (b *Builder) Add(f File, data []byte) {
	if !Text(data) {
		return
	}
	n := uint32(len(b.ix.Files))
	b.ix.Files = append(b.ix.Files, f)
	clear(b.seen)
	for i := 0; i+2 < len(data); i++ {
		t := trigram(data[i], data[i+1], data[i+2])
		if !b.seen[t] {
			b.seen[t] = true
			b.ix.postings[t] = append(b.ix.postings[t], n)
		}
	}
}

// Index is the index built.
func (b *Builder) Index() *Index { return b.ix }

// Encode writes an index: its magic, commit, files, then each trigram's postings, delta- and
// varint-coded, trigrams ascending.
func (ix *Index) Encode(w io.Writer) error {
	bw := bufio.NewWriter(w)
	var buf [binary.MaxVarintLen64]byte
	uv := func(v uint64) { n := binary.PutUvarint(buf[:], v); bw.Write(buf[:n]) }
	bw.Write(magic)
	bw.Write(ix.Commit[:])
	uv(uint64(len(ix.Files)))
	for _, f := range ix.Files {
		uv(uint64(len(f.Path)))
		bw.WriteString(f.Path)
		bw.Write(f.ID[:])
		uv(f.Size)
	}
	ts := make([]uint32, 0, len(ix.postings))
	for t := range ix.postings {
		ts = append(ts, t)
	}
	slices.Sort(ts)
	uv(uint64(len(ts)))
	for _, t := range ts {
		bw.Write([]byte{byte(t >> 16), byte(t >> 8), byte(t)})
		ps := ix.postings[t]
		uv(uint64(len(ps)))
		prev := uint32(0)
		for i, p := range ps {
			if i == 0 {
				uv(uint64(p))
			} else {
				uv(uint64(p - prev))
			}
			prev = p
		}
	}
	return bw.Flush()
}

// ErrCorrupt is an encoded index that does not decode.
var ErrCorrupt = errors.New("codesearch: the index does not decode")

// Decode reads an index Encode wrote.
func Decode(data []byte) (*Index, error) {
	r := bytes.NewReader(data)
	head := make([]byte, len(magic)+32)
	if _, err := io.ReadFull(r, head); err != nil || !bytes.Equal(head[:len(magic)], magic) {
		return nil, ErrCorrupt
	}
	ix := &Index{postings: map[uint32][]uint32{}}
	copy(ix.Commit[:], head[len(magic):])
	uv := func() (uint64, error) { return binary.ReadUvarint(r) }
	nf, err := uv()
	if err != nil || nf > uint64(len(data)) {
		return nil, ErrCorrupt
	}
	ix.Files = make([]File, nf)
	for i := range ix.Files {
		n, err := uv()
		if err != nil || n > uint64(r.Len()) {
			return nil, ErrCorrupt
		}
		p := make([]byte, n)
		if _, err := io.ReadFull(r, p); err != nil {
			return nil, ErrCorrupt
		}
		ix.Files[i].Path = string(p)
		if _, err := io.ReadFull(r, ix.Files[i].ID[:]); err != nil {
			return nil, ErrCorrupt
		}
		if ix.Files[i].Size, err = uv(); err != nil {
			return nil, ErrCorrupt
		}
	}
	nt, err := uv()
	if err != nil || nt > uint64(r.Len()) {
		return nil, ErrCorrupt
	}
	for range nt {
		var tb [3]byte
		if _, err := io.ReadFull(r, tb[:]); err != nil {
			return nil, ErrCorrupt
		}
		n, err := uv()
		if err != nil || n > nf {
			return nil, ErrCorrupt
		}
		ps := make([]uint32, n)
		prev := uint64(0)
		for i := range ps {
			d, err := uv()
			if err != nil {
				return nil, ErrCorrupt
			}
			if i > 0 {
				d += prev
			}
			if d >= nf {
				return nil, ErrCorrupt
			}
			ps[i], prev = uint32(d), d
		}
		ix.postings[uint32(tb[0])<<16|uint32(tb[1])<<8|uint32(tb[2])] = ps
	}
	if r.Len() != 0 {
		return nil, ErrCorrupt
	}
	return ix, nil
}

// Size is roughly what an index holds in memory, in bytes.
func (ix *Index) Size() int64 {
	n := int64(len(ix.Files)) * 64
	for _, f := range ix.Files {
		n += int64(len(f.Path))
	}
	for _, ps := range ix.postings {
		n += 16 + int64(len(ps))*4
	}
	return n
}

// candidates are the files a query's trigrams leave possible, ascending; all of them when it
// requires none.
func (ix *Index) candidates(q *query) []uint32 {
	switch q.op {
	case qAll:
		out := make([]uint32, len(ix.Files))
		for i := range out {
			out[i] = uint32(i)
		}
		return out
	case qNone:
		return nil
	case qAnd:
		var out []uint32
		first := true
		for _, t := range q.trigrams {
			ps := ix.postings[t]
			if first {
				out, first = slices.Clone(ps), false
			} else {
				out = intersect(out, ps)
			}
			if len(out) == 0 {
				return nil
			}
		}
		for _, s := range q.sub {
			c := ix.candidates(s)
			if first {
				out, first = c, false
			} else {
				out = intersect(out, c)
			}
			if len(out) == 0 {
				return nil
			}
		}
		if first {
			return ix.candidates(&query{op: qAll})
		}
		return out
	case qOr:
		var out []uint32
		for _, s := range q.sub {
			out = union(out, ix.candidates(s))
		}
		return out
	}
	return nil
}

func intersect(a, b []uint32) []uint32 {
	out := a[:0]
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

func union(a, b []uint32) []uint32 {
	out := make([]uint32, 0, len(a)+len(b))
	out = append(out, a...)
	out = append(out, b...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return slices.Compact(out)
}

func (q *query) String() string {
	switch q.op {
	case qAll:
		return "all"
	case qNone:
		return "none"
	case qAnd, qOr:
		op := "and"
		if q.op == qOr {
			op = "or"
		}
		var parts []string
		for _, t := range q.trigrams {
			parts = append(parts, string([]byte{byte(t >> 16), byte(t >> 8), byte(t)}))
		}
		for _, s := range q.sub {
			parts = append(parts, s.String())
		}
		return fmt.Sprintf("%s(%v)", op, parts)
	}
	return "?"
}
