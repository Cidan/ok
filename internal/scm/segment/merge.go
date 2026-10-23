package segment

import (
	"bytes"
	"container/heap"
	"context"
)

// Iterator walks records in key order.
type Iterator interface {
	// Next moves to the next record, reporting false at the end or on an error.
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
}

// Record is a key and its value.
type Record struct{ Key, Value []byte }

// Records iterates records already in key order.
func Records(rs []Record) Iterator { return &records{rs: rs, i: -1} }

type records struct {
	rs []Record
	i  int
}

func (r *records) Next() bool    { r.i++; return r.i < len(r.rs) }
func (r *records) Key() []byte   { return r.rs[r.i].Key }
func (r *records) Value() []byte { return r.rs[r.i].Value }
func (r *records) Err() error    { return nil }

// Iter walks the segment's records whose keys are at least from and before to (nil: the end).
func (r *Reader) Iter(ctx context.Context, from, to []byte) Iterator {
	return &readerIter{r: r, w: walker{r: r}, ctx: ctx, to: to, from: from, blk: max(r.blockOf(from), 0), rec: -1}
}

type readerIter struct {
	r        *Reader
	w        walker
	ctx      context.Context
	from, to []byte
	blk      int
	b        *Block
	rec      int
	key, val []byte
	err      error
}

func (it *readerIter) Next() bool {
	for it.err == nil && it.blk < len(it.r.fence) {
		if it.b == nil {
			if it.to != nil && bytes.Compare(it.r.fence[it.blk].first, it.to) >= 0 {
				return false
			}
			if it.b, it.err = it.w.get(it.ctx, it.blk, 1, it.within); it.err != nil {
				return false
			}
			it.rec = it.b.search(it.from) - 1
		}
		it.rec++
		if it.rec < len(it.b.offs) {
			it.key, it.val = it.b.record(it.rec)
			if it.to != nil && bytes.Compare(it.key, it.to) >= 0 {
				it.blk = len(it.r.fence)
				return false
			}
			if !it.r.kept(it.key, it.val) {
				continue
			}
			return true
		}
		it.b = nil
		it.blk++
	}
	return false
}

// within says whether block j may hold a key before the walk's end.
func (it *readerIter) within(j int) bool {
	return it.to == nil || bytes.Compare(it.r.fence[j].first, it.to) < 0
}

func (it *readerIter) Key() []byte   { return it.key }
func (it *readerIter) Value() []byte { return it.val }
func (it *readerIter) Err() error    { return it.err }

// IterDesc walks the segment's records whose keys are at least from (nil: the start) and before
// to (nil: the end), in descending key order.
func (r *Reader) IterDesc(ctx context.Context, from, to []byte) Iterator {
	last := len(r.fence) - 1
	if to != nil {
		last = r.blockOf(to)
	}
	return &descIter{r: r, w: walker{r: r}, ctx: ctx, from: from, to: to, blk: last}
}

type descIter struct {
	r        *Reader
	w        walker
	ctx      context.Context
	from, to []byte
	blk      int
	b        *Block
	rec      int
	key, val []byte
	err      error
}

func (it *descIter) Next() bool {
	for it.err == nil && it.blk >= 0 {
		if it.b == nil {
			if it.b, it.err = it.w.get(it.ctx, it.blk, -1, it.within); it.err != nil {
				return false
			}
			it.rec = len(it.b.offs)
			if it.to != nil {
				it.rec = it.b.search(it.to)
			}
		}
		it.rec--
		if it.rec >= 0 {
			it.key, it.val = it.b.record(it.rec)
			if it.from != nil && bytes.Compare(it.key, it.from) < 0 {
				it.blk = -1
				return false
			}
			if !it.r.kept(it.key, it.val) {
				continue
			}
			return true
		}
		it.b = nil
		it.blk--
	}
	return false
}

// within says whether block j may hold a key at or after the walk's start.
func (it *descIter) within(j int) bool {
	return it.from == nil || j+1 >= len(it.r.fence) || bytes.Compare(it.r.fence[j+1].first, it.from) > 0
}

func (it *descIter) Key() []byte   { return it.key }
func (it *descIter) Value() []byte { return it.val }
func (it *descIter) Err() error    { return it.err }

// Merge walks the records of its sources, newest first, as one run in key order: of a key several
// hold, the newest's record alone.
func Merge(newestFirst ...Iterator) Iterator { return merge(false, newestFirst) }

// MergeDesc is Merge over sources walked in descending key order (IterDesc), in that order.
func MergeDesc(newestFirst ...Iterator) Iterator { return merge(true, newestFirst) }

func merge(desc bool, newestFirst []Iterator) Iterator {
	m := &merged{h: sources{desc: desc}}
	for age, it := range newestFirst {
		if it.Next() {
			m.h.s = append(m.h.s, source{it: it, age: age})
		} else if it.Err() != nil {
			m.err = it.Err()
		}
	}
	heap.Init(&m.h)
	return m
}

type source struct {
	it  Iterator
	age int // 0 the newest
}

type sources struct {
	s    []source
	desc bool
}

func (h sources) Len() int { return len(h.s) }
func (h sources) Less(i, j int) bool {
	if c := bytes.Compare(h.s[i].it.Key(), h.s[j].it.Key()); c != 0 {
		return (c < 0) != h.desc
	}
	return h.s[i].age < h.s[j].age
}
func (h sources) Swap(i, j int) { h.s[i], h.s[j] = h.s[j], h.s[i] }
func (h *sources) Push(x any)   { h.s = append(h.s, x.(source)) }
func (h *sources) Pop() any {
	old := h.s
	x := old[len(old)-1]
	h.s = old[:len(old)-1]
	return x
}

type merged struct {
	h        sources
	key, val []byte
	err      error
}

func (m *merged) Next() bool {
	if m.err != nil || m.h.Len() == 0 {
		return false
	}
	top := m.h.s[0]
	m.key, m.val = bytes.Clone(top.it.Key()), bytes.Clone(top.it.Value())
	// Every source at this key moves past it: the newest's record stands for them all.
	for m.h.Len() > 0 && bytes.Equal(m.h.s[0].it.Key(), m.key) {
		s := m.h.s[0]
		if s.it.Next() {
			heap.Fix(&m.h, 0)
			continue
		}
		if err := s.it.Err(); err != nil {
			m.err = err
		}
		heap.Pop(&m.h)
	}
	return true
}

func (m *merged) Key() []byte   { return m.key }
func (m *merged) Value() []byte { return m.val }
func (m *merged) Err() error    { return m.err }

// Filter walks the records of it that keep keeps.
func Filter(it Iterator, keep func(key, value []byte) bool) Iterator {
	return &filtered{it: it, keep: keep}
}

type filtered struct {
	it   Iterator
	keep func(key, value []byte) bool
}

func (f *filtered) Next() bool {
	for f.it.Next() {
		if f.keep(f.it.Key(), f.it.Value()) {
			return true
		}
	}
	return false
}

func (f *filtered) Key() []byte   { return f.it.Key() }
func (f *filtered) Value() []byte { return f.it.Value() }
func (f *filtered) Err() error    { return f.it.Err() }

// Write adds the records of it to w and finishes the segment.
func Write(w *Writer, it Iterator) (Meta, error) {
	for it.Next() {
		if err := w.Add(it.Key(), it.Value()); err != nil {
			return Meta{}, err
		}
	}
	if err := it.Err(); err != nil {
		return Meta{}, err
	}
	return w.Finish()
}
