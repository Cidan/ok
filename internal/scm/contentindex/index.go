package contentindex

import (
	"bytes"
	"sort"
	"sync"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// Index is one repository's whole content index in memory: the blocks of its generation and
// every log row applied since, in the order they were written. Its methods are safe for
// concurrent use; an entry's Chunks are shared and must not be changed.
type Index struct {
	mu         sync.RWMutex
	generation int64
	entries    map[encoding.ID]Entry
	logs       map[int64]struct{}
}

// New returns an empty index of a generation.
func New(generation int64) *Index {
	return &Index{generation: generation, entries: map[encoding.ID]Entry{}, logs: map[int64]struct{}{}}
}

// Generation is the generation of the blocks it was built from.
func (x *Index) Generation() int64 { return x.generation }

// Len is how many entries it holds, bodiless ones included.
func (x *Index) Len() int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return len(x.entries)
}

func (x *Index) put(id encoding.ID, e Entry) {
	if have, ok := x.entries[id]; ok {
		e = combine(have, e)
	}
	x.entries[id] = e
}

// ApplyBlock adds a block's entries.
func (x *Index) ApplyBlock(body []byte) error {
	entries, err := DecodeBlock(body)
	if err != nil {
		return err
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, k := range entries {
		x.put(k.ID, k.Entry)
	}
	return nil
}

// ApplyLog applies a log row's updates, once: a row already applied is skipped.
func (x *Index) ApplyLog(id int64, body []byte) error {
	if x.HasLog(id) {
		return nil
	}
	updates, err := DecodeLog(body)
	if err != nil {
		return err
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if _, done := x.logs[id]; done {
		return nil
	}
	for _, u := range updates {
		switch u.Op {
		case Drop:
			delete(x.entries, u.ID)
		case Move:
			// A chunk dropped before compaction moved it stays dropped.
			if e, ok := x.entries[u.ID]; ok && e.Body {
				e.Loc = u.Entry.Loc
				x.entries[u.ID] = e
			}
		default:
			x.put(u.ID, u.entry())
		}
	}
	x.logs[id] = struct{}{}
	return nil
}

// HasLog reports whether a log row is applied.
func (x *Index) HasLog(id int64) bool {
	x.mu.RLock()
	defer x.mu.RUnlock()
	_, ok := x.logs[id]
	return ok
}

// Get returns a file's or chunk's entry, if the index knows its body.
func (x *Index) Get(id encoding.ID) (Entry, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	e, ok := x.entries[id]
	return e, ok && e.Body
}

// Lookup returns whatever the index holds of an ID, a bodiless entry included.
func (x *Index) Lookup(id encoding.ID) (Entry, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	e, ok := x.entries[id]
	return e, ok
}

// Range calls f with every entry that has a body, in no order, until f returns false. It holds
// the index's read lock: f must not call back into it.
func (x *Index) Range(f func(id encoding.ID, e Entry) bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	for id, e := range x.entries {
		if e.Body && !f(id, e) {
			return
		}
	}
}

// Sorted returns every entry, in ID order.
func (x *Index) Sorted() []Keyed {
	x.mu.RLock()
	out := make([]Keyed, 0, len(x.entries))
	for id, e := range x.entries {
		out = append(out, Keyed{ID: id, Entry: e})
	}
	x.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].ID[:], out[j].ID[:]) < 0 })
	return out
}

// Blocks splits entries, in ID order, into blocks of at most BlockEntries.
func Blocks(entries []Keyed) [][]Keyed {
	var out [][]Keyed
	for start := 0; start < len(entries); start += BlockEntries {
		out = append(out, entries[start:min(start+BlockEntries, len(entries))])
	}
	return out
}
