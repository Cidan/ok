package contentindex

import (
	"bytes"
	"sort"
	"sync"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// Index is a content index in memory, as far as the database's tables hold it — the blocks of its
// generation and every log row applied since, once each, in the order they apply — with the moves
// logged of chunks it lacks, whose entries are in segments, and the drops logged since, which hide
// whatever a segment holds of what they dropped until a fold writes their tombstones. Its methods
// are safe for concurrent use; an entry's Chunks are shared and must not be changed.
type Index struct {
	mu         sync.RWMutex
	generation int64
	entries    map[encoding.ID]Entry
	moved      map[encoding.ID]Location
	dropped    map[encoding.ID]encoding.Kind
}

// New returns an empty index of a generation.
func New(generation int64) *Index {
	return &Index{generation: generation, entries: map[encoding.ID]Entry{}, moved: map[encoding.ID]Location{},
		dropped: map[encoding.ID]encoding.Kind{}}
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

// ApplyLog applies a log row's updates. The reader applies each row once — a drop applied again
// after a later put of the entry would drop it — as its place in the log says (backend.LogPlace).
func (x *Index) ApplyLog(body []byte) error {
	updates, err := DecodeLog(body)
	if err != nil {
		return err
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, u := range updates {
		switch u.Op {
		case Drop:
			delete(x.entries, u.ID)
			delete(x.moved, u.ID)
			x.dropped[u.ID] = u.Entry.Kind
		case Move:
			if _, gone := x.dropped[u.ID]; gone {
				// A drop is final until the entry is put again: there is nothing to move.
				continue
			}
			if e, ok := x.entries[u.ID]; ok && e.Body {
				e.Loc = u.Entry.Loc
				x.entries[u.ID] = e
			} else {
				x.moved[u.ID] = u.Entry.Loc
			}
		default:
			if u.Op == Put {
				delete(x.dropped, u.ID)
			}
			x.put(u.ID, u.entry())
		}
	}
	return nil
}

// Dropped reports whether a drop of id was logged since, and none of its entries put after it:
// whatever a segment holds of it is gone.
func (x *Index) Dropped(id encoding.ID) bool {
	x.mu.RLock()
	defer x.mu.RUnlock()
	_, ok := x.dropped[id]
	return ok
}

// DroppedIDs returns what Dropped reports, in ID order, each with its kind.
func (x *Index) DroppedIDs() []Keyed {
	x.mu.RLock()
	out := make([]Keyed, 0, len(x.dropped))
	for id, k := range x.dropped {
		out = append(out, Keyed{ID: id, Entry: Entry{Kind: k}})
	}
	x.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].ID[:], out[j].ID[:]) < 0 })
	return out
}

// Moved returns the location a move logged of a chunk the index lacks, if one was.
func (x *Index) Moved(id encoding.ID) (Location, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	l, ok := x.moved[id]
	return l, ok
}

// MovedIDs returns the chunks the index lacks that a move was logged of, in ID order.
func (x *Index) MovedIDs() []encoding.ID {
	x.mu.RLock()
	out := make([]encoding.ID, 0, len(x.moved))
	for id := range x.moved {
		out = append(out, id)
	}
	x.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
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
