package contentindex

import (
	"bytes"
	"slices"
	"sync"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// Log is a content index's log since its generation's fold, as a replica holds it: each entry's
// updates, in the order they apply, every row applied once. What it says of an entry is its updates
// applied to the entry's newest record in the runs (Resolve). Its methods are safe for concurrent
// use; the updates it returns must not be changed.
type Log struct {
	mu         sync.RWMutex
	generation int64
	updates    map[encoding.ID][]Update
}

// NewLog returns an empty log of a generation.
func NewLog(generation int64) *Log {
	return &Log{generation: generation, updates: map[encoding.ID][]Update{}}
}

// Generation is the generation whose fold the log follows.
func (l *Log) Generation() int64 { return l.generation }

// Len is how many entries the log changed.
func (l *Log) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.updates)
}

// Apply applies a log row's updates, after every row applied before it. The reader applies each
// row once — a drop applied again after a later put of the entry would drop it — as its place in
// the log says (backend.LogPlace).
func (l *Log) Apply(body []byte) error {
	updates, err := DecodeLog(body)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, u := range updates {
		// Appended past the length any reader holds (Updates caps it): a reader's stays as it was.
		l.updates[u.ID] = append(l.updates[u.ID], u)
	}
	return nil
}

// Updates are an entry's updates, in order; none when the log did not change it.
func (l *Log) Updates(id encoding.ID) []Update {
	l.mu.RLock()
	defer l.mu.RUnlock()
	ups := l.updates[id]
	return ups[:len(ups):len(ups)]
}

// IDs are the entries the log changed, in ID order.
func (l *Log) IDs() []encoding.ID {
	l.mu.RLock()
	out := make([]encoding.ID, 0, len(l.updates))
	for id := range l.updates {
		out = append(out, id)
	}
	l.mu.RUnlock()
	slices.SortFunc(out, func(a, b encoding.ID) int { return bytes.Compare(a[:], b[:]) })
	return out
}

// Resolve applies an entry's updates, in order, to its newest record in the runs — base, when one
// was found; a tombstone, bodiless, is none — and returns the entry and whether the index holds it,
// its body known. A drop leaves nothing of what was before it; a put after a drop starts anew. A
// move relocates a chunk the index holds, and is nothing to one it lacks. A touch or a pin of what
// the index lacks is kept, bodiless, for a put after it, as puts, touches, and pins commute.
func Resolve(base Entry, found bool, updates []Update) (Entry, bool) {
	var e Entry
	have := found && base.Body
	if have {
		e = base
	}
	for _, u := range updates {
		switch u.Op {
		case Drop:
			e, have = Entry{}, false
		case Move:
			if have && e.Body {
				e.Loc = u.Entry.Loc
			}
		default:
			if have {
				e = combine(e, u.entry())
			} else {
				e, have = u.entry(), true
			}
		}
	}
	return e, have && e.Body
}
