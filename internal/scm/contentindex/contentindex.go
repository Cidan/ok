// Package contentindex is the model of ok scm's content index (docs/scm.md, "The
// content index"): for each of a repository's files, its size and its chunks, and for each
// chunk and each tree a publish stored in a pack, where it is stored — with what garbage
// collection needs of all three, whether it is pinned and the last day anything wrote or
// referenced it. The index is a log of what each
// transaction changed, which the database holds, and runs: sorted files in the object store,
// each record an entry as a fold found it, a newer run's record superseding an older's. A
// replica holds the log since the last fold (Log) and reads the runs through. This package is
// the entries, their encodings, and applying an entry's updates to its newest record
// (Resolve); the store reads and writes them.
//
// Puts, touches, and pins commute: an entry's body is kept once known — two copies of one
// chunk are both valid locations — pinning only sets, and a touch keeps the latest day. A
// move (compaction rewrote a chunk) and a drop (a sweep deleted an entry) do not, so log rows
// apply in the order they were written: the order of any two transactions that changed one
// entry, since the second waited on the first's row lock and logged after it committed. Each
// row applies once.
package contentindex

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// Day is a UTC day, as days since 1970-01-01: the granularity of freshness.
type Day uint32

// DayOf is the UTC day t falls on.
func DayOf(t time.Time) Day {
	return Day(t.UTC().Unix() / 86400)
}

// Time is the day's first instant, UTC.
func (d Day) Time() time.Time { return time.Unix(int64(d)*86400, 0).UTC() }

// Location is where a chunk or a tree is stored: its pack (by the pack's number), its record's
// offset, and how the record stores it.
type Location struct {
	Pack   uint64
	Offset int64
	Stored uint32
	Raw    uint32
	Codec  uint8
}

// FileChunk is one chunk of a file: its ID and length.
type FileChunk struct {
	ID     encoding.ID
	Length uint32
}

// Entry is what the index holds of one file, chunk, or tree. Body is whether its contents are
// known — a chunk's or a tree's location (in a network's index; a repository's holds none), a
// file's size and chunks — rather than only a pin or a touch that arrived before them.
type Entry struct {
	Kind   encoding.Kind
	Body   bool
	Pinned bool
	Day    Day
	Loc    Location    // a chunk's or a tree's (Located)
	Size   uint64      // a file's
	Chunks []FileChunk // a file's, in order
}

// Located reports whether entries of kind have a location: chunks, and trees, which a publish
// stores in its pack beside its chunks.
func Located(k encoding.Kind) bool { return k == encoding.KindChunk || k == encoding.KindTree }

// combine folds b into a: the body a has (or b's), pinned if either is, the later day.
func combine(a, b Entry) Entry {
	if !a.Body && b.Body {
		a.Body, a.Loc, a.Size, a.Chunks = true, b.Loc, b.Size, b.Chunks
	}
	a.Pinned = a.Pinned || b.Pinned
	a.Day = max(a.Day, b.Day)
	return a
}

// Equal reports whether two entries say the same.
func (e Entry) Equal(o Entry) bool {
	if e.Kind != o.Kind || e.Body != o.Body || e.Pinned != o.Pinned || e.Day != o.Day || e.Loc != o.Loc || e.Size != o.Size || len(e.Chunks) != len(o.Chunks) {
		return false
	}
	for i := range e.Chunks {
		if e.Chunks[i] != o.Chunks[i] {
			return false
		}
	}
	return true
}

// Op is what a logged update does.
type Op uint8

const (
	// Put records a new file or chunk: its body, day, and whether it is pinned.
	Put Op = 1
	// Touch records that something referenced it on Day.
	Touch Op = 2
	// Pin records that it became pinned.
	Pin Op = 3
	// Move records a chunk's or a tree's new location: compaction rewrote it. It moves what the
	// index holds, and nothing of what it lacks.
	Move Op = 4
	// Drop records that it is gone: a sweep deleted it.
	Drop Op = 5
)

// Update is one change a transaction logs.
type Update struct {
	Op    Op
	ID    encoding.ID
	Entry Entry // Put: all of it; Touch: Kind and Day; Pin and Drop: Kind; Move: Kind and Loc
}

// entry is the update as an entry to combine.
func (u Update) entry() Entry {
	switch u.Op {
	case Touch:
		return Entry{Kind: u.Entry.Kind, Day: u.Entry.Day}
	case Pin:
		return Entry{Kind: u.Entry.Kind, Pinned: true}
	}
	e := u.Entry
	e.Body = true
	return e
}

const (
	logVersion = 1

	flagPinned = 1 << 0
	flagBody   = 1 << 1
)

var errMalformed = errors.New("contentindex: malformed")

func validKind(k encoding.Kind) bool { return k == encoding.KindFile || Located(k) }

// appendEntry encodes an entry's kind, flags, day, and body.
func appendEntry(b []byte, e Entry) []byte {
	var flags byte
	if e.Pinned {
		flags |= flagPinned
	}
	if e.Body {
		flags |= flagBody
	}
	b = append(b, byte(e.Kind), flags)
	b = binary.AppendUvarint(b, uint64(e.Day))
	if !e.Body {
		return b
	}
	if Located(e.Kind) {
		b = binary.AppendUvarint(b, e.Loc.Pack)
		b = binary.AppendUvarint(b, uint64(e.Loc.Offset))
		b = binary.AppendUvarint(b, uint64(e.Loc.Stored))
		b = binary.AppendUvarint(b, uint64(e.Loc.Raw))
		return append(b, e.Loc.Codec)
	}
	b = binary.AppendUvarint(b, e.Size)
	b = binary.AppendUvarint(b, uint64(len(e.Chunks)))
	for _, c := range e.Chunks {
		b = append(b, c.ID[:]...)
		b = binary.AppendUvarint(b, uint64(c.Length))
	}
	return b
}

// reader decodes the encodings, failing once on anything malformed.
type reader struct {
	b   []byte
	err error
}

func (r *reader) fail() {
	if r.err == nil {
		r.err = errMalformed
	}
}

func (r *reader) byte() byte {
	if r.err != nil || len(r.b) < 1 {
		r.fail()
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

// u32 reads a uvarint that must fit 32 bits.
func (r *reader) u32() uint32 {
	v := r.uvarint()
	if v > 1<<32-1 {
		r.fail()
		return 0
	}
	return uint32(v)
}

func (r *reader) id() encoding.ID {
	var id encoding.ID
	if r.err != nil || len(r.b) < len(id) {
		r.fail()
		return id
	}
	copy(id[:], r.b)
	r.b = r.b[len(id):]
	return id
}

func (r *reader) entry() Entry {
	var e Entry
	e.Kind = encoding.Kind(r.byte())
	flags := r.byte()
	if r.err == nil && (!validKind(e.Kind) || flags&^(flagPinned|flagBody) != 0) {
		r.fail()
	}
	e.Pinned, e.Body = flags&flagPinned != 0, flags&flagBody != 0
	if d := r.uvarint(); d > 1<<32-1 {
		r.fail()
	} else {
		e.Day = Day(d)
	}
	if !e.Body || r.err != nil {
		return e
	}
	if Located(e.Kind) {
		e.Loc.Pack = r.uvarint()
		off := r.uvarint()
		if off > 1<<62 {
			r.fail()
		}
		e.Loc.Offset = int64(off)
		e.Loc.Stored = r.u32()
		e.Loc.Raw = r.u32()
		e.Loc.Codec = r.byte()
		return e
	}
	e.Size = r.uvarint()
	n := r.uvarint()
	// A chunk is at least 33 bytes encoded: never more chunks than bytes left.
	if r.err != nil || n > uint64(len(r.b))/33 {
		r.fail()
		return e
	}
	var total uint64
	e.Chunks = make([]FileChunk, n)
	for i := range e.Chunks {
		e.Chunks[i].ID = r.id()
		e.Chunks[i].Length = r.u32()
		total += uint64(e.Chunks[i].Length)
	}
	if r.err == nil && total != e.Size {
		r.fail()
	}
	return e
}

// EncodeLog encodes updates as one log row's body.
func EncodeLog(updates []Update) []byte {
	b := []byte{logVersion}
	b = binary.AppendUvarint(b, uint64(len(updates)))
	for _, u := range updates {
		b = appendUpdate(b, u)
	}
	return b
}

// EncodeLogs encodes updates as log rows of at most max bytes each — an update larger than max in
// a row of its own — in their order: applied one after another, as their rows' IDs order them, they
// are EncodeLog's one row. A row's value has a size limit (Spanner's is 10 MiB); a transaction's
// updates, pinning a large tree's files, may be more.
func EncodeLogs(updates []Update, max int) [][]byte {
	var out [][]byte
	var row []byte
	n := 0
	flush := func() {
		if n > 0 {
			b := append([]byte{logVersion}, binary.AppendUvarint(nil, uint64(n))...)
			out = append(out, append(b, row...))
			row, n = nil, 0
		}
	}
	for _, u := range updates {
		enc := appendUpdate(nil, u)
		if n > 0 && 1+binary.MaxVarintLen64+len(row)+len(enc) > max {
			flush()
		}
		row, n = append(row, enc...), n+1
	}
	flush()
	return out
}

// appendUpdate appends an update's encoding in a log row.
func appendUpdate(b []byte, u Update) []byte {
	b = append(b, byte(u.Op))
	b = append(b, u.ID[:]...)
	switch u.Op {
	case Put:
		b = appendEntry(b, u.entry())
	case Touch:
		b = append(b, byte(u.Entry.Kind))
		b = binary.AppendUvarint(b, uint64(u.Entry.Day))
	case Pin, Drop:
		b = append(b, byte(u.Entry.Kind))
	case Move:
		b = appendEntry(b, Entry{Kind: u.Entry.Kind, Body: true, Loc: u.Entry.Loc})
	}
	return b
}

// DecodeLog decodes a log row's body.
func DecodeLog(b []byte) ([]Update, error) {
	r := &reader{b: b}
	if r.byte() != logVersion {
		return nil, fmt.Errorf("%w: log version", errMalformed)
	}
	n := r.uvarint()
	if r.err != nil || n > uint64(len(r.b))/34 {
		return nil, fmt.Errorf("%w log", errMalformed)
	}
	out := make([]Update, 0, n)
	for range n {
		u := Update{Op: Op(r.byte()), ID: r.id()}
		switch u.Op {
		case Put:
			u.Entry = r.entry()
			if !u.Entry.Body {
				r.fail()
			}
		case Touch:
			u.Entry.Kind = encoding.Kind(r.byte())
			if d := r.uvarint(); d > 1<<32-1 {
				r.fail()
			} else {
				u.Entry.Day = Day(d)
			}
		case Pin, Drop:
			u.Entry.Kind = encoding.Kind(r.byte())
		case Move:
			e := r.entry()
			if !Located(e.Kind) || !e.Body {
				r.fail()
			}
			u.Entry = Entry{Kind: e.Kind, Loc: e.Loc}
		default:
			r.fail()
		}
		if r.err == nil && !validKind(u.Entry.Kind) {
			r.fail()
		}
		if r.err != nil {
			return nil, fmt.Errorf("%w log", errMalformed)
		}
		out = append(out, u)
	}
	if len(r.b) != 0 {
		return nil, fmt.Errorf("%w log: %d bytes after its updates", errMalformed, len(r.b))
	}
	return out, nil
}

// EncodeEntry encodes an entry alone: a run's record. A bodiless one is a tombstone: the entry
// is gone, whatever an older run holds of it.
func EncodeEntry(e Entry) []byte { return appendEntry(nil, e) }

// DecodeEntry decodes EncodeEntry's encoding.
func DecodeEntry(b []byte) (Entry, error) {
	r := &reader{b: b}
	e := r.entry()
	if r.err != nil || len(r.b) != 0 {
		return Entry{}, fmt.Errorf("%w entry", errMalformed)
	}
	return e, nil
}

// Keyed is an entry with its ID.
type Keyed struct {
	ID    encoding.ID
	Entry Entry
}
