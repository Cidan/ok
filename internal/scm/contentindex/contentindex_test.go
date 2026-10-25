package contentindex

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/Cidan/ok/internal/scm/encoding"
)

func id(n byte) encoding.ID {
	var x encoding.ID
	x[0], x[31] = n, n^0x5a
	return x
}

func chunkEntry(pack uint64, off int64, day Day) Entry {
	return Entry{Kind: encoding.KindChunk, Body: true, Day: day, Loc: Location{Pack: pack, Offset: off, Stored: 900, Raw: 2100, Codec: 1}}
}

func fileEntry(day Day, chunks ...FileChunk) Entry {
	var size uint64
	for _, c := range chunks {
		size += uint64(c.Length)
	}
	return Entry{Kind: encoding.KindFile, Body: true, Day: day, Size: size, Chunks: chunks}
}

// Logs round-trip.
func TestEncodings(t *testing.T) {
	updates := []Update{
		{Op: Put, ID: id(1), Entry: chunkEntry(7, 1<<33, 20000)},
		{Op: Put, ID: id(2), Entry: fileEntry(20001, FileChunk{id(1), 2100}, FileChunk{id(3), 5})},
		{Op: Touch, ID: id(4), Entry: Entry{Kind: encoding.KindFile, Day: 20002}},
		{Op: Pin, ID: id(5), Entry: Entry{Kind: encoding.KindChunk}},
	}
	got, err := DecodeLog(EncodeLog(updates))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(updates) {
		t.Fatalf("%d updates, want %d", len(got), len(updates))
	}
	for i := range got {
		if got[i].Op != updates[i].Op || got[i].ID != updates[i].ID || !got[i].entry().Equal(updates[i].entry()) {
			t.Errorf("update %d: %+v, want %+v", i, got[i], updates[i])
		}
	}
}

// EncodeLogs splits updates into rows of at most max bytes, an update too large for one in a row
// of its own, which decode, one after another, to the updates in order.
func TestEncodeLogs(t *testing.T) {
	var updates []Update
	for i := range 1000 {
		updates = append(updates, Update{Op: Pin, ID: id(byte(i)), Entry: Entry{Kind: encoding.KindChunk}})
	}
	big := make([]FileChunk, 200)
	for i := range big {
		big[i] = FileChunk{id(byte(i)), 10}
	}
	updates = append(updates, Update{Op: Put, ID: id(1), Entry: fileEntry(1, big...)})
	const max = 4096
	rows := EncodeLogs(updates, max)
	if len(rows) < 2 {
		t.Fatalf("%d rows, want several of at most %d bytes", len(rows), max)
	}
	var got []Update
	for i, row := range rows {
		us, err := DecodeLog(row)
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if len(row) > max && len(us) != 1 {
			t.Errorf("row %d: %d bytes of %d updates, over %d", i, len(row), len(us), max)
		}
		got = append(got, us...)
	}
	if len(got) != len(updates) {
		t.Fatalf("%d updates, want %d", len(got), len(updates))
	}
	for i := range got {
		if got[i].Op != updates[i].Op || got[i].ID != updates[i].ID || !got[i].entry().Equal(updates[i].entry()) {
			t.Errorf("update %d: %+v, want %+v", i, got[i], updates[i])
		}
	}
	if one := EncodeLogs(updates[:3], max); len(one) != 1 || !bytes.Equal(one[0], EncodeLog(updates[:3])) {
		t.Errorf("a few updates: %d rows, want EncodeLog's one", len(one))
	}
}

// Anything malformed is refused, never half read: truncations, trailing bytes, unknown kinds
// and ops, a file whose chunks do not add up.
func TestMalformed(t *testing.T) {
	log := EncodeLog([]Update{{Op: Put, ID: id(2), Entry: fileEntry(1, FileChunk{id(1), 10})}})
	for n := range log {
		if _, err := DecodeLog(log[:n]); err == nil {
			t.Fatalf("a log cut to %d of %d bytes decoded", n, len(log))
		}
	}
	if _, err := DecodeLog(append(append([]byte(nil), log...), 0)); err == nil {
		t.Fatal("a log with a trailing byte decoded")
	}
	bad := EncodeLog([]Update{{Op: Put, ID: id(2), Entry: Entry{Kind: encoding.KindFile, Size: 11, Chunks: []FileChunk{{id(1), 10}}}}})
	if _, err := DecodeLog(bad); err == nil {
		t.Fatal("a file whose chunks do not add up to its size decoded")
	}
	kind := EncodeLog([]Update{{Op: Pin, ID: id(1), Entry: Entry{Kind: encoding.KindSymlink}}})
	if _, err := DecodeLog(kind); err == nil {
		t.Fatal("a symlink's update decoded")
	}
	tree := Entry{Kind: encoding.KindTree, Body: true, Day: 4, Loc: Location{Pack: 3, Offset: 9, Stored: 40, Raw: 70}}
	if got, err := DecodeEntry(EncodeEntry(tree)); err != nil || !got.Equal(tree) {
		t.Fatalf("a tree's entry, located: %+v, %v", got, err)
	}
	op := EncodeLog([]Update{{Op: Pin, ID: id(1), Entry: Entry{Kind: encoding.KindChunk}}})
	op[2] = 9
	if _, err := DecodeLog(op); err == nil {
		t.Fatal("an unknown op decoded")
	}
}

// A move replaces a chunk's location and keeps the rest, and is nothing to a chunk the index lacks;
// a drop removes an entry, whatever its record held, and a put after it stores it anew; a tombstone
// is no record.
func TestResolve(t *testing.T) {
	moved := Location{Pack: 9, Offset: 77, Stored: 50, Raw: 2100, Codec: 2}
	pinned := chunkEntry(3, 10, 100)
	pinned.Pinned = true
	ck := Entry{Kind: encoding.KindChunk}
	for _, tc := range []struct {
		name  string
		base  *Entry
		ups   []Update
		want  Entry
		holds bool
	}{
		{"a record alone", &pinned, nil, pinned, true},
		{"a move of a held chunk", &pinned, []Update{{Op: Move, ID: id(1), Entry: Entry{Kind: encoding.KindChunk, Loc: moved}}},
			Entry{Kind: encoding.KindChunk, Body: true, Pinned: true, Day: 100, Loc: moved}, true},
		{"a move of a chunk the index lacks", nil, []Update{{Op: Move, ID: id(1), Entry: Entry{Kind: encoding.KindChunk, Loc: moved}}},
			Entry{}, false},
		{"a drop of a pinned record", &pinned, []Update{{Op: Drop, ID: id(1), Entry: ck}}, Entry{}, false},
		{"a put after a drop starts anew", &pinned, []Update{{Op: Drop, ID: id(1), Entry: ck}, {Op: Put, ID: id(1), Entry: chunkEntry(5, 30, 101)}},
			chunkEntry(5, 30, 101), true},
		{"a move after a drop", &pinned, []Update{{Op: Drop, ID: id(1), Entry: ck}, {Op: Move, ID: id(1), Entry: Entry{Kind: encoding.KindChunk, Loc: moved}}},
			Entry{}, false},
		{"a touch keeps the later day", &pinned, []Update{{Op: Touch, ID: id(1), Entry: Entry{Kind: encoding.KindChunk, Day: 99}},
			{Op: Touch, ID: id(1), Entry: Entry{Kind: encoding.KindChunk, Day: 120}}},
			Entry{Kind: encoding.KindChunk, Body: true, Pinned: true, Day: 120, Loc: pinned.Loc}, true},
		{"a put of a held entry keeps its body", &pinned, []Update{{Op: Put, ID: id(1), Entry: chunkEntry(8, 0, 130)}},
			Entry{Kind: encoding.KindChunk, Body: true, Pinned: true, Day: 130, Loc: pinned.Loc}, true},
		{"a tombstone is no record", &Entry{Kind: encoding.KindChunk}, []Update{{Op: Touch, ID: id(1), Entry: Entry{Kind: encoding.KindChunk, Day: 5}}},
			Entry{}, false},
		{"a pin before its body", nil, []Update{{Op: Pin, ID: id(1), Entry: ck}, {Op: Put, ID: id(1), Entry: chunkEntry(4, 0, 150)}},
			Entry{Kind: encoding.KindChunk, Body: true, Pinned: true, Day: 150, Loc: chunkEntry(4, 0, 150).Loc}, true},
	} {
		var base Entry
		if tc.base != nil {
			base = *tc.base
		}
		got, holds := Resolve(base, tc.base != nil, tc.ups)
		if holds != tc.holds || (holds && !got.Equal(tc.want)) {
			t.Errorf("%s: %+v %v, want %+v %v", tc.name, got, holds, tc.want, tc.holds)
		}
	}
	bad := EncodeLog([]Update{{Op: Move, ID: id(1), Entry: Entry{Kind: encoding.KindFile}}})
	if _, err := DecodeLog(bad); err == nil {
		t.Fatal("a file's move decoded")
	}
	treeMove := []Update{{Op: Move, ID: id(1), Entry: Entry{Kind: encoding.KindTree, Loc: Location{Pack: 7, Offset: 3}}}}
	if got, err := DecodeLog(EncodeLog(treeMove)); err != nil || got[0].Entry.Kind != encoding.KindTree || got[0].Entry.Loc.Pack != 7 {
		t.Fatalf("a tree's move: %+v, %v", got, err)
	}
}

// A log keeps each entry's updates in the order its rows applied, and an entry's updates read before
// more rows apply stay as they were read.
func TestLog(t *testing.T) {
	l := NewLog(4)
	rows := [][]Update{
		{{Op: Put, ID: id(2), Entry: chunkEntry(1, 0, 10)}, {Op: Touch, ID: id(1), Entry: Entry{Kind: encoding.KindFile, Day: 11}}},
		{{Op: Pin, ID: id(2), Entry: Entry{Kind: encoding.KindChunk}}},
	}
	if err := l.Apply(EncodeLog(rows[0])); err != nil {
		t.Fatal(err)
	}
	before := l.Updates(id(2))
	if err := l.Apply(EncodeLog(rows[1])); err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 || before[0].Op != Put {
		t.Fatalf("updates read before the second row: %+v", before)
	}
	if ups := l.Updates(id(2)); len(ups) != 2 || ups[0].Op != Put || ups[1].Op != Pin {
		t.Fatalf("chunk 2's updates: %+v", ups)
	}
	if ids := l.IDs(); len(ids) != 2 || ids[0] != id(1) || ids[1] != id(2) || l.Len() != 2 || l.Generation() != 4 {
		t.Fatalf("IDs %v, Len %d, generation %d", ids, l.Len(), l.Generation())
	}
	if ups := l.Updates(id(9)); len(ups) != 0 {
		t.Fatalf("an entry the log did not change: %+v", ups)
	}
}

// Folding is resolving: a history folded at any points — each fold writing what Resolve gives, a
// record for what the index holds and a tombstone for what it dropped — then resolved over the
// last fold's records, gives what resolving the whole history at once does. The histories are a
// writer's: puts of what the index lacks, or of what it holds; touches, pins, and moves of what it
// holds; drops.
func TestFoldingIsResolving(t *testing.T) {
	rnd := rand.New(rand.NewPCG(1, 2))
	const ids = 6
	for trial := range 2000 {
		var history []Update
		held := map[byte]bool{}
		for range 1 + rnd.IntN(30) {
			n := byte(rnd.IntN(ids))
			kind := encoding.KindChunk
			u := Update{ID: id(n)}
			switch op := rnd.IntN(6); {
			case op == 0 || !held[n]:
				u.Op, u.Entry = Put, chunkEntry(uint64(rnd.IntN(4)), int64(rnd.IntN(100)), Day(100+rnd.IntN(10)))
				held[n] = true
			case op == 1:
				u.Op, u.Entry = Touch, Entry{Kind: kind, Day: Day(100 + rnd.IntN(20))}
			case op == 2:
				u.Op, u.Entry = Pin, Entry{Kind: kind}
			case op == 3:
				u.Op, u.Entry = Move, Entry{Kind: kind, Loc: Location{Pack: uint64(10 + rnd.IntN(4))}}
			default:
				u.Op, u.Entry = Drop, Entry{Kind: kind}
				held[n] = false
			}
			history = append(history, u)
		}
		// Folds after random updates.
		records := map[encoding.ID]Entry{}
		start := 0
		for start < len(history) {
			end := start + 1 + rnd.IntN(len(history)-start)
			byID := map[encoding.ID][]Update{}
			for _, u := range history[start:end] {
				byID[u.ID] = append(byID[u.ID], u)
			}
			for k, ups := range byID {
				base, found := records[k]
				e, holds := Resolve(base, found, ups)
				switch {
				case holds:
					records[k] = e
				case found:
					records[k] = Entry{Kind: base.Kind} // a tombstone
				}
			}
			start = end
		}
		for n := range byte(ids) {
			k := id(n)
			var all []Update
			for _, u := range history {
				if u.ID == k {
					all = append(all, u)
				}
			}
			want, wantHolds := Resolve(Entry{}, false, all)
			rec, found := records[k]
			got, holds := Resolve(rec, found, nil)
			if holds != wantHolds || (holds && !got.Equal(want)) {
				t.Fatalf("trial %d, entry %d: folded %+v %v, at once %+v %v\nhistory %+v", trial, n, got, holds, want, wantHolds, all)
			}
		}
	}
}

// An entry alone round-trips, and trailing bytes are refused.
func TestEntryEncoding(t *testing.T) {
	for _, e := range []Entry{chunkEntry(3, 99, 7), fileEntry(5, FileChunk{ID: id(1), Length: 10}, FileChunk{ID: id(2), Length: 20}), {Kind: encoding.KindFile, Day: 3}} {
		got, err := DecodeEntry(EncodeEntry(e))
		if err != nil || !got.Equal(e) {
			t.Fatalf("%+v round-tripped to %+v (%v)", e, got, err)
		}
		if _, err := DecodeEntry(append(EncodeEntry(e), 0)); err == nil {
			t.Fatalf("%+v with a byte after it decoded", e)
		}
	}
}

// Puts, touches, and pins of an entry give the same entry in any order, any number of times; a pin
// or touch that arrives before its entry's body waits for it, and one alone gives none.
func TestOrderDoesNotMatter(t *testing.T) {
	base := fileEntry(100, FileChunk{id(1), 2100})
	ups := []Update{
		{Op: Touch, ID: id(2), Entry: Entry{Kind: encoding.KindFile, Day: 140}},
		{Op: Pin, ID: id(2), Entry: Entry{Kind: encoding.KindFile}},
		{Op: Put, ID: id(2), Entry: fileEntry(150, FileChunk{id(3), 2100})},
		{Op: Touch, ID: id(2), Entry: Entry{Kind: encoding.KindFile, Day: 120}},
	}
	want, ok := Resolve(base, true, ups)
	if !ok || !want.Pinned || want.Day != 150 || want.Chunks[0].ID != id(1) {
		t.Fatalf("resolved: %+v %v", want, ok)
	}
	for range 50 {
		order := rand.Perm(len(ups))
		order = append(order, order[:2]...)
		shuffled := make([]Update, len(order))
		for i, j := range order {
			shuffled[i] = ups[j]
		}
		if got, ok := Resolve(base, true, shuffled); !ok || !got.Equal(want) {
			t.Fatalf("order %v: %+v, want %+v", order, got, want)
		}
	}
	early := []Update{{Op: Pin, ID: id(6), Entry: Entry{Kind: encoding.KindChunk}}, {Op: Put, ID: id(6), Entry: chunkEntry(4, 0, 150)}}
	if e, ok := Resolve(Entry{}, false, early); !ok || !e.Pinned || e.Loc.Pack != 4 {
		t.Fatalf("a chunk pinned before its body arrived: %+v %v", e, ok)
	}
	if _, ok := Resolve(Entry{}, false, ups[:1]); ok {
		t.Fatal("a touch alone gave a file a body")
	}
}
