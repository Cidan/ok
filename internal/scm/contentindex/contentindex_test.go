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

// Logs and blocks round-trip.
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
	block := []Keyed{
		{ID: id(1), Entry: chunkEntry(7, 99, 20000)},
		{ID: id(2), Entry: fileEntry(20001, FileChunk{id(1), 2100})},
		{ID: id(9), Entry: Entry{Kind: encoding.KindChunk, Pinned: true, Day: 3}},
	}
	back, err := DecodeBlock(EncodeBlock(block))
	if err != nil {
		t.Fatal(err)
	}
	for i := range block {
		if back[i].ID != block[i].ID || !back[i].Entry.Equal(block[i].Entry) {
			t.Errorf("entry %d: %+v, want %+v", i, back[i], block[i])
		}
	}
}

// Anything malformed is refused, never half read: truncations, trailing bytes, unknown kinds
// and ops, a file whose chunks do not add up, a block out of order.
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
	kind := EncodeLog([]Update{{Op: Pin, ID: id(1), Entry: Entry{Kind: encoding.KindTree}}})
	if _, err := DecodeLog(kind); err == nil {
		t.Fatal("a tree's update decoded")
	}
	op := EncodeLog([]Update{{Op: Pin, ID: id(1), Entry: Entry{Kind: encoding.KindChunk}}})
	op[2] = 9
	if _, err := DecodeLog(op); err == nil {
		t.Fatal("an unknown op decoded")
	}
	block := EncodeBlock([]Keyed{{ID: id(2), Entry: chunkEntry(1, 0, 1)}, {ID: id(1), Entry: chunkEntry(1, 0, 1)}})
	if _, err := DecodeBlock(block); err == nil {
		t.Fatal("a block out of order decoded")
	}
}

// A move replaces a chunk's location and keeps the rest; a drop removes an entry, and a put
// after it stores it anew; both round-trip.
func TestMoveAndDrop(t *testing.T) {
	x := New(1)
	steps := [][]Update{
		{{Op: Put, ID: id(1), Entry: chunkEntry(3, 10, 100)}, {Op: Pin, ID: id(1), Entry: Entry{Kind: encoding.KindChunk}}},
		{{Op: Move, ID: id(1), Entry: Entry{Kind: encoding.KindChunk, Loc: Location{Pack: 9, Offset: 77, Stored: 50, Raw: 2100, Codec: 2}}}},
		{{Op: Put, ID: id(2), Entry: chunkEntry(3, 20, 100)}, {Op: Drop, ID: id(2), Entry: Entry{Kind: encoding.KindChunk}}},
		{{Op: Put, ID: id(2), Entry: chunkEntry(5, 30, 101)}},
	}
	for i, us := range steps {
		got, err := DecodeLog(EncodeLog(us))
		if err != nil {
			t.Fatal(err)
		}
		if err := x.ApplyLog(int64(i+1), EncodeLog(got)); err != nil {
			t.Fatal(err)
		}
	}
	if e, ok := x.Get(id(1)); !ok || !e.Pinned || e.Day != 100 || e.Loc != (Location{Pack: 9, Offset: 77, Stored: 50, Raw: 2100, Codec: 2}) {
		t.Fatalf("moved chunk: %+v %v", e, ok)
	}
	if e, ok := x.Get(id(2)); !ok || e.Loc.Pack != 5 || e.Day != 101 {
		t.Fatalf("a chunk dropped and stored anew: %+v %v", e, ok)
	}
	// A move of a chunk the index lacks — swept before compaction moved it — leaves it absent.
	if err := x.ApplyLog(9, EncodeLog([]Update{{Op: Move, ID: id(7), Entry: Entry{Kind: encoding.KindChunk, Loc: Location{Pack: 9}}}})); err != nil {
		t.Fatal(err)
	}
	if _, ok := x.Lookup(id(7)); ok {
		t.Fatal("a move created a chunk")
	}
	bad := EncodeLog([]Update{{Op: Move, ID: id(1), Entry: Entry{Kind: encoding.KindFile}}})
	if _, err := DecodeLog(bad); err == nil {
		t.Fatal("a file's move decoded")
	}
}

// Puts, touches, and pins give the same index in any order, applied any number of times; a pin
// or touch that arrives before its entry's body waits for it.
func TestOrderDoesNotMatter(t *testing.T) {
	type piece struct {
		log   int64
		body  []byte
		block bool
	}
	pieces := []piece{
		{block: true, body: EncodeBlock([]Keyed{{ID: id(1), Entry: chunkEntry(3, 10, 100)}, {ID: id(2), Entry: fileEntry(100, FileChunk{id(1), 2100})}})},
		{log: 1, body: EncodeLog([]Update{{Op: Touch, ID: id(2), Entry: Entry{Kind: encoding.KindFile, Day: 140}}})},
		{log: 2, body: EncodeLog([]Update{{Op: Pin, ID: id(1), Entry: Entry{Kind: encoding.KindChunk}}, {Op: Pin, ID: id(6), Entry: Entry{Kind: encoding.KindChunk}}})},
		{log: 3, body: EncodeLog([]Update{{Op: Put, ID: id(6), Entry: chunkEntry(4, 0, 150)}, {Op: Touch, ID: id(2), Entry: Entry{Kind: encoding.KindFile, Day: 120}}})},
		{log: 4, body: EncodeLog([]Update{{Op: Touch, ID: id(8), Entry: Entry{Kind: encoding.KindFile, Day: 150}}})},
	}
	var want []Keyed
	for trial := range 50 {
		x := New(1)
		order := rand.Perm(len(pieces))
		order = append(order, order[:2]...)
		for _, i := range order {
			p := pieces[i]
			var err error
			if p.block {
				err = x.ApplyBlock(p.body)
			} else {
				err = x.ApplyLog(p.log, p.body)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		got := x.Sorted()
		if trial == 0 {
			want = got
			continue
		}
		if len(got) != len(want) {
			t.Fatalf("order %v: %d entries, want %d", order, len(got), len(want))
		}
		for i := range got {
			if got[i].ID != want[i].ID || !got[i].Entry.Equal(want[i].Entry) {
				t.Fatalf("order %v: %+v, want %+v", order, got[i], want[i])
			}
		}
	}
	x := New(1)
	for _, p := range pieces {
		if p.block {
			_ = x.ApplyBlock(p.body)
		} else {
			_ = x.ApplyLog(p.log, p.body)
		}
	}
	if e, ok := x.Get(id(2)); !ok || e.Day != 140 || e.Size != 2100 {
		t.Fatalf("file: %+v %v", e, ok)
	}
	if e, ok := x.Get(id(6)); !ok || !e.Pinned || e.Loc.Pack != 4 {
		t.Fatalf("a chunk pinned before its body arrived: %+v %v", e, ok)
	}
	if _, ok := x.Get(id(8)); ok {
		t.Fatal("a touch alone gave a file a body")
	}
	if e, ok := x.Lookup(id(8)); !ok || e.Day != 150 {
		t.Fatalf("the touch was not kept: %+v %v", e, ok)
	}
	if first := id(1); !bytes.Equal(want[0].ID[:], first[:]) {
		t.Fatal("not in ID order")
	}
}
