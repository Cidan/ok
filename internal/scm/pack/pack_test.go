package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	builddict "github.com/klauspost/compress/dict"
	"github.com/klauspost/compress/zstd"

	"github.com/Cidan/ok/internal/scm/encoding"
)

type chunk struct {
	id  encoding.ID
	raw []byte
}

func chunks() []chunk {
	r := rand.New(rand.NewPCG(5, 6))
	random := make([]byte, 64<<10)
	for i := range random {
		random[i] = byte(r.Uint32())
	}
	raws := [][]byte{
		bytes.Repeat([]byte("compressible line of text\n"), 4000),
		random,
		{},
		[]byte("x"),
	}
	var out []chunk
	for _, raw := range raws {
		out = append(out, chunk{id: ChunkID(raw), raw: raw})
	}
	return out
}

func members(cs []chunk) []Member {
	out := make([]Member, len(cs))
	for i, c := range cs {
		out[i] = Member{Kind: encoding.KindChunk, ID: c.id, Raw: c.raw}
	}
	return out
}

func closePack(t *testing.T, w *Writer, buf *bytes.Buffer) ([]byte, encoding.ID) {
	t.Helper()
	id, size, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(buf.Len()) {
		t.Fatalf("Close size %d, wrote %d", size, buf.Len())
	}
	if id != ID(buf.Bytes()) {
		t.Fatalf("Close ID %s is not the BLAKE3 of the bytes", id)
	}
	return buf.Bytes(), id
}

func writePack(t *testing.T, cs []chunk) ([]byte, encoding.ID, []Location) {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	var locs []Location
	for _, c := range cs {
		loc, err := w.Add(c.id, c.raw)
		if err != nil {
			t.Fatal(err)
		}
		locs = append(locs, loc)
	}
	data, id := closePack(t, w, &buf)
	return data, id, locs
}

func storedAt(data []byte, l Location) []byte {
	return data[l.DataOffset() : l.DataOffset()+int64(l.StoredLength)]
}

func TestRoundTrip(t *testing.T) {
	cs := chunks()
	data, id, locs := writePack(t, cs)
	if locs[0].Codec != Zstd || locs[0].StoredLength >= locs[0].RawLength {
		t.Errorf("compressible chunk stored as %+v", locs[0])
	}
	if locs[1].Codec != Stored || locs[1].StoredLength != locs[1].RawLength {
		t.Errorf("random chunk stored as %+v", locs[1])
	}
	r := bytes.NewReader(data)
	records, err := Verify(r, int64(len(data)), id, nil)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(records) != len(cs) {
		t.Fatalf("%d records, want %d", len(records), len(cs))
	}
	index, err := ReadIndex(r, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range cs {
		if records[i].ID != c.id || records[i].Location != locs[i] {
			t.Errorf("record %d = %+v, want %s at %+v", i, records[i], c.id, locs[i])
		}
		if !locs[i].Framed || !locs[i].Whole() {
			t.Errorf("record %d: a frame of its own is %+v", i, locs[i])
		}
		found := false
		for _, e := range index {
			if e.ID == c.id && e.Offset == locs[i].Offset-FrameHeaderLen(1) {
				found = true
			}
		}
		if !found {
			t.Errorf("index lacks %s's frame", c.id)
		}
		raw, err := Decode(nil, c.id, locs[i].Codec, locs[i].RawLength, storedAt(data, locs[i]))
		if err != nil || !bytes.Equal(raw, c.raw) {
			t.Errorf("Decode chunk %d: %v", i, err)
		}
		if raw, err := Read(nil, encoding.KindChunk, c.id, locs[i], storedAt(data, locs[i])); err != nil || !bytes.Equal(raw, c.raw) {
			t.Errorf("Read chunk %d: %v", i, err)
		}
	}
}

func TestAddTwiceRefused(t *testing.T) {
	w := NewWriter(&bytes.Buffer{})
	c := chunks()[3]
	if _, err := w.Add(c.id, c.raw); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Add(c.id, c.raw); err == nil {
		t.Fatal("adding a chunk twice succeeded")
	}
	if _, err := w.AddFrame(nil, encoder, members([]chunk{chunks()[0], c})); err == nil {
		t.Fatal("a frame held a chunk the pack holds")
	}
}

func TestEmptyPack(t *testing.T) {
	data, id, _ := writePack(t, nil)
	records, err := Verify(bytes.NewReader(data), int64(len(data)), id, nil)
	if err != nil || len(records) != 0 {
		t.Fatalf("Verify(empty) = %v, %v", records, err)
	}
}

// A frame of many records stores them as one, compressed together; each reads back from it alone.
func TestFrames(t *testing.T) {
	cs := chunks()
	var small []chunk
	for i := range 50 {
		raw := []byte(fmt.Sprintf("package p\n\n// File %d holds a line much like its neighbours'.\nfunc F%d() int { return %d }\n", i, i, i*3))
		small = append(small, chunk{id: ChunkID(raw), raw: raw})
	}
	small = append(small, cs[2]) // the empty chunk, in the middle of nothing
	var buf bytes.Buffer
	w := NewWriter(&buf)
	best, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		t.Fatal(err)
	}
	locs, err := w.AddFrame(nil, best, members(small))
	if err != nil {
		t.Fatal(err)
	}
	big, err := w.Add(cs[0].id, cs[0].raw)
	if err != nil {
		t.Fatal(err)
	}
	if w.Frames() != 2 || w.Len() != len(small)+1 {
		t.Fatalf("%d frames of %d records", w.Frames(), w.Len())
	}
	data, id := closePack(t, w, &buf)
	var alone int
	for _, c := range small {
		alone += len(encoder.EncodeAll(c.raw, nil))
	}
	if int(locs[0].StoredLength) >= alone/2 {
		t.Errorf("the frame stores %d bytes, its records alone %d", locs[0].StoredLength, alone)
	}
	for i, c := range small {
		l := locs[i]
		if !l.Framed || l.Offset != locs[0].Offset || l.StoredLength != locs[0].StoredLength || l.RawLength != uint32(len(c.raw)) {
			t.Errorf("record %d at %+v", i, l)
		}
		if l.Whole() {
			t.Errorf("record %d of a frame of many is whole", i)
		}
		raw, err := Read(nil, encoding.KindChunk, c.id, l, storedAt(data, l))
		if err != nil || !bytes.Equal(raw, c.raw) {
			t.Errorf("Read record %d: %q, %v", i, raw, err)
		}
		if _, err := Read(nil, encoding.KindTree, c.id, l, storedAt(data, l)); err == nil {
			t.Errorf("record %d read as a tree", i)
		}
		if _, err := Read(nil, encoding.KindChunk, small[(i+1)%len(small)].id, l, storedAt(data, l)); err == nil {
			t.Errorf("record %d read under another's ID", i)
		}
	}
	records, err := Verify(bytes.NewReader(data), int64(len(data)), id, nil)
	if err != nil || len(records) != len(small)+1 {
		t.Fatalf("Verify = %d records, %v", len(records), err)
	}
	for i := range small {
		if records[i].Location != locs[i] || records[i].Kind != encoding.KindChunk {
			t.Errorf("record %d = %+v, want %+v", i, records[i], locs[i])
		}
	}
	if records[len(small)].Location != big {
		t.Errorf("the last record = %+v, want %+v", records[len(small)].Location, big)
	}
	if want := Overhead(len(small)+1, 2) + int64(locs[0].StoredLength) + int64(big.StoredLength); want != int64(len(data)) {
		t.Errorf("Overhead says %d bytes, the pack has %d", want, len(data))
	}

	// What AddFrame refuses: no records, a record under another's ID, one twice.
	w2 := NewWriter(&bytes.Buffer{})
	if _, err := w2.AddFrame(nil, best, nil); err == nil {
		t.Error("a frame of no records")
	}
	bad := members(small[:3])
	bad[1].ID = small[5].id
	if _, err := w2.AddFrame(nil, best, bad); err == nil {
		t.Error("a frame with a record under another's ID")
	}
	twice := members([]chunk{small[0], small[1], small[0]})
	if _, err := w2.AddFrame(nil, best, twice); err == nil {
		t.Error("a frame with a record twice")
	}
}

func TestVerifyDetectsCorruption(t *testing.T) {
	cs := chunks()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	locs, err := w.AddFrame(nil, encoder, members(cs))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := closePack(t, w, &buf)
	frameOff := locs[0].Offset - FrameHeaderLen(len(cs))
	corrupt := func(name string, mutate func([]byte) []byte, wantID func([]byte) encoding.ID) {
		b := mutate(bytes.Clone(data))
		if _, err := Verify(bytes.NewReader(b), int64(len(b)), wantID(b), nil); err == nil {
			t.Errorf("%s: Verify accepted a corrupt pack", name)
		}
	}
	rehash := func(b []byte) encoding.ID { return ID(b) }
	corrupt("flipped data byte", func(b []byte) []byte {
		b[locs[0].DataOffset()+int64(locs[0].StoredLength)/2] ^= 0xff
		return b
	}, rehash)
	corrupt("bad magic", func(b []byte) []byte { b[0] = 'X'; return b }, rehash)
	corrupt("bad end magic", func(b []byte) []byte { b[len(b)-1] = 'X'; return b }, rehash)
	corrupt("truncated", func(b []byte) []byte { return b[:len(b)-3] }, rehash)
	corrupt("wrong pack id", func(b []byte) []byte { return b }, func([]byte) encoding.ID { return encoding.ID{1} })
	corrupt("frame raw length lie", func(b []byte) []byte {
		b[frameOff+5]++
		return b
	}, rehash)
	corrupt("member raw length lie", func(b []byte) []byte {
		b[frameOff+frameFixedLen+encoding.IDLen]++
		return b
	}, rehash)
	corrupt("member ID lie", func(b []byte) []byte {
		b[frameOff+frameFixedLen+memberEntryLen]++
		return b
	}, rehash)
	corrupt("no members", func(b []byte) []byte {
		binary.LittleEndian.PutUint32(b[frameOff+1:], 0)
		return b
	}, rehash)
}

func TestDecodeChecksID(t *testing.T) {
	c := chunks()[3]
	if _, err := Decode(nil, encoding.ID{9}, Stored, uint32(len(c.raw)), c.raw); err == nil {
		t.Fatal("Decode accepted bytes under the wrong ID")
	}
}

// Every pack written is unique: the same chunks, in the same order, make a different pack.
func TestPacksAreUnique(t *testing.T) {
	cs := chunks()
	a, idA, _ := writePack(t, cs)
	b, idB, _ := writePack(t, cs)
	if idA == idB || bytes.Equal(a, b) {
		t.Fatal("two packs of the same chunks share their bytes")
	}
	for _, p := range []struct {
		data []byte
		id   encoding.ID
	}{{a, idA}, {b, idB}} {
		if _, err := Verify(bytes.NewReader(p.data), int64(len(p.data)), p.id, nil); err != nil {
			t.Fatal(err)
		}
	}
}

// writeRecords writes a pack of version v, 1 or 2, of records each compressed alone, as packs were
// written before frames: version 1 has no nonce.
func writeRecords(v uint32, cs []chunk) ([]byte, []Location) {
	var buf bytes.Buffer
	buf.WriteString(magic)
	buf.Write(binary.LittleEndian.AppendUint32(nil, v))
	if v == 2 {
		buf.Write(make([]byte, nonceLen))
	}
	var index []indexEntry
	var locs []Location
	for _, c := range cs {
		codec, stored := Stored, c.raw
		if z := encoder.EncodeAll(c.raw, nil); v == 2 && len(z) < len(c.raw) {
			codec, stored = Zstd, z
		}
		loc := Location{Offset: int64(buf.Len()), Codec: codec, RawLength: uint32(len(c.raw)), StoredLength: uint32(len(stored))}
		buf.Write(c.id[:])
		buf.WriteByte(byte(codec))
		buf.Write(binary.LittleEndian.AppendUint32(nil, loc.RawLength))
		buf.Write(binary.LittleEndian.AppendUint32(nil, loc.StoredLength))
		buf.Write(stored)
		index = append(index, indexEntry{id: c.id, off: loc.Offset})
		locs = append(locs, loc)
	}
	slices.SortFunc(index, func(a, b indexEntry) int { return bytes.Compare(a.id[:], b.id[:]) })
	indexOff := buf.Len()
	buf.Write(binary.LittleEndian.AppendUint64(nil, uint64(len(index))))
	for _, e := range index {
		buf.Write(e.id[:])
		buf.Write(binary.LittleEndian.AppendUint64(nil, uint64(e.off)))
	}
	buf.Write(binary.LittleEndian.AppendUint64(nil, uint64(indexOff)))
	buf.WriteString(endMagic)
	return buf.Bytes(), locs
}

// Packs written before frames read as they always did: each record at its location, whole.
func TestRecordPacksStillRead(t *testing.T) {
	cs := chunks()
	for _, v := range []uint32{1, 2} {
		data, locs := writeRecords(v, cs)
		records, err := Verify(bytes.NewReader(data), int64(len(data)), ID(data), nil)
		if err != nil || len(records) != len(cs) {
			t.Fatalf("Verify(v%d) = %d records, %v", v, len(records), err)
		}
		for i, r := range records {
			if r.ID != cs[i].id || r.Location != locs[i] || r.Framed || !r.Whole() {
				t.Errorf("v%d record %d = %+v, want %+v", v, i, r, locs[i])
			}
			raw, err := Read(nil, encoding.KindChunk, cs[i].id, r.Location, storedAt(data, r.Location))
			if err != nil || !bytes.Equal(raw, cs[i].raw) {
				t.Errorf("v%d record %d reads %v", v, i, err)
			}
		}
		if _, err := ReadIndex(bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		bad := bytes.Clone(data)
		binary.LittleEndian.PutUint32(bad[8:], 4)
		if _, err := Verify(bytes.NewReader(bad), int64(len(bad)), ID(bad), nil); err == nil {
			t.Error("an unknown version was accepted")
		}
	}
}

// AddStored copies a whole record verbatim, from a frame of its own or a version 2 record, as a
// frame of its own, and refuses stored bytes that do not decode to the chunk.
func TestAddStored(t *testing.T) {
	cs := chunks()
	src, _, locs := writePack(t, cs)
	old, oldLocs := writeRecords(2, cs)
	for _, from := range []struct {
		data []byte
		locs []Location
	}{{src, locs}, {old, oldLocs}} {
		var buf bytes.Buffer
		w := NewWriter(&buf)
		var copied []Location
		for i, c := range cs {
			l := from.locs[i]
			loc, err := w.AddStored(nil, encoding.KindChunk, c.id, l.Codec, l.RawLength, storedAt(from.data, l))
			if err != nil {
				t.Fatal(err)
			}
			if loc.Codec != l.Codec || loc.StoredLength != l.StoredLength || loc.RawLength != l.RawLength || !loc.Whole() {
				t.Errorf("chunk %d copied as %+v from %+v", i, loc, l)
			}
			copied = append(copied, loc)
		}
		if _, err := w.AddStored(nil, encoding.KindChunk, cs[0].id, locs[0].Codec, locs[0].RawLength, nil); err == nil {
			t.Error("a chunk added twice")
		}
		data, id := closePack(t, w, &buf)
		records, err := Verify(bytes.NewReader(data), int64(len(data)), id, nil)
		if err != nil || len(records) != len(cs) {
			t.Fatalf("Verify(copy) = %d records, %v", len(records), err)
		}
		for i := range records {
			if records[i].Location != copied[i] {
				t.Errorf("record %d = %+v, want %+v", i, records[i].Location, copied[i])
			}
		}
	}

	w2 := NewWriter(&bytes.Buffer{})
	if _, err := w2.AddStored(nil, encoding.KindChunk, cs[3].id, Stored, 1, []byte("y")); err == nil {
		t.Error("stored bytes of another chunk were accepted")
	}
	if _, err := w2.AddStored(nil, encoding.KindChunk, cs[3].id, Codec(9), 1, []byte("x")); err == nil {
		t.Error("an unknown codec was accepted")
	}
}

// A pack holds trees beside chunks, each record hashing to its ID as its own kind: Verify names each
// record's kind, a reader decodes a record as the kind it asks for and no other, AddStored copies a
// tree's record as a tree's, and a record whose bytes hash to its ID as neither fails Verify.
func TestTreesInAPack(t *testing.T) {
	tree, err := encoding.Encode(&encoding.Tree{Entries: []encoding.TreeEntry{{Name: "f",
		Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: encoding.ID{7}}}}})
	if err != nil {
		t.Fatal(err)
	}
	treeID, body := encoding.Hash(tree), tree[1:]
	c := chunks()[3]
	var buf bytes.Buffer
	w := NewWriter(&buf)
	cl, err := w.Add(c.id, c.raw)
	if err != nil {
		t.Fatal(err)
	}
	tl, err := w.Add(treeID, body)
	if err != nil {
		t.Fatal(err)
	}
	data, id := closePack(t, w, &buf)
	records, err := Verify(bytes.NewReader(data), int64(len(data)), id, nil)
	if err != nil || len(records) != 2 || records[0].Kind != encoding.KindChunk || records[1].Kind != encoding.KindTree {
		t.Fatalf("Verify: %+v, %v; want a chunk's record and a tree's", records, err)
	}
	stored := storedAt(data, tl)
	if raw, err := DecodeObject(nil, encoding.KindTree, treeID, tl.Codec, tl.RawLength, stored); err != nil || !bytes.Equal(raw, body) {
		t.Fatalf("the tree's record, as a tree: %v", err)
	}
	if _, err := DecodeObject(nil, encoding.KindChunk, treeID, tl.Codec, tl.RawLength, stored); err == nil {
		t.Fatal("the tree's record decoded as a chunk")
	}
	if _, err := Decode(nil, treeID, tl.Codec, tl.RawLength, stored); err == nil {
		t.Fatal("Decode, a chunk's, took the tree's record")
	}
	w2 := NewWriter(&bytes.Buffer{})
	if _, err := w2.AddStored(nil, encoding.KindTree, treeID, tl.Codec, tl.RawLength, stored); err != nil {
		t.Fatalf("copying the tree's record: %v", err)
	}
	if _, err := w2.AddStored(nil, encoding.KindTree, c.id, cl.Codec, cl.RawLength, storedAt(data, cl)); err == nil {
		t.Fatal("a chunk's record was copied as a tree's")
	}

	// A record under an ID its bytes hash to as no kind.
	var bad bytes.Buffer
	wb := NewWriter(&bad)
	if _, err := wb.Add(encoding.ID{1, 2, 3}, body); err != nil {
		t.Fatal(err)
	}
	bid, bsize, err := wb.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(bytes.NewReader(bad.Bytes()), bsize, bid, nil); err == nil {
		t.Fatal("a record of no kind verified")
	}
}

// dicts is a Dicts over dictionaries held in memory.
type dicts map[uint32]*zstd.Decoder

func (d dicts) Decoder(id uint32) (*zstd.Decoder, error) {
	dec, ok := d[id]
	if !ok {
		return nil, fmt.Errorf("no dictionary %d", id)
	}
	return dec, nil
}

// A record or a frame stored with a dictionary names it, and decodes only with it.
func TestDictionaryChunks(t *testing.T) {
	var samples [][]byte
	for i := range 2000 {
		samples = append(samples, []byte(fmt.Sprintf("package p%d\n\nimport \"fmt\"\n\nfunc F%d() { fmt.Println(%d) }\n", i%37, i, i*7)))
	}
	dict, err := builddict.BuildZstdDict(samples, builddict.Options{MaxDictSize: 16 << 10, HashBytes: 6, ZstdDictID: 40000, ZstdDictCompat: true})
	if err != nil {
		t.Fatal(err)
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression), zstd.WithEncoderDict(dict), zstd.WithEncoderCRC(true))
	if err != nil {
		t.Fatal(err)
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte("package p3\n\nimport \"fmt\"\n\nfunc F99999() { fmt.Println(1) }\n")
	stored := enc.EncodeAll(raw, nil)
	if got, err := DictionaryID(stored); err != nil || got != 40000 {
		t.Fatalf("the frame names %d, %v", got, err)
	}
	id := ChunkID(raw)
	got, err := Decode(dicts{40000: dec}, id, ZstdDict, uint32(len(raw)), stored)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("decoded %q, %v", got, err)
	}
	if _, err := Decode(nil, id, ZstdDict, uint32(len(raw)), stored); err == nil {
		t.Fatal("decoded without its dictionary")
	}
	if _, err := Decode(dicts{}, id, ZstdDict, uint32(len(raw)), stored); err == nil {
		t.Fatal("decoded with an unknown dictionary")
	}
	plain := encoder.EncodeAll(raw, nil)
	if _, err := Decode(dicts{40000: dec}, id, ZstdDict, uint32(len(raw)), plain); err == nil {
		t.Fatal("a frame naming no dictionary passed as a dictionary's")
	}
	// A pack holds it as stored, and verifies with the dictionary.
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.AddStored(dicts{40000: dec}, encoding.KindChunk, id, ZstdDict, uint32(len(raw)), stored); err != nil {
		t.Fatal(err)
	}
	// A frame of many compressed with the dictionary is a dictionary's, and names it.
	var ms []Member
	for i := range 20 {
		b := []byte(fmt.Sprintf("package p%d\n\nimport \"fmt\"\n\nfunc G%d() { fmt.Println(%d) }\n", i, i, i))
		ms = append(ms, Member{Kind: encoding.KindChunk, ID: ChunkID(b), Raw: b})
	}
	locs, err := w.AddFrame(dicts{40000: dec}, enc, ms)
	if err != nil {
		t.Fatal(err)
	}
	if locs[0].Codec != ZstdDict {
		t.Fatalf("a frame compressed with a dictionary stored as %d", locs[0].Codec)
	}
	if _, err := NewWriter(&bytes.Buffer{}).AddFrame(nil, enc, ms); err == nil {
		t.Fatal("a dictionary's frame was written that nothing could check")
	}
	pid, size, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(bytes.NewReader(buf.Bytes()), size, pid, dicts{40000: dec}); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(bytes.NewReader(buf.Bytes()), size, pid, nil); err == nil {
		t.Fatal("a pack with a dictionary's chunk verified without it")
	}
	for i, m := range ms {
		if raw, err := Read(dicts{40000: dec}, m.Kind, m.ID, locs[i], storedAt(buf.Bytes(), locs[i])); err != nil || !bytes.Equal(raw, m.Raw) {
			t.Errorf("member %d: %v", i, err)
		}
	}
	// What a dictionary saves on a small file like those it was trained on.
	if len(stored) >= len(plain) {
		t.Errorf("with the dictionary %d bytes, without %d", len(stored), len(plain))
	}
}
