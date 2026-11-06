// Package pack is the SCM's pack file format (docs/scm.md, "Packs"): the object that holds a
// network's file-content chunks, and the trees the publish that wrote it stored beside them, in
// frames — independently decodable runs of records, each compressed as one. Integers are
// little-endian.
//
//	magic      "OKSCMPAK"                        8 bytes
//	version    u32 = 3
//	nonce      16 random bytes
//	frames     repeated:
//	  codec          u8     0 = stored, 1 = zstd, 2 = zstd with a dictionary its header names
//	  members        u32    how many records it holds, at least one
//	  raw length     u32    of its members' raw bytes, one after another
//	  stored length  u32
//	  member table   members × (id 32 bytes: a chunk's or a tree's, raw length u32)
//	  data           stored-length bytes: the members' raw bytes, as codec stores them
//	index      u64 count, then (id, u64 offset of its frame), sorted by id
//	footer     u64 offset of the index, magic "OKSCMEND"
//
// A record is its frame's raw bytes from the sum of the raw lengths before it in the member table,
// for its own raw length. A frame of one record stores that record alone, as a version 2 record
// did; a frame of many is what compaction writes for small files, which compress far better
// together than alone and are read together, a directory at a time.
//
// A pack's ID is the BLAKE3-256 of all of its bytes. The nonce makes every pack written
// unique, so no two packs share an ID or an S3 key even when they hold the same chunks:
// once garbage collection deletes a pack, nothing can write that key again. The frames, their
// member tables, and the index make a pack self-describing, so fsck can verify it against the
// content index and rebuild locations from it. A record holds a chunk's bytes, or a tree's
// canonical body — its ID that body's under the tree's kind (encoding.IDOf) — which the content
// index says it is: a record carries no kind, and is checked against the ID of the kind its reader
// asks for (DecodeObject). A frame stored with a dictionary names it in its zstd frame's header
// (RFC 8878 §3.1.1.1.3), and decoding it needs the dictionary: a Dicts.
//
// Versions 1 and 2 held records, not frames — (id, codec u8, raw length u32, stored length u32,
// data), each compressed alone, the index naming each record's offset — version 1 without the
// nonce. They are read still: what a pack of theirs holds is where the content index says until
// compaction rewrites it.
package pack

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/blake3"

	"github.com/Cidan/ok/internal/scm/encoding"
)

const (
	magic    = "OKSCMPAK"
	endMagic = "OKSCMEND"
	version  = 3
	nonceLen = 16

	// minHeaderLen is a version 1 header's length, the shortest a pack has.
	minHeaderLen = 8 + 4
	headerLen    = minHeaderLen + nonceLen
	// recordHeaderLen is the size of a version 1 or 2 record's fixed fields before its data.
	recordHeaderLen = encoding.IDLen + 1 + 4 + 4
	// frameFixedLen is the size of a frame's fixed fields, before its member table.
	frameFixedLen  = 1 + 4 + 4 + 4
	memberEntryLen = encoding.IDLen + 4
	indexEntryLen  = encoding.IDLen + 8
	footerLen      = 8 + 8
)

// Codec is how stored bytes encode raw ones: a frame's, or a version 1 or 2 record's.
type Codec uint8

const (
	Stored Codec = 0
	Zstd   Codec = 1
	// ZstdDict is one zstd frame compressed with a dictionary its header names: the
	// network's, trained on its small files (docs/scm.md, "Dictionaries").
	ZstdDict Codec = 2
)

// Dicts decodes frames stored with dictionaries: a decoder that holds the one a frame names.
type Dicts interface {
	Decoder(id uint32) (*zstd.Decoder, error)
}

// DictionaryID is the dictionary a ZstdDict frame names.
func DictionaryID(stored []byte) (uint32, error) {
	id, err := frameDictionary(stored)
	if err != nil {
		return 0, err
	}
	if id == 0 {
		return 0, malformed("a dictionary's frame names no dictionary")
	}
	return id, nil
}

// frameDictionary is the dictionary a zstd frame's header names: 0 for none.
func frameDictionary(stored []byte) (uint32, error) {
	var h zstd.Header
	if err := h.Decode(stored); err != nil {
		return 0, malformed("a zstd frame's header: %v", err)
	}
	return h.DictionaryID, nil
}

// Location is where a record sits in its pack: the stored bytes to read — its frame's, or a
// version 1 or 2 record's own — and where in what they decode to the record is.
type Location struct {
	// Offset is of the frame's stored bytes (Framed), or of the version 1 or 2 record.
	Offset int64
	// Codec is how the stored bytes are encoded.
	Codec Codec
	// RawLength is the record's.
	RawLength uint32
	// StoredLength is of the stored bytes: the frame's, or the record's.
	StoredLength uint32
	// Framed is a version 3 location: the record is its frame's raw bytes, FrameRaw of them,
	// from InFrame for RawLength.
	Framed   bool
	FrameRaw uint32
	InFrame  uint32
}

// DataOffset is the offset of the stored bytes.
func (l Location) DataOffset() int64 {
	if l.Framed {
		return l.Offset
	}
	return l.Offset + recordHeaderLen
}

// Whole reports whether the stored bytes decode to the record alone — a version 1 or 2 record,
// or a frame whose raw bytes are just the record's — and so are the record's as stored, to copy or
// send as they are.
func (l Location) Whole() bool {
	return !l.Framed || (l.InFrame == 0 && l.FrameRaw == l.RawLength)
}

// Footprint is the bytes the record takes of its pack: its share of its frame's stored bytes, in
// proportion to its raw bytes — all of them, and the frame's fixed fields, a frame it is whole — with
// its entries in the frame's member table and the index; or, a version 1 or 2 record, its stored
// bytes, header, and index entry. A pack's records' footprints come to its size less its header and
// footer (PackOverhead), but for the fixed fields of frames of many and a byte of rounding a record.
func (l Location) Footprint() int64 {
	if !l.Framed {
		return int64(l.StoredLength) + recordHeaderLen + indexEntryLen
	}
	n := int64(memberEntryLen + indexEntryLen)
	if l.Whole() {
		return n + frameFixedLen + int64(l.StoredLength)
	}
	return n + int64(uint64(l.StoredLength)*uint64(l.RawLength)/uint64(l.FrameRaw))
}

// PackOverhead is what a pack spends on neither frames nor records: its header, the index's count,
// and the footer.
const PackOverhead = headerLen + 8 + footerLen

// Record is one record, as read back from a pack: a chunk's, or a tree's.
type Record struct {
	ID encoding.ID
	// Kind is what the record holds: what its bytes hash to its ID as — a chunk, or a tree (Verify).
	// A record read without its bytes has none.
	Kind encoding.Kind
	Location
}

// Kinds are what a pack's records hold.
var Kinds = []encoding.Kind{encoding.KindChunk, encoding.KindTree}

// KindOf is the kind, among Kinds, raw's ID is id as; false for none.
func KindOf(id encoding.ID, raw []byte) (encoding.Kind, bool) {
	for _, k := range Kinds {
		h := encoding.NewHasher(k)
		h.Write(raw)
		if h.Sum() == id {
			return k, true
		}
	}
	return 0, false
}

// Error reports a pack that is not well formed.
type Error struct{ Detail string }

func (e *Error) Error() string { return "pack: " + e.Detail }

func malformed(format string, args ...any) error { return &Error{Detail: fmt.Sprintf(format, args...)} }

var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	decoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
)

// Member is a record of a frame: its kind, ID, and raw bytes.
type Member struct {
	Kind encoding.Kind
	ID   encoding.ID
	Raw  []byte
}

// indexEntry is an ID and the offset of its frame (version 3), or of its record.
type indexEntry struct {
	id  encoding.ID
	off int64
}

// Writer streams a pack. Add frames in the order they should sit in the pack; Close writes the
// index and footer and returns the pack's ID.
type Writer struct {
	w      io.Writer
	hasher *blake3.Hasher
	off    int64
	index  []indexEntry
	seen   map[encoding.ID]bool
	frames int
	err    error
}

// NewWriter starts a pack on w, with a fresh random nonce.
func NewWriter(w io.Writer) *Writer {
	pw := &Writer{w: w, hasher: blake3.New(), seen: map[encoding.ID]bool{}}
	hdr := binary.LittleEndian.AppendUint32([]byte(magic), version)
	nonce := make([]byte, nonceLen)
	if _, err := rand.Read(nonce); err != nil {
		pw.err = err
		return pw
	}
	pw.write(append(hdr, nonce...))
	return pw
}

// headerLength is the length of a pack's header and its version, from its first minHeaderLen
// bytes.
func headerLength(hdr []byte) (int64, uint32, error) {
	if len(hdr) < minHeaderLen || string(hdr[:8]) != magic {
		return 0, 0, malformed("bad header %q", hdr)
	}
	switch v := binary.LittleEndian.Uint32(hdr[8:]); v {
	case 1:
		return minHeaderLen, v, nil
	case 2, 3:
		return headerLen, v, nil
	default:
		return 0, 0, malformed("unknown version %d", v)
	}
}

func (pw *Writer) write(b []byte) {
	if pw.err != nil {
		return
	}
	if _, err := pw.w.Write(b); err != nil {
		pw.err = err
		return
	}
	pw.hasher.Write(b)
	pw.off += int64(len(b))
}

// Add appends a record — a chunk's, or a tree's — as a frame of its own, zstd-compressed unless that
// does not shrink it, and returns where it landed. A pack holds each record once.
func (pw *Writer) Add(id encoding.ID, raw []byte) (Location, error) {
	if pw.err != nil {
		return Location{}, pw.err
	}
	if err := pw.check(id, len(raw)); err != nil {
		return Location{}, err
	}
	codec, stored := Stored, raw
	if c := encoder.EncodeAll(raw, nil); len(c) < len(raw) {
		codec, stored = Zstd, c
	}
	locs, err := pw.frame(codec, []encoding.ID{id}, []uint32{uint32(len(raw))}, stored)
	if err != nil {
		return Location{}, err
	}
	return locs[0], nil
}

// AddFrame appends members as one frame compressed by enc — at its level, with its dictionary if it
// has one (d decodes it) — unless that does not shrink them, and returns where each landed. Each
// member's bytes must hash to its ID as its kind, and the stored bytes must decode to theirs: what
// a frame holds is checked before it is written.
func (pw *Writer) AddFrame(d Dicts, enc *zstd.Encoder, members []Member) ([]Location, error) {
	if pw.err != nil {
		return nil, pw.err
	}
	if len(members) == 0 {
		return nil, errors.New("pack: a frame of no records")
	}
	ids := make([]encoding.ID, len(members))
	lengths := make([]uint32, len(members))
	var total int
	inFrame := map[encoding.ID]bool{}
	for i, m := range members {
		if err := pw.check(m.ID, len(m.Raw)); err != nil {
			return nil, err
		}
		if inFrame[m.ID] {
			return nil, fmt.Errorf("pack: %s added twice", m.ID)
		}
		inFrame[m.ID] = true
		if err := Check(m.Kind, m.ID, m.Raw); err != nil {
			return nil, err
		}
		ids[i], lengths[i] = m.ID, uint32(len(m.Raw))
		total += len(m.Raw)
	}
	if uint64(total) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("pack: a frame of %d bytes, over the format's limit", total)
	}
	raw := make([]byte, 0, total)
	for _, m := range members {
		raw = append(raw, m.Raw...)
	}
	codec, stored := Stored, raw
	if c := enc.EncodeAll(raw, nil); len(c) < len(raw) {
		dict, err := frameDictionary(c)
		if err != nil {
			return nil, err
		}
		codec, stored = Zstd, c
		if dict != 0 {
			codec = ZstdDict
		}
		back, err := decompress(d, "a frame", codec, uint32(total), stored)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(back, raw) {
			return nil, errors.New("pack: a frame decodes to other bytes than it was written from")
		}
	}
	return pw.frame(codec, ids, lengths, stored)
}

// AddStored appends a chunk's or a tree's (kind) record as a frame of its own, from its stored bytes
// as another frame of its own, or a version 1 or 2 record, stored them — its codec, raw length, and
// stored bytes — without re-encoding them, as compaction copies records. The stored bytes must
// decode to the record's (d decodes a dictionary's).
func (pw *Writer) AddStored(d Dicts, kind encoding.Kind, id encoding.ID, codec Codec, rawLength uint32, stored []byte) (Location, error) {
	if pw.err != nil {
		return Location{}, pw.err
	}
	if err := pw.check(id, int(rawLength)); err != nil {
		return Location{}, err
	}
	if _, err := DecodeObject(d, kind, id, codec, rawLength, stored); err != nil {
		return Location{}, err
	}
	locs, err := pw.frame(codec, []encoding.ID{id}, []uint32{rawLength}, stored)
	if err != nil {
		return Location{}, err
	}
	return locs[0], nil
}

// check refuses a record the pack holds already, or too large for the format.
func (pw *Writer) check(id encoding.ID, n int) error {
	if pw.seen[id] {
		return fmt.Errorf("pack: %s added twice", id)
	}
	if uint64(n) > uint64(^uint32(0)) {
		return fmt.Errorf("pack: %s is %d bytes, over the format's limit", id, n)
	}
	return nil
}

// frame writes one frame of records ids, of raw lengths, stored by codec as stored.
func (pw *Writer) frame(codec Codec, ids []encoding.ID, lengths []uint32, stored []byte) ([]Location, error) {
	var total uint32
	for _, n := range lengths {
		total += n
	}
	frameOff := pw.off
	hdr := make([]byte, 0, FrameHeaderLen(len(ids)))
	hdr = append(hdr, byte(codec))
	hdr = binary.LittleEndian.AppendUint32(hdr, uint32(len(ids)))
	hdr = binary.LittleEndian.AppendUint32(hdr, total)
	hdr = binary.LittleEndian.AppendUint32(hdr, uint32(len(stored)))
	for i, id := range ids {
		hdr = append(hdr, id[:]...)
		hdr = binary.LittleEndian.AppendUint32(hdr, lengths[i])
	}
	pw.write(hdr)
	pw.write(stored)
	if pw.err != nil {
		return nil, pw.err
	}
	dataOff := frameOff + int64(len(hdr))
	locs := make([]Location, len(ids))
	var at uint32
	for i, id := range ids {
		pw.seen[id] = true
		pw.index = append(pw.index, indexEntry{id: id, off: frameOff})
		locs[i] = Location{Offset: dataOff, Codec: codec, RawLength: lengths[i], StoredLength: uint32(len(stored)),
			Framed: true, FrameRaw: total, InFrame: at}
		at += lengths[i]
	}
	pw.frames++
	return locs, nil
}

// FrameHeaderLen is the length of a frame's fields before its data: its fixed fields and a member
// table of members.
func FrameHeaderLen(members int) int64 { return frameFixedLen + int64(members)*memberEntryLen }

// Overhead is the bytes a pack of records in frames spends beyond the frames' stored bytes: its
// header and nonce, the frames' fixed fields, a member table entry and an index entry a record,
// the index count, and the footer.
func Overhead(records, frames int) int64 {
	return headerLen + int64(frames)*frameFixedLen + int64(records)*(memberEntryLen+indexEntryLen) + 8 + footerLen
}

// Len is the number of records added.
func (pw *Writer) Len() int { return len(pw.index) }

// Frames is the number of frames added.
func (pw *Writer) Frames() int { return pw.frames }

// Size is the bytes written so far.
func (pw *Writer) Size() int64 { return pw.off }

// Close writes the index and footer and returns the pack's ID and size.
func (pw *Writer) Close() (encoding.ID, int64, error) {
	index := slices.Clone(pw.index)
	slices.SortFunc(index, func(a, b indexEntry) int { return bytes.Compare(a.id[:], b.id[:]) })
	indexOff := pw.off
	buf := binary.LittleEndian.AppendUint64(nil, uint64(len(index)))
	for _, e := range index {
		buf = append(buf, e.id[:]...)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(e.off))
	}
	buf = binary.LittleEndian.AppendUint64(buf, uint64(indexOff))
	buf = append(buf, endMagic...)
	pw.write(buf)
	if pw.err != nil {
		return encoding.ID{}, 0, pw.err
	}
	var id encoding.ID
	pw.hasher.Sum(id[:0])
	return id, pw.off, nil
}

// ID returns the ID of a whole pack's bytes.
func ID(packBytes []byte) encoding.ID { return blake3.Sum256(packBytes) }

// Decode turns a whole record's stored bytes (Location.Whole) back into the chunk's raw bytes and
// checks them against the chunk's ID. d decodes stored bytes compressed with a dictionary; nil
// decodes none.
func Decode(d Dicts, id encoding.ID, codec Codec, rawLength uint32, stored []byte) ([]byte, error) {
	return DecodeObject(d, encoding.KindChunk, id, codec, rawLength, stored)
}

// DecodeObject is Decode for a whole record of an object of kind — a chunk, or a tree — checked
// against the ID of the kind: BLAKE3 of the kind's byte and the record's raw bytes.
func DecodeObject(d Dicts, kind encoding.Kind, id encoding.ID, codec Codec, rawLength uint32, stored []byte) ([]byte, error) {
	raw, err := Decompress(d, id, codec, rawLength, stored)
	if err != nil {
		return nil, err
	}
	if err := Check(kind, id, raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// Check holds raw bytes to an ID as kind.
func Check(kind encoding.Kind, id encoding.ID, raw []byte) error {
	h := encoding.NewHasher(kind)
	h.Write(raw)
	if got := h.Sum(); got != id {
		return malformed("%s %s: bytes hash to %s", kind, id, got)
	}
	return nil
}

// Read is a record's raw bytes from the stored bytes at its location — decoded, taken from its
// frame, and checked against its ID as kind.
func Read(d Dicts, kind encoding.Kind, id encoding.ID, l Location, stored []byte) ([]byte, error) {
	if l.Whole() {
		return DecodeObject(d, kind, id, l.Codec, l.RawLength, stored)
	}
	frame, err := DecodeFrame(d, l, stored)
	if err != nil {
		return nil, err
	}
	raw, err := RecordOf(frame, l)
	if err != nil {
		return nil, err
	}
	if err := Check(kind, id, raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// DecodeFrame is the raw bytes of the frame at a version 3 location, from its stored bytes.
func DecodeFrame(d Dicts, l Location, stored []byte) ([]byte, error) {
	if !l.Framed {
		return nil, malformed("a version 2 record is no frame")
	}
	return decompress(d, fmt.Sprintf("the frame at %d", l.Offset), l.Codec, l.FrameRaw, stored)
}

// RecordOf is a record's raw bytes from its frame's raw bytes, which they share.
func RecordOf(frame []byte, l Location) ([]byte, error) {
	if uint64(len(frame)) != uint64(l.FrameRaw) || uint64(l.InFrame)+uint64(l.RawLength) > uint64(len(frame)) {
		return nil, malformed("a record at %d of a frame of %d bytes, for %d", l.InFrame, len(frame), l.RawLength)
	}
	return frame[l.InFrame : l.InFrame+l.RawLength : l.InFrame+l.RawLength], nil
}

// Decompress turns a whole record's stored bytes back into its raw bytes, checking their length
// but not their ID: for bytes Decode verified before. A Stored record's raw bytes are its stored
// bytes, not a copy.
func Decompress(d Dicts, id encoding.ID, codec Codec, rawLength uint32, stored []byte) ([]byte, error) {
	return decompress(d, "record "+id.String(), codec, rawLength, stored)
}

func decompress(d Dicts, what string, codec Codec, rawLength uint32, stored []byte) ([]byte, error) {
	var raw []byte
	switch codec {
	case Stored:
		raw = stored
	case Zstd:
		var err error
		if raw, err = decoder.DecodeAll(stored, make([]byte, 0, rawLength)); err != nil {
			return nil, malformed("%s: zstd: %v", what, err)
		}
	case ZstdDict:
		if d == nil {
			return nil, malformed("%s: stored with a dictionary, and none is known", what)
		}
		dict, err := DictionaryID(stored)
		if err != nil {
			return nil, malformed("%s: %v", what, err)
		}
		dec, err := d.Decoder(dict)
		if err != nil {
			return nil, fmt.Errorf("%s: dictionary %d: %w", what, dict, err)
		}
		if raw, err = dec.DecodeAll(stored, make([]byte, 0, rawLength)); err != nil {
			return nil, malformed("%s: zstd with dictionary %d: %v", what, dict, err)
		}
	default:
		return nil, malformed("%s: unknown codec %d", what, codec)
	}
	if uint32(len(raw)) != rawLength || uint64(len(raw)) != uint64(rawLength) {
		return nil, malformed("%s: %d bytes, %d recorded", what, len(raw), rawLength)
	}
	return raw, nil
}

// ChunkID is the ID of a chunk's raw bytes: BLAKE3(0x10 ‖ bytes).
func ChunkID(raw []byte) encoding.ID {
	h := encoding.NewHasher(encoding.KindChunk)
	h.Write(raw)
	return h.Sum()
}

// IndexEntry is an entry of a pack's index: a record's ID, and the offset of its frame — or, in a
// version 1 or 2 pack, of the record.
type IndexEntry struct {
	ID     encoding.ID
	Offset int64
}

// ReadIndex reads a pack's footer and index, checking the header and that the index is
// sorted, unique, and points inside the frames or records.
func ReadIndex(r io.ReaderAt, size int64) ([]IndexEntry, error) {
	entries, _, _, err := readIndex(r, size)
	return entries, err
}

// readIndex is ReadIndex, also returning where the frames or records start (the header's length)
// and the pack's version.
func readIndex(r io.ReaderAt, size int64) ([]IndexEntry, int64, uint32, error) {
	if size < minHeaderLen+8+footerLen {
		return nil, 0, 0, malformed("%d bytes is too short", size)
	}
	hdr := make([]byte, minHeaderLen)
	if _, err := r.ReadAt(hdr, 0); err != nil {
		return nil, 0, 0, err
	}
	hlen, v, err := headerLength(hdr)
	if err != nil {
		return nil, 0, 0, err
	}
	if size < hlen+8+footerLen {
		return nil, 0, 0, malformed("%d bytes is too short", size)
	}
	foot := make([]byte, footerLen)
	if _, err := r.ReadAt(foot, size-footerLen); err != nil {
		return nil, 0, 0, err
	}
	if string(foot[8:]) != endMagic {
		return nil, 0, 0, malformed("bad footer magic %q", foot[8:])
	}
	indexOff := int64(binary.LittleEndian.Uint64(foot))
	if indexOff < hlen || indexOff > size-footerLen-8 {
		return nil, 0, 0, malformed("index offset %d out of range", indexOff)
	}
	idx := make([]byte, size-footerLen-indexOff)
	if _, err := r.ReadAt(idx, indexOff); err != nil {
		return nil, 0, 0, err
	}
	count := binary.LittleEndian.Uint64(idx)
	if count > uint64(len(idx)-8)/indexEntryLen || uint64(len(idx)-8) != count*indexEntryLen {
		return nil, 0, 0, malformed("index of %d entries does not fill %d bytes", count, len(idx)-8)
	}
	fixed := int64(recordHeaderLen)
	if v >= 3 {
		fixed = FrameHeaderLen(1)
	}
	entries := make([]IndexEntry, 0, count)
	for i := uint64(0); i < count; i++ {
		e := idx[8+i*indexEntryLen:]
		var ent IndexEntry
		copy(ent.ID[:], e[:encoding.IDLen])
		ent.Offset = int64(binary.LittleEndian.Uint64(e[encoding.IDLen:]))
		if ent.Offset < hlen || ent.Offset+fixed > indexOff {
			return nil, 0, 0, malformed("%s: offset %d out of range", ent.ID, ent.Offset)
		}
		if i > 0 && bytes.Compare(entries[i-1].ID[:], ent.ID[:]) >= 0 {
			return nil, 0, 0, malformed("index not strictly sorted at %s", ent.ID)
		}
		entries = append(entries, ent)
	}
	return entries, hlen, v, nil
}

// Verify checks a whole pack: its bytes hash to wantID, its frames (or records) tile the space
// between the header and the index exactly, every record agrees with the index, and every frame
// decodes (d, a dictionary's) to its records' bytes, each hashing to its ID as one of Kinds. It
// returns the records in pack order, each with its kind and location.
func Verify(r io.ReaderAt, size int64, wantID encoding.ID, d Dicts) ([]Record, error) {
	h := blake3.New()
	if _, err := io.Copy(h, io.NewSectionReader(r, 0, size)); err != nil {
		return nil, err
	}
	var got encoding.ID
	h.Sum(got[:0])
	if got != wantID {
		return nil, malformed("bytes hash to %s, not %s", got, wantID)
	}
	index, hlen, v, err := readIndex(r, size)
	if err != nil {
		return nil, err
	}
	foot := make([]byte, 8)
	if _, err := r.ReadAt(foot, size-footerLen); err != nil {
		return nil, err
	}
	indexOff := int64(binary.LittleEndian.Uint64(foot))
	byID := make(map[encoding.ID]int64, len(index))
	for _, e := range index {
		byID[e.ID] = e.Offset
	}
	var records []Record
	for off := hlen; off < indexOff; {
		var recs []Record
		var end int64
		if v >= 3 {
			recs, end, err = verifyFrame(r, off, indexOff, d)
		} else {
			var rec Record
			rec, end, err = verifyRecord(r, off, indexOff, d)
			recs = []Record{rec}
		}
		if err != nil {
			return nil, err
		}
		for _, rec := range recs {
			if at, ok := byID[rec.ID]; !ok || at != off {
				return nil, malformed("%s at %d disagrees with the index", rec.ID, off)
			}
		}
		records = append(records, recs...)
		off = end
	}
	if len(records) != len(index) {
		return nil, malformed("%d records, %d index entries", len(records), len(index))
	}
	return records, nil
}

// verifyFrame reads and checks the frame at off, before limit: its records, and where it ends.
func verifyFrame(r io.ReaderAt, off, limit int64, d Dicts) ([]Record, int64, error) {
	if off+frameFixedLen > limit {
		return nil, 0, malformed("a frame at %d overruns the index", off)
	}
	fixed := make([]byte, frameFixedLen)
	if _, err := r.ReadAt(fixed, off); err != nil {
		return nil, 0, err
	}
	codec := Codec(fixed[0])
	n := int64(binary.LittleEndian.Uint32(fixed[1:]))
	frameRaw := binary.LittleEndian.Uint32(fixed[5:])
	storedLen := binary.LittleEndian.Uint32(fixed[9:])
	if n == 0 {
		return nil, 0, malformed("the frame at %d holds no records", off)
	}
	if n > (limit-off-frameFixedLen)/memberEntryLen {
		return nil, 0, malformed("the frame at %d: %d records overrun the index", off, n)
	}
	dataOff := off + FrameHeaderLen(int(n))
	end := dataOff + int64(storedLen)
	if end > limit {
		return nil, 0, malformed("the frame at %d overruns the index", off)
	}
	table := make([]byte, n*memberEntryLen)
	if _, err := r.ReadAt(table, off+frameFixedLen); err != nil {
		return nil, 0, err
	}
	stored := make([]byte, storedLen)
	if _, err := r.ReadAt(stored, dataOff); err != nil {
		return nil, 0, err
	}
	frame, err := decompress(d, fmt.Sprintf("the frame at %d", off), codec, frameRaw, stored)
	if err != nil {
		return nil, 0, err
	}
	recs := make([]Record, n)
	var at uint64
	for i := range recs {
		e := table[int64(i)*memberEntryLen:]
		rec := Record{Location: Location{Offset: dataOff, Codec: codec, StoredLength: storedLen,
			Framed: true, FrameRaw: frameRaw, InFrame: uint32(at)}}
		copy(rec.ID[:], e[:encoding.IDLen])
		rec.RawLength = binary.LittleEndian.Uint32(e[encoding.IDLen:])
		if at+uint64(rec.RawLength) > uint64(frameRaw) {
			return nil, 0, malformed("the frame at %d: its records overrun its %d bytes", off, frameRaw)
		}
		raw := frame[at : at+uint64(rec.RawLength)]
		kind, ok := KindOf(rec.ID, raw)
		if !ok {
			return nil, 0, malformed("record %s: its bytes hash to neither a chunk's ID nor a tree's", rec.ID)
		}
		rec.Kind = kind
		recs[i] = rec
		at += uint64(rec.RawLength)
	}
	if at != uint64(frameRaw) {
		return nil, 0, malformed("the frame at %d: its records hold %d of its %d bytes", off, at, frameRaw)
	}
	return recs, end, nil
}

// verifyRecord reads and checks the version 1 or 2 record at off, before limit, and where it ends.
func verifyRecord(r io.ReaderAt, off, limit int64, d Dicts) (Record, int64, error) {
	if off+recordHeaderLen > limit {
		return Record{}, 0, malformed("a record at %d overruns the index", off)
	}
	b := make([]byte, recordHeaderLen)
	if _, err := r.ReadAt(b, off); err != nil {
		return Record{}, 0, err
	}
	var rec Record
	copy(rec.ID[:], b[:encoding.IDLen])
	rec.Offset = off
	rec.Codec = Codec(b[encoding.IDLen])
	rec.RawLength = binary.LittleEndian.Uint32(b[encoding.IDLen+1:])
	rec.StoredLength = binary.LittleEndian.Uint32(b[encoding.IDLen+5:])
	end := rec.DataOffset() + int64(rec.StoredLength)
	if end > limit {
		return Record{}, 0, malformed("%s: record overruns the index", rec.ID)
	}
	stored := make([]byte, rec.StoredLength)
	if _, err := r.ReadAt(stored, rec.DataOffset()); err != nil {
		return Record{}, 0, err
	}
	raw, err := Decompress(d, rec.ID, rec.Codec, rec.RawLength, stored)
	if err != nil {
		return Record{}, 0, err
	}
	kind, ok := KindOf(rec.ID, raw)
	if !ok {
		return Record{}, 0, malformed("record %s: its bytes hash to neither a chunk's ID nor a tree's", rec.ID)
	}
	rec.Kind = kind
	return rec, end, nil
}

// IsMalformed reports whether err is a pack-format error.
func IsMalformed(err error) bool {
	var e *Error
	return errors.As(err, &e)
}
