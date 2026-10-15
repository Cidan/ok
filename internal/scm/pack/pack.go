// Package pack is the SCM's pack file format (docs/scm.md, "Storage"): the S3
// object that holds one repository's file-content chunks. Integers are little-endian.
//
//	magic      "OKSCMPAK"                        8 bytes
//	version    u32 = 2
//	nonce      16 random bytes
//	records    repeated:
//	  chunk id       32 bytes
//	  codec          u8     0 = stored, 1 = zstd, 2 = zstd with the repository's dictionary
//	  raw length     u32
//	  stored length  u32
//	  data           stored-length bytes
//	index      u64 count, then (chunk id, u64 offset of its record), sorted by chunk id
//	footer     u64 offset of the index, magic "OKSCMEND"
//
// A pack's ID is the BLAKE3-256 of all of its bytes. The nonce makes every pack written
// unique, so no two packs share an ID or an S3 key even when they hold the same chunks:
// once garbage collection deletes a pack, nothing can write that key again. Version 1
// packs, written before the nonce, have no nonce and are read the same way. The records and
// index make a pack self-describing, so fsck can verify it against PostgreSQL and rebuild
// chunk rows from it. A chunk stored with a dictionary names it in its zstd frame's header
// (RFC 8878 §3.1.1.1.3), and decoding it needs the dictionary: a Dicts.
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
	version  = 2
	nonceLen = 16

	// minHeaderLen is a version 1 header's length, the shortest a pack has.
	minHeaderLen = 8 + 4
	// RecordHeaderLen is the size of a record's fixed fields before its data.
	RecordHeaderLen = encoding.IDLen + 1 + 4 + 4
	indexEntryLen   = encoding.IDLen + 8
	footerLen       = 8 + 8
)

// Codec is how a chunk's bytes are stored.
type Codec uint8

const (
	Stored Codec = 0
	Zstd   Codec = 1
	// ZstdDict is one zstd frame compressed with a dictionary its header names: the
	// repository's, trained on its small files (docs/scm.md, "Dictionaries").
	ZstdDict Codec = 2
)

// Dicts decodes chunks stored with dictionaries: a decoder that holds the one a frame names.
type Dicts interface {
	Decoder(id uint32) (*zstd.Decoder, error)
}

// DictionaryID is the dictionary a ZstdDict chunk's frame names.
func DictionaryID(stored []byte) (uint32, error) {
	var h zstd.Header
	if err := h.Decode(stored); err != nil {
		return 0, malformed("a zstd frame's header: %v", err)
	}
	if h.DictionaryID == 0 {
		return 0, malformed("a dictionary's frame names no dictionary")
	}
	return h.DictionaryID, nil
}

// Location is where a chunk's record sits in its pack.
type Location struct {
	Offset       int64 // of the record
	Codec        Codec
	RawLength    uint32
	StoredLength uint32
}

// DataOffset is the offset of the record's stored bytes.
func (l Location) DataOffset() int64 { return l.Offset + RecordHeaderLen }

// Record is one chunk's record, as read back from a pack.
type Record struct {
	ID encoding.ID
	Location
}

// Error reports a pack that is not well formed.
type Error struct{ Detail string }

func (e *Error) Error() string { return "pack: " + e.Detail }

func malformed(format string, args ...any) error { return &Error{Detail: fmt.Sprintf(format, args...)} }

var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	decoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
)

// Writer streams a pack. Add records in the order they should sit in the pack; Close
// writes the index and footer and returns the pack's ID.
type Writer struct {
	w      io.Writer
	hasher *blake3.Hasher
	off    int64
	index  []Record
	seen   map[encoding.ID]bool
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

// headerLength is the length of a pack's header, from its first minHeaderLen bytes.
func headerLength(hdr []byte) (int64, error) {
	if len(hdr) < minHeaderLen || string(hdr[:8]) != magic {
		return 0, malformed("bad header %q", hdr)
	}
	switch v := binary.LittleEndian.Uint32(hdr[8:]); v {
	case 1:
		return minHeaderLen, nil
	case 2:
		return minHeaderLen + nonceLen, nil
	default:
		return 0, malformed("unknown version %d", v)
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

// Add appends a chunk's record, zstd-compressed unless that does not shrink it, and
// returns where it landed. A pack holds each chunk once.
func (pw *Writer) Add(id encoding.ID, raw []byte) (Location, error) {
	if pw.err != nil {
		return Location{}, pw.err
	}
	if pw.seen[id] {
		return Location{}, fmt.Errorf("pack: chunk %s added twice", id)
	}
	if uint64(len(raw)) > uint64(^uint32(0)) {
		return Location{}, fmt.Errorf("pack: chunk %s is %d bytes, over the format's limit", id, len(raw))
	}
	codec, stored := Stored, raw
	if c := encoder.EncodeAll(raw, nil); len(c) < len(raw) {
		codec, stored = Zstd, c
	}
	return pw.add(id, codec, uint32(len(raw)), stored)
}

// AddStored appends a chunk's record as another pack stored it — its codec, raw length, and
// stored bytes — without re-encoding them, as compaction copies chunks; or as compaction
// re-encoded it, with a dictionary. The stored bytes must decode to the chunk (d decodes a
// dictionary's).
func (pw *Writer) AddStored(d Dicts, id encoding.ID, codec Codec, rawLength uint32, stored []byte) (Location, error) {
	if pw.err != nil {
		return Location{}, pw.err
	}
	if pw.seen[id] {
		return Location{}, fmt.Errorf("pack: chunk %s added twice", id)
	}
	if _, err := Decode(d, id, codec, rawLength, stored); err != nil {
		return Location{}, err
	}
	return pw.add(id, codec, rawLength, stored)
}

func (pw *Writer) add(id encoding.ID, codec Codec, rawLength uint32, stored []byte) (Location, error) {
	loc := Location{Offset: pw.off, Codec: codec, RawLength: rawLength, StoredLength: uint32(len(stored))}
	rec := make([]byte, 0, RecordHeaderLen)
	rec = append(rec, id[:]...)
	rec = append(rec, byte(codec))
	rec = binary.LittleEndian.AppendUint32(rec, loc.RawLength)
	rec = binary.LittleEndian.AppendUint32(rec, loc.StoredLength)
	pw.write(rec)
	pw.write(stored)
	if pw.err != nil {
		return Location{}, pw.err
	}
	pw.seen[id] = true
	pw.index = append(pw.index, Record{ID: id, Location: loc})
	return loc, nil
}

// Overhead is the bytes a pack holding n chunks spends beyond their stored bytes: its
// header and nonce, n record headers and index entries, the index count, and the footer.
func Overhead(n int) int64 {
	return minHeaderLen + nonceLen + int64(n)*(RecordHeaderLen+indexEntryLen) + 8 + footerLen
}

// Len is the number of records added.
func (pw *Writer) Len() int { return len(pw.index) }

// Close writes the index and footer and returns the pack's ID and size.
func (pw *Writer) Close() (encoding.ID, int64, error) {
	index := slices.Clone(pw.index)
	slices.SortFunc(index, func(a, b Record) int { return bytes.Compare(a.ID[:], b.ID[:]) })
	indexOff := pw.off
	buf := binary.LittleEndian.AppendUint64(nil, uint64(len(index)))
	for _, r := range index {
		buf = append(buf, r.ID[:]...)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(r.Offset))
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

// Decode turns a record's stored bytes back into the chunk's raw bytes and checks them
// against the chunk's ID. d decodes a chunk stored with a dictionary; nil decodes none.
func Decode(d Dicts, id encoding.ID, codec Codec, rawLength uint32, stored []byte) ([]byte, error) {
	raw, err := Decompress(d, id, codec, rawLength, stored)
	if err != nil {
		return nil, err
	}
	if got := ChunkID(raw); got != id {
		return nil, malformed("chunk %s: bytes hash to %s", id, got)
	}
	return raw, nil
}

// Decompress turns a record's stored bytes back into the chunk's raw bytes, checking their
// length but not their ID: for bytes Decode verified before. A Stored record's raw bytes are its
// stored bytes, not a copy.
func Decompress(d Dicts, id encoding.ID, codec Codec, rawLength uint32, stored []byte) ([]byte, error) {
	var raw []byte
	switch codec {
	case Stored:
		raw = stored
	case Zstd:
		var err error
		if raw, err = decoder.DecodeAll(stored, make([]byte, 0, rawLength)); err != nil {
			return nil, malformed("chunk %s: zstd: %v", id, err)
		}
	case ZstdDict:
		if d == nil {
			return nil, malformed("chunk %s: stored with a dictionary, and none is known", id)
		}
		dict, err := DictionaryID(stored)
		if err != nil {
			return nil, malformed("chunk %s: %v", id, err)
		}
		dec, err := d.Decoder(dict)
		if err != nil {
			return nil, fmt.Errorf("chunk %s: dictionary %d: %w", id, dict, err)
		}
		if raw, err = dec.DecodeAll(stored, make([]byte, 0, rawLength)); err != nil {
			return nil, malformed("chunk %s: zstd with dictionary %d: %v", id, dict, err)
		}
	default:
		return nil, malformed("chunk %s: unknown codec %d", id, codec)
	}
	if uint32(len(raw)) != rawLength {
		return nil, malformed("chunk %s: %d bytes, record says %d", id, len(raw), rawLength)
	}
	return raw, nil
}

// ChunkID is the ID of a chunk's raw bytes: BLAKE3(0x10 ‖ bytes).
func ChunkID(raw []byte) encoding.ID {
	h := encoding.NewHasher(encoding.KindChunk)
	h.Write(raw)
	return h.Sum()
}

// ReadIndex reads a pack's footer and index, checking the header and that the index is
// sorted, unique, and points inside the records.
func ReadIndex(r io.ReaderAt, size int64) ([]Record, error) {
	records, _, err := readIndex(r, size)
	return records, err
}

// readIndex is ReadIndex, also returning where the records start (the header's length).
func readIndex(r io.ReaderAt, size int64) ([]Record, int64, error) {
	if size < minHeaderLen+8+footerLen {
		return nil, 0, malformed("%d bytes is too short", size)
	}
	hdr := make([]byte, minHeaderLen)
	if _, err := r.ReadAt(hdr, 0); err != nil {
		return nil, 0, err
	}
	headerLen, err := headerLength(hdr)
	if err != nil {
		return nil, 0, err
	}
	if size < headerLen+8+footerLen {
		return nil, 0, malformed("%d bytes is too short", size)
	}
	foot := make([]byte, footerLen)
	if _, err := r.ReadAt(foot, size-footerLen); err != nil {
		return nil, 0, err
	}
	if string(foot[8:]) != endMagic {
		return nil, 0, malformed("bad footer magic %q", foot[8:])
	}
	indexOff := int64(binary.LittleEndian.Uint64(foot))
	if indexOff < headerLen || indexOff > size-footerLen-8 {
		return nil, 0, malformed("index offset %d out of range", indexOff)
	}
	idx := make([]byte, size-footerLen-indexOff)
	if _, err := r.ReadAt(idx, indexOff); err != nil {
		return nil, 0, err
	}
	count := binary.LittleEndian.Uint64(idx)
	if count > uint64(len(idx)-8)/indexEntryLen || uint64(len(idx)-8) != count*indexEntryLen {
		return nil, 0, malformed("index of %d entries does not fill %d bytes", count, len(idx)-8)
	}
	records := make([]Record, 0, count)
	for i := uint64(0); i < count; i++ {
		e := idx[8+i*indexEntryLen:]
		var rec Record
		copy(rec.ID[:], e[:encoding.IDLen])
		rec.Offset = int64(binary.LittleEndian.Uint64(e[encoding.IDLen:]))
		if rec.Offset < headerLen || rec.Offset+RecordHeaderLen > indexOff {
			return nil, 0, malformed("chunk %s: record offset %d out of range", rec.ID, rec.Offset)
		}
		if i > 0 && bytes.Compare(records[i-1].ID[:], rec.ID[:]) >= 0 {
			return nil, 0, malformed("index not strictly sorted at %s", rec.ID)
		}
		records = append(records, rec)
	}
	return records, headerLen, nil
}

// ReadRecord reads the fixed fields of the record at offset.
func ReadRecord(r io.ReaderAt, offset int64) (Record, error) {
	b := make([]byte, RecordHeaderLen)
	if _, err := r.ReadAt(b, offset); err != nil {
		return Record{}, err
	}
	var rec Record
	copy(rec.ID[:], b[:encoding.IDLen])
	rec.Offset = offset
	rec.Codec = Codec(b[encoding.IDLen])
	rec.RawLength = binary.LittleEndian.Uint32(b[encoding.IDLen+1:])
	rec.StoredLength = binary.LittleEndian.Uint32(b[encoding.IDLen+5:])
	return rec, nil
}

// Verify checks a whole pack: its bytes hash to wantID, its records tile the space
// between the header and the index exactly, every record agrees with the index, and
// every chunk decodes (d, a dictionary's) to bytes that hash to its ID. It returns the records
// in pack order.
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
	index, headerLen, err := readIndex(r, size)
	if err != nil {
		return nil, err
	}
	foot := make([]byte, 8)
	if _, err := r.ReadAt(foot, size-footerLen); err != nil {
		return nil, err
	}
	indexOff := int64(binary.LittleEndian.Uint64(foot))
	byID := make(map[encoding.ID]int64, len(index))
	for _, rec := range index {
		byID[rec.ID] = rec.Offset
	}
	var records []Record
	for off := int64(headerLen); off < indexOff; {
		rec, err := ReadRecord(r, off)
		if err != nil {
			return nil, err
		}
		end := rec.DataOffset() + int64(rec.StoredLength)
		if end > indexOff {
			return nil, malformed("chunk %s: record overruns the index", rec.ID)
		}
		if at, ok := byID[rec.ID]; !ok || at != off {
			return nil, malformed("chunk %s at %d disagrees with the index", rec.ID, off)
		}
		stored := make([]byte, rec.StoredLength)
		if _, err := r.ReadAt(stored, rec.DataOffset()); err != nil {
			return nil, err
		}
		if _, err := Decode(d, rec.ID, rec.Codec, rec.RawLength, stored); err != nil {
			return nil, err
		}
		records = append(records, rec)
		off = end
	}
	if len(records) != len(index) {
		return nil, malformed("%d records, %d index entries", len(records), len(index))
	}
	return records, nil
}

// IsMalformed reports whether err is a pack-format error.
func IsMalformed(err error) bool {
	var e *Error
	return errors.As(err, &e)
}
