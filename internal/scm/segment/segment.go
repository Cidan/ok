// Package segment is the file format of the SCM's segments (docs/hosted.md, "Segments, as built"):
// an immutable, sorted run of records — a key and a value each — of one kind, written once to
// object storage and read by range, through a fence index of its blocks and a Bloom filter of its
// keys that a reader keeps in memory.
//
//	magic "OKSCMSEG", version u32 = 1, kind u8, nonce 16 random bytes
//	blocks  zstd frames of about 64 KiB of records: key length uvarint, key, value length uvarint, value
//	fence   per block: its first key (length uvarint, key), offset u64, stored length u32, raw length u32
//	bloom   a split-block Bloom filter over the keys
//	meta    records u64; fence offset u64, length u32; bloom offset u64, length u32; first and last
//	        key (length uvarint, key); flags u8 (1: the filter holds the keys' prefixes too)
//	tail    meta length u32, magic "OKSCMEND"
//
// The nonce makes every segment's bytes, so its ID — the BLAKE3 of them — and its key in the
// bucket, its own: a deleted segment's key is never written again. Integers are little-endian.
package segment

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/blake3"
	"golang.org/x/sync/errgroup"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// Kind is what a segment holds; the store names its kinds.
type Kind uint8

const (
	magic     = "OKSCMSEG"
	endMagic  = "OKSCMEND"
	version   = 1
	headerLen = 8 + 4 + 1 + 16
	tailLen   = 4 + 8
	// blockSize is the raw size past which a block is closed.
	blockSize = 64 << 10
	// maxRecord bounds a key or a value: a block holds at least one record.
	maxRecord = 64 << 20
	// flagPrefixes marks a segment whose Bloom filter holds its keys' prefixes too.
	flagPrefixes = 1
)

// Prefix is the part of a key that a read of a range names: a path's records' path, a parent's
// edges' parent. A segment written with one holds its keys' prefixes in its Bloom filter too, so a
// read of the keys with a prefix skips a segment that holds none.
type Prefix func(key []byte) []byte

// ErrCorrupt reports a segment whose bytes do not parse.
var ErrCorrupt = errors.New("segment: corrupt")

var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	decoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
)

// Meta is what a written segment is: its ID (the BLAKE3 of its bytes), size, records, and first
// and last keys.
type Meta struct {
	ID      encoding.ID
	Kind    Kind
	Size    int64
	Records int64
	First   []byte
	Last    []byte
}

// fenceEntry is a block's place.
type fenceEntry struct {
	first  []byte
	offset int64
	stored uint32
	raw    uint32
}

// Writer writes one segment to w, its records added in strictly increasing key order.
type Writer struct {
	w      io.Writer
	hash   *blake3.Hasher
	kind   Kind
	off    int64
	block  bytes.Buffer
	first  []byte // the open block's first key
	fence  []fenceEntry
	filter [][]byte // every key, and with prefix every distinct prefix: the Bloom filter's
	prefix Prefix
	// lastPrefix is the last key's prefix.
	lastPrefix []byte
	records    int64
	last       []byte
	err        error
	done       bool
}

// NewWriter starts a segment of kind on w; with prefix (nil: none), its Bloom filter holds its
// keys' prefixes too.
func NewWriter(w io.Writer, kind Kind, prefix Prefix) *Writer {
	sw := &Writer{w: w, hash: blake3.New(), kind: kind, prefix: prefix}
	var h [headerLen]byte
	copy(h[:], magic)
	binary.LittleEndian.PutUint32(h[8:], version)
	h[12] = byte(kind)
	rand.Read(h[13:])
	sw.write(h[:])
	return sw
}

func (sw *Writer) write(b []byte) {
	if sw.err != nil {
		return
	}
	if _, err := sw.w.Write(b); err != nil {
		sw.err = err
		return
	}
	sw.hash.Write(b)
	sw.off += int64(len(b))
}

// Add adds a record. Its key must sort after the last one added.
func (sw *Writer) Add(key, value []byte) error {
	if sw.done {
		return errors.New("segment: add after finish")
	}
	if sw.err != nil {
		return sw.err
	}
	if len(key) > maxRecord || len(value) > maxRecord {
		return fmt.Errorf("segment: a record of %d and %d bytes is past the most a record holds", len(key), len(value))
	}
	if sw.records > 0 && bytes.Compare(key, sw.last) <= 0 {
		return fmt.Errorf("segment: key %x added after %x", key, sw.last)
	}
	if sw.block.Len() == 0 {
		sw.first = bytes.Clone(key)
	}
	sw.block.Write(binary.AppendUvarint(nil, uint64(len(key))))
	sw.block.Write(key)
	sw.block.Write(binary.AppendUvarint(nil, uint64(len(value))))
	sw.block.Write(value)
	sw.last = bytes.Clone(key)
	sw.filter = append(sw.filter, sw.last)
	if sw.prefix != nil {
		// Keys are sorted, so a prefix's keys are together: each prefix is added once.
		if p := sw.prefix(sw.last); sw.lastPrefix == nil || !bytes.Equal(p, sw.lastPrefix) {
			sw.lastPrefix = bytes.Clone(p)
			sw.filter = append(sw.filter, sw.lastPrefix)
		}
	}
	sw.records++
	if sw.block.Len() >= blockSize {
		sw.closeBlock()
	}
	return sw.err
}

func (sw *Writer) closeBlock() {
	if sw.block.Len() == 0 {
		return
	}
	stored := encoder.EncodeAll(sw.block.Bytes(), nil)
	sw.fence = append(sw.fence, fenceEntry{first: sw.first, offset: sw.off, stored: uint32(len(stored)), raw: uint32(sw.block.Len())})
	sw.write(stored)
	sw.block.Reset()
}

// Finish writes the segment's fence, Bloom filter, and meta, and says what it wrote. A segment of
// no records is refused.
func (sw *Writer) Finish() (Meta, error) {
	if sw.done {
		return Meta{}, errors.New("segment: finished twice")
	}
	sw.done = true
	if sw.records == 0 {
		return Meta{}, errors.New("segment: no records")
	}
	sw.closeBlock()
	fenceOff := sw.off
	var fence []byte
	for _, f := range sw.fence {
		fence = binary.AppendUvarint(fence, uint64(len(f.first)))
		fence = append(fence, f.first...)
		fence = binary.LittleEndian.AppendUint64(fence, uint64(f.offset))
		fence = binary.LittleEndian.AppendUint32(fence, f.stored)
		fence = binary.LittleEndian.AppendUint32(fence, f.raw)
	}
	sw.write(fence)
	bloomOff := sw.off
	bl := newBloom(len(sw.filter))
	for _, k := range sw.filter {
		bl.add(k)
	}
	blb := bl.bytes()
	sw.write(blb)
	first := sw.fence[0].first
	var meta []byte
	meta = binary.LittleEndian.AppendUint64(meta, uint64(sw.records))
	meta = binary.LittleEndian.AppendUint64(meta, uint64(fenceOff))
	meta = binary.LittleEndian.AppendUint32(meta, uint32(len(fence)))
	meta = binary.LittleEndian.AppendUint64(meta, uint64(bloomOff))
	meta = binary.LittleEndian.AppendUint32(meta, uint32(len(blb)))
	meta = binary.AppendUvarint(meta, uint64(len(first)))
	meta = append(meta, first...)
	meta = binary.AppendUvarint(meta, uint64(len(sw.last)))
	meta = append(meta, sw.last...)
	var flags byte
	if sw.prefix != nil {
		flags |= flagPrefixes
	}
	meta = append(meta, flags)
	sw.write(meta)
	var tail [tailLen]byte
	binary.LittleEndian.PutUint32(tail[:4], uint32(len(meta)))
	copy(tail[4:], endMagic)
	sw.write(tail[:])
	if sw.err != nil {
		return Meta{}, sw.err
	}
	var id encoding.ID
	copy(id[:], sw.hash.Sum(nil))
	return Meta{ID: id, Kind: sw.kind, Size: sw.off, Records: sw.records, First: first, Last: sw.last}, nil
}

// Fetch reads length bytes of a segment from offset.
type Fetch func(ctx context.Context, offset, length int64) ([]byte, error)

// Block is a block's records, decoded: what a reader's cache holds.
type Block struct {
	raw  []byte
	offs []uint32 // each record's start in raw
}

// Bytes is how much memory the block holds, for a cache's budget.
func (b *Block) Bytes() int { return len(b.raw) + 4*len(b.offs) }

func (b *Block) record(i int) (key, value []byte) {
	r := b.raw[b.offs[i]:]
	kl, n := binary.Uvarint(r)
	key = r[n : n+int(kl)]
	r = r[n+int(kl):]
	vl, n := binary.Uvarint(r)
	return key, r[n : n+int(vl)]
}

// search is the index of the first record whose key is at least key.
func (b *Block) search(key []byte) int {
	return sort.Search(len(b.offs), func(i int) bool {
		k, _ := b.record(i)
		return bytes.Compare(k, key) >= 0
	})
}

func decodeBlock(raw []byte) (*Block, error) {
	b := &Block{raw: raw}
	for p := 0; p < len(raw); {
		b.offs = append(b.offs, uint32(p))
		kl, n := binary.Uvarint(raw[p:])
		if n <= 0 || uint64(len(raw)-p-n) < kl {
			return nil, ErrCorrupt
		}
		p += n + int(kl)
		vl, n := binary.Uvarint(raw[p:])
		if n <= 0 || uint64(len(raw)-p-n) < vl {
			return nil, ErrCorrupt
		}
		p += n + int(vl)
	}
	return b, nil
}

// Cache holds decoded blocks of segments; the reader's key for a block is its segment's ID and
// the block's index. Segments never change, so an entry is never stale.
type Cache interface {
	Get(id encoding.ID, block int) (*Block, bool)
	Put(id encoding.ID, block int, b *Block)
}

// Reader reads one segment: its fence and Bloom filter in memory, its blocks by range.
type Reader struct {
	id      encoding.ID
	kind    Kind
	records int64
	first   []byte
	last    []byte
	fence   []fenceEntry
	bloom   bloom
	// prefixed says the filter holds the keys' prefixes too.
	prefixed bool
	fetch    Fetch
	cache    Cache
	// keep, when set, hides every record it refuses (Bounded).
	keep func(key, value []byte) bool
}

// Bounded is the segment with only the records keep accepts: a reader sharing r's filter, fence,
// and blocks, whose lookups and walks skip the rest.
func (r *Reader) Bounded(keep func(key, value []byte) bool) *Reader {
	b := *r
	b.keep = keep
	return &b
}

// kept reports whether a record is the reader's.
func (r *Reader) kept(key, value []byte) bool { return r.keep == nil || r.keep(key, value) }

// openRead is how much of a segment's end Open reads at once, for the fence, filter, and meta of
// most segments in one read.
const openRead = 256 << 10

// Open reads segment id's fence, Bloom filter, and meta: the segment is size bytes, read by fetch,
// its blocks kept in cache (nil: none).
func Open(ctx context.Context, id encoding.ID, size int64, fetch Fetch, cache Cache) (*Reader, error) {
	if size < headerLen+tailLen {
		return nil, ErrCorrupt
	}
	readFrom := max(0, size-openRead)
	end, err := fetch(ctx, readFrom, size-readFrom)
	if err != nil {
		return nil, err
	}
	if int64(len(end)) != size-readFrom || string(end[len(end)-8:]) != endMagic {
		return nil, ErrCorrupt
	}
	metaLen := int64(binary.LittleEndian.Uint32(end[len(end)-tailLen:]))
	metaOff := size - tailLen - metaLen
	if metaOff < headerLen {
		return nil, ErrCorrupt
	}
	// at reads [off, off+n) of the segment, from what the first read holds where it can.
	at := func(off, n int64) ([]byte, error) {
		if off >= readFrom {
			return end[off-readFrom : off-readFrom+n], nil
		}
		return fetch(ctx, off, n)
	}
	meta, err := at(metaOff, metaLen)
	if err != nil {
		return nil, err
	}
	if len(meta) < 32 {
		return nil, ErrCorrupt
	}
	r := &Reader{id: id, fetch: fetch, cache: cache}
	r.records = int64(binary.LittleEndian.Uint64(meta))
	fenceOff := int64(binary.LittleEndian.Uint64(meta[8:]))
	fenceLen := int64(binary.LittleEndian.Uint32(meta[16:]))
	bloomOff := int64(binary.LittleEndian.Uint64(meta[20:]))
	bloomLen := int64(binary.LittleEndian.Uint32(meta[28:]))
	rest := meta[32:]
	var ok bool
	if r.first, rest, ok = lengthPrefixed(rest); !ok {
		return nil, ErrCorrupt
	}
	if r.last, rest, ok = lengthPrefixed(rest); !ok || len(rest) != 1 {
		return nil, ErrCorrupt
	}
	r.prefixed = rest[0]&flagPrefixes != 0
	if fenceOff < headerLen || fenceOff+fenceLen != bloomOff || bloomOff+bloomLen != metaOff {
		return nil, ErrCorrupt
	}
	fb, err := at(fenceOff, fenceLen+bloomLen)
	if err != nil {
		return nil, err
	}
	for f := fb[:fenceLen]; len(f) > 0; {
		var e fenceEntry
		if e.first, f, ok = lengthPrefixed(f); !ok || len(f) < 16 {
			return nil, ErrCorrupt
		}
		e.offset = int64(binary.LittleEndian.Uint64(f))
		e.stored = binary.LittleEndian.Uint32(f[8:])
		e.raw = binary.LittleEndian.Uint32(f[12:])
		f = f[16:]
		if e.offset < headerLen || e.offset+int64(e.stored) > fenceOff {
			return nil, ErrCorrupt
		}
		r.fence = append(r.fence, e)
	}
	if len(r.fence) == 0 {
		return nil, ErrCorrupt
	}
	if r.bloom, err = loadBloom(fb[fenceLen:]); err != nil {
		return nil, err
	}
	head, err := at(0, headerLen)
	if err != nil {
		return nil, err
	}
	if string(head[:8]) != magic || binary.LittleEndian.Uint32(head[8:]) != version {
		return nil, ErrCorrupt
	}
	r.kind = Kind(head[12])
	return r, nil
}

func lengthPrefixed(b []byte) (v, rest []byte, ok bool) {
	n, m := binary.Uvarint(b)
	if m <= 0 || uint64(len(b)-m) < n {
		return nil, nil, false
	}
	return b[m : m+int(n)], b[m+int(n):], true
}

// Kind is what the segment holds.
func (r *Reader) Kind() Kind { return r.kind }

// Records is how many records the segment holds.
func (r *Reader) Records() int64 { return r.records }

// Bytes is how much memory the reader holds — its fence and filter — for a cache's budget.
func (r *Reader) Bytes() int {
	n := 4*len(r.bloom.words) + len(r.first) + len(r.last)
	for _, f := range r.fence {
		n += len(f.first) + 40
	}
	return n
}

// MayContainPrefix reports whether a key beginning with p may be in the segment: false is certain.
// p must be what the writer's Prefix gives of such keys; a segment written with none answers by its
// range of keys alone.
func (r *Reader) MayContainPrefix(p []byte) bool {
	if bytes.Compare(p, r.last) > 0 {
		return false
	}
	if end := prefixEnd(p); end != nil && bytes.Compare(r.first, end) >= 0 {
		return false
	}
	return !r.prefixed || r.bloom.has(p)
}

// prefixEnd is the first key after every key beginning with p, or nil when there is none.
func prefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// MayContain reports whether key may be in the segment: false is certain.
func (r *Reader) MayContain(key []byte) bool {
	return bytes.Compare(key, r.first) >= 0 && bytes.Compare(key, r.last) <= 0 && r.bloom.has(key)
}

// Blocks is how many blocks the segment holds.
func (r *Reader) Blocks() int { return len(r.fence) }

// block reads block i, through the cache.
func (r *Reader) block(ctx context.Context, i int) (*Block, error) {
	if b, ok := r.cached(i); ok {
		return b, nil
	}
	bs, err := r.blocks(ctx, i, i)
	if err != nil {
		return nil, err
	}
	if r.cache != nil {
		r.cache.Put(r.id, i, bs[0])
	}
	return bs[0], nil
}

func (r *Reader) cached(i int) (*Block, bool) {
	if r.cache == nil {
		return nil, false
	}
	return r.cache.Get(r.id, i)
}

// blocks reads blocks lo through hi, which lie one after another in the segment, with one fetch.
func (r *Reader) blocks(ctx context.Context, lo, hi int) ([]*Block, error) {
	start := r.fence[lo].offset
	end := r.fence[hi].offset + int64(r.fence[hi].stored)
	stored, err := r.fetch(ctx, start, end-start)
	if err != nil {
		return nil, err
	}
	out := make([]*Block, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		f := r.fence[i]
		off := f.offset - start
		raw, err := decoder.DecodeAll(stored[off:off+int64(f.stored)], make([]byte, 0, f.raw))
		if err != nil || len(raw) != int(f.raw) {
			return nil, fmt.Errorf("%w: block %d of segment %x", ErrCorrupt, i, r.id[:8])
		}
		b, err := decodeBlock(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

// readAhead bounds what a walk over a segment reads with one fetch.
const readAhead = 4 << 20

// walker reads the blocks a walk over a segment needs: its first block as a point read does,
// through the cache, then each time it needs one it holds not, twice as many consecutive blocks as
// the last time with one fetch, up to readAhead bytes and no further than the walk's bound — so a
// scan reads a segment a few MiB a request, a short walk no more than it shows it needs, and
// blocks read ahead, a scan's, leave the cache to what lookups keep.
type walker struct {
	r       *Reader
	run     int
	ahead   []*Block // blocks read ahead, block aheadAt first
	aheadAt int
}

// get returns block i; within says whether a block, past i in the walk's direction, is one the
// walk may need.
func (w *walker) get(ctx context.Context, i, step int, within func(int) bool) (*Block, error) {
	if k := (i - w.aheadAt) * step; w.ahead != nil && k >= 0 && k < len(w.ahead) {
		return w.ahead[k], nil
	}
	if w.run == 0 {
		w.run = 1
		return w.r.block(ctx, i)
	}
	if b, ok := w.r.cached(i); ok {
		return b, nil
	}
	w.run *= 2
	last, size := i, int64(w.r.fence[i].stored)
	for n := 1; n < w.run; n++ {
		j := last + step
		if j < 0 || j >= len(w.r.fence) || !within(j) || size+int64(w.r.fence[j].stored) > readAhead {
			break
		}
		if _, ok := w.r.cached(j); ok {
			break
		}
		last, size = j, size+int64(w.r.fence[j].stored)
	}
	lo, hi := min(i, last), max(i, last)
	bs, err := w.r.blocks(ctx, lo, hi)
	if err != nil {
		return nil, err
	}
	if step < 0 {
		for a, b := 0, len(bs)-1; a < b; a, b = a+1, b-1 {
			bs[a], bs[b] = bs[b], bs[a]
		}
	}
	w.ahead, w.aheadAt = bs, i
	return bs[0], nil
}

// blockOf is the index of the block key would be in: the last whose first key is not past it.
func (r *Reader) blockOf(key []byte) int {
	return sort.Search(len(r.fence), func(i int) bool { return bytes.Compare(r.fence[i].first, key) > 0 }) - 1
}

// Get reads key's value.
func (r *Reader) Get(ctx context.Context, key []byte) (value []byte, ok bool, err error) {
	if !r.MayContain(key) {
		return nil, false, nil
	}
	i := r.blockOf(key)
	if i < 0 {
		return nil, false, nil
	}
	b, err := r.block(ctx, i)
	if err != nil {
		return nil, false, err
	}
	v, ok := b.get(key)
	if ok && !r.kept(key, v) {
		return nil, false, nil
	}
	return v, ok, nil
}

// get is key's value in the block, if it holds key.
func (b *Block) get(key []byte) ([]byte, bool) {
	j := b.search(key)
	if j == len(b.offs) {
		return nil, false
	}
	k, v := b.record(j)
	if !bytes.Equal(k, key) {
		return nil, false
	}
	return v, true
}

// GetMany looks up keys, returning each found's value by key. The blocks they lie in that the
// cache lacks are read with as few fetches as their order allows — consecutive blocks with one, up
// to readAhead bytes — at most parallel at once, and cached as a lookup's are.
func (r *Reader) GetMany(ctx context.Context, keys [][]byte, parallel int) (map[string][]byte, error) {
	byBlock := map[int][][]byte{}
	for _, k := range keys {
		if !r.MayContain(k) {
			continue
		}
		if i := r.blockOf(k); i >= 0 {
			byBlock[i] = append(byBlock[i], k)
		}
	}
	blocks := make(map[int]*Block, len(byBlock))
	var need []int
	for i := range byBlock {
		if b, ok := r.cached(i); ok {
			blocks[i] = b
		} else {
			need = append(need, i)
		}
	}
	sort.Ints(need)
	var runs [][2]int
	for k := 0; k < len(need); {
		lo, hi, size := need[k], need[k], int64(r.fence[need[k]].stored)
		for k+1 < len(need) && need[k+1] == hi+1 && size+int64(r.fence[hi+1].stored) <= readAhead {
			k++
			hi++
			size += int64(r.fence[hi].stored)
		}
		runs = append(runs, [2]int{lo, hi})
		k++
	}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(parallel, 1))
	for _, run := range runs {
		g.Go(func() error {
			bs, err := r.blocks(gctx, run[0], run[1])
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			for n, b := range bs {
				blocks[run[0]+n] = b
				if r.cache != nil {
					r.cache.Put(r.id, run[0]+n, b)
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	out := map[string][]byte{}
	for i, ks := range byBlock {
		for _, k := range ks {
			if v, ok := blocks[i].get(k); ok && r.kept(k, v) {
				out[string(k)] = v
			}
		}
	}
	return out, nil
}

// Scan calls fn with each record whose key is at least from and before to (nil: the end), in
// key order, until fn returns false.
func (r *Reader) Scan(ctx context.Context, from, to []byte, fn func(key, value []byte) bool) error {
	i := max(r.blockOf(from), 0)
	for ; i < len(r.fence); i++ {
		if to != nil && bytes.Compare(r.fence[i].first, to) >= 0 {
			return nil
		}
		b, err := r.block(ctx, i)
		if err != nil {
			return err
		}
		for j := b.search(from); j < len(b.offs); j++ {
			k, v := b.record(j)
			if to != nil && bytes.Compare(k, to) >= 0 {
				return nil
			}
			if !fn(k, v) {
				return nil
			}
		}
	}
	return nil
}
