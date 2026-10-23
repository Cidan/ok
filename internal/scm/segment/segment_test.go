package segment_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/zeebo/blake3"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
)

// file is a segment in memory, read by range, counting reads.
type file struct {
	b     []byte
	mu    sync.Mutex
	reads int
}

func (f *file) fetch(_ context.Context, off, n int64) ([]byte, error) {
	f.mu.Lock()
	f.reads++
	f.mu.Unlock()
	if off < 0 || off+n > int64(len(f.b)) {
		return nil, fmt.Errorf("a read of %d bytes at %d, past %d", n, off, len(f.b))
	}
	return f.b[off : off+n], nil
}

func write(t *testing.T, kind segment.Kind, rs []segment.Record) (*file, segment.Meta) {
	t.Helper()
	var b bytes.Buffer
	m, err := segment.Write(segment.NewWriter(&b, kind, nil), segment.Records(rs))
	if err != nil {
		t.Fatal(err)
	}
	return &file{b: b.Bytes()}, m
}

func open(t *testing.T, f *file, m segment.Meta, c segment.Cache) *segment.Reader {
	t.Helper()
	r, err := segment.Open(context.Background(), m.ID, int64(len(f.b)), f.fetch, c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// ids are n random 32-byte keys, sorted, each with a value of its own of a varied length.
func ids(n int) []segment.Record {
	rs := make([]segment.Record, n)
	for i := range rs {
		k := make([]byte, 32)
		rand.Read(k)
		rs[i] = segment.Record{Key: k, Value: bytes.Repeat([]byte{byte(i)}, i%300)}
	}
	slices.SortFunc(rs, func(a, b segment.Record) int { return bytes.Compare(a.Key, b.Key) })
	return rs
}

// A segment of many blocks reads back every record, finds none it lacks but for the filter's few
// false positives, and says what it is.
func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	rs := ids(50_000)
	f, m := write(t, 3, rs)
	if m.Records != 50_000 || m.Size != int64(len(f.b)) || m.Kind != 3 || !bytes.Equal(m.First, rs[0].Key) || !bytes.Equal(m.Last, rs[len(rs)-1].Key) {
		t.Fatalf("meta %+v", m)
	}
	if got := encoding.ID(blake3.Sum256(f.b)); got != m.ID {
		t.Fatalf("the ID is not the BLAKE3 of the bytes")
	}
	r := open(t, f, m, nil)
	if r.Kind() != 3 || r.Records() != 50_000 {
		t.Fatalf("opened kind %d, %d records", r.Kind(), r.Records())
	}
	for _, rec := range rs {
		v, ok, err := r.Get(ctx, rec.Key)
		if err != nil || !ok || !bytes.Equal(v, rec.Value) {
			t.Fatalf("get %x: %v %v, %d bytes", rec.Key[:4], ok, err, len(v))
		}
	}
	absent, maybe := ids(20_000), 0
	for _, rec := range absent {
		if r.MayContain(rec.Key) {
			maybe++
		}
		if _, ok, err := r.Get(ctx, rec.Key); ok || err != nil {
			t.Fatalf("found absent key %x: %v", rec.Key[:4], err)
		}
	}
	if rate := float64(maybe) / float64(len(absent)); rate > 0.03 {
		t.Errorf("the filter passed %.1f%% of absent keys", 100*rate)
	}
}

// A scan walks a range of keys in order, across blocks, and stops when told.
func TestScan(t *testing.T) {
	ctx := context.Background()
	var rs []segment.Record
	for i := range 20_000 {
		rs = append(rs, segment.Record{Key: fmt.Appendf(nil, "path/%06d", i), Value: []byte("v")})
	}
	f, m := write(t, 1, rs)
	r := open(t, f, m, nil)
	var got []string
	if err := r.Scan(ctx, []byte("path/001990"), []byte("path/002010"), func(k, _ []byte) bool {
		got = append(got, string(k))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 20 || got[0] != "path/001990" || got[19] != "path/002009" {
		t.Fatalf("scanned %d: %v", len(got), got)
	}
	n := 0
	if err := r.Scan(ctx, nil, nil, func(_, _ []byte) bool { n++; return n < 5 }); err != nil || n != 5 {
		t.Errorf("a scan told to stop at 5 ran %d (%v)", n, err)
	}
	it := r.Iter(ctx, []byte("path/019995"), nil)
	var tail []string
	for it.Next() {
		tail = append(tail, string(it.Key()))
	}
	if it.Err() != nil || len(tail) != 5 || tail[4] != "path/019999" {
		t.Errorf("an iterator to the end: %v (%v)", tail, it.Err())
	}
}

// cache is a segment.Cache in memory, counting what it served.
type cache struct {
	mu   sync.Mutex
	m    map[string]*segment.Block
	hits int
}

func (c *cache) Get(id encoding.ID, block int) (*segment.Block, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.m[fmt.Sprint(id, block)]
	if ok {
		c.hits++
	}
	return b, ok
}

func (c *cache) Put(id encoding.ID, block int, b *segment.Block) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[fmt.Sprint(id, block)] = b
}

// A block read once is served from the cache after.
func TestCache(t *testing.T) {
	ctx := context.Background()
	rs := ids(1000)
	f, m := write(t, 2, rs)
	c := &cache{m: map[string]*segment.Block{}}
	r := open(t, f, m, c)
	before := f.reads
	for range 3 {
		if _, ok, err := r.Get(ctx, rs[500].Key); !ok || err != nil {
			t.Fatal(ok, err)
		}
	}
	if f.reads-before != 1 || c.hits != 2 {
		t.Errorf("three gets read the bucket %d times and the cache %d", f.reads-before, c.hits)
	}
}

// Two segments of the same records are two segments: their bytes, and so their IDs, differ.
func TestUnique(t *testing.T) {
	rs := ids(10)
	_, a := write(t, 1, rs)
	_, b := write(t, 1, rs)
	if a.ID == b.ID {
		t.Error("two segments share an ID")
	}
}

// A writer refuses records out of order, and a segment of none; a reader, bytes that are not one.
func TestRefusals(t *testing.T) {
	w := segment.NewWriter(&bytes.Buffer{}, 1, nil)
	if err := w.Add([]byte("b"), nil); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"b", "a"} {
		if err := w.Add([]byte(k), nil); err == nil {
			t.Errorf("added %q after b", k)
		}
	}
	if _, err := segment.NewWriter(&bytes.Buffer{}, 1, nil).Finish(); err == nil {
		t.Error("finished a segment of no records")
	}
	f, m := write(t, 1, ids(100))
	for name, mangle := range map[string]func([]byte) []byte{
		"its magic": func(b []byte) []byte { b[0] ^= 1; return b },
		"its tail":  func(b []byte) []byte { b[len(b)-1] ^= 1; return b },
		"truncated": func(b []byte) []byte { return b[:len(b)/2] },
		"its fence": func(b []byte) []byte { return b[:10] },
	} {
		bad := &file{b: mangle(bytes.Clone(f.b))}
		if _, err := segment.Open(context.Background(), m.ID, int64(len(bad.b)), bad.fetch, nil); !errors.Is(err, segment.ErrCorrupt) {
			t.Errorf("a segment with %s mangled: %v, want ErrCorrupt", name, err)
		}
	}
}

// A merge of sources walks every key once, in order, each with the newest source's value.
func TestMerge(t *testing.T) {
	ctx := context.Background()
	rec := func(k, v string) segment.Record { return segment.Record{Key: []byte(k), Value: []byte(v)} }
	old, oldMeta := write(t, 1, []segment.Record{rec("a", "old"), rec("c", "old"), rec("e", "old")})
	r := open(t, old, oldMeta, nil)
	newer := segment.Records([]segment.Record{rec("b", "new"), rec("c", "new")})
	newest := segment.Records([]segment.Record{rec("c", "newest"), rec("f", "newest")})
	var b bytes.Buffer
	m, err := segment.Write(segment.NewWriter(&b, 1, nil), segment.Merge(newest, newer, r.Iter(ctx, nil, nil)))
	if err != nil {
		t.Fatal(err)
	}
	got := open(t, &file{b: b.Bytes()}, m, nil)
	var out []string
	got.Scan(ctx, nil, nil, func(k, v []byte) bool { out = append(out, string(k)+"="+string(v)); return true })
	if want := []string{"a=old", "b=new", "c=newest", "e=old", "f=newest"}; !slices.Equal(out, want) {
		t.Errorf("merged %v, want %v", out, want)
	}
}

// A segment whose fence and filter outgrow the end Open reads first is opened with one more read,
// and reads back.
func TestLargeIndex(t *testing.T) {
	ctx := context.Background()
	rs := make([]segment.Record, 300_000)
	for i := range rs {
		rs[i] = segment.Record{Key: binary.BigEndian.AppendUint64(nil, uint64(i)*7919)}
	}
	f, m := write(t, 4, rs)
	f.reads = 0
	r := open(t, f, m, nil)
	if f.reads != 3 {
		t.Errorf("opened with %d reads, want 3: the end, the rest of the fence and filter, and the header", f.reads)
	}
	for _, i := range []int{0, 1, 150_000, 299_999} {
		if _, ok, err := r.Get(ctx, rs[i].Key); !ok || err != nil {
			t.Fatalf("get record %d: %v, %v", i, ok, err)
		}
	}
}

// A descending walk reads a range newest key first, across blocks, and a descending merge of
// sources walks every key once, newest source winning.
func TestDescending(t *testing.T) {
	ctx := context.Background()
	var rs []segment.Record
	for i := range 20_000 {
		rs = append(rs, segment.Record{Key: fmt.Appendf(nil, "p/%06d", i), Value: []byte("old")})
	}
	f, m := write(t, 1, rs)
	r := open(t, f, m, nil)
	var got []string
	it := r.IterDesc(ctx, []byte("p/001990"), []byte("p/002010"))
	for it.Next() {
		got = append(got, string(it.Key()))
	}
	if it.Err() != nil || len(got) != 20 || got[0] != "p/002009" || got[19] != "p/001990" {
		t.Fatalf("a descending range: %d, %v ... (%v)", len(got), got[:min(3, len(got))], it.Err())
	}
	got = nil
	for it := r.IterDesc(ctx, nil, nil); it.Next() && len(got) < 3; {
		got = append(got, string(it.Key()))
	}
	if !slices.Equal(got, []string{"p/019999", "p/019998", "p/019997"}) {
		t.Fatalf("the end, descending: %v", got)
	}
	if it := r.IterDesc(ctx, nil, []byte("p/")); it.Next() {
		t.Fatalf("a range before every key yielded %q", it.Key())
	}
	newer := segment.Records([]segment.Record{{Key: []byte("p/019999"), Value: []byte("new")}})
	// A source walked descending: its records given in descending order.
	desc := func(rs ...segment.Record) segment.Iterator { return segment.Records(rs) }
	mg := segment.MergeDesc(desc(segment.Record{Key: []byte("p/020001"), Value: []byte("newest")}), newer, r.IterDesc(ctx, []byte("p/019997"), nil))
	var out []string
	for mg.Next() {
		out = append(out, string(mg.Key())+"="+string(mg.Value()))
	}
	if want := []string{"p/020001=newest", "p/019999=new", "p/019998=old", "p/019997=old"}; !slices.Equal(out, want) {
		t.Errorf("a descending merge: %v, want %v", out, want)
	}
}

// A segment written with a Prefix answers whether it may hold a key with a prefix from its filter:
// for every prefix it holds, yes; for nearly every other, no; and one written without answers by its
// range of keys alone.
func TestPrefixes(t *testing.T) {
	ctx := context.Background()
	parentOf := func(k []byte) []byte { return k[:4] }
	var rs []segment.Record
	for p := range 2000 {
		for c := range 3 {
			rs = append(rs, segment.Record{Key: binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, uint32(2*p)), uint32(c))})
		}
	}
	var b bytes.Buffer
	m, err := segment.Write(segment.NewWriter(&b, 3, parentOf), segment.Records(rs))
	if err != nil {
		t.Fatal(err)
	}
	r := open(t, &file{b: b.Bytes()}, m, nil)
	for p := range 2000 {
		if !r.MayContainPrefix(binary.BigEndian.AppendUint32(nil, uint32(2*p))) {
			t.Fatalf("prefix %d, which the segment holds, refused", 2*p)
		}
	}
	maybe := 0
	for p := range 2000 {
		if r.MayContainPrefix(binary.BigEndian.AppendUint32(nil, uint32(2*p+1))) {
			maybe++
		}
	}
	if maybe > 60 {
		t.Errorf("%d of 2000 absent prefixes passed the filter", maybe)
	}
	if r.MayContainPrefix(binary.BigEndian.AppendUint32(nil, 1<<31)) {
		t.Error("a prefix past the last key passed")
	}
	if v, ok, err := r.Get(ctx, rs[100].Key); err != nil || !ok || len(v) != 0 {
		t.Errorf("a whole key, with prefixes in the filter too: %v, %v", ok, err)
	}
	plain, pm := write(t, 3, rs)
	pr := open(t, plain, pm, nil)
	if !pr.MayContainPrefix(binary.BigEndian.AppendUint32(nil, 3)) || pr.MayContainPrefix(binary.BigEndian.AppendUint32(nil, 1<<31)) {
		t.Error("a segment without prefixes in its filter answered by more than its range")
	}
}

// countingCache is a Cache that counts what is put in it.
type countingCache struct {
	mu   sync.Mutex
	m    map[[2]any]*segment.Block
	puts int
}

func (c *countingCache) Get(id encoding.ID, block int) (*segment.Block, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.m[[2]any{id, block}]
	return b, ok
}

func (c *countingCache) Put(id encoding.ID, block int, b *segment.Block) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[[2]any{id, block}] = b
	c.puts++
}

// A walk over a segment reads ahead: a whole scan fetches a few MiB a request, not a block, in
// either direction, putting no more than its first block in the cache; a short walk reads only the
// blocks its range needs.
func TestReadAhead(t *testing.T) {
	ctx := context.Background()
	var rs []segment.Record
	for i := range 200_000 {
		v := make([]byte, 100)
		for j := range v {
			v[j] = byte(i*31 + j*7)
		}
		rs = append(rs, segment.Record{Key: fmt.Appendf(nil, "k/%07d", i), Value: v})
	}
	f, m := write(t, 1, rs)
	reads := func() int {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.reads
	}
	for _, desc := range []bool{false, true} {
		c := &countingCache{m: map[[2]any]*segment.Block{}}
		r := open(t, f, m, c)
		before := reads()
		var it segment.Iterator
		if desc {
			it = r.IterDesc(ctx, nil, nil)
		} else {
			it = r.Iter(ctx, nil, nil)
		}
		n := 0
		for it.Next() {
			n++
		}
		if it.Err() != nil || n != len(rs) {
			t.Fatalf("a scan (descending %v) walked %d of %d: %v", desc, n, len(rs), it.Err())
		}
		blocks, fetched := r.Blocks(), reads()-before
		// About 21 MiB of records, at most 4 MiB a fetch after the doubling's first few.
		if fetched > 20 || blocks < 100 {
			t.Errorf("a scan (descending %v) of %d blocks fetched %d times", desc, blocks, fetched)
		}
		if c.puts > 1 {
			t.Errorf("a scan (descending %v) put %d blocks in the cache", desc, c.puts)
		}
	}
	// A walk of a few records reads only their blocks.
	r := open(t, f, m, nil)
	before := reads()
	it := r.Iter(ctx, []byte("k/0100000"), []byte("k/0100010"))
	n := 0
	for it.Next() {
		n++
	}
	if n != 10 || reads()-before > 2 {
		t.Errorf("a walk of 10 records: %d, with %d fetches", n, reads()-before)
	}
}

// A batch of lookups answers as lookups one at a time do, reading the blocks its keys lie in with
// one fetch for each run of consecutive ones, and caching them as lookups do.
func TestGetMany(t *testing.T) {
	ctx := context.Background()
	var rs []segment.Record
	for i := range 50_000 {
		rs = append(rs, segment.Record{Key: fmt.Appendf(nil, "k/%06d", i), Value: fmt.Appendf(nil, "value of %d, long enough to fill blocks", i)})
	}
	f, m := write(t, 1, rs)
	c := &countingCache{m: map[[2]any]*segment.Block{}}
	r := open(t, f, m, c)
	// Every 50th key, then a run of 2,000 consecutive ones, and some absent.
	var keys [][]byte
	for i := 0; i < 50_000; i += 50 {
		keys = append(keys, fmt.Appendf(nil, "k/%06d", i))
	}
	for i := 20_000; i < 22_000; i++ {
		keys = append(keys, fmt.Appendf(nil, "k/%06d", i))
	}
	keys = append(keys, []byte("k/0000005x"), []byte("zz"))
	f.mu.Lock()
	before := f.reads
	f.mu.Unlock()
	got, err := r.GetMany(ctx, keys, 4)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	fetched := f.reads - before
	f.mu.Unlock()
	for _, k := range keys {
		v, ok, err := r.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		if g, gok := got[string(k)]; gok != ok || !bytes.Equal(g, v) {
			t.Fatalf("key %s: GetMany %q (%v), Get %q (%v)", k, g, gok, v, ok)
		}
	}
	if blocks := r.Blocks(); fetched >= blocks/4 || c.puts == 0 {
		t.Errorf("a batch over %d blocks fetched %d times and cached %d", blocks, fetched, c.puts)
	}
	f.mu.Lock()
	before = f.reads
	f.mu.Unlock()
	if _, err := r.GetMany(ctx, keys, 4); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reads != before {
		t.Errorf("the same batch again fetched %d times, its blocks cached", f.reads-before)
	}
}
