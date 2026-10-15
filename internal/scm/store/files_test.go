package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/serve/metrics"
)

// countingBlobs counts a store's range reads and the most in flight at once; it can slow each
// read down, and fail one.
type countingBlobs struct {
	blob.Store
	delay    time.Duration
	failAt   int64 // the 1-based range read that fails; 0 for none
	gets     atomic.Int64
	inFlight atomic.Int64
	peak     atomic.Int64
}

func (c *countingBlobs) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	n := c.gets.Add(1)
	cur := c.inFlight.Add(1)
	defer c.inFlight.Add(-1)
	for p := c.peak.Load(); cur > p && !c.peak.CompareAndSwap(p, cur); p = c.peak.Load() {
	}
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	if c.failAt != 0 && n == c.failAt {
		return nil, errors.New("injected failure")
	}
	return c.Store.GetRange(ctx, key, offset, length)
}

// counted opens a second Store over s's database and bucket, with cold caches, whose range reads
// are counted.
func counted(t *testing.T, s *Store) (*Store, *countingBlobs) {
	t.Helper()
	c := &countingBlobs{Store: s.blobs}
	return mustOpen(t, s.pool, c, Config{ObjectCacheBytes: 8 << 20, ChunkCacheBytes: 64 << 20, DiffCacheBytes: 8 << 20}), c
}

// textOf is n lines of text, line i reading "<prefix> i".
func textOf(prefix string, n int) []byte {
	var b strings.Builder
	for i := range n {
		fmt.Fprintf(&b, "%s %d\n", prefix, i)
	}
	return []byte(b.String())
}

// frames collects a ReadFiles call's frames, checking their order: ranges in order, each range's
// first frame carrying its file's size and its last setting Last.
func frames(t *testing.T, s *Store, repo int64, ranges []FileRange) map[int][]FileFrame {
	t.Helper()
	got := map[int][]FileFrame{}
	last := -1
	if err := s.ReadFiles(context.Background(), repo, ranges, RawChunks, func(f FileFrame) error {
		if f.Range < last {
			t.Fatalf("frame for range %d after range %d", f.Range, last)
		}
		if f.Range != last && last >= 0 {
			if fs := got[last]; !fs[len(fs)-1].Last {
				t.Fatalf("range %d ended without a last frame", last)
			}
		}
		last = f.Range
		if f.FileID != ranges[f.Range].FileID {
			t.Fatalf("range %d: frame for file %s, want %s", f.Range, f.FileID, ranges[f.Range].FileID)
		}
		got[f.Range] = append(got[f.Range], f)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

// checkWhole checks a range's frames carry exactly want, in order.
func checkWhole(t *testing.T, i int, fs []FileFrame, want []byte) {
	t.Helper()
	if len(fs) == 0 {
		t.Fatalf("range %d: no frames", i)
	}
	if fs[0].Size != uint64(len(want)) {
		t.Fatalf("range %d: size %d, want %d", i, fs[0].Size, len(want))
	}
	var b []byte
	for _, f := range fs {
		if f.Offset != uint64(len(b)) && len(f.Data) > 0 {
			t.Fatalf("range %d: chunk at %d, want %d", i, f.Offset, len(b))
		}
		b = append(b, f.Data...)
	}
	if !bytes.Equal(b, want) {
		t.Fatalf("range %d: %d bytes do not match the file's %d", i, len(b), len(want))
	}
	if !fs[len(fs)-1].Last {
		t.Fatalf("range %d: last frame not marked last", i)
	}
}

// A ReadFiles of many files reads their manifests and locations in a few queries and their
// packs with as few range reads as their layout allows — one for files published together — and
// answers every range in order: whole files, a duplicate, an absent file, the empty file, and a
// byte range of a large one.
func TestReadFilesBatched(t *testing.T) {
	s := testStore(t)
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	var files []encoding.ID
	contents := map[encoding.ID][]byte{}
	for i := range 120 {
		data := bytesOf(1000+(i*331)%40_000, byte(i))
		id := b.file(data)
		files, contents[id] = append(files, id), data
	}
	big := bytesOf(300_000, 200)
	fBig := b.file(big)
	empty := b.file(nil)
	mustPublish(t, s, repo, "alice", b, nil)

	c, blobs := counted(t, s)
	var ranges []FileRange
	for _, id := range files {
		ranges = append(ranges, FileRange{FileID: id})
	}
	ranges = append(ranges,
		FileRange{FileID: files[3]},
		FileRange{FileID: encoding.ID{9}},
		FileRange{FileID: empty},
		FileRange{FileID: fBig},
		FileRange{FileID: fBig, Offset: 70_000, Length: 10},
	)
	got := frames(t, c, repo, ranges)
	for i, id := range files {
		checkWhole(t, i, got[i], contents[id])
	}
	n := len(files)
	checkWhole(t, n, got[n], contents[files[3]])
	if fs := got[n+1]; len(fs) != 1 || !fs[0].NotFound || !fs[0].Last {
		t.Fatalf("absent file: frames %+v", fs)
	}
	if fs := got[n+2]; len(fs) != 1 || fs[0].Size != 0 || !fs[0].Last || len(fs[0].Data) != 0 {
		t.Fatalf("empty file: frames %+v", fs)
	}
	checkWhole(t, n+3, got[n+3], big)
	// The byte range is answered with the whole chunk covering it.
	if fs := got[n+4]; len(fs) != 1 || fs[0].Offset != testChunk || !bytes.Equal(fs[0].Data, big[testChunk:2*testChunk]) || fs[0].Size != uint64(len(big)) {
		t.Fatalf("byte range: %d frames, first at %d", len(fs), fs[0].Offset)
	}
	if g := blobs.gets.Load(); g != 1 {
		t.Fatalf("%d range reads for one pack's worth of files, want 1", g)
	}
	// Read again, everything is in the chunk cache.
	frames(t, c, repo, ranges)
	if g := blobs.gets.Load(); g != 1 {
		t.Fatalf("a second read made %d more range reads", g-1)
	}
}

// Bounded windows and unmerged chunks still answer every range exactly, with the pack reads of a
// window in parallel, at most fetchesAtOnce at a time.
func TestReadFilesWindowsAndParallelReads(t *testing.T) {
	s := testStore(t)
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	var files []encoding.ID
	contents := map[encoding.ID][]byte{}
	chunks := 0
	for i := range 40 {
		data := bytesOf(5000+i*3000, byte(i))
		id := b.file(data)
		files, contents[id] = append(files, id), data
		chunks += (len(data) + testChunk - 1) / testChunk
	}
	mustPublish(t, s, repo, "alice", b, nil)

	c, blobs := counted(t, s)
	c.reads.windowBytes = 64 << 10
	c.reads.windowChunks = 3
	c.reads.mergeGap = 0 // a record's header always separates two chunks' data: nothing merges
	c.reads.fetchesAtOnce = 4
	blobs.delay = 20 * time.Millisecond
	var ranges []FileRange
	for _, id := range files {
		ranges = append(ranges, FileRange{FileID: id})
	}
	got := frames(t, c, repo, ranges)
	for i, id := range files {
		checkWhole(t, i, got[i], contents[id])
	}
	if g := blobs.gets.Load(); g != int64(chunks) {
		t.Fatalf("%d range reads for %d unmerged chunks", g, chunks)
	}
	if p := blobs.peak.Load(); p < 2 || p > 4 {
		t.Fatalf("at most %d range reads in flight at once, want 2–4", p)
	}
}

// A range read that fails fails the call.
func TestReadFilesRangeReadFails(t *testing.T) {
	s := testStore(t)
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	f := b.file(bytesOf(10_000, 1))
	mustPublish(t, s, repo, "alice", b, nil)

	c, blobs := counted(t, s)
	blobs.failAt = 1
	err := c.ReadFiles(context.Background(), repo, []FileRange{{FileID: f}}, RawChunks, func(FileFrame) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "injected failure") {
		t.Fatalf("ReadFiles = %v, want the injected failure", err)
	}
}

// DiffFiles reads every side's manifest with one query and the pairs' contents together, and
// diffs the same whatever its window: one pair at a time, or all at once.
func TestDiffFilesBatched(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	var pairs []FilePair
	for i := range 30 {
		old := textOf(fmt.Sprintf("file %d line", i), 200+i)
		cur := bytes.Replace(old, []byte(fmt.Sprintf("line %d\n", 50+i)), []byte("changed\n"), 1)
		cur = append(cur, []byte("appended\n")...)
		o, n := b.file(old), b.file(cur)
		pairs = append(pairs, FilePair{Old: &o, New: &n})
	}
	added := b.file(textOf("added", 10))
	deleted := b.file(textOf("deleted", 7))
	binary := b.file([]byte("a\x00b\n"))
	text := b.file([]byte("text\n"))
	pairs = append(pairs,
		FilePair{New: &added},
		FilePair{Old: &deleted},
		FilePair{Old: &text, New: &binary},
	)
	mustPublish(t, s, repo, "alice", b, nil)

	c, blobs := counted(t, s)
	want, err := c.DiffFiles(ctx, repo, pairs, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	if g := blobs.gets.Load(); g != 1 {
		t.Fatalf("%d range reads for one pack's worth of pairs, want 1", g)
	}
	for i := range 30 {
		if d := want[i]; d.Additions != 2 || d.Deletions != 1 || len(d.Hunks) != 2 {
			t.Fatalf("pair %d: +%d −%d in %d hunks, want +2 −1 in 2", i, d.Additions, d.Deletions, len(d.Hunks))
		}
	}
	if d := want[30]; d.Additions != 10 || d.Deletions != 0 {
		t.Fatalf("added file: +%d −%d", d.Additions, d.Deletions)
	}
	if d := want[31]; d.Additions != 0 || d.Deletions != 7 {
		t.Fatalf("deleted file: +%d −%d", d.Additions, d.Deletions)
	}
	if !want[32].Binary {
		t.Fatal("a binary side is not marked binary")
	}

	one, _ := counted(t, s)
	one.reads.diffWindowBytes = 1
	got, err := one.DiffFiles(ctx, repo, pairs, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("diffing a pair at a time differs from diffing them together")
	}

	absent := encoding.ID{7}
	if _, err := c.DiffFiles(ctx, repo, []FilePair{{Old: pairs[0].Old, New: &absent}}, 3, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a pair with an absent side = %v, want not found", err)
	}
}

// A pair diffed before is answered from the diff cache: per context, its counts for any context,
// and only once the repository is seen to hold its files. A cached diff does not hold the files
// it came from.
func TestDiffFilesCached(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	old := textOf("line", 300)
	cur := bytes.Replace(old, []byte("line 150\n"), []byte("changed\n"), 1)
	o, n := b.file(old), b.file(cur)
	pair := FilePair{Old: &o, New: &n}
	mustPublish(t, s, repo, "alice", b, nil)

	c, _ := counted(t, s)
	if _, ok := c.diffs.get(diffKey(repo, pair, 3, false)); ok {
		t.Fatal("a cold store has the diff cached")
	}
	full, err := c.DiffFiles(ctx, repo, []FilePair{pair}, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	cached, ok := c.diffs.get(diffKey(repo, pair, 3, false))
	if !ok {
		t.Fatal("the diff was not cached")
	}
	if !reflect.DeepEqual(cached, full[0]) {
		t.Fatal("the cached diff differs from the one computed")
	}
	// The cached lines live in a buffer of their own, not in the files' contents.
	var text []byte
	for _, h := range cached.Hunks {
		for _, l := range h.Lines {
			text = append(text, l.Text...)
		}
	}
	first := cached.Hunks[0].Lines[0].Text
	if cap(first) != len(first) || len(text) == 0 {
		t.Fatal("a cached line can grow into memory beyond it")
	}

	stats, err := c.DiffFiles(ctx, repo, []FilePair{pair}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if d := stats[0]; d.Hunks != nil || d.Additions != full[0].Additions || d.Deletions != full[0].Deletions {
		t.Fatalf("counts alone: %+v, want +%d −%d and no hunks", d, full[0].Additions, full[0].Deletions)
	}
	if _, ok := c.diffs.get(diffKey(repo, pair, 0, false)); ok {
		t.Fatal("a full diff with no context lines was cached without being asked for")
	}
	zero, err := c.DiffFiles(ctx, repo, []FilePair{pair}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(zero[0].Hunks) != 1 || len(zero[0].Hunks[0].Lines) != 2 {
		t.Fatalf("no context lines: %+v", zero[0].Hunks)
	}

	// Counts alone, asked first, are computed without hunks and cached.
	fresh, _ := counted(t, s)
	if got, err := fresh.DiffFiles(ctx, repo, []FilePair{pair}, 3, true); err != nil || got[0].Hunks != nil || got[0].Additions != 1 {
		t.Fatalf("counts alone on a cold store = %+v, %v", got, err)
	}
	if _, ok := fresh.diffs.get(diffKey(repo, pair, 0, true)); !ok {
		t.Fatal("counts alone were not cached")
	}

	// Another repository that does not hold the files is refused, whatever the cache holds.
	other := newRepo(t, s).ID
	if _, err := c.DiffFiles(ctx, other, []FilePair{pair}, 3, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a repository without the files = %v, want not found", err)
	}
}

// Read stored, a chunk arrives as its pack stores it: zstd where that shrank it, raw where it did
// not, with its raw length; decoded, every file is whole. The chunk cache keeps chunks as stored,
// and a raw read is answered from it.
func TestReadFilesStored(t *testing.T) {
	s := testStore(t)
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	text := textOf("compressible line", 9000) // over two chunks
	noise := bytesOf(70_000, 3)               // incompressible: stored raw
	ft, fn := b.file(text), b.file(noise)
	mustPublish(t, s, repo, "alice", b, nil)

	c, blobs := counted(t, s)
	ranges := []FileRange{{FileID: ft}, {FileID: fn}}
	got := map[int][]byte{}
	codecs := map[int][]pack.Codec{}
	wire := 0
	if err := c.ReadFiles(context.Background(), repo, ranges, StoredChunks, func(f FileFrame) error {
		if len(f.Data) == 0 {
			return nil
		}
		raw, err := pack.Decode(nil, f.ChunkID, f.Codec, f.RawLength, f.Data)
		if err != nil {
			return err
		}
		got[f.Range] = append(got[f.Range], raw...)
		codecs[f.Range] = append(codecs[f.Range], f.Codec)
		wire += len(f.Data)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[0], text) || !bytes.Equal(got[1], noise) {
		t.Fatal("a file read stored does not decode to its contents")
	}
	for _, codec := range codecs[0] {
		if codec != pack.Zstd {
			t.Fatalf("a compressible chunk read as codec %d", codec)
		}
	}
	for _, codec := range codecs[1] {
		if codec != pack.Stored {
			t.Fatalf("an incompressible chunk read as codec %d", codec)
		}
	}
	if wire >= len(text)/4+len(noise) {
		t.Fatalf("%d bytes on the wire for %d of text and %d of noise", wire, len(text), len(noise))
	}
	// The cache holds what the packs store, not the raw bytes.
	c.chunks.mu.Lock()
	cached := c.chunks.smallBytes + c.chunks.mainBytes
	c.chunks.mu.Unlock()
	if cached != wire {
		t.Fatalf("the chunk cache holds %d bytes, the chunks are stored as %d", cached, wire)
	}
	gets := blobs.gets.Load()
	whole := frames(t, c, repo, ranges)
	checkWhole(t, 0, whole[0], text)
	checkWhole(t, 1, whole[1], noise)
	if blobs.gets.Load() != gets {
		t.Fatal("a raw read after a stored one went back to the packs")
	}
}

// ReadFiles reports how much it batched: the files each call read, its windows, and the chunks each
// range read of a pack fetched.
func TestReadFilesMetrics(t *testing.T) {
	s := testStore(t)
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	var ranges []FileRange
	for i := range 12 {
		ranges = append(ranges, FileRange{FileID: b.file(bytesOf(3000, byte(i+70)))})
	}
	mustPublish(t, s, repo, "alice", b, nil)
	r := metrics.Default()
	before := r.Read("ok_scm_read_files_ranges", "stored")
	windows := r.Read("ok_scm_read_files_windows", "stored")
	spans := r.Read("ok_scm_pack_range_chunks")
	c, _ := counted(t, s)
	if err := c.ReadFiles(context.Background(), repo, ranges, StoredChunks, func(FileFrame) error { return nil }); err != nil {
		t.Fatal(err)
	}
	after := r.Read("ok_scm_read_files_ranges", "stored")
	if after.Count-before.Count != 1 || after.Sum-before.Sum != 12 {
		t.Fatalf("ranges: %d calls of %v files, want 1 of 12", after.Count-before.Count, after.Sum-before.Sum)
	}
	if w := r.Read("ok_scm_read_files_windows", "stored"); w.Count-windows.Count != 1 || w.Sum-windows.Sum != 1 {
		t.Fatalf("windows: %d calls, %v windows; want 1 and 1", w.Count-windows.Count, w.Sum-windows.Sum)
	}
	got := r.Read("ok_scm_pack_range_chunks")
	if got.Sum-spans.Sum != 12 || got.Count-spans.Count < 1 {
		t.Fatalf("range reads fetched %v chunks in %d reads, want 12", got.Sum-spans.Sum, got.Count-spans.Count)
	}
}
