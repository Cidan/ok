package store

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// historySteps are a history's trees, each the last with one file changed by a line added: the
// first makes every file.
func historySteps(n int) []map[string]string {
	keys := []string{"a/x.txt", "a/y.txt", "b/z.txt", "top.txt"}
	files := map[string]string{"a/x.txt": "x\n", "a/y.txt": "y\n", "b/z.txt": "z\n", "top.txt": "t\n"}
	steps := []map[string]string{maps.Clone(files)}
	for i := 1; i < n; i++ {
		k := keys[i%len(keys)]
		files[k] += fmt.Sprintf("line %d\n", i)
		steps = append(steps, maps.Clone(files))
	}
	return steps
}

// landSteps lands each of steps on main in turn, returning main's commits.
func landSteps(t *testing.T, e *clEnv, steps []map[string]string) []encoding.ID {
	t.Helper()
	var mains []encoding.ID
	for i, files := range steps {
		c := e.commit(alice, newChange(), e.head(), fmt.Sprint("step ", i), files)
		e.mustLand(LandItem{Number: e.open(c), Patchset: 1})
		mains = append(mains, e.head())
	}
	return mains
}

// pathAnswers are what a repository's reads of main's path history answer — Log by paths in pages
// both ways, ReadTree's last commits, and Annotate — with each commit named by its place in mains,
// so two repositories of one history answer alike.
func pathAnswers(t *testing.T, s *Store, repo int64, mains []encoding.ID) string {
	t.Helper()
	ctx := context.Background()
	name := map[encoding.ID]string{}
	for i, m := range mains {
		name[m] = fmt.Sprint(i)
	}
	nameOf := func(id encoding.ID) string {
		if n, ok := name[id]; ok {
			return n
		}
		return "?" + id.String()[:8]
	}
	var b strings.Builder
	for _, paths := range [][]string{{"a"}, {"a/x.txt", "b/z.txt"}, {"top.txt"}, {"b"}, {"missing"}} {
		for _, size := range []int{2, 100} {
			fmt.Fprintf(&b, "log %v by %d:", paths, size)
			var older int64
			for range 20 {
				page, err := s.Log(ctx, repo, nil, paths, size, older, 0)
				if err != nil {
					t.Fatal(err)
				}
				for _, c := range page.Commits {
					b.WriteString(" " + nameOf(c.ID))
				}
				b.WriteString(" |")
				if page.Older == 0 {
					break
				}
				older = page.Older
			}
			fmt.Fprintf(&b, "\nnewer than the third %v by %d:", paths, size)
			third, err := s.generation(ctx, repo, mains[2])
			if err != nil {
				t.Fatal(err)
			}
			page, err := s.Log(ctx, repo, nil, paths, size, 0, third)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range page.Commits {
				b.WriteString(" " + nameOf(c.ID))
			}
			b.WriteString("\n")
		}
	}
	for _, at := range []int{0, len(mains) / 2, len(mains) - 1} {
		for _, dir := range []string{"", "a", "b"} {
			_, entries, err := s.ReadTree(ctx, repo, mains[at], dir, true)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&b, "tree %q at %d:", dir, at)
			for _, e := range entries {
				last := "none"
				if e.LastCommit != nil {
					last = nameOf(*e.LastCommit)
				}
				fmt.Fprintf(&b, " %s=%s", e.Name, last)
			}
			b.WriteString("\n")
		}
	}
	for _, p := range []string{"a/x.txt", "top.txt", "b/z.txt"} {
		ranges, err := s.Annotate(ctx, repo, mains[len(mains)-1], p)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "annotate %s:", p)
		for _, r := range ranges {
			fmt.Fprintf(&b, " %d+%d=%s", r.FirstLine, r.LineCount, nameOf(r.Commit))
		}
		b.WriteString("\n")
	}
	return b.String()
}

// compactAll compacts a repository until nothing pinned is left in its tail.
func compactAll(t *testing.T, s *Store, repo int64) {
	t.Helper()
	for {
		r, err := s.CompactSegments(context.Background(), repo, 1)
		if err != nil {
			t.Fatal(err)
		}
		if !r.More {
			return
		}
	}
}

// Main's path history answers the same — Log by paths, ReadTree's last commits, Annotate — wholly in
// the tail, partly moved into segments, wholly moved, and moved with more landed on top, as a
// repository of the same history never compacted, on this replica and on one with nothing cached.
func TestPathHistoryInSegments(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	plain := &clEnv{t: t, s: e.s, repo: newRepo(t, e.s).ID}
	steps := historySteps(12)
	mains := landSteps(t, e, steps[:8])
	plainMains := landSteps(t, plain, steps[:8])
	check := func(stage string, mains, plainMains []encoding.ID) {
		t.Helper()
		want := pathAnswers(t, e.s, plain.repo, plainMains)
		if !strings.Contains(want, "annotate a/x.txt: 1+1=0") {
			t.Fatalf("the uncompacted repository's answers lack what the history says:\n%s", want)
		}
		for name, s := range map[string]*Store{"this replica": e.s, "a replica with nothing cached": reopen(t, e.s)} {
			if got := pathAnswers(t, s, e.repo, mains); got != want {
				t.Fatalf("%s, %s:\n%s\nwant\n%s", stage, name, got, want)
			}
		}
	}
	check("wholly in the tail", mains, plainMains)

	small := mustOpen(t, dbOf(e.s), e.s.blobs, Config{SegmentBatch: 5})
	if r, err := small.CompactSegments(ctx, e.repo, 1); err != nil || r.Paths != 5 || !r.More {
		t.Fatalf("a compaction of 5 path rows: %+v, %v", r, err)
	}
	check("five path rows moved", mains, plainMains)
	compactAll(t, e.s, e.repo)
	if left := dbOf(e.s).count(t, "commit_path", `repository_id = @repo`, map[string]any{"repo": e.repo}); left != 0 {
		t.Fatalf("path rows left in the tail: %d", left)
	}
	check("wholly moved", mains, plainMains)

	mains = append(mains, landSteps(t, e, steps[8:])...)
	plainMains = append(plainMains, landSteps(t, plain, steps[8:])...)
	check("moved, and more landed on top", mains, plainMains)
	compactAll(t, e.s, e.repo)
	check("all moved again", mains, plainMains)
}

// pathIndexCounter counts the landings whose path history a store leaves to write after them.
type pathIndexCounter struct {
	backend.Backend
	marks *atomic.Int32
}

func (c pathIndexCounter) Txn(ctx context.Context, fn func(backend.Ops) error) error {
	return c.Backend.Txn(ctx, func(o backend.Ops) error { return fn(pathIndexOps{o, c.marks}) })
}

type pathIndexOps struct {
	backend.Ops
	marks *atomic.Int32
}

func (o pathIndexOps) InsertPathIndex(ctx context.Context, repo int64, p backend.PathIndex) error {
	o.marks.Add(1)
	return o.Ops.InsertPathIndex(ctx, repo, p)
}

// pendLandings makes main's path history of the landings mains[from:] yet to be written, as if each
// landed and its replica died before writing it: their rows deleted, and their marks recorded.
func pendLandings(t *testing.T, e *clEnv, mains []encoding.ID, from int) {
	t.Helper()
	ctx := context.Background()
	for i := from; i < len(mains); i++ {
		dbOf(e.s).exec(t, `DELETE FROM commit_path WHERE repository_id = @repo AND commit_id = @commit`,
			map[string]any{"repo": e.repo, "commit": mains[i][:]})
		gen, err := e.s.generation(ctx, e.repo, mains[i])
		if err != nil {
			t.Fatal(err)
		}
		if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			return tx.InsertPathIndex(ctx, e.repo, backend.PathIndex{Generation: gen, Commit: mains[i], Parent: mains[i-1]})
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// A landing that changed more paths than Config.LandingPaths leaves them to compaction, and main's
// path history answers the same — Log by paths, ReadTree's last commits, Annotate — as a repository
// whose landings wrote theirs: yet to be written, read from the trees; written by compaction and
// moved; and yet to be written beside what segments hold.
func TestPathHistoryAfterLanding(t *testing.T) {
	e := newCLEnv(t)
	plain := &clEnv{t: t, s: e.s, repo: newRepo(t, e.s).ID}
	var marks atomic.Int32
	small, err := New(pathIndexCounter{dbOf(e.s).open(t), &marks}, e.s.blobs, Config{Names: testNames, LandingPaths: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(small.Close)
	testDBs.Store(small, dbOf(e.s))
	d := &clEnv{t: t, s: small, repo: e.repo}
	steps := historySteps(12)
	mains := landSteps(t, d, steps[:8])
	plainMains := landSteps(t, plain, steps[:8])
	// Each step but the two that change only top.txt changes a file and its directory, two paths.
	if marks.Load() != 6 {
		t.Fatalf("%d of 8 landings left their paths for after their transactions, want 6", marks.Load())
	}
	pending := func() int {
		return dbOf(e.s).count(t, "path_index", `repository_id = @repo`, map[string]any{"repo": e.repo})
	}
	if n := pending(); n != 6 {
		t.Fatalf("%d landings' paths are yet to be written, want 6", n)
	}
	check := func(stage string) {
		t.Helper()
		want := pathAnswers(t, e.s, plain.repo, plainMains)
		for name, s := range map[string]*Store{"this replica": e.s, "a replica with nothing cached": reopen(t, e.s)} {
			if got := pathAnswers(t, s, e.repo, mains); got != want {
				t.Fatalf("%s, %s:\n%s\nwant\n%s", stage, name, got, want)
			}
		}
	}
	check("yet to be written")
	compactAll(t, e.s, e.repo)
	if n := pending(); n != 0 {
		t.Fatalf("after compaction %d landings' paths are unwritten", n)
	}
	check("written by compaction, and moved")

	mains = append(mains, landSteps(t, e, steps[8:])...)
	plainMains = append(plainMains, landSteps(t, plain, steps[8:])...)
	pendLandings(t, e, mains, 8)
	check("yet to be written, over segments")
	compactAll(t, e.s, e.repo)
	if n := pending(); n != 0 {
		t.Fatalf("after compaction %d landings' paths are unwritten", n)
	}
	check("all written and moved")
}

// segmentsCounter counts reads of a repository's manifest.
type segmentsCounter struct {
	backend.Backend
	reads atomic.Int32
}

func (c *segmentsCounter) Segments(ctx context.Context, sc backend.Scope, kind segment.Kind) ([]backend.Segment, error) {
	c.reads.Add(1)
	return c.Backend.Segments(ctx, sc, kind)
}

// readAllEvents reads a repository's events from after on, a page of limit at a time, as places.
func readAllEvents(t *testing.T, s *Store, repo, after int64, limit int) []int64 {
	t.Helper()
	var seqs []int64
	for {
		evs, err := s.ReadEvents(context.Background(), repo, after, limit)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) == 0 {
			return seqs
		}
		for _, ev := range evs {
			if ev.GetRepositoryId() != repo || ev.GetKind() == nil {
				t.Fatalf("an event read is %v", ev)
			}
			seqs = append(seqs, ev.GetRepositorySeq())
			after = ev.GetRepositorySeq()
		}
	}
}

// A repository's log reads the same from wherever its events are: published and moved into
// segments, still pending in the tail, and an earlier one pending beside a later one moved.
func TestEventsInSegments(t *testing.T) {
	ctx := context.Background()
	defer func(was time.Duration) { eventsSettle = was }(eventsSettle)
	eventsSettle = 0 // every published event settled
	e := newCLEnv(t)
	landSteps(t, e, historySteps(3))
	st := &stream{}
	drain(t, e.s, st)
	all := readAllEvents(t, e.s, e.repo, 0, eventPage)
	if len(all) < 5 {
		t.Fatalf("the history wrote %d events", len(all))
	}
	contiguous := func(stage string, got []int64, from, n int) {
		t.Helper()
		if len(got) != n {
			t.Fatalf("%s: %d events, want %d: %v", stage, len(got), n, got)
		}
		for i, seq := range got {
			if seq != int64(from+i) {
				t.Fatalf("%s: events %v, want %d on from %d", stage, got, n, from)
			}
		}
	}
	contiguous("in the tail", all, 1, len(all))
	r, err := e.s.CompactSegments(ctx, e.repo, 1)
	if err != nil || r.Events != len(all) {
		t.Fatalf("compacting the published events: %+v, %v", r, err)
	}
	for _, s := range []*Store{e.s, reopen(t, e.s)} {
		contiguous("moved", readAllEvents(t, s, e.repo, 0, eventPage), 1, len(all))
		contiguous("moved, three at a time", readAllEvents(t, s, e.repo, 0, 3), 1, len(all))
		contiguous("moved, from the middle", readAllEvents(t, s, e.repo, 2, 2), 3, len(all)-2)
	}

	// More, pending: the log reads on from the segments into the tail.
	for i := range 3 {
		if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			return appendEvent(ctx, tx, e.repo, &scmpb.Event{Kind: &scmpb.Event_RepositoryCreated{
				RepositoryCreated: &scmpb.RepositoryCreated{CreatedById: fmt.Sprint("prn_", i)}}})
		}); err != nil {
			t.Fatal(err)
		}
	}
	n := len(all) + 3
	contiguous("moved, then pending", readAllEvents(t, reopen(t, e.s), e.repo, 0, 4), 1, n)
	if r, err := e.s.CompactSegments(ctx, e.repo, 1); err != nil || r.Events != 0 {
		t.Fatalf("compacting pending events: %+v, %v", r, err)
	}

	// The last published before the one before it — another relay's — and moved: the one before
	// still pending in the tail, the last in a segment.
	dbOf(e.s).exec(t, `DELETE FROM event_pending WHERE repository_id = @repo AND repository_seq = @seq`, map[string]any{"repo": e.repo, "seq": int64(n)})
	if r, err := e.s.CompactSegments(ctx, e.repo, 1); err != nil || r.Events != 1 {
		t.Fatalf("compacting the last event alone: %+v, %v", r, err)
	}
	contiguous("the one before pending, the last moved", readAllEvents(t, reopen(t, e.s), e.repo, int64(n-3), 10), n-2, 3)

	// A page the tail holds without a gap reads no manifest.
	counter := &segmentsCounter{Backend: dbOf(e.s).open(t)}
	counted, err := New(counter, e.s.blobs, Config{Names: testNames})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(counted.Close)
	if evs, err := counted.ReadEvents(ctx, e.repo, int64(n-3), 1); err != nil || len(evs) != 1 || counter.reads.Load() != 0 {
		t.Errorf("a page of one pending event: %d events, %v, %d manifest reads", len(evs), err, counter.reads.Load())
	}
	if evs, err := counted.ReadEvents(ctx, e.repo, 0, 2); err != nil || len(evs) != 2 || counter.reads.Load() == 0 {
		t.Errorf("a page of moved events: %d events, %v, %d manifest reads", len(evs), err, counter.reads.Load())
	}
}
