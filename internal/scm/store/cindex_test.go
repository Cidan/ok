package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/work"
)

func logRows(t *testing.T, s *Store, repo int64) int {
	t.Helper()
	return dbOf(s).count(t, "content_log", `scope = 0 AND owner_id = @repo`, map[string]any{"repo": repo})
}

// runsOf are a content index's live runs of a kind.
func runsOf(t *testing.T, s *Store, sc backend.Scope, kind segment.Kind) []backend.Segment {
	t.Helper()
	segs, err := s.b.Segments(context.Background(), sc, kind)
	if err != nil {
		t.Fatal(err)
	}
	return segs
}

// A fold writes what the log changed into runs — unpinned entries into the unpinned index, pinned
// ones into the pinned runs forks take — every entry whole, and deletes the rows it folded; what
// is logged after it applies over the runs, and a replica's memory of the index after a fold is the
// log since alone.
func TestContentIndexRebuild(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	big := bytesOf(200_000, 3)
	commit := b.commit(b.tree(fileEntry("big", b.file(big)), fileEntry("small", b.file([]byte("small\n")))), "files", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, &OpHeadsUpdate{New: b.operation(b.view(commit), "snapshot", encoding.RootOperationID)})
	if logRows(t, e.s, e.repo) == 0 {
		t.Fatal("a publish logged nothing")
	}
	before := indexOf(t, e.s, e.repo).Sorted()
	gen, unpinned, pinned, err := e.s.RebuildContentIndex(ctx, e.repo)
	if err != nil || gen != 2 || unpinned != len(before) || pinned != 0 {
		t.Fatalf("rebuild: generation %d, %d unpinned and %d pinned records (%d entries before), %v", gen, unpinned, pinned, len(before), err)
	}
	if n := logRows(t, e.s, e.repo); n != 0 {
		t.Fatalf("%d log rows the rebuild folded in remain", n)
	}
	runs := runsOf(t, e.s, repoScope(e.repo), segIndexUnpinned)
	if len(runs) != 1 || runs[0].Records != int64(len(before)) || runs[0].Day == 0 {
		t.Fatalf("the unpinned index's runs: %+v", runs)
	}
	li := e.s.live(repoScope(e.repo))
	if err := e.s.refreshIndex(ctx, repoScope(e.repo), li); err != nil {
		t.Fatal(err)
	}
	if n := li.view.Load().log.Len(); n != 0 {
		t.Fatalf("after the fold, this replica's log of the index holds %d entries", n)
	}
	after := indexOf(t, reopen(t, e.s), e.repo).Sorted()
	if len(after) != len(before) {
		t.Fatalf("%d entries after the fold, %d before", len(after), len(before))
	}
	for i := range before {
		if before[i].ID != after[i].ID || !before[i].Entry.Equal(after[i].Entry) {
			t.Fatalf("entry %s changed in the rebuild: %+v, then %+v", before[i].ID, before[i].Entry, after[i].Entry)
		}
	}
	pinnedCommit := b.commit(b.tree(fileEntry("again", b.file([]byte("again\n")))), "more", commit)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		return e.s.pin(ctx, tx, e.repo, []ref{{table: tCommit, id: pinnedCommit}})
	}); err != nil {
		t.Fatal(err)
	}
	if logRows(t, e.s, e.repo) == 0 {
		t.Fatal("nothing logged after the rebuild")
	}
	clean(t, e.s, e.repo)
	if g, _, pinned, err := reopen(t, e.s).RebuildContentIndex(ctx, e.repo); err != nil || g != 3 || pinned == 0 {
		t.Fatalf("the second rebuild: generation %d, %d pinned records, %v", g, pinned, err)
	}
	if runs := runsOf(t, e.s, repoScope(e.repo), segIndex); len(runs) == 0 {
		t.Fatal("no pinned run after a fold of pinned entries")
	}
	clean(t, e.s, e.repo)
}

// A fold that finds another running changes nothing; one with nothing to fold, too. A fold of many
// entries writes them all to one run, read back whole by a replica with nothing cached.
func TestFoldWritesARun(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	for i := range 3000 {
		b.file([]byte(fmt.Sprintf("file %d\n", i)))
	}
	mustPublish(t, e.s, e.repo, alice, b, nil)
	before := indexOf(t, e.s, e.repo).Sorted()

	// Another fold holds the index: this one changes nothing.
	_, release, ok, err := work.Claim(ctx, e.s.Work(), FoldWork, e.repo, "another replica", time.Minute)
	if err != nil || !ok {
		t.Fatalf("claiming the fold: %v, %v", ok, err)
	}
	if g, _, _, err := e.s.RebuildContentIndex(ctx, e.repo); err != nil || g != 0 {
		t.Fatalf("a fold beside another's: generation %d, %v; want nothing done", g, err)
	}
	release()

	g, unpinned, _, err := e.s.RebuildContentIndex(ctx, e.repo)
	if err != nil || g != 2 || unpinned != len(before) {
		t.Fatalf("the fold: generation %d, %d unpinned records (%d entries before), %v", g, unpinned, len(before), err)
	}
	if g, _, _, err := e.s.RebuildContentIndex(ctx, e.repo); err != nil || g != 0 {
		t.Fatalf("a fold with nothing to fold: generation %d, %v", g, err)
	}
	after := indexOf(t, reopen(t, e.s), e.repo).Sorted()
	if len(after) != len(before) {
		t.Fatalf("%d entries after the fold, %d before", len(after), len(before))
	}
	for i := range before {
		if before[i].ID != after[i].ID || !before[i].Entry.Equal(after[i].Entry) {
			t.Fatalf("entry %s changed in the fold: %+v, then %+v", before[i].ID, before[i].Entry, after[i].Entry)
		}
	}
	clean(t, e.s, e.repo)
}

// fsck finds a chunk the index has where its pack does not.
func TestFsckFindsAWrongContentIndex(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	contents := []byte("contents\n")
	b.commit(b.tree(fileEntry("f", b.file(contents))), "f", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	clean(t, e.s, e.repo)
	chunk := pack.ChunkID(contents)
	ce, _ := locationsOf(t, e.s, e.repo).Get(chunk)
	wrong := ce.Loc
	wrong.Offset += 5
	if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		e.s.logIndex(tx, backend.NetworkScope(netOf(t, e.s, e.repo)), contentindex.Update{Op: contentindex.Move, ID: chunk, Entry: contentindex.Entry{Kind: encoding.KindChunk, Loc: wrong}})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r, err := e.s.Fsck(ctx, e.repo, false)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(r.Problems, "\n"), "disagrees with pack") {
		t.Fatalf("fsck missed a chunk the index has in the wrong place: %s", r)
	}
}

// Another replica reads what one just published — its index refreshing on the miss, or on the
// announcement — and keeps reading after the first rebuilds the index into a new generation.
func TestAnotherReplicaReadsThroughItsIndex(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	other := reopen(t, e.s)
	b := newBuilder(t)
	first := bytesOf(70_000, 1)
	f1 := b.file(first)
	c1 := b.commit(b.tree(fileEntry("one", f1)), "one", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, &OpHeadsUpdate{New: b.operation(b.view(c1), "one", encoding.RootOperationID)})
	if !bytes.Equal(readAll(t, other, e.repo, f1), first) {
		t.Fatal("the other replica read the first file wrong")
	}
	// Loaded now: what is published next is found by a refresh.
	second := bytesOf(5_000, 2)
	f2 := b.file(second)
	b.commit(b.tree(fileEntry("two", f2)), "two", c1)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	if !bytes.Equal(readAll(t, other, e.repo, f2), second) {
		t.Fatal("the other replica read the second file wrong")
	}
	// A rebuild moves the index to a new generation, folding the log in.
	if gen, _, _, err := e.s.RebuildContentIndex(ctx, e.repo); err != nil || gen < 2 {
		t.Fatalf("rebuild: %d, %v", gen, err)
	}
	third := bytesOf(3_000, 3)
	f3 := b.file(third)
	b.commit(b.tree(fileEntry("three", f3)), "three", c1)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	for f, want := range map[encoding.ID][]byte{f1: first, f2: second, f3: third} {
		if !bytes.Equal(readAll(t, other, e.repo, f), want) {
			t.Fatalf("after the rebuild, the other replica read %s wrong", f)
		}
	}
	// An absent file stays absent after a refresh.
	if _, err := other.readFile(ctx, e.repo, encoding.ID{9}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an absent file: %v", err)
	}
	clean(t, other, e.repo)
}

// A sweep keeps what the index says is fresh, though its row is stale: a transaction may have
// touched it in the index alone.
func TestSweepKeepsWhatTheIndexSaysIsFresh(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	contents := []byte("referenced in the index alone\n")
	f := b.file(contents)
	b.commit(b.tree(fileEntry("f", f)), "unreachable", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	age(t, e.s, e.repo, 20)
	chunk := pack.ChunkID(contents)
	today := contentindex.DayOf(time.Now())
	if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		e.s.logIndex(tx, repoScope(e.repo),
			contentindex.Update{Op: contentindex.Touch, ID: f, Entry: contentindex.Entry{Kind: encoding.KindFile, Day: today}},
			contentindex.Update{Op: contentindex.Touch, ID: chunk, Entry: contentindex.Entry{Kind: encoding.KindChunk, Day: today}})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.FilesSwept != 0 || r.ChunksSwept != 0 || !has(t, e.s, e.repo, "file", f) || !has(t, e.s, e.repo, "chunk", chunk) {
		t.Fatalf("swept what the index says is fresh: %s", r)
	}
	if !bytes.Equal(readAll(t, e.s, e.repo, f), contents) {
		t.Fatal("the file reads back wrong")
	}
}

// Writers beyond the pool's connections, each reading a repository's content index for the first
// time, all finish: a writer refreshes the index through its own transaction, and never waits for
// a second connection while it holds one — which, with every connection held so, would wait
// forever.
func TestWritersBeyondThePool(t *testing.T) {
	s := testStoreOf(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	repos := make([]int64, 12)
	for i := range repos {
		repos[i] = newRepo(t, s).ID
	}
	// Cold indexes, as on a replica that has never read these repositories.
	fresh := reopen(t, s)
	var wg sync.WaitGroup
	for i, repo := range repos {
		wg.Go(func() {
			b := newBuilder(t)
			b.change(newChange(), encoding.RootCommitID, "c", map[string]string{"f": fmt.Sprint(i)})
			if _, err := publish(ctx, fresh, repo, alice, b, nil); err != nil {
				t.Errorf("publishing to repository %d: %v", repo, err)
			}
		})
	}
	wg.Wait()
}
