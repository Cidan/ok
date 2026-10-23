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
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/work"
)

func logRows(t *testing.T, s *Store, repo int64) int {
	t.Helper()
	return dbOf(s).count(t, "content_log", `scope = 0 AND owner_id = @repo`, map[string]any{"repo": repo})
}

// A rebuild folds the log into the next generation's blocks, and what is logged after it applies
// over them.
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
	gen, entries, folded, err := e.s.RebuildContentIndex(ctx, e.repo)
	if err != nil || gen != 2 || entries != len(before) || folded != 0 {
		t.Fatalf("rebuild: generation %d of %d entries (%d before), %v", gen, entries, len(before), err)
	}
	if n := logRows(t, e.s, e.repo); n != 0 {
		t.Fatalf("%d log rows the rebuild folded in remain", n)
	}
	after := indexOf(t, e.s, e.repo).Sorted()
	for i := range before {
		if before[i].ID != after[i].ID || !before[i].Entry.Equal(after[i].Entry) {
			t.Fatalf("entry %s changed in the rebuild: %+v, then %+v", before[i].ID, before[i].Entry, after[i].Entry)
		}
	}
	pinned := b.commit(b.tree(fileEntry("again", b.file([]byte("again\n")))), "more", commit)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		return e.s.pin(ctx, tx, e.repo, []ref{{table: tCommit, id: pinned}})
	}); err != nil {
		t.Fatal(err)
	}
	if logRows(t, e.s, e.repo) == 0 {
		t.Fatal("nothing logged after the rebuild")
	}
	if blocks := dbOf(e.s).count(t, "index_block", `scope = 0 AND owner_id = @repo AND generation <> 2`, map[string]any{"repo": e.repo}); blocks != 0 {
		t.Fatalf("%d blocks of the old generation remain", blocks)
	}
	clean(t, e.s, e.repo)
	if _, _, _, err := reopen(t, e.s).RebuildContentIndex(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	clean(t, e.s, e.repo)
}

// A fold writes its next generation a few blocks a transaction (Config.FoldBatch) before the one
// that makes it the generation, after deleting what a fold that stopped short left, then deletes
// the generation it replaced a few rows a transaction; a fold that finds another running changes
// nothing.
func TestFoldInBatches(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	for i := range 3000 {
		b.file([]byte(fmt.Sprintf("file %d\n", i)))
	}
	mustPublish(t, e.s, e.repo, alice, b, nil)
	before := indexOf(t, e.s, e.repo).Sorted()
	gen, _, ok, err := e.s.ops(ctx).ContentIndexGeneration(ctx, repoScope(e.repo))
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	blocksOf := func(where string) int {
		return dbOf(e.s).count(t, "index_block", `scope = 0 AND owner_id = @repo AND `+where, map[string]any{"repo": e.repo, "gen": gen})
	}

	// Another fold holds the index: this one changes nothing.
	_, release, ok, err := work.Claim(ctx, e.s.Work(), FoldWork, e.repo, "another replica", time.Minute)
	if err != nil || !ok {
		t.Fatalf("claiming the fold: %v, %v", ok, err)
	}
	if g, _, _, err := e.s.RebuildContentIndex(ctx, e.repo); err != nil || g != 0 {
		t.Fatalf("a fold beside another's: generation %d, %v; want nothing done", g, err)
	}
	release()

	// What a fold that stopped short left: a block of the next generation.
	stray := contentindex.Keyed{ID: encoding.ID{0xee}, Entry: before[0].Entry}
	if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		return writeBlocks(ctx, tx, repoScope(e.repo), gen+1, []contentindex.Keyed{stray})
	}); err != nil {
		t.Fatal(err)
	}
	small := mustOpen(t, dbOf(e.s), e.s.blobs, Config{FoldBatch: 2, DeleteBatch: 3, Signals: e.s.signals})
	txns := 0
	small.txAttempts = func() { txns++ }
	folded, entries, _, err := small.RebuildContentIndex(ctx, e.repo)
	if err != nil || folded != gen+1 || entries != len(before) {
		t.Fatalf("the fold: generation %d of %d entries (%d before), %v", folded, entries, len(before), err)
	}
	if want := (len(before)+2*contentindex.BlockEntries-1)/(2*contentindex.BlockEntries) + 2; txns < want {
		t.Errorf("the fold took %d transactions, want at least %d: two blocks to one", txns, want)
	}
	if n := blocksOf(`generation <> @gen + 1`); n != 0 {
		t.Errorf("%d blocks of other generations remain", n)
	}
	if n, want := blocksOf(`generation = @gen + 1`), (len(before)+contentindex.BlockEntries-1)/contentindex.BlockEntries; n != want {
		t.Errorf("%d blocks of the new generation, want %d", n, want)
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
