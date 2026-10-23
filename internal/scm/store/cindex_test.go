package store

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
)

func logRows(t *testing.T, s *Store, repo int64) int {
	t.Helper()
	var n int
	if err := poolOf(s).QueryRow(context.Background(), `SELECT count(*) FROM content_log WHERE repository_id = $1`, repo).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
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
	gen, entries, err := e.s.RebuildContentIndex(ctx, e.repo)
	if err != nil || gen != 2 || entries != len(before) {
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
	if err := e.s.inTx(ctx, func(tx *txn) error { return e.s.pin(ctx, tx, e.repo, []ref{{table: tCommit, id: pinned}}) }); err != nil {
		t.Fatal(err)
	}
	if logRows(t, e.s, e.repo) == 0 {
		t.Fatal("nothing logged after the rebuild")
	}
	var blocks int
	if err := poolOf(e.s).QueryRow(ctx, `SELECT count(*) FROM index_block WHERE repository_id = $1 AND generation <> 2`, e.repo).Scan(&blocks); err != nil || blocks != 0 {
		t.Fatalf("%d blocks of the old generation remain (%v)", blocks, err)
	}
	clean(t, e.s, e.repo)
	if _, _, err := reopen(t, e.s).RebuildContentIndex(ctx, e.repo); err != nil {
		t.Fatal(err)
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
	ce, _ := indexOf(t, e.s, e.repo).Get(chunk)
	wrong := ce.Loc
	wrong.Offset += 5
	if err := e.s.inTx(ctx, func(tx *txn) error {
		e.s.logIndex(tx, e.repo, contentindex.Update{Op: contentindex.Move, ID: chunk, Entry: contentindex.Entry{Kind: encoding.KindChunk, Loc: wrong}})
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
	if gen, _, err := e.s.RebuildContentIndex(ctx, e.repo); err != nil || gen < 2 {
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
	if err := e.s.inTx(ctx, func(tx *txn) error {
		e.s.logIndex(tx, e.repo,
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
