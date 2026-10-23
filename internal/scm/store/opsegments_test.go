package store

import (
	"context"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// settleOps ages every operation and view of a repository's logs past collection's window, as if
// published long ago.
func settleOps(t *testing.T, s *Store, repo int64) {
	t.Helper()
	dbOf(s).ageOps(t, repo, 30)
}

// tailCount counts a repository's rows of a table joined to its operation logs.
func tailCount(t *testing.T, s *Store, repo int64, table string) int {
	t.Helper()
	return dbOf(s).opRows(t, repo, table)
}

// segmentHas reports whether any of a repository's live op segments holds a record of key.
func segmentHas(t *testing.T, s *Store, repo int64, key []byte) bool {
	t.Helper()
	readers, err := s.segmentReaders(context.Background(), repoScope(repo), segOps, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range readers {
		if _, ok, err := r.Get(context.Background(), key); err != nil {
			t.Fatal(err)
		} else if ok {
			return true
		}
	}
	return false
}

// An operation log moved into segments reads as before — operations, views, a prefix resolved,
// prefetched from the heads — and collection, finding an abandoned operation there, records
// tombstones of it and its view, which hide it, move into a segment themselves, and at the oldest
// level are merged away with what they bury.
func TestOpsInSegments(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	root := encoding.RootOperationID
	a, va := opViewing(t, s, repo, "alice", "a", &OpHeadsUpdate{Old: []encoding.ID{root}}, root)
	b, vb := opViewing(t, s, repo, "alice", "b", &OpHeadsUpdate{Old: []encoding.ID{a}}, a)
	x, vx := opViewing(t, s, repo, "alice", "x", &OpHeadsUpdate{Old: []encoding.ID{b}}, b)
	y, vy := opViewing(t, s, repo, "alice", "y", &OpHeadsUpdate{Old: []encoding.ID{x}}, b) // x abandoned
	settleOps(t, s, repo)
	r, err := s.CompactSegments(ctx, repo, 1)
	if err != nil || r.Ops != 8 {
		t.Fatalf("compacting four operations and their views: %+v, %v", r, err)
	}
	if n := tailCount(t, s, repo, "operation") + tailCount(t, s, repo, "view"); n != 0 {
		t.Fatalf("%d operations and views left in the tail", n)
	}

	fresh := reopen(t, s)
	for name, id := range map[string]encoding.ID{"a": a, "b": b, "x": x, "y": y} {
		if !opExists(t, fresh, repo, "alice", id) {
			t.Fatalf("operation %s, moved, is not read", name)
		}
	}
	res, err := fresh.ReadOperations(ctx, repo, "alice", testWorkspace, []ObjectRef{{encoding.KindView, va}, {encoding.KindView, vy}})
	if err != nil || !res[0].Found || !res[1].Found {
		t.Fatalf("moved views: %+v, %v", res, err)
	}
	if got, id, err := fresh.ResolveOperationPrefix(ctx, repo, "alice", testWorkspace, b[:4], false); err != nil || got != SingleMatch || id != b {
		t.Fatalf("resolving a moved operation's prefix: %v %s %v", got, id, err)
	}
	if got := prefetchOps(t, fresh, repo, "alice", nil, nil, 100); len(got) != 6 {
		t.Fatalf("prefetching from the heads gave %d objects, want y, b, a and their views", len(got))
	}

	// Collection, dry, counts the abandoned operation and its view, which only it names; then
	// records their tombstones, which hide them, and keeps the rest.
	log, err := logOf(ctx, s.ops(ctx), repo, "alice", testWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	if c, err := s.collectLog(ctx, repo, log, nil, time.Now(), true); err != nil || c.Operations != 1 || c.Views != 1 {
		t.Fatalf("a dry collection: %+v, %v", c, err)
	}
	if !opExists(t, reopen(t, s), repo, "alice", x) {
		t.Fatal("a dry collection took the abandoned operation")
	}
	if c, err := s.collectLog(ctx, repo, log, nil, time.Now(), false); err != nil || c.Operations != 1 || c.Views != 1 {
		t.Fatalf("collecting: %+v, %v", c, err)
	}
	if n := tailCount(t, s, repo, "op_tombstone"); n != 2 {
		t.Fatalf("%d tombstones, want the abandoned operation's and its view's", n)
	}
	check := func(stage string, want int) {
		t.Helper()
		fresh := reopen(t, s)
		if opExists(t, fresh, repo, "alice", x) {
			t.Fatalf("%s: the collected operation is read", stage)
		}
		for name, id := range map[string]encoding.ID{"a": a, "b": b, "y": y} {
			if !opExists(t, fresh, repo, "alice", id) {
				t.Fatalf("%s: operation %s is gone", stage, name)
			}
		}
		res, err := fresh.ReadOperations(ctx, repo, "alice", testWorkspace, []ObjectRef{{encoding.KindView, vx}, {encoding.KindView, vb}})
		if err != nil || res[0].Found || !res[1].Found {
			t.Fatalf("%s: the collected view found %v, a kept view found %v (%v)", stage, res[0].Found, res[1].Found, err)
		}
		ops, err := fresh.repositoryOps(ctx, repo, encoding.KindOperation)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range ops {
			if o.ID == x {
				t.Fatalf("%s: the repository's operations, GC's roots, hold the collected one", stage)
			}
		}
		if len(ops) != want {
			t.Fatalf("%s: the repository holds %d operations, want %d", stage, len(ops), want)
		}
	}
	check("tombstones in the tail", 3)
	if r, err := s.CompactSegments(ctx, repo, 1); err != nil || r.Ops != 2 {
		t.Fatalf("compacting the tombstones: %+v, %v", r, err)
	}
	if n := tailCount(t, s, repo, "op_tombstone"); n != 0 {
		t.Fatalf("%d tombstones left in the tail", n)
	}
	check("tombstones in a segment", 3)

	// Re-publishing an operation only a segment holds puts its row in the tail again.
	if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		return s.touchOps(ctx, tx, repo, log, encoding.KindOperation, []encoding.ID{a})
	}); err != nil {
		t.Fatal(err)
	}
	if n := tailCount(t, s, repo, "operation"); n != 1 {
		t.Fatalf("touching a moved operation left %d rows in the tail, want its own", n)
	}

	// Two more segments: four at level 0, the oldest level, merged — the tombstones and what they
	// bury gone from the merged segment.
	for i := range 2 {
		head := heads(t, s, repo, "alice")
		op(t, s, repo, "alice", "more", &OpHeadsUpdate{Old: head}, head...)
		settleOps(t, s, repo)
		if r, err := s.CompactSegments(ctx, repo, 1); err != nil || r.Ops == 0 {
			t.Fatalf("compaction %d: %+v, %v", i, r, err)
		}
	}
	segs, err := s.ops(ctx).Segments(ctx, repoScope(repo), segOps)
	if err != nil || len(segs) != 1 || segs[0].Level != 1 {
		t.Fatalf("op segments after four compactions: %+v, %v; want them merged into one at level 1", segs, err)
	}
	for name, key := range map[string][]byte{"x": opKey(log, encoding.KindOperation, x), "its view": opKey(log, encoding.KindView, vx)} {
		if segmentHas(t, s, repo, key) {
			t.Errorf("the merged segment still holds the collected %s's record", name)
		}
	}
	if !segmentHas(t, s, repo, opKey(log, encoding.KindOperation, b)) {
		t.Fatal("the merged segment lost a kept operation")
	}
	check("merged away", 5)
	if fr, err := reopen(t, s).Fsck(ctx, repo, false); err != nil || len(fr.Problems) != 0 {
		t.Fatalf("fsck: %v, %v", fr.Problems, err)
	}
}

// A merge of op segments drops the records of operation logs since deleted, and at the oldest
// level the tombstones; elsewhere it keeps them.
func TestOpMergeFilter(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	op(t, s, repo, "alice", "a", &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}}, encoding.RootOperationID)
	log, err := logOf(ctx, s.ops(ctx), repo, "alice", testWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	live := opKey(log, encoding.KindOperation, encoding.ID{1})
	gone := opKey(log+1000, encoding.KindOperation, encoding.ID{1})
	tomb := []byte{opTombstone}
	for _, oldest := range []bool{false, true} {
		keep, err := s.opMergeFilter(ctx, repo, oldest)
		if err != nil {
			t.Fatal(err)
		}
		if !keep(live, []byte{opLive, 0, 0, 0, 0, 0, 0, 0, 0}) || keep(gone, []byte{opLive}) || keep(live, tomb) != !oldest {
			t.Errorf("merging into the oldest level %v: a live record kept %v, a deleted log's %v, a tombstone %v",
				oldest, keep(live, []byte{opLive}), keep(gone, []byte{opLive}), keep(live, tomb))
		}
	}
}
