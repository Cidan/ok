package store

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// fullOpReads counts reads of every operation and view the tail holds: what a pass reads only
// before a repository's settled operations' content is pinned (settle.go).
type fullOpReads struct {
	backend.Backend
	n *atomic.Int32
}

func (f fullOpReads) RepositoryOperations(ctx context.Context, repo int64) ([]backend.OpRecord, error) {
	f.n.Add(1)
	return f.Backend.RepositoryOperations(ctx, repo)
}

func (f fullOpReads) RepositoryViews(ctx context.Context, repo int64) ([]backend.OpRecord, error) {
	f.n.Add(1)
	return f.Backend.RepositoryViews(ctx, repo)
}

// countingFullOpReads is a replica of s's database and bucket whose reads of every tail operation
// and view n counts.
func countingFullOpReads(t *testing.T, s *Store) (*Store, *atomic.Int32) {
	t.Helper()
	n := &atomic.Int32{}
	c, err := New(fullOpReads{dbOf(s).open(t), n}, s.blobs, Config{Names: testNames, Signals: s.signals})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, n
}

// What a settled operation reaches is pinned as compaction moves it into a segment, and kept for
// good: a pass long after sweeps none of it, and reads the tail's operations alone.
func TestSettledOperationsPinWhatTheyReach(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	work := b.commit(b.tree(fileEntry("f", b.file([]byte("work the log keeps")))), "work", encoding.RootCommitID)
	op := b.operation(b.view(work), "snapshot", encoding.RootOperationID)
	mustPublish(t, e.s, e.repo, alice, b, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op})
	age(t, e.s, e.repo, 20)
	if _, err := e.s.CompactSegments(ctx, e.repo, 1); err != nil {
		t.Fatal(err)
	}
	db := dbOf(e.s)
	if left := db.count(t, "operation", "TRUE", nil); left != 0 {
		t.Fatalf("%d settled operations left in the tail", left)
	}
	if unpinned := db.count(t, "commit", "NOT pinned", nil); unpinned != 0 {
		t.Fatalf("%d commits the settled operation reaches are not pinned", unpinned)
	}
	other, full := countingFullOpReads(t, e.s)
	age(t, e.s, e.repo, 40)
	r, err := other.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.CommitsSwept != 0 || r.ObjectsSwept != 0 || !has(t, e.s, e.repo, "commit", work) {
		t.Fatalf("a pass swept what a settled operation reaches: %s", r)
	}
	if full.Load() != 0 {
		t.Errorf("the pass read every operation %d times, not the tail's a page at a time", full.Load())
	}
	clean(t, e.s, e.repo)
}

// A repository whose settled operations moved into segments before compaction pinned what they
// reach has it pinned by its next pass, once — which reads them all that once — and is not swept.
func TestGCPinsWhatEarlierSettledOperationsReach(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	e.s.settleUnpinned = true
	b := newBuilder(t)
	work := b.commit(b.tree(fileEntry("f", b.file([]byte("work from before")))), "work", encoding.RootCommitID)
	op := b.operation(b.view(work), "snapshot", encoding.RootOperationID)
	mustPublish(t, e.s, e.repo, alice, b, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op})
	age(t, e.s, e.repo, 20)
	if _, err := e.s.CompactSegments(ctx, e.repo, 1); err != nil {
		t.Fatal(err)
	}
	e.s.settleUnpinned = false
	db := dbOf(e.s)
	db.unpinOpContent(t, e.repo)
	if unpinned := db.count(t, "commit", "NOT pinned", nil); unpinned != 1 {
		t.Fatalf("the operation's commit moved pinned (%d unpinned): the test makes nothing of before", unpinned)
	}
	other, full := countingFullOpReads(t, e.s)
	r, err := other.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.CommitsSwept != 0 || !has(t, e.s, e.repo, "commit", work) {
		t.Fatalf("the pass swept what an earlier settled operation reaches: %s", r)
	}
	if done, err := e.s.ops(ctx).OpContentPinned(ctx, e.repo); err != nil || !done {
		t.Fatalf("the repository's settled content is not recorded pinned: %v, %v", done, err)
	}
	full.Store(0)
	if _, err := other.GC(ctx, e.repo, time.Now(), GCOptions{}); err != nil {
		t.Fatal(err)
	}
	if full.Load() != 0 {
		t.Errorf("a pass after the one that pinned it read every operation %d times", full.Load())
	}
	clean(t, e.s, e.repo)
}

// Only a log whose heads moved other than forward is collected — again each pass while what that
// left unreached is within the window, and not after one that took all of it — and a log that
// only ever moved forward never is.
func TestCollectsOnlyLogsThatDiverged(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	// alice's log moves forward, a new operation on the last.
	a := newBuilder(t)
	a1 := a.operation(a.view(a.commit(a.tree(fileEntry("a", a.file([]byte("one")))), "one", encoding.RootCommitID)), "one", encoding.RootOperationID)
	mustPublish(t, e.s, e.repo, alice, a, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: a1})
	a2b := newBuilder(t)
	a2 := a2b.operation(a2b.view(a2b.commit(a2b.tree(fileEntry("a", a2b.file([]byte("two")))), "two", encoding.RootCommitID)), "two", a1)
	mustPublish(t, e.s, e.repo, alice, a2b, &OpHeadsUpdate{Old: []encoding.ID{a1}, New: a2})
	// bob's diverges: jj op abandon, a head that does not descend from the last.
	bb := newBuilder(t)
	abandoned := bb.commit(bb.tree(fileEntry("b", bb.file([]byte("abandoned")))), "abandoned", encoding.RootCommitID)
	b1 := bb.operation(bb.view(abandoned), "one", encoding.RootOperationID)
	mustPublish(t, e.s, e.repo, bob, bb, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: b1})
	rb := newBuilder(t)
	b2 := rb.operation(rb.view(rb.commit(rb.tree(fileEntry("b", rb.file([]byte("rewritten")))), "rewritten", encoding.RootCommitID)), "rewritten", encoding.RootOperationID)
	mustPublish(t, e.s, e.repo, bob, rb, &OpHeadsUpdate{Old: []encoding.ID{b1}, New: b2})

	aliceLog, err := e.s.OpLog(ctx, e.repo, alice, testWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	bobLog, err := e.s.OpLog(ctx, e.repo, bob, testWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	toCollect := func() []int64 {
		t.Helper()
		logs, err := e.s.ops(ctx).OpLogsToCollect(ctx, e.repo)
		if err != nil {
			t.Fatal(err)
		}
		var out []int64
		for _, l := range logs {
			out = append(out, l.OpLog)
		}
		return out
	}
	if got := toCollect(); !slices.Equal(got, []int64{bobLog}) {
		t.Fatalf("logs to collect %v, want bob's (%d) alone, not alice's (%d)", got, bobLog, aliceLog)
	}
	age(t, e.s, e.repo, 20)
	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.OperationsDeleted != 1 || has(t, e.s, e.repo, "commit", abandoned) {
		t.Fatalf("collecting bob's log: %s", r)
	}
	// It diverged within the window: what it left unreached may be younger, so it is collected
	// again — until a pass past the window.
	if got := toCollect(); !slices.Equal(got, []int64{bobLog}) {
		t.Fatalf("logs to collect after a pass within the window: %v", got)
	}
	if _, err := e.s.GC(ctx, e.repo, time.Now().Add(OperationRetention+time.Hour), GCOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := toCollect(); len(got) != 0 {
		t.Fatalf("logs to collect after a pass past the window: %v", got)
	}
	clean(t, e.s, e.repo)
}

// A checkpoint kept past its first week has its trees pinned by the next pass, and kept for good;
// the mark's roots are only the trees of the checkpoints not pinned.
func TestKeptCheckpointsArePinned(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	old := b.tree(fileEntry("old", b.file([]byte("kept past its week"))))
	recent := b.tree(fileEntry("recent", b.file([]byte("within its week"))))
	mustPublish(t, s, repo, alice, b, nil)
	now := time.Now().UTC()
	if _, err := s.CreateWorkspace(ctx, alice, repo, "w"); err != nil {
		t.Fatal(err)
	}
	a, err := s.Attach(ctx, alice, repo, "w", "m1", false, now)
	if err != nil {
		t.Fatal(err)
	}
	c1, err := s.RecordCheckpoint(ctx, alice, a.Lease, 0, []encoding.ID{old}, encoding.RootCommitID)
	if err != nil {
		t.Fatal(err)
	}
	dbOf(s).setCheckpointCreated(t, c1.ID, now.Add(-10*24*time.Hour))
	c2, err := s.RecordCheckpoint(ctx, alice, a.Lease, c1.ID, []encoding.ID{recent}, encoding.RootCommitID)
	if err != nil {
		t.Fatal(err)
	}
	dbOf(s).setCheckpointCreated(t, c2.ID, now.Add(-time.Hour))
	age(t, s, repo, 20)
	r, err := s.GC(ctx, repo, now, GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.CheckpointsDeleted != 0 || r.CheckpointsPinned != 1 {
		t.Fatalf("thinned %d and pinned %d checkpoints, want 0 and 1: %s", r.CheckpointsDeleted, r.CheckpointsPinned, r)
	}
	roots, err := s.ops(ctx).CheckpointTrees(ctx, repo)
	if err != nil || !slices.Equal(roots, []encoding.ID{recent}) {
		t.Fatalf("the mark's checkpoint roots %v, want the recent one's tree alone: %v", roots, err)
	}
	if !has(t, s, repo, "object", old) || !has(t, s, repo, "object", recent) {
		t.Fatalf("a kept checkpoint's tree was swept: the old one's kept %v, the recent one's %v: %s", has(t, s, repo, "object", old), has(t, s, repo, "object", recent), r)
	}
	age(t, s, repo, 40)
	if r, err := s.GC(ctx, repo, now, GCOptions{}); err != nil || r.CheckpointsPinned != 0 || !has(t, s, repo, "object", old) {
		t.Fatalf("a pass after: pinned %v, %v; the old tree kept %v", r, err, has(t, s, repo, "object", old))
	}
}
