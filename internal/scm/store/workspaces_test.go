package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// A workspace's lease: granted when free, bumped on every grant, renewed by its holder even
// after it expired while no one else took it, refused to a second machine while live, and
// handed over when the second asks to take over and the holder releases.
func TestWorkspaceLease(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	const alice = "alice.human.example.com"
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	// Only a workspace jj clone made is attached.
	if _, err := s.Attach(ctx, alice, repo, "laptop-ada", "m1", false, t0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("attaching a workspace never made: %v, want ErrNotFound", err)
	}
	if _, err := s.CreateWorkspace(ctx, alice, repo, "laptop-ada"); err != nil {
		t.Fatal(err)
	}
	a, err := s.Attach(ctx, alice, repo, "laptop-ada", "m1", false, t0)
	if err != nil || !a.Granted || a.Lease.Epoch != 1 || !a.Lease.ExpiresAt.Equal(t0.Add(LeaseTTL)) || a.Latest != nil {
		t.Fatalf("the first attach: %+v (%v)", a, err)
	}
	// Re-attaching on the same machine (a restarted daemon) is a new grant.
	again, err := s.Attach(ctx, alice, repo, "laptop-ada", "m1", false, t0.Add(time.Second))
	if err != nil || !again.Granted || again.Lease.Epoch != 2 {
		t.Fatalf("re-attaching: %+v (%v)", again, err)
	}
	if _, _, err := s.Renew(ctx, alice, a.Lease, t0.Add(2*time.Second)); !errors.Is(err, ErrPrecondition) {
		t.Errorf("renewing the outlived epoch: %v", err)
	}
	// Another machine is refused while the lease is live.
	b, err := s.Attach(ctx, alice, repo, "laptop-ada", "m2", false, t0.Add(5*time.Second))
	if err != nil || b.Granted || b.Holder != "m1" {
		t.Fatalf("a second machine: %+v (%v)", b, err)
	}
	// Asking to take over: the holder learns at its next renewal, hands over, and the asker
	// is granted.
	if b, err = s.Attach(ctx, alice, repo, "laptop-ada", "m2", true, t0.Add(6*time.Second)); err != nil || b.Granted {
		t.Fatalf("asking to take over: %+v (%v)", b, err)
	}
	renewed, takeover, err := s.Renew(ctx, alice, again.Lease, t0.Add(7*time.Second))
	if err != nil || takeover != "m2" || !renewed.ExpiresAt.Equal(t0.Add(7*time.Second+LeaseTTL)) {
		t.Fatalf("the holder's renewal: %+v %q (%v)", renewed, takeover, err)
	}
	if err := s.Release(ctx, alice, renewed); err != nil {
		t.Fatal(err)
	}
	if b, err = s.Attach(ctx, alice, repo, "laptop-ada", "m2", true, t0.Add(8*time.Second)); err != nil || !b.Granted || b.Lease.Epoch != 3 {
		t.Fatalf("after the handover: %+v (%v)", b, err)
	}
	ws, err := s.ListWorkspaces(ctx, alice, repo)
	var laptop *WorkspaceState
	for i := range ws {
		if ws[i].Name == "laptop-ada" {
			laptop = &ws[i]
		}
	}
	if err != nil || laptop == nil || laptop.Holder != "m2" || laptop.TakeoverBy != "" || laptop.Epoch != 3 || !laptop.OwnLog {
		t.Fatalf("the workspace: %+v (%v)", ws, err)
	}
	// m2 goes quiet; its lease expires but, untaken, still renews.
	if _, _, err := s.Renew(ctx, alice, b.Lease, t0.Add(time.Hour)); err != nil {
		t.Errorf("renewing an expired lease no one took: %v", err)
	}
	// Expired, it is taken without asking.
	c, err := s.Attach(ctx, alice, repo, "laptop-ada", "m3", false, t0.Add(3*time.Hour))
	if err != nil || !c.Granted || c.Lease.Epoch != 4 {
		t.Fatalf("taking an expired lease: %+v (%v)", c, err)
	}
	// Workspaces are their principal's: bob sees none of alice's, only his own.
	if ws, _ := s.ListWorkspaces(ctx, "bob.human.example.com", repo); len(ws) != 1 || ws[0].Name != testWorkspace {
		t.Errorf("bob sees %+v", ws)
	}
	if _, _, err := s.Renew(ctx, "bob.human.example.com", c.Lease, t0.Add(3*time.Hour)); !errors.Is(err, ErrPrecondition) {
		t.Errorf("bob renewing alice's lease: %v", err)
	}
}

// Checkpoints: under the current lease and off the latest, the latest; otherwise a fork, kept.
// Their trees must be published first; they are their principal's alone.
func TestCheckpoints(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	const alice = "alice.human.example.com"
	b := newBuilder(t)
	t1 := b.tree(fileEntry("a", b.file([]byte("one"))))
	t2 := b.tree(fileEntry("a", b.file([]byte("two"))))
	t3 := b.tree(fileEntry("a", b.file([]byte("three"))))
	base := b.commit(t1, "base", encoding.RootCommitID)
	mustPublish(t, s, repo, alice, b, nil)
	now := time.Now()
	if _, err := s.CreateWorkspace(ctx, alice, repo, "w"); err != nil {
		t.Fatal(err)
	}

	a, err := s.Attach(ctx, alice, repo, "w", "m1", false, now)
	if err != nil || !a.Granted {
		t.Fatal(a, err)
	}
	c1, err := s.RecordCheckpoint(ctx, alice, a.Lease, 0, []encoding.ID{t1}, base)
	if err != nil || c1.Fork || c1.ID == 0 {
		t.Fatalf("the first checkpoint: %+v (%v)", c1, err)
	}
	c2, err := s.RecordCheckpoint(ctx, alice, a.Lease, c1.ID, []encoding.ID{t2}, base)
	if err != nil || c2.Fork || c2.ParentID != c1.ID {
		t.Fatalf("the second: %+v (%v)", c2, err)
	}
	// A parent that is not the latest forks.
	stale, err := s.RecordCheckpoint(ctx, alice, a.Lease, c1.ID, []encoding.ID{t3}, base)
	if err != nil || !stale.Fork {
		t.Fatalf("off a stale parent: %+v (%v)", stale, err)
	}
	// Another machine takes over; the first, reconnecting, records a fork.
	m2, err := s.Attach(ctx, alice, repo, "w", "m2", false, now.Add(time.Hour))
	if err != nil || !m2.Granted || m2.Latest == nil || m2.Latest.ID != c2.ID {
		t.Fatalf("m2 attaching at the latest: %+v (%v)", m2, err)
	}
	offline, err := s.RecordCheckpoint(ctx, alice, a.Lease, c2.ID, []encoding.ID{t3}, base)
	if err != nil || !offline.Fork {
		t.Fatalf("the offline machine's checkpoint: %+v (%v)", offline, err)
	}
	c3, err := s.RecordCheckpoint(ctx, alice, m2.Lease, c2.ID, []encoding.ID{t3}, base)
	if err != nil || c3.Fork {
		t.Fatalf("m2's checkpoint: %+v (%v)", c3, err)
	}
	list, err := s.ListCheckpoints(ctx, alice, repo, "w", 0, 10)
	if err != nil || len(list) != 5 || list[0].ID != c3.ID || !list[1].Fork || list[4].ID != c1.ID {
		t.Fatalf("the list: %+v (%v)", list, err)
	}
	if page, _ := s.ListCheckpoints(ctx, alice, repo, "w", list[1].ID, 2); len(page) != 2 || page[0].ID != list[2].ID {
		t.Errorf("the next page: %+v", page)
	}
	got, err := s.GetCheckpoint(ctx, alice, repo, c2.ID)
	if err != nil || got.Trees[0] != t2 || got.BaseCommit != base {
		t.Errorf("checkpoint %d: %+v (%v)", c2.ID, got, err)
	}
	if _, err := s.GetCheckpoint(ctx, "bob.human.example.com", repo, c2.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("bob reading alice's checkpoint: %v", err)
	}
	// A tree the SCM does not hold is refused, naming it.
	unpublished := newBuilder(t)
	t4 := unpublished.tree(fileEntry("b", unpublished.file([]byte("never sent"))))
	var rej *Rejection
	if _, err := s.RecordCheckpoint(ctx, alice, m2.Lease, c3.ID, []encoding.ID{t4}, base); !errors.As(err, &rej) || len(rej.Missing) == 0 {
		t.Errorf("an unpublished tree: %v", err)
	}
	if _, err := s.RecordCheckpoint(ctx, alice, m2.Lease, c3.ID, []encoding.ID{t1, t2}, base); !errors.Is(err, ErrInvalid) {
		t.Errorf("an even number of tree terms: %v", err)
	}
	if _, err := s.RecordCheckpoint(ctx, alice, m2.Lease, offline.ID+1000, []encoding.ID{t1}, base); !errors.Is(err, ErrInvalid) {
		t.Errorf("a parent that does not exist: %v", err)
	}
}

// Retention keeps every checkpoint for a week, then the last of each hour, then the last of
// each day, then none — but always a workspace's latest — and garbage collection keeps every
// kept checkpoint's tree.
func TestCheckpointRetentionAndGC(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	const alice = "alice.human.example.com"
	b := newBuilder(t)
	kept := b.tree(fileEntry("kept", b.file([]byte("a checkpoint's"))))
	dropped := b.tree(fileEntry("dropped", b.file([]byte("thinned away"))))
	mustPublish(t, s, repo, alice, b, nil)
	now := time.Now().UTC()
	if _, err := s.CreateWorkspace(ctx, alice, repo, "w"); err != nil {
		t.Fatal(err)
	}
	a, err := s.Attach(ctx, alice, repo, "w", "m1", false, now)
	if err != nil {
		t.Fatal(err)
	}
	record := func(parent int64, tree encoding.ID, at time.Time) Checkpoint {
		t.Helper()
		c, err := s.RecordCheckpoint(ctx, alice, a.Lease, parent, []encoding.ID{tree}, encoding.RootCommitID)
		if err != nil {
			t.Fatal(err)
		}
		dbOf(s).setCheckpointCreated(t, c.ID, at)
		return c
	}
	hour := now.Add(-10 * 24 * time.Hour).Truncate(time.Hour)
	day := now.Add(-60 * 24 * time.Hour).Truncate(24 * time.Hour)
	ancient := record(0, dropped, now.Add(-400*24*time.Hour))
	inDay1 := record(ancient.ID, dropped, day.Add(time.Hour))
	inDay2 := record(inDay1.ID, kept, day.Add(2*time.Hour))
	inHour1 := record(inDay2.ID, dropped, hour.Add(time.Minute))
	inHour2 := record(inHour1.ID, kept, hour.Add(2*time.Minute))
	recent1 := record(inHour2.ID, kept, now.Add(-time.Hour))
	latest := record(recent1.ID, kept, now.Add(-time.Minute))

	age(t, s, repo, 20)
	r, err := s.GC(ctx, repo, now, GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.CheckpointsDeleted != 3 {
		t.Errorf("thinned %d checkpoints, want 3 (the ancient, the earlier in its day, the earlier in its hour)", r.CheckpointsDeleted)
	}
	list, _ := s.ListCheckpoints(ctx, alice, repo, "w", 0, 100)
	var left []int64
	for _, c := range list {
		left = append(left, c.ID)
	}
	want := []int64{latest.ID, recent1.ID, inHour2.ID, inDay2.ID}
	if len(left) != len(want) {
		t.Fatalf("left %v, want %v", left, want)
	}
	for i := range want {
		if left[i] != want[i] {
			t.Fatalf("left %v, want %v", left, want)
		}
	}
	if !has(t, s, repo, "object", kept) {
		t.Error("the sweep took a kept checkpoint's tree")
	}
	if has(t, s, repo, "object", dropped) {
		t.Error("the sweep kept a tree only thinned checkpoints named")
	}
	if err := s.RecordGC(ctx, r, nil); err != nil {
		t.Errorf("recording the pass: %v", err)
	}
}

// Manifests: each file's size and chunks, without data, in one query per batch; an absent file
// is not found, and the empty file is one frame.
func TestReadManifests(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	big := bytesOf(3*testChunk+17, 9)
	fBig := b.file(big)
	fEmpty := b.file(nil)
	b.tree(fileEntry("big", fBig), fileEntry("empty", fEmpty))
	mustPublish(t, s, repo, "alice.human.example.com", b, nil)
	absent := encoding.ID{7}
	var frames []FileFrame
	if err := s.ReadManifests(ctx, repo, []encoding.ID{fBig, absent, fEmpty}, func(f FileFrame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(frames) != 6 {
		t.Fatalf("%d frames: %+v", len(frames), frames)
	}
	for i, f := range frames[:4] {
		if f.FileID != fBig || len(f.Data) != 0 || f.Offset != uint64(i*testChunk) || f.Last != (i == 3) || (i == 0) != (f.Size == uint64(len(big))) {
			t.Errorf("frame %d: %+v", i, f)
		}
		if f.ChunkID != chunkIDs(big)[i] {
			t.Errorf("frame %d's chunk %s, want %s", i, f.ChunkID, chunkIDs(big)[i])
		}
	}
	if !frames[4].NotFound || frames[4].FileID != absent {
		t.Errorf("the absent file: %+v", frames[4])
	}
	if frames[5].FileID != fEmpty || !frames[5].Last || frames[5].Size != 0 || frames[5].ChunkID != (encoding.ID{}) {
		t.Errorf("the empty file: %+v", frames[5])
	}
}

// Checkpoints of different workspaces in one repository, recorded at once, do not abort one
// another: each takes one transaction.
func TestConcurrentCheckpoints(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	const people, each = 16, 10
	b := newBuilder(t)
	var trees []encoding.ID
	for i := range people * each {
		trees = append(trees, b.tree(fileEntry("a", b.file([]byte(fmt.Sprint(i))))))
	}
	base := b.commit(trees[0], "base", encoding.RootCommitID)
	mustPublish(t, s, repo, "alice.human.example.com", b, nil)
	leases := make([]Lease, people)
	for i := range people {
		who := fmt.Sprintf("p%d.human.example.com", i)
		if _, err := s.CreateWorkspace(ctx, who, repo, "w"); err != nil {
			t.Fatal(err)
		}
		a, err := s.Attach(ctx, who, repo, "w", "m", false, time.Now())
		if err != nil || !a.Granted {
			t.Fatal(a, err)
		}
		leases[i] = a.Lease
	}
	var attempts atomic.Int32
	s.txAttempts = func() { attempts.Add(1) }
	defer func() { s.txAttempts = nil }()
	var wg sync.WaitGroup
	errs := make(chan error, people)
	for i := range people {
		wg.Go(func() {
			who := fmt.Sprintf("p%d.human.example.com", i)
			var parent int64
			for j := range each {
				c, err := s.RecordCheckpoint(ctx, who, leases[i], parent, []encoding.ID{trees[i*each+j]}, base)
				if err != nil {
					errs <- fmt.Errorf("%s: %w", who, err)
					return
				}
				parent = c.ID
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if n := attempts.Load(); n > people*each*11/10 {
		t.Errorf("%d checkpoints took %d transaction attempts", people*each, n)
	}
}
