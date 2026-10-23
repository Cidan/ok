package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// A fork stays whole when its parent is deleted: it reads exactly what it read, through the
// parent's collection and the deletion of its retired segments and packs, and stands alone; the
// chunks only the parent held lose their locations — pinned ones too, folded into tombstones — and
// the dictionary training passes to the fork. Deleting the last repository deletes the network.
func TestDeleteParentKeepsFork(t *testing.T) {
	ctx := context.Background()
	p := newCLEnv(t)
	p.landChange("base", map[string]string{"a": "a0\n", "shared": "both hold it\n"})
	fr := newFork(t, p.s, p.repo)
	f := &clEnv{t: t, s: p.s, repo: fr.ID}
	p.landChange("parent's", map[string]string{"a": "a1\n", "shared": "both hold it\n", "ponly": "parent only\n"})
	f.landChange("fork's", map[string]string{"a": "a0\n", "shared": "both hold it\n", "fonly": "fork only\n"})
	// Pinned, the locations fold into the chunk index's segments.
	compactAll(t, p.s, p.repo)
	compactAll(t, p.s, f.repo)
	net := netOf(t, p.s, p.repo)
	before := viewOf(t, p.s, f.repo, "a")
	ponly, shared, fonly := pack.ChunkID([]byte("parent only\n")), pack.ChunkID([]byte("both hold it\n")), pack.ChunkID([]byte("fork only\n"))
	if locs := locationsView(t, p.s, p.repo); locs[ponly].Stored == 0 || locs[shared].Stored == 0 || locs[fonly].Stored == 0 {
		t.Fatalf("locations before the deletion: %v", locs)
	}

	if err := p.s.DeleteRepository(ctx, p.repo); err != nil {
		t.Fatal(err)
	}
	if _, err := p.s.GetRepository(ctx, p.repo); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the deleted repository: %v", err)
	}
	if r := mustRepo(t, p.s, f.repo); r.Parent != 0 || r.Network != net {
		t.Errorf("the fork after its parent's deletion: %+v; want it in its network, standing alone", r)
	}
	checkLocations := func(stage string) {
		t.Helper()
		locs := locationsView(t, p.s, f.repo)
		if _, ok := locs[ponly]; ok {
			t.Errorf("%s: the chunk only the deleted parent held keeps its location", stage)
		}
		if _, ok := locs[shared]; !ok {
			t.Errorf("%s: the chunk the fork holds too lost its location", stage)
		}
		if _, ok := locs[fonly]; !ok {
			t.Errorf("%s: the fork's own chunk lost its location", stage)
		}
	}
	checkLocations("after the deletion")
	if got := viewOf(t, reopen(t, p.s), f.repo, "a"); !reflect.DeepEqual(got, before) {
		t.Fatalf("after its parent's deletion the fork reads\n%+v\nwant\n%+v", got, before)
	}

	// The fork's collection repacks, folds the drops into tombstones, and deletes the parent's
	// retired segments and packs past their grace.
	if _, err := p.s.GC(ctx, f.repo, time.Now(), GCOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.s.DeleteRetiredOf(ctx, f.repo, time.Now().Add(2*RetiredSegmentGrace)); err != nil {
		t.Fatal(err)
	}
	checkLocations("after the fork's collection")
	if got := viewOf(t, reopen(t, p.s), f.repo, "a"); !reflect.DeepEqual(got, before) {
		t.Fatalf("after the parent's segments were deleted the fork reads\n%+v\nwant\n%+v", got, before)
	}
	clean(t, p.s, f.repo)
	if trainer, err := p.s.ops(ctx).NetworkTrainer(ctx, net); err != nil || trainer != f.repo {
		t.Errorf("the network's trainer after the deletion: %d, %v; want the fork", trainer, err)
	}
	if _, err := p.s.Sync(ctx, f.repo, 0, bob); !errors.Is(err, ErrPrecondition) {
		t.Errorf("syncing a fork whose parent is gone: %v", err)
	}
	if _, err := p.s.GetRepository(ctx, p.repo); !errors.Is(err, ErrNotFound) {
		t.Errorf("the deleted repository: %v", err)
	}

	// The last repository's deletion deletes the network: its objects and rows.
	if err := p.s.DeleteRepository(ctx, f.repo); err != nil {
		t.Fatal(err)
	}
	networkGone(t, p.s, net)
}

// A repository's tail, its content index, and its network go a transaction of a few rows at a time
// (Config.DeleteBatch), and every row goes.
func TestDeleteInBatches(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	for i := range 3 {
		e.landChange(fmt.Sprint("change ", i), map[string]string{"a": fmt.Sprint("a", i, "\n"), fmt.Sprint("f", i): "new\n"})
	}
	compactAll(t, e.s, e.repo)
	e.landChange("in the tail", map[string]string{"a": "tail\n"})
	net := netOf(t, e.s, e.repo)
	small := mustOpen(t, dbOf(e.s), e.s.blobs, Config{DeleteBatch: 2, Signals: e.s.signals})
	txns := 0
	small.txAttempts = func() { txns++ }
	if err := small.DeleteRepository(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	if txns < 10 {
		t.Errorf("the deletion took %d transactions, want one a couple of rows", txns)
	}
	for _, table := range []string{"object", "commit", "commit_parent", "commit_path", "head_log", "changelist", "patchset", "op_log",
		"workspace", "event", "event_pending"} {
		if n := dbOf(e.s).count(t, table, `repository_id = @repo`, map[string]any{"repo": e.repo}); n != 0 {
			t.Errorf("%s: %d rows left", table, n)
		}
	}
	networkGone(t, small, net)
}

// A deletion a replica stopped short of — after the transaction that deleted the repository's row —
// is finished by the next one that looks.
func TestFinishDeletions(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	e.landChange("base", map[string]string{"a": "a0\n"})
	net := netOf(t, e.s, e.repo)
	if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		_, _, err := tx.DeleteRepository(ctx, e.repo)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if n, err := e.s.FinishDeletions(ctx, 1000); err != nil || n == 0 {
		t.Fatalf("finishing deletions: %d, %v", n, err)
	}
	networkGone(t, e.s, net)
	pending, err := e.s.ops(ctx).PendingDeletions(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range pending {
		if d.Repo == e.repo {
			t.Error("the deletion is still pending")
		}
	}
}

// networkGone checks a deleted network left nothing: no object under its keys, no row.
func networkGone(t *testing.T, s *Store, net backend.Network) {
	t.Helper()
	ctx := context.Background()
	objs, err := s.blobs.List(ctx, fmt.Sprintf("nets/%d/", net))
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 0 {
		t.Errorf("%d objects left under the deleted network's keys", len(objs))
	}
	if n := dbOf(s).networks(t, int64(net)); n != 0 {
		t.Errorf("the network's row is left")
	}
	for table, where := range map[string]string{
		"segment":             `network_id = @net`,
		"pack":                `network_id = @net`,
		"content_index":       `scope = 1 AND owner_id = @net`,
		"repository_deletion": `network_id = @net`,
	} {
		if n := dbOf(s).count(t, table, where, map[string]any{"net": int64(net)}); n != 0 {
			t.Errorf("%s: %d rows left", table, n)
		}
	}
}

func mustRepo(t *testing.T, s *Store, repo int64) Repository {
	t.Helper()
	r, err := s.GetRepository(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
