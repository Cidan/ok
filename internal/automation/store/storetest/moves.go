package storetest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/move/movetest"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// A network's repositories move between two cells' ok automation: their runs, logs — whole, and a
// lease's segments — and the SCM's feed's cursors. Frozen, the run a worker held goes back to the
// queue, unspent, and no worker takes it, nor may anyone write about it; at the destination it
// waits until it opens, then runs there; the source refuses the repository, naming the cell, never
// hands out its runs, and drops them, logs and all.
func testMoves(t *testing.T, e *env) {
	ctx := context.Background()
	src, dst := e.newStore(t, store.DefaultConfig), e.newStore(t, store.DefaultConfig)
	const repo, other = 7, 9
	// A finished run, its log stored; one a worker holds, its log in segments; one of a repository
	// not moving, held too.
	done := start(t, src, repo, build("//a:done"))
	lease(t, src, "w1")
	if _, err := src.AppendLog(ctx, done.ID, "w1", 0, []byte("built\n")); err != nil {
		t.Fatal(err)
	}
	if err := src.Finish(ctx, done.ID, "w1", store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	held := start(t, src, repo, build("//a:held"))
	lease(t, src, "w2")
	if _, err := src.AppendLog(ctx, held.ID, "w2", 0, []byte("half")); err != nil {
		t.Fatal(err)
	}
	stays := start(t, src, other, build("//b:stays"))
	lease(t, src, "w3")
	if err := src.SetCursor(ctx, repo, 42); err != nil {
		t.Fatal(err)
	}
	readLog := func(s *store.Store, id int64) string {
		t.Helper()
		var b strings.Builder
		if err := s.ReadLog(ctx, id, 0, false, func(_ int64, data []byte, _ bool) error {
			b.Write(data)
			return nil
		}); err != nil {
			t.Fatalf("run %d's log: %v", id, err)
		}
		return b.String()
	}
	wantLog := readLog(src, done.ID)
	if wantLog != "built\n" {
		t.Fatalf("the finished run's log: %q", wantLog)
	}

	scope := &okpb.MoveScope{NetworkId: repo, Repositories: []int64{repo}, Role: cells.Automation}
	srcMoves, dstMoves := movetest.Pair(t, src.Mover(), dst.Mover(), okpb.Caller_CALLER_AUTOMATION)
	frozenWrite := func(s *store.Store) error {
		_, err := s.Start(ctx, store.StartRequest{RepositoryID: repo, CommitID: commit(), Job: build("//a:new"),
			Origin: store.Origin{Phase: store.PhaseManual}, RequestedBy: "alice.human.example.com"})
		return err
	}
	movetest.Move(t, srcMoves, dstMoves, scope, movetest.Steps{
		Frozen: func() {
			r, err := src.Get(ctx, held.ID)
			if err != nil || r.Status != store.StatusQueued || r.Attempt != held.Attempt {
				t.Errorf("the held run once frozen: %+v, %v; want it queued, its attempt unspent", r, err)
			}
			if r, err := src.Lease(ctx, "w4", "", 0); err != nil || r != nil {
				t.Errorf("a lease while frozen: %+v, %v; want none", r, err)
			}
			if err := frozenWrite(src); !move.IsFrozen(err) {
				t.Errorf("starting a run while frozen: %v", err)
			}
			if _, _, err := src.Heartbeat(ctx, stays.ID, "w3"); err != nil {
				t.Errorf("the repository not moving's run: %v", err)
			}
		},
		Imported: func() {
			if err := frozenWrite(dst); !move.IsFrozen(err) {
				t.Errorf("starting a run at the destination before it opened: %v", err)
			}
		},
	})
	if got := readLog(dst, done.ID); got != wantLog {
		t.Errorf("the finished run's log at the destination: %q, want %q", got, wantLog)
	}
	if c, err := dst.Cursor(ctx, repo); err != nil || c != 42 {
		t.Errorf("the feed's cursor at the destination: %d, %v", c, err)
	}
	if r := lease(t, dst, "w5"); r.ID != held.ID {
		t.Errorf("the run leased at the destination: %d, want the one given back, %d", r.ID, held.ID)
	}
	if err := frozenWrite(src); !moved(err) {
		t.Errorf("starting a run at the source after the move: %v", err)
	}
	if r, err := src.Lease(ctx, "w6", "", 0); err != nil || r != nil {
		t.Errorf("a lease at the source after the move: %+v, %v; want none", r, err)
	}

	// A day later, the source drops it, logs and all.
	if due, err := e.backends[src].Moves().MovedBefore(ctx, time.Now().Add(move.Kept+time.Minute)); err != nil || len(due) != 1 {
		t.Fatalf("moved away: %v, %v", due, err)
	}
	if _, err := srcMoves.Drop(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Get(ctx, done.ID); err == nil {
		t.Error("the finished run at the source after the drop")
	}
	if objs, err := e.blobs[src].List(ctx, "runs/"); err != nil || len(objs) != 0 {
		t.Errorf("logs at the source after the drop: %v, %v", objs, err)
	}
	if r, err := src.Get(ctx, stays.ID); err != nil || r.Status != store.StatusRunning {
		t.Errorf("the repository not moving's run after the drop: %+v, %v", r, err)
	}
}

func moved(err error) bool {
	_, ok := cells.MovedOf(err)
	return ok
}
