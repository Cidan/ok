package storetest

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/move/movetest"
	"github.com/Cidan/ok/internal/workflow"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// A repository moves between two cells' deployers while none of its deployments runs: its plans,
// deployments, services, main's history, head and what is planned, and the SCM's feed's cursor —
// not its name, which every cell's deployer keeps as the directory says. A freeze while one runs
// is refused, and thaws. Frozen, nothing about it is written or started; at the destination it
// waits until it opens, then its waiting deployments start there; the source refuses it, naming
// the cell, and drops it, keeping its name.
func testMoves(t *testing.T, src, dst *store.Store) {
	ctx := context.Background()
	moveMain(t, src, 2)
	if err := dst.ApplyDirectory(ctx, store.DirectoryEvent{Seq: 2, Repository: 1, FullName: "renamed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := src.RecordPlan(ctx, store.PlanResult{Repository: 1, Head: commit(2), HeadSeq: 2, By: self, Services: []workflow.Deploy{web(), data()},
		Deployments: []store.NewDeployment{{Deploy: web(), Why: "w", Wave: 1}, {Deploy: data(), Why: "d", Wave: 2}}}); err != nil {
		t.Fatal(err)
	}
	open, err := src.Unfinished(ctx)
	if err != nil || len(open) != 2 {
		t.Fatalf("the plan's deployments: %v, %v", open, err)
	}
	w, d := open[0], open[1]
	if err := src.Start(ctx, w.ID, ""); err != nil {
		t.Fatal(err)
	}
	scope := &okpb.MoveScope{NetworkId: 1, Repositories: []int64{1}, Role: cells.Deploy}
	srcMoves, dstMoves := movetest.Pair(t, src.Mover(), dst.Mover(), okpb.Caller_CALLER_DEPLOY)
	if _, err := srcMoves.Freeze(ctx, scope); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("freezing while a deployment runs: %v", err)
	}
	if err := src.Log(ctx, w.ID, "still writable"); err != nil {
		t.Fatalf("a write once the refused freeze thawed: %v", err)
	}
	if err := src.Finish(ctx, w.ID, store.Succeeded, ""); err != nil {
		t.Fatal(err)
	}
	read := func(s *store.Store) string {
		t.Helper()
		plans, err1 := s.Plans(ctx, 1)
		latest, err2 := s.Latest(ctx, 1, 10)
		cursor, err3 := s.Cursor(ctx, 1)
		evs, err4 := s.Events(ctx, w.ID)
		for _, err := range []error{err1, err2, err3, err4} {
			if err != nil {
				t.Fatal(err)
			}
		}
		var ds []store.Deployment
		for _, d := range latest {
			ds = append(ds, *d)
		}
		return fmt.Sprintf("%+v\n%+v\ncursor %d\n%+v", plans, ds, cursor, evs)
	}
	want := read(src)

	movetest.Move(t, srcMoves, dstMoves, scope, movetest.Steps{
		Frozen: func() {
			if err := src.Log(ctx, w.ID, "while frozen"); !move.IsFrozen(err) {
				t.Errorf("a write while frozen: %v", err)
			}
			if started, err := src.StartTurns(ctx, ""); err != nil || len(started) != 0 {
				t.Errorf("starts while frozen: %v, %v; want none", started, err)
			}
			if err := src.Apply(ctx, store.FeedEvent{Seq: 9, Repository: 1, Head: commit(9), HeadSeq: 9}); !move.IsFrozen(err) {
				t.Errorf("the feed while frozen: %v", err)
			}
		},
		Imported: func() {
			if err := dst.Log(ctx, w.ID, "before it opened"); !move.IsFrozen(err) {
				t.Errorf("a write at the destination before it opened: %v", err)
			}
		},
	})
	if got := read(dst); got != want {
		t.Errorf("the destination:\n%s\nwant\n%s", got, want)
	}
	if un, err := dst.Unplanned(ctx); err != nil || len(un) != 0 {
		t.Errorf("unplanned at the destination: %+v, %v; want its plan moved with it", un, err)
	}
	if svs, err := dst.Services(ctx, 1); err != nil || len(svs) == 0 || svs[0].RepoName != "renamed" {
		t.Errorf("the destination's services, and its name of the repository: %+v, %v", svs, err)
	}
	if started, err := dst.StartTurns(ctx, ""); err != nil || len(started) != 1 || started[0].ID != d.ID {
		t.Errorf("starts at the destination: %+v, %v; want the waiting deployment", started, err)
	}
	if _, moved := cells.MovedOf(src.Log(ctx, w.ID, "after")); !moved {
		t.Error("a write at the source after the move is not refused as moved")
	}
	if svs, err := src.Services(ctx, 0); err != nil || len(svs) != 0 {
		t.Errorf("the source's services to observe: %+v, %v; want none of what moved", svs, err)
	}

	// A day later, the source drops it, keeping its name.
	if _, err := srcMoves.Drop(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Get(ctx, w.ID); err == nil {
		t.Error("a deployment at the source after the drop")
	}
	if un, err := src.Unplanned(ctx); err != nil || len(un) != 0 {
		t.Errorf("unplanned at the source after the drop: %+v, %v", un, err)
	}
}
