package storetest

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/wait"
)

// Recording a plan tells those waiting on the repository's plans, even a plan with nothing to
// deploy: the commits it reaches are planned from then on, which a CL's status shows.
func testPlanTellsWaiters(t *testing.T, s *store.Store) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	moveMain(t, s, 2)
	q := bus.NewLocalQueue()
	go s.RunWaits(ctx, q)
	w, err := wait.Open(ctx, q, wait.Reg{Store: s.Waits(), Subjects: []string{wait.PlanSubject(1), wait.PlanSubject(2)}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := s.RecordPlan(ctx, store.PlanResult{Repository: 1, Head: commit(2), HeadSeq: 2, By: self}); err != nil {
		t.Fatal(err)
	}
	next, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	got, err := w.Next(next)
	if err != nil || !slices.Equal(got, []string{wait.PlanSubject(1)}) {
		t.Fatalf("after a plan with nothing to deploy: %v, %v; want %s", got, err, wait.PlanSubject(1))
	}
}
