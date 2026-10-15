package store

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/lock"
)

// plan records a patchset's plan on repository 9, CL 4.
func plan(t *testing.T, s *Store, patchset int32) (*Plan, []byte) {
	t.Helper()
	c := commit()
	p, created, err := s.CreatePlan(context.Background(), PlanRequest{RepositoryID: 9, CLNumber: 4, Patchset: patchset,
		CommitID: c, WorkflowsCommit: commit(), Context: []byte(`{"phase":"pre","cl":4}`), RequestedBy: "alice.human.example.com"})
	if err != nil || !created {
		t.Fatalf("planning patchset %d: %v (created %v)", patchset, err, created)
	}
	return p, c
}

func pre(t *testing.T, s *Store, patchset int32) *Pre {
	t.Helper()
	p, err := s.GetPre(context.Background(), 9, 4, patchset)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

var twoJobs = &PlanResult{
	Jobs: []PlannedJob{
		{Job: "build", Spec: build("//a:a"), Why: "every target the change affects must build"},
		{Job: "test //a:test", Spec: Job{Kind: KindTest, Targets: []string{"//a:test"}}, Why: "the test is affected"},
	},
	Skipped: []SkippedJob{{Job: "//.ok/workflow/ci.yaml:pre:bench", Target: "//b:bench", Why: "its target is not affected"}},
}

func TestPlan(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	if p := pre(t, s, 1); p.Verdict != VerdictNone || p.Plan != nil {
		t.Fatalf("before planning: %+v", p)
	}
	p, c := plan(t, s, 1)

	// A patchset is planned once, however often it is announced.
	again, created, err := s.CreatePlan(ctx, PlanRequest{RepositoryID: 9, CLNumber: 4, Patchset: 1, CommitID: commit(),
		WorkflowsCommit: commit(), Context: []byte(`{}`), RequestedBy: "alice.human.example.com"})
	if err != nil || created || again.ID != p.ID || !bytes.Equal(again.CommitID, c) {
		t.Fatalf("planning it again: %+v, %v, %v", again, created, err)
	}

	// Its planning run is queued, carrying the plan, and the worker gets the job context.
	r := lease(t, s, "w")
	if r.Job.Kind != KindPlan || r.PlanID != p.ID || r.Origin.Phase != PhasePre || r.Origin.CLNumber != 4 || r.Origin.Job != "plan" ||
		!bytes.Equal(r.CommitID, c) || r.Why != "patchset 1 was uploaded" {
		t.Fatalf("the planning run: %+v", r)
	}
	if got, err := s.GetPlan(ctx, r.PlanID); err != nil || !strings.Contains(string(got.Context), `"cl": 4`) && !strings.Contains(string(got.Context), `"cl":4`) {
		t.Fatalf("its plan: %+v, %v", got, err)
	}
	if v := pre(t, s, 1).Verdict; v != VerdictPending {
		t.Errorf("while planning: %d", v)
	}

	// A planning run that passes carries its plan; nothing else does.
	if err := s.Finish(ctx, r.ID, "w", StatusPassed, 0, "", nil); err == nil {
		t.Fatal("a planning run passed without its plan")
	}
	if err := s.Finish(ctx, r.ID, "w", StatusPassed, 0, "", &PlanResult{Jobs: []PlannedJob{{Job: "x", Spec: Job{Kind: KindPlan}}}}); err == nil {
		t.Fatal("a plan queued a planning run")
	}
	if err := s.Finish(ctx, r.ID, "w", StatusPassed, 0, "", twoJobs); err != nil {
		t.Fatal(err)
	}
	got := pre(t, s, 1)
	if got.Verdict != VerdictPending || got.PlanRun.Status != StatusPassed || len(got.Jobs) != 2 ||
		got.Jobs[0].Origin.Job != "build" || got.Jobs[1].Origin.Job != "test //a:test" || got.Jobs[1].Why != "the test is affected" ||
		len(got.Plan.Skipped) != 1 || got.Plan.Skipped[0].Target != "//b:bench" || !got.Plan.Planned {
		t.Fatalf("after planning: %+v", got)
	}
	for _, j := range got.Jobs {
		if j.PlanID != p.ID || j.Origin.Patchset != 1 || j.Status != StatusQueued || !bytes.Equal(j.CommitID, c) {
			t.Errorf("a planned job: %+v", j)
		}
	}
	// A planned patchset is not planned again.
	if _, err := s.Rerun(ctx, r.ID, "alice.human.example.com"); err == nil {
		t.Error("a planned patchset's planning run ran again")
	}

	// A job that fails fails pre, whatever is still running; its rerun is what counts.
	b, tst := lease(t, s, "w"), lease(t, s, "w")
	if err := s.Finish(ctx, b.ID, "w", StatusFailed, 1, "", nil); err != nil {
		t.Fatal(err)
	}
	if v := pre(t, s, 1).Verdict; v != VerdictFailed {
		t.Errorf("with a failed job and one running: %d", v)
	}
	again2, err := s.Rerun(ctx, b.ID, "alice.human.example.com")
	if err != nil || again2.PlanID != p.ID || again2.Why != b.Why || again2.Origin.Job != "build" {
		t.Fatalf("rerunning the build: %+v, %v", again2, err)
	}
	if v := pre(t, s, 1).Verdict; v != VerdictPending {
		t.Errorf("with the build rerun: %d", v)
	}
	if err := s.Finish(ctx, tst.ID, "w", StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	b2 := lease(t, s, "w")
	if err := s.Finish(ctx, b2.ID, "w", StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	got = pre(t, s, 1)
	if got.Verdict != VerdictPassed || len(got.Jobs) != 2 || got.Jobs[0].ID != b2.ID {
		t.Errorf("with every job passed: %+v", got)
	}
}

func TestPlanFails(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	plan(t, s, 1)
	r := lease(t, s, "w")
	if err := s.Finish(ctx, r.ID, "w", StatusFailed, 1, "the build graph: a cycle", nil); err != nil {
		t.Fatal(err)
	}
	if p := pre(t, s, 1); p.Verdict != VerdictFailed || len(p.Jobs) != 0 {
		t.Fatalf("a failed plan: %+v", p)
	}
	// An unplanned patchset's planning run may run again.
	if _, err := s.Rerun(ctx, r.ID, "alice.human.example.com"); err != nil {
		t.Fatal(err)
	}
	if p := pre(t, s, 1); p.Verdict != VerdictPending || p.PlanRun.RerunOf != r.ID {
		t.Fatalf("planning again: %+v", p)
	}
}

// A new patchset supersedes the last: its pre runs that have not finished are canceled, and a
// planning run that passes after that queues its jobs canceled.
func TestPlanSupersedes(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	plan(t, s, 1)
	r := lease(t, s, "w")
	if err := s.Finish(ctx, r.ID, "w", StatusPassed, 0, "", twoJobs); err != nil {
		t.Fatal(err)
	}
	running := lease(t, s, "w") // the build runs; the test stays queued
	manual := start(t, s, 9, build("//a:a"))

	plan(t, s, 2)
	old := pre(t, s, 1)
	if old.Verdict != VerdictFailed {
		t.Errorf("patchset 1 after patchset 2: %d", old.Verdict)
	}
	for _, j := range old.Jobs {
		switch j.ID {
		case running.ID:
			if stop, reason, _ := s.Heartbeat(ctx, j.ID, "w"); stop != StopCanceled || reason != "patchset 2 superseded it" {
				t.Errorf("the running build: %d %q", stop, reason)
			}
		default:
			if j.Status != StatusCanceled || j.Reason != "patchset 2 superseded it" {
				t.Errorf("the queued test: %+v", j)
			}
		}
	}
	if m, _ := s.Get(ctx, manual.ID); m.Status != StatusQueued {
		t.Errorf("a manual run was canceled: %+v", m)
	}

	// Patchset 2's planning run is queued; patchset 3's supersedes it while it runs, and it
	// passes anyway: its jobs are recorded canceled.
	var two *Run
	for two == nil || two.Job.Kind != KindPlan {
		two = lease(t, s, "w2")
	}
	plan(t, s, 3)
	if err := s.Finish(ctx, two.ID, "w2", StatusPassed, 0, "", twoJobs); err != nil {
		t.Fatal(err)
	}
	p2 := pre(t, s, 2)
	if len(p2.Jobs) != 2 || p2.Verdict != VerdictFailed {
		t.Fatalf("patchset 2: %+v", p2)
	}
	for _, j := range p2.Jobs {
		if j.Status != StatusCanceled || j.Reason != "patchset 3 superseded it" {
			t.Errorf("a superseded plan's job: %+v", j)
		}
	}
}

func TestPlanRefuses(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	var inv *InvalidError
	for name, req := range map[string]PlanRequest{
		"no patchset":   {RepositoryID: 1, CLNumber: 1, CommitID: commit(), WorkflowsCommit: commit(), Context: []byte(`{}`), RequestedBy: "a"},
		"short commit":  {RepositoryID: 1, CLNumber: 1, Patchset: 1, CommitID: []byte{1}, WorkflowsCommit: commit(), Context: []byte(`{}`), RequestedBy: "a"},
		"no context":    {RepositoryID: 1, CLNumber: 1, Patchset: 1, CommitID: commit(), WorkflowsCommit: commit(), Context: []byte(`{`), RequestedBy: "a"},
		"no one asking": {RepositoryID: 1, CLNumber: 1, Patchset: 1, CommitID: commit(), WorkflowsCommit: commit(), Context: []byte(`{}`)},
	} {
		if _, _, err := s.CreatePlan(ctx, req); !errors.As(err, &inv) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// Only automation plans.
	_, err := s.Start(ctx, StartRequest{RepositoryID: 1, CommitID: commit(), Job: Job{Kind: KindPlan},
		Origin: Origin{Phase: PhasePre, CLNumber: 1, Patchset: 1}, RequestedBy: "a"})
	if !errors.As(err, &inv) {
		t.Errorf("starting a planning run: %v", err)
	}
	// A plan's jobs each have an identity.
	plan(t, s, 1)
	r := lease(t, s, "w")
	dup := &PlanResult{Jobs: []PlannedJob{{Job: "build", Spec: build("//a:a")}, {Job: "build", Spec: build("//b:b")}}}
	if err := s.Finish(ctx, r.ID, "w", StatusPassed, 0, "", dup); !errors.As(err, &inv) {
		t.Errorf("a plan with a job twice: %v", err)
	}
}

func TestFeedCursor(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	if c, err := s.Cursor(ctx); err != nil || c != 0 {
		t.Fatalf("a new cursor: %d, %v", c, err)
	}
	for _, seq := range []int64{5, 9, 7} {
		if err := s.SetCursor(ctx, seq); err != nil {
			t.Fatal(err)
		}
	}
	if c, _ := s.Cursor(ctx); c != 9 {
		t.Errorf("the cursor moved back: %d", c)
	}
	// One replica follows at a time: no second leader while the first leads, and a new one once
	// it gives up.
	err := s.LeadFeed(ctx, func(ctx context.Context) error {
		if err := s.LeadFeed(ctx, func(context.Context) error { t.Error("two leaders"); return nil }); !errors.Is(err, lock.ErrNotAcquired) {
			t.Errorf("a second leader: %v, want lock.ErrNotAcquired", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("leading: %v", err)
	}
	led := false
	if err := s.LeadFeed(ctx, func(context.Context) error { led = true; return nil }); err != nil || !led {
		t.Errorf("no leader after the first gave up: %v", err)
	}
}

// A CL abandoned has its unfinished pre runs canceled; restored, its pre resumes.
func TestEndAndResumePre(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	plan(t, s, 1)
	r := lease(t, s, "w")
	if err := s.Finish(ctx, r.ID, "w", StatusPassed, 0, "", twoJobs); err != nil {
		t.Fatal(err)
	}
	running := lease(t, s, "w") // the build runs; the test stays queued

	if err := s.EndPre(ctx, 9, 4, "CL 4 was abandoned"); err != nil {
		t.Fatal(err)
	}
	p := pre(t, s, 1)
	if p.Verdict != VerdictFailed || p.Jobs[1].Status != StatusCanceled || p.Jobs[1].Reason != "CL 4 was abandoned" {
		t.Fatalf("after abandoning: %+v", p)
	}

	// Restored before the build's worker heard the stop: the build keeps running, and the
	// test is queued again.
	n, err := s.ResumePre(ctx, 9, 4, 1, "alice.human.example.com")
	if err != nil || n != 1 {
		t.Fatalf("resuming: %d, %v", n, err)
	}
	if stop, _, _ := s.Heartbeat(ctx, running.ID, "w"); stop != StopNone {
		t.Errorf("the running build is still told to stop: %d", stop)
	}
	p = pre(t, s, 1)
	if p.Verdict != VerdictPending || p.Jobs[1].Status != StatusQueued || p.Jobs[1].RerunOf == 0 || p.Jobs[1].Why != "the test is affected" {
		t.Fatalf("after restoring: %+v", p)
	}
	// Nothing canceled, nothing resumed.
	if n, err := s.ResumePre(ctx, 9, 4, 1, "alice.human.example.com"); err != nil || n != 0 {
		t.Errorf("resuming again: %d, %v", n, err)
	}

	// A CL abandoned while planning resumes its planning.
	plan(t, s, 2)
	if err := s.EndPre(ctx, 9, 4, "CL 4 was abandoned"); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ResumePre(ctx, 9, 4, 2, "alice.human.example.com"); err != nil || n != 1 {
		t.Fatalf("resuming the planning: %d, %v", n, err)
	}
	if p := pre(t, s, 2); p.Verdict != VerdictPending || p.PlanRun.Status != StatusQueued {
		t.Errorf("patchset 2 after restoring: %+v", p.PlanRun)
	}
}
