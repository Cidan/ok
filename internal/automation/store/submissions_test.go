package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
)

const submitter = "alice.human.example.com"

func submit(t *testing.T, s *Store, repo int64, cls ...SubmittedCL) *Submission {
	t.Helper()
	sub, err := s.Submit(context.Background(), SubmitRequest{RepositoryID: repo, CLs: cls, SubmittedBy: submitter})
	if err != nil {
		t.Fatal(err)
	}
	return sub
}

func postContext(t *testing.T, cl int64) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"phase": "post", "cl": cl, "patchset": 1})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSubmit(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	var inv *InvalidError
	for name, req := range map[string]SubmitRequest{
		"no repository": {CLs: []SubmittedCL{{Number: 1, Patchset: 1}}, SubmittedBy: submitter},
		"no CLs":        {RepositoryID: 1, SubmittedBy: submitter},
		"a CL twice":    {RepositoryID: 1, CLs: []SubmittedCL{{Number: 1, Patchset: 1}, {Number: 1, Patchset: 1}}, SubmittedBy: submitter},
		"no patchset":   {RepositoryID: 1, CLs: []SubmittedCL{{Number: 1}}, SubmittedBy: submitter},
		"no one":        {RepositoryID: 1, CLs: []SubmittedCL{{Number: 1, Patchset: 1}}},
	} {
		if _, err := s.Submit(ctx, req); !errors.As(err, &inv) {
			t.Errorf("%s: %v", name, err)
		}
	}

	stack := submit(t, s, 1, SubmittedCL{Number: 5, Patchset: 2}, SubmittedCL{Number: 6, Patchset: 1})
	if stack.Status != SubmissionActive || len(stack.CLs) != 2 || stack.CLs[0].Number != 5 || stack.CLs[1].Patchset != 1 || stack.SubmittedBy != submitter {
		t.Fatalf("the stack: %+v", stack)
	}
	// A CL is in one active submission at a time.
	var conflict *ConflictError
	if _, err := s.Submit(ctx, SubmitRequest{RepositoryID: 1, CLs: []SubmittedCL{{Number: 6, Patchset: 1}}, SubmittedBy: submitter}); !errors.As(err, &conflict) ||
		!strings.Contains(conflict.Message, "CL 6 is already submitting") {
		t.Fatalf("submitting a CL twice: %v", err)
	}
	// Another repository's CL 6 is another CL.
	submit(t, s, 2, SubmittedCL{Number: 6, Patchset: 1})

	canceled, err := s.CancelSubmission(ctx, stack.ID, "not now")
	if err != nil || canceled.Status != SubmissionCanceled || canceled.Reason != "not now" || canceled.FinishedAt.IsZero() {
		t.Fatalf("canceling: %+v, %v", canceled, err)
	}
	again := submit(t, s, 1, SubmittedCL{Number: 6, Patchset: 2})
	if list, err := s.ListSubmissions(ctx, 1, 6, 0); err != nil || len(list) != 2 || list[0].ID != again.ID || list[1].ID != stack.ID {
		t.Fatalf("CL 6's submissions: %+v, %v", list, err)
	}
	if got, err := s.GetSubmissions(ctx, []int64{stack.ID, again.ID, 9999}); err != nil || len(got) != 2 || got[stack.ID].Status != SubmissionCanceled {
		t.Fatalf("getting submissions: %v, %v", got, err)
	}

	// A new patchset cancels the submissions of earlier ones; leaving review cancels every one.
	if n, err := s.CancelSubmissionsOf(ctx, 1, 6, 2, "patchset 2 was uploaded"); err != nil || n != 0 {
		t.Errorf("a patchset no older: %d, %v", n, err)
	}
	if n, err := s.CancelSubmissionsOf(ctx, 1, 6, 3, "patchset 3 was uploaded"); err != nil || n != 1 {
		t.Errorf("an older patchset: %d, %v", n, err)
	}
	if got, _ := s.GetSubmission(ctx, again.ID); got.Status != SubmissionCanceled || got.Reason != "patchset 3 was uploaded" {
		t.Errorf("after a new patchset: %+v", got)
	}
}

// An attempt's post: its planning runs go first, its jobs wait for the queue to release it,
// and it ends discarded or landed.
func TestAttempts(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	pre := start(t, s, 1, build("//a:a")) // a run queued before, at the default priority
	sub := submit(t, s, 1, SubmittedCL{Number: 5, Patchset: 1})
	base, stone, cand := commit(), commit(), commit()
	id, err := s.CreateAttempt(ctx, NewAttempt{SubmissionID: sub.ID, Base: base, BaseSeq: 7, Chain: []int64{99},
		Why: "CL 5 was submitted", Candidates: []NewCandidate{
			{SubmissionID: 99, CLNumber: 4, Patchset: 1, Commit: stone, Base: base},
			{SubmissionID: sub.ID, CLNumber: 5, Patchset: 1, Commit: cand, Base: stone, Own: true, WorkflowsCommit: base, Context: postContext(t, 5),
				Touched: []string{"lib/lib.go"}},
		}})
	if err != nil {
		t.Fatal(err)
	}

	// Post goes first: its planning run is leased before the older pre run.
	plan := lease(t, s, "w")
	if plan.Job.Kind != KindPlan || plan.Origin.Phase != PhasePost || plan.Origin.CLNumber != 5 || !bytes.Equal(plan.CommitID, cand) ||
		plan.RequestedBy != submitter || plan.Why != "CL 5 was submitted" {
		t.Fatalf("the planning run: %+v", plan)
	}
	if p, err := s.GetPlan(ctx, plan.PlanID); err != nil || !slices.Equal(p.Touched, []string{"lib/lib.go"}) {
		t.Errorf("the post plan's footprint: %v (%v)", p.Touched, err)
	}
	if err := s.ReleaseAttempt(ctx, id); err == nil {
		t.Fatal("an attempt was released before it was planned")
	}
	result := &PlanResult{
		Jobs:     []PlannedJob{{Job: "build", Spec: build("//lib:lib"), Why: "every target the change affects must build"}},
		Affected: []string{"//lib:lib"}, Upstream: []string{"//base:base"},
	}
	if err := s.Finish(ctx, plan.ID, "w", StatusPassed, 0, "", result); err != nil {
		t.Fatal(err)
	}
	// Its jobs wait: the pre run is next.
	if r := lease(t, s, "w"); r.ID != pre.ID {
		t.Fatalf("leased %+v, want the pre run", r)
	}
	attempts, err := s.Attempts(ctx, sub.ID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts: %v, %v", attempts, err)
	}
	a := attempts[0]
	own := a.Own()
	if a.BaseSeq != 7 || a.Released || len(a.Chain) != 1 || a.Chain[0] != 99 || len(a.Candidates) != 2 || len(own) != 1 ||
		!own[0].Plan.Planned || own[0].PlanRun.Status != StatusPassed || len(own[0].Jobs) != 0 ||
		own[0].Plan.Affected[0] != "//lib:lib" || own[0].Plan.Upstream[0] != "//base:base" || own[0].Plan.Jobs[0].Job != "build" {
		t.Fatalf("the planned attempt: %+v; %+v", a, own[0])
	}

	// Released, its jobs run on the candidate, ahead of pre.
	start(t, s, 1, build("//b:b"))
	if err := s.ReleaseAttempt(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseAttempt(ctx, id); err == nil {
		t.Error("an attempt was released twice")
	}
	job := lease(t, s, "w")
	if job.Origin.Phase != PhasePost || job.Origin.Job != "build" || !bytes.Equal(job.CommitID, cand) || job.PlanID != own[0].PlanID ||
		job.Why != "every target the change affects must build" {
		t.Fatalf("the post job: %+v", job)
	}
	// A post run is its submission's: no one cancels or reruns it alone.
	if _, err := s.Cancel(ctx, job.ID, "no"); err == nil {
		t.Error("a post run was canceled alone")
	}
	if err := s.Finish(ctx, job.ID, "w", StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Rerun(ctx, job.ID, submitter); err == nil {
		t.Error("a post run was rerun alone")
	}
	if _, err := s.Start(ctx, StartRequest{RepositoryID: 1, CommitID: commit(), Job: build("//a:a"),
		Origin: Origin{Phase: PhasePost, CLNumber: 5, Patchset: 1}, RequestedBy: submitter}); err == nil {
		t.Error("a post run was started by hand")
	}

	// In use, the attempt's candidates stay; reused on the same base.
	if inUse, err := s.CandidatesInUse(ctx, 1); err != nil || !inUse[string(stone)] || !inUse[string(cand)] {
		t.Errorf("candidates in use: %v, %v", inUse, err)
	}
	if c, ok, err := s.ReusableCandidate(ctx, 1, base, 4, 1); err != nil || !ok || !bytes.Equal(c, stone) {
		t.Errorf("a reusable candidate: %x, %v, %v", c, ok, err)
	}
	if _, ok, _ := s.ReusableCandidate(ctx, 1, base, 4, 2); ok {
		t.Error("another patchset's candidate was reused")
	}

	// Landed: the submission, the attempt, main's history, and each CL's commit.
	landed := commit()
	if err := s.RecordLanding(ctx, sub.ID, id, []LandedCL{{CLNumber: 5, Commit: landed}}, 7); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetSubmission(ctx, sub.ID)
	if got.Status != SubmissionLanded || !bytes.Equal(got.CLs[0].LandedCommit, landed) {
		t.Fatalf("the landed submission: %+v", got)
	}
	if inUse, _ := s.CandidatesInUse(ctx, 1); len(inUse) != 0 {
		t.Errorf("a landed attempt's candidates are still in use: %v", inUse)
	}
	st, err := s.QueueState(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Submissions) != 0 || len(st.Main) != 1 || st.Main[0].Seq != 8 || st.Main[0].SubmissionID != sub.ID ||
		st.Main[0].CLNumber != 5 || st.Others[sub.ID] == nil || st.Landings[sub.ID] == nil || st.Landings[sub.ID].ID != id {
		t.Errorf("the queue after landing: %+v", st)
	}
	if err := s.RecordLanding(ctx, sub.ID, id, nil, 0); err == nil {
		t.Error("a submission landed twice")
	}
}

func TestDiscardAndFail(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	sub := submit(t, s, 1, SubmittedCL{Number: 5, Patchset: 1})
	cand := commit()
	attempt := func() int64 {
		t.Helper()
		id, err := s.CreateAttempt(ctx, NewAttempt{SubmissionID: sub.ID, Base: commit(), BaseSeq: 1,
			Candidates: []NewCandidate{{SubmissionID: sub.ID, CLNumber: 5, Patchset: 1, Commit: cand, Base: commit(), Own: true,
				WorkflowsCommit: commit(), Context: postContext(t, 5)}}})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	first := attempt()
	running := lease(t, s, "w") // its planning run
	if err := s.DiscardAttempt(ctx, first, "CL 4, which it was tested on, did not land"); err != nil {
		t.Fatal(err)
	}
	if stop, reason, _ := s.Heartbeat(ctx, running.ID, "w"); stop != StopCanceled || reason != "CL 4, which it was tested on, did not land" {
		t.Errorf("the discarded attempt's running plan: %d %q", stop, reason)
	}
	second := attempt()
	if err := s.FailSubmission(ctx, sub.ID, "CL 5's test //a:test failed"); err != nil {
		t.Fatal(err)
	}
	attempts, _ := s.Attempts(ctx, sub.ID)
	if attempts[0].Status != AttemptDiscarded || attempts[1].ID != second || attempts[1].Status != AttemptFailed ||
		attempts[1].Own()[0].PlanRun.Status != StatusCanceled {
		t.Errorf("after failing: %+v %+v", attempts[0], attempts[1])
	}
	if got, _ := s.GetSubmission(ctx, sub.ID); got.Status != SubmissionFailed || got.Reason != "CL 5's test //a:test failed" {
		t.Errorf("the failed submission: %+v", got)
	}
	var conflict *ConflictError
	if _, err := s.CreateAttempt(ctx, NewAttempt{SubmissionID: sub.ID, Base: commit(), BaseSeq: 1}); !errors.As(err, &conflict) {
		t.Errorf("an attempt of a failed submission: %v", err)
	}
}

// A submission has one active attempt at a time: a second, even from two queues at once, is
// refused, so its post never runs twice; once the first is discarded, another may be made.
func TestOneActiveAttempt(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	sub := submit(t, s, 1, SubmittedCL{Number: 5, Patchset: 1})
	create := func() (int64, error) {
		return s.CreateAttempt(ctx, NewAttempt{SubmissionID: sub.ID, Base: commit(), BaseSeq: 1,
			Candidates: []NewCandidate{{SubmissionID: sub.ID, CLNumber: 5, Patchset: 1, Commit: commit(), Base: commit(), Own: true,
				WorkflowsCommit: commit(), Context: postContext(t, 5)}}})
	}
	const queues = 8
	ids := make(chan int64, queues)
	errs := make(chan error, queues)
	var wg sync.WaitGroup
	for range queues {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := create()
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	var made []int64
	for id := range ids {
		made = append(made, id)
	}
	if len(made) != 1 {
		t.Fatalf("%d attempts made at once, want 1: %v", len(made), made)
	}
	for err := range errs {
		var conflict *ConflictError
		if !errors.As(err, &conflict) || !strings.Contains(err.Error(), "already has an active attempt") {
			t.Errorf("a second attempt: %v, want a conflict", err)
		}
	}
	if err := s.DiscardAttempt(ctx, made[0], "main moved"); err != nil {
		t.Fatal(err)
	}
	if _, err := create(); err != nil {
		t.Errorf("an attempt once the first was discarded: %v", err)
	}
}

// The queue's view: the active submissions in order, what their chains name, and main's
// history as the feed saw it.
func TestQueueState(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	one := submit(t, s, 3, SubmittedCL{Number: 1, Patchset: 1})
	two := submit(t, s, 3, SubmittedCL{Number: 2, Patchset: 1}, SubmittedCL{Number: 3, Patchset: 1})
	if _, err := s.CreateAttempt(ctx, NewAttempt{SubmissionID: two.ID, Base: commit(), BaseSeq: 4, Chain: []int64{one.ID}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelSubmission(ctx, one.ID, "no"); err != nil {
		t.Fatal(err)
	}
	c5, c6 := commit(), commit()
	for seq, c := range map[int64][]byte{5: c5, 6: c6} {
		if err := s.RecordMainCommit(ctx, 3, seq, c); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RecordLandedChange(ctx, 3, c6, 3); err != nil {
		t.Fatal(err)
	}
	st, err := s.QueueState(ctx, 3, 9)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Submissions) != 1 || st.Submissions[0].ID != two.ID || len(st.Submissions[0].Attempts) != 1 || st.From != 4 ||
		len(st.Main) != 2 || st.Main[1].CLNumber != 3 || st.Main[1].SubmissionID != 0 ||
		st.Others[one.ID] == nil || st.Others[one.ID].Status != SubmissionCanceled || !bytes.Equal(st.LandedCLs[3], c6) {
		t.Fatalf("the queue: %+v", st)
	}
	if repos, err := s.QueueRepositories(ctx, 0); err != nil || len(repos) != 1 || repos[0] != 3 {
		t.Errorf("the queue's repositories: %v, %v", repos, err)
	}
}

// Post carries a pass from pre for each job its plan says nothing changed for since pre: the
// same built-in job, or a workflow job that ran the same Bazel command. A job pre failed, never
// ran, or that something changed for since runs on the candidate.
func TestCarryFromPre(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	test := func(target string, args ...string) Job {
		return Job{Kind: KindTest, Targets: []string{target}, Args: args}
	}

	// Pre on CL 4's patchset 1: everything passes but the flaky test.
	prePlan, preCommit := plan(t, s, 1)
	if err := s.Finish(ctx, lease(t, s, "w").ID, "w", StatusPassed, 0, "", &PlanResult{Jobs: []PlannedJob{
		{Job: "build", Spec: build("//lib:lib", "//app:app")},
		{Job: "test //lib:test", Spec: test("//lib:test")},
		{Job: "test //lib:flaky", Spec: test("//lib:flaky")},
		{Job: "lint", Spec: Job{Kind: KindLint}},
		{Job: "//.ok/workflow/ci.yaml:pre:check", Spec: test("//lib:check", "-v")},
	}}); err != nil {
		t.Fatal(err)
	}
	preRuns := map[string]int64{}
	for range 5 {
		r := lease(t, s, "w")
		status := StatusPassed
		if r.Origin.Job == "test //lib:flaky" {
			status = StatusFailed
		}
		if err := s.Finish(ctx, r.ID, "w", status, 0, "", nil); err != nil {
			t.Fatal(err)
		}
		preRuns[r.Origin.Job] = r.ID
	}

	sub := submit(t, s, 9, SubmittedCL{Number: 4, Patchset: 1})
	cand := commit()
	attempt, err := s.CreateAttempt(ctx, NewAttempt{SubmissionID: sub.ID, Base: commit(), BaseSeq: 1,
		Candidates: []NewCandidate{{SubmissionID: sub.ID, CLNumber: 4, Patchset: 1, Commit: cand, Base: commit(), Own: true,
			WorkflowsCommit: commit(), Context: postContext(t, 4)}}})
	if err != nil {
		t.Fatal(err)
	}
	planning := lease(t, s, "w")
	if p, err := s.GetPlan(ctx, planning.PlanID); err != nil || !bytes.Equal(p.PreCommit, preCommit) || prePlan.PreCommit != nil {
		t.Fatalf("the post plan's pre commit: %x, want %x (%v)", p.PreCommit, preCommit, err)
	}
	const check = "//.ok/workflow/ci.yaml:post:check"
	if err := s.Finish(ctx, planning.ID, "w", StatusPassed, 0, "", &PlanResult{Jobs: []PlannedJob{
		{Job: "build", Spec: build("//lib:lib"), UnchangedSincePre: true},
		{Job: "build since pre", Spec: build("//app:app")},
		{Job: "test //lib:test", Spec: test("//lib:test"), UnchangedSincePre: true},
		{Job: "test //lib:flaky", Spec: test("//lib:flaky"), UnchangedSincePre: true},
		{Job: "test //lib:new", Spec: test("//lib:new"), UnchangedSincePre: true},
		{Job: "lint", Spec: Job{Kind: KindLint}},
		{Job: check, Spec: test("//lib:check", "-v"), UnchangedSincePre: true},
	}, Affected: []string{"//lib:lib"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}

	attempts, err := s.Attempts(ctx, sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	carried := map[string]int64{"build": preRuns["build"], "test //lib:test": preRuns["test //lib:test"], check: preRuns["//.ok/workflow/ci.yaml:pre:check"]}
	queued := 0
	for _, j := range attempts[0].Own()[0].Jobs {
		from, ok := carried[j.Origin.Job]
		switch {
		case ok && (j.Status != StatusPassed || j.CarriedFrom != from || !bytes.Equal(j.CommitID, cand) || !j.LogStored || j.Worker != ""):
			t.Errorf("carried %s: %+v, want a pass from run %d", j.Origin.Job, j, from)
		case !ok && (j.Status != StatusQueued || j.CarriedFrom != 0):
			t.Errorf("%s was not run: %+v", j.Origin.Job, j)
		case !ok:
			queued++
		}
	}
	if queued != 4 {
		t.Errorf("%d jobs queued, want 4", queued)
	}
	var log strings.Builder
	build := attempts[0].Own()[0].Jobs[0]
	if err := s.ReadLog(ctx, build.ID, 0, false, func(_ int64, data []byte, _ bool) error { log.Write(data); return nil }); err != nil ||
		!strings.Contains(log.String(), fmt.Sprintf("not run: it passed in pre as run %d, on patchset 1's commit %.10x", preRuns["build"], preCommit)) {
		t.Errorf("the carried build's log: %q (%v)", log.String(), err)
	}
	if st, reason, _ := attemptVerdict(t, s, sub.ID); st != "testing" {
		t.Errorf("the attempt with jobs left: %s %s", st, reason)
	}

	// A CL without pre has nothing to compare with.
	other := submit(t, s, 9, SubmittedCL{Number: 8, Patchset: 1})
	if _, err := s.CreateAttempt(ctx, NewAttempt{SubmissionID: other.ID, Base: commit(), BaseSeq: 1,
		Candidates: []NewCandidate{{SubmissionID: other.ID, CLNumber: 8, Patchset: 1, Commit: commit(), Base: commit(), Own: true,
			WorkflowsCommit: commit(), Context: postContext(t, 8)}}}); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		lease(t, s, "w")
	}
	if p, err := s.GetPlan(ctx, lease(t, s, "w").PlanID); err != nil || p.PreCommit != nil {
		t.Errorf("a post plan without pre: %+v, %v", p, err)
	}
}

// attemptVerdict says whether a submission's latest attempt's jobs are all finished.
func attemptVerdict(t *testing.T, s *Store, sub int64) (string, string, error) {
	t.Helper()
	attempts, err := s.Attempts(context.Background(), sub)
	if err != nil {
		return "", "", err
	}
	for _, j := range attempts[len(attempts)-1].Own()[0].Jobs {
		if !j.Status.Finished() {
			return "testing", j.Origin.Job, nil
		}
	}
	return "finished", "", nil
}
