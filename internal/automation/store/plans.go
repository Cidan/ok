package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Plan is what a patchset's pre runs, or post on one of a submission's candidates: the commit
// under test, the workflow files that select its workflow jobs, its job context, and — once its
// planning run has passed — its jobs, each a run of the plan, the workflow jobs it leaves to
// main, and the change's footprint: the targets it affects and what they build on.
type Plan struct {
	ID              int64
	RepositoryID    int64
	CLNumber        int64
	Patchset        int32
	Phase           Phase
	CommitID        []byte
	WorkflowsCommit []byte
	Context         []byte // the change, as the job context holds it (workflow.Change), JSON
	// Touched is every path a post plan's change touches (workflow.Context.Touched): its
	// footprint in the landing queue. A pre plan's is empty.
	Touched   []string
	Planned   bool // its planning run passed
	Jobs      []PlannedJob
	Skipped   []SkippedJob
	Affected  []string
	Upstream  []string
	CreatedAt time.Time
	// PreCommit is, for post, the commit the CL's pre ran on, which the planning run compares the
	// candidate with (nil: none).
	PreCommit []byte
}

// PlannedJob is one job a plan selected.
type PlannedJob struct {
	Job  string `json:"job"` // its identity
	Spec Job    `json:"spec"`
	Why  string `json:"why"`
	// UnchangedSincePre is, for post, that nothing it reads differs between the commit pre ran
	// on and the candidate: a pass of it in pre holds, and is carried instead of run.
	UnchangedSincePre bool `json:"unchanged_since_pre,omitempty"`
}

// SkippedJob is a workflow job a plan selected but does not run: its target is not affected,
// so its result on main stands.
type SkippedJob struct {
	Job    string `json:"job"`
	Target string `json:"target"`
	Why    string `json:"why"`
}

// PlanResult is what a planning run found: the jobs to run, the workflow jobs left to main, and
// the change's footprint.
type PlanResult struct {
	Jobs     []PlannedJob
	Skipped  []SkippedJob
	Affected []string // the targets the change affects, by label
	Upstream []string // the targets they build on
}

// PlanRequest is a patchset to plan.
type PlanRequest struct {
	RepositoryID    int64
	CLNumber        int64
	Patchset        int32
	CommitID        []byte
	WorkflowsCommit []byte
	Context         []byte // the job context, JSON
	RequestedBy     string // who uploaded the patchset
	Why             string // why pre runs, as its planning run says; unset, the patchset's upload
}

// planJob is a planning run's job identity.
const planJob = "plan"

// CreatePlan records a patchset's pre and queues its planning run, once: a patchset already
// planned is left as it is (created is false). The pre runs of the CL's earlier patchsets that
// have not finished are canceled: a new patchset supersedes them.
func (s *Store) CreatePlan(ctx context.Context, req PlanRequest) (plan *Plan, created bool, err error) {
	switch {
	case req.RepositoryID <= 0 || req.CLNumber <= 0 || req.Patchset <= 0:
		return nil, false, invalid("a plan names its repository, CL, and patchset")
	case len(req.CommitID) != 32 || len(req.WorkflowsCommit) != 32:
		return nil, false, invalid("a commit ID is 32 bytes")
	case !json.Valid(req.Context):
		return nil, false, invalid("a plan's context is JSON")
	case req.RequestedBy == "":
		return nil, false, invalid("a run is requested by someone")
	}
	err = s.txn(ctx, func(tx Ops) error {
		created = false
		if err := tx.LockCL(ctx, req.RepositoryID, req.CLNumber); err != nil {
			return err
		}
		var err error
		plan, err = tx.PrePlan(ctx, req.RepositoryID, req.CLNumber, req.Patchset)
		if err == nil || !errors.Is(err, ErrNotFound) {
			return err
		}
		plan, err = tx.InsertPlan(ctx, NewPlan{RepositoryID: req.RepositoryID, CLNumber: req.CLNumber, Patchset: req.Patchset,
			Phase: PhasePre, CommitID: req.CommitID, WorkflowsCommit: req.WorkflowsCommit, Context: req.Context})
		if err != nil {
			return err
		}
		created = true
		if _, err := tx.InsertRun(ctx, NewRun{
			RepositoryID: req.RepositoryID, CommitID: req.CommitID, Job: Job{Kind: KindPlan, Pool: s.cfg.PlanPool},
			Origin: Origin{Phase: PhasePre, CLNumber: req.CLNumber, Patchset: req.Patchset, Job: planJob}, PlanID: plan.ID,
			Why: orElse(req.Why, fmt.Sprintf("patchset %d was uploaded", req.Patchset)), RequestedBy: req.RequestedBy, Status: StatusQueued,
			TimeoutSeconds: int32(s.cfg.DefaultTimeout / time.Second),
		}); err != nil {
			return err
		}
		return s.cancelPre(ctx, tx, req.RepositoryID, req.CLNumber, req.Patchset, fmt.Sprintf("patchset %d superseded it", req.Patchset))
	})
	return plan, created, err
}

// EndPre cancels every pre run of a CL that has not finished: the CL landed, or was abandoned,
// and no pre of it can matter now.
func (s *Store) EndPre(ctx context.Context, repo, cl int64, reason string) error {
	return s.txn(ctx, func(tx Ops) error {
		if err := tx.LockCL(ctx, repo, cl); err != nil {
			return err
		}
		return s.cancelPre(ctx, tx, repo, cl, math.MaxInt32, reason)
	})
}

// ResumePre resumes a patchset's pre — its CL was abandoned and is restored: a running run
// told to stop, whose worker has not yet heard it, keeps running, and each job whose latest run
// ended canceled is queued again. It returns how many were queued. (A run whose worker already
// heard the stop ends canceled; it is run again by hand.)
func (s *Store) ResumePre(ctx context.Context, repo, cl int64, patchset int32, requestedBy string) (int, error) {
	var n int
	err := s.txn(ctx, func(tx Ops) error {
		n = 0
		if err := tx.LockCL(ctx, repo, cl); err != nil {
			return err
		}
		if err := tx.ClearPreStops(ctx, repo, cl, patchset); err != nil {
			return err
		}
		plan, err := tx.PrePlan(ctx, repo, cl, patchset)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		runs, err := tx.PlanRuns(ctx, []int64{plan.ID})
		if err != nil {
			return err
		}
		latest := latestByJob(runs)
		jobs := make([]string, 0, len(latest))
		for j := range latest {
			jobs = append(jobs, j)
		}
		sort.Strings(jobs)
		for _, j := range jobs {
			if r := latest[j]; r.Status == StatusCanceled {
				if _, err := tx.InsertRun(ctx, rerunOf(r, requestedBy)); err != nil {
					return err
				}
				n++
			}
		}
		return nil
	})
	return n, err
}

// latestByJob is each job's latest run among runs, which are in ID order.
func latestByJob(runs []*Run) map[string]*Run {
	out := map[string]*Run{}
	for _, r := range runs {
		out[r.Origin.Job] = r
	}
	return out
}

// cancelPre cancels the pre runs of a CL's patchsets before below that have not finished: a
// queued run ends at once, and a running one's worker is told to stop it.
func (s *Store) cancelPre(ctx context.Context, tx Ops, repo, cl int64, below int32, reason string) error {
	ended, err := tx.CancelQueuedPre(ctx, repo, cl, below, reason)
	if err != nil {
		return err
	}
	for _, id := range ended {
		if err := changed(ctx, tx, id); err != nil {
			return err
		}
	}
	return tx.StopRunningPre(ctx, repo, cl, below, reason)
}

// GetPlan returns a plan.
func (s *Store) GetPlan(ctx context.Context, id int64) (*Plan, error) { return s.b.Plan(ctx, id) }

// queuePlan records what a planning run found, within the transaction that records the run
// passed: the plan's jobs, the workflow jobs it leaves to main, and the change's footprint.
// A pre plan's jobs are queued at once — recorded canceled when a newer patchset has
// superseded the plan, since they would test a patchset no one can submit. A post plan's wait
// for the landing queue to release its attempt.
func (s *Store) queuePlan(ctx context.Context, tx Ops, run *Run, result *PlanResult) error {
	if err := tx.LockCL(ctx, run.RepositoryID, run.Origin.CLNumber); err != nil {
		return err
	}
	seen := map[string]bool{planJob: true}
	for _, j := range result.Jobs {
		if j.Job == "" || seen[j.Job] {
			return invalid("a plan's jobs each have an identity of their own, not %q", j.Job)
		}
		seen[j.Job] = true
		if err := validJob(j.Spec); err != nil {
			return invalid("job %s: %s", j.Job, err)
		}
	}
	for _, sk := range result.Skipped {
		if sk.Job == "" || seen[sk.Job] {
			return invalid("a plan's jobs each have an identity of their own, not %q", sk.Job)
		}
		seen[sk.Job] = true
	}
	for _, l := range append(append([]string(nil), result.Affected...), result.Upstream...) {
		if !strings.HasPrefix(l, "//") {
			return invalid("%q is not a Bazel label", l)
		}
	}
	plan, err := tx.LockPlan(ctx, run.PlanID)
	if err != nil {
		return err
	}
	if plan.Planned {
		return &ConflictError{Message: "the patchset is planned already"}
	}
	if err := tx.SetPlanned(ctx, run.PlanID, PlanResult{
		Jobs: orEmptyJobs(result.Jobs), Skipped: orEmptySkipped(result.Skipped),
		Affected: orEmpty(BoundTargets(result.Affected)), Upstream: orEmpty(BoundTargets(result.Upstream)),
	}); err != nil {
		return err
	}
	if run.Origin.Phase == PhasePost {
		// The landing queue decides when the attempt runs; a released attempt's plans are all
		// planned, so this plan's is not released yet.
		return markQueue(ctx, tx, run.RepositoryID)
	}
	newer, err := tx.MaxPrePatchset(ctx, run.RepositoryID, run.Origin.CLNumber)
	if err != nil {
		return err
	}
	status, reason := StatusQueued, ""
	if newer > run.Origin.Patchset {
		status, reason = StatusCanceled, fmt.Sprintf("patchset %d superseded it", newer)
	}
	return s.insertJobs(ctx, tx, run.RepositoryID, run.CommitID, run.Origin, run.PlanID, run.RequestedBy, result.Jobs, status, reason)
}

// insertJobs queues a plan's jobs as runs on its commit — or records them ended, canceled for
// reason, when status says so.
func (s *Store) insertJobs(ctx context.Context, tx Ops, repo int64, commit []byte, origin Origin, planID int64,
	requestedBy string, jobs []PlannedJob, status Status, reason string) error {
	if len(jobs) == 0 {
		return nil
	}
	rs := make([]NewRun, 0, len(jobs))
	for _, j := range jobs {
		o := origin
		o.Job = j.Job
		rs = append(rs, NewRun{
			RepositoryID: repo, CommitID: commit, Job: j.Spec, Origin: o, PlanID: planID, Why: j.Why,
			RequestedBy: requestedBy, Status: status, Reason: reason, TimeoutSeconds: int32(s.cfg.DefaultTimeout / time.Second),
			Finished: status != StatusQueued, LogStored: status != StatusQueued,
		})
	}
	_, err := tx.InsertRuns(ctx, rs)
	return err
}

func orEmptyJobs(j []PlannedJob) []PlannedJob {
	if j == nil {
		return []PlannedJob{}
	}
	return j
}

func orEmptySkipped(s []SkippedJob) []SkippedJob {
	if s == nil {
		return []SkippedJob{}
	}
	return s
}

// Verdict is whether a patchset's pre has passed.
type Verdict int

const (
	VerdictNone    Verdict = 1 // not planned (yet)
	VerdictPending Verdict = 2 // the plan or a job has not finished
	VerdictPassed  Verdict = 3 // the plan and the latest run of every job passed
	VerdictFailed  Verdict = 4 // the plan, or the latest run of a job, did not pass
)

// Pre is a patchset's pre: its plan, its planning run and the latest run of each job the plan
// selected (in the plan's order), and its verdict.
type Pre struct {
	Plan    *Plan // nil when the patchset has no plan
	PlanRun *Run
	Jobs    []*Run
	Verdict Verdict
}

// GetPre returns a patchset's pre.
func (s *Store) GetPre(ctx context.Context, repo, cl int64, patchset int32) (*Pre, error) {
	if repo <= 0 || cl <= 0 || patchset <= 0 {
		return nil, invalid("name the repository, CL, and patchset")
	}
	pre := &Pre{Verdict: VerdictNone}
	err := s.b.Snapshot(ctx, func(tx Ops) error {
		*pre = Pre{Verdict: VerdictNone}
		plan, err := tx.PrePlan(ctx, repo, cl, patchset)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		runs, err := tx.PlanRuns(ctx, []int64{plan.ID})
		if err != nil {
			return err
		}
		pre.Plan = plan
		pre.PlanRun, pre.Jobs = planRuns(runs)
		if pre.PlanRun == nil {
			return ErrNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if pre.Plan != nil {
		pre.Verdict = verdict(pre.PlanRun, pre.Jobs)
	}
	return pre, nil
}

// planRuns is, of a plan's runs in ID order, its latest planning run and the latest run of each
// of its jobs, in the order the plan queued them.
func planRuns(runs []*Run) (*Run, []*Run) {
	var plan *Run
	var jobs []*Run
	at := map[string]int{}
	for _, r := range runs {
		if r.Job.Kind == KindPlan {
			plan = r
			continue
		}
		if i, ok := at[r.Origin.Job]; ok {
			jobs[i] = r
			continue
		}
		at[r.Origin.Job] = len(jobs)
		jobs = append(jobs, r)
	}
	return plan, jobs
}

// verdict is a pre's verdict from its planning run and its jobs' latest runs: any that did not
// pass fails it, whatever else is still running.
func verdict(plan *Run, jobs []*Run) Verdict {
	switch {
	case !plan.Status.Finished():
		return VerdictPending
	case plan.Status != StatusPassed:
		return VerdictFailed
	}
	pending := false
	for _, j := range jobs {
		switch {
		case !j.Status.Finished():
			pending = true
		case j.Status != StatusPassed:
			return VerdictFailed
		}
	}
	if pending {
		return VerdictPending
	}
	return VerdictPassed
}

// orElse is s, or else when s is empty.
func orElse(s, otherwise string) string {
	if s == "" {
		return otherwise
	}
	return s
}
