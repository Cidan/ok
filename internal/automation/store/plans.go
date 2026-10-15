package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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
}

// planJob is a planning run's job identity.
const planJob = "plan"

// lockCL serializes, within a transaction, what plans and cancels a CL's runs: a new
// patchset's plan, and a planning run's queuing of its jobs.
func lockCL(ctx context.Context, tx pgx.Tx, repo, cl int64) error {
	h := fnv.New64a()
	fmt.Fprintf(h, "ok-automation plan %d/%d", repo, cl)
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(h.Sum64()))
	return err
}

const planColumns = `id, repository_id, cl_number, patchset, phase, commit_id, workflows_commit, context, touched,
	planned, jobs, skipped, affected, upstream, created_at, pre_commit`

func scanPlan(row pgx.Row) (*Plan, error) {
	var p Plan
	var touched, jobs, skipped, affected, upstream []byte
	err := row.Scan(&p.ID, &p.RepositoryID, &p.CLNumber, &p.Patchset, &p.Phase, &p.CommitID, &p.WorkflowsCommit,
		&p.Context, &touched, &p.Planned, &jobs, &skipped, &affected, &upstream, &p.CreatedAt, &p.PreCommit)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	for _, f := range []struct {
		data []byte
		into any
		what string
	}{{touched, &p.Touched, "touched paths"}, {jobs, &p.Jobs, "jobs"}, {skipped, &p.Skipped, "skipped jobs"}, {affected, &p.Affected, "affected targets"}, {upstream, &p.Upstream, "upstream targets"}} {
		if err := json.Unmarshal(f.data, f.into); err != nil {
			return nil, fmt.Errorf("plan %d's %s: %w", p.ID, f.what, err)
		}
	}
	return &p, nil
}

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
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockCL(ctx, tx, req.RepositoryID, req.CLNumber); err != nil {
			return err
		}
		plan, err = scanPlan(tx.QueryRow(ctx, `INSERT INTO plan (repository_id, cl_number, patchset, phase, commit_id,
				workflows_commit, context)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (repository_id, cl_number, patchset) WHERE phase = 1 DO NOTHING
			RETURNING `+planColumns,
			req.RepositoryID, req.CLNumber, req.Patchset, PhasePre, req.CommitID, req.WorkflowsCommit, req.Context))
		if errors.Is(err, ErrNotFound) {
			plan, err = scanPlan(tx.QueryRow(ctx, `SELECT `+planColumns+` FROM plan
				WHERE repository_id = $1 AND cl_number = $2 AND patchset = $3 AND phase = $4`,
				req.RepositoryID, req.CLNumber, req.Patchset, PhasePre))
			return err
		}
		if err != nil {
			return err
		}
		created = true
		if _, err := tx.Exec(ctx, `INSERT INTO run (repository_id, commit_id, kind, targets, args, phase,
				cl_number, patchset, job, plan_id, why, requested_by, status, timeout_seconds)
			VALUES ($1, $2, $3, '{}', '{}', $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			req.RepositoryID, req.CommitID, KindPlan, PhasePre, req.CLNumber, req.Patchset, planJob, plan.ID,
			fmt.Sprintf("patchset %d was uploaded", req.Patchset), req.RequestedBy, StatusQueued,
			int32(s.cfg.DefaultTimeout/time.Second)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, '')`, chanQueue); err != nil {
			return err
		}
		return s.cancelPre(ctx, tx, req.RepositoryID, req.CLNumber, req.Patchset, fmt.Sprintf("patchset %d superseded it", req.Patchset))
	})
	return plan, created, err
}

// EndPre cancels every pre run of a CL that has not finished: the CL landed, or was abandoned,
// and no pre of it can matter now.
func (s *Store) EndPre(ctx context.Context, repo, cl int64, reason string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockCL(ctx, tx, repo, cl); err != nil {
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
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockCL(ctx, tx, repo, cl); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE run SET stop_reason = NULL
			WHERE repository_id = $1 AND cl_number = $2 AND patchset = $3 AND phase = $4 AND status = $5`,
			repo, cl, patchset, PhasePre, StatusRunning); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `INSERT INTO run (repository_id, commit_id, kind, targets, args, phase, cl_number,
				patchset, job, plan_id, why, requested_by, rerun_of, status, timeout_seconds)
			SELECT repository_id, commit_id, kind, targets, args, phase, cl_number, patchset, job, plan_id, why,
				$5, id, $6, timeout_seconds
			FROM (SELECT DISTINCT ON (job) * FROM run
				WHERE plan_id = (SELECT id FROM plan WHERE repository_id = $1 AND cl_number = $2 AND patchset = $3 AND phase = $4)
				ORDER BY job, id DESC) latest
			WHERE status = $7`, repo, cl, patchset, PhasePre, requestedBy, StatusQueued, StatusCanceled)
		if err != nil {
			return err
		}
		n = int(tag.RowsAffected())
		if n > 0 {
			_, err = tx.Exec(ctx, `SELECT pg_notify($1, '')`, chanQueue)
		}
		return err
	})
	return n, err
}

// cancelPre cancels the pre runs of a CL's patchsets before below that have not finished: a
// queued run ends at once, and a running one's worker is told to stop it.
func (s *Store) cancelPre(ctx context.Context, tx pgx.Tx, repo, cl int64, below int32, reason string) error {
	rows, err := tx.Query(ctx, `UPDATE run SET status = $5, reason = $6, finished_at = now(), log_stored = true
		WHERE repository_id = $1 AND cl_number = $2 AND phase = $3 AND patchset < $4 AND status = $7
		RETURNING id`, repo, cl, PhasePre, below, StatusCanceled, reason, StatusQueued)
	if err != nil {
		return err
	}
	var ended []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ended = append(ended, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ended {
		if _, err := notify(ctx, tx, chanDone, id); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE run SET stop_reason = $5
		WHERE repository_id = $1 AND cl_number = $2 AND phase = $3 AND patchset < $4 AND status = $6
			AND stop_reason IS NULL`, repo, cl, PhasePre, below, reason, StatusRunning)
	return err
}

// GetPlan returns a plan.
func (s *Store) GetPlan(ctx context.Context, id int64) (*Plan, error) {
	return scanPlan(s.pool.QueryRow(ctx, `SELECT `+planColumns+` FROM plan WHERE id = $1`, id))
}

// queuePlan records what a planning run found, within the transaction that records the run
// passed: the plan's jobs, the workflow jobs it leaves to main, and the change's footprint.
// A pre plan's jobs are queued at once — recorded canceled when a newer patchset has
// superseded the plan, since they would test a patchset no one can submit. A post plan's wait
// for the landing queue to release its attempt.
func (s *Store) queuePlan(ctx context.Context, tx pgx.Tx, run *Run, result *PlanResult) error {
	if err := lockCL(ctx, tx, run.RepositoryID, run.Origin.CLNumber); err != nil {
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
	var planned bool
	if err := tx.QueryRow(ctx, `SELECT planned FROM plan WHERE id = $1 FOR UPDATE`, run.PlanID).Scan(&planned); err != nil {
		return err
	}
	if planned {
		return &ConflictError{Message: "the patchset is planned already"}
	}
	enc := func(v any) ([]byte, error) { return json.Marshal(v) }
	jobs, err := enc(orEmptyJobs(result.Jobs))
	if err != nil {
		return err
	}
	skipped, err := enc(orEmptySkipped(result.Skipped))
	if err != nil {
		return err
	}
	affected, err := enc(orEmpty(result.Affected))
	if err != nil {
		return err
	}
	upstream, err := enc(orEmpty(result.Upstream))
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE plan SET planned = true, jobs = $2, skipped = $3, affected = $4, upstream = $5
		WHERE id = $1`, run.PlanID, jobs, skipped, affected, upstream); err != nil {
		return err
	}
	if run.Origin.Phase == PhasePost {
		// The landing queue decides when the attempt runs; a released attempt's plans are all
		// planned, so this plan's is not released yet.
		_, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, chanSubmission, strconv.FormatInt(run.RepositoryID, 10))
		return err
	}
	var newer int32
	if err := tx.QueryRow(ctx, `SELECT coalesce(max(patchset), 0) FROM plan
		WHERE repository_id = $1 AND cl_number = $2 AND phase = $3`,
		run.RepositoryID, run.Origin.CLNumber, PhasePre).Scan(&newer); err != nil {
		return err
	}
	status, reason := StatusQueued, ""
	if newer > run.Origin.Patchset {
		status, reason = StatusCanceled, fmt.Sprintf("patchset %d superseded it", newer)
	}
	return s.insertJobs(ctx, tx, run.RepositoryID, run.CommitID, run.Origin, run.PlanID, run.RequestedBy, result.Jobs, 0, status, reason)
}

// insertJobs queues a plan's jobs as runs on its commit — or records them ended, canceled for
// reason, when status says so.
func (s *Store) insertJobs(ctx context.Context, tx pgx.Tx, repo int64, commit []byte, origin Origin, planID int64,
	requestedBy string, jobs []PlannedJob, priority int16, status Status, reason string) error {
	if len(jobs) == 0 {
		return nil
	}
	var finished any
	if status != StatusQueued {
		finished = time.Now()
	}
	batch := &pgx.Batch{}
	for _, j := range jobs {
		batch.Queue(`INSERT INTO run (repository_id, commit_id, kind, targets, args, phase, cl_number, patchset,
				job, plan_id, why, requested_by, status, reason, timeout_seconds, finished_at, log_stored, priority)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
			repo, commit, j.Spec.Kind, orEmpty(j.Spec.Targets), orEmpty(j.Spec.Args),
			origin.Phase, origin.CLNumber, origin.Patchset, j.Job, planID, j.Why, requestedBy, status, reason,
			int32(s.cfg.DefaultTimeout/time.Second), finished, status != StatusQueued, priority)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return err
	}
	if status == StatusQueued {
		_, err := tx.Exec(ctx, `SELECT pg_notify($1, '')`, chanQueue)
		return err
	}
	return nil
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
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		plan, err := scanPlan(tx.QueryRow(ctx, `SELECT `+planColumns+` FROM plan
			WHERE repository_id = $1 AND cl_number = $2 AND patchset = $3 AND phase = $4`, repo, cl, patchset, PhasePre))
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		pre.Plan = plan
		pre.PlanRun, pre.Jobs, err = planRuns(ctx, tx, plan.ID)
		return err
	})
	if err != nil {
		return nil, err
	}
	if pre.Plan != nil {
		pre.Verdict = verdict(pre.PlanRun, pre.Jobs)
	}
	return pre, nil
}

// planRuns returns a plan's latest planning run and the latest run of each of its jobs, in the
// order the plan queued them.
func planRuns(ctx context.Context, q querier, planID int64) (*Run, []*Run, error) {
	planRun, err := scanRun(q.QueryRow(ctx, `SELECT `+runColumns+` FROM run
		WHERE plan_id = $1 AND kind = $2 ORDER BY id DESC LIMIT 1`, planID, KindPlan))
	if err != nil {
		return nil, nil, err
	}
	rows, err := q.Query(ctx, `SELECT `+runColumns+` FROM (
			SELECT DISTINCT ON (job) *, min(id) OVER (PARTITION BY job) AS first
			FROM run WHERE plan_id = $1 AND kind <> $2 ORDER BY job, id DESC
		) latest ORDER BY first`, planID, KindPlan)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var jobs []*Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, nil, err
		}
		jobs = append(jobs, r)
	}
	return planRun, jobs, rows.Err()
}

// querier is a pool or a transaction.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
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
