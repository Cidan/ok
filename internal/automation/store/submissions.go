package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// SubmissionStatus is where a submission is.
type SubmissionStatus int16

const (
	SubmissionActive   SubmissionStatus = 1 // in the queue
	SubmissionLanded   SubmissionStatus = 2
	SubmissionFailed   SubmissionStatus = 3 // post failed, or the SCM refused it
	SubmissionCanceled SubmissionStatus = 4 // by a person, or by a new patchset or the CL's leaving review
)

// SubmittedCL is one CL of a submission: the patchset submitted, and the commit it landed as.
type SubmittedCL struct {
	Number       int64
	Patchset     int32
	LandedCommit []byte // nil until it lands
}

// Submission is what ok control handed automation to land: one CL, or a stack of them bottom
// first, on behalf of the person who submitted it (docs/automation.md, "The landing
// queue").
type Submission struct {
	ID           int64
	RepositoryID int64
	SubmittedBy  string
	Status       SubmissionStatus
	Reason       string // why it failed or was canceled
	Note         string // what the queue is waiting for, while it is active
	CLs          []SubmittedCL
	CreatedAt    time.Time
	FinishedAt   time.Time // zero while active
}

// AttemptStatus is where an attempt is.
type AttemptStatus int16

const (
	AttemptActive    AttemptStatus = 1
	AttemptDiscarded AttemptStatus = 2 // it no longer holds for main: the queue tries another
	AttemptLanded    AttemptStatus = 3 // its submission landed on it
	AttemptFailed    AttemptStatus = 4 // its failure failed its submission
)

// Attempt is a submission's candidates on one arrangement of main — main's head when it was
// prepared, and the earlier submissions it speculates land first (its chain) — with the post
// plan of each of its own.
type Attempt struct {
	ID           int64
	SubmissionID int64
	Base         []byte // main's head it was prepared on
	BaseSeq      int64  // that head's place in main's history
	Chain        []int64
	Status       AttemptStatus
	Released     bool   // its plans' jobs are queued
	PrepareError string // the SCM refused one of its candidates
	Reason       string // why it was discarded
	CreatedAt    time.Time
	FinishedAt   time.Time
	Candidates   []*AttemptCandidate // in landing order: its chain's, then its own
}

// Own returns the attempt's own candidates, those with post plans.
func (a *Attempt) Own() []*AttemptCandidate {
	var out []*AttemptCandidate
	for _, c := range a.Candidates {
		if c.PlanID > 0 {
			out = append(out, c)
		}
	}
	return out
}

// AttemptCandidate is one candidate of an attempt: a CL's patchset prepared on base.
type AttemptCandidate struct {
	SubmissionID int64 // whose CL it is
	CLNumber     int64
	Patchset     int32
	Commit       []byte
	Base         []byte
	PlanID       int64 // an own candidate's post plan; 0 for the chain's
	Plan         *Plan
	PlanRun      *Run   // the latest planning run
	Jobs         []*Run // the latest run of each of the plan's jobs
}

// postPriority is post's runs' priority: main waits on them.
const postPriority = 1

// maxSubmissionCLs bounds a submission's stack.
const maxSubmissionCLs = 100

// SubmitRequest is what ok control asks automation to land.
type SubmitRequest struct {
	RepositoryID int64
	CLs          []SubmittedCL // bottom first
	SubmittedBy  string
}

// Submit queues a submission. A CL already in an active submission is refused.
func (s *Store) Submit(ctx context.Context, req SubmitRequest) (*Submission, error) {
	switch {
	case req.RepositoryID <= 0:
		return nil, invalid("a repository is required")
	case len(req.CLs) == 0 || len(req.CLs) > maxSubmissionCLs:
		return nil, invalid("a submission names between 1 and %d CLs", maxSubmissionCLs)
	case req.SubmittedBy == "":
		return nil, invalid("a submission is submitted by someone")
	}
	numbers := make([]int64, len(req.CLs))
	seen := map[int64]bool{}
	for i, c := range req.CLs {
		if c.Number <= 0 || c.Patchset <= 0 {
			return nil, invalid("a CL and its patchset are positive")
		}
		if seen[c.Number] {
			return nil, invalid("CL %d is named twice", c.Number)
		}
		seen[c.Number] = true
		numbers[i] = c.Number
	}
	var id int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		locked := append([]int64(nil), numbers...)
		sort.Slice(locked, func(i, j int) bool { return locked[i] < locked[j] })
		for _, n := range locked {
			if err := lockCL(ctx, tx, req.RepositoryID, n); err != nil {
				return err
			}
		}
		var cl, other int64
		err := tx.QueryRow(ctx, `SELECT sc.cl_number, s.id FROM submission_cl sc JOIN submission s ON s.id = sc.submission_id
			WHERE sc.repository_id = $1 AND sc.cl_number = ANY($2) AND s.status = $3 ORDER BY s.id LIMIT 1`,
			req.RepositoryID, numbers, SubmissionActive).Scan(&cl, &other)
		if err == nil {
			return &ConflictError{Message: fmt.Sprintf("CL %d is already submitting (submission %d)", cl, other)}
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO submission (repository_id, submitted_by, status) VALUES ($1, $2, $3)
			RETURNING id`, req.RepositoryID, req.SubmittedBy, SubmissionActive).Scan(&id); err != nil {
			return err
		}
		for i, c := range req.CLs {
			if _, err := tx.Exec(ctx, `INSERT INTO submission_cl (submission_id, position, repository_id, cl_number, patchset)
				VALUES ($1, $2, $3, $4, $5)`, id, i, req.RepositoryID, c.Number, c.Patchset); err != nil {
				return err
			}
		}
		return notifyQueue(ctx, tx, req.RepositoryID)
	})
	if err != nil {
		return nil, err
	}
	return s.GetSubmission(ctx, id)
}

// notifyQueue wakes the landing queue for a repository, when tx commits.
func notifyQueue(ctx context.Context, tx pgx.Tx, repo int64) error {
	_, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, chanSubmission, strconv.FormatInt(repo, 10))
	return err
}

const submissionColumns = `id, repository_id, submitted_by, status, reason, note, created_at, finished_at`

// loadSubmissions returns the submissions a query selects (by submissionColumns), with their
// CLs, in the query's order.
func loadSubmissions(ctx context.Context, q querier, sql string, args ...any) ([]*Submission, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	var out []*Submission
	byID := map[int64]*Submission{}
	for rows.Next() {
		var sub Submission
		var finished *time.Time
		if err := rows.Scan(&sub.ID, &sub.RepositoryID, &sub.SubmittedBy, &sub.Status, &sub.Reason, &sub.Note,
			&sub.CreatedAt, &finished); err != nil {
			rows.Close()
			return nil, err
		}
		if finished != nil {
			sub.FinishedAt = *finished
		}
		out = append(out, &sub)
		byID[sub.ID] = &sub
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(out) == 0 {
		return out, err
	}
	ids := make([]int64, 0, len(out))
	for _, sub := range out {
		ids = append(ids, sub.ID)
	}
	rows, err = q.Query(ctx, `SELECT submission_id, cl_number, patchset, landed_commit FROM submission_cl
		WHERE submission_id = ANY($1) ORDER BY submission_id, position`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var c SubmittedCL
		if err := rows.Scan(&id, &c.Number, &c.Patchset, &c.LandedCommit); err != nil {
			return nil, err
		}
		byID[id].CLs = append(byID[id].CLs, c)
	}
	return out, rows.Err()
}

// GetSubmission returns a submission.
func (s *Store) GetSubmission(ctx context.Context, id int64) (*Submission, error) {
	subs, err := loadSubmissions(ctx, s.pool, `SELECT `+submissionColumns+` FROM submission WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	if len(subs) == 0 {
		return nil, ErrNotFound
	}
	return subs[0], nil
}

// GetSubmissions returns submissions by ID; an ID that names none is left out.
func (s *Store) GetSubmissions(ctx context.Context, ids []int64) (map[int64]*Submission, error) {
	subs, err := loadSubmissions(ctx, s.pool, `SELECT `+submissionColumns+` FROM submission WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	out := map[int64]*Submission{}
	for _, sub := range subs {
		out[sub.ID] = sub
	}
	return out, nil
}

// ListSubmissions returns the submissions of a repository's CL, newest first.
func (s *Store) ListSubmissions(ctx context.Context, repo, cl int64, limit int) ([]*Submission, error) {
	if repo <= 0 || cl <= 0 {
		return nil, invalid("name the repository and the CL")
	}
	switch {
	case limit <= 0:
		limit = 50
	case limit > 500:
		limit = 500
	}
	return loadSubmissions(ctx, s.pool, `SELECT `+submissionColumns+` FROM submission WHERE id IN (
			SELECT submission_id FROM submission_cl WHERE repository_id = $1 AND cl_number = $2)
		ORDER BY id DESC LIMIT $3`, repo, cl, limit)
}

// Attempts returns a submission's attempts, oldest first, with their candidates, the own
// candidates' post plans, and their runs.
func (s *Store) Attempts(ctx context.Context, submission int64) ([]*Attempt, error) {
	var out []*Attempt
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var err error
		out, err = loadAttempts(ctx, tx, `SELECT `+attemptColumns+` FROM attempt WHERE submission_id = $1 ORDER BY id`, submission)
		return err
	})
	return out, err
}

const attemptColumns = `id, submission_id, base, base_seq, chain, status, released, prepare_error, reason, created_at, finished_at`

// loadAttempts returns the attempts a query selects (by attemptColumns), in its order, with
// their candidates, plans, and runs.
func loadAttempts(ctx context.Context, q querier, sql string, args ...any) ([]*Attempt, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	var out []*Attempt
	byID := map[int64]*Attempt{}
	for rows.Next() {
		var a Attempt
		var finished *time.Time
		if err := rows.Scan(&a.ID, &a.SubmissionID, &a.Base, &a.BaseSeq, &a.Chain, &a.Status, &a.Released,
			&a.PrepareError, &a.Reason, &a.CreatedAt, &finished); err != nil {
			rows.Close()
			return nil, err
		}
		if finished != nil {
			a.FinishedAt = *finished
		}
		out = append(out, &a)
		byID[a.ID] = &a
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(out) == 0 {
		return out, err
	}
	ids := make([]int64, 0, len(out))
	for _, a := range out {
		ids = append(ids, a.ID)
	}
	rows, err = q.Query(ctx, `SELECT attempt_id, submission_id, cl_number, patchset, commit_id, base, coalesce(plan_id, 0)
		FROM attempt_candidate WHERE attempt_id = ANY($1) ORDER BY attempt_id, position`, ids)
	if err != nil {
		return nil, err
	}
	plans := map[int64]*AttemptCandidate{}
	var planIDs []int64
	for rows.Next() {
		var id int64
		var c AttemptCandidate
		if err := rows.Scan(&id, &c.SubmissionID, &c.CLNumber, &c.Patchset, &c.Commit, &c.Base, &c.PlanID); err != nil {
			rows.Close()
			return nil, err
		}
		byID[id].Candidates = append(byID[id].Candidates, &c)
		if c.PlanID > 0 {
			plans[c.PlanID] = &c
			planIDs = append(planIDs, c.PlanID)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(planIDs) == 0 {
		return out, err
	}
	rows, err = q.Query(ctx, `SELECT `+planColumns+` FROM plan WHERE id = ANY($1)`, planIDs)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		p, err := scanPlan(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		plans[p.ID].Plan = p
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Each plan's latest planning run, and the latest run of each job in the order its first
	// run was queued.
	rows, err = q.Query(ctx, `SELECT `+runColumns+` FROM run WHERE plan_id = ANY($1) ORDER BY id`, planIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	latest := map[int64]map[string]int{} // plan -> job -> index in Jobs
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		c := plans[r.PlanID]
		if r.Job.Kind == KindPlan {
			c.PlanRun = r
			continue
		}
		if latest[r.PlanID] == nil {
			latest[r.PlanID] = map[string]int{}
		}
		if i, ok := latest[r.PlanID][r.Origin.Job]; ok {
			c.Jobs[i] = r
			continue
		}
		latest[r.PlanID][r.Origin.Job] = len(c.Jobs)
		c.Jobs = append(c.Jobs, r)
	}
	return out, rows.Err()
}

// cancelAttemptRuns cancels the runs of attempts that have not finished.
func cancelAttemptRuns(ctx context.Context, tx pgx.Tx, attempts []int64, reason string) error {
	if len(attempts) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT r.id FROM run r JOIN attempt_candidate ac ON ac.plan_id = r.plan_id
		WHERE ac.attempt_id = ANY($1) AND r.status IN ($2, $3) ORDER BY r.id`, attempts, StatusQueued, StatusRunning)
	if err != nil {
		return err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if err := cancelRun(ctx, tx, id, reason); err != nil {
			return err
		}
	}
	return nil
}

// endAttempts ends a submission's active attempts with status, canceling their runs.
func endAttempts(ctx context.Context, tx pgx.Tx, submission int64, status AttemptStatus, reason string) error {
	rows, err := tx.Query(ctx, `UPDATE attempt SET status = $3, reason = $4, finished_at = now()
		WHERE submission_id = $1 AND status = $2 RETURNING id`, submission, AttemptActive, status, reason)
	if err != nil {
		return err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	return cancelAttemptRuns(ctx, tx, ids, reason)
}

// finishSubmission ends an active submission with status and reason, and its attempts; it
// reports whether the submission was active.
func finishSubmission(ctx context.Context, tx pgx.Tx, id int64, status SubmissionStatus, reason string) (bool, error) {
	var repo int64
	err := tx.QueryRow(ctx, `UPDATE submission SET status = $3, reason = $4, note = '', finished_at = now()
		WHERE id = $1 AND status = $2 RETURNING repository_id`, id, SubmissionActive, status, reason).Scan(&repo)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	attempt := AttemptDiscarded
	if status == SubmissionFailed {
		attempt = AttemptFailed
	}
	if err := endAttempts(ctx, tx, id, attempt, reason); err != nil {
		return false, err
	}
	return true, notifyQueue(ctx, tx, repo)
}

// CancelSubmission cancels an active submission: its runs stop, and nothing of it lands. A
// finished submission is returned as it is.
func (s *Store) CancelSubmission(ctx context.Context, id int64, reason string) (*Submission, error) {
	if reason == "" {
		reason = "canceled"
	}
	if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := finishSubmission(ctx, tx, id, SubmissionCanceled, reason)
		return err
	}); err != nil {
		return nil, err
	}
	return s.GetSubmission(ctx, id)
}

// CancelSubmissionsOf cancels the active submissions of a CL — those of a patchset below
// patchset, or every one when patchset is 0 — and returns how many.
func (s *Store) CancelSubmissionsOf(ctx context.Context, repo, cl int64, patchset int32, reason string) (int, error) {
	n := 0
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		n = 0
		rows, err := tx.Query(ctx, `SELECT s.id FROM submission s JOIN submission_cl sc ON sc.submission_id = s.id
			WHERE sc.repository_id = $1 AND sc.cl_number = $2 AND s.status = $3 AND ($4 = 0 OR sc.patchset < $4)
			ORDER BY s.id`, repo, cl, SubmissionActive, patchset)
		if err != nil {
			return err
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, id := range ids {
			ok, err := finishSubmission(ctx, tx, id, SubmissionCanceled, reason)
			if err != nil {
				return err
			}
			if ok {
				n++
			}
		}
		return nil
	})
	return n, err
}

// FailSubmission fails an active submission: nothing of it lands.
func (s *Store) FailSubmission(ctx context.Context, id int64, reason string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := finishSubmission(ctx, tx, id, SubmissionFailed, reason)
		return err
	})
}

// SetNote records what the queue is waiting for on an active submission.
func (s *Store) SetNote(ctx context.Context, id int64, note string) error {
	_, err := s.pool.Exec(ctx, `UPDATE submission SET note = $3 WHERE id = $1 AND status = $2 AND note <> $3`,
		id, SubmissionActive, note)
	return err
}

// NewCandidate is a candidate of an attempt the queue prepared. An own candidate carries its
// post plan's inputs: the workflow files' commit and the job context.
type NewCandidate struct {
	SubmissionID    int64
	CLNumber        int64
	Patchset        int32
	Commit          []byte
	Base            []byte
	Own             bool
	WorkflowsCommit []byte
	Context         []byte   // the change, as the job context holds it (workflow.Change), JSON
	Touched         []string // every path the change touches: its footprint
}

// NewAttempt is an attempt the queue prepared: its base, its chain, and the candidates the SCM
// prepared — all of them, or those it prepared before refusing one (PrepareError).
type NewAttempt struct {
	SubmissionID int64
	Base         []byte
	BaseSeq      int64
	Chain        []int64
	Candidates   []NewCandidate
	PrepareError string
	Why          string // what its planning runs say about it
}

// CreateAttempt records an attempt and queues the planning run of each of its own candidates'
// post plans. A submission no longer active is refused, and so is one with an active attempt:
// the queue discards a submission's attempt before it prepares another, so a second would be a
// second queue's, and run its post twice.
func (s *Store) CreateAttempt(ctx context.Context, na NewAttempt) (int64, error) {
	if len(na.Base) != 32 {
		return 0, invalid("a base is a 32-byte commit ID")
	}
	for _, c := range na.Candidates {
		if len(c.Commit) != 32 || len(c.Base) != 32 {
			return 0, invalid("a candidate and its base are 32-byte commit IDs")
		}
		if c.Own && (len(c.WorkflowsCommit) != 32 || !json.Valid(c.Context)) {
			return 0, invalid("an own candidate carries its workflows commit and its job context")
		}
	}
	var id int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var repo int64
		var status SubmissionStatus
		var submittedBy string
		err := tx.QueryRow(ctx, `SELECT repository_id, status, submitted_by FROM submission WHERE id = $1 FOR UPDATE`,
			na.SubmissionID).Scan(&repo, &status, &submittedBy)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if status != SubmissionActive {
			return &ConflictError{Message: fmt.Sprintf("submission %d is no longer active", na.SubmissionID)}
		}
		var active int64
		err = tx.QueryRow(ctx, `SELECT id FROM attempt WHERE submission_id = $1 AND status = $2 ORDER BY id LIMIT 1`,
			na.SubmissionID, AttemptActive).Scan(&active)
		if err == nil {
			return &ConflictError{Message: fmt.Sprintf("submission %d already has an active attempt, %d", na.SubmissionID, active)}
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		chain := na.Chain
		if chain == nil {
			chain = []int64{}
		}
		if err := tx.QueryRow(ctx, `INSERT INTO attempt (submission_id, base, base_seq, chain, status, prepare_error)
			VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`, na.SubmissionID, na.Base, na.BaseSeq, chain, AttemptActive,
			na.PrepareError).Scan(&id); err != nil {
			return err
		}
		queued := false
		for i, c := range na.Candidates {
			var planID *int64
			if c.Own {
				var p int64
				touched, err := json.Marshal(orEmpty(c.Touched))
				if err != nil {
					return err
				}
				// The planning run compares the candidate with the commit the patchset's pre ran on.
				if err := tx.QueryRow(ctx, `INSERT INTO plan (repository_id, cl_number, patchset, phase, commit_id,
						workflows_commit, context, touched, pre_commit) VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
						(SELECT commit_id FROM plan WHERE repository_id = $1 AND cl_number = $2 AND patchset = $3 AND phase = $9))
					RETURNING id`,
					repo, c.CLNumber, c.Patchset, PhasePost, c.Commit, c.WorkflowsCommit, c.Context, touched, PhasePre).Scan(&p); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `INSERT INTO run (repository_id, commit_id, kind, targets, args, phase,
						cl_number, patchset, job, plan_id, why, requested_by, status, timeout_seconds, priority)
					VALUES ($1, $2, $3, '{}', '{}', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
					repo, c.Commit, KindPlan, PhasePost, c.CLNumber, c.Patchset, planJob, p, na.Why, submittedBy,
					StatusQueued, int32(s.cfg.DefaultTimeout/time.Second), postPriority); err != nil {
					return err
				}
				planID, queued = &p, true
			}
			if _, err := tx.Exec(ctx, `INSERT INTO attempt_candidate (attempt_id, position, repository_id, submission_id,
					cl_number, patchset, commit_id, base, plan_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				id, i, repo, c.SubmissionID, c.CLNumber, c.Patchset, c.Commit, c.Base, planID); err != nil {
				return err
			}
		}
		if queued {
			if _, err := tx.Exec(ctx, `SELECT pg_notify($1, '')`, chanQueue); err != nil {
				return err
			}
		}
		return notifyQueue(ctx, tx, repo)
	})
	return id, err
}

// ReleaseAttempt queues the jobs of an attempt's post plans: the queue decided the attempt is
// the one to test. Every one of its plans must be planned. A job nothing changed for since pre,
// which passed there, is recorded passed at once instead, carrying that pass.
func (s *Store) ReleaseAttempt(ctx context.Context, id int64) error {
	var carried []int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		carried = nil
		var status AttemptStatus
		var released bool
		var submission int64
		err := tx.QueryRow(ctx, `SELECT status, released, submission_id FROM attempt WHERE id = $1 FOR UPDATE`, id).
			Scan(&status, &released, &submission)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if status != AttemptActive || released {
			return &ConflictError{Message: fmt.Sprintf("attempt %d is not waiting to run", id)}
		}
		var submittedBy string
		var repo int64
		if err := tx.QueryRow(ctx, `SELECT submitted_by, repository_id FROM submission WHERE id = $1`, submission).
			Scan(&submittedBy, &repo); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT `+planColumns+` FROM plan WHERE id IN (
				SELECT plan_id FROM attempt_candidate WHERE attempt_id = $1 AND plan_id IS NOT NULL)
			ORDER BY id`, id)
		if err != nil {
			return err
		}
		var plans []*Plan
		for rows.Next() {
			p, err := scanPlan(rows)
			if err != nil {
				rows.Close()
				return err
			}
			plans = append(plans, p)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, p := range plans {
			if !p.Planned {
				return &ConflictError{Message: fmt.Sprintf("attempt %d's plan for CL %d is not planned", id, p.CLNumber)}
			}
		}
		for _, p := range plans {
			c, err := s.releasePlan(ctx, tx, repo, p, submittedBy)
			if err != nil {
				return err
			}
			carried = append(carried, c...)
		}
		if _, err := tx.Exec(ctx, `UPDATE attempt SET released = true WHERE id = $1`, id); err != nil {
			return err
		}
		return notifyQueue(ctx, tx, repo)
	})
	if err != nil {
		return err
	}
	// The carried runs' logs move to the bucket as any finished run's; a failed move is the
	// sweep's to retry.
	var errs []error
	for _, r := range carried {
		errs = append(errs, s.storeLog(ctx, r))
	}
	return errors.Join(errs...)
}

// releasePlan queues a post plan's jobs on its candidate, but records each job that is unchanged
// since pre and passed there as passed at once, carrying that pass (docs/automation.md,
// "post"). It returns the runs it carried.
func (s *Store) releasePlan(ctx context.Context, tx pgx.Tx, repo int64, p *Plan, requestedBy string) ([]int64, error) {
	origin := Origin{Phase: PhasePost, CLNumber: p.CLNumber, Patchset: p.Patchset}
	passes, err := prePasses(ctx, tx, repo, p.CLNumber, p.Patchset)
	if err != nil {
		return nil, err
	}
	var queued []PlannedJob
	var carried []int64
	for _, j := range p.Jobs {
		from := passes.of(j)
		if !j.UnchangedSincePre || from == nil {
			queued = append(queued, j)
			continue
		}
		note := fmt.Sprintf("ok: not run: it passed in pre as run %d, on patchset %d's commit %.10x, and nothing it reads differs on this candidate\n",
			from.ID, p.Patchset, from.CommitID)
		var run int64
		if err := tx.QueryRow(ctx, `INSERT INTO run (repository_id, commit_id, kind, targets, args, phase, cl_number, patchset,
				job, plan_id, why, requested_by, status, timeout_seconds, priority, started_at, finished_at, carried_from, log_size)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, now(), now(), $16, $17) RETURNING id`,
			repo, p.CommitID, j.Spec.Kind, orEmpty(j.Spec.Targets), orEmpty(j.Spec.Args), origin.Phase, origin.CLNumber, origin.Patchset,
			j.Job, p.ID, j.Why, requestedBy, StatusPassed, int32(s.cfg.DefaultTimeout/time.Second), postPriority, from.ID, len(note)).Scan(&run); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO log_chunk (run_id, at, data) VALUES ($1, 0, $2)`, run, []byte(note)); err != nil {
			return nil, err
		}
		if _, err := notify(ctx, tx, chanDone, run); err != nil {
			return nil, err
		}
		carried = append(carried, run)
	}
	return carried, s.insertJobs(ctx, tx, repo, p.CommitID, origin, p.ID, requestedBy, queued, postPriority, StatusQueued, "")
}

// passes are the jobs of a patchset's pre whose latest run passed.
type passes struct {
	byJob map[string]*Run
	all   []*Run
}

// prePasses returns the passes of a patchset's pre.
func prePasses(ctx context.Context, tx pgx.Tx, repo, cl int64, patchset int32) (*passes, error) {
	rows, err := tx.Query(ctx, `SELECT `+runColumns+` FROM (
			SELECT DISTINCT ON (job) * FROM run
			WHERE plan_id = (SELECT id FROM plan WHERE repository_id = $1 AND cl_number = $2 AND patchset = $3 AND phase = $4)
				AND kind <> $5
			ORDER BY job, id DESC
		) latest WHERE status = $6 ORDER BY id`, repo, cl, patchset, PhasePre, KindPlan, StatusPassed)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := &passes{byJob: map[string]*Run{}}
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out.byJob[r.Origin.Job] = r
		out.all = append(out.all, r)
	}
	return out, rows.Err()
}

// of returns the pre pass a post job carries: the same job's — a built-in job's in post is the
// same as in pre, whatever its targets, which its plan proved built and linted there — or, for a
// workflow job, that of any job that ran the same Bazel command.
func (p *passes) of(j PlannedJob) *Run {
	if r := p.byJob[j.Job]; r != nil && r.Job.Kind == j.Spec.Kind {
		return r
	}
	for _, r := range p.all {
		if sameJob(r.Job, j.Spec) {
			return r
		}
	}
	return nil
}

// sameJob reports whether two jobs run the same Bazel commands: the same kind, targets, and
// arguments.
func sameJob(a, b Job) bool {
	return a.Kind == b.Kind && slices.Equal(slices.Sorted(slices.Values(a.Targets)), slices.Sorted(slices.Values(b.Targets))) &&
		slices.Equal(a.Args, b.Args)
}

// DiscardAttempt ends an active attempt that no longer holds for main, canceling its runs.
func (s *Store) DiscardAttempt(ctx context.Context, id int64, reason string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE attempt SET status = $3, reason = $4, finished_at = now()
			WHERE id = $1 AND status = $2`, id, AttemptActive, AttemptDiscarded, reason)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		return cancelAttemptRuns(ctx, tx, []int64{id}, reason)
	})
}

// LandedCL is one CL a landing landed, and the commit it landed as.
type LandedCL struct {
	CLNumber int64
	Commit   []byte
}

// RecordLanding records that a submission landed on an attempt: the commit each CL landed as,
// and — at fromSeq, main's place before the landing, when it is known — main's history.
func (s *Store) RecordLanding(ctx context.Context, submission, attempt int64, landed []LandedCL, fromSeq int64) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var repo int64
		err := tx.QueryRow(ctx, `UPDATE submission SET status = $3, reason = '', note = '', finished_at = now()
			WHERE id = $1 AND status = $2 RETURNING repository_id`, submission, SubmissionActive, SubmissionLanded).Scan(&repo)
		if errors.Is(err, pgx.ErrNoRows) {
			return &ConflictError{Message: fmt.Sprintf("submission %d is no longer active", submission)}
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE attempt SET status = $3, finished_at = now() WHERE id = $1 AND submission_id = $2`,
			attempt, submission, AttemptLanded); err != nil {
			return err
		}
		if err := endAttempts(ctx, tx, submission, AttemptDiscarded, "another attempt landed"); err != nil {
			return err
		}
		for i, l := range landed {
			if _, err := tx.Exec(ctx, `UPDATE submission_cl SET landed_commit = $3 WHERE submission_id = $1 AND cl_number = $2`,
				submission, l.CLNumber, l.Commit); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO landed_change (repository_id, commit_id, cl_number) VALUES ($1, $2, $3)
				ON CONFLICT DO NOTHING`, repo, l.Commit, l.CLNumber); err != nil {
				return err
			}
			if fromSeq > 0 {
				if _, err := tx.Exec(ctx, `INSERT INTO main_commit (repository_id, seq, commit_id, submission_id)
					VALUES ($1, $2, $3, $4) ON CONFLICT (repository_id, seq) DO UPDATE SET submission_id = EXCLUDED.submission_id`,
					repo, fromSeq+int64(i)+1, l.Commit, submission); err != nil {
					return err
				}
				continue
			}
			if _, err := tx.Exec(ctx, `UPDATE main_commit SET submission_id = $3 WHERE repository_id = $1 AND commit_id = $2`,
				repo, l.Commit, submission); err != nil {
				return err
			}
		}
		return notifyQueue(ctx, tx, repo)
	})
}

// RecordMainCommit records a commit main moved to, at its place in main's history.
func (s *Store) RecordMainCommit(ctx context.Context, repo, seq int64, commit []byte) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO main_commit (repository_id, seq, commit_id) VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING`, repo, seq, commit); err != nil {
			return err
		}
		return notifyQueue(ctx, tx, repo)
	})
}

// RecordLandedChange records the CL a landed commit is.
func (s *Store) RecordLandedChange(ctx context.Context, repo int64, commit []byte, cl int64) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO landed_change (repository_id, commit_id, cl_number) VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING`, repo, commit, cl)
	return err
}

// ReusableCandidate returns a candidate an active attempt already prepared from a CL's
// patchset on base, if there is one.
func (s *Store) ReusableCandidate(ctx context.Context, repo int64, base []byte, cl int64, patchset int32) ([]byte, bool, error) {
	var commit []byte
	err := s.pool.QueryRow(ctx, `SELECT ac.commit_id FROM attempt_candidate ac JOIN attempt a ON a.id = ac.attempt_id
		WHERE ac.repository_id = $1 AND ac.base = $2 AND ac.cl_number = $3 AND ac.patchset = $4 AND a.status = $5
		ORDER BY a.id DESC LIMIT 1`, repo, base, cl, patchset, AttemptActive).Scan(&commit)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	return commit, err == nil, err
}

// CandidatesInUse returns the candidates a repository's active attempts hold, by commit ID.
func (s *Store) CandidatesInUse(ctx context.Context, repo int64) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `SELECT ac.commit_id FROM attempt_candidate ac JOIN attempt a ON a.id = ac.attempt_id
		WHERE ac.repository_id = $1 AND a.status = $2`, repo, AttemptActive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var c []byte
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out[string(c)] = true
	}
	return out, rows.Err()
}

// QueueRepositories returns the repositories the landing queue has work in: those with active
// submissions, and those where a submission finished within recent, whose candidates it
// cleans up.
func (s *Store) QueueRepositories(ctx context.Context, recent time.Duration) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT repository_id FROM submission
		WHERE status = $1 OR finished_at > now() - make_interval(secs => $2) ORDER BY repository_id`,
		SubmissionActive, recent.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// MainCommit is one commit main moved to: its place in main's history, the CL it is (0 when
// automation has not seen it land), and the submission that landed it (0 when the queue did not).
type MainCommit struct {
	Seq          int64
	Commit       []byte
	CLNumber     int64
	SubmissionID int64
}

// QueuedSubmission is an active submission with every one of its attempts.
type QueuedSubmission struct {
	*Submission
	Attempts []*Attempt // oldest first
}

// QueueState is what the landing queue decides a repository's next steps from.
type QueueState struct {
	// Submissions are the repository's active submissions, in queue order.
	Submissions []*QueuedSubmission
	// Others are the submissions the active ones' attempts speculate on, or that landed Main's
	// commits, that are no longer active.
	Others map[int64]*Submission
	// Main is main's history after From, as automation has seen it.
	From int64
	Main []MainCommit
	// Landings are the attempts Main's submissions landed on, by submission.
	Landings map[int64]*Attempt
	// LandedCLs are the active submissions' CLs that have landed, and the commit each landed as.
	LandedCLs map[int64][]byte
}

// QueueState reads a repository's landing queue, consistently. from bounds main's history from
// below when no active attempt names an earlier base.
func (s *Store) QueueState(ctx context.Context, repo, from int64) (*QueueState, error) {
	st := &QueueState{Others: map[int64]*Submission{}, Landings: map[int64]*Attempt{}, LandedCLs: map[int64][]byte{}}
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		*st = QueueState{Others: map[int64]*Submission{}, Landings: map[int64]*Attempt{}, LandedCLs: map[int64][]byte{}}
		subs, err := loadSubmissions(ctx, tx, `SELECT `+submissionColumns+` FROM submission
			WHERE repository_id = $1 AND status = $2 ORDER BY id`, repo, SubmissionActive)
		if err != nil {
			return err
		}
		var ids, cls []int64
		byID := map[int64]*QueuedSubmission{}
		for _, sub := range subs {
			q := &QueuedSubmission{Submission: sub}
			st.Submissions = append(st.Submissions, q)
			byID[sub.ID] = q
			ids = append(ids, sub.ID)
			for _, c := range sub.CLs {
				cls = append(cls, c.Number)
			}
		}
		attempts, err := loadAttempts(ctx, tx, `SELECT `+attemptColumns+` FROM attempt WHERE submission_id = ANY($1) ORDER BY id`, ids)
		if err != nil {
			return err
		}
		st.From = from
		named := map[int64]bool{}
		for _, a := range attempts {
			byID[a.SubmissionID].Attempts = append(byID[a.SubmissionID].Attempts, a)
			if a.Status == AttemptActive {
				st.From = min(st.From, a.BaseSeq)
				for _, c := range a.Chain {
					named[c] = true
				}
			}
		}
		rows, err := tx.Query(ctx, `SELECT mc.seq, mc.commit_id, coalesce(lc.cl_number, 0), coalesce(mc.submission_id, 0)
			FROM main_commit mc LEFT JOIN landed_change lc ON lc.repository_id = mc.repository_id AND lc.commit_id = mc.commit_id
			WHERE mc.repository_id = $1 AND mc.seq > $2 ORDER BY mc.seq`, repo, st.From)
		if err != nil {
			return err
		}
		for rows.Next() {
			var m MainCommit
			if err := rows.Scan(&m.Seq, &m.Commit, &m.CLNumber, &m.SubmissionID); err != nil {
				rows.Close()
				return err
			}
			st.Main = append(st.Main, m)
			if m.SubmissionID > 0 {
				named[m.SubmissionID] = true
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		var others []int64
		for id := range named {
			if byID[id] == nil {
				others = append(others, id)
			}
		}
		if len(others) > 0 {
			subs, err := loadSubmissions(ctx, tx, `SELECT `+submissionColumns+` FROM submission WHERE id = ANY($1)`, others)
			if err != nil {
				return err
			}
			for _, sub := range subs {
				st.Others[sub.ID] = sub
			}
			landings, err := loadAttempts(ctx, tx, `SELECT `+attemptColumns+` FROM attempt
				WHERE submission_id = ANY($1) AND status = $2 ORDER BY id`, others, AttemptLanded)
			if err != nil {
				return err
			}
			for _, a := range landings {
				st.Landings[a.SubmissionID] = a
			}
		}
		rows, err = tx.Query(ctx, `SELECT cl_number, commit_id FROM landed_change WHERE repository_id = $1 AND cl_number = ANY($2)`,
			repo, cls)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var cl int64
			var commit []byte
			if err := rows.Scan(&cl, &commit); err != nil {
				return err
			}
			st.LandedCLs[cl] = commit
		}
		return rows.Err()
	})
	return st, err
}

// WatchQueue starts watching for what the landing queue acts on — a submission's change, a
// post plan's, a run's end, a commit on main — and returns the wait: it returns once one came
// after WatchQueue was called, max passes, or ctx ends.
func (s *Store) WatchQueue() func(ctx context.Context, max time.Duration) error {
	changed, done := s.notify.watch(chanSubmission, ""), s.notify.watch(chanDone, "")
	return func(ctx context.Context, max time.Duration) error {
		return s.notify.waitAny(ctx, max, changed, done)
	}
}
