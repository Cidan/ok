package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/Cidan/ok/internal/work"
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
	err := s.b.Txn(ctx, func(tx Ops) error {
		locked := append([]int64(nil), numbers...)
		sort.Slice(locked, func(i, j int) bool { return locked[i] < locked[j] })
		for _, n := range locked {
			if err := tx.LockCL(ctx, req.RepositoryID, n); err != nil {
				return err
			}
		}
		cl, other, ok, err := tx.ActiveSubmissionOf(ctx, req.RepositoryID, numbers)
		if err != nil {
			return err
		}
		if ok {
			return &ConflictError{Message: fmt.Sprintf("CL %d is already submitting (submission %d)", cl, other)}
		}
		if id, err = tx.InsertSubmission(ctx, req.RepositoryID, req.SubmittedBy, req.CLs); err != nil {
			return err
		}
		return notifyQueue(ctx, tx, req.RepositoryID)
	})
	if err != nil {
		return nil, err
	}
	return s.GetSubmission(ctx, id)
}

// notifyQueue wakes the landing queue for a repository, when tx commits.
func notifyQueue(ctx context.Context, tx Ops, repo int64) error {
	if err := tx.MarkWork(ctx, QueueWork, repo); err != nil {
		return err
	}
	return notify(ctx, tx, chanSubmission, repo)
}

// QueueWork is a repository's landing queue, as keyed work: marked whenever what the queue acts on
// changes, and done by one replica at a time.
const QueueWork = "queue"

// Work is automation's keyed work's store (internal/work).
func (s *Store) Work() work.Store { return workStore{s.b} }

type workStore struct{ b Backend }

func (w workStore) Mark(ctx context.Context, kind string, key int64) error {
	return w.b.MarkWork(ctx, kind, key)
}
func (w workStore) Take(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	return w.b.TakeWork(ctx, kind, key, holder, ttl)
}
func (w workStore) Renew(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	return w.b.RenewWork(ctx, kind, key, holder, ttl)
}
func (w workStore) Release(ctx context.Context, kind string, key int64, holder string) (bool, error) {
	return w.b.ReleaseWork(ctx, kind, key, holder)
}
func (w workStore) Due(ctx context.Context, kind string, limit int) ([]int64, error) {
	return w.b.DueWork(ctx, kind, limit)
}

// GetSubmission returns a submission.
func (s *Store) GetSubmission(ctx context.Context, id int64) (*Submission, error) {
	subs, err := s.b.Submissions(ctx, []int64{id})
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
	subs, err := s.b.Submissions(ctx, ids)
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
	return s.b.SubmissionsOfCL(ctx, repo, cl, limit)
}

// Attempts returns a submission's attempts, oldest first, with their candidates, the own
// candidates' post plans, and their runs.
func (s *Store) Attempts(ctx context.Context, submission int64) ([]*Attempt, error) {
	var out []*Attempt
	err := s.b.Snapshot(ctx, func(tx Ops) error {
		var err error
		if out, err = tx.Attempts(ctx, []int64{submission}, 0); err != nil {
			return err
		}
		return loadPlans(ctx, tx, out)
	})
	return out, err
}

// loadPlans gives attempts' own candidates their post plans, each plan's latest planning run,
// and the latest run of each of its jobs, in the order its first run was queued.
func loadPlans(ctx context.Context, tx Ops, attempts []*Attempt) error {
	byPlan := map[int64]*AttemptCandidate{}
	var ids []int64
	for _, a := range attempts {
		for _, c := range a.Candidates {
			if c.PlanID > 0 {
				byPlan[c.PlanID] = c
				ids = append(ids, c.PlanID)
			}
		}
	}
	if len(ids) == 0 {
		return nil
	}
	plans, err := tx.Plans(ctx, ids)
	if err != nil {
		return err
	}
	for _, p := range plans {
		byPlan[p.ID].Plan = p
	}
	runs, err := tx.PlanRuns(ctx, ids)
	if err != nil {
		return err
	}
	of := map[int64][]*Run{}
	for _, r := range runs {
		of[r.PlanID] = append(of[r.PlanID], r)
	}
	for id, c := range byPlan {
		c.PlanRun, c.Jobs = planRuns(of[id])
	}
	return nil
}

// cancelAttemptRuns cancels the runs of attempts that have not finished.
func cancelAttemptRuns(ctx context.Context, tx Ops, attempts []int64, reason string) error {
	if len(attempts) == 0 {
		return nil
	}
	ids, err := tx.UnfinishedRunsOfAttempts(ctx, attempts)
	if err != nil {
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
func endAttempts(ctx context.Context, tx Ops, submission int64, status AttemptStatus, reason string) error {
	ids, err := tx.EndActiveAttempts(ctx, submission, status, reason)
	if err != nil {
		return err
	}
	return cancelAttemptRuns(ctx, tx, ids, reason)
}

// finishSubmission ends an active submission with status and reason, and its attempts; it
// reports whether the submission was active.
func finishSubmission(ctx context.Context, tx Ops, id int64, status SubmissionStatus, reason string) (bool, error) {
	repo, ok, err := tx.FinishSubmission(ctx, id, status, reason)
	if err != nil || !ok {
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
	if err := s.b.Txn(ctx, func(tx Ops) error {
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
	err := s.b.Txn(ctx, func(tx Ops) error {
		n = 0
		ids, err := tx.ActiveSubmissionsOfCL(ctx, repo, cl, patchset)
		if err != nil {
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
	return s.b.Txn(ctx, func(tx Ops) error {
		_, err := finishSubmission(ctx, tx, id, SubmissionFailed, reason)
		return err
	})
}

// SetNote records what the queue is waiting for on an active submission.
func (s *Store) SetNote(ctx context.Context, id int64, note string) error {
	return s.b.SetNote(ctx, id, note)
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
	err := s.b.Txn(ctx, func(tx Ops) error {
		sub, err := tx.LockSubmission(ctx, na.SubmissionID)
		if err != nil {
			return err
		}
		if sub.Status != SubmissionActive {
			return &ConflictError{Message: fmt.Sprintf("submission %d is no longer active", na.SubmissionID)}
		}
		active, ok, err := tx.ActiveAttempt(ctx, na.SubmissionID)
		if err != nil {
			return err
		}
		if ok {
			return &ConflictError{Message: fmt.Sprintf("submission %d already has an active attempt, %d", na.SubmissionID, active)}
		}
		chain := na.Chain
		if chain == nil {
			chain = []int64{}
		}
		if id, err = tx.InsertAttempt(ctx, NewAttemptRow{SubmissionID: na.SubmissionID, Base: na.Base, BaseSeq: na.BaseSeq,
			Chain: chain, PrepareError: na.PrepareError}); err != nil {
			return err
		}
		repo := sub.RepositoryID
		anyQueued := false
		for i, c := range na.Candidates {
			var planID int64
			if c.Own {
				// The planning run compares the candidate with the commit the patchset's pre ran on.
				var preCommit []byte
				pre, err := tx.PrePlan(ctx, repo, c.CLNumber, c.Patchset)
				switch {
				case err == nil:
					preCommit = pre.CommitID
				case !errors.Is(err, ErrNotFound):
					return err
				}
				p, err := tx.InsertPlan(ctx, NewPlan{RepositoryID: repo, CLNumber: c.CLNumber, Patchset: c.Patchset, Phase: PhasePost,
					CommitID: c.Commit, WorkflowsCommit: c.WorkflowsCommit, Context: c.Context, Touched: orEmpty(c.Touched), PreCommit: preCommit})
				if err != nil {
					return err
				}
				if _, err := tx.InsertRun(ctx, NewRun{
					RepositoryID: repo, CommitID: c.Commit, Job: Job{Kind: KindPlan},
					Origin: Origin{Phase: PhasePost, CLNumber: c.CLNumber, Patchset: c.Patchset, Job: planJob}, PlanID: p.ID,
					Why: na.Why, RequestedBy: sub.SubmittedBy, Status: StatusQueued,
					TimeoutSeconds: int32(s.cfg.DefaultTimeout / time.Second), Priority: postPriority,
				}); err != nil {
					return err
				}
				planID, anyQueued = p.ID, true
			}
			if err := tx.InsertCandidate(ctx, id, i, repo, AttemptCandidate{SubmissionID: c.SubmissionID, CLNumber: c.CLNumber,
				Patchset: c.Patchset, Commit: c.Commit, Base: c.Base, PlanID: planID}); err != nil {
				return err
			}
		}
		if anyQueued {
			if err := queued(ctx, tx); err != nil {
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
	err := s.b.Txn(ctx, func(tx Ops) error {
		carried = nil
		a, err := tx.LockAttempt(ctx, id)
		if err != nil {
			return err
		}
		if a.Status != AttemptActive || a.Released {
			return &ConflictError{Message: fmt.Sprintf("attempt %d is not waiting to run", id)}
		}
		subs, err := tx.Submissions(ctx, []int64{a.SubmissionID})
		if err != nil {
			return err
		}
		if len(subs) == 0 {
			return ErrNotFound
		}
		sub := subs[0]
		var ids []int64
		for _, c := range a.Candidates {
			if c.PlanID > 0 {
				ids = append(ids, c.PlanID)
			}
		}
		plans, err := tx.Plans(ctx, ids)
		if err != nil {
			return err
		}
		for _, p := range plans {
			if !p.Planned {
				return &ConflictError{Message: fmt.Sprintf("attempt %d's plan for CL %d is not planned", id, p.CLNumber)}
			}
		}
		for _, p := range plans {
			c, err := s.releasePlan(ctx, tx, sub.RepositoryID, p, sub.SubmittedBy)
			if err != nil {
				return err
			}
			carried = append(carried, c...)
		}
		if err := tx.SetReleased(ctx, id); err != nil {
			return err
		}
		return notifyQueue(ctx, tx, sub.RepositoryID)
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
func (s *Store) releasePlan(ctx context.Context, tx Ops, repo int64, p *Plan, requestedBy string) ([]int64, error) {
	origin := Origin{Phase: PhasePost, CLNumber: p.CLNumber, Patchset: p.Patchset}
	passes, err := prePasses(ctx, tx, repo, p.CLNumber, p.Patchset)
	if err != nil {
		return nil, err
	}
	var toQueue []PlannedJob
	var carried []int64
	for _, j := range p.Jobs {
		from := passes.of(j)
		if !j.UnchangedSincePre || from == nil {
			toQueue = append(toQueue, j)
			continue
		}
		note := fmt.Sprintf("ok: not run: it passed in pre as run %d, on patchset %d's commit %.10x, and nothing it reads differs on this candidate\n",
			from.ID, p.Patchset, from.CommitID)
		o := origin
		o.Job = j.Job
		run, err := tx.InsertRun(ctx, NewRun{
			RepositoryID: repo, CommitID: p.CommitID, Job: j.Spec, Origin: o, PlanID: p.ID, Why: j.Why, RequestedBy: requestedBy,
			Status: StatusPassed, TimeoutSeconds: int32(s.cfg.DefaultTimeout / time.Second), Priority: postPriority,
			Started: true, Finished: true, CarriedFrom: from.ID,
		})
		if err != nil {
			return nil, err
		}
		if err := tx.AppendLogChunk(ctx, run.ID, 0, []byte(note)); err != nil {
			return nil, err
		}
		if err := notify(ctx, tx, chanDone, run.ID); err != nil {
			return nil, err
		}
		carried = append(carried, run.ID)
	}
	return carried, s.insertJobs(ctx, tx, repo, p.CommitID, origin, p.ID, requestedBy, toQueue, postPriority, StatusQueued, "")
}

// passes are the jobs of a patchset's pre whose latest run passed.
type passes struct {
	byJob map[string]*Run
	all   []*Run
}

// prePasses returns the passes of a patchset's pre.
func prePasses(ctx context.Context, tx Ops, repo, cl int64, patchset int32) (*passes, error) {
	out := &passes{byJob: map[string]*Run{}}
	plan, err := tx.PrePlan(ctx, repo, cl, patchset)
	if errors.Is(err, ErrNotFound) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	runs, err := tx.PlanRuns(ctx, []int64{plan.ID})
	if err != nil {
		return nil, err
	}
	_, jobs := planRuns(runs)
	for _, r := range jobs {
		if r.Status == StatusPassed {
			out.byJob[r.Origin.Job] = r
			out.all = append(out.all, r)
		}
	}
	sort.Slice(out.all, func(i, j int) bool { return out.all[i].ID < out.all[j].ID })
	return out, nil
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
	return s.b.Txn(ctx, func(tx Ops) error {
		moved, err := tx.SetAttemptStatus(ctx, id, AttemptActive, AttemptDiscarded, reason)
		if err != nil || !moved {
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
	return s.b.Txn(ctx, func(tx Ops) error {
		repo, ok, err := tx.FinishSubmission(ctx, submission, SubmissionLanded, "")
		if err != nil {
			return err
		}
		if !ok {
			return &ConflictError{Message: fmt.Sprintf("submission %d is no longer active", submission)}
		}
		a, err := tx.LockAttempt(ctx, attempt)
		switch {
		case err == nil && a.SubmissionID == submission:
			if _, err := tx.SetAttemptStatus(ctx, attempt, 0, AttemptLanded, a.Reason); err != nil {
				return err
			}
		case err != nil && !errors.Is(err, ErrNotFound):
			return err
		}
		if err := endAttempts(ctx, tx, submission, AttemptDiscarded, "another attempt landed"); err != nil {
			return err
		}
		for i, l := range landed {
			if err := tx.SetLandedCommit(ctx, submission, l.CLNumber, l.Commit); err != nil {
				return err
			}
			if err := tx.InsertLandedChange(ctx, repo, l.Commit, l.CLNumber); err != nil {
				return err
			}
			if fromSeq > 0 {
				if err := tx.PutMainCommit(ctx, repo, fromSeq+int64(i)+1, l.Commit, submission); err != nil {
					return err
				}
				continue
			}
			if err := tx.SetMainCommitSubmission(ctx, repo, l.Commit, submission); err != nil {
				return err
			}
		}
		return notifyQueue(ctx, tx, repo)
	})
}

// RecordMainCommit records a commit main moved to, at its place in main's history.
func (s *Store) RecordMainCommit(ctx context.Context, repo, seq int64, commit []byte) error {
	return s.b.Txn(ctx, func(tx Ops) error {
		if err := tx.InsertMainCommit(ctx, repo, seq, commit); err != nil {
			return err
		}
		return notifyQueue(ctx, tx, repo)
	})
}

// RecordLandedChange records the CL a landed commit is.
func (s *Store) RecordLandedChange(ctx context.Context, repo int64, commit []byte, cl int64) error {
	return s.b.InsertLandedChange(ctx, repo, commit, cl)
}

// ReusableCandidate returns a candidate an active attempt already prepared from a CL's
// patchset on base, if there is one.
func (s *Store) ReusableCandidate(ctx context.Context, repo int64, base []byte, cl int64, patchset int32) ([]byte, bool, error) {
	return s.b.ReusableCandidate(ctx, repo, base, cl, patchset)
}

// CandidatesInUse returns the candidates a repository's active attempts hold, by commit ID.
func (s *Store) CandidatesInUse(ctx context.Context, repo int64) (map[string]bool, error) {
	return s.b.CandidatesInUse(ctx, repo)
}

// QueueRepositories returns the repositories the landing queue has work in: those with active
// submissions, and those where a submission finished within recent, whose candidates it
// cleans up.
func (s *Store) QueueRepositories(ctx context.Context, recent time.Duration) ([]int64, error) {
	return s.b.QueueRepositories(ctx, recent)
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
	st := &QueueState{}
	err := s.b.Snapshot(ctx, func(tx Ops) error {
		*st = QueueState{Others: map[int64]*Submission{}, Landings: map[int64]*Attempt{}, LandedCLs: map[int64][]byte{}}
		subs, err := tx.ActiveSubmissions(ctx, repo)
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
		attempts, err := tx.Attempts(ctx, ids, 0)
		if err != nil {
			return err
		}
		if err := loadPlans(ctx, tx, attempts); err != nil {
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
		if st.Main, err = tx.MainCommitsAfter(ctx, repo, st.From); err != nil {
			return err
		}
		for _, m := range st.Main {
			if m.SubmissionID > 0 {
				named[m.SubmissionID] = true
			}
		}
		var others []int64
		for id := range named {
			if byID[id] == nil {
				others = append(others, id)
			}
		}
		slices.Sort(others)
		if len(others) > 0 {
			subs, err := tx.Submissions(ctx, others)
			if err != nil {
				return err
			}
			for _, sub := range subs {
				st.Others[sub.ID] = sub
			}
			landings, err := tx.Attempts(ctx, others, AttemptLanded)
			if err != nil {
				return err
			}
			if err := loadPlans(ctx, tx, landings); err != nil {
				return err
			}
			for _, a := range landings {
				st.Landings[a.SubmissionID] = a
			}
		}
		if len(cls) > 0 {
			landedCLs, err := tx.LandedCLs(ctx, repo, cls)
			if err != nil {
				return err
			}
			st.LandedCLs = landedCLs
		}
		return nil
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
