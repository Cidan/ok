package store

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/Cidan/ok/internal/move"
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
	SubmittedBy  string // a person's stable ID
	Status       SubmissionStatus
	Reason       string // why it failed or was canceled
	CanceledBy   string // who canceled it, by stable ID; "" for no one
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
// plan of its top.
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

// Own returns the attempt's own candidates: its submission's CLs, bottom first, which it lands.
func (a *Attempt) Own() []*AttemptCandidate {
	var out []*AttemptCandidate
	for _, c := range a.Candidates {
		if c.SubmissionID == a.SubmissionID {
			out = append(out, c)
		}
	}
	return out
}

// Planned returns the own candidates with post plans: a stack's top alone, whose plan is over the
// whole submission (docs/automation.md, "post").
func (a *Attempt) Planned() []*AttemptCandidate {
	var out []*AttemptCandidate
	for _, c := range a.Own() {
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

// maxSubmissionCLs bounds a submission's stack: the SCM's bound on one landing. A stack's post is
// one plan, at its top, so a CL under it costs a candidate and its part of the landing.
const maxSubmissionCLs = 1000

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
	err := s.txn(ctx, func(tx Ops) error {
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
		return markQueue(ctx, tx, req.RepositoryID)
	})
	if err != nil {
		return nil, err
	}
	return s.GetSubmission(ctx, id)
}

// markQueue marks a repository's landing queue's work: its job, enqueued before tx commits, brings
// the queue.
func markQueue(ctx context.Context, tx Ops, repo int64) error {
	return tx.MarkWork(ctx, QueueWork, repo)
}

// QueueWork is a repository's landing queue, as keyed work: marked whenever what the queue acts on
// changes, and done by one replica at a time. CollectWork is the collection of its candidates no
// attempt holds, booked by a pass of the queue that finds nothing left to do, and done holding the
// queue's claim.
const (
	QueueWork   = "queue"
	CollectWork = "collect"
)

// Work is automation's keyed work's store (internal/work).
func (s *Store) Work() work.Store { return s.b.Work() }

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
		if err := cancelRun(ctx, tx, id, reason, ""); err != nil {
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
func finishSubmission(ctx context.Context, tx Ops, id int64, status SubmissionStatus, reason, by string) (bool, error) {
	repo, ok, err := tx.FinishSubmission(ctx, id, status, reason, by)
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
	return true, markQueue(ctx, tx, repo)
}

// CancelSubmission cancels an active submission: its runs stop, and nothing of it lands. A
// finished submission is returned as it is.
func (s *Store) CancelSubmission(ctx context.Context, id int64, reason, by string) (*Submission, error) {
	if reason == "" {
		reason = "canceled"
	}
	if err := s.txn(ctx, func(tx Ops) error {
		_, err := finishSubmission(ctx, tx, id, SubmissionCanceled, reason, by)
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
	err := s.txn(ctx, func(tx Ops) error {
		n = 0
		ids, err := tx.ActiveSubmissionsOfCL(ctx, repo, cl, patchset)
		if err != nil {
			return err
		}
		for _, id := range ids {
			ok, err := finishSubmission(ctx, tx, id, SubmissionCanceled, reason, "")
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
	return s.txn(ctx, func(tx Ops) error {
		_, err := finishSubmission(ctx, tx, id, SubmissionFailed, reason, "")
		return err
	})
}

// FencedWritesBesideEachOther reports whether writes resting on one keyed-work lease may run beside
// each other (Backend.FencedWritesBesideEachOther).
func (s *Store) FencedWritesBesideEachOther() bool { return s.b.FencedWritesBesideEachOther() }

// SetNotes records what the queue is waiting for on active submissions, by submission, in one
// transaction, in the order of their IDs: a pass's notes, one write.
func (s *Store) SetNotes(ctx context.Context, notes map[int64]string) error {
	if len(notes) == 0 {
		return nil
	}
	ids := slices.Sorted(maps.Keys(notes))
	return s.txn(ctx, func(tx Ops) error {
		for _, id := range ids {
			if _, err := tx.LockSubmission(ctx, id); errors.Is(err, ErrNotFound) {
				continue
			} else if err != nil {
				return err
			}
			if err := tx.SetNote(ctx, id, notes[id]); err != nil {
				return err
			}
		}
		return nil
	})
}

// SetNote records what the queue is waiting for on an active submission.
func (s *Store) SetNote(ctx context.Context, id int64, note string) error {
	return s.txn(ctx, func(tx Ops) error {
		if _, err := tx.LockSubmission(ctx, id); err != nil {
			return err
		}
		return tx.SetNote(ctx, id, note)
	})
}

// NewCandidate is a candidate of an attempt the queue prepared: the chain's, or its own
// submission's (Own), of which the top alone is planned.
type NewCandidate struct {
	SubmissionID int64
	CLNumber     int64
	Patchset     int32
	Commit       []byte
	Base         []byte
	Own          bool
	// A planned own candidate — a submission's top — carries its workflows commit, its job
	// context, and its footprint's paths, over the whole submission; the rest carry none.
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
		if c.Own && c.Context != nil && (len(c.WorkflowsCommit) != 32 || !json.Valid(c.Context)) {
			return 0, invalid("a planned own candidate carries its workflows commit and its job context")
		}
		if !c.Own && c.Context != nil {
			return 0, invalid("only an own candidate is planned")
		}
	}
	if n := len(na.Candidates); n > 0 && na.PrepareError == "" {
		if top := na.Candidates[n-1]; top.Own && top.Context == nil {
			return 0, invalid("a submission's top candidate is planned")
		}
	}
	var id int64
	err := s.txn(ctx, func(tx Ops) error {
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
		for i, c := range na.Candidates {
			var planID int64
			if c.Own && c.Context != nil {
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
					CommitID: c.Commit, WorkflowsCommit: c.WorkflowsCommit, Context: c.Context, Touched: orEmpty(BoundPaths(c.Touched)), PreCommit: preCommit})
				if err != nil {
					return err
				}
				if _, err := tx.InsertRun(ctx, NewRun{
					RepositoryID: repo, CommitID: c.Commit, Job: Job{Kind: KindPlan, Pool: s.cfg.PlanPool},
					Origin: Origin{Phase: PhasePost, CLNumber: c.CLNumber, Patchset: c.Patchset, Job: planJob}, PlanID: p.ID,
					Why: na.Why, RequestedBy: sub.SubmittedBy, Status: StatusQueued,
					TimeoutSeconds: int32(s.cfg.DefaultTimeout / time.Second),
				}); err != nil {
					return err
				}
				planID = p.ID
			}
			if err := tx.InsertCandidate(ctx, id, i, repo, AttemptCandidate{SubmissionID: c.SubmissionID, CLNumber: c.CLNumber,
				Patchset: c.Patchset, Commit: c.Commit, Base: c.Base, PlanID: planID}); err != nil {
				return err
			}
		}
		return markQueue(ctx, tx, repo)
	})
	return id, err
}

// ReleaseAttempt queues the jobs of an attempt's post plans: the queue decided the attempt is
// the one to test. Every one of its plans must be planned. A job nothing changed for since pre,
// which passed there, is recorded passed at once instead, carrying that pass.
func (s *Store) ReleaseAttempt(ctx context.Context, id int64) error {
	var carried []int64
	err := s.txn(ctx, func(tx Ops) error {
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
		return markQueue(ctx, tx, sub.RepositoryID)
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
		// Its log is a note naming the pass, which the store makes from the two runs.
		note := carriedNote(from.ID, p.Patchset, from.CommitID)
		o := origin
		o.Job = j.Job
		run, err := tx.InsertRun(ctx, NewRun{
			RepositoryID: repo, CommitID: p.CommitID, Job: j.Spec, Origin: o, PlanID: p.ID, Why: j.Why, RequestedBy: requestedBy,
			Status: StatusPassed, TimeoutSeconds: int32(s.cfg.DefaultTimeout / time.Second),
			Started: true, Finished: true, CarriedFrom: from.ID, LogSize: int64(len(note)),
		})
		if err != nil {
			return nil, err
		}
		if err := changed(ctx, tx, run.ID); err != nil {
			return nil, err
		}
		carried = append(carried, run.ID)
	}
	return carried, s.insertJobs(ctx, tx, repo, p.CommitID, origin, p.ID, requestedBy, toQueue, StatusQueued, "")
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
	sort.Slice(out.all, func(i, j int) bool {
		a, b := out.all[i], out.all[j]
		return a.QueuedAt.Before(b.QueuedAt) || a.QueuedAt.Equal(b.QueuedAt) && a.ID < b.ID
	})
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
	return s.txn(ctx, func(tx Ops) error {
		a, err := tx.LockAttempt(ctx, id)
		if err != nil {
			return err
		}
		moved, err := tx.SetAttemptStatus(ctx, id, AttemptActive, AttemptDiscarded, reason)
		if err != nil || !moved {
			return err
		}
		if err := cancelAttemptRuns(ctx, tx, []int64{id}, reason); err != nil {
			return err
		}
		subs, err := tx.Submissions(ctx, []int64{a.SubmissionID})
		if err != nil {
			return err
		}
		if len(subs) == 0 {
			return ErrNotFound
		}
		return markQueue(ctx, tx, subs[0].RepositoryID)
	})
}

// LandedCL is one CL a landing landed, and the commit it landed as.
type LandedCL struct {
	CLNumber int64
	Commit   []byte
}

// AttemptOf is an attempt, and the submission it is of.
type AttemptOf struct{ Attempt, Submission int64 }

// SubmissionCommit is the commit a submission's CL landed as.
type SubmissionCommit struct {
	Submission, CLNumber int64
	Commit               []byte
}

// MainUnknown is a landing's place on main before it when the queue does not know it (an adopted
// landing): main's history comes from the SCM's events alone, which find the submission by its
// CLs' commits as they record each one.
const MainUnknown = -1

// RecordLanding records that a submission landed on an attempt: the commit each CL landed as,
// and — at fromSeq, main's place before the landing, 0 for a main with nothing on it, unless it is
// MainUnknown — main's history.
func (s *Store) RecordLanding(ctx context.Context, submission, attempt int64, landed []LandedCL, fromSeq int64) error {
	skipped, err := s.RecordLandings(ctx, []Landing{{Submission: submission, Attempt: attempt, Landed: landed, FromSeq: fromSeq}})
	if err == nil && len(skipped) > 0 {
		return &ConflictError{Message: fmt.Sprintf("submission %d is no longer active", submission)}
	}
	return err
}

// Landing is one submission a landing landed, for RecordLandings: on which attempt, the commit each
// of its CLs landed as, and main's place before them (RecordLanding's fromSeq).
type Landing struct {
	Submission, Attempt int64
	Landed              []LandedCL
	FromSeq             int64
}

// RecordLandings records a landing's submissions, all of one repository, in one transaction: a
// batch's landings recorded one after another, each locking main's history, held the queue up as
// long as there were submissions in it. It skips, and returns, those no longer active. The
// submissions are finished, locked in the order of their IDs, before main is locked; then each kind
// of row is written for all of them in a statement of its own (recordLanded).
func (s *Store) RecordLandings(ctx context.Context, landings []Landing) (skipped []int64, err error) {
	sorted := slices.Clone(landings)
	slices.SortFunc(sorted, func(a, b Landing) int { return cmp.Compare(a.Submission, b.Submission) })
	ids := make([]int64, len(sorted))
	for i, l := range sorted {
		ids[i] = l.Submission
	}
	err = s.txn(ctx, func(tx Ops) error {
		skipped = nil
		ended, err := tx.FinishSubmissions(ctx, ids, SubmissionLanded, "", "")
		if err != nil {
			return err
		}
		repo := int64(0)
		var finished []Landing
		for _, l := range sorted {
			r, ok := ended[l.Submission]
			if !ok {
				skipped = append(skipped, l.Submission)
				continue
			}
			if repo != 0 && r != repo {
				return invalid("a landing's submissions are of repositories %d and %d", repo, r)
			}
			repo = r
			finished = append(finished, l)
		}
		if len(finished) == 0 {
			return nil
		}
		// The SCM's event of the move may be recorded at once (RecordMainCommit): the two take
		// turns, or each — an adopted landing's record of its submission, the event's of the row —
		// would miss the other.
		if err := tx.LockMain(ctx, repo); err != nil {
			return err
		}
		if err := recordLanded(ctx, tx, repo, finished); err != nil {
			return err
		}
		return markQueue(ctx, tx, repo)
	})
	return skipped, err
}

// recordLanded records, in tx holding main's history, what finished submissions landed: each one's
// attempt landed (if it is the submission's), its other active attempts discarded and their runs
// canceled, the commits its CLs landed as, and its place on main.
func recordLanded(ctx context.Context, tx Ops, repo int64, landings []Landing) error {
	if t, ok := tx.(txOps); ok {
		for _, l := range landings {
			for _, c := range l.Landed {
				t.touched.cls[[2]int64{repo, c.CLNumber}] = true
			}
		}
	}
	pairs := make([]AttemptOf, len(landings))
	subs := make([]int64, len(landings))
	var commits []SubmissionCommit
	var changes []LandedCL
	var put, set []MainCommit
	for i, l := range landings {
		pairs[i], subs[i] = AttemptOf{Attempt: l.Attempt, Submission: l.Submission}, l.Submission
		for j, c := range l.Landed {
			commits = append(commits, SubmissionCommit{Submission: l.Submission, CLNumber: c.CLNumber, Commit: c.Commit})
			changes = append(changes, c)
			if l.FromSeq != MainUnknown {
				put = append(put, MainCommit{Seq: l.FromSeq + int64(j) + 1, Commit: c.Commit, SubmissionID: l.Submission})
			} else {
				set = append(set, MainCommit{Commit: c.Commit, SubmissionID: l.Submission})
			}
		}
	}
	if err := tx.LandAttempts(ctx, pairs); err != nil {
		return err
	}
	const discarded = "another attempt landed"
	others, err := tx.EndActiveAttemptsOf(ctx, subs, AttemptDiscarded, discarded)
	if err != nil {
		return err
	}
	if err := cancelAttemptRuns(ctx, tx, others, discarded); err != nil {
		return err
	}
	if err := tx.SetLandedCommits(ctx, commits); err != nil {
		return err
	}
	if err := tx.InsertLandedChanges(ctx, repo, changes); err != nil {
		return err
	}
	if err := tx.PutMainCommits(ctx, repo, put); err != nil {
		return err
	}
	return tx.SetMainCommitSubmissions(ctx, repo, set)
}

// RecordMainCommit records a commit main moved to, at its place in main's history.
func (s *Store) RecordMainCommit(ctx context.Context, repo, seq int64, commit []byte) error {
	return s.txn(ctx, func(tx Ops) error {
		if err := tx.LockMain(ctx, repo); err != nil {
			return err
		}
		if err := tx.InsertMainCommit(ctx, repo, seq, commit); err != nil {
			return err
		}
		return markQueue(ctx, tx, repo)
	})
}

// RecordLandedChange records the CL a landed commit is.
func (s *Store) RecordLandedChange(ctx context.Context, repo int64, commit []byte, cl int64) error {
	return s.b.InsertLandedChange(move.WithRepository(ctx, repo), repo, commit, cl)
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
