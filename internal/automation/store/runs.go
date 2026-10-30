package store

import (
	"context"
	"time"

	"github.com/Cidan/ok/internal/wait"
)

// changed says, in tx, that the run of id changed — its log grew, or it ended — for its readers.
func changed(ctx context.Context, tx Ops, id int64) error {
	kind, key, body := wait.Changed(wait.RunSubject(id))
	return tx.PendStep(ctx, kind, key, time.Time{}, body)
}

// StartRequest is a job to run on a commit.
type StartRequest struct {
	RepositoryID int64
	CommitID     []byte
	Job          Job
	Origin       Origin
	RequestedBy  string
	Timeout      time.Duration // 0: the default
}

// Start queues a job on a commit.
func (s *Store) Start(ctx context.Context, req StartRequest) (*Run, error) {
	if req.RepositoryID <= 0 {
		return nil, invalid("a repository is required")
	}
	if len(req.CommitID) != 32 {
		return nil, invalid("a commit ID is 32 bytes")
	}
	if err := validJob(req.Job); err != nil {
		return nil, err
	}
	if err := validOrigin(req.Origin); err != nil {
		return nil, err
	}
	if req.Origin.Phase == PhasePost {
		return nil, invalid("post runs are the landing queue's: submit the CL")
	}
	if (req.Job.Kind == KindPush) != (req.Origin.Phase == PhaseDeploy) {
		return nil, invalid("a push is a deployment's, and a deployment's run is a push")
	}
	if req.RequestedBy == "" {
		return nil, invalid("a run is requested by someone")
	}
	timeout := req.Timeout
	switch {
	case timeout == 0:
		timeout = s.cfg.DefaultTimeout
	case timeout < time.Second || timeout > s.cfg.MaxTimeout:
		return nil, invalid("a timeout is between a second and %s", s.cfg.MaxTimeout)
	}
	var run *Run
	err := s.txn(ctx, func(tx Ops) error {
		var err error
		run, err = tx.InsertRun(ctx, NewRun{
			RepositoryID: req.RepositoryID, CommitID: req.CommitID, Job: req.Job, Origin: req.Origin,
			RequestedBy: req.RequestedBy, Status: StatusQueued, TimeoutSeconds: int32(timeout / time.Second),
		})
		return err
	})
	return run, err
}

// Cancel ends a queued run at once, and asks a running run's worker to stop it; a finished
// run is returned as it is. A post run is its submission's: canceling the submission stops it.
func (s *Store) Cancel(ctx context.Context, id int64, reason, by string) (*Run, error) {
	if reason == "" {
		reason = "canceled"
	}
	err := s.txn(ctx, func(tx Ops) error {
		r, err := tx.Run(ctx, id)
		if err != nil {
			return err
		}
		if r.Origin.Phase == PhasePost {
			return &ConflictError{Message: "a post run is its submission's: cancel the submission"}
		}
		return cancelRun(ctx, tx, id, reason, by)
	})
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, id)
}

// cancelRun ends a queued run canceled, or asks a running run's worker to stop it, for by (a
// stable ID; "" for no one).
func cancelRun(ctx context.Context, tx Ops, id int64, reason, by string) error {
	// Held, through its repository's move gate.
	if _, err := tx.LockRun(ctx, id); err != nil {
		return err
	}
	// A queued run has no log: it is stored, empty, as it ends.
	ended, err := tx.CancelQueuedRun(ctx, id, reason, by)
	if err != nil {
		return err
	}
	if ended {
		return changed(ctx, tx, id)
	}
	return tx.RequestStop(ctx, id, reason, by)
}

// rerunOf is a new run of prev's job on its commit, requested by by: a rerun of it.
func rerunOf(prev *Run, by string) NewRun {
	return NewRun{
		RepositoryID: prev.RepositoryID, CommitID: prev.CommitID, Job: prev.Job, Origin: prev.Origin,
		PlanID: prev.PlanID, Why: prev.Why, RequestedBy: by, RerunOf: prev.ID, Status: StatusQueued,
		TimeoutSeconds: prev.TimeoutSeconds,
	}
}

// Rerun queues a finished run's job on its commit again, as a new run.
func (s *Store) Rerun(ctx context.Context, id int64, requestedBy string) (*Run, error) {
	if requestedBy == "" {
		return nil, invalid("a run is requested by someone")
	}
	var run *Run
	err := s.txn(ctx, func(tx Ops) error {
		prev, err := tx.Run(ctx, id)
		if err != nil {
			return err
		}
		if !prev.Status.Finished() {
			return &ConflictError{Message: "only a finished run can be run again"}
		}
		if prev.Origin.Phase == PhasePost {
			return &ConflictError{Message: "a post run is its submission's: submit the CL again"}
		}
		if prev.Origin.Phase == PhaseDeploy {
			return &ConflictError{Message: "a push is its deployment's: retry the deployment"}
		}
		if prev.Job.Kind == KindPlan {
			plan, err := tx.LockPlan(ctx, prev.PlanID)
			if err != nil {
				return err
			}
			if plan.Planned {
				return &ConflictError{Message: "the patchset is planned: run its jobs again instead"}
			}
		}
		run, err = tx.InsertRun(ctx, rerunOf(prev, requestedBy))
		return err
	})
	return run, err
}

// MainCommits returns each repository's newest n commits on main, as automation has seen main
// move, newest first.
func (s *Store) MainCommits(ctx context.Context, n int) (map[int64][][]byte, error) {
	return s.b.MainCommits(ctx, n)
}

// RunCommits returns the commits of every run queued or running, or finished since since, by
// repository.
func (s *Store) RunCommits(ctx context.Context, since time.Time) (map[int64][][]byte, error) {
	return s.b.RunCommits(ctx, since)
}
