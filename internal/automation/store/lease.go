package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/move"
)

// RunOfLease is a run, when it is running under the lease whose key its worker was given
// (docs/secrets.md, "Readers that are not people"): ErrNotFound otherwise — no such run, another
// lease of it, or none.
func (s *Store) RunOfLease(ctx context.Context, id int64, key []byte) (*Run, error) {
	h := sha256.Sum256(key)
	ok, err := s.b.LeaseKeyHolds(ctx, id, h[:])
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("run %d is not running under that lease: %w", id, ErrNotFound)
	}
	return s.Get(ctx, id)
}

// Stop says whether, and why, a worker must stop the run it holds.
type Stop int

const (
	StopNone      Stop = 0 // keep running it
	StopCanceled  Stop = 1 // finish it canceled
	StopTimedOut  Stop = 2 // finish it failed
	StopLeaseLost Stop = 3 // the run is no longer the worker's: abandon it
)

// Heartbeat renews a worker's lease on a run, and says whether the worker must stop it, and
// why: it was canceled, it ran past its timeout, or the lease is no longer the worker's.
func (s *Store) Heartbeat(ctx context.Context, id int64, worker string) (stop Stop, reason string, err error) {
	// A renewal changes only a held run's lease, and a frozen repository's runs are held by none:
	// no move gates it.
	l, ok, err := s.b.RenewLease(move.Untagged(ctx), id, worker, s.leaseTTL())
	switch {
	case err != nil:
		return StopNone, "", err
	case !ok:
		return StopLeaseLost, ErrLeaseLost.Error(), nil
	}
	// The run's lease is its job's delivery: kept as long as the worker reports.
	s.extend(ctx, l.Handle, s.leaseTTL())
	switch {
	case l.Stopping:
		return StopCanceled, l.StopReason, nil
	case l.Overdue:
		return StopTimedOut, fmt.Sprintf("it ran past its timeout of %s", time.Duration(l.TimeoutSeconds)*time.Second), nil
	}
	return StopNone, "", nil
}

// leased reads a run, held, if it is running and worker's lease; ErrLeaseLost otherwise.
func leased(ctx context.Context, tx Ops, id int64, worker string) (*Run, error) {
	run, err := tx.LockRun(ctx, id)
	if errors.Is(err, ErrNotFound) || err == nil && (run.Status != StatusRunning || run.Worker != worker) {
		return nil, ErrLeaseLost
	}
	return run, err
}

// Release gives a leased run back to the queue, its log cleared, without spending an
// attempt: its worker is shutting down. It is dispatched by a job of its own, and the delivery it
// ran under acknowledged. A run canceled meanwhile ends canceled instead.
func (s *Store) Release(ctx context.Context, id int64, worker, reason string) error {
	return s.txn(ctx, func(tx Ops) error {
		run, err := leased(ctx, tx, id, worker)
		if err != nil {
			return err
		}
		stopping, stopReason, err := tx.StopRequest(ctx, id)
		if err != nil {
			return err
		}
		if stopping {
			if err := tx.FinishRun(ctx, id, StatusCanceled, 0, stopReason); err != nil {
				return err
			}
			tx.(txOps).end(run.LeaseHandle)
			if err := finishedPost(ctx, tx, run.RepositoryID, run.Origin.Phase); err != nil {
				return err
			}
			return changed(ctx, tx, id)
		}
		if reason == "" {
			reason = "released by its worker"
		}
		return tx.(txOps).requeue(ctx, run, reason, false)
	})
}

// Finish records a leased run's outcome, ends the lease — its job's delivery acknowledged — and
// moves its log to the bucket. A planning run that passed carries its plan, whose jobs are queued
// with the outcome.
func (s *Store) Finish(ctx context.Context, id int64, worker string, status Status, exitCode int32, reason string, plan *PlanResult, pushed ...Pushed) error {
	if !status.Finished() {
		return invalid("a run finishes passed, failed, canceled, or in error")
	}
	err := s.txn(ctx, func(tx Ops) error {
		run, err := leased(ctx, tx, id, worker)
		if err != nil {
			return err
		}
		tx.(txOps).end(run.LeaseHandle)
		switch {
		case run.Job.Kind == KindPlan && status == StatusPassed && plan == nil:
			return invalid("a planning run that passed carries its plan")
		case plan != nil && (run.Job.Kind != KindPlan || status != StatusPassed):
			return invalid("only a planning run that passed carries a plan")
		case run.Job.Kind == KindPush && status == StatusPassed && len(pushed) != 1:
			return invalid("a push run that passed carries the image it pushed")
		case len(pushed) > 0 && (run.Job.Kind != KindPush || status != StatusPassed):
			return invalid("only a push run that passed carries what it pushed")
		}
		for _, p := range pushed {
			if p.Target != run.Job.Targets[0] || p.Registry == "" || p.Repository == "" || !validDigest(p.Digest) {
				return invalid("a pushed image names the run's oci_push, where it went, and a sha256 digest")
			}
		}
		if len(pushed) > 0 {
			if err := tx.InsertPushed(ctx, id, pushed); err != nil {
				return err
			}
		}
		if err := tx.FinishRun(ctx, id, status, exitCode, reason); err != nil {
			return err
		}
		if plan != nil {
			if err := s.queuePlan(ctx, tx, run, plan); err != nil {
				return err
			}
		}
		if err := finishedPost(ctx, tx, run.RepositoryID, run.Origin.Phase); err != nil {
			return err
		}
		return changed(ctx, tx, id)
	})
	if err != nil {
		return err
	}
	// The log moves after the outcome is recorded; a failed move is its log job's to retry.
	if err := s.storeLog(ctx, id); err != nil {
		slog.Warn("automation: storing a run's log; its job retries it", "run", id, "err", err)
	}
	return nil
}

// finishedPost marks the landing queue of a repository a run of phase finished in, if the run was
// post: its end is the queue's to act on, however it ended.
func finishedPost(ctx context.Context, tx Ops, repo int64, phase Phase) error {
	if phase != PhasePost {
		return nil
	}
	return markQueue(ctx, tx, repo)
}

// validDigest reports whether d is a sha256 digest, sha256:<64 hex>.
func validDigest(d string) bool {
	hexPart, ok := strings.CutPrefix(d, "sha256:")
	if !ok || len(hexPart) != 64 {
		return false
	}
	for _, c := range hexPart {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
