package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/workflow"
)

// Lease assigns a queued run of a pool to a worker — the one admission and fair share choose
// (claim): of the accounts under their quotas, the one with the fewest running in the pool for its
// weight, its highest priority's oldest — waiting up to wait for one; it returns nil when none
// came.
func (s *Store) Lease(ctx context.Context, worker, pool string, wait time.Duration) (*Run, error) {
	if worker == "" {
		return nil, invalid("a worker names itself")
	}
	if pool == "" {
		pool = workflow.DefaultPool
	}
	deadline := time.Now().Add(wait)
	for {
		// In line before the claim, so a run queued between an empty claim and the wait still
		// wakes it.
		w := s.leases.join(pool)
		run, err := s.claim(ctx, worker, pool)
		if err != nil || run != nil {
			s.leases.leave(pool, w)
			if run != nil {
				// The run came queued with others, maybe: the next in line looks.
				s.leases.wakeNext(pool)
			}
			return run, err
		}
		left := time.Until(deadline)
		if left <= 0 {
			s.leases.leave(pool, w)
			return nil, nil
		}
		err = s.notify.wait(ctx, w, min(left, s.cfg.Poll))
		s.leases.leave(pool, w)
		if err != nil {
			return nil, err
		}
	}
}

// leaseLines are this replica's leases waiting for a run, in line by pool. A run queued wakes the
// first in each pool's line, and a lease that claims a run wakes the next, for the runs queued
// with it: every waiting lease woken by every run queued had each claim race the rest of them for
// the one run, a dozen transactions a run, which the claims' retries and the database's CPU
// showed. A lease looks on its own every Config.Poll still, as before.
type leaseLines struct {
	mu    sync.Mutex
	lines map[string][]chan struct{}
}

func newLeaseLines() *leaseLines { return &leaseLines{lines: map[string][]chan struct{}{}} }

// join puts a lease at the end of its pool's line: the channel is closed when it is woken.
func (l *leaseLines) join(pool string) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	w := make(chan struct{})
	l.lines[pool] = append(l.lines[pool], w)
	return w
}

// leave takes a lease out of its line, if it is in it still.
func (l *leaseLines) leave(pool string, w chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	line := l.lines[pool]
	for i, x := range line {
		if x == w {
			l.lines[pool] = append(line[:i:i], line[i+1:]...)
			return
		}
	}
}

// wakeNext wakes the first lease in a pool's line, taking it out.
func (l *leaseLines) wakeNext(pool string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if line := l.lines[pool]; len(line) > 0 {
		close(line[0])
		l.lines[pool] = line[1:]
	}
}

// wakeEach wakes the first lease in every pool's line: a run was queued, in a pool the signal
// does not say.
func (l *leaseLines) wakeEach() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for pool, line := range l.lines {
		if len(line) > 0 {
			close(line[0])
			l.lines[pool] = line[1:]
		}
	}
}

// wakeAll wakes every lease in line.
func (l *leaseLines) wakeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for pool, line := range l.lines {
		for _, w := range line {
			close(w)
		}
		delete(l.lines, pool)
	}
}

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
// attempt: its worker is shutting down. A run canceled meanwhile ends canceled instead.
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
			if err := finishedPost(ctx, tx, run.RepositoryID, run.Origin.Phase); err != nil {
				return err
			}
			return notify(ctx, tx, chanDone, id)
		}
		if reason == "" {
			reason = "released by its worker"
		}
		if err := tx.RequeueRun(ctx, id, reason, false); err != nil {
			return err
		}
		return queued(ctx, tx)
	})
}

// Finish records a leased run's outcome, ends the lease, and moves its log to the bucket. A
// planning run that passed carries its plan, whose jobs are queued with the outcome.
func (s *Store) Finish(ctx context.Context, id int64, worker string, status Status, exitCode int32, reason string, plan *PlanResult, pushed ...Pushed) error {
	if !status.Finished() {
		return invalid("a run finishes passed, failed, canceled, or in error")
	}
	err := s.txn(ctx, func(tx Ops) error {
		run, err := leased(ctx, tx, id, worker)
		if err != nil {
			return err
		}
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
		return notify(ctx, tx, chanDone, id)
	})
	if err != nil {
		return err
	}
	// The log moves after the outcome is recorded; a failed move is the sweep's to retry.
	return s.storeLog(ctx, id)
}

// finishedPost marks the landing queue of a repository a run of phase finished in, if the run was
// post: its end is the queue's to act on, however it ended.
func finishedPost(ctx context.Context, tx Ops, repo int64, phase Phase) error {
	if phase != PhasePost {
		return nil
	}
	return notifyQueue(ctx, tx, repo)
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

// sweepLeases acts on the leases that lapsed: a canceled run ends canceled; any other is
// queued again, once, with its log cleared, and then ends in error. The lapsed are found outside
// any transaction, and each acted on in one of its own that finds it still lapsed, so the sweep
// locks no lease a worker renews.
func (s *Store) sweepLeases(ctx context.Context) error {
	ids, err := s.b.LapsedRunIDs(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, id := range ids {
		if err := s.txn(ctx, func(tx Ops) error { return s.sweepLease(ctx, tx, id) }); err != nil {
			errs = append(errs, fmt.Errorf("run %d: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// sweepLease acts on a run whose lease lapsed, if it still has.
func (s *Store) sweepLease(ctx context.Context, tx Ops, id int64) error {
	l, ok, err := tx.LapsedRun(ctx, id)
	if err != nil || !ok {
		return err
	}
	// Its repository's network may have frozen since it was read: then it is the move's.
	if err := tx.Gate(ctx, l.RepositoryID); err != nil {
		return err
	}
	switch {
	case l.Stopping:
		if err := tx.FinishRun(ctx, l.ID, StatusCanceled, 0, l.StopReason); err != nil {
			return err
		}
	case l.Attempt < s.cfg.Attempts:
		// The lost attempt's log goes; the next attempt writes its own from the start.
		if err := tx.RequeueRun(ctx, l.ID, "its worker "+l.Worker+" was lost; it runs again", true); err != nil {
			return err
		}
		return queued(ctx, tx)
	default:
		if err := tx.FinishRun(ctx, l.ID, StatusError, 0, "its worker "+l.Worker+" was lost, on its last attempt"); err != nil {
			return err
		}
	}
	if err := finishedPost(ctx, tx, l.RepositoryID, l.Phase); err != nil {
		return err
	}
	return notify(ctx, tx, chanDone, l.ID)
}
