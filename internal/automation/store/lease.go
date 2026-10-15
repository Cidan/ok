package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Lease assigns a queued run to a worker — the highest priority's oldest, so post, which main
// waits on, goes first — waiting up to wait for one; it returns nil when none came.
func (s *Store) Lease(ctx context.Context, worker string, wait time.Duration) (*Run, error) {
	if worker == "" {
		return nil, invalid("a worker names itself")
	}
	deadline := time.Now().Add(wait)
	for {
		// Waiting for a notification starts before the claim, so a run queued between an
		// empty claim and the wait still wakes it.
		woken := s.notify.watch(chanQueue, "")
		run, err := scanRun(s.pool.QueryRow(ctx, `UPDATE run
			SET status = $2, worker = $3, lease_expires = now() + make_interval(secs => $4), started_at = now()
			WHERE id = (SELECT id FROM run WHERE status = $1 ORDER BY priority DESC, id LIMIT 1 FOR UPDATE SKIP LOCKED)
			RETURNING `+runColumns, StatusQueued, StatusRunning, worker, s.leaseTTL().Seconds()))
		switch {
		case err == nil:
			return run, nil
		case err != ErrNotFound:
			return nil, err
		}
		left := time.Until(deadline)
		if left <= 0 {
			return nil, nil
		}
		if err := s.notify.wait(ctx, woken, min(left, s.cfg.Poll)); err != nil {
			return nil, err
		}
	}
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
	var stopReason *string
	var overdue bool
	var timeout int32
	err = s.pool.QueryRow(ctx, `UPDATE run SET lease_expires = now() + make_interval(secs => $3)
		WHERE id = $1 AND status = $4 AND worker = $2
		RETURNING stop_reason, now() > started_at + make_interval(secs => timeout_seconds), timeout_seconds`,
		id, worker, s.leaseTTL().Seconds(), StatusRunning).Scan(&stopReason, &overdue, &timeout)
	switch {
	case err == pgx.ErrNoRows:
		return StopLeaseLost, ErrLeaseLost.Error(), nil
	case err != nil:
		return StopNone, "", err
	case stopReason != nil:
		return StopCanceled, *stopReason, nil
	case overdue:
		return StopTimedOut, fmt.Sprintf("it ran past its timeout of %s", time.Duration(timeout)*time.Second), nil
	}
	return StopNone, "", nil
}

// Release gives a leased run back to the queue, its log cleared, without spending an
// attempt: its worker is shutting down. A run canceled meanwhile ends canceled instead.
func (s *Store) Release(ctx context.Context, id int64, worker, reason string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var stopReason *string
		err := tx.QueryRow(ctx, `SELECT stop_reason FROM run WHERE id = $1 AND status = $2 AND worker = $3 FOR UPDATE`,
			id, StatusRunning, worker).Scan(&stopReason)
		if err == pgx.ErrNoRows {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if stopReason != nil {
			if _, err := tx.Exec(ctx, `UPDATE run SET status = $2, reason = stop_reason, finished_at = now(),
					lease_expires = NULL, stop_reason = NULL WHERE id = $1`, id, StatusCanceled); err != nil {
				return err
			}
			_, err = notify(ctx, tx, chanDone, id)
			return err
		}
		if reason == "" {
			reason = "released by its worker"
		}
		if _, err := tx.Exec(ctx, `UPDATE run SET status = $2, reason = $3, worker = NULL, lease_expires = NULL,
				started_at = NULL, log_size = 0 WHERE id = $1`, id, StatusQueued, reason); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM log_chunk WHERE run_id = $1`, id); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `SELECT pg_notify($1, '')`, chanQueue)
		return err
	})
}

// Finish records a leased run's outcome, ends the lease, and moves its log to the bucket. A
// planning run that passed carries its plan, whose jobs are queued with the outcome.
func (s *Store) Finish(ctx context.Context, id int64, worker string, status Status, exitCode int32, reason string, plan *PlanResult, pushed ...Pushed) error {
	if !status.Finished() {
		return invalid("a run finishes passed, failed, canceled, or in error")
	}
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		run, err := scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM run
			WHERE id = $1 AND status = $3 AND worker = $2 FOR UPDATE`, id, worker, StatusRunning))
		if errors.Is(err, ErrNotFound) {
			return ErrLeaseLost
		}
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
		for i, p := range pushed {
			if p.Target != run.Job.Targets[0] || p.Registry == "" || p.Repository == "" || !validDigest(p.Digest) {
				return invalid("a pushed image names the run's oci_push, where it went, and a sha256 digest")
			}
			if _, err := tx.Exec(ctx, `INSERT INTO pushed_image (run_id, position, target, registry, repository, digest, tags)
				VALUES ($1, $2, $3, $4, $5, $6, $7)`, id, i, p.Target, p.Registry, p.Repository, p.Digest, orEmpty(p.Tags)); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE run SET status = $2, exit_code = $3, reason = $4, finished_at = now(),
				lease_expires = NULL, stop_reason = NULL WHERE id = $1`,
			id, status, exitCode, reason); err != nil {
			return err
		}
		if plan != nil {
			if err := s.queuePlan(ctx, tx, run, plan); err != nil {
				return err
			}
		}
		_, err = notify(ctx, tx, chanDone, id)
		return err
	})
	if err != nil {
		return err
	}
	// The log moves after the outcome is recorded; a failed move is the sweep's to retry.
	return s.storeLog(ctx, id)
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
// queued again, once, with its log cleared, and then ends in error.
func (s *Store) sweepLeases(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `UPDATE run SET
				status = CASE WHEN stop_reason IS NOT NULL THEN $2::smallint
					WHEN attempt < $3 THEN $4::smallint ELSE $5::smallint END,
				reason = CASE WHEN stop_reason IS NOT NULL THEN stop_reason
					WHEN attempt < $3 THEN 'its worker ' || worker || ' was lost; it runs again'
					ELSE 'its worker ' || worker || ' was lost, on its last attempt' END,
				attempt = CASE WHEN stop_reason IS NULL AND attempt < $3 THEN attempt + 1 ELSE attempt END,
				finished_at = CASE WHEN stop_reason IS NULL AND attempt < $3 THEN NULL ELSE now() END,
				started_at = CASE WHEN stop_reason IS NULL AND attempt < $3 THEN NULL ELSE started_at END,
				worker = CASE WHEN stop_reason IS NULL AND attempt < $3 THEN NULL ELSE worker END,
				log_size = CASE WHEN stop_reason IS NULL AND attempt < $3 THEN 0 ELSE log_size END,
				lease_expires = NULL, stop_reason = NULL
			WHERE status = $1 AND lease_expires < now()
			RETURNING id, status`, StatusRunning, StatusCanceled, s.cfg.Attempts, StatusQueued, StatusError)
		if err != nil {
			return err
		}
		type swept struct {
			id     int64
			status Status
		}
		var all []swept
		for rows.Next() {
			var w swept
			if err := rows.Scan(&w.id, &w.status); err != nil {
				rows.Close()
				return err
			}
			all = append(all, w)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, w := range all {
			if w.status == StatusQueued {
				// The lost attempt's log goes; the next attempt writes its own from the start.
				if _, err := tx.Exec(ctx, `DELETE FROM log_chunk WHERE run_id = $1`, w.id); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `SELECT pg_notify($1, '')`, chanQueue); err != nil {
					return err
				}
				continue
			}
			if _, err := notify(ctx, tx, chanDone, w.id); err != nil {
				return err
			}
		}
		return nil
	})
}
