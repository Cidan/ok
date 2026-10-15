package store

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Notification channels: a run was queued; a run's log grew (payload: its ID); a run
// finished (payload: its ID); something the landing queue acts on changed — a submission, a
// post plan, main's history (payload: the repository's ID).
const (
	chanQueue      = "ok_automation_queue"
	chanLog        = "ok_automation_log"
	chanDone       = "ok_automation_done"
	chanSubmission = "ok_automation_submission"
)

// notify sends a notification about a run, delivered when tx commits.
func notify(ctx context.Context, tx pgx.Tx, channel string, id int64) (pgconn.CommandTag, error) {
	return tx.Exec(ctx, `SELECT pg_notify($1, $2)`, channel, strconv.FormatInt(id, 10))
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
	var priority int16
	if req.Origin.Phase == PhaseDeploy {
		priority = deployPriority
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
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		run, err = scanRun(tx.QueryRow(ctx, `INSERT INTO run (repository_id, commit_id, kind, targets, args,
				phase, cl_number, patchset, job, requested_by, status, timeout_seconds, priority)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			RETURNING `+runColumns,
			req.RepositoryID, req.CommitID, req.Job.Kind, orEmpty(req.Job.Targets), orEmpty(req.Job.Args),
			req.Origin.Phase, nullable(req.Origin.CLNumber), nullable(req.Origin.Patchset),
			req.Origin.Job, req.RequestedBy, StatusQueued, int32(timeout/time.Second), priority))
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `SELECT pg_notify($1, '')`, chanQueue)
		return err
	})
	return run, err
}

// Cancel ends a queued run at once, and asks a running run's worker to stop it; a finished
// run is returned as it is. A post run is its submission's: canceling the submission stops it.
func (s *Store) Cancel(ctx context.Context, id int64, reason string) (*Run, error) {
	if reason == "" {
		reason = "canceled"
	}
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var phase Phase
		if err := tx.QueryRow(ctx, `SELECT phase FROM run WHERE id = $1`, id).Scan(&phase); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if phase == PhasePost {
			return &ConflictError{Message: "a post run is its submission's: cancel the submission"}
		}
		return cancelRun(ctx, tx, id, reason)
	})
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, id)
}

// cancelRun ends a queued run canceled, or asks a running run's worker to stop it.
func cancelRun(ctx context.Context, tx pgx.Tx, id int64, reason string) error {
	// A queued run has no log: it is stored, empty, as it ends.
	tag, err := tx.Exec(ctx, `UPDATE run SET status = $2, reason = $3, finished_at = now(), log_stored = true
		WHERE id = $1 AND status = $4`, id, StatusCanceled, reason, StatusQueued)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		_, err = notify(ctx, tx, chanDone, id)
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE run SET stop_reason = $2 WHERE id = $1 AND status = $3 AND stop_reason IS NULL`,
		id, reason, StatusRunning)
	return err
}

// Rerun queues a finished run's job on its commit again, as a new run.
func (s *Store) Rerun(ctx context.Context, id int64, requestedBy string) (*Run, error) {
	if requestedBy == "" {
		return nil, invalid("a run is requested by someone")
	}
	var run *Run
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		prev, err := scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM run WHERE id = $1`, id))
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
			var planned bool
			if err := tx.QueryRow(ctx, `SELECT planned FROM plan WHERE id = $1 FOR UPDATE`, prev.PlanID).Scan(&planned); err != nil {
				return err
			}
			if planned {
				return &ConflictError{Message: "the patchset is planned: run its jobs again instead"}
			}
		}
		run, err = scanRun(tx.QueryRow(ctx, `INSERT INTO run (repository_id, commit_id, kind, targets, args,
				phase, cl_number, patchset, job, plan_id, why, requested_by, rerun_of, status, timeout_seconds)
			SELECT repository_id, commit_id, kind, targets, args, phase, cl_number, patchset, job, plan_id, why,
				$2, id, $3, timeout_seconds
			FROM run WHERE id = $1
			RETURNING `+runColumns, id, requestedBy, StatusQueued))
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `SELECT pg_notify($1, '')`, chanQueue)
		return err
	})
	return run, err
}

// MainCommits returns each repository's newest n commits on main, as automation has seen main
// move (main_commit), newest first.
func (s *Store) MainCommits(ctx context.Context, n int) (map[int64][][]byte, error) {
	rows, err := s.pool.Query(ctx, `SELECT repository_id, commit_id FROM (
			SELECT repository_id, commit_id, row_number() OVER (PARTITION BY repository_id ORDER BY seq DESC) AS r
			FROM main_commit) m
		WHERE r <= $1 ORDER BY repository_id, r`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64][][]byte{}
	for rows.Next() {
		var repo int64
		var commit []byte
		if err := rows.Scan(&repo, &commit); err != nil {
			return nil, err
		}
		out[repo] = append(out[repo], commit)
	}
	return out, rows.Err()
}

// RunCommits returns the commits of every run queued or running, or finished since since, by
// repository.
func (s *Store) RunCommits(ctx context.Context, since time.Time) (map[int64][][]byte, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT repository_id, commit_id FROM run
		WHERE status IN ($1, $2) OR finished_at >= $3`, StatusQueued, StatusRunning, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64][][]byte{}
	for rows.Next() {
		var repo int64
		var commit []byte
		if err := rows.Scan(&repo, &commit); err != nil {
			return nil, err
		}
		out[repo] = append(out[repo], commit)
	}
	return out, rows.Err()
}
