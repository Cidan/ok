// Package spannerstore is automation's store backend (internal/automation/store) on Spanner, in a
// database of its own (docs/hosted.md, "Spanner"). Every write is DML, so a transaction reads what
// it wrote, as on PostgreSQL; an operation outside a transaction is a transaction of its own
// (spannerdata.Ops). Spanner has no SKIP LOCKED: a queued run is claimed by reading the queue's
// head and moving it to running in one transaction, two claims at once meeting there and Spanner
// running one again, after the other.
package spannerstore

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/lock/spannerlock"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are automation's schema on Spanner.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Backend is automation's store backend on Spanner.
type Backend struct {
	queries
	moves *move.SpannerRows
}

var _ store.Backend = (*Backend)(nil)

// Work is the backend's keyed work's store.
func (b *Backend) Work() work.Store { return spannerdata.Work(b.Client) }

func (b *Backend) Waits() wait.Store { return spannerdata.Waits(b.Client) }

// Open brings the database client reaches to automation's schema, through admin, and returns a
// backend over it.
func Open(ctx context.Context, admin *database.DatabaseAdminClient, client *spanner.Client) (*Backend, error) {
	if _, err := spannerdata.Migrate(ctx, admin, client, Migrations); err != nil {
		return nil, err
	}
	return &Backend{queries: queries{spannerdata.Ops{Client: client, Gate: move.SpannerGate}},
		moves: &move.SpannerRows{Client: client, Tables: moveTables, Where: moveWhere, Renames: moveRenames}}, nil
}

func (b *Backend) Txn(ctx context.Context, fn func(store.Ops) error) error {
	return spannerdata.Txn(ctx, b.Client, func(_ context.Context, tx *spanner.ReadWriteTransaction) error {
		if err := move.SpannerGate(ctx, tx); err != nil {
			return err
		}
		return fn(queries{spannerdata.Ops{Client: b.Client, Tx: tx}})
	})
}

// moveTables are the tables of rows about repositories this cell holds, which a move copies
// (docs/hosted.md, "Cells, as built"): their plans, runs, submissions, main's commits, and where
// the SCM's feed of each was applied to.
var moveTables = []string{"plan", "run", "pushed_image", "submission", "submission_cl", "attempt", "attempt_candidate", "main_commit",
	"landed_change", "follow_cursor"}

// moveRenames are the columns this schema names otherwise than PostgreSQL's.
var moveRenames = spannerdata.Renames{
	"plan":       {"id": "plan_id"},
	"run":        {"id": "run_id"},
	"submission": {"id": "submission_id"},
	"attempt":    {"id": "attempt_id"},
}

// moveWhere is the condition a table's rows about repositories (@repos) meet, by Spanner's names.
func moveWhere(table string) string {
	const submissions = "SELECT submission_id FROM submission WHERE repository_id IN UNNEST(@repos)"
	switch table {
	case "pushed_image":
		return "run_id IN (SELECT run_id FROM run WHERE repository_id IN UNNEST(@repos))"
	case "submission_cl", "attempt":
		return "submission_id IN (" + submissions + ")"
	case "attempt_candidate":
		return "attempt_id IN (SELECT attempt_id FROM attempt WHERE submission_id IN (" + submissions + "))"
	case "follow_cursor":
		return "topic = '" + scmfeed.Topic + "' AND key IN UNNEST(@repos)"
	}
	return "repository_id IN UNNEST(@repos)"
}

// Moves are the backend's rows about repositories, for a move.
func (b *Backend) Moves() move.Rows { return b.moves }

// FencedWritesBesideEachOther is true: a work fence reads the holder's cell, which a mark does not
// write (spannerdata.CheckWorkFence).
func (b *Backend) FencedWritesBesideEachOther() bool { return true }

func (x queries) RunningRuns(ctx context.Context, repos []int64) ([]int64, error) {
	if repos == nil {
		repos = []int64{}
	}
	var out []int64
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT run_id FROM run WHERE repository_id IN UNNEST(@repos) AND status = @running ORDER BY run_id`,
			map[string]any{"repos": repos, "running": int64(store.StatusRunning)}, func(row *spanner.Row) error {
				var id int64
				if err := row.Columns(&id); err != nil {
					return err
				}
				out = append(out, id)
				return nil
			})
	})
	return out, err
}

// Gate reads a repository's move gate in the operation's transaction.
func (x queries) Gate(ctx context.Context, repo int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return move.SpannerGateRepository(ctx, tx, repo) })
}

// Snapshot runs fn in a read-only transaction: every read in it at one moment.
func (b *Backend) Snapshot(ctx context.Context, fn func(store.Ops) error) error {
	ro := b.Client.ReadOnlyTransaction()
	defer ro.Close()
	return fn(queries{spannerdata.Ops{Client: b.Client, Snapshot: ro}})
}

// queries are the backend's operations, in a transaction, a snapshot, or each in one of its own.
type queries struct{ spannerdata.Ops }

// nullable is a positive number, or NULL.
func nullable[T int16 | int32 | int64](v T) spanner.NullInt64 {
	return spanner.NullInt64{Int64: int64(v), Valid: v > 0}
}

func nullString(s string) spanner.NullString { return spanner.NullString{StringVal: s, Valid: s != ""} }

func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func (x queries) ids(ctx context.Context, sql string, params map[string]any) ([]int64, error) {
	var out []int64
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, sql, params, func(row *spanner.Row) error {
			var id int64
			if err := row.Columns(&id); err != nil {
				return err
			}
			out = append(out, id)
			return nil
		})
	})
	return out, err
}

// returningIDs runs DML with THEN RETURN of an ID, in ID order.
func (x queries) returningIDs(ctx context.Context, sql string, params map[string]any) ([]int64, error) {
	var out []int64
	err := x.Returning(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		var id int64
		if err := row.Columns(&id); err != nil {
			return err
		}
		out = append(out, id)
		return nil
	})
	slices.Sort(out)
	return out, err
}

func (x queries) commitsByRepository(ctx context.Context, sql string, params map[string]any) (map[int64][][]byte, error) {
	out := map[int64][][]byte{}
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, sql, params, func(row *spanner.Row) error {
			var repo int64
			var commit []byte
			if err := row.Columns(&repo, &commit); err != nil {
				return err
			}
			out[repo] = append(out[repo], commit)
			return nil
		})
	})
	return out, err
}

// ─── Purging ───

func (x queries) RunIDsOf(ctx context.Context, repo, after int64, limit int) ([]int64, error) {
	return x.ids(ctx, `SELECT run_id FROM run WHERE repository_id = @repo AND run_id > @after ORDER BY run_id LIMIT @limit`,
		map[string]any{"repo": repo, "after": after, "limit": int64(limit)})
}

// DeleteRepository deletes a repository's rows by partitioned DML, each statement apart from any
// transaction: a repository's rows are too many for one transaction's mutations, and a purged
// repository is written no more, so a statement run again deletes nothing more.
func (x queries) DeleteRepository(ctx context.Context, repo int64) error {
	p := map[string]any{"repo": repo, "plans": strconv.FormatInt(repo, 10) + "/", "main": strconv.FormatInt(repo, 10)}
	for _, sql := range []string{
		`DELETE FROM main_commit WHERE repository_id = @repo`,
		`DELETE FROM landed_change WHERE repository_id = @repo`,
		`DELETE FROM attempt WHERE repository_id = @repo`,
		`DELETE FROM submission WHERE repository_id = @repo`,
		`DELETE FROM account_slot WHERE repository_id = @repo`,
		`DELETE FROM run WHERE repository_id = @repo`,
		`DELETE FROM plan WHERE repository_id = @repo`,
		`DELETE FROM locks WHERE bucket = 'plans' AND STARTS_WITH(id, @plans)`, // its CLs' (LockCL)
		`DELETE FROM locks WHERE bucket = 'main' AND id = @main`,               // its main's (LockMain)
	} {
		if _, err := x.Client.PartitionedUpdate(ctx, spanner.Statement{SQL: sql, Params: p}); err != nil {
			return fmt.Errorf("purging repository %d: %w", repo, err)
		}
	}
	return nil
}

// ─── Runs ───

// runColumns are a run's columns, in scanRun's order.
const runColumns = `run_id, repository_id, commit_id, kind, targets, args, phase, IFNULL(cl_number, 0), IFNULL(patchset, 0), job,
	requested_by, IFNULL(rerun_of, 0), status, attempt, IFNULL(worker, ''), IFNULL(exit_code, 0), reason, log_size,
	log_stored, timeout_seconds, IFNULL(plan_id, 0), why, queued_at, started_at, finished_at, IFNULL(carried_from, 0),
	IFNULL(canceled_by, ''), pool, IFNULL(dispatch_job, ''), lease_handle`

func scanRun(row *spanner.Row) (*store.Run, error) {
	var r store.Run
	var kind, phase, patchset, status, attempt, exitCode, timeout int64
	var started, finished spanner.NullTime
	if err := row.Columns(&r.ID, &r.RepositoryID, &r.CommitID, &kind, &r.Job.Targets, &r.Job.Args,
		&phase, &r.Origin.CLNumber, &patchset, &r.Origin.Job, &r.RequestedBy, &r.RerunOf, &status, &attempt, &r.Worker,
		&exitCode, &r.Reason, &r.LogSize, &r.LogStored, &timeout, &r.PlanID, &r.Why, &r.QueuedAt, &started, &finished,
		&r.CarriedFrom, &r.CanceledBy, &r.Job.Pool, &r.DispatchJob, &r.LeaseHandle); err != nil {
		return nil, err
	}
	r.Job.Kind, r.Origin.Phase, r.Origin.Patchset, r.Status, r.Attempt = store.Kind(kind), store.Phase(phase), int32(patchset), store.Status(status), int32(attempt)
	r.ExitCode, r.TimeoutSeconds, r.StartedAt, r.FinishedAt = int32(exitCode), int32(timeout), started.Time, finished.Time
	if r.Job.Targets == nil {
		r.Job.Targets = []string{}
	}
	if r.Job.Args == nil {
		r.Job.Args = []string{}
	}
	return &r, nil
}

func runs(ctx context.Context, r spannerdata.Reader, sql string, params map[string]any) ([]*store.Run, error) {
	var out []*store.Run
	err := spannerdata.Each(ctx, r, sql, params, func(row *spanner.Row) error {
		run, err := scanRun(row)
		if err != nil {
			return err
		}
		out = append(out, run)
		return nil
	})
	return out, err
}

func (x queries) readRuns(ctx context.Context, sql string, params map[string]any) ([]*store.Run, error) {
	var out []*store.Run
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		out, err = runs(ctx, r, sql, params)
		return err
	})
	return out, err
}

func oneRun(out []*store.Run, err error) (*store.Run, error) {
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, store.ErrNotFound
	}
	return out[0], nil
}

// insertRun makes a run under a random ID. Its queue time is the clock's as the statement runs, so
// the runs one transaction queues keep their order.
func insertRun(ctx context.Context, tx *spanner.ReadWriteTransaction, r store.NewRun) (int64, error) {
	id := data.NewID()
	_, err := spannerdata.Update(ctx, tx, `INSERT INTO run (run_id, repository_id, commit_id, kind, targets, args, phase, cl_number, patchset, job,
			plan_id, why, requested_by, rerun_of, status, attempt, reason, timeout_seconds, priority,
			started_at, finished_at, log_stored, carried_from, log_size, queued_at, pool, dispatch_job)
		VALUES (@id, @repo, @commit, @kind, @targets, @args, @phase, @cl, @patchset, @job, @plan, @why, @by, @rerun, @status, 1, @reason,
			@timeout, @priority, IF(@started, CURRENT_TIMESTAMP(), NULL), IF(@finished, CURRENT_TIMESTAMP(), NULL), @stored, @carried, @logSize,
			CURRENT_TIMESTAMP(), @pool, @dispatch)`,
		map[string]any{"id": id, "repo": r.RepositoryID, "commit": r.CommitID, "kind": int64(r.Job.Kind), "targets": orEmpty(r.Job.Targets),
			"args": orEmpty(r.Job.Args), "phase": int64(r.Origin.Phase), "cl": nullable(r.Origin.CLNumber), "patchset": nullable(r.Origin.Patchset),
			"job": r.Origin.Job, "plan": nullable(r.PlanID), "why": r.Why, "by": r.RequestedBy, "rerun": nullable(r.RerunOf),
			"status": int64(r.Status), "reason": r.Reason, "timeout": int64(r.TimeoutSeconds), "priority": int64(r.Priority),
			"started": r.Started, "finished": r.Finished, "stored": r.LogStored, "carried": nullable(r.CarriedFrom), "logSize": r.LogSize,
			"pool": poolOf(r.Job.Pool), "dispatch": spanner.NullString{StringVal: r.DispatchJob, Valid: r.DispatchJob != ""}})
	return id, err
}

func (x queries) InsertRun(ctx context.Context, r store.NewRun) (*store.Run, error) {
	var out *store.Run
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		id, err := insertRun(ctx, tx, r)
		if err != nil {
			return err
		}
		out, err = oneRun(runs(ctx, tx, `SELECT `+runColumns+` FROM run WHERE run_id = @id`, map[string]any{"id": id}))
		return err
	})
	return out, err
}

func (x queries) InsertRuns(ctx context.Context, rs []store.NewRun) ([]int64, error) {
	if len(rs) == 0 {
		return nil, nil
	}
	var ids []int64
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		ids = make([]int64, len(rs))
		for i, r := range rs {
			id, err := insertRun(ctx, tx, r)
			if err != nil {
				return err
			}
			ids[i] = id
		}
		return nil
	})
	return ids, err
}

func (x queries) Run(ctx context.Context, id int64) (*store.Run, error) {
	return oneRun(x.readRuns(ctx, `SELECT `+runColumns+` FROM run WHERE run_id = @id`, map[string]any{"id": id}))
}

func (x queries) LockRun(ctx context.Context, id int64) (*store.Run, error) {
	var out *store.Run
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		out, err = oneRun(runs(ctx, tx, `SELECT `+runColumns+` FROM run WHERE run_id = @id FOR UPDATE`, map[string]any{"id": id}))
		return err
	})
	return out, err
}

// before is a page's cursor: runs queued before the run before (0: from the newest).
const before = `(@before = 0 OR queued_at < (SELECT queued_at FROM run WHERE run_id = @before)
	OR (queued_at = (SELECT queued_at FROM run WHERE run_id = @before) AND run_id < @before))`

func (x queries) RunsOfCL(ctx context.Context, repo, cl int64, patchset int32, beforeID int64, limit int) ([]*store.Run, error) {
	return x.readRuns(ctx, `SELECT `+runColumns+` FROM run
		WHERE repository_id = @repo AND cl_number = @cl AND (@patchset = 0 OR patchset = @patchset) AND `+before+`
		ORDER BY queued_at DESC, run_id DESC LIMIT @limit`,
		map[string]any{"repo": repo, "cl": cl, "patchset": int64(patchset), "before": beforeID, "limit": int64(limit)})
}

func (x queries) RunsOfCommit(ctx context.Context, repo int64, commit []byte, beforeID int64, limit int) ([]*store.Run, error) {
	return x.readRuns(ctx, `SELECT `+runColumns+` FROM run WHERE repository_id = @repo AND commit_id = @commit AND `+before+`
		ORDER BY queued_at DESC, run_id DESC LIMIT @limit`,
		map[string]any{"repo": repo, "commit": commit, "before": beforeID, "limit": int64(limit)})
}

func (x queries) PlanRuns(ctx context.Context, plans []int64) ([]*store.Run, error) {
	return x.readRuns(ctx, `SELECT `+runColumns+` FROM run WHERE plan_id IN UNNEST(@plans) ORDER BY queued_at, run_id`,
		map[string]any{"plans": plans})
}

func (x queries) Pushed(ctx context.Context, ids []int64) (map[int64][]store.Pushed, error) {
	out := map[int64][]store.Pushed{}
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, `SELECT run_id, target, registry, repository, digest, tags FROM pushed_image
			WHERE run_id IN UNNEST(@runs) ORDER BY run_id, position`, map[string]any{"runs": ids}, func(row *spanner.Row) error {
			var id int64
			var p store.Pushed
			if err := row.Columns(&id, &p.Target, &p.Registry, &p.Repository, &p.Digest, &p.Tags); err != nil {
				return err
			}
			out[id] = append(out[id], p)
			return nil
		})
	})
	return out, err
}

func (x queries) InsertPushed(ctx context.Context, run int64, pushed []store.Pushed) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		for i, p := range pushed {
			if _, err := spannerdata.Update(ctx, tx, `INSERT INTO pushed_image (run_id, position, target, registry, repository, digest, tags)
				VALUES (@run, @position, @target, @registry, @repository, @digest, @tags)`,
				map[string]any{"run": run, "position": int64(i), "target": p.Target, "registry": p.Registry, "repository": p.Repository,
					"digest": p.Digest, "tags": orEmpty(p.Tags)}); err != nil {
				return err
			}
		}
		return nil
	})
}

func micros(d time.Duration) int64 { return d.Microseconds() }

func jsonMarshal(v any) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}

func jsonUnmarshal(s string, into any) error { return json.Unmarshal([]byte(s), into) }

func (x queries) RenewLease(ctx context.Context, id int64, worker string, ttl time.Duration) (store.Lease, bool, error) {
	var l store.Lease
	var ok bool
	err := x.Returning(ctx, `UPDATE run SET lease_expires = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl MICROSECOND)
		WHERE run_id = @id AND status = @running AND worker = @worker
		THEN RETURN stop_reason, CURRENT_TIMESTAMP() > TIMESTAMP_ADD(started_at, INTERVAL timeout_seconds SECOND), timeout_seconds, lease_handle`,
		map[string]any{"id": id, "worker": worker, "ttl": micros(ttl), "running": int64(store.StatusRunning)},
		func() { l, ok = store.Lease{}, false },
		func(row *spanner.Row) error {
			var stop spanner.NullString
			var timeout int64
			if err := row.Columns(&stop, &l.Overdue, &timeout, &l.Handle); err != nil {
				return err
			}
			l.Stopping, l.StopReason, l.TimeoutSeconds, ok = stop.Valid, stop.StringVal, int32(timeout), true
			return nil
		})
	return l, ok, err
}

func (x queries) RequeueRun(ctx context.Context, id int64, job, reason string, nextAttempt bool) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if err := stopRunning(ctx, tx, id); err != nil {
			return err
		}
		_, err := spannerdata.Update(ctx, tx, `UPDATE run SET status = @queued, reason = @reason, attempt = attempt + IF(@next, 1, 0),
				worker = NULL, lease_expires = NULL, started_at = NULL, finished_at = NULL, stop_reason = NULL, log_size = 0,
				slot_account = NULL, slot = NULL, dispatch_job = @job, lease_handle = NULL
			WHERE run_id = @id`, map[string]any{"id": id, "queued": int64(store.StatusQueued), "reason": reason, "next": nextAttempt,
			"job": spanner.NullString{StringVal: job, Valid: job != ""}})
		return err
	})
}

func (x queries) FinishRun(ctx context.Context, id int64, status store.Status, exitCode int32, reason string) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if err := stopRunning(ctx, tx, id); err != nil {
			return err
		}
		_, err := spannerdata.Update(ctx, tx, `UPDATE run SET status = @status, exit_code = @exit, reason = @reason, finished_at = CURRENT_TIMESTAMP(),
			lease_expires = NULL, stop_reason = NULL, slot_account = NULL, slot = NULL, lease_handle = NULL WHERE run_id = @id`,
			map[string]any{"id": id, "status": int64(status), "exit": int64(exitCode), "reason": reason})
		return err
	})
}

func (x queries) CancelQueuedRun(ctx context.Context, id int64, reason, by string) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE run SET status = @canceled, reason = @reason, finished_at = CURRENT_TIMESTAMP(), log_stored = TRUE,
		canceled_by = @by WHERE run_id = @id AND status = @queued`,
		map[string]any{"id": id, "canceled": int64(store.StatusCanceled), "reason": reason, "queued": int64(store.StatusQueued), "by": nullString(by)})
	return n > 0, err
}

func (x queries) RequestStop(ctx context.Context, id int64, reason, by string) error {
	_, err := x.Exec(ctx, `UPDATE run SET stop_reason = @reason, canceled_by = @by
		WHERE run_id = @id AND status = @running AND stop_reason IS NULL`,
		map[string]any{"id": id, "reason": reason, "running": int64(store.StatusRunning), "by": nullString(by)})
	return err
}

func (x queries) StopRequest(ctx context.Context, id int64) (bool, string, error) {
	var stop spanner.NullString
	var ok bool
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT stop_reason FROM run WHERE run_id = @id`, map[string]any{"id": id}, &stop)
		return err
	})
	if err != nil {
		return false, "", err
	}
	if !ok {
		return false, "", store.ErrNotFound
	}
	return stop.Valid, stop.StringVal, nil
}

// ─── Logs ───

func (x queries) AdvanceLog(ctx context.Context, id int64, worker string, started time.Time, from, to int64) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE run SET log_size = @to
		WHERE run_id = @id AND status = @running AND worker = @worker AND started_at = @started AND log_size = @from`,
		map[string]any{"id": id, "worker": worker, "started": started, "from": from, "to": to, "running": int64(store.StatusRunning)})
	return n > 0, err
}

func (x queries) MarkLogStored(ctx context.Context, id int64) error {
	_, err := x.Exec(ctx, `UPDATE run SET log_stored = TRUE WHERE run_id = @id`, map[string]any{"id": id})
	return err
}

func (x queries) UnstoredLogs(ctx context.Context, repos []int64, limit int) ([]int64, error) {
	if repos == nil {
		repos = []int64{}
	}
	return x.ids(ctx, `SELECT run_id FROM run WHERE NOT log_stored AND status >= @passed
		AND (ARRAY_LENGTH(@repos) = 0 OR repository_id IN UNNEST(@repos))
		AND NOT EXISTS (SELECT 1 FROM move_gate g WHERE g.repository_id = run.repository_id AND g.move_state != '')
		ORDER BY run_id LIMIT @limit`,
		map[string]any{"passed": int64(store.StatusPassed), "repos": repos, "limit": int64(limit)})
}

// ─── Keyed work ───

func (x queries) MarkWork(ctx context.Context, kind string, key int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.MarkWork(ctx, tx, kind, key) })
}

func (x queries) MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.MarkWorkAt(ctx, tx, kind, key, at) })
}

// CLsOf reads the runs' plans by key, then the plans by key.
func (x queries) CLsOf(ctx context.Context, runs, plans []int64) ([][2]int64, error) {
	ids := slices.Clone(plans)
	if len(runs) > 0 {
		got, err := x.ids(ctx, `SELECT plan_id FROM run WHERE run_id IN UNNEST(@runs) AND plan_id IS NOT NULL`, map[string]any{"runs": runs})
		if err != nil {
			return nil, err
		}
		ids = append(ids, got...)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	seen := map[[2]int64]bool{}
	var out [][2]int64
	err := x.Read(func(r spannerdata.Reader) error {
		return spannerdata.Each(ctx, r, `SELECT repository_id, cl_number FROM plan WHERE plan_id IN UNNEST(@ids) AND cl_number > 0`,
			map[string]any{"ids": ids}, func(row *spanner.Row) error {
				var c [2]int64
				if err := row.Columns(&c[0], &c[1]); err != nil {
					return err
				}
				if !seen[c] {
					seen[c] = true
					out = append(out, c)
				}
				return nil
			})
	})
	return out, err
}

func (x queries) PendStep(ctx context.Context, kind string, key int64, at time.Time, body []byte) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.PendStep(tx, kind, key, at, body) })
}

// ─── The feed, and the build cache's commits ───

func (x queries) FollowCursors(ctx context.Context, topic string, keys []int64) (map[int64]int64, error) {
	var out map[int64]int64
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		out, err = spannerdata.Cursors(ctx, r, topic, keys)
		return err
	})
	return out, err
}

func (x queries) FollowCursor(ctx context.Context, topic string, key int64) (int64, error) {
	var seq int64
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		seq, err = spannerdata.Cursor(ctx, r, topic, key)
		return err
	})
	return seq, err
}

func (x queries) AdvanceFollowCursor(ctx context.Context, topic string, key, seq int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return spannerdata.AdvanceCursor(ctx, tx, topic, key, seq)
	})
}

func (x queries) MainCommits(ctx context.Context, n int) (map[int64][][]byte, error) {
	out := map[int64][][]byte{}
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, `SELECT repository_id,
				ARRAY(SELECT m.commit_id FROM main_commit m WHERE m.repository_id = r.repository_id ORDER BY m.seq DESC LIMIT @n)
			FROM (SELECT DISTINCT repository_id FROM main_commit) r ORDER BY repository_id`, map[string]any{"n": int64(n)},
			func(row *spanner.Row) error {
				var repo int64
				var commits [][]byte
				if err := row.Columns(&repo, &commits); err != nil {
					return err
				}
				out[repo] = commits
				return nil
			})
	})
	return out, err
}

func (x queries) RunCommits(ctx context.Context, since time.Time) (map[int64][][]byte, error) {
	return x.commitsByRepository(ctx, `SELECT DISTINCT repository_id, commit_id FROM run
		WHERE status IN (@queued, @running) OR finished_at >= @since`,
		map[string]any{"queued": int64(store.StatusQueued), "running": int64(store.StatusRunning), "since": since})
}

// ─── Plans ───

func (x queries) LockCL(ctx context.Context, repo, cl int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return spannerlock.InTx(ctx, tx, "plans", fmt.Sprintf("%d/%d", repo, cl))
	})
}

const planColumns = `plan_id, repository_id, cl_number, patchset, phase, commit_id, workflows_commit, context, touched,
	planned, jobs, skipped, affected, upstream, created_at, pre_commit`

func scanPlan(row *spanner.Row) (*store.Plan, error) {
	var p store.Plan
	var patchset, phase int64
	var change, touched, jobs, skipped, affected, upstream string
	if err := row.Columns(&p.ID, &p.RepositoryID, &p.CLNumber, &patchset, &phase, &p.CommitID, &p.WorkflowsCommit,
		&change, &touched, &p.Planned, &jobs, &skipped, &affected, &upstream, &p.CreatedAt, &p.PreCommit); err != nil {
		return nil, err
	}
	p.Patchset, p.Phase, p.Context = int32(patchset), store.Phase(phase), []byte(change)
	for _, f := range []struct {
		data string
		into any
		what string
	}{{touched, &p.Touched, "touched paths"}, {jobs, &p.Jobs, "jobs"}, {skipped, &p.Skipped, "skipped jobs"}, {affected, &p.Affected, "affected targets"}, {upstream, &p.Upstream, "upstream targets"}} {
		if err := jsonUnmarshal(f.data, f.into); err != nil {
			return nil, fmt.Errorf("plan %d's %s: %w", p.ID, f.what, err)
		}
	}
	return &p, nil
}

func plans(ctx context.Context, r spannerdata.Reader, sql string, params map[string]any) ([]*store.Plan, error) {
	var out []*store.Plan
	err := spannerdata.Each(ctx, r, sql, params, func(row *spanner.Row) error {
		p, err := scanPlan(row)
		if err != nil {
			return err
		}
		out = append(out, p)
		return nil
	})
	return out, err
}

func onePlan(out []*store.Plan, err error) (*store.Plan, error) {
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, store.ErrNotFound
	}
	return out[0], nil
}

func (x queries) readPlans(ctx context.Context, sql string, params map[string]any) ([]*store.Plan, error) {
	var out []*store.Plan
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		out, err = plans(ctx, r, sql, params)
		return err
	})
	return out, err
}

func (x queries) InsertPlan(ctx context.Context, p store.NewPlan) (*store.Plan, error) {
	touched, err := jsonMarshal(orEmpty(p.Touched))
	if err != nil {
		return nil, err
	}
	id := data.NewID()
	var out *store.Plan
	err = x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO plan (plan_id, repository_id, cl_number, patchset, phase, commit_id,
				workflows_commit, context, touched, planned, jobs, skipped, affected, upstream, pre_commit, created_at)
			VALUES (@id, @repo, @cl, @patchset, @phase, @commit, @workflows, @context, @touched, FALSE, '[]', '[]', '[]', '[]', @pre,
				CURRENT_TIMESTAMP())`,
			map[string]any{"id": id, "repo": p.RepositoryID, "cl": p.CLNumber, "patchset": int64(p.Patchset), "phase": int64(p.Phase),
				"commit": p.CommitID, "workflows": p.WorkflowsCommit, "context": string(p.Context), "touched": touched, "pre": p.PreCommit}); err != nil {
			return err
		}
		var err error
		out, err = onePlan(plans(ctx, tx, `SELECT `+planColumns+` FROM plan WHERE plan_id = @id`, map[string]any{"id": id}))
		return err
	})
	return out, err
}

func (x queries) Plan(ctx context.Context, id int64) (*store.Plan, error) {
	return onePlan(x.readPlans(ctx, `SELECT `+planColumns+` FROM plan WHERE plan_id = @id`, map[string]any{"id": id}))
}

func (x queries) LockPlan(ctx context.Context, id int64) (*store.Plan, error) {
	var out *store.Plan
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		out, err = onePlan(plans(ctx, tx, `SELECT `+planColumns+` FROM plan WHERE plan_id = @id FOR UPDATE`, map[string]any{"id": id}))
		return err
	})
	return out, err
}

func (x queries) Plans(ctx context.Context, ids []int64) ([]*store.Plan, error) {
	return x.readPlans(ctx, `SELECT `+planColumns+` FROM plan WHERE plan_id IN UNNEST(@ids) ORDER BY created_at, plan_id`, map[string]any{"ids": ids})
}

func (x queries) PrePlan(ctx context.Context, repo, cl int64, patchset int32) (*store.Plan, error) {
	return onePlan(x.readPlans(ctx, `SELECT `+planColumns+` FROM plan
		WHERE repository_id = @repo AND cl_number = @cl AND patchset = @patchset AND phase = @pre`,
		map[string]any{"repo": repo, "cl": cl, "patchset": int64(patchset), "pre": int64(store.PhasePre)}))
}

func (x queries) MaxPrePatchset(ctx context.Context, repo, cl int64) (int32, error) {
	var n int64
	err := x.Read(func(r spannerdata.Reader) error {
		_, err := spannerdata.First(ctx, r, `SELECT IFNULL(MAX(patchset), 0) FROM plan WHERE repository_id = @repo AND cl_number = @cl AND phase = @pre`,
			map[string]any{"repo": repo, "cl": cl, "pre": int64(store.PhasePre)}, &n)
		return err
	})
	return int32(n), err
}

func (x queries) SetPlanned(ctx context.Context, id int64, r store.PlanResult) error {
	var enc [4]string
	for i, v := range []any{orEmpty(r.Jobs), orEmpty(r.Skipped), orEmpty(r.Affected), orEmpty(r.Upstream)} {
		b, err := jsonMarshal(v)
		if err != nil {
			return err
		}
		enc[i] = b
	}
	_, err := x.Exec(ctx, `UPDATE plan SET planned = TRUE, jobs = @jobs, skipped = @skipped, affected = @affected, upstream = @upstream
		WHERE plan_id = @id`, map[string]any{"id": id, "jobs": enc[0], "skipped": enc[1], "affected": enc[2], "upstream": enc[3]})
	return err
}

// The statements over a CL's pre runs read them through run_cl, by the CL. Unhinted, Spanner reads
// them through run_leased, by status — every queued or running run of every repository — and a
// read-write transaction locks what it reads: as every plan's transaction inserts a queued run and
// cancels the CL's queued pre, plans of different repositories at once aborted each other, one
// committing a round.
const (
	cancelQueuedPre = `UPDATE run@{FORCE_INDEX=run_cl} SET status = @canceled, reason = @reason, finished_at = CURRENT_TIMESTAMP(),
		log_stored = TRUE
		WHERE repository_id = @repo AND cl_number = @cl AND phase = @pre AND patchset < @below AND status = @queued
		THEN RETURN run_id`
	stopRunningPre = `UPDATE run@{FORCE_INDEX=run_cl} SET stop_reason = @reason
		WHERE repository_id = @repo AND cl_number = @cl AND phase = @pre AND patchset < @below AND status = @running AND stop_reason IS NULL`
	clearPreStops = `UPDATE run@{FORCE_INDEX=run_cl} SET stop_reason = NULL
		WHERE repository_id = @repo AND cl_number = @cl AND patchset = @patchset AND phase = @pre AND status = @running`
)

func (x queries) CancelQueuedPre(ctx context.Context, repo, cl int64, below int32, reason string) ([]int64, error) {
	return x.returningIDs(ctx, cancelQueuedPre,
		map[string]any{"repo": repo, "cl": cl, "pre": int64(store.PhasePre), "below": int64(below), "canceled": int64(store.StatusCanceled),
			"reason": reason, "queued": int64(store.StatusQueued)})
}

func (x queries) StopRunningPre(ctx context.Context, repo, cl int64, below int32, reason string) error {
	_, err := x.Exec(ctx, stopRunningPre,
		map[string]any{"repo": repo, "cl": cl, "pre": int64(store.PhasePre), "below": int64(below), "reason": reason,
			"running": int64(store.StatusRunning)})
	return err
}

func (x queries) ClearPreStops(ctx context.Context, repo, cl int64, patchset int32) error {
	_, err := x.Exec(ctx, clearPreStops,
		map[string]any{"repo": repo, "cl": cl, "patchset": int64(patchset), "pre": int64(store.PhasePre), "running": int64(store.StatusRunning)})
	return err
}

// ─── Submissions ───

func (x queries) ActiveSubmissionOf(ctx context.Context, repo int64, cls []int64) (int64, int64, bool, error) {
	var cl, sub int64
	var ok bool
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT sc.cl_number, s.submission_id FROM submission_cl sc JOIN submission s ON s.submission_id = sc.submission_id
			WHERE sc.repository_id = @repo AND sc.cl_number IN UNNEST(@cls) AND s.status = @active ORDER BY s.created_at, s.submission_id LIMIT 1`,
			map[string]any{"repo": repo, "cls": cls, "active": int64(store.SubmissionActive)}, &cl, &sub)
		return err
	})
	return cl, sub, ok, err
}

func (x queries) InsertSubmission(ctx context.Context, repo int64, by string, cls []store.SubmittedCL) (int64, error) {
	id := data.NewID()
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO submission (submission_id, repository_id, submitted_by, status, reason, note, created_at)
			VALUES (@id, @repo, @by, @active, '', '', CURRENT_TIMESTAMP())`,
			map[string]any{"id": id, "repo": repo, "by": by, "active": int64(store.SubmissionActive)}); err != nil {
			return err
		}
		for i, c := range cls {
			if _, err := spannerdata.Update(ctx, tx, `INSERT INTO submission_cl (submission_id, position, repository_id, cl_number, patchset)
				VALUES (@id, @position, @repo, @cl, @patchset)`,
				map[string]any{"id": id, "position": int64(i), "repo": repo, "cl": c.Number, "patchset": int64(c.Patchset)}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

const submissionColumns = `submission_id, repository_id, submitted_by, status, reason, note, created_at, finished_at, IFNULL(canceled_by, '')`

// loadSubmissions reads the submissions a query selects (by submissionColumns), with their CLs, in
// the query's order, at one moment.
func (x queries) loadSubmissions(ctx context.Context, sql string, params map[string]any, forUpdate bool) ([]*store.Submission, error) {
	var out []*store.Submission
	read := func(r spannerdata.Reader) error {
		out = nil
		byID := map[int64]*store.Submission{}
		if err := spannerdata.Each(ctx, r, sql, params, func(row *spanner.Row) error {
			var sub store.Submission
			var status int64
			var finished spanner.NullTime
			if err := row.Columns(&sub.ID, &sub.RepositoryID, &sub.SubmittedBy, &status, &sub.Reason, &sub.Note, &sub.CreatedAt, &finished,
				&sub.CanceledBy); err != nil {
				return err
			}
			sub.Status, sub.FinishedAt = store.SubmissionStatus(status), finished.Time
			out = append(out, &sub)
			byID[sub.ID] = &sub
			return nil
		}); err != nil || len(out) == 0 {
			return err
		}
		ids := make([]int64, 0, len(out))
		for _, sub := range out {
			ids = append(ids, sub.ID)
		}
		return spannerdata.Each(ctx, r, `SELECT submission_id, cl_number, patchset, landed_commit FROM submission_cl
			WHERE submission_id IN UNNEST(@ids) ORDER BY submission_id, position`, map[string]any{"ids": ids}, func(row *spanner.Row) error {
			var id, patchset int64
			var c store.SubmittedCL
			if err := row.Columns(&id, &c.Number, &patchset, &c.LandedCommit); err != nil {
				return err
			}
			c.Patchset = int32(patchset)
			byID[id].CLs = append(byID[id].CLs, c)
			return nil
		})
	}
	if forUpdate {
		return out, x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return read(tx) })
	}
	return out, x.Read(read)
}

func (x queries) Submissions(ctx context.Context, ids []int64) ([]*store.Submission, error) {
	return x.loadSubmissions(ctx, `SELECT `+submissionColumns+` FROM submission WHERE submission_id IN UNNEST(@ids) ORDER BY created_at, submission_id`,
		map[string]any{"ids": ids}, false)
}

func (x queries) ActiveSubmissions(ctx context.Context, repo int64) ([]*store.Submission, error) {
	return x.loadSubmissions(ctx, `SELECT `+submissionColumns+` FROM submission WHERE repository_id = @repo AND status = @active
		ORDER BY created_at, submission_id`, map[string]any{"repo": repo, "active": int64(store.SubmissionActive)}, false)
}

func (x queries) SubmissionsOfCL(ctx context.Context, repo, cl int64, limit int) ([]*store.Submission, error) {
	return x.loadSubmissions(ctx, `SELECT `+submissionColumns+` FROM submission WHERE submission_id IN (
			SELECT submission_id FROM submission_cl WHERE repository_id = @repo AND cl_number = @cl)
		ORDER BY created_at DESC, submission_id DESC LIMIT @limit`, map[string]any{"repo": repo, "cl": cl, "limit": int64(limit)}, false)
}

func (x queries) LockSubmission(ctx context.Context, id int64) (*store.Submission, error) {
	subs, err := x.loadSubmissions(ctx, `SELECT `+submissionColumns+` FROM submission WHERE submission_id = @id FOR UPDATE`,
		map[string]any{"id": id}, true)
	if err != nil {
		return nil, err
	}
	if len(subs) == 0 {
		return nil, store.ErrNotFound
	}
	return subs[0], nil
}

func (x queries) FinishSubmission(ctx context.Context, id int64, status store.SubmissionStatus, reason, by string) (int64, bool, error) {
	var repo int64
	var ok bool
	err := x.Returning(ctx, `UPDATE submission SET status = @status, reason = @reason, note = '', finished_at = CURRENT_TIMESTAMP(),
		canceled_by = @by WHERE submission_id = @id AND status = @active THEN RETURN repository_id`,
		map[string]any{"id": id, "active": int64(store.SubmissionActive), "status": int64(status), "reason": reason, "by": nullString(by)},
		func() { repo, ok = 0, false },
		func(row *spanner.Row) error {
			ok = true
			return row.Columns(&repo)
		})
	return repo, ok, err
}

func (x queries) ActiveSubmissionsOfCL(ctx context.Context, repo, cl int64, below int32) ([]int64, error) {
	return x.ids(ctx, `SELECT s.submission_id FROM submission s JOIN submission_cl sc ON sc.submission_id = s.submission_id
		WHERE sc.repository_id = @repo AND sc.cl_number = @cl AND s.status = @active AND (@below = 0 OR sc.patchset < @below)
		ORDER BY s.created_at, s.submission_id`,
		map[string]any{"repo": repo, "cl": cl, "active": int64(store.SubmissionActive), "below": int64(below)})
}

func (x queries) SetNote(ctx context.Context, id int64, note string) error {
	_, err := x.Exec(ctx, `UPDATE submission SET note = @note WHERE submission_id = @id AND status = @active AND note != @note`,
		map[string]any{"id": id, "note": note, "active": int64(store.SubmissionActive)})
	return err
}

const attemptColumns = `attempt_id, submission_id, base, base_seq, chain, status, released, prepare_error, reason, created_at, finished_at`

// loadAttempts reads the attempts a query selects (by attemptColumns), in its order, with their
// candidates, at one moment.
func (x queries) loadAttempts(ctx context.Context, sql string, params map[string]any, forUpdate bool) ([]*store.Attempt, error) {
	var out []*store.Attempt
	read := func(r spannerdata.Reader) error {
		out = nil
		byID := map[int64]*store.Attempt{}
		if err := spannerdata.Each(ctx, r, sql, params, func(row *spanner.Row) error {
			var a store.Attempt
			var status int64
			var finished spanner.NullTime
			if err := row.Columns(&a.ID, &a.SubmissionID, &a.Base, &a.BaseSeq, &a.Chain, &status, &a.Released, &a.PrepareError, &a.Reason,
				&a.CreatedAt, &finished); err != nil {
				return err
			}
			a.Status, a.FinishedAt = store.AttemptStatus(status), finished.Time
			if a.Chain == nil {
				a.Chain = []int64{}
			}
			out = append(out, &a)
			byID[a.ID] = &a
			return nil
		}); err != nil || len(out) == 0 {
			return err
		}
		ids := make([]int64, 0, len(out))
		for _, a := range out {
			ids = append(ids, a.ID)
		}
		return spannerdata.Each(ctx, r, `SELECT attempt_id, submission_id, cl_number, patchset, commit_id, base, IFNULL(plan_id, 0)
			FROM attempt_candidate WHERE attempt_id IN UNNEST(@ids) ORDER BY attempt_id, position`, map[string]any{"ids": ids},
			func(row *spanner.Row) error {
				var id, patchset int64
				var c store.AttemptCandidate
				if err := row.Columns(&id, &c.SubmissionID, &c.CLNumber, &patchset, &c.Commit, &c.Base, &c.PlanID); err != nil {
					return err
				}
				c.Patchset = int32(patchset)
				byID[id].Candidates = append(byID[id].Candidates, &c)
				return nil
			})
	}
	if forUpdate {
		return out, x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return read(tx) })
	}
	return out, x.Read(read)
}

func (x queries) Attempts(ctx context.Context, submissions []int64, status store.AttemptStatus) ([]*store.Attempt, error) {
	return x.loadAttempts(ctx, `SELECT `+attemptColumns+` FROM attempt
		WHERE submission_id IN UNNEST(@subs) AND (@status = 0 OR status = @status) ORDER BY created_at, attempt_id`,
		map[string]any{"subs": submissions, "status": int64(status)}, false)
}

func (x queries) ActiveAttempt(ctx context.Context, submission int64) (int64, bool, error) {
	var id int64
	var ok bool
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT attempt_id FROM attempt WHERE submission_id = @sub AND status = @active
			ORDER BY created_at, attempt_id LIMIT 1`, map[string]any{"sub": submission, "active": int64(store.AttemptActive)}, &id)
		return err
	})
	return id, ok, err
}

func (x queries) LockAttempt(ctx context.Context, id int64) (*store.Attempt, error) {
	as, err := x.loadAttempts(ctx, `SELECT `+attemptColumns+` FROM attempt WHERE attempt_id = @id FOR UPDATE`, map[string]any{"id": id}, true)
	if err != nil {
		return nil, err
	}
	if len(as) == 0 {
		return nil, store.ErrNotFound
	}
	return as[0], nil
}

// InsertAttempt records an attempt with its submission's repository: partitioned DML, which a
// purge is, deletes by a column of the row's own, not by a subquery.
func (x queries) InsertAttempt(ctx context.Context, a store.NewAttemptRow) (int64, error) {
	id := data.NewID()
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		p := map[string]any{"id": id, "sub": a.SubmissionID, "base": a.Base, "baseSeq": a.BaseSeq, "chain": orEmpty(a.Chain),
			"active": int64(store.AttemptActive), "prepareError": a.PrepareError}
		var repo int64
		ok, err := spannerdata.First(ctx, tx, `SELECT repository_id FROM submission WHERE submission_id = @sub`, p, &repo)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("no submission %d", a.SubmissionID)
		}
		p["repo"] = repo
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO attempt (attempt_id, submission_id, repository_id, base, base_seq, chain, status, released,
				prepare_error, reason, created_at)
			VALUES (@id, @sub, @repo, @base, @baseSeq, @chain, @active, FALSE, @prepareError, '', CURRENT_TIMESTAMP())`, p)
		return err
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (x queries) InsertCandidate(ctx context.Context, attempt int64, position int, repo int64, c store.AttemptCandidate) error {
	_, err := x.Exec(ctx, `INSERT INTO attempt_candidate (attempt_id, position, repository_id, submission_id, cl_number, patchset, commit_id, base, plan_id)
		VALUES (@attempt, @position, @repo, @sub, @cl, @patchset, @commit, @base, @plan)`,
		map[string]any{"attempt": attempt, "position": int64(position), "repo": repo, "sub": c.SubmissionID, "cl": c.CLNumber,
			"patchset": int64(c.Patchset), "commit": c.Commit, "base": c.Base, "plan": nullable(c.PlanID)})
	return err
}

func (x queries) SetReleased(ctx context.Context, id int64) error {
	_, err := x.Exec(ctx, `UPDATE attempt SET released = TRUE WHERE attempt_id = @id`, map[string]any{"id": id})
	return err
}

func (x queries) SetAttemptStatus(ctx context.Context, id int64, from, to store.AttemptStatus, reason string) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE attempt SET status = @to, reason = @reason, finished_at = CURRENT_TIMESTAMP()
		WHERE attempt_id = @id AND (@from = 0 OR status = @from)`,
		map[string]any{"id": id, "from": int64(from), "to": int64(to), "reason": reason})
	return n > 0, err
}

func (x queries) EndActiveAttempts(ctx context.Context, submission int64, status store.AttemptStatus, reason string) ([]int64, error) {
	return x.returningIDs(ctx, `UPDATE attempt SET status = @status, reason = @reason, finished_at = CURRENT_TIMESTAMP()
		WHERE submission_id = @sub AND status = @active THEN RETURN attempt_id`,
		map[string]any{"sub": submission, "active": int64(store.AttemptActive), "status": int64(status), "reason": reason})
}

func (x queries) UnfinishedRunsOfAttempts(ctx context.Context, attempts []int64) ([]int64, error) {
	return x.ids(ctx, `SELECT r.run_id FROM run r JOIN attempt_candidate ac ON ac.plan_id = r.plan_id
		WHERE ac.attempt_id IN UNNEST(@attempts) AND r.status IN (@queued, @running) ORDER BY r.queued_at, r.run_id`,
		map[string]any{"attempts": attempts, "queued": int64(store.StatusQueued), "running": int64(store.StatusRunning)})
}

func (x queries) ReusableCandidate(ctx context.Context, repo int64, base []byte, cl int64, patchset int32) ([]byte, bool, error) {
	var commit []byte
	var ok bool
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT ac.commit_id FROM attempt_candidate ac JOIN attempt a ON a.attempt_id = ac.attempt_id
			WHERE ac.repository_id = @repo AND ac.base = @base AND ac.cl_number = @cl AND ac.patchset = @patchset AND a.status = @active
			ORDER BY a.created_at DESC, a.attempt_id DESC LIMIT 1`,
			map[string]any{"repo": repo, "base": base, "cl": cl, "patchset": int64(patchset), "active": int64(store.AttemptActive)}, &commit)
		return err
	})
	return commit, ok, err
}

func (x queries) CandidatesInUse(ctx context.Context, repo int64) (map[string]bool, error) {
	out := map[string]bool{}
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, `SELECT ac.commit_id FROM attempt_candidate ac JOIN attempt a ON a.attempt_id = ac.attempt_id
			WHERE ac.repository_id = @repo AND a.status = @active`, map[string]any{"repo": repo, "active": int64(store.AttemptActive)},
			func(row *spanner.Row) error {
				var c []byte
				if err := row.Columns(&c); err != nil {
					return err
				}
				out[string(c)] = true
				return nil
			})
	})
	return out, err
}

func (x queries) SetLandedCommit(ctx context.Context, submission, cl int64, commit []byte) error {
	_, err := x.Exec(ctx, `UPDATE submission_cl SET landed_commit = @commit WHERE submission_id = @sub AND cl_number = @cl`,
		map[string]any{"sub": submission, "cl": cl, "commit": commit})
	return err
}

func (x queries) InsertLandedChange(ctx context.Context, repo int64, commit []byte, cl int64) error {
	p := map[string]any{"repo": repo, "commit": commit, "cl": cl}
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var one int64
		ok, err := spannerdata.First(ctx, tx, `SELECT 1 FROM landed_change WHERE repository_id = @repo AND commit_id = @commit`, p, &one)
		if err != nil || ok {
			return err
		}
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO landed_change (repository_id, commit_id, cl_number) VALUES (@repo, @commit, @cl)`, p)
		return err
	})
}

func (x queries) LockMain(ctx context.Context, repo int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return spannerlock.InTx(ctx, tx, "main", strconv.FormatInt(repo, 10))
	})
}

// mainCommitAt reads, in tx, whether main has a commit recorded at seq.
func mainCommitAt(ctx context.Context, tx *spanner.ReadWriteTransaction, repo, seq int64) (bool, error) {
	var one int64
	return spannerdata.First(ctx, tx, `SELECT 1 FROM main_commit WHERE repository_id = @repo AND seq = @seq`,
		map[string]any{"repo": repo, "seq": seq}, &one)
}

func (x queries) InsertMainCommit(ctx context.Context, repo, seq int64, commit []byte) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		ok, err := mainCommitAt(ctx, tx, repo, seq)
		if err != nil || ok {
			return err
		}
		p := map[string]any{"repo": repo, "seq": seq, "commit": commit}
		var sub spanner.NullInt64
		if _, err := spannerdata.First(ctx, tx, `SELECT sc.submission_id FROM landed_change lc
				JOIN submission_cl sc ON sc.repository_id = lc.repository_id AND sc.cl_number = lc.cl_number AND sc.landed_commit = lc.commit_id
			WHERE lc.repository_id = @repo AND lc.commit_id = @commit LIMIT 1`, p, &sub); err != nil {
			return err
		}
		p["sub"] = sub
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO main_commit (repository_id, seq, commit_id, submission_id) VALUES (@repo, @seq, @commit, @sub)`, p)
		return err
	})
}

func (x queries) PutMainCommit(ctx context.Context, repo, seq int64, commit []byte, submission int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		ok, err := mainCommitAt(ctx, tx, repo, seq)
		if err != nil {
			return err
		}
		p := map[string]any{"repo": repo, "seq": seq, "commit": commit, "sub": submission}
		sql := `INSERT INTO main_commit (repository_id, seq, commit_id, submission_id) VALUES (@repo, @seq, @commit, @sub)`
		if ok {
			sql = `UPDATE main_commit SET submission_id = @sub WHERE repository_id = @repo AND seq = @seq`
		}
		_, err = spannerdata.Update(ctx, tx, sql, p)
		return err
	})
}

func (x queries) SetMainCommitSubmission(ctx context.Context, repo int64, commit []byte, submission int64) error {
	_, err := x.Exec(ctx, `UPDATE main_commit SET submission_id = @sub WHERE repository_id = @repo AND commit_id = @commit`,
		map[string]any{"repo": repo, "commit": commit, "sub": submission})
	return err
}

func (x queries) MainCommitsAfter(ctx context.Context, repo, from int64) ([]store.MainCommit, error) {
	var out []store.MainCommit
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT mc.seq, mc.commit_id, IFNULL(lc.cl_number, 0), IFNULL(mc.submission_id, 0)
			FROM main_commit mc LEFT JOIN landed_change lc ON lc.repository_id = mc.repository_id AND lc.commit_id = mc.commit_id
			WHERE mc.repository_id = @repo AND mc.seq > @from ORDER BY mc.seq`, map[string]any{"repo": repo, "from": from},
			func(row *spanner.Row) error {
				var m store.MainCommit
				if err := row.Columns(&m.Seq, &m.Commit, &m.CLNumber, &m.SubmissionID); err != nil {
					return err
				}
				out = append(out, m)
				return nil
			})
	})
	return out, err
}

func (x queries) LandedCLs(ctx context.Context, repo int64, cls []int64) (map[int64][]byte, error) {
	out := map[int64][]byte{}
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, `SELECT cl_number, commit_id FROM landed_change WHERE repository_id = @repo AND cl_number IN UNNEST(@cls)`,
			map[string]any{"repo": repo, "cls": cls}, func(row *spanner.Row) error {
				var cl int64
				var commit []byte
				if err := row.Columns(&cl, &commit); err != nil {
					return err
				}
				out[cl] = commit
				return nil
			})
	})
	return out, err
}
