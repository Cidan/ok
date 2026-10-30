// Package postgres is automation's store backend (internal/automation/store) on PostgreSQL, in
// the schema automation: a queued run claimed with FOR UPDATE SKIP LOCKED.
package postgres

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are automation's schema.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Backend is automation's store backend on PostgreSQL.
type Backend struct {
	queries
	pool  *pgxpool.Pool
	moves *move.PostgresRows
}

var _ store.Backend = (*Backend)(nil)

// Work is the backend's keyed work's store.
func (b *Backend) Work() work.Store { return pgdata.Work(b.pool) }

func (b *Backend) Waits() wait.Store { return pgdata.Waits(b.pool) }

// Open brings the database behind pool to automation's schema and returns a backend over it.
func Open(ctx context.Context, pool *pgxpool.Pool) (*Backend, error) {
	if _, err := migrate.Apply(ctx, pool, "automation", Migrations); err != nil {
		return nil, err
	}
	return &Backend{queries: queries{move.GatedPool{Pool: pool, Tables: moveTableSet}}, pool: pool,
		moves: &move.PostgresRows{Pool: pool, Tables: moveTables, Where: moveWhere}}, nil
}

// moveTables are the tables of rows about repositories this cell holds, which a move copies
// (docs/hosted.md, "Cells, as built"): their plans, runs, submissions, main's commits, and where
// the SCM's feed of each was applied to.
var moveTables = []string{"plan", "run", "pushed_image", "submission", "submission_cl", "attempt", "attempt_candidate", "main_commit",
	"landed_change", "follow_cursor"}

var moveTableSet = func() map[string]bool {
	m := map[string]bool{}
	for _, t := range moveTables {
		m[t] = true
	}
	return m
}()

// moveWhere is the condition a table's rows about repositories (@repos) meet: a submission's rows
// go with it, whichever repositories its CLs name.
func moveWhere(table string) string {
	const submissions = "SELECT id FROM submission WHERE repository_id = ANY(@repos)"
	switch table {
	case "pushed_image":
		return "run_id IN (SELECT id FROM run WHERE repository_id = ANY(@repos))"
	case "submission_cl", "attempt":
		return "submission_id IN (" + submissions + ")"
	case "attempt_candidate":
		return "attempt_id IN (SELECT id FROM attempt WHERE submission_id IN (" + submissions + "))"
	case "follow_cursor":
		return "topic = '" + scmfeed.Topic + "' AND key = ANY(@repos)"
	}
	return "repository_id = ANY(@repos)"
}

// Moves are the backend's rows about repositories, for a move.
func (b *Backend) Moves() move.Rows { return b.moves }

// FencedWritesBesideEachOther is false: a work fence holds its row FOR SHARE, which a mark of the
// row in the same transaction upgrades (pgdata.CheckWorkFence).
func (b *Backend) FencedWritesBesideEachOther() bool { return false }

func (x queries) RunningRuns(ctx context.Context, repos []int64) ([]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT id FROM run WHERE repository_id = ANY($1) AND status = $2 ORDER BY id`, repos, store.StatusRunning)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func (x queries) Gate(ctx context.Context, repo int64) error {
	return move.GateRepository(ctx, x.q, repo)
}

// Txn runs fn in a transaction, again if PostgreSQL aborts it to break a deadlock or a
// serialization conflict.
func (b *Backend) Txn(ctx context.Context, fn func(store.Ops) error) error {
	return pgdata.Txn(ctx, b.pool, func(tx pgx.Tx) error {
		// What ctx's lease guards is written only while its holder holds it (data.Fence).
		if err := pgdata.CheckFence(ctx, tx); err != nil {
			return err
		}
		if err := move.PostgresGate(ctx, tx); err != nil {
			return err
		}
		return fn(queries{&move.CheckedTx{Tx: tx, Tables: moveTableSet}})
	})
}

// Snapshot runs fn in a read-only, repeatable-read transaction.
func (b *Backend) Snapshot(ctx context.Context, fn func(store.Ops) error) error {
	return pgdata.Snapshot(ctx, b.pool, func(tx pgx.Tx) error { return fn(queries{tx}) })
}

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// queries are the backend's operations over a pool or a transaction.
type queries struct{ q querier }

// nullable is a positive number, or SQL NULL.
func nullable[T int16 | int32 | int64](v T) *T {
	if v <= 0 {
		return nil
	}
	return &v
}

func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func ids(rows pgx.Rows, err error) ([]int64, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func commitsByRepository(rows pgx.Rows, err error) (map[int64][][]byte, error) {
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

// ─── Runs ───

// runColumns are a run's columns, in scanRun's order.
const runColumns = `id, repository_id, commit_id, kind, targets, args, phase, cl_number, patchset, job,
	requested_by, rerun_of, status, attempt, coalesce(worker, ''), coalesce(exit_code, 0), reason, log_size,
	log_stored, timeout_seconds, coalesce(plan_id, 0), why, queued_at, started_at, finished_at, coalesce(carried_from, 0),
	coalesce(canceled_by, ''), pool, coalesce(dispatch_job, ''), lease_handle`

func scanRun(row pgx.Row) (*store.Run, error) {
	var r store.Run
	var cl, rerun *int64
	var patchset *int32
	var started, finished *time.Time
	err := row.Scan(&r.ID, &r.RepositoryID, &r.CommitID, &r.Job.Kind, &r.Job.Targets, &r.Job.Args,
		&r.Origin.Phase, &cl, &patchset, &r.Origin.Job, &r.RequestedBy, &rerun, &r.Status, &r.Attempt, &r.Worker,
		&r.ExitCode, &r.Reason, &r.LogSize, &r.LogStored, &r.TimeoutSeconds, &r.PlanID, &r.Why, &r.QueuedAt, &started, &finished,
		&r.CarriedFrom, &r.CanceledBy, &r.Job.Pool, &r.DispatchJob, &r.LeaseHandle)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if cl != nil {
		r.Origin.CLNumber = *cl
	}
	if patchset != nil {
		r.Origin.Patchset = *patchset
	}
	if rerun != nil {
		r.RerunOf = *rerun
	}
	if started != nil {
		r.StartedAt = *started
	}
	if finished != nil {
		r.FinishedAt = *finished
	}
	return &r, nil
}

func collectRuns(rows pgx.Rows, err error) ([]*store.Run, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// insertRun makes a run under a random ID ($23). Its queue time is the clock's, not the
// transaction's start, so the runs one transaction queues keep their order.
const insertRun = `INSERT INTO run (repository_id, commit_id, kind, targets, args, phase, cl_number, patchset, job,
		plan_id, why, requested_by, rerun_of, status, reason, timeout_seconds, priority,
		started_at, finished_at, log_stored, carried_from, log_size, id, queued_at, pool, dispatch_job)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
		CASE WHEN $18::boolean THEN now() END, CASE WHEN $19::boolean THEN now() END, $20, $21, $22, $23, clock_timestamp(), coalesce(nullif($24, ''), 'default'),
		nullif($25, ''))`

// runArgs are a run's insertRun arguments, under the ID id.
func runArgs(r store.NewRun, id int64) []any {
	return []any{r.RepositoryID, r.CommitID, r.Job.Kind, orEmpty(r.Job.Targets), orEmpty(r.Job.Args), r.Origin.Phase,
		nullable(r.Origin.CLNumber), nullable(r.Origin.Patchset), r.Origin.Job, nullable(r.PlanID), r.Why, r.RequestedBy,
		nullable(r.RerunOf), r.Status, r.Reason, r.TimeoutSeconds, r.Priority, r.Started, r.Finished, r.LogStored,
		nullable(r.CarriedFrom), r.LogSize, id, r.Job.Pool, r.DispatchJob}
}

func (x queries) InsertRun(ctx context.Context, r store.NewRun) (*store.Run, error) {
	return scanRun(x.q.QueryRow(ctx, insertRun+` RETURNING `+runColumns, runArgs(r, data.NewID())...))
}

func (x queries) InsertRuns(ctx context.Context, rs []store.NewRun) ([]int64, error) {
	if len(rs) == 0 {
		return nil, nil
	}
	ids := make([]int64, len(rs))
	batch := &pgx.Batch{}
	for i, r := range rs {
		ids[i] = data.NewID()
		batch.Queue(insertRun, runArgs(r, ids[i])...)
	}
	return ids, x.q.SendBatch(ctx, batch).Close()
}

func (x queries) Run(ctx context.Context, id int64) (*store.Run, error) {
	return scanRun(x.q.QueryRow(ctx, `SELECT `+runColumns+` FROM run WHERE id = $1`, id))
}

func (x queries) LockRun(ctx context.Context, id int64) (*store.Run, error) {
	return scanRun(x.q.QueryRow(ctx, `SELECT `+runColumns+` FROM run WHERE id = $1 FOR UPDATE`, id))
}

func (x queries) RunsOfCL(ctx context.Context, repo, cl int64, patchset int32, before int64, limit int) ([]*store.Run, error) {
	return collectRuns(x.q.Query(ctx, `SELECT `+runColumns+` FROM run
		WHERE repository_id = $1 AND cl_number = $2 AND ($3 = 0 OR patchset = $3)
			AND ($4::bigint = 0 OR (queued_at, id) < (SELECT queued_at, id FROM run WHERE id = $4))
		ORDER BY queued_at DESC, id DESC LIMIT $5`, repo, cl, patchset, before, limit))
}

func (x queries) RunsOfCommit(ctx context.Context, repo int64, commit []byte, before int64, limit int) ([]*store.Run, error) {
	return collectRuns(x.q.Query(ctx, `SELECT `+runColumns+` FROM run
		WHERE repository_id = $1 AND commit_id = $2 AND ($3::bigint = 0 OR (queued_at, id) < (SELECT queued_at, id FROM run WHERE id = $3))
		ORDER BY queued_at DESC, id DESC LIMIT $4`, repo, commit, before, limit))
}

func (x queries) PlanRuns(ctx context.Context, plans []int64) ([]*store.Run, error) {
	return collectRuns(x.q.Query(ctx, `SELECT `+runColumns+` FROM run WHERE plan_id = ANY($1) ORDER BY queued_at, id`, plans))
}

func (x queries) Pushed(ctx context.Context, runs []int64) (map[int64][]store.Pushed, error) {
	rows, err := x.q.Query(ctx, `SELECT run_id, target, registry, repository, digest, tags FROM pushed_image
		WHERE run_id = ANY($1) ORDER BY run_id, position`, runs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64][]store.Pushed{}
	for rows.Next() {
		var id int64
		var p store.Pushed
		if err := rows.Scan(&id, &p.Target, &p.Registry, &p.Repository, &p.Digest, &p.Tags); err != nil {
			return nil, err
		}
		out[id] = append(out[id], p)
	}
	return out, rows.Err()
}

func (x queries) InsertPushed(ctx context.Context, run int64, pushed []store.Pushed) error {
	batch := &pgx.Batch{}
	for i, p := range pushed {
		batch.Queue(`INSERT INTO pushed_image (run_id, position, target, registry, repository, digest, tags)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, run, i, p.Target, p.Registry, p.Repository, p.Digest, orEmpty(p.Tags))
	}
	return x.q.SendBatch(ctx, batch).Close()
}

func (x queries) RenewLease(ctx context.Context, id int64, worker string, ttl time.Duration) (store.Lease, bool, error) {
	var l store.Lease
	var stop *string
	err := x.q.QueryRow(ctx, `UPDATE run SET lease_expires = now() + make_interval(secs => $3)
		WHERE id = $1 AND status = $4 AND worker = $2
		RETURNING stop_reason, now() > started_at + make_interval(secs => timeout_seconds), timeout_seconds, lease_handle`,
		id, worker, ttl.Seconds(), store.StatusRunning).Scan(&stop, &l.Overdue, &l.TimeoutSeconds, &l.Handle)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Lease{}, false, nil
	}
	if err != nil {
		return store.Lease{}, false, err
	}
	if stop != nil {
		l.Stopping, l.StopReason = true, *stop
	}
	return l, true, nil
}

// stopRunning is a statement's first part that charges a run that was running ($1) its seconds
// since it started to its account's month, and lets its slot go, as it is read before the
// statement changes it.
const stopRunning = `WITH ran AS (
		SELECT r.repository_id, r.started_at FROM run r WHERE r.id = $1 AND r.status = 2 AND r.started_at IS NOT NULL
	), freed AS (
		DELETE FROM account_slot s USING run r WHERE r.id = $1 AND r.status = 2 AND s.account_id = r.slot_account AND s.slot = r.slot
	), charged AS (
		INSERT INTO account_usage (account_id, month, seconds)
		SELECT coalesce(o.account_id, ''), to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM'),
			greatest(0, floor(extract(epoch FROM now() - ran.started_at)))::bigint
		FROM ran LEFT JOIN repository_owner o ON o.repository_id = ran.repository_id
		ON CONFLICT (account_id, month) DO UPDATE SET seconds = account_usage.seconds + EXCLUDED.seconds
	) `

func (x queries) RequeueRun(ctx context.Context, id int64, job, reason string, nextAttempt bool) error {
	_, err := x.q.Exec(ctx, stopRunning+`UPDATE run SET status = $2, reason = $3, attempt = attempt + CASE WHEN $4::boolean THEN 1 ELSE 0 END,
			worker = NULL, lease_expires = NULL, started_at = NULL, finished_at = NULL, stop_reason = NULL, log_size = 0,
			slot_account = NULL, slot = NULL, dispatch_job = nullif($5, ''), lease_handle = NULL
		WHERE id = $1`, id, store.StatusQueued, reason, nextAttempt, job)
	return err
}

func (x queries) FinishRun(ctx context.Context, id int64, status store.Status, exitCode int32, reason string) error {
	_, err := x.q.Exec(ctx, stopRunning+`UPDATE run SET status = $2, exit_code = $3, reason = $4, finished_at = now(),
		lease_expires = NULL, stop_reason = NULL, slot_account = NULL, slot = NULL, lease_handle = NULL WHERE id = $1`, id, status, exitCode, reason)
	return err
}

func (x queries) CancelQueuedRun(ctx context.Context, id int64, reason, by string) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE run SET status = $2, reason = $3, finished_at = now(), log_stored = true,
		canceled_by = nullif($5, '') WHERE id = $1 AND status = $4`, id, store.StatusCanceled, reason, store.StatusQueued, by)
	return tag.RowsAffected() > 0, err
}

func (x queries) RequestStop(ctx context.Context, id int64, reason, by string) error {
	_, err := x.q.Exec(ctx, `UPDATE run SET stop_reason = $2, canceled_by = nullif($4, '')
		WHERE id = $1 AND status = $3 AND stop_reason IS NULL`, id, reason, store.StatusRunning, by)
	return err
}

func (x queries) StopRequest(ctx context.Context, id int64) (bool, string, error) {
	var stop *string
	err := x.q.QueryRow(ctx, `SELECT stop_reason FROM run WHERE id = $1`, id).Scan(&stop)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", store.ErrNotFound
	}
	if err != nil || stop == nil {
		return false, "", err
	}
	return true, *stop, nil
}

// ─── Logs ───

func (x queries) AdvanceLog(ctx context.Context, id int64, worker string, started time.Time, from, to int64) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE run SET log_size = $5
		WHERE id = $1 AND status = $6 AND worker = $2 AND started_at = $3 AND log_size = $4`,
		id, worker, started, from, to, store.StatusRunning)
	return tag.RowsAffected() > 0, err
}

func (x queries) MarkLogStored(ctx context.Context, id int64) error {
	_, err := x.q.Exec(ctx, `UPDATE run SET log_stored = true WHERE id = $1`, id)
	return err
}

func (x queries) UnstoredLogs(ctx context.Context, repos []int64, limit int) ([]int64, error) {
	if repos == nil {
		repos = []int64{}
	}
	return ids(x.q.Query(ctx, `SELECT id FROM run WHERE status >= $1 AND NOT log_stored
		AND (cardinality($3::bigint[]) = 0 OR repository_id = ANY($3))
		AND NOT EXISTS (SELECT 1 FROM move_gate g WHERE g.repository_id = run.repository_id AND g.move_state <> '')
		ORDER BY id LIMIT $2`, store.StatusPassed, limit, repos))
}

// ─── The feed, and the build cache's commits ───

func (x queries) MarkWork(ctx context.Context, kind string, key int64) error {
	return pgdata.MarkWork(ctx, x.q, kind, key)
}

func (x queries) MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return pgdata.MarkWorkAt(ctx, x.q, kind, key, at)
}

func (x queries) CLsOf(ctx context.Context, runs, plans []int64) ([][2]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT DISTINCT p.repository_id, p.cl_number FROM plan p
		WHERE p.cl_number > 0 AND (p.id = ANY($2) OR p.id IN (SELECT r.plan_id FROM run r WHERE r.id = ANY($1)))`, runs, plans)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]int64
	for rows.Next() {
		var c [2]int64
		if err := rows.Scan(&c[0], &c[1]); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (x queries) PendStep(_ context.Context, kind string, key int64, at time.Time, body []byte) error {
	return pgdata.PendStep(x.q, kind, key, at, body)
}

func (x queries) FollowCursors(ctx context.Context, topic string, keys []int64) (map[int64]int64, error) {
	return pgdata.Cursors(ctx, x.q, topic, keys)
}

func (x queries) FollowCursor(ctx context.Context, topic string, key int64) (int64, error) {
	return pgdata.Cursor(ctx, x.q, topic, key)
}

func (x queries) AdvanceFollowCursor(ctx context.Context, topic string, key, seq int64) error {
	return pgdata.AdvanceCursor(ctx, x.q, topic, key, seq)
}

func (x queries) MainCommits(ctx context.Context, n int) (map[int64][][]byte, error) {
	return commitsByRepository(x.q.Query(ctx, `SELECT repository_id, commit_id FROM (
			SELECT repository_id, commit_id, row_number() OVER (PARTITION BY repository_id ORDER BY seq DESC) AS r
			FROM main_commit) m
		WHERE r <= $1 ORDER BY repository_id, r`, n))
}

func (x queries) RunCommits(ctx context.Context, since time.Time) (map[int64][][]byte, error) {
	return commitsByRepository(x.q.Query(ctx, `SELECT DISTINCT repository_id, commit_id FROM run
		WHERE status IN ($1, $2) OR finished_at >= $3`, store.StatusQueued, store.StatusRunning, since))
}

// ─── Plans ───

func (x queries) LockCL(ctx context.Context, repo, cl int64) error {
	return lock.InTx(ctx, x.q, "plans", fmt.Sprintf("%d/%d", repo, cl))
}

const planColumns = `id, repository_id, cl_number, patchset, phase, commit_id, workflows_commit, context, touched,
	planned, jobs, skipped, affected, upstream, created_at, pre_commit`

func scanPlan(row pgx.Row) (*store.Plan, error) {
	var p store.Plan
	var touched, jobs, skipped, affected, upstream []byte
	err := row.Scan(&p.ID, &p.RepositoryID, &p.CLNumber, &p.Patchset, &p.Phase, &p.CommitID, &p.WorkflowsCommit,
		&p.Context, &touched, &p.Planned, &jobs, &skipped, &affected, &upstream, &p.CreatedAt, &p.PreCommit)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
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

func (x queries) InsertPlan(ctx context.Context, p store.NewPlan) (*store.Plan, error) {
	touched, err := json.Marshal(orEmpty(p.Touched))
	if err != nil {
		return nil, err
	}
	return scanPlan(x.q.QueryRow(ctx, `INSERT INTO plan (id, repository_id, cl_number, patchset, phase, commit_id,
			workflows_commit, context, touched, pre_commit, created_at)
		VALUES ($10, $1, $2, $3, $4, $5, $6, $7, $8, $9, clock_timestamp()) RETURNING `+planColumns,
		p.RepositoryID, p.CLNumber, p.Patchset, p.Phase, p.CommitID, p.WorkflowsCommit, p.Context, touched, p.PreCommit, data.NewID()))
}

func (x queries) Plan(ctx context.Context, id int64) (*store.Plan, error) {
	return scanPlan(x.q.QueryRow(ctx, `SELECT `+planColumns+` FROM plan WHERE id = $1`, id))
}

func (x queries) LockPlan(ctx context.Context, id int64) (*store.Plan, error) {
	return scanPlan(x.q.QueryRow(ctx, `SELECT `+planColumns+` FROM plan WHERE id = $1 FOR UPDATE`, id))
}

func (x queries) Plans(ctx context.Context, ids []int64) ([]*store.Plan, error) {
	rows, err := x.q.Query(ctx, `SELECT `+planColumns+` FROM plan WHERE id = ANY($1) ORDER BY created_at, id`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.Plan
	for rows.Next() {
		p, err := scanPlan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (x queries) PrePlan(ctx context.Context, repo, cl int64, patchset int32) (*store.Plan, error) {
	return scanPlan(x.q.QueryRow(ctx, `SELECT `+planColumns+` FROM plan
		WHERE repository_id = $1 AND cl_number = $2 AND patchset = $3 AND phase = $4`, repo, cl, patchset, store.PhasePre))
}

func (x queries) MaxPrePatchset(ctx context.Context, repo, cl int64) (int32, error) {
	var n int32
	err := x.q.QueryRow(ctx, `SELECT coalesce(max(patchset), 0) FROM plan
		WHERE repository_id = $1 AND cl_number = $2 AND phase = $3`, repo, cl, store.PhasePre).Scan(&n)
	return n, err
}

func (x queries) SetPlanned(ctx context.Context, id int64, r store.PlanResult) error {
	var enc [4][]byte
	for i, v := range []any{orEmpty(r.Jobs), orEmpty(r.Skipped), orEmpty(r.Affected), orEmpty(r.Upstream)} {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		enc[i] = b
	}
	_, err := x.q.Exec(ctx, `UPDATE plan SET planned = true, jobs = $2, skipped = $3, affected = $4, upstream = $5
		WHERE id = $1`, id, enc[0], enc[1], enc[2], enc[3])
	return err
}

func (x queries) CancelQueuedPre(ctx context.Context, repo, cl int64, below int32, reason string) ([]int64, error) {
	got, err := ids(x.q.Query(ctx, `UPDATE run SET status = $5, reason = $6, finished_at = now(), log_stored = true
		WHERE repository_id = $1 AND cl_number = $2 AND phase = $3 AND patchset < $4 AND status = $7
		RETURNING id`, repo, cl, store.PhasePre, below, store.StatusCanceled, reason, store.StatusQueued))
	if err != nil {
		return nil, err
	}
	sortIDs(got)
	return got, nil
}

func sortIDs(s []int64) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func (x queries) StopRunningPre(ctx context.Context, repo, cl int64, below int32, reason string) error {
	_, err := x.q.Exec(ctx, `UPDATE run SET stop_reason = $5
		WHERE repository_id = $1 AND cl_number = $2 AND phase = $3 AND patchset < $4 AND status = $6
			AND stop_reason IS NULL`, repo, cl, store.PhasePre, below, reason, store.StatusRunning)
	return err
}

func (x queries) ClearPreStops(ctx context.Context, repo, cl int64, patchset int32) error {
	_, err := x.q.Exec(ctx, `UPDATE run SET stop_reason = NULL
		WHERE repository_id = $1 AND cl_number = $2 AND patchset = $3 AND phase = $4 AND status = $5`,
		repo, cl, patchset, store.PhasePre, store.StatusRunning)
	return err
}

// ─── Submissions ───

func (x queries) ActiveSubmissionOf(ctx context.Context, repo int64, cls []int64) (int64, int64, bool, error) {
	var cl, sub int64
	err := x.q.QueryRow(ctx, `SELECT sc.cl_number, s.id FROM submission_cl sc JOIN submission s ON s.id = sc.submission_id
		WHERE sc.repository_id = $1 AND sc.cl_number = ANY($2) AND s.status = $3 ORDER BY s.created_at, s.id LIMIT 1`,
		repo, cls, store.SubmissionActive).Scan(&cl, &sub)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, false, nil
	}
	return cl, sub, err == nil, err
}

func (x queries) InsertSubmission(ctx context.Context, repo int64, by string, cls []store.SubmittedCL) (int64, error) {
	numbers, patchsets := make([]int64, len(cls)), make([]int32, len(cls))
	for i, c := range cls {
		numbers[i], patchsets[i] = c.Number, c.Patchset
	}
	var id int64
	err := x.q.QueryRow(ctx, `WITH s AS (
			INSERT INTO submission (id, repository_id, submitted_by, status, created_at) VALUES ($6, $1, $2, $3, clock_timestamp()) RETURNING id),
		c AS (
			INSERT INTO submission_cl (submission_id, position, repository_id, cl_number, patchset)
			SELECT s.id, u.ord - 1, $1, u.cl, u.ps FROM s, unnest($4::bigint[], $5::int[]) WITH ORDINALITY AS u(cl, ps, ord))
		SELECT id FROM s`, repo, by, store.SubmissionActive, numbers, patchsets, data.NewID()).Scan(&id)
	return id, err
}

const submissionColumns = `id, repository_id, submitted_by, status, reason, note, created_at, finished_at,
	coalesce(canceled_by, '')`

// loadSubmissions reads the submissions a query selects (by submissionColumns), with their CLs,
// in the query's order.
func (x queries) loadSubmissions(ctx context.Context, sql string, args ...any) ([]*store.Submission, error) {
	rows, err := x.q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	var out []*store.Submission
	byID := map[int64]*store.Submission{}
	for rows.Next() {
		var sub store.Submission
		var finished *time.Time
		if err := rows.Scan(&sub.ID, &sub.RepositoryID, &sub.SubmittedBy, &sub.Status, &sub.Reason, &sub.Note,
			&sub.CreatedAt, &finished, &sub.CanceledBy); err != nil {
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
	subIDs := make([]int64, 0, len(out))
	for _, sub := range out {
		subIDs = append(subIDs, sub.ID)
	}
	rows, err = x.q.Query(ctx, `SELECT submission_id, cl_number, patchset, landed_commit FROM submission_cl
		WHERE submission_id = ANY($1) ORDER BY submission_id, position`, subIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var c store.SubmittedCL
		if err := rows.Scan(&id, &c.Number, &c.Patchset, &c.LandedCommit); err != nil {
			return nil, err
		}
		byID[id].CLs = append(byID[id].CLs, c)
	}
	return out, rows.Err()
}

func (x queries) Submissions(ctx context.Context, ids []int64) ([]*store.Submission, error) {
	return x.loadSubmissions(ctx, `SELECT `+submissionColumns+` FROM submission WHERE id = ANY($1) ORDER BY created_at, id`, ids)
}

func (x queries) ActiveSubmissions(ctx context.Context, repo int64) ([]*store.Submission, error) {
	return x.loadSubmissions(ctx, `SELECT `+submissionColumns+` FROM submission
		WHERE repository_id = $1 AND status = $2 ORDER BY created_at, id`, repo, store.SubmissionActive)
}

func (x queries) SubmissionsOfCL(ctx context.Context, repo, cl int64, limit int) ([]*store.Submission, error) {
	return x.loadSubmissions(ctx, `SELECT `+submissionColumns+` FROM submission WHERE id IN (
			SELECT submission_id FROM submission_cl WHERE repository_id = $1 AND cl_number = $2)
		ORDER BY created_at DESC, id DESC LIMIT $3`, repo, cl, limit)
}

func (x queries) LockSubmission(ctx context.Context, id int64) (*store.Submission, error) {
	subs, err := x.loadSubmissions(ctx, `SELECT `+submissionColumns+` FROM submission WHERE id = $1 FOR UPDATE`, id)
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
	err := x.q.QueryRow(ctx, `UPDATE submission SET status = $3, reason = $4, note = '', finished_at = now(),
		canceled_by = nullif($5, '') WHERE id = $1 AND status = $2 RETURNING repository_id`,
		id, store.SubmissionActive, status, reason, by).Scan(&repo)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return repo, err == nil, err
}

func (x queries) ActiveSubmissionsOfCL(ctx context.Context, repo, cl int64, below int32) ([]int64, error) {
	return ids(x.q.Query(ctx, `SELECT s.id FROM submission s JOIN submission_cl sc ON sc.submission_id = s.id
		WHERE sc.repository_id = $1 AND sc.cl_number = $2 AND s.status = $3 AND ($4 = 0 OR sc.patchset < $4)
		ORDER BY s.created_at, s.id`, repo, cl, store.SubmissionActive, below))
}

func (x queries) SetNote(ctx context.Context, id int64, note string) error {
	_, err := x.q.Exec(ctx, `UPDATE submission SET note = $3 WHERE id = $1 AND status = $2 AND note <> $3`,
		id, store.SubmissionActive, note)
	return err
}

const attemptColumns = `id, submission_id, base, base_seq, chain, status, released, prepare_error, reason, created_at, finished_at`

// loadAttempts reads the attempts a query selects (by attemptColumns), in its order, with their
// candidates.
func (x queries) loadAttempts(ctx context.Context, sql string, args ...any) ([]*store.Attempt, error) {
	rows, err := x.q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	var out []*store.Attempt
	byID := map[int64]*store.Attempt{}
	for rows.Next() {
		var a store.Attempt
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
	attemptIDs := make([]int64, 0, len(out))
	for _, a := range out {
		attemptIDs = append(attemptIDs, a.ID)
	}
	rows, err = x.q.Query(ctx, `SELECT attempt_id, submission_id, cl_number, patchset, commit_id, base, coalesce(plan_id, 0)
		FROM attempt_candidate WHERE attempt_id = ANY($1) ORDER BY attempt_id, position`, attemptIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var c store.AttemptCandidate
		if err := rows.Scan(&id, &c.SubmissionID, &c.CLNumber, &c.Patchset, &c.Commit, &c.Base, &c.PlanID); err != nil {
			return nil, err
		}
		byID[id].Candidates = append(byID[id].Candidates, &c)
	}
	return out, rows.Err()
}

func (x queries) Attempts(ctx context.Context, submissions []int64, status store.AttemptStatus) ([]*store.Attempt, error) {
	return x.loadAttempts(ctx, `SELECT `+attemptColumns+` FROM attempt
		WHERE submission_id = ANY($1) AND ($2 = 0 OR status = $2) ORDER BY created_at, id`, submissions, status)
}

func (x queries) ActiveAttempt(ctx context.Context, submission int64) (int64, bool, error) {
	var id int64
	err := x.q.QueryRow(ctx, `SELECT id FROM attempt WHERE submission_id = $1 AND status = $2 ORDER BY created_at, id LIMIT 1`,
		submission, store.AttemptActive).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return id, err == nil, err
}

func (x queries) LockAttempt(ctx context.Context, id int64) (*store.Attempt, error) {
	as, err := x.loadAttempts(ctx, `SELECT `+attemptColumns+` FROM attempt WHERE id = $1 FOR UPDATE`, id)
	if err != nil {
		return nil, err
	}
	if len(as) == 0 {
		return nil, store.ErrNotFound
	}
	return as[0], nil
}

func (x queries) InsertAttempt(ctx context.Context, a store.NewAttemptRow) (int64, error) {
	var id int64
	err := x.q.QueryRow(ctx, `INSERT INTO attempt (id, submission_id, base, base_seq, chain, status, prepare_error, created_at)
		VALUES ($7, $1, $2, $3, $4, $5, $6, clock_timestamp()) RETURNING id`, a.SubmissionID, a.Base, a.BaseSeq, orEmpty(a.Chain), store.AttemptActive,
		a.PrepareError, data.NewID()).Scan(&id)
	return id, err
}

func (x queries) InsertCandidate(ctx context.Context, attempt int64, position int, repo int64, c store.AttemptCandidate) error {
	_, err := x.q.Exec(ctx, `INSERT INTO attempt_candidate (attempt_id, position, repository_id, submission_id,
			cl_number, patchset, commit_id, base, plan_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		attempt, position, repo, c.SubmissionID, c.CLNumber, c.Patchset, c.Commit, c.Base, nullable(c.PlanID))
	return err
}

func (x queries) SetReleased(ctx context.Context, id int64) error {
	_, err := x.q.Exec(ctx, `UPDATE attempt SET released = true WHERE id = $1`, id)
	return err
}

func (x queries) SetAttemptStatus(ctx context.Context, id int64, from, to store.AttemptStatus, reason string) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE attempt SET status = $3, reason = $4, finished_at = now()
		WHERE id = $1 AND ($2 = 0 OR status = $2)`, id, from, to, reason)
	return tag.RowsAffected() > 0, err
}

func (x queries) EndActiveAttempts(ctx context.Context, submission int64, status store.AttemptStatus, reason string) ([]int64, error) {
	got, err := ids(x.q.Query(ctx, `UPDATE attempt SET status = $3, reason = $4, finished_at = now()
		WHERE submission_id = $1 AND status = $2 RETURNING id`, submission, store.AttemptActive, status, reason))
	if err != nil {
		return nil, err
	}
	sortIDs(got)
	return got, nil
}

func (x queries) UnfinishedRunsOfAttempts(ctx context.Context, attempts []int64) ([]int64, error) {
	return ids(x.q.Query(ctx, `SELECT r.id FROM run r JOIN attempt_candidate ac ON ac.plan_id = r.plan_id
		WHERE ac.attempt_id = ANY($1) AND r.status IN ($2, $3) ORDER BY r.queued_at, r.id`, attempts, store.StatusQueued, store.StatusRunning))
}

func (x queries) ReusableCandidate(ctx context.Context, repo int64, base []byte, cl int64, patchset int32) ([]byte, bool, error) {
	var commit []byte
	err := x.q.QueryRow(ctx, `SELECT ac.commit_id FROM attempt_candidate ac JOIN attempt a ON a.id = ac.attempt_id
		WHERE ac.repository_id = $1 AND ac.base = $2 AND ac.cl_number = $3 AND ac.patchset = $4 AND a.status = $5
		ORDER BY a.created_at DESC, a.id DESC LIMIT 1`, repo, base, cl, patchset, store.AttemptActive).Scan(&commit)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	return commit, err == nil, err
}

func (x queries) CandidatesInUse(ctx context.Context, repo int64) (map[string]bool, error) {
	rows, err := x.q.Query(ctx, `SELECT ac.commit_id FROM attempt_candidate ac JOIN attempt a ON a.id = ac.attempt_id
		WHERE ac.repository_id = $1 AND a.status = $2`, repo, store.AttemptActive)
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

func (x queries) SetLandedCommit(ctx context.Context, submission, cl int64, commit []byte) error {
	_, err := x.q.Exec(ctx, `UPDATE submission_cl SET landed_commit = $3 WHERE submission_id = $1 AND cl_number = $2`,
		submission, cl, commit)
	return err
}

func (x queries) InsertLandedChange(ctx context.Context, repo int64, commit []byte, cl int64) error {
	_, err := x.q.Exec(ctx, `INSERT INTO landed_change (repository_id, commit_id, cl_number) VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING`, repo, commit, cl)
	return err
}

func (x queries) LockMain(ctx context.Context, repo int64) error {
	return lock.InTx(ctx, x.q, "main", strconv.FormatInt(repo, 10))
}

func (x queries) InsertMainCommit(ctx context.Context, repo, seq int64, commit []byte) error {
	_, err := x.q.Exec(ctx, `INSERT INTO main_commit (repository_id, seq, commit_id, submission_id)
		VALUES ($1, $2, $3, (SELECT sc.submission_id FROM landed_change lc
			JOIN submission_cl sc ON sc.repository_id = lc.repository_id AND sc.cl_number = lc.cl_number AND sc.landed_commit = lc.commit_id
			WHERE lc.repository_id = $1 AND lc.commit_id = $3 LIMIT 1))
		ON CONFLICT DO NOTHING`, repo, seq, commit)
	return err
}

func (x queries) PutMainCommit(ctx context.Context, repo, seq int64, commit []byte, submission int64) error {
	_, err := x.q.Exec(ctx, `INSERT INTO main_commit (repository_id, seq, commit_id, submission_id)
		VALUES ($1, $2, $3, $4) ON CONFLICT (repository_id, seq) DO UPDATE SET submission_id = EXCLUDED.submission_id`,
		repo, seq, commit, submission)
	return err
}

func (x queries) SetMainCommitSubmission(ctx context.Context, repo int64, commit []byte, submission int64) error {
	_, err := x.q.Exec(ctx, `UPDATE main_commit SET submission_id = $3 WHERE repository_id = $1 AND commit_id = $2`,
		repo, commit, submission)
	return err
}

func (x queries) MainCommitsAfter(ctx context.Context, repo, from int64) ([]store.MainCommit, error) {
	rows, err := x.q.Query(ctx, `SELECT mc.seq, mc.commit_id, coalesce(lc.cl_number, 0), coalesce(mc.submission_id, 0)
		FROM main_commit mc LEFT JOIN landed_change lc ON lc.repository_id = mc.repository_id AND lc.commit_id = mc.commit_id
		WHERE mc.repository_id = $1 AND mc.seq > $2 ORDER BY mc.seq`, repo, from)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.MainCommit, error) {
		var m store.MainCommit
		err := r.Scan(&m.Seq, &m.Commit, &m.CLNumber, &m.SubmissionID)
		return m, err
	})
}

func (x queries) LandedCLs(ctx context.Context, repo int64, cls []int64) (map[int64][]byte, error) {
	rows, err := x.q.Query(ctx, `SELECT cl_number, commit_id FROM landed_change WHERE repository_id = $1 AND cl_number = ANY($2)`,
		repo, cls)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64][]byte{}
	for rows.Next() {
		var cl int64
		var commit []byte
		if err := rows.Scan(&cl, &commit); err != nil {
			return nil, err
		}
		out[cl] = commit
	}
	return out, rows.Err()
}
