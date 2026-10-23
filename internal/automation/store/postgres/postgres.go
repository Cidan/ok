// Package postgres is automation's store backend (internal/automation/store) on PostgreSQL, in
// the schema automation: a queued run claimed with FOR UPDATE SKIP LOCKED, the waits woken with
// LISTEN and NOTIFY.
package postgres

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/migrate"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are automation's schema.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Backend is automation's store backend on PostgreSQL.
type Backend struct {
	queries
	pool  *pgxpool.Pool
	locks *lock.Lock
}

var _ store.Backend = (*Backend)(nil)

// Open brings the database behind pool to automation's schema and returns a backend over it.
func Open(ctx context.Context, pool *pgxpool.Pool) (*Backend, error) {
	if _, err := migrate.Apply(ctx, pool, "automation", Migrations); err != nil {
		return nil, err
	}
	return &Backend{queries: queries{pool}, pool: pool, locks: lock.New(pool)}, nil
}

// Txn runs fn in a transaction, again if PostgreSQL aborts it to break a deadlock or a
// serialization conflict.
func (b *Backend) Txn(ctx context.Context, fn func(store.Ops) error) error {
	return pgdata.Txn(ctx, b.pool, func(tx pgx.Tx) error { return fn(queries{tx}) })
}

// Snapshot runs fn in a read-only, repeatable-read transaction.
func (b *Backend) Snapshot(ctx context.Context, fn func(store.Ops) error) error {
	return pgdata.Snapshot(ctx, b.pool, func(tx pgx.Tx) error { return fn(queries{tx}) })
}

func (b *Backend) Lead(ctx context.Context, role string, fn lock.LockFn) error {
	return b.locks.LockFn(ctx, "leaders", role, 0, fn)
}

// Listen holds a connection of its own, out of the pool, listening on channels.
func (b *Backend) Listen(ctx context.Context, channels []string, ready func(), deliver func(channel, payload string)) error {
	return pgdata.Listen(ctx, b.pool, channels, ready, deliver)
}

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// queries are the backend's operations over a pool or a transaction.
type queries struct{ q querier }

func (x queries) Notify(ctx context.Context, channel, payload string) error {
	_, err := x.q.Exec(ctx, `SELECT pg_notify($1, $2)`, channel, payload)
	return err
}

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
	log_stored, timeout_seconds, coalesce(plan_id, 0), why, queued_at, started_at, finished_at, coalesce(carried_from, 0)`

func scanRun(row pgx.Row) (*store.Run, error) {
	var r store.Run
	var cl, rerun *int64
	var patchset *int32
	var started, finished *time.Time
	err := row.Scan(&r.ID, &r.RepositoryID, &r.CommitID, &r.Job.Kind, &r.Job.Targets, &r.Job.Args,
		&r.Origin.Phase, &cl, &patchset, &r.Origin.Job, &r.RequestedBy, &rerun, &r.Status, &r.Attempt, &r.Worker,
		&r.ExitCode, &r.Reason, &r.LogSize, &r.LogStored, &r.TimeoutSeconds, &r.PlanID, &r.Why, &r.QueuedAt, &started, &finished,
		&r.CarriedFrom)
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

const insertRun = `INSERT INTO run (repository_id, commit_id, kind, targets, args, phase, cl_number, patchset, job,
		plan_id, why, requested_by, rerun_of, status, reason, timeout_seconds, priority,
		started_at, finished_at, log_stored, carried_from)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
		CASE WHEN $18::boolean THEN now() END, CASE WHEN $19::boolean THEN now() END, $20, $21)`

func runArgs(r store.NewRun) []any {
	return []any{r.RepositoryID, r.CommitID, r.Job.Kind, orEmpty(r.Job.Targets), orEmpty(r.Job.Args), r.Origin.Phase,
		nullable(r.Origin.CLNumber), nullable(r.Origin.Patchset), r.Origin.Job, nullable(r.PlanID), r.Why, r.RequestedBy,
		nullable(r.RerunOf), r.Status, r.Reason, r.TimeoutSeconds, r.Priority, r.Started, r.Finished, r.LogStored,
		nullable(r.CarriedFrom)}
}

func (x queries) InsertRun(ctx context.Context, r store.NewRun) (*store.Run, error) {
	return scanRun(x.q.QueryRow(ctx, insertRun+` RETURNING `+runColumns, runArgs(r)...))
}

func (x queries) InsertRuns(ctx context.Context, rs []store.NewRun) error {
	if len(rs) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range rs {
		batch.Queue(insertRun, runArgs(r)...)
	}
	return x.q.SendBatch(ctx, batch).Close()
}

func (x queries) Run(ctx context.Context, id int64) (*store.Run, error) {
	return scanRun(x.q.QueryRow(ctx, `SELECT `+runColumns+` FROM run WHERE id = $1`, id))
}

func (x queries) LockRun(ctx context.Context, id int64) (*store.Run, error) {
	return scanRun(x.q.QueryRow(ctx, `SELECT `+runColumns+` FROM run WHERE id = $1 FOR UPDATE`, id))
}

func (x queries) RunsOfCL(ctx context.Context, repo, cl int64, patchset int32, before int64, limit int) ([]*store.Run, error) {
	return collectRuns(x.q.Query(ctx, `SELECT `+runColumns+` FROM run
		WHERE repository_id = $1 AND cl_number = $2 AND ($3 = 0 OR patchset = $3) AND id < $4
		ORDER BY id DESC LIMIT $5`, repo, cl, patchset, before, limit))
}

func (x queries) RunsOfCommit(ctx context.Context, repo int64, commit []byte, before int64, limit int) ([]*store.Run, error) {
	return collectRuns(x.q.Query(ctx, `SELECT `+runColumns+` FROM run
		WHERE repository_id = $1 AND commit_id = $2 AND id < $3
		ORDER BY id DESC LIMIT $4`, repo, commit, before, limit))
}

func (x queries) PlanRuns(ctx context.Context, plans []int64) ([]*store.Run, error) {
	return collectRuns(x.q.Query(ctx, `SELECT `+runColumns+` FROM run WHERE plan_id = ANY($1) ORDER BY id`, plans))
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

func (x queries) ClaimRun(ctx context.Context, worker string, ttl time.Duration) (*store.Run, error) {
	r, err := scanRun(x.q.QueryRow(ctx, `UPDATE run
		SET status = $2, worker = $3, lease_expires = now() + make_interval(secs => $4), started_at = now()
		WHERE id = (SELECT id FROM run WHERE status = $1 ORDER BY priority DESC, id LIMIT 1 FOR UPDATE SKIP LOCKED)
		RETURNING `+runColumns, store.StatusQueued, store.StatusRunning, worker, ttl.Seconds()))
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	return r, err
}

func (x queries) RenewLease(ctx context.Context, id int64, worker string, ttl time.Duration) (store.Lease, bool, error) {
	var l store.Lease
	var stop *string
	err := x.q.QueryRow(ctx, `UPDATE run SET lease_expires = now() + make_interval(secs => $3)
		WHERE id = $1 AND status = $4 AND worker = $2
		RETURNING stop_reason, now() > started_at + make_interval(secs => timeout_seconds), timeout_seconds`,
		id, worker, ttl.Seconds(), store.StatusRunning).Scan(&stop, &l.Overdue, &l.TimeoutSeconds)
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

func (x queries) LapsedRuns(ctx context.Context) ([]store.Lapsed, error) {
	rows, err := x.q.Query(ctx, `SELECT id, attempt, coalesce(worker, ''), stop_reason FROM run
		WHERE status = $1 AND lease_expires < now() ORDER BY id FOR UPDATE SKIP LOCKED`, store.StatusRunning)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.Lapsed, error) {
		var l store.Lapsed
		var stop *string
		if err := r.Scan(&l.ID, &l.Attempt, &l.Worker, &stop); err != nil {
			return l, err
		}
		if stop != nil {
			l.Stopping, l.StopReason = true, *stop
		}
		return l, nil
	})
}

func (x queries) RequeueRun(ctx context.Context, id int64, reason string, nextAttempt bool) error {
	_, err := x.q.Exec(ctx, `WITH cleared AS (DELETE FROM log_chunk WHERE run_id = $1)
		UPDATE run SET status = $2, reason = $3, attempt = attempt + CASE WHEN $4::boolean THEN 1 ELSE 0 END,
			worker = NULL, lease_expires = NULL, started_at = NULL, finished_at = NULL, stop_reason = NULL, log_size = 0
		WHERE id = $1`, id, store.StatusQueued, reason, nextAttempt)
	return err
}

func (x queries) FinishRun(ctx context.Context, id int64, status store.Status, exitCode int32, reason string) error {
	_, err := x.q.Exec(ctx, `UPDATE run SET status = $2, exit_code = $3, reason = $4, finished_at = now(),
		lease_expires = NULL, stop_reason = NULL WHERE id = $1`, id, status, exitCode, reason)
	return err
}

func (x queries) CancelQueuedRun(ctx context.Context, id int64, reason string) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE run SET status = $2, reason = $3, finished_at = now(), log_stored = true
		WHERE id = $1 AND status = $4`, id, store.StatusCanceled, reason, store.StatusQueued)
	return tag.RowsAffected() > 0, err
}

func (x queries) RequestStop(ctx context.Context, id int64, reason string) error {
	_, err := x.q.Exec(ctx, `UPDATE run SET stop_reason = $2 WHERE id = $1 AND status = $3 AND stop_reason IS NULL`,
		id, reason, store.StatusRunning)
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

func (x queries) AppendLogChunk(ctx context.Context, id, at int64, data []byte) error {
	_, err := x.q.Exec(ctx, `WITH chunk AS (INSERT INTO log_chunk (run_id, at, data) VALUES ($1, $2, $3))
		UPDATE run SET log_size = $4 WHERE id = $1`, id, at, data, at+int64(len(data)))
	return err
}

func (x queries) LogChunks(ctx context.Context, id, from int64) ([]store.LogChunk, error) {
	rows, err := x.q.Query(ctx, `SELECT at, data FROM log_chunk
		WHERE run_id = $1 AND at + length(data) > $2 ORDER BY at`, id, from)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.LogChunk, error) {
		var c store.LogChunk
		err := r.Scan(&c.At, &c.Data)
		return c, err
	})
}

func (x queries) MarkLogStored(ctx context.Context, id int64) error {
	_, err := x.q.Exec(ctx, `WITH cleared AS (DELETE FROM log_chunk WHERE run_id = $1)
		UPDATE run SET log_stored = true WHERE id = $1`, id)
	return err
}

func (x queries) UnstoredLogs(ctx context.Context, limit int) ([]int64, error) {
	return ids(x.q.Query(ctx, `SELECT id FROM run WHERE status >= $1 AND NOT log_stored ORDER BY id LIMIT $2`, store.StatusPassed, limit))
}

// ─── The feed, and the build cache's commits ───

func (x queries) MarkWork(ctx context.Context, kind string, key int64) error {
	return pgdata.MarkWork(ctx, x.q, kind, key)
}

func (x queries) TakeWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	return pgdata.TakeWork(ctx, x.q, kind, key, holder, ttl)
}

func (x queries) RenewWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	return pgdata.RenewWork(ctx, x.q, kind, key, holder, ttl)
}

func (x queries) ReleaseWork(ctx context.Context, kind string, key int64, holder string) (bool, error) {
	return pgdata.ReleaseWork(ctx, x.q, kind, key, holder)
}

func (x queries) DueWork(ctx context.Context, kind string, limit int) ([]int64, error) {
	return pgdata.DueWork(ctx, x.q, kind, limit)
}

func (x queries) LegacyCursor(ctx context.Context) (int64, error) {
	var seq int64
	err := x.q.QueryRow(ctx, `SELECT coalesce((SELECT seq FROM feed_cursor), 0)`).Scan(&seq)
	return seq, err
}

func (x queries) ForgetLegacyCursor(ctx context.Context) error {
	_, err := x.q.Exec(ctx, `DELETE FROM feed_cursor`)
	return err
}

func (x queries) FollowCursor(ctx context.Context, topic string, key int64) (int64, error) {
	return pgdata.Cursor(ctx, x.q, topic, key)
}

func (x queries) AdvanceFollowCursor(ctx context.Context, topic string, key, seq int64) error {
	return pgdata.AdvanceCursor(ctx, x.q, topic, key, seq)
}

func (x queries) SeedFollowCursor(ctx context.Context, topic string, key, seq int64) error {
	return pgdata.SeedCursor(ctx, x.q, topic, key, seq)
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
	h := fnv.New64a()
	fmt.Fprintf(h, "ok-automation plan %d/%d", repo, cl)
	_, err := x.q.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(h.Sum64()))
	return err
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
	return scanPlan(x.q.QueryRow(ctx, `INSERT INTO plan (repository_id, cl_number, patchset, phase, commit_id,
			workflows_commit, context, touched, pre_commit)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING `+planColumns,
		p.RepositoryID, p.CLNumber, p.Patchset, p.Phase, p.CommitID, p.WorkflowsCommit, p.Context, touched, p.PreCommit))
}

func (x queries) Plan(ctx context.Context, id int64) (*store.Plan, error) {
	return scanPlan(x.q.QueryRow(ctx, `SELECT `+planColumns+` FROM plan WHERE id = $1`, id))
}

func (x queries) LockPlan(ctx context.Context, id int64) (*store.Plan, error) {
	return scanPlan(x.q.QueryRow(ctx, `SELECT `+planColumns+` FROM plan WHERE id = $1 FOR UPDATE`, id))
}

func (x queries) Plans(ctx context.Context, ids []int64) ([]*store.Plan, error) {
	rows, err := x.q.Query(ctx, `SELECT `+planColumns+` FROM plan WHERE id = ANY($1) ORDER BY id`, ids)
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
		WHERE sc.repository_id = $1 AND sc.cl_number = ANY($2) AND s.status = $3 ORDER BY s.id LIMIT 1`,
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
			INSERT INTO submission (repository_id, submitted_by, status) VALUES ($1, $2, $3) RETURNING id),
		c AS (
			INSERT INTO submission_cl (submission_id, position, repository_id, cl_number, patchset)
			SELECT s.id, u.ord - 1, $1, u.cl, u.ps FROM s, unnest($4::bigint[], $5::int[]) WITH ORDINALITY AS u(cl, ps, ord))
		SELECT id FROM s`, repo, by, store.SubmissionActive, numbers, patchsets).Scan(&id)
	return id, err
}

const submissionColumns = `id, repository_id, submitted_by, status, reason, note, created_at, finished_at`

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
	return x.loadSubmissions(ctx, `SELECT `+submissionColumns+` FROM submission WHERE id = ANY($1) ORDER BY id`, ids)
}

func (x queries) ActiveSubmissions(ctx context.Context, repo int64) ([]*store.Submission, error) {
	return x.loadSubmissions(ctx, `SELECT `+submissionColumns+` FROM submission
		WHERE repository_id = $1 AND status = $2 ORDER BY id`, repo, store.SubmissionActive)
}

func (x queries) SubmissionsOfCL(ctx context.Context, repo, cl int64, limit int) ([]*store.Submission, error) {
	return x.loadSubmissions(ctx, `SELECT `+submissionColumns+` FROM submission WHERE id IN (
			SELECT submission_id FROM submission_cl WHERE repository_id = $1 AND cl_number = $2)
		ORDER BY id DESC LIMIT $3`, repo, cl, limit)
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

func (x queries) FinishSubmission(ctx context.Context, id int64, status store.SubmissionStatus, reason string) (int64, bool, error) {
	var repo int64
	err := x.q.QueryRow(ctx, `UPDATE submission SET status = $3, reason = $4, note = '', finished_at = now()
		WHERE id = $1 AND status = $2 RETURNING repository_id`, id, store.SubmissionActive, status, reason).Scan(&repo)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return repo, err == nil, err
}

func (x queries) ActiveSubmissionsOfCL(ctx context.Context, repo, cl int64, below int32) ([]int64, error) {
	return ids(x.q.Query(ctx, `SELECT s.id FROM submission s JOIN submission_cl sc ON sc.submission_id = s.id
		WHERE sc.repository_id = $1 AND sc.cl_number = $2 AND s.status = $3 AND ($4 = 0 OR sc.patchset < $4)
		ORDER BY s.id`, repo, cl, store.SubmissionActive, below))
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
		WHERE submission_id = ANY($1) AND ($2 = 0 OR status = $2) ORDER BY id`, submissions, status)
}

func (x queries) ActiveAttempt(ctx context.Context, submission int64) (int64, bool, error) {
	var id int64
	err := x.q.QueryRow(ctx, `SELECT id FROM attempt WHERE submission_id = $1 AND status = $2 ORDER BY id LIMIT 1`,
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
	err := x.q.QueryRow(ctx, `INSERT INTO attempt (submission_id, base, base_seq, chain, status, prepare_error)
		VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`, a.SubmissionID, a.Base, a.BaseSeq, orEmpty(a.Chain), store.AttemptActive,
		a.PrepareError).Scan(&id)
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
		WHERE ac.attempt_id = ANY($1) AND r.status IN ($2, $3) ORDER BY r.id`, attempts, store.StatusQueued, store.StatusRunning))
}

func (x queries) ReusableCandidate(ctx context.Context, repo int64, base []byte, cl int64, patchset int32) ([]byte, bool, error) {
	var commit []byte
	err := x.q.QueryRow(ctx, `SELECT ac.commit_id FROM attempt_candidate ac JOIN attempt a ON a.id = ac.attempt_id
		WHERE ac.repository_id = $1 AND ac.base = $2 AND ac.cl_number = $3 AND ac.patchset = $4 AND a.status = $5
		ORDER BY a.id DESC LIMIT 1`, repo, base, cl, patchset, store.AttemptActive).Scan(&commit)
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

func (x queries) QueueRepositories(ctx context.Context, recent time.Duration) ([]int64, error) {
	return ids(x.q.Query(ctx, `SELECT DISTINCT repository_id FROM submission
		WHERE status = $1 OR finished_at > now() - make_interval(secs => $2) ORDER BY repository_id`,
		store.SubmissionActive, recent.Seconds()))
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

func (x queries) InsertMainCommit(ctx context.Context, repo, seq int64, commit []byte) error {
	_, err := x.q.Exec(ctx, `INSERT INTO main_commit (repository_id, seq, commit_id) VALUES ($1, $2, $3)
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
