// Package postgres is the deployer's store backend (internal/deploy/store) on PostgreSQL, in the
// schema deploy.
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

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/workflow"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are the deployer's schema.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Backend is the deployer's store backend on PostgreSQL.
type Backend struct {
	queries
	pool  *pgxpool.Pool
	locks *lock.Lock
}

var _ store.Backend = (*Backend)(nil)

// Open brings the database behind pool to the deployer's schema and returns a backend over it.
func Open(ctx context.Context, pool *pgxpool.Pool) (*Backend, error) {
	if _, err := migrate.Apply(ctx, pool, "deploy", Migrations); err != nil {
		return nil, err
	}
	return &Backend{queries: queries{pool}, pool: pool, locks: lock.New(pool)}, nil
}

// Txn runs fn in a transaction, again if PostgreSQL aborts it to break a deadlock or a
// serialization conflict.
func (b *Backend) Txn(ctx context.Context, fn func(store.Ops) error) error {
	return pgdata.Txn(ctx, b.pool, func(tx pgx.Tx) error { return fn(queries{tx}) })
}

func (b *Backend) Lead(ctx context.Context, role string, fn lock.LockFn) error {
	return b.locks.LockFn(ctx, "leaders", role, 0, fn)
}

func (b *Backend) TryLockDeployment(ctx context.Context, id int64) (release func(), ok bool, err error) {
	key := strconv.FormatInt(id, 10)
	held, cancel := context.WithCancel(context.WithoutCancel(ctx))
	ok, err = b.locks.TryLock(held, "deployments", key)
	if err != nil || !ok {
		cancel()
		return nil, false, err
	}
	return func() {
		unlockCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.locks.Unlock(unlockCtx, "deployments", key)
		done()
		cancel()
	}, true, nil
}

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// queries are the backend's operations over a pool or a transaction.
type queries struct{ q querier }

// ─── The feed ───

func (x queries) LegacyCursor(ctx context.Context) (int64, error) {
	var seq int64
	err := x.q.QueryRow(ctx, `SELECT coalesce(max(seq), 0) FROM feed_cursor`).Scan(&seq)
	return seq, err
}

func (x queries) ForgetLegacyCursor(ctx context.Context) error {
	_, err := x.q.Exec(ctx, `DELETE FROM feed_cursor`)
	return err
}

func (x queries) FollowCursor(ctx context.Context, topic string, key int64) (int64, error) {
	return pgdata.Cursor(ctx, x.q, topic, key)
}

func (x queries) LockFollowCursor(ctx context.Context, topic string, key int64) (int64, error) {
	return pgdata.LockCursor(ctx, x.q, topic, key)
}

func (x queries) SetFollowCursor(ctx context.Context, topic string, key, seq int64) error {
	return pgdata.SetCursor(ctx, x.q, topic, key, seq)
}

func (x queries) SeedFollowCursor(ctx context.Context, topic string, key, seq int64) error {
	return pgdata.SeedCursor(ctx, x.q, topic, key, seq)
}

func (x queries) PutRepository(ctx context.Context, id int64, name string) error {
	_, err := x.q.Exec(ctx, `INSERT INTO repository (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = $2`, id, name)
	return err
}

func (x queries) RenameRepository(ctx context.Context, id int64, name string) error {
	_, err := x.q.Exec(ctx, `UPDATE repository SET name = $2 WHERE id = $1`, id, name)
	return err
}

func (x queries) EnsureRepository(ctx context.Context, id int64) error {
	_, err := x.q.Exec(ctx, `INSERT INTO repository (id, name) VALUES ($1, '') ON CONFLICT DO NOTHING`, id)
	return err
}

func (x queries) AdvanceHead(ctx context.Context, id int64, head []byte, seq int64) error {
	_, err := x.q.Exec(ctx, `UPDATE repository SET head = $2, head_seq = $3 WHERE id = $1 AND head_seq < $3`, id, head, seq)
	return err
}

func (x queries) AddMainCommit(ctx context.Context, repo, seq int64, commit []byte) error {
	_, err := x.q.Exec(ctx, `INSERT INTO main_commit (repository_id, seq, commit_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, repo, seq, commit)
	return err
}

func (x queries) MainCommit(ctx context.Context, repo int64, commit []byte) (seq int64, planned, ok bool, err error) {
	err = x.q.QueryRow(ctx, `SELECT m.seq, r.planned_seq >= m.seq FROM main_commit m JOIN repository r ON r.id = m.repository_id
		WHERE m.repository_id = $1 AND m.commit_id = $2`, repo, commit).Scan(&seq, &planned)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, false, nil
	}
	return seq, planned, err == nil, err
}

func (x queries) Unplanned(ctx context.Context) ([]store.Repository, error) {
	rows, err := x.q.Query(ctx, `SELECT id, name, head, head_seq, planned_head, planned_seq FROM repository
		WHERE head_seq > planned_seq ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.Repository, error) {
		var x store.Repository
		err := r.Scan(&x.ID, &x.Name, &x.Head, &x.HeadSeq, &x.PlannedHead, &x.PlannedSeq)
		return x, err
	})
}

// ─── Plans ───

func (x queries) LockPlannedSeq(ctx context.Context, repo int64) (int64, error) {
	var planned int64
	err := x.q.QueryRow(ctx, `SELECT planned_seq FROM repository WHERE id = $1 FOR UPDATE`, repo).Scan(&planned)
	return planned, err
}

func (x queries) InsertPlan(ctx context.Context, repo int64, head []byte, headSeq, baseSeq int64, notes []string) (int64, error) {
	var id int64
	err := x.q.QueryRow(ctx, `INSERT INTO plan (repository_id, head, head_seq, base_seq, notes) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		repo, head, headSeq, baseSeq, notes).Scan(&id)
	return id, err
}

func (x queries) SetPlanned(ctx context.Context, repo int64, head []byte, seq int64) error {
	_, err := x.q.Exec(ctx, `UPDATE repository SET planned_head = $2, planned_seq = $3 WHERE id = $1`, repo, head, seq)
	return err
}

func (x queries) Plans(ctx context.Context, repo int64) ([]store.Plan, error) {
	rows, err := x.q.Query(ctx, `SELECT id, repository_id, head, head_seq, base_seq, notes, created_at FROM plan
		WHERE $1 = 0 OR repository_id = $1 ORDER BY id`, repo)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.Plan, error) {
		var p store.Plan
		err := r.Scan(&p.ID, &p.Repository, &p.Head, &p.HeadSeq, &p.BaseSeq, &p.Notes, &p.CreatedAt)
		return p, err
	})
}

func (x queries) RecordServices(ctx context.Context, repo, seq int64, deploys []workflow.Deploy) error {
	ids := []string{}
	for _, d := range deploys {
		w := d.Workload
		if w == nil {
			continue
		}
		ids = append(ids, d.ID())
		if _, err := x.q.Exec(ctx, `INSERT INTO service (repository_id, deploy, name, namespace, kind, workload, declared, declared_seq, group_name)
			VALUES ($1, $2, $3, $4, $5, $6, true, $7, $8)
			ON CONFLICT (repository_id, deploy) DO UPDATE SET name = $3, namespace = $4, kind = $5, workload = $6, declared = true, declared_seq = $7, group_name = $8`,
			repo, d.ID(), d.Name, w.Namespace, string(w.Kind), w.Name, seq, d.Group); err != nil {
			return err
		}
	}
	_, err := x.q.Exec(ctx, `UPDATE service SET declared = false WHERE repository_id = $1 AND declared AND NOT deploy = ANY($2)`, repo, ids)
	return err
}

// ─── Deployments ───

func (x queries) InsertDeployment(ctx context.Context, d store.DeploymentRow) (int64, error) {
	var ns, kind, workload *string
	if w := d.Deploy.Workload; w != nil {
		k := string(w.Kind)
		ns, kind, workload = &w.Namespace, &k, &w.Name
	}
	var plan, retry *int64
	if d.Plan > 0 {
		plan = &d.Plan
	}
	if d.RetryOf > 0 {
		retry = &d.RetryOf
	}
	var finished *time.Time
	if !d.Status.Open() {
		now := time.Now()
		finished = &now
	}
	var id int64
	if err := x.q.QueryRow(ctx, `INSERT INTO deployment (repository_id, plan_id, retry_of, deploy, name, commit_id, from_seq, to_seq,
			why, wave, requested_by, namespace, kind, workload, status, stage, reason, self, finished_at, alone)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20) RETURNING id`,
		d.Repository, plan, retry, d.Deploy.ID(), d.Deploy.Name, d.Commit, d.FromSeq, d.ToSeq, d.Why, d.Wave, d.RequestedBy,
		ns, kind, workload, d.Status, d.Stage, d.Reason, d.Self, finished, d.Deploy.Alone).Scan(&id); err != nil {
		return 0, err
	}
	for _, im := range d.Deploy.Images {
		if _, err := x.q.Exec(ctx, `INSERT INTO deployment_image (deployment_id, container, target) VALUES ($1, $2, $3)`,
			id, im.Container, im.Target); err != nil {
			return 0, err
		}
	}
	for i, m := range d.Deploy.Migrate {
		if _, err := x.q.Exec(ctx, `INSERT INTO deployment_migration (deployment_id, position, target) VALUES ($1, $2, $3)`,
			id, i, m.Target); err != nil {
			return 0, err
		}
	}
	return id, nil
}

func (x queries) SetGates(ctx context.Context, id int64, gates []store.Gate) error {
	plans, waves := make([]int64, len(gates)), make([]int32, len(gates))
	for i, g := range gates {
		plans[i], waves[i] = g.Plan, int32(g.Wave)
	}
	_, err := x.q.Exec(ctx, `UPDATE deployment SET gate_plans = $2, gate_waves = $3 WHERE id = $1`, id, plans, waves)
	return err
}

func gatesOf(plans []int64, waves []int32) []store.Gate {
	out := make([]store.Gate, 0, len(plans))
	for i := range plans {
		out = append(out, store.Gate{Plan: plans[i], Wave: int(waves[i])})
	}
	return out
}

func (x queries) CancelWaitingOf(ctx context.Context, repo int64, deploy, reason string) ([]store.Replaced, error) {
	rows, err := x.q.Query(ctx, `UPDATE deployment SET status = $3, stage = $4, reason = $5, finished_at = now()
		WHERE repository_id = $1 AND deploy = $2 AND status = $6
		RETURNING coalesce(plan_id, 0), wave, gate_plans, gate_waves`,
		repo, deploy, store.Canceled, store.Done, reason, store.Waiting)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.Replaced, error) {
		var c store.Replaced
		var gp []int64
		var gw []int32
		if err := r.Scan(&c.Plan, &c.Wave, &gp, &gw); err != nil {
			return c, err
		}
		c.Gates = gatesOf(gp, gw)
		return c, nil
	})
}

func (x queries) CancelWaitingIn(ctx context.Context, plan, except int64, reason string) ([]int64, error) {
	rows, err := x.q.Query(ctx, `UPDATE deployment SET status = $3, stage = $4, reason = $5, finished_at = now()
		WHERE (plan_id = $1 OR $1 = ANY(gate_plans)) AND id <> $2 AND status = $6 RETURNING id`,
		plan, except, store.Canceled, store.Done, reason, store.Waiting)
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, err
	}
	sortIDs(ids)
	return ids, nil
}

func sortIDs(ids []int64) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
}

func (x queries) HaltRunningIn(ctx context.Context, plan, except int64, wave int, reason string) error {
	_, err := x.q.Exec(ctx, `UPDATE deployment SET halt_by = $3 WHERE id <> $2 AND status = $4 AND halt_by IS NULL
		AND (plan_id = $1 OR EXISTS (SELECT 1 FROM unnest(gate_plans, gate_waves) AS g(p, w) WHERE g.p = $1 AND g.w >= $5))`,
		plan, except, reason, store.Running, wave)
	return err
}

const deploymentColumns = `id, repository_id, coalesce(plan_id, 0), coalesce(retry_of, 0), deploy, name, commit_id, from_seq, to_seq,
	why, wave, gate_plans, gate_waves, requested_by, namespace, kind, workload, status, stage, reason, roll, self, coalesce(driver, ''), coalesce(halt_by, ''),
	created_at, started_at, stage_at, finished_at, alone`

func scanDeployment(row pgx.Row) (*store.Deployment, error) {
	var d store.Deployment
	var ns, kind, workload *string
	var started, stageAt, finished *time.Time
	var gp []int64
	var gw []int32
	if err := row.Scan(&d.ID, &d.Repository, &d.PlanID, &d.RetryOf, &d.Deploy, &d.Name, &d.Commit, &d.FromSeq, &d.ToSeq,
		&d.Why, &d.Wave, &gp, &gw, &d.RequestedBy, &ns, &kind, &workload, &d.Status, &d.Stage, &d.Reason, &d.Roll, &d.Self, &d.Driver, &d.HaltBy,
		&d.CreatedAt, &started, &stageAt, &finished, &d.Alone); err != nil {
		return nil, err
	}
	if ns != nil {
		d.Workload = &workflow.Workload{Namespace: *ns, Kind: workflow.WorkloadKind(*kind), Name: *workload}
	}
	d.Gates = gatesOf(gp, gw)
	for _, t := range []struct {
		src *time.Time
		dst *time.Time
	}{{started, &d.StartedAt}, {stageAt, &d.StageAt}, {finished, &d.FinishedAt}} {
		if t.src != nil {
			*t.dst = *t.src
		}
	}
	return &d, nil
}

// load reads the deployments a query selects, with their images and migrations, in its order.
func (x queries) load(ctx context.Context, sql string, args ...any) ([]*store.Deployment, error) {
	rows, err := x.q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	var out []*store.Deployment
	byID := map[int64]*store.Deployment{}
	for rows.Next() {
		d, err := scanDeployment(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, d)
		byID[d.ID] = d
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(out) == 0 {
		return out, err
	}
	ids := make([]int64, len(out))
	for i, d := range out {
		ids[i] = d.ID
	}
	irows, err := x.q.Query(ctx, `SELECT deployment_id, container, target, run_id, reference FROM deployment_image
		WHERE deployment_id = ANY($1) ORDER BY deployment_id, container`, ids)
	if err != nil {
		return nil, err
	}
	for irows.Next() {
		var id int64
		var im store.Image
		if err := irows.Scan(&id, &im.Container, &im.Target, &im.RunID, &im.Reference); err != nil {
			irows.Close()
			return nil, err
		}
		byID[id].Images = append(byID[id].Images, im)
	}
	irows.Close()
	if err := irows.Err(); err != nil {
		return nil, err
	}
	mrows, err := x.q.Query(ctx, `SELECT deployment_id, position, target, status, output, report, refused, allowed, coalesce(allowed_by, '')
		FROM deployment_migration WHERE deployment_id = ANY($1) ORDER BY deployment_id, position`, ids)
	if err != nil {
		return nil, err
	}
	defer mrows.Close()
	for mrows.Next() {
		var id int64
		var m store.Migration
		if err := mrows.Scan(&id, &m.Position, &m.Target, &m.Status, &m.Output, &m.Report, &m.Refused, &m.Allowed, &m.AllowedBy); err != nil {
			return nil, err
		}
		byID[id].Migrations = append(byID[id].Migrations, m)
	}
	return out, mrows.Err()
}

func (x queries) Deployments(ctx context.Context, ids []int64) ([]*store.Deployment, error) {
	return x.load(ctx, `SELECT `+deploymentColumns+` FROM deployment WHERE id = ANY($1) ORDER BY id`, ids)
}

func (x queries) LockDeployment(ctx context.Context, id int64) (*store.Deployment, error) {
	ds, err := x.load(ctx, `SELECT `+deploymentColumns+` FROM deployment WHERE id = $1 FOR UPDATE`, id)
	if err != nil || len(ds) == 0 {
		return nil, err
	}
	return ds[0], nil
}

func (x queries) OpenDeployments(ctx context.Context) ([]*store.Deployment, error) {
	return x.load(ctx, `SELECT `+deploymentColumns+` FROM deployment WHERE status IN ($1, $2) ORDER BY id`, store.Waiting, store.Running)
}

func (x queries) RetiringDeployments(ctx context.Context) ([]*store.Deployment, error) {
	return x.load(ctx, `SELECT `+deploymentColumns+` FROM deployment
		WHERE self AND status = $1 AND (roll->>'retire')::boolean ORDER BY id`, store.Succeeded)
}

func (x queries) CoveringDeployments(ctx context.Context, repo, seq int64) ([]*store.Deployment, error) {
	return x.load(ctx, `SELECT `+deploymentColumns+` FROM deployment
		WHERE repository_id = $1 AND from_seq < $2 AND to_seq >= $2 ORDER BY id DESC`, repo, seq)
}

func (x queries) LatestDeployments(ctx context.Context, repo int64, limit int) ([]*store.Deployment, error) {
	return x.load(ctx, `SELECT `+deploymentColumns+` FROM deployment WHERE repository_id = $1 ORDER BY id DESC LIMIT $2`, repo, limit)
}

func (x queries) DeployDeployments(ctx context.Context, repo int64, deploy string, limit int) ([]*store.Deployment, error) {
	return x.load(ctx, `SELECT `+deploymentColumns+` FROM deployment WHERE repository_id = $1 AND deploy = $2 ORDER BY id DESC LIMIT $3`,
		repo, deploy, limit)
}

func (x queries) NewerOf(ctx context.Context, repo int64, deploy string, id int64) (int64, bool, error) {
	var newer int64
	err := x.q.QueryRow(ctx, `SELECT id FROM deployment WHERE repository_id = $1 AND deploy = $2 AND id > $3 ORDER BY id DESC LIMIT 1`,
		repo, deploy, id).Scan(&newer)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return newer, err == nil, err
}

func (x queries) PlanStatuses(ctx context.Context, plan int64) (map[int][]store.Status, error) {
	rows, err := x.q.Query(ctx, `SELECT wave, status FROM deployment WHERE plan_id = $1`, plan)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int][]store.Status{}
	for rows.Next() {
		var w int
		var st store.Status
		if err := rows.Scan(&w, &st); err != nil {
			return nil, err
		}
		out[w] = append(out[w], st)
	}
	return out, rows.Err()
}

func (x queries) AppendEvent(ctx context.Context, id int64, message string) error {
	_, err := x.q.Exec(ctx, `INSERT INTO deployment_event (deployment_id, message) VALUES ($1, $2)`, id, message)
	return err
}

func (x queries) Events(ctx context.Context, id int64) ([]store.Event, error) {
	rows, err := x.q.Query(ctx, `SELECT at, message FROM deployment_event WHERE deployment_id = $1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.Event, error) {
		var e store.Event
		err := r.Scan(&e.At, &e.Message)
		return e, err
	})
}

func (x queries) StartDeployment(ctx context.Context, id int64, driver string) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE deployment SET status = $2, stage = $3, started_at = now(), stage_at = now(), driver = nullif($4, '')
		WHERE id = $1 AND status = $5`, id, store.Running, store.Push, driver, store.Waiting)
	return tag.RowsAffected() > 0, err
}

func (x queries) SetStage(ctx context.Context, id int64, stage store.Stage) error {
	_, err := x.q.Exec(ctx, `UPDATE deployment SET stage = $2, stage_at = now() WHERE id = $1 AND status = $3`, id, stage, store.Running)
	return err
}

func (x queries) SetRoll(ctx context.Context, id int64, roll []byte) error {
	_, err := x.q.Exec(ctx, `UPDATE deployment SET roll = $2 WHERE id = $1`, id, roll)
	return err
}

func (x queries) SetPushRun(ctx context.Context, id int64, container string, run int64) error {
	_, err := x.q.Exec(ctx, `UPDATE deployment_image SET run_id = $3 WHERE deployment_id = $1 AND container = $2`, id, container, run)
	return err
}

func (x queries) SetPushed(ctx context.Context, id int64, container, reference string) error {
	_, err := x.q.Exec(ctx, `UPDATE deployment_image SET reference = $3 WHERE deployment_id = $1 AND container = $2`, id, container, reference)
	return err
}

func (x queries) SetMigration(ctx context.Context, id int64, position int, status store.MigrationStatus, output string, report []byte, refused []string) error {
	_, err := x.q.Exec(ctx, `UPDATE deployment_migration SET status = $3, output = $4, report = $5, refused = $6
		WHERE deployment_id = $1 AND position = $2`, id, position, status, output, report, refused)
	return err
}

func (x queries) FinishDeployment(ctx context.Context, id int64, status store.Status, reason string) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE deployment SET status = $2, stage = $3, reason = $4, finished_at = now()
		WHERE id = $1 AND status IN ($5, $6)`, id, status, store.Done, reason, store.Waiting, store.Running)
	return tag.RowsAffected() > 0, err
}

func (x queries) SetHaltBy(ctx context.Context, id int64, by string) error {
	_, err := x.q.Exec(ctx, `UPDATE deployment SET halt_by = $2 WHERE id = $1 AND halt_by IS NULL`, id, by)
	return err
}

func (x queries) WaitingMigration(ctx context.Context, id int64) (int, []string, bool, error) {
	var position int
	var refused []string
	err := x.q.QueryRow(ctx, `SELECT position, refused FROM deployment_migration WHERE deployment_id = $1 AND status = $2`,
		id, store.MigrationWaiting).Scan(&position, &refused)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, false, nil
	}
	return position, refused, err == nil, err
}

func (x queries) AllowMigration(ctx context.Context, id int64, position int, allowed []string, by string) error {
	_, err := x.q.Exec(ctx, `UPDATE deployment_migration SET allowed = $3, allowed_by = $4, status = $5
		WHERE deployment_id = $1 AND position = $2`, id, position, allowed, by, store.MigrationPending)
	return err
}

// ─── Services ───

var serviceColumns = fmt.Sprintf(`s.id, s.repository_id, r.name, s.deploy, s.name, s.namespace, s.kind, s.workload, s.declared, s.declared_seq, s.group_name,
	coalesce((SELECT d.id FROM deployment d WHERE d.repository_id = s.repository_id AND d.deploy = s.deploy ORDER BY d.id DESC LIMIT 1), 0),
	coalesce((SELECT d.id FROM deployment d WHERE d.repository_id = s.repository_id AND d.deploy = s.deploy AND d.status = %d ORDER BY d.id DESC LIMIT 1), 0)`,
	store.Succeeded)

func (x queries) services(ctx context.Context, where string, arg int64) ([]store.ServiceRow, error) {
	rows, err := x.q.Query(ctx, `SELECT `+serviceColumns+` FROM service s JOIN repository r ON r.id = s.repository_id `+where, arg)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.ServiceRow, error) {
		var sv store.ServiceRow
		var kind string
		err := r.Scan(&sv.ID, &sv.Repository, &sv.RepoName, &sv.Deploy, &sv.Name, &sv.Workload.Namespace, &kind, &sv.Workload.Name,
			&sv.Declared, &sv.DeclaredSeq, &sv.Group, &sv.LatestID, &sv.DeployedID)
		sv.Workload.Kind = workflow.WorkloadKind(kind)
		return sv, err
	})
}

func (x queries) Services(ctx context.Context, repo int64) ([]store.ServiceRow, error) {
	return x.services(ctx, `WHERE ($1 = 0 OR s.repository_id = $1) ORDER BY r.name, s.name, s.id`, repo)
}

func (x queries) Service(ctx context.Context, id int64) (store.ServiceRow, bool, error) {
	out, err := x.services(ctx, `WHERE s.id = $1`, id)
	if err != nil || len(out) == 0 {
		return store.ServiceRow{}, false, err
	}
	return out[0], true, nil
}

func (x queries) RecordObservation(ctx context.Context, service int64, o store.Observation) error {
	imagesMap := o.Images
	if imagesMap == nil {
		imagesMap = map[string]string{}
	}
	images, err := json.Marshal(imagesMap)
	if err != nil {
		return err
	}
	pods := o.Pods
	if pods == nil {
		pods = []store.ObservedPod{}
	}
	podsJSON, err := json.Marshal(pods)
	if err != nil {
		return err
	}
	_, err = x.q.Exec(ctx, `INSERT INTO service_observation (service_id, error, found, live, replicas, ready, images, pods)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (service_id) DO UPDATE SET error = EXCLUDED.error, found = EXCLUDED.found,
			live = EXCLUDED.live, replicas = EXCLUDED.replicas, ready = EXCLUDED.ready, images = EXCLUDED.images, pods = EXCLUDED.pods`,
		service, o.Error, o.Found, o.Live, o.Replicas, o.Ready, images, podsJSON)
	return err
}

func (x queries) Observations(ctx context.Context, ids []int64) (map[int64]*store.Observation, error) {
	rows, err := x.q.Query(ctx, `SELECT service_id, error, found, live, replicas, ready, images, pods
		FROM service_observation WHERE service_id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]*store.Observation{}
	for rows.Next() {
		var id int64
		var o store.Observation
		var images, pods []byte
		if err := rows.Scan(&id, &o.Error, &o.Found, &o.Live, &o.Replicas, &o.Ready, &images, &pods); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(images, &o.Images); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(pods, &o.Pods); err != nil {
			return nil, err
		}
		out[id] = &o
	}
	return out, rows.Err()
}
