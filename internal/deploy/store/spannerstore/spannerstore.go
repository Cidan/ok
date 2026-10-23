// Package spannerstore is the deployer's store backend (internal/deploy/store) on Spanner, in a
// database of its own (docs/hosted.md, "Spanner"). Every write is DML, so a transaction reads what
// it wrote, as on PostgreSQL; an operation outside a transaction is a transaction of its own, and a
// read of several queries is one read-only transaction. A Lock… read is SELECT … FOR UPDATE.
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

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/lock/spannerlock"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/workflow"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are the deployer's schema on Spanner.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Backend is the deployer's store backend on Spanner.
type Backend struct {
	queries
	locks *lock.Lock
	moves *moveRows
}

var _ store.Backend = (*Backend)(nil)

// Open brings the database client reaches to the deployer's schema, through admin, and returns a
// backend over it.
func Open(ctx context.Context, admin *database.DatabaseAdminClient, client *spanner.Client) (*Backend, error) {
	if _, err := spannerdata.Migrate(ctx, admin, client, Migrations); err != nil {
		return nil, err
	}
	return &Backend{queries: queries{spannerdata.Ops{Client: client, Gate: move.SpannerGate}}, locks: lock.NewOn(spannerlock.DB(client)),
		moves: &moveRows{SpannerRows: &move.SpannerRows{Client: client, Tables: moveTables, Where: moveWhere, Renames: moveRenames}, client: client}}, nil
}

func (b *Backend) Txn(ctx context.Context, fn func(store.Ops) error) error {
	return spannerdata.Txn(ctx, b.Client, func(_ context.Context, tx *spanner.ReadWriteTransaction) error {
		if err := move.SpannerGate(ctx, tx); err != nil {
			return err
		}
		return fn(queries{spannerdata.Ops{Client: b.Client, Tx: tx}})
	})
}

func (b *Backend) TryLockDeployment(ctx context.Context, id int64) (fence data.Fence, release func(), ok bool, err error) {
	key := strconv.FormatInt(id, 10)
	held, cancel := context.WithCancel(context.WithoutCancel(ctx))
	fence, ok, err = b.locks.TryLockFenced(held, "deployments", key)
	if err != nil || !ok {
		cancel()
		return data.Fence{}, nil, false, err
	}
	return fence, func() {
		unlockCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		_ = b.locks.Unlock(unlockCtx, "deployments", key)
		done()
		cancel()
	}, true, nil
}

// queries are the backend's operations, in a transaction or each in one of its own.
type queries struct{ spannerdata.Ops }

func nullString(s string) spanner.NullString { return spanner.NullString{StringVal: s, Valid: s != ""} }

// jsonParam is JSON for PARSE_JSON: NULL for none.
func jsonParam(b []byte) spanner.NullString {
	return spanner.NullString{StringVal: string(b), Valid: b != nil}
}

// jsonColumn reads a JSON column as its text, NULL as nil.
func jsonColumn(col string) string {
	return fmt.Sprintf("IF(%s IS NULL, NULL, TO_JSON_STRING(%s))", col, col)
}

func bytesOf(s spanner.NullString) []byte {
	if !s.Valid {
		return nil
	}
	return []byte(s.StringVal)
}

// ─── The feed ───

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

func (x queries) LockFollowCursor(ctx context.Context, topic string, key int64) (int64, error) {
	var seq int64
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		seq, err = spannerdata.LockCursor(ctx, tx, topic, key)
		return err
	})
	return seq, err
}

func (x queries) SetFollowCursor(ctx context.Context, topic string, key, seq int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.SetCursor(ctx, tx, topic, key, seq) })
}

// DropRepository deletes a repository's rows by partitioned DML, each statement apart from any
// transaction: its deployments, their logs, and its services are too many rows for one
// transaction's mutations, and a purged repository is written no more, so a statement run again
// deletes nothing more. main's history and the plans are interleaved in the repository; each
// deployment's and service's own rows are interleaved in it.
func (x queries) DropRepository(ctx context.Context, id int64) error {
	for _, sql := range []string{
		`DELETE FROM deployment WHERE repository_id = @id`,
		`DELETE FROM service WHERE repository_id = @id`,
		`DELETE FROM repository WHERE repository_id = @id`,
	} {
		if _, err := x.Client.PartitionedUpdate(ctx, spanner.Statement{SQL: sql, Params: map[string]any{"id": id}}); err != nil {
			return fmt.Errorf("dropping repository %d: %w", id, err)
		}
	}
	return nil
}

// repositoryExists reports, in tx, whether a repository is recorded.
func repositoryExists(ctx context.Context, tx *spanner.ReadWriteTransaction, id int64) (bool, error) {
	var one int64
	return spannerdata.First(ctx, tx, `SELECT 1 FROM repository WHERE repository_id = @id`, map[string]any{"id": id}, &one)
}

func (x queries) NameRepository(ctx context.Context, id int64, name, owner string) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		ok, err := repositoryExists(ctx, tx, id)
		if err != nil {
			return err
		}
		sql := `INSERT INTO repository (repository_id, name, owner_id) VALUES (@id, @name, @owner)`
		if ok {
			sql = `UPDATE repository SET name = @name, owner_id = IF(@owner = '', owner_id, @owner) WHERE repository_id = @id`
		}
		_, err = spannerdata.Update(ctx, tx, sql, map[string]any{"id": id, "name": name, "owner": owner})
		return err
	})
}

func (x queries) EnsureRepository(ctx context.Context, id int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		ok, err := repositoryExists(ctx, tx, id)
		if err != nil || ok {
			return err
		}
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO repository (repository_id, name) VALUES (@id, '')`, map[string]any{"id": id})
		return err
	})
}

func (x queries) AdvanceHead(ctx context.Context, id int64, head []byte, seq int64) error {
	_, err := x.Exec(ctx, `UPDATE repository SET head = @head, head_seq = @seq WHERE repository_id = @id AND head_seq < @seq`,
		map[string]any{"id": id, "head": head, "seq": seq})
	return err
}

func (x queries) AddMainCommit(ctx context.Context, repo, seq int64, commit []byte) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		p := map[string]any{"repo": repo, "seq": seq, "commit": commit}
		var one int64
		ok, err := spannerdata.First(ctx, tx, `SELECT 1 FROM main_commit WHERE repository_id = @repo AND seq = @seq`, p, &one)
		if err != nil || ok {
			return err
		}
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO main_commit (repository_id, seq, commit_id) VALUES (@repo, @seq, @commit)`, p)
		return err
	})
}

func (x queries) MainCommit(ctx context.Context, repo int64, commit []byte) (seq int64, planned, ok bool, err error) {
	err = x.Read(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT m.seq, r.planned_seq >= m.seq FROM main_commit m JOIN repository r ON r.repository_id = m.repository_id
			WHERE m.repository_id = @repo AND m.commit_id = @commit`, map[string]any{"repo": repo, "commit": commit}, &seq, &planned)
		return err
	})
	return seq, planned, ok, err
}

func (x queries) Unplanned(ctx context.Context) ([]store.Repository, error) {
	var out []store.Repository
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		// A repository the directory has not named yet waits: what is planned depends on its name.
		return spannerdata.Each(ctx, r, `SELECT repository_id, name, head, head_seq, planned_head, planned_seq FROM repository
			WHERE head_seq > planned_seq AND name != '' ORDER BY repository_id`, nil, func(row *spanner.Row) error {
			var rp store.Repository
			if err := row.Columns(&rp.ID, &rp.Name, &rp.Head, &rp.HeadSeq, &rp.PlannedHead, &rp.PlannedSeq); err != nil {
				return err
			}
			out = append(out, rp)
			return nil
		})
	})
	return out, err
}

// ─── Plans ───

func (x queries) LockPlannedSeq(ctx context.Context, repo int64) (int64, error) {
	var planned int64
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		ok, err := spannerdata.First(ctx, tx, `SELECT planned_seq FROM repository WHERE repository_id = @repo FOR UPDATE`, map[string]any{"repo": repo}, &planned)
		if err == nil && !ok {
			err = fmt.Errorf("no repository %d", repo)
		}
		return err
	})
	return planned, err
}

func (x queries) InsertPlan(ctx context.Context, repo int64, head []byte, headSeq, baseSeq int64, notes []string) (int64, error) {
	id := data.NewID()
	_, err := x.Exec(ctx, `INSERT INTO plan (repository_id, id, head, head_seq, base_seq, notes, created_at)
		VALUES (@repo, @id, @head, @headSeq, @baseSeq, @notes, CURRENT_TIMESTAMP())`,
		map[string]any{"repo": repo, "id": id, "head": head, "headSeq": headSeq, "baseSeq": baseSeq, "notes": nonNil(notes)})
	return id, err
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func (x queries) SetPlanned(ctx context.Context, repo int64, head []byte, seq int64) error {
	_, err := x.Exec(ctx, `UPDATE repository SET planned_head = @head, planned_seq = @seq WHERE repository_id = @repo`,
		map[string]any{"repo": repo, "head": head, "seq": seq})
	return err
}

func (x queries) Plans(ctx context.Context, repo int64) ([]store.Plan, error) {
	var out []store.Plan
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT id, repository_id, head, head_seq, base_seq, notes, created_at FROM plan
			WHERE @repo = 0 OR repository_id = @repo ORDER BY created_at, id`, map[string]any{"repo": repo}, func(row *spanner.Row) error {
			var p store.Plan
			if err := row.Columns(&p.ID, &p.Repository, &p.Head, &p.HeadSeq, &p.BaseSeq, &p.Notes, &p.CreatedAt); err != nil {
				return err
			}
			out = append(out, p)
			return nil
		})
	})
	return out, err
}

func (x queries) RecordServices(ctx context.Context, repo, seq int64, deploys []workflow.Deploy) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		ids := []string{}
		for _, d := range deploys {
			w := d.Workload
			if w == nil {
				continue
			}
			ids = append(ids, d.ID())
			p := map[string]any{"repo": repo, "deploy": d.ID(), "name": d.Name, "namespace": w.Namespace, "kind": string(w.Kind),
				"workload": w.Name, "seq": seq, "group": d.Group}
			var id int64
			ok, err := spannerdata.First(ctx, tx, `SELECT service_id FROM service WHERE repository_id = @repo AND deploy = @deploy`, p, &id)
			if err != nil {
				return err
			}
			sql := `UPDATE service SET name = @name, namespace = @namespace, kind = @kind, workload = @workload, declared = TRUE,
				declared_seq = @seq, group_name = @group WHERE service_id = @id`
			if ok {
				p["id"] = id
			} else {
				p["id"] = data.NewID()
				sql = `INSERT INTO service (service_id, repository_id, deploy, name, namespace, kind, workload, declared, declared_seq, group_name)
					VALUES (@id, @repo, @deploy, @name, @namespace, @kind, @workload, TRUE, @seq, @group)`
			}
			if _, err := spannerdata.Update(ctx, tx, sql, p); err != nil {
				return err
			}
		}
		_, err := spannerdata.Update(ctx, tx, `UPDATE service SET declared = FALSE WHERE repository_id = @repo AND declared AND deploy NOT IN UNNEST(@ids)`,
			map[string]any{"repo": repo, "ids": ids})
		return err
	})
}

// ─── Deployments ───

func (x queries) InsertDeployment(ctx context.Context, d store.DeploymentRow) (int64, error) {
	var ns, kind, workload spanner.NullString
	if w := d.Deploy.Workload; w != nil {
		ns, kind, workload = spanner.NullString{StringVal: w.Namespace, Valid: true}, spanner.NullString{StringVal: string(w.Kind), Valid: true},
			spanner.NullString{StringVal: w.Name, Valid: true}
	}
	id := data.NewID()
	p := map[string]any{"id": id, "repo": d.Repository, "plan": spanner.NullInt64{Int64: d.Plan, Valid: d.Plan > 0},
		"retry": spanner.NullInt64{Int64: d.RetryOf, Valid: d.RetryOf > 0}, "deploy": d.Deploy.ID(), "name": d.Deploy.Name, "commit": d.Commit,
		"from": d.FromSeq, "to": d.ToSeq, "why": d.Why, "wave": int64(d.Wave), "by": d.RequestedBy, "ns": ns, "kind": kind, "workload": workload,
		"status": int64(d.Status), "stage": int64(d.Stage), "reason": d.Reason, "self": d.Self, "finished": !d.Status.Open(), "alone": d.Deploy.Alone,
		"noPlans": []int64{}, "noWaves": []int64{}}
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		// Its creation time is the clock's as the statement runs, not the transaction's start, so
		// a plan's deployments keep the order they were made in.
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO deployment (deployment_id, repository_id, plan_id, retry_of, deploy, name, commit_id, from_seq, to_seq,
				why, wave, gate_plans, gate_waves, requested_by, namespace, kind, workload, status, stage, reason, self, finished_at, alone, created_at)
			VALUES (@id, @repo, @plan, @retry, @deploy, @name, @commit, @from, @to, @why, @wave, @noPlans, @noWaves, @by, @ns, @kind, @workload,
				@status, @stage, @reason, @self, IF(@finished, CURRENT_TIMESTAMP(), NULL), @alone, CURRENT_TIMESTAMP())`, p); err != nil {
			return err
		}
		for _, im := range d.Deploy.Images {
			if _, err := spannerdata.Update(ctx, tx, `INSERT INTO deployment_image (deployment_id, container, target, run_id, reference)
				VALUES (@id, @container, @target, 0, '')`, map[string]any{"id": id, "container": im.Container, "target": im.Target}); err != nil {
				return err
			}
		}
		for i, m := range d.Deploy.Migrate {
			if _, err := spannerdata.Update(ctx, tx, `INSERT INTO deployment_migration (deployment_id, position, target, status, output, refused, allowed)
				VALUES (@id, @position, @target, @status, '', @none, @none)`,
				map[string]any{"id": id, "position": int64(i), "target": m.Target, "status": int64(store.MigrationPending), "none": []string{}}); err != nil {
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

func (x queries) SetGates(ctx context.Context, id int64, gates []store.Gate) error {
	plans, waves := make([]int64, len(gates)), make([]int64, len(gates))
	for i, g := range gates {
		plans[i], waves[i] = g.Plan, int64(g.Wave)
	}
	_, err := x.Exec(ctx, `UPDATE deployment SET gate_plans = @plans, gate_waves = @waves WHERE deployment_id = @id`,
		map[string]any{"id": id, "plans": plans, "waves": waves})
	return err
}

func gatesOf(plans, waves []int64) []store.Gate {
	out := make([]store.Gate, 0, len(plans))
	for i := range plans {
		out = append(out, store.Gate{Plan: plans[i], Wave: int(waves[i])})
	}
	return out
}

func (x queries) CancelWaitingOf(ctx context.Context, repo int64, deploy, reason string) ([]store.Replaced, error) {
	var out []store.Replaced
	err := x.Returning(ctx, `UPDATE deployment SET status = @canceled, stage = @done, reason = @reason, finished_at = CURRENT_TIMESTAMP()
		WHERE repository_id = @repo AND deploy = @deploy AND status = @waiting
		THEN RETURN IFNULL(plan_id, 0), wave, gate_plans, gate_waves`,
		map[string]any{"repo": repo, "deploy": deploy, "reason": reason, "canceled": int64(store.Canceled), "done": int64(store.Done),
			"waiting": int64(store.Waiting)},
		func() { out = nil },
		func(row *spanner.Row) error {
			var c store.Replaced
			var wave int64
			var gp, gw []int64
			if err := row.Columns(&c.Plan, &wave, &gp, &gw); err != nil {
				return err
			}
			c.Wave, c.Gates = int(wave), gatesOf(gp, gw)
			out = append(out, c)
			return nil
		})
	return out, err
}

func (x queries) CancelWaitingIn(ctx context.Context, plan, except int64, reason string) ([]int64, error) {
	var ids []int64
	err := x.Returning(ctx, `UPDATE deployment SET status = @canceled, stage = @done, reason = @reason, finished_at = CURRENT_TIMESTAMP()
		WHERE (plan_id = @plan OR @plan IN UNNEST(gate_plans)) AND deployment_id != @except AND status = @waiting THEN RETURN deployment_id`,
		map[string]any{"plan": plan, "except": except, "reason": reason, "canceled": int64(store.Canceled), "done": int64(store.Done),
			"waiting": int64(store.Waiting)},
		func() { ids = nil },
		func(row *spanner.Row) error {
			var id int64
			if err := row.Columns(&id); err != nil {
				return err
			}
			ids = append(ids, id)
			return nil
		})
	slices.Sort(ids)
	return ids, err
}

func (x queries) HaltRunningIn(ctx context.Context, plan, except int64, wave int, reason string) error {
	_, err := x.Exec(ctx, `UPDATE deployment SET halt_reason = @reason
		WHERE deployment_id != @except AND status = @running AND halt_by IS NULL AND halt_reason IS NULL
		AND (plan_id = @plan OR EXISTS (SELECT 1 FROM UNNEST(gate_plans) AS p WITH OFFSET o WHERE p = @plan AND gate_waves[OFFSET(o)] >= @wave))`,
		map[string]any{"plan": plan, "except": except, "reason": reason, "running": int64(store.Running), "wave": int64(wave)})
	return err
}

var deploymentColumns = `deployment_id, repository_id, IFNULL(plan_id, 0), IFNULL(retry_of, 0), deploy, name, commit_id, from_seq, to_seq,
	why, wave, gate_plans, gate_waves, requested_by, namespace, kind, workload, status, stage, reason, ` + jsonColumn("roll") + `, self,
	IFNULL(driver, ''), IFNULL(halt_by, ''), created_at, started_at, stage_at, finished_at, alone, IFNULL(halt_reason, '')`

func scanDeployment(row *spanner.Row) (*store.Deployment, error) {
	var d store.Deployment
	var wave, status, stage int64
	var gp, gw []int64
	var ns, kind, workload, roll spanner.NullString
	var started, stageAt, finished spanner.NullTime
	if err := row.Columns(&d.ID, &d.Repository, &d.PlanID, &d.RetryOf, &d.Deploy, &d.Name, &d.Commit, &d.FromSeq, &d.ToSeq,
		&d.Why, &wave, &gp, &gw, &d.RequestedBy, &ns, &kind, &workload, &status, &stage, &d.Reason, &roll, &d.Self,
		&d.Driver, &d.HaltBy, &d.CreatedAt, &started, &stageAt, &finished, &d.Alone, &d.HaltReason); err != nil {
		return nil, err
	}
	d.Wave, d.Status, d.Stage, d.Roll, d.Gates = int(wave), store.Status(status), store.Stage(stage), bytesOf(roll), gatesOf(gp, gw)
	if ns.Valid {
		d.Workload = &workflow.Workload{Namespace: ns.StringVal, Kind: workflow.WorkloadKind(kind.StringVal), Name: workload.StringVal}
	}
	d.StartedAt, d.StageAt, d.FinishedAt = started.Time, stageAt.Time, finished.Time
	return &d, nil
}

// load reads the deployments a query selects, with their images and migrations, in its order, at
// one moment.
func (x queries) load(ctx context.Context, where string, params map[string]any) ([]*store.Deployment, error) {
	var out []*store.Deployment
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		byID := map[int64]*store.Deployment{}
		if err := spannerdata.Each(ctx, r, `SELECT `+deploymentColumns+` FROM deployment `+where, params, func(row *spanner.Row) error {
			d, err := scanDeployment(row)
			if err != nil {
				return err
			}
			out = append(out, d)
			byID[d.ID] = d
			return nil
		}); err != nil || len(out) == 0 {
			return err
		}
		ids := make([]int64, len(out))
		for i, d := range out {
			ids[i] = d.ID
		}
		if err := spannerdata.Each(ctx, r, `SELECT deployment_id, container, target, run_id, reference FROM deployment_image
			WHERE deployment_id IN UNNEST(@ids) ORDER BY deployment_id, container`, map[string]any{"ids": ids}, func(row *spanner.Row) error {
			var id int64
			var im store.Image
			if err := row.Columns(&id, &im.Container, &im.Target, &im.RunID, &im.Reference); err != nil {
				return err
			}
			byID[id].Images = append(byID[id].Images, im)
			return nil
		}); err != nil {
			return err
		}
		return spannerdata.Each(ctx, r, `SELECT deployment_id, position, target, status, output, `+jsonColumn("report")+`, refused, allowed, IFNULL(allowed_by, '')
			FROM deployment_migration WHERE deployment_id IN UNNEST(@ids) ORDER BY deployment_id, position`, map[string]any{"ids": ids},
			func(row *spanner.Row) error {
				var id, position, status int64
				var report spanner.NullString
				var m store.Migration
				if err := row.Columns(&id, &position, &m.Target, &status, &m.Output, &report, &m.Refused, &m.Allowed, &m.AllowedBy); err != nil {
					return err
				}
				m.Position, m.Status, m.Report = int(position), store.MigrationStatus(status), bytesOf(report)
				byID[id].Migrations = append(byID[id].Migrations, m)
				return nil
			})
	})
	return out, err
}

func (x queries) Deployments(ctx context.Context, ids []int64) ([]*store.Deployment, error) {
	return x.load(ctx, `WHERE deployment_id IN UNNEST(@ids) ORDER BY created_at, deployment_id`, map[string]any{"ids": ids})
}

func (x queries) LockDeployment(ctx context.Context, id int64) (*store.Deployment, error) {
	ds, err := x.load(ctx, `WHERE deployment_id = @id FOR UPDATE`, map[string]any{"id": id})
	if err != nil || len(ds) == 0 {
		return nil, err
	}
	return ds[0], nil
}

func (x queries) OpenDeployments(ctx context.Context) ([]*store.Deployment, error) {
	return x.load(ctx, `WHERE status IN (@waiting, @running) ORDER BY created_at, deployment_id`,
		map[string]any{"waiting": int64(store.Waiting), "running": int64(store.Running)})
}

func (x queries) RetiringDeployments(ctx context.Context) ([]*store.Deployment, error) {
	return x.load(ctx, `WHERE self AND status = @succeeded AND JSON_VALUE(roll, '$.retire') = 'true' ORDER BY created_at, deployment_id`,
		map[string]any{"succeeded": int64(store.Succeeded)})
}

func (x queries) CoveringDeployments(ctx context.Context, repo, seq int64) ([]*store.Deployment, error) {
	return x.load(ctx, `WHERE repository_id = @repo AND from_seq < @seq AND to_seq >= @seq ORDER BY created_at DESC, deployment_id DESC`,
		map[string]any{"repo": repo, "seq": seq})
}

func (x queries) LatestDeployments(ctx context.Context, repo int64, limit int) ([]*store.Deployment, error) {
	return x.load(ctx, `WHERE repository_id = @repo ORDER BY created_at DESC, deployment_id DESC LIMIT @limit`,
		map[string]any{"repo": repo, "limit": int64(limit)})
}

func (x queries) DeployDeployments(ctx context.Context, repo int64, deploy string, limit int) ([]*store.Deployment, error) {
	return x.load(ctx, `WHERE repository_id = @repo AND deploy = @deploy ORDER BY created_at DESC, deployment_id DESC LIMIT @limit`,
		map[string]any{"repo": repo, "deploy": deploy, "limit": int64(limit)})
}

func (x queries) NewerOf(ctx context.Context, repo int64, deploy string, id int64) (int64, bool, error) {
	var newer int64
	var ok bool
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT d.deployment_id FROM deployment d, (SELECT created_at FROM deployment WHERE deployment_id = @id) ref
			WHERE d.repository_id = @repo AND d.deploy = @deploy AND (d.created_at > ref.created_at OR (d.created_at = ref.created_at AND d.deployment_id > @id))
			ORDER BY d.created_at DESC, d.deployment_id DESC LIMIT 1`, map[string]any{"repo": repo, "deploy": deploy, "id": id}, &newer)
		return err
	})
	return newer, ok, err
}

func (x queries) PlanStatuses(ctx context.Context, plan int64) (map[int][]store.Status, error) {
	var out map[int][]store.Status
	err := x.Read(func(r spannerdata.Reader) error {
		out = map[int][]store.Status{}
		return spannerdata.Each(ctx, r, `SELECT wave, status FROM deployment WHERE plan_id = @plan`, map[string]any{"plan": plan}, func(row *spanner.Row) error {
			var wave, status int64
			if err := row.Columns(&wave, &status); err != nil {
				return err
			}
			out[int(wave)] = append(out[int(wave)], store.Status(status))
			return nil
		})
	})
	return out, err
}

func (x queries) AppendEvent(ctx context.Context, id int64, message string) error {
	_, err := x.Exec(ctx, `INSERT INTO deployment_event (deployment_id, id, event_at, message) VALUES (@deployment, @id, CURRENT_TIMESTAMP(), @message)`,
		map[string]any{"deployment": id, "id": data.NewID(), "message": message})
	return err
}

func (x queries) Events(ctx context.Context, id int64) ([]store.Event, error) {
	var out []store.Event
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT event_at, message FROM deployment_event WHERE deployment_id = @id ORDER BY event_at, id`,
			map[string]any{"id": id}, func(row *spanner.Row) error {
				var e store.Event
				if err := row.Columns(&e.At, &e.Message); err != nil {
					return err
				}
				out = append(out, e)
				return nil
			})
	})
	return out, err
}

func (x queries) LockStarts(ctx context.Context) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerlock.InTx(ctx, tx, "deploy", "starts") })
}

func (x queries) StartDeployment(ctx context.Context, id int64, driver string) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE deployment SET status = @running, stage = @push, started_at = CURRENT_TIMESTAMP(), stage_at = CURRENT_TIMESTAMP(),
		driver = @driver WHERE deployment_id = @id AND status = @waiting`,
		map[string]any{"id": id, "driver": nullString(driver), "running": int64(store.Running), "push": int64(store.Push), "waiting": int64(store.Waiting)})
	return n > 0, err
}

func (x queries) SetStage(ctx context.Context, id int64, stage store.Stage) error {
	_, err := x.Exec(ctx, `UPDATE deployment SET stage = @stage, stage_at = CURRENT_TIMESTAMP() WHERE deployment_id = @id AND status = @running`,
		map[string]any{"id": id, "stage": int64(stage), "running": int64(store.Running)})
	return err
}

func (x queries) SetRoll(ctx context.Context, id int64, roll []byte) error {
	_, err := x.Exec(ctx, `UPDATE deployment SET roll = PARSE_JSON(@roll) WHERE deployment_id = @id`, map[string]any{"id": id, "roll": jsonParam(roll)})
	return err
}

func (x queries) SetPushRun(ctx context.Context, id int64, container string, run int64) error {
	_, err := x.Exec(ctx, `UPDATE deployment_image SET run_id = @run WHERE deployment_id = @id AND container = @container`,
		map[string]any{"id": id, "container": container, "run": run})
	return err
}

func (x queries) SetPushed(ctx context.Context, id int64, container, reference string) error {
	_, err := x.Exec(ctx, `UPDATE deployment_image SET reference = @reference WHERE deployment_id = @id AND container = @container`,
		map[string]any{"id": id, "container": container, "reference": reference})
	return err
}

func (x queries) SetMigration(ctx context.Context, id int64, position int, status store.MigrationStatus, output string, report []byte, refused []string) error {
	_, err := x.Exec(ctx, `UPDATE deployment_migration SET status = @status, output = @output, report = PARSE_JSON(@report), refused = @refused
		WHERE deployment_id = @id AND position = @position`,
		map[string]any{"id": id, "position": int64(position), "status": int64(status), "output": output, "report": jsonParam(report),
			"refused": nonNil(refused)})
	return err
}

func (x queries) FinishDeployment(ctx context.Context, id int64, status store.Status, reason string) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE deployment SET status = @status, stage = @done, reason = @reason, finished_at = CURRENT_TIMESTAMP()
		WHERE deployment_id = @id AND status IN (@waiting, @running)`,
		map[string]any{"id": id, "status": int64(status), "done": int64(store.Done), "reason": reason, "waiting": int64(store.Waiting),
			"running": int64(store.Running)})
	return n > 0, err
}

func (x queries) SetHaltBy(ctx context.Context, id int64, by string) error {
	_, err := x.Exec(ctx, `UPDATE deployment SET halt_by = @by WHERE deployment_id = @id AND halt_by IS NULL AND halt_reason IS NULL`,
		map[string]any{"id": id, "by": by})
	return err
}

func (x queries) WaitingMigration(ctx context.Context, id int64) (int, []string, bool, error) {
	var position int64
	var refused []string
	var ok bool
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT position, refused FROM deployment_migration WHERE deployment_id = @id AND status = @waiting`,
			map[string]any{"id": id, "waiting": int64(store.MigrationWaiting)}, &position, &refused)
		return err
	})
	return int(position), refused, ok, err
}

func (x queries) AllowMigration(ctx context.Context, id int64, position int, allowed []string, by string) error {
	_, err := x.Exec(ctx, `UPDATE deployment_migration SET allowed = @allowed, allowed_by = @by, status = @pending
		WHERE deployment_id = @id AND position = @position`,
		map[string]any{"id": id, "position": int64(position), "allowed": nonNil(allowed), "by": by, "pending": int64(store.MigrationPending)})
	return err
}

// ─── Services ───

var serviceColumns = fmt.Sprintf(`s.service_id, s.repository_id, r.name, s.deploy, s.name, s.namespace, s.kind, s.workload, s.declared, s.declared_seq, s.group_name,
	IFNULL((SELECT d.deployment_id FROM deployment d WHERE d.repository_id = s.repository_id AND d.deploy = s.deploy
		ORDER BY d.created_at DESC, d.deployment_id DESC LIMIT 1), 0),
	IFNULL((SELECT d.deployment_id FROM deployment d WHERE d.repository_id = s.repository_id AND d.deploy = s.deploy AND d.status = %d
		ORDER BY d.created_at DESC, d.deployment_id DESC LIMIT 1), 0)`,
	store.Succeeded)

func (x queries) services(ctx context.Context, where string, params map[string]any) ([]store.ServiceRow, error) {
	var out []store.ServiceRow
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT `+serviceColumns+` FROM service s JOIN repository r ON r.repository_id = s.repository_id `+where, params,
			func(row *spanner.Row) error {
				var sv store.ServiceRow
				var kind string
				if err := row.Columns(&sv.ID, &sv.Repository, &sv.RepoName, &sv.Deploy, &sv.Name, &sv.Workload.Namespace, &kind, &sv.Workload.Name,
					&sv.Declared, &sv.DeclaredSeq, &sv.Group, &sv.LatestID, &sv.DeployedID); err != nil {
					return err
				}
				sv.Workload.Kind = workflow.WorkloadKind(kind)
				out = append(out, sv)
				return nil
			})
	})
	return out, err
}

func (x queries) Services(ctx context.Context, repo int64) ([]store.ServiceRow, error) {
	return x.services(ctx, `WHERE (@repo = 0 AND NOT EXISTS (SELECT 1 FROM move_gate g WHERE g.repository_id = s.repository_id AND g.move_state != '')
		OR s.repository_id = @repo) ORDER BY r.name, s.name, s.service_id`, map[string]any{"repo": repo})
}

func (x queries) Service(ctx context.Context, id int64) (store.ServiceRow, bool, error) {
	out, err := x.services(ctx, `WHERE s.service_id = @id`, map[string]any{"id": id})
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
	p := map[string]any{"service": service, "error": o.Error, "found": o.Found, "live": o.Live, "replicas": int64(o.Replicas),
		"ready": int64(o.Ready), "images": string(images), "pods": string(podsJSON)}
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var one int64
		ok, err := spannerdata.First(ctx, tx, `SELECT 1 FROM service_observation WHERE service_id = @service`, p, &one)
		if err != nil {
			return err
		}
		sql := `INSERT INTO service_observation (service_id, error, found, live, replicas, ready, images, pods)
			VALUES (@service, @error, @found, @live, @replicas, @ready, PARSE_JSON(@images), PARSE_JSON(@pods))`
		if ok {
			sql = `UPDATE service_observation SET error = @error, found = @found, live = @live, replicas = @replicas, ready = @ready,
				images = PARSE_JSON(@images), pods = PARSE_JSON(@pods) WHERE service_id = @service`
		}
		_, err = spannerdata.Update(ctx, tx, sql, p)
		return err
	})
}

func (x queries) Observations(ctx context.Context, ids []int64) (map[int64]*store.Observation, error) {
	var out map[int64]*store.Observation
	err := x.Read(func(r spannerdata.Reader) error {
		out = map[int64]*store.Observation{}
		return spannerdata.Each(ctx, r, `SELECT service_id, error, found, live, replicas, ready, TO_JSON_STRING(images), TO_JSON_STRING(pods)
			FROM service_observation WHERE service_id IN UNNEST(@ids)`, map[string]any{"ids": ids}, func(row *spanner.Row) error {
			var id, replicas, ready int64
			var o store.Observation
			var images, pods string
			if err := row.Columns(&id, &o.Error, &o.Found, &o.Live, &replicas, &ready, &images, &pods); err != nil {
				return err
			}
			o.Replicas, o.Ready = int32(replicas), int32(ready)
			if err := json.Unmarshal([]byte(images), &o.Images); err != nil {
				return err
			}
			if err := json.Unmarshal([]byte(pods), &o.Pods); err != nil {
				return err
			}
			out[id] = &o
			return nil
		})
	})
	return out, err
}

// ─── Keyed work ───

func (x queries) MarkWork(ctx context.Context, kind string, key int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.MarkWork(ctx, tx, kind, key) })
}

func (x queries) MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.MarkWorkAt(ctx, tx, kind, key, at) })
}

func (x queries) MarkWorkIdle(ctx context.Context, kind string, key int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.MarkWorkIdle(ctx, tx, kind, key) })
}

func (x queries) TakeWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	var ok bool
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		ok, err = spannerdata.TakeWork(ctx, tx, kind, key, holder, ttl)
		return err
	})
	return ok, err
}

func (x queries) RenewWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	var ok bool
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		ok, err = spannerdata.RenewWork(ctx, tx, kind, key, holder, ttl)
		return err
	})
	return ok, err
}

func (x queries) ReleaseWork(ctx context.Context, kind string, key int64, holder string) (bool, error) {
	var again bool
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		again, err = spannerdata.ReleaseWork(ctx, tx, kind, key, holder)
		return err
	})
	return again, err
}

func (x queries) DueWork(ctx context.Context, kind string, limit int) ([]int64, error) {
	var keys []int64
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		keys, err = spannerdata.DueWork(ctx, r, kind, limit)
		return err
	})
	return keys, err
}
