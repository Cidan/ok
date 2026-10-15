// Package store is the deployer's storage, in its schema
// (docs/deploy.md): each repository's main as the SCM's change feed told it, the plans
// made at its heads, and every deployment — what it deploys, its stage, what it pushed and
// migrated, its roll's resumable state, and its log.
package store

import (
	"context"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/workflow"
)

// Status is where a deployment is.
type Status int16

const (
	Waiting   Status = 1 // for its turn: its wave, or a running deployment of its deploy
	Running   Status = 2
	Succeeded Status = 3
	Failed    Status = 4
	Halted    Status = 5 // stopped and rolled back, at a person's request or because its plan failed
	Canceled  Status = 6 // never ran: superseded, or its plan failed first
)

// Open reports whether a deployment in this status is not over.
func (s Status) Open() bool { return s == Waiting || s == Running }

func (s Status) String() string {
	switch s {
	case Waiting:
		return "waiting"
	case Running:
		return "running"
	case Succeeded:
		return "succeeded"
	case Failed:
		return "failed"
	case Halted:
		return "halted"
	case Canceled:
		return "canceled"
	}
	return fmt.Sprintf("status %d", s)
}

// Stage is how far a deployment has come.
type Stage int16

const (
	Queued  Stage = 1
	Push    Stage = 2
	Migrate Stage = 3
	Hazard  Stage = 4 // a migration waits for a person to allow its hazards
	Roll    Stage = 5
	Done    Stage = 6
)

// MigrationStatus is where one migration is.
type MigrationStatus int16

const (
	MigrationPending MigrationStatus = 1
	MigrationApplied MigrationStatus = 2
	MigrationWaiting MigrationStatus = 3 // for an allowance of its refused hazards
	MigrationFailed  MigrationStatus = 4
)

// Deployment is one deploy at one commit of main.
type Deployment struct {
	ID         int64
	Repository int64
	PlanID     int64 // 0 for a retry
	RetryOf    int64
	Deploy     string // the deploy's identity
	Name       string
	Commit     []byte
	FromSeq    int64 // it covers main's commits after FromSeq, up to ToSeq
	ToSeq      int64
	Why        string
	Wave       int
	// Gates are the places in earlier plans it took (docs/deploy.md, "When a deploy
	// runs"): it starts only once each plan's earlier waves succeeded.
	Gates       []Gate
	RequestedBy string
	Workload    *workflow.Workload // nil for a deploy that only migrates
	Status      Status
	Stage       Stage
	Reason      string
	Roll        []byte // the roll's resumable state, JSON
	Self        bool   // it rolls the deployer itself
	Alone       bool   // it runs while no other deployment does (workflow.Deploy.Alone)
	Driver      string // a self deployment's driving copy
	HaltBy      string
	CreatedAt   time.Time
	StartedAt   time.Time
	StageAt     time.Time
	FinishedAt  time.Time
	Images      []Image
	Migrations  []Migration
}

// Gate is a place in a plan: a wave, which starts once the plan's earlier waves succeeded.
type Gate struct {
	Plan int64
	Wave int
}

// Image is one container a deployment rolls.
type Image struct {
	Container string
	Target    string
	RunID     int64
	Reference string // registry/repository@sha256:… once pushed
}

// Migration is one migration a deployment applies.
type Migration struct {
	Position  int
	Target    string
	Status    MigrationStatus
	Output    string
	Report    []byte
	Refused   []string
	Allowed   []string
	AllowedBy string
}

// Event is one line of a deployment's log.
type Event struct {
	At      time.Time
	Message string
}

// Repository is a repository as the deployer follows it.
type Repository struct {
	ID          int64
	Name        string
	Head        []byte
	HeadSeq     int64
	PlannedHead []byte
	PlannedSeq  int64
}

// Errors, which the service maps onto gRPC codes.
var ErrNotFound = errors.New("no such deployment")

// ConflictError is a request the deployment's state does not allow.
type ConflictError struct{ Message string }

func (e *ConflictError) Error() string { return e.Message }

func conflict(format string, args ...any) error {
	return &ConflictError{Message: fmt.Sprintf(format, args...)}
}

// Store is the deployer's storage.
type Store struct {
	pool  *pgxpool.Pool
	locks *lock.Lock
}

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are the deployer's schema.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Open brings the database behind pool to the deployer's schema and builds a store over it.
func Open(ctx context.Context, pool *pgxpool.Pool) (*Store, error) {
	if _, err := migrate.Apply(ctx, pool, "deploy", Migrations); err != nil {
		return nil, err
	}
	return &Store{pool: pool, locks: lock.New(pool)}, nil
}

// Pool is the store's pool.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// ─── Locks ───

// The deployer's locks (internal/lock): the leader's and the feed's, which one replica
// holds at a time, and each deployment's, which whoever steps it holds.

// Lead runs fn holding the leader's lock, with a context that ends if the lock is lost, and
// gives the lock up when fn returns. It returns lock.ErrNotAcquired when another replica holds
// the lock, and otherwise fn's error.
func (s *Store) Lead(ctx context.Context, fn lock.LockFn) error {
	return s.locks.LockFn(ctx, "leaders", "leader", 0, fn)
}

// LeadFeed runs fn holding the change feed's lock, as Lead does the leader's.
func (s *Store) LeadFeed(ctx context.Context, fn lock.LockFn) error {
	return s.locks.LockFn(ctx, "leaders", "feed", 0, fn)
}

// LeadObserver runs fn holding the observer's lock, as Lead does the leader's.
func (s *Store) LeadObserver(ctx context.Context, fn lock.LockFn) error {
	return s.locks.LockFn(ctx, "leaders", "observer", 0, fn)
}

// LockDeployment takes a deployment's lock, if no one holds it, until release: whoever steps a
// deployment holds it, and hands it on to a migration that outlives the pass that started it,
// so it is held past ctx — refreshed until released, or until this process dies.
func (s *Store) LockDeployment(ctx context.Context, id int64) (release func(), ok bool, err error) {
	key := strconv.FormatInt(id, 10)
	held, cancel := context.WithCancel(context.WithoutCancel(ctx))
	ok, err = s.locks.TryLock(held, "deployments", key)
	if err != nil || !ok {
		cancel()
		return nil, false, err
	}
	return func() {
		unlockCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.locks.Unlock(unlockCtx, "deployments", key)
		done()
		cancel()
	}, true, nil
}

// ─── The feed ───

// Cursor is the sequence of the last event of the SCM's change feed the store applied.
func (s *Store) Cursor(ctx context.Context) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx, `SELECT seq FROM feed_cursor`).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

// FeedEvent is what the store keeps of one event of the change feed: a repository's creation
// or renaming, or main's moving.
type FeedEvent struct {
	Seq        int64
	Repository int64
	Name       string // a creation's or renaming's
	Created    bool
	Head       []byte // main's move: its new head, and its place in main's history
	HeadSeq    int64
}

// Apply applies one event of the change feed, once: an event at or before the cursor is
// skipped.
func (s *Store) Apply(ctx context.Context, ev FeedEvent) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO feed_cursor (seq) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM feed_cursor)`); err != nil {
			return err
		}
		var cursor int64
		if err := tx.QueryRow(ctx, `SELECT seq FROM feed_cursor FOR UPDATE`).Scan(&cursor); err != nil {
			return err
		}
		if ev.Seq <= cursor {
			return nil
		}
		switch {
		case ev.Created:
			if _, err := tx.Exec(ctx, `INSERT INTO repository (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = $2`,
				ev.Repository, ev.Name); err != nil {
				return err
			}
		case ev.Name != "":
			if _, err := tx.Exec(ctx, `UPDATE repository SET name = $2 WHERE id = $1`, ev.Repository, ev.Name); err != nil {
				return err
			}
		case ev.HeadSeq > 0:
			if _, err := tx.Exec(ctx, `INSERT INTO repository (id, name) VALUES ($1, '') ON CONFLICT DO NOTHING`, ev.Repository); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE repository SET head = $2, head_seq = $3 WHERE id = $1 AND head_seq < $3`,
				ev.Repository, ev.Head, ev.HeadSeq); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO main_commit (repository_id, seq, commit_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
				ev.Repository, ev.HeadSeq, ev.Head); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `UPDATE feed_cursor SET seq = $1`, ev.Seq)
		return err
	})
}

// Unplanned are the repositories whose main moved past their latest plan.
func (s *Store) Unplanned(ctx context.Context) ([]Repository, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name, head, head_seq, planned_head, planned_seq FROM repository
		WHERE head_seq > planned_seq ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Repository, error) {
		var x Repository
		err := r.Scan(&x.ID, &x.Name, &x.Head, &x.HeadSeq, &x.PlannedHead, &x.PlannedSeq)
		return x, err
	})
}

// ─── Plans ───

// NewDeployment is a deployment a plan makes.
type NewDeployment struct {
	Deploy     workflow.Deploy
	Why        string
	Wave       int
	Self       bool
	FailedWhy  string // when set, it is recorded failed and never runs
	References map[string]string
}

// PlanResult is a plan the deployer made at a repository's head.
type PlanResult struct {
	Repository  int64
	Head        []byte
	HeadSeq     int64
	BaseSeq     int64
	Notes       []string
	Deployments []NewDeployment
	// Services are every deploy at the head that rolls a workload: the repository's services
	// from now on. Its others are no longer declared.
	Services []workflow.Deploy
	By       string // the deployer's principal
}

// RecordPlan records a plan and its deployments, and moves the repository's planned head to
// it. A deployment of a deploy that another deployment still waits for replaces it — the newest
// commit wins — and takes its place: its gates, and its wave in its plan.
func (s *Store) RecordPlan(ctx context.Context, p PlanResult) (int64, error) {
	var id int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var planned int64
		if err := tx.QueryRow(ctx, `SELECT planned_seq FROM repository WHERE id = $1 FOR UPDATE`, p.Repository).Scan(&planned); err != nil {
			return err
		}
		if planned >= p.HeadSeq {
			return conflict("the repository is planned at %d already", planned)
		}
		notes := p.Notes
		if notes == nil {
			notes = []string{}
		}
		if err := tx.QueryRow(ctx, `INSERT INTO plan (repository_id, head, head_seq, base_seq, notes) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			p.Repository, p.Head, p.HeadSeq, p.BaseSeq, notes).Scan(&id); err != nil {
			return err
		}
		for _, d := range p.Deployments {
			var gates []Gate
			if d.FailedWhy == "" {
				rows, err := tx.Query(ctx, `UPDATE deployment SET status = $3, stage = $4, reason = $5, finished_at = now()
					WHERE repository_id = $1 AND deploy = $2 AND status = $6
					RETURNING coalesce(plan_id, 0), wave, gate_plans, gate_waves`,
					p.Repository, d.Deploy.ID(), Canceled, Done, fmt.Sprintf("replaced by the deployment at %s", short(p.Head)), Waiting)
				if err != nil {
					return err
				}
				for rows.Next() {
					var plan int64
					var wave int
					var gp []int64
					var gw []int32
					if err := rows.Scan(&plan, &wave, &gp, &gw); err != nil {
						rows.Close()
						return err
					}
					gates = append(gates, gatesOf(gp, gw)...)
					if plan != 0 {
						gates = append(gates, Gate{Plan: plan, Wave: wave})
					}
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					return err
				}
			}
			status, stage, reason := Waiting, Queued, ""
			if d.FailedWhy != "" {
				status, stage, reason = Failed, Done, d.FailedWhy
			}
			nid, err := insertDeployment(ctx, tx, p.Repository, &id, 0, d.Deploy, p.Head, p.BaseSeq, p.HeadSeq, d.Why, d.Wave, p.By, d.Self, status, stage, reason)
			if err != nil {
				return err
			}
			if len(gates) > 0 {
				gp, gw := gateColumns(gates)
				if _, err := tx.Exec(ctx, `UPDATE deployment SET gate_plans = $2, gate_waves = $3 WHERE id = $1`, nid, gp, gw); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `INSERT INTO deployment_event (deployment_id, message) VALUES ($1, $2)`, nid,
					fmt.Sprintf("it takes the place of the deployment it replaced: %s", describeGates(gates))); err != nil {
					return err
				}
			}
		}
		if err := recordServices(ctx, tx, p.Repository, p.HeadSeq, p.Services); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE repository SET planned_head = $2, planned_seq = $3 WHERE id = $1`, p.Repository, p.Head, p.HeadSeq)
		return err
	})
	return id, err
}

// recordServices records the services a repository's main declares at a head; its services
// not among them are no longer declared.
func recordServices(ctx context.Context, tx pgx.Tx, repo, seq int64, deploys []workflow.Deploy) error {
	var ids []string
	for _, d := range deploys {
		w := d.Workload
		if w == nil {
			continue
		}
		ids = append(ids, d.ID())
		if _, err := tx.Exec(ctx, `INSERT INTO service (repository_id, deploy, name, namespace, kind, workload, declared, declared_seq, group_name)
			VALUES ($1, $2, $3, $4, $5, $6, true, $7, $8)
			ON CONFLICT (repository_id, deploy) DO UPDATE SET name = $3, namespace = $4, kind = $5, workload = $6, declared = true, declared_seq = $7, group_name = $8`,
			repo, d.ID(), d.Name, w.Namespace, string(w.Kind), w.Name, seq, d.Group); err != nil {
			return err
		}
	}
	if ids == nil {
		ids = []string{}
	}
	_, err := tx.Exec(ctx, `UPDATE service SET declared = false WHERE repository_id = $1 AND declared AND NOT deploy = ANY($2)`, repo, ids)
	return err
}

// Service is a deploy that rolls a workload, with its latest deployment and the latest that
// succeeded — what runs, as ok deployed it.
type Service struct {
	ID          int64
	Repository  int64
	RepoName    string
	Deploy      string
	Name        string
	Workload    workflow.Workload
	Declared    bool
	DeclaredSeq int64
	// Group is its deploy's group (workflow.Deploy.Group), as main last declared it; empty: a
	// group of its own.
	Group    string
	Latest   *Deployment // nil: never deployed
	Deployed *Deployment // nil: never deployed successfully
	// Observation is what the cluster runs of its workload, as the observer last read it; nil:
	// not read yet.
	Observation *Observation
}

var serviceColumns = fmt.Sprintf(`s.id, s.repository_id, r.name, s.deploy, s.name, s.namespace, s.kind, s.workload, s.declared, s.declared_seq, s.group_name,
	(SELECT d.id FROM deployment d WHERE d.repository_id = s.repository_id AND d.deploy = s.deploy ORDER BY d.id DESC LIMIT 1),
	(SELECT d.id FROM deployment d WHERE d.repository_id = s.repository_id AND d.deploy = s.deploy AND d.status = %d ORDER BY d.id DESC LIMIT 1)`,
	Succeeded)

// Services are the services, by repository and name: a repository's (0: every repository).
func (s *Store) Services(ctx context.Context, repo int64) ([]*Service, error) {
	return s.services(ctx, `WHERE ($1 = 0 OR s.repository_id = $1) ORDER BY r.name, s.name, s.id`, repo)
}

// GetService reads a service.
func (s *Store) GetService(ctx context.Context, id int64) (*Service, error) {
	out, err := s.services(ctx, `WHERE s.id = $1`, id)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return out[0], nil
}

func (s *Store) services(ctx context.Context, where string, arg int64) ([]*Service, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+serviceColumns+` FROM service s JOIN repository r ON r.id = s.repository_id `+where, arg)
	if err != nil {
		return nil, err
	}
	type ref struct{ latest, deployed *int64 }
	var out []*Service
	var refs []ref
	for rows.Next() {
		var sv Service
		var kind string
		var r ref
		if err := rows.Scan(&sv.ID, &sv.Repository, &sv.RepoName, &sv.Deploy, &sv.Name, &sv.Workload.Namespace, &kind, &sv.Workload.Name,
			&sv.Declared, &sv.DeclaredSeq, &sv.Group, &r.latest, &r.deployed); err != nil {
			rows.Close()
			return nil, err
		}
		sv.Workload.Kind = workflow.WorkloadKind(kind)
		out = append(out, &sv)
		refs = append(refs, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return out, nil
	}
	svIDs := make([]int64, len(out))
	for i, sv := range out {
		svIDs[i] = sv.ID
	}
	obs, err := s.observations(ctx, svIDs)
	if err != nil {
		return nil, err
	}
	for _, sv := range out {
		sv.Observation = obs[sv.ID]
	}
	var ids []int64
	for _, r := range refs {
		for _, id := range []*int64{r.latest, r.deployed} {
			if id != nil {
				ids = append(ids, *id)
			}
		}
	}
	if len(ids) == 0 {
		return out, nil
	}
	ds, err := s.load(ctx, s.pool, `SELECT `+deploymentColumns+` FROM deployment WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	byID := map[int64]*Deployment{}
	for _, d := range ds {
		byID[d.ID] = d
	}
	for i, r := range refs {
		if r.latest != nil {
			out[i].Latest = byID[*r.latest]
		}
		if r.deployed != nil {
			out[i].Deployed = byID[*r.deployed]
		}
	}
	return out, nil
}

// Recent are a service's latest deployments, newest first.
func (s *Store) Recent(ctx context.Context, sv *Service, limit int) ([]*Deployment, error) {
	return s.load(ctx, s.pool, `SELECT `+deploymentColumns+` FROM deployment WHERE repository_id = $1 AND deploy = $2 ORDER BY id DESC LIMIT $3`,
		sv.Repository, sv.Deploy, limit)
}

func insertDeployment(ctx context.Context, tx pgx.Tx, repo int64, plan *int64, retryOf int64, d workflow.Deploy, commit []byte,
	from, to int64, why string, wave int, by string, self bool, status Status, stage Stage, reason string) (int64, error) {
	var ns, kind, workload *string
	if w := d.Workload; w != nil {
		k := string(w.Kind)
		ns, kind, workload = &w.Namespace, &k, &w.Name
	}
	var retry *int64
	if retryOf > 0 {
		retry = &retryOf
	}
	var finished *time.Time
	if !status.Open() {
		now := time.Now()
		finished = &now
	}
	var id int64
	if err := tx.QueryRow(ctx, `INSERT INTO deployment (repository_id, plan_id, retry_of, deploy, name, commit_id, from_seq, to_seq,
			why, wave, requested_by, namespace, kind, workload, status, stage, reason, self, finished_at, alone)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20) RETURNING id`,
		repo, plan, retry, d.ID(), d.Name, commit, from, to, why, wave, by, ns, kind, workload, status, stage, reason, self, finished, d.Alone).Scan(&id); err != nil {
		return 0, err
	}
	for _, im := range d.Images {
		if _, err := tx.Exec(ctx, `INSERT INTO deployment_image (deployment_id, container, target) VALUES ($1, $2, $3)`,
			id, im.Container, im.Target); err != nil {
			return 0, err
		}
	}
	for i, m := range d.Migrate {
		if _, err := tx.Exec(ctx, `INSERT INTO deployment_migration (deployment_id, position, target) VALUES ($1, $2, $3)`,
			id, i, m.Target); err != nil {
			return 0, err
		}
	}
	msg := fmt.Sprintf("planned at %s: %s", short(commit), why)
	if retryOf > 0 {
		msg = fmt.Sprintf("a retry of deployment %d at %s, by %s", retryOf, short(commit), by)
	}
	if reason != "" {
		msg += "; " + reason
	}
	if _, err := tx.Exec(ctx, `INSERT INTO deployment_event (deployment_id, message) VALUES ($1, $2)`, id, msg); err != nil {
		return 0, err
	}
	return id, nil
}

func gatesOf(plans []int64, waves []int32) []Gate {
	out := make([]Gate, 0, len(plans))
	for i := range plans {
		out = append(out, Gate{Plan: plans[i], Wave: int(waves[i])})
	}
	return out
}

func gateColumns(gates []Gate) ([]int64, []int32) {
	plans, waves := make([]int64, len(gates)), make([]int32, len(gates))
	for i, g := range gates {
		plans[i], waves[i] = g.Plan, int32(g.Wave)
	}
	return plans, waves
}

func describeGates(gates []Gate) string {
	var parts []string
	for _, g := range gates {
		parts = append(parts, fmt.Sprintf("wave %d of plan %d", g.Wave, g.Plan))
	}
	return strings.Join(parts, ", ")
}

func short(commit []byte) string {
	h := hex.EncodeToString(commit)
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

// ─── Deployments ───

const deploymentColumns = `id, repository_id, coalesce(plan_id, 0), coalesce(retry_of, 0), deploy, name, commit_id, from_seq, to_seq,
	why, wave, gate_plans, gate_waves, requested_by, namespace, kind, workload, status, stage, reason, roll, self, coalesce(driver, ''), coalesce(halt_by, ''),
	created_at, started_at, stage_at, finished_at, alone`

type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func scanDeployment(row pgx.Row) (*Deployment, error) {
	var d Deployment
	var ns, kind, workload *string
	var started, stageAt, finished *time.Time
	var gp []int64
	var gw []int32
	err := row.Scan(&d.ID, &d.Repository, &d.PlanID, &d.RetryOf, &d.Deploy, &d.Name, &d.Commit, &d.FromSeq, &d.ToSeq,
		&d.Why, &d.Wave, &gp, &gw, &d.RequestedBy, &ns, &kind, &workload, &d.Status, &d.Stage, &d.Reason, &d.Roll, &d.Self, &d.Driver, &d.HaltBy,
		&d.CreatedAt, &started, &stageAt, &finished, &d.Alone)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
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

// load reads deployments a query selects, with their images and migrations.
func (s *Store) load(ctx context.Context, q querier, sql string, args ...any) ([]*Deployment, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	var out []*Deployment
	byID := map[int64]*Deployment{}
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
	irows, err := q.Query(ctx, `SELECT deployment_id, container, target, run_id, reference FROM deployment_image
		WHERE deployment_id = ANY($1) ORDER BY deployment_id, container`, ids)
	if err != nil {
		return nil, err
	}
	for irows.Next() {
		var id int64
		var im Image
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
	mrows, err := q.Query(ctx, `SELECT deployment_id, position, target, status, output, report, refused, allowed, coalesce(allowed_by, '')
		FROM deployment_migration WHERE deployment_id = ANY($1) ORDER BY deployment_id, position`, ids)
	if err != nil {
		return nil, err
	}
	defer mrows.Close()
	for mrows.Next() {
		var id int64
		var m Migration
		if err := mrows.Scan(&id, &m.Position, &m.Target, &m.Status, &m.Output, &m.Report, &m.Refused, &m.Allowed, &m.AllowedBy); err != nil {
			return nil, err
		}
		byID[id].Migrations = append(byID[id].Migrations, m)
	}
	return out, mrows.Err()
}

// Get reads a deployment.
func (s *Store) Get(ctx context.Context, id int64) (*Deployment, error) {
	ds, err := s.load(ctx, s.pool, `SELECT `+deploymentColumns+` FROM deployment WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	if len(ds) == 0 {
		return nil, ErrNotFound
	}
	return ds[0], nil
}

// Unfinished are the deployments not over, oldest first.
func (s *Store) Unfinished(ctx context.Context) ([]*Deployment, error) {
	return s.load(ctx, s.pool, `SELECT `+deploymentColumns+` FROM deployment WHERE status IN ($1, $2) ORDER BY id`, Waiting, Running)
}

// Retiring are the self deployments that succeeded and whose old copy is still to be scaled
// to 0: their roll says so (roll.State.Retire).
func (s *Store) Retiring(ctx context.Context) ([]*Deployment, error) {
	return s.load(ctx, s.pool, `SELECT `+deploymentColumns+` FROM deployment
		WHERE self AND status = $1 AND (roll->>'retire')::boolean ORDER BY id`, Succeeded)
}

// PlanStatuses are the statuses of a plan's deployments, by wave.
func (s *Store) PlanStatuses(ctx context.Context, plan int64) (map[int][]Status, error) {
	rows, err := s.pool.Query(ctx, `SELECT wave, status FROM deployment WHERE plan_id = $1`, plan)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int][]Status{}
	for rows.Next() {
		var w int
		var st Status
		if err := rows.Scan(&w, &st); err != nil {
			return nil, err
		}
		out[w] = append(out[w], st)
	}
	return out, rows.Err()
}

// Covering are the deployments that cover a commit of main — whose range includes it — newest
// first; planned reports whether a plan has reached the commit, after which no deployment of it
// is yet to be made.
func (s *Store) Covering(ctx context.Context, repo int64, commit []byte) (ds []*Deployment, planned bool, err error) {
	var seq int64
	err = s.pool.QueryRow(ctx, `SELECT m.seq, r.planned_seq >= m.seq FROM main_commit m JOIN repository r ON r.id = m.repository_id
		WHERE m.repository_id = $1 AND m.commit_id = $2`, repo, commit).Scan(&seq, &planned)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	ds, err = s.load(ctx, s.pool, `SELECT `+deploymentColumns+` FROM deployment
		WHERE repository_id = $1 AND from_seq < $2 AND to_seq >= $2 ORDER BY id DESC`, repo, seq)
	return ds, planned, err
}

// Latest are a repository's latest deployments, newest first.
func (s *Store) Latest(ctx context.Context, repo int64, limit int) ([]*Deployment, error) {
	return s.load(ctx, s.pool, `SELECT `+deploymentColumns+` FROM deployment WHERE repository_id = $1 ORDER BY id DESC LIMIT $2`, repo, limit)
}

// Events are a deployment's log, oldest first.
func (s *Store) Events(ctx context.Context, id int64) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT at, message FROM deployment_event WHERE deployment_id = $1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Event, error) {
		var e Event
		err := r.Scan(&e.At, &e.Message)
		return e, err
	})
}

// Log adds lines to a deployment's log.
func (s *Store) Log(ctx context.Context, id int64, messages ...string) error {
	for _, m := range messages {
		if _, err := s.pool.Exec(ctx, `INSERT INTO deployment_event (deployment_id, message) VALUES ($1, $2)`, id, m); err != nil {
			return err
		}
	}
	return nil
}

// Start begins a waiting deployment, at its first stage; driver names the copy of the
// deployer that drives a self deployment.
func (s *Store) Start(ctx context.Context, id int64, driver string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE deployment SET status = $2, stage = $3, started_at = now(), stage_at = now(), driver = nullif($4, '')
		WHERE id = $1 AND status = $5`, id, Running, Push, driver, Waiting)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return conflict("deployment %d is not waiting", id)
	}
	return s.Log(ctx, id, "started")
}

// SetStage moves a running deployment to a stage.
func (s *Store) SetStage(ctx context.Context, id int64, stage Stage) error {
	_, err := s.pool.Exec(ctx, `UPDATE deployment SET stage = $2, stage_at = now() WHERE id = $1 AND status = $3`, id, stage, Running)
	return err
}

// SetRoll stores a running deployment's roll state.
func (s *Store) SetRoll(ctx context.Context, id int64, roll []byte) error {
	_, err := s.pool.Exec(ctx, `UPDATE deployment SET roll = $2 WHERE id = $1`, id, roll)
	return err
}

// SetPushRun records the run that pushes a deployment's container's image.
func (s *Store) SetPushRun(ctx context.Context, id int64, container string, run int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE deployment_image SET run_id = $3 WHERE deployment_id = $1 AND container = $2`, id, container, run)
	return err
}

// SetPushed records the image a container's push pushed.
func (s *Store) SetPushed(ctx context.Context, id int64, container, reference string) error {
	_, err := s.pool.Exec(ctx, `UPDATE deployment_image SET reference = $3 WHERE deployment_id = $1 AND container = $2`, id, container, reference)
	return err
}

// SetMigration records a migration's outcome: its status, what it printed, and its report.
func (s *Store) SetMigration(ctx context.Context, id int64, position int, status MigrationStatus, output string, report []byte, refused []string) error {
	if refused == nil {
		refused = []string{}
	}
	_, err := s.pool.Exec(ctx, `UPDATE deployment_migration SET status = $3, output = $4, report = $5, refused = $6
		WHERE deployment_id = $1 AND position = $2`, id, position, status, output, report, refused)
	return err
}

// Finish ends a running deployment.
func (s *Store) Finish(ctx context.Context, id int64, status Status, reason string) error {
	if status.Open() {
		return fmt.Errorf("a deployment finishes succeeded, failed, halted, or canceled")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE deployment SET status = $2, stage = $3, reason = $4, finished_at = now()
		WHERE id = $1 AND status IN ($5, $6)`, id, status, Done, reason, Waiting, Running)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return conflict("deployment %d is over", id)
	}
	msg := status.String()
	if reason != "" {
		msg += ": " + reason
	}
	return s.Log(ctx, id, msg)
}

// StopPlan stops a failed deployment's plan: the deployments waiting in it, or in a place in
// it they took, are canceled, and those running in its failed wave, or in a place in it at or
// after that wave, asked to halt.
func (s *Store) StopPlan(ctx context.Context, plan, failed int64) error {
	reason := fmt.Sprintf("deployment %d of its plan failed", failed)
	var wave int
	if err := s.pool.QueryRow(ctx, `SELECT wave FROM deployment WHERE id = $1`, failed).Scan(&wave); err != nil {
		return err
	}
	rows, err := s.pool.Query(ctx, `UPDATE deployment SET status = $3, stage = $4, reason = $5, finished_at = now()
		WHERE (plan_id = $1 OR $1 = ANY(gate_plans)) AND id <> $2 AND status = $6 RETURNING id`, plan, failed, Canceled, Done, reason, Waiting)
	if err != nil {
		return err
	}
	canceled, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return err
	}
	for _, id := range canceled {
		if err := s.Log(ctx, id, "canceled: "+reason); err != nil {
			return err
		}
	}
	_, err = s.pool.Exec(ctx, `UPDATE deployment SET halt_by = $3 WHERE id <> $2 AND status = $4 AND halt_by IS NULL
		AND (plan_id = $1 OR EXISTS (SELECT 1 FROM unnest(gate_plans, gate_waves) AS g(p, w) WHERE g.p = $1 AND g.w >= $5))`,
		plan, failed, reason, Running, wave)
	return err
}

// ─── What people ask ───

// Retry queues a deployment again, at its commit, as a new deployment: one that failed, was
// halted, or was canceled, and is its deploy's latest.
func (s *Store) Retry(ctx context.Context, id int64, by string) (*Deployment, error) {
	var nid int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		ds, err := s.load(ctx, tx, `SELECT `+deploymentColumns+` FROM deployment WHERE id = $1 FOR UPDATE`, id)
		if err != nil {
			return err
		}
		if len(ds) == 0 {
			return ErrNotFound
		}
		d := ds[0]
		switch d.Status {
		case Waiting, Running:
			return conflict("deployment %d is %s: only one that is over is retried", id, d.Status)
		case Succeeded:
			return conflict("deployment %d succeeded", id)
		}
		var newer int64
		err = tx.QueryRow(ctx, `SELECT id FROM deployment WHERE repository_id = $1 AND deploy = $2 AND id > $3 ORDER BY id DESC LIMIT 1`,
			d.Repository, d.Deploy, id).Scan(&newer)
		switch {
		case err == nil:
			return conflict("deployment %d of the same deploy is newer: retry that one", newer)
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		spec := workflow.Deploy{File: deployFile(d.Deploy), Name: d.Name, Workload: d.Workload, Alone: d.Alone}
		for _, im := range d.Images {
			spec.Images = append(spec.Images, workflow.Image{Container: im.Container, Target: im.Target})
		}
		for _, m := range d.Migrations {
			spec.Migrate = append(spec.Migrate, workflow.Ref{Target: m.Target})
		}
		nid, err = insertDeployment(ctx, tx, d.Repository, nil, id, spec, d.Commit, d.FromSeq, d.ToSeq,
			fmt.Sprintf("a retry of deployment %d", id), 1, by, d.Self, Waiting, Queued, "")
		return err
	})
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, nid)
}

// deployFile is the workflow file of a deploy's identity, //<file>:deploy:<name>.
func deployFile(id string) string {
	i := strings.LastIndex(id, ":"+string(workflow.DeployPhase)+":")
	if i < 2 {
		return ""
	}
	return id[2:i]
}

// Halt asks a deployment to stop: one waiting is canceled at once, and one running is rolled
// back by its driver.
func (s *Store) Halt(ctx context.Context, id int64, by string) (*Deployment, error) {
	d, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	switch d.Status {
	case Waiting:
		if err := s.Finish(ctx, id, Canceled, "canceled by "+by); err != nil {
			return nil, err
		}
	case Running:
		if _, err := s.pool.Exec(ctx, `UPDATE deployment SET halt_by = $2 WHERE id = $1 AND halt_by IS NULL`, id, by); err != nil {
			return nil, err
		}
		if err := s.Log(ctx, id, "asked to halt by "+by); err != nil {
			return nil, err
		}
	default:
		return nil, conflict("deployment %d is %s", id, d.Status)
	}
	return s.Get(ctx, id)
}

// Allow allows the refused hazards of a deployment's waiting migration, recording who did; the
// driver applies it again with them.
func (s *Store) Allow(ctx context.Context, id int64, by string) (*Deployment, error) {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var stage Stage
		var status Status
		if err := tx.QueryRow(ctx, `SELECT stage, status FROM deployment WHERE id = $1 FOR UPDATE`, id).Scan(&stage, &status); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if status != Running || stage != Hazard {
			return conflict("deployment %d waits for no allowance", id)
		}
		var position int
		var refused []string
		if err := tx.QueryRow(ctx, `SELECT position, refused FROM deployment_migration WHERE deployment_id = $1 AND status = $2`,
			id, MigrationWaiting).Scan(&position, &refused); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE deployment_migration SET allowed = $3, allowed_by = $4, status = $5
			WHERE deployment_id = $1 AND position = $2`, id, position, refused, by, MigrationPending); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE deployment SET stage = $2, stage_at = now() WHERE id = $1`, id, Migrate); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO deployment_event (deployment_id, message) VALUES ($1, $2)`,
			id, fmt.Sprintf("%s allowed %v", by, refused))
		return err
	})
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, id)
}
