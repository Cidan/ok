// Package store is the deployer's storage (docs/deploy.md): each repository's main as the SCM's
// change feed told it, the plans made at its heads, and every deployment — what it deploys, its
// stage, what it pushed and migrated, its roll's resumable state, and its log. Store holds the
// rules; a Backend (postgres) is the database it keeps them in.
package store

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/work"
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
	RequestedBy string             // a principal's stable ID
	Workload    *workflow.Workload // nil for a deploy that only migrates
	Status      Status
	Stage       Stage
	Reason      string
	Roll        []byte // the roll's resumable state, JSON
	Self        bool   // it rolls the deployer itself
	Alone       bool   // it runs while no other deployment does (workflow.Deploy.Alone)
	Driver      string // a self deployment's driving copy
	HaltBy      string // who asked it to halt, by stable ID
	// HaltReason is why ok asked it to halt, itself: another deployment of its plan failed.
	HaltReason string
	CreatedAt  time.Time
	StartedAt  time.Time
	StageAt    time.Time
	FinishedAt time.Time
	Images     []Image
	Migrations []Migration
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
	b Backend
}

// New is a store over a backend.
func New(b Backend) *Store { return &Store{b: b} }

// ─── Leases ───

// The deployer has no leader (docs/hosted.md, "Work without leaders"): each repository's planning
// and each service's observation is keyed work (internal/work), leased to one replica at a time;
// each deployment is stepped by whoever takes its lock; and which deployments start is decided by
// one transaction at a time (StartTurns). Whatever a lease's holder writes is fenced by it.

// Kinds of the deployer's keyed work.
const (
	// PlanWork plans a repository whose main moved.
	PlanWork = "plan"
	// ObserveWork reads a service's workload, due ObservePeriod after the last read.
	ObserveWork = "observe"
)

// Work is the deployer's keyed work's store (internal/work).
func (s *Store) Work() work.Store { return workStore{s.b} }

type workStore struct{ b Backend }

func (w workStore) Mark(ctx context.Context, kind string, key int64) error {
	return w.b.MarkWork(ctx, kind, key)
}
func (w workStore) MarkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return w.b.MarkWorkAt(ctx, kind, key, at)
}
func (w workStore) Take(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	return w.b.TakeWork(ctx, kind, key, holder, ttl)
}
func (w workStore) Renew(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	return w.b.RenewWork(ctx, kind, key, holder, ttl)
}
func (w workStore) Release(ctx context.Context, kind string, key int64, holder string) (bool, error) {
	return w.b.ReleaseWork(ctx, kind, key, holder)
}
func (w workStore) Let(ctx context.Context, kind string, key int64, holder string) error {
	return w.b.LetWork(ctx, kind, key, holder)
}
func (w workStore) Due(ctx context.Context, kind string, limit int) ([]int64, error) {
	return w.b.DueWork(ctx, kind, limit)
}

// MarkIdle marks key's work of kind due now if nothing marks it and no one holds it: what a sweep
// marks — work that is new, or whose holder died before marking it again — leaving work its
// holder is doing, or marked to do later, as it is.
func (s *Store) MarkIdle(ctx context.Context, kind string, key int64) error {
	return s.b.MarkWorkIdle(ctx, kind, key)
}

// LockDeployment takes a deployment's lock, if no one holds it, until release: whoever steps a
// deployment holds it, and hands it on to a migration that outlives the pass that started it,
// so it is held past ctx — refreshed until released, or until this process dies. held is ctx
// carrying the lock's fence: what is written with it is written only while the lock is held.
func (s *Store) LockDeployment(ctx context.Context, id int64) (held context.Context, release func(), ok bool, err error) {
	fence, release, ok, err := s.b.TryLockDeployment(ctx, id)
	if err != nil || !ok {
		return nil, nil, ok, err
	}
	// What is written with it is about its repository, through its move gate
	// (docs/hosted.md, "Cells, as built").
	ds, err := s.b.Deployments(ctx, []int64{id})
	if err != nil {
		release()
		return nil, nil, false, err
	}
	held = data.WithFence(ctx, fence)
	if len(ds) > 0 {
		held = move.WithRepository(held, ds[0].Repository)
	}
	return held, release, true, nil
}

// aboutDeployment is ctx naming the repository of a deployment, if it names none: a write about a
// deployment passes its repository's move gate (docs/hosted.md, "Cells, as built").
func (s *Store) aboutDeployment(ctx context.Context, id int64) (context.Context, error) {
	if _, ok := move.RepositoryOf(ctx); ok {
		return ctx, nil
	}
	ds, err := s.b.Deployments(ctx, []int64{id})
	if err != nil {
		return nil, err
	}
	if len(ds) == 0 {
		return nil, ErrNotFound
	}
	return move.WithRepository(ctx, ds[0].Repository), nil
}

// write runs fn on the database: in a transaction of its own when ctx carries a fence, so that
// the write is fenced (data.Fence), and as one statement otherwise.
func (s *Store) write(ctx context.Context, fn func(Ops) error) error {
	if _, ok := data.FenceOf(ctx); ok {
		return s.b.Txn(ctx, fn)
	}
	return fn(s.b)
}

// ─── The feed ───

// Cursor is the place in a repository's log of the last of its events the store applied, a
// follow.Applier's.
func (s *Store) Cursor(ctx context.Context, repo int64) (int64, error) {
	return s.b.FollowCursor(ctx, scmfeed.Topic, repo)
}

// Cursors are Cursor's of repos, at once.
func (s *Store) Cursors(ctx context.Context, repos []int64) (map[int64]int64, error) {
	return s.b.FollowCursors(ctx, scmfeed.Topic, repos)
}

// FeedEvent is what the store keeps of one event of the change feed: a repository's creation, or
// main's moving. Seq is its place in its repository's log. A repository's name is the directory's
// (ApplyDirectory).
type FeedEvent struct {
	Seq        int64
	Repository int64
	Created    bool
	Head       []byte // main's move: its new head, and its place in main's history
	HeadSeq    int64
}

// Apply applies one event of the change feed, once: an event at or before its repository's
// cursor is skipped.
func (s *Store) Apply(ctx context.Context, ev FeedEvent) error {
	ctx = move.WithRepository(ctx, ev.Repository)
	return s.b.Txn(ctx, func(tx Ops) error {
		cursor, err := tx.LockFollowCursor(ctx, scmfeed.Topic, ev.Repository)
		if err != nil {
			return err
		}
		if ev.Seq <= cursor {
			return nil
		}
		switch {
		case ev.Created:
			if err := tx.EnsureRepository(ctx, ev.Repository); err != nil {
				return err
			}
		case ev.HeadSeq > 0:
			if err := tx.EnsureRepository(ctx, ev.Repository); err != nil {
				return err
			}
			if err := tx.AdvanceHead(ctx, ev.Repository, ev.Head, ev.HeadSeq); err != nil {
				return err
			}
			if err := tx.AddMainCommit(ctx, ev.Repository, ev.HeadSeq, ev.Head); err != nil {
				return err
			}
		}
		return tx.SetFollowCursor(ctx, scmfeed.Topic, ev.Repository, ev.Seq)
	})
}

// DirectoryEvent is what the store keeps of one event of the directory's log (dirfeed): a
// repository's full name as it is after it, "" for none; Purged, when the directory purged it.
// Seq is its place in its repository's log.
type DirectoryEvent struct {
	Seq        int64
	Repository int64
	FullName   string
	// Owner is its owner's stable ID: whose agent its deployments go through.
	Owner  string
	Purged bool
}

// DirectoryCursors are the places in each repository's directory log applied.
func (s *Store) DirectoryCursors(ctx context.Context, repos []int64) (map[int64]int64, error) {
	return s.b.FollowCursors(ctx, dirfeed.Topic, repos)
}

// ApplyDirectory applies one event of the directory's log, once: an event at or before its
// repository's cursor is skipped. A repository's name is its full name as ok shows it; purged, it
// is forgotten whole. What it deployed runs on as it is, the deployer managing it no more.
func (s *Store) ApplyDirectory(ctx context.Context, ev DirectoryEvent) error {
	// The directory's events are every repository's, wherever it is or moves: no move gates them.
	ctx = move.Untagged(ctx)
	return s.b.Txn(ctx, func(tx Ops) error {
		cursor, err := tx.LockFollowCursor(ctx, dirfeed.Topic, ev.Repository)
		if err != nil {
			return err
		}
		if ev.Seq <= cursor {
			return nil
		}
		switch {
		case ev.Purged:
			if err := tx.DropRepository(ctx, ev.Repository); err != nil {
				return err
			}
		case ev.FullName != "":
			if err := tx.NameRepository(ctx, ev.Repository, ev.FullName, ev.Owner); err != nil {
				return err
			}
		}
		return tx.SetFollowCursor(ctx, dirfeed.Topic, ev.Repository, ev.Seq)
	})
}

// Unplanned are the repositories whose main moved past their latest plan, once the directory has
// named them: whether a plan applies the groups-and-ACLs tree depends on the name.
func (s *Store) Unplanned(ctx context.Context) ([]Repository, error) { return s.b.Unplanned(ctx) }

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
	ctx = move.WithRepository(ctx, p.Repository)
	err := s.b.Txn(ctx, func(tx Ops) error {
		planned, err := tx.LockPlannedSeq(ctx, p.Repository)
		if err != nil {
			return err
		}
		if planned >= p.HeadSeq {
			return conflict("the repository is planned at %d already", planned)
		}
		notes := p.Notes
		if notes == nil {
			notes = []string{}
		}
		if id, err = tx.InsertPlan(ctx, p.Repository, p.Head, p.HeadSeq, p.BaseSeq, notes); err != nil {
			return err
		}
		for _, d := range p.Deployments {
			var gates []Gate
			if d.FailedWhy == "" {
				replaced, err := tx.CancelWaitingOf(ctx, p.Repository, d.Deploy.ID(), fmt.Sprintf("replaced by the deployment at %s", short(p.Head)))
				if err != nil {
					return err
				}
				for _, r := range replaced {
					gates = append(gates, r.Gates...)
					if r.Plan != 0 {
						gates = append(gates, Gate{Plan: r.Plan, Wave: r.Wave})
					}
				}
			}
			status, stage, reason := Waiting, Queued, ""
			if d.FailedWhy != "" {
				status, stage, reason = Failed, Done, d.FailedWhy
			}
			nid, err := insertDeployment(ctx, tx, DeploymentRow{
				Repository: p.Repository, Plan: id, Deploy: d.Deploy, Commit: p.Head, FromSeq: p.BaseSeq, ToSeq: p.HeadSeq,
				Why: d.Why, Wave: d.Wave, RequestedBy: p.By, Self: d.Self, Status: status, Stage: stage, Reason: reason,
			})
			if err != nil {
				return err
			}
			if len(gates) > 0 {
				if err := tx.SetGates(ctx, nid, gates); err != nil {
					return err
				}
				if err := tx.AppendEvent(ctx, nid, fmt.Sprintf("it takes the place of the deployment it replaced: %s", describeGates(gates))); err != nil {
					return err
				}
			}
		}
		if err := tx.RecordServices(ctx, p.Repository, p.HeadSeq, p.Services); err != nil {
			return err
		}
		return tx.SetPlanned(ctx, p.Repository, p.Head, p.HeadSeq)
	})
	return id, err
}

// Plans are a repository's plans (0: every repository's), oldest first.
func (s *Store) Plans(ctx context.Context, repo int64) ([]Plan, error) { return s.b.Plans(ctx, repo) }

// insertDeployment records a deployment and the first line of its log.
func insertDeployment(ctx context.Context, tx Ops, d DeploymentRow) (int64, error) {
	id, err := tx.InsertDeployment(ctx, d)
	if err != nil {
		return 0, err
	}
	msg := fmt.Sprintf("planned at %s: %s", short(d.Commit), d.Why)
	if d.RetryOf > 0 {
		msg = fmt.Sprintf("a retry of deployment %d at %s", d.RetryOf, short(d.Commit))
	}
	if d.Reason != "" {
		msg += "; " + d.Reason
	}
	if err := tx.AppendEvent(ctx, id, msg); err != nil {
		return 0, err
	}
	return id, nil
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

// Services are the services, by repository and name: a repository's (0: every repository).
func (s *Store) Services(ctx context.Context, repo int64) ([]*Service, error) {
	rows, err := s.b.Services(ctx, repo)
	if err != nil {
		return nil, err
	}
	return s.fill(ctx, rows)
}

// GetService reads a service.
func (s *Store) GetService(ctx context.Context, id int64) (*Service, error) {
	row, ok, err := s.b.Service(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	out, err := s.fill(ctx, []ServiceRow{row})
	if err != nil {
		return nil, err
	}
	return out[0], nil
}

// fill gives services their observations and their latest and deployed deployments.
func (s *Store) fill(ctx context.Context, rows []ServiceRow) ([]*Service, error) {
	out := make([]*Service, len(rows))
	if len(rows) == 0 {
		return out, nil
	}
	svIDs := make([]int64, len(rows))
	var ids []int64
	for i := range rows {
		sv := rows[i].Service
		out[i] = &sv
		svIDs[i] = sv.ID
		for _, id := range []int64{rows[i].LatestID, rows[i].DeployedID} {
			if id != 0 {
				ids = append(ids, id)
			}
		}
	}
	obs, err := s.b.Observations(ctx, svIDs)
	if err != nil {
		return nil, err
	}
	for _, sv := range out {
		sv.Observation = obs[sv.ID]
	}
	if len(ids) == 0 {
		return out, nil
	}
	ds, err := s.b.Deployments(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := map[int64]*Deployment{}
	for _, d := range ds {
		byID[d.ID] = d
	}
	for i, r := range rows {
		if r.LatestID != 0 {
			out[i].Latest = byID[r.LatestID]
		}
		if r.DeployedID != 0 {
			out[i].Deployed = byID[r.DeployedID]
		}
	}
	return out, nil
}

// Recent are a service's latest deployments, newest first.
func (s *Store) Recent(ctx context.Context, sv *Service, limit int) ([]*Deployment, error) {
	return s.b.DeployDeployments(ctx, sv.Repository, sv.Deploy, limit)
}

// RecordObservation stores what the observer read of a service's workload.
func (s *Store) RecordObservation(ctx context.Context, service int64, o Observation) error {
	sv, err := s.GetService(ctx, service)
	if err != nil {
		return err
	}
	ctx = move.WithRepository(ctx, sv.Repository)
	return s.write(ctx, func(tx Ops) error { return tx.RecordObservation(ctx, service, o) })
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

// Get reads a deployment.
func (s *Store) Get(ctx context.Context, id int64) (*Deployment, error) {
	ds, err := s.b.Deployments(ctx, []int64{id})
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
	return s.b.OpenDeployments(ctx)
}

// Retiring are the self deployments that succeeded and whose old copy is still to be scaled
// to 0: their roll says so (roll.State.Retire).
func (s *Store) Retiring(ctx context.Context) ([]*Deployment, error) {
	return s.b.RetiringDeployments(ctx)
}

// PlanStatuses are the statuses of a plan's deployments, by wave.
func (s *Store) PlanStatuses(ctx context.Context, plan int64) (map[int][]Status, error) {
	return s.b.PlanStatuses(ctx, plan)
}

// Covering are the deployments that cover a commit of main — whose range includes it — newest
// first; planned reports whether a plan has reached the commit, after which no deployment of it
// is yet to be made.
func (s *Store) Covering(ctx context.Context, repo int64, commit []byte) (ds []*Deployment, planned bool, err error) {
	seq, planned, ok, err := s.b.MainCommit(ctx, repo, commit)
	if err != nil || !ok {
		return nil, false, err
	}
	ds, err = s.b.CoveringDeployments(ctx, repo, seq)
	return ds, planned, err
}

// Latest are a repository's latest deployments, newest first.
func (s *Store) Latest(ctx context.Context, repo int64, limit int) ([]*Deployment, error) {
	return s.b.LatestDeployments(ctx, repo, limit)
}

// Events are a deployment's log, oldest first.
func (s *Store) Events(ctx context.Context, id int64) ([]Event, error) { return s.b.Events(ctx, id) }

// Log adds lines to a deployment's log.
func (s *Store) Log(ctx context.Context, id int64, messages ...string) error {
	ctx, err := s.aboutDeployment(ctx, id)
	if err != nil {
		return err
	}
	return s.write(ctx, func(tx Ops) error {
		for _, m := range messages {
			if err := tx.AppendEvent(ctx, id, m); err != nil {
				return err
			}
		}
		return nil
	})
}

// Start begins a waiting deployment, at its first stage; driver names the copy of the
// deployer that drives a self deployment.
func (s *Store) Start(ctx context.Context, id int64, driver string) error {
	ctx, err := s.aboutDeployment(ctx, id)
	if err != nil {
		return err
	}
	return s.write(ctx, func(tx Ops) error { return start(ctx, tx, id, driver) })
}

func start(ctx context.Context, tx Ops, id int64, driver string) error {
	ok, err := tx.StartDeployment(ctx, id, driver)
	if err != nil {
		return err
	}
	if !ok {
		return conflict("deployment %d is not waiting", id)
	}
	return tx.AppendEvent(ctx, id, "started")
}

// SetStage moves a running deployment to a stage.
func (s *Store) SetStage(ctx context.Context, id int64, stage Stage) error {
	ctx, err := s.aboutDeployment(ctx, id)
	if err != nil {
		return err
	}
	return s.write(ctx, func(tx Ops) error { return tx.SetStage(ctx, id, stage) })
}

// SetRoll stores a running deployment's roll state.
func (s *Store) SetRoll(ctx context.Context, id int64, roll []byte) error {
	ctx, err := s.aboutDeployment(ctx, id)
	if err != nil {
		return err
	}
	return s.write(ctx, func(tx Ops) error { return tx.SetRoll(ctx, id, roll) })
}

// SetPushRun records the run that pushes a deployment's container's image.
func (s *Store) SetPushRun(ctx context.Context, id int64, container string, run int64) error {
	ctx, err := s.aboutDeployment(ctx, id)
	if err != nil {
		return err
	}
	return s.write(ctx, func(tx Ops) error { return tx.SetPushRun(ctx, id, container, run) })
}

// SetPushed records the image a container's push pushed.
func (s *Store) SetPushed(ctx context.Context, id int64, container, reference string) error {
	ctx, err := s.aboutDeployment(ctx, id)
	if err != nil {
		return err
	}
	return s.write(ctx, func(tx Ops) error { return tx.SetPushed(ctx, id, container, reference) })
}

// SetMigration records a migration's outcome: its status, what it printed, and its report.
func (s *Store) SetMigration(ctx context.Context, id int64, position int, status MigrationStatus, output string, report []byte, refused []string) error {
	ctx, err := s.aboutDeployment(ctx, id)
	if err != nil {
		return err
	}
	if refused == nil {
		refused = []string{}
	}
	return s.write(ctx, func(tx Ops) error { return tx.SetMigration(ctx, id, position, status, output, report, refused) })
}

// Finish ends a running deployment.
func (s *Store) Finish(ctx context.Context, id int64, status Status, reason string) error {
	ctx, err := s.aboutDeployment(ctx, id)
	if err != nil {
		return err
	}
	if status.Open() {
		return fmt.Errorf("a deployment finishes succeeded, failed, halted, or canceled")
	}
	return s.write(ctx, func(tx Ops) error {
		ok, err := tx.FinishDeployment(ctx, id, status, reason)
		if err != nil {
			return err
		}
		if !ok {
			return conflict("deployment %d is over", id)
		}
		msg := status.String()
		if reason != "" {
			msg += ": " + reason
		}
		return tx.AppendEvent(ctx, id, msg)
	})
}

// StopPlan stops a failed deployment's plan: the deployments waiting in it, or in a place in
// it they took, are canceled, and those running in its failed wave, or in a place in it at or
// after that wave, asked to halt.
func (s *Store) StopPlan(ctx context.Context, plan, failed int64) error {
	reason := fmt.Sprintf("deployment %d of its plan failed", failed)
	f, err := s.Get(ctx, failed)
	if err != nil {
		return err
	}
	ctx = move.WithRepository(ctx, f.Repository)
	return s.write(ctx, func(tx Ops) error {
		canceled, err := tx.CancelWaitingIn(ctx, plan, failed, reason)
		if err != nil {
			return err
		}
		for _, id := range canceled {
			if err := tx.AppendEvent(ctx, id, "canceled: "+reason); err != nil {
				return err
			}
		}
		return tx.HaltRunningIn(ctx, plan, failed, f.Wave, reason)
	})
}

// ─── What people ask ───

// Retry queues a deployment again, at its commit, as a new deployment: one that failed, was
// halted, or was canceled, and is its deploy's latest.
func (s *Store) Retry(ctx context.Context, id int64, by string) (*Deployment, error) {
	var nid int64
	err := s.b.Txn(ctx, func(tx Ops) error {
		d, err := tx.LockDeployment(ctx, id)
		if err != nil {
			return err
		}
		if d == nil {
			return ErrNotFound
		}
		if err := tx.Gate(ctx, d.Repository); err != nil {
			return err
		}
		switch d.Status {
		case Waiting, Running:
			return conflict("deployment %d is %s: only one that is over is retried", id, d.Status)
		case Succeeded:
			return conflict("deployment %d succeeded", id)
		}
		newer, ok, err := tx.NewerOf(ctx, d.Repository, d.Deploy, id)
		if err != nil {
			return err
		}
		if ok {
			return conflict("deployment %d of the same deploy is newer: retry that one", newer)
		}
		spec := workflow.Deploy{File: deployFile(d.Deploy), Name: d.Name, Workload: d.Workload, Alone: d.Alone}
		for _, im := range d.Images {
			spec.Images = append(spec.Images, workflow.Image{Container: im.Container, Target: im.Target})
		}
		for _, m := range d.Migrations {
			spec.Migrate = append(spec.Migrate, workflow.Ref{Target: m.Target})
		}
		nid, err = insertDeployment(ctx, tx, DeploymentRow{
			Repository: d.Repository, RetryOf: id, Deploy: spec, Commit: d.Commit, FromSeq: d.FromSeq, ToSeq: d.ToSeq,
			Why: fmt.Sprintf("a retry of deployment %d", id), Wave: 1, RequestedBy: by, Self: d.Self, Status: Waiting, Stage: Queued,
		})
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
	ctx = move.WithRepository(ctx, d.Repository)
	switch d.Status {
	case Waiting:
		if err := s.b.SetHaltBy(ctx, id, by); err != nil {
			return nil, err
		}
		if err := s.Finish(ctx, id, Canceled, "halted before it started"); err != nil {
			return nil, err
		}
	case Running:
		if err := s.b.SetHaltBy(ctx, id, by); err != nil {
			return nil, err
		}
		if err := s.Log(ctx, id, "asked to halt"); err != nil {
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
	err := s.b.Txn(ctx, func(tx Ops) error {
		d, err := tx.LockDeployment(ctx, id)
		if err != nil {
			return err
		}
		if d == nil {
			return ErrNotFound
		}
		if err := tx.Gate(ctx, d.Repository); err != nil {
			return err
		}
		if d.Status != Running || d.Stage != Hazard {
			return conflict("deployment %d waits for no allowance", id)
		}
		position, refused, ok, err := tx.WaitingMigration(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return conflict("deployment %d waits for no allowance", id)
		}
		if err := tx.AllowMigration(ctx, id, position, refused, by); err != nil {
			return err
		}
		if err := tx.SetStage(ctx, id, Migrate); err != nil {
			return err
		}
		return tx.AppendEvent(ctx, id, fmt.Sprintf("%s allowed %v", by, refused))
	})
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, id)
}

// ─── Agents (docs/hosted.md, "Deploy agents, as built") ───

// AgentSession is an organization's agent's connection to this cell: the replica holding it, and
// where the other replicas reach that replica.
type AgentSession struct {
	Account     string
	Holder      string
	Address     string
	LeaseUntil  time.Time
	ConnectedAt time.Time
}

// RepositoryOwner is a repository's owner's stable ID, as the directory last said; "" before it has.
func (s *Store) RepositoryOwner(ctx context.Context, repo int64) (string, error) {
	return s.b.RepositoryOwner(ctx, repo)
}

// ClaimAgentSession records an agent's connection as a replica's, replacing any other's.
func (s *Store) ClaimAgentSession(ctx context.Context, a AgentSession) error {
	return s.b.ClaimAgentSession(move.Untagged(ctx), a)
}

// RenewAgentSession extends a replica's lease on a session, reporting false once it is another's.
func (s *Store) RenewAgentSession(ctx context.Context, account, holder string, until time.Time) (bool, error) {
	return s.b.RenewAgentSession(move.Untagged(ctx), account, holder, until)
}

// ReleaseAgentSession forgets a session, if it is the replica's.
func (s *Store) ReleaseAgentSession(ctx context.Context, account, holder string) error {
	return s.b.ReleaseAgentSession(move.Untagged(ctx), account, holder)
}

// AgentSession is an account's agent's session, whether its lease lasts or not.
func (s *Store) AgentSession(ctx context.Context, account string) (AgentSession, bool, error) {
	return s.b.AgentSession(ctx, account)
}
