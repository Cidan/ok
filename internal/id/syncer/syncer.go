// Package syncer runs the sync from the system of record — as each replica starts, every interval,
// and soon after a SCIM client changes something — and deletes what has expired: each keyed work
// (internal/work), booked for when it is next due, done by one replica at a time.
package syncer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/work"
)

var (
	runs = metrics.NewCounter("ok_identity_sync_runs_total", "Syncs from the system of record, by how they ended.", "result")
	took = metrics.NewHistogram("ok_identity_sync_seconds", "How long a sync took.", metrics.Seconds)
)

// claimWait is how long a sync asked for waits for another replica's to finish.
const claimWait = 2 * time.Minute

// settle is how long after a SCIM change the sync waits for more, so a client provisioning many
// changes at once causes one sync.
const settle = 2 * time.Second

// Syncer is the sync's node: an Application.
type Syncer struct {
	store    *store.Store
	source   directory.Source
	name     string // the source's kind: static, google, entra, ldap, scim
	interval time.Duration
	holder   string
}

var _ node.Application = (*Syncer)(nil)

// New is a syncer of source (of kind name) into st, every interval.
func New(st *store.Store, source directory.Source, name string, interval time.Duration) *Syncer {
	return &Syncer{store: st, source: source, name: name, interval: interval, holder: work.NewHolder()}
}

func (s *Syncer) Name() string                 { return "ok/identity/sync" }
func (s *Syncer) Dependencies() []node.Node    { return nil }
func (s *Syncer) Setup(context.Context) error  { return nil }
func (s *Syncer) Healthy(context.Context) bool { return true }

// Source names the source's kind.
func (s *Syncer) Source() string { return s.name }

// Changed books a sync soon: a SCIM client changed something. Changes within settle of each other
// share it (work.Store.MarkAt keeps the sooner).
func (s *Syncer) Changed() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.store.Work().MarkAt(ctx, store.DirectorySyncWork, 0, time.Now().Add(settle)); err != nil {
		slog.Error("identity sync: booking a sync after a SCIM change", "err", err)
	}
}

// Sync runs one sync now, holding the sync's claim — waiting for another replica's sync to let go,
// told as it does (store.Claims) — and returns what it did.
func (s *Syncer) Sync(ctx context.Context) (*directory.Plan, error) {
	held, release, ok, err := s.store.Claims().Claim(ctx, store.DirectorySyncWork, 0, s.holder, time.Minute, claimWait)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("another replica's sync is still running")
	}
	defer release()
	return s.sync(held)
}

// sync syncs, holding the sync's claim, and records how it went.
func (s *Syncer) sync(ctx context.Context) (*directory.Plan, error) {
	start := time.Now()
	plan, err := func() (*directory.Plan, error) {
		opts, err := s.store.SyncOptions(ctx)
		if err != nil {
			return nil, err
		}
		return directory.Sync(ctx, s.store.Names(), s.source, s.store, opts)
	}()
	took.Since(start)
	if err != nil {
		runs.With("failed").Inc()
		if rerr := s.store.SyncFailed(context.WithoutCancel(ctx), err); rerr != nil {
			slog.Error("identity sync: recording the failure", "err", rerr)
		}
		return nil, err
	}
	runs.With("done").Inc()
	if len(plan.Humans)+len(plan.Groups) > 0 {
		if err := s.store.Audit(ctx, "sync", "sync", map[string]any{"source": s.name, "humans": len(plan.Humans), "groups": len(plan.Groups), "waiting": len(plan.Waiting)}); err != nil {
			slog.Error("identity sync: audit", "err", err)
		}
	}
	return plan, nil
}

// Start books a sync now — as each replica starts, as identity did — and runs the syncs as their
// jobs come, each booking the next an interval on, until ctx ends. A sync that fails is recorded
// (store.SyncFailed) and tried again then.
func (s *Syncer) Start(ctx context.Context) error {
	w := s.store.Work()
	if err := w.Mark(ctx, store.DirectorySyncWork, 0); err != nil {
		return err
	}
	return (&work.Runner{
		Kind:  store.DirectorySyncWork,
		Store: w,
		Do: func(ctx context.Context, _ int64) error {
			if _, err := s.sync(ctx); err != nil && ctx.Err() == nil {
				slog.Error("identity sync", "source", s.name, "err", err)
			}
			return w.MarkAt(ctx, store.DirectorySyncWork, 0, time.Now().Add(s.interval))
		},
		Workers: 1,
		// A sync asked for waits for this one to let go (Sync).
		LetGo: s.store.Claims().LetGo(store.DirectorySyncWork),
	}).Run(ctx)
}

// housekeepEvery is how often what has expired is deleted.
const housekeepEvery = 10 * time.Minute

// Housekeeper deletes what has expired — sessions, ceremonies, device logins, proofs' nonces —
// every housekeepEvery.
type Housekeeper struct{ Store *store.Store }

var _ node.Application = Housekeeper{}

func (Housekeeper) Name() string                 { return "ok/identity/housekeeping" }
func (Housekeeper) Dependencies() []node.Node    { return nil }
func (Housekeeper) Setup(context.Context) error  { return nil }
func (Housekeeper) Healthy(context.Context) bool { return true }

// Start books housekeeping now — as each replica starts, as identity did — and does it as its jobs
// come, each booking the next, until ctx ends.
func (h Housekeeper) Start(ctx context.Context) error {
	w := h.Store.Work()
	if err := w.Mark(ctx, store.HousekeepingWork, 0); err != nil {
		return err
	}
	return (&work.Runner{
		Kind:  store.HousekeepingWork,
		Store: w,
		Do: func(ctx context.Context, _ int64) error {
			if err := h.Store.Expire(ctx); err != nil && ctx.Err() == nil {
				slog.Error("identity housekeeping", "err", err)
			}
			return w.MarkAt(ctx, store.HousekeepingWork, 0, time.Now().Add(housekeepEvery))
		},
		Workers: 1,
	}).Run(ctx)
}
