// Package syncer runs the sync from the system of record: at start, every interval, and soon after
// a SCIM client changes something — one replica at a time, under a lock in the database.
package syncer

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/serve/node"
)

var (
	runs = metrics.NewCounter("ok_identity_sync_runs_total", "Syncs from the system of record, by how they ended.", "result")
	took = metrics.NewHistogram("ok_identity_sync_seconds", "How long a sync took.", metrics.Seconds)
)

// lockWait is how long a sync waits for another replica's to finish.
const lockWait = 2 * time.Minute

// settle is how long after a SCIM change the sync waits for more, so a client provisioning many
// changes at once causes one sync.
const settle = 2 * time.Second

// Syncer is the sync's node: an Application.
type Syncer struct {
	store    *store.Store
	source   directory.Source
	name     string // the source's kind: static, google, entra, ldap, scim
	interval time.Duration
	poke     chan struct{}
	failing  atomic.Bool
}

var _ node.Application = (*Syncer)(nil)

// New is a syncer of source (of kind name) into st, every interval.
func New(st *store.Store, source directory.Source, name string, interval time.Duration) *Syncer {
	return &Syncer{store: st, source: source, name: name, interval: interval, poke: make(chan struct{}, 1)}
}

func (s *Syncer) Name() string                 { return "ok/identity/sync" }
func (s *Syncer) Dependencies() []node.Node    { return nil }
func (s *Syncer) Setup(context.Context) error  { return nil }
func (s *Syncer) Healthy(context.Context) bool { return true }

// Source names the source's kind.
func (s *Syncer) Source() string { return s.name }

// Changed asks for a sync soon: a SCIM client changed something.
func (s *Syncer) Changed() {
	select {
	case s.poke <- struct{}{}:
	default:
	}
}

// Sync runs one sync now, waiting for another replica's to finish, and returns what it did.
func (s *Syncer) Sync(ctx context.Context) (*directory.Plan, error) {
	var plan *directory.Plan
	start := time.Now()
	err := s.store.LockSync(ctx, lockWait, func(ctx context.Context) error {
		opts, err := s.store.SyncOptions(ctx)
		if err != nil {
			return err
		}
		plan, err = directory.Sync(ctx, s.store.Names(), s.source, s.store, opts)
		return err
	})
	took.Since(start)
	if err != nil {
		runs.With("failed").Inc()
		s.failing.Store(true)
		if errors.Is(err, lock.ErrNotAcquired) {
			return nil, errors.New("another replica's sync is still running")
		}
		if rerr := s.store.SyncFailed(context.WithoutCancel(ctx), err); rerr != nil {
			slog.Error("identity sync: recording the failure", "err", rerr)
		}
		return nil, err
	}
	runs.With("done").Inc()
	s.failing.Store(false)
	if len(plan.Humans)+len(plan.Groups) > 0 {
		if err := s.store.Audit(ctx, "sync", "sync", map[string]any{"source": s.name, "humans": len(plan.Humans), "groups": len(plan.Groups), "waiting": len(plan.Waiting)}); err != nil {
			slog.Error("identity sync: audit", "err", err)
		}
	}
	return plan, nil
}

// Start syncs at once, then every interval and soon after each change, until ctx ends.
func (s *Syncer) Start(ctx context.Context) error {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		if _, err := s.Sync(ctx); err != nil && ctx.Err() == nil {
			slog.Error("identity sync", "source", s.name, "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		case <-s.poke:
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(settle):
			}
		}
	}
}

// Housekeeper deletes what has expired — sessions, ceremonies, device logins — every ten minutes.
type Housekeeper struct{ Store *store.Store }

var _ node.Application = Housekeeper{}

func (Housekeeper) Name() string                 { return "ok/identity/housekeeping" }
func (Housekeeper) Dependencies() []node.Node    { return nil }
func (Housekeeper) Setup(context.Context) error  { return nil }
func (Housekeeper) Healthy(context.Context) bool { return true }

func (h Housekeeper) Start(ctx context.Context) error {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		if err := h.Store.Expire(ctx); err != nil && ctx.Err() == nil {
			slog.Error("identity housekeeping", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
