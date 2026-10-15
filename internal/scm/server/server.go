// Package server is the SCM role's nodes: the service node serves the Repositories, Store,
// ChangeLists, Review, Landing, LandingChecks, Browse, Events, and Workspaces services over the
// storage core; the GC node runs garbage collection (docs/scm.md, "Garbage collection and
// compaction").
package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/scm/service"
	"github.com/Cidan/ok/internal/scm/store"
	"github.com/Cidan/ok/internal/serve/node"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Service is the SCM's services, a node.Service.
type Service struct {
	store *store.Store
	dir   service.Directory
}

var _ node.Service = (*Service)(nil)

// New serves the store, asking dir whether OWNERS users, reviewers, and ACLs exist.
func New(s *store.Store, dir service.Directory) *Service { return &Service{store: s, dir: dir} }

func (*Service) Name() string                       { return "ok/scm/service" }
func (*Service) Dependencies() []node.Node          { return nil }
func (*Service) Setup(context.Context) error        { return nil }
func (n *Service) Healthy(ctx context.Context) bool { return n.store.Healthy(ctx) }

func (n *Service) RegisterRPC(_ context.Context, reg grpc.ServiceRegistrar) error {
	svc := service.New(n.store, n.dir)
	scmpb.RegisterRepositoriesServer(reg, svc.Repositories)
	scmpb.RegisterStoreServer(reg, svc.Store)
	scmpb.RegisterChangeListsServer(reg, svc.ChangeLists)
	scmpb.RegisterReviewServer(reg, svc.Review)
	scmpb.RegisterLandingServer(reg, svc.Landing)
	scmpb.RegisterLandingChecksServer(reg, svc.LandingChecks)
	scmpb.RegisterBrowseServer(reg, svc.Browse)
	scmpb.RegisterEventsServer(reg, svc.Events)
	scmpb.RegisterWorkspacesServer(reg, svc.Workspaces)
	return nil
}

// gcInterval is how often the GC node looks for work: retired packs to delete, and
// repositories due their daily pass.
const gcInterval = 10 * time.Minute

// GC is garbage collection's node, a node.Application. Every replica runs it; the one holding
// the store's GC lock does the work.
type GC struct {
	svc  *Service
	hour int
}

var _ node.Application = (*GC)(nil)

// NewGC collects the service's store, each repository's daily pass due from hour (UTC).
func NewGC(svc *Service, hour int) *GC { return &GC{svc: svc, hour: hour} }

func (*GC) Name() string                 { return "ok/scm/gc" }
func (n *GC) Dependencies() []node.Node  { return []node.Node{n.svc} }
func (*GC) Setup(context.Context) error  { return nil }
func (*GC) Healthy(context.Context) bool { return true }

func (n *GC) Start(ctx context.Context) error {
	for {
		n.run(ctx, n.svc.store)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(gcInterval):
		}
	}
}

// run does one round of garbage collection if this replica takes the lock.
func (n *GC) run(ctx context.Context, s *store.Store) {
	err := s.LeadGC(ctx, func(ctx context.Context) error {
		n.round(ctx, s)
		return nil
	})
	if err != nil && !errors.Is(err, lock.ErrNotAcquired) && ctx.Err() == nil {
		slog.Warn("gc: taking the lock", "err", err)
	}
}

// round deletes the retired packs past their grace, then runs and records each due
// repository's pass, until ctx ends — the lock lost among them.
func (n *GC) round(ctx context.Context, s *store.Store) {
	if deleted, err := s.DeleteRetiredPacks(ctx, time.Now()); err != nil {
		slog.Error("gc: deleting retired packs", "err", err)
	} else if deleted > 0 {
		slog.Info("gc: deleted retired packs", "packs", deleted)
	}
	due, err := s.DueForGC(ctx, time.Now(), n.hour)
	if err != nil {
		slog.Error("gc: listing repositories due a pass", "err", err)
		return
	}
	for _, repo := range due {
		if ctx.Err() != nil {
			return
		}
		report, err := s.GC(ctx, repo, time.Now(), store.GCOptions{})
		if report != nil {
			if rerr := s.RecordGC(ctx, report, err); rerr != nil {
				slog.Error("gc: recording a pass", "repository", repo, "err", rerr)
			}
		}
		if err != nil {
			slog.Error("gc: pass failed", "repository", repo, "err", err)
			continue
		}
		slog.Info("gc: pass", "report", report.String())
	}
}
