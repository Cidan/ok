// Package server is the SCM role's nodes: the service node serves the Repositories, Store,
// ChangeLists, Review, Landing, LandingChecks, Browse, Events, and Workspaces services over the
// storage core; the GC node runs garbage collection (docs/scm.md, "Garbage collection and
// compaction"), each repository's on one replica at a time; the relay node publishes the SCM's
// events on the bus's stream.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/scm/service"
	"github.com/Cidan/ok/internal/scm/store"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/work"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Service is the SCM's services, a node.Service.
type Service struct {
	store *store.Store
	dir   service.Directory
	ten   service.Tenancy
	facts authz.Source
	// cell, peers, and routes are InCell's.
	cell   string
	peers  service.PeerSCMs
	routes service.Router
}

// InCell says which cell the SCM serves, how it reaches the others', and where the directory routes
// repositories (service.Services.InCell); before it serves.
func (n *Service) InCell(name string, peers service.PeerSCMs, routes service.Router) {
	n.cell, n.peers, n.routes = name, peers, routes
}

var _ node.Service = (*Service)(nil)

// New serves the store, asking dir whether OWNERS users, reviewers, and ACLs exist, ten — the
// directory's tenancy — to make, find, and name repositories, and facts what Authorize decides
// people's requests from.
func New(s *store.Store, dir service.Directory, ten service.Tenancy, facts authz.Source) *Service {
	return &Service{store: s, dir: dir, ten: ten, facts: facts}
}

func (*Service) Name() string                       { return "ok/scm/service" }
func (*Service) Dependencies() []node.Node          { return nil }
func (*Service) Setup(context.Context) error        { return nil }
func (n *Service) Healthy(ctx context.Context) bool { return n.store.Healthy(ctx) }

func (n *Service) RegisterRPC(_ context.Context, reg grpc.ServiceRegistrar) error {
	s := service.New(n.store, n.dir, n.ten, n.facts)
	if n.cell != "" {
		s.InCell(n.cell, n.peers, n.routes)
	}
	// Each call reads what it needs once for its life, and nothing for longer (store.WithRequestScope).
	// Moves, shared by a process's roles on one server, is registered on the server's own registrar.
	s.Register(store.ScopedRegistrar(reg))
	move.NewServer(n.store.Mover(), okpb.Caller_CALLER_SCM, func(ctx context.Context, cell string) (okpb.MovesClient, error) {
		if n.peers == nil {
			return nil, fmt.Errorf("this SCM reaches no other cell")
		}
		return n.peers.Moves(ctx, cell)
	}).Register(reg)
	return nil
}

// MovePurge deletes the networks that moved away from this cell a day after (store.MovePurgeWork),
// a node.Application.
type MovePurge struct{ svc *Service }

var _ node.Application = (*MovePurge)(nil)

// NewMovePurge purges the service's networks that moved away.
func NewMovePurge(svc *Service) *MovePurge { return &MovePurge{svc: svc} }

func (*MovePurge) Name() string                 { return "ok/scm/move-purge" }
func (n *MovePurge) Dependencies() []node.Node  { return []node.Node{n.svc} }
func (*MovePurge) Setup(context.Context) error  { return nil }
func (*MovePurge) Healthy(context.Context) bool { return true }

func (n *MovePurge) Start(ctx context.Context) error {
	s := n.svc.store
	return (&work.Runner{
		Kind:    store.MovePurgeWork,
		Store:   s.Work(),
		Do:      s.PurgeMoved,
		Workers: 1, // a network's rows and objects, a bounded transaction at a time
	}).Run(ctx)
}

// aboutRepository is keyed work about a repository, its writes passing its network's gate
// (move.WithRepository): refused while the network moves, and so tried again later; and done, as
// the work runner takes it, once it moved to another cell, which does its own.
func aboutRepository(do func(context.Context, int64) error) func(context.Context, int64) error {
	return func(ctx context.Context, repo int64) error { return do(move.WithRepository(ctx, repo), repo) }
}

// GC is garbage collection's node, a node.Application. Every replica runs it, each repository's
// collection keyed work (store.GCWork) done by one replica at a time: booked as the repository is
// made, and by each run for the next — its next daily pass, a failed one's retry, or the end of its
// retired packs' and segments' grace, whichever comes first.
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
	s := n.svc.store
	go func() {
		if err := work.Once(ctx, s.Work(), scheduleWork, n.schedule, slog.Default()); err != nil && ctx.Err() == nil {
			slog.Warn("gc: booking the repositories' collections and purges", "err", err)
		}
	}()
	return (&work.Runner{
		Kind:    store.GCWork,
		Store:   s.Work(),
		Do:      aboutRepository(n.repository),
		Workers: 1, // a pass reads and rewrites a repository's packs: one at a time on a replica
	}).Run(ctx)
}

// scheduleWork books, once, what a release that looked for its keyed work left unbooked
// (schedule).
const scheduleWork = "schedule"

// schedule books every repository's collection, now for one due, and its purge's look: now for one
// the directory says has expired, and a retention period on for every other — whose deletion, if it
// came before the SCM followed the directory's events, it heard nothing of.
func (n *GC) schedule(ctx context.Context) error {
	s, now := n.svc.store, time.Now()
	w := s.Work()
	var after int64
	for {
		repos, err := s.RepositoryIDs(ctx, after, 1000)
		if err != nil {
			return err
		}
		for _, repo := range repos {
			at := store.NextGCSlot(now, n.hour)
			if due, err := s.GCDue(ctx, repo, now, n.hour); err != nil {
				return err
			} else if due {
				at = now
			}
			if err := w.MarkAt(ctx, store.GCWork, repo, at); err != nil {
				return err
			}
			if err := w.MarkAt(ctx, store.PurgeWork, repo, now.Add(dirfeed.RetentionPeriod+time.Minute)); err != nil {
				return err
			}
		}
		if len(repos) < 1000 {
			break
		}
		after = repos[len(repos)-1]
	}
	resp, err := n.svc.ten.ListExpiredRepositories(ctx, &identitypb.ListExpiredRepositoriesRequest{Limit: 1000})
	if err != nil {
		return fmt.Errorf("listing expired repositories: %w", err)
	}
	for _, e := range resp.GetRepositories() {
		if err := w.Mark(ctx, store.PurgeWork, e.GetRepositoryId()); err != nil {
			return err
		}
	}
	return nil
}

// repository collects one repository: its daily pass, if it is due one — recorded, and a failed
// one tried again an hour later (store.GCRetry) — and otherwise its retired packs past their
// grace; then books its next run.
func (n *GC) repository(ctx context.Context, repo int64) error {
	s, now := n.svc.store, time.Now()
	due, err := s.GCDue(ctx, repo, now, n.hour)
	if err != nil {
		return err
	}
	if !due {
		packs, segments, err := s.DeleteRetiredOf(ctx, repo, now)
		if packs+segments > 0 {
			slog.Info("gc: deleted retired packs and segments", "repository", repo, "packs", packs, "segments", segments)
		}
		if err != nil {
			return err
		}
	} else {
		report, err := s.GC(ctx, repo, now, store.GCOptions{})
		if report != nil {
			if rerr := s.RecordGC(ctx, report, err); rerr != nil {
				return errors.Join(err, rerr)
			}
		}
		if err != nil {
			slog.Error("gc: pass failed", "repository", repo, "err", err) // recorded: due again after GCRetry
		} else {
			slog.Info("gc: pass", "report", report.String())
		}
	}
	return n.book(ctx, repo)
}

// book books a repository's next collection: the soonest of its next daily pass, a failed pass's
// retry, and its retired packs' and segments' grace's end. A repository no longer here is booked
// no more.
func (n *GC) book(ctx context.Context, repo int64) error {
	s, now := n.svc.store, time.Now()
	next := store.NextGCSlot(now, n.hour)
	if retry := now.Add(store.GCRetry); retry.Before(next) {
		// A pass due by then is a failed one's retry: a successful one is not due before the slot.
		if due, err := s.GCDue(ctx, repo, retry, n.hour); err != nil {
			return err
		} else if due {
			next = retry
		}
	}
	retired, err := s.RetiredDue(ctx, repo)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !retired.IsZero() && retired.Before(next) {
		next = retired
		if next.Before(now) {
			next = now
		}
	}
	return s.Work().MarkAt(ctx, store.GCWork, repo, next)
}

// Segments is compaction's node, a node.Application: every replica runs it, each repository's
// compaction keyed work (store.SegmentWork), marked when something is pinned, done by one replica
// at a time (docs/hosted.md, "Segments, as built").
type Segments struct {
	svc *Service
	min int
}

var _ node.Application = (*Segments)(nil)

// NewSegments compacts the service's store, once at least min pinned rows wait in a repository's
// tail (0: store.DefaultSegmentMin).
func NewSegments(svc *Service, min int) *Segments {
	if min <= 0 {
		min = store.DefaultSegmentMin
	}
	return &Segments{svc: svc, min: min}
}

func (*Segments) Name() string                 { return "ok/scm/segments" }
func (n *Segments) Dependencies() []node.Node  { return []node.Node{n.svc} }
func (*Segments) Setup(context.Context) error  { return nil }
func (*Segments) Healthy(context.Context) bool { return true }

func (n *Segments) Start(ctx context.Context) error {
	s := n.svc.store
	return (&work.Runner{
		Kind:    store.SegmentWork,
		Store:   s.Work(),
		Do:      aboutRepository(n.repository),
		Workers: 2, // each writes segments of up to a batch of rows
		// A full compaction waits for this one to let go (store.Claims).
		LetGo: s.Claims().LetGo(store.SegmentWork),
	}).Run(ctx)
}

// repository compacts one repository's tail into segments, and goes on at once while more waits
// than one compaction moves.
func (n *Segments) repository(ctx context.Context, repo int64) error {
	s := n.svc.store
	r, err := s.CompactSegments(ctx, repo, n.min)
	if err != nil {
		return err
	}
	if r.Written > 0 || r.Merged > 0 {
		slog.Info("segments: compacted", "repository", repo, "objects", r.Objects, "tree_sizes", r.TreeSizes,
			"copy_edges", r.CopyEdges, "commits", r.Commits, "paths", r.Paths, "events", r.Events,
			"written", r.Written, "merged", r.Merged)
	}
	if r.More {
		return s.Work().Mark(ctx, store.SegmentWork, repo)
	}
	return nil
}

// Folds is the content indexes' folds' node, a node.Application: every replica runs it, each
// repository's and network's fold keyed work (store.FoldDueWork, store.FoldNetworkDueWork) —
// marked by a replica that found the index's tail long — done by one replica at a time.
type Folds struct{ svc *Service }

var _ node.Application = (*Folds)(nil)

// NewFolds folds the service's store's content indexes as their tails grow long.
func NewFolds(svc *Service) *Folds { return &Folds{svc: svc} }

func (*Folds) Name() string                 { return "ok/scm/folds" }
func (n *Folds) Dependencies() []node.Node  { return []node.Node{n.svc} }
func (*Folds) Setup(context.Context) error  { return nil }
func (*Folds) Healthy(context.Context) bool { return true }

func (n *Folds) Start(ctx context.Context) error {
	s := n.svc.store
	errs := make(chan error, 2)
	for _, kind := range []string{store.FoldDueWork, store.FoldNetworkDueWork} {
		go func() {
			errs <- (&work.Runner{
				Kind:  kind,
				Store: s.Work(),
				Do: func(ctx context.Context, id int64) error {
					// Tagged with what it writes about: a network that moves is not folded here.
					if kind == store.FoldNetworkDueWork {
						return s.FoldDue(move.WithNetwork(ctx, id), kind, id)
					}
					return s.FoldDue(move.WithRepository(ctx, id), kind, id)
				},
				Workers: 2,
			}).Run(ctx)
		}()
	}
	return errors.Join(<-errs, <-errs)
}

// Relay is the relay's node, a node.Application: every replica runs one, each relaying pending
// events beside the others (store.Relay).
type Relay struct {
	svc *Service
	q   bus.Queue
}

var _ node.Application = (*Relay)(nil)

// NewRelay relays the service's store's events to their followers' queues on q.
func NewRelay(svc *Service, q bus.Queue) *Relay { return &Relay{svc: svc, q: q} }

func (*Relay) Name() string                 { return "ok/scm/relay" }
func (n *Relay) Dependencies() []node.Node  { return []node.Node{n.svc} }
func (*Relay) Setup(context.Context) error  { return nil }
func (*Relay) Healthy(context.Context) bool { return true }

func (n *Relay) Start(ctx context.Context) error {
	if err := n.svc.store.Relay(ctx, n.q); ctx.Err() == nil {
		return err
	}
	return nil
}

// Checkouts derives the checkout of each repository's main as it moves (store.CheckoutWork), a
// node.Application: every replica runs it, each repository's by one at a time.
type Checkouts struct{ svc *Service }

var _ node.Application = (*Checkouts)(nil)

// NewCheckouts derives the service's repositories' checkouts.
func NewCheckouts(svc *Service) *Checkouts { return &Checkouts{svc: svc} }

func (*Checkouts) Name() string                 { return "ok/scm/checkouts" }
func (n *Checkouts) Dependencies() []node.Node  { return []node.Node{n.svc} }
func (*Checkouts) Setup(context.Context) error  { return nil }
func (*Checkouts) Healthy(context.Context) bool { return true }

func (n *Checkouts) Start(ctx context.Context) error {
	s := n.svc.store
	return (&work.Runner{
		Kind:    store.CheckoutWork,
		Store:   s.Work(),
		Do:      s.DeriveMainCheckout,
		Workers: 4,
	}).Run(ctx)
}

// CodeSearch indexes repositories' main as it moves (store.CodeSearchWork), a node.Application:
// every replica runs it, each repository's index built by one at a time.
type CodeSearch struct{ svc *Service }

var _ node.Application = (*CodeSearch)(nil)

// NewCodeSearch indexes the service's repositories.
func NewCodeSearch(svc *Service) *CodeSearch { return &CodeSearch{svc: svc} }

func (*CodeSearch) Name() string                 { return "ok/scm/codesearch" }
func (n *CodeSearch) Dependencies() []node.Node  { return []node.Node{n.svc} }
func (*CodeSearch) Setup(context.Context) error  { return nil }
func (*CodeSearch) Healthy(context.Context) bool { return true }

func (n *CodeSearch) Start(ctx context.Context) error {
	s := n.svc.store
	return (&work.Runner{
		Kind:    store.CodeSearchWork,
		Store:   s.Work(),
		Do:      s.BuildCodeIndex,
		Workers: 2, // an index's build reads all of main
	}).Run(ctx)
}
