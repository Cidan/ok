package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/scm/store"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/work"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Purge removes expired repositories (docs/hosted.md, "Deleting, renaming, transferring"): those
// deleted more than dirfeed.RetentionPeriod ago, and those still being made after
// dirfeed.CreatingTimeout. Each repository's purge is keyed work (store.PurgeWork), booked for when
// it may expire: by the SCM's transaction that makes the repository, for its making's timeout; and,
// as the SCM follows the directory's events, by a repository's deletion, for its retention's end.
// Its job asks the directory then, by the directory's clock, whether it has expired; if it has, it
// deletes the repository's storage, then has the directory remove the entry — a deleted one's purge
// logged, for ok control, ok automation, and the deployer to delete what they hold. A deletion the
// SCM stopped short of is finished first: the transaction that deleted the repository's row marked
// its purge. Every replica runs it, each repository's purge done by one replica at a time; one
// interrupted is done again from the start, each step taking what is gone already as done.
type Purge struct {
	svc    *Service
	events dirfeed.Events
}

var _ node.Application = (*Purge)(nil)

// NewPurge purges the service's expired repositories, following the directory's events, its logs
// read through events.
func NewPurge(svc *Service, events dirfeed.Events) *Purge {
	return &Purge{svc: svc, events: events}
}

func (*Purge) Name() string                 { return "ok/scm/purge" }
func (n *Purge) Dependencies() []node.Node  { return []node.Node{n.svc} }
func (*Purge) Setup(context.Context) error  { return nil }
func (*Purge) Healthy(context.Context) bool { return true }

func (n *Purge) Start(ctx context.Context) error {
	go n.follow(ctx)
	return (&work.Runner{
		Kind:    store.PurgeWork,
		Store:   n.svc.store.Work(),
		Do:      aboutRepository(n.repository),
		Workers: 1, // a deletion releases every chunk the repository held: one at a time on a replica
	}).Run(ctx)
}

// follow follows the directory's events until ctx ends.
func (n *Purge) follow(ctx context.Context) {
	fl := &follow.Follower{Topic: dirfeed.Topic, Log: dirfeed.Log{Events: n.events}, Apply: directoryApplier{n},
		Work: n.svc.store.Work(), Register: dirfeed.Register(n.events)}
	fl.Run(ctx)
}

// directoryApplier applies the directory's events: a repository's deletion books its purge for its
// retention's end. Booking is idempotent, and a cursor never moves back: replicas applying one
// repository's events at once agree.
type directoryApplier struct{ *Purge }

func (a directoryApplier) Cursors(ctx context.Context, repos []int64) (map[int64]int64, error) {
	return a.svc.store.FollowCursors(move.Untagged(ctx), dirfeed.Topic, repos)
}

func (a directoryApplier) Apply(ctx context.Context, e bus.Event) error {
	// The directory's events are every repository's, wherever it is or moves: no move gates them.
	ctx = move.Untagged(ctx)
	s := a.svc.store
	cursor, err := s.FollowCursor(ctx, dirfeed.Topic, e.Key)
	if err != nil || e.Seq <= cursor {
		return err
	}
	ev, err := dirfeed.Decode(e)
	if err != nil {
		return err
	}
	if ev.GetKind() == identitypb.DirectoryEvent_KIND_DELETED {
		at := ev.GetAt().AsTime().Add(dirfeed.RetentionPeriod + time.Minute)
		if err := s.Work().MarkAt(ctx, store.PurgeWork, e.Key, at); err != nil {
			return fmt.Errorf("booking repository %d's purge: %w", e.Key, err)
		}
	}
	return s.AdvanceFollowCursor(ctx, dirfeed.Topic, e.Key, e.Seq)
}

// repository finishes a deletion of a repository stopped short, and purges the repository if the
// directory says now that it has expired: its storage, then its entry.
func (n *Purge) repository(ctx context.Context, repo int64) error {
	if finished, err := n.svc.store.FinishDeletion(ctx, repo); err != nil {
		return fmt.Errorf("finishing repository %d's deletion: %w", repo, err)
	} else if finished {
		slog.Info("purge: finished a repository's deletion", "repository", repo)
	}
	e, err := n.svc.ten.GetExpiry(ctx, &identitypb.RepositoryRef{Id: repo})
	if err != nil {
		return fmt.Errorf("repository %d's expiry: %w", repo, err)
	}
	if e.GetKind() != identitypb.Expiry_KIND_DELETED && e.GetKind() != identitypb.Expiry_KIND_CREATING {
		// One that will expire is looked at again then: a booking for later than another may have
		// been folded into the sooner, done now.
		if at := e.GetExpiresAt(); at != nil {
			return n.svc.store.Work().MarkAt(ctx, store.PurgeWork, repo, at.AsTime().Add(time.Minute))
		}
		return nil
	}
	if err := n.svc.store.DeleteRepository(ctx, repo); err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("deleting repository %d's storage: %w", repo, err)
	}
	if _, err := n.svc.ten.PurgeRepository(ctx, &identitypb.RepositoryRef{Id: repo}); err != nil {
		return fmt.Errorf("purging repository %d's entry: %w", repo, err)
	}
	slog.Info("purge: removed an expired repository", "repository", repo, "expired", e.GetKind().String())
	return nil
}
