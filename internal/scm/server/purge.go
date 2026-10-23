package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Cidan/ok/internal/scm/store"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/work"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// purgeInterval is how often the purge node asks the directory what has expired.
const purgeInterval = 5 * time.Minute

// Purge removes expired repositories (docs/hosted.md, "Deleting, renaming, transferring"): those
// deleted more than 30 days ago, and those still being made after an hour. For each, as the
// directory's clock says, it deletes the repository's storage, then has the directory remove the
// entry — a deleted one's purge logged, for ok control, ok automation, and the deployer to delete
// what they hold. Every replica runs it, each repository's purge keyed work (store.PurgeWork) done
// by one replica at a time; one interrupted is done again from the start, each step taking what
// is gone already as done.
type Purge struct{ svc *Service }

var _ node.Application = (*Purge)(nil)

// NewPurge purges the service's expired repositories.
func NewPurge(svc *Service) *Purge { return &Purge{svc: svc} }

func (*Purge) Name() string                 { return "ok/scm/purge" }
func (n *Purge) Dependencies() []node.Node  { return []node.Node{n.svc} }
func (*Purge) Setup(context.Context) error  { return nil }
func (*Purge) Healthy(context.Context) bool { return true }

func (n *Purge) Start(ctx context.Context) error {
	return (&work.Runner{
		Kind:    store.PurgeWork,
		Store:   n.svc.store.Work(),
		Do:      aboutRepository(n.repository),
		Sweep:   n.sweep,
		Idle:    purgeInterval,
		Workers: 1, // a deletion releases every chunk the repository held: one at a time on a replica
	}).Run(ctx)
}

// sweep marks every repository the directory says has expired.
func (n *Purge) sweep(ctx context.Context) error {
	resp, err := n.svc.ten.ListExpiredRepositories(ctx, &identitypb.ListExpiredRepositoriesRequest{})
	if err != nil {
		return fmt.Errorf("listing expired repositories: %w", err)
	}
	w := n.svc.store.Work()
	for _, e := range resp.GetRepositories() {
		if err := w.Mark(ctx, store.PurgeWork, e.GetRepositoryId()); err != nil {
			return err
		}
	}
	return nil
}

// repository purges one repository, if the directory says now that it has expired: its storage,
// then its entry.
func (n *Purge) repository(ctx context.Context, repo int64) error {
	e, err := n.svc.ten.GetExpiry(ctx, &identitypb.RepositoryRef{Id: repo})
	if err != nil {
		return fmt.Errorf("repository %d's expiry: %w", repo, err)
	}
	if e.GetKind() != identitypb.Expiry_KIND_DELETED && e.GetKind() != identitypb.Expiry_KIND_CREATING {
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
