package service

import (
	"context"
	"log/slog"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// GitHub's rules (docs/hosted.md, "GitHub's rules"): removing someone's access to a private
// repository deletes their forks of it, except forks an organization holds. After each change
// that may take access away — a grant revoked, a member or a team's member removed, a team
// deleted, an organization's base role changed, a repository transferred — the people's forks in
// the networks it touches are checked against their fork trees' roots, and each whose owner cannot
// read its root now is deleted (restorable for the retention period, as any deletion is). The
// change itself stands whatever the checking finds; a failure to check is logged.

// pruneForks deletes the people's forks in networks whose owners can no longer read their roots.
func (s *Server) pruneForks(ctx context.Context, networks []int64) {
	if len(networks) == 0 {
		return
	}
	forks, err := s.cfg.Store.PersonalForksIn(ctx, networks)
	if err != nil {
		slog.Error("identity: finding the forks to check after an access change", "err", err)
		return
	}
	for _, f := range forks {
		if err := s.pruneFork(ctx, f); err != nil {
			slog.Error("identity: checking a fork after an access change", "repository", f.ID, "err", err)
		}
	}
}

func (s *Server) pruneFork(ctx context.Context, f store.Repository) error {
	root, err := s.root(ctx, f)
	if err != nil || root.ID == f.ID || root.Visibility == tenancy.Public || root.Owner == f.Owner {
		return err
	}
	facts, err := s.facts(ctx, f.Owner, root)
	if err != nil {
		return err
	}
	if authz.Decide(facts, authz.ReadCode).Allowed {
		return nil
	}
	if _, err := s.cfg.Store.DeleteRepository(ctx, f.ID); err != nil {
		return err
	}
	s.audit(ctx, "", "repository.delete", map[string]any{"repository": f.ID, "why": "its owner can no longer read the repository it forks", "root": root.ID})
	return nil
}

// pruneOrganizationForks prunes the forks of an organization's repositories.
func (s *Server) pruneOrganizationForks(ctx context.Context, org string) {
	repos, err := s.cfg.Store.RepositoriesOf(ctx, org)
	if err != nil {
		slog.Error("identity: finding an organization's repositories to check their forks", "organization", org, "err", err)
		return
	}
	seen := map[int64]bool{}
	var networks []int64
	for _, r := range repos {
		if r.Visibility != tenancy.Public && !seen[r.Network] {
			seen[r.Network] = true
			networks = append(networks, r.Network)
		}
	}
	s.pruneForks(ctx, networks)
}

// pruneRepositoryForks prunes the forks in a repository's network.
func (s *Server) pruneRepositoryForks(ctx context.Context, repo int64) {
	r, err := s.cfg.Store.Repository(ctx, repo)
	if err != nil {
		slog.Error("identity: finding a repository to check its forks", "repository", repo, "err", err)
		return
	}
	s.pruneForks(ctx, []int64{r.Network})
}
