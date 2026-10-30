package review

import (
	"context"
	"log/slog"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/serve/authn"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// What a person may do in ok control is Authorize's to decide (docs/hosted.md, "Tenancy, as
// built", "Roles and actions"): ok control calls the SCM as itself, so it decides for each person
// before it acts for them. A CL's lifecycle is its author's, or a triager's; landing, rerunning
// checks, and acting on deployments need write; allowing a migration's hazards, admin.

// Access decides what a person may do to a repository, and says who administers one (NewAccess).
type Access interface {
	Decide(ctx context.Context, caller string, token *authz.Token, repo int64, a authz.Action) (authz.Decision, error)
	// Admins are the active people who administer a repository, by stable ID.
	Admins(ctx context.Context, repo int64) ([]string, error)
}

// NewAccess is Access over ok identity's Authorization, asked each time (authz.Checker).
func NewAccess(client identitypb.AuthorizationClient) Access {
	return &access{Checker: authz.NewChecker(client), client: client}
}

type access struct {
	*authz.Checker
	client identitypb.AuthorizationClient
}

// Admins are ok identity's answer now.
func (a *access) Admins(ctx context.Context, repo int64) ([]string, error) {
	resp, err := a.client.Admins(ctx, &identitypb.AdminsRequest{RepositoryId: repo})
	if err != nil {
		return nil, err
	}
	return resp.GetPrincipalIds(), nil
}

// Decide is Authorize's decision on principal (by stable ID; "" for no one signed in) doing an
// action to repo. When ok identity cannot say, they may not: the answer gates actions.
func (s *Service) Decide(ctx context.Context, repo *scmpb.Repository, principal string, a authz.Action) authz.Decision {
	// A personal token the person presented narrows what they may do.
	var token *authz.Token
	if c, ok := authn.FromContext(ctx); ok && c.ID == principal {
		token = c.Token
	}
	d, err := s.access.Decide(ctx, principal, token, repo.GetId(), a)
	if err != nil {
		slog.Warn("review: asking ok identity what someone may do", "principal", principal, "repository", repo.GetId(), "action", a, "err", err)
		return authz.Decision{Why: "ok could not say whether you may: try again in a moment"}
	}
	return d
}

// May reports whether principal may do an action to repo.
func (s *Service) May(ctx context.Context, repo *scmpb.Repository, principal string, a authz.Action) bool {
	return s.Decide(ctx, repo, principal, a).Allowed
}

// IsAdmin reports whether principal administers the repository.
func (s *Service) IsAdmin(ctx context.Context, repo *scmpb.Repository, principal string) bool {
	return principal != "" && s.May(ctx, repo, principal, authz.Administer)
}

// CanManage reports whether principal may drive a CL's lifecycle — Request Review, Return to
// Draft, Abandon, Restore, its reviewers: its author, while they may propose, or a triager.
func (s *Service) CanManage(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string) bool {
	if principal == "" {
		return false
	}
	if principal == cl.Author && s.May(ctx, repo, principal, authz.Propose) {
		return true
	}
	return s.May(ctx, repo, principal, authz.Triage)
}

// CanSubmit reports whether principal may submit a CL to land: its author or an admin, who may
// land on the repository (write).
func (s *Service) CanSubmit(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string) bool {
	if principal == "" || !s.May(ctx, repo, principal, authz.Land) {
		return false
	}
	return principal == cl.Author || s.IsAdmin(ctx, repo, principal)
}

// requireAction refuses a person Authorize does not let do an action, saying why.
func (s *Service) requireAction(ctx context.Context, repo *scmpb.Repository, principal string, a authz.Action) error {
	if d := s.Decide(ctx, repo, principal, a); !d.Allowed {
		return refuse("%s", upperFirst(d.Why))
	}
	return nil
}
