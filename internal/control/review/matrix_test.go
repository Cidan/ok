package review

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/authz/authztest"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/tenancy"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// matrixAccess is Access over the permission matrix: its repository at one visibility, its callers'
// facts as ok identity answers them.
type matrixAccess struct{ v tenancy.Visibility }

func (m matrixAccess) Decide(_ context.Context, caller string, token *authz.Token, repo int64, a authz.Action) (authz.Decision, error) {
	f := authz.Facts{Repository: authztest.Repository(m.v)}
	for _, c := range authztest.Callers {
		if c.Caller.ID == caller {
			f.Caller = c.Caller
		}
	}
	f.Caller.Token = token
	return authz.Decide(f, a), nil
}

func (matrixAccess) Admins(context.Context, int64) ([]string, error) { return nil, nil }
func (matrixAccess) Forget(int64)                                    {}

// ok control's operations hold to the permission matrix: for every kind of caller — a personal or
// installation token presented with the request — at every visibility, each action is required as
// Authorize allows it, and a CL's lifecycle, submitting, its checks, and deployments are each
// allowed exactly as the actions they need are.
func TestAccessMatrix(t *testing.T) {
	repo := &scmpb.Repository{Id: authztest.RepositoryID}
	for v := tenancy.Private; v <= tenancy.Public; v++ {
		s := &Service{access: matrixAccess{v}}
		for _, c := range authztest.Callers {
			who := c.Caller.ID
			ctx := context.Background()
			if who != "" {
				ctx = authn.NewContext(ctx, authn.Caller{Principal: c.Name, ID: who, Human: true, Token: c.Caller.Token})
			}
			may := func(a authz.Action) bool { return authztest.Allowed(c, v, a) }
			for _, a := range authz.Actions {
				if got := s.requireAction(ctx, repo, who, a) == nil; got != may(a) {
					t.Errorf("%s, %s, %s: allowed %v, want %v", c.Name, v, a, got, may(a))
				}
			}
			own, theirs := db.ChangeList{Repo: authztest.RepositoryID, Number: 1, Author: who}, db.ChangeList{Repo: authztest.RepositoryID, Number: 2, Author: "prn_other"}
			signedIn := who != ""
			for name, tc := range map[string]struct{ got, want bool }{
				"managing their own CL":         {s.CanManage(ctx, repo, own, who), signedIn && (may(authz.Propose) || may(authz.Triage))},
				"managing another's CL":         {s.CanManage(ctx, repo, theirs, who), signedIn && may(authz.Triage)},
				"submitting their own CL":       {s.CanSubmit(ctx, repo, own, who), signedIn && may(authz.Land)},
				"submitting another's CL":       {s.CanSubmit(ctx, repo, theirs, who), may(authz.Land) && may(authz.Administer)},
				"canceling a CL's checks":       {s.MayCancelRuns(ctx, repo, theirs, who) == nil, may(authz.Land)},
				"retrying another's deployment": {s.MayActOnDeployment(ctx, repo, who, DeploymentRetry, false) == nil, may(authz.Administer) || may(authz.Maintain)},
				"halting their own deployment":  {s.MayActOnDeployment(ctx, repo, who, DeploymentHalt, true) == nil, may(authz.Administer) || may(authz.Maintain) || may(authz.Land)},
				"allowing a deployment":         {s.MayActOnDeployment(ctx, repo, who, DeploymentAllow, true) == nil, may(authz.Administer)},
				"administering":                 {s.IsAdmin(ctx, repo, who), signedIn && may(authz.Administer)},
			} {
				if tc.got != tc.want {
					t.Errorf("%s, %s, %s: %v, want %v", c.Name, v, name, tc.got, tc.want)
				}
			}
		}
	}
}
