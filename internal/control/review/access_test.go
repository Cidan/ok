package review

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// roles is an Access over Authorize itself: repository 1 is an organization's, private, its
// people granted the roles listed; anyone else is a stranger.
type roles struct {
	granted map[string]tenancy.Role
	down    bool
}

func (r roles) Decide(_ context.Context, caller string, token *authz.Token, repo int64, a authz.Action) (authz.Decision, error) {
	if r.down {
		return authz.Decision{}, errors.New("ok identity is unreachable")
	}
	f := authz.Facts{Repository: authz.Repository{ID: repo, Visibility: tenancy.Private, Propose: tenancy.Read, Owner: "prn_org", OwnedByOrg: true}}
	if caller != "" {
		f.Caller = authz.Caller{ID: caller, Active: true, Token: token}
		if g, ok := r.granted[caller]; ok {
			f.Caller.Granted = []tenancy.Role{g}
		}
	}
	return authz.Decide(f, a), nil
}

func (roles) Forget(int64) {}

func (r roles) Admins(context.Context, int64) ([]string, error) {
	var out []string
	for who, g := range r.granted {
		if g == tenancy.Admin {
			out = append(out, who)
		}
	}
	return out, nil
}

// Who may do what to a CL, as Authorize's roles say: its author while they may propose, or a
// triager, drives its lifecycle; its author or an admin who may land submits it; whoever may land
// reruns and cancels its checks; admins and maintainers act on any deployment, the author of a CL
// it deploys who may land on theirs, and admins alone allow hazards. A stranger, and anyone when
// ok identity cannot say, may do nothing.
func TestAccess(t *testing.T) {
	ctx := context.Background()
	access := roles{granted: map[string]tenancy.Role{
		"prn_reader": tenancy.Read, "prn_triager": tenancy.Triage, "prn_writer": tenancy.Write,
		"prn_maintainer": tenancy.Maintain, "prn_admin": tenancy.Admin,
	}}
	s := &Service{access: access}
	repo := &scmpb.Repository{Id: 1}
	mine := func(author string) db.ChangeList { return db.ChangeList{Repo: 1, Number: 7, Author: author} }

	for who, want := range map[string][4]bool{ // manage own, manage another's, submit own, submit another's
		"prn_reader":     {true, false, false, false},
		"prn_triager":    {true, true, false, false},
		"prn_writer":     {true, true, true, false},
		"prn_maintainer": {true, true, true, false},
		"prn_admin":      {true, true, true, true},
		"prn_stranger":   {false, false, false, false},
		"":               {false, false, false, false},
	} {
		got := [4]bool{s.CanManage(ctx, repo, mine(who), who), s.CanManage(ctx, repo, mine("prn_other"), who),
			s.CanSubmit(ctx, repo, mine(who), who), s.CanSubmit(ctx, repo, mine("prn_other"), who)}
		if got != want {
			t.Errorf("%q: manage own, another's, submit own, another's = %v, want %v", who, got, want)
		}
	}

	for who, allowed := range map[string]bool{"prn_reader": false, "prn_triager": false, "prn_writer": true, "prn_admin": true} {
		if err := s.MayCancelRuns(ctx, repo, mine("prn_other"), who); (err == nil) != allowed {
			t.Errorf("%s canceling another's checks: %v, want allowed %v", who, err, allowed)
		}
	}
	if err := s.MayCancelRuns(ctx, repo, mine("prn_reader"), "prn_reader"); err == nil || !IsUserError(err) ||
		!strings.Contains(err.Error(), "needs write") {
		t.Errorf("a reader canceling their own CL's checks: %v", err)
	}

	for _, tc := range []struct {
		who    string
		action string
		author bool
		want   bool
	}{
		{"prn_admin", DeploymentAllow, false, true},
		{"prn_maintainer", DeploymentAllow, false, false},
		{"prn_maintainer", DeploymentRetry, false, true},
		{"prn_writer", DeploymentHalt, true, true},
		{"prn_writer", DeploymentHalt, false, false},
		{"prn_reader", DeploymentRetry, true, false},
	} {
		if err := s.MayActOnDeployment(ctx, repo, tc.who, tc.action, tc.author); (err == nil) != tc.want {
			t.Errorf("%s to %s (author %v): %v, want allowed %v", tc.who, tc.action, tc.author, err, tc.want)
		}
	}

	if err := s.requireAction(ctx, repo, "prn_stranger", authz.Propose); err == nil || !strings.Contains(err.Error(), "To propose needs read") {
		t.Errorf("a stranger proposing: %v", err)
	}
	if err := s.requireAction(ctx, repo, "", authz.Propose); err == nil || err.Error() != "To propose, sign in" {
		t.Errorf("no one proposing: %v", err)
	}
	if got := s.Admins(ctx, repo); len(got) != 1 || got[0] != "prn_admin" {
		t.Errorf("Admins = %v", got)
	}

	// When ok identity cannot say, no one may.
	s.access = roles{granted: access.granted, down: true}
	if s.IsAdmin(ctx, repo, "prn_admin") || s.CanSubmit(ctx, repo, mine("prn_admin"), "prn_admin") {
		t.Error("an admin acted while ok identity was unreachable")
	}
	if err := s.requireAction(ctx, repo, "prn_admin", authz.Propose); err == nil || !strings.Contains(err.Error(), "try again") {
		t.Errorf("proposing while ok identity was unreachable: %v", err)
	}
}

// adminsSource is ok identity's Authorization, counting the times it is asked for admins.
type adminsSource struct {
	identitypb.AuthorizationClient
	asked int
}

func (a *adminsSource) Admins(context.Context, *identitypb.AdminsRequest, ...grpc.CallOption) (*identitypb.AdminsResponse, error) {
	a.asked++
	return &identitypb.AdminsResponse{PrincipalIds: []string{"prn_admin"}}, nil
}

// A repository's admins are kept a few seconds, as facts are, and forgotten with them: every
// status a watch reads names them.
func TestAdminsKept(t *testing.T) {
	ctx := context.Background()
	src := &adminsSource{}
	a := NewAccess(src)
	for range 5 {
		if got, err := a.Admins(ctx, 7); err != nil || len(got) != 1 || got[0] != "prn_admin" {
			t.Fatalf("admins: %v, %v", got, err)
		}
	}
	if src.asked != 1 {
		t.Fatalf("five reads asked ok identity %d times, want once", src.asked)
	}
	if _, err := a.Admins(ctx, 8); err != nil || src.asked != 2 {
		t.Fatalf("another repository: asked %d times, %v", src.asked, err)
	}
	a.Forget(7)
	if _, err := a.Admins(ctx, 7); err != nil || src.asked != 3 {
		t.Fatalf("after the directory said it changed: asked %d times, %v", src.asked, err)
	}
}
