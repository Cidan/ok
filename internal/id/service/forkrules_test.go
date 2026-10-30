package service_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/authz"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// GitHub's rules, as Authorize and the directory apply them: a person's fork of an organization's
// private repository is read by the upstream's teams and owners — not by the upstream's other
// members — and once the person loses access to the upstream, their fork is deleted; an
// organization's fork of it stays.
func TestForkRulesAccess(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	if _, err := e.bea.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.UpdateOrganization(ctx, &identitypb.UpdateOrganizationRequest{Handle: "acme", BaseRole: "none", PrivateForks: true}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{e.adaID, e.calID} {
		if _, err := e.bea.SetMember(ctx, &identitypb.SetMemberRequest{Handle: "acme", PrincipalId: id, Role: "member"}); err != nil {
			t.Fatal(err)
		}
	}
	eng, err := e.bea.CreateTeam(ctx, &identitypb.CreateTeamRequest{Handle: "acme", Team: "eng"})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{e.adaID, e.calID} {
		if _, err := e.bea.AddTeamMember(ctx, &identitypb.TeamMemberRequest{Team: eng.Name, PrincipalId: id}); err != nil {
			t.Fatal(err)
		}
	}
	made := func(owner, name, person string, parent int64) *identitypb.RepositoryEntry {
		t.Helper()
		r, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: owner, Name: name, ParentId: parent, ForPersonId: person})
		if err != nil {
			t.Fatal(err)
		}
		if r, err = e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: r.Id}); err != nil {
			t.Fatal(err)
		}
		return r
	}
	app := made("acme", "app", e.beaID, 0)
	if _, err := e.bea.SetGrant(ctx, &identitypb.SetGrantRequest{Id: app.Id, Grant: &identitypb.RepositoryGrant{Subject: eng.Name, Role: "write"}}); err != nil {
		t.Fatal(err)
	}
	fork := made("ada", "app", e.adaID, app.Id)
	if fork.Visibility != "private" || fork.ParentId != app.Id {
		t.Fatalf("ada's fork = %v", fork)
	}
	if _, err := e.bea.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "globex"}); err != nil {
		t.Fatal(err)
	}
	orgFork := made("globex", "app", e.beaID, app.Id)

	checker := authz.NewChecker(identitypb.NewAuthorizationClient(e.as(scm)))
	reads := func(who string, repo int64) bool {
		t.Helper()
		d, err := checker.Decide(ctx, who, nil, repo, authz.ReadCode)
		if err != nil {
			t.Fatal(err)
		}
		return d.Allowed
	}
	if !reads(e.calID, fork.Id) {
		t.Error("the upstream's team's member reading ada's fork")
	}
	if !reads(e.beaID, fork.Id) {
		t.Error("the upstream's owner reading ada's fork")
	}
	if _, err := e.bea.RemoveTeamMember(ctx, &identitypb.TeamMemberRequest{Team: eng.Name, PrincipalId: e.calID}); err != nil {
		t.Fatal(err)
	}
	if reads(e.calID, fork.Id) {
		t.Error("a member of acme in no team reading ada's fork")
	}

	// ada leaves acme: she reads app no more, and her fork goes; globex's stays.
	if _, err := e.bea.RemoveMember(ctx, &identitypb.RemoveMemberRequest{Handle: "acme", PrincipalId: e.adaID}); err != nil {
		t.Fatal(err)
	}
	if got, err := e.scm.GetRepositoryEntry(ctx, &identitypb.RepositoryRef{Id: fork.Id}); err != nil || got.State != "deleted" {
		t.Errorf("ada's fork after she left acme = %v, %v", got, err)
	}
	if got, err := e.scm.GetRepositoryEntry(ctx, &identitypb.RepositoryRef{Id: orgFork.Id}); err != nil || got.State != "active" {
		t.Errorf("globex's fork after ada left acme = %v, %v", got, err)
	}
	// A fork's visibility changes only once it stands alone.
	if _, err := e.bea.UpdateRepository(ctx, &identitypb.UpdateRepositoryRequest{Id: orgFork.Id, Visibility: "public"}); code(err) != codes.FailedPrecondition {
		t.Errorf("a fork's visibility changed: %v", err)
	}
}
