package service_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// ok identity answers the facts Authorize decides from, and they decide as the matrix says: an
// organization's owner, its members by its base role, a team's grant through the team, a synced
// group's grant, an outside collaborator, the enterprise reading what is internal, a person's own
// repository, a suspended caller, no one signed in, and only ok's roles asking.
func TestAuthorization(t *testing.T) {
	ctx := context.Background()
	e := newEnvWith(t, true)
	id := func(name string) string {
		t.Helper()
		p, err := e.store.Principal(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		return p.ID
	}
	adaID, beaID, calID := id(ada), id(bea), id("cal.human.example.com")
	acme, err := e.store.CreateOrganization(ctx, "acme", adaID, store.OrganizationSettings{BaseRole: tenancy.Read, Enterprise: "e1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.SetMember(ctx, acme.ID, beaID, tenancy.Member); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.CreateOrganization(ctx, "other", calID, store.OrganizationSettings{BaseRole: tenancy.None, Enterprise: "e1"}); err != nil {
		t.Fatal(err)
	}
	web, err := e.store.CreateTeam(ctx, acme.ID, "web", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.AddTeamMember(ctx, web.Name, beaID); err != nil {
		t.Fatal(err)
	}
	reserve := func(owner, name string, v tenancy.Visibility) store.Repository {
		t.Helper()
		r, err := e.store.ReserveRepository(ctx, store.NewRepository{Owner: owner, Name: name, Visibility: v, Propose: tenancy.Read, CreatedBy: adaID})
		if err != nil {
			t.Fatal(err)
		}
		if r, err = e.store.ActivateRepository(ctx, r.ID); err != nil {
			t.Fatal(err)
		}
		return r
	}
	app := reserve(acme.ID, "app", tenancy.Internal)
	for _, g := range []store.Grant{{Repository: app.ID, Subject: web.Name, Role: tenancy.Maintain}} {
		if err := e.store.SetGrant(ctx, g); err != nil {
			t.Fatal(err)
		}
	}
	lib := reserve(acme.ID, "lib", tenancy.Private)
	if err := e.store.SetGrant(ctx, store.Grant{Repository: lib.ID, Subject: "it.group.example.com", Role: tenancy.Triage}); err != nil {
		t.Fatal(err)
	}
	mine := reserve(beaID, "notes", tenancy.Private)
	if err := e.store.SetGrant(ctx, store.Grant{Repository: mine.ID, Subject: calID, Role: tenancy.Write}); err != nil {
		t.Fatal(err)
	}
	creating, err := e.store.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: "fresh", Propose: tenancy.Read, CreatedBy: adaID})
	if err != nil {
		t.Fatal(err)
	}

	checker := authz.NewChecker(identitypb.NewAuthorizationClient(e.as(control)))
	for _, tc := range []struct {
		name   string
		caller string
		repo   int64
		role   tenancy.Role
	}{
		{"acme's owner on app", adaID, app.ID, tenancy.Admin},
		{"a member of web, granted maintain on app", beaID, app.ID, tenancy.Maintain},
		{"a member of acme on lib, by its base role", beaID, lib.ID, tenancy.Read},
		{"cal on app, internal to acme's enterprise", calID, app.ID, tenancy.Read},
		{"cal on lib, through the it group's grant", calID, lib.ID, tenancy.Triage},
		{"bea on her own repository", beaID, mine.ID, tenancy.Admin},
		{"cal, a collaborator on bea's", calID, mine.ID, tenancy.Write},
		{"ada, a stranger to bea's", adaID, mine.ID, tenancy.None},
		{"no one signed in, on app", "", app.ID, tenancy.None},
		{"an ID no one has", "prn_nobody", app.ID, tenancy.None},
	} {
		f, err := checker.Facts(ctx, tc.caller, tc.repo)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got := authz.RoleOf(f); got != tc.role {
			t.Errorf("%s: %s, want %s (%+v)", tc.name, got, tc.role, f)
		}
	}
	if d, err := checker.Decide(ctx, adaID, nil, creating.ID, authz.ReadCode); err != nil || d.Visible {
		t.Errorf("a repository being created: %+v, %v", d, err)
	}

	// A deleted repository is its admins' alone, to restore.
	if _, err := e.store.DeleteRepository(ctx, app.ID); err != nil {
		t.Fatal(err)
	}
	checker.Forget(app.ID)
	if d, err := checker.Decide(ctx, adaID, nil, app.ID, authz.Restore); err != nil || !d.Allowed {
		t.Errorf("the owner restoring a deleted repository: %+v, %v", d, err)
	}
	if d, err := checker.Decide(ctx, beaID, nil, app.ID, authz.ReadCode); err != nil || d.Visible {
		t.Errorf("a maintainer reading a deleted repository: %+v, %v", d, err)
	}

	// Only ok's roles ask.
	if _, err := identitypb.NewAuthorizationClient(e.as(ada)).Facts(ctx, &identitypb.FactsRequest{RepositoryId: lib.ID}); code(err) != codes.PermissionDenied {
		t.Errorf("a person asking for facts: %v", err)
	}
}
