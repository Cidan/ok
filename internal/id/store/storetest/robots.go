package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// An organization's robots: named under it, unique; none for the installation's organization;
// granted its own repositories alone; deleted by it alone, their names retired.
func testOrganizationRobots(t *testing.T, s *store.Store) {
	ctx := context.Background()
	src := &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true)}}}
	sync(t, s, src)
	ada, _ := s.Principal(ctx, "ada.human.example.com")
	inst, err := s.EnsureOrganization(ctx, "example", store.OrganizationSettings{BaseRole: tenancy.Write, MembersCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	s.SetInstallation(inst.ID)
	acme, err := s.CreateOrganization(ctx, "acme", ada.ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}
	globex, err := s.CreateOrganization(ctx, "globex", ada.ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}

	ci, err := s.CreateOrganizationRobot(ctx, acme.ID, "ci")
	if err != nil || ci.Name != "ci.acme.robot.example.com" || ci.Kind != store.Robot || !ci.Active() {
		t.Fatalf("CreateOrganizationRobot = %+v, %v", ci, err)
	}
	if _, err := s.CreateOrganizationRobot(ctx, acme.ID, "ci"); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a robot's name twice: %v", err)
	}
	for name, org := range map[string]string{"Bad": acme.ID, "a.b": acme.ID} {
		if _, err := s.CreateOrganizationRobot(ctx, org, name); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("robot %q: %v", name, err)
		}
	}
	if _, err := s.CreateOrganizationRobot(ctx, inst.ID, "ci"); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a robot of the installation's organization: %v", err)
	}
	if of, err := s.RobotOrganization(ctx, ci.ID); err != nil || of != acme.ID {
		t.Errorf("RobotOrganization = %q, %v", of, err)
	}
	installed, err := s.CreateRobot(ctx, "deploy.robot.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RobotOrganization(ctx, installed.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("an installation robot's organization: %v", err)
	}
	if rs, err := s.OrganizationRobots(ctx, acme.ID); err != nil || len(rs) != 1 || rs[0].ID != ci.ID {
		t.Errorf("OrganizationRobots = %+v, %v", rs, err)
	}

	// Granted its own organization's repositories alone.
	own, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: "app", Propose: tenancy.Read, CreatedBy: ada.ID})
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.ReserveRepository(ctx, store.NewRepository{Owner: globex.ID, Name: "app", Propose: tenancy.Read, CreatedBy: ada.ID})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetGrant(ctx, store.Grant{Repository: own.ID, Subject: ci.ID, Role: tenancy.Write}); err != nil {
		t.Errorf("granting acme's robot acme's repository: %v", err)
	}
	if err := s.SetGrant(ctx, store.Grant{Repository: other.ID, Subject: ci.ID, Role: tenancy.Read}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("granting acme's robot globex's repository: %v", err)
	}
	if err := s.SetGrant(ctx, store.Grant{Repository: other.ID, Subject: installed.ID, Role: tenancy.Read}); err != nil {
		t.Errorf("granting an installation robot: %v", err)
	}

	// Federation: the issuers it trusts, and its robots' bindings to them alone; the same subject may
	// name a robot in each organization; distrusting an issuer unbinds its robots from it.
	const gh = "https://token.actions.githubusercontent.com"
	sub := "repo:acme/app:ref:refs/heads/main"
	for _, bad := range []string{"token.actions.githubusercontent.com", "https://", "https://u:p@issuer.example", "https://issuer.example/?a=b", "ftp://issuer.example"} {
		if err := s.TrustIssuer(ctx, acme.ID, bad); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("trusting %q: %v", bad, err)
		}
	}
	if err := s.TrustIssuer(ctx, inst.ID, gh); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("the installation trusting an issuer: %v", err)
	}
	if err := s.BindOrganizationRobot(ctx, acme.ID, ci.ID, gh, sub); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("binding to an issuer it trusts not: %v", err)
	}
	for _, org := range []string{acme.ID, acme.ID, globex.ID} {
		if err := s.TrustIssuer(ctx, org, gh); err != nil {
			t.Fatal(err)
		}
	}
	if is, err := s.Issuers(ctx, acme.ID); err != nil || len(is) != 1 || is[0] != gh {
		t.Errorf("Issuers = %v, %v", is, err)
	}
	if err := s.BindOrganizationRobot(ctx, acme.ID, ci.ID, gh, sub); err != nil {
		t.Fatal(err)
	}
	gci, err := s.CreateOrganizationRobot(ctx, globex.ID, "ci")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BindOrganizationRobot(ctx, globex.ID, gci.ID, gh, sub); err != nil {
		t.Errorf("the same subject for another organization's robot: %v", err)
	}
	if err := s.BindOrganizationRobot(ctx, globex.ID, ci.ID, gh, sub+"x"); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("globex binding acme's robot: %v", err)
	}
	if err := s.BindOrganizationRobot(ctx, acme.ID, ci.ID, gh, sub); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a binding twice: %v", err)
	}
	for org, want := range map[string]string{acme.ID: ci.ID, globex.ID: gci.ID} {
		if p, err := s.BoundRobot(ctx, store.Binding{Organization: org, Issuer: gh, Subject: sub}); err != nil || p.ID != want {
			t.Errorf("the robot %s bound = %+v, %v", org, p, err)
		}
	}
	if _, err := s.BoundRobot(ctx, store.Binding{Issuer: gh, Subject: sub}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("an organization's binding, as the installation's: %v", err)
	}
	if err := s.Bind(ctx, ci.Name, store.Binding{Issuer: gh, Subject: sub}); err == nil {
		t.Error("an organization's robot bound to the installation's issuers")
	}
	if ok, err := s.DistrustIssuer(ctx, globex.ID, gh); err != nil || !ok {
		t.Fatalf("DistrustIssuer = %v, %v", ok, err)
	}
	if _, err := s.BoundRobot(ctx, store.Binding{Organization: globex.ID, Issuer: gh, Subject: sub}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a binding to an issuer distrusted: %v", err)
	}
	if bs, err := s.OrganizationBindings(ctx, acme.ID); err != nil || len(bs[ci.ID]) != 1 {
		t.Errorf("acme's bindings, after globex distrusted the issuer = %v, %v", bs, err)
	}
	if ok, err := s.UnbindOrganizationRobot(ctx, acme.ID, ci.ID, gh, sub); err != nil || !ok {
		t.Errorf("UnbindOrganizationRobot = %v, %v", ok, err)
	}

	// Deleted by its organization alone: its name retired.
	if err := s.DeleteOrganizationRobot(ctx, globex.ID, ci.ID); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("globex deleting acme's robot: %v", err)
	}
	if err := s.DeleteOrganizationRobot(ctx, acme.ID, ci.ID); err != nil {
		t.Fatal(err)
	}
	if p, err := s.PrincipalByID(ctx, ci.ID); err != nil || p.Active() {
		t.Errorf("the deleted robot = %+v, %v", p, err)
	}
	if _, err := s.CreateOrganizationRobot(ctx, acme.ID, "ci"); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a deleted robot's name again: %v", err)
	}
}
