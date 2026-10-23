package service_test

import (
	"context"
	"crypto/x509"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/federation/fedtest"
	"github.com/Cidan/ok/internal/id/service"
	"github.com/Cidan/ok/internal/serve/authn"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// An organization's robots: its owners make, certify, and delete them, its members list them; a
// robot is a member of nothing, and reaches the organization's repositories through its grants
// alone — no base role, none of another organization's.
func TestOrganizationRobots(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	if _, err := e.bea.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.UpdateOrganization(ctx, &identitypb.UpdateOrganizationRequest{Handle: "acme", BaseRole: "write"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.SetMember(ctx, &identitypb.SetMemberRequest{Handle: "acme", PrincipalId: e.adaID, Role: "member"}); err != nil {
		t.Fatal(err)
	}

	if _, err := e.ada.CreateOrganizationRobot(ctx, &identitypb.CreateOrganizationRobotRequest{Handle: "acme", Name: "ci"}); code(err) != codes.PermissionDenied {
		t.Errorf("a member making a robot: %v", err)
	}
	made, err := e.bea.CreateOrganizationRobot(ctx, &identitypb.CreateOrganizationRobotRequest{Handle: "acme", Name: "ci"})
	if err != nil || made.Principal.GetName() != "ci.acme.robot.example.com" {
		t.Fatalf("CreateOrganizationRobot = %v, %v", made, err)
	}
	robot := made.Principal
	if ls, err := e.ada.ListOrganizationRobots(ctx, &identitypb.ListOrganizationRobotsRequest{Handle: "acme"}); err != nil || len(ls.Robots) != 1 {
		t.Errorf("a member listing robots = %v, %v", ls, err)
	}
	if _, err := e.cal.ListOrganizationRobots(ctx, &identitypb.ListOrganizationRobotsRequest{Handle: "acme"}); code(err) != codes.PermissionDenied {
		t.Errorf("a stranger listing robots: %v", err)
	}

	// Its key certified by an owner alone.
	key, _ := credential.NewKey()
	spki, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	certify := &identitypb.CertifyOrganizationRobotRequest{Handle: "acme", Robot: robot.Name, PublicKey: spki, Label: "ci runner"}
	if _, err := e.ada.CertifyOrganizationRobot(ctx, certify); code(err) != codes.PermissionDenied {
		t.Errorf("a member certifying a robot: %v", err)
	}
	if got, err := e.bea.CertifyOrganizationRobot(ctx, certify); err != nil || len(got.GetCertificate().GetChain()) == 0 {
		t.Errorf("CertifyOrganizationRobot = %v, %v", got, err)
	}

	// What it may do: its grants, and nothing of the base role a member has.
	app, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "acme", Name: "app", ForPersonId: e.beaID})
	if err != nil {
		t.Fatal(err)
	}
	if app, err = e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: app.Id}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "globex"}); err != nil {
		t.Fatal(err)
	}
	other, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "globex", Name: "app", ForPersonId: e.beaID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: other.Id}); err != nil {
		t.Fatal(err)
	}
	checker := authz.NewChecker(identitypb.NewAuthorizationClient(e.as(scm)))
	decide := func(repo int64, a authz.Action) authz.Decision {
		t.Helper()
		checker.Forget(repo)
		d, err := checker.Decide(ctx, robot.Id, nil, repo, a)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	if d := decide(app.Id, authz.ReadCode); d.Visible {
		t.Errorf("acme's private repository, to its robot granted nothing: %+v", d)
	}
	if _, err := e.bea.SetGrant(ctx, &identitypb.SetGrantRequest{Id: app.Id, Grant: &identitypb.RepositoryGrant{Subject: robot.Id, Role: "write"}}); err != nil {
		t.Fatal(err)
	}
	if d := decide(app.Id, authz.Land); !d.Allowed {
		t.Errorf("its robot granted write, landing: %+v", d)
	}
	if d := decide(app.Id, authz.Administer); d.Allowed {
		t.Errorf("its robot granted write, administering: %+v", d)
	}
	if _, err := e.bea.SetGrant(ctx, &identitypb.SetGrantRequest{Id: other.Id, Grant: &identitypb.RepositoryGrant{Subject: robot.Id, Role: "read"}}); code(err) != codes.InvalidArgument {
		t.Errorf("granting acme's robot globex's repository: %v", err)
	}

	// Deleted by an owner: it may do nothing.
	if _, err := e.ada.DeleteOrganizationRobot(ctx, &identitypb.DeleteOrganizationRobotRequest{Handle: "acme", Robot: robot.Name}); code(err) != codes.PermissionDenied {
		t.Errorf("a member deleting a robot: %v", err)
	}
	if _, err := e.bea.DeleteOrganizationRobot(ctx, &identitypb.DeleteOrganizationRobotRequest{Handle: "globex", Robot: robot.Name}); code(err) != codes.NotFound {
		t.Errorf("deleting acme's robot as globex's: %v", err)
	}
	if _, err := e.bea.DeleteOrganizationRobot(ctx, &identitypb.DeleteOrganizationRobotRequest{Handle: "acme", Robot: robot.Name}); err != nil {
		t.Fatal(err)
	}
	if d := decide(app.Id, authz.ReadCode); d.Allowed {
		t.Errorf("a deleted robot reading: %+v", d)
	}
}

// Federation per organization: its owners trust issuers and bind its robots, its members list
// them; a robot signs in with its CI issuer's token for the organization's audience — and once the
// issuer is distrusted, does not; a hosted service trusts no issuer over plaintext.
func TestOrganizationFederation(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	gh := fedtest.New(t)
	if _, err := e.bea.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.SetMember(ctx, &identitypb.SetMemberRequest{Handle: "acme", PrincipalId: e.adaID, Role: "member"}); err != nil {
		t.Fatal(err)
	}
	made, err := e.bea.CreateOrganizationRobot(ctx, &identitypb.CreateOrganizationRobotRequest{Handle: "acme", Name: "ci"})
	if err != nil {
		t.Fatal(err)
	}
	robot := made.Principal
	trust := &identitypb.OrganizationIssuerRequest{Handle: "acme", Issuer: gh.URL}
	if _, err := e.ada.TrustIssuer(ctx, trust); code(err) != codes.PermissionDenied {
		t.Errorf("a member trusting an issuer: %v", err)
	}
	is, err := e.bea.TrustIssuer(ctx, trust)
	if err != nil || len(is.Issuers) != 1 || !strings.HasPrefix(is.Audience, "prn_") {
		t.Fatalf("TrustIssuer = %v, %v", is, err)
	}
	if got, err := e.ada.ListIssuers(ctx, &identitypb.ListIssuersRequest{Handle: "acme"}); err != nil || got.Audience != is.Audience {
		t.Errorf("a member listing issuers = %v, %v", got, err)
	}
	if _, err := e.cal.ListIssuers(ctx, &identitypb.ListIssuersRequest{Handle: "acme"}); code(err) != codes.PermissionDenied {
		t.Errorf("a stranger listing issuers: %v", err)
	}
	sub := "repo:acme/app:ref:refs/heads/main"
	bind := &identitypb.OrganizationRobotBindingRequest{Handle: "acme", Robot: robot.Name, Binding: &identitypb.Binding{Issuer: gh.URL, Subject: sub}}
	if got, err := e.bea.BindOrganizationRobot(ctx, bind); err != nil || len(got.Bindings) != 1 {
		t.Fatalf("BindOrganizationRobot = %v, %v", got, err)
	}

	// The robot signs in with its CI's token for acme's audience: it certifies a key.
	certify := func(token string) error {
		conn, err := dial.Dial(e.addr, dial.Options{Token: func(context.Context) (string, error) { return token, nil }})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		key, _ := credential.NewKey()
		spki, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
		_, err = identitypb.NewCredentialsClient(conn).Certify(ctx, &identitypb.CertifyRequest{PublicKey: spki, Label: "a runner"})
		return err
	}
	if err := certify(gh.Token(t, map[string]any{"aud": is.Audience, "sub": sub})); err != nil {
		t.Errorf("acme's robot signing in with its CI's token: %v", err)
	}
	if err := certify(gh.Token(t, map[string]any{"aud": "https://ok.example.com", "sub": sub})); code(err) != codes.Unauthenticated {
		t.Errorf("a token for another audience: %v", err)
	}
	if _, err := e.bea.DistrustIssuer(ctx, trust); err != nil {
		t.Fatal(err)
	}
	if err := certify(gh.Token(t, map[string]any{"aud": is.Audience, "sub": sub})); code(err) != codes.Unauthenticated {
		t.Errorf("a token from an issuer distrusted: %v", err)
	}
	if ls, err := e.bea.ListOrganizationRobots(ctx, &identitypb.ListOrganizationRobotsRequest{Handle: "acme"}); err != nil || len(ls.Robots[0].Bindings) != 0 {
		t.Errorf("the robot's bindings, its issuer distrusted = %v, %v", ls, err)
	}

	// A hosted service, not a development server, trusts no issuer over plaintext.
	strict := service.New(service.Config{Store: e.store, CA: e.ca, Hosted: true})
	beaP, _ := e.store.Principal(ctx, bea)
	asBea := authn.NewContext(ctx, authn.Caller{Principal: beaP.Name, ID: beaP.ID, Human: true, Via: authn.ViaDev})
	if _, err := strict.TrustIssuer(asBea, trust); code(err) != codes.InvalidArgument {
		t.Errorf("trusting an issuer over plaintext: %v", err)
	}
}
