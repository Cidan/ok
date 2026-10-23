package service_test

import (
	"bytes"
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/id/service"
	"github.com/Cidan/ok/internal/serve/authn"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// An enterprise organization's identity provider: its owners set it — its secrets sealed, never
// answered, kept when left out — sync it, and reset its SCIM token; what its provider provisions
// becomes the organization's people and teams, who reach its repositories as members; a member is
// refused all of it, and identity with no sealing key keeps no provider.
func TestIdentityProvider(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	if _, err := e.bea.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.SetMember(ctx, &identitypb.SetMemberRequest{Handle: "acme", PrincipalId: e.adaID, Role: "member"}); err != nil {
		t.Fatal(err)
	}
	settings := &identitypb.IdentityProviderSettings{Scim: &identitypb.SCIMProvider{},
		SignIn: &identitypb.OIDCSignIn{Issuer: "https://idp.acme.example", ClientId: "ok", ClientSecret: "the secret", Match: "email"}}
	set := &identitypb.SetIdentityProviderRequest{Handle: "acme", Settings: settings}
	if _, err := e.ada.SetIdentityProvider(ctx, set); code(err) != codes.PermissionDenied {
		t.Errorf("a member setting the provider: %v", err)
	}
	for name, bad := range map[string]*identitypb.IdentityProviderSettings{
		"no system of record":         {SignIn: settings.SignIn},
		"two systems of record":       {Scim: &identitypb.SCIMProvider{}, Entra: &identitypb.EntraProvider{TenantId: "t", ClientId: "c", ClientSecret: "s"}},
		"ldap in the clear":           {Ldap: &identitypb.LDAPProvider{Url: "ldap://ldap.acme.example", People: &identitypb.LDAPPeople{BaseDn: "dc=acme"}}},
		"a SCIM provider by its IDs":  {Scim: &identitypb.SCIMProvider{}, SignIn: &identitypb.OIDCSignIn{Issuer: "https://idp", ClientId: "c", ClientSecret: "s", Match: "external_id"}},
		"a sign-in issuer over http":  {Scim: &identitypb.SCIMProvider{}, SignIn: &identitypb.OIDCSignIn{Issuer: "http://idp", ClientId: "c", ClientSecret: "s", Match: "email"}},
		"entra with no client secret": {Entra: &identitypb.EntraProvider{TenantId: "t", ClientId: "c"}},
	} {
		if _, err := e.bea.SetIdentityProvider(ctx, &identitypb.SetIdentityProviderRequest{Handle: "acme", Settings: bad}); code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
	}
	info, err := e.bea.SetIdentityProvider(ctx, set)
	if err != nil || info.ScimUrl != "https://ok.example.com/id/scim/v2/orgs/acme" || info.SignInUrl != "https://ok.example.com/id/login/org/acme" ||
		info.Settings.SignIn.ClientSecret != "" {
		t.Fatalf("SetIdentityProvider = %v, %v", info, err)
	}
	// Set again without the secret: the secret stays.
	settings.SignIn.ClientSecret = ""
	if _, err := e.bea.SetIdentityProvider(ctx, set); err != nil {
		t.Fatal(err)
	}
	acme, _ := e.store.OrganizationByHandle(ctx, "acme")
	p, err := e.store.IdentityProviderOf(ctx, acme.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Sealed) == 0 || bytes.Contains(p.Sealed, []byte("the secret")) {
		t.Error("the provider's secret is stored in the clear")
	}
	if got, err := e.bea.GetIdentityProvider(ctx, &identitypb.IdentityProviderRequest{Handle: "acme"}); err != nil || got.Settings.SignIn.ClientSecret != "" {
		t.Errorf("GetIdentityProvider = %v, %v", got, err)
	}
	if tok, err := e.bea.ResetSCIMToken(ctx, &identitypb.IdentityProviderRequest{Handle: "acme"}); err != nil || len(tok.Token) < 32 {
		t.Errorf("ResetSCIMToken = %v, %v", tok, err)
	}

	// What the provider provisions becomes acme's people and teams.
	sc := e.store.SCIMOf(acme.ID)
	if _, err := sc.CreateUser(ctx, scim.User{ID: "u1", UserName: "erin@acme.example", Active: true, Emails: []scim.Email{{Value: "erin@acme.example", Primary: true}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := sc.CreateGroup(ctx, scim.Group{ID: "g1", DisplayName: "Platform", Members: []string{"u1"}}); err != nil {
		t.Fatal(err)
	}
	synced, err := e.bea.SyncIdentityProvider(ctx, &identitypb.IdentityProviderRequest{Handle: "acme"})
	if err != nil || synced.LastRun == nil || synced.LastError != "" {
		t.Fatalf("SyncIdentityProvider = %v, %v", synced, err)
	}
	erin, err := e.store.OrganizationPerson(ctx, acme.ID, "u1")
	if err != nil || erin.Name != "erin-acme.human.example.com" {
		t.Fatalf("the provisioned person = %+v, %v", erin, err)
	}
	if team, err := e.store.Team(ctx, "platform.acme.team.example.com"); err != nil || len(team.Members) != 1 || team.Members[0] != erin.ID {
		t.Errorf("the synced team = %+v, %v", team, err)
	}
	app, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "acme", Name: "app", ForPersonId: e.beaID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: app.Id}); err != nil {
		t.Fatal(err)
	}
	checker := authz.NewChecker(identitypb.NewAuthorizationClient(e.as(scm)))
	if d, err := checker.Decide(ctx, erin.ID, nil, app.Id, authz.ReadCode); err != nil || !d.Allowed {
		t.Errorf("a provisioned member reading acme's repository = %+v, %v", d, err)
	}

	// Removed: its people stay; identity with no sealing key keeps no provider.
	if r, err := e.bea.RemoveIdentityProvider(ctx, &identitypb.IdentityProviderRequest{Handle: "acme"}); err != nil || !r.Removed {
		t.Errorf("RemoveIdentityProvider = %v, %v", r, err)
	}
	if _, err := e.store.OrganizationPerson(ctx, acme.ID, "u1"); err != nil {
		t.Errorf("a person after the provider's removal: %v", err)
	}
	strict := service.New(service.Config{Store: e.store, CA: e.ca, Hosted: true})
	beaP, _ := e.store.Principal(ctx, bea)
	asBea := authn.NewContext(ctx, authn.Caller{Principal: beaP.Name, ID: beaP.ID, Human: true, Via: authn.ViaDev})
	if _, err := strict.SetIdentityProvider(asBea, set); code(err) != codes.FailedPrecondition {
		t.Errorf("a provider where identity has no sealing key: %v", err)
	}
}
