package web_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/id/enterprise"
	"github.com/Cidan/ok/internal/id/seal"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/id/web"
	"github.com/Cidan/ok/internal/tenancy"
)

// An enterprise organization's people sign in through its own provider at /id/login/org/<handle>,
// matched by their verified email among its people alone: someone the provider vouches for whom
// the organization did not provision is refused, as is anyone with no verified email.
func TestOrganizationSignIn(t *testing.T) {
	sealer, err := seal.New(bytes.Repeat([]byte{3}, 32))
	if err != nil {
		t.Fatal(err)
	}
	var syncer *enterprise.Syncer
	e := newEnvWith(t, func(c *web.Config) {
		syncer = &enterprise.Syncer{Store: c.Store, Sealer: sealer, AllowPrivate: true}
		c.Enterprise = syncer
	})
	ctx := context.Background()
	ada, _ := e.store.Principal(ctx, "ada.human.example.com")
	acme, err := e.store.CreateOrganization(ctx, "acme", ada.ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}
	set := enterprise.Settings{SCIM: &enterprise.SCIM{}, SignIn: &enterprise.SignIn{Issuer: e.idp.URL, ClientID: e.idp.ClientID, Match: "email"}}
	raw, _ := json.Marshal(set)
	sealed, err := enterprise.Seal(sealer, acme.ID, enterprise.Secrets{OIDCClientSecret: e.idp.ClientSecret})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.SetIdentityProvider(ctx, store.IdentityProvider{Organization: acme.ID, Source: "scim", Settings: raw, Sealed: sealed}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.SCIMOf(acme.ID).CreateUser(ctx, scim.User{ID: "u1", UserName: "erin@acme.example", Active: true,
		Emails: []scim.Email{{Value: "erin@acme.example", Primary: true}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := syncer.Sync(ctx, acme.ID); err != nil {
		t.Fatal(err)
	}

	e.idp.SignInAs(map[string]any{"email": "erin@acme.example", "email_verified": true})
	if _, body, where := e.get(t, "/id/login/org/acme?next="+url.QueryEscape("/id/account")); where.Path != "/id/account" || !strings.Contains(body, "erin-acme.human.example.com") {
		t.Fatalf("an organization's person signing in: %s\n%s", where, body)
	}
	for name, claims := range map[string]map[string]any{
		"someone it did not provision": {"email": "ada@example.com", "email_verified": true},
		"an email it has not verified": {"email": "erin@acme.example", "email_verified": false},
	} {
		e.fresh()
		e.idp.SignInAs(claims)
		if _, body, where := e.get(t, "/id/login/org/acme"); where.Path == "/id/account" || !strings.Contains(body, "You are not one of acme") {
			t.Errorf("%s: %s\n%s", name, where, body)
		}
	}
	if code, _, _ := e.get(t, "/id/login/org/nobody"); code != 404 {
		t.Errorf("an organization there is none of: %d", code)
	}
}
