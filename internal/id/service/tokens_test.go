package service_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/dial"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// withToken dials the server presenting a personal token.
func (e *env) withToken(secret string) *grpc.ClientConn {
	e.t.Helper()
	conn, err := dial.Dial(e.addr, dial.Options{Token: func(context.Context) (string, error) { return secret, nil }})
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { conn.Close() })
	return conn
}

// A person makes personal tokens for repositories they may see, shown once; with one, they reach
// those repositories as far as it allows, and nothing else — no other repository, no token, no
// organization; ok's roles read what one allows; its holder lists and revokes theirs.
func TestTokens(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	if _, err := e.bea.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	made := func(owner, name, person string) *identitypb.RepositoryEntry {
		t.Helper()
		r, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: owner, Name: name, ForPersonId: person})
		if err != nil {
			t.Fatal(err)
		}
		if r, err = e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: r.Id}); err != nil {
			t.Fatal(err)
		}
		return r
	}
	app := made("acme", "app", e.beaID)
	lib := made("acme", "lib", e.beaID)
	made("ada", "notes", e.adaID)
	if _, err := e.bea.SetGrant(ctx, &identitypb.SetGrantRequest{Id: app.Id, Grant: &identitypb.RepositoryGrant{Subject: e.adaID, Role: "admin"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.SetGrant(ctx, &identitypb.SetGrantRequest{Id: lib.Id, Grant: &identitypb.RepositoryGrant{Subject: e.adaID, Role: "admin"}}); err != nil {
		t.Fatal(err)
	}

	tokens := identitypb.NewTokensClient(e.as(ada))
	for name, req := range map[string]*identitypb.CreateTokenRequest{
		"another's private repository":  {Label: "x", Repositories: []string{"bea/secret"}},
		"a repository there is none of": {Label: "x", Repositories: []string{"acme/nope"}},
		"no label":                      {Repositories: []string{"acme/app"}},
		"a role there is none of":       {Label: "x", Role: "boss"},
		"over a year":                   {Label: "x", LifetimeDays: 400},
	} {
		if _, err := tokens.CreateToken(ctx, req); err == nil {
			t.Errorf("%s: made", name)
		}
	}
	if _, err := identitypb.NewTokensClient(e.as(control)).CreateToken(ctx, &identitypb.CreateTokenRequest{Label: "x"}); code(err) != codes.PermissionDenied {
		t.Errorf("ok control making a token: %v", err)
	}
	made1, err := tokens.CreateToken(ctx, &identitypb.CreateTokenRequest{Label: "ci", Role: "read", Repositories: []string{"acme/app"}})
	if err != nil || !strings.HasPrefix(made1.Secret, "okp_") || made1.Token.Role != "read" || len(made1.Token.Repositories) != 1 ||
		made1.Token.Repositories[0] != "acme/app" {
		t.Fatalf("CreateToken = %v, %v", made1, err)
	}

	// With it, ada reaches acme/app as a reader, though she administers it, and nothing else.
	withTok := identitypb.NewTenancyClient(e.withToken(made1.Secret))
	if got, err := withTok.ResolveRepository(ctx, &identitypb.ResolveRepositoryRequest{Owner: "acme", Name: "app"}); err != nil || got.Role != "read" {
		t.Errorf("acme/app through the token = %v, %v; want read", got, err)
	}
	for _, name := range []string{"lib", "notes"} {
		owner := "acme"
		if name == "notes" {
			owner = "ada"
		}
		if _, err := withTok.ResolveRepository(ctx, &identitypb.ResolveRepositoryRequest{Owner: owner, Name: name}); code(err) != codes.NotFound {
			t.Errorf("%s/%s, which the token does not name: %v", owner, name, err)
		}
	}
	if _, err := withTok.UpdateRepository(ctx, &identitypb.UpdateRepositoryRequest{Id: app.Id, Visibility: "public"}); code(err) != codes.PermissionDenied {
		t.Errorf("administering through a read token: %v", err)
	}
	if ls, err := withTok.ListRepositoryEntries(ctx, &identitypb.ListRepositoryEntriesRequest{Owner: "acme"}); err != nil || len(ls.Repositories) != 1 {
		t.Errorf("acme's repositories through the token = %v, %v", ls, err)
	}
	// A token reaches repositories alone.
	if _, err := identitypb.NewTokensClient(e.withToken(made1.Secret)).CreateToken(ctx, &identitypb.CreateTokenRequest{Label: "wider", Role: "admin"}); code(err) != codes.PermissionDenied {
		t.Errorf("a token making a token: %v", err)
	}
	if _, err := withTok.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "tokenorg"}); code(err) != codes.PermissionDenied {
		t.Errorf("a token making an organization: %v", err)
	}
	if _, err := identitypb.NewCredentialsClient(e.withToken(made1.Secret)).List(ctx, &identitypb.ListCredentialsRequest{}); code(err) != codes.PermissionDenied {
		t.Errorf("a token listing credentials: %v", err)
	}
	if _, err := identitypb.NewDirectoryClient(e.withToken(made1.Secret)).GetPrincipal(ctx, &identitypb.GetPrincipalRequest{
		Key: &identitypb.GetPrincipalRequest_Name{Name: bea}}); err != nil {
		t.Errorf("a token reading the directory: %v", err)
	}

	// ok's roles read what a token allows.
	verify := identitypb.NewVerifyClient(e.as(control))
	got, err := verify.GetToken(ctx, &identitypb.GetTokenRequest{Token: made1.Secret})
	if err != nil || got.Principal.GetName() != ada || got.Role != "read" || len(got.RepositoryIds) != 1 || got.RepositoryIds[0] != app.Id {
		t.Errorf("GetToken = %v, %v", got, err)
	}

	// Its holder lists and revokes it; revoked, it is no token.
	if ls, err := tokens.ListTokens(ctx, &identitypb.ListTokensRequest{}); err != nil || len(ls.Tokens) != 1 || ls.Tokens[0].LastUsed == nil {
		t.Errorf("ListTokens = %v, %v", ls, err)
	}
	if r, err := identitypb.NewTokensClient(e.as(bea)).RevokeToken(ctx, &identitypb.RevokeTokenRequest{Id: made1.Token.Id}); err != nil || r.Revoked {
		t.Errorf("another revoking it: %v, %v", r, err)
	}
	if r, err := tokens.RevokeToken(ctx, &identitypb.RevokeTokenRequest{Id: made1.Token.Id}); err != nil || !r.Revoked {
		t.Errorf("RevokeToken = %v, %v", r, err)
	}
	if _, err := verify.GetToken(ctx, &identitypb.GetTokenRequest{Token: made1.Secret}); code(err) != codes.NotFound {
		t.Errorf("a revoked token: %v", err)
	}
}
