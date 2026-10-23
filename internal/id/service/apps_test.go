package service_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// assertion is an app's JWT, signed with key as alg, with claims over the defaults: iss the app,
// aud ok's public URL, issued now, expiring in five minutes.
func assertion(t *testing.T, alg jose.SignatureAlgorithm, key any, app string, over func(*jwt.Claims)) string {
	t.Helper()
	sig, err := jose.NewSigner(jose.SigningKey{Algorithm: alg, Key: key}, (&jose.SignerOptions{}).WithType("JWT"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	c := jwt.Claims{Issuer: app, Audience: jwt.Audience{"https://ok.example.com"}, IssuedAt: jwt.NewNumericDate(now),
		Expiry: jwt.NewNumericDate(now.Add(5 * time.Minute))}
	if over != nil {
		over(&c)
	}
	raw, err := jwt.Signed(sig).Claims(c).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Apps end to end: acme's owner makes an app and installs it with write on one repository; the app
// signs a JWT with its key, lists its installations, and trades it for an hour's installation
// token, which reaches that repository as far as write — not acme's others, and not another
// organization's where another app is installed everywhere; a bad JWT is refused; uninstalled, the
// token is none.
func TestApps(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	for who, org := range map[identitypb.TenancyClient]string{e.bea: "acme", e.ada: "globex"} {
		if _, err := who.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: org}); err != nil {
			t.Fatal(err)
		}
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
	app, lib, other := made("acme", "app", e.beaID), made("acme", "lib", e.beaID), made("globex", "app", e.adaID)

	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	spki, _ := x509.MarshalPKIXPublicKey(&ec.PublicKey)
	beaApps, adaApps := identitypb.NewAppsClient(e.as(bea)), identitypb.NewAppsClient(e.as(ada))
	if _, err := adaApps.CreateApp(ctx, &identitypb.CreateAppRequest{Handle: "acme", Name: "deployer", PublicKey: spki}); code(err) != codes.PermissionDenied {
		t.Errorf("a stranger making acme's app: %v", err)
	}
	deployer, err := beaApps.CreateApp(ctx, &identitypb.CreateAppRequest{Handle: "acme", Name: "deployer", PublicKey: spki})
	if err != nil || deployer.Robot.GetName() != "deployer.acme.robot.example.com" || deployer.Organization != "acme" {
		t.Fatalf("CreateApp = %v, %v", deployer, err)
	}
	_, edKey, _ := ed25519.GenerateKey(rand.Reader)
	edSPKI, _ := x509.MarshalPKIXPublicKey(edKey.Public())
	helper, err := beaApps.CreateApp(ctx, &identitypb.CreateAppRequest{Handle: "acme", Name: "helper", Public: true, PublicKey: edSPKI})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adaApps.InstallApp(ctx, &identitypb.InstallAppRequest{AppId: deployer.Id, Handle: "globex", Role: "read"}); code(err) != codes.InvalidArgument {
		t.Errorf("globex installing acme's private app: %v", err)
	}
	if _, err := adaApps.InstallApp(ctx, &identitypb.InstallAppRequest{AppId: helper.Id, Handle: "globex", Role: "write"}); err != nil {
		t.Errorf("globex installing acme's public app everywhere: %v", err)
	}
	ins, err := beaApps.InstallApp(ctx, &identitypb.InstallAppRequest{AppId: deployer.Id, Handle: "acme", Role: "write", Repositories: []string{"acme/app"}})
	if err != nil || len(ins.Repositories) != 1 || ins.Repositories[0] != "acme/app" {
		t.Fatalf("InstallApp = %v, %v", ins, err)
	}
	if ls, err := beaApps.ListInstallations(ctx, &identitypb.ListInstallationsRequest{Handle: "acme"}); err != nil || len(ls.Installations) != 1 {
		t.Errorf("ListInstallations = %v, %v", ls, err)
	}

	// The app signs in with its key.
	tokens := identitypb.NewAppTokensClient(e.as(""))
	good := assertion(t, jose.ES256, ec, deployer.Id, nil)
	if ls, err := tokens.ListAppInstallations(ctx, &identitypb.AppAssertion{Jwt: good}); err != nil || len(ls.Installations) != 1 || ls.Installations[0].Id != ins.Id {
		t.Errorf("the app's installations = %v, %v", ls, err)
	}
	other256, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	for name, raw := range map[string]string{
		"signed with another key": assertion(t, jose.ES256, other256, deployer.Id, nil),
		"for another audience":    assertion(t, jose.ES256, ec, deployer.Id, func(c *jwt.Claims) { c.Audience = jwt.Audience{"https://evil.example.com"} }),
		"expired":                 assertion(t, jose.ES256, ec, deployer.Id, func(c *jwt.Claims) { c.Expiry = jwt.NewNumericDate(time.Now().Add(-2 * time.Minute)) }),
		"living an hour":          assertion(t, jose.ES256, ec, deployer.Id, func(c *jwt.Claims) { c.Expiry = jwt.NewNumericDate(time.Now().Add(time.Hour)) }),
		"issued in the future":    assertion(t, jose.ES256, ec, deployer.Id, func(c *jwt.Claims) { c.IssuedAt = jwt.NewNumericDate(time.Now().Add(time.Hour)) }),
		"naming another app":      assertion(t, jose.ES256, ec, helper.Id, nil),
		"naming no app":           assertion(t, jose.ES256, ec, "app_nobody", nil),
		"HS256":                   assertion(t, jose.HS256, []byte("a shared secret long enough to sign with"), deployer.Id, nil),
		"not a JWT":               "nope",
	} {
		if _, err := tokens.CreateInstallationToken(ctx, &identitypb.CreateInstallationTokenRequest{Jwt: raw, InstallationId: ins.Id}); code(err) != codes.Unauthenticated {
			t.Errorf("an assertion %s: %v", name, err)
		}
	}
	globexIns, err := adaApps.ListInstallations(ctx, &identitypb.ListInstallationsRequest{Handle: "globex"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tokens.CreateInstallationToken(ctx, &identitypb.CreateInstallationTokenRequest{Jwt: good, InstallationId: globexIns.Installations[0].Id}); code(err) != codes.NotFound {
		t.Errorf("a token of another app's installation: %v", err)
	}
	minted, err := tokens.CreateInstallationToken(ctx, &identitypb.CreateInstallationTokenRequest{Jwt: good, InstallationId: ins.Id})
	if err != nil || !strings.HasPrefix(minted.Token, "oki_") || time.Until(minted.Expires.AsTime()) > time.Hour {
		t.Fatalf("CreateInstallationToken = %v, %v", minted, err)
	}

	// What its token reaches: acme/app as far as write, nothing else of acme's, nothing of globex's.
	verify := identitypb.NewVerifyClient(e.as(control))
	got, err := verify.GetToken(ctx, &identitypb.GetTokenRequest{Token: minted.Token})
	if err != nil || got.Principal.GetId() != deployer.Robot.GetId() || got.Role != "write" || got.OwnerId == "" {
		t.Fatalf("GetToken = %v, %v", got, err)
	}
	checker := authz.NewChecker(identitypb.NewAuthorizationClient(e.as(scm)))
	role, err := tenancy.ParseRole(got.Role)
	if err != nil {
		t.Fatal(err)
	}
	tok := &authz.Token{Role: role, Repositories: got.RepositoryIds, Owner: got.OwnerId}
	for repo, want := range map[int64]bool{app.Id: true, lib.Id: false, other.Id: false} {
		d, err := checker.Decide(ctx, got.Principal.GetId(), tok, repo, authz.Land)
		if err != nil || d.Allowed != want {
			t.Errorf("the installation token landing on %d = %+v, %v; want %v", repo, d, err, want)
		}
	}
	if d, _ := checker.Decide(ctx, got.Principal.GetId(), tok, app.Id, authz.Administer); d.Allowed {
		t.Error("an app installed with write administering")
	}
	// It reaches repositories, nothing of identity's else.
	if _, err := identitypb.NewTokensClient(e.withToken(minted.Token)).ListTokens(ctx, &identitypb.ListTokensRequest{}); code(err) != codes.PermissionDenied {
		t.Errorf("an installation token listing tokens: %v", err)
	}

	// Uninstalled: its token is none.
	if r, err := beaApps.UninstallApp(ctx, &identitypb.UninstallAppRequest{Id: ins.Id}); err != nil || !r.Uninstalled {
		t.Fatalf("UninstallApp = %v, %v", r, err)
	}
	if _, err := verify.GetToken(ctx, &identitypb.GetTokenRequest{Token: minted.Token}); code(err) != codes.NotFound {
		t.Errorf("an uninstalled installation's token: %v", err)
	}
}
