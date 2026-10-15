package web_test

import (
	"context"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/service"
	"github.com/Cidan/ok/internal/id/signin"
	"github.com/Cidan/ok/internal/id/signin/oidctest"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/id/web"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/health"
	"github.com/Cidan/ok/internal/serve/interceptor"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/serve/serving"
	"github.com/Cidan/ok/internal/testdb"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

var names = identity.MustNames("example.com")

type svcNode struct{ s *service.Server }

func (svcNode) Name() string                 { return "ok/identity/test" }
func (svcNode) Dependencies() []node.Node    { return nil }
func (svcNode) Healthy(context.Context) bool { return true }
func (svcNode) Setup(context.Context) error  { return nil }
func (n svcNode) RegisterRPC(_ context.Context, r grpc.ServiceRegistrar) error {
	n.s.Register(r)
	return nil
}

type source struct{ snap *directory.Snapshot }

func (s source) Snapshot(context.Context) (*directory.Snapshot, error) { return s.snap, nil }
func (source) Changes(context.Context, string) (*directory.Delta, error) {
	return nil, directory.ErrNoDelta
}

type env struct {
	base   string
	addr   string
	store  *store.Store
	idp    *oidctest.Provider
	client *http.Client
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	st, err := store.Open(ctx, pool, names)
	if err != nil {
		t.Fatal(err)
	}
	snap := &directory.Snapshot{People: []directory.Person{
		{ExternalID: "1", Handle: "ada", Email: "Ada@Example.com", Name: "Ada", Active: true},
		{ExternalID: "2", Handle: "bea", Email: "bea@example.com", Name: "Bea", Active: true},
	}}
	opts, _ := st.SyncOptions(ctx)
	if _, err := directory.Sync(ctx, names, source{snap}, st, opts); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	ca.Init(dir, "Example")
	authority, err := ca.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + lis.Addr().String()
	idp := oidctest.New(t)
	provider, err := signin.NewOIDC(ctx, signin.OIDCConfig{ID: "fake", Name: "Fake", Issuer: idp.URL, ClientID: idp.ClientID, ClientSecret: idp.ClientSecret}, base+"/id/login/oidc/callback")
	if err != nil {
		t.Fatal(err)
	}
	passkeys, err := signin.NewPasskeys("localhost", "Example", base, st)
	if err != nil {
		t.Fatal(err)
	}
	svc := service.New(service.Config{Store: st, CA: authority, PublicURL: base, BootstrapAdmins: []string{"ada"}})
	pages := web.New(web.Config{Store: st, Services: svc, Installation: "Example ok", PublicURL: base, OIDC: []*signin.OIDC{provider}, Passkeys: passkeys})
	resolver := authn.NewResolver(authn.Config{Directory: service.Directory{Store: st}, Credentials: credential.NewVerifier(authority.Pool)})
	chain := interceptor.New()
	chain.UseUnary(resolver.UnaryInterceptor())
	chain.UseStream(resolver.StreamInterceptor())
	sctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- serving.Serve(sctx, serving.Config{
			Nodes: []node.Node{svcNode{svc}, pages}, Chain: chain, Listener: lis, Middleware: resolver.Middleware,
			Health: health.New(func(context.Context) bool { return true }),
		})
	}()
	t.Cleanup(func() { cancel(); <-done })
	jar, _ := cookiejar.New(nil)
	return &env{base: base, addr: lis.Addr().String(), store: st, idp: idp, client: &http.Client{Jar: jar}}
}

func (e *env) get(t *testing.T, path string) (int, string, *url.URL) {
	t.Helper()
	resp, err := e.client.Get(e.base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), resp.Request.URL
}

func (e *env) post(t *testing.T, path string, form url.Values, origin string) (int, string, *url.URL) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, e.base+path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), resp.Request.URL
}

func (e *env) signIn(t *testing.T, claims map[string]any) (int, string, *url.URL) {
	t.Helper()
	e.idp.SignInAs(claims)
	return e.get(t, "/id/login/oidc/fake?next="+url.QueryEscape("/id/account"))
}

func TestSignInWithOIDC(t *testing.T) {
	e := newEnv(t)
	code, body, _ := e.get(t, "/id/login")
	if code != 200 || !strings.Contains(body, "Sign in with Fake") || !strings.Contains(body, "Sign in with a passkey") {
		t.Fatalf("the sign-in page: %d\n%s", code, body)
	}
	// An unverified email, and someone the directory does not hold, are refused.
	code, body, _ = e.signIn(t, map[string]any{"email": "ada@example.com", "email_verified": false})
	if code != http.StatusUnauthorized || !strings.Contains(body, "has not verified your email") {
		t.Errorf("an unverified email: %d\n%s", code, body)
	}
	code, body, _ = e.signIn(t, map[string]any{"email": "zed@example.com", "email_verified": true})
	if code != http.StatusUnauthorized || !strings.Contains(body, "not a person ok") {
		t.Errorf("a stranger: %d\n%s", code, body)
	}
	// ada signs in; her email matches whatever its case.
	code, body, where := e.signIn(t, map[string]any{"email": "ADA@example.com", "email_verified": true})
	if code != 200 || where.Path != "/id/account" || !strings.Contains(body, "ada.human.example.com") || !strings.Contains(body, "Administration") {
		t.Fatalf("signed in: %d at %s\n%s", code, where, body)
	}
	// A state used once is used up.
	if code, _, _ := e.get(t, "/id/login/oidc/callback?state=nope&code=x"); code != http.StatusBadRequest {
		t.Errorf("a callback with no ceremony: %d", code)
	}
	// Signing out ends the session; the account then asks to sign in.
	if code, _, _ := e.post(t, "/id/logout", nil, "https://evil.example.org"); code != http.StatusForbidden {
		t.Errorf("a cross-site sign-out: %d", code)
	}
	if _, _, where := e.post(t, "/id/logout", nil, e.base); where.Path != "/id/login" {
		t.Errorf("signed out to %s", where)
	}
	if _, body, where := e.get(t, "/id/account"); where.Path != "/id/login" || !strings.Contains(body, "Sign in with Fake") {
		t.Errorf("the account after signing out: %s", where)
	}
	// An open redirect is no redirect.
	e.idp.SignInAs(map[string]any{"email": "ada@example.com", "email_verified": true})
	if _, _, where = e.get(t, "/id/login/oidc/fake?next="+url.QueryEscape("//evil.example.org/x")); where.Host != strings.TrimPrefix(e.base, "http://") || where.Path != "/id/account" {
		t.Errorf("an off-site next went to %s", where)
	}
}

func TestDeviceApproval(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	key, _ := credential.NewKey()
	spki, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	conn, err := dial.Dial(e.addr, dial.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	login := identitypb.NewDeviceLoginClient(conn)
	start, err := login.Start(ctx, &identitypb.StartRequest{PublicKey: spki, Label: "bea's laptop"})
	if err != nil {
		t.Fatal(err)
	}
	// The verification link asks a person who is not signed in to sign in, and comes back.
	devicePath := strings.TrimPrefix(start.VerificationUriComplete, e.base)
	if _, body, where := e.get(t, devicePath); where.Path != "/id/login" || !strings.Contains(body, "Sign in") {
		t.Fatalf("the device page before signing in: %s", where)
	}
	e.idp.SignInAs(map[string]any{"email": "bea@example.com", "email_verified": true})
	if _, _, where := e.get(t, "/id/login/oidc/fake?next="+url.QueryEscape(devicePath)); where.Path != "/id/device" {
		t.Fatalf("after signing in, at %s", where)
	}
	code, body, _ := e.get(t, devicePath)
	if code != 200 || !strings.Contains(body, "bea&#39;s laptop") || !strings.Contains(body, "bea.human.example.com") {
		t.Fatalf("the confirmation: %d\n%s", code, body)
	}
	if code, _, _ := e.get(t, "/id/device?code=ZZZZ-ZZZZ"); code != http.StatusNotFound {
		t.Errorf("an unknown code: %d", code)
	}
	if code, _, _ := e.post(t, "/id/device", url.Values{"code": {start.UserCode}, "decision": {"approve"}}, "https://evil.example.org"); code != http.StatusForbidden {
		t.Errorf("a cross-site approval: %d", code)
	}
	code, body, _ = e.post(t, "/id/device", url.Values{"code": {start.UserCode}, "decision": {"approve"}}, e.base)
	if code != 200 || !strings.Contains(body, "Approved") {
		t.Fatalf("approving: %d\n%s", code, body)
	}
	poll, err := login.Poll(ctx, &identitypb.PollRequest{DeviceCode: start.DeviceCode})
	if err != nil || poll.State != identitypb.PollState_POLL_STATE_APPROVED || poll.Principal != "bea.human.example.com" {
		t.Fatalf("Poll = %v, %v", poll, err)
	}
	// The account lists the device, and can sign it out.
	_, body, _ = e.get(t, "/id/account")
	if !strings.Contains(body, "bea&#39;s laptop") {
		t.Errorf("the account does not list the device:\n%s", body)
	}
	if _, _, where := e.post(t, "/id/account/credentials/revoke", url.Values{"id": {poll.Certificate.Credential}}, e.base); where.Path != "/id/account" {
		t.Errorf("revoking went to %s", where)
	}
	c, _ := e.store.Credential(ctx, poll.Certificate.Credential)
	if c.Standing() {
		t.Error("the device is still signed in")
	}
}

func TestAdminAndPasskeyBegin(t *testing.T) {
	e := newEnv(t)
	e.signIn(t, map[string]any{"email": "bea@example.com", "email_verified": true})
	if code, body, _ := e.get(t, "/id/admin"); code != http.StatusForbidden || !strings.Contains(body, "is not a bootstrap administrator") {
		t.Errorf("bea's administration: %d\n%s", code, body)
	}
	e.signIn(t, map[string]any{"email": "ada@example.com", "email_verified": true})
	code, body, _ := e.get(t, "/id/admin")
	if code != 200 || !strings.Contains(body, "none configured") {
		t.Errorf("ada's administration: %d\n%s", code, body)
	}
	if code, body, _ := e.post(t, "/id/admin/sync", nil, e.base); code != 200 || !strings.Contains(body, "names no system of record") {
		t.Errorf("a sync with no system of record: %d\n%s", code, body)
	}
	req, _ := http.NewRequest(http.MethodPost, e.base+"/id/login/passkey/begin", strings.NewReader("{}"))
	req.Header.Set("Origin", e.base)
	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(b), `"ceremony"`) || !strings.Contains(string(b), `"challenge"`) {
		t.Errorf("passkey begin: %d %s", resp.StatusCode, b)
	}
	req, _ = http.NewRequest(http.MethodPost, e.base+"/id/account/passkeys/begin", strings.NewReader("{}"))
	resp, err = e.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(b), `"residentKey":"required"`) {
		t.Errorf("passkey registration begin: %d %s", resp.StatusCode, b)
	}
	if code, _, _ := e.get(t, "/id/assets/app.css"); code != 200 {
		t.Errorf("app.css: %d", code)
	}
}
