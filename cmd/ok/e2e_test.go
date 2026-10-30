package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/client/auth"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/id/signin/oidctest"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/testdb"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// TestIdentityEndToEnd runs ok identity from a configuration file, and signs a machine in with ok
// auth login — the browser's half through an OpenID Connect provider — then administers with it.
func TestIdentityEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "people.yaml"), []byte(`
people:
  - {id: "1", email: ada@example.com, name: Ada}
  - {id: "2", email: bea@example.com, name: Bea}
groups:
  - {id: g1, name: eng, members: ["1", "2"]}
`), 0o644)
	idp := oidctest.New(t)
	os.WriteFile(filepath.Join(dir, "client-secret"), []byte(idp.ClientSecret+"\n"), 0o600)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + lis.Addr().String()
	cfg, err := config.Parse([]byte(`
domain: example.com
installation: Example ok
public_url: ` + base + `
roles: {identity: identity.ok, deploy: deploy.ok, control: control.ok}
identity:
  ca_dir: ` + filepath.Join(dir, "ca") + `
  bootstrap_admins: [ada]
  sign_in:
    oidc:
      - id: fake
        name: Fake
        issuer: ` + idp.URL + `
        client_id: ` + idp.ClientID + `
        client_secret_file: ` + filepath.Join(dir, "client-secret") + `
  system_of_record:
    static: {file: ` + filepath.Join(dir, "people.yaml") + `}
  robots:
    - name: ci
      bindings: [{issuer: "https://token.actions.githubusercontent.com", subject: "repo:acme/app:ref:refs/heads/main"}]
`))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Bus = bustest.Config(t)
	pool, err := pgxpool.New(ctx, testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	r, err := role.Build(ctx, cfg, role.Options{Pool: pool})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, r.Root, serve.WithAuthn(r.Resolver), serve.WithListener(lis)) }()
	defer func() { cancel(); <-done }()

	// The sync runs at start: wait for ada.
	conn, err := dial.Dial(lis.Addr().String(), dial.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for i := 0; ; i++ {
		_, err := r.Store.Principal(ctx, "ada.human.example.com")
		if err == nil {
			break
		}
		if i == 100 {
			t.Fatalf("the sync never made ada: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The browser: a cookie jar that signs in with the provider and approves the code.
	jar, _ := cookiejar.New(nil)
	browser := &http.Client{Jar: jar}
	idp.SignInAs(map[string]any{"email": "ada@example.com", "email_verified": true})
	approve := func(link string) error {
		u, err := url.Parse(link)
		if err != nil {
			return err
		}
		code := u.Query().Get("code")
		// Sign in with the provider, and come back to the device page.
		resp, err := browser.Get(base + "/id/login/oidc/fake?next=" + url.QueryEscape(u.RequestURI()))
		if err != nil {
			return err
		}
		page, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.Request.URL.Path != "/id/device" || !strings.Contains(string(page), "ada&#39;s laptop") {
			t.Errorf("after signing in, at %s:\n%s", resp.Request.URL, page)
		}
		req, _ := http.NewRequest(http.MethodPost, base+"/id/device", strings.NewReader(url.Values{"code": {code}, "decision": {"approve"}}.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Origin", base)
		resp, err = browser.Do(req)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !strings.Contains(string(body), "Approved") {
			t.Errorf("approving: %d %s", resp.StatusCode, body)
		}
		return nil
	}
	t.Setenv("OK_CONFIG_DIR", filepath.Join(dir, "home"))
	credDir, _ := auth.Dir()
	srv, _ := auth.ParseServer(base)
	var out bytes.Buffer
	who, err := auth.Login(ctx, srv, credDir, "ada's laptop", &out, approve)
	if err != nil || who != "ada.human.example.com" {
		t.Fatalf("Login = %q, %v\n%s", who, err, out.String())
	}
	if !strings.Contains(out.String(), "/id/device?code=") {
		t.Errorf("Login said:\n%s", out.String())
	}
	if info, err := os.Stat(filepath.Join(credDir, "key.pem")); err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("the key file: %v %v", info, err)
	}

	// The commands, as the signed-in administrator.
	cmd := func(args ...string) string {
		t.Helper()
		var out bytes.Buffer
		if err := run(args, strings.NewReader(""), &out); err != nil {
			t.Fatalf("ok %s: %v\n%s", strings.Join(args, " "), err, out.String())
		}
		return out.String()
	}
	if got := cmd("auth", "status"); !strings.Contains(got, "as ada.human.example.com") {
		t.Errorf("auth status: %s", got)
	}
	if got := cmd("admin", "robot", "create", "builder"); got != "builder.robot.example.com\n" {
		t.Errorf("robot create: %q", got)
	}
	// A robot no issuer vouches for gets a credential an administrator certified.
	robotDir := filepath.Join(dir, "builder-credential")
	if got := cmd("admin", "robot", "certify", "builder", "-dir", robotDir); !strings.Contains(got, "certified builder's key") {
		t.Errorf("robot certify: %q", got)
	}
	rc, err := auth.Load(robotDir)
	if err != nil || rc.Principal() != "builder.robot.example.com" {
		t.Fatalf("the robot's credential: %v, %v", rc, err)
	}
	rconn, err := srv.Dial(dial.Options{Credential: rc})
	if err != nil {
		t.Fatal(err)
	}
	defer rconn.Close()
	if _, err := identitypb.NewDirectoryClient(rconn).ListSets(ctx, &identitypb.ListSetsRequest{}); err != nil {
		t.Errorf("the robot reading the directory: %v", err)
	}
	var robots []string
	for _, line := range strings.Split(strings.TrimSpace(cmd("admin", "robots")), "\n") {
		robots = append(robots, strings.Join(strings.Fields(line), " "))
	}
	want := []string{
		"builder.robot.example.com",
		"ci.robot.example.com https://token.actions.githubusercontent.com repo:acme/app:ref:refs/heads/main",
		"control.ok.robot.example.com", "deploy.ok.robot.example.com", "identity.ok.robot.example.com",
	}
	if strings.Join(robots, "\n") != strings.Join(want, "\n") {
		t.Errorf("robots:\n%s\nwant:\n%s", strings.Join(robots, "\n"), strings.Join(want, "\n"))
	}
	if got := cmd("admin", "status"); !strings.Contains(got, "system of record: static") || strings.Contains(got, "last sync: never") {
		t.Errorf("admin status:\n%s", got)
	}
	if got := cmd("admin", "sync"); got != "nothing changed\n" {
		t.Errorf("admin sync:\n%s", got)
	}
	// The directory through the credential: the synced group.
	c, err := auth.Load(credDir)
	if err != nil {
		t.Fatal(err)
	}
	dconn, err := srv.Dial(dial.Options{Credential: c})
	if err != nil {
		t.Fatal(err)
	}
	defer dconn.Close()
	ex, err := identitypb.NewDirectoryClient(dconn).ExpandSet(ctx, &identitypb.ExpandSetRequest{Name: "eng.group.example.com"})
	if err != nil || len(ex.GetPrincipals()) != 2 {
		t.Errorf("ExpandSet = %v, %v", ex, err)
	}
	// Signing out revokes the credential.
	cmd("auth", "logout")
	if _, err := auth.Load(credDir); err == nil {
		t.Error("the credential is still on the machine")
	}
	ada, _ := r.Store.Principal(ctx, "ada.human.example.com")
	creds, _ := r.Store.CredentialsOf(ctx, ada.ID)
	if len(creds) != 1 || creds[0].Revoked.IsZero() {
		t.Errorf("after logout: %+v", creds)
	}
}

// ok identity certify makes a credential for a robot the configuration names from identity's
// database and CA alone, which identity then accepts; run again, it keeps the credential.
func TestIdentityCertify(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	dsn := testdb.Postgres(t)
	os.WriteFile(filepath.Join(dir, "database-url"), []byte(dsn), 0o600)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "ok.yaml")
	os.WriteFile(cfgPath, []byte(`
domain: example.com
public_url: http://`+lis.Addr().String()+`
database: {url_file: `+filepath.Join(dir, "database-url")+`}
roles: {identity: identity.ok, worker: worker.ok}
services: {identity: `+lis.Addr().String()+`}
identity: {ca_dir: `+filepath.Join(dir, "ca")+`}
`+busYAML(t)), 0o644)
	credDir := filepath.Join(dir, "worker-credential")
	cmd := func(args ...string) (string, error) {
		var out bytes.Buffer
		err := run(args, strings.NewReader(""), &out)
		return out.String(), err
	}
	if out, err := cmd("identity", "certify", "-config", cfgPath, "-dir", credDir, "worker.ok"); err != nil || !strings.Contains(out, "certified worker.ok.robot.example.com's key") {
		t.Fatalf("certify: %v\n%s", err, out)
	}
	if out, err := cmd("identity", "certify", "-config", cfgPath, "-dir", credDir, "worker.ok"); err != nil || !strings.HasPrefix(out, "kept ") {
		t.Errorf("certify again: %v\n%s", err, out)
	}
	if _, err := cmd("identity", "certify", "-config", cfgPath, "-dir", filepath.Join(dir, "x"), "stranger"); err == nil || !strings.Contains(err.Error(), "not a robot the configuration names") {
		t.Errorf("certifying a robot the configuration does not name: %v", err)
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	// Identity's own tables, as it opens them without a pool given it.
	pool, err := pgdb.Open(ctx, dsn, role.Schema, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	r, err := role.Build(ctx, cfg, role.Options{Pool: pool})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, r.Root, serve.WithAuthn(r.Resolver), serve.WithListener(lis)) }()
	defer func() { cancel(); <-done }()
	c, err := auth.Load(credDir)
	if err != nil || c.Principal() != "worker.ok.robot.example.com" {
		t.Fatalf("the credential: %v, %v", c, err)
	}
	srv, _ := auth.ParseServer("http://" + lis.Addr().String())
	conn, err := srv.Dial(dial.Options{Credential: c})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := identitypb.NewDirectoryClient(conn).ListSets(ctx, &identitypb.ListSetsRequest{}); err != nil {
		t.Errorf("the robot reading the directory with the certified credential: %v", err)
	}
}
