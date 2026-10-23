package main

import (
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

	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/signin/oidctest"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
)

// ok serve runs identity, the SCM, automation, and control in one process on one port, each role
// calling the others as its own robot with a key identity certified in process: a person signed in
// in a browser sees control's pages, creates a repository through control — which asks the SCM as
// control's robot — and finds it once control's feed, following the SCM as that robot, brings it.
func TestServe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "people.yaml"), []byte("people:\n  - {id: \"1\", email: ada@example.com, name: Ada}\n"), 0o644)
	idp := oidctest.New(t)
	secret := func(name, value string) string {
		p := filepath.Join(dir, name)
		os.WriteFile(p, []byte(value+"\n"), 0o600)
		return p
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	base := "http://" + addr
	bucket := func(field string) string { return field + `: ` + testobjects.New(t).YAML(secret) }
	config := filepath.Join(dir, "ok.yaml")
	os.WriteFile(config, []byte(`
domain: example.com
installation: Example ok
public_url: `+base+`
listen: `+addr+`
metrics_listen: 127.0.0.1:0
database: {url_file: `+secret("database-url", testdb.Postgres(t))+`}
roles: {identity: identity.ok, scm: scm.ok, control: control.ok, automation: automation.ok, worker: worker.ok, deploy: deploy.ok}
identity:
  ca_dir: `+filepath.Join(dir, "ca")+`
  sign_in:
    oidc:
      - {id: fake, name: Fake, issuer: `+idp.URL+`, client_id: `+idp.ClientID+`, client_secret_file: `+secret("client-secret", idp.ClientSecret)+`}
  system_of_record: {static: {file: `+filepath.Join(dir, "people.yaml")+`}}
scm:
  `+bucket("objects")+`
automation:
  `+bucket("logs")+`
`), 0o644)
	done := make(chan error, 1)
	go func() {
		done <- serveRolesUntil(ctx, []string{"-config", config, "-roles", "identity,scm,automation,control"})
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("ok serve: %v", err)
		}
	})
	for i := 0; ; i++ {
		if resp, err := http.Get(base + "/readyz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if i == 200 {
			t.Fatal("ok serve never became ready")
		}
		select {
		case err := <-done:
			t.Fatalf("ok serve ended: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// ada signs in, in a browser, and lands on control's pages.
	jar, _ := cookiejar.New(nil)
	browser := &http.Client{Jar: jar}
	get := func(path string) (int, string) {
		t.Helper()
		resp, err := browser.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	idp.SignInAs(map[string]any{"email": "ada@example.com", "email_verified": true})
	var code int
	var page string
	for i := 0; ; i++ {
		// The sync makes ada as identity starts: until it has, she cannot sign in.
		if code, page = get("/id/login/oidc/fake?next=" + url.QueryEscape("/")); code == http.StatusOK && strings.Contains(page, "<title>Repositories · Example ok</title>") {
			break
		}
		if i == 100 {
			t.Fatalf("signing ada in: %d\n%.600s", code, page)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// She creates a repository: control asks the SCM, as its robot.
	req, _ := http.NewRequest(http.MethodPost, base+"/repositories/new", strings.NewReader(url.Values{"name": {"app"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Origin", base)
	resp, err := browser.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Request.URL.Path != "/app" {
		t.Fatalf("creating a repository: %d at %s\n%.600s", resp.StatusCode, resp.Request.URL, body)
	}
	// Control's feed, following the SCM as its robot, brings it to the finder.
	for i := 0; ; i++ {
		if _, page = get("/repositories/list"); strings.Contains(page, `href="/app"`) {
			break
		}
		if i == 100 {
			t.Fatalf("the finder never listed app:\n%.600s", page)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
