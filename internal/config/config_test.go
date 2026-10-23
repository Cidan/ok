package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

// The configuration docs/configuration.md shows is one ok reads.
func TestDocsExample(t *testing.T) {
	doc, err := os.ReadFile("../../docs/configuration.md")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(doc), "```yaml\ndomain:")
	if start < 0 {
		t.Fatal("docs/configuration.md shows no configuration")
	}
	body := string(doc)[start+len("```yaml\n"):]
	body = body[:strings.Index(body, "```")]
	c, err := Parse([]byte(body))
	if err != nil {
		t.Fatalf("the documented configuration: %v", err)
	}
	if c.Identity.SystemOfRecord.Google == nil || c.Identity.SystemOfRecord.Interval != 10*time.Minute || len(c.Identity.Federation) != 1 || c.Roles.Control != "control.ok" ||
		c.SCM.Objects.Bucket != "example-ok-scm" || c.SCM.GCHour == nil || *c.SCM.GCHour != 9 || c.Services.Identity != "ok-identity.ok.svc:8472" || c.Workload.TokenFile == "" || c.Database.MaxConnections != 20 {
		t.Errorf("parsed %+v", c)
	}
}

func TestDefaultsAndRefusals(t *testing.T) {
	c, err := Parse([]byte("domain: example.com\npublic_url: https://ok.example.com\nidentity: {system_of_record: {static: {file: p.yaml}}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":8472" || c.MetricsListen != ":8473" || c.Installation != "ok" || c.Identity.SystemOfRecord.Interval != 10*time.Minute ||
		c.Bus.Stream.Type != "postgres" || c.Bus.Queue.Type != "postgres" {
		t.Errorf("defaults: %+v", c)
	}
	for name, bad := range map[string]string{
		"empty":             "",
		"unknown key":       "domain: example.com\npublic_url: https://ok.example.com\ndomian: x\n",
		"no domain":         "public_url: https://ok.example.com\n",
		"a path in the url": "domain: example.com\npublic_url: https://ok.example.com/ok\n",
		"a bad robot":       "domain: example.com\npublic_url: https://ok.example.com\nroles: {scm: Bad_Name}\n",
		"two sources":       "domain: example.com\npublic_url: https://ok.example.com\nidentity: {system_of_record: {static: {file: a}, scim: {token_file: b}}}\n",
		"a bad admin":       "domain: example.com\npublic_url: https://ok.example.com\nidentity: {bootstrap_admins: [Ada Lovelace]}\n",
		"oidc ids twice":    "domain: example.com\npublic_url: https://ok.example.com\nidentity: {sign_in: {oidc: [{id: g}, {id: g}]}}\n",
		"no audience":       "domain: example.com\npublic_url: https://ok.example.com\nidentity: {federation: [{issuer: https://x}]}\n",
		"an unknown stream": "domain: example.com\npublic_url: https://ok.example.com\nbus: {stream: {type: carrier-pigeon}}\n",
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestSecret(t *testing.T) {
	p := t.TempDir() + "/s"
	os.WriteFile(p, []byte("hunter2\r\n"), 0o600)
	if s, err := Secret(p); err != nil || s != "hunter2" {
		t.Errorf("Secret = %q, %v", s, err)
	}
	if s, err := Secret(""); err != nil || s != "" {
		t.Errorf("no file: %q, %v", s, err)
	}
	if _, err := Secret(p + "-missing"); err == nil {
		t.Error("a missing file was read")
	}
}

// Every secret a configuration names is among SecretPaths, wherever it is nested; a CA's
// certificate, which is no secret, is not.
func TestSecretPaths(t *testing.T) {
	cfg, err := Parse([]byte(`
domain: example.com
public_url: https://ok.example.com
tls: {cert: /tls/tls.crt, key: /tls/tls.key}
database: {url_file: /s/db}
roles: {identity: identity.ok, worker: worker.ok}
services: {tls: true, ca_file: /tls/ca.crt, identity: id:8472, scm: scm:8472, automation: a:8472}
workload: {token_file: /var/run/secrets/ok/token}
identity:
  ca_dir: /etc/ok/ca
  sign_in:
    oidc: [{id: g, name: G, issuer: https://g.example.com, client_id: c, client_secret_file: /s/oidc, ca_file: /s/oidc-ca.crt}]
  system_of_record: {static: {file: /s/people.yaml}}
scm: {objects: {endpoint: https://s3, bucket: b, region: r, access_key_file: /s/ak, secret_key_file: /s/sk}}
worker: {state: /var/lib/ok}
deploy:
  state: /var/lib/ok-deploy
  migration_env: {APP_DB: {file: /s/app-db}, PLAIN: {value: x}}
`))
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(cfg.SecretPaths(), " ")
	want := "/etc/ok/ca /s/ak /s/app-db /s/db /s/oidc /s/people.yaml /s/sk /tls/tls.key /var/run/secrets/ok/token"
	if got != want {
		t.Errorf("SecretPaths:\n got %s\nwant %s", got, want)
	}
}
