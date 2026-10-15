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
	if c.Listen != ":8472" || c.MetricsListen != ":8473" || c.Installation != "ok" || c.Identity.SystemOfRecord.Interval != 10*time.Minute {
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
