package federation

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/federation/fedtest"
	"github.com/Cidan/ok/internal/id/store"
)

type bindings map[store.Binding]store.Principal

func (b bindings) BoundRobot(_ context.Context, k store.Binding) (store.Principal, error) {
	if p, ok := b[k]; ok {
		return p, nil
	}
	return store.Principal{}, store.ErrNotFound
}

// TrustsIssuer: an organization trusts an issuer some binding of its names.
func (b bindings) TrustsIssuer(_ context.Context, org, issuer string) (bool, error) {
	for k := range b {
		if k.Organization == org && k.Issuer == issuer {
			return true, nil
		}
	}
	return false, nil
}

func TestVerify(t *testing.T) {
	ci := fedtest.New(t)
	cluster := fedtest.New(t)
	cluster.KeyAuth = "sa-token"
	tokenFile := filepath.Join(t.TempDir(), "token")
	os.WriteFile(tokenFile, []byte("sa-token\n"), 0o600)

	b := bindings{
		{Issuer: ci.URL, Subject: "repo:acme/app:ref:refs/heads/main"}:     {Name: "ci.robot.example.com", Kind: store.Robot, Status: directory.Active},
		{Issuer: cluster.URL, Subject: "system:serviceaccount:ok:control"}: {Name: "control.ok.robot.example.com", Kind: store.Robot, Status: directory.Active},
		{Issuer: ci.URL, Subject: "repo:acme/old:ref:refs/heads/main"}:     {Name: "old.robot.example.com", Kind: store.Robot, Status: directory.Deleted},
	}
	v, err := New(context.Background(), []Issuer{
		{URL: ci.URL, Audience: "https://ok.example.com"},
		{URL: cluster.URL, Audience: "ok", JWKSURL: cluster.URL + "/keys", TokenFile: tokenFile},
	}, b, Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	got, err := v.Verify(ctx, ci.Token(t, map[string]any{"aud": "https://ok.example.com", "sub": "repo:acme/app:ref:refs/heads/main"}))
	if err != nil || got != "ci.robot.example.com" {
		t.Errorf("a CI token = %q, %v", got, err)
	}
	got, err = v.Verify(ctx, cluster.Token(t, map[string]any{"aud": []string{"ok"}, "sub": "system:serviceaccount:ok:control"}))
	if err != nil || got != "control.ok.robot.example.com" {
		t.Errorf("a ServiceAccount token = %q, %v", got, err)
	}
	stranger := fedtest.New(t)
	for name, tok := range map[string]string{
		"another audience":   ci.Token(t, map[string]any{"aud": "https://other.example.com", "sub": "repo:acme/app:ref:refs/heads/main"}),
		"expired":            ci.Token(t, map[string]any{"aud": "https://ok.example.com", "sub": "repo:acme/app:ref:refs/heads/main", "exp": time.Now().Add(-time.Hour).Unix()}),
		"an unbound subject": ci.Token(t, map[string]any{"aud": "https://ok.example.com", "sub": "repo:acme/fork:ref:refs/heads/main"}),
		"a deleted robot":    ci.Token(t, map[string]any{"aud": "https://ok.example.com", "sub": "repo:acme/old:ref:refs/heads/main"}),
		"an unknown issuer":  stranger.Token(t, map[string]any{"aud": "https://ok.example.com", "sub": "repo:acme/app:ref:refs/heads/main"}),
		"not a token":        "nope",
	} {
		if got, err := v.Verify(ctx, tok); err == nil {
			t.Errorf("%s: verified as %q", name, got)
		}
	}
	// A token signed with another key, claiming the CI issuer, proves nothing.
	forged := stranger.Token(t, map[string]any{"iss": ci.URL, "aud": "https://ok.example.com", "sub": "repo:acme/app:ref:refs/heads/main"})
	if got, err := v.Verify(ctx, forged); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Errorf("a forged token: %q, %v", got, err)
	}
	if _, err := New(ctx, []Issuer{{URL: ci.URL}}, b, Options{}); err == nil {
		t.Error("an issuer with no audience was accepted")
	}
}

// An organization's issuer signs its robots in with the organization's ID as the audience: not the
// configuration's audience, not another organization's, and not for a subject the organization did
// not bind; the same subject may name a robot in each organization; the issuer is discovered once,
// and kept.
func TestOrganizationIssuers(t *testing.T) {
	gh := fedtest.New(t)
	sub := "repo:acme/app:ref:refs/heads/main"
	acmeCI := store.Principal{Name: "ci.acme.robot.example.com", Kind: store.Robot, Status: directory.Active}
	globexCI := store.Principal{Name: "ci.globex.robot.example.com", Kind: store.Robot, Status: directory.Active}
	b := bindings{
		{Organization: "prn_acme", Issuer: gh.URL, Subject: sub}:   acmeCI,
		{Organization: "prn_globex", Issuer: gh.URL, Subject: sub}: globexCI,
		{Issuer: gh.URL, Subject: sub}:                             {Name: "ci.robot.example.com", Kind: store.Robot, Status: directory.Active},
	}
	v, err := New(context.Background(), []Issuer{{URL: gh.URL, Audience: "https://ok.example.com"}}, b, Options{AllowPrivate: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for aud, want := range map[string]string{"prn_acme": acmeCI.Name, "prn_globex": globexCI.Name, "https://ok.example.com": "ci.robot.example.com"} {
		if got, err := v.Verify(ctx, gh.Token(t, map[string]any{"aud": aud, "sub": sub})); err != nil || got != want {
			t.Errorf("audience %s = %q, %v; want %s", aud, got, err, want)
		}
	}
	fetches := gh.KeyFetches.Load()
	for i := 0; i < 3; i++ {
		v.Verify(ctx, gh.Token(t, map[string]any{"aud": "prn_acme", "sub": sub}))
	}
	if gh.KeyFetches.Load() != fetches {
		t.Errorf("the keys fetched again for every token: %d, then %d", fetches, gh.KeyFetches.Load())
	}
	stranger := fedtest.New(t)
	for name, tok := range map[string]string{
		"an organization that trusts it not":   gh.Token(t, map[string]any{"aud": "prn_initech", "sub": sub}),
		"a subject the organization bound not": gh.Token(t, map[string]any{"aud": "prn_acme", "sub": "repo:acme/fork:ref:refs/heads/main"}),
		"an issuer it trusts not":              stranger.Token(t, map[string]any{"aud": "prn_acme", "sub": sub}),
		"forged, claiming its issuer":          stranger.Token(t, map[string]any{"iss": gh.URL, "aud": "prn_acme", "sub": sub}),
		"no audience":                          gh.Token(t, map[string]any{"sub": sub}),
	} {
		if got, err := v.Verify(ctx, tok); err == nil {
			t.Errorf("%s: verified as %q", name, got)
		}
	}
}

// A hosted service reaches an organization's issuer over HTTPS at a public address alone: the
// test's, on loopback over plaintext, is refused twice over.
func TestOrganizationIssuersGuarded(t *testing.T) {
	gh := fedtest.New(t)
	sub := "repo:acme/app:ref:refs/heads/main"
	b := bindings{{Organization: "prn_acme", Issuer: gh.URL, Subject: sub}: {Name: "ci.acme.robot.example.com", Kind: store.Robot, Status: directory.Active}}
	v, err := New(context.Background(), nil, b, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := v.Verify(context.Background(), gh.Token(t, map[string]any{"aud": "prn_acme", "sub": sub})); err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Errorf("a loopback issuer over plaintext = %q, %v", got, err)
	}
	if gh.KeyFetches.Load() != 0 {
		t.Error("the guarded client fetched from a loopback issuer")
	}
}
