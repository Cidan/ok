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
	}, b)
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
	if _, err := New(ctx, []Issuer{{URL: ci.URL}}, b); err == nil {
		t.Error("an issuer with no audience was accepted")
	}
}
