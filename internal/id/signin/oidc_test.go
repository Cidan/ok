package signin_test

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"golang.org/x/oauth2"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/signin"
	"github.com/Cidan/ok/internal/id/signin/oidctest"
	"github.com/Cidan/ok/internal/id/store"
)

// people is a directory of one person, ada.
type people struct{}

func (people) ByEmail(_ context.Context, email string) (store.Principal, error) {
	if email != "ada@example.com" {
		return store.Principal{}, store.ErrNotFound
	}
	return store.Principal{Name: "ada.human.example.com", Kind: store.Human, Status: directory.Active, Email: email}, nil
}

func (people) ByExternalID(context.Context, string) (store.Principal, error) {
	return store.Principal{}, store.ErrNotFound
}

// A provider behind a CA the system does not trust is reached with its ca_file; one that is down
// when ok starts keeps only its sign-in from working, until it is back.
func TestOIDCCAFileAndLateProvider(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.NewTLS(t)
	idp.SetDown(true)
	cfg := signin.OIDCConfig{ID: "corp", Name: "Corp", Issuer: idp.URL, ClientID: idp.ClientID, ClientSecret: idp.ClientSecret}
	const redirect = "https://ok.example.com/id/login/oidc/callback"

	// Without the CA, ok starts, and the sign-in says it cannot reach the provider.
	untrusting, err := signin.NewOIDC(ctx, cfg, redirect)
	if err != nil {
		t.Fatalf("a provider's sign-in is made without reaching it: %v", err)
	}
	if _, err := untrusting.Begin(ctx, "s", signin.OIDCState{Nonce: "n", Verifier: oauth2.GenerateVerifier()}); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Errorf("a provider behind an untrusted CA: %v", err)
	}

	// With it, while the provider is down: ok starts, and the sign-in says so.
	cfg.CAFile = idp.CAFile
	o, err := signin.NewOIDC(ctx, cfg, redirect)
	if err != nil {
		t.Fatal(err)
	}
	st := signin.OIDCState{Provider: "corp", Nonce: "nonce-1", Verifier: oauth2.GenerateVerifier()}
	if _, err := o.Begin(ctx, "state-1", st); err == nil || !strings.Contains(err.Error(), "cannot be reached") {
		t.Errorf("a provider that is down: %v", err)
	}

	// Back up, the sign-in works end to end.
	idp.SetDown(false)
	idp.SignInAs(map[string]any{"email": "ada@example.com", "email_verified": true})
	to, err := o.Begin(ctx, "state-1", st)
	if err != nil {
		t.Fatalf("the provider is back: %v", err)
	}
	browser := idp.Client()
	browser.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := browser.Get(to)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	back, err := url.Parse(resp.Header.Get("Location"))
	if err != nil || back.Query().Get("state") != "state-1" {
		t.Fatalf("the provider sent the browser to %q (%v)", resp.Header.Get("Location"), err)
	}
	who, err := o.Finish(ctx, people{}, st, back.Query().Get("code"))
	if err != nil || who.Name != "ada.human.example.com" {
		t.Fatalf("Finish = %+v, %v", who, err)
	}
}
