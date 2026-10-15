// Package federation lets robots sign in with tokens another issuer signs: a Kubernetes
// ServiceAccount's projected token, a CI system's OIDC token (GitHub Actions, GitLab, Buildkite).
// A token is verified against its issuer's keys and the audience ok expects, and its issuer and
// subject name the robot an administrator bound them to.
package federation

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/Cidan/ok/internal/id/store"
)

// Issuer is an issuer whose tokens ok accepts.
type Issuer struct {
	// URL is the issuer, as its tokens' iss claim says it.
	URL string
	// Audience is what its tokens' aud claim must hold for ok: an installation's own value, never a
	// value another service would also accept.
	Audience string
	// JWKSURL, when set, is where its keys are, discovery skipped: a Kubernetes API server whose
	// discovery document is not public.
	JWKSURL string
	// CAFile verifies the issuer's TLS certificate, when the system does not trust it: the cluster's
	// CA.
	CAFile string
	// TokenFile, when set, is a bearer token ok presents to fetch the keys: its own ServiceAccount's,
	// for an API server that serves keys only to the authenticated.
	TokenFile string
}

// Bindings finds the robot an issuer's subject is bound to.
type Bindings interface {
	BoundRobot(ctx context.Context, b store.Binding) (store.Principal, error)
}

// Verifier verifies federated tokens and names their robots: an authn.TokenVerifier.
type Verifier struct {
	issuers  map[string]*oidc.IDTokenVerifier
	bindings Bindings
}

// New makes a verifier for issuers. Issuers that use discovery are discovered here.
func New(ctx context.Context, issuers []Issuer, bindings Bindings) (*Verifier, error) {
	v := &Verifier{issuers: map[string]*oidc.IDTokenVerifier{}, bindings: bindings}
	for _, iss := range issuers {
		if iss.URL == "" || iss.Audience == "" {
			return nil, errors.New("a federated issuer needs a URL and the audience ok expects")
		}
		client, err := httpClient(iss)
		if err != nil {
			return nil, fmt.Errorf("issuer %s: %w", iss.URL, err)
		}
		cctx := oidc.ClientContext(ctx, client)
		cfg := &oidc.Config{ClientID: iss.Audience}
		if iss.JWKSURL != "" {
			v.issuers[iss.URL] = oidc.NewVerifier(iss.URL, oidc.NewRemoteKeySet(cctx, iss.JWKSURL), cfg)
			continue
		}
		p, err := oidc.NewProvider(cctx, iss.URL)
		if err != nil {
			return nil, fmt.Errorf("issuer %s: %w", iss.URL, err)
		}
		v.issuers[iss.URL] = p.Verifier(cfg)
	}
	return v, nil
}

// httpClient is the client that fetches an issuer's keys: its CA trusted, its token presented.
func httpClient(iss Issuer) (*http.Client, error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if iss.CAFile != "" {
		pem, err := os.ReadFile(iss.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s holds no certificate", iss.CAFile)
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	var rt http.RoundTripper = tr
	if iss.TokenFile != "" {
		rt = bearer{next: tr, file: iss.TokenFile}
	}
	return &http.Client{Transport: rt, Timeout: 30 * time.Second}, nil
}

// bearer presents the token in a file, read on each request: a projected token is renewed in
// place.
type bearer struct {
	next http.RoundTripper
	file string
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	tok, err := os.ReadFile(b.file)
	if err != nil {
		return nil, err
	}
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	return b.next.RoundTrip(r)
}

// unverifiedIssuer reads a token's iss claim, to choose the issuer that verifies it.
func unverifiedIssuer(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errors.New("not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", errors.New("not a JWT")
	}
	var claims struct {
		Iss string `json:"iss"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", errors.New("not a JWT")
	}
	return claims.Iss, nil
}

// Verify verifies a token — its issuer's signature, its audience, its expiry — and returns the
// active robot its issuer and subject are bound to.
func (v *Verifier) Verify(ctx context.Context, token string) (string, error) {
	iss, err := unverifiedIssuer(token)
	if err != nil {
		return "", err
	}
	verifier, ok := v.issuers[iss]
	if !ok {
		return "", fmt.Errorf("ok accepts no tokens from %q", iss)
	}
	t, err := verifier.Verify(ctx, token)
	if err != nil {
		return "", err
	}
	p, err := v.bindings.BoundRobot(ctx, store.Binding{Issuer: t.Issuer, Subject: t.Subject})
	if errors.Is(err, store.ErrNotFound) {
		return "", fmt.Errorf("no robot is bound to %s's subject %s", t.Issuer, t.Subject)
	}
	if err != nil {
		return "", err
	}
	if p.Kind != store.Robot || !p.Active() {
		return "", fmt.Errorf("%s is not an active robot", p.Name)
	}
	return p.Name, nil
}
