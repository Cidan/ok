// Package federation lets robots sign in with tokens another issuer signs: a Kubernetes
// ServiceAccount's projected token, a CI system's OIDC token (GitHub Actions, GitLab, Buildkite).
// A token is verified against its issuer's keys and the audience ok expects, and its issuer and
// subject name the robot an administrator bound them to.
//
// The issuers are the configuration's, for the installation's robots, and each organization's own
// (docs/hosted.md, "Robots and tokens per organization"): a token for an organization carries the
// organization's ID as its audience, and names a robot the organization bound to its issuer and
// subject. An organization names its issuers, so ok reaches them only over HTTPS at public
// addresses, and keeps what it learns of each a while.
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
	"slices"
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

// Bindings finds the robot an issuer's subject is bound to, and the issuers each organization
// trusts (the directory).
type Bindings interface {
	BoundRobot(ctx context.Context, b store.Binding) (store.Principal, error)
	TrustsIssuer(ctx context.Context, org, issuer string) (bool, error)
}

// Options are how a verifier reaches organizations' issuers.
type Options struct {
	// AllowPrivate lets an organization's issuer be reached over plaintext, and at a loopback or
	// private address: a development server's, never a hosted service's.
	AllowPrivate bool
}

// Verifier verifies federated tokens and names their robots: an authn.TokenVerifier.
type Verifier struct {
	issuers  map[string]configured
	bindings Bindings
	orgs     *orgIssuers
}

// configured is one of the configuration's issuers: its verifier, and the audience ok expects.
type configured struct {
	verifier *oidc.IDTokenVerifier
	audience string
}

// New makes a verifier for the configuration's issuers, and the organizations' the directory
// names. The configuration's issuers that use discovery are discovered here.
func New(ctx context.Context, issuers []Issuer, bindings Bindings, o Options) (*Verifier, error) {
	v := &Verifier{issuers: map[string]configured{}, bindings: bindings, orgs: newOrgIssuers(o.AllowPrivate)}
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
			v.issuers[iss.URL] = configured{oidc.NewVerifier(iss.URL, oidc.NewRemoteKeySet(cctx, iss.JWKSURL), cfg), iss.Audience}
			continue
		}
		p, err := oidc.NewProvider(cctx, iss.URL)
		if err != nil {
			return nil, fmt.Errorf("issuer %s: %w", iss.URL, err)
		}
		v.issuers[iss.URL] = configured{p.Verifier(cfg), iss.Audience}
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

// unverifiedClaims reads a token's iss and aud claims, to choose what verifies it.
func unverifiedClaims(token string) (string, []string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", nil, errors.New("not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", nil, errors.New("not a JWT")
	}
	var claims struct {
		Iss string          `json:"iss"`
		Aud json.RawMessage `json:"aud"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", nil, errors.New("not a JWT")
	}
	var aud []string
	if len(claims.Aud) > 0 && json.Unmarshal(claims.Aud, &aud) != nil {
		var one string
		if json.Unmarshal(claims.Aud, &one) != nil {
			return "", nil, errors.New("not a JWT: its aud is neither a string nor strings")
		}
		aud = []string{one}
	}
	return claims.Iss, aud, nil
}

// Verify verifies a token — its issuer's signature, its audience, its expiry — and returns the
// active robot its issuer and subject are bound to: the installation's, for a configured issuer and
// its audience; an organization's, for an issuer the organization trusts and its ID as the audience.
func (v *Verifier) Verify(ctx context.Context, token string) (string, error) {
	iss, aud, err := unverifiedClaims(token)
	if err != nil {
		return "", err
	}
	if c, ok := v.issuers[iss]; ok && slices.Contains(aud, c.audience) {
		t, err := c.verifier.Verify(ctx, token)
		if err != nil {
			return "", err
		}
		return v.robot(ctx, store.Binding{Issuer: t.Issuer, Subject: t.Subject})
	}
	for _, org := range aud {
		if !strings.HasPrefix(org, "prn_") {
			continue
		}
		trusted, err := v.bindings.TrustsIssuer(ctx, org, iss)
		if err != nil {
			return "", err
		}
		if !trusted {
			continue
		}
		keys, err := v.orgs.keys(ctx, iss)
		if err != nil {
			return "", fmt.Errorf("issuer %s: %w", iss, err)
		}
		t, err := oidc.NewVerifier(iss, keys, &oidc.Config{ClientID: store.OrganizationAudience(org)}).Verify(ctx, token)
		if err != nil {
			return "", err
		}
		return v.robot(ctx, store.Binding{Organization: org, Issuer: t.Issuer, Subject: t.Subject})
	}
	return "", fmt.Errorf("ok accepts no tokens from %q for %q", iss, aud)
}

// robot is the active robot a binding names.
func (v *Verifier) robot(ctx context.Context, b store.Binding) (string, error) {
	p, err := v.bindings.BoundRobot(ctx, b)
	if errors.Is(err, store.ErrNotFound) {
		return "", fmt.Errorf("no robot is bound to %s's subject %s", b.Issuer, b.Subject)
	}
	if err != nil {
		return "", err
	}
	if p.Kind != store.Robot || !p.Active() {
		return "", fmt.Errorf("%s is not an active robot", p.Name)
	}
	return p.Name, nil
}
