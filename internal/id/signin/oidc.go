// Package signin is how people sign in to ok in a browser: an OpenID Connect provider (Google,
// Microsoft Entra ID, Okta, Keycloak — any), an LDAP directory's password, or a passkey. Each
// authenticator names the person it vouched for by an attribute the system of record also holds —
// their email, or their ID there — and the directory finds them by it.
package signin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"

	"github.com/Cidan/ok/internal/id/store"
)

// Match is what an authenticator's assertion is matched to a person by.
type Match string

const (
	// ByEmail matches the email the authenticator vouches for to a person's email.
	ByEmail Match = "email"
	// ByExternalID matches an ID the authenticator gives — a claim, an LDAP attribute — to the
	// system of record's ID for the person: when both are the same directory.
	ByExternalID Match = "external_id"
)

// ErrNoPerson is an authenticator vouching for someone the directory holds no active person for.
var ErrNoPerson = errors.New("you are not a person ok's directory knows, or your account is not active: ask an administrator")

// People finds the person an authenticator vouched for.
type People interface {
	ByEmail(ctx context.Context, email string) (store.Principal, error)
	ByExternalID(ctx context.Context, id string) (store.Principal, error)
}

// find matches an assertion to an active person.
func find(ctx context.Context, people People, match Match, value string) (store.Principal, error) {
	if value == "" {
		return store.Principal{}, ErrNoPerson
	}
	var p store.Principal
	var err error
	switch match {
	case ByExternalID:
		p, err = people.ByExternalID(ctx, value)
	default:
		p, err = people.ByEmail(ctx, value)
	}
	if errors.Is(err, store.ErrNotFound) || err == nil && (!p.Active() || p.Kind != store.Human) {
		return store.Principal{}, ErrNoPerson
	}
	return p, err
}

// OIDCConfig is an OpenID Connect provider.
type OIDCConfig struct {
	// ID names it in ok's URLs: google, entra.
	ID string
	// Name is what the sign-in button says: "Google".
	Name         string
	Issuer       string
	ClientID     string
	ClientSecret string
	// Scopes are requested besides openid; default email and profile.
	Scopes []string
	// Match is how the ID token names the person; default ByEmail, with Claim email.
	Match Match
	// Claim is the ID token's claim Match reads: email, sub, oid.
	Claim string
	// TrustUnverifiedEmail matches an email the provider has not marked verified. Leave it off but
	// for a provider that verifies every address it issues and says nothing.
	TrustUnverifiedEmail bool
	// HostedDomain, for Google, is the Workspace domain the person must belong to (the hd claim).
	HostedDomain string
	// CAFile verifies the provider's certificate, when it is not one the system trusts: a
	// provider inside the organization, behind its own CA.
	CAFile string
}

// OIDC signs people in with one OpenID Connect provider: the authorization code flow with PKCE.
// It discovers the provider's endpoints and keys when someone first signs in with it, and again
// after a failure, so a provider that is down when ok starts keeps only its own sign-in from
// working, until it is back.
type OIDC struct {
	cfg         OIDCConfig
	redirectURL string
	client      *http.Client

	mu       sync.Mutex
	oauth    *oauth2.Config
	verifier *oidc.IDTokenVerifier
}

// NewOIDC checks a provider's configuration; it reaches the provider on the first sign-in.
func NewOIDC(ctx context.Context, cfg OIDCConfig, redirectURL string) (*OIDC, error) {
	if cfg.ID == "" || cfg.Issuer == "" || cfg.ClientID == "" {
		return nil, errors.New("an OpenID Connect provider needs an id, an issuer, and a client ID")
	}
	if cfg.Match == "" {
		cfg.Match = ByEmail
	}
	if cfg.Claim == "" {
		cfg.Claim = "email"
		if cfg.Match == ByExternalID {
			cfg.Claim = "sub"
		}
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = []string{"email", "profile"}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("the OpenID Connect provider %s's ca_file: %w", cfg.ID, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("the OpenID Connect provider %s's ca_file %s holds no certificate", cfg.ID, cfg.CAFile)
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &OIDC{cfg: cfg, redirectURL: redirectURL, client: &http.Client{Transport: tr, Timeout: 30 * time.Second}}, nil
}

// discover returns the provider's OAuth configuration and token verifier, discovering them the
// first time and after a failure.
func (o *OIDC) discover(ctx context.Context) (*oauth2.Config, *oidc.IDTokenVerifier, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.oauth != nil {
		return o.oauth, o.verifier, nil
	}
	ctx, cancel := context.WithTimeout(oidc.ClientContext(ctx, o.client), 20*time.Second)
	defer cancel()
	p, err := oidc.NewProvider(ctx, o.cfg.Issuer)
	if err != nil {
		return nil, nil, fmt.Errorf("%s cannot be reached to sign you in; try again shortly (%w)", o.cfg.Name, err)
	}
	o.oauth = &oauth2.Config{
		ClientID: o.cfg.ClientID, ClientSecret: o.cfg.ClientSecret, Endpoint: p.Endpoint(),
		RedirectURL: o.redirectURL, Scopes: append([]string{oidc.ScopeOpenID}, o.cfg.Scopes...),
	}
	// The verifier fetches the provider's keys with the provider's client, CA and all.
	o.verifier = p.VerifierContext(oidc.ClientContext(context.Background(), o.client), &oidc.Config{ClientID: o.cfg.ClientID})
	return o.oauth, o.verifier, nil
}

// ID names the provider in URLs.
func (o *OIDC) ID() string { return o.cfg.ID }

// Name is the provider's name, for its button.
func (o *OIDC) Name() string { return o.cfg.Name }

// OIDCState is a sign-in's state between the redirect and its return.
type OIDCState struct {
	Provider string
	Nonce    string
	Verifier string // PKCE
	Next     string
}

// Begin returns the provider's authorization URL for a sign-in whose state the caller keeps under
// stateID.
func (o *OIDC) Begin(ctx context.Context, stateID string, st OIDCState) (string, error) {
	oc, _, err := o.discover(ctx)
	if err != nil {
		return "", err
	}
	opts := []oauth2.AuthCodeOption{oidc.Nonce(st.Nonce), oauth2.S256ChallengeOption(st.Verifier)}
	if o.cfg.HostedDomain != "" {
		opts = append(opts, oauth2.SetAuthURLParam("hd", o.cfg.HostedDomain))
	}
	return oc.AuthCodeURL(stateID, opts...), nil
}

// Finish exchanges the code the provider returned for an ID token, verifies it — issuer,
// audience, expiry, signature, the nonce — and returns the person it names.
func (o *OIDC) Finish(ctx context.Context, people People, st OIDCState, code string) (store.Principal, error) {
	oc, verifier, err := o.discover(ctx)
	if err != nil {
		return store.Principal{}, err
	}
	ctx, cancel := context.WithTimeout(oidc.ClientContext(ctx, o.client), 20*time.Second)
	defer cancel()
	tok, err := oc.Exchange(ctx, code, oauth2.VerifierOption(st.Verifier))
	if err != nil {
		return store.Principal{}, fmt.Errorf("%s refused the sign-in: %w", o.cfg.Name, err)
	}
	raw, ok := tok.Extra("id_token").(string)
	if !ok {
		return store.Principal{}, fmt.Errorf("%s returned no ID token", o.cfg.Name)
	}
	idt, err := verifier.Verify(ctx, raw)
	if err != nil {
		return store.Principal{}, fmt.Errorf("%s's ID token: %w", o.cfg.Name, err)
	}
	if idt.Nonce != st.Nonce {
		return store.Principal{}, errors.New("the ID token is not this sign-in's")
	}
	return o.person(ctx, people, idt)
}

// person matches a verified ID token's claims to a person.
func (o *OIDC) person(ctx context.Context, people People, idt *oidc.IDToken) (store.Principal, error) {
	claims := map[string]any{}
	if err := idt.Claims(&claims); err != nil {
		return store.Principal{}, err
	}
	if o.cfg.HostedDomain != "" && claims["hd"] != o.cfg.HostedDomain {
		return store.Principal{}, fmt.Errorf("you signed in with an account outside %s", o.cfg.HostedDomain)
	}
	value, _ := claims[o.cfg.Claim].(string)
	if o.cfg.Match == ByEmail {
		if verified, ok := claims["email_verified"].(bool); (!ok || !verified) && !o.cfg.TrustUnverifiedEmail {
			return store.Principal{}, fmt.Errorf("%s has not verified your email address", o.cfg.Name)
		}
		value = strings.ToLower(value)
	}
	return find(ctx, people, o.cfg.Match, value)
}
