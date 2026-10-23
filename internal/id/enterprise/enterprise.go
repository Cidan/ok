// Package enterprise is an organization's own identity provider (docs/hosted.md, "Enterprise
// organizations"): what its settings mean — its system of record, Google Workspace, Entra ID, LDAP,
// or SCIM, and its single sign-on, OpenID Connect — its secrets sealed with the installation's key,
// and the sync that provisions and deprovisions its people from it.
package enterprise

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/directory/entra"
	"github.com/Cidan/ok/internal/directory/google"
	"github.com/Cidan/ok/internal/directory/ldapdir"
	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/id/seal"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/ldapdial"
	"github.com/Cidan/ok/internal/netguard"
)

// Settings are an organization's identity provider's, but its secrets.
type Settings struct {
	Google *Google `json:"google,omitempty"`
	Entra  *Entra  `json:"entra,omitempty"`
	LDAP   *LDAP   `json:"ldap,omitempty"`
	SCIM   *SCIM   `json:"scim,omitempty"`
	SignIn *SignIn `json:"sign_in,omitempty"`
}

// Google is a Workspace, read as an administrator.
type Google struct {
	Subject    string `json:"subject"`
	Customer   string `json:"customer,omitempty"`
	GroupQuery string `json:"group_query,omitempty"`
}

// Entra is a Microsoft Entra ID tenant.
type Entra struct {
	TenantID      string `json:"tenant_id"`
	ClientID      string `json:"client_id"`
	GroupFilter   string `json:"group_filter,omitempty"`
	IncludeGuests bool   `json:"include_guests,omitempty"`
}

// LDAP is a directory the organization runs, reached at a public address over TLS.
type LDAP struct {
	URL      string         `json:"url"`
	StartTLS bool           `json:"start_tls,omitempty"`
	CA       string         `json:"ca,omitempty"` // PEM
	BindDN   string         `json:"bind_dn"`
	People   ldapdir.People `json:"people"`
	Groups   ldapdir.Groups `json:"groups"`
}

// SCIM is a provider that pushes to ok, at the organization's endpoint with its token.
type SCIM struct{}

// SignIn is the organization's single sign-on: its people sign in through its OpenID Connect
// provider, matched among them by their external ID (a claim: oid for Entra, sub) or their email.
type SignIn struct {
	Issuer   string `json:"issuer"`
	ClientID string `json:"client_id"`
	Match    string `json:"match"` // external_id, or email
	Claim    string `json:"claim,omitempty"`
}

// Secrets are a provider's secrets, sealed whole.
type Secrets struct {
	GoogleKey         []byte `json:"google_key,omitempty"`
	EntraClientSecret string `json:"entra_client_secret,omitempty"`
	LDAPBindPassword  string `json:"ldap_bind_password,omitempty"`
	OIDCClientSecret  string `json:"oidc_client_secret,omitempty"`
}

// Source is the source a settings' system of record is: google, entra, ldap, or scim.
func (s Settings) Source() (string, error) {
	var named []string
	if s.Google != nil {
		named = append(named, "google")
	}
	if s.Entra != nil {
		named = append(named, "entra")
	}
	if s.LDAP != nil {
		named = append(named, "ldap")
	}
	if s.SCIM != nil {
		named = append(named, "scim")
	}
	if len(named) != 1 {
		return "", errors.New("an identity provider names exactly one system of record: google, entra, ldap, or scim")
	}
	return named[0], nil
}

// Check refuses settings and secrets a provider cannot run with.
func (s Settings) Check(sec Secrets) error {
	if _, err := s.Source(); err != nil {
		return err
	}
	switch {
	case s.Google != nil && (s.Google.Subject == "" || len(sec.GoogleKey) == 0):
		return errors.New("google names the administrator it reads as (subject), and the service account's key")
	case s.Entra != nil && (s.Entra.TenantID == "" || s.Entra.ClientID == "" || sec.EntraClientSecret == ""):
		return errors.New("entra names its tenant_id and client_id, and the client's secret")
	case s.LDAP != nil && (s.LDAP.URL == "" || s.LDAP.People.BaseDN == ""):
		return errors.New("ldap names its url and where its people are (people.base_dn)")
	case s.LDAP != nil && !strings.HasPrefix(s.LDAP.URL, "ldaps://") && !s.LDAP.StartTLS:
		return errors.New("ldap is reached over TLS: ldaps://, or start_tls")
	}
	if si := s.SignIn; si != nil {
		if !strings.HasPrefix(si.Issuer, "https://") || si.ClientID == "" || sec.OIDCClientSecret == "" {
			return errors.New("sign_in names its https issuer and client_id, and the client's secret")
		}
		if si.Match != "email" && si.Match != "external_id" {
			return errors.New("sign_in matches people by email or external_id")
		}
		if si.Match == "external_id" && s.SCIM != nil {
			return errors.New("a SCIM provider's people are known by ok's own IDs, which no sign-in names: match them by email")
		}
	}
	return nil
}

// purpose binds an organization's sealed secrets to it.
func purpose(org string) string { return "org:" + org + ":identity" }

// Seal seals a provider's secrets for an organization.
func Seal(s *seal.Sealer, org string, sec Secrets) ([]byte, error) {
	raw, err := json.Marshal(sec)
	if err != nil {
		return nil, err
	}
	return s.Seal(raw, purpose(org)), nil
}

// Open opens a provider's secrets.
func Open(s *seal.Sealer, p store.IdentityProvider) (Settings, Secrets, error) {
	var set Settings
	if err := json.Unmarshal(p.Settings, &set); err != nil {
		return Settings{}, Secrets{}, err
	}
	var sec Secrets
	if len(p.Sealed) > 0 {
		raw, err := s.Open(p.Sealed, purpose(p.Organization))
		if err != nil {
			return Settings{}, Secrets{}, err
		}
		if err := json.Unmarshal(raw, &sec); err != nil {
			return Settings{}, Secrets{}, err
		}
	}
	return set, sec, nil
}

// SourceOf is the directory.Source an organization's provider is.
func SourceOf(ctx context.Context, st *store.Store, org string, set Settings, sec Secrets, allowPrivate bool) (directory.Source, error) {
	switch {
	case set.Google != nil:
		return google.New(ctx, google.Config{KeyFile: sec.GoogleKey, Subject: set.Google.Subject, Customer: set.Google.Customer,
			GroupQuery: set.Google.GroupQuery})
	case set.Entra != nil:
		return entra.New(ctx, entra.Config{TenantID: set.Entra.TenantID, ClientID: set.Entra.ClientID, ClientSecret: sec.EntraClientSecret,
			GroupFilter: set.Entra.GroupFilter, IncludeGuests: set.Entra.IncludeGuests})
	case set.LDAP != nil:
		return ldapdir.New(ldapdir.Config{
			Dial:   ldapdial.Config{URL: set.LDAP.URL, StartTLS: set.LDAP.StartTLS, CAPEM: []byte(set.LDAP.CA), Dialer: netguard.Dialer(allowPrivate)},
			BindDN: set.LDAP.BindDN, BindPassword: sec.LDAPBindPassword, People: set.LDAP.People, Groups: set.LDAP.Groups,
		})
	case set.SCIM != nil:
		return scim.Source{Store: st.SCIMOf(org)}, nil
	}
	return nil, errors.New("the provider names no system of record")
}

// Syncer syncs organizations' providers: each when its interval is up, or when asked.
type Syncer struct {
	Store  *store.Store
	Sealer *seal.Sealer
	// AllowPrivate lets a development server reach an organization's directory anywhere.
	AllowPrivate bool
	// Poll is how often it looks for syncs due.
	Poll time.Duration
}

// Sync syncs one organization's provider, under the organization's lock, and records how it went.
func (s *Syncer) Sync(ctx context.Context, org string) (*directory.Plan, error) {
	var plan *directory.Plan
	err := s.Store.LockOrganizationSync(ctx, org, time.Minute, func(ctx context.Context) error {
		var err error
		plan, err = s.sync(ctx, org)
		if err != nil {
			if rerr := s.Store.OrganizationSyncFailed(ctx, org, err); rerr != nil {
				slog.Error("enterprise: recording a failed sync", "organization", org, "err", rerr)
			}
		}
		return err
	})
	return plan, err
}

func (s *Syncer) sync(ctx context.Context, org string) (*directory.Plan, error) {
	p, err := s.Store.IdentityProviderOf(ctx, org)
	if err != nil {
		return nil, err
	}
	if o, err := s.Store.Organization(ctx, org); err != nil {
		return nil, err
	} else if !o.Active() {
		return nil, fmt.Errorf("%s is being deleted, and syncs nothing", o.Name)
	}
	set, sec, err := Open(s.Sealer, p)
	if err != nil {
		return nil, fmt.Errorf("opening the provider's secrets: %w", err)
	}
	src, err := SourceOf(ctx, s.Store, org, set, sec, s.AllowPrivate)
	if err != nil {
		return nil, err
	}
	sc := s.Store.OrganizationSync(org)
	opts, err := sc.Options(ctx)
	if err != nil {
		return nil, err
	}
	return directory.Sync(ctx, s.Store.Names(), src, sc, opts)
}

// Start syncs every organization's provider as its interval comes up, until ctx ends: a
// node.Application's.
func (s *Syncer) Start(ctx context.Context) error {
	poll := s.Poll
	if poll == 0 {
		poll = time.Minute
	}
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		due, err := s.Store.DueIdentityProviders(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Error("enterprise: finding syncs due", "err", err)
		}
		for _, org := range due {
			if _, err := s.Sync(ctx, org); err != nil && ctx.Err() == nil {
				slog.Warn("enterprise: an organization's sync", "organization", org, "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
