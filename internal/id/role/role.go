// Package role assembles ok identity: its store and CA, the robots the installation names, the sync
// from its system of record, sign-in, the services, the pages, SCIM, and how its callers are
// resolved.
package role

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/directory/entra"
	"github.com/Cidan/ok/internal/directory/google"
	"github.com/Cidan/ok/internal/directory/ldapdir"
	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/federation"
	"github.com/Cidan/ok/internal/id/service"
	"github.com/Cidan/ok/internal/id/signin"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/id/syncer"
	"github.com/Cidan/ok/internal/id/web"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/ldapdial"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
)

// Schema is the PostgreSQL schema the identity role keeps its tables in.
const Schema = "identity"

// Role is the identity role, assembled.
type Role struct {
	// Root is the node graph's root.
	Root node.Node
	// Resolver resolves the role's callers.
	Resolver *authn.Resolver
	// Store is the role's store.
	Store *store.Store
	// Services are the role's gRPC services.
	Services *service.Server
	// CA is the identity CA's certificates, which credentials are checked against: for roles in
	// the same process (ok serve).
	CA   func() *x509.CertPool
	pool *pgxpool.Pool
}

// Close releases what the role holds.
func (r *Role) Close() { r.pool.Close() }

// Options are how the role runs beside its configuration.
type Options struct {
	// Dev accepts development principals: a role on loopback, for local development.
	Dev bool
	// Pool, when set, is the database instead of the configuration's: tests.
	Pool *pgxpool.Pool
	// Source, when set, is the system of record instead of the configuration's: tests.
	Source directory.Source
}

// Build assembles the identity role from its configuration.
func Build(ctx context.Context, cfg *config.Config, opts Options) (*Role, error) {
	names := cfg.Names()
	pool := opts.Pool
	if pool == nil {
		dsn, err := config.Secret(cfg.Database.URLFile)
		if err != nil {
			return nil, fmt.Errorf("database.url_file: %w", err)
		}
		if dsn == "" {
			return nil, errors.New("database.url_file names no database")
		}
		if pool, err = pgdb.Open(ctx, dsn, Schema, cfg.Database.MaxConnections); err != nil {
			return nil, err
		}
	}
	r := &Role{pool: pool}
	ok := false
	defer func() {
		if !ok && opts.Pool == nil {
			pool.Close()
		}
	}()
	st, err := store.Open(ctx, pool, names)
	if err != nil {
		return nil, fmt.Errorf("the identity store: %w", err)
	}
	r.Store = st
	authority, err := ca.Load(cfg.Identity.CADir)
	if err != nil {
		return nil, fmt.Errorf("identity.ca_dir: %w (make one with: ok identity ca init)", err)
	}

	// The robots the installation names.
	roles := authn.Roles{}
	for role, short := range cfg.Roles.All() {
		name := names.Robot(short)
		if _, err := st.EnsureRobot(ctx, name); err != nil {
			return nil, fmt.Errorf("roles.%s: %w", role, err)
		}
		roles[idclient.Callers[role]] = name
	}
	for _, rb := range cfg.Identity.Robots {
		name := names.Robot(rb.Name)
		if _, err := st.EnsureRobot(ctx, name); err != nil {
			return nil, fmt.Errorf("identity.robots: %w", err)
		}
		for _, b := range rb.Bindings {
			if err := st.EnsureBinding(ctx, name, store.Binding{Issuer: b.Issuer, Subject: b.Subject}); err != nil {
				return nil, fmt.Errorf("identity.robots: %w", err)
			}
		}
	}

	nodes := []node.Node{}
	var sync *syncer.Syncer
	var scimNode node.Node
	if sor := cfg.Identity.SystemOfRecord; sor != nil || opts.Source != nil {
		src, kind, err := source(ctx, sor, st)
		if err != nil {
			return nil, err
		}
		if opts.Source != nil {
			src, kind = opts.Source, "test"
		}
		interval := 10 * time.Minute
		if sor != nil {
			interval = sor.Interval
		}
		sync = syncer.New(st, src, kind, interval)
		nodes = append(nodes, sync)
		if sor != nil && sor.SCIM != nil {
			tok, err := config.Secret(sor.SCIM.TokenFile)
			if err != nil {
				return nil, fmt.Errorf("identity.system_of_record.scim.token_file: %w", err)
			}
			h, err := scim.NewHandler(st.SCIM(), tok, strings.TrimSuffix(cfg.PublicURL, "/")+"/id/scim/v2", sync.Changed)
			if err != nil {
				return nil, fmt.Errorf("identity.system_of_record.scim: %w", err)
			}
			scimNode = scimWeb{h}
			nodes = append(nodes, scimNode)
		}
	}
	nodes = append(nodes, syncer.Housekeeper{Store: st})

	var syncerFor service.Syncer
	if sync != nil {
		syncerFor = sync
	}
	svc := service.New(service.Config{Store: st, CA: authority, Syncer: syncerFor, PublicURL: cfg.PublicURL,
		BootstrapAdmins: cfg.Identity.BootstrapAdmins, SessionIdle: cfg.Identity.Session.Idle})
	r.Services = svc
	nodes = append(nodes, services{svc})

	wc := web.Config{Store: st, Services: svc, Installation: cfg.Installation, PublicURL: cfg.PublicURL,
		SessionLifetime: cfg.Identity.Session.Lifetime, SessionIdle: cfg.Identity.Session.Idle}
	callback := strings.TrimSuffix(cfg.PublicURL, "/") + "/id/login/oidc/callback"
	for _, o := range cfg.Identity.SignIn.OIDC {
		secret, err := config.Secret(o.ClientSecretFile)
		if err != nil {
			return nil, fmt.Errorf("identity.sign_in.oidc.%s: %w", o.ID, err)
		}
		p, err := signin.NewOIDC(ctx, signin.OIDCConfig{ID: o.ID, Name: o.Name, Issuer: o.Issuer, ClientID: o.ClientID, ClientSecret: secret,
			Scopes: o.Scopes, Match: signin.Match(o.Match), Claim: o.Claim, TrustUnverifiedEmail: o.TrustUnverifiedEmail, HostedDomain: o.HostedDomain}, callback)
		if err != nil {
			return nil, err
		}
		wc.OIDC = append(wc.OIDC, p)
	}
	if l := cfg.Identity.SignIn.LDAP; l != nil {
		pw, err := config.Secret(l.BindPasswordFile)
		if err != nil {
			return nil, fmt.Errorf("identity.sign_in.ldap: %w", err)
		}
		a, err := signin.NewLDAP(signin.LDAPConfig{URL: l.URL, StartTLS: l.StartTLS, CAFile: l.CAFile, BindDN: l.BindDN, BindPassword: pw,
			BaseDN: l.BaseDN, UserFilter: l.UserFilter, Match: signin.Match(l.Match), Attribute: l.Attribute})
		if err != nil {
			return nil, fmt.Errorf("identity.sign_in.ldap: %w", err)
		}
		wc.LDAP, wc.LDAPName = a, l.Name
		if wc.LDAPName == "" {
			wc.LDAPName = "your directory"
		}
	}
	// WebAuthn binds a passkey to a domain: ok reached at an IP address — in development — has none.
	if u, _ := url.Parse(cfg.PublicURL); net.ParseIP(u.Hostname()) == nil {
		if wc.Passkeys, err = signin.NewPasskeys(u.Hostname(), cfg.Installation, strings.TrimSuffix(cfg.PublicURL, "/"), st); err != nil {
			return nil, err
		}
	}
	nodes = append(nodes, web.New(wc))

	var tokens authn.TokenVerifier
	if len(cfg.Identity.Federation) > 0 {
		var issuers []federation.Issuer
		for _, i := range cfg.Identity.Federation {
			issuers = append(issuers, federation.Issuer{URL: i.Issuer, Audience: i.Audience, JWKSURL: i.JWKSURL, CAFile: i.CAFile, TokenFile: i.TokenFile})
		}
		if tokens, err = federation.New(ctx, issuers, st); err != nil {
			return nil, err
		}
	}
	r.CA = authority.Pool
	r.Resolver = authn.NewResolver(authn.Config{
		Directory:     service.Directory{Store: st, Idle: cfg.Identity.Session.Idle},
		Credentials:   credential.NewVerifier(authority.Pool),
		Tokens:        tokens,
		TrustedHeader: cfg.Identity.TrustedHeader,
		Dev:           opts.Dev,
		Roles:         roles,
	})
	r.Root = &assembly{deps: nodes}
	ok = true
	return r, nil
}

// source makes the system of record the configuration names.
func source(ctx context.Context, sor *config.SystemOfRecord, st *store.Store) (directory.Source, string, error) {
	switch {
	case sor == nil:
		return nil, "", nil
	case sor.Static != nil:
		return directory.Static{Path: sor.Static.File}, "static", nil
	case sor.Google != nil:
		key, err := os.ReadFile(sor.Google.KeyFile)
		if err != nil {
			return nil, "", fmt.Errorf("identity.system_of_record.google.key_file: %w", err)
		}
		s, err := google.New(ctx, google.Config{KeyFile: key, Subject: sor.Google.Subject, Customer: sor.Google.Customer, GroupQuery: sor.Google.GroupQuery})
		return s, "google", err
	case sor.Entra != nil:
		secret, err := config.Secret(sor.Entra.ClientSecretFile)
		if err != nil {
			return nil, "", fmt.Errorf("identity.system_of_record.entra.client_secret_file: %w", err)
		}
		s, err := entra.New(ctx, entra.Config{TenantID: sor.Entra.TenantID, ClientID: sor.Entra.ClientID, ClientSecret: secret,
			GroupFilter: sor.Entra.GroupFilter, IncludeGuests: sor.Entra.IncludeGuests})
		return s, "entra", err
	case sor.LDAP != nil:
		l := sor.LDAP
		pw, err := config.Secret(l.BindPasswordFile)
		if err != nil {
			return nil, "", fmt.Errorf("identity.system_of_record.ldap.bind_password_file: %w", err)
		}
		s, err := ldapdir.New(ldapdir.Config{
			Dial:   ldapdial.Config{URL: l.URL, StartTLS: l.StartTLS, CAFile: l.CAFile},
			BindDN: l.BindDN, BindPassword: pw,
			People: ldapdir.People{BaseDN: l.People.BaseDN, Filter: l.People.Filter, DisabledFilter: l.People.DisabledFilter,
				IDAttribute: l.People.IDAttribute, HandleAttribute: l.People.HandleAttribute, EmailAttribute: l.People.EmailAttribute, NameAttribute: l.People.NameAttribute},
			Groups: ldapdir.Groups{BaseDN: l.Groups.BaseDN, Filter: l.Groups.Filter, IDAttribute: l.Groups.IDAttribute,
				NameAttribute: l.Groups.NameAttribute, MemberAttribute: l.Groups.MemberAttribute},
		})
		return s, "ldap", err
	case sor.SCIM != nil:
		return scim.Source{Store: st.SCIM()}, "scim", nil
	}
	return nil, "", errors.New("identity.system_of_record names no source")
}

// assembly is the role's root.
type assembly struct{ deps []node.Node }

func (assembly) Name() string                 { return "ok/identity" }
func (a assembly) Dependencies() []node.Node  { return a.deps }
func (assembly) Healthy(context.Context) bool { return true }
func (assembly) Setup(context.Context) error  { return nil }

// services is the gRPC services' node.
type services struct{ s *service.Server }

func (services) Name() string                 { return "ok/identity/services" }
func (services) Dependencies() []node.Node    { return nil }
func (services) Healthy(context.Context) bool { return true }
func (services) Setup(context.Context) error  { return nil }
func (n services) RegisterRPC(_ context.Context, r grpc.ServiceRegistrar) error {
	n.s.Register(r)
	return nil
}

// scimWeb serves SCIM under /id/scim, authenticating clients itself.
type scimWeb struct{ h *scim.Handler }

var _ node.SelfAuthenticated = scimWeb{}

func (scimWeb) Name() string                 { return "ok/identity/scim" }
func (scimWeb) Dependencies() []node.Node    { return nil }
func (scimWeb) Healthy(context.Context) bool { return true }
func (scimWeb) Setup(context.Context) error  { return nil }
func (scimWeb) AuthenticatesItself()         {}
func (s scimWeb) Mount(r *chi.Mux) (string, error) {
	r.Handle("/*", http.Handler(s.h))
	return "/id/scim", nil
}
