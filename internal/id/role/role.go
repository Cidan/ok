// Package role assembles ok identity: its store and CA, the robots the installation names, the sync
// from its system of record, sign-in, the services, the pages, SCIM, and how its callers are
// resolved.
package role

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/busconfig"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/directory/entra"
	"github.com/Cidan/ok/internal/directory/google"
	"github.com/Cidan/ok/internal/directory/ldapdir"
	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/enterprise"
	"github.com/Cidan/ok/internal/id/federation"
	"github.com/Cidan/ok/internal/id/mail"
	"github.com/Cidan/ok/internal/id/mover"
	"github.com/Cidan/ok/internal/id/seal"
	"github.com/Cidan/ok/internal/id/service"
	"github.com/Cidan/ok/internal/id/signin"
	"github.com/Cidan/ok/internal/id/store"
	idpostgres "github.com/Cidan/ok/internal/id/store/postgres"
	idspanner "github.com/Cidan/ok/internal/id/store/spannerstore"
	"github.com/Cidan/ok/internal/id/syncer"
	"github.com/Cidan/ok/internal/id/web"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/ldapdial"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/spannerdb"
	"github.com/Cidan/ok/internal/tenancy"
	"github.com/Cidan/ok/internal/work"
	okpb "github.com/Cidan/ok/proto/ok/v1"
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
	CA func(context.Context) (*x509.CertPool, error)
	// Mover runs the directory's moves of fork networks between cells.
	Mover   *mover.Runner
	pool    *pgxpool.Pool
	closers []func()
}

// Close releases what the role holds.
func (r *Role) Close() {
	for i := len(r.closers) - 1; i >= 0; i-- {
		r.closers[i]()
	}
	if r.pool != nil {
		r.pool.Close()
	}
}

// Options are how the role runs beside its configuration.
type Options struct {
	// Dev accepts development principals: a role on loopback, for local development.
	Dev bool
	// Pool, when set, is the database instead of the configuration's: tests.
	Pool *pgxpool.Pool
	// Source, when set, is the system of record instead of the configuration's: tests.
	Source directory.Source
	// Peers, when set, connects to a cell's role at an address, as ok identity: tests.
	Peers func(addr string) grpc.ClientConnInterface
}

// OpenBackend opens identity's store backend in the database the configuration names, with what
// closes it: on PostgreSQL its pool, returned too for a bus kept beside it; on Spanner a database
// of identity's own, and no pool. Its keyed work's jobs go on the configuration's bus, which it
// opens too. The admin commands that work on identity's tables beside the role open it so.
func OpenBackend(ctx context.Context, cfg *config.Config, maxConns int32) (store.Backend, *pgxpool.Pool, func(), error) {
	b, pool, done, err := openDB(ctx, cfg, maxConns)
	if err != nil {
		return nil, nil, nil, err
	}
	bb, err := openBus(ctx, cfg, b, pool)
	if err != nil {
		done()
		return nil, nil, nil, err
	}
	return b, pool, func() { bb.Close(); done() }, nil
}

// openBus opens the configuration's bus — over pool, on PostgreSQL — which b's keyed work's jobs go
// on (docs/hosted.md, "Work without leaders").
func openBus(ctx context.Context, cfg *config.Config, b store.Backend, pool *pgxpool.Pool) (*busconfig.Bus, error) {
	bb, err := busconfig.Open(ctx, cfg.Bus)
	if err != nil {
		return nil, err
	}
	if err := bb.RegisterWork(b.Work(), "identity"); err != nil {
		bb.Close()
		return nil, err
	}
	return bb, nil
}

// openDB opens the identity store's database as the configuration says.
func openDB(ctx context.Context, cfg *config.Config, maxConns int32) (store.Backend, *pgxpool.Pool, func(), error) {
	if cfg.Database.Type == "spanner" {
		db, err := spannerdb.Open(ctx, cfg.Database.Spanner, "identity")
		if err != nil {
			return nil, nil, nil, err
		}
		b, err := idspanner.Open(ctx, db.Admin, db.Client)
		if err != nil {
			db.Close()
			return nil, nil, nil, fmt.Errorf("the identity store: %w", err)
		}
		return b, nil, db.Close, nil
	}
	dsn, err := config.Secret(cfg.Database.URLFile)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("database.url_file: %w", err)
	}
	if dsn == "" {
		return nil, nil, nil, errors.New("database.url_file names no database")
	}
	pool, err := pgdb.Open(ctx, dsn, Schema, maxConns)
	if err != nil {
		return nil, nil, nil, err
	}
	b, err := idpostgres.Open(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, nil, nil, fmt.Errorf("the identity store: %w", err)
	}
	return b, pool, pool.Close, nil
}

// Build assembles the identity role from its configuration.
func Build(ctx context.Context, cfg *config.Config, opts Options) (*Role, error) {
	names := cfg.Names()
	var b store.Backend
	pool := opts.Pool
	r := &Role{pool: pool}
	if pool == nil {
		var closeDB func()
		var err error
		if b, pool, closeDB, err = openDB(ctx, cfg, cfg.Database.MaxConnections); err != nil {
			return nil, err
		}
		r.closers = append(r.closers, closeDB)
	} else {
		pb, err := idpostgres.Open(ctx, pool)
		if err != nil {
			return nil, fmt.Errorf("the identity store: %w", err)
		}
		b = pb
	}
	// The directory's events are relayed, and its keyed work's jobs go, on the bus.
	bb, err := openBus(ctx, cfg, b, pool)
	if err != nil {
		for i := len(r.closers) - 1; i >= 0; i-- {
			r.closers[i]()
		}
		return nil, err
	}
	r.closers = append(r.closers, bb.Close)
	ok := false
	defer func() {
		if ok {
			return
		}
		for i := len(r.closers) - 1; i >= 0; i-- {
			r.closers[i]()
		}
	}()
	st := store.New(b, names)
	r.Store = st
	// An installation's one organization (docs/hosted.md, "Tenancy, as built"): its members are its
	// people and robots, its owners its administrators, and every person may make and land.
	var installation string
	movedIn, err := st.CheckMode(ctx, cfg.Tenancy.Hosted())
	if err != nil {
		return nil, fmt.Errorf("tenancy.mode: %w", err)
	}
	st.SetMovedIn(movedIn)
	var cells []store.Cell
	for _, c := range cfg.Cells() {
		cells = append(cells, store.Cell{Name: c.Name, Region: c.Region, Open: !c.Closed})
	}
	st.SetCells(cells, cfg.Identity.DefaultRegion)
	dq := cfg.Identity.DefaultQuota
	st.SetDefaultQuota(store.Quota{Concurrent: dq.Concurrent, Minutes: dq.Minutes})
	if !cfg.Tenancy.Hosted() {
		o, err := st.EnsureOrganization(ctx, cfg.Tenancy.Organization, store.OrganizationSettings{BaseRole: tenancy.Write, MembersCreate: true})
		if err != nil {
			return nil, fmt.Errorf("tenancy.organization %s: %w", cfg.Tenancy.Organization, err)
		}
		installation = o.ID
		st.SetInstallation(o.ID)
	}
	authority, err := ca.Load(cfg.Identity.CADir)
	if err != nil {
		return nil, fmt.Errorf("identity.ca_dir: %w (make one with: ok identity ca init)", err)
	}

	// The robots the installation names: its roles', whose calls within the process are theirs
	// (authn.Config.InProcess).
	roles := authn.Roles{}
	inProcess := map[string]authn.Principal{}
	for role, short := range cfg.Roles.All() {
		name := names.Robot(short)
		p, err := st.EnsureRobot(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("roles.%s: %w", role, err)
		}
		roles[idclient.Callers[role]] = name
		inProcess[name] = authn.Principal{Name: p.Name, ID: p.ID, Active: true}
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
	nodes = append(nodes, syncer.Housekeeper{Store: st}, waits{store: st, q: bb.Queue})
	// The directory's events, published for ok's roles to follow.
	nodes = append(nodes, relay{store: st, q: bb.Queue})

	var syncerFor service.Syncer
	if sync != nil {
		syncerFor = sync
	}
	// Organizations' own identity providers, their secrets sealed with the installation's key.
	var ent *enterprise.Syncer
	if cfg.Identity.SealingKeyFile != "" {
		sealer, err := seal.Load(cfg.Identity.SealingKeyFile)
		if err != nil {
			return nil, fmt.Errorf("identity.sealing_key_file: %w", err)
		}
		ent = &enterprise.Syncer{Store: st, Sealer: sealer, AllowPrivate: opts.Dev}
		nodes = append(nodes, enterpriseSync{ent}, &orgSCIM{st: st, base: strings.TrimSuffix(cfg.PublicURL, "/")})
	}
	// Notifications' mail, sent as keyed work (docs/hosted.md, "ok control at scale, as built").
	if m := cfg.Identity.Mail; m != nil {
		sender, err := smtpSender("identity.mail.smtp", m.SMTP)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, &mailer{store: st, send: sender.Send})
	}
	// WebAuthn binds a passkey to a domain: ok reached at an IP address — in development — has none.
	publicURL, err := url.Parse(cfg.PublicURL)
	if err != nil {
		return nil, fmt.Errorf("public_url: %w", err)
	}
	passkeys := net.ParseIP(publicURL.Hostname()) == nil
	svc := service.New(service.Config{Store: st, CA: authority, Syncer: syncerFor, PublicURL: cfg.PublicURL, Passkeys: passkeys, Mail: cfg.Identity.Mail != nil,
		Enterprise:      ent,
		BootstrapAdmins: cfg.Identity.BootstrapAdmins, SessionIdle: cfg.Identity.Session.Idle,
		Hosted: cfg.Tenancy.Hosted(), Installation: installation, Roles: idclient.Roles(cfg), PlaintextIssuers: opts.Dev,
		Cells: cfg.Cells()})
	r.Services = svc
	nodes = append(nodes, services{s: svc, ca: authority})

	// Moves of fork networks between cells, driven here, each cell's roles reached as ok
	// identity's robot (docs/hosted.md, "Cells, as built").
	local := idclient.NewLocal(svc, cfg.Roles.Identity)
	dialer := &moveDialer{conns: map[string]okpb.MovesClient{}, dial: func(addr string) (grpc.ClientConnInterface, error) {
		if opts.Peers != nil {
			return opts.Peers(addr), nil
		}
		o, err := idclient.Options(cfg.Services)
		if err != nil {
			return nil, err
		}
		if opts.Dev {
			o.DevPrincipal = cfg.RobotName(cfg.Roles.Identity)
		} else {
			o.Credential = local
		}
		c, err := dial.Dial(addr, o)
		if err != nil {
			return nil, err
		}
		r.closers = append(r.closers, func() { c.Close() })
		return c, nil
	}}
	r.Mover = &mover.Runner{Store: st, Cells: cfg.Cells(), Log: slog.Default(), Moves: dialer.moves}
	nodes = append(nodes, r.Mover)

	wc := web.Config{Store: st, Services: svc, Installation: cfg.Installation, PublicURL: cfg.PublicURL,
		SessionLifetime: cfg.Identity.Session.Lifetime, SessionIdle: cfg.Identity.Session.Idle}
	callback := strings.TrimSuffix(cfg.PublicURL, "/") + "/id/login/oidc/callback"
	for _, o := range cfg.Identity.SignIn.OIDC {
		secret, err := config.Secret(o.ClientSecretFile)
		if err != nil {
			return nil, fmt.Errorf("identity.sign_in.oidc.%s: %w", o.ID, err)
		}
		p, err := signin.NewOIDC(ctx, signin.OIDCConfig{ID: o.ID, Name: o.Name, Issuer: o.Issuer, ClientID: o.ClientID, ClientSecret: secret,
			Scopes: o.Scopes, Match: signin.Match(o.Match), Claim: o.Claim, TrustUnverifiedEmail: o.TrustUnverifiedEmail, HostedDomain: o.HostedDomain,
			CAFile: o.CAFile}, callback)
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
	if passkeys {
		if wc.Passkeys, err = signin.NewPasskeys(publicURL.Hostname(), cfg.Installation, strings.TrimSuffix(cfg.PublicURL, "/"), st); err != nil {
			return nil, err
		}
	}
	wc.Enterprise = ent
	wc.Hosted = cfg.Tenancy.Hosted()
	if su := cfg.Identity.SignUp; su != nil {
		wc.SignUp = &web.SignUp{Social: map[string]bool{}}
		for _, id := range su.OIDC {
			wc.SignUp.Social[id] = true
		}
		if e := su.Email; e != nil {
			sender, err := smtpSender("identity.sign_up.email.smtp", e.SMTP)
			if err != nil {
				return nil, err
			}
			wc.SignUp.Mail = sender
		}
	}
	nodes = append(nodes, web.New(wc))

	// Robots sign in with tokens from the configuration's issuers, and from each organization's
	// (a development server reaching an organization's at any address, over plaintext too).
	var issuers []federation.Issuer
	for _, i := range cfg.Identity.Federation {
		issuers = append(issuers, federation.Issuer{URL: i.Issuer, Audience: i.Audience, JWKSURL: i.JWKSURL, CAFile: i.CAFile, TokenFile: i.TokenFile})
	}
	tokens, err := federation.New(ctx, issuers, st, federation.Options{AllowPrivate: opts.Dev})
	if err != nil {
		return nil, err
	}
	r.CA = authority.Pool
	r.Resolver = authn.NewResolver(authn.Config{
		Directory:     service.Directory{Store: st, Idle: cfg.Identity.Session.Idle},
		Credentials:   credential.NewVerifier(authority.Pool),
		Tokens:        tokens,
		TrustedHeader: cfg.Identity.TrustedHeader,
		Dev:           opts.Dev,
		Roles:         roles,
		InProcess:     inProcess,
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

// services is the gRPC services' node, healthy while the CA can be read: without it, every
// credential is refused.
type services struct {
	s  *service.Server
	ca *ca.CA
}

func (services) Name() string                   { return "ok/identity/services" }
func (services) Dependencies() []node.Node      { return nil }
func (n services) Healthy(context.Context) bool { return n.ca == nil || n.ca.Readable() == nil }
func (services) Setup(context.Context) error    { return nil }
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

// relay publishes the directory's events: every replica runs one, each publishing pending events
// beside the others (store.Relay).
// mailer sends the notifications' mail, each message on one replica at a time, as it is queued and
// as its retries fall due: its work marked by the transaction that queues it, and by each try.
type mailer struct {
	store *store.Store
	send  store.Sender
}

var _ node.Application = (*mailer)(nil)

func (mailer) Name() string                 { return "ok/identity/mail" }
func (mailer) Dependencies() []node.Node    { return nil }
func (mailer) Setup(context.Context) error  { return nil }
func (mailer) Healthy(context.Context) bool { return true }

func (m mailer) Start(ctx context.Context) error {
	return (&work.Runner{
		Kind:  store.MailWork,
		Store: m.store.Work(),
		Do:    func(ctx context.Context, id int64) error { return m.store.SendMail(ctx, id, m.send) },
		Log:   slog.Default(),
	}).Run(ctx)
}

// waits removes the lapsed registrations of the live requests waiting on identity's claims, on
// every replica (store.RunWaits).
type waits struct {
	store *store.Store
	q     bus.Queue
}

var _ node.Application = waits{}

func (waits) Name() string                      { return "ok/identity/waits" }
func (waits) Dependencies() []node.Node         { return nil }
func (waits) Setup(context.Context) error       { return nil }
func (waits) Healthy(context.Context) bool      { return true }
func (w waits) Start(ctx context.Context) error { w.store.RunWaits(ctx, w.q); return nil }

type relay struct {
	store *store.Store
	q     bus.Queue
}

var _ node.Application = relay{}

func (relay) Name() string                 { return "ok/identity/relay" }
func (relay) Dependencies() []node.Node    { return nil }
func (relay) Setup(context.Context) error  { return nil }
func (relay) Healthy(context.Context) bool { return true }

func (n relay) Start(ctx context.Context) error {
	if err := n.store.Relay(ctx, n.q); ctx.Err() == nil {
		return err
	}
	return nil
}

// moveDialer reaches cells' roles' Moves, each address dialed once.
type moveDialer struct {
	dial  func(addr string) (grpc.ClientConnInterface, error)
	mu    sync.Mutex
	conns map[string]okpb.MovesClient
}

func (d *moveDialer) moves(addr string) (okpb.MovesClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.conns[addr]; ok {
		return c, nil
	}
	conn, err := d.dial(addr)
	if err != nil {
		return nil, err
	}
	d.conns[addr] = okpb.NewMovesClient(conn)
	return d.conns[addr], nil
}

// smtpSender is a mail server's sender, as the configuration at what names it.
func smtpSender(what string, c config.SMTP) (mail.SMTP, error) {
	sender := mail.SMTP{Addr: c.Address, Username: c.Username, From: c.From}
	if c.PasswordFile != "" {
		pw, err := os.ReadFile(c.PasswordFile)
		if err != nil {
			return mail.SMTP{}, fmt.Errorf("%s.password_file: %w", what, err)
		}
		sender.Password = strings.TrimSpace(string(pw))
	}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return mail.SMTP{}, fmt.Errorf("%s.ca_file: %w", what, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return mail.SMTP{}, fmt.Errorf("%s.ca_file %s holds no certificate", what, c.CAFile)
		}
		sender.TLS = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return sender, nil
}
