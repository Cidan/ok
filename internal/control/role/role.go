// Package role assembles ok control: its database (schema control), the site and the ChangeLists
// service on the role's one port, the follower of the SCM's change feed, and its callers resolved
// through ok identity — the SCM, ok automation, the deployer, and identity each dialed with the
// role's own credential.
package role

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/control/api"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/feed"
	"github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	"github.com/Cidan/ok/internal/control/web"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	controlpb "github.com/Cidan/ok/proto/ok/control/v1"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Schema is the PostgreSQL schema ok control keeps its tables in.
const Schema = "control"

// Role is ok control, assembled.
type Role struct {
	Root     node.Node
	Resolver *authn.Resolver
	DB       *db.DB
	Review   *review.Service
	closers  []func()
}

// Close releases what the role holds.
func (r *Role) Close() {
	for i := len(r.closers) - 1; i >= 0; i-- {
		r.closers[i]()
	}
}

// Options are how the role runs beside its configuration.
type Options struct {
	// Dev accepts development principals: a role on loopback, for local development and tests.
	Dev bool
	// Pool, when set, is the database instead of the configuration's: tests.
	Pool *pgxpool.Pool
	// The other roles, when set, dialed as ok control: tests. Unset, each is dialed at its
	// services address.
	Identity, SCM, Automation, Deploy grpc.ClientConnInterface
	// Poll, when set, is how often the ChangeLists service's Watch reads a CL again: tests.
	Poll time.Duration
	// Roots, when set, are the identity CA's certificates as identity in the same process holds
	// them (ok serve); unset, they are read from identity.
	Roots *idclient.Roots
}

// unavailable is a connection to a role the installation does not name: every call is
// UNAVAILABLE, which ok control shows as that role being out of reach.
type unavailable struct{ what string }

func (u unavailable) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return status.Errorf(codes.Unavailable, "%s is not configured (services)", u.what)
}

func (u unavailable) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Errorf(codes.Unavailable, "%s is not configured (services)", u.what)
}

// Build assembles ok control from its configuration.
func Build(ctx context.Context, cfg *config.Config, opts Options) (*Role, error) {
	r := &Role{}
	ok := false
	defer func() {
		if !ok {
			r.Close()
		}
	}()
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
		r.closers = append(r.closers, pool.Close)
	}
	d, err := db.Open(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("ok control's database: %w", err)
	}
	r.DB = d

	var base dial.Options
	var cred dial.Credential
	dialRole := func(given grpc.ClientConnInterface, addr, what string) (grpc.ClientConnInterface, error) {
		if given != nil {
			return given, nil
		}
		if addr == "" {
			return unavailable{what: what}, nil
		}
		if cred == nil {
			if base, err = idclient.Options(cfg.Services); err != nil {
				return nil, err
			}
			if cred, err = idclient.Credential(cfg, base); err != nil {
				return nil, err
			}
		}
		o := base
		o.Credential = cred
		c, err := dial.Dial(addr, o)
		if err != nil {
			return nil, fmt.Errorf("dialing %s: %w", what, err)
		}
		r.closers = append(r.closers, func() { c.Close() })
		return c, nil
	}
	if opts.Identity == nil && cfg.Services.Identity == "" {
		return nil, errors.New("services.identity is where ok control reaches ok identity")
	}
	if opts.SCM == nil && cfg.Services.SCM == "" {
		return nil, errors.New("services.scm is where ok control reaches the SCM")
	}
	idConn, err := dialRole(opts.Identity, cfg.Services.Identity, "ok identity")
	if err != nil {
		return nil, err
	}
	scmConn, err := dialRole(opts.SCM, cfg.Services.SCM, "the SCM")
	if err != nil {
		return nil, err
	}
	autoConn, err := dialRole(opts.Automation, cfg.Services.Automation, "ok automation")
	if err != nil {
		return nil, err
	}
	deployConn, err := dialRole(opts.Deploy, cfg.Services.Deploy, "ok's deployer")
	if err != nil {
		return nil, err
	}

	people.Use(cfg.Names(), cfg.SCM.EmailDomain)
	pages.Brand = cfg.Installation
	roots := opts.Roots
	if roots == nil {
		if roots, err = idclient.NewRoots(ctx, idConn, "control"); err != nil {
			return nil, err
		}
	}
	r.Resolver = authn.NewResolver(authn.Config{
		Directory:     idclient.NewDirectory(idConn),
		Credentials:   credential.NewVerifier(roots.Pool),
		TrustedHeader: cfg.Identity.TrustedHeader,
		Dev:           opts.Dev,
		Roles:         idclient.Roles(cfg),
	})
	dir := people.NewDirectory(identitypb.NewDirectoryClient(idConn))
	sc := scm.New(scmConn)
	runs := automationpb.NewRunsClient(autoConn)
	deploys := deploypb.NewDeploymentsClient(deployConn)
	r.Review = review.New(d, sc, runs, dir, cfg.Identity.TreeRepository)
	site := &site{handler: web.New(d, sc, runs, deploys, r.Review, dir).Handler()}
	cl := api.New(d, sc, runs, deploys, r.Review, cfg.PublicURL)
	if opts.Poll > 0 {
		cl.SetPoll(opts.Poll)
	}
	cls := &changeLists{api: cl}
	follow := &follower{f: feed.New(d, sc, r.Review)}
	r.Root = &assembly{deps: []node.Node{site, cls, follow, roots}}
	ok = true
	return r, nil
}

// site is the web site, at the role's root.
type site struct{ handler http.Handler }

var _ node.WebServer = (*site)(nil)

func (*site) Name() string                   { return "ok/control/site" }
func (*site) Dependencies() []node.Node      { return nil }
func (*site) Setup(context.Context) error    { return nil }
func (s *site) Healthy(context.Context) bool { return true }

func (s *site) Mount(r *chi.Mux) (string, error) {
	r.Mount("/", s.handler)
	return "/", nil
}

// changeLists is the ChangeLists service: a CL's lifecycle for a person, beside the site.
type changeLists struct{ api *api.API }

var _ node.Service = (*changeLists)(nil)

func (*changeLists) Name() string                 { return "ok/control/changelists" }
func (*changeLists) Dependencies() []node.Node    { return nil }
func (*changeLists) Setup(context.Context) error  { return nil }
func (*changeLists) Healthy(context.Context) bool { return true }

func (c *changeLists) RegisterRPC(_ context.Context, reg grpc.ServiceRegistrar) error {
	controlpb.RegisterChangeListsServer(reg, c.api)
	return nil
}

// follower applies the SCM's change feed while the role runs, on one replica at a time.
type follower struct{ f *feed.Follower }

var _ node.Application = (*follower)(nil)

func (*follower) Name() string                      { return "ok/control/follower" }
func (*follower) Dependencies() []node.Node         { return nil }
func (*follower) Setup(context.Context) error       { return nil }
func (*follower) Healthy(context.Context) bool      { return true }
func (f *follower) Start(ctx context.Context) error { return f.f.Run(ctx) }

// assembly is the role's root.
type assembly struct{ deps []node.Node }

func (*assembly) Name() string                 { return "ok/control" }
func (a *assembly) Dependencies() []node.Node  { return a.deps }
func (*assembly) Healthy(context.Context) bool { return true }
func (*assembly) Setup(context.Context) error  { return nil }
