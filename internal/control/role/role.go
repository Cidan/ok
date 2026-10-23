// Package role assembles ok control: its database (schema control), the site and the ChangeLists
// service on the role's one port, the follower of the SCM's change feed, and its callers resolved
// through ok identity — the SCM, ok automation, the deployer, and identity each dialed with the
// role's own credential.
package role

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/busconfig"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/control/api"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/db/postgres"
	"github.com/Cidan/ok/internal/control/db/spannerstore"
	"github.com/Cidan/ok/internal/control/feed"
	"github.com/Cidan/ok/internal/control/outbox"
	"github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	"github.com/Cidan/ok/internal/control/web"
	"github.com/Cidan/ok/internal/control/webhooks"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/seal"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/names"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/spannerdb"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	controlpb "github.com/Cidan/ok/proto/ok/control/v1"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Schema is the PostgreSQL schema ok control keeps its tables in.
const Schema = "control"

// Role is ok control, assembled.
type Role struct {
	Root     node.Node
	Resolver *authn.Resolver
	DB       db.Store
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
	// Pool and Stream, when set, are the database and the bus's stream instead of the
	// configuration's: tests.
	Pool   *pgxpool.Pool
	Stream bus.Stream
	// The other roles, when set, dialed as ok control: tests. Unset, each is dialed at its
	// services address.
	Identity, SCM, Automation, Deploy grpc.ClientConnInterface
	// Poll, when set, is how often the ChangeLists service's Watch reads a CL again, and the outboxes
	// are looked at: tests.
	Poll time.Duration
	// Roots, when set, are the identity CA's certificates as identity in the same process holds
	// them (ok serve); unset, they are read from identity.
	Roots *idclient.Roots
	// Peers, when set, connects to another cell's ok control at an address, as the role: tests.
	Peers func(addr string) grpc.ClientConnInterface
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

// noDeployer is the deployer of an installation that names none (services.deploy): one that
// deploys nothing, so a landed CL has nothing to deploy and no repository a service — rather than a
// deployer out of reach. What only a deployment could answer stays unavailable.
type noDeployer struct{ deploypb.DeploymentsClient }

func (noDeployer) ListDeployments(context.Context, *deploypb.ListDeploymentsRequest, ...grpc.CallOption) (*deploypb.ListDeploymentsResponse, error) {
	return &deploypb.ListDeploymentsResponse{Planned: true}, nil
}

func (noDeployer) ListServices(context.Context, *deploypb.ListServicesRequest, ...grpc.CallOption) (*deploypb.ListServicesResponse, error) {
	return &deploypb.ListServicesResponse{}, nil
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
	var d db.Store
	var err error
	pool := opts.Pool
	if pool == nil && cfg.Database.Type == "spanner" {
		// Its own database in the instance; the bus is another backend's (config).
		sdb, err := spannerdb.Open(ctx, cfg.Database.Spanner, "control")
		if err != nil {
			return nil, err
		}
		r.closers = append(r.closers, sdb.Close)
		if d, err = spannerstore.Open(ctx, sdb.Admin, sdb.Client); err != nil {
			return nil, fmt.Errorf("ok control's database: %w", err)
		}
	} else {
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
		pd, err := postgres.Open(ctx, pool)
		if err != nil {
			return nil, fmt.Errorf("ok control's database: %w", err)
		}
		d = pd
	}
	stream := opts.Stream
	if stream == nil {
		bb, err := busconfig.Open(ctx, cfg.Bus, pool)
		if err != nil {
			return nil, err
		}
		r.closers = append(r.closers, bb.Close)
		stream = bb.Stream
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

	people.Use(cfg.Names(), cfg.SCM.EmailDomain, names.New(idclient.NewDirectory(idConn)), cfg.Tenancy.Hosted())
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
	var deploys deploypb.DeploymentsClient = deploypb.NewDeploymentsClient(deployConn)
	if opts.Deploy == nil && cfg.Services.Deploy == "" {
		deploys = noDeployer{deploys}
	}
	access := review.NewAccess(identitypb.NewAuthorizationClient(idConn))
	r.Review = review.New(d, sc, runs, dir, access, cfg.Identity.TreeRepository)
	installation := ""
	if !cfg.Tenancy.Hosted() {
		installation = cfg.Tenancy.Organization
	}
	// Repositories' webhooks, their secrets sealed under the cells' shared key (docs/hosted.md, "ok
	// control at scale, as built").
	hooks := &webhooks.Hooks{DB: d, AllowPrivate: cfg.Control.Webhooks.AllowPrivateNetworks, PublicURL: cfg.PublicURL, Installation: installation,
		Log: slog.Default(), Look: opts.Poll}
	if f := cfg.Control.SealingKeyFile; f != "" {
		if hooks.Sealer, err = seal.Load(f); err != nil {
			return nil, fmt.Errorf("control.sealing_key_file: %w", err)
		}
	}
	site := &site{handler: web.New(d, sc, runs, deploys, r.Review, dir, idConn, hooks, installation).Handler()}
	cl := api.New(d, sc, runs, deploys, r.Review, cfg.PublicURL, installation)
	if opts.Poll > 0 {
		cl.SetPoll(opts.Poll)
	}
	cls := &changeLists{api: cl}
	follow := &follower{f: feed.New(d, stream, sc, r.Review)}
	directory := &directoryFollower{f: feed.NewDirectory(d, stream, identitypb.NewDirectoryEventsClient(idConn), access.Forget)}
	// Moves of its repositories' rows between cells, from the same role of the source cell
	// (docs/hosted.md, "Cells, as built").
	peers := cells.NewPeers(identitypb.NewCellsClient(idConn), func(addr string) (grpc.ClientConnInterface, func(), error) {
		if opts.Peers != nil {
			return opts.Peers(addr), nil, nil
		}
		if opts.Dev && cfg.Workload.TokenFile == "" && cfg.Workload.CredentialDir == "" {
			// Development, with no workload credential: ok control names itself to the other
			// cell's development ok control, as to the roles of its own.
			o, err := idclient.Options(cfg.Services)
			if err != nil {
				return nil, nil, err
			}
			o.DevPrincipal = cfg.RobotName(cfg.Roles.Control)
			c, err := dial.Dial(addr, o)
			if err != nil {
				return nil, nil, err
			}
			return c, func() { c.Close() }, nil
		}
		c, err := dialRole(nil, addr, "another cell's ok control")
		return c, nil, err
	})
	r.closers = append(r.closers, peers.Close)
	mover := &move.RepositoryRole{RoleName: cells.Control, Rows: d.Moves(), Arrived: func(ctx context.Context, repos []int64) error {
		// Its row names it as the directory does; its head is what this cell's SCM's feed would
		// have recorded, had the repository been here.
		repositories := scmpb.NewRepositoriesClient(scmConn)
		for _, id := range repos {
			r, err := repositories.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: id}})
			if err != nil {
				return fmt.Errorf("repository %d at this cell's SCM: %w", id, err)
			}
			if err := d.MoveHead(ctx, id, r.GetHeadSeq(), time.Now()); err != nil {
				return err
			}
		}
		return nil
	}}
	moves := move.NewNode("ok/control/moves", move.NewServer(mover, okpb.Caller_CALLER_CONTROL,
		func(ctx context.Context, cell string) (okpb.MovesClient, error) {
			c, err := peers.Conn(ctx, cell, cells.Control)
			if err != nil {
				return nil, err
			}
			return okpb.NewMovesClient(c), nil
		}), mover.Purge)
	// The outboxes' draining: inboxes, notifications, webhooks (docs/hosted.md, "ok control at scale,
	// as built").
	delivery := identitypb.NewDeliveryClient(idConn)
	drain := &outboxDrainer{d: &outbox.Drainer{DB: d, Delivery: delivery, Tenancy: identitypb.NewTenancyClient(idConn),
		Consumers: []outbox.Consumer{&outbox.Notifier{DB: d, Delivery: delivery, Installation: installation}}, Log: slog.Default(), Look: opts.Poll}}
	if hooks.Sealer != nil {
		drain.d.Consumers = append(drain.d.Consumers, hooks)
	}
	deps := []node.Node{site, cls, follow, directory, moves, drain, roots}
	if hooks.Sealer != nil {
		deps = append(deps, &webhookSender{h: hooks})
	}
	r.Root = &assembly{deps: deps}
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

// directoryFollower applies the directory's events while the role runs: repositories' names.
type directoryFollower struct{ f *feed.Directory }

var _ node.Application = (*directoryFollower)(nil)

func (*directoryFollower) Name() string                      { return "ok/control/directory" }
func (*directoryFollower) Dependencies() []node.Node         { return nil }
func (*directoryFollower) Setup(context.Context) error       { return nil }
func (*directoryFollower) Healthy(context.Context) bool      { return true }
func (f *directoryFollower) Start(ctx context.Context) error { return f.f.Run(ctx) }

// outboxDrainer drains the outboxes on every replica, each repository's on one at a time.
type outboxDrainer struct{ d *outbox.Drainer }

var _ node.Application = (*outboxDrainer)(nil)

func (*outboxDrainer) Name() string                      { return "ok/control/outbox" }
func (*outboxDrainer) Dependencies() []node.Node         { return nil }
func (*outboxDrainer) Setup(context.Context) error       { return nil }
func (*outboxDrainer) Healthy(context.Context) bool      { return true }
func (o *outboxDrainer) Start(ctx context.Context) error { return o.d.Run(ctx) }

// webhookSender sends webhooks' deliveries on every replica, each repository's on one at a time.
type webhookSender struct{ h *webhooks.Hooks }

var _ node.Application = (*webhookSender)(nil)

func (*webhookSender) Name() string                      { return "ok/control/webhooks" }
func (*webhookSender) Dependencies() []node.Node         { return nil }
func (*webhookSender) Setup(context.Context) error       { return nil }
func (*webhookSender) Healthy(context.Context) bool      { return true }
func (w *webhookSender) Start(ctx context.Context) error { return w.h.Run(ctx) }

// assembly is the role's root.
type assembly struct{ deps []node.Node }

func (*assembly) Name() string                 { return "ok/control" }
func (a *assembly) Dependencies() []node.Node  { return a.deps }
func (*assembly) Healthy(context.Context) bool { return true }
func (*assembly) Setup(context.Context) error  { return nil }
