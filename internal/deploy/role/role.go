// Package role assembles ok's deployer: its tables (schema deploy), the Deployments service on the
// role's one port, the follower of the SCM's change feed, the observer of what the cluster runs, and
// the engine that plans and drives deployments — each of the last three on one replica at a time,
// each copy of the deployer driving its own deployment. It reaches identity, the SCM, and ok
// automation as its own robot, and the cluster as its pod's ServiceAccount.
package role

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/Cidan/ok/internal/buildsys/bazel"
	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/busconfig"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/deploy/engine"
	"github.com/Cidan/ok/internal/deploy/observe"
	"github.com/Cidan/ok/internal/deploy/roll"
	"github.com/Cidan/ok/internal/deploy/service"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/deploy/store/postgres"
	"github.com/Cidan/ok/internal/deploy/store/spannerstore"
	"github.com/Cidan/ok/internal/deploy/tunnel"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/names"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/spannerdb"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Schema is the PostgreSQL schema the deployer keeps its tables in.
const Schema = "deploy"

// Interval is how often the engine plans and drives.
const Interval = 5 * time.Second

// Role is the deployer, assembled.
type Role struct {
	Root     node.Node
	Resolver *authn.Resolver
	Store    *store.Store
	Engine   *engine.Engine
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
	// Dev accepts development principals: a role on loopback.
	Dev bool
	// Color is the copy of the deployer this process runs in: roll.Blue or roll.Green.
	Color string
	// Checkout and Source are the engine's: where it checks commits out, and the SCM it reads them
	// from. Build is how it asks the build system; nil for Bazel at the checkout.
	Checkout checkout.Checkout
	Source   checkout.Source
	Build    engine.Build
	// Bazel is Bazel's path, and Env the environment of the Bazel it runs and the migrations they
	// run, for the Bazel Build.
	Bazel string
	Env   []string
	// Pool, Stream, Kube, Identity, SCM, and Automation, when set, are the database, the bus's
	// stream, the cluster, and the roles, dialed as the deployer already: tests.
	Pool                      *pgxpool.Pool
	Stream                    bus.Stream
	Kube                      kubernetes.Interface
	Identity, SCM, Automation grpc.ClientConnInterface
	// Secrets, when set, is ok secrets, dialed as the deployer: tests, and ok serve. Unset, it is
	// dialed at services.secrets, where the installation runs it.
	Secrets grpc.ClientConnInterface
	// Timing and Interval, when set, are the watch's and the engine's: tests.
	Timing   *roll.Timing
	Interval time.Duration
	// Roots, when set, are the identity CA's certificates as identity in the same process holds
	// them (ok serve); unset, they are read from identity.
	Roots *idclient.Roots
	// Peers, when set, connects to another cell's deployer at an address, as the role: tests.
	Peers func(addr string) grpc.ClientConnInterface
	// Address is where this cell's other replicas reach this one: its pod's address and port, through
	// which they relay to the deploy agents connected here (docs/hosted.md, "Deploy agents, as
	// built").
	Address string
}

// Build assembles the deployer from its configuration.
func Build(ctx context.Context, cfg *config.Config, opts Options) (*Role, error) {
	r := &Role{}
	ok := false
	defer func() {
		if !ok {
			r.Close()
		}
	}()
	if opts.Color != roll.Blue && opts.Color != roll.Green {
		return nil, fmt.Errorf("the deployer's copy is %s or %s, not %q", roll.Blue, roll.Green, opts.Color)
	}
	if opts.Checkout == nil || opts.Source == nil {
		return nil, errors.New("the deployer needs a checkout, and the SCM to read commits from")
	}
	var b store.Backend
	var err error
	pool := opts.Pool
	if pool == nil && cfg.Database.Type == "spanner" {
		// Its own database in the instance; the bus is another backend's (config).
		db, err := spannerdb.Open(ctx, cfg.Database.Spanner, "deploy")
		if err != nil {
			return nil, err
		}
		r.closers = append(r.closers, db.Close)
		if b, err = spannerstore.Open(ctx, db.Admin, db.Client); err != nil {
			return nil, fmt.Errorf("the deployer's store: %w", err)
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
		pb, err := postgres.Open(ctx, pool)
		if err != nil {
			return nil, fmt.Errorf("the deployer's store: %w", err)
		}
		b = pb
	}
	st := store.New(b)
	r.Store = st
	stream := opts.Stream
	if stream == nil {
		bb, err := busconfig.Open(ctx, cfg.Bus, pool)
		if err != nil {
			return nil, err
		}
		r.closers = append(r.closers, bb.Close)
		stream = bb.Stream
	}
	kube := opts.Kube
	if kube == nil {
		var rc *rest.Config
		if cfg.Deploy.Kubeconfig != "" {
			rc, err = clientcmd.BuildConfigFromFlags("", cfg.Deploy.Kubeconfig)
		} else {
			rc, err = rest.InClusterConfig()
		}
		if err != nil {
			return nil, fmt.Errorf("the cluster: %w", err)
		}
		if kube, err = kubernetes.NewForConfig(rc); err != nil {
			return nil, err
		}
	}

	idConn, scmConn, autoConn := opts.Identity, opts.SCM, opts.Automation
	if idConn == nil || scmConn == nil || autoConn == nil {
		if cfg.Services.Identity == "" || cfg.Services.SCM == "" || cfg.Services.Automation == "" {
			return nil, errors.New("services.identity, services.scm, and services.automation are where the deployer applies the tree, reads main, and pushes")
		}
		base, err := idclient.Options(cfg.Services)
		if err != nil {
			return nil, err
		}
		cred, err := idclient.Credential(cfg, base)
		if err != nil {
			return nil, err
		}
		o := base
		o.Credential = cred
		for _, d := range []struct {
			conn *grpc.ClientConnInterface
			addr string
		}{{&idConn, cfg.Services.Identity}, {&scmConn, cfg.Services.SCM}, {&autoConn, cfg.Services.Automation}} {
			if *d.conn != nil {
				continue
			}
			c, err := dial.Dial(d.addr, o)
			if err != nil {
				return nil, fmt.Errorf("dialing %s: %w", d.addr, err)
			}
			r.closers = append(r.closers, func() { c.Close() })
			*d.conn = c
		}
	}
	// ok secrets, which gives a deploy's migrations the secrets its stanza names.
	secConn := opts.Secrets
	if secConn == nil && cfg.Services.Secrets != "" {
		base, err := idclient.Options(cfg.Services)
		if err != nil {
			return nil, err
		}
		if base.Credential, err = idclient.Credential(cfg, base); err != nil {
			return nil, err
		}
		c, err := dial.Dial(cfg.Services.Secrets, base)
		if err != nil {
			return nil, fmt.Errorf("dialing %s: %w", cfg.Services.Secrets, err)
		}
		r.closers = append(r.closers, func() { c.Close() })
		secConn = c
	}
	var deploySecrets engine.DeploySecrets
	if secConn != nil {
		deploySecrets = secretspb.NewDeploySecretsClient(secConn)
	}
	roots := opts.Roots
	if roots == nil {
		if roots, err = idclient.NewRoots(ctx, idConn, "deploy"); err != nil {
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

	// Organizations' deploy agents, connected to whichever replica they reached, every replica
	// reaching them (docs/hosted.md, "Deploy agents, as built").
	if opts.Address == "" {
		slog.Warn("ok deploy: no address of this replica's own (OK_DEPLOY_ADDRESS): the others relay nothing to the deploy agents connected here")
	}
	agents := identitypb.NewDeployAgentsClient(idConn)
	hub := tunnel.NewHub(tunnel.HubConfig{Store: st, Address: opts.Address, Cell: cfg.Cell.Name, Log: slog.Default(),
		Account: func(ctx context.Context, robot string) (string, error) {
			a, err := agents.GetDeployAgent(ctx, &identitypb.GetDeployAgentRequest{Key: &identitypb.GetDeployAgentRequest_RobotId{RobotId: robot}})
			return a.GetAccountId(), err
		},
		Dial: func(ctx context.Context, addr string) (deploypb.AgentRelayClient, error) {
			c, done, err := dialPeer(cfg, opts, addr)
			if err != nil {
				return nil, err
			}
			if done != nil {
				context.AfterFunc(ctx, done)
			}
			return deploypb.NewAgentRelayClient(c), nil
		}})
	r.closers = append(r.closers, hub.Close)
	designations := &engine.Designations{TTL: 30 * time.Second, Ask: func(ctx context.Context, account string) (bool, error) {
		_, err := agents.GetDeployAgent(ctx, &identitypb.GetDeployAgentRequest{Key: &identitypb.GetDeployAgentRequest_AccountId{AccountId: account}})
		if status.Code(err) == codes.NotFound {
			return false, nil
		}
		return err == nil, err
	}}
	clusters := &engine.AgentClusters{Local: kube, Hub: hub, Owner: st.RepositoryOwner, Designated: designations.Designated, Hosted: cfg.Tenancy.Hosted()}
	clusterOf := func(ctx context.Context, repo int64) (kubernetes.Interface, error) {
		c, err := clusters.For(ctx, repo)
		return c.Kube, err
	}

	labels := roll.LabelsFor(cfg.Domain)
	build := opts.Build
	if build == nil {
		build = &engine.Bazel{Path: opts.Bazel, Flags: cfg.Deploy.BazelFlags, Env: opts.Env,
			OutputBase: filepath.Join(cfg.Deploy.State, "bazel"), Checkout: opts.Checkout, Source: opts.Source, Log: slog.Default(),
			// The heads it planned, each the next plan's base.
			Graphs: bazel.Graphs{Dir: filepath.Join(cfg.Deploy.State, "graphs"), Keep: 32}}
	}
	timing := roll.DefaultTiming
	if opts.Timing != nil {
		timing = *opts.Timing
	}
	r.Engine = engine.New(engine.Config{
		Store:          st,
		Kube:           kube,
		Clusters:       clusters,
		Runs:           automationpb.NewPushesClient(autoConn),
		Build:          build,
		Main:           engine.SCM{Browse: scmpb.NewBrowseClient(scmConn)},
		Source:         opts.Source,
		Checkout:       opts.Checkout,
		Timing:         timing,
		Labels:         labels,
		Self:           engine.Self{Namespace: cfg.Deploy.Self.Namespace, Name: cfg.Deploy.Self.Deployment, Color: opts.Color},
		Principal:      cfg.RobotName(cfg.Roles.Deploy),
		Principals:     names.New(idclient.NewDirectory(idConn)),
		Tree:           identitypb.NewTreeClient(idConn),
		Secrets:        deploySecrets,
		TreeRepository: cfg.Identity.TreeRepository,
		Log:            slog.Default(),
	})
	interval := Interval
	if opts.Interval > 0 {
		interval = opts.Interval
	}
	events := scmpb.NewEventsClient(scmConn)
	// Moves of its repositories' rows between cells, from the same role of the source cell
	// (docs/hosted.md, "Cells, as built").
	peers := cells.NewPeers(identitypb.NewCellsClient(idConn), func(addr string) (grpc.ClientConnInterface, func(), error) {
		return dialPeer(cfg, opts, addr)
	})
	r.closers = append(r.closers, peers.Close)
	mover := st.Mover()
	moves := move.NewNode("ok/deploy/moves", move.NewServer(mover, okpb.Caller_CALLER_DEPLOY,
		func(ctx context.Context, cell string) (okpb.MovesClient, error) {
			c, err := peers.Conn(ctx, cell, cells.Deploy)
			if err != nil {
				return nil, err
			}
			return okpb.NewMovesClient(c), nil
		}), mover.Purge)
	r.Root = &assembly{deps: []node.Node{
		moves,
		&services{svc: service.New(st), hub: hub},
		&app{name: "ok/deploy/feed", run: func(ctx context.Context) error { return engine.Follow(ctx, st, stream, events, slog.Default()) }},
		&app{name: "ok/deploy/directory", run: func(ctx context.Context) error {
			return engine.FollowDirectory(ctx, st, stream, identitypb.NewDirectoryEventsClient(idConn), slog.Default())
		}},
		&app{name: "ok/deploy/observer", run: (&observe.Observer{Store: st, Kube: kube, Cluster: clusterOf, Labels: labels, Log: slog.Default()}).Run},
		&app{name: "ok/deploy/engine", run: func(ctx context.Context) error {
			defer r.Engine.Wait()
			return r.Engine.Run(ctx, interval)
		}},
		roots,
	}}
	ok = true
	return r, nil
}

// dialPeer connects to another deployer — of this cell, or another's — as the role.
func dialPeer(cfg *config.Config, opts Options, addr string) (grpc.ClientConnInterface, func(), error) {
	if opts.Peers != nil {
		return opts.Peers(addr), nil, nil
	}
	base, err := idclient.Options(cfg.Services)
	if err != nil {
		return nil, nil, err
	}
	if opts.Dev && cfg.Workload.TokenFile == "" && cfg.Workload.CredentialDir == "" {
		// Development, with no workload credential: the role names itself to the other deployer.
		base.DevPrincipal = cfg.RobotName(cfg.Roles.Deploy)
	} else if base.Credential, err = idclient.Credential(cfg, base); err != nil {
		return nil, nil, err
	}
	c, err := dial.Dial(addr, base)
	if err != nil {
		return nil, nil, err
	}
	return c, func() { c.Close() }, nil
}

// services are the Deployments service, and the deploy agents' (Agents, AgentRelay).
type services struct {
	svc *service.Service
	hub *tunnel.Hub
}

var _ node.Service = (*services)(nil)

func (*services) Name() string                 { return "ok/deploy/services" }
func (*services) Dependencies() []node.Node    { return nil }
func (*services) Setup(context.Context) error  { return nil }
func (*services) Healthy(context.Context) bool { return true }

func (s *services) RegisterRPC(_ context.Context, reg grpc.ServiceRegistrar) error {
	deploypb.RegisterDeploymentsServer(reg, s.svc)
	deploypb.RegisterAgentsServer(reg, s.hub)
	deploypb.RegisterAgentRelayServer(reg, s.hub)
	return nil
}

// app is one of the deployer's loops.
type app struct {
	name string
	run  func(context.Context) error
}

var _ node.Application = (*app)(nil)

func (a *app) Name() string                    { return a.name }
func (*app) Dependencies() []node.Node         { return nil }
func (*app) Setup(context.Context) error       { return nil }
func (*app) Healthy(context.Context) bool      { return true }
func (a *app) Start(ctx context.Context) error { return a.run(ctx) }

// assembly is the role's root.
type assembly struct{ deps []node.Node }

func (*assembly) Name() string                 { return "ok/deploy" }
func (a *assembly) Dependencies() []node.Node  { return a.deps }
func (*assembly) Healthy(context.Context) bool { return true }
func (*assembly) Setup(context.Context) error  { return nil }
