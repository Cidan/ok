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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/Cidan/ok/internal/buildsys/bazel"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/deploy/engine"
	"github.com/Cidan/ok/internal/deploy/observe"
	"github.com/Cidan/ok/internal/deploy/roll"
	"github.com/Cidan/ok/internal/deploy/service"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
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
	// Pool, Kube, Identity, SCM, and Automation, when set, are the database, the cluster, and the
	// roles, dialed as the deployer already: tests.
	Pool                      *pgxpool.Pool
	Kube                      kubernetes.Interface
	Identity, SCM, Automation grpc.ClientConnInterface
	// Timing and Interval, when set, are the watch's and the engine's: tests.
	Timing   *roll.Timing
	Interval time.Duration
	// Roots, when set, are the identity CA's certificates as identity in the same process holds
	// them (ok serve); unset, they are read from identity.
	Roots *idclient.Roots
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
	st, err := store.Open(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("the deployer's store: %w", err)
	}
	r.Store = st
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
		Runs:           automationpb.NewPushesClient(autoConn),
		Build:          build,
		Main:           engine.SCM{Browse: scmpb.NewBrowseClient(scmConn)},
		Source:         opts.Source,
		Checkout:       opts.Checkout,
		Timing:         timing,
		Labels:         labels,
		Self:           engine.Self{Namespace: cfg.Deploy.Self.Namespace, Name: cfg.Deploy.Self.Deployment, Color: opts.Color},
		Principal:      cfg.RobotName(cfg.Roles.Deploy),
		Tree:           identitypb.NewTreeClient(idConn),
		TreeRepository: cfg.Identity.TreeRepository,
		Log:            slog.Default(),
	})
	interval := Interval
	if opts.Interval > 0 {
		interval = opts.Interval
	}
	events := scmpb.NewEventsClient(scmConn)
	r.Root = &assembly{deps: []node.Node{
		&services{svc: service.New(st)},
		&app{name: "ok/deploy/feed", run: func(ctx context.Context) error { return engine.Follow(ctx, st, events, slog.Default()) }},
		&app{name: "ok/deploy/observer", run: (&observe.Observer{Store: st, Kube: kube, Labels: labels, Log: slog.Default()}).Run},
		&app{name: "ok/deploy/engine", run: func(ctx context.Context) error {
			defer r.Engine.Wait()
			return r.Engine.Run(ctx, interval)
		}},
		roots,
	}}
	ok = true
	return r, nil
}

// services is the Deployments service.
type services struct{ svc *service.Service }

var _ node.Service = (*services)(nil)

func (*services) Name() string                 { return "ok/deploy/services" }
func (*services) Dependencies() []node.Node    { return nil }
func (*services) Setup(context.Context) error  { return nil }
func (*services) Healthy(context.Context) bool { return true }

func (s *services) RegisterRPC(_ context.Context, reg grpc.ServiceRegistrar) error {
	deploypb.RegisterDeploymentsServer(reg, s.svc)
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
