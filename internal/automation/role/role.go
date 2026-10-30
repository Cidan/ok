// Package role assembles ok automation: its database (schema automation) and logs' bucket, the
// Runs, Pushes, and Work services on the role's one port, the consumer of the jobs that store
// finished runs' logs, the follower of the SCM's change feed that plans each patchset's pre, and
// the landing queue — each of the last two on one replica at a time. It reaches the SCM and identity as its
// own robot.
package role

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/automation/cache"
	"github.com/Cidan/ok/internal/automation/feed"
	"github.com/Cidan/ok/internal/automation/queue"
	"github.com/Cidan/ok/internal/automation/quota"
	"github.com/Cidan/ok/internal/automation/scale"
	"github.com/Cidan/ok/internal/automation/service"
	"github.com/Cidan/ok/internal/automation/store"
	autopostgres "github.com/Cidan/ok/internal/automation/store/postgres"
	"github.com/Cidan/ok/internal/automation/store/spannerstore"
	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/bus/busconfig"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/spannerdb"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Schema is the PostgreSQL schema ok automation keeps its tables in.
const Schema = "automation"

// Role is ok automation, assembled.
type Role struct {
	Root     node.Node
	Resolver *authn.Resolver
	Store    *store.Store
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
	// Dev accepts development principals, and makes the logs' bucket: a role on loopback.
	Dev bool
	// Pool and Logs, when set, are the database and bucket instead of the configuration's: tests.
	Pool *pgxpool.Pool
	Logs blob.Store
	// Identity and SCM, when set, are dialed as ok automation already: tests.
	Identity, SCM grpc.ClientConnInterface
	// Deployments, when set, sizes the pools' worker Deployments instead of the cluster's: tests.
	Deployments scale.Deployments
	// Roots, when set, are the identity CA's certificates as identity in the same process holds
	// them (ok serve); unset, they are read from identity.
	Roots *idclient.Roots
	// Peers, when set, connects to another cell's ok automation at an address, as the role: tests.
	Peers func(addr string) grpc.ClientConnInterface
}

// Build assembles ok automation from its configuration.
func Build(ctx context.Context, cfg *config.Config, opts Options) (*Role, error) {
	r := &Role{}
	ok := false
	defer func() {
		if !ok {
			r.Close()
		}
	}()
	var b store.Backend
	var err error
	pool := opts.Pool
	if pool == nil && cfg.Database.Type == "spanner" {
		// Its own database in the instance; the bus is another backend's (config).
		db, err := spannerdb.Open(ctx, cfg.Database.Spanner, "automation")
		if err != nil {
			return nil, err
		}
		r.closers = append(r.closers, db.Close)
		if b, err = spannerstore.Open(ctx, db.Admin, db.Client); err != nil {
			return nil, fmt.Errorf("ok automation's store: %w", err)
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
		pb, err := autopostgres.Open(ctx, pool)
		if err != nil {
			return nil, fmt.Errorf("ok automation's store: %w", err)
		}
		b = pb
	}
	logs := opts.Logs
	if logs == nil {
		s3, err := cfg.Automation.Logs.Open(ctx, "automation.logs", opts.Dev)
		if err != nil {
			return nil, err
		}
		logs = s3
	}
	bb, err := busconfig.Open(ctx, cfg.Bus)
	if err != nil {
		return nil, err
	}
	r.closers = append(r.closers, bb.Close)

	idConn, scmConn := opts.Identity, opts.SCM
	if idConn == nil || scmConn == nil {
		if cfg.Services.Identity == "" || cfg.Services.SCM == "" {
			return nil, errors.New("services.identity and services.scm are where ok automation reaches ok identity and the SCM")
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
		}{{&idConn, cfg.Services.Identity}, {&scmConn, cfg.Services.SCM}} {
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
		roots = idclient.NewRoots(idConn)
	}
	r.Resolver = authn.NewResolver(authn.Config{
		Directory:     idclient.NewDirectory(idConn),
		Credentials:   credential.NewVerifier(roots.Pool),
		TrustedHeader: cfg.Identity.TrustedHeader,
		Dev:           opts.Dev,
		Roles:         idclient.Roles(cfg),
	})
	scfg := store.DefaultConfig
	// The bus's queue dispatches runs to workers (docs/hosted.md, "Hosted automation, as built"),
	// and carries the inboxes of the readers following runs' logs.
	scfg.Dispatch = bb.Queue
	// Admission by the directory's quotas, with the other cells' use (docs/hosted.md, "Hosted
	// automation, as built").
	quotas := identitypb.NewQuotasClient(idConn)
	scfg.PlanPool, scfg.Quotas = cfg.Automation.PlanPool, quota.Reader{Quotas: quotas, Cell: cfg.Cell.Name}.Get
	st := store.New(b, logs, scfg)
	// Its keyed work's jobs go on the bus's queue, as its runs do (docs/hosted.md, "Work without
	// leaders").
	if err := bb.RegisterWork(st.Work(), "automation"); err != nil {
		return nil, err
	}
	r.Store = st
	report := &app{name: "ok/automation/usage", run: (&quota.Reporter{Store: st, Quotas: quotas, Cell: cfg.Cell.Name, Log: slog.Default()}).Run}
	qcfg := queue.DefaultConfig
	if d := cfg.Automation.SpeculationDepth; d != nil {
		qcfg.Depth = *d
	}
	if b := cfg.Automation.LandingBatch; b != nil {
		qcfg.Batch = *b
	}
	if p := cfg.Automation.QueueParallel; p != nil {
		qcfg.Parallel = *p
	}
	// Runs.StartPre plans pre as the feed does, reading the SCM.
	follow := &follower{f: feed.New(st, feed.SCM{
		Events:       scmpb.NewEventsClient(scmConn),
		ChangeLists:  scmpb.NewChangeListsClient(scmConn),
		Repositories: scmpb.NewRepositoriesClient(scmConn),
		Browse:       scmpb.NewBrowseClient(scmConn),
	}, slog.Default())}
	svc := &services{svc: service.New(st, store.DefaultConfig.Heartbeat, cfg.Automation.Cache != nil, follow.f), waits: st.Waits()}
	// The remote cache its workers' Bazel reaches, in the logs' bucket (docs/hosted.md, "Hosted
	// automation, as built").
	if ac := cfg.Automation.Cache; ac != nil {
		ns := &service.Namespaces{Store: st, Checker: authz.NewChecker(identitypb.NewAuthorizationClient(idConn))}
		svc.cache = cache.New(logs, ns.Resolve, int64(ac.MemoryMiB)<<20)
		svc.sweep = &app{name: "ok/automation/cache", run: func(ctx context.Context) error {
			return cache.Expire(ctx, svc.cache, st.Work(), ac.TTL, slog.Default())
		}}
	}
	storer := &logStorer{store: st}
	lander := &lander{q: queue.New(st, queue.SCM{
		ChangeLists:  scmpb.NewChangeListsClient(scmConn),
		Landing:      scmpb.NewLandingClient(scmConn),
		Repositories: scmpb.NewRepositoriesClient(scmConn),
		Browse:       scmpb.NewBrowseClient(scmConn),
	}, qcfg, slog.Default())}
	directory := &dirFollower{d: feed.NewDirectory(st, identitypb.NewDirectoryEventsClient(idConn), slog.Default())}
	// Moves of its repositories' rows and logs between cells, from the same role of the source
	// cell (docs/hosted.md, "Cells, as built").
	peers := cells.NewPeers(identitypb.NewCellsClient(idConn), func(addr string) (grpc.ClientConnInterface, func(), error) {
		if opts.Peers != nil {
			return opts.Peers(addr), nil, nil
		}
		base, err := idclient.Options(cfg.Services)
		if err != nil {
			return nil, nil, err
		}
		if opts.Dev && cfg.Workload.TokenFile == "" && cfg.Workload.CredentialDir == "" {
			// Development, with no workload credential: the role names itself to the other cell's.
			base.DevPrincipal = cfg.RobotName(cfg.Roles.Automation)
		} else if base.Credential, err = idclient.Credential(cfg, base); err != nil {
			return nil, nil, err
		}
		c, err := dial.Dial(addr, base)
		if err != nil {
			return nil, nil, err
		}
		return c, func() { c.Close() }, nil
	})
	r.closers = append(r.closers, peers.Close)
	mover := st.Mover()
	moves := move.NewNode("ok/automation/moves", move.NewServer(mover, okpb.Caller_CALLER_AUTOMATION,
		func(ctx context.Context, cell string) (okpb.MovesClient, error) {
			c, err := peers.Conn(ctx, cell, cells.Automation)
			if err != nil {
				return nil, err
			}
			return okpb.NewMovesClient(c), nil
		}), mover.Purge)
	deps := []node.Node{svc, storer, follow, directory, lander, moves, report}
	// The pools of workers, sized to what they are asked for (docs/hosted.md, "Hosted automation,
	// as built").
	if len(cfg.Automation.Pools) > 0 {
		ds := opts.Deployments
		if ds == nil {
			var rc *rest.Config
			if cfg.Automation.Kubeconfig != "" {
				rc, err = clientcmd.BuildConfigFromFlags("", cfg.Automation.Kubeconfig)
			} else {
				rc, err = rest.InClusterConfig()
			}
			if err != nil {
				return nil, fmt.Errorf("the pools' cluster: %w", err)
			}
			kube, err := kubernetes.NewForConfig(rc)
			if err != nil {
				return nil, err
			}
			ds = scale.Kube{Client: kube}
		}
		var pools []scale.Pool
		for _, p := range cfg.Automation.Pools {
			pools = append(pools, scale.Pool{Name: p.Name, Namespace: p.Namespace, Deployment: p.Deployment, Slots: int32(p.Slots),
				Min: int32(p.Min), Max: int32(p.Max)})
		}
		sc := &scale.Scaler{Store: st, Pools: pools, Deployments: ds, Log: slog.Default()}
		deps = append(deps, &app{name: "ok/automation/pools", run: func(ctx context.Context) error { return sc.Run(ctx, st.Work()) }})
	}
	if svc.sweep != nil {
		deps = append(deps, svc.sweep)
	}
	r.Root = &assembly{deps: deps}
	ok = true
	return r, nil
}

// services are ok automation's gRPC services.
type services struct {
	svc   service.Services
	cache *cache.Server
	sweep node.Node
	waits wait.Store
}

var _ node.Service = (*services)(nil)

func (*services) Name() string                 { return "ok/automation/services" }
func (*services) Dependencies() []node.Node    { return nil }
func (*services) Setup(context.Context) error  { return nil }
func (*services) Healthy(context.Context) bool { return true }

func (s *services) RegisterRPC(_ context.Context, reg grpc.ServiceRegistrar) error {
	automationpb.RegisterRunsServer(reg, s.svc.Runs)
	automationpb.RegisterPushesServer(reg, s.svc.Pushes)
	automationpb.RegisterWorkServer(reg, s.svc.Work)
	automationpb.RegisterSecretRunsServer(reg, s.svc.SecretRuns)
	okpb.RegisterWaitsServer(reg, &wait.Server{Store: s.waits})
	if s.cache != nil {
		s.cache.Register(reg)
	}
	return nil
}

// logStorer stores finished runs' logs as their jobs come, on every replica, and listens for the
// bus's signals. Once, once the release before has stopped (work.Once), it takes up what that
// release left: its queued runs dispatched, its logs stored.
type logStorer struct{ store *store.Store }

var _ node.Application = (*logStorer)(nil)

func (*logStorer) Name() string                 { return "ok/automation/logs" }
func (*logStorer) Dependencies() []node.Node    { return nil }
func (*logStorer) Setup(context.Context) error  { return nil }
func (*logStorer) Healthy(context.Context) bool { return true }

func (s *logStorer) Start(ctx context.Context) error {
	go s.store.RunWaits(ctx)
	go func() {
		if err := work.Once(ctx, s.store.Work(), "automation-upgrade", s.store.Started, slog.Default()); err != nil && ctx.Err() == nil {
			slog.Warn("automation: taking up what an older release left", "err", err)
		}
	}()
	return s.store.StoreLogs(ctx)
}

// follower plans each patchset the SCM's change feed announces.
type follower struct{ f *feed.Follower }

var _ node.Application = (*follower)(nil)

func (*follower) Name() string                      { return "ok/automation/feed" }
func (*follower) Dependencies() []node.Node         { return nil }
func (*follower) Setup(context.Context) error       { return nil }
func (*follower) Healthy(context.Context) bool      { return true }
func (f *follower) Start(ctx context.Context) error { return f.f.Run(ctx) }

// dirFollower forgets the repositories the directory purges.
type dirFollower struct{ d *feed.Directory }

var _ node.Application = (*dirFollower)(nil)

func (*dirFollower) Name() string                      { return "ok/automation/directory" }
func (*dirFollower) Dependencies() []node.Node         { return nil }
func (*dirFollower) Setup(context.Context) error       { return nil }
func (*dirFollower) Healthy(context.Context) bool      { return true }
func (f *dirFollower) Start(ctx context.Context) error { return f.d.Run(ctx) }

// lander is the landing queue.
type lander struct{ q *queue.Runner }

var _ node.Application = (*lander)(nil)

func (*lander) Name() string                      { return "ok/automation/queue" }
func (*lander) Dependencies() []node.Node         { return nil }
func (*lander) Setup(context.Context) error       { return nil }
func (*lander) Healthy(context.Context) bool      { return true }
func (l *lander) Start(ctx context.Context) error { return l.q.Run(ctx) }

// assembly is the role's root.
type assembly struct{ deps []node.Node }

func (*assembly) Name() string                 { return "ok/automation" }
func (a *assembly) Dependencies() []node.Node  { return a.deps }
func (*assembly) Healthy(context.Context) bool { return true }
func (*assembly) Setup(context.Context) error  { return nil }

// app runs a function while the role runs: a node.Application.
type app struct {
	name string
	run  func(ctx context.Context) error
}

var _ node.Application = (*app)(nil)

func (a *app) Name() string                    { return a.name }
func (*app) Dependencies() []node.Node         { return nil }
func (*app) Setup(context.Context) error       { return nil }
func (*app) Healthy(context.Context) bool      { return true }
func (a *app) Start(ctx context.Context) error { return a.run(ctx) }
