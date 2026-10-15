// Package role assembles ok automation: its database (schema automation) and logs' bucket, the
// Runs, Pushes, and Work services on the role's one port, the sweep of lapsed leases and stranded
// logs, the follower of the SCM's change feed that plans each patchset's pre, and the landing
// queue — each of the last two on one replica at a time. It reaches the SCM and identity as its
// own robot.
package role

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/automation/feed"
	"github.com/Cidan/ok/internal/automation/queue"
	"github.com/Cidan/ok/internal/automation/service"
	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Schema is the PostgreSQL schema ok automation keeps its tables in.
const Schema = "automation"

// sweepInterval is how often the sweep acts on lapsed leases and stranded logs.
const sweepInterval = 15 * time.Second

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
	// Roots, when set, are the identity CA's certificates as identity in the same process holds
	// them (ok serve); unset, they are read from identity.
	Roots *idclient.Roots
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
	logs := opts.Logs
	if logs == nil {
		s3, err := cfg.Automation.Logs.Open(ctx, "automation.logs", opts.Dev)
		if err != nil {
			return nil, err
		}
		logs = s3
	}
	st, err := store.Open(ctx, pool, logs, store.DefaultConfig)
	if err != nil {
		return nil, fmt.Errorf("ok automation's store: %w", err)
	}
	r.Store = st

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
		if roots, err = idclient.NewRoots(ctx, idConn, "automation"); err != nil {
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
	qcfg := queue.DefaultConfig
	if d := cfg.Automation.SpeculationDepth; d != nil {
		qcfg.Depth = *d
	}
	svc := &services{svc: service.New(st, store.DefaultConfig.Heartbeat)}
	sweep := &sweeper{store: st}
	follow := &follower{f: feed.New(st, feed.SCM{
		Events:       scmpb.NewEventsClient(scmConn),
		ChangeLists:  scmpb.NewChangeListsClient(scmConn),
		Repositories: scmpb.NewRepositoriesClient(scmConn),
		Browse:       scmpb.NewBrowseClient(scmConn),
	}, slog.Default())}
	lander := &lander{q: queue.New(st, queue.SCM{
		ChangeLists:  scmpb.NewChangeListsClient(scmConn),
		Landing:      scmpb.NewLandingClient(scmConn),
		Repositories: scmpb.NewRepositoriesClient(scmConn),
		Browse:       scmpb.NewBrowseClient(scmConn),
	}, qcfg, slog.Default())}
	r.Root = &assembly{deps: []node.Node{svc, sweep, follow, lander, roots}}
	ok = true
	return r, nil
}

// services are ok automation's gRPC services.
type services struct{ svc service.Services }

var _ node.Service = (*services)(nil)

func (*services) Name() string                 { return "ok/automation/services" }
func (*services) Dependencies() []node.Node    { return nil }
func (*services) Setup(context.Context) error  { return nil }
func (*services) Healthy(context.Context) bool { return true }

func (s *services) RegisterRPC(_ context.Context, reg grpc.ServiceRegistrar) error {
	automationpb.RegisterRunsServer(reg, s.svc.Runs)
	automationpb.RegisterPushesServer(reg, s.svc.Pushes)
	automationpb.RegisterWorkServer(reg, s.svc.Work)
	return nil
}

// sweeper requeues runs whose leases lapsed and moves finished runs' logs, on every replica, and
// listens for the database's notifications.
type sweeper struct{ store *store.Store }

var _ node.Application = (*sweeper)(nil)

func (*sweeper) Name() string                 { return "ok/automation/sweep" }
func (*sweeper) Dependencies() []node.Node    { return nil }
func (*sweeper) Setup(context.Context) error  { return nil }
func (*sweeper) Healthy(context.Context) bool { return true }

func (s *sweeper) Start(ctx context.Context) error {
	go s.store.Listen(ctx)
	for {
		if err := s.store.Sweep(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("automation: sweep", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(sweepInterval):
		}
	}
}

// follower plans each patchset the SCM's change feed announces.
type follower struct{ f *feed.Follower }

var _ node.Application = (*follower)(nil)

func (*follower) Name() string                      { return "ok/automation/feed" }
func (*follower) Dependencies() []node.Node         { return nil }
func (*follower) Setup(context.Context) error       { return nil }
func (*follower) Healthy(context.Context) bool      { return true }
func (f *follower) Start(ctx context.Context) error { return f.f.Run(ctx) }

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
