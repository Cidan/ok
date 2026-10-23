// Package role assembles the SCM role: its store over the installation's database (schema scm)
// and bucket, its services and garbage collection, and its callers resolved through ok identity.
package role

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/busconfig"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/names"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/scm/server"
	"github.com/Cidan/ok/internal/scm/service"
	"github.com/Cidan/ok/internal/scm/store"
	"github.com/Cidan/ok/internal/scm/store/backend"
	scmpostgres "github.com/Cidan/ok/internal/scm/store/postgres"
	scmspanner "github.com/Cidan/ok/internal/scm/store/spannerstore"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/spannerdb"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
)

// Schema is the PostgreSQL schema the SCM keeps its tables in.
const Schema = "scm"

// DefaultGCHour is the hour (UTC) each repository's daily garbage collection is due from, unless
// the configuration says otherwise.
const DefaultGCHour = 9

// Role is the SCM role, assembled.
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
	// Dev accepts development principals: a role on loopback, for local development and tests.
	// With no services.identity, the SCM then needs no identity: every principal a caller names
	// is trusted. Its bucket is made if it does not exist.
	Dev bool
	// Pool and Blobs, when set, are the database and bucket instead of the configuration's: tests.
	Pool  *pgxpool.Pool
	Blobs blob.Store
	// Stream, when set, is the bus's stream the relay publishes the SCM's events on, instead of
	// the configuration's: tests.
	Stream bus.Stream
	// Identity, when set, is ok identity, dialed as the role: tests.
	Identity grpc.ClientConnInterface
	// Secrets, when set, is ok secrets, dialed as the role: tests, and ok serve.
	Secrets grpc.ClientConnInterface
	// Peers, when set, connects to another cell's SCM at an address, as the role: tests.
	Peers func(addr string) grpc.ClientConnInterface
	// Roots, when set, are the identity CA's certificates as identity in the same process holds
	// them (ok serve); unset, they are read from identity.
	Roots *idclient.Roots
}

// OpenStore opens the SCM's store as the configuration says, or over the pool and bucket the
// options give; close what it returns when done.
func OpenStore(ctx context.Context, cfg *config.Config, opts Options) (*store.Store, func(), error) {
	st, _, done, err := open(ctx, cfg, opts)
	return st, done, err
}

// open is OpenStore, with the pool the store is over: none on Spanner, where the SCM's database is
// its own and the bus another backend's (config).
func open(ctx context.Context, cfg *config.Config, opts Options) (*store.Store, *pgxpool.Pool, func(), error) {
	done := func() {}
	var b backend.Backend
	pool := opts.Pool
	if pool == nil && cfg.Database.Type == "spanner" {
		db, err := spannerdb.Open(ctx, cfg.Database.Spanner, "scm")
		if err != nil {
			return nil, nil, nil, err
		}
		done = db.Close
		if b, err = scmspanner.Open(ctx, db.Admin, db.Client); err != nil {
			done()
			return nil, nil, nil, fmt.Errorf("the SCM's store: %w", err)
		}
	} else if pool == nil {
		dsn, err := config.Secret(cfg.Database.URLFile)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("database.url_file: %w", err)
		}
		if dsn == "" {
			return nil, nil, nil, errors.New("database.url_file names no database")
		}
		if pool, err = pgdb.Open(ctx, dsn, Schema, cfg.Database.MaxConnections); err != nil {
			return nil, nil, nil, err
		}
		done = pool.Close
	}
	blobs := opts.Blobs
	if blobs == nil {
		s3, err := cfg.SCM.Objects.Open(ctx, "scm.objects", opts.Dev)
		if err != nil {
			done()
			return nil, nil, nil, err
		}
		blobs = s3
	}
	if b == nil {
		pb, err := scmpostgres.Open(ctx, pool)
		if err != nil {
			done()
			return nil, nil, nil, fmt.Errorf("the SCM's store: %w", err)
		}
		b = pb
	}
	st, err := store.New(b, blobs, store.Config{
		Names: cfg.Names(), EmailDomain: cfg.SCM.EmailDomain,
		ObjectCacheBytes: cfg.SCM.Cache.Objects, ChunkCacheBytes: cfg.SCM.Cache.Chunks, DiffCacheBytes: cfg.SCM.Cache.Diffs,
		CodeIndexCacheBytes: cfg.SCM.Cache.CodeIndexes,
	})
	if err != nil {
		done()
		return nil, nil, nil, fmt.Errorf("the SCM's store: %w", err)
	}
	return st, pool, func() { st.Close(); done() }, nil
}

// Build assembles the SCM role from its configuration.
func Build(ctx context.Context, cfg *config.Config, opts Options) (*Role, error) {
	r := &Role{}
	ok := false
	defer func() {
		if !ok {
			r.Close()
		}
	}()
	st, pool, done, err := open(ctx, cfg, opts)
	if err != nil {
		return nil, err
	}
	r.Store = st
	r.closers = append(r.closers, done)
	stream := opts.Stream
	if stream == nil {
		b, err := busconfig.Open(ctx, cfg.Bus, pool)
		if err != nil {
			return nil, err
		}
		r.closers = append(r.closers, b.Close)
		stream = b.Stream
	}
	// The stream's backend carries the signals (docs/hosted.md, "Signals").
	if sig, ok := stream.(bus.Signals); ok {
		st.SetSignals(sig)
	}

	hour := DefaultGCHour
	if cfg.SCM.GCHour != nil {
		hour = *cfg.SCM.GCHour
	}
	// dialOptions are how the SCM dials ok's other roles, as itself.
	dialOptions := func() (dial.Options, error) {
		base, err := idclient.Options(cfg.Services)
		if err != nil {
			return dial.Options{}, err
		}
		o := base
		if opts.Dev && cfg.Workload.TokenFile == "" && cfg.Workload.CredentialDir == "" {
			// Development, with no workload credential: the SCM names itself to a development
			// identity, as ok serve's roles do.
			o.DevPrincipal = cfg.RobotName(cfg.Roles.SCM)
		} else if o.Credential, err = idclient.Credential(cfg, base); err != nil {
			return dial.Options{}, err
		}
		return o, nil
	}
	conn := opts.Identity
	if conn == nil {
		if cfg.Services.Identity == "" {
			return nil, errors.New("services.identity is where the SCM reaches ok identity, which knows its callers and names its repositories")
		}
		o, err := dialOptions()
		if err != nil {
			return nil, err
		}
		c, err := dial.Dial(cfg.Services.Identity, o)
		if err != nil {
			return nil, err
		}
		r.closers = append(r.closers, func() { c.Close() })
		conn = c
	}
	roots := opts.Roots
	if roots == nil {
		if roots, err = idclient.NewRoots(ctx, conn, "scm"); err != nil {
			return nil, err
		}
	}
	r.Resolver = authn.NewResolver(authn.Config{
		Directory:     idclient.NewDirectory(conn),
		Credentials:   credential.NewVerifier(roots.Pool),
		TrustedHeader: cfg.Identity.TrustedHeader,
		Dev:           opts.Dev,
		Roles:         idclient.Roles(cfg),
	})
	st.SetPrincipals(names.New(idclient.NewDirectory(conn)))
	// ok secrets, which upload asks about secrets' versions and writers (docs/secrets.md, "The
	// SCM"), where the installation runs it.
	secConn := opts.Secrets
	if secConn == nil && cfg.Services.Secrets != "" {
		o, err := dialOptions()
		if err != nil {
			return nil, err
		}
		c, err := dial.Dial(cfg.Services.Secrets, o)
		if err != nil {
			return nil, err
		}
		r.closers = append(r.closers, func() { c.Close() })
		secConn = c
	}
	if secConn != nil {
		st.SetSecrets(service.SecretVersions{Client: secretspb.NewVersionsClient(secConn)})
	}
	ten := identitypb.NewTenancyClient(conn)
	svc := server.New(st, identitypb.NewDirectoryClient(conn), ten, identitypb.NewAuthorizationClient(conn))
	// Its cell's repositories, and other cells' SCMs where the directory says (docs/hosted.md,
	// "Cells, as built").
	peers := cells.NewPeers(identitypb.NewCellsClient(conn), func(addr string) (grpc.ClientConnInterface, func(), error) {
		if opts.Peers != nil {
			return opts.Peers(addr), nil, nil
		}
		o, err := dialOptions()
		if err != nil {
			return nil, nil, err
		}
		c, err := dial.Dial(addr, o)
		if err != nil {
			return nil, nil, err
		}
		return c, func() { c.Close() }, nil
	})
	r.closers = append(r.closers, peers.Close)
	svc.InCell(cfg.Cell.Name, peerSCMs{peers})
	r.Root = &assembly{deps: []node.Node{svc, server.NewGC(svc, hour), server.NewPurge(svc), server.NewSegments(svc, 0), server.NewFolds(svc), server.NewRelay(svc, stream),
		server.NewMovePurge(svc), server.NewCodeSearch(svc), roots}}
	ok = true
	return r, nil
}

// assembly is the role's root.
type assembly struct{ deps []node.Node }

func (*assembly) Name() string                 { return "ok/scm" }
func (a *assembly) Dependencies() []node.Node  { return a.deps }
func (*assembly) Healthy(context.Context) bool { return true }
func (*assembly) Setup(context.Context) error  { return nil }
