// Package role assembles the SCM role: its store over the installation's database (schema scm)
// and bucket, its services and garbage collection, and its callers resolved through ok identity.
package role

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/devdir"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/scm/server"
	"github.com/Cidan/ok/internal/scm/store"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
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
	// Identity, when set, is ok identity, dialed as the role: tests.
	Identity grpc.ClientConnInterface
	// Roots, when set, are the identity CA's certificates as identity in the same process holds
	// them (ok serve); unset, they are read from identity.
	Roots *idclient.Roots
}

// OpenStore opens the SCM's store as the configuration says, or over the pool and bucket the
// options give; close what it returns when done.
func OpenStore(ctx context.Context, cfg *config.Config, opts Options) (*store.Store, func(), error) {
	done := func() {}
	pool := opts.Pool
	if pool == nil {
		dsn, err := config.Secret(cfg.Database.URLFile)
		if err != nil {
			return nil, nil, fmt.Errorf("database.url_file: %w", err)
		}
		if dsn == "" {
			return nil, nil, errors.New("database.url_file names no database")
		}
		if pool, err = pgdb.Open(ctx, dsn, Schema, cfg.Database.MaxConnections); err != nil {
			return nil, nil, err
		}
		done = pool.Close
	}
	blobs := opts.Blobs
	if blobs == nil {
		s3, err := cfg.SCM.Objects.Open(ctx, "scm.objects", opts.Dev)
		if err != nil {
			done()
			return nil, nil, err
		}
		blobs = s3
	}
	st, err := store.Open(ctx, pool, blobs, store.Config{
		Names: cfg.Names(), EmailDomain: cfg.SCM.EmailDomain,
		ObjectCacheBytes: cfg.SCM.Cache.Objects, ChunkCacheBytes: cfg.SCM.Cache.Chunks, DiffCacheBytes: cfg.SCM.Cache.Diffs,
	})
	if err != nil {
		done()
		return nil, nil, fmt.Errorf("the SCM's store: %w", err)
	}
	return st, func() { st.Close(); done() }, nil
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
	st, done, err := OpenStore(ctx, cfg, opts)
	if err != nil {
		return nil, err
	}
	r.Store = st
	r.closers = append(r.closers, done)

	hour := DefaultGCHour
	if cfg.SCM.GCHour != nil {
		hour = *cfg.SCM.GCHour
	}
	conn := opts.Identity
	if conn == nil && opts.Dev && cfg.Services.Identity == "" {
		// Development with no identity: callers are the principals they name, on loopback.
		slog.Warn("scm: no services.identity, so every principal a caller names is trusted (-dev only)")
		dir := devdir.Directory{Names: cfg.Names()}
		r.Resolver = authn.NewResolver(authn.Config{Directory: dir, Dev: true, Roles: idclient.Roles(cfg)})
		svc := server.New(st, dir)
		r.Root = &assembly{deps: []node.Node{svc, server.NewGC(svc, hour)}}
		ok = true
		return r, nil
	}
	if conn == nil {
		if cfg.Services.Identity == "" {
			return nil, errors.New("services.identity is where the SCM reaches ok identity")
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
	svc := server.New(st, identitypb.NewDirectoryClient(conn))
	r.Root = &assembly{deps: []node.Node{svc, server.NewGC(svc, hour), roots}}
	ok = true
	return r, nil
}

// assembly is the role's root.
type assembly struct{ deps []node.Node }

func (*assembly) Name() string                 { return "ok/scm" }
func (a *assembly) Dependencies() []node.Node  { return a.deps }
func (*assembly) Healthy(context.Context) bool { return true }
func (*assembly) Setup(context.Context) error  { return nil }
