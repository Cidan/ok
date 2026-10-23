// Package role assembles ok secrets (docs/secrets.md): its store over its own database (schema
// secrets, or a Spanner database of its own) under the installation's root, its services, and its
// side of moves between cells. It reads main from the SCM and memberships from ok identity as its
// own robot, and reaches its root's key management service as its workload.
package role

import (
	"context"
	"errors"
	"fmt"

	kmsapi "cloud.google.com/go/kms/apiv1"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/secrets/keys"
	"github.com/Cidan/ok/internal/secrets/secretstore"
	"github.com/Cidan/ok/internal/secrets/secretstore/awssm"
	"github.com/Cidan/ok/internal/secrets/secretstore/gcpsm"
	"github.com/Cidan/ok/internal/secrets/secretstore/vault"
	"github.com/Cidan/ok/internal/secrets/service"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/secrets/store/postgres"
	"github.com/Cidan/ok/internal/secrets/store/spannerstore"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/spannerdb"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Schema is the PostgreSQL schema ok secrets keeps its tables in.
const Schema = "secrets"

// Role is ok secrets, assembled.
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
	// Dev accepts development principals: a role on loopback.
	Dev bool
	// Pool, when set, is the database instead of the configuration's: tests.
	Pool *pgxpool.Pool
	// Identity, SCM, and Automation, when set, are those roles dialed as ok secrets already: tests,
	// and ok serve.
	Identity, SCM, Automation grpc.ClientConnInterface
	// Opener, when set, opens roots instead of the KMSes the workload reaches: tests' fakes.
	Opener *keys.Opener
	// Stores, when set, are the external stores instead of those secrets.stores configures: tests'.
	Stores secretstore.Stores
	// RootOf, when set, is each repository's organization's own root: the role's reads ok
	// identity's (SecretsRoots); an administrator's command, OpenStore alone, uses the
	// installation's for keys it makes.
	RootOf func(ctx context.Context, repo int64) (string, error)
	// Approvals, when set, asks people for passkeys instead of ok identity: tests.
	Approvals service.Approvals
	// Roots, when set, are the identity CA's certificates as identity in the same process holds
	// them (ok serve); unset, they are read from identity.
	Roots *idclient.Roots
	// Peers, when set, connects to another cell's ok secrets at an address, as the role: tests.
	Peers func(addr string) grpc.ClientConnInterface
}

// opener reaches the KMSes a root names as the workload: Cloud KMS and AWS KMS by their default
// credentials — workload identity — made when a root first needs one.
func opener() *keys.Opener {
	return &keys.Opener{
		DialGCP: func(ctx context.Context) (keys.GCPKMS, error) { return kmsapi.NewKeyManagementClient(ctx) },
		DialAWS: func(ctx context.Context) (keys.AWSKMS, error) {
			c, err := awsconfig.LoadDefaultConfig(ctx)
			if err != nil {
				return nil, err
			}
			return awskms.NewFromConfig(c), nil
		},
	}
}

// OpenStore opens ok secrets' store as the configuration says: for the role, and for an
// administrator's commands (ok secrets rotate, rewrap, audit). Close what it returns when done.
func OpenStore(ctx context.Context, cfg *config.Config, opts Options) (*store.Store, func(), error) {
	if cfg.Secrets.Root == "" {
		return nil, nil, errors.New("secrets.root names no root: the key that wraps repositories' keys (keyring:<file>, gcp-kms:<key>, aws-kms:<ARN>)")
	}
	db := cfg.Database
	if cfg.Secrets.Database != nil {
		db = *cfg.Secrets.Database
	}
	var b store.Backend
	done := func() {}
	pool := opts.Pool
	switch {
	case pool == nil && db.Type == "spanner":
		sdb, err := spannerdb.Open(ctx, db.Spanner, "secrets")
		if err != nil {
			return nil, nil, err
		}
		done = sdb.Close
		if b, err = spannerstore.Open(ctx, sdb.Admin, sdb.Client); err != nil {
			done()
			return nil, nil, fmt.Errorf("ok secrets' store: %w", err)
		}
	default:
		if pool == nil {
			dsn, err := config.Secret(db.URLFile)
			if err != nil {
				return nil, nil, fmt.Errorf("the database's url_file: %w", err)
			}
			if dsn == "" {
				return nil, nil, errors.New("the database's url_file names no database")
			}
			if pool, err = pgdb.Open(ctx, dsn, Schema, db.MaxConnections); err != nil {
				return nil, nil, err
			}
			done = pool.Close
		}
		pb, err := postgres.Open(ctx, pool)
		if err != nil {
			done()
			return nil, nil, fmt.Errorf("ok secrets' store: %w", err)
		}
		b = pb
	}
	o := opts.Opener
	if o == nil {
		o = opener()
	}
	// The root is reached now, so a root ok secrets cannot use fails it at start.
	if _, err := o.Root(ctx, cfg.Secrets.Root); err != nil {
		done()
		return nil, nil, fmt.Errorf("secrets.root: %w", err)
	}
	stores := opts.Stores
	if stores == nil {
		var closeStores func()
		var err error
		if stores, closeStores, err = openStores(ctx, cfg.Secrets.Stores); err != nil {
			done()
			return nil, nil, err
		}
		closeDB := done
		done = func() { closeStores(); closeDB() }
	}
	scfg := store.Config{Root: cfg.Secrets.Root, Opener: o, Stores: stores}
	if opts.RootOf != nil {
		scfg.RootOf = opts.RootOf
	}
	st, err := store.New(b, scfg)
	if err != nil {
		done()
		return nil, nil, err
	}
	return st, done, nil
}

// openStores are the external stores the configuration names (docs/secrets.md, "Stores"), each
// reached as the workload, and what closes them.
func openStores(ctx context.Context, configured map[string]config.SecretStore) (secretstore.Stores, func(), error) {
	out := secretstore.Stores{}
	var closers []func() error
	done := func() {
		for _, c := range closers {
			_ = c()
		}
	}
	for name, c := range configured {
		var st secretstore.Store
		var err error
		switch c.Kind {
		case config.StoreVault:
			st, err = vault.FromConfig(c)
		case config.StoreAWSSecretsManager:
			st, err = awssm.FromConfig(ctx, c)
		case config.StoreGCPSecretManager:
			var g *gcpsm.Store
			if g, err = gcpsm.FromConfig(ctx, c); err == nil {
				st, closers = g, append(closers, g.Close)
			}
		default:
			err = fmt.Errorf("no store is of kind %q", c.Kind)
		}
		if err != nil {
			done()
			return nil, nil, fmt.Errorf("secrets.stores.%s: %w", name, err)
		}
		out[name] = st
	}
	return out, done, nil
}

// Build assembles ok secrets from its configuration.
func Build(ctx context.Context, cfg *config.Config, opts Options) (*Role, error) {
	r := &Role{}
	ok := false
	defer func() {
		if !ok {
			r.Close()
		}
	}()
	idConn, scmConn := opts.Identity, opts.SCM
	if idConn == nil || scmConn == nil {
		if cfg.Services.Identity == "" || cfg.Services.SCM == "" {
			return nil, errors.New("services.identity and services.scm are where ok secrets reads who is in which set, and main")
		}
		o, err := dialOptions(cfg, opts)
		if err != nil {
			return nil, err
		}
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
	// Each repository's keys are wrapped by its organization's own root, where it names one.
	if opts.RootOf == nil {
		roots := identitypb.NewSecretsRootsClient(idConn)
		opts.RootOf = func(ctx context.Context, repo int64) (string, error) {
			r, err := roots.GetRepositorySecretsRoot(ctx, &identitypb.GetRepositorySecretsRootRequest{RepositoryId: repo})
			return r.GetRoot(), err
		}
	}
	st, done, err := OpenStore(ctx, cfg, opts)
	if err != nil {
		return nil, err
	}
	r.closers = append(r.closers, done)
	r.Store = st
	roots := opts.Roots
	if roots == nil {
		if roots, err = idclient.NewRoots(ctx, idConn, "secrets"); err != nil {
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
	policy := service.NewPolicy(service.SCM{Repositories: scmpb.NewRepositoriesClient(scmConn), Browse: scmpb.NewBrowseClient(scmConn)},
		service.Directory{Client: identitypb.NewDirectoryClient(idConn)}, cfg.Names())
	approvals := opts.Approvals
	if approvals == nil {
		approvals = service.IdentityApprovals{Client: identitypb.NewApprovalsClient(idConn)}
	}
	scfg := service.Config{Store: st, Policy: policy, Approvals: approvals}

	// Moves of its repositories' rows between cells, from the same role of the source cell.
	peers := cells.NewPeers(identitypb.NewCellsClient(idConn), func(addr string) (grpc.ClientConnInterface, func(), error) {
		if opts.Peers != nil {
			return opts.Peers(addr), nil, nil
		}
		o, err := dialOptions(cfg, opts)
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
	mover := st.Mover()
	moves := move.NewNode("ok/secrets/moves", move.NewServer(mover, okpb.Caller_CALLER_SECRETS,
		func(ctx context.Context, cell string) (okpb.MovesClient, error) {
			c, err := peers.Conn(ctx, cell, cells.Secrets)
			if err != nil {
				return nil, err
			}
			return okpb.NewMovesClient(c), nil
		}), mover.Purge)
	// Each repository's daily collection, asking the SCM what it still holds.
	collect := &service.Collector{Store: st, Refs: service.SCMReferences{Client: scmpb.NewSecretReferencesClient(scmConn)},
		Hour: service.DefaultCollectHour}
	// ok automation, which says whose a post run is, where the installation runs it.
	autoConn := opts.Automation
	if autoConn == nil && cfg.Services.Automation != "" {
		o, err := dialOptions(cfg, opts)
		if err != nil {
			return nil, err
		}
		c, err := dial.Dial(cfg.Services.Automation, o)
		if err != nil {
			return nil, fmt.Errorf("dialing %s: %w", cfg.Services.Automation, err)
		}
		r.closers = append(r.closers, func() { c.Close() })
		autoConn = c
	}
	var runs service.Runs
	if autoConn != nil {
		runs = service.AutomationRuns{Client: automationpb.NewSecretRunsClient(autoConn)}
	}
	r.Root = &assembly{deps: []node.Node{
		moves,
		&services{secrets: service.NewSecrets(scfg), versions: service.NewVersions(scfg), deploys: service.NewDeploySecrets(scfg),
			runs: service.NewRunSecrets(scfg, runs)},
		&app{name: "ok/secrets/collect", run: collect.Run},
		roots,
	}}
	ok = true
	return r, nil
}

// dialOptions are how ok secrets dials ok's other roles, as itself.
func dialOptions(cfg *config.Config, opts Options) (dial.Options, error) {
	o, err := idclient.Options(cfg.Services)
	if err != nil {
		return dial.Options{}, err
	}
	if opts.Dev && cfg.Workload.TokenFile == "" && cfg.Workload.CredentialDir == "" {
		// Development, with no workload credential: the role names itself to a development
		// identity, as ok serve's roles do.
		o.DevPrincipal = cfg.RobotName(cfg.Roles.Secrets)
	} else if o.Credential, err = idclient.Credential(cfg, o); err != nil {
		return dial.Options{}, err
	}
	return o, nil
}

// services are ok secrets' services on the role's port.
type services struct {
	secrets  *service.Secrets
	versions *service.Versions
	deploys  *service.DeploySecrets
	runs     *service.RunSecrets
}

var _ node.Service = (*services)(nil)

func (*services) Name() string                 { return "ok/secrets/services" }
func (*services) Dependencies() []node.Node    { return nil }
func (*services) Setup(context.Context) error  { return nil }
func (*services) Healthy(context.Context) bool { return true }

func (s *services) RegisterRPC(_ context.Context, reg grpc.ServiceRegistrar) error {
	secretspb.RegisterSecretsServer(reg, s.secrets)
	secretspb.RegisterVersionsServer(reg, s.versions)
	secretspb.RegisterDeploySecretsServer(reg, s.deploys)
	secretspb.RegisterRunSecretsServer(reg, s.runs)
	return nil
}

// app is one of the role's loops.
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

func (*assembly) Name() string                 { return "ok/secrets" }
func (a *assembly) Dependencies() []node.Node  { return a.deps }
func (*assembly) Healthy(context.Context) bool { return true }
func (*assembly) Setup(context.Context) error  { return nil }
