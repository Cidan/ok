// Package role builds ok's edge from an installation's configuration (docs/hosted.md, "Cells, as
// built"): `ok edge serve`.
package role

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/edge"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Role is a built edge.
type Role struct {
	Root    node.Node
	Edge    *edge.Edge
	closers []func()
}

// Close releases what the role holds.
func (r *Role) Close() {
	for i := len(r.closers) - 1; i >= 0; i-- {
		r.closers[i]()
	}
}

// Options are how the role runs beside its configuration.
type Options struct {
	// Dev accepts development principals, which the edge passes on to development roles behind it:
	// for tests, on loopback.
	Dev bool
	// Identity, when set, is ok identity, dialed as the edge: tests.
	Identity grpc.ClientConnInterface
	// Roots, when set, are the identity CA's certificates: tests.
	Roots *idclient.Roots
}

// Build builds the edge: it reaches ok identity at services.identity as its robot (roles.edge),
// and the cells' roles where the directory lists them, over TLS or cleartext as services says.
func Build(ctx context.Context, cfg *config.Config, opts Options) (_ *Role, err error) {
	r := &Role{}
	defer func() {
		if err != nil {
			r.Close()
		}
	}()
	if cfg.Roles.Edge == "" {
		return nil, errors.New("roles.edge names the robot the edge runs as")
	}
	if cfg.Services.Identity == "" {
		return nil, errors.New("services.identity is where the edge reaches ok identity, which it authenticates requests with and sends identity's to")
	}
	base, err := idclient.Options(cfg.Services)
	if err != nil {
		return nil, err
	}
	conn := opts.Identity
	if conn == nil {
		o := base
		if opts.Dev && cfg.Workload.TokenFile == "" && cfg.Workload.CredentialDir == "" {
			o.DevPrincipal = cfg.RobotName(cfg.Roles.Edge)
		} else if o.Credential, err = idclient.Credential(cfg, base); err != nil {
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
		if roots, err = idclient.NewRoots(ctx, conn, "edge"); err != nil {
			return nil, err
		}
	}
	resolver := authn.NewResolver(authn.Config{
		Directory:     idclient.NewDirectory(conn),
		Credentials:   credential.NewVerifier(roots.Pool),
		TrustedHeader: cfg.Identity.TrustedHeader,
		Dev:           opts.Dev,
		Roles:         idclient.Roles(cfg),
	})
	r.Edge = edge.New(edge.Config{Resolver: resolver, Cells: identitypb.NewCellsClient(conn), Directory: cfg.Services.Identity,
		UpstreamTLS: base.TLS, Edge: cfg.Edge})
	r.Root = &assembly{deps: []node.Node{r.Edge, roots}}
	return r, nil
}

type assembly struct{ deps []node.Node }

func (*assembly) Name() string                 { return "ok/edge/role" }
func (a *assembly) Dependencies() []node.Node  { return a.deps }
func (*assembly) Healthy(context.Context) bool { return true }
func (*assembly) Setup(context.Context) error  { return nil }
