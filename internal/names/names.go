// Package names turns principals' stable IDs into the names people see, and names into IDs
// (docs/hosted.md, "Tenancy, as built"). Every role stores principals by ID — a CL's author, a
// run's requester, a deployment's actor — so a rename, or an installation moving in, changes no
// row outside the directory; a role presents names at its boundary, and resolves the names people
// send it, through a Resolver.
package names

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/serve/authn"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Directory is what a Resolver asks: an authn.Directory answers it, and so does identity's
// Directory service (Client).
type Directory interface {
	// Principal returns the principal with a name; authn.ErrNotFound for none.
	Principal(ctx context.Context, name string) (authn.Principal, error)
	// PrincipalByID returns the principal with a stable ID; authn.ErrNotFound for none.
	PrincipalByID(ctx context.Context, id string) (authn.Principal, error)
}

// GetPrincipaler is the part of identity's Directory service a Client asks.
type GetPrincipaler interface {
	GetPrincipal(ctx context.Context, in *identitypb.GetPrincipalRequest, opts ...grpc.CallOption) (*identitypb.GetPrincipalResponse, error)
}

// Client is a Directory over identity's Directory service.
type Client struct{ Dir GetPrincipaler }

func (c Client) get(ctx context.Context, req *identitypb.GetPrincipalRequest) (authn.Principal, error) {
	resp, err := c.Dir.GetPrincipal(ctx, req)
	if status.Code(err) == codes.NotFound {
		return authn.Principal{}, authn.ErrNotFound
	}
	if err != nil {
		return authn.Principal{}, err
	}
	p := resp.GetPrincipal()
	return authn.Principal{Name: p.GetName(), ID: p.GetId(), Human: p.GetKind() == identitypb.Kind_KIND_HUMAN, Org: p.GetKind() == identitypb.Kind_KIND_ORG,
		Active: p.GetStatus() == identitypb.Status_STATUS_ACTIVE}, nil
}

func (c Client) Principal(ctx context.Context, name string) (authn.Principal, error) {
	return c.get(ctx, &identitypb.GetPrincipalRequest{Key: &identitypb.GetPrincipalRequest_Name{Name: name}})
}

func (c Client) PrincipalByID(ctx context.Context, id string) (authn.Principal, error) {
	return c.get(ctx, &identitypb.GetPrincipalRequest{Key: &identitypb.GetPrincipalRequest_Id{Id: id}})
}

// Resolver resolves principals through the directory, asking it each time: a rename shows at once,
// and no replica keeps a principal (docs/state.md). It is safe for concurrent use.
type Resolver struct{ dir Directory }

// New resolves through dir.
func New(dir Directory) *Resolver { return &Resolver{dir: dir} }

// ErrUnknown is a name or ID the directory has no principal for.
var ErrUnknown = errors.New("no such principal")

// WithScope is ctx resolving each principal once for what it is used for — one request, one status
// built — and asking the directory again for the next: what a page or a status shows of a person
// it names many times (an author, a reviewer, an approver), it reads once, and nothing outlives
// it (docs/state.md).
func WithScope(ctx context.Context) context.Context {
	if _, ok := ctx.Value(scopeKey{}).(*scope); ok {
		return ctx
	}
	return context.WithValue(ctx, scopeKey{}, &scope{byID: map[string]answer{}, byName: map[string]answer{}})
}

type scopeKey struct{}

// scope is what a WithScope context resolved: each principal it found, and each it did not.
type scope struct {
	mu     sync.Mutex
	byID   map[string]answer
	byName map[string]answer
}

type answer struct {
	p       authn.Principal
	unknown bool
}

// scoped answers from ctx's scope, if it has one, or asks the directory through ask and keeps the
// answer — a principal, or none — for the scope's life; an error is not kept.
func scoped(ctx context.Context, byID bool, key string, ask func() (authn.Principal, error)) (authn.Principal, error) {
	sc, ok := ctx.Value(scopeKey{}).(*scope)
	if !ok {
		return ask()
	}
	m := sc.byName
	if byID {
		m = sc.byID
	}
	sc.mu.Lock()
	a, found := m[key]
	sc.mu.Unlock()
	if found {
		if a.unknown {
			return authn.Principal{}, authn.ErrNotFound
		}
		return a.p, nil
	}
	p, err := ask()
	switch {
	case errors.Is(err, authn.ErrNotFound):
		a = answer{unknown: true}
	case err != nil:
		return p, err
	default:
		a = answer{p: p}
	}
	sc.mu.Lock()
	m[key] = a
	sc.mu.Unlock()
	return p, err
}

// ByID is the principal with a stable ID, under its current name — a suspended or deleted one's
// too, so history shows who it was. ErrUnknown for none.
func (r *Resolver) ByID(ctx context.Context, id string) (authn.Principal, error) {
	p, err := scoped(ctx, true, id, func() (authn.Principal, error) { return r.dir.PrincipalByID(ctx, id) })
	if errors.Is(err, authn.ErrNotFound) {
		return authn.Principal{}, fmt.Errorf("principal %s: %w", id, ErrUnknown)
	}
	return p, err
}

// ByName is the principal a name names now. ErrUnknown for none.
func (r *Resolver) ByName(ctx context.Context, name string) (authn.Principal, error) {
	p, err := scoped(ctx, false, name, func() (authn.Principal, error) { return r.dir.Principal(ctx, name) })
	switch {
	case errors.Is(err, authn.ErrNotFound):
		return authn.Principal{}, fmt.Errorf("%s: %w", name, ErrUnknown)
	case err != nil:
		return authn.Principal{}, err
	case p.Name != name:
		// The directory answered for a name the principal no longer has.
		return authn.Principal{}, fmt.Errorf("%s: %w", name, ErrUnknown)
	}
	return p, nil
}

// Name is the current name of the principal with an ID; a principal the directory no longer has
// is shown by its ID.
func (r *Resolver) Name(ctx context.Context, id string) (string, error) {
	if id == "" {
		return "", nil
	}
	p, err := r.ByID(ctx, id)
	if errors.Is(err, ErrUnknown) {
		return id, nil
	}
	return p.Name, err
}

// Names are Name of each ID, in order: each distinct ID asked once.
func (r *Resolver) Names(ctx context.Context, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	asked := map[string]string{}
	out := make([]string, len(ids))
	for i, id := range ids {
		n, ok := asked[id]
		if !ok {
			var err error
			if n, err = r.Name(ctx, id); err != nil {
				return nil, err
			}
			asked[id] = n
		}
		out[i] = n
	}
	return out, nil
}

// ID is the stable ID of the principal a name names.
func (r *Resolver) ID(ctx context.Context, name string) (string, error) {
	p, err := r.ByName(ctx, name)
	return p.ID, err
}

// IDs are ID of each name, in order.
func (r *Resolver) IDs(ctx context.Context, ns []string) ([]string, error) {
	if len(ns) == 0 {
		return nil, nil
	}
	asked := map[string]string{}
	out := make([]string, len(ns))
	for i, n := range ns {
		id, ok := asked[n]
		if !ok {
			var err error
			if id, err = r.ID(ctx, n); err != nil {
				return nil, err
			}
			asked[n] = id
		}
		out[i] = id
	}
	return out, nil
}
