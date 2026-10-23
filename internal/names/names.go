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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/serve/authn"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// TTL is how long a Resolver keeps an answer: a rename shows within it.
const TTL = time.Minute

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

// Resolver resolves principals through the directory, keeping each answer TTL. It is safe for
// concurrent use.
type Resolver struct {
	dir Directory
	now func() time.Time

	mu     sync.Mutex
	byID   map[string]entry
	byName map[string]entry
}

type entry struct {
	p     authn.Principal
	found bool
	at    time.Time
}

// New resolves through dir.
func New(dir Directory) *Resolver {
	return &Resolver{dir: dir, now: time.Now, byID: map[string]entry{}, byName: map[string]entry{}}
}

// ErrUnknown is a name or ID the directory has no principal for.
var ErrUnknown = errors.New("no such principal")

func (r *Resolver) cached(m map[string]entry, key string) (entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := m[key]
	if !ok || r.now().Sub(e.at) > TTL {
		return entry{}, false
	}
	return e, true
}

func (r *Resolver) keep(p authn.Principal, found bool, id, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := entry{p: p, found: found, at: r.now()}
	if id != "" {
		r.byID[id] = e
	}
	if name != "" {
		r.byName[name] = e
	}
	if found {
		r.byID[p.ID] = e
		r.byName[p.Name] = e
	}
}

// ByID is the principal with a stable ID, under its current name — a suspended or deleted one's
// too, so history shows who it was. ErrUnknown for none.
func (r *Resolver) ByID(ctx context.Context, id string) (authn.Principal, error) {
	if e, ok := r.cached(r.byID, id); ok {
		if !e.found {
			return authn.Principal{}, fmt.Errorf("principal %s: %w", id, ErrUnknown)
		}
		return e.p, nil
	}
	p, err := r.dir.PrincipalByID(ctx, id)
	switch {
	case errors.Is(err, authn.ErrNotFound):
		r.keep(authn.Principal{}, false, id, "")
		return authn.Principal{}, fmt.Errorf("principal %s: %w", id, ErrUnknown)
	case err != nil:
		return authn.Principal{}, err
	}
	r.keep(p, true, id, "")
	return p, nil
}

// ByName is the principal a name names now. ErrUnknown for none.
func (r *Resolver) ByName(ctx context.Context, name string) (authn.Principal, error) {
	if e, ok := r.cached(r.byName, name); ok {
		if !e.found {
			return authn.Principal{}, fmt.Errorf("%s: %w", name, ErrUnknown)
		}
		return e.p, nil
	}
	p, err := r.dir.Principal(ctx, name)
	switch {
	case errors.Is(err, authn.ErrNotFound):
		r.keep(authn.Principal{}, false, "", name)
		return authn.Principal{}, fmt.Errorf("%s: %w", name, ErrUnknown)
	case err != nil:
		return authn.Principal{}, err
	}
	if p.Name != name {
		// The directory answered for a name the principal no longer has.
		r.keep(authn.Principal{}, false, "", name)
		return authn.Principal{}, fmt.Errorf("%s: %w", name, ErrUnknown)
	}
	r.keep(p, true, "", name)
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

// Names are Name of each ID, in order.
func (r *Resolver) Names(ctx context.Context, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	out := make([]string, len(ids))
	for i, id := range ids {
		n, err := r.Name(ctx, id)
		if err != nil {
			return nil, err
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
	out := make([]string, len(ns))
	for i, n := range ns {
		id, err := r.ID(ctx, n)
		if err != nil {
			return nil, err
		}
		out[i] = id
	}
	return out, nil
}
