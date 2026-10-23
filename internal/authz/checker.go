package authz

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// TTL is how long a Checker keeps a caller's facts about a repository: a change in the directory —
// a grant, a membership, a visibility — is decided on within it, or at once where the role follows
// the directory's changes (Forget, ForgetCaller).
const TTL = 5 * time.Second

// maxCached bounds a Checker's facts; past it, expired ones are swept as new ones are kept.
const maxCached = 100_000

// ErrNotFound is a repository there is none of, or one being created.
var ErrNotFound = errors.New("no such repository")

// Source is where a Checker reads facts: ok identity's Authorization service.
type Source interface {
	Facts(ctx context.Context, in *identitypb.FactsRequest, opts ...grpc.CallOption) (*identitypb.FactsResponse, error)
}

// Checker decides what callers may do to repositories for a role, from facts ok identity answers,
// each kept TTL. It is safe for concurrent use.
type Checker struct {
	src Source
	now func() time.Time

	mu    sync.Mutex
	cache map[key]entry
}

type key struct {
	caller string
	repo   int64
}

type entry struct {
	f  Facts
	at time.Time
}

// NewChecker decides from src's facts.
func NewChecker(src Source) *Checker {
	return &Checker{src: src, now: time.Now, cache: map[key]entry{}}
}

// Facts are a caller's (by stable ID; "" for no one signed in) facts about a repository;
// ErrNotFound for none.
func (c *Checker) Facts(ctx context.Context, caller string, repo int64) (Facts, error) {
	k := key{caller, repo}
	c.mu.Lock()
	e, ok := c.cache[k]
	c.mu.Unlock()
	if ok && c.now().Sub(e.at) < TTL {
		return e.f, nil
	}
	resp, err := c.src.Facts(ctx, &identitypb.FactsRequest{CallerId: caller, RepositoryId: repo})
	if status.Code(err) == codes.NotFound {
		return Facts{}, fmt.Errorf("repository %d: %w", repo, ErrNotFound)
	}
	if err != nil {
		return Facts{}, err
	}
	f, err := FactsOf(resp, caller)
	if err != nil {
		return Facts{}, err
	}
	now := c.now()
	c.mu.Lock()
	if len(c.cache) >= maxCached {
		for k, e := range c.cache {
			if now.Sub(e.at) >= TTL {
				delete(c.cache, k)
			}
		}
	}
	c.cache[k] = entry{f: f, at: now}
	c.mu.Unlock()
	return f, nil
}

// Decide decides whether a caller, with token (nil for none), may do an action to a repository. A
// repository there is none of is decided as one the caller cannot see.
func (c *Checker) Decide(ctx context.Context, caller string, token *Token, repo int64, a Action) (Decision, error) {
	f, err := c.Facts(ctx, caller, repo)
	if errors.Is(err, ErrNotFound) {
		return Decision{Why: "no such repository"}, nil
	}
	if err != nil {
		return Decision{}, err
	}
	f.Caller.Token = token
	return Decide(f, a), nil
}

// Forget drops what the Checker keeps about a repository: its visibility, owner, or grants
// changed.
func (c *Checker) Forget(repo int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.cache {
		if k.repo == repo {
			delete(c.cache, k)
		}
	}
}

// ForgetCaller drops what the Checker keeps about a caller: their memberships, teams, or status
// changed.
func (c *Checker) ForgetCaller(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.cache {
		if k.caller == id {
			delete(c.cache, k)
		}
	}
}

// FactsOf are the facts ok identity answered for a caller.
func FactsOf(resp *identitypb.FactsResponse, caller string) (Facts, error) {
	r, cl := resp.GetRepository(), resp.GetCaller()
	v, err := tenancy.ParseVisibility(r.GetVisibility())
	if err != nil {
		return Facts{}, err
	}
	propose, err := tenancy.ParseRole(r.GetPropose())
	if err != nil {
		return Facts{}, err
	}
	base, err := tenancy.ParseRole(r.GetBaseRole())
	if err != nil {
		return Facts{}, err
	}
	m := tenancy.NotMember
	if cl.GetMembership() != tenancy.NotMember.String() {
		if m, err = tenancy.ParseMembership(cl.GetMembership()); err != nil {
			return Facts{}, err
		}
	}
	f := Facts{
		Repository: Repository{ID: r.GetId(), Visibility: v, Propose: propose, Owner: r.GetOwnerId(), OwnedByOrg: r.GetOwnedByOrg(),
			Org: Org{BaseRole: base, PrivateForks: r.GetPrivateForks()}, Cell: r.GetCell()},
		Caller: Caller{ID: caller, Active: cl.GetActive(), Membership: m, Internal: cl.GetInternal()},
	}
	if r.GetDeleted() {
		f.Repository.State = Deleted
	}
	for _, g := range cl.GetGranted() {
		role, err := tenancy.ParseRole(g)
		if err != nil {
			return Facts{}, err
		}
		f.Caller.Granted = append(f.Caller.Granted, role)
	}
	return f, nil
}
