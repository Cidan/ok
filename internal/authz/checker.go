package authz

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// ErrNotFound is a repository there is none of, or one being created.
var ErrNotFound = errors.New("no such repository")

// Source is where a Checker reads facts: ok identity's Authorization service.
type Source interface {
	Facts(ctx context.Context, in *identitypb.FactsRequest, opts ...grpc.CallOption) (*identitypb.FactsResponse, error)
}

// Checker decides what callers may do to repositories for a role, from facts ok identity answers
// for each decision: no replica keeps them, so a change in the directory — a grant, a membership, a
// visibility, a move — is decided on at once (docs/state.md). It is safe for concurrent use.
type Checker struct{ src Source }

// NewChecker decides from src's facts.
func NewChecker(src Source) *Checker { return &Checker{src: src} }

// Facts are a caller's (by stable ID; "" for no one signed in) facts about a repository, as ok
// identity answers them now; ErrNotFound for none.
func (c *Checker) Facts(ctx context.Context, caller string, repo int64) (Facts, error) {
	resp, err := c.src.Facts(ctx, &identitypb.FactsRequest{CallerId: caller, RepositoryId: repo})
	if status.Code(err) == codes.NotFound {
		return Facts{}, fmt.Errorf("repository %d: %w", repo, ErrNotFound)
	}
	if err != nil {
		return Facts{}, err
	}
	return FactsOf(resp, caller)
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
