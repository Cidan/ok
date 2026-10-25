package service

import (
	"context"
	"errors"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Facts gathers what Authorize decides from (internal/authz): the repository, its owner, and the
// caller's place — their membership of the owning organization, the roles granted them directly
// and through every set they are in, and whether they read its internal visibility.
func (s *Server) Facts(ctx context.Context, req *identitypb.FactsRequest) (*identitypb.FactsResponse, error) {
	r, err := s.cfg.Store.Repository(ctx, req.GetRepositoryId())
	if err != nil {
		return nil, grpcError(err)
	}
	if r.State == store.Creating {
		return nil, status.Errorf(codes.NotFound, "repository %d is being created", r.ID)
	}
	f, err := s.facts(ctx, req.GetCallerId(), r)
	if err != nil {
		return nil, grpcError(err)
	}
	return factsOut(f), nil
}

func factsOut(f authz.Facts) *identitypb.FactsResponse {
	r, c := f.Repository, f.Caller
	out := &identitypb.FactsResponse{
		Repository: &identitypb.FactsResponse_Repository{Id: r.ID, Visibility: r.Visibility.String(), Propose: r.Propose.String(),
			Deleted: r.State == authz.Deleted, OwnerId: r.Owner, OwnedByOrg: r.OwnedByOrg, BaseRole: r.Org.BaseRole.String(),
			PrivateForks: r.Org.PrivateForks, Cell: r.Cell},
		Caller: &identitypb.FactsResponse_Caller{Active: c.Active, Membership: c.Membership.String(), Internal: c.Internal},
	}
	for _, g := range c.Granted {
		out.Caller.Granted = append(out.Caller.Granted, g.String())
	}
	return out
}

// facts are a caller's (by stable ID; "" for no one) facts about a repository.
func (s *Server) facts(ctx context.Context, callerID string, r store.Repository) (authz.Facts, error) {
	owner, err := s.cfg.Store.PrincipalByID(ctx, r.Owner)
	if err != nil {
		return authz.Facts{}, err
	}
	f := authz.Facts{Repository: authz.Repository{ID: r.ID, Visibility: r.Visibility, Propose: r.Propose, Owner: r.Owner,
		OwnedByOrg: owner.Kind == store.Org, Cell: s.cfg.Store.CellOf(r)}}
	if r.State == store.Deleted {
		f.Repository.State = authz.Deleted
	}
	var org store.Organization
	if owner.Kind == store.Org {
		if org, err = s.cfg.Store.Organization(ctx, r.Owner); err != nil {
			return authz.Facts{}, err
		}
		f.Repository.Org = authz.Org{BaseRole: org.BaseRole, PrivateForks: org.PrivateForks}
	}
	if callerID == "" {
		return f, nil
	}
	caller, err := s.cfg.Store.PrincipalByID(ctx, callerID)
	if errors.Is(err, store.ErrNotFound) {
		return f, nil // no one the directory knows: as good as no one
	}
	if err != nil {
		return authz.Facts{}, err
	}
	f.Caller = authz.Caller{ID: callerID, Active: caller.Active()}
	sets := s.setsOnce(ctx, caller)
	memberships, err := s.memberships(ctx, caller, sets)
	if err != nil {
		return authz.Facts{}, err
	}
	if owner.Kind == store.Org {
		f.Caller.Membership = memberships[r.Owner]
		if f.Caller.Internal, err = s.internal(ctx, org, memberships); err != nil {
			return authz.Facts{}, err
		}
	}
	grants, err := s.cfg.Store.Grants(ctx, r.ID)
	if err != nil {
		return authz.Facts{}, err
	}
	if len(grants) > 0 {
		in, err := sets()
		if err != nil {
			return authz.Facts{}, err
		}
		for _, g := range grants {
			if g.Subject == callerID || in[g.Subject] {
				f.Caller.Granted = append(f.Caller.Granted, g.Role)
			}
		}
	}
	// A fork that is not public reads as its fork tree's root does (GitHub's rules): it takes the
	// root's teams' grants, the root's owners read it, and an internal one the root's enterprise.
	if r.Parent != 0 && r.Visibility != tenancy.Public {
		if err := s.forkFacts(ctx, &f, caller, r, memberships, sets); err != nil {
			return authz.Facts{}, err
		}
	}
	// An app holds what its installation in the repository's owner gives it.
	if caller.Kind == store.Robot {
		app, err := s.cfg.Store.AppOfRobot(ctx, caller.ID)
		if err == nil {
			i, err := s.cfg.Store.InstallationOf(ctx, app.ID, r.Owner)
			if err == nil && i.Covers(r.ID) {
				f.Caller.Granted = append(f.Caller.Granted, i.Role)
			} else if err != nil && !errors.Is(err, store.ErrNotFound) {
				return authz.Facts{}, err
			}
		} else if !errors.Is(err, store.ErrNotFound) {
			return authz.Facts{}, err
		}
	}
	return f, nil
}

// maxForkDepth bounds the walk from a fork to its fork tree's root.
const maxForkDepth = 64

// root is a fork's fork tree's root: the repository its upstreams lead to.
func (s *Server) root(ctx context.Context, r store.Repository) (store.Repository, error) {
	for i := 0; r.Parent != 0 && i < maxForkDepth; i++ {
		p, err := s.cfg.Store.Repository(ctx, r.Parent)
		if errors.Is(err, store.ErrNotFound) {
			break
		}
		if err != nil {
			return store.Repository{}, err
		}
		r = p
	}
	return r, nil
}

// forkFacts adds to a non-public fork's facts what its fork tree's root gives the caller, whose
// memberships and sets the facts already read.
func (s *Server) forkFacts(ctx context.Context, f *authz.Facts, caller store.Principal, r store.Repository,
	memberships map[string]tenancy.Membership, sets func() (map[string]bool, error)) error {
	root, err := s.root(ctx, r)
	if err != nil || root.ID == r.ID {
		return err
	}
	owner, err := s.cfg.Store.PrincipalByID(ctx, root.Owner)
	if err != nil {
		return err
	}
	switch {
	case owner.Kind != store.Org && root.Owner == caller.ID, owner.Kind == store.Org && memberships[root.Owner] == tenancy.Owner:
		f.Caller.Granted = append(f.Caller.Granted, tenancy.Read)
	}
	if r.Visibility == tenancy.Internal && owner.Kind == store.Org {
		org, err := s.cfg.Store.Organization(ctx, root.Owner)
		if err != nil {
			return err
		}
		in, err := s.internal(ctx, org, memberships)
		if err != nil {
			return err
		}
		f.Caller.Internal = f.Caller.Internal || in
	}
	grants, err := s.cfg.Store.Grants(ctx, root.ID)
	if err != nil {
		return err
	}
	for _, g := range grants {
		if !s.cfg.Store.IsSet(g.Subject) {
			continue // the root's own people's grants are its, not its forks'
		}
		in, err := sets()
		if err != nil {
			return err
		}
		if in[g.Subject] {
			f.Caller.Granted = append(f.Caller.Granted, g.Role)
		}
	}
	return nil
}

// setsOnce is the sets p is in (Store.SetsOf), read the first time they are asked for and kept for
// the rest of the request.
func (s *Server) setsOnce(ctx context.Context, p store.Principal) func() (map[string]bool, error) {
	var sets map[string]bool
	var err error
	read := false
	return func() (map[string]bool, error) {
		if !read {
			sets, err = s.cfg.Store.SetsOf(ctx, p)
			read = true
		}
		return sets, err
	}
}

// memberships are a principal's memberships, by organization ID: the organizations' records, and
// the installation's, which its administrators own and its active people and robots are members of
// — but an organization's robots, which are members of nothing and reach repositories through their
// grants alone. sets are the sets p is in.
func (s *Server) memberships(ctx context.Context, p store.Principal, sets func() (map[string]bool, error)) (map[string]tenancy.Membership, error) {
	ms, err := s.cfg.Store.MembershipsOf(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	if p.Kind == store.Robot {
		if _, err := s.cfg.Store.RobotOrganization(ctx, p.ID); err == nil {
			return ms, nil
		} else if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	}
	if s.cfg.Installation != "" && p.Active() && (p.Kind == store.Human || p.Kind == store.Robot) {
		ms[s.cfg.Installation] = tenancy.Member
		admin, err := s.administers(ctx, p, sets)
		if err != nil {
			return nil, err
		}
		if admin {
			ms[s.cfg.Installation] = tenancy.Owner
		}
	}
	return ms, nil
}

// internal reports whether a member of the organizations of memberships reads org's internal
// repositories: one of org's enterprise, or of org when it has none.
func (s *Server) internal(ctx context.Context, org store.Organization, memberships map[string]tenancy.Membership) (bool, error) {
	if org.Enterprise == "" {
		return memberships[org.ID] != tenancy.NotMember, nil
	}
	for id, m := range memberships {
		if m == tenancy.NotMember {
			continue
		}
		o, err := s.cfg.Store.Organization(ctx, id)
		if err != nil {
			return false, err
		}
		if o.Enterprise == org.Enterprise {
			return true, nil
		}
	}
	return false, nil
}

// Admins are the active people who administer a repository: its owner, if a person; its
// organization's owners — an installation's, ok's administrators; and those granted admin on it,
// directly or through a set.
func (s *Server) Admins(ctx context.Context, req *identitypb.AdminsRequest) (*identitypb.AdminsResponse, error) {
	r, err := s.cfg.Store.Repository(ctx, req.GetRepositoryId())
	if err != nil {
		return nil, grpcError(err)
	}
	owner, err := s.cfg.Store.PrincipalByID(ctx, r.Owner)
	if err != nil {
		return nil, grpcError(err)
	}
	names := map[string]bool{}
	switch {
	case owner.Kind != store.Org:
		names[owner.Name] = true
	case owner.ID == s.cfg.Installation:
		admins, err := s.ListAdmins(ctx, &identitypb.ListAdminsRequest{})
		if err != nil {
			return nil, err
		}
		for _, n := range admins.GetPrincipals() {
			names[n] = true
		}
	default:
		members, err := s.cfg.Store.Members(ctx, owner.ID)
		if err != nil {
			return nil, grpcError(err)
		}
		for _, m := range members {
			if m.Role != tenancy.Owner {
				continue
			}
			p, err := s.cfg.Store.PrincipalByID(ctx, m.Principal)
			if err != nil {
				return nil, grpcError(err)
			}
			names[p.Name] = true
		}
	}
	grants, err := s.cfg.Store.Grants(ctx, r.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	for _, g := range grants {
		if g.Role != tenancy.Admin {
			continue
		}
		if p, err := s.cfg.Store.PrincipalByID(ctx, g.Subject); err == nil {
			names[p.Name] = true
			continue
		} else if !errors.Is(err, store.ErrNotFound) {
			return nil, grpcError(err)
		}
		members, _, err := s.cfg.Store.Expand(ctx, g.Subject)
		if err != nil {
			return nil, grpcError(err)
		}
		for _, n := range members {
			names[n] = true
		}
	}
	all := make([]string, 0, len(names))
	for n := range names {
		all = append(all, n)
	}
	sort.Strings(all)
	active, ids, err := s.active(ctx, all)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.AdminsResponse{Principals: active, PrincipalIds: ids}, nil
}
