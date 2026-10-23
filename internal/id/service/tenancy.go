package service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// The Tenancy service (docs/hosted.md, "Tenancy, as built"). Each request acts for a person: its
// caller, or the one ok control or the SCM names. What that person may do to a repository,
// Authorize decides; an organization is its owners' to manage.

// actor is the person a request acts for: a person calling, who names no one else; or the person
// ok control or the SCM names, active.
func (s *Server) actor(ctx context.Context, forPerson string) (store.Principal, error) {
	c, err := callerOf(ctx)
	if err != nil {
		return store.Principal{}, err
	}
	id := c.ID
	if c.Human {
		if forPerson != "" && forPerson != c.ID {
			return store.Principal{}, status.Error(codes.PermissionDenied, "a person acts for themselves")
		}
	} else {
		if forPerson == "" {
			return store.Principal{}, status.Error(codes.InvalidArgument, "name the person the request is for (for_person_id)")
		}
		id = forPerson
	}
	p, err := s.cfg.Store.PrincipalByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) || (err == nil && (p.Kind != store.Human || !p.Active())) {
		return store.Principal{}, status.Errorf(codes.PermissionDenied, "%s is no active person", id)
	}
	return p, err
}

// isRole reports whether the caller is ok's role r.
func (s *Server) isRole(ctx context.Context, r okpb.Caller) bool {
	c, ok := authn.FromContext(ctx)
	return ok && !c.Human && c.Principal != "" && c.Principal == s.cfg.Roles[r]
}

// requireRole refuses a caller that is not ok's role r.
func (s *Server) requireRole(ctx context.Context, r okpb.Caller, what string) error {
	if !s.isRole(ctx, r) {
		return status.Errorf(codes.PermissionDenied, "%s is the %s's", what, roleName(r))
	}
	return nil
}

func roleName(r okpb.Caller) string {
	switch r {
	case okpb.Caller_CALLER_SCM:
		return "SCM"
	case okpb.Caller_CALLER_CONTROL:
		return "ok control"
	}
	return r.String()
}

func invalid(err error) error { return status.Error(codes.InvalidArgument, err.Error()) }

// ─── Organizations ───

func orgOut(o store.Organization, installation bool) *identitypb.Organization {
	return &identitypb.Organization{Id: o.ID, Handle: o.Handle(), BaseRole: o.BaseRole.String(), MembersCreate: o.MembersCreate,
		PrivateForks: o.PrivateForks, Enterprise: o.Enterprise, Installation: installation}
}

// organization is the organization with a handle; NOT_FOUND for none.
func (s *Server) organization(ctx context.Context, handle string) (store.Organization, error) {
	o, err := s.cfg.Store.OrganizationByHandle(ctx, handle)
	if errors.Is(err, store.ErrNotFound) {
		return store.Organization{}, status.Errorf(codes.NotFound, "no organization is called %s", handle)
	}
	return o, err
}

// membership is a person's membership of an organization.
func (s *Server) membership(ctx context.Context, org string, p store.Principal) (tenancy.Membership, error) {
	ms, err := s.memberships(ctx, p)
	if err != nil {
		return tenancy.NotMember, err
	}
	return ms[org], nil
}

// requireOwner refuses a person who does not own an organization.
func (s *Server) requireOwner(ctx context.Context, o store.Organization, p store.Principal, what string) error {
	m, err := s.membership(ctx, o.ID, p)
	if err != nil {
		return err
	}
	if m != tenancy.Owner {
		return status.Errorf(codes.PermissionDenied, "only an owner of %s may %s", o.Handle(), what)
	}
	return nil
}

// requireMember refuses a person who is not in an organization.
func (s *Server) requireMember(ctx context.Context, o store.Organization, p store.Principal) error {
	m, err := s.membership(ctx, o.ID, p)
	if err != nil {
		return err
	}
	if m == tenancy.NotMember {
		return status.Errorf(codes.PermissionDenied, "only %s's members see that", o.Handle())
	}
	return nil
}

// notInstallation refuses to manage the installation's organization by hand: its members and
// teams are its people and groups, as the system of record and the groups-and-ACLs tree say.
func (s *Server) notInstallation(o store.Organization, what string) error {
	if o.ID == s.cfg.Installation {
		return status.Errorf(codes.FailedPrecondition, "an installation's %s are its people and groups: the system of record and the groups-and-ACLs tree say who they are", what)
	}
	return nil
}

func (s *Server) CreateOrganization(ctx context.Context, req *identitypb.CreateOrganizationRequest) (*identitypb.Organization, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if !s.cfg.Hosted {
		return nil, status.Error(codes.FailedPrecondition, "an installation has one organization (tenancy.mode: installation)")
	}
	role := tenancy.Read
	if req.GetBaseRole() != "" {
		if role, err = tenancy.ParseRole(req.GetBaseRole()); err != nil {
			return nil, invalid(err)
		}
	}
	o, err := s.cfg.Store.CreateOrganization(ctx, req.GetHandle(), p.ID, store.OrganizationSettings{BaseRole: role,
		MembersCreate: req.GetMembersCreate(), PrivateForks: req.GetPrivateForks()})
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "organization.create", map[string]any{"organization": o.Name})
	return orgOut(o, false), nil
}

func (s *Server) GetOrganization(ctx context.Context, req *identitypb.GetOrganizationRequest) (*identitypb.Organization, error) {
	if _, err := callerOf(ctx); err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	return orgOut(o, o.ID == s.cfg.Installation), nil
}

func (s *Server) UpdateOrganization(ctx context.Context, req *identitypb.UpdateOrganizationRequest) (*identitypb.Organization, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.requireOwner(ctx, o, p, "change its settings"); err != nil {
		return nil, grpcError(err)
	}
	role, err := tenancy.ParseRole(req.GetBaseRole())
	if err != nil {
		return nil, invalid(err)
	}
	o, err = s.cfg.Store.SetOrganization(ctx, o.ID, store.OrganizationSettings{BaseRole: role, MembersCreate: req.GetMembersCreate(),
		PrivateForks: req.GetPrivateForks(), Enterprise: req.GetEnterprise()})
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "organization.update", map[string]any{"organization": o.Name, "base_role": role.String()})
	s.pruneOrganizationForks(ctx, o.ID)
	return orgOut(o, o.ID == s.cfg.Installation), nil
}

func (s *Server) memberOut(ctx context.Context, m store.Member) (*identitypb.Member, error) {
	p, err := s.cfg.Store.PrincipalByID(ctx, m.Principal)
	if err != nil {
		return nil, err
	}
	return &identitypb.Member{PrincipalId: m.Principal, Principal: p.Name, Role: m.Role.String()}, nil
}

func (s *Server) ListMembers(ctx context.Context, req *identitypb.ListMembersRequest) (*identitypb.ListMembersResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.notInstallation(o, "members"); err != nil {
		return nil, err
	}
	if err := s.requireMember(ctx, o, p); err != nil {
		return nil, grpcError(err)
	}
	ms, err := s.cfg.Store.Members(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.ListMembersResponse{}
	for _, m := range ms {
		mo, err := s.memberOut(ctx, m)
		if err != nil {
			return nil, grpcError(err)
		}
		out.Members = append(out.Members, mo)
	}
	return out, nil
}

func (s *Server) SetMember(ctx context.Context, req *identitypb.SetMemberRequest) (*identitypb.SetMemberResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.notInstallation(o, "members"); err != nil {
		return nil, err
	}
	if err := s.requireOwner(ctx, o, p, "set its members"); err != nil {
		return nil, grpcError(err)
	}
	role, err := tenancy.ParseMembership(req.GetRole())
	if err != nil {
		return nil, invalid(err)
	}
	if err := s.cfg.Store.SetMember(ctx, o.ID, req.GetPrincipalId(), role); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "organization.member", map[string]any{"organization": o.Name, "principal": req.GetPrincipalId(), "role": role.String()})
	return &identitypb.SetMemberResponse{}, nil
}

func (s *Server) RemoveMember(ctx context.Context, req *identitypb.RemoveMemberRequest) (*identitypb.RemoveMemberResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.notInstallation(o, "members"); err != nil {
		return nil, err
	}
	if req.GetPrincipalId() != p.ID {
		if err := s.requireOwner(ctx, o, p, "remove its members"); err != nil {
			return nil, grpcError(err)
		}
	}
	removed, err := s.cfg.Store.RemoveMember(ctx, o.ID, req.GetPrincipalId())
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "organization.remove", map[string]any{"organization": o.Name, "principal": req.GetPrincipalId()})
	if removed {
		s.pruneOrganizationForks(ctx, o.ID)
	}
	return &identitypb.RemoveMemberResponse{Removed: removed}, nil
}

// ─── Teams ───

func (s *Server) teamOut(ctx context.Context, t store.Team) (*identitypb.Team, error) {
	out := &identitypb.Team{Name: t.Name, Parent: t.Parent}
	for _, id := range t.Members {
		m, err := s.memberOut(ctx, store.Member{Principal: id, Role: tenancy.Member})
		if err != nil {
			return nil, err
		}
		out.Members = append(out.Members, m)
	}
	return out, nil
}

// teamOf is a team and its organization, which a person owns; NOT_FOUND for none.
func (s *Server) teamOf(ctx context.Context, name string, p store.Principal, what string) (store.Team, store.Organization, error) {
	t, err := s.cfg.Store.Team(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return store.Team{}, store.Organization{}, status.Errorf(codes.NotFound, "no team is called %s", name)
	}
	if err != nil {
		return store.Team{}, store.Organization{}, err
	}
	o, err := s.cfg.Store.Organization(ctx, t.Organization)
	if err != nil {
		return store.Team{}, store.Organization{}, err
	}
	return t, o, s.requireOwner(ctx, o, p, what)
}

func (s *Server) CreateTeam(ctx context.Context, req *identitypb.CreateTeamRequest) (*identitypb.Team, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.notInstallation(o, "teams"); err != nil {
		return nil, err
	}
	if err := s.requireOwner(ctx, o, p, "make its teams"); err != nil {
		return nil, grpcError(err)
	}
	t, err := s.cfg.Store.CreateTeam(ctx, o.ID, req.GetTeam(), req.GetParent())
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "team.create", map[string]any{"team": t.Name})
	return s.teamOut(ctx, t)
}

func (s *Server) ListTeams(ctx context.Context, req *identitypb.ListTeamsRequest) (*identitypb.ListTeamsResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.requireMember(ctx, o, p); err != nil {
		return nil, grpcError(err)
	}
	ts, err := s.cfg.Store.Teams(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.ListTeamsResponse{}
	for _, t := range ts {
		to, err := s.teamOut(ctx, t)
		if err != nil {
			return nil, grpcError(err)
		}
		out.Teams = append(out.Teams, to)
	}
	return out, nil
}

func (s *Server) AddTeamMember(ctx context.Context, req *identitypb.TeamMemberRequest) (*identitypb.Team, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if _, _, err := s.teamOf(ctx, req.GetTeam(), p, "fill its teams"); err != nil {
		return nil, grpcError(err)
	}
	if err := s.cfg.Store.AddTeamMember(ctx, req.GetTeam(), req.GetPrincipalId()); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "team.add", map[string]any{"team": req.GetTeam(), "principal": req.GetPrincipalId()})
	t, err := s.cfg.Store.Team(ctx, req.GetTeam())
	if err != nil {
		return nil, grpcError(err)
	}
	return s.teamOut(ctx, t)
}

func (s *Server) RemoveTeamMember(ctx context.Context, req *identitypb.TeamMemberRequest) (*identitypb.Team, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if _, _, err := s.teamOf(ctx, req.GetTeam(), p, "fill its teams"); err != nil {
		return nil, grpcError(err)
	}
	if _, err := s.cfg.Store.RemoveTeamMember(ctx, req.GetTeam(), req.GetPrincipalId()); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "team.remove", map[string]any{"team": req.GetTeam(), "principal": req.GetPrincipalId()})
	t, err := s.cfg.Store.Team(ctx, req.GetTeam())
	if err != nil {
		return nil, grpcError(err)
	}
	s.pruneOrganizationForks(ctx, t.Organization)
	return s.teamOut(ctx, t)
}

func (s *Server) DeleteTeam(ctx context.Context, req *identitypb.DeleteTeamRequest) (*identitypb.DeleteTeamResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	t, _, err := s.teamOf(ctx, req.GetTeam(), p, "delete its teams")
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.cfg.Store.DeleteTeam(ctx, req.GetTeam()); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "team.delete", map[string]any{"team": req.GetTeam()})
	s.pruneOrganizationForks(ctx, t.Organization)
	return &identitypb.DeleteTeamResponse{}, nil
}

// ─── Repositories ───

func (s *Server) entryOut(ctx context.Context, r store.Repository, role tenancy.Role, redirected bool) (*identitypb.RepositoryEntry, error) {
	e, err := s.cfg.Store.Entry(ctx, r)
	if err != nil {
		return nil, err
	}
	e.Role, e.Redirected = role.String(), redirected
	return e, nil
}

// owner is the person or organization a handle names — the installation's organization for none —
// and whether it is an organization; NOT_FOUND for none.
func (s *Server) owner(ctx context.Context, handle string) (store.Principal, bool, error) {
	// No owner is the installation's organization, or the one an installation moved in as.
	p, err := s.cfg.Store.Owner(ctx, handle)
	if errors.Is(err, store.ErrNotFound) && handle == "" {
		return store.Principal{}, false, status.Error(codes.InvalidArgument, "name the owner")
	}
	if errors.Is(err, store.ErrNotFound) {
		return store.Principal{}, false, status.Errorf(codes.NotFound, "no one is called %s", handle)
	}
	return p, p.Kind == store.Org, err
}

// mayCreate refuses a person who may not make repositories of an owner: their own, or an
// organization's whose owner they are, or member where members make repositories.
func (s *Server) mayCreate(ctx context.Context, owner store.Principal, isOrg bool, p store.Principal) error {
	if !isOrg {
		if owner.ID != p.ID {
			return status.Errorf(codes.PermissionDenied, "a person makes repositories of their own, not %s's", owner.Name)
		}
		return nil
	}
	o, err := s.cfg.Store.Organization(ctx, owner.ID)
	if err != nil {
		return err
	}
	m, err := s.membership(ctx, o.ID, p)
	if err != nil {
		return err
	}
	if m == tenancy.Owner || (m == tenancy.Member && o.MembersCreate) {
		return nil
	}
	return status.Errorf(codes.PermissionDenied, "%s's repositories are made by its owners%s", o.Handle(), map[bool]string{true: " and members", false: ""}[o.MembersCreate])
}

// decide is a repository's entry, and Authorize's decision on a person doing an action to it: a
// repository they cannot see answers NOT_FOUND, an action refused PERMISSION_DENIED.
func (s *Server) decide(ctx context.Context, id int64, p store.Principal, a authz.Action) (store.Repository, authz.Decision, error) {
	r, err := s.cfg.Store.Repository(ctx, id)
	if errors.Is(err, store.ErrNotFound) || (err == nil && r.State == store.Creating) {
		return store.Repository{}, authz.Decision{}, status.Errorf(codes.NotFound, "no repository has the ID %d", id)
	}
	if err != nil {
		return store.Repository{}, authz.Decision{}, err
	}
	f, err := s.facts(ctx, p.ID, r)
	if err != nil {
		return store.Repository{}, authz.Decision{}, err
	}
	f.Caller.Token = tokenOf(ctx, p.ID)
	d := authz.Decide(f, a)
	switch {
	case !d.Visible:
		return r, d, status.Errorf(codes.NotFound, "no repository has the ID %d", id)
	case !d.Allowed:
		return r, d, status.Error(codes.PermissionDenied, d.Why)
	}
	return r, d, nil
}

func (s *Server) ReserveRepository(ctx context.Context, req *identitypb.ReserveRepositoryRequest) (*identitypb.RepositoryEntry, error) {
	if err := s.requireRole(ctx, okpb.Caller_CALLER_SCM, "making a repository"); err != nil {
		return nil, err
	}
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	owner, isOrg, err := s.owner(ctx, req.GetOwner())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.mayCreate(ctx, owner, isOrg, p); err != nil {
		return nil, grpcError(err)
	}
	v := tenancy.Private
	if owner.ID == s.cfg.Installation {
		v = tenancy.Internal
	}
	if req.GetVisibility() != "" {
		if v, err = tenancy.ParseVisibility(req.GetVisibility()); err != nil {
			return nil, invalid(err)
		}
	}
	if parent := req.GetParentId(); parent != 0 {
		// A fork's visibility is its upstream's, which the store gives it (GitHub's rules).
		if _, _, err := s.decide(ctx, parent, p, authz.Fork); err != nil {
			return nil, grpcError(err)
		}
	}
	r, err := s.cfg.Store.ReserveRepository(ctx, store.NewRepository{Owner: owner.ID, Name: req.GetName(), Visibility: v,
		Propose: tenancy.Read, Parent: req.GetParentId(), Cell: req.GetCell(), CreatedBy: p.ID})
	if err != nil {
		return nil, tenancyError(err)
	}
	if isOrg {
		// Its maker administers it, as its owners do.
		if err := s.cfg.Store.SetGrant(ctx, store.Grant{Repository: r.ID, Subject: p.ID, Role: tenancy.Admin}); err != nil {
			return nil, grpcError(err)
		}
	}
	s.audit(ctx, p.Name, "repository.reserve", map[string]any{"repository": r.ID, "owner": owner.Name, "name": r.Name})
	return s.entryOut(ctx, r, tenancy.Admin, false)
}

func (s *Server) ActivateRepository(ctx context.Context, req *identitypb.RepositoryRef) (*identitypb.RepositoryEntry, error) {
	if err := s.requireRole(ctx, okpb.Caller_CALLER_SCM, "activating a repository"); err != nil {
		return nil, err
	}
	r, err := s.cfg.Store.ActivateRepository(ctx, req.GetId())
	if err != nil {
		return nil, tenancyError(err)
	}
	return s.entryOut(ctx, r, tenancy.None, false)
}

func (s *Server) AbandonRepository(ctx context.Context, req *identitypb.RepositoryRef) (*identitypb.AbandonRepositoryResponse, error) {
	if err := s.requireRole(ctx, okpb.Caller_CALLER_SCM, "abandoning a repository"); err != nil {
		return nil, err
	}
	if err := s.cfg.Store.AbandonRepository(ctx, req.GetId()); err != nil {
		return nil, tenancyError(err)
	}
	return &identitypb.AbandonRepositoryResponse{}, nil
}

func expiryPB(id int64, e store.Expiry) *identitypb.Expiry {
	kind := identitypb.Expiry_KIND_NONE
	switch e {
	case store.ExpiredDeleted:
		kind = identitypb.Expiry_KIND_DELETED
	case store.ExpiredCreating:
		kind = identitypb.Expiry_KIND_CREATING
	}
	return &identitypb.Expiry{RepositoryId: id, Kind: kind}
}

func (s *Server) ListExpiredRepositories(ctx context.Context, req *identitypb.ListExpiredRepositoriesRequest) (*identitypb.ListExpiredRepositoriesResponse, error) {
	if err := s.requireRole(ctx, okpb.Caller_CALLER_SCM, "listing expired repositories"); err != nil {
		return nil, err
	}
	limit := int(req.GetLimit())
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	deleted, creating, err := s.cfg.Store.Expired(ctx, limit)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.ListExpiredRepositoriesResponse{}
	for _, r := range deleted {
		out.Repositories = append(out.Repositories, expiryPB(r.ID, store.ExpiredDeleted))
	}
	for _, r := range creating {
		out.Repositories = append(out.Repositories, expiryPB(r.ID, store.ExpiredCreating))
	}
	return out, nil
}

func (s *Server) GetExpiry(ctx context.Context, req *identitypb.RepositoryRef) (*identitypb.Expiry, error) {
	if err := s.requireRole(ctx, okpb.Caller_CALLER_SCM, "reading a repository's expiry"); err != nil {
		return nil, err
	}
	e, err := s.cfg.Store.ExpiryOf(ctx, req.GetId())
	if err != nil {
		return nil, grpcError(err)
	}
	return expiryPB(req.GetId(), e), nil
}

func (s *Server) PurgeRepository(ctx context.Context, req *identitypb.RepositoryRef) (*identitypb.Expiry, error) {
	if err := s.requireRole(ctx, okpb.Caller_CALLER_SCM, "purging a repository"); err != nil {
		return nil, err
	}
	e, err := s.cfg.Store.RemoveExpired(ctx, req.GetId())
	if err != nil {
		return nil, tenancyError(err)
	}
	if c, _ := authn.FromContext(ctx); e != store.NotExpired {
		s.audit(ctx, c.Principal, "repository.purge", map[string]any{"repository": req.GetId(), "expired": expiryPB(req.GetId(), e).GetKind().String()})
	}
	return expiryPB(req.GetId(), e), nil
}

// readerRole is a request's reader's role on a repository they look up: ok's roles, asking for
// themselves, see every repository; a person, what Authorize lets them see. NOT_FOUND for one they
// cannot see, or one being created.
func (s *Server) readerRole(ctx context.Context, r store.Repository, forPerson string) (tenancy.Role, error) {
	c, err := callerOf(ctx)
	if err != nil {
		return tenancy.None, err
	}
	if !c.Human && forPerson == "" {
		if r.State == store.Creating {
			return tenancy.None, status.Errorf(codes.NotFound, "no repository has the ID %d", r.ID)
		}
		return tenancy.None, nil
	}
	p, err := s.actor(ctx, forPerson)
	if err != nil {
		return tenancy.None, err
	}
	if r.State == store.Creating {
		return tenancy.None, status.Errorf(codes.NotFound, "no repository has the ID %d", r.ID)
	}
	f, err := s.facts(ctx, p.ID, r)
	if err != nil {
		return tenancy.None, err
	}
	f.Caller.Token = tokenOf(ctx, p.ID)
	// Seeing it is enough: a deleted repository's admins see it, to restore it.
	if d := authz.Decide(f, authz.ReadCode); !d.Visible {
		return tenancy.None, status.Errorf(codes.NotFound, "no repository has the ID %d", r.ID)
	}
	return authz.RoleOf(f), nil
}

func (s *Server) ResolveRepository(ctx context.Context, req *identitypb.ResolveRepositoryRequest) (*identitypb.RepositoryEntry, error) {
	owner, _, err := s.owner(ctx, req.GetOwner())
	if err != nil {
		return nil, grpcError(err)
	}
	r, redirected, err := s.cfg.Store.ResolveRepository(ctx, owner.ID, req.GetName())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "no repository is called %s/%s", req.GetOwner(), req.GetName())
	}
	if err != nil {
		return nil, grpcError(err)
	}
	role, err := s.readerRole(ctx, r, req.GetForPersonId())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Errorf(codes.NotFound, "no repository is called %s/%s", req.GetOwner(), req.GetName())
		}
		return nil, grpcError(err)
	}
	return s.entryOut(ctx, r, role, redirected)
}

func (s *Server) GetRepositoryEntry(ctx context.Context, req *identitypb.RepositoryRef) (*identitypb.RepositoryEntry, error) {
	r, err := s.cfg.Store.Repository(ctx, req.GetId())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "no repository has the ID %d", req.GetId())
	}
	if err != nil {
		return nil, grpcError(err)
	}
	role, err := s.readerRole(ctx, r, req.GetForPersonId())
	if err != nil {
		return nil, grpcError(err)
	}
	return s.entryOut(ctx, r, role, false)
}

func (s *Server) ListRepositoryEntries(ctx context.Context, req *identitypb.ListRepositoryEntriesRequest) (*identitypb.ListRepositoryEntriesResponse, error) {
	owner, _, err := s.owner(ctx, req.GetOwner())
	if err != nil {
		return nil, grpcError(err)
	}
	rs, err := s.cfg.Store.RepositoriesOf(ctx, owner.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.ListRepositoryEntriesResponse{}
	for _, r := range rs {
		if r.State == store.Creating {
			continue
		}
		role, err := s.readerRole(ctx, r, req.GetForPersonId())
		if status.Code(err) == codes.NotFound {
			continue
		}
		if err != nil {
			return nil, grpcError(err)
		}
		e, err := s.entryOut(ctx, r, role, false)
		if err != nil {
			return nil, grpcError(err)
		}
		out.Repositories = append(out.Repositories, e)
	}
	return out, nil
}

func (s *Server) UpdateRepository(ctx context.Context, req *identitypb.UpdateRepositoryRequest) (*identitypb.RepositoryEntry, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	var r store.Repository
	if req.GetPropose() != "" {
		role, err := tenancy.ParseRole(req.GetPropose())
		if err != nil {
			return nil, invalid(err)
		}
		if _, _, err := s.decide(ctx, req.GetId(), p, authz.Maintain); err != nil {
			return nil, grpcError(err)
		}
		if r, err = s.cfg.Store.SetPropose(ctx, req.GetId(), role); err != nil {
			return nil, tenancyError(err)
		}
	}
	if req.GetVisibility() != "" {
		v, err := tenancy.ParseVisibility(req.GetVisibility())
		if err != nil {
			return nil, invalid(err)
		}
		if _, _, err := s.decide(ctx, req.GetId(), p, authz.Administer); err != nil {
			return nil, grpcError(err)
		}
		if r, err = s.cfg.Store.SetVisibility(ctx, req.GetId(), v); err != nil {
			return nil, tenancyError(err)
		}
	}
	if r.ID == 0 {
		var d authz.Decision
		if r, d, err = s.decide(ctx, req.GetId(), p, authz.ReadCode); err != nil {
			return nil, grpcError(err)
		}
		return s.entryOut(ctx, r, d.Role, false)
	}
	s.audit(ctx, p.Name, "repository.update", map[string]any{"repository": r.ID, "visibility": r.Visibility.String(), "propose": r.Propose.String()})
	return s.entryOut(ctx, r, tenancy.Admin, false)
}

func (s *Server) RenameRepository(ctx context.Context, req *identitypb.RenameRepositoryEntryRequest) (*identitypb.RepositoryEntry, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if _, _, err := s.decide(ctx, req.GetId(), p, authz.Administer); err != nil {
		return nil, grpcError(err)
	}
	r, err := s.cfg.Store.RenameRepository(ctx, req.GetId(), req.GetName())
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "repository.rename", map[string]any{"repository": r.ID, "name": r.Name})
	return s.entryOut(ctx, r, tenancy.Admin, false)
}

func (s *Server) TransferRepository(ctx context.Context, req *identitypb.TransferRepositoryRequest) (*identitypb.RepositoryEntry, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if _, _, err := s.decide(ctx, req.GetId(), p, authz.Administer); err != nil {
		return nil, grpcError(err)
	}
	owner, isOrg, err := s.owner(ctx, req.GetOwner())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.mayCreate(ctx, owner, isOrg, p); err != nil {
		return nil, grpcError(err)
	}
	r, err := s.cfg.Store.TransferRepository(ctx, req.GetId(), owner.ID)
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "repository.transfer", map[string]any{"repository": r.ID, "owner": owner.Name})
	s.pruneRepositoryForks(ctx, r.ID)
	return s.entryOut(ctx, r, tenancy.Admin, false)
}

func (s *Server) DeleteRepository(ctx context.Context, req *identitypb.RepositoryRef) (*identitypb.RepositoryEntry, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if _, _, err := s.decide(ctx, req.GetId(), p, authz.Administer); err != nil {
		return nil, grpcError(err)
	}
	r, err := s.cfg.Store.DeleteRepository(ctx, req.GetId())
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "repository.delete", map[string]any{"repository": r.ID})
	return s.entryOut(ctx, r, tenancy.Admin, false)
}

func (s *Server) RestoreRepository(ctx context.Context, req *identitypb.RepositoryRef) (*identitypb.RepositoryEntry, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if _, _, err := s.decide(ctx, req.GetId(), p, authz.Restore); err != nil {
		return nil, grpcError(err)
	}
	r, err := s.cfg.Store.RestoreRepository(ctx, req.GetId())
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "repository.restore", map[string]any{"repository": r.ID})
	return s.entryOut(ctx, r, tenancy.Admin, false)
}

func (s *Server) ListGrants(ctx context.Context, req *identitypb.RepositoryRef) (*identitypb.ListGrantsResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if _, _, err := s.decide(ctx, req.GetId(), p, authz.Administer); err != nil {
		return nil, grpcError(err)
	}
	gs, err := s.cfg.Store.Grants(ctx, req.GetId())
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.ListGrantsResponse{}
	for _, g := range gs {
		out.Grants = append(out.Grants, &identitypb.RepositoryGrant{Subject: g.Subject, Role: g.Role.String()})
	}
	return out, nil
}

func (s *Server) SetGrant(ctx context.Context, req *identitypb.SetGrantRequest) (*identitypb.SetGrantResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if _, _, err := s.decide(ctx, req.GetId(), p, authz.Administer); err != nil {
		return nil, grpcError(err)
	}
	role, err := tenancy.ParseRole(req.GetGrant().GetRole())
	if err != nil {
		return nil, invalid(err)
	}
	g := store.Grant{Repository: req.GetId(), Subject: req.GetGrant().GetSubject(), Role: role}
	if err := s.cfg.Store.SetGrant(ctx, g); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "repository.grant", map[string]any{"repository": g.Repository, "subject": g.Subject, "role": role.String()})
	return &identitypb.SetGrantResponse{}, nil
}

func (s *Server) RemoveGrant(ctx context.Context, req *identitypb.RemoveGrantRequest) (*identitypb.RemoveGrantResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if _, _, err := s.decide(ctx, req.GetId(), p, authz.Administer); err != nil {
		return nil, grpcError(err)
	}
	removed, err := s.cfg.Store.RemoveGrant(ctx, req.GetId(), req.GetSubject())
	if err != nil {
		return nil, grpcError(err)
	}
	s.audit(ctx, p.Name, "repository.revoke", map[string]any{"repository": req.GetId(), "subject": req.GetSubject()})
	if removed {
		s.pruneRepositoryForks(ctx, req.GetId())
	}
	return &identitypb.RemoveGrantResponse{Removed: removed}, nil
}

// tenancyError maps the tenancy's rules' errors to gRPC statuses.
func tenancyError(err error) error {
	switch {
	case errors.Is(err, store.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, store.ErrLastOwner), errors.Is(err, store.ErrRepositoryState), errors.Is(err, store.ErrCreationExpired):
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return grpcError(err)
}
