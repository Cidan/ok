package service

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/id/store"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// An organization's robots (docs/hosted.md, "Robots and tokens per organization"): its owners make,
// certify, and delete them; its members list them.

// organizationRobot is the organization a request names, and its robot named robot (full name).
func (s *Server) organizationRobot(ctx context.Context, o store.Organization, robot string) (store.Principal, error) {
	p, err := s.cfg.Store.Principal(ctx, robot)
	if err == nil {
		var of string
		if of, err = s.cfg.Store.RobotOrganization(ctx, p.ID); err == nil && of != o.ID {
			err = store.ErrNotFound
		}
	}
	if err != nil {
		if status.Code(grpcError(err)) == codes.NotFound {
			return store.Principal{}, status.Errorf(codes.NotFound, "%s has no robot %s", o.Handle(), robot)
		}
		return store.Principal{}, grpcError(err)
	}
	return p, nil
}

func (s *Server) CreateOrganizationRobot(ctx context.Context, req *identitypb.CreateOrganizationRobotRequest) (*identitypb.OrganizationRobot, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.requireOwner(ctx, o, p, "make its robots"); err != nil {
		return nil, grpcError(err)
	}
	r, err := s.cfg.Store.CreateOrganizationRobot(ctx, o.ID, req.GetName())
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "robot.create", map[string]any{"organization": o.Handle(), "robot": r.Name})
	return &identitypb.OrganizationRobot{Principal: principalPB(r)}, nil
}

func (s *Server) ListOrganizationRobots(ctx context.Context, req *identitypb.ListOrganizationRobotsRequest) (*identitypb.ListOrganizationRobotsResponse, error) {
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
	robots, err := s.cfg.Store.OrganizationRobots(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	bs, err := s.cfg.Store.OrganizationBindings(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.ListOrganizationRobotsResponse{}
	for _, r := range robots {
		out.Robots = append(out.Robots, &identitypb.OrganizationRobot{Principal: principalPB(r), Bindings: bindingsOut(bs[r.ID])})
	}
	return out, nil
}

func (s *Server) CertifyOrganizationRobot(ctx context.Context, req *identitypb.CertifyOrganizationRobotRequest) (*identitypb.CertifyRobotResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.requireOwner(ctx, o, p, "certify its robots"); err != nil {
		return nil, grpcError(err)
	}
	if _, err := parseKey(req.GetPublicKey()); err != nil {
		return nil, err
	}
	if err := checkLabel(req.GetLabel()); err != nil {
		return nil, err
	}
	r, err := s.organizationRobot(ctx, o, req.GetRobot())
	if err != nil {
		return nil, err
	}
	if !r.Active() {
		return nil, status.Errorf(codes.FailedPrecondition, "%s is not an active robot", r.Name)
	}
	c, err := s.cfg.Store.EnrollKey(ctx, r.ID, req.GetLabel(), req.GetPublicKey())
	if err != nil {
		return nil, grpcError(err)
	}
	cert, err := s.issue(ctx, c)
	if err != nil {
		return nil, err
	}
	s.audit(ctx, p.Name, "robot.certify", map[string]any{"organization": o.Handle(), "robot": r.Name, "credential": c.ID})
	return &identitypb.CertifyRobotResponse{Certificate: cert}, nil
}

func (s *Server) DeleteOrganizationRobot(ctx context.Context, req *identitypb.DeleteOrganizationRobotRequest) (*identitypb.DeleteOrganizationRobotResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.requireOwner(ctx, o, p, "delete its robots"); err != nil {
		return nil, grpcError(err)
	}
	r, err := s.organizationRobot(ctx, o, req.GetRobot())
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Store.DeleteOrganizationRobot(ctx, o.ID, r.ID); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "robot.delete", map[string]any{"organization": o.Handle(), "robot": r.Name})
	return &identitypb.DeleteOrganizationRobotResponse{}, nil
}

func bindingsOut(bs []store.Binding) []*identitypb.Binding {
	var out []*identitypb.Binding
	for _, b := range bs {
		out = append(out, &identitypb.Binding{Issuer: b.Issuer, Subject: b.Subject})
	}
	return out
}

// issuers is what an organization trusts, and the audience its tokens carry.
func (s *Server) issuers(ctx context.Context, o store.Organization) (*identitypb.OrganizationIssuers, error) {
	is, err := s.cfg.Store.Issuers(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.OrganizationIssuers{Issuers: is, Audience: store.OrganizationAudience(o.ID)}, nil
}

// ownedOrganization is the organization a request names, which the person asking owns.
func (s *Server) ownedOrganization(ctx context.Context, handle, forPerson, what string) (store.Organization, store.Principal, error) {
	p, err := s.actor(ctx, forPerson)
	if err != nil {
		return store.Organization{}, store.Principal{}, err
	}
	o, err := s.organization(ctx, handle)
	if err != nil {
		return store.Organization{}, store.Principal{}, grpcError(err)
	}
	if err := s.requireOwner(ctx, o, p, what); err != nil {
		return store.Organization{}, store.Principal{}, grpcError(err)
	}
	return o, p, nil
}

func (s *Server) TrustIssuer(ctx context.Context, req *identitypb.OrganizationIssuerRequest) (*identitypb.OrganizationIssuers, error) {
	o, p, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "trust issuers for its robots")
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(req.GetIssuer(), "https://") && !s.cfg.PlaintextIssuers {
		return nil, status.Errorf(codes.InvalidArgument, "%q: ok reaches an issuer over HTTPS", req.GetIssuer())
	}
	if err := s.cfg.Store.TrustIssuer(ctx, o.ID, req.GetIssuer()); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "issuer.trust", map[string]any{"organization": o.Handle(), "issuer": req.GetIssuer()})
	return s.issuers(ctx, o)
}

func (s *Server) DistrustIssuer(ctx context.Context, req *identitypb.OrganizationIssuerRequest) (*identitypb.OrganizationIssuers, error) {
	o, p, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "distrust its issuers")
	if err != nil {
		return nil, err
	}
	removed, err := s.cfg.Store.DistrustIssuer(ctx, o.ID, req.GetIssuer())
	if err != nil {
		return nil, grpcError(err)
	}
	if removed {
		s.audit(ctx, p.Name, "issuer.distrust", map[string]any{"organization": o.Handle(), "issuer": req.GetIssuer()})
	}
	return s.issuers(ctx, o)
}

func (s *Server) ListIssuers(ctx context.Context, req *identitypb.ListIssuersRequest) (*identitypb.OrganizationIssuers, error) {
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
	return s.issuers(ctx, o)
}

// robotOut is one of an organization's robots, with its bindings.
func (s *Server) robotOut(ctx context.Context, o store.Organization, r store.Principal) (*identitypb.OrganizationRobot, error) {
	bs, err := s.cfg.Store.OrganizationBindings(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.OrganizationRobot{Principal: principalPB(r), Bindings: bindingsOut(bs[r.ID])}, nil
}

func (s *Server) BindOrganizationRobot(ctx context.Context, req *identitypb.OrganizationRobotBindingRequest) (*identitypb.OrganizationRobot, error) {
	o, p, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "bind its robots")
	if err != nil {
		return nil, err
	}
	r, err := s.organizationRobot(ctx, o, req.GetRobot())
	if err != nil {
		return nil, err
	}
	b := req.GetBinding()
	if err := s.cfg.Store.BindOrganizationRobot(ctx, o.ID, r.ID, b.GetIssuer(), b.GetSubject()); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "robot.bind", map[string]any{"organization": o.Handle(), "robot": r.Name, "issuer": b.GetIssuer(), "subject": b.GetSubject()})
	return s.robotOut(ctx, o, r)
}

func (s *Server) UnbindOrganizationRobot(ctx context.Context, req *identitypb.OrganizationRobotBindingRequest) (*identitypb.OrganizationRobot, error) {
	o, p, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "unbind its robots")
	if err != nil {
		return nil, err
	}
	r, err := s.organizationRobot(ctx, o, req.GetRobot())
	if err != nil {
		return nil, err
	}
	b := req.GetBinding()
	removed, err := s.cfg.Store.UnbindOrganizationRobot(ctx, o.ID, r.ID, b.GetIssuer(), b.GetSubject())
	if err != nil {
		return nil, grpcError(err)
	}
	if removed {
		s.audit(ctx, p.Name, "robot.unbind", map[string]any{"organization": o.Handle(), "robot": r.Name, "issuer": b.GetIssuer(), "subject": b.GetSubject()})
	}
	return s.robotOut(ctx, o, r)
}
