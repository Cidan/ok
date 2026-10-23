package service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// Deploy agents (docs/hosted.md, "Deploy agents, as built").

// SetDeployAgent designates an organization's deploy agent, or none: its owners'.
func (s *Server) SetDeployAgent(ctx context.Context, req *identitypb.SetDeployAgentRequest) (*identitypb.DeployAgent, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.requireOwner(ctx, o, p, "designate its deploy agent"); err != nil {
		return nil, grpcError(err)
	}
	if err := s.cfg.Store.SetDeployAgent(ctx, o.ID, req.GetRobotId(), p.ID); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "deploy_agent.set", map[string]any{"organization": o.Handle(), "robot": req.GetRobotId()})
	if req.GetRobotId() == "" {
		return &identitypb.DeployAgent{AccountId: o.ID, Handle: o.Handle()}, nil
	}
	a, err := s.cfg.Store.DeployAgentOf(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	return s.agentOut(ctx, a)
}

// GetDeployAgent is an organization's agent, or the organization a robot is the agent of.
func (s *Server) GetDeployAgent(ctx context.Context, req *identitypb.GetDeployAgentRequest) (*identitypb.DeployAgent, error) {
	deployer := s.isRole(ctx, okpb.Caller_CALLER_DEPLOY)
	var a store.DeployAgent
	var err error
	switch k := req.GetKey().(type) {
	case *identitypb.GetDeployAgentRequest_RobotId:
		if !deployer {
			return nil, status.Error(codes.PermissionDenied, "which organization a robot deploys for is the deployer's to ask")
		}
		a, err = s.cfg.Store.AgentAccount(ctx, k.RobotId)
	case *identitypb.GetDeployAgentRequest_Handle, *identitypb.GetDeployAgentRequest_AccountId:
		var o store.Organization
		if h, ok := k.(*identitypb.GetDeployAgentRequest_Handle); ok {
			o, err = s.organization(ctx, h.Handle)
		} else {
			o, err = s.cfg.Store.Organization(ctx, req.GetAccountId())
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "no such organization")
		}
		if err != nil {
			return nil, grpcError(err)
		}
		if !deployer {
			p, err := s.actor(ctx, req.GetForPersonId())
			if err != nil {
				return nil, err
			}
			if err := s.requireMember(ctx, o, p); err != nil {
				return nil, grpcError(err)
			}
		}
		a, err = s.cfg.Store.DeployAgentOf(ctx, o.ID)
	default:
		return nil, status.Error(codes.InvalidArgument, "name an organization, or a robot")
	}
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "no deploy agent is designated")
	}
	if err != nil {
		return nil, grpcError(err)
	}
	return s.agentOut(ctx, a)
}

// DeployCells are the cells holding the calling agent's organization's repositories.
func (s *Server) DeployCells(ctx context.Context, _ *identitypb.DeployCellsRequest) (*identitypb.DeployCellsResponse, error) {
	c, err := callerOf(ctx)
	if err != nil {
		return nil, err
	}
	if c.Human || c.IsRole() {
		return nil, status.Error(codes.PermissionDenied, "an organization's deploy agent asks which cells it serves")
	}
	a, err := s.cfg.Store.AgentAccount(ctx, c.ID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.PermissionDenied, "%s is no organization's deploy agent", c.Principal)
	}
	if err != nil {
		return nil, grpcError(err)
	}
	cells, err := s.cfg.Store.OwnerCells(ctx, a.Organization)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.DeployCellsResponse{AccountId: a.Organization, Cells: cells}, nil
}

// AgentRepository says whether a repository is the calling agent's organization's.
func (s *Server) AgentRepository(ctx context.Context, req *identitypb.RepositoryRef) (*identitypb.AgentRepositoryResponse, error) {
	c, err := callerOf(ctx)
	if err != nil {
		return nil, err
	}
	if c.Human || c.IsRole() {
		return nil, status.Error(codes.PermissionDenied, "an organization's deploy agent asks whose a repository is")
	}
	a, err := s.cfg.Store.AgentAccount(ctx, c.ID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.PermissionDenied, "%s is no organization's deploy agent", c.Principal)
	}
	if err != nil {
		return nil, grpcError(err)
	}
	r, err := s.cfg.Store.Repository(ctx, req.GetId())
	if errors.Is(err, store.ErrNotFound) || err == nil && r.Owner != a.Organization {
		return &identitypb.AgentRepositoryResponse{}, nil
	}
	if err != nil {
		return nil, grpcError(err)
	}
	e, err := s.entryOut(ctx, r, tenancy.None, false)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.AgentRepositoryResponse{Owned: r.State == store.Active, FullName: e.GetFullName()}, nil
}

func (s *Server) agentOut(ctx context.Context, a store.DeployAgent) (*identitypb.DeployAgent, error) {
	out := &identitypb.DeployAgent{AccountId: a.Organization, RobotId: a.Robot, SetById: a.SetBy, SetAt: timestamppb.New(a.SetAt)}
	if o, err := s.cfg.Store.Organization(ctx, a.Organization); err == nil {
		out.Handle = o.Handle()
	}
	if p, err := s.cfg.Store.PrincipalByID(ctx, a.Robot); err == nil {
		out.Robot = p.Name
	}
	return out, nil
}
