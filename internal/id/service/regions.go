package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// The region an owner's new repositories are placed in (docs/hosted.md, "Cells, as built").

// SetRegion sets the region an owner's new repositories are placed in: a person's own, or an
// organization's, by its owners.
func (s *Server) SetRegion(ctx context.Context, req *identitypb.SetRegionRequest) (*identitypb.Region, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	owner, isOrg, err := s.owner(ctx, req.GetOwner())
	if err != nil {
		return nil, grpcError(err)
	}
	if isOrg {
		o, err := s.organization(ctx, req.GetOwner())
		if err != nil {
			return nil, grpcError(err)
		}
		if err := s.requireOwner(ctx, o, p, "set its region"); err != nil {
			return nil, grpcError(err)
		}
	} else if owner.ID != p.ID {
		return nil, status.Errorf(codes.PermissionDenied, "a person sets their own region, not %s's", owner.Name)
	}
	if err := s.cfg.Store.SetOwnerRegion(ctx, owner.ID, req.GetRegion()); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "owner.region", map[string]any{"owner": owner.Name, "region": req.GetRegion()})
	return s.regionOut(ctx, req.GetOwner(), owner.ID)
}

// GetRegion is the region an owner's new repositories are placed in.
func (s *Server) GetRegion(ctx context.Context, req *identitypb.GetRegionRequest) (*identitypb.Region, error) {
	if _, err := callerOf(ctx); err != nil {
		return nil, err
	}
	owner, _, err := s.owner(ctx, req.GetOwner())
	if err != nil {
		return nil, grpcError(err)
	}
	return s.regionOut(ctx, req.GetOwner(), owner.ID)
}

func (s *Server) regionOut(ctx context.Context, handle, owner string) (*identitypb.Region, error) {
	region, err := s.cfg.Store.OwnerRegion(ctx, owner)
	if err != nil {
		return nil, grpcError(err)
	}
	placed, err := s.cfg.Store.PlacedIn(ctx, owner)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.Region{Owner: handle, Region: region, PlacedIn: placed}, nil
}
