package service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/id/store"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Deleting accounts and organizations (docs/hosted.md, "Deleting, renaming, transferring").

func deletionOut(d store.Deletion) *identitypb.DeletionInfo {
	return &identitypb.DeletionInfo{Requested: timestamppb.New(d.Requested), Final: timestamppb.New(d.Requested.Add(store.RetentionPeriod))}
}

func deletionError(err error) error {
	switch {
	case errors.Is(err, store.ErrDeleting):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrLastOwner):
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return tenancyError(err)
}

func (s *Server) DeleteAccount(ctx context.Context, req *identitypb.DeleteAccountRequest) (*identitypb.DeletionInfo, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if !s.cfg.Hosted {
		return nil, status.Error(codes.FailedPrecondition, "an installation's people are its system of record's, which deletes them")
	}
	if h, _ := s.cfg.Store.Names().HandleOf(p.Name); req.GetConfirm() != h {
		return nil, status.Error(codes.InvalidArgument, "type your handle to delete your account")
	}
	if err := s.cfg.Store.DeleteAccount(ctx, p.ID, p.ID); err != nil {
		return nil, deletionError(err)
	}
	s.audit(ctx, p.Name, "account.delete", map[string]any{"principal": p.ID})
	d, err := s.cfg.Store.DeletionOf(ctx, p.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	return deletionOut(d), nil
}

func (s *Server) DeleteOrganization(ctx context.Context, req *identitypb.OrganizationDeletionRequest) (*identitypb.DeletionInfo, error) {
	o, p, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "delete it")
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Store.DeleteOrganization(ctx, o.ID, p.ID); err != nil {
		return nil, deletionError(err)
	}
	s.audit(ctx, p.Name, "organization.delete", map[string]any{"organization": o.Name})
	d, err := s.cfg.Store.DeletionOf(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	return deletionOut(d), nil
}

func (s *Server) RestoreOrganization(ctx context.Context, req *identitypb.OrganizationDeletionRequest) (*identitypb.RestoreResponse, error) {
	// Owners of an organization being deleted are its members still.
	o, p, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "restore it")
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Store.RestoreOrganization(ctx, o.ID); err != nil {
		return nil, deletionError(err)
	}
	s.audit(ctx, p.Name, "organization.restore", map[string]any{"organization": o.Name})
	return &identitypb.RestoreResponse{}, nil
}

func (s *Server) RestoreAccount(ctx context.Context, req *identitypb.RestoreAccountRequest) (*identitypb.RestoreResponse, error) {
	caller, err := s.requireAdmin(ctx)
	if err != nil {
		return nil, err
	}
	p, err := s.cfg.Store.Principal(ctx, req.GetName())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.cfg.Store.RestoreAccount(ctx, p.ID); err != nil {
		return nil, deletionError(err)
	}
	s.audit(ctx, caller.Principal, "account.restore", map[string]any{"principal": p.ID})
	return &identitypb.RestoreResponse{}, nil
}
