package service

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/id/store"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Approvals (docs/secrets.md, "Unsealing"): ok secrets asks a person to approve exactly what a
// request names, and they answer on identity's page with their passkey.

// StartApproval asks a person to approve a scope, for store.DefaultApprovalLifetime.
func (s *Server) StartApproval(ctx context.Context, req *identitypb.StartApprovalRequest) (*identitypb.Approval, error) {
	switch {
	case !s.cfg.Passkeys:
		return nil, status.Error(codes.FailedPrecondition, "this installation signs no one in with passkeys (its public URL is an address, not a name), so it asks no one to approve with one")
	case req.GetScope() == "" || len(req.GetScope()) > 16<<10:
		return nil, status.Error(codes.InvalidArgument, "an approval's scope is from 1 byte to 16 KiB")
	case req.GetDescription() == "" || utf8.RuneCountInString(req.GetDescription()) > 2000 || !utf8.ValidString(req.GetDescription()):
		return nil, status.Error(codes.InvalidArgument, "an approval's description is from 1 to 2,000 characters of UTF-8")
	}
	p, err := s.cfg.Store.PrincipalByID(ctx, req.GetPrincipalId())
	if err != nil {
		return nil, grpcError(err)
	}
	if !p.Active() || p.Kind != store.Human {
		return nil, status.Errorf(codes.FailedPrecondition, "%s is no active person, who could approve", p.Name)
	}
	pks, err := s.cfg.Store.Passkeys(ctx, p.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	if len(pks) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "%s has no passkey to approve with: add one at %s/id/account first", p.Name,
			strings.TrimSuffix(s.cfg.PublicURL, "/"))
	}
	a, err := s.cfg.Store.StartApproval(ctx, p.ID, req.GetScope(), req.GetDescription(), store.DefaultApprovalLifetime)
	if err != nil {
		return nil, grpcError(err)
	}
	s.audit(ctx, p.Name, "approval.ask", map[string]any{"approval": a.ID, "description": a.Description})
	return s.approvalOut(a, time.Now()), nil
}

// GetApproval reads an approval.
func (s *Server) GetApproval(ctx context.Context, req *identitypb.GetApprovalRequest) (*identitypb.Approval, error) {
	a, err := s.cfg.Store.Approval(ctx, req.GetId())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "no such approval: it may have expired a day ago")
	}
	if err != nil {
		return nil, grpcError(err)
	}
	return s.approvalOut(a, time.Now()), nil
}

func (s *Server) approvalOut(a store.Approval, now time.Time) *identitypb.Approval {
	out := &identitypb.Approval{Id: a.ID, PrincipalId: a.PrincipalID, Scope: a.Scope, ExpiresAt: timestamppb.New(a.Expires),
		Url: strings.TrimSuffix(s.cfg.PublicURL, "/") + "/id/approve/" + a.ID}
	switch {
	case a.Decision == store.ApprovalApproved:
		out.State = identitypb.Approval_STATE_APPROVED
	case a.Decision == store.ApprovalDeclined:
		out.State = identitypb.Approval_STATE_DECLINED
	case !now.Before(a.Expires):
		out.State = identitypb.Approval_STATE_EXPIRED
	default:
		out.State = identitypb.Approval_STATE_WAITING
	}
	return out
}
