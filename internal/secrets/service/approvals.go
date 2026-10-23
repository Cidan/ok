package service

import (
	"context"
	"time"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// IdentityApprovals asks people for passkeys through ok identity's Approvals.
type IdentityApprovals struct{ Client identitypb.ApprovalsClient }

func (a IdentityApprovals) Start(ctx context.Context, principal, scope, description string) (string, string, time.Time, error) {
	r, err := a.Client.StartApproval(ctx, &identitypb.StartApprovalRequest{PrincipalId: principal, Scope: scope, Description: description})
	if err != nil {
		return "", "", time.Time{}, err
	}
	return r.GetId(), r.GetUrl(), r.GetExpiresAt().AsTime(), nil
}

// Status reads an approval: one of another person, or of anything but exactly scope, approves
// nothing.
func (a IdentityApprovals) Status(ctx context.Context, id, principal, scope string) (bool, bool, error) {
	r, err := a.Client.GetApproval(ctx, &identitypb.GetApprovalRequest{Id: id})
	if err != nil {
		return false, false, err
	}
	if r.GetPrincipalId() != principal || r.GetScope() != scope {
		return false, true, nil
	}
	switch r.GetState() {
	case identitypb.Approval_STATE_WAITING:
		return false, false, nil
	case identitypb.Approval_STATE_APPROVED:
		return true, true, nil
	}
	return false, true, nil
}
