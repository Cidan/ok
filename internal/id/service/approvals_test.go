package service_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/id/store"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// ok secrets asks a person with a passkey to approve, and reads their answer; no one else may.
func TestApprovals(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	if _, err := e.store.CreateRobot(ctx, secretsBot); err != nil {
		t.Fatal(err)
	}
	ap := identitypb.NewApprovalsClient(e.as(secretsBot))
	adaID, _ := e.store.Principal(ctx, ada)
	req := &identitypb.StartApprovalRequest{PrincipalId: adaID.ID, Scope: "secrets/unseal/7/secret/prod/db", Description: "Unseal secret/prod/db"}
	if _, err := ap.StartApproval(ctx, req); code(err) != codes.FailedPrecondition {
		t.Errorf("asking a person with no passkey: %v", err)
	}
	if err := e.store.AddPasskey(ctx, store.Passkey{ID: []byte("key"), PrincipalID: adaID.ID, Data: []byte(`{}`), Label: "k"}); err != nil {
		t.Fatal(err)
	}
	a, err := ap.StartApproval(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if a.GetState() != identitypb.Approval_STATE_WAITING || a.GetPrincipalId() != adaID.ID || a.GetScope() != req.Scope ||
		a.GetUrl() != "https://ok.example.com/id/approve/"+a.GetId() {
		t.Errorf("the approval: %+v", a)
	}
	if err := e.store.DecideApproval(ctx, a.GetId(), adaID.ID, true); err != nil {
		t.Fatal(err)
	}
	if got, err := ap.GetApproval(ctx, &identitypb.GetApprovalRequest{Id: a.GetId()}); err != nil || got.GetState() != identitypb.Approval_STATE_APPROVED {
		t.Errorf("approved: %+v, %v", got, err)
	}
	if _, err := ap.GetApproval(ctx, &identitypb.GetApprovalRequest{Id: "forged"}); code(err) != codes.NotFound {
		t.Errorf("a forged approval: %v", err)
	}
	for name, bad := range map[string]*identitypb.StartApprovalRequest{
		"no scope":       {PrincipalId: adaID.ID, Description: "d"},
		"no description": {PrincipalId: adaID.ID, Scope: "s"},
		"a long one":     {PrincipalId: adaID.ID, Scope: "s", Description: strings.Repeat("x", 2001)},
	} {
		if _, err := ap.StartApproval(ctx, bad); code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
	}
	for who, conn := range map[string]string{"a person": ada, "the deployer": deploy, "the SCM": scm} {
		if _, err := identitypb.NewApprovalsClient(e.as(conn)).GetApproval(ctx, &identitypb.GetApprovalRequest{Id: a.GetId()}); code(err) != codes.PermissionDenied {
			t.Errorf("%s reading an approval: %v", who, err)
		}
	}
}
