package service_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// A hosted service's person deletes their own account, typing their handle; an administrator
// alone restores it. An organization's owners delete and restore it, its members may not. An
// installation's people are its system of record's to delete.
func TestDeletions(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	if _, err := e.store.CreateAccount(ctx, "dee", "dee@example.com", "github", "gh-dee"); err != nil {
		t.Fatal(err)
	}
	dee := identitypb.NewTenancyClient(e.as("dee.human.example.com"))
	if _, err := dee.DeleteAccount(ctx, &identitypb.DeleteAccountRequest{Confirm: "not dee"}); code(err) != codes.InvalidArgument {
		t.Errorf("deleting an account without its handle: %v", err)
	}
	d, err := dee.DeleteAccount(ctx, &identitypb.DeleteAccountRequest{Confirm: "dee"})
	if err != nil || d.Final.AsTime().Sub(d.Requested.AsTime()) < 29*24*time.Hour {
		t.Fatalf("DeleteAccount = %v, %v", d, err)
	}
	if _, err := identitypb.NewAdminClient(e.as(bea)).RestoreAccount(ctx, &identitypb.RestoreAccountRequest{Name: "dee.human.example.com"}); code(err) != codes.PermissionDenied {
		t.Errorf("a person who is no administrator restoring an account: %v", err)
	}
	if _, err := identitypb.NewAdminClient(e.as(ada)).RestoreAccount(ctx, &identitypb.RestoreAccountRequest{Name: "dee.human.example.com"}); err != nil {
		t.Errorf("an administrator restoring an account: %v", err)
	}
	if p, _ := e.store.Principal(ctx, "dee.human.example.com"); !p.Active() {
		t.Errorf("the restored account = %+v", p)
	}

	if _, err := e.bea.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.SetMember(ctx, &identitypb.SetMemberRequest{Handle: "acme", PrincipalId: e.adaID, Role: "member"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ada.DeleteOrganization(ctx, &identitypb.OrganizationDeletionRequest{Handle: "acme"}); code(err) != codes.PermissionDenied {
		t.Errorf("a member deleting the organization: %v", err)
	}
	if _, err := e.bea.DeleteOrganization(ctx, &identitypb.OrganizationDeletionRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.DeleteOrganization(ctx, &identitypb.OrganizationDeletionRequest{Handle: "acme"}); code(err) != codes.FailedPrecondition {
		t.Errorf("deleting an organization twice: %v", err)
	}
	if _, err := e.bea.RestoreOrganization(ctx, &identitypb.OrganizationDeletionRequest{Handle: "acme"}); err != nil {
		t.Errorf("an owner restoring the organization: %v", err)
	}

	inst := newTenancyEnv(t, false)
	if _, err := inst.ada.DeleteAccount(ctx, &identitypb.DeleteAccountRequest{Confirm: "ada"}); code(err) != codes.FailedPrecondition {
		t.Errorf("deleting an installation's account: %v", err)
	}
}
