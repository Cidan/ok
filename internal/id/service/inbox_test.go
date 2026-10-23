package service_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// ok control sets a CL's inbox entries; people list their own, of repositories they may still see,
// and may write none.
func TestInbox(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	if _, err := e.ada.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	repo, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "acme", Name: "app", Visibility: "private", ForPersonId: e.adaID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: repo.Id}); err != nil {
		t.Fatal(err)
	}
	entries := []*identitypb.InboxEntry{
		{PrincipalId: e.adaID, RepositoryId: repo.Id, Number: 1, Relation: identitypb.InboxEntry_RELATION_AUTHORED, Repository: "acme/app",
			Title: "a change", Status: "open"},
		{PrincipalId: e.calID, RepositoryId: repo.Id, Number: 1, Relation: identitypb.InboxEntry_RELATION_REVIEWING, Attention: true,
			Repository: "acme/app", Title: "a change", Status: "open"},
	}
	people := identitypb.NewDeliveryClient(e.as(ada))
	if _, err := people.SetInbox(ctx, &identitypb.SetInboxRequest{RepositoryId: repo.Id, Number: 1, Version: 1, Entries: entries}); code(err) != codes.PermissionDenied {
		t.Errorf("a person setting an inbox: %v", err)
	}
	delivery := identitypb.NewDeliveryClient(e.as(control))
	if r, err := delivery.SetInbox(ctx, &identitypb.SetInboxRequest{RepositoryId: repo.Id, Number: 1, Version: 1, Entries: entries}); err != nil || !r.Applied {
		t.Fatalf("SetInbox = %v, %v", r, err)
	}
	ada := identitypb.NewInboxClient(e.as(ada))
	if r, err := ada.ListInbox(ctx, &identitypb.ListInboxRequest{}); err != nil || len(r.Entries) != 1 || r.Entries[0].Title != "a change" {
		t.Errorf("ada's inbox: %v, %v", r, err)
	}
	if _, err := ada.ListInbox(ctx, &identitypb.ListInboxRequest{ForPersonId: e.calID}); code(err) != codes.PermissionDenied {
		t.Errorf("ada listing cal's inbox: %v", err)
	}
	// cal reviews a CL of a private repository she cannot see: it is not listed to her.
	inbox := identitypb.NewInboxClient(e.as(control))
	if r, err := inbox.ListInbox(ctx, &identitypb.ListInboxRequest{ForPersonId: e.calID}); err != nil || len(r.Entries) != 0 {
		t.Errorf("cal's inbox, of a repository she cannot see: %v, %v", r, err)
	}
	if _, err := e.ada.UpdateRepository(ctx, &identitypb.UpdateRepositoryRequest{Id: repo.Id, Visibility: "public"}); err != nil {
		t.Fatal(err)
	}
	if r, err := inbox.ListInbox(ctx, &identitypb.ListInboxRequest{ForPersonId: e.calID}); err != nil || len(r.Entries) != 1 || !r.Entries[0].Attention {
		t.Errorf("cal's inbox once she may see it: %v, %v", r, err)
	}
}
