package service_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/id/dirfeed"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// A follower reads the directory's log through identity's DirectoryEvents, as dirfeed.Log does:
// each repository's events in order, and how far each log reaches. Only ok's roles read it.
func TestDirectoryEvents(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	if _, err := e.ada.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	app, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "acme", Name: "app", ForPersonId: e.adaID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: app.Id}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ada.RenameRepository(ctx, &identitypb.RenameRepositoryEntryRequest{Id: app.Id, Name: "service"}); err != nil {
		t.Fatal(err)
	}

	log := dirfeed.Log{Events: identitypb.NewDirectoryEventsClient(e.as(control))}
	evs, err := log.Read(ctx, app.Id, 0)
	if err != nil || len(evs) != 2 {
		t.Fatalf("Read = %v, %v", evs, err)
	}
	var decoded []*identitypb.DirectoryEvent
	for _, b := range evs {
		ev, err := dirfeed.Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		decoded = append(decoded, ev)
	}
	if decoded[0].GetKind() != identitypb.DirectoryEvent_KIND_CREATED || decoded[1].GetKind() != identitypb.DirectoryEvent_KIND_RENAMED ||
		decoded[1].GetOldName() != "app" || decoded[1].GetRepository().GetName() != "service" || decoded[1].GetRepository().GetOwnerId() == "" {
		t.Errorf("the events = %v", decoded)
	}
	var heads []follow.Head
	if err := log.Heads(ctx, func(page []follow.Head) error { heads = append(heads, page...); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(heads) != 1 || heads[0] != (follow.Head{Key: app.Id, Seq: 2}) {
		t.Errorf("Heads = %v", heads)
	}
	if after, err := log.Read(ctx, app.Id, 2); err != nil || len(after) != 0 {
		t.Errorf("after the last = %v, %v", after, err)
	}
	if _, err := identitypb.NewDirectoryEventsClient(e.as(ada)).Heads(ctx, &identitypb.DirectoryHeadsRequest{}); code(err) != codes.PermissionDenied {
		t.Errorf("a person reading the log: %v", err)
	}
	// An event whose key or place disagrees with its body is refused.
	bad := evs[0]
	bad.Seq = 9
	if _, err := dirfeed.Decode(bus.Event(bad)); err == nil {
		t.Error("a misnumbered event decoded")
	}
}
