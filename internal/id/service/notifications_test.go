package service_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// ok control notifies; a person lists their own, of repositories they may see, marks them read, and
// chooses what is mailed, of the kinds there are.
func TestNotifications(t *testing.T) {
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
	note := func(to, source string) *identitypb.Notification {
		return &identitypb.Notification{Source: source, PrincipalId: to, Kind: identitypb.Notification_KIND_REVIEW_REQUESTED, RepositoryId: repo.Id,
			Number: 1, Repository: "acme/app", Title: "a change", ActorId: e.adaID, Text: "asked you to review it", Link: "/acme/app/cl/1"}
	}
	if _, err := identitypb.NewDeliveryClient(e.as(bea)).Notify(ctx, &identitypb.NotifyRequest{Notifications: []*identitypb.Notification{note(e.calID, "x")}}); code(err) != codes.PermissionDenied {
		t.Errorf("a person notifying: %v", err)
	}
	delivery := identitypb.NewDeliveryClient(e.as(control))
	r, err := delivery.Notify(ctx, &identitypb.NotifyRequest{Notifications: []*identitypb.Notification{note(e.adaID, "e:1"), note(e.calID, "e:1"), note(e.adaID, "e:1")}})
	if err != nil || r.Added != 2 || r.Mailed != 0 {
		t.Fatalf("Notify = %v, %v; want two added, none mailed with no mail server", r, err)
	}
	ada := identitypb.NewNotificationsClient(e.as(ada))
	list, err := ada.ListNotifications(ctx, &identitypb.ListNotificationsRequest{})
	if err != nil || len(list.Notifications) != 1 || list.Unread != 1 || list.Notifications[0].Text != "asked you to review it" {
		t.Errorf("ada's notifications: %v, %v", list, err)
	}
	if _, err := ada.ListNotifications(ctx, &identitypb.ListNotificationsRequest{ForPersonId: e.calID}); code(err) != codes.PermissionDenied {
		t.Errorf("ada listing cal's: %v", err)
	}
	// cal's is of a private repository she may not see: not listed.
	cal := identitypb.NewNotificationsClient(e.as("cal.human.example.com"))
	if list, err := cal.ListNotifications(ctx, &identitypb.ListNotificationsRequest{}); err != nil || len(list.Notifications) != 0 {
		t.Errorf("cal's, of a repository she may not see: %v, %v", list, err)
	}
	if _, err := ada.MarkRead(ctx, &identitypb.MarkReadRequest{All: true}); err != nil {
		t.Fatal(err)
	}
	if list, err := ada.ListNotifications(ctx, &identitypb.ListNotificationsRequest{Unread: true}); err != nil || list.Unread != 0 || len(list.Notifications) != 0 {
		t.Errorf("ada's unread once read: %v, %v", list, err)
	}

	prefs, err := ada.GetNotificationPreferences(ctx, &identitypb.GetNotificationPreferencesRequest{})
	if err != nil || !prefs.Default || len(prefs.Mailed) != 2 || prefs.Mailed[0] != identitypb.Notification_KIND_REVIEW_REQUESTED {
		t.Errorf("ada's default preferences: %v, %v", prefs, err)
	}
	prefs, err = ada.SetNotificationPreferences(ctx, &identitypb.NotificationPreferences{Mailed: []identitypb.Notification_Kind{identitypb.Notification_KIND_COMMENTED}})
	if err != nil || prefs.Default || len(prefs.Mailed) != 1 || prefs.Mailed[0] != identitypb.Notification_KIND_COMMENTED {
		t.Errorf("ada's preferences once set: %v, %v", prefs, err)
	}
	if _, err := ada.SetNotificationPreferences(ctx, &identitypb.NotificationPreferences{Mailed: []identitypb.Notification_Kind{42}}); code(err) != codes.InvalidArgument {
		t.Errorf("a kind there is not: %v", err)
	}
}
