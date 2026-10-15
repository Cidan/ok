package people

import (
	"context"
	"reflect"
	"testing"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/identity"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

func TestNames(t *testing.T) {
	Use(identity.MustNames("example.com"), "acme.org")
	t.Cleanup(func() { Use(identity.MustNames("example.com"), "") })
	for _, tc := range []struct{ in, principal, handle string }{
		{"ada", "ada.human.example.com", "ada"},
		{" Ada@acme.org ", "ada.human.example.com", "ada"},
		{"ada.human.example.com", "ada.human.example.com", "ada"},
		{"control.ok.robot.example.com", "control.ok.robot.example.com", "control.ok.robot.example.com"},
	} {
		if got := Principal(tc.in); got != tc.principal {
			t.Errorf("Principal(%q) = %q, want %q", tc.in, got, tc.principal)
		}
		if got := Handle(tc.principal); got != tc.handle {
			t.Errorf("Handle(%q) = %q, want %q", tc.principal, got, tc.handle)
		}
	}
	if FromEmail("Bob@acme.org") != "bob.human.example.com" || FromEmail("bob@example.com") != "" || FromEmail("@acme.org") != "" {
		t.Error("FromEmail")
	}
	if Email("bob.human.example.com") != "bob@acme.org" || !IsHuman("bob.human.example.com") || IsHuman("x.y.robot.example.com") {
		t.Error("Email/IsHuman")
	}
	// The email domain is the domain unless set.
	Use(identity.MustNames("corp.test"), "")
	if Email("bob.human.corp.test") != "bob@corp.test" || Principal("bob") != "bob.human.corp.test" {
		t.Errorf("the default email domain: %s, %s", Email("bob.human.corp.test"), Principal("bob"))
	}
}

type fakeDirectory struct {
	identitypb.DirectoryClient
	calls, adminCalls int
}

func (f *fakeDirectory) ListPrincipals(_ context.Context, in *identitypb.ListPrincipalsRequest, _ ...grpc.CallOption) (*identitypb.ListPrincipalsResponse, error) {
	f.calls++
	if in.GetKind() != identitypb.Kind_KIND_HUMAN {
		panic("asked for non-humans")
	}
	active := identitypb.Status_STATUS_ACTIVE
	if in.GetPageToken() == "" {
		return &identitypb.ListPrincipalsResponse{NextPageToken: "2", Principals: []*identitypb.Principal{
			{Name: "lina.human.example.com", Status: active},
			{Name: "gone.human.example.com", Status: identitypb.Status_STATUS_DELETED},
		}}, nil
	}
	return &identitypb.ListPrincipalsResponse{Principals: []*identitypb.Principal{
		{Name: "olin.human.example.com", Status: active},
		{Name: "linda.human.example.com", Status: active},
	}}, nil
}

func (f *fakeDirectory) CheckAdmin(_ context.Context, in *identitypb.CheckAdminRequest, _ ...grpc.CallOption) (*identitypb.CheckAdminResponse, error) {
	f.adminCalls++
	return &identitypb.CheckAdminResponse{Admin: in.GetPrincipal() == "lina.human.example.com"}, nil
}

func (f *fakeDirectory) ListAdmins(context.Context, *identitypb.ListAdminsRequest, ...grpc.CallOption) (*identitypb.ListAdminsResponse, error) {
	f.adminCalls++
	return &identitypb.ListAdminsResponse{Principals: []string{"lina.human.example.com"}}, nil
}

func (f *fakeDirectory) ListSets(context.Context, *identitypb.ListSetsRequest, ...grpc.CallOption) (*identitypb.ListSetsResponse, error) {
	return &identitypb.ListSetsResponse{Sets: []*identitypb.Set{
		{Name: "eng.group.example.com", Source: identitypb.SetSource_SET_SOURCE_SYNCED, Members: []string{"olin.human.example.com"}},
		{Name: "team.acl.example.com", Source: identitypb.SetSource_SET_SOURCE_TREE, Members: []string{"eng.group.example.com"}},
	}}, nil
}

func (f *fakeDirectory) ListRetiredNames(context.Context, *identitypb.ListRetiredNamesRequest, ...grpc.CallOption) (*identitypb.ListRetiredNamesResponse, error) {
	return &identitypb.ListRetiredNamesResponse{Names: []*identitypb.RetiredName{{Name: "old.group.example.com"}, {Name: "gone.human.example.com"}}}, nil
}

func TestDirectory(t *testing.T) {
	ctx := context.Background()
	f := &fakeDirectory{}
	d := NewDirectory(f)
	humans, err := d.Humans(ctx)
	if err != nil || !reflect.DeepEqual(humans, []string{"lina.human.example.com", "linda.human.example.com", "olin.human.example.com"}) {
		t.Fatalf("humans %v, %v", humans, err)
	}
	if ok, _ := d.IsActiveHuman(ctx, "gone.human.example.com"); ok {
		t.Error("a deleted person is active")
	}
	got, err := d.Search(ctx, "lin", map[string]bool{"linda.human.example.com": true}, 5)
	if err != nil || !reflect.DeepEqual(got, []string{"lina.human.example.com", "olin.human.example.com"}) {
		t.Errorf("search: %v, %v", got, err)
	}
	if f.calls != 2 {
		t.Errorf("identity was asked %d times; the cache should hold one listing", f.calls)
	}

	for who, want := range map[string]bool{"lina.human.example.com": true, "olin.human.example.com": false} {
		for range 2 {
			if got, err := d.IsAdmin(ctx, who); err != nil || got != want {
				t.Errorf("IsAdmin(%s) = %v, %v", who, got, err)
			}
		}
	}
	if admins, err := d.Admins(ctx); err != nil || !reflect.DeepEqual(admins, []string{"lina.human.example.com"}) {
		t.Errorf("Admins = %v, %v", admins, err)
	}
	d.Admins(ctx)
	if f.adminCalls != 3 {
		t.Errorf("identity was asked about administrators %d times; each answer is cached", f.adminCalls)
	}
	synced, err := d.SyncedGroups(ctx)
	if err != nil || !reflect.DeepEqual(synced, map[string][]string{"eng.group.example.com": {"olin.human.example.com"}}) {
		t.Errorf("SyncedGroups = %v, %v", synced, err)
	}
	retired, err := d.RetiredSetNames(ctx)
	if err != nil || !reflect.DeepEqual(retired, map[string]bool{"old.group.example.com": true}) {
		t.Errorf("RetiredSetNames = %v, %v", retired, err)
	}
}
