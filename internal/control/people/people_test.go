package people

import (
	"context"
	"reflect"
	"testing"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/names"
	"github.com/Cidan/ok/internal/serve/authn"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// principals is a directory of principals by stable ID, for the resolver.
type principals map[string]authn.Principal

func (p principals) Principal(_ context.Context, name string) (authn.Principal, error) {
	for _, x := range p {
		if x.Name == name {
			return x, nil
		}
	}
	return authn.Principal{}, authn.ErrNotFound
}

func (p principals) PrincipalByID(_ context.Context, id string) (authn.Principal, error) {
	x, ok := p[id]
	if !ok {
		return authn.Principal{}, authn.ErrNotFound
	}
	return x, nil
}

func TestNames(t *testing.T) {
	ctx := context.Background()
	dir := principals{
		"prn_ada": {ID: "prn_ada", Name: "ada.human.example.com", Human: true, Active: true},
		"prn_bob": {ID: "prn_bob", Name: "bob.human.example.com", Human: true, Active: true},
		"prn_ctl": {ID: "prn_ctl", Name: "control.ok.robot.example.com", Active: true},
	}
	Use(identity.MustNames("example.com"), "acme.org", names.New(dir), false)
	t.Cleanup(func() { Use(identity.MustNames("example.com"), "", nil, false) })
	for _, tc := range []struct{ in, principal, id, handle string }{
		{"ada", "ada.human.example.com", "prn_ada", "ada"},
		{" Ada@acme.org ", "ada.human.example.com", "prn_ada", "ada"},
		{"ada.human.example.com", "ada.human.example.com", "prn_ada", "ada"},
		{"control.ok.robot.example.com", "control.ok.robot.example.com", "prn_ctl", "control.ok.robot.example.com"},
		{"@ada", "ada.human.example.com", "prn_ada", "ada"},
	} {
		if got := Principal(tc.in); got != tc.principal {
			t.Errorf("Principal(%q) = %q, want %q", tc.in, got, tc.principal)
		}
		if got, err := ID(ctx, tc.in); err != nil || got != tc.id {
			t.Errorf("ID(%q) = %q, %v; want %q", tc.in, got, err, tc.id)
		}
		if got := Handle(ctx, tc.id); got != tc.handle {
			t.Errorf("Handle(%q) = %q, want %q", tc.id, got, tc.handle)
		}
		if got := HandleOf(tc.principal); got != tc.handle {
			t.Errorf("HandleOf(%q) = %q, want %q", tc.principal, got, tc.handle)
		}
	}
	if _, err := ID(ctx, "nobody"); err == nil {
		t.Error("a handle no one has resolved")
	}
	if got := Handle(ctx, "prn_gone"); got != "prn_gone" {
		t.Errorf("a principal the directory cannot name shows as %q; want its ID", got)
	}
	if FromEmail(ctx, "Bob@acme.org") != "prn_bob" || FromEmail(ctx, "bob@example.com") != "" || FromEmail(ctx, "@acme.org") != "" ||
		FromEmail(ctx, "carol@acme.org") != "" || EmailHandle("Carol@acme.org") != "carol" {
		t.Error("FromEmail, EmailHandle")
	}
	if Email(ctx, "prn_bob") != "bob@acme.org" || !IsHumanID(ctx, "prn_bob") || IsHumanID(ctx, "prn_ctl") || !IsHuman("bob.human.example.com") {
		t.Error("Email/IsHuman")
	}
	for _, tc := range []struct{ reason, by, want string }{
		{"canceled", "", "canceled"},
		{"canceled", "prn_ada", "canceled by ada"},
		{"", "prn_ada", "canceled by ada"},
		{"not wanted", "prn_ada", "not wanted (canceled by ada)"},
	} {
		if got := Reason(ctx, tc.reason, tc.by); got != tc.want {
			t.Errorf("Reason(%q, %q) = %q, want %q", tc.reason, tc.by, got, tc.want)
		}
	}
	for _, tc := range []struct{ reason, by, want string }{
		{"rolled back: unhealthy", "", "rolled back: unhealthy"},
		{"halted", "prn_bob", "halted by bob"},
		{"halted before it started", "prn_bob", "halted by bob before it started"},
		{"its pushes failed", "prn_bob", "its pushes failed (halted by bob)"},
	} {
		if got := HaltReason(ctx, tc.reason, tc.by); got != tc.want {
			t.Errorf("HaltReason(%q, %q) = %q, want %q", tc.reason, tc.by, got, tc.want)
		}
	}
	// The email domain is the domain unless set; with no resolver, a principal's ID is its name.
	Use(identity.MustNames("corp.test"), "", nil, false)
	if Email(ctx, "bob.human.corp.test") != "bob@corp.test" || Principal("bob") != "bob.human.corp.test" {
		t.Errorf("the default email domain: %s, %s", Email(ctx, "bob.human.corp.test"), Principal("bob"))
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
			{Id: "prn_3", Name: "lina.human.example.com", Status: active},
			{Id: "prn_9", Name: "gone.human.example.com", Status: identitypb.Status_STATUS_DELETED},
		}}, nil
	}
	return &identitypb.ListPrincipalsResponse{Principals: []*identitypb.Principal{
		{Id: "prn_1", Name: "olin.human.example.com", Status: active},
		{Id: "prn_2", Name: "linda.human.example.com", Status: active},
	}}, nil
}

func (f *fakeDirectory) CheckAdmin(_ context.Context, in *identitypb.CheckAdminRequest, _ ...grpc.CallOption) (*identitypb.CheckAdminResponse, error) {
	f.adminCalls++
	return &identitypb.CheckAdminResponse{Admin: in.GetPrincipalId() == "prn_3"}, nil
}

func (f *fakeDirectory) ListAdmins(context.Context, *identitypb.ListAdminsRequest, ...grpc.CallOption) (*identitypb.ListAdminsResponse, error) {
	f.adminCalls++
	return &identitypb.ListAdminsResponse{Principals: []string{"lina.human.example.com"}, PrincipalIds: []string{"prn_3"}}, nil
}

func (f *fakeDirectory) ExpandSet(_ context.Context, in *identitypb.ExpandSetRequest, _ ...grpc.CallOption) (*identitypb.ExpandSetResponse, error) {
	return &identitypb.ExpandSetResponse{Principals: []string{"olin.human.example.com", "lina.human.example.com"}, PrincipalIds: []string{"prn_1", "prn_3"}}, nil
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

// The directory answers by stable ID: people in their names' order, a search by handle, ACL
// members, and administrators.
func TestDirectory(t *testing.T) {
	ctx := context.Background()
	f := &fakeDirectory{}
	d := NewDirectory(f)
	humans, err := d.Humans(ctx)
	if err != nil || !reflect.DeepEqual(humans, []string{"prn_3", "prn_2", "prn_1"}) {
		t.Fatalf("humans %v, %v", humans, err)
	}
	if ok, _ := d.IsActiveHuman(ctx, "prn_9"); ok {
		t.Error("a deleted person is active")
	}
	if ok, _ := d.IsActiveHuman(ctx, "prn_2"); !ok {
		t.Error("an active person is not")
	}
	got, err := d.Search(ctx, "lin", map[string]bool{"prn_2": true}, 5)
	if err != nil || !reflect.DeepEqual(got, []string{"prn_3", "prn_1"}) {
		t.Errorf("search: %v, %v", got, err)
	}
	if f.calls != 2 {
		t.Errorf("identity was asked %d times; the cache should hold one listing", f.calls)
	}
	if members, err := d.ACLMembers(ctx, "team.acl.example.com"); err != nil || !reflect.DeepEqual(members, []string{"prn_1", "prn_3"}) {
		t.Errorf("ACLMembers = %v, %v", members, err)
	}
	if in, err := d.InACL(ctx, "team.acl.example.com", "prn_3"); err != nil || !in {
		t.Errorf("InACL = %v, %v", in, err)
	}

	for who, want := range map[string]bool{"prn_3": true, "prn_1": false} {
		for range 2 {
			if got, err := d.IsAdmin(ctx, who); err != nil || got != want {
				t.Errorf("IsAdmin(%s) = %v, %v", who, got, err)
			}
		}
	}
	if admins, err := d.Admins(ctx); err != nil || !reflect.DeepEqual(admins, []string{"prn_3"}) {
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

// A hosted service shows names as one does — @ada, ok/control — and takes them as written either
// way.
func TestNamesHosted(t *testing.T) {
	ctx := context.Background()
	dir := principals{
		"prn_ada": {ID: "prn_ada", Name: "ada.human.example.com", Human: true, Active: true},
		"prn_ctl": {ID: "prn_ctl", Name: "control.ok.robot.example.com", Active: true},
	}
	Use(identity.MustNames("example.com"), "", names.New(dir), true)
	t.Cleanup(func() { Use(identity.MustNames("example.com"), "", nil, false) })
	for id, want := range map[string]string{"prn_ada": "@ada", "prn_ctl": "ok/control"} {
		if got := Handle(ctx, id); got != want {
			t.Errorf("Handle(%q) = %q, want %q", id, got, want)
		}
	}
	for _, in := range []string{"@ada", "ada", "ada.human.example.com"} {
		if got, err := ID(ctx, in); err != nil || got != "prn_ada" {
			t.Errorf("ID(%q) = %q, %v", in, got, err)
		}
	}
}
