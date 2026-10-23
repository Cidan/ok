package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Moving an installation in: its organization takes the new handle, the old one an alias of it;
// its people its members, its administrators its owners; its synced groups its teams — one an
// administrator renamed keeping its name — nested ones flattened, their grants with them, each old
// name an alias; its own robot the organization's, ok's role's kept; its system of record the
// organization's provider, its people linked, whose syncs after rename nothing; its repositories
// <org>/<name>, each a rename in its log, and their bare names the organization's. The directory
// serves a hosted service after, and moves in once.
func testMoveIn(t *testing.T, s *store.Store) {
	ctx := context.Background()
	src := &source{snap: &directory.Snapshot{
		People: []directory.Person{person("1", "ada", true), person("2", "bea", true), person("3", "cal", false)},
		Groups: []directory.Group{
			{ExternalID: "g-eng", Name: "eng", Members: []directory.Member{{ExternalID: "1"}, {ExternalID: "g-web", Group: true}}},
			{ExternalID: "g-web", Name: "web", Members: []directory.Member{{ExternalID: "2"}}},
		},
	}}
	if _, err := s.CheckMode(ctx, false); err != nil {
		t.Fatal(err)
	}
	if err := s.SetName(ctx, "g-web", true, "frontend"); err != nil {
		t.Fatal(err)
	}
	sync(t, s, src)
	ada, _ := s.Principal(ctx, "ada.human.example.com")
	bea, _ := s.Principal(ctx, "bea.human.example.com")
	inst, err := s.EnsureOrganization(ctx, "example", store.OrganizationSettings{BaseRole: tenancy.Write, MembersCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	s.SetInstallation(inst.ID)
	ci, err := s.CreateRobot(ctx, "ci.robot.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateRobot(ctx, "scm.ok.robot.example.com"); err != nil {
		t.Fatal(err)
	}
	app, err := s.ReserveRepository(ctx, store.NewRepository{Owner: inst.ID, Name: "app", Propose: tenancy.Write, CreatedBy: ada.ID})
	if err != nil {
		t.Fatal(err)
	}
	if app, err = s.ActivateRepository(ctx, app.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.SetGrant(ctx, store.Grant{Repository: app.ID, Subject: "eng.group.example.com", Role: tenancy.Maintain}); err != nil {
		t.Fatal(err)
	}

	provider := &store.IdentityProvider{Source: "scim", Settings: json.RawMessage(`{"scim":{}}`), PlainHandles: true}
	rep, err := s.MoveIn(ctx, store.MoveInPlan{Organization: inst.ID, Handle: "acme", Owners: []string{ada.ID}, Provider: provider})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Handle != "acme" || !rep.Renamed || rep.Repositories != 1 || rep.Members != 2 || rep.Owners != 1 || rep.Teams != 2 || rep.Robots != 1 ||
		rep.Linked != 3 || len(rep.KeptRobots) != 1 || rep.KeptRobots[0] != "scm.ok.robot.example.com" {
		t.Errorf("MoveIn = %+v", rep)
	}
	s.SetInstallation("")

	// The organization, under its new handle and its old.
	if o, err := s.Owner(ctx, "acme"); err != nil || o.ID != inst.ID {
		t.Errorf("the new handle = %+v, %v", o, err)
	}
	if o, err := s.Owner(ctx, "example"); err != nil || o.ID != inst.ID {
		t.Errorf("the old handle = %+v, %v", o, err)
	}
	if err := s.HandleAvailable(ctx, "example"); !errors.Is(err, store.ErrTaken) {
		t.Errorf("the old handle free for anyone: %v", err)
	}
	// Its people, its members; its administrator, an owner.
	ms, err := s.MembershipsOf(ctx, ada.ID)
	if err != nil || ms[inst.ID] != tenancy.Owner {
		t.Errorf("ada's membership = %v, %v", ms, err)
	}
	if ms, _ := s.MembershipsOf(ctx, bea.ID); ms[inst.ID] != tenancy.Member {
		t.Errorf("bea's membership = %v", ms)
	}
	// Its groups, its teams, nested ones flattened, the grant with them; the old names aliases.
	eng, err := s.Team(ctx, "eng.acme.team.example.com")
	if err != nil || eng.ExternalID != "g-eng" || len(eng.Members) != 2 {
		t.Errorf("the eng team = %+v, %v", eng, err)
	}
	if to, err := s.Alias(ctx, "eng.group.example.com"); err != nil || to != eng.Name {
		t.Errorf("eng.group's alias = %q, %v", to, err)
	}
	if web, err := s.Team(ctx, "frontend.acme.team.example.com"); err != nil || web.ExternalID != "g-web" {
		t.Errorf("the team its administrators named = %+v, %v", web, err)
	}
	grants, err := s.Grants(ctx, app.ID)
	if err != nil || len(grants) != 1 || grants[0].Subject != eng.Name || grants[0].Role != tenancy.Maintain {
		t.Errorf("app's grants = %+v, %v", grants, err)
	}
	// Its robot, the organization's, under its old name too; ok's role's kept.
	if r, err := s.Principal(ctx, "ci.robot.example.com"); err != nil || r.ID != ci.ID || r.Name != "ci.acme.robot.example.com" {
		t.Errorf("the robot under its old name = %+v, %v", r, err)
	}
	if of, err := s.RobotOrganization(ctx, ci.ID); err != nil || of != inst.ID {
		t.Errorf("the robot's organization = %q, %v", of, err)
	}
	// Its repository, acme/app, its bare name the organization's, its rename in its log.
	r, redirected, err := s.ResolveFullName(ctx, "app")
	if err != nil || r.ID != app.ID {
		t.Errorf("the bare name = %+v, %v, %v", r, redirected, err)
	}
	if e, err := s.Entry(ctx, r); err != nil || e.GetFullName() != "acme/app" {
		t.Errorf("the entry = %v, %v", e, err)
	}
	evs, err := s.DirectoryEvents(ctx, app.ID, 0)
	if err != nil || len(evs) == 0 {
		t.Fatalf("app's log = %v, %v", evs, err)
	}
	renamed := false
	for _, row := range evs {
		ev := &identitypb.DirectoryEvent{}
		if err := proto.Unmarshal(row.Body, ev); err != nil {
			t.Fatal(err)
		}
		renamed = renamed || ev.GetKind() == identitypb.DirectoryEvent_KIND_RENAMED && ev.GetOldFullName() == "app" &&
			ev.GetRepository().GetFullName() == "acme/app"
	}
	if !renamed {
		t.Errorf("app's log holds no rename from app to acme/app: %d events", len(evs))
	}

	// The organization's syncs keep every name — its people's handles, the name an administrator
	// set — and, once the first has read the provider, change nothing.
	sc := s.OrganizationSync(inst.ID)
	for i := 0; i < 2; i++ {
		opts, err := sc.Options(ctx)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := directory.Sync(ctx, names, src, sc, opts)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range plan.Humans {
			if h.Before == nil || h.Before.Name != h.After.Name {
				t.Errorf("sync %d: %s", i+1, h.What())
			}
		}
		for _, g := range plan.Groups {
			if g.Before == nil || g.Before.Name != g.After.Name || g.After.Deleted {
				t.Errorf("sync %d: %s: %v", i+1, g.After.Name, g.What())
			}
		}
		if i == 1 && !plan.Empty() {
			t.Errorf("the second sync changed %+v", plan)
		}
	}
	if eng, err := s.Team(ctx, "eng.acme.team.example.com"); err != nil || len(eng.Members) != 2 {
		t.Errorf("the eng team after its syncs = %+v, %v", eng, err)
	}

	// A hosted service serves it now; it moves in once.
	if _, err := s.CheckMode(ctx, false); !errors.Is(err, store.ErrMode) {
		t.Errorf("an installation serving it: %v", err)
	}
	if movedIn, err := s.CheckMode(ctx, true); err != nil || movedIn != inst.ID {
		t.Errorf("a hosted service serving it = %q, %v", movedIn, err)
	}
	if _, err := s.MoveIn(ctx, store.MoveInPlan{Organization: inst.ID}); !errors.Is(err, store.ErrMode) {
		t.Errorf("moving in twice: %v", err)
	}
}
