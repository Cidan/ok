package storetest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// Organizations, their members and teams, repositories' entries and the names they had, and the
// roles granted on them (docs/hosted.md, "Tenancy, as built").
func testTenancy(t *testing.T, s *store.Store) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0)
	s.SetClock(func() time.Time { return now })
	src := &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true), person("2", "bea", true),
		person("3", "cal", true), person("4", "gone", false)}}}
	sync(t, s, src)
	id := func(name string) string {
		t.Helper()
		p, err := s.Principal(ctx, name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return p.ID
	}
	ada, bea, cal, gone := id("ada.human.example.com"), id("bea.human.example.com"), id("cal.human.example.com"), id("gone.human.example.com")

	// An organization: a handle no person has, owned by an active person.
	set := store.OrganizationSettings{BaseRole: tenancy.Read, MembersCreate: true}
	acme, err := s.CreateOrganization(ctx, "acme", ada, set)
	if err != nil || acme.Name != "acme.org.example.com" || acme.Kind != store.Org || acme.Handle() != "acme" || acme.BaseRole != tenancy.Read {
		t.Fatalf("CreateOrganization = %+v, %v", acme, err)
	}
	for handle, want := range map[string]error{"acme": store.ErrTaken, "ada": store.ErrTaken, "settings": store.ErrInvalid, "-x": store.ErrInvalid} {
		if _, err := s.CreateOrganization(ctx, handle, bea, set); !errors.Is(err, want) {
			t.Errorf("an organization %q: %v, want %v", handle, err, want)
		}
	}
	if _, err := s.CreateOrganization(ctx, "nope", gone, set); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("an organization owned by a suspended person: %v", err)
	}
	if got, err := s.OrganizationByHandle(ctx, "acme"); err != nil || got.ID != acme.ID {
		t.Errorf("OrganizationByHandle = %+v, %v", got, err)
	}
	// The sync gives no person an organization's handle.
	src.snap.People = append(src.snap.People, person("5", "acme", true))
	plan := sync(t, s, src)
	if _, err := s.Principal(ctx, "acme.human.example.com"); !errors.Is(err, store.ErrNotFound) || len(plan.Waiting) != 1 {
		t.Errorf("a person wanting an organization's handle: %v, waiting %v", err, plan.Waiting)
	}
	// An organization with no owner: an installation's own.
	inst, err := s.CreateOrganization(ctx, "example", "", store.OrganizationSettings{BaseRole: tenancy.Write})
	if err != nil {
		t.Fatal(err)
	}
	if ms, err := s.Members(ctx, inst.ID); err != nil || len(ms) != 0 {
		t.Errorf("an installation's organization's members = %v, %v", ms, err)
	}
	if acme, err = s.SetOrganization(ctx, acme.ID, store.OrganizationSettings{BaseRole: tenancy.Triage, PrivateForks: true, Enterprise: "e1"}); err != nil ||
		acme.BaseRole != tenancy.Triage || acme.MembersCreate || !acme.PrivateForks || acme.Enterprise != "e1" {
		t.Errorf("SetOrganization = %+v, %v", acme, err)
	}

	// Members; the last owner stays one.
	if err := s.SetMember(ctx, acme.ID, bea, tenancy.Member); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMember(ctx, acme.ID, gone, tenancy.Member); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a suspended person as a member: %v", err)
	}
	if err := s.SetMember(ctx, acme.ID, ada, tenancy.Member); !errors.Is(err, store.ErrLastOwner) {
		t.Errorf("the last owner made a member: %v", err)
	}
	if _, err := s.RemoveMember(ctx, acme.ID, ada); !errors.Is(err, store.ErrLastOwner) {
		t.Errorf("the last owner removed: %v", err)
	}
	if err := s.SetMember(ctx, acme.ID, bea, tenancy.Owner); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMember(ctx, acme.ID, ada, tenancy.Member); err != nil {
		t.Errorf("an owner made a member beside another owner: %v", err)
	}
	if ms, err := s.MembershipsOf(ctx, ada); err != nil || ms[acme.ID] != tenancy.Member {
		t.Errorf("MembershipsOf(ada) = %v, %v", ms, err)
	}

	// Teams: nested, of members only; a deleted team's name retired and its grants gone.
	eng, err := s.CreateTeam(ctx, acme.ID, "eng", "")
	if err != nil || eng.Name != "eng.acme.team.example.com" {
		t.Fatalf("CreateTeam = %+v, %v", eng, err)
	}
	web, err := s.CreateTeam(ctx, acme.ID, "web", eng.Name)
	if err != nil || web.Parent != eng.Name {
		t.Fatalf("a nested team = %+v, %v", web, err)
	}
	if _, err := s.CreateTeam(ctx, acme.ID, "eng", ""); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a team twice: %v", err)
	}
	if _, err := s.CreateTeam(ctx, inst.ID, "x", eng.Name); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a team under another organization's: %v", err)
	}
	if err := s.AddTeamMember(ctx, web.Name, ada); err != nil {
		t.Fatal(err)
	}
	if err := s.AddTeamMember(ctx, web.Name, cal); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a non-member in a team: %v", err)
	}
	if got, _ := s.Team(ctx, web.Name); !reflect.DeepEqual(got.Members, []string{ada}) {
		t.Errorf("web's members = %v", got.Members)
	}
	if err := s.DeleteTeam(ctx, eng.Name); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("deleting a team with one below it: %v", err)
	}

	// Repositories: an entry made Creating, then Active; a name unique per owner.
	r, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: "app", Visibility: tenancy.Private, Propose: tenancy.Read, CreatedBy: ada})
	if err != nil || r.State != store.Creating || r.Network == 0 || r.ID == 0 {
		t.Fatalf("ReserveRepository = %+v, %v", r, err)
	}
	if again, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: "app", Propose: tenancy.Read, CreatedBy: ada}); err != nil || again.ID != r.ID {
		t.Errorf("its maker asking again while it is made = %+v, %v; want entry %d", again, err, r.ID)
	}
	if _, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: "app", Propose: tenancy.Read, CreatedBy: bea}); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a name twice: %v", err)
	}
	for name, n := range map[string]store.NewRepository{
		"a bad name":           {Owner: acme.ID, Name: "App", Propose: tenancy.Read, CreatedBy: ada},
		"proposing with admin": {Owner: acme.ID, Name: "x", Propose: tenancy.Admin, CreatedBy: ada},
		"a team owning":        {Owner: eng.Name, Name: "x", Propose: tenancy.Read, CreatedBy: ada},
		"a suspended owner":    {Owner: gone, Name: "x", Propose: tenancy.Read, CreatedBy: ada},
	} {
		if _, err := s.ReserveRepository(ctx, n); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := s.RenameRepository(ctx, r.ID, "app2"); !errors.Is(err, store.ErrRepositoryState) {
		t.Errorf("renaming one being created: %v", err)
	}
	if r, err = s.ActivateRepository(ctx, r.ID); err != nil || r.State != store.Active {
		t.Fatalf("ActivateRepository = %+v, %v", r, err)
	}
	// A fork joins its parent's network.
	fork, err := s.ReserveRepository(ctx, store.NewRepository{Owner: cal, Name: "app", Visibility: tenancy.Private, Propose: tenancy.Read, Parent: r.ID, CreatedBy: cal})
	if err != nil || fork.Network != r.Network || fork.Parent != r.ID {
		t.Errorf("a fork = %+v, %v; want network %d", fork, err, r.Network)
	}
	// A creating entry abandoned is gone; an active one is not abandoned.
	if err := s.AbandonRepository(ctx, fork.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Repository(ctx, fork.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("an abandoned entry: %v", err)
	}
	if err := s.AbandonRepository(ctx, r.ID); !errors.Is(err, store.ErrRepositoryState) {
		t.Errorf("abandoning an active repository: %v", err)
	}

	// Grants: to a principal by ID, to a set by name; none to an organization or to no one.
	for _, g := range []store.Grant{{Repository: r.ID, Subject: cal, Role: tenancy.Write}, {Repository: r.ID, Subject: web.Name, Role: tenancy.Maintain},
		{Repository: r.ID, Subject: "eng.group.example.com", Role: tenancy.Read}} {
		if err := s.SetGrant(ctx, g); err != nil {
			t.Fatalf("SetGrant(%+v): %v", g, err)
		}
	}
	for name, g := range map[string]store.Grant{
		"an organization": {Repository: r.ID, Subject: acme.ID, Role: tenancy.Read},
		"no one":          {Repository: r.ID, Subject: "prn_nobody", Role: tenancy.Read},
		"no role":         {Repository: r.ID, Subject: cal, Role: tenancy.None},
	} {
		if err := s.SetGrant(ctx, g); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("a grant to %s: %v", name, err)
		}
	}
	if gs, err := s.Grants(ctx, r.ID); err != nil || len(gs) != 3 {
		t.Errorf("Grants = %v, %v", gs, err)
	}

	// A rename redirects the old name; a new repository taking it ends the redirect.
	if r, err = s.RenameRepository(ctx, r.ID, "service"); err != nil || r.Name != "service" {
		t.Fatalf("RenameRepository = %+v, %v", r, err)
	}
	if got, redirected, err := s.ResolveRepository(ctx, acme.ID, "app"); err != nil || !redirected || got.ID != r.ID {
		t.Errorf("resolving the old name = %+v, %v, %v", got, redirected, err)
	}
	if got, redirected, err := s.ResolveRepository(ctx, acme.ID, "service"); err != nil || redirected || got.ID != r.ID {
		t.Errorf("resolving the new name = %+v, %v, %v", got, redirected, err)
	}
	other, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: "app", Propose: tenancy.Write, CreatedBy: bea})
	if err != nil {
		t.Fatal(err)
	}
	if got, redirected, err := s.ResolveRepository(ctx, acme.ID, "app"); err != nil || redirected || got.ID != other.ID {
		t.Errorf("a name taken again = %+v, %v, %v", got, redirected, err)
	}
	if _, err := s.ActivateRepository(ctx, other.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenameRepository(ctx, other.ID, "service"); !errors.Is(err, store.ErrTaken) {
		t.Errorf("renaming onto a name held: %v", err)
	}

	// A transfer redirects from the old owner, and drops the sets' grants but the people's.
	if r, err = s.TransferRepository(ctx, r.ID, cal); err != nil || r.Owner != cal {
		t.Fatalf("TransferRepository = %+v, %v", r, err)
	}
	if got, redirected, err := s.ResolveRepository(ctx, acme.ID, "service"); err != nil || !redirected || got.ID != r.ID {
		t.Errorf("the old owner's name after a transfer = %+v, %v, %v", got, redirected, err)
	}
	if gs, _ := s.Grants(ctx, r.ID); len(gs) != 1 || gs[0].Subject != cal || gs[0].Role != tenancy.Write {
		t.Errorf("grants after a transfer = %+v", gs)
	}
	if _, err := s.TransferRepository(ctx, r.ID, acme.ID); err != nil {
		t.Errorf("a transfer back onto its own redirect: %v", err)
	}

	// Visibility and proposing.
	if r, err = s.SetVisibility(ctx, r.ID, tenancy.Public); err != nil || r.Visibility != tenancy.Public {
		t.Errorf("SetVisibility = %+v, %v", r, err)
	}
	if r, err = s.SetPropose(ctx, r.ID, tenancy.Write); err != nil || r.Propose != tenancy.Write {
		t.Errorf("SetPropose = %+v, %v", r, err)
	}

	// Deleting: restorable for the retention period, its name its own; then purged.
	if r, err = s.DeleteRepository(ctx, r.ID); err != nil || r.State != store.Deleted {
		t.Fatalf("DeleteRepository = %+v, %v", r, err)
	}
	if _, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: r.Name, Propose: tenancy.Read, CreatedBy: ada}); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a deleted repository's name taken: %v", err)
	}
	if err := s.PurgeRepository(ctx, r.ID); !errors.Is(err, store.ErrRepositoryState) {
		t.Errorf("purging within the retention period: %v", err)
	}
	now = now.Add(time.Hour)
	if r, err = s.RestoreRepository(ctx, r.ID); err != nil || r.State != store.Active {
		t.Errorf("RestoreRepository = %+v, %v", r, err)
	}
	if _, err := s.DeleteRepository(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.RetentionPeriod + time.Hour)
	if _, err := s.RestoreRepository(ctx, r.ID); !errors.Is(err, store.ErrRepositoryState) {
		t.Errorf("restoring past the retention period: %v", err)
	}
	stuck, err := s.ReserveRepository(ctx, store.NewRepository{Owner: bea, Name: "stuck", Propose: tenancy.Read, CreatedBy: bea})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.CreatingTimeout + time.Hour)
	deleted, creating, err := s.Expired(ctx, 10)
	if err != nil || len(deleted) != 1 || deleted[0].ID != r.ID || len(creating) != 1 || creating[0].ID != stuck.ID {
		t.Errorf("Expired = %v, %v, %v", deleted, creating, err)
	}
	// By the directory's clock, each has expired; an expired creation is neither resumed nor made
	// active, and its name is not free until it is removed.
	for id, want := range map[int64]store.Expiry{r.ID: store.ExpiredDeleted, stuck.ID: store.ExpiredCreating, other.ID: store.NotExpired, 1 << 40: store.NotExpired} {
		if e, err := s.ExpiryOf(ctx, id); err != nil || e != want {
			t.Errorf("ExpiryOf(%d) = %v, %v; want %v", id, e, err, want)
		}
	}
	if _, err := s.ReserveRepository(ctx, store.NewRepository{Owner: bea, Name: "stuck", Propose: tenancy.Read, CreatedBy: bea}); !errors.Is(err, store.ErrCreationExpired) {
		t.Errorf("resuming an expired creation: %v", err)
	}
	if _, err := s.ActivateRepository(ctx, stuck.ID); !errors.Is(err, store.ErrCreationExpired) {
		t.Errorf("activating an expired creation: %v", err)
	}
	if e, err := s.RemoveExpired(ctx, other.ID); err != nil || e != store.NotExpired {
		t.Errorf("removing a repository that has not expired = %v, %v", e, err)
	}
	if _, err := s.Repository(ctx, other.ID); err != nil {
		t.Errorf("a repository that has not expired, after RemoveExpired: %v", err)
	}
	if e, err := s.RemoveExpired(ctx, stuck.ID); err != nil || e != store.ExpiredCreating {
		t.Errorf("removing an expired creation = %v, %v", e, err)
	}
	if _, err := s.ReserveRepository(ctx, store.NewRepository{Owner: bea, Name: "stuck", Propose: tenancy.Read, CreatedBy: bea}); err != nil {
		t.Errorf("the name of an expired creation, once removed: %v", err)
	}
	if e, err := s.RemoveExpired(ctx, r.ID); err != nil || e != store.ExpiredDeleted {
		t.Fatalf("removing an expired deletion = %v, %v", e, err)
	}
	if e, err := s.RemoveExpired(ctx, r.ID); err != nil || e != store.NotExpired {
		t.Errorf("removing it again = %v, %v", e, err)
	}
	if _, err := s.Repository(ctx, r.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a purged repository: %v", err)
	}
	if _, _, err := s.ResolveRepository(ctx, acme.ID, "service"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a purged repository's redirect: %v", err)
	}

	// A team deleted: its name retired, its grants gone; a member removed leaves its teams.
	if err := s.SetGrant(ctx, store.Grant{Repository: other.ID, Subject: web.Name, Role: tenancy.Write}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RemoveMember(ctx, acme.ID, ada); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Team(ctx, web.Name); len(got.Members) != 0 {
		t.Errorf("a removed member's team = %v", got.Members)
	}
	if err := s.DeleteTeam(ctx, web.Name); err != nil {
		t.Fatal(err)
	}
	if gs, _ := s.Grants(ctx, other.ID); len(gs) != 0 {
		t.Errorf("a deleted team's grants = %v", gs)
	}
	if _, err := s.CreateTeam(ctx, acme.ID, "web", ""); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a deleted team's name given again: %v", err)
	}
	if teams, err := s.AllTeams(ctx); err != nil || len(teams) != 1 || teams[0].Name != eng.Name {
		t.Errorf("AllTeams = %v, %v", teams, err)
	}
}
