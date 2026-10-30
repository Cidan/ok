package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// An enterprise organization's identity provider: none for the installation's organization; its
// sync names the organization's people <handle>-<org>, members of it, and its groups as its teams;
// a later sync renames, suspends, and deletes them, a renamed team's grants following it and a
// deleted one's going; a name another holds waits; what it provisions over SCIM is its own.
func testEnterprise(t *testing.T, s *store.Store) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0)
	s.SetClock(func() time.Time { return now })
	inst := &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "bea", true), person("2", "ada-acme", true)}}}
	sync(t, s, inst)
	bea, _ := s.Principal(ctx, "bea.human.example.com")
	installation, err := s.EnsureOrganization(ctx, "example", store.OrganizationSettings{BaseRole: tenancy.Write, MembersCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	s.SetInstallation(installation.ID)
	acme, err := s.CreateOrganization(ctx, "acme", bea.ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}
	settings := json.RawMessage(`{"scim":{}}`)
	if err := s.SetIdentityProvider(ctx, store.IdentityProvider{Organization: installation.ID, Source: "scim", Settings: settings}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("the installation's organization's provider: %v", err)
	}
	if err := s.SetIdentityProvider(ctx, store.IdentityProvider{Organization: acme.ID, Source: "okta", Settings: settings}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a provider of no kind ok knows: %v", err)
	}
	if err := s.SetIdentityProvider(ctx, store.IdentityProvider{Organization: acme.ID, Source: "scim", Settings: settings, Sealed: []byte("sealed"), Interval: time.Second}); err != nil {
		t.Fatal(err)
	}
	p, err := s.IdentityProviderOf(ctx, acme.ID)
	if err != nil || p.Interval != store.MinSyncInterval || string(p.Sealed) != "sealed" || p.Source != "scim" {
		t.Errorf("IdentityProviderOf = %+v, %v", p, err)
	}
	// Its sync is keyed work under a key of its own, booked as it is set; set again, it keeps the
	// key, and booking a release before's providers keys none twice.
	if org, err := s.IdentityProviderOrg(ctx, p.Key); err != nil || org != acme.ID || p.Key == 0 {
		t.Errorf("the provider's sync key %d names %q, %v", p.Key, org, err)
	}
	if peek, err := s.Work().Peek(ctx, store.SyncWork, p.Key); err != nil || !peek.Marked {
		t.Errorf("the provider's sync, as it is set: %+v, %v; want it booked", peek, err)
	}
	if err := s.SetIdentityProvider(ctx, store.IdentityProvider{Organization: acme.ID, Source: "scim", Settings: settings, Sealed: []byte("sealed"), Interval: time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := s.KeyIdentityProviders(ctx); err != nil {
		t.Fatal(err)
	}
	if again, err := s.IdentityProviderOf(ctx, acme.ID); err != nil || again.Key != p.Key {
		t.Errorf("the provider's key after it was set again, and providers booked: %d, %v; want %d", again.Key, err, p.Key)
	}
	if _, err := s.IdentityProviderOrg(ctx, p.Key+1); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a key no provider holds: %v", err)
	}
	if due, err := s.DueIdentityProviders(ctx); err != nil || !slices.Contains(due, acme.ID) {
		t.Errorf("a provider never synced is due: %v, %v", due, err)
	}

	orgSync := func(src *source) *directory.Plan {
		t.Helper()
		sc := s.OrganizationSync(acme.ID)
		opts, err := sc.Options(ctx)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := directory.Sync(ctx, names, src, sc, opts)
		if err != nil {
			t.Fatal(err)
		}
		return plan
	}
	idp := &source{snap: &directory.Snapshot{
		People: []directory.Person{person("u1", "cal", true), person("u2", "dee", true), person("u3", "ada", true)},
		Groups: []directory.Group{{ExternalID: "g1", Name: "eng", Members: []directory.Member{{ExternalID: "u1"}, {ExternalID: "u2"}}},
			{ExternalID: "g2", Name: "ops", Members: []directory.Member{{ExternalID: "u2"}}},
			{ExternalID: "g3", Name: "all", Members: []directory.Member{{ExternalID: "g1", Group: true}, {ExternalID: "u2"}}}},
	}}
	plan := orgSync(idp)
	cal, err := s.OrganizationPerson(ctx, acme.ID, "u1")
	if err != nil || cal.Name != "cal-acme.human.example.com" || !cal.Active() {
		t.Fatalf("a provisioned person = %+v, %v", cal, err)
	}
	if ms, _ := s.MembershipsOf(ctx, cal.ID); ms[acme.ID] != tenancy.Member {
		t.Errorf("a provisioned person's membership: %v", ms)
	}
	if _, err := s.OrganizationPerson(ctx, acme.ID, "u3"); !errors.Is(err, store.ErrNotFound) || len(plan.Waiting) != 1 {
		t.Errorf("a person whose name another holds (ada-acme): %v, waiting %v", err, plan.Waiting)
	}
	eng, err := s.Team(ctx, "eng.acme.team.example.com")
	if err != nil || eng.ExternalID != "g1" || len(eng.Members) != 2 {
		t.Fatalf("a synced team = %+v, %v", eng, err)
	}
	if org, ext, err := s.PersonOrganization(ctx, cal.ID); err != nil || org != acme.ID || ext != "u1" {
		t.Errorf("PersonOrganization = %q, %q, %v", org, ext, err)
	}
	if due, _ := s.DueIdentityProviders(ctx); slices.Contains(due, acme.ID) {
		t.Error("a provider just synced is due")
	}
	// A group of groups is a team of the people they come to; a sync with nothing new changes
	// nothing; the provider alone changes a synced team.
	if all, err := s.Team(ctx, "all.acme.team.example.com"); err != nil || len(all.Members) != 2 {
		t.Errorf("a group of groups' team = %+v, %v", all, err)
	}
	if again := orgSync(idp); !again.Empty() {
		t.Errorf("a sync with nothing new: %+v", again)
	}
	if err := s.AddTeamMember(ctx, "eng.acme.team.example.com", bea.ID); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("an owner adding to a synced team: %v", err)
	}
	if err := s.DeleteTeam(ctx, "eng.acme.team.example.com"); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("an owner deleting a synced team: %v", err)
	}

	// Renamed, suspended, deleted; a team renamed — its grant following — and one deleted.
	repo, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: "app", Propose: tenancy.Read, CreatedBy: bea.ID})
	if err != nil {
		t.Fatal(err)
	}
	for _, team := range []string{"eng.acme.team.example.com", "ops.acme.team.example.com"} {
		if err := s.SetGrant(ctx, store.Grant{Repository: repo.ID, Subject: team, Role: tenancy.Write}); err != nil {
			t.Fatal(err)
		}
	}
	idp.snap.People = []directory.Person{person("u1", "calvin", true), person("u2", "dee", false)}
	idp.snap.Groups = []directory.Group{{ExternalID: "g1", Name: "engineering", Members: []directory.Member{{ExternalID: "u1"}}}}
	orgSync(idp)
	if p, err := s.OrganizationPerson(ctx, acme.ID, "u1"); err != nil || p.ID != cal.ID || p.Name != "calvin-acme.human.example.com" {
		t.Errorf("a renamed person = %+v, %v", p, err)
	}
	if p, _ := s.OrganizationPerson(ctx, acme.ID, "u2"); p.Status != directory.Suspended {
		t.Errorf("a suspended person = %+v", p)
	}
	if err := s.HandleAvailable(ctx, "cal-acme"); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a renamed person's old handle: %v", err)
	}
	grants, err := s.Grants(ctx, repo.ID)
	if err != nil {
		t.Fatal(err)
	}
	subjects := map[string]bool{}
	for _, g := range grants {
		subjects[g.Subject] = true
	}
	if !subjects["engineering.acme.team.example.com"] || subjects["eng.acme.team.example.com"] || subjects["ops.acme.team.example.com"] {
		t.Errorf("grants after a team's rename and another's deletion: %v", subjects)
	}
	idp.snap.People = []directory.Person{person("u1", "calvin", true)}
	orgSync(idp)
	if p, _ := s.OrganizationPerson(ctx, acme.ID, "u2"); p.Status != directory.Deleted {
		t.Errorf("a deleted person = %+v", p)
	}
	if ms, _ := s.MembershipsOf(ctx, cal.ID); ms[acme.ID] != tenancy.Member {
		t.Errorf("the remaining person's membership: %v", ms)
	}

	// What the provider provisions over SCIM is the organization's alone; its token names it.
	if _, err := s.SCIMOf(acme.ID).CreateUser(ctx, scim.User{ID: "s1", UserName: "erin@acme.com", Active: true}); err != nil {
		t.Fatal(err)
	}
	if us, err := s.SCIM().Users(ctx); err != nil || len(us) != 0 {
		t.Errorf("the installation's SCIM users = %v, %v", us, err)
	}
	if us, err := s.SCIMOf(acme.ID).Users(ctx); err != nil || len(us) != 1 {
		t.Errorf("acme's SCIM users = %v, %v", us, err)
	}
	tok, err := s.SetSCIMToken(ctx, acme.ID)
	if err != nil {
		t.Fatal(err)
	}
	if org, err := s.SCIMTokenOrganization(ctx, tok); err != nil || org != acme.ID {
		t.Errorf("SCIMTokenOrganization = %q, %v", org, err)
	}
	if again, _ := s.SetSCIMToken(ctx, acme.ID); again == tok {
		t.Error("a reset token is the same")
	} else if _, err := s.SCIMTokenOrganization(ctx, tok); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a replaced token: %v", err)
	}
	if ok, err := s.RemoveIdentityProvider(ctx, acme.ID); err != nil || !ok {
		t.Errorf("RemoveIdentityProvider = %v, %v", ok, err)
	}
}
