package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// Deleting accounts and organizations: an organization's only owner is refused, and a person the
// system of record made; deleted, an account is suspended and its repositories deleted; restored,
// both come back; deleted again and past the retention period, housekeeping finishes it — its
// handle retired, its memberships and tokens gone. An organization deleted likewise, then for
// good: its teams, robots, and memberships gone, its handle never a person's.
func testDeletions(t *testing.T, s *store.Store) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0)
	s.SetClock(func() time.Time { return now })
	sync(t, s, &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "bea", true)}}})
	bea, _ := s.Principal(ctx, "bea.human.example.com")
	ada, err := s.CreateAccount(ctx, "ada", "ada@example.com", "github", "gh-ada")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAccount(ctx, bea.ID, bea.ID); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("deleting a person the system of record made: %v", err)
	}
	acme, err := s.CreateOrganization(ctx, "acme", ada.ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAccount(ctx, ada.ID, ada.ID); !errors.Is(err, store.ErrLastOwner) {
		t.Errorf("deleting an organization's only owner: %v", err)
	}
	if err := s.SetMember(ctx, acme.ID, bea.ID, tenancy.Owner); err != nil {
		t.Fatal(err)
	}
	mine, err := s.ReserveRepository(ctx, store.NewRepository{Owner: ada.ID, Name: "notes", Propose: tenancy.Read, CreatedBy: ada.ID})
	if err != nil {
		t.Fatal(err)
	}
	if mine, err = s.ActivateRepository(ctx, mine.ID); err != nil {
		t.Fatal(err)
	}
	secret, _, err := s.CreateToken(ctx, store.NewToken{Principal: ada.ID, Label: "ci", Role: tenancy.Read, Lifetime: time.Hour * 24 * 60})
	if err != nil {
		t.Fatal(err)
	}

	// Deleted, restored, deleted again.
	if err := s.DeleteAccount(ctx, ada.ID, ada.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAccount(ctx, ada.ID, ada.ID); !errors.Is(err, store.ErrDeleting) {
		t.Errorf("deleting twice: %v", err)
	}
	if p, _ := s.PrincipalByID(ctx, ada.ID); p.Status != directory.Suspended {
		t.Errorf("a deleted account = %+v", p)
	}
	if r, _ := s.Repository(ctx, mine.ID); r.State != store.Deleted {
		t.Errorf("a deleted account's repository = %+v", r)
	}
	now = now.Add(time.Hour)
	if err := s.RestoreAccount(ctx, ada.ID); err != nil {
		t.Fatal(err)
	}
	if p, _ := s.PrincipalByID(ctx, ada.ID); !p.Active() {
		t.Errorf("a restored account = %+v", p)
	}
	if r, _ := s.Repository(ctx, mine.ID); r.State != store.Active {
		t.Errorf("a restored account's repository = %+v", r)
	}
	if err := s.DeleteAccount(ctx, ada.ID, ada.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := s.FinishDeletions(ctx, 10); err != nil || n != 0 {
		t.Errorf("finishing within the retention period = %d, %v", n, err)
	}
	now = now.Add(store.RetentionPeriod + time.Hour)
	if err := s.RestoreAccount(ctx, ada.ID); err == nil {
		t.Error("restoring past the retention period")
	}
	if n, err := s.FinishDeletions(ctx, 10); err != nil || n != 1 {
		t.Fatalf("FinishDeletions = %d, %v", n, err)
	}
	if p, _ := s.PrincipalByID(ctx, ada.ID); p.Status != directory.Deleted {
		t.Errorf("a finished account = %+v", p)
	}
	if err := s.HandleAvailable(ctx, "ada"); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a finished account's handle: %v", err)
	}
	if ms, _ := s.MembershipsOf(ctx, ada.ID); len(ms) != 0 {
		t.Errorf("a finished account's memberships: %v", ms)
	}
	if _, _, err := s.TokenFor(ctx, secret); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a finished account's token: %v", err)
	}
	if _, err := s.LinkedAccount(ctx, "github", "gh-ada"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a finished account's sign-in: %v", err)
	}

	// An organization: deleted, restored, deleted, finished.
	eng, err := s.CreateTeam(ctx, acme.ID, "eng", "")
	if err != nil {
		t.Fatal(err)
	}
	web, err := s.CreateTeam(ctx, acme.ID, "web", eng.Name)
	if err != nil {
		t.Fatal(err)
	}
	robot, err := s.CreateOrganizationRobot(ctx, acme.ID, "ci")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: "app", Propose: tenancy.Read, CreatedBy: bea.ID})
	if err != nil {
		t.Fatal(err)
	}
	if app, err = s.ActivateRepository(ctx, app.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteOrganization(ctx, acme.ID, bea.ID); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Repository(ctx, app.ID); r.State != store.Deleted {
		t.Errorf("a deleted organization's repository = %+v", r)
	}
	if err := s.RestoreOrganization(ctx, acme.ID); err != nil {
		t.Fatal(err)
	}
	if o, _ := s.Organization(ctx, acme.ID); !o.Active() {
		t.Errorf("a restored organization = %+v", o)
	}
	if err := s.DeleteOrganization(ctx, acme.ID, bea.ID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.RetentionPeriod + time.Hour)
	if n, err := s.FinishDeletions(ctx, 10); err != nil || n != 1 {
		t.Fatalf("FinishDeletions = %d, %v", n, err)
	}
	if o, _ := s.Organization(ctx, acme.ID); o.Status != directory.Deleted {
		t.Errorf("a finished organization = %+v", o)
	}
	for _, team := range []string{eng.Name, web.Name} {
		if _, err := s.Team(ctx, team); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("a finished organization's team %s: %v", team, err)
		}
	}
	if p, _ := s.PrincipalByID(ctx, robot.ID); p.Status != directory.Deleted {
		t.Errorf("a finished organization's robot = %+v", p)
	}
	if ms, _ := s.Members(ctx, acme.ID); len(ms) != 0 {
		t.Errorf("a finished organization's members: %v", ms)
	}
	if err := s.HandleAvailable(ctx, "acme"); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a finished organization's handle: %v", err)
	}
}
