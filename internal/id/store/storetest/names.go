package storetest

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// lastEvent is the last event of a repository's log.
func lastEvent(t *testing.T, s *store.Store, repo int64) *identitypb.DirectoryEvent {
	t.Helper()
	rows, err := s.DirectoryEvents(context.Background(), repo, 0)
	if err != nil || len(rows) == 0 {
		t.Fatalf("repository %d's log = %v, %v", repo, rows, err)
	}
	ev := &identitypb.DirectoryEvent{}
	if err := proto.Unmarshal(rows[len(rows)-1].Body, ev); err != nil {
		t.Fatal(err)
	}
	return ev
}

// Full names — the installation's organization's repositories by their names alone, others by
// owner and name — in entries and events; an owner's rename renaming each of its repositories; and
// a creation a stopped replica left half done, resumed by its maker alone.
func testNames(t *testing.T, s *store.Store) {
	ctx := context.Background()
	src := &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true), person("2", "bea", true)}}}
	sync(t, s, src)
	ada, _ := s.Principal(ctx, "ada.human.example.com")
	bea, _ := s.Principal(ctx, "bea.human.example.com")
	inst, err := s.EnsureOrganization(ctx, "example", store.OrganizationSettings{BaseRole: tenancy.Write, MembersCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	s.SetInstallation(inst.ID)

	made := func(owner, name string) store.Repository {
		t.Helper()
		r, err := s.ReserveRepository(ctx, store.NewRepository{Owner: owner, Name: name, Propose: tenancy.Read, CreatedBy: ada.ID})
		if err != nil {
			t.Fatal(err)
		}
		if r, err = s.ActivateRepository(ctx, r.ID); err != nil {
			t.Fatal(err)
		}
		return r
	}
	app := made(inst.ID, "app")
	notes := made(bea.ID, "notes")
	for r, want := range map[int64]string{app.ID: "app", notes.ID: "bea/notes"} {
		if e := lastEvent(t, s, r).GetRepository(); e.GetFullName() != want {
			t.Errorf("repository %d's full name = %q, want %q", r, e.GetFullName(), want)
		}
	}
	if e, err := s.Entry(ctx, notes); err != nil || e.GetFullName() != "bea/notes" || e.GetOwner() != "bea" {
		t.Errorf("Entry = %v, %v", e, err)
	}

	// A creation a stopped replica left half done resumes: its maker, asking again, gets the entry
	// still being made; anyone else, or anyone once it is made, finds the name taken.
	half, err := s.ReserveRepository(ctx, store.NewRepository{Owner: inst.ID, Name: "half", Propose: tenancy.Read, CreatedBy: ada.ID})
	if err != nil {
		t.Fatal(err)
	}
	if again, err := s.ReserveRepository(ctx, store.NewRepository{Owner: inst.ID, Name: "half", Propose: tenancy.Read, CreatedBy: ada.ID}); err != nil || again.ID != half.ID {
		t.Errorf("its maker asking again = %+v, %v; want entry %d", again, err, half.ID)
	}
	for who, n := range map[string]store.NewRepository{
		"another person":    {Owner: inst.ID, Name: "half", Propose: tenancy.Read, CreatedBy: bea.ID},
		"as a fork instead": {Owner: inst.ID, Name: "half", Propose: tenancy.Read, CreatedBy: ada.ID, Parent: app.ID},
	} {
		if _, err := s.ReserveRepository(ctx, n); !errors.Is(err, store.ErrTaken) {
			t.Errorf("%s asking for a name being made: %v", who, err)
		}
	}
	if _, err := s.ActivateRepository(ctx, half.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveRepository(ctx, store.NewRepository{Owner: inst.ID, Name: "half", Propose: tenancy.Read, CreatedBy: ada.ID}); !errors.Is(err, store.ErrTaken) {
		t.Errorf("its maker asking once it is made: %v", err)
	}

	// bea is renamed bee: her repository is renamed with her.
	src.snap.People[1] = person("2", "bee", true)
	sync(t, s, src)
	ev := lastEvent(t, s, notes.ID)
	if ev.GetKind() != identitypb.DirectoryEvent_KIND_RENAMED || ev.GetOldFullName() != "bea/notes" || ev.GetRepository().GetFullName() != "bee/notes" ||
		ev.GetOldOwnerId() != bea.ID || ev.GetOldName() != "notes" {
		t.Errorf("an owner's rename = %v", ev)
	}
}
