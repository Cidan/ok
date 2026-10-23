package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	"google.golang.org/protobuf/proto"
)

// GitHub's rules for forks: a fork's visibility is its upstream's, and does not change on its own;
// an internal repository is forked once; an upstream's visibility changing leaves each fork the
// root of a tree of its own; a private upstream deleted deletes its forks and theirs; a public one
// deleted makes its oldest active fork the others' upstream; each change in its fork's log.
func testForkRules(t *testing.T, s *store.Store) {
	ctx := context.Background()
	sync(t, s, &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true), person("2", "bea", true)}}})
	ada, _ := s.Principal(ctx, "ada.human.example.com")
	bea, _ := s.Principal(ctx, "bea.human.example.com")
	acme, err := s.CreateOrganization(ctx, "acme", ada.ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}
	made := func(owner, name string, v tenancy.Visibility, parent int64) store.Repository {
		t.Helper()
		r, err := s.ReserveRepository(ctx, store.NewRepository{Owner: owner, Name: name, Visibility: v, Propose: tenancy.Read, Parent: parent, CreatedBy: ada.ID})
		if err != nil {
			t.Fatal(err)
		}
		if r, err = s.ActivateRepository(ctx, r.ID); err != nil {
			t.Fatal(err)
		}
		return r
	}
	lastKind := func(id int64) identitypb.DirectoryEvent_Kind {
		t.Helper()
		evs, err := s.DirectoryEvents(ctx, id, 0)
		if err != nil || len(evs) == 0 {
			t.Fatalf("repository %d's log: %v, %v", id, evs, err)
		}
		ev := &identitypb.DirectoryEvent{}
		if err := proto.Unmarshal(evs[len(evs)-1].Body, ev); err != nil {
			t.Fatal(err)
		}
		return ev.GetKind()
	}

	// Visibility: the upstream's, whatever is asked; not changed on a fork's own.
	pub := made(acme.ID, "pub", tenancy.Public, 0)
	f1 := made(ada.ID, "pub", tenancy.Private, pub.ID)
	if f1.Visibility != tenancy.Public || f1.Parent != pub.ID {
		t.Errorf("a fork of a public repository = %+v", f1)
	}
	if _, err := s.SetVisibility(ctx, f1.ID, tenancy.Private); !errors.Is(err, store.ErrRepositoryState) {
		t.Errorf("a fork's visibility changed on its own: %v", err)
	}
	internal := made(acme.ID, "inner", tenancy.Internal, 0)
	inFork := made(ada.ID, "inner", tenancy.Private, internal.ID)
	if inFork.Visibility != tenancy.Internal {
		t.Errorf("a fork of an internal repository = %+v", inFork)
	}
	if _, err := s.ReserveRepository(ctx, store.NewRepository{Owner: bea.ID, Name: "inner", Propose: tenancy.Read, Parent: inFork.ID, CreatedBy: bea.ID}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("forking a fork of an internal repository: %v", err)
	}
	if forks, err := s.PersonalForksIn(ctx, []int64{pub.Network, internal.Network}); err != nil || len(forks) != 2 {
		t.Errorf("PersonalForksIn = %v, %v", forks, err)
	}

	// The upstream made private: its fork stays public, standing alone.
	f2 := made(bea.ID, "pub", tenancy.Public, pub.ID)
	if _, err := s.SetVisibility(ctx, pub.ID, tenancy.Private); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{f1.ID, f2.ID} {
		r, err := s.Repository(ctx, id)
		if err != nil || r.Parent != 0 || r.Visibility != tenancy.Public || lastKind(id) != identitypb.DirectoryEvent_KIND_UPSTREAM {
			t.Errorf("a fork after its upstream was made private = %+v, %v", r, err)
		}
	}

	// A private upstream deleted: its forks, and theirs, with it.
	priv := made(acme.ID, "priv", tenancy.Private, 0)
	pf := made(ada.ID, "priv", tenancy.Private, priv.ID)
	pff := made(acme.ID, "priv-of-fork", tenancy.Private, pf.ID)
	if _, err := s.DeleteRepository(ctx, priv.ID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{pf.ID, pff.ID} {
		if r, _ := s.Repository(ctx, id); r.State != store.Deleted || lastKind(id) != identitypb.DirectoryEvent_KIND_DELETED {
			t.Errorf("a fork of a deleted private repository = %+v", r)
		}
	}

	// A public upstream deleted: its oldest active fork takes its place, the others' upstream.
	open := made(acme.ID, "open", tenancy.Public, 0)
	oldest := made(ada.ID, "open", tenancy.Public, open.ID)
	gone := made(bea.ID, "open-gone", tenancy.Public, open.ID)
	if _, err := s.DeleteRepository(ctx, gone.ID); err != nil {
		t.Fatal(err)
	}
	other := made(bea.ID, "open", tenancy.Public, open.ID)
	if _, err := s.DeleteRepository(ctx, open.ID); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Repository(ctx, oldest.ID); r.Parent != 0 || r.State != store.Active || lastKind(oldest.ID) != identitypb.DirectoryEvent_KIND_UPSTREAM {
		t.Errorf("the oldest fork of a deleted public repository = %+v", r)
	}
	if r, _ := s.Repository(ctx, other.ID); r.Parent != oldest.ID || r.State != store.Active {
		t.Errorf("another fork of a deleted public repository = %+v", r)
	}
	if r, _ := s.Repository(ctx, gone.ID); r.Parent != open.ID {
		t.Errorf("a deleted fork moved to the new upstream = %+v", r)
	}
}
