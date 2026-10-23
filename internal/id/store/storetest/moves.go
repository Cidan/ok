package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// A network's move between cells, as the directory records it: asked of a repository, for its
// whole network, refused while one runs, into a cell it is in, an unknown one, or one closed;
// stepped, failed and tried again later, canceled before it names the destination; named in every
// entry of the network in one transaction, each logged; and asked again once finished.
func testNetworkMoves(t *testing.T, s *store.Store) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0).UTC()
	s.SetClock(func() time.Time { return now })
	sync(t, s, &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true)}}})
	ada, err := s.Principal(ctx, "ada.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	s.SetCells([]store.Cell{{Name: "c1", Open: true}, {Name: "c2", Open: true}, {Name: "shut"}}, "")
	reserve := func(name string, parent int64) store.Repository {
		t.Helper()
		r, err := s.ReserveRepository(ctx, store.NewRepository{Owner: ada.ID, Name: name, Visibility: tenancy.Private, Propose: tenancy.Read,
			Parent: parent, Cell: map[bool]string{true: "c1"}[parent == 0], CreatedBy: ada.ID})
		if err != nil {
			t.Fatal(err)
		}
		if r, err = s.ActivateRepository(ctx, r.ID); err != nil {
			t.Fatal(err)
		}
		return r
	}
	app := reserve("app", 0)
	fork := reserve("fork", app.ID)
	if repos, err := s.NetworkRepositories(ctx, app.Network); err != nil || len(repos) != 2 {
		t.Fatalf("the network's repositories: %+v, %v", repos, err)
	}
	for cell, want := range map[string]error{"c1": store.ErrInvalid, "nowhere": store.ErrInvalid, "shut": store.ErrInvalid} {
		if _, err := s.AskMove(ctx, fork.ID, cell, ada.ID); !errors.Is(err, want) {
			t.Errorf("a move to %s: %v, want %v", cell, err, want)
		}
	}
	m, err := s.AskMove(ctx, fork.ID, "c2", ada.ID)
	if err != nil || m.Network != app.Network || m.Source != "c1" || m.Destination != "c2" || m.Phase != store.MoveCopying {
		t.Fatalf("asking the move: %+v, %v", m, err)
	}
	if _, err := s.AskMove(ctx, app.ID, "c2", ada.ID); !errors.Is(err, store.ErrMoving) {
		t.Errorf("asking another while it runs: %v", err)
	}
	if due, err := s.DueMoves(ctx, 10); err != nil || len(due) != 1 || due[0] != app.Network {
		t.Errorf("the moves due: %v, %v", due, err)
	}
	// A step fails: tried again later.
	if _, err := s.UpdateMove(ctx, app.Network, func(m *store.Move) error {
		m.Error, m.NextAt = "cell c2's scm: down", now.Add(time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if due, err := s.DueMoves(ctx, 10); err != nil || len(due) != 0 {
		t.Errorf("the moves due after a failed step: %v, %v", due, err)
	}
	if got, err := s.GetMove(ctx, app.Network); err != nil || got.Error != "cell c2's scm: down" || !got.NextAt.Equal(now.Add(time.Minute)) {
		t.Errorf("the move after a failed step: %+v, %v", got, err)
	}
	// Frozen, then imported; the destination named in both entries, each logged.
	if _, err := s.UpdateMove(ctx, app.Network, func(m *store.Move) error {
		m.Phase, m.FrozenAt, m.Error, m.NextAt = store.MoveImported, now, "", now
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.NameDestination(ctx, app.Network); err != nil {
		t.Fatal(err)
	}
	for _, r := range []store.Repository{app, fork} {
		e, err := s.Repository(ctx, r.ID)
		if err != nil || e.Cell != "c2" {
			t.Errorf("repository %d after naming: %+v, %v", r.ID, e, err)
		}
		evs, err := s.DirectoryEvents(ctx, r.ID, 0)
		if err != nil || len(evs) == 0 {
			t.Fatalf("repository %d's events: %v, %v", r.ID, evs, err)
		}
		last := &identitypb.DirectoryEvent{}
		if err := proto.Unmarshal(evs[len(evs)-1].Body, last); err != nil || last.GetKind() != identitypb.DirectoryEvent_KIND_MOVED ||
			last.GetRepository().GetCell() != "c2" {
			t.Errorf("repository %d's last event: %v, %v", r.ID, last, err)
		}
	}
	got, err := s.GetMove(ctx, app.Network)
	if err != nil || got.Phase != store.MoveNamed || !got.FrozenAt.Equal(now) {
		t.Fatalf("the move once named: %+v, %v", got, err)
	}
	if _, err := s.CancelMove(ctx, app.Network); !errors.Is(err, store.ErrNamed) {
		t.Errorf("canceling a named move: %v", err)
	}
	if err := s.NameDestination(ctx, app.Network); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("naming it twice: %v", err)
	}
	if _, err := s.UpdateMove(ctx, app.Network, func(m *store.Move) error {
		m.Phase = store.MoveDone
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Asked again, back, once finished; canceled before it is named.
	now = now.Add(time.Hour)
	back, err := s.AskMove(ctx, app.ID, "c1", ada.ID)
	if err != nil || back.Source != "c2" || back.Phase != store.MoveCopying || !back.FrozenAt.IsZero() {
		t.Fatalf("asking it back: %+v, %v", back, err)
	}
	if canceled, err := s.CancelMove(ctx, app.Network); err != nil || canceled.Phase != store.MoveAbandoning {
		t.Errorf("canceling it: %+v, %v", canceled, err)
	}
	if ms, err := s.ListMoves(ctx, 10); err != nil || len(ms) != 1 || ms[0].Destination != "c1" {
		t.Errorf("the moves: %+v, %v", ms, err)
	}
}

// An owner's region, which its new repositories are placed in: one a cell is in, or none, the
// directory's default then.
func testOwnerRegions(t *testing.T, s *store.Store) {
	ctx := context.Background()
	sync(t, s, &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true)}}})
	ada, err := s.Principal(ctx, "ada.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	s.SetCells([]store.Cell{{Name: "us-1", Region: "us", Open: true}, {Name: "eu-1", Region: "eu", Open: true}}, "us")
	if got, err := s.PlacedIn(ctx, ada.ID); err != nil || got != "us" {
		t.Errorf("placed in before one is set: %q, %v", got, err)
	}
	if err := s.SetOwnerRegion(ctx, ada.ID, "mars"); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a region no cell is in: %v", err)
	}
	for _, region := range []string{"eu", "us"} {
		if err := s.SetOwnerRegion(ctx, ada.ID, region); err != nil {
			t.Fatal(err)
		}
		if got, err := s.OwnerRegion(ctx, ada.ID); err != nil || got != region {
			t.Errorf("the region once set to %s: %q, %v", region, got, err)
		}
	}
	if err := s.SetOwnerRegion(ctx, ada.ID, ""); err != nil {
		t.Fatal(err)
	}
	if got, err := s.OwnerRegion(ctx, ada.ID); err != nil || got != "" {
		t.Errorf("the region once forgotten: %q, %v", got, err)
	}
}
