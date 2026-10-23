package storetest

import (
	"context"
	"reflect"
	gosync "sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// stream is a bus stream that keeps what is published.
type stream struct {
	mu        gosync.Mutex
	published []bus.Event
}

func (s *stream) Publish(_ context.Context, es ...bus.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published = append(s.published, es...)
	return nil
}
func (*stream) CreateGroup(context.Context, string, string) error { return nil }
func (*stream) Subscribe(ctx context.Context, _, _ string, _ bus.Handler) error {
	<-ctx.Done()
	return ctx.Err()
}

func (s *stream) events() []bus.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bus.Event(nil), s.published...)
}

// The directory's log: each change to an entry that stands is an event in its repository's log, in
// order, carrying the entry after it; an entry being made is in none; the relay publishes each
// once.
func testDirectoryLog(t *testing.T, s *store.Store) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0)
	s.SetClock(func() time.Time { return now })
	sync(t, s, &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true), person("2", "bea", true)}}})
	ada, _ := s.Principal(ctx, "ada.human.example.com")
	bea, _ := s.Principal(ctx, "bea.human.example.com")
	acme, err := s.CreateOrganization(ctx, "acme", ada.ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}
	team, err := s.CreateTeam(ctx, acme.ID, "eng", "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: "app", Propose: tenancy.Read, CreatedBy: ada.ID})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetGrant(ctx, store.Grant{Repository: r.ID, Subject: ada.ID, Role: tenancy.Admin}); err != nil {
		t.Fatal(err)
	}
	if evs, err := s.DirectoryEvents(ctx, r.ID, 0); err != nil || len(evs) != 0 {
		t.Errorf("an entry being made logged %v, %v", evs, err)
	}
	steps := []func() error{
		func() error { _, err := s.ActivateRepository(ctx, r.ID); return err },
		func() error {
			return s.SetGrant(ctx, store.Grant{Repository: r.ID, Subject: team.Name, Role: tenancy.Write})
		},
		func() error { _, err := s.RenameRepository(ctx, r.ID, "service"); return err },
		func() error { _, err := s.SetVisibility(ctx, r.ID, tenancy.Public); return err },
		func() error { _, err := s.SetVisibility(ctx, r.ID, tenancy.Public); return err }, // no change: no event
		func() error { _, err := s.TransferRepository(ctx, r.ID, bea.ID); return err },
		func() error { _, err := s.RemoveGrant(ctx, r.ID, ada.ID); return err },
		func() error { _, err := s.DeleteRepository(ctx, r.ID); return err },
		func() error { _, err := s.RestoreRepository(ctx, r.ID); return err },
		func() error { _, err := s.DeleteRepository(ctx, r.ID); return err },
		func() error { now = now.Add(store.RetentionPeriod + time.Hour); return s.PurgeRepository(ctx, r.ID) },
	}
	for i, step := range steps {
		if err := step(); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	rows, err := s.DirectoryEvents(ctx, r.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	type want struct {
		kind              identitypb.DirectoryEvent_Kind
		owner, name       string
		state, visibility string
		oldOwner, oldName string
	}
	wants := []want{
		{identitypb.DirectoryEvent_KIND_CREATED, acme.ID, "app", "active", "private", "", ""},
		{identitypb.DirectoryEvent_KIND_GRANTS, acme.ID, "app", "active", "private", "", ""},
		{identitypb.DirectoryEvent_KIND_RENAMED, acme.ID, "service", "active", "private", acme.ID, "app"},
		{identitypb.DirectoryEvent_KIND_SETTINGS, acme.ID, "service", "active", "public", "", ""},
		{identitypb.DirectoryEvent_KIND_TRANSFERRED, bea.ID, "service", "active", "public", acme.ID, "service"},
		{identitypb.DirectoryEvent_KIND_GRANTS, bea.ID, "service", "active", "public", "", ""},
		{identitypb.DirectoryEvent_KIND_DELETED, bea.ID, "service", "deleted", "public", "", ""},
		{identitypb.DirectoryEvent_KIND_RESTORED, bea.ID, "service", "active", "public", "", ""},
		{identitypb.DirectoryEvent_KIND_DELETED, bea.ID, "service", "deleted", "public", "", ""},
		{identitypb.DirectoryEvent_KIND_PURGED, "", "", "", "", "", ""},
	}
	if len(rows) != len(wants) {
		t.Fatalf("logged %d events, want %d", len(rows), len(wants))
	}
	for i, row := range rows {
		ev := &identitypb.DirectoryEvent{}
		if err := proto.Unmarshal(row.Body, ev); err != nil {
			t.Fatal(err)
		}
		e := ev.GetRepository()
		got := want{ev.GetKind(), e.GetOwnerId(), e.GetName(), e.GetState(), e.GetVisibility(), ev.GetOldOwnerId(), ev.GetOldName()}
		if row.Seq != int64(i+1) || ev.GetSeq() != row.Seq || ev.GetRepositoryId() != r.ID || got != wants[i] || (i < len(wants)-1 && e.GetId() != r.ID) {
			t.Errorf("event %d (seq %d) = %+v, want %+v", i, row.Seq, got, wants[i])
		}
	}
	if page, err := s.DirectoryEvents(ctx, r.ID, 7); err != nil || len(page) != 3 || page[0].Seq != 8 {
		t.Errorf("the events after 7 = %v, %v", page, err)
	}

	// Another repository, a team's deletion logging the grants it took from it, and the heads.
	other, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: "lib", Propose: tenancy.Read, CreatedBy: ada.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ActivateRepository(ctx, other.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.SetGrant(ctx, store.Grant{Repository: other.ID, Subject: team.Name, Role: tenancy.Read}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteTeam(ctx, team.Name); err != nil {
		t.Fatal(err)
	}
	heads, err := s.DirectoryHeads(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int64]int64{}
	for _, h := range heads {
		got[h.Repository] = h.Seq
	}
	if want := map[int64]int64{r.ID: 10, other.ID: 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("heads = %v, want %v", got, want)
	}

	// The relay publishes each event once, on the directory's topic.
	st := &stream{}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- s.Relay(rctx, st) }()
	deadline := time.Now().Add(10 * time.Second)
	for len(st.events()) < 13 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := s.SetVisibility(ctx, other.ID, tenancy.Internal); err != nil {
		t.Fatal(err)
	}
	for len(st.events()) < 14 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done
	pub := st.events()
	if len(pub) != 14 {
		t.Fatalf("published %d events, want 14", len(pub))
	}
	for _, e := range pub {
		ev, err := dirfeed.Decode(e)
		if err != nil || e.Topic != dirfeed.Topic {
			t.Errorf("published %v: %v", e, err)
		}
		if e.Key == other.ID && e.Seq == 4 && ev.GetRepository().GetVisibility() != "internal" {
			t.Errorf("the last event = %v", ev)
		}
	}
}
