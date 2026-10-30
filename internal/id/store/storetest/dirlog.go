package storetest

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
	"github.com/Cidan/ok/internal/work"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// relayed are the keys, each through its last seq, that the jobs on queue name, taken as they come
// until want is met or the wait ends.
func relayed(t *testing.T, q bus.Queue, queue string, want map[int64]int64) map[int64]int64 {
	t.Helper()
	got := map[int64]int64{}
	deadline := time.Now().Add(10 * time.Second)
	for !reflect.DeepEqual(got, want) && time.Now().Before(deadline) {
		ls, err := q.Lease(context.Background(), queue, bus.LeaseOptions{Max: 100, Visibility: time.Minute, Wait: 100 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range ls {
			var key, seq int64
			if _, err := fmt.Sscanf(l.Job().ID, "%d/%d", &key, &seq); err != nil {
				t.Fatalf("a relay's job %q: %v", l.Job().ID, err)
			}
			got[key] = max(got[key], seq)
			l.Ack(context.Background())
		}
	}
	return got
}

// The directory's log: each change to an entry that stands is an event in its repository's log, in
// order, carrying the entry after it; an entry being made is in none; the relay publishes each
// once to each follower registered.
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

	// The relay enqueues, on a registered follower's queue, a job naming each repository whose log
	// grew, through its last event; on none for a role that never registered.
	if err := s.AddFollower(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddFollower(ctx, "x"); err != nil {
		t.Fatalf("registering twice: %v", err)
	}
	q := bus.NewLocalQueue()
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- s.Relay(rctx, q) }()
	queue := work.Jobs{Queue: q, Prefix: "x"}.QueueOf("relay." + dirfeed.Topic)
	if got := relayed(t, q, queue, map[int64]int64{r.ID: 10, other.ID: 3}); !reflect.DeepEqual(got, map[int64]int64{r.ID: 10, other.ID: 3}) {
		t.Fatalf("relayed %v, want each repository through its head", got)
	}
	if _, err := s.SetVisibility(ctx, other.ID, tenancy.Internal); err != nil {
		t.Fatal(err)
	}
	if got := relayed(t, q, queue, map[int64]int64{other.ID: 4}); !reflect.DeepEqual(got, map[int64]int64{other.ID: 4}) {
		t.Fatalf("relayed %v after a change, want the repository through it", got)
	}
	cancel()
	<-done
	if ls, err := q.Lease(ctx, work.Jobs{Queue: q, Prefix: "y"}.QueueOf("relay."+dirfeed.Topic), bus.LeaseOptions{Max: 1, Visibility: time.Minute}); err != nil || len(ls) != 0 {
		t.Errorf("a role that never registered got %d jobs, %v", len(ls), err)
	}
}
