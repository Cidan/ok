package storetest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// A sync applies a directory of any size, a chunk at a time; one cut short partway leaves what it
// applied, and the next does the rest.
func testSyncAtScale(t *testing.T, s *store.Store) {
	ctx := context.Background()
	const people, groups = 250, 120
	// The founder of the organization that takes a handle below, synced first.
	founderPerson := person("1", "founder", true)
	src := &source{snap: &directory.Snapshot{People: []directory.Person{founderPerson}}}
	sync(t, s, src)
	founder, err := s.Principal(ctx, "founder.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	snap := &directory.Snapshot{People: []directory.Person{founderPerson}}
	for i := range people {
		snap.People = append(snap.People, person(fmt.Sprint(1000+i), fmt.Sprintf("scale%03d", i), true))
	}
	for i := range groups {
		snap.Groups = append(snap.Groups, directory.Group{ExternalID: fmt.Sprintf("g%03d", i), Name: fmt.Sprintf("team%03d", i),
			Members: []directory.Member{{ExternalID: fmt.Sprint(1000 + i)}, {ExternalID: fmt.Sprint(1000 + i + 1)}}})
	}
	src.snap = snap

	// Planned, then an organization takes a handle of the plan's second chunk before it is applied:
	// the first chunk stands, and the sync fails.
	st, _, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := s.SyncOptions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := directory.Reconcile(names, st, snap, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Humans) != people || len(plan.Groups) != groups {
		t.Fatalf("the plan makes %d people and %d groups", len(plan.Humans), len(plan.Groups))
	}
	if _, err := s.CreateOrganization(ctx, "scale150", founder.ID, store.OrganizationSettings{BaseRole: tenancy.Read}); err != nil {
		t.Fatal(err)
	}
	before, _ := s.SyncStatus(ctx)
	if err := s.Apply(ctx, plan, snap); !errors.Is(err, store.ErrTaken) {
		t.Fatalf("applying a plan a taken handle cuts short: %v", err)
	}
	if _, err := s.Principal(ctx, "scale000.human.example.com"); err != nil {
		t.Errorf("the first chunk, applied before the taken handle: %v", err)
	}
	if st, _ := s.SyncStatus(ctx); !st.LastRun.Equal(before.LastRun) {
		t.Errorf("a sync cut short recorded its snapshot: %v, before %v", st.LastRun, before.LastRun)
	}

	// The next sync does the rest: everyone but the one whose handle was taken, who waits.
	plan = sync(t, s, src)
	if len(plan.Waiting) != 1 || plan.Waiting[0].ExternalID != "1150" {
		t.Errorf("waiting = %v", plan.Waiting)
	}
	for i := range people {
		_, err := s.Principal(ctx, fmt.Sprintf("scale%03d.human.example.com", i))
		if i == 150 {
			if !errors.Is(err, store.ErrNotFound) {
				t.Errorf("the person whose handle was taken: %v", err)
			}
			continue
		}
		if err != nil {
			t.Errorf("person %d: %v", i, err)
		}
	}
	synced, err := s.SyncedGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for name, members := range synced {
		if len(name) > 4 && name[:4] == "team" {
			n++
			if len(members) == 0 {
				t.Errorf("%s has no members", name)
			}
		}
	}
	if n != groups {
		t.Errorf("%d synced teams, want %d", n, groups)
	}
	if st, _ := s.SyncStatus(ctx); !st.LastRun.After(before.LastRun) || len(st.Waiting) != 1 {
		t.Errorf("SyncStatus = %+v, before %+v", st, before)
	}
	// And a third changes nothing.
	if plan := sync(t, s, src); len(plan.Humans)+len(plan.Groups) != 0 {
		t.Errorf("a sync of what is synced: %d people, %d groups", len(plan.Humans), len(plan.Groups))
	}
}
