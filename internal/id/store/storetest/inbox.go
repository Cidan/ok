package storetest

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// A CL's inbox entries are set whole, at a version, an older one changing nothing; a person's are
// listed those with attention first, then the latest changed, closed CLs only when asked for; and
// a repository's purge removes its entries.
func testInbox(t *testing.T, s *store.Store) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0).UTC()
	s.SetClock(func() time.Time { return now })
	ada, err := s.CreateAccount(ctx, "ada", "ada@example.com", "github", "gh-ada")
	if err != nil {
		t.Fatal(err)
	}
	repo, err := s.ReserveRepository(ctx, store.NewRepository{Owner: ada.ID, Name: "notes", Propose: tenancy.Read, CreatedBy: ada.ID})
	if err != nil {
		t.Fatal(err)
	}
	entry := func(number int64, who string, rel store.InboxRelation, attention bool, status string, at time.Duration) store.InboxEntry {
		return store.InboxEntry{Principal: who, Repository: repo.ID, Number: number, Relation: rel, Attention: attention, RepositoryName: "ada/notes",
			Title: "cl", Status: status, UpdatedAt: now.Add(at)}
	}
	set := func(number, version int64, es ...store.InboxEntry) bool {
		t.Helper()
		ok, err := s.SetInbox(ctx, repo.ID, number, version, es)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	list := func(who string, rel store.InboxRelation, closed bool) []int64 {
		t.Helper()
		es, err := s.Inbox(ctx, who, rel, closed, 50)
		if err != nil {
			t.Fatal(err)
		}
		var out []int64
		for _, e := range es {
			out = append(out, e.Number*10+int64(e.Relation))
		}
		return out
	}
	if !set(1, 10, entry(1, "ada", store.Authored, false, "open", time.Minute), entry(1, "bea", store.Reviewing, true, "open", time.Minute)) {
		t.Fatal("the first set was not applied")
	}
	set(2, 5, entry(2, "bea", store.Authored, false, "open", 2*time.Minute), entry(2, "ada", store.Reviewing, false, "open", 2*time.Minute))
	set(3, 5, entry(3, "ada", store.Authored, false, "submitted", 3*time.Minute))
	if set(1, 9, entry(1, "ada", store.Authored, true, "abandoned", time.Hour)) {
		t.Error("an older version was applied")
	}
	// ada: CL 2 she reviews (newer), then CL 1 she wrote; CL 3, submitted, only when asked.
	for _, tc := range []struct {
		who    string
		rel    store.InboxRelation
		closed bool
		want   []int64
	}{
		{"ada", 0, false, []int64{22, 11}},
		{"ada", 0, true, []int64{31, 22, 11}},
		{"ada", store.Authored, false, []int64{11}},
		{"bea", 0, false, []int64{12, 21}}, // CL 1 waits on bea
	} {
		if got := list(tc.who, tc.rel, tc.closed); !slices.Equal(got, tc.want) {
			t.Errorf("%s's inbox (%d, closed %v): %v, want %v", tc.who, tc.rel, tc.closed, got, tc.want)
		}
	}
	// A newer set replaces every entry: bea is no longer a reviewer of CL 1.
	set(1, 11, entry(1, "ada", store.Authored, true, "open", 4*time.Minute))
	if got := list("bea", 0, false); !slices.Equal(got, []int64{21}) {
		t.Errorf("bea's inbox once she no longer reviews CL 1: %v", got)
	}
	if got := list("ada", 0, false); !slices.Equal(got, []int64{11, 22}) {
		t.Errorf("ada's inbox once CL 1 waits on her: %v", got)
	}
	if _, err := s.SetInbox(ctx, repo.ID, 1, 12, []store.InboxEntry{entry(2, "ada", store.Authored, false, "open", 0)}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("an entry of another CL: %v", err)
	}

	// Purged, the repository's entries go.
	if _, err := s.ActivateRepository(ctx, repo.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteRepository(ctx, repo.ID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.RetentionPeriod + time.Hour)
	if err := s.PurgeRepository(ctx, repo.ID); err != nil {
		t.Fatal(err)
	}
	if got := list("ada", 0, true); len(got) != 0 {
		t.Errorf("a purged repository's entries: %v", got)
	}
}
