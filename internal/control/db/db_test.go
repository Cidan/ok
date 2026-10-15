package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/testdb"
)

const (
	alice = "alice.human.example.com"
	bob   = "bob.human.example.com"
)

func testDB(t *testing.T) *DB {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	d, err := Open(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

var epoch = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

func cl(number int64, author string, status Status, parent int64, updated int) ChangeList {
	return ChangeList{
		Repo: 1, Number: number, ChangeID: []byte{byte(number)}, Author: author, Status: status, ParentNumber: parent,
		CurrentPatchset: 1, Description: "change " + string(rune('a'+number)) + "\n\nbody",
		CreatedAt: epoch, UpdatedAt: epoch.Add(time.Duration(updated) * time.Minute),
	}
}

func numbers(cls []ChangeList) []int64 {
	var out []int64
	for _, c := range cls {
		out = append(out, c.Number)
	}
	return out
}

func TestChangeListProjection(t *testing.T) {
	ctx := context.Background()
	d := testDB(t)
	q := d.Q()
	for _, c := range []ChangeList{
		cl(1, alice, StatusOpen, 0, 1),
		cl(2, alice, StatusDraft, 1, 2),
		cl(3, bob, StatusDraft, 2, 3),
		cl(4, bob, StatusOpen, 0, 4),
		cl(5, bob, StatusSubmitted, 0, 5),
		cl(6, alice, StatusAbandoned, 0, 6),
	} {
		if _, err := UpsertChangeList(ctx, q, c); err != nil {
			t.Fatal(err)
		}
	}

	// An older state never overwrites a newer one; approved is left alone.
	if _, err := SetApproved(ctx, q, 1, 4, true); err != nil {
		t.Fatal(err)
	}
	stale := cl(4, bob, StatusDraft, 0, 3)
	if wrote, err := UpsertChangeList(ctx, q, stale); err != nil || wrote {
		t.Fatalf("a stale upsert wrote: %v %v", wrote, err)
	}
	fresh := cl(4, bob, StatusOpen, 0, 9)
	fresh.CurrentPatchset = 2
	if wrote, err := UpsertChangeList(ctx, q, fresh); err != nil || !wrote {
		t.Fatalf("a fresh upsert: %v %v", wrote, err)
	}
	got, ok, err := GetChangeList(ctx, q, 1, 4)
	if err != nil || !ok || got.CurrentPatchset != 2 || got.Status != StatusOpen || !got.Approved || got.Title() != "change e" {
		t.Fatalf("CL 4: %+v %v %v", got, ok, err)
	}

	// Bob reviews CL 1 and has granted nothing; he reviews CL 4 too, but granted.
	if _, err := AddReviewer(ctx, q, 1, 1, bob, alice); err != nil {
		t.Fatal(err)
	}
	if _, err := AddReviewer(ctx, q, 1, 4, alice, bob); err != nil {
		t.Fatal(err)
	}
	if _, err := SetGrant(ctx, q, 1, 4, alice, LabelLGTM, true, 1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		f      Filter
		viewer string
		want   []int64
	}{
		{FilterOpen, alice, []int64{4, 2, 1}}, // open CLs and alice's own drafts
		{FilterOpen, bob, []int64{4, 3, 1}},
		{FilterDrafts, alice, []int64{3, 2}},
		{FilterNeedsReview, bob, []int64{1}},
		{FilterNeedsReview, alice, nil}, // she reviews CL 4 but has granted on it
		{FilterMerged, alice, []int64{5}},
		{FilterAbandoned, alice, []int64{6}},
		{FilterAll, alice, []int64{4, 6, 5, 3, 2, 1}},
	} {
		got, err := ListChangeLists(ctx, q, 1, tc.f, tc.viewer, 100)
		if err != nil || !reflect.DeepEqual(numbers(got), tc.want) {
			t.Errorf("%s for %s: %v, %v; want %v", tc.f, tc.viewer, numbers(got), err, tc.want)
		}
	}
	if ParseFilter("bogus") != FilterOpen || ParseFilter("merged") != FilterMerged {
		t.Error("ParseFilter")
	}

	// The stack: ancestors bottom first, then descendants.
	stack, err := Stack(ctx, q, 1, 2)
	if err != nil || !reflect.DeepEqual(numbers(stack), []int64{1, 2, 3}) {
		t.Errorf("CL 2's stack: %v, %v", numbers(stack), err)
	}
	if active, err := ActiveChangeLists(ctx, q, 1); err != nil || !reflect.DeepEqual(numbers(active), []int64{1, 2, 3, 4}) {
		t.Errorf("active: %v, %v", numbers(active), err)
	}
}

func TestReviewData(t *testing.T) {
	ctx := context.Background()
	d := testDB(t)
	q := d.Q()
	if _, err := UpsertChangeList(ctx, q, cl(1, alice, StatusOpen, 0, 1)); err != nil {
		t.Fatal(err)
	}

	// Revoking what was never granted changes nothing; granting twice changes once.
	for _, tc := range []struct {
		granted bool
		want    bool
	}{{false, false}, {true, true}, {true, false}, {false, true}, {false, false}} {
		changed, err := SetGrant(ctx, q, 1, 1, bob, LabelLGTM, tc.granted, 1)
		if err != nil || changed != tc.want {
			t.Errorf("SetGrant(%v) changed %v, %v; want %v", tc.granted, changed, err, tc.want)
		}
	}

	// Threads: a CL-level comment is not a thread; a file thread is, until resolved.
	if _, err := CreateComment(ctx, q, 1, Comment{Number: 1, Patchset: 1, Author: bob, Body: "overall"}); err != nil {
		t.Fatal(err)
	}
	root, err := CreateComment(ctx, q, 1, Comment{Number: 1, Patchset: 1, Author: bob, FilePath: "a.go", Line: 3, Side: SidePatchset, Body: "here"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateComment(ctx, q, 1, Comment{Number: 1, Patchset: 1, Author: alice, ParentID: root, FilePath: "a.go", Line: 3, Side: SidePatchset, Body: "done"}); err != nil {
		t.Fatal(err)
	}
	if n, err := UnresolvedThreads(ctx, q, 1, 1); err != nil || n != 1 {
		t.Fatalf("unresolved: %d, %v", n, err)
	}
	if changed, err := SetResolved(ctx, q, 1, 1, root, true); err != nil || !changed {
		t.Fatalf("resolve: %v %v", changed, err)
	}
	if n, _ := UnresolvedThreads(ctx, q, 1, 1); n != 0 {
		t.Errorf("unresolved after resolving: %d", n)
	}
	if _, err := CreateComment(ctx, q, 1, Comment{Number: 1, Patchset: 1, Author: bob, FilePath: "a.go", Line: 3, Body: "no side"}); err == nil {
		t.Error("an inline comment without a side was stored")
	}
	if err := DeleteComment(ctx, q, 1, 1, root); err != nil {
		t.Fatal(err)
	}
	cs, err := Comments(ctx, q, 1, 1)
	if err != nil || len(cs) != 1 || cs[0].Body != "overall" {
		t.Errorf("deleting a thread's root deletes its replies: %+v, %v", cs, err)
	}

	// The change log, and the last abandonment.
	for _, e := range []Event{
		{Actor: alice, Kind: EventAbandoned, From: StatusOpen, To: StatusAbandoned, At: epoch},
		{Actor: alice, Kind: EventRestored, From: StatusAbandoned, To: StatusOpen, At: epoch.Add(time.Minute)},
		{Actor: alice, Kind: EventAbandoned, From: StatusDraft, To: StatusAbandoned, At: epoch.Add(2 * time.Minute)},
	} {
		if err := RecordEvent(ctx, q, 1, 1, e); err != nil {
			t.Fatal(err)
		}
	}
	last, ok, err := LastAbandon(ctx, q, 1, 1)
	if err != nil || !ok || last.From != StatusDraft {
		t.Errorf("last abandon: %+v %v %v", last, ok, err)
	}
	evs, err := Events(ctx, q, 1, 1)
	if err != nil || len(evs) != 3 || evs[1].Kind != EventRestored {
		t.Errorf("events: %+v %v", evs, err)
	}
}

func TestRepositoriesAndFeed(t *testing.T) {
	ctx := context.Background()
	d := testDB(t)
	q := d.Q()
	for i, name := range []string{"alpha", "beta", "gamma_x", "gammax"} {
		if err := CreateRepository(ctx, q, Repository{ID: int64(i + 1), Name: name, CreatedBy: alice, CreatedAt: epoch.Add(time.Duration(i) * time.Minute)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := MoveHead(ctx, q, 1, 3, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := MoveHead(ctx, q, 1, 2, epoch.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := SetFavorite(ctx, q, bob, 2, true); err != nil {
		t.Fatal(err)
	}
	names := func(rs []Repository) []string {
		var out []string
		for _, r := range rs {
			out = append(out, r.Name)
		}
		return out
	}
	rs, err := ListRepositories(ctx, q, bob, "", 0, 10)
	if err != nil || !reflect.DeepEqual(names(rs), []string{"beta", "alpha", "gammax", "gamma_x"}) || !rs[0].Favorite || rs[1].HeadSeq != 3 {
		t.Errorf("bob's finder: %v %+v, %v", names(rs), rs, err)
	}
	if rs, _ := ListRepositories(ctx, q, bob, "gamma_", 0, 10); !reflect.DeepEqual(names(rs), []string{"gamma_x"}) {
		t.Errorf("a search is literal, not a pattern: %v", names(rs))
	}
	if rs, _ := ListRepositories(ctx, q, bob, "", 1, 2); !reflect.DeepEqual(names(rs), []string{"alpha", "gammax"}) {
		t.Errorf("a page: %v", names(rs))
	}
	if err := RenameRepository(ctx, q, 3, "delta"); err != nil {
		t.Fatal(err)
	}
	if rs, _ := ListRepositories(ctx, q, bob, "", 0, 10); !reflect.DeepEqual(names(rs), []string{"beta", "alpha", "gammax", "delta"}) {
		t.Errorf("after a rename: %v", names(rs))
	}

	// The feed's cursor, and its leader lock: one holder at a time.
	if c, err := Cursor(ctx, q); err != nil || c != 0 {
		t.Fatalf("a new cursor: %d %v", c, err)
	}
	if err := SetCursor(ctx, q, 42); err != nil {
		t.Fatal(err)
	}
	if c, _ := Cursor(ctx, q); c != 42 {
		t.Errorf("cursor %d", c)
	}
	// One replica follows at a time: no second leader while the first leads, and a new one
	// once it gives up.
	err = d.LeadFeed(ctx, func(ctx context.Context) error {
		if err := d.LeadFeed(ctx, func(context.Context) error { t.Error("a second leader"); return nil }); !errors.Is(err, lock.ErrNotAcquired) {
			t.Errorf("a second leader: %v, want lock.ErrNotAcquired", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	led := false
	if err := d.LeadFeed(ctx, func(context.Context) error { led = true; return nil }); err != nil || !led {
		t.Fatalf("leader after the first gave up: %v", err)
	}
}

// A repository is recorded by both the request that created it and the change-feed follower,
// often at the same instant: whichever is second does nothing, rather than failing on the
// name's uniqueness while the first is still committing.
func TestCreateRepositoryConcurrently(t *testing.T) {
	ctx := context.Background()
	d := testDB(t)
	for i := range int64(200) {
		r := Repository{ID: 1000 + i, Name: fmt.Sprintf("repo-%d", i), CreatedBy: alice, CreatedAt: epoch}
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for j := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[j] = CreateRepository(ctx, d.Q(), r)
			}()
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				t.Fatalf("recording %s twice at once: %v", r.Name, err)
			}
		}
	}
}
