// Package dbtest is the conformance suite every ok control store (internal/control/db) passes:
// run it against a backend with Run, giving it a new, empty store for each test.
package dbtest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/control/db"
)

const (
	alice = "alice.human.example.com"
	bob   = "bob.human.example.com"
)

var epoch = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

func cl(number int64, author string, status db.Status, parent int64, updated int) db.ChangeList {
	return db.ChangeList{
		Repo: 1, Number: number, ChangeID: []byte{byte(number)}, Author: author, Status: status, ParentNumber: parent,
		CurrentPatchset: 1, Description: "change " + string(rune('a'+number)) + "\n\nbody",
		CreatedAt: epoch, UpdatedAt: epoch.Add(time.Duration(updated) * time.Minute),
	}
}

func numbers(cls []db.ChangeList) []int64 {
	var out []int64
	for _, c := range cls {
		out = append(out, c.Number)
	}
	return out
}

// Run runs the suite, each test on a new, empty store that open makes.
func Run(t *testing.T, open func(t *testing.T) db.Store) {
	for _, tc := range []struct {
		name string
		fn   func(t *testing.T, s db.Store)
	}{
		{"ChangeListProjection", testChangeListProjection},
		{"ReviewData", testReviewData},
		{"RepositoriesAndFeed", testRepositoriesAndFeed},
		{"CreateRepositoryConcurrently", testCreateRepositoryConcurrently},
		{"Txn", testTxn},
		{"LockChangeList", testLockChangeList},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, open(t)) })
	}
}

func testChangeListProjection(t *testing.T, s db.Store) {
	ctx := context.Background()
	for _, c := range []db.ChangeList{
		cl(1, alice, db.StatusOpen, 0, 1),
		cl(2, alice, db.StatusDraft, 1, 2),
		cl(3, bob, db.StatusDraft, 2, 3),
		cl(4, bob, db.StatusOpen, 0, 4),
		cl(5, bob, db.StatusSubmitted, 0, 5),
		cl(6, alice, db.StatusAbandoned, 0, 6),
	} {
		if _, err := s.UpsertChangeList(ctx, c); err != nil {
			t.Fatal(err)
		}
	}

	// An older state never overwrites a newer one; approved is left alone.
	if _, err := s.SetApproved(ctx, 1, 4, true); err != nil {
		t.Fatal(err)
	}
	stale := cl(4, bob, db.StatusDraft, 0, 3)
	if wrote, err := s.UpsertChangeList(ctx, stale); err != nil || wrote {
		t.Fatalf("a stale upsert wrote: %v %v", wrote, err)
	}
	fresh := cl(4, bob, db.StatusOpen, 0, 9)
	fresh.CurrentPatchset = 2
	if wrote, err := s.UpsertChangeList(ctx, fresh); err != nil || !wrote {
		t.Fatalf("a fresh upsert: %v %v", wrote, err)
	}
	got, ok, err := s.GetChangeList(ctx, 1, 4)
	if err != nil || !ok || got.CurrentPatchset != 2 || got.Status != db.StatusOpen || !got.Approved || got.Title() != "change e" {
		t.Fatalf("CL 4: %+v %v %v", got, ok, err)
	}
	if _, ok, err := s.GetChangeList(ctx, 1, 99); err != nil || ok {
		t.Fatalf("a missing CL: %v %v", ok, err)
	}

	// Bob reviews CL 1 and has granted nothing; he reviews CL 4 too, but granted.
	if _, err := s.AddReviewer(ctx, 1, 1, bob, alice); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddReviewer(ctx, 1, 4, alice, bob); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetGrant(ctx, 1, 4, alice, db.LabelLGTM, true, 1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		f      db.Filter
		viewer string
		want   []int64
	}{
		{db.FilterOpen, alice, []int64{4, 2, 1}}, // open CLs and alice's own drafts
		{db.FilterOpen, bob, []int64{4, 3, 1}},
		{db.FilterDrafts, alice, []int64{3, 2}},
		{db.FilterNeedsReview, bob, []int64{1}},
		{db.FilterNeedsReview, alice, nil}, // she reviews CL 4 but has granted on it
		{db.FilterMerged, alice, []int64{5}},
		{db.FilterAbandoned, alice, []int64{6}},
		{db.FilterAll, alice, []int64{4, 6, 5, 3, 2, 1}},
	} {
		got, err := s.ListChangeLists(ctx, 1, tc.f, tc.viewer, 100)
		if err != nil || !reflect.DeepEqual(numbers(got), tc.want) {
			t.Errorf("%s for %s: %v, %v; want %v", tc.f, tc.viewer, numbers(got), err, tc.want)
		}
	}
	if got, err := s.ListChangeLists(ctx, 1, db.FilterAll, alice, 2); err != nil || !reflect.DeepEqual(numbers(got), []int64{4, 6}) {
		t.Errorf("a limited listing: %v, %v", numbers(got), err)
	}
	if db.ParseFilter("bogus") != db.FilterOpen || db.ParseFilter("merged") != db.FilterMerged {
		t.Error("ParseFilter")
	}

	// The stack: ancestors bottom first, then descendants.
	stack, err := s.Stack(ctx, 1, 2)
	if err != nil || !reflect.DeepEqual(numbers(stack), []int64{1, 2, 3}) {
		t.Errorf("CL 2's stack: %v, %v", numbers(stack), err)
	}
	if active, err := s.ActiveChangeLists(ctx, 1); err != nil || !reflect.DeepEqual(numbers(active), []int64{1, 2, 3, 4}) {
		t.Errorf("active: %v, %v", numbers(active), err)
	}

	// A rollback CL, and what landed as which commit.
	rb := cl(7, alice, db.StatusOpen, 0, 10)
	rb.RollsBack = []int64{5}
	if _, err := s.UpsertChangeList(ctx, rb); err != nil {
		t.Fatal(err)
	}
	if rbs, err := s.RollbacksOf(ctx, 1, 5); err != nil || !reflect.DeepEqual(numbers(rbs), []int64{7}) {
		t.Errorf("rollbacks of CL 5: %v, %v", numbers(rbs), err)
	}
	landed := cl(5, bob, db.StatusSubmitted, 0, 11)
	landed.LandedCommit = []byte("commit-5")
	if _, err := s.UpsertChangeList(ctx, landed); err != nil {
		t.Fatal(err)
	}
	if m, err := s.LandedBy(ctx, 1, [][]byte{[]byte("commit-5"), []byte("other")}); err != nil || !reflect.DeepEqual(m, map[string]int64{"commit-5": 5}) {
		t.Errorf("landed by: %v, %v", m, err)
	}
}

func testReviewData(t *testing.T, s db.Store) {
	ctx := context.Background()
	if _, err := s.UpsertChangeList(ctx, cl(1, alice, db.StatusOpen, 0, 1)); err != nil {
		t.Fatal(err)
	}

	// Reviewers, in the order they were added; each added once.
	if added, err := s.AddReviewer(ctx, 1, 1, bob, alice); err != nil || !added {
		t.Fatalf("add bob: %v %v", added, err)
	}
	if added, err := s.AddReviewer(ctx, 1, 1, bob, alice); err != nil || added {
		t.Fatalf("add bob again: %v %v", added, err)
	}
	if rs, err := s.Reviewers(ctx, 1, 1); err != nil || len(rs) != 1 || rs[0].Principal != bob || rs[0].AddedBy != alice {
		t.Fatalf("reviewers: %+v %v", rs, err)
	}
	if removed, err := s.RemoveReviewer(ctx, 1, 1, bob); err != nil || !removed {
		t.Fatalf("remove bob: %v %v", removed, err)
	}
	if removed, err := s.RemoveReviewer(ctx, 1, 1, bob); err != nil || removed {
		t.Fatalf("remove bob again: %v %v", removed, err)
	}

	// Revoking what was never granted changes nothing; granting twice changes once.
	for _, tc := range []struct {
		granted bool
		want    bool
	}{{false, false}, {true, true}, {true, false}, {false, true}, {false, false}} {
		changed, err := s.SetGrant(ctx, 1, 1, bob, db.LabelLGTM, tc.granted, 1)
		if err != nil || changed != tc.want {
			t.Errorf("SetGrant(%v) changed %v, %v; want %v", tc.granted, changed, err, tc.want)
		}
	}
	if _, err := s.SetGrant(ctx, 1, 1, alice, db.LabelApproval, true, 2); err != nil {
		t.Fatal(err)
	}
	gs, err := s.Grants(ctx, 1, 1)
	if err != nil || len(gs) != 2 || gs[0].Principal != alice || !gs[0].Granted || gs[0].Patchset != 2 || gs[1].Principal != bob || gs[1].Granted {
		t.Fatalf("grants: %+v %v", gs, err)
	}
	if gs, err := s.GrantsOf(ctx, 1, []int64{1, 2}); err != nil || len(gs) != 2 {
		t.Fatalf("grants of 1 and 2: %+v %v", gs, err)
	}

	// Threads: a CL-level comment is not a thread; a file thread is, until resolved.
	if _, err := s.CreateComment(ctx, 1, db.Comment{Number: 1, Patchset: 1, Author: bob, Body: "overall"}); err != nil {
		t.Fatal(err)
	}
	root, err := s.CreateComment(ctx, 1, db.Comment{Number: 1, Patchset: 1, Author: bob, FilePath: "a.go", Line: 3, Side: db.SidePatchset, Body: "here"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateComment(ctx, 1, db.Comment{Number: 1, Patchset: 1, Author: alice, ParentID: root, FilePath: "a.go", Line: 3, Side: db.SidePatchset, Body: "done"}); err != nil {
		t.Fatal(err)
	}
	if c, ok, err := s.GetComment(ctx, 1, 1, root); err != nil || !ok || c.Line != 3 || c.Side != db.SidePatchset || c.FilePath != "a.go" || c.Body != "here" {
		t.Fatalf("the root: %+v %v %v", c, ok, err)
	}
	if n, err := s.UnresolvedThreads(ctx, 1, 1); err != nil || n != 1 {
		t.Fatalf("unresolved: %d, %v", n, err)
	}
	if changed, err := s.SetResolved(ctx, 1, 1, root, true); err != nil || !changed {
		t.Fatalf("resolve: %v %v", changed, err)
	}
	if changed, err := s.SetResolved(ctx, 1, 1, root, true); err != nil || changed {
		t.Fatalf("resolve again: %v %v", changed, err)
	}
	if n, _ := s.UnresolvedThreads(ctx, 1, 1); n != 0 {
		t.Errorf("unresolved after resolving: %d", n)
	}
	if _, err := s.CreateComment(ctx, 1, db.Comment{Number: 1, Patchset: 1, Author: bob, FilePath: "a.go", Line: 3, Body: "no side"}); err == nil {
		t.Error("an inline comment without a side was stored")
	}
	if err := s.DeleteComment(ctx, 1, 1, root); err != nil {
		t.Fatal(err)
	}
	cs, err := s.Comments(ctx, 1, 1)
	if err != nil || len(cs) != 1 || cs[0].Body != "overall" {
		t.Errorf("deleting a thread's root deletes its replies: %+v, %v", cs, err)
	}
	if _, ok, err := s.GetComment(ctx, 1, 1, root); err != nil || ok {
		t.Errorf("a deleted comment: %v %v", ok, err)
	}

	// The change log, and the last abandonment.
	if _, ok, err := s.LastAbandon(ctx, 1, 1); err != nil || ok {
		t.Fatalf("a CL never abandoned: %v %v", ok, err)
	}
	for _, e := range []db.Event{
		{Actor: alice, Kind: db.EventAbandoned, From: db.StatusOpen, To: db.StatusAbandoned, At: epoch},
		{Actor: alice, Kind: db.EventRestored, From: db.StatusAbandoned, To: db.StatusOpen, At: epoch.Add(time.Minute)},
		{Actor: alice, Kind: db.EventAbandoned, From: db.StatusDraft, To: db.StatusAbandoned, At: epoch.Add(2 * time.Minute), Message: "why"},
	} {
		if err := s.RecordEvent(ctx, 1, 1, e); err != nil {
			t.Fatal(err)
		}
	}
	last, ok, err := s.LastAbandon(ctx, 1, 1)
	if err != nil || !ok || last.From != db.StatusDraft {
		t.Errorf("last abandon: %+v %v %v", last, ok, err)
	}
	evs, err := s.Events(ctx, 1, 1)
	if err != nil || len(evs) != 3 || evs[1].Kind != db.EventRestored || evs[2].Message != "why" || !evs[0].At.Equal(epoch) {
		t.Errorf("events: %+v %v", evs, err)
	}
	if err := s.RecordEvent(ctx, 1, 1, db.Event{Kind: db.EventApproved}); err != nil {
		t.Fatal(err)
	}
	if evs, _ := s.Events(ctx, 1, 1); len(evs) != 4 || time.Since(evs[3].At) > time.Hour {
		t.Errorf("an event with no time is now: %+v", evs)
	}
}

func testRepositoriesAndFeed(t *testing.T, s db.Store) {
	ctx := context.Background()
	for i, name := range []string{"alpha", "beta", "gamma_x", "gammax"} {
		if err := s.CreateRepository(ctx, db.Repository{ID: int64(i + 1), Name: name, CreatedBy: alice, CreatedAt: epoch.Add(time.Duration(i) * time.Minute)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.MoveHead(ctx, 1, 3, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.MoveHead(ctx, 1, 2, epoch.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetFavorite(ctx, bob, 2, true); err != nil {
		t.Fatal(err)
	}
	if fs, err := s.Favorites(ctx, bob); err != nil || !reflect.DeepEqual(fs, map[int64]bool{2: true}) {
		t.Errorf("bob's favorites: %v %v", fs, err)
	}
	names := func(rs []db.Repository) []string {
		var out []string
		for _, r := range rs {
			out = append(out, r.Name)
		}
		return out
	}
	rs, err := s.ListRepositories(ctx, bob, "", 0, 10)
	if err != nil || !reflect.DeepEqual(names(rs), []string{"beta", "alpha", "gammax", "gamma_x"}) || !rs[0].Favorite || rs[1].HeadSeq != 3 {
		t.Errorf("bob's finder: %v %+v, %v", names(rs), rs, err)
	}
	if rs, _ := s.ListRepositories(ctx, bob, "gamma_", 0, 10); !reflect.DeepEqual(names(rs), []string{"gamma_x"}) {
		t.Errorf("a search is literal, not a pattern: %v", names(rs))
	}
	if rs, _ := s.ListRepositories(ctx, bob, "", 1, 2); !reflect.DeepEqual(names(rs), []string{"alpha", "gammax"}) {
		t.Errorf("a page: %v", names(rs))
	}
	if err := s.RenameRepository(ctx, 3, "delta"); err != nil {
		t.Fatal(err)
	}
	if rs, _ := s.ListRepositories(ctx, bob, "", 0, 10); !reflect.DeepEqual(names(rs), []string{"beta", "alpha", "gammax", "delta"}) {
		t.Errorf("after a rename: %v", names(rs))
	}
	if r, ok, err := s.RepositoryByName(ctx, "delta"); err != nil || !ok || r.ID != 3 || r.CreatedBy != alice {
		t.Errorf("delta by name: %+v %v %v", r, ok, err)
	}
	if r, ok, err := s.RepositoryByName(ctx, "alpha"); err != nil || !ok || r.HeadSeq != 3 {
		t.Errorf("alpha by name: %+v %v %v", r, ok, err)
	}
	if _, ok, err := s.RepositoryByName(ctx, "gamma_x"); err != nil || ok {
		t.Errorf("a renamed-away name: %v %v", ok, err)
	}
	if err := s.SetFavorite(ctx, bob, 2, false); err != nil {
		t.Fatal(err)
	}
	if fs, _ := s.Favorites(ctx, bob); len(fs) != 0 {
		t.Errorf("bob's favorites after unfavoriting: %v", fs)
	}

	// The feed's cursors, one per repository's log.
	const topic = "scm.events"
	if c, err := s.FollowCursor(ctx, topic, 1); err != nil || c != 0 {
		t.Fatalf("a new cursor: %d %v", c, err)
	}
	if err := s.SetFollowCursor(ctx, topic, 1, 42); err != nil {
		t.Fatal(err)
	}
	if c, _ := s.FollowCursor(ctx, topic, 1); c != 42 {
		t.Errorf("cursor %d", c)
	}
	// Appliers that each lock a repository's cursor, then advance it by one, never lose an
	// advance — the first of them making its row.
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		for _, repo := range []int64{1, 2} {
			wg.Go(func() {
				for i := 0; i < 10; i++ {
					if err := s.Txn(ctx, func(tx db.Ops) error {
						c, err := tx.LockFollowCursor(ctx, topic, repo)
						if err != nil {
							return err
						}
						return tx.SetFollowCursor(ctx, topic, repo, c+1)
					}); err != nil {
						t.Error(err)
						return
					}
				}
			})
		}
	}
	wg.Wait()
	if c, _ := s.FollowCursor(ctx, topic, 1); c != 82 {
		t.Errorf("after 40 locked advances from 42 repository 1's cursor is %d", c)
	}
	if c, _ := s.FollowCursor(ctx, topic, 2); c != 40 {
		t.Errorf("after 40 locked advances from none repository 2's cursor is %d", c)
	}
	for _, seed := range [][2]int64{{3, 7}, {3, 9}, {1, 1}} {
		if err := s.SeedFollowCursor(ctx, topic, seed[0], seed[1]); err != nil {
			t.Fatal(err)
		}
	}
	for repo, want := range map[int64]int64{1: 82, 3: 7} {
		if c, _ := s.FollowCursor(ctx, topic, repo); c != want {
			t.Errorf("repository %d's cursor after seeding: %d, want %d", repo, c, want)
		}
	}
	if at, err := s.LegacyCursor(ctx); err != nil || at != 0 {
		t.Errorf("a new store's place in the legacy feed: %d, %v", at, err)
	}
	if err := s.ForgetLegacyCursor(ctx); err != nil {
		t.Errorf("forgetting a legacy cursor it never had: %v", err)
	}
}

// A repository is recorded by both the request that created it and the change-feed follower,
// often at the same instant: whichever is second does nothing, rather than failing on the
// name's uniqueness while the first is still committing.
func testCreateRepositoryConcurrently(t *testing.T, s db.Store) {
	ctx := context.Background()
	for i := range int64(100) {
		r := db.Repository{ID: 1000 + i, Name: fmt.Sprintf("repo-%d", i), CreatedBy: alice, CreatedAt: epoch}
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for j := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[j] = s.CreateRepository(ctx, r)
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

// A transaction's writes are all or nothing.
func testTxn(t *testing.T, s db.Store) {
	ctx := context.Background()
	boom := errors.New("boom")
	err := s.Txn(ctx, func(tx db.Ops) error {
		if _, err := tx.UpsertChangeList(ctx, cl(1, alice, db.StatusOpen, 0, 1)); err != nil {
			return err
		}
		if err := tx.RecordEvent(ctx, 1, 1, db.Event{Kind: db.EventUploaded, Patchset: 1}); err != nil {
			return err
		}
		if _, ok, err := tx.GetChangeList(ctx, 1, 1); err != nil || !ok {
			t.Errorf("a transaction does not see its own write: %v %v", ok, err)
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Txn = %v, want fn's error", err)
	}
	if _, ok, err := s.GetChangeList(ctx, 1, 1); err != nil || ok {
		t.Fatalf("a failed transaction's write survived: %v %v", ok, err)
	}
	if evs, err := s.Events(ctx, 1, 1); err != nil || len(evs) != 0 {
		t.Fatalf("a failed transaction's event survived: %v %v", evs, err)
	}
	if err := s.Txn(ctx, func(tx db.Ops) error {
		_, err := tx.UpsertChangeList(ctx, cl(1, alice, db.StatusOpen, 0, 1))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.GetChangeList(ctx, 1, 1); err != nil || !ok {
		t.Fatalf("a committed transaction's write: %v %v", ok, err)
	}
}

// A CL locked in a transaction holds off other writers of it until the transaction ends: two
// transactions that each read and increment its patchset never lose an increment.
func testLockChangeList(t *testing.T, s db.Store) {
	ctx := context.Background()
	c := cl(1, alice, db.StatusOpen, 0, 1)
	if _, err := s.UpsertChangeList(ctx, c); err != nil {
		t.Fatal(err)
	}
	const writers, each = 4, 10
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				err := s.Txn(ctx, func(tx db.Ops) error {
					cur, ok, err := tx.LockChangeList(ctx, 1, 1)
					if err != nil || !ok {
						return fmt.Errorf("lock: %v %v", ok, err)
					}
					cur.CurrentPatchset++
					cur.UpdatedAt = cur.UpdatedAt.Add(time.Second)
					_, err = tx.UpsertChangeList(ctx, cur)
					return err
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	got, _, err := s.GetChangeList(ctx, 1, 1)
	if err != nil || got.CurrentPatchset != 1+writers*each {
		t.Fatalf("after %d locked increments the patchset is %d, %v", writers*each, got.CurrentPatchset, err)
	}
}
