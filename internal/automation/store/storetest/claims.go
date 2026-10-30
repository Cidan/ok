package storetest

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/automation/store"
)

// leaseAtOnce has workers lease at once, each until none comes within wait, and returns every run
// they were given.
func leaseAtOnce(t *testing.T, s *store.Store, workers int, wait time.Duration) []*store.Run {
	t.Helper()
	var mu sync.Mutex
	var got []*store.Run
	var errs []error
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			for {
				r, err := s.Lease(context.Background(), "w"+strconv.Itoa(i), "", wait)
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
				}
				if r != nil {
					got = append(got, r)
				}
				mu.Unlock()
				if err != nil || r == nil {
					return
				}
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		t.Error(err)
	}
	return got
}

// Workers claiming at once each take a different run: of an unlimited account's, every one exactly
// once; of an account with a concurrent quota, exactly its quota, however many claim beside each
// other, the rest waiting. A run that stops running — finished, or given back — lets its slot go to
// a waiting run, and so does a purged repository's.
func testConcurrentClaims(t *testing.T, e *env) {
	ctx := context.Background()
	cfg := store.DefaultConfig
	cfg.Quotas, cfg.QuotaWait = quotas(map[string]store.Quota{"acme": {Concurrent: 3}, "bea": {Concurrent: 1}}), 200*time.Millisecond
	back := 3 * cfg.QuotaWait // a waiting run's job comes back
	s := e.newStore(t, cfg)
	for repo, account := range map[int64]string{1: "flood", 2: "acme", 3: "acme", 4: "bea", 5: "bea"} {
		if err := s.SetOwner(ctx, repo, account); err != nil {
			t.Fatal(err)
		}
	}

	const runs = 40
	for range runs {
		start(t, s, 1, build("//f:f"))
	}
	got := leaseAtOnce(t, s, 24, 0)
	seen := map[int64]bool{}
	for _, r := range got {
		if seen[r.ID] {
			t.Errorf("run %d was leased twice", r.ID)
		}
		seen[r.ID] = true
	}
	if len(seen) != runs {
		t.Errorf("an unlimited account's %d runs: %d leased", runs, len(seen))
	}
	for _, r := range got {
		if err := s.Finish(ctx, r.ID, r.Worker, store.StatusPassed, 0, "", nil); err != nil {
			t.Fatal(err)
		}
	}

	// acme, with a quota of 3, floods: 24 workers at once are given 3 of its runs.
	for range 12 {
		start(t, s, 2, build("//a:a"))
	}
	start(t, s, 3, build("//a:other"))
	got = leaseAtOnce(t, s, 24, 0)
	if len(got) != 3 {
		t.Fatalf("an account of quota 3, claimed at once: %d leased", len(got))
	}
	// One finished, one given back: two more, and no more.
	if err := s.Finish(ctx, got[0].ID, got[0].Worker, store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, got[1].ID, got[1].Worker, "shutting down"); err != nil {
		t.Fatal(err)
	}
	if more := leaseAtOnce(t, s, 24, back); len(more) != 2 {
		t.Errorf("after one finished and one given back: %d leased, want 2", len(more))
	}
	// A purged repository's running run lets its slot go: bea, of quota 1, runs one of its runs —
	// whichever claim of the two at once takes its slot — and then, once that run's repository is
	// purged, the other.
	start(t, s, 4, build("//b:b"))
	start(t, s, 5, build("//b:b"))
	got = leaseAtOnce(t, s, 8, 0)
	if len(got) != 1 {
		t.Fatalf("bea, of quota 1: %d leased, want one", len(got))
	}
	if more := leaseAtOnce(t, s, 8, back); len(more) != 0 {
		t.Fatalf("bea's quota full: %d more leased", len(more))
	}
	ran := got[0].RepositoryID
	if err := s.PurgeRepository(ctx, ran); err != nil {
		t.Fatal(err)
	}
	if after := leaseAtOnce(t, s, 8, back); len(after) != 1 || after[0].RepositoryID != 9-ran {
		t.Errorf("after repository %d was purged: %v leased, want repository %d's run", ran, after, 9-ran)
	}
}
