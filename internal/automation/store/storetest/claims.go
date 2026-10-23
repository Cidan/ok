package storetest

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/Cidan/ok/internal/automation/store"
)

// leaseAtOnce has workers lease at once, each until none is admitted it, and returns every run they
// were given.
func leaseAtOnce(t *testing.T, s *store.Store, workers int) []*store.Run {
	t.Helper()
	var mu sync.Mutex
	var got []*store.Run
	var errs []error
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			for {
				r, err := s.Lease(context.Background(), "w"+strconv.Itoa(i), "", 0)
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
// other. A run that stops running — finished, or given back — lets its slot go, and so does a
// purged repository's.
func testConcurrentClaims(t *testing.T, e *env) {
	ctx := context.Background()
	cfg := store.DefaultConfig
	cfg.Quotas = quotas(map[string]store.Quota{"acme": {Concurrent: 3}, "bea": {Concurrent: 1}})
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
	got := leaseAtOnce(t, s, 24)
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
	got = leaseAtOnce(t, s, 24)
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
	if more := leaseAtOnce(t, s, 24); len(more) != 2 {
		t.Errorf("after one finished and one given back: %d leased, want 2", len(more))
	}
	// A purged repository's running run lets its slot go: bea, of quota 1, runs repository 4's, the
	// older, and then — once 4 is purged — 5's.
	start(t, s, 4, build("//b:b"))
	start(t, s, 5, build("//b:b"))
	if got := leaseAtOnce(t, s, 8); len(got) != 1 || got[0].RepositoryID != 4 {
		t.Fatalf("bea, of quota 1: %v leased, want repository 4's run", got)
	}
	if more := leaseAtOnce(t, s, 8); len(more) != 0 {
		t.Fatalf("bea's quota full: %d more leased", len(more))
	}
	if err := s.PurgeRepository(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if got := leaseAtOnce(t, s, 8); len(got) != 1 || got[0].RepositoryID != 5 {
		t.Errorf("after repository 4 was purged: %v leased, want repository 5's run", got)
	}
}
