package storetest

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/automation/store"
)

// quotas are a test's accounts' quotas, every other account's unlimited.
func quotas(q map[string]store.Quota) store.Quotas {
	var mu sync.Mutex
	return quotasOf(&mu, q)
}

// quotasOf are quotas read from q under mu each time, so a test may change them.
func quotasOf(mu *sync.Mutex, q map[string]store.Quota) store.Quotas {
	return func(_ context.Context, accounts []string) (map[string]store.Quota, error) {
		mu.Lock()
		defer mu.Unlock()
		out := map[string]store.Quota{}
		for _, a := range accounts {
			out[a] = q[a]
		}
		return out, nil
	}
}

// leaseAll leases runs until none is admitted, naming each its account.
func leaseAll(t *testing.T, s *store.Store, pool string, owners map[int64]string) ([]string, []*store.Run) {
	t.Helper()
	var accounts []string
	var runs []*store.Run
	for i := 0; ; i++ {
		r, err := s.Lease(context.Background(), "w"+strconv.Itoa(i), pool, 0)
		if err != nil {
			t.Fatal(err)
		}
		if r == nil {
			return accounts, runs
		}
		accounts, runs = append(accounts, owners[r.RepositoryID]), append(runs, r)
	}
}

// leaseWaiting leases runs until none comes within wait, naming each its account.
func leaseWaiting(t *testing.T, s *store.Store, owners map[int64]string, wait time.Duration) []string {
	t.Helper()
	var accounts []string
	for i := 0; ; i++ {
		r, err := s.Lease(context.Background(), "wq"+strconv.Itoa(i), "", wait)
		if err != nil {
			t.Fatal(err)
		}
		if r == nil {
			return accounts
		}
		accounts = append(accounts, owners[r.RepositoryID])
	}
}

// An account at its concurrent quota — its runs here and as the other cells report them — has its
// further runs wait, their jobs delivered again a while later, holding up no other account's:
// another account's runs queued behind them are leased at once. As a run of the account stops, one
// of its waiting runs starts once its job comes back; and once its quota rises, as many more as it
// admits.
func testQuotas(t *testing.T, e *env) {
	ctx := context.Background()
	var mu sync.Mutex
	q := map[string]store.Quota{"acme": {Concurrent: 3, RunningElsewhere: 1}}
	cfg := store.DefaultConfig
	cfg.Quotas, cfg.QuotaWait = quotasOf(&mu, q), 200*time.Millisecond
	s := e.newStore(t, cfg)
	owners := map[int64]string{1: "acme", 2: "bea"}
	for repo, account := range owners {
		if err := s.SetOwner(ctx, repo, account); err != nil {
			t.Fatal(err)
		}
	}
	for range 10 {
		start(t, s, 1, build("//a:a"))
	}
	for range 2 {
		start(t, s, 2, build("//b:b"))
	}
	got, leased := leaseAll(t, s, "", owners)
	// acme is admitted 2 (its 3, less the 1 running elsewhere); bea's come straight after.
	if want := []string{"acme", "acme", "bea", "bea"}; !slices.Equal(got, want) {
		t.Fatalf("admitted %v, want %v", got, want)
	}
	if got := leaseWaiting(t, s, owners, 3*cfg.QuotaWait); len(got) != 0 {
		t.Fatalf("acme at its quota: %v admitted", got)
	}
	if err := s.Finish(ctx, leased[0].ID, leased[0].Worker, store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	if got := leaseWaiting(t, s, owners, 3*cfg.QuotaWait); !slices.Equal(got, []string{"acme"}) {
		t.Fatalf("after acme's run finished: %v admitted, want one of acme's", got)
	}
	// Raised to 6, acme is admitted 3 more, as its runs' jobs come back.
	mu.Lock()
	q["acme"] = store.Quota{Concurrent: 6, RunningElsewhere: 1}
	mu.Unlock()
	if got := leaseWaiting(t, s, owners, 3*cfg.QuotaWait); !slices.Equal(got, []string{"acme", "acme", "acme"}) {
		t.Errorf("after acme's quota rose: %v admitted, want 3 of acme's", got)
	}
}

// A run waits for a worker of its pool: the default pool's take none of another's.
func testPools(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	gpu := build("//g:g")
	gpu.Pool = "gpu"
	r := start(t, s, 1, gpu)
	if r.Job.Pool != "gpu" {
		t.Errorf("the run's pool: %q", r.Job.Pool)
	}
	plain := start(t, s, 1, build("//a:a"))
	if plain.Job.Pool != "default" {
		t.Errorf("a run naming no pool is in %q, want default", plain.Job.Pool)
	}
	if got, err := s.Lease(ctx, "w1", "", 0); err != nil || got == nil || got.ID != plain.ID {
		t.Errorf("the default pool's lease: %v, %v; want %d", got, err, plain.ID)
	}
	if got, err := s.Lease(ctx, "w2", "", 0); err != nil || got != nil {
		t.Errorf("the default pool's second lease: %v, %v; want none", got, err)
	}
	if got, err := s.Lease(ctx, "w3", "gpu", 0); err != nil || got == nil || got.ID != r.ID {
		t.Errorf("the gpu pool's lease: %v, %v; want %d", got, err, r.ID)
	}
}

// A run that ran is charged its seconds to its account's month as it finishes, or is given back;
// the month's use names each account's seconds and running runs; and an account past its month's
// seconds is admitted no more, its runs parked, holding up no other account's.
func testUsage(t *testing.T, e *env) {
	ctx := context.Background()
	cfg := store.DefaultConfig
	cfg.Quotas = quotas(map[string]store.Quota{"acme": {Seconds: 1}})
	s := e.newStore(t, cfg)
	if err := s.SetOwner(ctx, 1, "acme"); err != nil {
		t.Fatal(err)
	}
	a := start(t, s, 1, build("//a:a"))
	start(t, s, 1, build("//a:b"))
	start(t, s, 2, build("//n:n")) // a repository the directory has not named
	if r := lease(t, s, "w1"); r.ID != a.ID {
		t.Fatalf("leased %d, want %d", r.ID, a.ID)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := s.Finish(ctx, a.ID, "w1", store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	month, usage, err := s.MonthUsage(ctx)
	if err != nil || month != store.MonthOf(time.Now()) {
		t.Fatalf("the month's use: %v, %v, %v", month, usage, err)
	}
	var acme *store.Usage
	for i := range usage {
		if usage[i].Account == "acme" {
			acme = &usage[i]
		}
	}
	if acme == nil || acme.Seconds < 1 || acme.Running != 0 {
		t.Errorf("acme's month: %+v (of %+v)", acme, usage)
	}
	// acme is past its second; the unnamed repository's run is admitted, its account "".
	n, err := s.Lease(ctx, "w2", "", 0)
	if err != nil || n == nil || n.RepositoryID != 2 {
		t.Fatalf("the lease past acme's quota: %+v, %v", n, err)
	}
	if r, err := s.Lease(ctx, "w3", "", 0); err != nil || r != nil {
		t.Errorf("a lease of acme's past its quota: %+v, %v", r, err)
	}
	_, usage, err = s.MonthUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, u := range usage {
		found = found || u.Account == "" && u.Running == 1
	}
	if !found {
		t.Errorf("the month's use names no running run under no account: %+v", usage)
	}
}
