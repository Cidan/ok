package storetest

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/automation/store"
)

// quotas are a test's accounts' quotas, every other account's unlimited.
func quotas(q map[string]store.Quota) store.Quotas {
	return func(_ context.Context, accounts []string) (map[string]store.Quota, error) {
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

// An account flooding a pool takes no more than its share of it: each lease admits the account
// with the fewest running for its weight, under its quota — its concurrent runs here and as the
// other cells report them — and, within an account, its deploys before its manual runs.
func testFairShare(t *testing.T, e *env) {
	ctx := context.Background()
	cfg := store.DefaultConfig
	cfg.Quotas = quotas(map[string]store.Quota{
		"acme": {Concurrent: 4, RunningElsewhere: 1},
		"bea":  {Weight: 2},
	})
	s := e.newStore(t, cfg)
	owners := map[int64]string{1: "acme", 2: "bea", 3: "cal"}
	for repo, account := range owners {
		if err := s.SetOwner(ctx, repo, account); err != nil {
			t.Fatal(err)
		}
	}
	// acme floods first; bea and cal come after.
	for range 10 {
		start(t, s, 1, build("//a:a"))
	}
	for range 6 {
		start(t, s, 2, build("//b:b"))
	}
	for range 3 {
		start(t, s, 3, build("//c:c"))
	}
	got, leased := leaseAll(t, s, "", owners)
	// acme is admitted 3 (its 4, less the 1 running elsewhere); bea, of weight 2, twice what cal
	// is until cal runs out; acme's head is the oldest, so it goes first among equals.
	want := []string{"acme", "bea", "cal", "bea", "acme", "bea", "cal", "bea", "acme", "bea", "cal", "bea"}
	if !slices.Equal(got, want) {
		t.Errorf("admitted %v, want %v", got, want)
	}
	// acme's run finishing admits its next.
	one := leased[0]
	if err := s.Finish(ctx, one.ID, one.Worker, store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := leaseAll(t, s, "", owners); !slices.Equal(got, []string{"acme"}) {
		t.Errorf("after acme's run finished, admitted %v, want acme's next", got)
	}

	// Within an account, its deploy's push before its manual runs.
	start(t, s, 3, build("//c:manual"))
	push, err := s.Start(ctx, store.StartRequest{RepositoryID: 3, CommitID: commit(), Job: store.Job{Kind: store.KindPush, Targets: []string{"//c:push"}},
		Origin: store.Origin{Phase: store.PhaseDeploy, Job: "deploy"}, RequestedBy: "deploy.ok.robot.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if r := lease(t, s, "wp"); r.ID != push.ID {
		t.Errorf("leased %d (%v), want the push, %d", r.ID, r.Job.Kind, push.ID)
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
// seconds is admitted no more.
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

// An account floods a pool of eight workers with a thousand runs; another's ten arrive behind
// them. As the flood's runs finish, the late account takes every freed worker until it holds its
// share — half, the weights equal — and keeps it while it has runs: none of them waits behind the
// flood (docs/hosted.md, "Hosted automation, as built", done when).
func testFlood(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	for repo, account := range map[int64]string{1: "flood", 2: "late"} {
		if err := s.SetOwner(ctx, repo, account); err != nil {
			t.Fatal(err)
		}
	}
	reqs := make([]store.StartRequest, 0, 1000)
	for range 1000 {
		reqs = append(reqs, store.StartRequest{RepositoryID: 1, CommitID: commit(), Job: build("//f:f"), Origin: store.Origin{Phase: store.PhaseManual},
			RequestedBy: "alice.human.example.com"})
	}
	for _, r := range reqs {
		if _, err := s.Start(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	const workers = 8
	running := map[string]*store.Run{}
	lease := func(w string) string {
		t.Helper()
		r, err := s.Lease(ctx, w, "", 0)
		if err != nil || r == nil {
			t.Fatalf("lease for %s: %v, %v", w, r, err)
		}
		running[w] = r
		if r.RepositoryID == 2 {
			return "late"
		}
		return "flood"
	}
	for i := range workers {
		if got := lease("w" + strconv.Itoa(i)); got != "flood" {
			t.Fatalf("before the late account's runs, a lease went to %s", got)
		}
	}
	for range 10 {
		start(t, s, 2, build("//l:l"))
	}
	// Workers free one at a time, the oldest first.
	late := 0
	var order []string
	for i := 0; late < 10; i++ {
		w := "w" + strconv.Itoa(i%workers)
		r := running[w]
		if err := s.Finish(ctx, r.ID, w, store.StatusPassed, 0, "", nil); err != nil {
			t.Fatal(err)
		}
		got := lease(w)
		order = append(order, got)
		if got == "late" {
			late++
		}
		if i > 40 {
			t.Fatalf("the late account's runs waited behind the flood: %v", order)
		}
		// While the late account has runs, it holds its share: half the workers, from its fourth.
		held := 0
		for _, r := range running {
			if r.RepositoryID == 2 {
				held++
			}
		}
		if len(order) >= 4 && late < 10 && held < workers/2 {
			t.Fatalf("after %d freed workers the late account holds %d: %v", len(order), held, order)
		}
	}
	if want := []string{"late", "late", "late", "late"}; !slices.Equal(order[:4], want) {
		t.Errorf("the first freed workers went to %v, want the late account's", order[:4])
	}
}
