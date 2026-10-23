package scale

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/automation/store/postgres"
	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/testdb"
)

// fakeDeployments is a cluster's Deployments, as the scaler sees them.
type fakeDeployments struct {
	replicas map[string]int32
	pods     map[string][]string
	costs    map[string]int
}

func (f *fakeDeployments) Replicas(_ context.Context, ns, name string) (int32, error) {
	return f.replicas[ns+"/"+name], nil
}
func (f *fakeDeployments) Scale(_ context.Context, ns, name string, n int32) error {
	f.replicas[ns+"/"+name] = n
	return nil
}
func (f *fakeDeployments) Pods(_ context.Context, ns, name string) ([]string, error) {
	return f.pods[ns+"/"+name], nil
}
func (f *fakeDeployments) Cost(_ context.Context, _, pod string, cost int) error {
	f.costs[pod] = cost
	return nil
}

func newStore(t *testing.T, quotas map[string]store.Quota) *store.Store {
	t.Helper()
	b, err := postgres.Open(context.Background(), testdb.Pool(t))
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := blob.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := store.DefaultConfig
	cfg.Quotas = func(_ context.Context, accounts []string) (map[string]store.Quota, error) {
		out := map[string]store.Quota{}
		for _, a := range accounts {
			out[a] = quotas[a]
		}
		return out, nil
	}
	return store.New(b, blobs, cfg)
}

func queue(t *testing.T, s *store.Store, repo int64, pool string, n int) []int64 {
	t.Helper()
	var ids []int64
	for range n {
		r, err := s.Start(context.Background(), store.StartRequest{RepositoryID: repo, CommitID: make([]byte, 32),
			Job: store.Job{Kind: store.KindBuild, Targets: []string{"//a:a"}, Pool: pool}, Origin: store.Origin{Phase: store.PhaseManual},
			RequestedBy: "ada"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.ID)
	}
	return ids
}

// A pool grows at once to what its running and claimable runs need over its workers' slots, within
// its bounds — an account at its quota's runs not counted — and shrinks only once fewer have been
// enough for Down, its idlest workers costing least to delete.
func TestScaler(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, map[string]store.Quota{"capped": {Concurrent: 1}})
	for repo, account := range map[int64]string{1: "acme", 2: "capped"} {
		if err := s.SetOwner(ctx, repo, account); err != nil {
			t.Fatal(err)
		}
	}
	d := &fakeDeployments{replicas: map[string]int32{"ci/workers": 1, "ci/gpus": 0}, pods: map[string][]string{}, costs: map[string]int{}}
	now := time.Unix(1_800_000_000, 0)
	sc := &Scaler{Store: s, Deployments: d, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), now: func() time.Time { return now },
		Pools: []Pool{{Name: "default", Namespace: "ci", Deployment: "workers", Slots: 2, Min: 1, Max: 4},
			{Name: "gpu", Namespace: "ci", Deployment: "gpus", Slots: 1, Min: 0, Max: 2}}}

	// acme queues 5, capped 4 of which 1 is claimable: 6 claimable over 2 slots, 3 workers. The gpu
	// pool asks for nothing.
	queued := append(queue(t, s, 1, "", 5), queue(t, s, 2, "", 4)...)
	if err := sc.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if d.replicas["ci/workers"] != 3 || d.replicas["ci/gpus"] != 0 {
		t.Fatalf("grown to %v, want 3 workers and no gpus", d.replicas)
	}
	// More than its max: held to it.
	queued = append(queued, queue(t, s, 1, "", 10)...)
	queued = append(queued, queue(t, s, 1, "gpu", 1)...)
	if err := sc.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if d.replicas["ci/workers"] != 4 || d.replicas["ci/gpus"] != 1 {
		t.Fatalf("grown to %v, want the default pool's max, 4, and a gpu", d.replicas)
	}

	// Leased: two running on w-a's slots, one on w-b; the rest canceled.
	leased := map[int64]bool{}
	for _, w := range []string{"w-a/1", "w-a/2", "w-b"} {
		r, err := s.Lease(ctx, w, "", 0)
		if err != nil || r == nil {
			t.Fatalf("leasing for %s: %v, %v", w, r, err)
		}
		leased[r.ID] = true
	}
	for _, id := range queued {
		if !leased[id] {
			if _, err := s.Cancel(ctx, id, "test", ""); err != nil {
				t.Fatal(err)
			}
		}
	}
	d.pods["ci/workers"] = []string{"w-a", "w-b", "w-c", "w-d"}
	// Fewer would do: not yet.
	if err := sc.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if d.replicas["ci/workers"] != 4 {
		t.Fatalf("shrunk at once to %d", d.replicas["ci/workers"])
	}
	now = now.Add(Down)
	if err := sc.Step(ctx); err != nil {
		t.Fatal(err)
	}
	// Three running over two slots: two workers.
	if d.replicas["ci/workers"] != 2 {
		t.Errorf("shrunk to %d, want 2", d.replicas["ci/workers"])
	}
	if want := map[string]int{"w-a": 2, "w-b": 1, "w-c": 0, "w-d": 0}; !maps.Equal(d.costs, want) {
		t.Errorf("the pods' deletion costs: %v, want %v", d.costs, want)
	}
}
