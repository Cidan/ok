package observe

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"github.com/Cidan/ok/internal/deploy/kubesim"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/deploy/store/postgres"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/workflow"
)

const self = "deploy.ok.robot.example.com"

// replica is one replica of the deployer: its own store, so its own locker, over the shared
// database.
func replica(t *testing.T, url string) *store.Store {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	b, err := postgres.Open(context.Background(), pool)
	s := store.New(b)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// withService declares one service, serve, rolling deployment smoke/serve.
func withService(t *testing.T, st *store.Store) {
	t.Helper()
	ctx := context.Background()
	for _, ev := range []store.FeedEvent{{Seq: 1, Repository: 1, Created: true}, {Seq: 2, Repository: 1, Head: make([]byte, 32), HeadSeq: 1}} {
		if err := st.Apply(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.ApplyDirectory(ctx, store.DirectoryEvent{Seq: 1, Repository: 1, FullName: "smoke"}); err != nil {
		t.Fatal(err)
	}
	serve := workflow.Deploy{File: ".ok/workflow/deploy.yaml", Name: "serve", Images: []workflow.Image{{Container: "serve", Target: "//serve:push"}},
		Workload: &workflow.Workload{Namespace: "smoke", Kind: workflow.Deployment, Name: "serve"}}
	if _, err := st.RecordPlan(ctx, store.PlanResult{Repository: 1, Head: make([]byte, 32), HeadSeq: 1, By: self, Services: []workflow.Deploy{serve}}); err != nil {
		t.Fatal(err)
	}
}

// Two replicas observe, one at a time, and each round starts a period after the last one
// ended: with a cluster that takes 300ms to read and a 200ms period, rounds start at least
// 500ms apart, and what the cluster runs is recorded.
func TestRounds(t *testing.T) {
	const slow, period = 300 * time.Millisecond, 200 * time.Millisecond
	url := testdb.Postgres(t)
	one, two := replica(t, url), replica(t, url)
	withService(t, one)
	cluster := kubesim.New(t, kubesim.Deployment("smoke", "serve", 2, "registry.example.com/smoke/serve@sha256:aa"))
	var mu sync.Mutex
	var starts []time.Time
	cluster.Kube.PrependReactor("get", "deployments", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.(k8stesting.GetAction).GetName() == "serve" {
			mu.Lock()
			starts = append(starts, time.Now())
			mu.Unlock()
			time.Sleep(slow)
		}
		return false, nil, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var wg sync.WaitGroup
	for _, st := range []*store.Store{one, two} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = (&Observer{Store: st, Kube: cluster.Kube, Log: log, Period: period}).Run(ctx)
		}()
	}
	// Three rounds, however long a loaded machine takes to make them: what is held to time is the
	// space between them.
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		mu.Lock()
		n := len(starts)
		mu.Unlock()
		if n >= 3 || time.Now().After(deadline) {
			break
		}
	}
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(starts) < 3 {
		t.Fatalf("%d rounds in 30 seconds", len(starts))
	}
	for i := 1; i < len(starts); i++ {
		if gap := starts[i].Sub(starts[i-1]); gap < slow+period-20*time.Millisecond {
			t.Errorf("round %d started %s after round %d: want at least %s, the read and the period after it", i, gap, i-1, slow+period)
		}
	}
	svs, err := one.Services(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	o := svs[0].Observation
	if o == nil || !o.Found || o.Replicas != 2 || o.Ready != 2 || len(o.Pods) != 2 || o.Images["serve"] != "registry.example.com/smoke/serve@sha256:aa" ||
		o.Error != "" {
		t.Errorf("the observation %+v", o)
	}
}
