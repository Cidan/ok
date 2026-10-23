package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/Cidan/ok/internal/deploy/kubesim"
	"github.com/Cidan/ok/internal/deploy/roll"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/deploy/tunnel"
)

// agentCluster is a repository's cluster through its owner's agent, which comes and goes.
type agentCluster struct {
	kube kubernetes.Interface

	mu        sync.Mutex
	connected bool
	migrated  []string
}

func (c *agentCluster) set(connected bool) {
	c.mu.Lock()
	c.connected = connected
	c.mu.Unlock()
}

func (c *agentCluster) For(context.Context, int64) (Cluster, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.connected {
		return Cluster{}, fmt.Errorf("%w (acme's)", tunnel.ErrNoAgent)
	}
	return Cluster{Kube: c.kube, Migrate: func(_ context.Context, _ int64, _ []byte, target string, _ []string, _ map[string][]byte) (Applied, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.migrated = append(c.migrated, target)
		return Applied{Output: "applied at the agent"}, nil
	}}, nil
}

// A repository whose owner's agent is not connected has its plan wait, quietly; once it connects,
// its deployments run, their migrations at the agent rather than the deployer; and when the agent
// goes mid-roll, the deployment says once that it waits, and goes on once it is back.
func TestThroughClusters(t *testing.T) {
	e := newEnv(t, kubesim.Deployment("web", "web", 3, "old/web:1"), kubesim.Deployment("api", "api", 1, "old/api:1"))
	e.kinds()
	ac := &agentCluster{kube: e.cluster.Kube}
	en := New(Config{Store: e.store, Kube: nil, Clusters: ac, Runs: e.runs, Build: e.build, Main: e.repo, Source: e.repo, Tree: e.tree,
		Labels: roll.LabelsFor("example.com"), Checkout: e.repo, Timing: roll.DefaultTiming,
		Self: Self{Namespace: "ok-deploy", Name: "ok-deploy", Color: roll.Blue}, Principal: principal,
		Now: func() time.Time { return e.cluster.Now }})
	e.land(1, map[string]string{".ok/workflow/deploy.yaml": workflowFile, "web/web.go": "v1"})
	for range 3 {
		e.tick(en)
	}
	if n := len(e.deployments()); n != 0 {
		t.Fatalf("%d deployments planned with no agent connected", n)
	}
	ac.set(true)
	e.until(en, "web to reach its canary", func() bool { return e.cluster.Replicas("web", "web-green") == 1 })
	ac.set(false)
	for range 5 {
		e.tick(en)
	}
	web := e.byName("web")
	if web.Status != store.Running {
		t.Errorf("web while its agent is away: %s (%s)", web.Status, web.Reason)
	}
	if log := e.log(web); strings.Count(log, "waiting: the organization's deploy agent is not connected") != 1 {
		t.Errorf("web's log says it waits not once:\n%s", log)
	}
	ac.set(true)
	e.until(en, "everything to settle", e.settled)
	for _, d := range e.deployments() {
		if d.Status != store.Succeeded {
			t.Errorf("%s: %s (%s)\n%s", d.Name, d.Status, d.Reason, e.log(d))
		}
	}
	if !slices.Equal(ac.migrated, []string{"//web/schema:apply", "//data:apply"}) || len(e.build.applied) != 0 {
		t.Errorf("migrations at the agent %v, at the deployer %v", ac.migrated, e.build.applied)
	}
	if log := e.log(e.byName("web")); !strings.Contains(log, "applying //web/schema:apply, in its cluster by its organization's deploy agent") {
		t.Errorf("web's log does not say where it migrated:\n%s", log)
	}
}

// Each repository deploys to its owner's agent if its owner designated one; to the deployer's own
// cluster otherwise, in an installation alone.
func TestAgentClusters(t *testing.T) {
	ctx := context.Background()
	local := kubesim.New(t).Kube
	owners := map[int64]string{1: "org_acme", 2: "org_globex", 3: ""}
	c := func(hosted bool) *AgentClusters {
		return &AgentClusters{Local: local, Hub: tunnel.NewHub(tunnel.HubConfig{}), Hosted: hosted,
			Owner:      func(_ context.Context, repo int64) (string, error) { return owners[repo], nil },
			Designated: func(_ context.Context, account string) (bool, error) { return account == "org_acme", nil }}
	}
	for _, tc := range []struct {
		hosted bool
		repo   int64
		want   string // local, agent, or waits
	}{
		{false, 1, "agent"}, {false, 2, "local"}, {false, 3, "local"},
		{true, 1, "agent"}, {true, 2, "waits"}, {true, 3, "waits"},
	} {
		got, err := c(tc.hosted).For(ctx, tc.repo)
		var is string
		switch {
		case errors.Is(err, tunnel.ErrNoAgent):
			is = "waits"
		case err != nil:
			t.Fatalf("hosted %v, repository %d: %v", tc.hosted, tc.repo, err)
		case got.Kube == local && got.Migrate == nil:
			is = "local"
		case got.Kube != local && got.Migrate != nil:
			is = "agent"
		}
		if is != tc.want {
			t.Errorf("hosted %v, repository %d: %s, want %s", tc.hosted, tc.repo, is, tc.want)
		}
	}
}
