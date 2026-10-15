package roll

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Cidan/ok/internal/deploy/kubesim"
	"github.com/Cidan/ok/internal/workflow"
)

// Observe reads a Deployment mid-canary — blue live, a green pod beside it — then once green is
// live; a StatefulSet; and a workload that does not exist.
func TestObserve(t *testing.T) {
	ctx := context.Background()
	c := newCluster(t, deployment("web", 3, "reg/web@sha256:v1"))
	w := workflow.Workload{Namespace: "svc", Kind: workflow.Deployment, Name: "web"}
	r := &Roller{Kube: c.Kube, Timing: DefaultTiming, Labels: testLabels}
	tgt := target("web", "reg/web@sha256:v2")
	var s State
	for s.Stage != StageCanary || len(s.Seen) == 0 {
		if err := r.Step(ctx, tgt, &s, c.Now); err != nil {
			t.Fatal(err)
		}
		c.Advance(5 * time.Second)
	}
	o, err := Observe(ctx, c.Kube, testLabels, w)
	if err != nil {
		t.Fatal(err)
	}
	if !o.Found || o.Live != Blue || o.Replicas != 3 || o.Ready != 3 || o.Images["web"] != "reg/web@sha256:v1" || len(o.Pods) != 4 {
		t.Fatalf("mid-canary: %+v", o)
	}
	greens := 0
	for _, p := range o.Pods {
		if p.Copy == Green {
			greens++
			if !p.Ready || p.Images["web"] != "reg/web@sha256:v2" || p.Phase != "Running" {
				t.Errorf("the canary %+v", p)
			}
		}
	}
	if greens != 1 {
		t.Errorf("%d green pods", greens)
	}
	for !s.Done() {
		if err := r.Step(ctx, tgt, &s, c.Now); err != nil {
			t.Fatal(err)
		}
		c.Advance(5 * time.Second)
	}
	c.Advance(10 * time.Second)
	if o, _ = Observe(ctx, c.Kube, testLabels, w); o.Live != Green || o.Replicas != 3 || o.Ready != 3 || o.Images["web"] != "reg/web@sha256:v2" || len(o.Pods) != 3 {
		t.Errorf("rolled: %+v", o)
	}

	// A StatefulSet's pods have no copy; one that cannot pull its image says why.
	c.Behavior["reg/gw@sha256:bad"] = kubesim.NoPull
	if _, err := c.Kube.AppsV1().StatefulSets("svc").Create(ctx, kubesim.StatefulSet("svc", "pg", 2, "reg/gw@sha256:bad"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	c.Advance(10 * time.Second)
	o, err = Observe(ctx, c.Kube, testLabels, workflow.Workload{Namespace: "svc", Kind: workflow.StatefulSet, Name: "pg"})
	if err != nil || !o.Found || o.Live != "" || o.Replicas != 2 || o.Ready != 0 || len(o.Pods) != 2 || o.Pods[0].Reason != "container pg: ErrImagePull" {
		t.Errorf("a StatefulSet that cannot pull: %+v (%v)", o, err)
	}
	if o, err := Observe(ctx, c.Kube, testLabels, workflow.Workload{Namespace: "svc", Kind: workflow.Deployment, Name: "none"}); err != nil || o.Found {
		t.Errorf("a workload that does not exist: %+v (%v)", o, err)
	}
}
