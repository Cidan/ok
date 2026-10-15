package roll

import (
	"context"
	"errors"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clienttesting "k8s.io/client-go/testing"

	"github.com/Cidan/ok/internal/deploy/kubesim"
	"github.com/Cidan/ok/internal/workflow"
)

// refuse makes the cluster refuse every read of the resource, as RBAC refuses the deployer a
// workload it holds no grant for.
func refuse(c *kubesim.Cluster, resource string) {
	c.Kube.PrependReactor("get", resource, func(a clienttesting.Action) (bool, runtime.Object, error) {
		name := a.(clienttesting.GetAction).GetName()
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: resource}, name, errors.New("RBAC: no grant"))
	})
}

// A workload that grants the deployer nothing is a configuration error that names the missing
// grant, in a roll and on the Services page alike; it is never read as a workload that does not
// exist.
func TestUngranted(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		kind     workflow.WorkloadKind
		resource string
		want     string
	}{
		"a deployment":  {workflow.Deployment, "deployments", "ok's deployer holds no grant for deployment svc/web"},
		"a statefulset": {workflow.StatefulSet, "statefulsets", "ok's deployer holds no grant for statefulset svc/web"},
	} {
		t.Run(name, func(t *testing.T) {
			c := kubesim.New(t, kubesim.Deployment("svc", "web", 2, "reg/web@sha256:v1"))
			if tc.kind == workflow.StatefulSet {
				if _, err := c.Kube.AppsV1().StatefulSets("svc").Create(ctx, kubesim.StatefulSet("svc", "web", 2, "reg/web@sha256:v1"), metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			refuse(c, tc.resource)

			r := &Roller{Kube: c.Kube, Timing: DefaultTiming, Labels: testLabels}
			tgt := Target{Namespace: "svc", Kind: tc.kind, Name: "web", Images: map[string]string{"web": "reg/web@sha256:v2"}}
			var s State
			err := r.Step(ctx, tgt, &s, c.Now)
			var cfg *ConfigError
			if !errors.As(err, &cfg) || !strings.Contains(cfg.Message, tc.want) || !strings.Contains(cfg.Message, "grant it the workload") {
				t.Errorf("a roll: %v, want a ConfigError saying %q", err, tc.want)
			}

			o, err := Observe(ctx, c.Kube, testLabels, workflow.Workload{Namespace: "svc", Kind: tc.kind, Name: "web"})
			if err == nil || !strings.Contains(err.Error(), tc.want) || o.Found {
				t.Errorf("observing: %+v, %v, want an error saying %q", o, err, tc.want)
			}
		})
	}
}
