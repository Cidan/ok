package roll

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/Cidan/ok/internal/deploy/kubesim"
	"github.com/Cidan/ok/internal/workflow"
)

// cluster is the simulated cluster, as these tests use it: everything in namespace svc.
type cluster struct {
	*kubesim.Cluster
	t *testing.T
}

func newCluster(t *testing.T, objs ...*appsv1.Deployment) *cluster {
	var ro []runtime.Object
	for _, d := range objs {
		ro = append(ro, d)
	}
	return &cluster{Cluster: kubesim.New(t, ro...), t: t}
}

func deployment(name string, replicas int32, image string) *appsv1.Deployment {
	return kubesim.Deployment("svc", name, replicas, image)
}

func (c *cluster) running() []string          { return c.Running("svc") }
func (c *cluster) replicas(name string) int32 { return c.Replicas("svc", name) }

// roll steps a roll to its end, the cluster reconciling and the clock moving 5 seconds between
// steps, and returns its State and everything it said. Each step's State goes through JSON,
// as the deployer stores it, so every step is a resumption.
func (c *cluster) roll(tgt Target, timing Timing) (State, []string) {
	c.t.Helper()
	r := &Roller{Kube: c.Kube, Timing: timing, Labels: testLabels}
	var s State
	var said []string
	for i := 0; i < 2000; i++ {
		if err := r.Step(context.Background(), tgt, &s, c.Now); err != nil {
			c.t.Fatalf("step %d: %v", i, err)
		}
		said = append(said, s.TakeEvents()...)
		if s.Done() {
			return s, said
		}
		data, err := json.Marshal(s)
		if err != nil {
			c.t.Fatal(err)
		}
		s = State{}
		if err := json.Unmarshal(data, &s); err != nil {
			c.t.Fatal(err)
		}
		c.Advance(5 * time.Second)
	}
	c.t.Fatalf("the roll never ended: %+v", s)
	return s, said
}

func target(name string, image string) Target {
	return Target{Namespace: "svc", Kind: workflow.Deployment, Name: name, Images: map[string]string{name: image}}
}

func has(said []string, want string) bool {
	return slices.ContainsFunc(said, func(s string) bool { return strings.Contains(s, want) })
}

// A three-replica Deployment rolls blue to green — a canary beside the live pods, then every
// replica, traffic moving once they are Ready — and the next roll goes back to blue.
func TestRollDeployment(t *testing.T) {
	c := newCluster(t, deployment("web", 3, "reg/web@sha256:v1"))
	start := c.Now
	s, said := c.roll(target("web", "reg/web@sha256:v2"), DefaultTiming)
	if s.Outcome != Rolled || s.Live != Blue || s.New != Green || s.Replicas != 3 {
		t.Fatalf("the roll: %+v\n%s", s, strings.Join(said, "\n"))
	}
	for _, want := range []string{"canary: one green pod beside 3 blue", "canary passed", "full: 3 green pod(s)", "every green pod is Ready: the blue copy goes to 0", "full passed"} {
		if !has(said, want) {
			t.Errorf("the roll never said %q:\n%s", want, strings.Join(said, "\n"))
		}
	}
	if took := c.Now.Sub(start); took < 2*DefaultTiming.Watch || took > 2*DefaultTiming.Watch+30*time.Second {
		t.Errorf("two watched stages took %s", took)
	}
	if c.replicas("web") != 0 || c.replicas("web-green") != 3 {
		t.Errorf("blue %d, green %d", c.replicas("web"), c.replicas("web-green"))
	}
	green, err := c.Kube.AppsV1().Deployments("svc").Get(context.Background(), "web-green", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tpl := green.Spec.Template
	if tpl.Labels[testLabels.Color] != Green || tpl.Labels["app"] != "web" || green.Spec.Selector.MatchLabels[testLabels.Color] != Green ||
		tpl.Spec.Containers[0].Image != "reg/web@sha256:v2" || tpl.Spec.Containers[1].Image != "reg/sidecar@sha256:s" ||
		tpl.Spec.Containers[0].Env[0].Value != "prod" {
		t.Errorf("green is not blue's template with the new image: %+v", tpl)
	}
	c.Advance(10 * time.Second)
	if got := c.running(); !slices.Equal(got, []string{"deploy/web-green reg/web@sha256:v2", "deploy/web-green reg/web@sha256:v2", "deploy/web-green reg/web@sha256:v2"}) {
		t.Errorf("running %v", got)
	}

	// The next roll moves blue onto the new images, and back.
	s, _ = c.roll(target("web", "reg/web@sha256:v3"), DefaultTiming)
	if s.Outcome != Rolled || s.Live != Green || s.New != Blue || c.replicas("web") != 3 || c.replicas("web-green") != 0 {
		t.Errorf("the second roll: %+v; blue %d, green %d", s, c.replicas("web"), c.replicas("web-green"))
	}
	// Images already running roll nothing.
	s, _ = c.roll(target("web", "reg/web@sha256:v3"), DefaultTiming)
	if s.Outcome != Unchanged {
		t.Errorf("an unchanged roll: %+v", s)
	}
}

// A canary that crash-loops rolls back: green to 0, blue as it was.
func TestRollDeploymentCanaryFails(t *testing.T) {
	for name, tc := range map[string]struct {
		behavior, reason string
	}{
		"crash loop":       {kubesim.Crashes, "container web: CrashLoopBackOff"},
		"one restart":      {kubesim.Blips, "restarted 1 time(s)"},
		"never Ready":      {kubesim.Never, "was not Ready within 3m0s"},
		"image not pulled": {kubesim.NoPull, "ErrImagePull"},
		"Ready, then not":  {kubesim.Flaps, "was Ready and is not any more"},
	} {
		t.Run(name, func(t *testing.T) {
			c := newCluster(t, deployment("web", 2, "reg/web@sha256:v1"))
			c.Behavior["reg/web@sha256:bad"] = tc.behavior
			s, said := c.roll(target("web", "reg/web@sha256:bad"), DefaultTiming)
			if s.Outcome != RolledBack || !strings.Contains(s.Reason, "canary") || !strings.Contains(s.Reason, tc.reason) {
				t.Fatalf("the roll: %+v\n%s", s, strings.Join(said, "\n"))
			}
			c.Advance(10 * time.Second)
			if c.replicas("web") != 2 || c.replicas("web-green") != 0 ||
				!slices.Equal(c.running(), []string{"deploy/web reg/web@sha256:v1", "deploy/web reg/web@sha256:v1"}) {
				t.Errorf("after the rollback: blue %d, green %d, running %v", c.replicas("web"), c.replicas("web-green"), c.running())
			}
		})
	}
}

// A one-replica Deployment's pod goes before its replacement starts; a replacement that fails
// brings it back. One whose old pods do not come back is stuck.
func TestRollOneReplica(t *testing.T) {
	c := newCluster(t, deployment("db-app", 1, "reg/app@sha256:v1"))
	s, said := c.roll(target("db-app", "reg/app@sha256:v2"), DefaultTiming)
	if s.Outcome != Rolled || !has(said, "the one blue pod goes first") || has(said, "canary") {
		t.Fatalf("the roll: %+v\n%s", s, strings.Join(said, "\n"))
	}

	c = newCluster(t, deployment("db-app", 1, "reg/app@sha256:v1"))
	c.Behavior["reg/app@sha256:bad"] = kubesim.Crashes
	s, said = c.roll(target("db-app", "reg/app@sha256:bad"), DefaultTiming)
	if s.Outcome != RolledBack || c.replicas("db-app") != 1 || c.replicas("db-app-green") != 0 {
		t.Fatalf("the failed roll: %+v\n%s", s, strings.Join(said, "\n"))
	}

	c = newCluster(t, deployment("db-app", 1, "reg/app@sha256:v1"))
	c.Behavior["reg/app@sha256:bad"] = kubesim.Crashes
	c.Behavior["reg/app@sha256:v1"] = kubesim.Never // the old image no longer starts either
	s, said = c.roll(target("db-app", "reg/app@sha256:bad"), DefaultTiming)
	if s.Outcome != Stuck || !has(said, "the rollback is stuck") {
		t.Fatalf("the stuck roll: %+v\n%s", s, strings.Join(said, "\n"))
	}
}

// The deployer's own roll keeps the old copy running through every stage — the old copy
// drives it — and retires it only once the caller has recorded the success.
func TestRollSelf(t *testing.T) {
	c := newCluster(t, deployment("ok-deploy", 1, "reg/deploy@sha256:v1"))
	tgt := target("ok-deploy", "reg/deploy@sha256:v2")
	tgt.Self = true
	r := &Roller{Kube: c.Kube, Timing: DefaultTiming, Labels: testLabels}
	var s State
	for !s.Done() {
		if err := r.Step(context.Background(), tgt, &s, c.Now); err != nil {
			t.Fatal(err)
		}
		if !s.Done() && c.replicas("ok-deploy") != 1 {
			t.Fatalf("the driving copy went down mid-roll: %+v", s)
		}
		c.Advance(5 * time.Second)
	}
	if s.Outcome != Rolled || !s.Retire || c.replicas("ok-deploy") != 1 || c.replicas("ok-deploy-green") != 1 {
		t.Fatalf("the self roll: %+v", s)
	}
	if err := r.Retire(context.Background(), tgt, &s); err != nil {
		t.Fatal(err)
	}
	if s.Retire || c.replicas("ok-deploy") != 0 {
		t.Errorf("after Retire: %+v, blue %d", s, c.replicas("ok-deploy"))
	}
}

// A StatefulSet rolls in place: its highest pod first, then the rest; a failure restores the
// previous images.
func TestRollStatefulSet(t *testing.T) {
	c := newCluster(t)
	if _, err := c.Kube.AppsV1().StatefulSets("svc").Create(context.Background(), kubesim.StatefulSet("svc", "pg", 2, "reg/gw@sha256:v1"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	c.Reconcile()
	c.Advance(time.Hour)
	tgt := Target{Namespace: "svc", Kind: workflow.StatefulSet, Name: "pg", Images: map[string]string{"pg": "reg/gw@sha256:v2"}}
	s, said := c.roll(tgt, DefaultTiming)
	if s.Outcome != Rolled || !has(said, "canary: statefulset svc/pg's highest pod") || s.Previous["pg"] != "reg/gw@sha256:v1" {
		t.Fatalf("the roll: %+v\n%s", s, strings.Join(said, "\n"))
	}
	c.Advance(10 * time.Second)
	if got := c.running(); !slices.Equal(got, []string{"sts/pg reg/gw@sha256:v2", "sts/pg reg/gw@sha256:v2"}) {
		t.Errorf("running %v", got)
	}

	c.Behavior["reg/gw@sha256:bad"] = kubesim.Never
	tgt.Images["pg"] = "reg/gw@sha256:bad"
	s, said = c.roll(tgt, DefaultTiming)
	if s.Outcome != RolledBack || !strings.Contains(s.Reason, "pg-1 was not Ready") {
		t.Fatalf("the failed roll: %+v\n%s", s, strings.Join(said, "\n"))
	}
	c.Advance(10 * time.Second)
	if got := c.running(); !slices.Equal(got, []string{"sts/pg reg/gw@sha256:v2", "sts/pg reg/gw@sha256:v2"}) {
		t.Errorf("after the rollback, running %v", got)
	}
}

// A StatefulSet's roll records the images it replaces before it changes them, so a step whose
// save is lost — the roll restarting the database its state is saved to — runs again: the roll
// is still watched, and still rolls back to what ran before.
func TestRollStatefulSetLostSave(t *testing.T) {
	for _, c := range []struct {
		name, image string
		want        Outcome
		halt        bool // halted where the lost save left it
	}{
		{"good", "reg/gw@sha256:v2", Rolled, false},
		{"bad", "reg/gw@sha256:bad", RolledBack, false},
		{"halted", "reg/gw@sha256:v2", RolledBack, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			k := newCluster(t)
			if _, err := k.Kube.AppsV1().StatefulSets("svc").Create(context.Background(), kubesim.StatefulSet("svc", "pg", 1, "reg/gw@sha256:v1"), metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			k.Reconcile()
			k.Advance(time.Hour)
			k.Behavior["reg/gw@sha256:bad"] = kubesim.Never
			r := &Roller{Kube: k.Kube, Timing: DefaultTiming, Labels: testLabels}
			tgt := Target{Namespace: "svc", Kind: workflow.StatefulSet, Name: "pg", Images: map[string]string{"pg": c.image}}
			var s State
			if err := r.Step(context.Background(), tgt, &s, k.Now); err != nil {
				t.Fatal(err)
			}
			if s.Stage != StageApply || s.Previous["pg"] != "reg/gw@sha256:v1" {
				t.Fatalf("after preparing: %+v", s)
			}
			if got := k.running(); !slices.Equal(got, []string{"sts/pg reg/gw@sha256:v1"}) {
				t.Fatalf("preparing changed the workload: %v", got)
			}
			saved := s
			// The step that puts the new images on runs, and its save is lost.
			if err := r.Step(context.Background(), tgt, &s, k.Now); err != nil || s.Stage != StageFull {
				t.Fatalf("applying: %+v (%v)", s, err)
			}
			s = saved
			if c.halt {
				if err := r.Abort(context.Background(), tgt, &s, k.Now, "halted by bob"); err != nil || s.Stage != StageRollback {
					t.Fatalf("halting: %+v (%v)", s, err)
				}
			}
			for i := 0; !s.Done(); i++ {
				if i > 200 {
					t.Fatalf("the roll never ended: %+v", s)
				}
				k.Advance(5 * time.Second)
				if err := r.Step(context.Background(), tgt, &s, k.Now); err != nil {
					t.Fatal(err)
				}
			}
			if s.Outcome != c.want || s.Previous["pg"] != "reg/gw@sha256:v1" {
				t.Fatalf("the roll ended %+v, want %s", s, c.want)
			}
			k.Advance(10 * time.Second)
			want := c.image
			if c.want == RolledBack {
				want = "reg/gw@sha256:v1"
			}
			if got := k.running(); !slices.Equal(got, []string{"sts/pg " + want}) {
				t.Errorf("running %v, want %s", got, want)
			}
		})
	}
}

// A workload scaled to 0 rolls nothing but its idle copy; one that lacks a container the
// deploy names, or does not exist, is an error.
func TestRollEdges(t *testing.T) {
	c := newCluster(t, deployment("idle", 0, "reg/idle@sha256:v1"))
	s, _ := c.roll(target("idle", "reg/idle@sha256:v2"), DefaultTiming)
	if s.Outcome != Rolled || c.replicas("idle-green") != 0 {
		t.Errorf("a workload at 0: %+v", s)
	}
	r := &Roller{Kube: c.Kube, Timing: DefaultTiming, Labels: testLabels}
	for name, tgt := range map[string]Target{
		"a missing container": {Namespace: "svc", Kind: workflow.Deployment, Name: "idle", Images: map[string]string{"other": "x"}},
		"a missing workload":  target("nothing", "x"),
		"no images":           {Namespace: "svc", Kind: workflow.Deployment, Name: "idle"},
	} {
		var s State
		var cfg *ConfigError
		if err := r.Step(context.Background(), tgt, &s, c.Now); !errors.As(err, &cfg) {
			t.Errorf("%s: %v, not a configuration error", name, err)
		}
	}
}

// A roll asked to stop mid-stage rolls back; one that has not begun ends at once.
func TestAbort(t *testing.T) {
	c := newCluster(t, deployment("web", 2, "reg/web@sha256:v1"))
	r := &Roller{Kube: c.Kube, Timing: DefaultTiming, Labels: testLabels}
	tgt := target("web", "reg/web@sha256:v2")
	var s State
	for s.Stage != StageFull {
		if err := r.Step(context.Background(), tgt, &s, c.Now); err != nil {
			t.Fatal(err)
		}
		c.Advance(5 * time.Second)
	}
	if err := r.Abort(context.Background(), tgt, &s, c.Now, "halted by bob"); err != nil {
		t.Fatal(err)
	}
	for !s.Done() {
		c.Advance(5 * time.Second)
		if err := r.Step(context.Background(), tgt, &s, c.Now); err != nil {
			t.Fatal(err)
		}
	}
	c.Advance(10 * time.Second)
	if s.Outcome != RolledBack || s.Reason != "halted by bob" || c.replicas("web") != 2 || c.replicas("web-green") != 0 ||
		!slices.Equal(c.running(), []string{"deploy/web reg/web@sha256:v1", "deploy/web reg/web@sha256:v1"}) {
		t.Errorf("after the abort: %+v, running %v", s, c.running())
	}
	var fresh State
	if err := r.Abort(context.Background(), tgt, &fresh, c.Now, "halted"); err != nil || fresh.Outcome != RolledBack || !fresh.Done() {
		t.Errorf("aborting a roll that has not begun: %+v (%v)", fresh, err)
	}
}

// testLabels are the labels of an installation at example.com.
var testLabels = LabelsFor("example.com")
