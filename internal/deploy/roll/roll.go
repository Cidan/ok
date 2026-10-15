// Package roll rolls a workload onto new images in watched stages, and back
// (docs/deploy.md, "Roll" and "Watch"). A Deployment rolls blue/green: the Deployment
// the installation's manifests declare is blue, and the deployer's copy of it, <name>-green, green; each roll moves the
// idle copy onto the new images and shapes traffic by scaling the two. A StatefulSet rolls in
// place, a partition at a time.
//
// A roll is a State the caller keeps: Step reads the cluster, takes the next action, judges the
// stage it is in, and returns; the caller stores the State and calls again. Every action is
// idempotent, so a roll resumes from its stored State wherever it was interrupted.
package roll

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/Cidan/ok/internal/workflow"
)

// The copies of a Deployment.
const (
	Blue  = "blue"
	Green = "green"
)

// Labels are the label and annotation the deployer marks a green copy with, under a domain the
// installation owns.
type Labels struct {
	// Color marks a green copy, its pods, and its selector, so the two copies' pods stay apart
	// while the workload's Service selects both. The deployer's own pods carry it too, for each
	// copy to know which it is.
	Color string
	// CopyOf annotates a green copy with the Deployment it copies.
	CopyOf string
}

// LabelsFor are the labels under an installation's domain: deploy.ok.<domain>/color, and
// deploy.ok.<domain>/copy-of.
func LabelsFor(domain string) Labels {
	p := "deploy.ok." + domain
	return Labels{Color: p + "/color", CopyOf: p + "/copy-of"}
}

// Target is what a roll rolls: a workload, and the image reference each container rolls to.
type Target struct {
	Namespace string
	Kind      workflow.WorkloadKind
	Name      string
	Images    map[string]string // container → registry/repository@sha256:…
	// Self reports that the workload runs the deployer itself: its live copy keeps running
	// through every stage — it drives the roll — and goes to 0 only after the roll succeeds.
	Self bool
}

// Stage is where a roll is.
type Stage string

const (
	StagePrepare  Stage = "prepare"  // read the workload and ready the copy that rolls
	StageApply    Stage = "apply"    // a StatefulSet's previous images are recorded: put the new ones on it
	StageCanary   Stage = "canary"   // one new pod beside the live ones, watched
	StageFull     Stage = "full"     // every replica new, watched
	StageRollback Stage = "rollback" // back to the live copy, until its pods are Ready
	StageDone     Stage = "done"
)

// Outcome is how a finished roll ended.
type Outcome string

const (
	Unchanged  Outcome = "unchanged"   // the workload already runs the images
	Rolled     Outcome = "rolled"      // every replica runs the images, and passed its watch
	RolledBack Outcome = "rolled back" // a stage failed; the workload runs what it ran before
	Stuck      Outcome = "stuck"       // a stage failed, and the rollback did not come back Ready
)

// State is a roll's progress. The zero State begins a roll.
type State struct {
	Stage    Stage     `json:"stage"`
	Outcome  Outcome   `json:"outcome,omitempty"`
	Reason   string    `json:"reason,omitempty"` // why it rolled back or is stuck
	Live     string    `json:"live,omitempty"`   // a Deployment's copy that was live
	New      string    `json:"new,omitempty"`    // and the one that rolls
	Replicas int32     `json:"replicas"`         // the live copy's replicas: what the roll ends with
	Started  time.Time `json:"started"`          // when the current stage began
	// LiveDown reports that a one-replica roll's live copy is down, so the new one may start,
	// and a multi-replica roll's full stage has scaled the live copy to 0.
	LiveDown bool `json:"live_down,omitempty"`
	// Previous are a StatefulSet's images before the roll, by container, which a rollback
	// restores.
	Previous map[string]string `json:"previous,omitempty"`
	// Seen are the stage's pods seen Ready.
	Seen []string `json:"seen,omitempty"`
	// Retire reports that a roll of the deployer itself succeeded and its old copy is still
	// up: the caller records the success, then calls Retire, which ends the old copy — the
	// process that is likely making the call.
	Retire bool `json:"retire,omitempty"`
	// Events say what the roll did, in order, since the caller last took them (TakeEvents).
	Events []string `json:"-"`
}

// Done reports whether the roll is over.
func (s *State) Done() bool { return s.Stage == StageDone }

func (s *State) logf(format string, args ...any) {
	s.Events = append(s.Events, fmt.Sprintf(format, args...))
}

// TakeEvents returns what the roll did since it was last asked, and forgets it.
func (s *State) TakeEvents() []string {
	out := s.Events
	s.Events = nil
	return out
}

// ConfigError is a roll that cannot happen as the deploy declares it — its workload or a
// container it names does not exist — as opposed to an error talking to the cluster, which a
// later step may not meet.
type ConfigError struct{ Message string }

func (e *ConfigError) Error() string { return e.Message }

func configErrorf(format string, args ...any) error {
	return &ConfigError{Message: fmt.Sprintf(format, args...)}
}

// ungranted explains a refusal to read a workload: the deployer may read only the workloads that
// grant it access (docs/deploy.md, "Grants"). A missing grant does not fix itself, so it is a
// ConfigError. Any other error is returned as it is.
func ungranted(err error, kind, namespace, name string) error {
	if !apierrors.IsForbidden(err) {
		return err
	}
	return configErrorf("ok's deployer holds no grant for %s %s/%s: grant it the workload (docs/deploy.md, \"Grants\")", kind, namespace, name)
}

// Roller rolls workloads through a Kubernetes API.
type Roller struct {
	Kube   kubernetes.Interface
	Timing Timing
	Labels Labels
}

// Step takes a roll's next action and judges its stage at now.
func (r *Roller) Step(ctx context.Context, t Target, s *State, now time.Time) error {
	if len(t.Images) == 0 {
		return configErrorf("a roll names the images it rolls")
	}
	switch t.Kind {
	case workflow.Deployment:
		return r.stepDeployment(ctx, t, s, now)
	case workflow.StatefulSet:
		return r.stepStatefulSet(ctx, t, s, now)
	}
	return configErrorf("a workload is a deployment or a statefulset, not %q", t.Kind)
}

// ─── Deployments ───

func greenName(name string) string { return name + "-green" }

func (r *Roller) copyName(t Target, color string) string {
	if color == Green {
		return greenName(t.Name)
	}
	return t.Name
}

func (r *Roller) getDeployment(ctx context.Context, ns, name string) (*appsv1.Deployment, error) {
	d, err := r.Kube.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return d, ungranted(err, "deployment", ns, name)
}

func replicas(d *appsv1.Deployment) int32 {
	if d == nil || d.Spec.Replicas == nil {
		if d != nil {
			return 1
		}
		return 0
	}
	return *d.Spec.Replicas
}

func (r *Roller) stepDeployment(ctx context.Context, t Target, s *State, now time.Time) error {
	switch s.Stage {
	case "", StagePrepare:
		return r.prepareDeployment(ctx, t, s, now)
	case StageCanary:
		return r.watchDeployment(ctx, t, s, now, 1)
	case StageFull:
		return r.fullDeployment(ctx, t, s, now)
	case StageRollback:
		return r.rollbackDeployment(ctx, t, s, now)
	}
	return nil
}

// prepareDeployment finds the live copy and readies the other on the new images, at 0
// replicas, then begins the first stage.
func (r *Roller) prepareDeployment(ctx context.Context, t Target, s *State, now time.Time) error {
	blue, err := r.getDeployment(ctx, t.Namespace, t.Name)
	if err != nil {
		return err
	}
	if blue == nil {
		return configErrorf("deployment %s/%s does not exist", t.Namespace, t.Name)
	}
	for c := range t.Images {
		if !slices.ContainsFunc(blue.Spec.Template.Spec.Containers, func(k corev1.Container) bool { return k.Name == c }) {
			return configErrorf("deployment %s/%s has no container %s", t.Namespace, t.Name, c)
		}
	}
	green, err := r.getDeployment(ctx, t.Namespace, greenName(t.Name))
	if err != nil {
		return err
	}
	live, liveDep := Blue, blue
	if green != nil && replicas(green) > replicas(blue) {
		live, liveDep = Green, green
	}
	s.Live, s.Replicas = live, replicas(liveDep)
	s.New = Green
	if live == Green {
		s.New = Blue
	}
	if runs(liveDep.Spec.Template, t.Images) {
		s.Stage, s.Outcome = StageDone, Unchanged
		s.logf("deployment %s/%s already runs the images (its %s copy)", t.Namespace, t.Name, live)
		return nil
	}
	if err := r.readyCopy(ctx, t, s.New, blue, green); err != nil {
		return err
	}
	s.logf("the %s copy of deployment %s/%s has the new images; %s is live with %d replica(s)", s.New, t.Namespace, t.Name, live, s.Replicas)
	switch {
	case s.Replicas == 0:
		s.Stage, s.Outcome = StageDone, Rolled
		s.logf("the workload runs no replicas: the %s copy is ready for when it does", s.New)
		return nil
	case s.Replicas == 1:
		return r.enterFull(ctx, t, s, now)
	}
	if err := r.scale(ctx, t, s.New, 1); err != nil {
		return err
	}
	s.Stage, s.Started, s.Seen = StageCanary, now, nil
	s.logf("canary: one %s pod beside %d %s", s.New, s.Replicas, s.Live)
	return nil
}

// runs reports whether a pod template runs the images.
func runs(tpl corev1.PodTemplateSpec, images map[string]string) bool {
	for _, c := range tpl.Spec.Containers {
		if want, ok := images[c.Name]; ok && c.Image != want {
			return false
		}
	}
	return true
}

// readyCopy puts the copy that rolls on the new images, at 0 replicas. Blue keeps its declared
// template, with the images; green is blue's template, with the images and the color label.
func (r *Roller) readyCopy(ctx context.Context, t Target, color string, blue, green *appsv1.Deployment) error {
	api := r.Kube.AppsV1().Deployments(t.Namespace)
	zero := int32(0)
	if color == Blue {
		d := blue.DeepCopy()
		d.Spec.Template.Spec.Containers = withImages(d.Spec.Template.Spec.Containers, t.Images)
		d.Spec.Replicas = &zero
		_, err := api.Update(ctx, d, metav1.UpdateOptions{})
		return err
	}
	tpl := *blue.Spec.Template.DeepCopy()
	tpl.Labels = maps.Clone(tpl.Labels)
	if tpl.Labels == nil {
		tpl.Labels = map[string]string{}
	}
	tpl.Labels[r.Labels.Color] = Green
	tpl.Spec.Containers = withImages(tpl.Spec.Containers, t.Images)
	if green == nil {
		sel := blue.Spec.Selector.DeepCopy()
		if sel.MatchLabels == nil {
			sel.MatchLabels = map[string]string{}
		}
		sel.MatchLabels[r.Labels.Color] = Green
		lbls := maps.Clone(blue.Labels)
		if lbls == nil {
			lbls = map[string]string{}
		}
		lbls[r.Labels.Color] = Green
		_, err := api.Create(ctx, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: greenName(t.Name), Namespace: t.Namespace, Labels: lbls,
				Annotations: map[string]string{r.Labels.CopyOf: t.Name}},
			Spec: appsv1.DeploymentSpec{Replicas: &zero, Selector: sel, Template: tpl, Strategy: blue.Spec.Strategy,
				MinReadySeconds: blue.Spec.MinReadySeconds, RevisionHistoryLimit: blue.Spec.RevisionHistoryLimit},
		}, metav1.CreateOptions{})
		return err
	}
	g := green.DeepCopy()
	g.Spec.Template = tpl
	g.Spec.Replicas = &zero
	_, err := api.Update(ctx, g, metav1.UpdateOptions{})
	return err
}

func withImages(cs []corev1.Container, images map[string]string) []corev1.Container {
	out := make([]corev1.Container, len(cs))
	for i, c := range cs {
		if img, ok := images[c.Name]; ok {
			c.Image = img
		}
		out[i] = c
	}
	return out
}

func (r *Roller) scale(ctx context.Context, t Target, color string, n int32) error {
	api := r.Kube.AppsV1().Deployments(t.Namespace)
	name := r.copyName(t, color)
	for {
		d, err := api.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if d.Spec.Replicas != nil && *d.Spec.Replicas == n {
			return nil
		}
		d.Spec.Replicas = &n
		_, err = api.Update(ctx, d, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			continue
		}
		return err
	}
}

// copyPods are a copy's pods that are not going away: green's by its label, blue's those its
// selector matches that are not green's.
func (r *Roller) copyPods(ctx context.Context, t Target, color string) ([]corev1.Pod, error) {
	blue, err := r.getDeployment(ctx, t.Namespace, t.Name)
	if err != nil || blue == nil {
		return nil, err
	}
	sel, err := metav1.LabelSelectorAsSelector(blue.Spec.Selector)
	if err != nil {
		return nil, err
	}
	list, err := r.Kube.CoreV1().Pods(t.Namespace).List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return nil, err
	}
	var out []corev1.Pod
	for _, p := range list.Items {
		if p.DeletionTimestamp != nil {
			continue
		}
		if (p.Labels[r.Labels.Color] == Green) == (color == Green) {
			out = append(out, p)
		}
	}
	return out, nil
}

// newPods are a copy's pods that run the images.
func newPods(pods []corev1.Pod, images map[string]string) []Pod {
	var out []Pod
	for _, p := range pods {
		if runs(corev1.PodTemplateSpec{Spec: p.Spec}, images) {
			out = append(out, podOf(&p))
		}
	}
	return out
}

// enterFull begins the full stage: every replica on the new copy. With one replica, the live
// copy goes first — the two never run together — unless it is the deployer's own.
func (r *Roller) enterFull(ctx context.Context, t Target, s *State, now time.Time) error {
	s.Stage, s.Started, s.Seen, s.LiveDown = StageFull, now, nil, false
	if s.Replicas == 1 && !t.Self {
		if err := r.scale(ctx, t, s.Live, 0); err != nil {
			return err
		}
		s.logf("the one %s pod goes first", s.Live)
		return nil
	}
	if err := r.scale(ctx, t, s.New, s.Replicas); err != nil {
		return err
	}
	s.logf("full: %d %s pod(s)", s.Replicas, s.New)
	return nil
}

func (r *Roller) fullDeployment(ctx context.Context, t Target, s *State, now time.Time) error {
	if s.Replicas == 1 && !t.Self && !s.LiveDown {
		// The live pod must be gone before the new one starts.
		pods, err := r.copyPods(ctx, t, s.Live)
		if err != nil {
			return err
		}
		if len(pods) > 0 {
			if now.Sub(s.Started) > r.Timing.ReadyWithin {
				return r.startRollback(ctx, t, s, now, fmt.Sprintf("the %s pod did not stop within %s", s.Live, r.Timing.ReadyWithin))
			}
			return nil
		}
		if err := r.scale(ctx, t, s.New, 1); err != nil {
			return err
		}
		s.LiveDown, s.Started = true, now
		s.logf("the %s pod stopped; one %s pod starts", s.Live, s.New)
		return nil
	}
	if err := r.watchDeployment(ctx, t, s, now, int(s.Replicas)); err != nil || s.Stage != StageFull {
		return err
	}
	// Traffic moves to the new copy once all its pods are Ready.
	if s.Replicas > 1 && !t.Self && !s.LiveDown && len(s.Seen) >= int(s.Replicas) {
		if err := r.scale(ctx, t, s.Live, 0); err != nil {
			return err
		}
		s.LiveDown = true
		s.logf("every %s pod is Ready: the %s copy goes to 0", s.New, s.Live)
	}
	return nil
}

// watchDeployment judges the current stage over the new copy's pods, and moves on.
func (r *Roller) watchDeployment(ctx context.Context, t Target, s *State, now time.Time, want int) error {
	pods, err := r.copyPods(ctx, t, s.New)
	if err != nil {
		return err
	}
	j := Evaluate(r.Timing, now, s.Started, want, newPods(pods, t.Images), s.Seen)
	s.Seen = j.Seen
	switch j.Verdict {
	case Failed:
		return r.startRollback(ctx, t, s, now, fmt.Sprintf("%s: %s", s.Stage, j.Reason))
	case Passed:
		if s.Stage == StageCanary {
			s.logf("canary passed")
			return r.enterFull(ctx, t, s, now)
		}
		s.Stage, s.Outcome = StageDone, Rolled
		s.logf("full passed: deployment %s/%s runs the images on its %s copy", t.Namespace, t.Name, s.New)
		if t.Self {
			s.Retire = true
			return nil
		}
		return r.scale(ctx, t, s.Live, 0)
	}
	return nil
}

// Abort rolls a roll back because it was asked to stop: a roll that has changed nothing ends at
// once; one in a stage rolls back, and the caller steps it until its rollback is over.
func (r *Roller) Abort(ctx context.Context, t Target, s *State, now time.Time, reason string) error {
	switch s.Stage {
	case "", StagePrepare:
		s.Stage, s.Outcome, s.Reason = StageDone, RolledBack, reason
		s.logf("stopped before it changed anything: %s", reason)
		return nil
	case StageApply, StageCanary, StageFull:
		// A StatefulSet at StageApply may already run the new images — the step that put them on
		// may have lost its save — so it is put back on the images it recorded and watched;
		// otherwise that changes nothing.
		return r.startRollback(ctx, t, s, now, reason)
	}
	return nil
}

// Retire ends the old copy of a successful roll of the deployer itself, once the caller has
// recorded the success (State.Retire).
func (r *Roller) Retire(ctx context.Context, t Target, s *State) error {
	if !s.Retire {
		return nil
	}
	if err := r.scale(ctx, t, s.Live, 0); err != nil {
		return err
	}
	s.Retire = false
	s.logf("the %s copy, the deployer that drove the roll, goes to 0", s.Live)
	return nil
}

func (r *Roller) startRollback(ctx context.Context, t Target, s *State, now time.Time, reason string) error {
	s.Reason = reason
	s.logf("failed: %s; rolling back", reason)
	s.Stage, s.Started, s.Seen = StageRollback, now, nil
	if t.Kind == workflow.StatefulSet {
		return r.restoreStatefulSet(ctx, t, s)
	}
	if err := r.scale(ctx, t, s.New, 0); err != nil {
		return err
	}
	return r.scale(ctx, t, s.Live, s.Replicas)
}

func (r *Roller) rollbackDeployment(ctx context.Context, t Target, s *State, now time.Time) error {
	pods, err := r.copyPods(ctx, t, s.Live)
	if err != nil {
		return err
	}
	var ready int
	for _, p := range pods {
		if podOf(&p).Ready {
			ready++
		}
	}
	switch {
	case ready >= int(s.Replicas):
		s.Stage, s.Outcome = StageDone, RolledBack
		s.logf("rolled back: %d %s pod(s) Ready", ready, s.Live)
	case now.Sub(s.Started) > r.Timing.ReadyWithin:
		s.Stage, s.Outcome = StageDone, Stuck
		s.logf("the rollback is stuck: %d of %d %s pod(s) Ready after %s", ready, s.Replicas, s.Live, r.Timing.ReadyWithin)
	}
	return nil
}

// ─── StatefulSets ───

func (r *Roller) getStatefulSet(ctx context.Context, t Target) (*appsv1.StatefulSet, error) {
	sts, err := r.Kube.AppsV1().StatefulSets(t.Namespace).Get(ctx, t.Name, metav1.GetOptions{})
	return sts, ungranted(err, "statefulset", t.Namespace, t.Name)
}

func (r *Roller) stepStatefulSet(ctx context.Context, t Target, s *State, now time.Time) error {
	switch s.Stage {
	case "", StagePrepare:
		// The images it replaces are recorded before any change, in a step of their own: the
		// state is saved between steps, and a StatefulSet's roll can restart the very database it
		// is saved to (a database the deployer's own tables are in). Were the change made first and that save
		// lost, the next step would find the new images in place with no record of the old.
		sts, err := r.getStatefulSet(ctx, t)
		if apierrors.IsNotFound(err) {
			return configErrorf("statefulset %s/%s does not exist", t.Namespace, t.Name)
		}
		if err != nil {
			return err
		}
		s.Replicas = 1
		if sts.Spec.Replicas != nil {
			s.Replicas = *sts.Spec.Replicas
		}
		s.Previous = map[string]string{}
		for _, c := range sts.Spec.Template.Spec.Containers {
			if _, ok := t.Images[c.Name]; ok {
				s.Previous[c.Name] = c.Image
			}
		}
		if len(s.Previous) != len(t.Images) {
			return configErrorf("statefulset %s/%s lacks a container the deploy rolls", t.Namespace, t.Name)
		}
		if runs(sts.Spec.Template, t.Images) {
			s.Stage, s.Outcome = StageDone, Unchanged
			s.logf("statefulset %s/%s already runs the images", t.Namespace, t.Name)
			return nil
		}
		s.Stage = StageApply
		s.logf("statefulset %s/%s: its %d pod(s) run %s before the roll", t.Namespace, t.Name, s.Replicas, describeImages(s.Previous))
		return nil
	case StageApply:
		// Putting the images on is the same however often it runs, so a step whose save was lost
		// runs again.
		partition := int32(0)
		stage := StageFull
		if s.Replicas > 1 {
			partition, stage = s.Replicas-1, StageCanary
		}
		if err := r.setStatefulSet(ctx, t, t.Images, partition); err != nil {
			return err
		}
		s.Stage, s.Started, s.Seen = stage, now, nil
		if stage == StageCanary {
			s.logf("canary: statefulset %s/%s's highest pod on the new images", t.Namespace, t.Name)
		} else {
			s.logf("statefulset %s/%s on the new images", t.Namespace, t.Name)
		}
		return nil
	case StageCanary, StageFull:
		want := int(s.Replicas)
		if s.Stage == StageCanary {
			want = 1
		}
		pods, err := r.statefulSetPods(ctx, t)
		if err != nil {
			return err
		}
		j := Evaluate(r.Timing, now, s.Started, want, newPods(pods, t.Images), s.Seen)
		s.Seen = j.Seen
		switch j.Verdict {
		case Failed:
			return r.startRollback(ctx, t, s, now, fmt.Sprintf("%s: %s", s.Stage, j.Reason))
		case Passed:
			if s.Stage == StageCanary {
				if err := r.setStatefulSet(ctx, t, t.Images, 0); err != nil {
					return err
				}
				s.Stage, s.Started, s.Seen = StageFull, now, nil
				s.logf("canary passed; full: every pod on the new images")
				return nil
			}
			s.Stage, s.Outcome = StageDone, Rolled
			s.logf("full passed: statefulset %s/%s runs the images", t.Namespace, t.Name)
		}
		return nil
	case StageRollback:
		pods, err := r.statefulSetPods(ctx, t)
		if err != nil {
			return err
		}
		ready := 0
		for _, p := range newPods(pods, s.Previous) {
			if p.Ready {
				ready++
			}
		}
		switch {
		case ready >= int(s.Replicas):
			s.Stage, s.Outcome = StageDone, RolledBack
			s.logf("rolled back: %d pod(s) Ready on the previous images", ready)
		case now.Sub(s.Started) > r.Timing.ReadyWithin:
			s.Stage, s.Outcome = StageDone, Stuck
			s.logf("the rollback is stuck: %d of %d pod(s) Ready after %s", ready, s.Replicas, r.Timing.ReadyWithin)
		}
	}
	return nil
}

// describeImages words images by container: "api reg/api@sha256:v1, proxy …".
func describeImages(images map[string]string) string {
	names := make([]string, 0, len(images))
	for c := range images {
		names = append(names, c)
	}
	slices.Sort(names)
	for i, c := range names {
		names[i] = c + " " + images[c]
	}
	return strings.Join(names, ", ")
}

func (r *Roller) restoreStatefulSet(ctx context.Context, t Target, s *State) error {
	return r.setStatefulSet(ctx, t, s.Previous, 0)
}

// setStatefulSet puts a StatefulSet's containers on images, rolling the pods at or above
// partition.
func (r *Roller) setStatefulSet(ctx context.Context, t Target, images map[string]string, partition int32) error {
	api := r.Kube.AppsV1().StatefulSets(t.Namespace)
	for {
		sts, err := api.Get(ctx, t.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		sts.Spec.Template.Spec.Containers = withImages(sts.Spec.Template.Spec.Containers, images)
		sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
			Type:          appsv1.RollingUpdateStatefulSetStrategyType,
			RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{Partition: &partition},
		}
		_, err = api.Update(ctx, sts, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			continue
		}
		return err
	}
}

func (r *Roller) statefulSetPods(ctx context.Context, t Target) ([]corev1.Pod, error) {
	sts, err := r.getStatefulSet(ctx, t)
	if err != nil {
		return nil, err
	}
	sel, err := metav1.LabelSelectorAsSelector(sts.Spec.Selector)
	if err != nil {
		return nil, err
	}
	list, err := r.Kube.CoreV1().Pods(t.Namespace).List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return nil, err
	}
	var out []corev1.Pod
	for _, p := range list.Items {
		if p.DeletionTimestamp == nil {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
