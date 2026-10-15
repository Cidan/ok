package roll

import (
	"context"
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

// Observation is a workload as it runs: which copy is live, what it wants and has, the images its
// template names, and every pod — with the image each container actually runs.
type Observation struct {
	Found    bool   // the workload exists
	Live     string // a Deployment's live copy, Blue or Green; "" for a StatefulSet
	Replicas int32  // the live copy's wanted replicas
	Ready    int32  // its pods that are Ready and run its template's images
	Images   map[string]string
	Pods     []ObservedPod
}

// ObservedPod is one of a workload's pods.
type ObservedPod struct {
	Name        string
	Copy        string // Blue or Green; "" for a StatefulSet's
	Ready       bool
	Restarts    int32
	Phase       string
	Reason      string            // why it is not running: a waiting container's reason, or its failure
	Images      map[string]string // container → the image it runs: the digest the node pulled, once it has
	Started     time.Time
	Terminating bool
}

// Observe reads a workload as it runs.
func Observe(ctx context.Context, kube kubernetes.Interface, labels Labels, w workflow.Workload) (Observation, error) {
	if w.Kind == workflow.StatefulSet {
		return observeStatefulSet(ctx, kube, w)
	}
	apps := kube.AppsV1().Deployments(w.Namespace)
	blue, err := apps.Get(ctx, w.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return Observation{}, nil
	}
	if err != nil {
		return Observation{}, ungranted(err, "deployment", w.Namespace, w.Name)
	}
	green, err := apps.Get(ctx, greenName(w.Name), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		green, err = nil, nil
	}
	if err != nil {
		return Observation{}, err
	}
	live, liveDep := Blue, blue
	if green != nil && replicas(green) > replicas(blue) {
		live, liveDep = Green, green
	}
	o := Observation{Found: true, Live: live, Replicas: replicas(liveDep), Images: imagesOf(liveDep.Spec.Template.Spec)}
	sel, err := metav1.LabelSelectorAsSelector(blue.Spec.Selector)
	if err != nil {
		return o, err
	}
	pods, err := kube.CoreV1().Pods(w.Namespace).List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return o, err
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		copyOf := Blue
		if p.Labels[labels.Color] == Green {
			copyOf = Green
		}
		op := observePod(p, copyOf)
		if copyOf == live && op.Ready && !op.Terminating && runs(corev1.PodTemplateSpec{Spec: p.Spec}, o.Images) {
			o.Ready++
		}
		o.Pods = append(o.Pods, op)
	}
	sortPods(o.Pods)
	return o, nil
}

func observeStatefulSet(ctx context.Context, kube kubernetes.Interface, w workflow.Workload) (Observation, error) {
	sts, err := kube.AppsV1().StatefulSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return Observation{}, nil
	}
	if err != nil {
		return Observation{}, ungranted(err, "statefulset", w.Namespace, w.Name)
	}
	o := Observation{Found: true, Replicas: statefulSetReplicas(sts), Images: imagesOf(sts.Spec.Template.Spec)}
	sel, err := metav1.LabelSelectorAsSelector(sts.Spec.Selector)
	if err != nil {
		return o, err
	}
	pods, err := kube.CoreV1().Pods(w.Namespace).List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return o, err
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		op := observePod(p, "")
		if op.Ready && !op.Terminating && runs(corev1.PodTemplateSpec{Spec: p.Spec}, o.Images) {
			o.Ready++
		}
		o.Pods = append(o.Pods, op)
	}
	sortPods(o.Pods)
	return o, nil
}

func statefulSetReplicas(s *appsv1.StatefulSet) int32 {
	if s.Spec.Replicas == nil {
		return 1
	}
	return *s.Spec.Replicas
}

func imagesOf(spec corev1.PodSpec) map[string]string {
	out := map[string]string{}
	for _, c := range spec.Containers {
		out[c.Name] = c.Image
	}
	return out
}

// observePod reads a pod: the image each container runs is the digest its status reports once the
// node has pulled it, and its spec's image until then.
func observePod(p *corev1.Pod, copyOf string) ObservedPod {
	w := podOf(p)
	op := ObservedPod{Name: p.Name, Copy: copyOf, Ready: w.Ready, Restarts: w.Restarts, Phase: string(p.Status.Phase),
		Images: imagesOf(p.Spec), Started: p.CreationTimestamp.Time, Terminating: p.DeletionTimestamp != nil}
	switch {
	case w.Stuck != "":
		op.Reason = w.Stuck
	case w.Failed != "":
		op.Reason = w.Failed
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.ImageID != "" {
			op.Images[cs.Name] = pulledImage(cs.ImageID)
		}
	}
	return op
}

// pulledImage is a container status's image ID as a reference: the runtime may prefix it with a
// scheme (docker-pullable://).
func pulledImage(id string) string {
	for _, prefix := range []string{"docker-pullable://", "docker://"} {
		if rest, ok := strings.CutPrefix(id, prefix); ok {
			return rest
		}
	}
	return id
}

func sortPods(pods []ObservedPod) {
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Copy != pods[j].Copy {
			return pods[i].Copy < pods[j].Copy
		}
		return pods[i].Name < pods[j].Name
	})
}
