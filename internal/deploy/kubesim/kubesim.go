// Package kubesim is a Kubernetes cluster for the deployer's tests: client-go's fake API, and a
// controller that keeps each Deployment's and StatefulSet's pods as their specs say, on a clock
// the test moves. A pod behaves as its first container's image says (Behavior; Healthy by
// default), and a deleted pod takes 5 seconds to go.
package kubesim

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

// Behaviors of an image. Each failure comes within the deployer's 30-second watch.
const (
	Healthy = "healthy" // Ready 10 seconds after it starts
	Crashes = "crashes" // Ready at 10 seconds, crash-loops from 20
	Never   = "never"   // never Ready
	NoPull  = "no-pull" // its image cannot be pulled
	Flaps   = "flaps"   // Ready at 10 seconds, not Ready from 20
	Blips   = "blips"   // Ready at 10 seconds; restarts once at 20, and is Ready again
)

// OwnerLabel names the workload that made a pod: deploy/<name> or sts/<name>.
const OwnerLabel = "sim/owner"

// Cluster is the simulated cluster.
type Cluster struct {
	t        testing.TB
	Kube     *fake.Clientset
	Now      time.Time
	Behavior map[string]string // image → behavior
	started  map[string]time.Time
	deleting map[string]time.Time
	seq      int
	// oldImages are each StatefulSet's images before its template last changed: what its pods
	// below the partition keep.
	oldImages map[string]map[string]string
	current   map[string]map[string]string
}

// New makes a cluster holding objs, reconciled, and an hour on: what runs at the start is long
// settled.
func New(t testing.TB, objs ...runtime.Object) *Cluster {
	c := &Cluster{t: t, Kube: fake.NewClientset(objs...), Now: time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC),
		Behavior: map[string]string{}, started: map[string]time.Time{}, deleting: map[string]time.Time{},
		oldImages: map[string]map[string]string{}, current: map[string]map[string]string{}}
	c.Reconcile()
	c.Advance(time.Hour)
	return c
}

// Deployment is a Deployment of name in namespace, its pods labelled app=<name>, with a
// container of the name on image and a sidecar.
func Deployment(namespace, name string, replicas int32, image string) *appsv1.Deployment {
	labels := map[string]string{"app": name}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: name, Image: image, Env: []corev1.EnvVar{{Name: "MODE", Value: "prod"}}},
					{Name: "sidecar", Image: "reg/sidecar@sha256:s"},
				}},
			},
		},
	}
}

// StatefulSet is a StatefulSet of name in namespace with one container of the name on image.
func StatefulSet(namespace, name string, replicas int32, image string) *appsv1.StatefulSet {
	labels := map[string]string{"app": name}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: image}}},
			},
		},
	}
}

// Advance moves the clock and reconciles.
func (c *Cluster) Advance(d time.Duration) {
	c.Now = c.Now.Add(d)
	c.Reconcile()
}

func (c *Cluster) pods() []corev1.Pod {
	list, err := c.Kube.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		c.t.Fatal(err)
	}
	return list.Items
}

func imagesOf(spec corev1.PodSpec) map[string]string {
	out := map[string]string{}
	for _, k := range spec.Containers {
		out[k.Name] = k.Image
	}
	return out
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

// Reconcile brings pods to what the workloads' specs say, and each pod's status to what its
// image's behavior says now.
func (c *Cluster) Reconcile() {
	ctx := context.Background()
	for key, at := range c.deleting {
		if c.Now.Sub(at) >= 5*time.Second {
			ns, name := splitKey(key)
			_ = c.Kube.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{})
			delete(c.deleting, key)
		}
	}
	owned := map[string][]corev1.Pod{}
	for _, p := range c.pods() {
		owned[p.Namespace+"/"+p.Labels[OwnerLabel]] = append(owned[p.Namespace+"/"+p.Labels[OwnerLabel]], p)
	}
	create := func(ns, owner, name string, tpl corev1.PodTemplateSpec) {
		lbls := maps.Clone(tpl.Labels)
		if lbls == nil {
			lbls = map[string]string{}
		}
		lbls[OwnerLabel] = owner
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: lbls,
			CreationTimestamp: metav1.NewTime(c.Now)}, Spec: *tpl.Spec.DeepCopy()}
		if _, err := c.Kube.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{}); err != nil {
			c.t.Fatal(err)
		}
		c.started[ns+"/"+name] = c.Now
	}
	del := func(p corev1.Pod) {
		if p.DeletionTimestamp != nil {
			return
		}
		p.DeletionTimestamp = &metav1.Time{Time: c.Now}
		if _, err := c.Kube.CoreV1().Pods(p.Namespace).Update(ctx, &p, metav1.UpdateOptions{}); err != nil {
			c.t.Fatal(err)
		}
		c.deleting[p.Namespace+"/"+p.Name] = c.Now
	}
	deps, _ := c.Kube.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
	for _, d := range deps.Items {
		want := 1
		if d.Spec.Replicas != nil {
			want = int(*d.Spec.Replicas)
		}
		var keep []corev1.Pod
		for _, p := range owned[d.Namespace+"/deploy/"+d.Name] {
			if p.DeletionTimestamp != nil {
				continue
			}
			if !maps.Equal(imagesOf(p.Spec), imagesOf(d.Spec.Template.Spec)) || len(keep) >= want {
				del(p)
				continue
			}
			keep = append(keep, p)
		}
		for i := len(keep); i < want; i++ {
			c.seq++
			create(d.Namespace, "deploy/"+d.Name, fmt.Sprintf("%s-%d", d.Name, c.seq), d.Spec.Template)
		}
	}
	sets, _ := c.Kube.AppsV1().StatefulSets("").List(ctx, metav1.ListOptions{})
	for _, s := range sets.Items {
		key := s.Namespace + "/" + s.Name
		tplImages := imagesOf(s.Spec.Template.Spec)
		if cur, ok := c.current[key]; !ok || !maps.Equal(cur, tplImages) {
			if ok {
				c.oldImages[key] = cur
			}
			c.current[key] = tplImages
		}
		partition := int32(0)
		if ru := s.Spec.UpdateStrategy.RollingUpdate; ru != nil && ru.Partition != nil {
			partition = *ru.Partition
		}
		byName := map[string]corev1.Pod{}
		for _, p := range owned[s.Namespace+"/sts/"+s.Name] {
			byName[p.Name] = p
		}
		for i := int32(0); i < *s.Spec.Replicas; i++ {
			name := fmt.Sprintf("%s-%d", s.Name, i)
			want := tplImages
			if i < partition && c.oldImages[key] != nil {
				want = c.oldImages[key]
			}
			p, ok := byName[name]
			switch {
			case ok && p.DeletionTimestamp != nil:
			case ok && !maps.Equal(imagesOf(p.Spec), want):
				del(p)
			case !ok:
				tpl := *s.Spec.Template.DeepCopy()
				tpl.Spec.Containers = withImages(tpl.Spec.Containers, want)
				create(s.Namespace, "sts/"+s.Name, name, tpl)
			}
		}
	}
	for _, p := range c.pods() {
		if p.DeletionTimestamp != nil {
			continue
		}
		age := c.Now.Sub(c.started[p.Namespace+"/"+p.Name])
		st := corev1.PodStatus{Phase: corev1.PodRunning}
		ready := false
		cs := corev1.ContainerStatus{Name: p.Spec.Containers[0].Name, ImageID: "docker-pullable://" + p.Spec.Containers[0].Image}
		switch c.Behavior[p.Spec.Containers[0].Image] {
		case Never:
		case NoPull:
			st.Phase = corev1.PodPending
			cs.ImageID = ""
			cs.State.Waiting = &corev1.ContainerStateWaiting{Reason: "ErrImagePull"}
		case Crashes:
			ready = age >= 10*time.Second && age < 20*time.Second
			if age >= 20*time.Second {
				cs.RestartCount = 1
				cs.State.Waiting = &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}
			}
		case Flaps:
			ready = age >= 10*time.Second && age < 20*time.Second
		case Blips:
			ready = age >= 10*time.Second
			if age >= 20*time.Second {
				cs.RestartCount = 1
			}
		default:
			ready = age >= 10*time.Second
		}
		st.ContainerStatuses = []corev1.ContainerStatus{cs}
		status := corev1.ConditionFalse
		if ready {
			status = corev1.ConditionTrue
		}
		st.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}
		p.Status = st
		if _, err := c.Kube.CoreV1().Pods(p.Namespace).UpdateStatus(ctx, &p, metav1.UpdateOptions{}); err != nil {
			c.t.Fatal(err)
		}
	}
}

func splitKey(key string) (string, string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			return key[:i], key[i+1:]
		}
	}
	return "", key
}

// Running are the pods of a namespace that are up (not going away), each as "<owner> <image>",
// sorted.
func (c *Cluster) Running(namespace string) []string {
	var out []string
	for _, p := range c.pods() {
		if p.Namespace == namespace && p.DeletionTimestamp == nil {
			out = append(out, p.Labels[OwnerLabel]+" "+p.Spec.Containers[0].Image)
		}
	}
	slices.Sort(out)
	return out
}

// Replicas are a Deployment's replicas: 0 for one that does not exist.
func (c *Cluster) Replicas(namespace, name string) int32 {
	d, err := c.Kube.AppsV1().Deployments(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return 0
	}
	if err != nil {
		c.t.Fatal(err)
	}
	return *d.Spec.Replicas
}

// Image is a Deployment's or StatefulSet's container's image in its template.
func (c *Cluster) Image(namespace, kind, name, container string) string {
	var spec corev1.PodSpec
	switch kind {
	case "statefulset":
		s, err := c.Kube.AppsV1().StatefulSets(namespace).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			c.t.Fatal(err)
		}
		spec = s.Spec.Template.Spec
	default:
		d, err := c.Kube.AppsV1().Deployments(namespace).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			c.t.Fatal(err)
		}
		spec = d.Spec.Template.Spec
	}
	return imagesOf(spec)[container]
}
