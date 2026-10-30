// Package scale sizes ok automation's pools of workers to what they are asked for (docs/hosted.md,
// "Hosted automation, as built"): each pool's worker Deployment's replicas, through Kubernetes'
// scale subresource, from its runs running and those admission could claim, up at once and down
// only once fewer have been enough a while — the idlest workers first.
package scale

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/work"
)

// Pool is a pool of workers and its Deployment.
type Pool struct {
	Name                  string
	Namespace, Deployment string
	// Slots are each worker's runs at once; Min and Max bound its workers.
	Slots, Min, Max int32
}

// Deployments reads and sets a Deployment's replicas, and orders which of its pods go first when
// it shrinks.
type Deployments interface {
	Replicas(ctx context.Context, namespace, name string) (int32, error)
	Scale(ctx context.Context, namespace, name string, replicas int32) error
	// Pods are the Deployment's pods' names; Cost sets a pod's cost of being deleted, which a
	// shrinking ReplicaSet deletes the cheapest of first.
	Pods(ctx context.Context, namespace, name string) ([]string, error)
	Cost(ctx context.Context, namespace, pod string, cost int) error
}

// Down is how long a pool must have needed fewer workers before it shrinks.
const Down = 5 * time.Minute

// Every is how often the pools are sized.
const Every = 15 * time.Second

var (
	queuedRuns  = metrics.NewGauge("ok_automation_pool_queued_runs", "Runs queued in a pool of workers.", "pool")
	runningRuns = metrics.NewGauge("ok_automation_pool_running_runs", "Runs running in a pool of workers.", "pool")
	oldestAge   = metrics.NewGauge("ok_automation_pool_oldest_queued_seconds", "How long a pool's oldest queued run has waited.", "pool")
	wanted      = metrics.NewGauge("ok_automation_pool_workers_wanted", "The workers a pool is sized to.", "pool")
)

// Scaler sizes pools.
type Scaler struct {
	Store       *store.Store
	Pools       []Pool
	Deployments Deployments
	Log         *slog.Logger
	now         func() time.Time

	mu    sync.Mutex
	fewer map[string]time.Time // since when each pool has needed fewer workers than it has
}

func (s *Scaler) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// Step sizes every pool once.
func (s *Scaler) Step(ctx context.Context) error {
	var errs []string
	for _, p := range s.Pools {
		if err := s.size(ctx, p); err != nil {
			errs = append(errs, fmt.Sprintf("pool %s: %v", p.Name, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("scale: %s", strings.Join(errs, "; "))
	}
	return nil
}

// want is the workers a demand needs: its running and claimable runs over each worker's slots,
// within the pool's bounds.
func want(p Pool, d store.Demand) int32 {
	slots := int64(max(p.Slots, 1))
	n := int32((d.Running + d.Claimable + slots - 1) / slots)
	return min(max(n, p.Min), p.Max)
}

func (s *Scaler) size(ctx context.Context, p Pool) error {
	d, err := s.Store.PoolDemand(ctx, p.Name)
	if err != nil {
		return err
	}
	now := s.clock()
	queuedRuns.With(p.Name).Set(float64(d.Queued))
	runningRuns.With(p.Name).Set(float64(d.Running))
	if d.Oldest.IsZero() {
		oldestAge.With(p.Name).Set(0)
	} else {
		oldestAge.With(p.Name).Set(now.Sub(d.Oldest).Seconds())
	}
	n := want(p, d)
	wanted.With(p.Name).Set(float64(n))
	have, err := s.Deployments.Replicas(ctx, p.Namespace, p.Deployment)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.fewer == nil {
		s.fewer = map[string]time.Time{}
	}
	if n >= have {
		delete(s.fewer, p.Name)
	} else if _, ok := s.fewer[p.Name]; !ok {
		s.fewer[p.Name] = now
	}
	since := s.fewer[p.Name]
	s.mu.Unlock()
	switch {
	case n > have:
		s.Log.Info("scale: growing a pool", "pool", p.Name, "from", have, "to", n, "running", d.Running, "claimable", d.Claimable)
		return s.Deployments.Scale(ctx, p.Namespace, p.Deployment, n)
	case n < have && now.Sub(since) >= Down:
		// The idlest workers go first: each pod costs the runs it holds.
		pods, err := s.Deployments.Pods(ctx, p.Namespace, p.Deployment)
		if err != nil {
			return err
		}
		for _, pod := range pods {
			if err := s.Deployments.Cost(ctx, p.Namespace, pod, int(held(d.Workers, pod))); err != nil {
				return err
			}
		}
		s.Log.Info("scale: shrinking a pool", "pool", p.Name, "from", have, "to", n, "running", d.Running, "claimable", d.Claimable)
		s.mu.Lock()
		delete(s.fewer, p.Name)
		s.mu.Unlock()
		return s.Deployments.Scale(ctx, p.Namespace, p.Deployment, n)
	}
	return nil
}

// held are the runs a pod's worker holds: its own name's, and its slots' (<pod>/<n>).
func held(workers map[string]int64, pod string) int64 {
	var n int64
	for w, c := range workers {
		if w == pod || strings.HasPrefix(w, pod+"/") {
			n += c
		}
	}
	return n
}

// Work is the pools' sizing, as keyed work: one replica at a time sizes them, every Every.
const Work = "pools"

// Run sizes the pools every Every, on one replica at a time, until ctx ends.
func (s *Scaler) Run(ctx context.Context, ws work.Store) error {
	if err := ws.MarkAt(ctx, Work, 0, time.Now()); err != nil && ctx.Err() == nil {
		s.Log.Warn("scale: scheduling the pools' sizing", "err", err)
	}
	return (&work.Runner{
		Kind:  Work,
		Store: ws,
		Do: func(ctx context.Context, _ int64) error {
			err := s.Step(ctx)
			if merr := ws.MarkAt(ctx, Work, 0, time.Now().Add(Every)); merr != nil && err == nil {
				err = merr
			}
			return err
		},
		Log: s.Log,
	}).Run(ctx)
}

// Kube is Deployments over a cluster: Deployments' scale subresource, their pods by their
// selector, and the pod-deletion-cost annotation.
type Kube struct{ Client kubernetes.Interface }

func (k Kube) Replicas(ctx context.Context, namespace, name string) (int32, error) {
	sc, err := k.Client.AppsV1().Deployments(namespace).GetScale(ctx, name, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	return sc.Spec.Replicas, nil
}

func (k Kube) Scale(ctx context.Context, namespace, name string, replicas int32) error {
	sc, err := k.Client.AppsV1().Deployments(namespace).GetScale(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	sc.Spec = autoscalingv1.ScaleSpec{Replicas: replicas}
	_, err = k.Client.AppsV1().Deployments(namespace).UpdateScale(ctx, name, sc, metav1.UpdateOptions{})
	return err
}

func (k Kube) Pods(ctx context.Context, namespace, name string) ([]string, error) {
	d, err := k.Client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	sel, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := k.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, p := range pods.Items {
		if p.DeletionTimestamp == nil {
			out = append(out, p.Name)
		}
	}
	return out, nil
}

// costAnnotation is the cost of deleting a pod, which a shrinking ReplicaSet deletes the cheapest
// of first.
const costAnnotation = "controller.kubernetes.io/pod-deletion-cost"

func (k Kube) Cost(ctx context.Context, namespace, pod string, cost int) error {
	patch := `{"metadata":{"annotations":{"` + costAnnotation + `":"` + strconv.Itoa(cost) + `"}}}`
	_, err := k.Client.CoreV1().Pods(namespace).Patch(ctx, pod, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}
