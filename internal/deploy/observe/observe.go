// Package observe keeps what the cluster runs of every service in the deployer's database. Each
// service's reading is keyed work (store.ObserveWork; internal/work), due Period after its last
// read ended: whichever replica takes it reads the service's workload and records it, fenced by
// its lease, so a service is read by one replica at a time, every replica shares the reading, and
// none leads. The Services pages read what it recorded, never the cluster.
package observe

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/Cidan/ok/internal/deploy/roll"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/work"
)

const (
	// Period is how long after a service's read ends it is read again.
	Period = 10 * time.Second
	// readTimeout bounds the read of one service's workload, so a cluster that does not answer
	// holds up that service's reading alone.
	readTimeout = 10 * time.Second
)

// Observer reads every service's workload into the store.
type Observer struct {
	Store *store.Store
	// Kube is the deployer's own cluster; Cluster, when set, is a repository's (docs/hosted.md,
	// "Deploy agents, as built").
	Kube    kubernetes.Interface
	Cluster func(ctx context.Context, repo int64) (kubernetes.Interface, error)
	Labels  roll.Labels
	Log     *slog.Logger
	Period  time.Duration // 0: Period
	// Holder names this replica's leases (internal/work). Default work.NewHolder.
	Holder string
}

func (o *Observer) period() time.Duration {
	if o.Period == 0 {
		return Period
	}
	return o.Period
}

// Run observes until ctx ends: each service as it comes due — first when a plan declares it, then
// a Period after each read. A reader that dies reading brings nothing back of its own: its job
// does, once its lease lapses.
func (o *Observer) Run(ctx context.Context) error {
	return (&work.Runner{
		Kind:   store.ObserveWork,
		Store:  o.Store.Work(),
		Do:     o.observe,
		Holder: o.Holder,
		Log:    o.Log,
	}).Run(ctx)
}

// Round reads every service's workload now, and records it, each under its lease: one another
// replica is reading is left to it.
func (o *Observer) Round(ctx context.Context) error {
	if o.Holder == "" {
		o.Holder = work.NewHolder()
	}
	svs, err := o.Store.Services(ctx, 0)
	if err != nil {
		return err
	}
	var errs []error
	for _, sv := range svs {
		held, release, ok, err := work.Claim(ctx, o.Store.Work(), store.ObserveWork, sv.ID, o.Holder, time.Minute)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !ok {
			continue
		}
		err = o.observe(held, sv.ID)
		release()
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// observe reads one service's workload and records it — what runs, or why the cluster could not
// be read — then marks it due a Period after.
func (o *Observer) observe(ctx context.Context, id int64) error {
	sv, err := o.Store.GetService(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil // no longer declared
	}
	if err != nil {
		return err
	}
	readCtx, cancel := context.WithTimeout(ctx, readTimeout)
	kube := o.Kube
	if o.Cluster != nil {
		kube, err = o.Cluster(readCtx, sv.Repository)
	}
	var seen roll.Observation
	if err == nil {
		seen, err = roll.Observe(readCtx, kube, o.Labels, sv.Workload)
	}
	cancel()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	obs := store.Observation{Error: errorText(err)}
	if err == nil {
		obs = observation(seen)
	}
	if err := o.Store.RecordObservation(ctx, sv.ID, obs); err != nil {
		return err
	}
	return o.Store.Work().MarkAt(ctx, store.ObserveWork, sv.ID, time.Now().Add(o.period()))
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// observation is a workload as the store records it.
func observation(seen roll.Observation) store.Observation {
	out := store.Observation{Found: seen.Found, Live: seen.Live, Replicas: seen.Replicas, Ready: seen.Ready, Images: seen.Images}
	for _, p := range seen.Pods {
		out.Pods = append(out.Pods, store.ObservedPod{Name: p.Name, Copy: p.Copy, Ready: p.Ready, Restarts: p.Restarts, Phase: p.Phase,
			Reason: p.Reason, Images: p.Images, Started: p.Started, Terminating: p.Terminating})
	}
	return out
}
