// Package observe keeps what the cluster runs of every service in the deployer's database: one
// replica at a time (the observer's lock) reads each service's workload, then waits ten seconds
// from the end of that round before the next, so a slow round never overlaps the next. The
// Services pages read what it recorded, never the cluster.
package observe

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/Cidan/ok/internal/deploy/roll"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/lock"
)

const (
	// Period is how long the observer waits after a round ends before it starts the next.
	Period = 10 * time.Second
	// readTimeout bounds the read of one service's workload, so a cluster that does not answer
	// holds up one service, not the round.
	readTimeout = 10 * time.Second
	// retryLead is how often a replica that is not the observer tries to become it.
	retryLead = 2 * time.Second
)

// Observer reads every service's workload into the store.
type Observer struct {
	Store  *store.Store
	Kube   kubernetes.Interface
	Labels roll.Labels
	Log    *slog.Logger
	Period time.Duration // 0: Period
}

// Run observes until ctx ends: while it holds the observer's lock, a round, then Period's wait.
func (o *Observer) Run(ctx context.Context) error {
	for {
		err := o.Store.LeadObserver(ctx, o.lead)
		if err != nil && !errors.Is(err, lock.ErrNotAcquired) && ctx.Err() == nil {
			o.Log.Warn("observe: leading", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retryLead):
		}
	}
}

// lead runs rounds, each starting Period after the last one ended, until ctx ends — the lock
// lost among them.
func (o *Observer) lead(ctx context.Context) error {
	period := o.Period
	if period == 0 {
		period = Period
	}
	for {
		if err := o.Round(ctx); err != nil && ctx.Err() == nil {
			o.Log.Warn("observe: a round", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(period):
		}
	}
}

// Round reads every service's workload and records it: what runs, or why the cluster could not
// be read.
func (o *Observer) Round(ctx context.Context) error {
	svs, err := o.Store.Services(ctx, 0)
	if err != nil {
		return err
	}
	var errs []error
	for _, sv := range svs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		readCtx, cancel := context.WithTimeout(ctx, readTimeout)
		seen, err := roll.Observe(readCtx, o.Kube, o.Labels, sv.Workload)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		obs := store.Observation{Error: errorText(err)}
		if err == nil {
			obs = observation(seen)
		}
		if err := o.Store.RecordObservation(ctx, sv.ID, obs); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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
