// Package work runs keyed work without leaders (docs/hosted.md, "Work without leaders"): each
// piece of work — a repository's landing queue, its garbage collection — is a row of the role's
// database, marked when what the work acts on changes, and leased to one replica at a time by
// whichever replica takes it first. Every replica runs every kind of work; each key is done by one
// at a time, by many replicas between them.
package work

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Store is where a role keeps its keyed work (pgdata's work operations, on PostgreSQL).
type Store interface {
	// Mark marks key's work of kind.
	Mark(ctx context.Context, kind string, key int64) error
	// Take leases key's work to holder for ttl, clearing its mark, if no one holds it or its
	// holder's lease lapsed; it reports whether it did.
	Take(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error)
	// Renew extends holder's lease by ttl from now; false if the lease is another's.
	Renew(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error)
	// Release ends holder's lease, unless the work was marked meanwhile, when it clears the mark
	// and keeps the lease, reporting again.
	Release(ctx context.Context, kind string, key int64, holder string) (again bool, err error)
	// Due are up to limit keys of kind's work marked and held by no one, or whose holder's lease
	// lapsed.
	Due(ctx context.Context, kind string, limit int) ([]int64, error)
}

// ErrLost is the cause of the end of the context a Runner does work with when its lease is lost.
var ErrLost = errors.New("work: the lease was lost")

// Runner does one kind of work on this replica.
type Runner struct {
	Kind  string
	Store Store
	// Do does key's work, with a context that ends if the lease is lost. An error is logged, and
	// the work is marked to be done again a while later.
	Do func(ctx context.Context, key int64) error
	// Watch, if set, starts watching for marks and returns the wait, which returns once one came,
	// max passes, or ctx ends; unset, the runner looks every Idle.
	Watch func() func(ctx context.Context, max time.Duration) error
	// Sweep, if set, runs every Idle before the runner looks for work: it marks what no change
	// marks (the passing of time).
	Sweep func(ctx context.Context) error
	// Idle bounds how long the runner waits between looks. Default ten seconds.
	Idle time.Duration
	// TTL is a lease's length, renewed every third of it. Default thirty seconds.
	TTL time.Duration
	// Workers bounds the keys this replica does at once. Default four.
	Workers int
	// Holder names this replica's leases. Default its host, process, and a random suffix.
	Holder string
	Log    *slog.Logger
}

// due is how many keys one look takes.
const due = 100

// Run does the kind's work as it is marked until ctx ends, then lets go of what it holds.
func (r *Runner) Run(ctx context.Context) error {
	r.defaults()
	sem := make(chan struct{}, r.Workers)
	var mu sync.Mutex
	doing := map[int64]bool{}
	var wg sync.WaitGroup
	defer wg.Wait()
	lastSweep := time.Time{}
	for ctx.Err() == nil {
		var wait func(context.Context, time.Duration) error
		if r.Watch != nil {
			wait = r.Watch()
		}
		if r.Sweep != nil && time.Since(lastSweep) >= r.Idle {
			lastSweep = time.Now()
			if err := r.Sweep(ctx); err != nil && ctx.Err() == nil {
				r.Log.Warn("work: sweeping", "kind", r.Kind, "err", err)
			}
		}
		keys, err := r.Store.Due(ctx, r.Kind, due)
		if err != nil && ctx.Err() == nil {
			r.Log.Warn("work: looking for work", "kind", r.Kind, "err", err)
		}
		started := 0
	start:
		for _, key := range keys {
			mu.Lock()
			busy := doing[key]
			mu.Unlock()
			if busy {
				continue
			}
			// Every worker busy, the rest waits for one to finish.
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				break start
			}
			started++
			mu.Lock()
			doing[key] = true
			mu.Unlock()
			wg.Go(func() {
				defer func() {
					mu.Lock()
					delete(doing, key)
					mu.Unlock()
					<-sem
				}()
				r.one(ctx, key)
			})
		}
		if len(keys) == due && started > 0 {
			continue // more may be due than one look takes
		}
		if wait == nil {
			select {
			case <-ctx.Done():
			case <-time.After(r.Idle):
			}
			continue
		}
		if err := wait(ctx, r.Idle); err != nil && ctx.Err() == nil {
			r.Log.Warn("work: waiting", "kind", r.Kind, "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
	return nil
}

func (r *Runner) defaults() {
	if r.Idle <= 0 {
		r.Idle = 10 * time.Second
	}
	if r.TTL <= 0 {
		r.TTL = 30 * time.Second
	}
	if r.Workers <= 0 {
		r.Workers = 4
	}
	if r.Holder == "" {
		host, _ := os.Hostname()
		r.Holder = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), rand.Text()[:8])
	}
	if r.Log == nil {
		r.Log = slog.Default()
	}
}

// one takes key's work, if no one holds it, and does it until it stays unmarked.
func (r *Runner) one(ctx context.Context, key int64) {
	taken, err := r.Store.Take(ctx, r.Kind, key, r.Holder, r.TTL)
	if err != nil {
		if ctx.Err() == nil {
			r.Log.Warn("work: taking", "kind", r.Kind, "key", key, "err", err)
		}
		return
	}
	if !taken {
		return
	}
	held, lost := context.WithCancelCause(ctx)
	defer lost(nil)
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		tick := time.NewTicker(r.TTL / 3)
		defer tick.Stop()
		for {
			select {
			case <-held.Done():
				return
			case <-tick.C:
				if ok, err := r.Store.Renew(held, r.Kind, key, r.Holder, r.TTL); err != nil || !ok {
					if held.Err() == nil {
						r.Log.Warn("work: lost a lease", "kind", r.Kind, "key", key, "err", err)
					}
					lost(ErrLost)
					return
				}
			}
		}
	}()
	defer func() { lost(nil); <-renewed }()
	for {
		err := r.Do(held, key)
		if errors.Is(context.Cause(held), ErrLost) {
			return // another holds it now
		}
		// Released with a context of its own: on shutdown, what this replica held is let go at
		// once, rather than when its lease lapses.
		bg, done := context.WithTimeout(context.WithoutCancel(ctx), r.TTL)
		if err != nil || ctx.Err() != nil {
			if err != nil && ctx.Err() == nil {
				r.Log.Warn("work: doing", "kind", r.Kind, "key", key, "err", err)
			}
			// Done again a while later, by whichever replica looks next.
			_, rerr := r.Store.Release(bg, r.Kind, key, r.Holder)
			if merr := r.Store.Mark(bg, r.Kind, key); merr != nil || rerr != nil {
				r.Log.Warn("work: letting go", "kind", r.Kind, "key", key, "err", errors.Join(rerr, merr))
			}
			done()
			return
		}
		again, err := r.Store.Release(bg, r.Kind, key, r.Holder)
		done()
		if err != nil {
			r.Log.Warn("work: releasing", "kind", r.Kind, "key", key, "err", err)
			return
		}
		if !again {
			return
		}
	}
}
