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

	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/data"
)

// Store is where a role keeps its keyed work (pgdata's work operations, on PostgreSQL).
type Store interface {
	// Mark marks key's work of kind, due now.
	Mark(ctx context.Context, kind string, key int64) error
	// MarkAt marks key's work of kind, due at at — or sooner, if it is marked so already.
	MarkAt(ctx context.Context, kind string, key int64, at time.Time) error
	// Take leases key's work to holder for ttl, clearing its mark, if no one holds it or its
	// holder's lease lapsed — making its row if it has none; it reports whether it did.
	Take(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error)
	// Renew extends holder's lease by ttl from now; false if the lease is another's.
	Renew(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error)
	// Release ends holder's lease, unless the work was marked meanwhile, due now, when it clears
	// the mark and keeps the lease, reporting again; a mark due later stays.
	Release(ctx context.Context, kind string, key int64, holder string) (again bool, err error)
	// Let ends holder's lease and leaves the work's mark as it is: work a claim (Claim) held while
	// it was marked is done by whoever takes it next.
	Let(ctx context.Context, kind string, key int64, holder string) error
	// Due are up to limit keys of kind's work marked, due, and held by no one, or whose holder's
	// lease lapsed.
	Due(ctx context.Context, kind string, limit int) ([]int64, error)
}

// ErrLost is the cause of the end of the context a Runner does work with when its lease is lost.
var ErrLost = errors.New("work: the lease was lost")

// Runner does one kind of work on this replica.
type Runner struct {
	Kind  string
	Store Store
	// Do does key's work, with a context that ends if the lease is lost and carries the lease's
	// fence (data.WithFence), so what it writes through a store that checks it is never written
	// once another holds the lease. An error is logged, and the work is marked to be done again a
	// while later.
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
		r.Holder = NewHolder()
	}
	if r.Log == nil {
		r.Log = slog.Default()
	}
}

// NewHolder names this process's leases: its host, its process, and a random suffix.
func NewHolder() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), rand.Text()[:8])
}

// hold keeps holder's lease on key's work, renewing it every third of ttl, until stop is called:
// the context it returns ends with ErrLost if a renewal fails, and with ctx otherwise.
func hold(ctx context.Context, s Store, kind string, key int64, holder string, ttl time.Duration, log *slog.Logger) (held context.Context, stop func()) {
	held, lost := context.WithCancelCause(ctx)
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		tick := time.NewTicker(ttl / 3)
		defer tick.Stop()
		for {
			select {
			case <-held.Done():
				return
			case <-tick.C:
				if ok, err := s.Renew(held, kind, key, holder, ttl); err != nil || !ok {
					if held.Err() == nil {
						log.Warn("work: lost a lease", "kind", kind, "key", key, "err", err)
					}
					lost(ErrLost)
					return
				}
			}
		}
	}()
	return held, func() { lost(nil); <-renewed }
}

// Claim takes holder's lease on key's work, if no one holds it — ok false if another does — and
// keeps it, renewed, until release: the context it returns ends with ErrLost if the lease is lost,
// and carries its fence, as Runner.Do's does.
// release lets the lease go at once, rather than when it would lapse, and leaves a mark made
// meanwhile for whoever takes the work next (Store.Let): a claimer does its work once, and Release,
// which keeps the lease for a runner to do the work again, would hold it to its end unused.
func Claim(ctx context.Context, s Store, kind string, key int64, holder string, ttl time.Duration) (held context.Context, release func(), ok bool, err error) {
	if ok, err = s.Take(ctx, kind, key, holder, ttl); err != nil || !ok {
		return nil, nil, ok, err
	}
	held, stop := hold(ctx, s, kind, key, holder, ttl, slog.Default())
	return data.WithFence(held, data.Fence{Kind: kind, Key: key, Holder: holder}), func() {
		stop()
		bg, done := context.WithTimeout(context.WithoutCancel(ctx), ttl)
		defer done()
		if err := s.Let(bg, kind, key, holder); err != nil {
			slog.Warn("work: letting go", "kind", kind, "key", key, "err", err)
		}
	}, true, nil
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
	held, stop := hold(ctx, r.Store, r.Kind, key, r.Holder, r.TTL, r.Log)
	defer stop()
	held = data.WithFence(held, data.Fence{Kind: r.Kind, Key: key, Holder: r.Holder})
	for {
		err := r.Do(held, key)
		if errors.Is(context.Cause(held), ErrLost) {
			return // another holds it now
		}
		if _, moved := cells.MovedOf(err); moved {
			// Its repository moved to another cell, which does its work now (docs/hosted.md,
			// "Cells, as built").
			err = nil
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
