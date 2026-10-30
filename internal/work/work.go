// Package work runs keyed work without leaders (docs/hosted.md, "Work without leaders"): each
// piece of work — a repository's landing queue, its garbage collection — is a row of the role's
// database, marked when what the work acts on changes, and a job on the bus (jobs.go). A replica's
// lease takes the job, and the work's row lease after it: every replica runs every kind of work,
// each key done by one at a time, by many replicas between them. Nothing looks for work: a job
// brings it, and the bus brings back the job of a holder that died.
package work

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/data"
)

// Store is where a role keeps its keyed work (pgdata's work operations, on PostgreSQL).
type Store interface {
	// Mark marks key's work of kind, due now, and MarkAt due at at — or sooner, if it is marked so
	// already — each in a transaction of its own that enqueues the mark's job.
	Mark(ctx context.Context, kind string, key int64) error
	MarkAt(ctx context.Context, kind string, key int64, at time.Time) error
	// Peek reads who holds key's work, without a lock: a job's worker that finds it held by a holder
	// under a job leaves the mark to it.
	Peek(ctx context.Context, kind string, key int64) (Peeked, error)
	// Take leases key's work to holder for ttl, if no one holds it or its holder's lease lapsed —
	// making its row if it has none — clearing its mark if it is due now (a mark due later stays);
	// it reports whether it took it, and whether the work is due: marked due now, or held by a
	// holder whose lease lapsed, which never finished it.
	Take(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (Taken, error)
	// Renew extends holder's lease by ttl from now; false if the lease is another's.
	Renew(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error)
	// Release ends holder's lease, unless the work was marked meanwhile, due now, when it clears
	// the mark and keeps the lease, reporting again; a mark due later stays.
	Release(ctx context.Context, kind string, key int64, holder string) (again bool, err error)
	// Let ends holder's lease and leaves the work's mark as it is: work a claim (Claim) held while
	// it was marked is done by whoever takes it next.
	Let(ctx context.Context, kind string, key int64, holder string) error
	// Marked are up to limit pieces of kind's work after key after, in key order, that are marked —
	// due now, or at their At — or held by a holder whose lease lapsed, due now: what a release
	// before jobs left to its runners' looks, adopted once (Runner).
	Marked(ctx context.Context, kind string, after int64, limit int) ([]Mark, error)
	// Jobs is where the store's marks' jobs go (JobsOf its database).
	Jobs() Jobs
}

// Peeked is who holds a piece of work, as Peek read it: its holder, and its lease's time left;
// whether the work has a row at all — was ever marked, or taken — and whether it is marked.
type Peeked struct {
	Exists, Marked bool
	Holder         string
	Held           bool
	Left           time.Duration
}

// Taken is what a Take did: whether it took the work, whether the work is due — marked, or taken
// over from a holder whose lease lapsed — and when a mark it kept falls due (zero for none).
type Taken struct {
	Took, Due bool
	Later     time.Time
}

// ErrLost is the cause of the end of the context a Runner does work with when its lease is lost.
var ErrLost = errors.New("work: the lease was lost")

// Runner does one kind of work on this replica, as its jobs come.
type Runner struct {
	Kind  string
	Store Store
	// Do does key's work, with a context that ends if the lease is lost and carries the lease's
	// fence (data.WithFence), so what it writes through a store that checks it is never written
	// once another holds the lease. An error is logged, and the work is marked to be done again
	// RetryAfter later.
	Do func(ctx context.Context, key int64) error
	// TTL is a lease's length, renewed every third of it. Default thirty seconds.
	TTL time.Duration
	// Workers bounds the keys this replica does at once. Default four.
	Workers int
	// RetryAfter is how long after a failure the work is done again. Default ten seconds.
	RetryAfter time.Duration
	// Holder names this replica's leases. Default its host, process, and a random suffix.
	Holder string
	// LetGo, when set, is told each time the runner lets go of key's work: those waiting to claim
	// it (wait.Claim) hear of it.
	LetGo func(ctx context.Context, key int64)
	Log   *slog.Logger
}

// jobHolder is what a holder's name holds after its replica's: the token of the job it holds the
// work under, which brings the work back to another replica should it die.
const jobHolder = "/job:"

// adoptKey is the key of a kind's adoption (Runner.adopt): no piece of work's, which are IDs.
const adoptKey = math.MinInt64

// UpgradeAfter is how long after the first replica of a release starts the work a release before it
// left without jobs is taken up (Runner.adopt, Once): by then the release before has stopped, and
// marks nothing more. A rollout takes less.
var UpgradeAfter = 15 * time.Minute

// wait is how long a worker waits for a job before it asks again.
const wait = 30 * time.Second

// Run does the kind's work as its jobs come until ctx ends, then lets go of what it holds. As it
// starts, it books the kind's adoption (adopt), once for the life of the store's database.
func (r *Runner) Run(ctx context.Context) error {
	r.defaults()
	r.bookAdoption(ctx)
	var wg sync.WaitGroup
	for range r.Workers {
		wg.Go(func() {
			for ctx.Err() == nil {
				// The store's jobs as its database is registered now (Register), each lease.
				jobs := r.Store.Jobs()
				ls, err := jobs.Queue.Lease(ctx, jobs.QueueOf(r.Kind), bus.LeaseOptions{Max: 1, Visibility: r.visibility(), Wait: wait})
				if err != nil {
					if ctx.Err() == nil {
						r.Log.Warn("work: leasing a job", "kind", r.Kind, "err", err)
						sleep(ctx, time.Second)
					}
					continue
				}
				for _, l := range ls {
					r.job(ctx, jobs, l)
				}
			}
		})
	}
	wg.Wait()
	return nil
}

// RunReady does the kind's jobs ready now, each once, and returns how many: tests, which step a
// role's work themselves. What the jobs mark is the next call's.
func (r *Runner) RunReady(ctx context.Context) int {
	r.defaults()
	jobs := r.Store.Jobs()
	ls, err := jobs.Queue.Lease(ctx, jobs.QueueOf(r.Kind), bus.LeaseOptions{Max: 10000, Visibility: r.visibility()})
	if err != nil {
		r.Log.Warn("work: leasing jobs", "kind", r.Kind, "err", err)
		return 0
	}
	for _, l := range ls {
		r.job(ctx, jobs, l)
	}
	return len(ls)
}

// bookAdoption books the kind's adoption UpgradeAfter on, unless it was booked — or done — before:
// a job on the kind's own queue, under adoptKey, whose run is adopt.
func (r *Runner) bookAdoption(ctx context.Context) {
	p, err := r.Store.Peek(ctx, r.Kind, adoptKey)
	if err == nil && !p.Exists {
		err = r.Store.MarkAt(ctx, r.Kind, adoptKey, time.Now().Add(UpgradeAfter))
	}
	if err != nil && ctx.Err() == nil {
		r.Log.Warn("work: booking the adoption of what a release before left", "kind", r.Kind, "err", err)
	}
}

// adopt enqueues a job for every piece of the kind's work that is marked, or held by a holder that
// died, due when its mark is: a release before jobs marked work and left its runners to look for it,
// and a mark it made as it stopped would be brought by nothing. A job of one that has its own is a
// look more.
func (r *Runner) adopt(ctx context.Context) error {
	jobs := r.Store.Jobs()
	after := int64(adoptKey)
	adopted := 0
	for {
		marks, err := r.Store.Marked(ctx, r.Kind, after, 1000)
		if err != nil {
			return err
		}
		if len(marks) == 0 {
			break
		}
		if err := jobs.Enqueue(ctx, marks...); err != nil {
			return err
		}
		adopted += len(marks)
		after = marks[len(marks)-1].Key
	}
	r.Log.Info("work: adopted what a release before left", "kind", r.Kind, "jobs", adopted)
	return nil
}

// do does key's work: the kind's, or, under adoptKey, its adoption.
func (r *Runner) do(ctx context.Context, key int64) error {
	if key == adoptKey {
		return r.adopt(ctx)
	}
	return r.Do(ctx, key)
}

// visibility is how long a job stays leased between extensions: past the work's lease, so a job
// whose holder died comes back once the lease has lapsed, and its next worker takes it over.
func (r *Runner) visibility() time.Duration { return r.TTL + r.TTL/3 }

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (r *Runner) defaults() {
	if r.TTL <= 0 {
		r.TTL = 30 * time.Second
	}
	if r.Workers <= 0 {
		r.Workers = 4
	}
	if r.RetryAfter <= 0 {
		r.RetryAfter = 10 * time.Second
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

// job does the work a job names. Held by a holder under a job, the work is left to it: its release
// sees the mark, and its job comes back should it die. Held by a claim, or by this job's own holder
// — the job back before the holder's lease lapsed — it is looked at again once the lease may have
// ended. Otherwise it is taken, and done until it stays unmarked.
func (r *Runner) job(ctx context.Context, jobs Jobs, l bus.Leased) {
	key, token, ok := parseJob(l.Job())
	if !ok {
		r.ack(ctx, l)
		return
	}
	holder := r.Holder + jobHolder + token
	p, err := r.Store.Peek(ctx, r.Kind, key)
	if err != nil {
		r.later(ctx, jobs, l, key, token, time.Second, err)
		return
	}
	if p.Held {
		r.held(ctx, jobs, l, key, token, p)
		return
	}
	t, err := r.Store.Take(ctx, r.Kind, key, holder, r.TTL)
	switch {
	case err != nil:
		r.later(ctx, jobs, l, key, token, time.Second, err)
		return
	case !t.Took:
		p, err := r.Store.Peek(ctx, r.Kind, key)
		if err != nil {
			r.later(ctx, jobs, l, key, token, time.Second, err)
			return
		}
		r.held(ctx, jobs, l, key, token, p)
		return
	case !t.Due && !t.Later.IsZero():
		// Its mark falls due later — the job came early, by the bus's clock — and is looked at then.
		if r.let(ctx, key, holder) {
			r.later(ctx, jobs, l, key, token, time.Until(t.Later)+time.Second, nil)
		}
		return
	case !t.Due:
		// Nothing due: the mark was done, or its transaction did not commit — or one made since the
		// take, whose job found the work held under this one.
		r.letOn(ctx, jobs, l, key, token, holder)
		return
	}
	held, stop := hold(ctx, r.Store, r.Kind, key, holder, r.TTL, r.Log, l, r.visibility())
	defer stop()
	held = data.WithFence(held, data.Fence{Kind: r.Kind, Key: key, Holder: holder})
	for {
		err := r.do(held, key)
		if errors.Is(context.Cause(held), ErrLost) {
			// Another holds it now, under this job, or one of its own: its delivery is not this one's
			// to end.
			return
		}
		if _, moved := cells.MovedOf(err); moved {
			// Its repository moved to another cell, which does its work now (docs/hosted.md,
			// "Cells, as built").
			err = nil
		}
		// With a context of its own: on shutdown, what this replica held is let go at once, rather
		// than when its lease lapses.
		bg, done := context.WithTimeout(context.WithoutCancel(ctx), r.TTL)
		switch {
		case ctx.Err() != nil:
			// Stopped short: marked due now, let go, and handed on by this job, which another
			// replica takes (letOn).
			if merr := r.Store.Mark(bg, r.Kind, key); merr != nil {
				r.Log.Warn("work: marking what was stopped short", "kind", r.Kind, "key", key, "err", merr)
				done()
				r.nack(bg, l, 0)
				return
			}
			r.letOn(bg, jobs, l, key, token, holder)
			done()
			return
		case err != nil:
			r.Log.Warn("work: doing", "kind", r.Kind, "key", key, "err", err)
			// Done again a while later: marked then, its job's, and let go, its marks kept — one made
			// meanwhile, due now, brings a job at once (Let). Not Release, which, finding such a mark,
			// keeps the lease for its holder to do the work again: this one walks away.
			if merr := r.Store.MarkAt(bg, r.Kind, key, time.Now().Add(r.RetryAfter)); merr != nil {
				r.Log.Warn("work: marking a failure to do again", "kind", r.Kind, "key", key, "err", merr)
				// Held, its lease left to lapse, not let go: let go unmarked, the work would look done
				// to the job that comes back — which takes it, its holder's lease lapsed, as due.
				done()
				r.nack(bg, l, r.RetryAfter)
				return
			}
			r.letOn(bg, jobs, l, key, token, holder)
			done()
			return
		}
		again, err := r.Store.Release(bg, r.Kind, key, holder)
		if err != nil {
			r.Log.Warn("work: releasing", "kind", r.Kind, "key", key, "err", err)
			done()
			return // the lease lapses, and the job, unacknowledged, brings the work back
		}
		if !again {
			r.letGo(bg, key)
			r.ack(bg, l)
			done()
			return
		}
		done()
	}
}

func (r *Runner) letGo(ctx context.Context, key int64) {
	if r.LetGo != nil {
		r.LetGo(ctx, key)
	}
}

// held is what a job does with work another holds.
func (r *Runner) held(ctx context.Context, jobs Jobs, l bus.Leased, key int64, token string, p Peeked) {
	_, under, ok := strings.Cut(p.Holder, jobHolder)
	switch {
	case ok && under != token:
		// Its holder's release sees this job's mark, and its own job brings the work back should it
		// die.
		r.ack(ctx, l)
	default:
		// A claim, which brings nothing back should it die; or this job's own holder, its lease not
		// lapsed yet: looked at again once it may have.
		r.later(ctx, jobs, l, key, token, p.Left+time.Second, nil)
	}
}

// later has a job looked at again after delay — a fresh job of its token, enqueued before this
// one is acknowledged, so no number of looks spends its attempts; or, that failing, this one
// returned.
func (r *Runner) later(ctx context.Context, jobs Jobs, l bus.Leased, key int64, token string, delay time.Duration, cause error) {
	if cause != nil && ctx.Err() == nil {
		r.Log.Warn("work: looking at a job again later", "kind", r.Kind, "key", key, "err", cause)
	}
	ctx = context.WithoutCancel(ctx)
	if err := jobs.Queue.Enqueue(ctx, jobs.jobWith(r.Kind, key, time.Now().Add(delay), token)); err != nil {
		r.nack(ctx, l, delay)
		return
	}
	r.ack(ctx, l)
}

func (r *Runner) ack(ctx context.Context, l bus.Leased) {
	if err := l.Ack(context.WithoutCancel(ctx)); err != nil && !errors.Is(err, bus.ErrLeaseLost) {
		r.Log.Warn("work: acknowledging a job", "kind", r.Kind, "err", err)
	}
}

func (r *Runner) nack(ctx context.Context, l bus.Leased, delay time.Duration) {
	if err := l.Nack(context.WithoutCancel(ctx), delay); err != nil && !errors.Is(err, bus.ErrLeaseLost) {
		r.Log.Warn("work: returning a job", "kind", r.Kind, "err", err)
	}
}

// letOn lets go of key's work and ends this job: handed on — a copy enqueued once the let has
// committed — when the work is left marked and no one holds it, acknowledged otherwise. A job that
// finds work held under another job leaves its mark to that holder (held), and so does a job a
// let enqueues before it commits, which may be leased while the work is still held: the holder's
// own job is what brings back work it lets go of marked. Unacknowledged when the let fails, or the
// process dies before the copy is enqueued: the job's lease lapses, and brings the work back.
func (r *Runner) letOn(ctx context.Context, jobs Jobs, l bus.Leased, key int64, token, holder string) {
	if !r.let(ctx, key, holder) {
		return
	}
	p, err := r.Store.Peek(context.WithoutCancel(ctx), r.Kind, key)
	if err != nil || p.Marked && !p.Held {
		r.later(ctx, jobs, l, key, token, 0, err)
		return
	}
	r.ack(ctx, l)
}

// let lets go of key's work, and reports whether it did. A job is acknowledged only once the work
// it held is let go: a job that finds work held under another job leaves the mark to it, so a held
// row's job must still bring the work back should its holder be gone — unacknowledged, it does,
// once its lease lapses.
func (r *Runner) let(ctx context.Context, key int64, holder string) bool {
	bg, done := context.WithTimeout(context.WithoutCancel(ctx), r.TTL)
	defer done()
	if err := r.Store.Let(bg, r.Kind, key, holder); err != nil {
		r.Log.Warn("work: letting go", "kind", r.Kind, "key", key, "err", err)
		return false
	}
	r.letGo(bg, key)
	return true
}

// hold keeps holder's lease on key's work, renewing it every third of ttl — and, with it, the
// lease of the job it holds the work under, by visibility — until stop is called: the context it
// returns ends with ErrLost if a renewal fails, and with ctx otherwise.
func hold(ctx context.Context, s Store, kind string, key int64, holder string, ttl time.Duration, log *slog.Logger,
	job bus.Leased, visibility time.Duration) (held context.Context, stop func()) {
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
				if job != nil {
					if err := job.Extend(held, visibility); err != nil && held.Err() == nil {
						log.Warn("work: extending a job's lease", "kind", kind, "key", key, "err", err)
					}
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
// meanwhile for whoever takes the work next (Store.Let): a mark's job finds the claim and looks
// again once it may have let go.
func Claim(ctx context.Context, s Store, kind string, key int64, holder string, ttl time.Duration) (held context.Context, release func(), ok bool, err error) {
	t, err := s.Take(ctx, kind, key, holder, ttl)
	if err != nil || !t.Took {
		return nil, nil, false, err
	}
	held, stop := hold(ctx, s, kind, key, holder, ttl, slog.Default(), nil, 0)
	return data.WithFence(held, data.Fence{Kind: kind, Key: key, Holder: holder}), func() {
		stop()
		bg, done := context.WithTimeout(context.WithoutCancel(ctx), ttl)
		defer done()
		if err := s.Let(bg, kind, key, holder); err != nil {
			slog.Warn("work: letting go", "kind", kind, "key", key, "err", err)
		}
	}, true, nil
}

// Once has do done once for the life of the store's database, by one replica, UpgradeAfter after
// the first replica that calls it starts — once the release before it, whose leftovers do takes
// up, has stopped: every replica calls it as it starts, and the first to find the work never marked
// marks it for then (key 0 of kind); its job brings do, done again later should it fail. It returns
// at once if do was done before; once this replica does it; or once ctx ends — a replica another
// did it before looks at kind's queue until then.
func Once(ctx context.Context, s Store, kind string, do func(context.Context) error, log *slog.Logger) error {
	p, err := s.Peek(ctx, kind, 0)
	switch {
	case err != nil:
		return err
	case p.Exists && !p.Marked && !p.Held:
		return nil
	case !p.Exists:
		if err := s.MarkAt(ctx, kind, 0, time.Now().Add(UpgradeAfter)); err != nil {
			return err
		}
	}
	var done bool
	r := &Runner{Kind: kind, Store: s, Workers: 1, Log: log, Do: func(ctx context.Context, _ int64) error {
		if err := do(ctx); err != nil {
			return err
		}
		done = true
		return nil
	}}
	r.defaults()
	jobs := s.Jobs()
	for ctx.Err() == nil && !done {
		ls, err := jobs.Queue.Lease(ctx, jobs.QueueOf(kind), bus.LeaseOptions{Max: 1, Visibility: r.visibility(), Wait: wait})
		if err != nil {
			if ctx.Err() == nil {
				r.Log.Warn("work: leasing a job", "kind", kind, "err", err)
				sleep(ctx, time.Second)
			}
			continue
		}
		for _, l := range ls {
			r.job(ctx, jobs, l)
		}
	}
	return nil
}
