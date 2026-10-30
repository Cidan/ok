package work_test

import (
	"cmp"
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/work"
)

// store is a work.Store in memory, its marks' jobs on a queue of its own: each mark, written with
// the store's lock held, enqueues its job before it lets go, as a transaction enqueues before it
// commits.
type store struct {
	jobs    work.Jobs
	mu      sync.Mutex
	rows    map[int64]*row
	renew   func(holder string) bool // false loses the lease
	looks   atomic.Int64             // Peeks and Takes
	failLet atomic.Int32             // Lets to fail, as a database going away fails them
	failAt  atomic.Int32             // MarkAts to fail, as a bus going away fails their jobs
	// beforeLet, when set, runs as a Let begins; inLet once its job is enqueued, before it lets go.
	beforeLet, inLet atomic.Pointer[func()]
}

type row struct {
	dirty  bool
	due    time.Time // zero: now
	holder string
	until  time.Time
}

func (r *row) dueNow() bool   { return r.dirty && !time.Now().Before(r.due) }
func (r *row) dueLater() bool { return r.dirty && time.Now().Before(r.due) }
func (r *row) held() bool     { return r.holder != "" && time.Now().Before(r.until) }

func newStore() *store {
	return &store{jobs: work.Jobs{Queue: bus.NewLocalQueue(), Prefix: "test"}, rows: map[int64]*row{}}
}

func (s *store) Jobs() work.Jobs { return s.jobs }

func (s *store) row(key int64) *row {
	if s.rows[key] == nil {
		s.rows[key] = &row{}
	}
	return s.rows[key]
}

func (s *store) Mark(ctx context.Context, kind string, key int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.row(key)
	r.dirty, r.due = true, time.Time{}
	return s.jobs.Enqueue(ctx, work.Mark{Kind: kind, Key: key})
}

func (s *store) MarkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	if s.failAt.Add(-1) >= 0 {
		return errors.New("the bus is going away")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.row(key)
	switch {
	case !r.dirty:
		r.due = at
	case !r.due.IsZero() && at.Before(r.due):
		r.due = at
	}
	r.dirty = true
	return s.jobs.Enqueue(ctx, work.Mark{Kind: kind, Key: key, At: at})
}

func (s *store) Peek(_ context.Context, _ string, key int64) (work.Peeked, error) {
	if key != math.MinInt64 { // a kind's adoption's booking, as its runner starts
		s.looks.Add(1)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[key]
	if !ok {
		return work.Peeked{}, nil
	}
	return work.Peeked{Exists: true, Marked: r.dirty, Holder: r.holder, Held: r.held(), Left: max(time.Until(r.until), 0)}, nil
}

func (s *store) Marked(_ context.Context, kind string, after int64, limit int) ([]work.Mark, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []work.Mark
	for k, r := range s.rows {
		if k > after && (r.dirty || r.holder != "" && !r.held()) {
			m := work.Mark{Kind: kind, Key: k}
			if r.dueLater() {
				m.At = r.due
			}
			out = append(out, m)
		}
	}
	slices.SortFunc(out, func(a, b work.Mark) int { return cmp.Compare(a.Key, b.Key) })
	return out[:min(limit, len(out))], nil
}

// markOld marks key's work as a release before jobs did: its row, and no job.
func (s *store) markOld(key int64, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.row(key)
	r.dirty, r.due = true, at
}

func (s *store) Take(_ context.Context, _ string, key int64, holder string, ttl time.Duration) (work.Taken, error) {
	s.looks.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.row(key)
	if r.held() {
		return work.Taken{}, nil
	}
	t := work.Taken{Took: true, Due: r.dueNow() || r.holder != ""}
	if r.dueLater() {
		t.Later = r.due
	} else {
		r.dirty, r.due = false, time.Time{}
	}
	r.holder, r.until = holder, time.Now().Add(ttl)
	return t, nil
}

func (s *store) Renew(_ context.Context, _ string, key int64, holder string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.row(key)
	if r.holder != holder || (s.renew != nil && !s.renew(holder)) {
		return false, nil
	}
	r.until = time.Now().Add(ttl)
	return true, nil
}

func (s *store) Release(_ context.Context, _ string, key int64, holder string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.row(key)
	if r.holder != holder {
		return false, nil
	}
	if r.dueNow() {
		r.dirty, r.due = false, time.Time{}
		return true, nil
	}
	r.holder = ""
	return false, nil
}

func (s *store) Let(ctx context.Context, kind string, key int64, holder string) error {
	if s.failLet.Add(-1) >= 0 {
		return errors.New("the database went away")
	}
	if f := s.beforeLet.Load(); f != nil {
		(*f)()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.row(key)
	if r.holder != holder {
		return nil
	}
	// As a database's: its job enqueued as its transaction is about to commit, so leased, it may
	// find the work still held.
	if r.dueNow() {
		if err := s.jobs.Enqueue(ctx, work.Mark{Kind: kind, Key: key}); err != nil {
			return err
		}
	}
	if f := s.inLet.Load(); f != nil {
		s.mu.Unlock()
		(*f)()
		s.mu.Lock()
		r = s.row(key)
	}
	r.holder = ""
	return nil
}

func (s *store) state(key int64) row {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.row(key)
}

// doer records the work done, refusing to do one key twice at once.
type doer struct {
	mu    sync.Mutex
	doing map[int64]string
	done  map[int64]int
	twice atomic.Bool
}

func newDoer() *doer { return &doer{doing: map[int64]string{}, done: map[int64]int{}} }

func (d *doer) do(holder string, f func(ctx context.Context, key int64) error) func(context.Context, int64) error {
	return func(ctx context.Context, key int64) error {
		d.mu.Lock()
		if other, ok := d.doing[key]; ok {
			d.twice.Store(true)
			d.mu.Unlock()
			return errors.New(other + " is doing it too")
		}
		d.doing[key] = holder
		d.mu.Unlock()
		defer func() {
			d.mu.Lock()
			delete(d.doing, key)
			d.done[key]++
			d.mu.Unlock()
		}()
		if f != nil {
			return f(ctx, key)
		}
		time.Sleep(time.Millisecond)
		return nil
	}
}

func (d *doer) count(key int64) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.done[key]
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("never: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func run(t *testing.T, r *work.Runner) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return func() { cancel(); <-done }
}

// Two replicas do every key's work between them, each key on one at a time, as its marks' jobs
// come.
func TestRunners(t *testing.T) {
	s, d := newStore(), newDoer()
	for _, h := range []string{"a", "b"} {
		run(t, &work.Runner{Kind: "q", Store: s, Do: d.do(h, nil), TTL: time.Second, Holder: h})
	}
	ctx := context.Background()
	for round := range 3 {
		for k := int64(1); k <= 20; k++ {
			s.Mark(ctx, "q", k)
		}
		eventually(t, "every key's work done", func() bool {
			for k := int64(1); k <= 20; k++ {
				if d.count(k) <= round {
					return false
				}
			}
			return true
		})
	}
	if d.twice.Load() {
		t.Error("a key's work was done by two replicas at once")
	}
}

// Nothing looks for work: a runner with no jobs reads no row.
func TestNoLooking(t *testing.T) {
	s, d := newStore(), newDoer()
	run(t, &work.Runner{Kind: "q", Store: s, Do: d.do("a", nil), TTL: time.Second, Holder: "a"})
	time.Sleep(300 * time.Millisecond)
	if n := s.looks.Load(); n != 0 {
		t.Errorf("a runner with no jobs looked at work %d times", n)
	}
	s.Mark(context.Background(), "q", 1)
	eventually(t, "the marked work done", func() bool { return d.count(1) == 1 })
}

// Work marked while it is being done is done again by its holder before it lets it go: the mark's
// job, finding it held under a job, is left to the holder.
func TestMarkedWhileDoing(t *testing.T) {
	s, d := newStore(), newDoer()
	var calls atomic.Int32
	run(t, &work.Runner{Kind: "q", Store: s, TTL: time.Second, Holder: "a", Do: d.do("a", func(ctx context.Context, key int64) error {
		if calls.Add(1) == 1 {
			s.Mark(ctx, "q", key)
			time.Sleep(50 * time.Millisecond) // the mark's job finds the work held
		}
		return nil
	})})
	s.Mark(context.Background(), "q", 1)
	eventually(t, "the work done twice", func() bool { return d.count(1) == 2 })
	eventually(t, "the work let go", func() bool { r := s.state(1); return r.holder == "" && !r.dirty })
	time.Sleep(100 * time.Millisecond)
	if n := d.count(1); n != 2 {
		t.Errorf("the work was done %d times, want 2", n)
	}
}

// Work that fails is marked again, and done again RetryAfter later.
func TestFailures(t *testing.T) {
	s, d := newStore(), newDoer()
	var fails atomic.Int32
	run(t, &work.Runner{Kind: "q", Store: s, TTL: time.Second, RetryAfter: 20 * time.Millisecond, Holder: "a",
		Do: d.do("a", func(context.Context, int64) error {
			if fails.Add(1) <= 2 {
				return errors.New("not yet")
			}
			return nil
		})})
	s.Mark(context.Background(), "q", 1)
	eventually(t, "the work done once it stopped failing", func() bool { return d.count(1) == 3 })
	eventually(t, "the work let go", func() bool { r := s.state(1); return r.holder == "" && !r.dirty })
}

// Work whose failure cannot be marked to be done again — its mark's job refused, the bus going
// away as its replica stops — is not let go, which would leave it looking done: its lease lapses,
// and the job that comes back takes it, due.
func TestFailureUnmarked(t *testing.T) {
	s, d := newStore(), newDoer()
	var fails atomic.Int32
	run(t, &work.Runner{Kind: "q", Store: s, TTL: 150 * time.Millisecond, RetryAfter: 20 * time.Millisecond, Holder: "a",
		Do: d.do("a", func(context.Context, int64) error {
			if fails.Add(1) == 1 {
				s.failAt.Store(1)
				return errors.New("not yet")
			}
			return nil
		})})
	s.Mark(context.Background(), "q", 1)
	eventually(t, "the work done again once its holder's lease lapsed", func() bool { return d.count(1) == 2 })
	eventually(t, "the work let go", func() bool { r := s.state(1); return r.holder == "" && !r.dirty })
}

// Work marked, due now, while it fails is let go — not held by a holder that walked away — and done
// again at once by the mark's job, the failure's retry folded into it.
func TestMarkedWhileFailing(t *testing.T) {
	s, d := newStore(), newDoer()
	var calls atomic.Int32
	run(t, &work.Runner{Kind: "q", Store: s, TTL: time.Second, RetryAfter: 300 * time.Millisecond, Holder: "a",
		Do: d.do("a", func(ctx context.Context, key int64) error {
			if calls.Add(1) == 1 {
				s.Mark(ctx, "q", key)
				time.Sleep(50 * time.Millisecond) // the mark's job finds the work held, and leaves it
				return errors.New("the SCM is down")
			}
			return nil
		})})
	s.Mark(context.Background(), "q", 1)
	eventually(t, "the work marked while it failed done again", func() bool { return d.count(1) >= 2 })
	if r := s.state(1); r.holder != "" && !r.held() {
		t.Errorf("the work is left held by a holder that walked away: %+v", r)
	}
	eventually(t, "the work let go, done", func() bool { r := s.state(1); return r.holder == "" && !r.dirty })
	time.Sleep(500 * time.Millisecond) // the retry's job, come and gone
	if r := s.state(1); r.holder != "" || r.dirty || d.count(1) != 2 {
		t.Errorf("after the retry's job: %+v, done %d times; want it let go, clean, done twice", r, d.count(1))
	}
}

// A lease lost ends the work's context with ErrLost; one whose runner stops is marked and let go
// at once, and another replica takes it.
func TestLostAndLetGo(t *testing.T) {
	s, d := newStore(), newDoer()
	s.renew = func(string) bool { return false }
	lostIt := make(chan error, 1)
	run(t, &work.Runner{Kind: "q", Store: s, TTL: 30 * time.Millisecond, Holder: "a", Do: d.do("a", func(ctx context.Context, key int64) error {
		<-ctx.Done()
		lostIt <- context.Cause(ctx)
		return ctx.Err()
	})})
	s.Mark(context.Background(), "q", 1)
	select {
	case err := <-lostIt:
		if !errors.Is(err, work.ErrLost) {
			t.Errorf("the work's context ended with %v, want ErrLost", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the lease's loss never reached the work")
	}

	s2, d2 := newStore(), newDoer()
	started := make(chan struct{})
	stop := run(t, &work.Runner{Kind: "q", Store: s2, TTL: time.Hour, Holder: "a", Do: d2.do("a", func(ctx context.Context, key int64) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})})
	s2.Mark(context.Background(), "q", 1)
	<-started
	stop()
	if r := s2.state(1); r.holder != "" || !r.dueNow() {
		t.Fatalf("what a stopped runner held: %+v; want it let go, marked", r)
	}
	run(t, &work.Runner{Kind: "q", Store: s2, TTL: time.Hour, Holder: "b", Do: d2.do("b", nil)})
	eventually(t, "another replica taking what the first let go", func() bool { return d2.count(1) == 2 })
}

// A holder that dies — no renewal, no release, its job never acknowledged — loses its lease, and
// its job comes back once the lease has lapsed: another replica takes the work over then, not
// before.
func TestHolderDies(t *testing.T) {
	s := newStore()
	ttl := 150 * time.Millisecond
	dead := make(chan struct{})
	defer close(dead) // before the runners' cleanups, which wait for what they do
	diedAt := make(chan time.Time, 1)
	s.renew = func(holder string) bool { return holder[:1] != "a" }
	// One worker, held by what it does: nothing of the dead process leases again.
	run(t, &work.Runner{Kind: "q", Store: s, TTL: ttl, Holder: "a", Workers: 1, Do: func(ctx context.Context, key int64) error {
		diedAt <- time.Now()
		<-dead // never returns: the process is gone
		return nil
	}})
	s.Mark(context.Background(), "q", 1)
	died := <-diedAt
	took := make(chan time.Time, 1)
	run(t, &work.Runner{Kind: "q", Store: s, TTL: ttl, Holder: "b", Do: func(ctx context.Context, key int64) error {
		took <- time.Now()
		return nil
	}})
	select {
	case at := <-took:
		if at.Sub(died) < ttl {
			t.Errorf("taken over %s after its holder died, before its lease of %s lapsed", at.Sub(died), ttl)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the dead holder's work was never taken over")
	}
}

// One worker does every key marked, one after another.
func TestOneWorker(t *testing.T) {
	s, d := newStore(), newDoer()
	for k := int64(1); k <= 10; k++ {
		s.Mark(context.Background(), "q", k)
	}
	run(t, &work.Runner{Kind: "q", Store: s, Do: d.do("a", nil), TTL: time.Second, Workers: 1, Holder: "a"})
	eventually(t, "every key's work done", func() bool {
		for k := int64(1); k <= 10; k++ {
			if d.count(k) != 1 {
				return false
			}
		}
		return true
	})
}

// A claim is one holder's at a time, its context ending if the lease is lost; released, another
// takes it.
func TestClaim(t *testing.T) {
	ctx := context.Background()
	s := newStore()
	held, release, ok, err := work.Claim(ctx, s, "feed", 7, "a", time.Hour)
	if err != nil || !ok {
		t.Fatalf("claiming a key no one holds: %v, %v", ok, err)
	}
	if _, _, ok, _ := work.Claim(ctx, s, "feed", 7, "b", time.Hour); ok {
		t.Fatal("a second holder claimed it")
	}
	release()
	if held.Err() == nil {
		t.Error("the released claim's context still runs")
	}
	_, release, ok, err = work.Claim(ctx, s, "feed", 7, "b", 30*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("claiming it once released: %v, %v", ok, err)
	}
	release()

	s.renew = func(string) bool { return false }
	held, release, ok, _ = work.Claim(ctx, s, "feed", 8, "a", 30*time.Millisecond)
	if !ok {
		t.Fatal("claiming another key")
	}
	defer release()
	select {
	case <-held.Done():
		if !errors.Is(context.Cause(held), work.ErrLost) {
			t.Errorf("the lost claim's context ended with %v", context.Cause(held))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a lost lease never ended the claim")
	}
}

// Work marked while a claim holds it is done once the claim lets go — its release brings a job at
// once — and, should the claimer die holding it, once its lease lapses: the mark's job, finding a
// claim, looks again then.
func TestMarkedWhileClaimed(t *testing.T) {
	ctx := context.Background()
	s, d := newStore(), newDoer()
	run(t, &work.Runner{Kind: "compact", Store: s, Do: d.do("r", nil), TTL: time.Second, Holder: "r"})
	_, release, ok, err := work.Claim(ctx, s, "compact", 7, "a", time.Hour)
	if err != nil || !ok {
		t.Fatalf("claiming: %v, %v", ok, err)
	}
	if err := s.Mark(ctx, "compact", 7); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if d.count(7) != 0 {
		t.Fatal("work a claim holds was done")
	}
	release()
	eventually(t, "the work done once the claim let go", func() bool { return d.count(7) == 1 })

	s.renew = func(holder string) bool { return holder != "dies" }
	_, _, ok, err = work.Claim(ctx, s, "compact", 8, "dies", 100*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("claiming: %v, %v", ok, err)
	}
	if err := s.Mark(ctx, "compact", 8); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the work done once the dead claim's lease lapsed", func() bool { return d.count(8) == 1 })
}

// Work marked for a time is done then, not before — a job that comes early looks again then — and
// one marked for later while it is being done is let go, and done again when due.
func TestMarkAt(t *testing.T) {
	ctx := context.Background()
	s, d := newStore(), newDoer()
	var calls atomic.Int32
	run(t, &work.Runner{Kind: "q", Store: s, TTL: time.Second, Holder: "a", Do: d.do("a", func(ctx context.Context, key int64) error {
		if calls.Add(1) == 1 {
			s.MarkAt(ctx, "q", key, time.Now().Add(100*time.Millisecond))
		}
		return nil
	})})
	start := time.Now()
	s.MarkAt(ctx, "q", 1, start.Add(100*time.Millisecond))
	// A job that comes before the mark is due, as a bus whose clock runs ahead would bring it.
	if err := s.jobs.Enqueue(ctx, work.Mark{Kind: "q", Key: 1}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the work done when due", func() bool { return d.count(1) == 1 })
	if took := time.Since(start); took < 100*time.Millisecond {
		t.Errorf("the work marked for 100ms on was done after %s", took)
	}
	eventually(t, "the lease let go, or the work done again", func() bool { return s.state(1).holder == "" || d.count(1) > 1 })
	if d.count(1) != 1 {
		t.Error("work marked for later during it was held until it was done again")
	}
	eventually(t, "the work done again when due", func() bool { return d.count(1) == 2 })
}

// A transaction's attempt enqueues its marks' jobs as it is about to commit, each piece of work's
// once at the soonest it was marked; an attempt that ends without committing enqueues none, and a
// mark outside any attempt is refused.
func TestAttempts(t *testing.T) {
	ctx := context.Background()
	q := bus.NewLocalQueue()
	db, tx, other := new(int), new(int), new(int)
	work.Register(db, work.Jobs{Queue: q, Prefix: "t"})
	work.Begin(db, tx)
	soon := time.Now().Add(time.Hour)
	for _, m := range []work.Mark{{Kind: "k", Key: 1, At: soon.Add(time.Hour)}, {Kind: "k", Key: 1, At: soon}, {Kind: "k", Key: 2}, {Kind: "k", Key: 2, At: soon}} {
		if err := work.Marked(tx, m.Kind, m.Key, m.At); err != nil {
			t.Fatal(err)
		}
	}
	if ls, _ := q.Lease(ctx, "work.t.k", bus.LeaseOptions{Max: 10, Visibility: time.Minute}); len(ls) != 0 {
		t.Fatal("a mark's job was enqueued before its attempt was about to commit")
	}
	if err := work.Flush(ctx, tx); err != nil {
		t.Fatal(err)
	}
	work.End(tx)
	ls, err := q.Lease(ctx, "work.t.k", bus.LeaseOptions{Max: 10, Visibility: time.Minute})
	if err != nil || len(ls) != 1 || ls[0].Job().Key != 2 {
		t.Fatalf("the jobs due now: %d, %v; want key 2's alone, key 1's due in an hour", len(ls), err)
	}
	if err := work.Marked(tx, "k", 3, time.Time{}); err == nil {
		t.Error("a mark after its attempt ended was recorded")
	}
	if err := work.Marked(other, "k", 3, time.Time{}); err == nil {
		t.Error("a mark in a transaction no attempt began was recorded")
	}
	work.Begin(db, other)
	work.Marked(other, "k", 4, time.Time{})
	work.End(other)
	if ls, _ := q.Lease(ctx, "work.t.k", bus.LeaseOptions{Max: 10, Visibility: time.Minute}); len(ls) != 0 {
		t.Error("an attempt that ended without committing enqueued its marks' jobs")
	}
}

// Once's work is done once, by one of the replicas that start it, UpgradeAfter on, again until it
// succeeds; a replica started after it was done does nothing, and returns at once.
func TestOnce(t *testing.T) {
	defer upgradeAfter(100 * time.Millisecond)()
	s := newStore()
	var calls atomic.Int32
	do := func(context.Context) error {
		if calls.Add(1) == 1 {
			return errors.New("not yet")
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for range 3 {
		go work.Once(ctx, s, "once", do, nil)
	}
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("the work was done before UpgradeAfter passed")
	}
	// A failure is done again RetryAfter — ten seconds — later.
	for deadline := time.Now().Add(20 * time.Second); calls.Load() != 2; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the work was done %d times, never again once it failed", calls.Load())
		}
	}
	time.Sleep(200 * time.Millisecond)
	returned := make(chan error, 1)
	go func() { returned <- work.Once(ctx, s, "once", do, nil) }()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a replica started after the work was done did not return")
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("the work was done %d times, want twice: a failure, and once", n)
	}
}

func upgradeAfter(d time.Duration) (restore func()) {
	was := work.UpgradeAfter
	work.UpgradeAfter = d
	return func() { work.UpgradeAfter = was }
}

// Work a release before jobs marked, with no job — due now, due later, or held by a holder that
// died — is adopted UpgradeAfter after a runner of this release starts: done, each when due, and
// once however many replicas start; work marked meanwhile with its job is done as it comes.
func TestAdopt(t *testing.T) {
	defer upgradeAfter(200 * time.Millisecond)()
	s, d := newStore(), newDoer()
	later := time.Now().Add(600 * time.Millisecond)
	s.markOld(1, time.Time{})
	s.markOld(2, later)
	s.mu.Lock()
	s.row(3).holder, s.row(3).until = "a dead replica", time.Now().Add(-time.Second)
	s.mu.Unlock()
	for _, h := range []string{"a", "b"} {
		run(t, &work.Runner{Kind: "q", Store: s, Do: d.do(h, nil), TTL: time.Second, Holder: h})
	}
	s.Mark(context.Background(), "q", 4)
	eventually(t, "the work marked with its job done", func() bool { return d.count(4) == 1 })
	if d.count(1)+d.count(2)+d.count(3) != 0 {
		t.Fatal("what a release before left was done before UpgradeAfter passed")
	}
	eventually(t, "what a release before left due now done", func() bool { return d.count(1) == 1 && d.count(3) == 1 })
	if d.count(2) != 0 {
		t.Fatal("work a release before marked for later was done before it was due")
	}
	eventually(t, "what a release before marked for later done when due", func() bool { return d.count(2) == 1 })
	if time.Now().Before(later) {
		t.Error("work marked for later was done before it was due")
	}
	time.Sleep(300 * time.Millisecond)
	for k := int64(1); k <= 4; k++ {
		if n := d.count(k); n != 1 {
			t.Errorf("key %d's work was done %d times, want once", k, n)
		}
	}
}

// A step pended in an attempt is enqueued as the attempt is about to commit, and done by a
// consumer with its body and its age: looked at again when it says later, tried again when it
// fails, and done once.
func TestSteps(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobs := work.Jobs{Queue: bus.NewLocalQueue(), Prefix: "t"}
	db, tx := new(int), new(int)
	work.Register(db, jobs)
	work.Begin(db, tx)
	if err := work.Pend(tx, "relay", 7, time.Time{}, []byte("seq 3")); err != nil {
		t.Fatal(err)
	}
	if err := work.Flush(ctx, tx); err != nil {
		t.Fatal(err)
	}
	work.End(tx)
	var mu sync.Mutex
	var calls []string
	done := make(chan struct{})
	go work.Consume(ctx, jobsOf{jobs}, "relay", 1, 20*time.Millisecond, func(_ context.Context, key int64, body []byte, age time.Duration) error {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, string(body))
		switch len(calls) {
		case 1:
			return work.Later{After: 30 * time.Millisecond}
		case 2:
			return errors.New("not yet")
		}
		if key != 7 || age < 50*time.Millisecond {
			t.Errorf("the step of key %d, %s old", key, age)
		}
		close(done)
		return nil
	}, nil)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the step was never done")
	}
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 3 || calls[2] != "seq 3" {
		t.Errorf("the step's calls: %q", calls)
	}
}

// RunReady does the jobs ready as it is called, each once — work they mark is the next call's —
// and reads no row with none.
func TestRunReady(t *testing.T) {
	ctx := context.Background()
	s, d := newStore(), newDoer()
	var r *work.Runner
	r = &work.Runner{Kind: "q", Store: s, TTL: time.Second, Holder: "a", Do: d.do("a", func(ctx context.Context, key int64) error {
		if key < 3 {
			return s.Mark(ctx, "q", key+1)
		}
		return nil
	})}
	if n := r.RunReady(ctx); n != 0 || s.looks.Load() != 0 {
		t.Fatalf("with no jobs: %d done, %d looks", n, s.looks.Load())
	}
	s.Mark(ctx, "q", 1)
	for want := int64(1); want <= 3; want++ {
		if n := r.RunReady(ctx); n != 1 || d.count(want) != 1 {
			t.Fatalf("call %d: %d jobs, key %d done %d times", want, n, want, d.count(want))
		}
	}
	if n := r.RunReady(ctx); n != 0 {
		t.Errorf("a call after the chain ended did %d jobs", n)
	}
}

// A runner stopping short whose letting go fails — its database going away beneath it — leaves its
// job unacknowledged: once the lease lapses, the job brings the work to another replica, which does
// it. Acknowledged, the work would be held by a holder that walked away, the mark's own job leaving
// it to that holder: brought by nothing.
func TestLetFailsAsItStops(t *testing.T) {
	s, d := newStore(), newDoer()
	ttl := 150 * time.Millisecond
	started := make(chan struct{})
	stop := run(t, &work.Runner{Kind: "q", Store: s, TTL: ttl, Holder: "a", Do: d.do("a", func(ctx context.Context, key int64) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})})
	s.Mark(context.Background(), "q", 1)
	<-started
	s.failLet.Store(1)
	stop()
	if r := s.state(1); r.holder == "" {
		t.Fatal("the let went through; the test needs it to fail")
	}
	run(t, &work.Runner{Kind: "q", Store: s, TTL: ttl, Holder: "b", Do: d.do("b", nil)})
	eventually(t, "another replica doing what the stopped one held", func() bool { return d.count(1) == 2 })
}

// A runner stopped short marks the work and lets it go; another replica that takes the mark's job,
// or the let's — both enqueued before their transactions commit — while the work is still held
// finds it held under another job, and leaves the mark to its holder, which walks away. The work is
// brought back by the stopped runner's own job, handed on once the work is let go, and done.
func TestStoppedShortWhileItsMarkIsTaken(t *testing.T) {
	s, d := newStore(), newDoer()
	started := make(chan struct{})
	stopA := run(t, &work.Runner{Kind: "q", Store: s, TTL: time.Minute, Holder: "a", Do: d.do("a", func(ctx context.Context, key int64) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})})
	s.Mark(context.Background(), "q", 1)
	<-started
	run(t, &work.Runner{Kind: "q", Store: s, TTL: time.Minute, Holder: "b", Workers: 1, Do: d.do("b", nil)})
	// The stopped runner's let holds the work until b has looked at both jobs — the mark's and
	// the let's — and left them.
	before := s.looks.Load()
	wait := func() {
		for deadline := time.Now().Add(5 * time.Second); s.looks.Load() < before+2 && time.Now().Before(deadline); {
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.inLet.Store(&wait)
	stopA()
	s.inLet.Store(nil)
	if s.looks.Load() < before+2 {
		t.Fatalf("b looked %d times while the work was held, want both jobs", s.looks.Load()-before)
	}
	eventually(t, "the work the stopped runner held done again", func() bool { return d.count(1) == 2 })
	eventually(t, "the work left clean and free", func() bool { r := s.state(1); return !r.dirty && r.holder == "" })
}

// jobsOf is jobs as a work.Source.
type jobsOf struct{ j work.Jobs }

func (j jobsOf) Jobs() work.Jobs { return j.j }

// A database's jobs are its latest standing registration's: replicas that share a database handle,
// as a test's do, each register their own bus, and one that stops withdraws its own, leaving the
// others'; none standing, the last withdrawn.
func TestRegistrations(t *testing.T) {
	db := new(int)
	a, b := work.Jobs{Queue: bus.NewLocalQueue(), Prefix: "a"}, work.Jobs{Queue: bus.NewLocalQueue(), Prefix: "b"}
	withdrawA := work.Register(db, a)
	withdrawB := work.Register(db, b)
	if got := work.JobsOf(db); got.Prefix != "b" {
		t.Fatalf("with both standing: %q, want the latest", got.Prefix)
	}
	withdrawB()
	if got := work.JobsOf(db); got.Prefix != "a" {
		t.Errorf("the latest withdrawn: %q, want the one standing", got.Prefix)
	}
	withdrawB()
	if got := work.JobsOf(db); got.Prefix != "a" {
		t.Errorf("withdrawn twice: %q", got.Prefix)
	}
	withdrawA()
	if got := work.JobsOf(db); got.Prefix != "a" {
		t.Errorf("none standing: %q, want the last withdrawn", got.Prefix)
	}
	work.Register(db, b)
	if got := work.JobsOf(db); got.Prefix != "b" {
		t.Errorf("registered again: %q", got.Prefix)
	}
}
