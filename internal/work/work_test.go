package work_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/work"
)

// store is a work.Store in memory.
type store struct {
	mu    sync.Mutex
	rows  map[int64]*row
	renew func(key int64) bool // false loses the lease
}

type row struct {
	dirty  bool
	holder string
	until  time.Time
}

func newStore() *store { return &store{rows: map[int64]*row{}} }

func (s *store) Mark(_ context.Context, _ string, key int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rows[key] == nil {
		s.rows[key] = &row{}
	}
	s.rows[key].dirty = true
	return nil
}

func (s *store) Take(_ context.Context, _ string, key int64, holder string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.rows[key]
	if r == nil || (r.holder != "" && time.Now().Before(r.until)) {
		return false, nil
	}
	r.holder, r.until, r.dirty = holder, time.Now().Add(ttl), false
	return true, nil
}

func (s *store) Renew(_ context.Context, _ string, key int64, holder string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.rows[key]
	if r == nil || r.holder != holder || (s.renew != nil && !s.renew(key)) {
		return false, nil
	}
	r.until = time.Now().Add(ttl)
	return true, nil
}

func (s *store) Release(_ context.Context, _ string, key int64, holder string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.rows[key]
	if r == nil || r.holder != holder {
		return false, nil
	}
	if r.dirty {
		r.dirty = false
		return true, nil
	}
	r.holder = ""
	return false, nil
}

func (s *store) Due(_ context.Context, _ string, limit int) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []int64
	for k, r := range s.rows {
		if (r.dirty && r.holder == "") || (r.holder != "" && time.Now().After(r.until)) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out[:min(limit, len(out))], nil
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

// Two replicas do every key's work between them, each key on one at a time.
func TestRunners(t *testing.T) {
	s, d := newStore(), newDoer()
	for _, h := range []string{"a", "b"} {
		run(t, &work.Runner{Kind: "q", Store: s, Do: d.do(h, nil), Idle: 5 * time.Millisecond, TTL: time.Second, Holder: h})
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

// Work marked while it is being done is done again by its holder before it lets it go, with no
// second look: the runner looks once, then waits an hour.
func TestMarkedWhileDoing(t *testing.T) {
	s, d := newStore(), newDoer()
	s.Mark(context.Background(), "q", 1)
	var calls atomic.Int32
	run(t, &work.Runner{Kind: "q", Store: s, Idle: time.Hour, TTL: time.Second, Holder: "a", Do: d.do("a", func(ctx context.Context, key int64) error {
		if calls.Add(1) == 1 {
			s.Mark(ctx, "q", key)
		}
		return nil
	})})
	eventually(t, "the work done twice", func() bool { return d.count(1) == 2 })
	eventually(t, "the work let go", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.rows[1].holder == "" && !s.rows[1].dirty
	})
}

// Work that fails is marked again, and done again a while later.
func TestFailures(t *testing.T) {
	s, d := newStore(), newDoer()
	var fails atomic.Int32
	run(t, &work.Runner{Kind: "q", Store: s, Idle: 5 * time.Millisecond, TTL: time.Second, Holder: "a", Do: d.do("a", func(context.Context, int64) error {
		if fails.Add(1) <= 2 {
			return errors.New("not yet")
		}
		return nil
	})})
	s.Mark(context.Background(), "q", 1)
	eventually(t, "the work done once it stopped failing", func() bool { return d.count(1) == 3 })
	eventually(t, "the work let go", func() bool {
		keys, _ := s.Due(context.Background(), "q", 10)
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(keys) == 0 && s.rows[1].holder == ""
	})
}

// A lease lost ends the work's context with ErrLost; one whose runner stops is let go at once, and
// another replica takes it.
func TestLostAndLetGo(t *testing.T) {
	s, d := newStore(), newDoer()
	s.renew = func(int64) bool { return false }
	lostIt := make(chan error, 1)
	// Marked before the runner looks: it looks once, then waits an hour.
	s.Mark(context.Background(), "q", 1)
	run(t, &work.Runner{Kind: "q", Store: s, Idle: time.Hour, TTL: 30 * time.Millisecond, Holder: "a", Do: d.do("a", func(ctx context.Context, key int64) error {
		<-ctx.Done()
		lostIt <- context.Cause(ctx)
		return ctx.Err()
	})})
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
	s2.Mark(context.Background(), "q", 1)
	stop := run(t, &work.Runner{Kind: "q", Store: s2, Idle: time.Hour, TTL: time.Hour, Holder: "a", Do: d2.do("a", func(ctx context.Context, key int64) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})})
	<-started
	stop()
	run(t, &work.Runner{Kind: "q", Store: s2, Idle: 5 * time.Millisecond, TTL: time.Hour, Holder: "b", Do: d2.do("b", nil)})
	eventually(t, "another replica taking what the first let go", func() bool { return d2.count(1) == 2 })
}

// One worker does every key due, one after another, with no look between them: an hour's idle
// does not hold up the rest.
func TestOneWorker(t *testing.T) {
	s, d := newStore(), newDoer()
	for k := int64(1); k <= 10; k++ {
		s.Mark(context.Background(), "q", k)
	}
	run(t, &work.Runner{Kind: "q", Store: s, Do: d.do("a", nil), Idle: time.Hour, TTL: time.Second, Workers: 1, Holder: "a"})
	eventually(t, "every key's work done", func() bool {
		for k := int64(1); k <= 10; k++ {
			if d.count(k) != 1 {
				return false
			}
		}
		return true
	})
}
