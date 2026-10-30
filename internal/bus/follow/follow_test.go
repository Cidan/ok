package follow_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/work"
)

// log is a Log in memory: each key's events numbered from 1, read two at a time, its heads two
// keys a page. A key in block waits to be read until its channel closes.
type log struct {
	mu    sync.Mutex
	keys  map[int64]int64 // key -> its last seq
	block map[int64]chan struct{}
	pages int
}

func ev(key, seq int64) bus.Event { return bus.Event{Topic: "t", Key: key, Seq: seq} }

func (l *log) Read(ctx context.Context, key, after int64) ([]bus.Event, error) {
	l.mu.Lock()
	wait := l.block[key]
	l.mu.Unlock()
	if wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []bus.Event
	for s := after + 1; s <= l.keys[key] && len(out) < 2; s++ {
		out = append(out, ev(key, s))
	}
	return out, nil
}

func (l *log) Heads(_ context.Context, fn func([]follow.Head) error) error {
	l.mu.Lock()
	var heads []follow.Head
	for k, s := range l.keys {
		heads = append(heads, follow.Head{Key: k, Seq: s})
	}
	l.mu.Unlock()
	slices.SortFunc(heads, func(a, b follow.Head) int { return int(a.Key - b.Key) })
	for len(heads) > 0 {
		page := heads[:min(2, len(heads))]
		heads = heads[len(page):]
		l.mu.Lock()
		l.pages++
		l.mu.Unlock()
		if err := fn(page); err != nil {
			return err
		}
	}
	return nil
}

// applier records what it applies, and how it was asked for cursors: reads of several keys, and
// the keys read alone. A key in busy is another member's to apply.
type applier struct {
	mu      sync.Mutex
	cursors map[int64]int64
	applied []bus.Event
	fail    func(bus.Event) error
	busy    map[int64]bool
	pages   int
	alone   []int64
}

func (a *applier) Cursors(_ context.Context, keys []int64) (map[int64]int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(keys) == 1 {
		a.alone = append(a.alone, keys[0])
	} else {
		a.pages++
	}
	out := map[int64]int64{}
	for _, k := range keys {
		out[k] = a.cursors[k]
	}
	return out, nil
}

// at is key's cursor.
func (a *applier) at(key int64) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cursors[key]
}

func (a *applier) Apply(_ context.Context, e bus.Event) error {
	if a.fail != nil {
		if err := a.fail(e); err != nil {
			return err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.busy[e.Key] {
		return follow.ErrBusy
	}
	if a.cursors[e.Key] >= e.Seq {
		return nil
	}
	if a.cursors[e.Key] != e.Seq-1 {
		return errors.New("applied out of order")
	}
	a.cursors[e.Key] = e.Seq
	a.applied = append(a.applied, e)
	return nil
}

// once checks every key's events were applied once each, in order, through want.
func (a *applier) once(t *testing.T, want map[int64]int64) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	next := map[int64]int64{}
	for k, c := range a.cursors {
		next[k] = c
	}
	got := map[int64][]int64{}
	for _, e := range a.applied {
		got[e.Key] = append(got[e.Key], e.Seq)
	}
	for k, last := range want {
		if a.cursors[k] != last {
			t.Errorf("key %d's cursor %d, want %d", k, a.cursors[k], last)
		}
		for i := 1; i < len(got[k]); i++ {
			if got[k][i] != got[k][i-1]+1 {
				t.Errorf("key %d applied %v", k, got[k])
			}
		}
	}
}

// env is a follower's service: its keyed work in a database of its own, on a queue in the process,
// and how often it registered.
type env struct {
	q          *bus.LocalQueue
	work       work.Store
	registered atomic.Int32
}

func newEnv(t *testing.T) *env {
	t.Helper()
	p := testdb.Pool(t)
	if _, err := p.Exec(context.Background(), pgdata.WorkSchema); err != nil {
		t.Fatal(err)
	}
	e := &env{q: bus.NewLocalQueue(), work: pgdata.Work(p)}
	if _, err := work.RegisterStore(e.work, work.Jobs{Queue: e.q, Prefix: "f"}); err != nil {
		t.Fatal(err)
	}
	return e
}

// follow runs a follower of l into a until the returned stop is called.
func (e *env) follow(t *testing.T, l *log, a follow.Applier) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- (&follow.Follower{Topic: "t", Log: l, Apply: a, Work: e.work, Workers: 4,
			Register: func(context.Context) error { e.registered.Add(1); return nil }}).Run(ctx)
	}()
	return func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}
}

// relay relays events to the follower, as their log's source does.
func (e *env) relay(t *testing.T, events ...bus.Event) {
	t.Helper()
	if err := follow.Relay(context.Background(), e.q, "t", []string{"f"}, events); err != nil {
		t.Fatal(err)
	}
}

// until waits for every key of want to have its cursor there.
func until(t *testing.T, a *applier, want map[int64]int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		reached := true
		for k, c := range want {
			reached = reached && a.at(k) >= c
		}
		if reached {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the follower never reached %v", want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A follower registers, catches up every key behind its log, then applies what each relay names,
// once each, in each key's order: a relay naming a key the follower is past, or past the key's
// last event the log already held, applies through the log.
func TestFollow(t *testing.T) {
	e := newEnv(t)
	l := &log{keys: map[int64]int64{1: 5, 2: 3, 3: 0}}
	a := &applier{cursors: map[int64]int64{1: 2}}
	stop := e.follow(t, l, a)
	defer stop()
	until(t, a, map[int64]int64{1: 5, 2: 3})
	l.mu.Lock()
	l.keys[2], l.keys[1], l.keys[4] = 6, 6, 2
	l.mu.Unlock()
	e.relay(t, ev(1, 4), ev(2, 6), ev(2, 4), ev(1, 6), ev(4, 2))
	until(t, a, map[int64]int64{1: 6, 2: 6, 4: 2})
	time.Sleep(50 * time.Millisecond)
	a.once(t, map[int64]int64{1: 6, 2: 6, 4: 2})
	if len(a.applied) != 4+6+2 {
		t.Errorf("applied %d events, want each of key 1's 3–6, key 2's 1–6, and key 4's 1–2 once", len(a.applied))
	}
	if e.registered.Load() != 1 {
		t.Errorf("registered %d times", e.registered.Load())
	}
}

// Catching up reads a page of keys' cursors at once, marks only the keys behind, and is done once
// for the service: a follower started again catches nothing up.
func TestCatchUpOnce(t *testing.T) {
	e := newEnv(t)
	heads := map[int64]int64{1: 3, 2: 2, 3: 4, 4: 1, 5: 2, 6: 1}
	l := &log{keys: maps.Clone(heads)}
	a := &applier{cursors: map[int64]int64{1: 3, 2: 2, 4: 1, 6: 1}}
	stop := e.follow(t, l, a)
	until(t, a, heads)
	stop()
	a.once(t, heads)
	slices.Sort(a.alone)
	if l.pages != 3 || a.pages != 3 || !slices.Equal(a.alone, []int64{3, 5}) {
		t.Errorf("%d pages of heads, %d reads of a page's cursors, keys read alone %v; want 3, 3, and keys 3 and 5", l.pages, a.pages, a.alone)
	}
	l.mu.Lock()
	l.keys[1] = 4
	l.mu.Unlock()
	stop = e.follow(t, l, a)
	time.Sleep(300 * time.Millisecond)
	stop()
	if l.pages != 3 || a.at(1) != 3 {
		t.Errorf("started again: %d pages of heads, key 1 at %d; want no catching up", l.pages, a.at(1))
	}
}

// A key's work that fails is done again a while later, and applies then.
func TestRefusals(t *testing.T) {
	e := newEnv(t)
	var fails atomic.Int32
	l := &log{keys: map[int64]int64{1: 2}}
	a := &applier{cursors: map[int64]int64{}, fail: func(ev bus.Event) error {
		if ev.Seq == 2 && fails.Add(1) == 1 {
			return errors.New("down")
		}
		return nil
	}}
	stop := e.follow(t, l, a)
	defer stop()
	until(t, a, map[int64]int64{1: 2})
	a.once(t, map[int64]int64{1: 2})
}

// What a relay names is applied while catching up waits on another key's log.
func TestFollowBesideCatchingUp(t *testing.T) {
	e := newEnv(t)
	stuck := make(chan struct{})
	l := &log{keys: map[int64]int64{1: 3, 2: 1}, block: map[int64]chan struct{}{1: stuck}}
	a := &applier{cursors: map[int64]int64{}}
	stop := e.follow(t, l, a)
	defer stop()
	until(t, a, map[int64]int64{2: 1})
	l.mu.Lock()
	l.keys[2] = 2
	l.mu.Unlock()
	e.relay(t, ev(2, 2))
	until(t, a, map[int64]int64{2: 2})
	close(stuck)
	until(t, a, map[int64]int64{1: 3})
	a.once(t, map[int64]int64{1: 3, 2: 2})
}

// A key its applier finds held is done again a moment later, and applies once freed.
func TestBusy(t *testing.T) {
	e := newEnv(t)
	l := &log{keys: map[int64]int64{1: 2, 2: 1}}
	a := &applier{cursors: map[int64]int64{}, busy: map[int64]bool{1: true}}
	stop := e.follow(t, l, a)
	defer stop()
	until(t, a, map[int64]int64{2: 1})
	time.Sleep(300 * time.Millisecond)
	if a.at(1) != 0 {
		t.Fatalf("a held key applied to %d", a.at(1))
	}
	a.mu.Lock()
	a.busy[1] = false
	a.mu.Unlock()
	until(t, a, map[int64]int64{1: 2})
	a.once(t, map[int64]int64{1: 2, 2: 1})
}

// batcher is an applier that applies a page at a time: it records each page, and counts the events
// handed it one alone, through Apply.
type batcher struct {
	*applier
	batches [][]int64
	alone   int
}

func (b *batcher) Apply(ctx context.Context, e bus.Event) error {
	b.mu.Lock()
	b.alone++
	b.mu.Unlock()
	return b.applier.Apply(ctx, e)
}

func (b *batcher) ApplyBatch(ctx context.Context, evs []bus.Event) error {
	b.mu.Lock()
	if b.busy[evs[0].Key] {
		b.mu.Unlock()
		return follow.ErrBusy
	}
	var seqs []int64
	for _, e := range evs {
		seqs = append(seqs, e.Seq)
	}
	b.batches = append(b.batches, seqs)
	b.mu.Unlock()
	for _, e := range evs {
		if err := b.applier.Apply(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// A batch applier is handed each page of a key's log the follower reads, from the event after its
// cursor, so every event is applied once, in order, a page at a time, and none alone through Apply.
func TestBatches(t *testing.T) {
	e := newEnv(t)
	l := &log{keys: map[int64]int64{1: 6, 2: 3}}
	b := &batcher{applier: &applier{cursors: map[int64]int64{1: 1}}}
	stop := e.follow(t, l, b)
	until(t, b.applier, map[int64]int64{1: 6, 2: 3})
	e.relay(t, ev(1, 6), ev(1, 3), ev(2, 3), ev(2, 1))
	time.Sleep(100 * time.Millisecond)
	stop()
	b.once(t, map[int64]int64{1: 6, 2: 3})
	if len(b.applied) != 5+3 {
		t.Errorf("applied %d events, want key 1's 2–6 and key 2's 1–3 once", len(b.applied))
	}
	if b.alone != 0 {
		t.Errorf("%d events handed the batch applier one alone", b.alone)
	}
	for _, page := range b.batches {
		if len(page) == 0 || len(page) > 2 {
			t.Errorf("a page of %v: the log reads two at a time", page)
		}
	}
}

// A relay enqueues, on each follower's queue, one job a key, through its last event's seq.
func TestRelay(t *testing.T) {
	q := bus.NewLocalQueue()
	if err := follow.Relay(context.Background(), q, "t", []string{"a", "b"}, []bus.Event{ev(1, 2), ev(2, 1), ev(1, 3)}); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"a", "b"} {
		ls, err := q.Lease(context.Background(), work.Jobs{Queue: q, Prefix: f}.QueueOf("relay.t"), bus.LeaseOptions{Max: 10, Visibility: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, l := range ls {
			ids = append(ids, l.Job().ID)
		}
		slices.Sort(ids)
		if !slices.Equal(ids, []string{"1/3", "2/1"}) {
			t.Errorf("%s's jobs: %v, want each key once, through its last", f, ids)
		}
	}
}
