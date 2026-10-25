package follow_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
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

// stream delivers the events it is given, in the order given, then says so and waits.
type stream struct {
	mu        sync.Mutex
	deliver   []bus.Event
	group     bool
	errs      []error
	delivered chan struct{}
}

func (*stream) Publish(context.Context, ...bus.Event) error { return nil }
func (s *stream) CreateGroup(context.Context, string, string) error {
	s.group = true
	return nil
}
func (s *stream) Subscribe(ctx context.Context, _, _ string, h bus.Handler) error {
	if !s.group {
		return errors.New("subscribed before the group was made")
	}
	for _, e := range s.deliver {
		err := h(ctx, e)
		s.mu.Lock()
		s.errs = append(s.errs, err)
		s.mu.Unlock()
	}
	close(s.delivered)
	<-ctx.Done()
	return ctx.Err()
}

// run follows s until it has delivered everything and every key in want has its cursor there,
// returning Run's error if it stopped first.
func run(t *testing.T, s *stream, l *log, a *applier, want map[int64]int64) error {
	t.Helper()
	s.delivered = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (&follow.Follower{Stream: s, Topic: "t", Group: "g", Log: l, Apply: a, BusyWait: 50 * time.Millisecond}).Run(ctx)
	}()
	deadline := time.After(10 * time.Second)
	for delivered := false; ; {
		select {
		case err := <-done:
			return err
		case <-s.delivered:
			delivered, s.delivered = true, nil
		case <-deadline:
			t.Fatal("the follower never caught up")
		case <-time.After(5 * time.Millisecond):
		}
		reached := delivered
		for k, c := range want {
			reached = reached && a.at(k) >= c
		}
		if reached {
			break
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// A follower catches up every key behind its log, then applies what the stream brings once each,
// in each key's order: duplicates acknowledged, gaps filled from the log first.
func TestFollow(t *testing.T) {
	l := &log{keys: map[int64]int64{1: 5, 2: 3, 3: 0}}
	a := &applier{cursors: map[int64]int64{1: 2}}
	// The stream: a duplicate, an event two past what the log held at the start (its gap read
	// from the log), the gap's own events late, and a key the log had not yet reached.
	s := &stream{deliver: []bus.Event{ev(1, 4), ev(2, 6), ev(2, 4), ev(2, 5), ev(1, 6), ev(4, 2)}}
	l.keys[2], l.keys[1], l.keys[4] = 6, 6, 2
	if err := run(t, s, l, a, map[int64]int64{1: 6, 2: 6, 4: 2}); err != nil {
		t.Fatal(err)
	}
	for i, err := range s.errs {
		if err != nil {
			t.Errorf("delivery %d: %v", i, err)
		}
	}
	a.once(t, map[int64]int64{1: 6, 2: 6, 4: 2})
	if len(a.applied) != 4+6+2 {
		t.Errorf("applied %d events, want each of key 1's 3–6, key 2's 1–6, and key 4's 1–2 once", len(a.applied))
	}
}

// An event the applier cannot apply yet is refused, to come again; one whose log ends before it is
// an error.
func TestRefusals(t *testing.T) {
	down := errors.New("down")
	l := &log{keys: map[int64]int64{1: 2}}
	a := &applier{cursors: map[int64]int64{}, fail: func(e bus.Event) error {
		if e.Seq == 2 {
			return down
		}
		return nil
	}}
	if err := run(t, &stream{}, l, a, map[int64]int64{1: 2}); !errors.Is(err, down) {
		t.Errorf("catching up through a failure: %v", err)
	}
	a.fail = nil
	s := &stream{deliver: []bus.Event{ev(1, 9)}}
	if err := run(t, s, l, a, map[int64]int64{1: 2}); err != nil {
		t.Fatal(err)
	}
	if len(s.errs) != 1 || s.errs[0] == nil {
		t.Errorf("an event past its log's end: %v", s.errs)
	}
	a.once(t, map[int64]int64{1: 2})
}

// Catching up reads a page of keys' cursors at once, and goes no further for a key its cursor has
// reached.
func TestCatchUpInPages(t *testing.T) {
	heads := map[int64]int64{1: 3, 2: 2, 3: 4, 4: 1, 5: 2, 6: 1}
	l := &log{keys: maps.Clone(heads)}
	a := &applier{cursors: map[int64]int64{1: 3, 2: 2, 4: 1, 6: 1}}
	if err := run(t, &stream{}, l, a, heads); err != nil {
		t.Fatal(err)
	}
	a.once(t, heads)
	slices.Sort(a.alone)
	if l.pages != 3 || a.pages != 3 || !slices.Equal(a.alone, []int64{3, 5}) {
		t.Errorf("%d pages of heads, %d reads of a page's cursors, keys read alone %v; want 3, 3, and keys 3 and 5", l.pages, a.pages, a.alone)
	}
}

// What the stream brings is applied while catching up waits on a key's log.
func TestFollowBesideCatchingUp(t *testing.T) {
	stuck := make(chan struct{})
	l := &log{keys: map[int64]int64{1: 3, 2: 1}, block: map[int64]chan struct{}{1: stuck}}
	a := &applier{cursors: map[int64]int64{}}
	s := &stream{deliver: []bus.Event{ev(2, 2)}}
	l.keys[2] = 2
	go func() {
		// Released once key 2 has its event from the stream, which needs no catching up first.
		for a.at(2) < 2 {
			time.Sleep(time.Millisecond)
		}
		close(stuck)
	}()
	if err := run(t, s, l, a, map[int64]int64{1: 3, 2: 2}); err != nil {
		t.Fatal(err)
	}
	a.once(t, map[int64]int64{1: 3, 2: 2})
}

// A key another member is applying is left to it as the follower catches up, and an event of it the
// stream brings waits a while for it, then is refused, to come again; one freed meanwhile applies.
func TestBusy(t *testing.T) {
	l := &log{keys: map[int64]int64{1: 2, 2: 1}}
	a := &applier{cursors: map[int64]int64{}, busy: map[int64]bool{1: true}}
	s := &stream{deliver: []bus.Event{ev(1, 3)}}
	l.keys[1] = 3
	if err := run(t, s, l, a, map[int64]int64{2: 1}); err != nil {
		t.Fatal(err)
	}
	if len(s.errs) != 1 || !errors.Is(s.errs[0], follow.ErrBusy) {
		t.Errorf("an event of a key another member applies: %v", s.errs)
	}
	a.once(t, map[int64]int64{1: 0, 2: 1})
}

func TestBusyWhileWaiting(t *testing.T) {
	l := &log{keys: map[int64]int64{1: 2}}
	a := &applier{cursors: map[int64]int64{1: 1}, busy: map[int64]bool{1: true}}
	s := &stream{deliver: []bus.Event{ev(1, 2)}}
	go func() {
		time.Sleep(20 * time.Millisecond)
		a.mu.Lock()
		a.busy[1] = false
		a.mu.Unlock()
	}()
	if err := run(t, s, l, a, map[int64]int64{1: 2}); err != nil {
		t.Fatal(err)
	}
	if len(s.errs) != 1 || s.errs[0] != nil {
		t.Errorf("an event of a key freed as it waited: %v", s.errs)
	}
	a.once(t, map[int64]int64{1: 2})
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
// cursor — what the stream brings applied through the log, its page with it — so every event is
// applied once, in order, a page at a time, and none alone through Apply.
func TestBatches(t *testing.T) {
	l := &log{keys: map[int64]int64{1: 6, 2: 3}}
	b := &batcher{applier: &applier{cursors: map[int64]int64{1: 1}}}
	s := &stream{deliver: []bus.Event{ev(1, 6), ev(1, 3), ev(2, 3), ev(2, 1)}, delivered: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (&follow.Follower{Stream: s, Topic: "t", Group: "g", Log: l, Apply: b, BusyWait: 50 * time.Millisecond}).Run(ctx)
	}()
	deadline := time.After(10 * time.Second)
	for b.at(1) < 6 || b.at(2) < 3 {
		select {
		case err := <-done:
			t.Fatal(err)
		case <-deadline:
			t.Fatal("the follower never caught up")
		case <-time.After(5 * time.Millisecond):
		}
	}
	<-s.delivered
	cancel()
	<-done
	for i, err := range s.errs {
		if err != nil {
			t.Errorf("delivery %d: %v", i, err)
		}
	}
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
