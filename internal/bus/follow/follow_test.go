package follow_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
)

// log is a Log in memory: each key's events numbered from 1, read two at a time.
type log struct {
	mu   sync.Mutex
	keys map[int64]int64 // key -> its last seq
	// asOf, by legacy place, is how far each key's log reached then.
	asOf map[int64]map[int64]int64
}

func ev(key, seq int64) bus.Event { return bus.Event{Topic: "t", Key: key, Seq: seq} }

func (l *log) Read(_ context.Context, key, after int64) ([]bus.Event, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []bus.Event
	for s := after + 1; s <= l.keys[key] && len(out) < 2; s++ {
		out = append(out, ev(key, s))
	}
	return out, nil
}

func (l *log) Heads(_ context.Context, asOf int64, fn func(key, seq int64) error) error {
	l.mu.Lock()
	heads := l.keys
	if asOf > 0 {
		heads = l.asOf[asOf]
	}
	keys := make([]int64, 0, len(heads))
	for k := range heads {
		keys = append(keys, k)
	}
	l.mu.Unlock()
	slices.Sort(keys)
	for _, k := range keys {
		if err := fn(k, heads[k]); err != nil {
			return err
		}
	}
	return nil
}

// applier records what it applies; legacy, when set, is the legacy feed's cursor.
type applier struct {
	mu      sync.Mutex
	cursors map[int64]int64
	applied []bus.Event
	legacy  int64
	left    bool
	fail    func(bus.Event) error
}

func (a *applier) Cursor(_ context.Context, key int64) (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cursors[key], nil
}

func (a *applier) Apply(_ context.Context, e bus.Event) error {
	if a.fail != nil {
		if err := a.fail(e); err != nil {
			return err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
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

func (a *applier) LegacyCursor(context.Context) (int64, error) { return a.legacy, nil }
func (a *applier) Seed(_ context.Context, key, seq int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.cursors[key]; !ok {
		a.cursors[key] = seq
	}
	return nil
}
func (a *applier) LeaveLegacy(context.Context) error { a.legacy, a.left = 0, true; return nil }

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
		s.errs = append(s.errs, h(ctx, e))
	}
	close(s.delivered)
	<-ctx.Done()
	return ctx.Err()
}

// run follows s until it has delivered everything, returning Run's error if it stopped first.
func run(t *testing.T, s *stream, l *log, a *applier) error {
	t.Helper()
	s.delivered = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (&follow.Follower{Stream: s, Topic: "t", Group: "g", Log: l, Apply: a}).Run(ctx)
	}()
	select {
	case err := <-done:
		return err
	case <-s.delivered:
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
	if err := run(t, s, l, a); err != nil {
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

// A follower of a legacy feed seeds each key's cursor from where that feed had reached, once, and
// catches up from there.
func TestLeaveLegacy(t *testing.T) {
	l := &log{keys: map[int64]int64{1: 4, 2: 2}, asOf: map[int64]map[int64]int64{7: {1: 3, 2: 2}}}
	a := &applier{cursors: map[int64]int64{}, legacy: 7}
	if err := run(t, &stream{}, l, a); err != nil {
		t.Fatal(err)
	}
	if !a.left || len(a.applied) != 1 || a.applied[0].Key != 1 || a.applied[0].Seq != 4 {
		t.Errorf("left %v, applied %v: want key 1's 4 alone, the rest applied under the legacy feed", a.left, a.applied)
	}
	a.once(t, map[int64]int64{1: 4, 2: 2})
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
	if err := run(t, &stream{}, l, a); !errors.Is(err, down) {
		t.Errorf("catching up through a failure: %v", err)
	}
	a.fail = nil
	s := &stream{deliver: []bus.Event{ev(1, 9)}}
	if err := run(t, s, l, a); err != nil {
		t.Fatal(err)
	}
	if len(s.errs) != 1 || s.errs[0] == nil {
		t.Errorf("an event past its log's end: %v", s.errs)
	}
	a.once(t, map[int64]int64{1: 2})
}
