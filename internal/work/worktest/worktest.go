// Package worktest is the conformance suite every keyed work store (internal/work) passes, and
// what tests of the work a role marks read of it: run it against a store with Run, giving it a
// new, empty store each test.
package worktest

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/work"
)

// Ready takes the jobs of kind ready now on s's queue, and returns the keys they name, each once,
// in order: the work a runner would be brought now. Taken, they are gone.
func Ready(t testing.TB, s work.Store, kind string) []int64 {
	t.Helper()
	j := s.Jobs()
	var keys []int64
	for {
		ls, err := j.Queue.Lease(context.Background(), j.QueueOf(kind), bus.LeaseOptions{Max: 100, Visibility: time.Minute})
		if err != nil {
			t.Fatalf("leasing %s's jobs: %v", kind, err)
		}
		if len(ls) == 0 {
			break
		}
		for _, l := range ls {
			if len(l.Job().Body) >= 8 {
				keys = append(keys, l.Job().Key)
			}
			if err := l.Ack(context.Background()); err != nil {
				t.Fatalf("acknowledging %s's job: %v", kind, err)
			}
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// Subject is a store under test, and what the suite needs of its database beyond it.
type Subject struct {
	Store work.Store
	// MarkOpen marks key's work of kind in a transaction of its own on the store's database, which
	// stays open once the mark is written until commit is closed, then commits: written is closed
	// once the mark is, and done receives how the transaction ended.
	MarkOpen func(ctx context.Context, kind string, key int64, commit <-chan struct{}) (written <-chan struct{}, done <-chan error)
}

// Run runs the suite, each test on a new store open makes.
func Run(t *testing.T, open func(t *testing.T) Subject) {
	for _, tc := range []struct {
		name string
		fn   func(t *testing.T, s work.Store, o Subject)
	}{
		{"MarksAreJobs", testMarksAreJobs},
		{"TakeAndRelease", testTakeAndRelease},
		{"MarkLaterKept", testMarkLaterKept},
		{"LapsedTakenOver", testLapsedTakenOver},
		{"LetBringsAJob", testLetBringsAJob},
		{"TakeWaitsForAMark", testTakeWaitsForAMark},
		{"Marked", testMarked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := open(t)
			tc.fn(t, o.Store, o)
		})
	}
}

func take(t *testing.T, s work.Store, kind string, key int64, holder string, ttl time.Duration) work.Taken {
	t.Helper()
	got, err := s.Take(context.Background(), kind, key, holder, ttl)
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	return got
}

// Every mark is a job of its kind's queue, due when the mark is; nothing else is.
func testMarksAreJobs(t *testing.T, s work.Store, _ Subject) {
	ctx := context.Background()
	if got := Ready(t, s, "q"); len(got) != 0 {
		t.Fatalf("jobs before any mark: %v", got)
	}
	for _, k := range []int64{2, 1, 2} {
		if err := s.Mark(ctx, "q", k); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.MarkAt(ctx, "q", 3, time.Now().Add(500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := s.Mark(ctx, "other", 9); err != nil {
		t.Fatal(err)
	}
	if got := Ready(t, s, "q"); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("q's jobs ready: %v, want 1 and 2", got)
	}
	time.Sleep(700 * time.Millisecond)
	if got := Ready(t, s, "q"); !slices.Equal(got, []int64{3}) {
		t.Errorf("q's jobs ready once 3's fell due: %v, want 3", got)
	}
	if got := Ready(t, s, "other"); !slices.Equal(got, []int64{9}) {
		t.Errorf("other's jobs: %v", got)
	}
	if p, err := s.Peek(ctx, "q", 4); err != nil || p.Held || p.Exists {
		t.Errorf("work never marked: %+v, %v", p, err)
	}
	if p, err := s.Peek(ctx, "q", 1); err != nil || p.Held || !p.Exists {
		t.Errorf("work marked: %+v, %v", p, err)
	}
}

// Work marked is taken by one holder at a time, its mark cleared and the work due; released
// unless marked again meanwhile, when its holder keeps it to do it again.
func testTakeAndRelease(t *testing.T, s work.Store, _ Subject) {
	ctx := context.Background()
	if err := s.Mark(ctx, "q", 1); err != nil {
		t.Fatal(err)
	}
	if got := take(t, s, "q", 1, "a", time.Minute); !got.Took || !got.Due {
		t.Fatalf("taking marked work: %+v", got)
	}
	if got := take(t, s, "q", 1, "b", time.Minute); got.Took {
		t.Fatal("a second holder took it")
	}
	if p, err := s.Peek(ctx, "q", 1); err != nil || !p.Held || p.Holder != "a" || p.Left <= 50*time.Second || p.Left > time.Minute {
		t.Fatalf("peeking at the held work: %+v, %v", p, err)
	}
	if err := s.Mark(ctx, "q", 1); err != nil {
		t.Fatal(err)
	}
	if again, err := s.Release(ctx, "q", 1, "b"); err != nil || again {
		t.Errorf("another's release: %v, %v", again, err)
	}
	if again, err := s.Release(ctx, "q", 1, "a"); err != nil || !again {
		t.Fatalf("releasing work marked meanwhile: again %v, %v", again, err)
	}
	if again, err := s.Release(ctx, "q", 1, "a"); err != nil || again {
		t.Fatalf("releasing it once done again: again %v, %v", again, err)
	}
	if p, err := s.Peek(ctx, "q", 1); err != nil || p.Held {
		t.Errorf("released work: %+v, %v", p, err)
	}
	if got := take(t, s, "q", 1, "b", time.Minute); !got.Took || got.Due {
		t.Errorf("taking released, unmarked work: %+v; want taken, nothing due", got)
	}
	if renewed, err := s.Renew(ctx, "q", 1, "a", time.Minute); err != nil || renewed {
		t.Errorf("a former holder's renewal: %v, %v", renewed, err)
	}
	if renewed, err := s.Renew(ctx, "q", 1, "b", time.Minute); err != nil || !renewed {
		t.Errorf("the holder's renewal: %v, %v", renewed, err)
	}
}

// A mark due later is kept by a take — the take tells when it falls due — and by a release, for
// its job; marked sooner, it is due sooner.
func testMarkLaterKept(t *testing.T, s work.Store, _ Subject) {
	ctx := context.Background()
	later := time.Now().Add(time.Hour)
	if err := s.MarkAt(ctx, "q", 1, later); err != nil {
		t.Fatal(err)
	}
	got := take(t, s, "q", 1, "a", time.Minute)
	if !got.Took || got.Due || got.Later.Sub(later).Abs() > time.Second {
		t.Fatalf("taking work marked for an hour on: %+v", got)
	}
	if again, err := s.Release(ctx, "q", 1, "a"); err != nil || again {
		t.Fatalf("releasing it: %v, %v", again, err)
	}
	if got := take(t, s, "q", 1, "a", time.Minute); got.Due || got.Later.IsZero() {
		t.Fatalf("after the release, the mark for later: %+v", got)
	}
	if err := s.MarkAt(ctx, "q", 1, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if again, err := s.Release(ctx, "q", 1, "a"); err != nil || !again {
		t.Errorf("work marked sooner, now due, as its holder lets go: again %v, %v", again, err)
	}
}

// Work whose holder's lease lapsed before it let go is taken by another, and due: its holder never
// finished it.
func testLapsedTakenOver(t *testing.T, s work.Store, _ Subject) {
	ctx := context.Background()
	if err := s.Mark(ctx, "q", 1); err != nil {
		t.Fatal(err)
	}
	if got := take(t, s, "q", 1, "a", 50*time.Millisecond); !got.Took {
		t.Fatal("taking it")
	}
	time.Sleep(time.Second)
	if p, err := s.Peek(ctx, "q", 1); err != nil || p.Held || p.Holder != "a" {
		t.Errorf("work whose lease lapsed: %+v, %v", p, err)
	}
	if got := take(t, s, "q", 1, "b", time.Minute); !got.Took || !got.Due {
		t.Errorf("taking over work whose lease lapsed: %+v; want it taken, due", got)
	}
}

// A holder's Let leaves the work's mark — and brings a job for one due now — and lets the lease go
// at once.
func testLetBringsAJob(t *testing.T, s work.Store, _ Subject) {
	ctx := context.Background()
	if got := take(t, s, "q", 1, "a", time.Minute); !got.Took {
		t.Fatal("taking it")
	}
	if err := s.Mark(ctx, "q", 1); err != nil {
		t.Fatal(err)
	}
	Ready(t, s, "q") // the mark's own
	if err := s.Let(ctx, "q", 1, "a"); err != nil {
		t.Fatal(err)
	}
	if got := Ready(t, s, "q"); !slices.Equal(got, []int64{1}) {
		t.Errorf("after letting go of work marked meanwhile, jobs %v; want 1's", got)
	}
	if got := take(t, s, "q", 1, "b", time.Minute); !got.Took || !got.Due {
		t.Errorf("taking work let go, marked: %+v", got)
	}
	if err := s.Let(ctx, "q", 1, "b"); err != nil {
		t.Fatal(err)
	}
	if got := Ready(t, s, "q"); len(got) != 0 {
		t.Errorf("letting go of unmarked work brought jobs: %v", got)
	}
}

// A take of work a transaction marks, open, waits for it to commit, and finds the work due: a
// worker brought the mark's job before its transaction committed acts after it.
func testTakeWaitsForAMark(t *testing.T, s work.Store, o Subject) {
	ctx := context.Background()
	for _, fresh := range []bool{true, false} {
		key := int64(1)
		if !fresh {
			key = 2
			if got := take(t, s, "q", key, "a", time.Minute); !got.Took {
				t.Fatal("taking it")
			}
			if again, err := s.Release(ctx, "q", key, "a"); err != nil || again {
				t.Fatal(again, err)
			}
		}
		commit := make(chan struct{})
		written, committed := o.MarkOpen(ctx, "q", key, commit)
		<-written
		taken := make(chan work.Taken, 1)
		go func() {
			got, err := s.Take(ctx, "q", key, "b", time.Minute)
			if err != nil {
				t.Error(err)
			}
			taken <- got
		}()
		select {
		case got := <-taken:
			t.Fatalf("work a transaction marks, open, was taken (%+v); fresh %v", got, fresh)
		case <-time.After(2 * time.Second):
		}
		close(commit)
		if err := <-committed; err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-taken:
			if !got.Took || !got.Due {
				t.Errorf("the take once the mark committed: %+v; want taken, due (fresh %v)", got, fresh)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the take never ended")
		}
	}
}

// Marked lists, in key order after a key, what is marked — due now, or when its mark is — and what
// is held by a holder whose lease lapsed, due now; not what is clean, nor what a live holder holds
// unmarked. Peek tells a marked piece of work from a clean one.
func testMarked(t *testing.T, s work.Store, _ Subject) {
	ctx := context.Background()
	later := time.Now().Add(time.Hour)
	if err := s.Mark(ctx, "q", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkAt(ctx, "q", 2, later); err != nil {
		t.Fatal(err)
	}
	if got := take(t, s, "q", 3, "dies", 50*time.Millisecond); !got.Took {
		t.Fatal("taking 3")
	}
	if got := take(t, s, "q", 4, "lives", time.Minute); !got.Took {
		t.Fatal("taking 4")
	}
	if got := take(t, s, "q", 5, "done", time.Minute); !got.Took {
		t.Fatal("taking 5")
	}
	if again, err := s.Release(ctx, "q", 5, "done"); err != nil || again {
		t.Fatal(again, err)
	}
	if err := s.Mark(ctx, "other", 6); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	got, err := s.Marked(ctx, "q", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]int64, len(got))
	for i, m := range got {
		keys[i] = m.Key
		if m.Kind != "q" {
			t.Errorf("a mark of kind %q", m.Kind)
		}
	}
	if !slices.Equal(keys, []int64{1, 2, 3}) {
		t.Fatalf("marked %v, want 1, 2, and 3", keys)
	}
	if !got[0].At.IsZero() || got[1].At.Sub(later).Abs() > time.Second || !got[2].At.IsZero() {
		t.Errorf("their times: %v, %v, %v", got[0].At, got[1].At, got[2].At)
	}
	if after, err := s.Marked(ctx, "q", 1, 1); err != nil || len(after) != 1 || after[0].Key != 2 {
		t.Errorf("one marked after 1: %+v, %v", after, err)
	}
	if p, err := s.Peek(ctx, "q", 1); err != nil || !p.Marked {
		t.Errorf("peeking at marked work: %+v, %v", p, err)
	}
	if p, err := s.Peek(ctx, "q", 5); err != nil || p.Marked || !p.Exists {
		t.Errorf("peeking at work done: %+v, %v", p, err)
	}
}
