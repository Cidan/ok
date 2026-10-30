package queue

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

// Attempts a pass prepares at once that need one candidate — a chain's CL on one base — share one
// preparation; another base, CL, or patchset is its own.
func TestPassPreparesOnce(t *testing.T) {
	p := &pass{prepared: map[string]*preparing{}}
	var calls atomic.Int32
	release := make(chan struct{})
	do := func() ([]byte, error) {
		calls.Add(1)
		<-release
		return []byte{7}, nil
	}
	var wg sync.WaitGroup
	got := make([][]byte, 8)
	for i := range got {
		wg.Go(func() {
			c, err := p.prepare([]byte{1}, 12, 1, do)
			if err != nil {
				t.Error(err)
			}
			got[i] = c
		})
	}
	for calls.Load() == 0 {
	}
	close(release)
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("8 attempts needing one candidate prepared it %d times", n)
	}
	for i, c := range got {
		if len(c) != 1 || c[0] != 7 {
			t.Fatalf("attempt %d got %v", i, c)
		}
	}
	for _, k := range []struct {
		base     byte
		cl       int64
		patchset int32
	}{{2, 12, 1}, {1, 13, 1}, {1, 12, 2}} {
		if _, err := p.prepare([]byte{k.base}, k.cl, k.patchset, do); err != nil {
			t.Fatal(err)
		}
	}
	if n := calls.Load(); n != 4 {
		t.Fatalf("three other candidates, prepared %d times in all, want 4", n)
	}
	// Outside a pass, every call prepares.
	var none *pass
	if _, err := none.prepare([]byte{1}, 12, 1, do); err != nil || calls.Load() != 5 {
		t.Fatalf("outside a pass: %d, %v", calls.Load(), err)
	}
}

// A step acts on the first Parallel submissions that have something to do, in the decision's order,
// each its actions whole, and every note; the rest wait for the next step.
func TestStepRound(t *testing.T) {
	r := &Runner{cfg: Config{Parallel: 2}}
	acts := []Action{
		{Kind: Discard, Submission: 1, Attempt: 10}, {Kind: Create, Submission: 1}, {Kind: Note, Submission: 1, Reason: "planning post"},
		{Kind: Note, Submission: 2, Reason: "testing"},
		{Kind: Release, Submission: 3, Attempt: 30}, {Kind: Note, Submission: 3, Reason: "testing"},
		{Kind: Create, Submission: 4}, {Kind: Note, Submission: 4, Reason: "planning post"},
		{Kind: Fail, Submission: 5},
	}
	got, more := r.round(acts)
	want := []Action{
		{Kind: Discard, Submission: 1, Attempt: 10}, {Kind: Create, Submission: 1}, {Kind: Note, Submission: 1, Reason: "planning post"},
		{Kind: Note, Submission: 2, Reason: "testing"},
		{Kind: Release, Submission: 3, Attempt: 30}, {Kind: Note, Submission: 3, Reason: "testing"},
		{Kind: Note, Submission: 4, Reason: "planning post"},
	}
	if !more || !reflect.DeepEqual(got, want) {
		t.Fatalf("a round of 2: %v, more %v\nwant %v, more", actions(got), more, actions(want))
	}
	if got, more := (&Runner{cfg: Config{Parallel: 5}}).round(acts); more || len(got) != len(acts) {
		t.Fatalf("a round of 5, as many as there are: %d of %d actions, more %v", len(got), len(acts), more)
	}
}
