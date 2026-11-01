package wait_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/wait/waittest"
)

func run(t *testing.T) (*waittest.Store, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := waittest.New()
	go wait.Run(ctx, s, s.Jobs().Queue, nil)
	return s, ctx
}

// A waiter hears each change of a subject it waits on — those already come read together — and
// none of another's; closed, it is registered no more.
func TestWaiter(t *testing.T) {
	settle := wait.Settle
	wait.Settle = time.Hour
	t.Cleanup(func() { wait.Settle = settle })
	s, ctx := run(t)
	w, err := wait.Open(ctx, s.Jobs().Queue, wait.Reg{Store: s, Subjects: []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	s.Change(ctx, "c")
	s.Change(ctx, "a")
	next, cancel := context.WithTimeout(ctx, 5*time.Second)
	got, err := w.Next(next)
	cancel()
	if err != nil || !slices.Equal(got, []string{"a"}) {
		t.Fatalf("Next: %v, %v; want a", got, err)
	}
	s.Change(ctx, "a")
	s.Change(ctx, "b")
	time.Sleep(200 * time.Millisecond)
	next, cancel = context.WithTimeout(ctx, 5*time.Second)
	got, err = w.Next(next)
	cancel()
	slices.Sort(got)
	if err != nil || !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("Next after two changes: %v, %v; want both at once", got, err)
	}
	quiet, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	got, err = w.Next(quiet)
	cancel()
	if err == nil {
		t.Fatalf("Next with nothing changed: %v", got)
	}
	w.Close()
	if s.Registered("a") != 0 || s.Registered("b") != 0 {
		t.Error("registered after it closed")
	}
}

// A waiter renews its registration as it waits; one whose request died — never renewed, never
// closed — is removed once it lapses.
func TestLapse(t *testing.T) {
	old := wait.Lease
	wait.Lease = 300 * time.Millisecond
	defer func() { wait.Lease = old }()
	s, ctx := run(t)
	w, err := wait.Open(ctx, s.Jobs().Queue, wait.Reg{Store: s, Subjects: []string{"live"}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	waiting, cancel := context.WithTimeout(ctx, time.Second)
	w.Next(waiting)
	cancel()
	if s.Registered("live") != 1 {
		t.Error("a waiter's registration lapsed as it waited")
	}
	if err := s.Register(ctx, "a dead request's inbox", []string{"dead"}, time.Now().Add(100*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.Registered("dead") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("a lapsed registration was never removed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A change's step is enqueued just before its transaction commits, so its reply can reach a waiter
// before the commit does: what the waiter then reads is what was there before. Replied to once
// more, once the commit has had time, the waiter hears of it again, and only that once.
func TestAReplyThatBeatsItsCommitComesAgain(t *testing.T) {
	settle := wait.Settle
	wait.Settle = 200 * time.Millisecond
	t.Cleanup(func() { wait.Settle = settle })
	s, ctx := run(t)
	w, err := wait.Open(ctx, s.Jobs().Queue, wait.Reg{Store: s, Subjects: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	s.Change(ctx, "a")
	for _, which := range []string{"first", "second"} {
		next, cancel := context.WithTimeout(ctx, 5*time.Second)
		got, err := w.Next(next)
		cancel()
		if err != nil || !slices.Equal(got, []string{"a"}) {
			t.Fatalf("the %s reply: %v, %v; want a", which, got, err)
		}
	}
	quiet, cancel := context.WithTimeout(ctx, 2*wait.Settle)
	defer cancel()
	if got, err := w.Next(quiet); err == nil {
		t.Errorf("a third reply: %v", got)
	}
}
