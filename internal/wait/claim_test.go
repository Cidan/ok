package wait_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
)

// claims are a database's claims, its waits run as a service runs them.
func claims(t *testing.T) (wait.Claims, context.Context) {
	t.Helper()
	p := testdb.Pool(t)
	if _, err := p.Exec(context.Background(), pgdata.WorkSchema+pgdata.WaitsSchema); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ws := pgdata.Work(p)
	c := wait.Claims{Waits: pgdata.Waits(p), Queue: ws.Jobs().Queue, Work: ws}
	go wait.Run(ctx, c.Waits, c.Queue, nil)
	return c, ctx
}

type claimed struct {
	ok  bool
	err error
	at  time.Time
}

// claimLater claims key's work for holder in the background, waiting up to wait.
func claimLater(ctx context.Context, c wait.Claims, key int64, holder string, wait time.Duration) <-chan claimed {
	out := make(chan claimed, 1)
	go func() {
		_, release, ok, err := c.Claim(ctx, "k", key, holder, time.Minute, wait)
		out <- claimed{ok, err, time.Now()}
		if ok {
			release()
		}
	}()
	return out
}

// registered waits until a waiter is registered on key's claim.
func registered(t *testing.T, ctx context.Context, c wait.Claims, key int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		in, err := c.Waits.Waiting(ctx, []string{wait.ClaimSubject("k", key)}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if len(in) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no waiter registered on the claim")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A waiter to claim work another holds is told as the holder lets go — through its release, or a
// runner's — long before the holder's lease would lapse; one whose holder died takes it as the
// lease lapses; one whose wait passes first does without.
func TestClaimWaits(t *testing.T) {
	c, ctx := claims(t)

	// Released: told at once, not a minute on as the lease would lapse.
	_, release, ok, err := c.Claim(ctx, "k", 1, "a", time.Minute, 0)
	if err != nil || !ok {
		t.Fatalf("the first claim: %v, %v", ok, err)
	}
	if _, _, ok, err := c.Claim(ctx, "k", 1, "b", time.Minute, 0); err != nil || ok {
		t.Fatalf("a second claim, not waiting: %v, %v", ok, err)
	}
	got := claimLater(ctx, c, 1, "b", 30*time.Second)
	registered(t, ctx, c, 1)
	let := time.Now()
	release()
	select {
	case r := <-got:
		if r.err != nil || !r.ok || r.at.Sub(let) > 5*time.Second {
			t.Fatalf("the waiter, once the holder let go: %+v, %v after", r, r.at.Sub(let))
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the waiter was not told the holder let go")
	}

	// A holder that dies tells no one: its waiter takes the work as its lease lapses.
	dying, die := context.WithCancel(ctx)
	if _, _, ok, err := work.Claim(dying, c.Work, "k", 2, "dead", time.Second); err != nil || !ok {
		t.Fatalf("the dying holder's claim: %v, %v", ok, err)
	}
	died := time.Now()
	die()
	select {
	case r := <-claimLater(ctx, c, 2, "heir", 30*time.Second):
		if r.err != nil || !r.ok || r.at.Sub(died) > 10*time.Second {
			t.Fatalf("the waiter on a dead holder: %+v, %v after", r, r.at.Sub(died))
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the waiter on a dead holder never took the work")
	}

	// A wait that passes first.
	_, keep, ok, err := c.Claim(ctx, "k", 3, "a", time.Minute, 0)
	if err != nil || !ok {
		t.Fatalf("the holder's claim: %v, %v", ok, err)
	}
	defer keep()
	start := time.Now()
	r := <-claimLater(ctx, c, 3, "b", 300*time.Millisecond)
	if r.err != nil || r.ok || r.at.Sub(start) > 5*time.Second {
		t.Fatalf("a wait that passed: %+v, %v after", r, r.at.Sub(start))
	}

	// A runner's work: told as the runner lets go of it.
	var once sync.Once
	running, finish := make(chan struct{}), make(chan struct{})
	runner := &work.Runner{Kind: "k", Store: c.Work, TTL: time.Minute, LetGo: c.LetGo("k"), Do: func(context.Context, int64) error {
		once.Do(func() { close(running) })
		<-finish
		return nil
	}}
	go runner.Run(ctx)
	if err := c.Work.Mark(ctx, "k", 4); err != nil {
		t.Fatal(err)
	}
	select {
	case <-running:
	case <-time.After(20 * time.Second):
		t.Fatal("the runner never did the work")
	}
	got = claimLater(ctx, c, 4, "b", 30*time.Second)
	registered(t, ctx, c, 4)
	done := time.Now()
	close(finish)
	select {
	case r := <-got:
		if r.err != nil || !r.ok || r.at.Sub(done) > 5*time.Second {
			t.Fatalf("the waiter, once the runner let go: %+v, %v after", r, r.at.Sub(done))
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the waiter was not told the runner let go")
	}
}
