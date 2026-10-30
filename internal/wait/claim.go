package wait

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/work"
)

// ClaimSubject is a piece of keyed work's claim, as its holder lets it go: what a waiter to claim
// it registers on (Claim).
func ClaimSubject(kind string, key int64) string {
	return "claim/" + kind + "/" + strconv.FormatInt(key, 10)
}

// Claims are a service's claims that waiters wait on: its registrations, the queue their inboxes
// are on, and its keyed work.
type Claims struct {
	Waits Store
	Queue bus.Queue
	Work  work.Store
}

// Forever is a Claim's wait that ends only with its context.
const Forever time.Duration = math.MaxInt64

// claimMargin is how long past a holder's lease a waiter wakes, should the holder have died
// holding it: by then its lease has lapsed, and the work is the waiter's to take.
const claimMargin = 250 * time.Millisecond

// Claim takes holder's lease on key's work of kind, as work.Claim does, waiting up to wait while
// another holds it (Forever: until ctx ends) — ok false when the wait passed first. The waiter's inbox registers on the
// claim's subject before it tries again, and is told as the holder lets go: through the release
// Claim returns, or a runner's LetGo (LetGo). A holder that dies tells no one: the waiter wakes as
// its lease would lapse — renewed meanwhile, it waits again. No one asks again on a timer.
func (c Claims) Claim(ctx context.Context, kind string, key int64, holder string, ttl, wait time.Duration) (held context.Context, release func(), ok bool, err error) {
	take := func() (bool, error) {
		var rel func()
		held, rel, ok, err = work.Claim(ctx, c.Work, kind, key, holder, ttl)
		if ok {
			release = func() {
				rel()
				c.tell(ctx, kind, key)
			}
		}
		return ok, err
	}
	if ok, err := take(); err != nil || ok || wait <= 0 {
		return held, release, ok, err
	}
	var deadline time.Time
	if wait != Forever {
		deadline = time.Now().Add(wait)
	}
	w, err := Open(ctx, c.Queue, Reg{Store: c.Waits, Subjects: []string{ClaimSubject(kind, key)}})
	if err != nil {
		return nil, nil, false, err
	}
	defer w.Close()
	for {
		// After the registration: a let go before it is found here, one after is told.
		if ok, err := take(); err != nil || ok {
			return held, release, ok, err
		}
		p, err := c.Work.Peek(ctx, kind, key)
		if err != nil {
			return nil, nil, false, err
		}
		now := time.Now()
		if !deadline.IsZero() && !now.Before(deadline) {
			return nil, nil, false, nil
		}
		until := now.Add(p.Left + claimMargin)
		if !deadline.IsZero() && until.After(deadline) {
			until = deadline
		}
		next, cancel := context.WithDeadline(ctx, until)
		_, err = w.Next(next)
		cancel()
		switch {
		case ctx.Err() != nil:
			return nil, nil, false, ctx.Err()
		case err != nil && !errors.Is(err, context.DeadlineExceeded):
			return nil, nil, false, err
		}
	}
}

// LetGo is a runner's LetGo of kind's work: it tells those waiting to claim it (work.Runner).
func (c Claims) LetGo(kind string) func(ctx context.Context, key int64) {
	return func(ctx context.Context, key int64) { c.tell(ctx, kind, key) }
}

// tell replies to the inboxes waiting to claim key's work: the release has committed, so a waiter
// told tries again and finds it free.
func (c Claims) tell(ctx context.Context, kind string, key int64) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	subject := ClaimSubject(kind, key)
	inboxes, err := c.Waits.Waiting(ctx, []string{subject}, time.Now())
	if err == nil && len(inboxes) > 0 {
		err = bus.Reply(ctx, c.Queue, inboxes, "", []byte(subject))
	}
	if err != nil {
		// Its waiters wake as its lease would have lapsed.
		slog.Warn("wait: telling the waiters to claim work", "kind", kind, "key", key, "err", err)
	}
}
