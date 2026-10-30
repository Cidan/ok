package review

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/wait/waittest"
)

// cursorDB is a store whose feed cursors are set by the test, and that counts their reads.
type cursorDB struct {
	db.Store
	waits   *waittest.Store
	mu      sync.Mutex
	cursors map[int64]int64
	reads   int
}

func (c *cursorDB) FollowCursor(_ context.Context, _ string, key int64) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	return c.cursors[key], nil
}

func (c *cursorDB) Waits() wait.Store { return c.waits }

func (c *cursorDB) set(repo, seq int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursors[repo] = seq
}

// An action waits for the feed's cursor, reading it again as the feed says it applied a page of its
// repository's events — not otherwise — and returns once the cursor reaches its event; it ends its
// registration as it returns.
func TestWaitForFeed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &cursorDB{waits: waittest.New(), cursors: map[int64]int64{}}
	go wait.Run(ctx, d.waits, d.waits.Jobs().Queue, nil)
	s := &Service{db: d, feedWait: time.Minute}
	done := make(chan error, 1)
	go func() { done <- s.WaitForFeed(ctx, 7, 3) }()
	for d.waits.Registered(FeedSubject(7)) == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	d.mu.Lock()
	reads := d.reads
	d.mu.Unlock()
	if reads > 1 {
		t.Errorf("a waiting action read its cursor %d times with nothing applied", reads)
	}
	d.set(7, 2)
	d.waits.Change(ctx, FeedSubject(8)) // another repository's
	d.waits.Change(ctx, FeedSubject(7))
	select {
	case err := <-done:
		t.Fatalf("returned before the cursor reached its event: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	d.set(7, 3)
	d.waits.Change(ctx, FeedSubject(7))
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("never returned once the cursor reached its event")
	}
	if n := d.waits.Registered(FeedSubject(7)); n != 0 {
		t.Errorf("%d registrations left after the action returned", n)
	}
}
