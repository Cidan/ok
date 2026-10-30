// Package bustest is the conformance suite every bus.Queue passes: run RunQueue against an
// implementation.
package bustest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus"
)

// names makes topic, group, and queue names no two tests share, so suites may share a backend.
var names atomic.Int64

func name(kind string) string {
	return fmt.Sprintf("bustest.%s.%d.%d", kind, time.Now().UnixNano()%1e6, names.Add(1))
}

// patience is how long a test waits for what it expects before failing.
const patience = 60 * time.Second

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(patience)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("waited %v for %s", patience, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// QueueOptions says how a queue behaves beyond what every queue must.
type QueueOptions struct {
	// Dedupe: a job enqueued while one with its ID waits is dropped.
	Dedupe bool
	// DetectsLostLease: Ack, Extend, and Nack of an ended lease return bus.ErrLeaseLost (SQS
	// cannot tell).
	DetectsLostLease bool
	// MinVisibility is the shortest visibility the queue keeps (Pub/Sub's is ten seconds); the
	// suite's waits stretch to it.
	MinVisibility time.Duration
	// Shared: queues open makes carry one another's jobs, as processes of one installation do.
	Shared bool
}

// next takes an inbox's next job, waiting up to wait.
func next(t *testing.T, in bus.Inbox, wait time.Duration) (bus.Job, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	return in.Next(ctx)
}

func openInbox(t *testing.T, q bus.Queue) bus.Inbox {
	t.Helper()
	in, err := q.Inbox(context.Background())
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	t.Cleanup(func() { in.Close() })
	return in
}

// runInboxes is RunQueue's suite of inboxes.
func runInboxes(t *testing.T, open func(t *testing.T, maxAttempts int) bus.Queue, o QueueOptions) {
	// A job enqueued on an inbox's address, by another process where queues are shared, reaches that
	// inbox alone, promptly — a reply of one change to several inboxes each of them — and nothing
	// else does.
	t.Run("Inbox", func(t *testing.T) {
		ctx := context.Background()
		q := open(t, 5)
		t.Cleanup(func() { q.CloseInboxes(context.Background()) })
		sender := q
		if o.Shared {
			sender = open(t, 5)
		}
		a, b := openInbox(t, q), openInbox(t, q)
		if a.Address() == b.Address() {
			t.Fatalf("two inboxes at %s", a.Address())
		}
		start := time.Now()
		if err := bus.Reply(ctx, sender, []string{a.Address(), b.Address()}, "change-1", []byte("one")); err != nil {
			t.Fatal(err)
		}
		enqueue(t, sender, bus.Job{ID: "two", Queue: a.Address(), Body: []byte("two")})
		got := map[string]bool{}
		for range 2 {
			j, err := next(t, a, 10*time.Second)
			if err != nil {
				t.Fatalf("a's job: %v", err)
			}
			got[string(j.Body)] = true
		}
		if !got["one"] || !got["two"] {
			t.Fatalf("a got %v, want one and two", got)
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Errorf("a's jobs took %s to arrive", took)
		}
		if j, err := next(t, b, 10*time.Second); err != nil || string(j.Body) != "one" {
			t.Fatalf("b's job: %q, %v; want one", j.Body, err)
		}
		if j, err := next(t, b, time.Second); err == nil {
			t.Fatalf("b got %q, enqueued on a", j.Body)
		}
	})
	// An inbox closed takes nothing more, and a job enqueued on it is dropped; the process's other
	// inboxes, and those it opens after none was open, go on.
	t.Run("InboxClosed", func(t *testing.T) {
		q := open(t, 5)
		t.Cleanup(func() { q.CloseInboxes(context.Background()) })
		a := openInbox(t, q)
		b := openInbox(t, q)
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := next(t, b, time.Second); !errors.Is(err, bus.ErrClosed) {
			t.Fatalf("Next on a closed inbox: %v, want ErrClosed", err)
		}
		enqueue(t, q, bus.Job{Queue: b.Address(), Body: []byte("for b")}, bus.Job{Queue: a.Address(), Body: []byte("for a")})
		if j, err := next(t, a, 10*time.Second); err != nil || string(j.Body) != "for a" {
			t.Fatalf("a's job: %q, %v", j.Body, err)
		}
		a.Close()
		c := openInbox(t, q)
		enqueue(t, q, bus.Job{Queue: c.Address(), Body: []byte("for c")})
		if j, err := next(t, c, 10*time.Second); err != nil || string(j.Body) != "for c" {
			t.Fatalf("an inbox opened after none was: %q, %v", j.Body, err)
		}
	})
	// Closing a process's inboxes ends every one, and no more open.
	t.Run("CloseInboxes", func(t *testing.T) {
		q := open(t, 5)
		a := openInbox(t, q)
		if err := q.CloseInboxes(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := next(t, a, time.Second); !errors.Is(err, bus.ErrClosed) {
			t.Fatalf("Next after CloseInboxes: %v, want ErrClosed", err)
		}
		if _, err := q.Inbox(context.Background()); !errors.Is(err, bus.ErrClosed) {
			t.Fatalf("Inbox after CloseInboxes: %v, want ErrClosed", err)
		}
	})
}

func enqueue(t *testing.T, q bus.Queue, jobs ...bus.Job) {
	t.Helper()
	if err := q.Enqueue(context.Background(), jobs...); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

func lease(t *testing.T, q bus.Queue, queue string, o bus.LeaseOptions) []bus.Leased {
	t.Helper()
	ls, err := q.Lease(context.Background(), queue, o)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	return ls
}

// leaseSoon leases from queue, waiting up to a few seconds for a job to be ready: a backend may
// take a moment to make a new job visible.
func leaseSoon(t *testing.T, q bus.Queue, queue string, max int, vis time.Duration) []bus.Leased {
	t.Helper()
	return lease(t, q, queue, bus.LeaseOptions{Max: max, Visibility: vis, Wait: 5 * time.Second})
}

// RunQueue runs the queue suite on queues open makes, each giving up on a job after
// maxAttempts leases.
func RunQueue(t *testing.T, open func(t *testing.T, maxAttempts int) bus.Queue, o QueueOptions) {
	vis := 2 * time.Second
	if o.MinVisibility > vis {
		vis = o.MinVisibility
	}
	long := vis * 30
	runInboxes(t, open, o)
	// A lease of many that waits returns as the first job is ready, not once it has many.
	t.Run("PromptLease", func(t *testing.T) {
		q := open(t, 5)
		queue := name("prompt")
		lease(t, q, queue, bus.LeaseOptions{Max: 1, Visibility: vis}) // the queue made, where leasing makes it
		go func() {
			time.Sleep(300 * time.Millisecond)
			enqueue(t, q, bus.Job{ID: "one", Queue: queue, Body: []byte("one")})
		}()
		start := time.Now()
		got := lease(t, q, queue, bus.LeaseOptions{Max: 8, Visibility: vis, Wait: 15 * time.Second})
		if len(got) != 1 {
			t.Fatalf("leased %d jobs, want the one", len(got))
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Errorf("a lease of up to 8 took %s to return the one job ready", took)
		}
		got[0].Ack(context.Background())
	})
	t.Run("EnqueueLeaseAck", func(t *testing.T) {
		q := open(t, 10)
		queue := name("queue")
		enqueue(t, q,
			bus.Job{ID: "a", Queue: queue, Key: 1, Body: []byte("alpha")},
			bus.Job{ID: "b", Queue: queue, Key: 2, Body: []byte("beta")},
			bus.Job{ID: "c", Queue: queue, Key: 3, Body: nil})
		got := map[string]bus.Leased{}
		deadline := time.Now().Add(patience)
		for len(got) < 3 && time.Now().Before(deadline) {
			for _, l := range leaseSoon(t, q, queue, 10, long) {
				got[l.Job().ID] = l
			}
		}
		if len(got) != 3 {
			t.Fatalf("leased %d of 3", len(got))
		}
		for id, want := range map[string]struct {
			key  int64
			body string
		}{"a": {1, "alpha"}, "b": {2, "beta"}, "c": {3, ""}} {
			j := got[id].Job()
			if j.Key != want.key || string(j.Body) != want.body || j.Queue != queue || got[id].Attempt() != 1 {
				t.Fatalf("job %s = %+v, attempt %d", id, j, got[id].Attempt())
			}
		}
		for _, l := range got {
			if err := l.Ack(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if ls := lease(t, q, queue, bus.LeaseOptions{Max: 10, Visibility: vis, Wait: vis}); len(ls) != 0 {
			t.Fatalf("after acks, %d jobs leased", len(ls))
		}
	})
	t.Run("Visibility", func(t *testing.T) {
		q := open(t, 10)
		queue := name("queue")
		enqueue(t, q, bus.Job{ID: "v", Queue: queue})
		first := leaseSoon(t, q, queue, 1, vis)
		if len(first) != 1 {
			t.Fatal("the job was not leased")
		}
		if ls := lease(t, q, queue, bus.LeaseOptions{Max: 1, Visibility: vis}); len(ls) != 0 {
			t.Fatal("a leased job was leased again while invisible")
		}
		var again []bus.Leased
		eventually(t, "the job, once its visibility passed", func() bool {
			again = lease(t, q, queue, bus.LeaseOptions{Max: 1, Visibility: long, Wait: time.Second})
			return len(again) == 1
		})
		if again[0].Attempt() != 2 {
			t.Fatalf("the second lease is attempt %d", again[0].Attempt())
		}
		if o.DetectsLostLease {
			if err := first[0].Ack(context.Background()); !errors.Is(err, bus.ErrLeaseLost) {
				t.Fatalf("Ack of an ended lease = %v, want ErrLeaseLost", err)
			}
		}
		if err := again[0].Ack(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Extend", func(t *testing.T) {
		q := open(t, 10)
		queue := name("queue")
		enqueue(t, q, bus.Job{ID: "x", Queue: queue})
		ls := leaseSoon(t, q, queue, 1, vis)
		if len(ls) != 1 {
			t.Fatal("the job was not leased")
		}
		if err := ls[0].Extend(context.Background(), long); err != nil {
			t.Fatal(err)
		}
		time.Sleep(vis + vis/2)
		if again := lease(t, q, queue, bus.LeaseOptions{Max: 1, Visibility: vis}); len(again) != 0 {
			t.Fatal("an extended lease's job was leased again")
		}
		if err := ls[0].Ack(context.Background()); err != nil {
			t.Fatalf("Ack after Extend: %v", err)
		}
	})
	t.Run("Resume", func(t *testing.T) {
		q := open(t, 10)
		queue := name("queue")
		ctx := context.Background()
		enqueue(t, q, bus.Job{ID: "held", Queue: queue, Body: []byte("held")})
		ls := leaseSoon(t, q, queue, 1, vis)
		if len(ls) != 1 {
			t.Fatal("the job was not leased")
		}
		r, err := q.Resume(ls[0].Handle())
		if err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if r.Job().Queue != queue || r.Job().ID != "held" {
			t.Fatalf("the resumed lease's job: %+v", r.Job())
		}
		// Kept from its handle past the visibility it was leased with, it is no one else's.
		for end := time.Now().Add(3 * vis); time.Now().Before(end); time.Sleep(vis / 3) {
			if err := r.Extend(ctx, vis); err != nil {
				t.Fatalf("Extend from a handle: %v", err)
			}
			if again := lease(t, q, queue, bus.LeaseOptions{Max: 1, Visibility: vis}); len(again) != 0 {
				t.Fatal("a job a resumed lease kept was leased again")
			}
		}
		if err := r.Ack(ctx); err != nil {
			t.Fatalf("Ack from a handle: %v", err)
		}
		if again := lease(t, q, queue, bus.LeaseOptions{Max: 1, Visibility: long, Wait: 2 * vis}); len(again) != 0 {
			t.Fatal("a job acknowledged from its handle was leased again")
		}
		if _, err := q.Resume([]byte("not a handle")); err == nil {
			t.Error("Resume of bytes no lease made succeeded")
		}
	})
	t.Run("ResumeNack", func(t *testing.T) {
		q := open(t, 10)
		queue := name("queue")
		enqueue(t, q, bus.Job{ID: "back", Queue: queue, Body: []byte("back")})
		ls := leaseSoon(t, q, queue, 1, long)
		if len(ls) != 1 {
			t.Fatal("the job was not leased")
		}
		r, err := q.Resume(ls[0].Handle())
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Nack(context.Background(), 0); err != nil {
			t.Fatalf("Nack from a handle: %v", err)
		}
		var again []bus.Leased
		eventually(t, "the job returned from its handle", func() bool {
			again = lease(t, q, queue, bus.LeaseOptions{Max: 1, Visibility: long, Wait: time.Second})
			return len(again) == 1
		})
		if again[0].Attempt() != 2 || string(again[0].Job().Body) != "back" {
			t.Fatalf("the redelivery: attempt %d, body %q", again[0].Attempt(), again[0].Job().Body)
		}
	})
	t.Run("Nack", func(t *testing.T) {
		q := open(t, 10)
		queue := name("queue")
		enqueue(t, q, bus.Job{ID: "n", Queue: queue, Body: []byte("again")})
		ls := leaseSoon(t, q, queue, 1, long)
		if len(ls) != 1 {
			t.Fatal("the job was not leased")
		}
		if err := ls[0].Nack(context.Background(), vis); err != nil {
			t.Fatal(err)
		}
		if again := lease(t, q, queue, bus.LeaseOptions{Max: 1, Visibility: long}); len(again) != 0 {
			t.Fatal("a job nacked with a delay was leased at once")
		}
		var again []bus.Leased
		eventually(t, "the nacked job, after its delay", func() bool {
			again = lease(t, q, queue, bus.LeaseOptions{Max: 1, Visibility: long, Wait: time.Second})
			return len(again) == 1
		})
		if again[0].Attempt() != 2 || string(again[0].Job().Body) != "again" {
			t.Fatalf("the redelivery: attempt %d, body %q", again[0].Attempt(), again[0].Job().Body)
		}
	})
	t.Run("NotBefore", func(t *testing.T) {
		q := open(t, 10)
		queue := name("queue")
		start := time.Now()
		enqueue(t, q, bus.Job{ID: "later", Queue: queue, NotBefore: start.Add(vis)})
		var ls []bus.Leased
		eventually(t, "the delayed job", func() bool {
			ls = lease(t, q, queue, bus.LeaseOptions{Max: 1, Visibility: long, Wait: time.Second})
			return len(ls) == 1
		})
		if time.Since(start) < vis-200*time.Millisecond {
			t.Fatalf("the job was leased %v after it was enqueued, before its NotBefore %v", time.Since(start), vis)
		}
	})
	t.Run("Dedupe", func(t *testing.T) {
		if !o.Dedupe {
			t.Skip("the queue does not drop duplicate IDs")
		}
		q := open(t, 10)
		queue := name("queue")
		enqueue(t, q, bus.Job{ID: "same", Queue: queue, Body: []byte("1")})
		enqueue(t, q, bus.Job{ID: "same", Queue: queue, Body: []byte("2")})
		ls := leaseSoon(t, q, queue, 10, long)
		more := lease(t, q, queue, bus.LeaseOptions{Max: 10, Visibility: long, Wait: vis})
		if len(ls)+len(more) != 1 {
			t.Fatalf("a duplicate ID was enqueued twice: %d leased", len(ls)+len(more))
		}
	})
	t.Run("DeadLetter", func(t *testing.T) {
		q := open(t, 2)
		queue := name("queue")
		enqueue(t, q, bus.Job{ID: "doomed", Queue: queue})
		for attempt := 1; attempt <= 2; attempt++ {
			var ls []bus.Leased
			eventually(t, fmt.Sprintf("attempt %d", attempt), func() bool {
				ls = lease(t, q, queue, bus.LeaseOptions{Max: 1, Visibility: long, Wait: time.Second})
				return len(ls) == 1
			})
			if err := ls[0].Nack(context.Background(), 0); err != nil {
				t.Fatal(err)
			}
		}
		if ls := lease(t, q, queue, bus.LeaseOptions{Max: 1, Visibility: long, Wait: vis}); len(ls) != 0 {
			t.Fatalf("a job past its attempts was leased again (attempt %d)", ls[0].Attempt())
		}
	})
	t.Run("Workers", func(t *testing.T) {
		q := open(t, 10)
		queue := name("queue")
		const n = 60
		var jobs []bus.Job
		for i := 0; i < n; i++ {
			jobs = append(jobs, bus.Job{ID: fmt.Sprintf("w%d", i), Queue: queue, Key: int64(i)})
		}
		enqueue(t, q, jobs...)
		var (
			mu   sync.Mutex
			seen = map[string]int{}
			wg   sync.WaitGroup
		)
		ctx, cancel := context.WithTimeout(context.Background(), patience)
		defer cancel()
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ctx.Err() == nil {
					mu.Lock()
					done := len(seen) == n
					mu.Unlock()
					if done {
						return
					}
					ls, err := q.Lease(ctx, queue, bus.LeaseOptions{Max: 5, Visibility: long, Wait: time.Second})
					if err != nil {
						if ctx.Err() == nil {
							t.Error(err)
						}
						return
					}
					for _, l := range ls {
						mu.Lock()
						seen[l.Job().ID]++
						mu.Unlock()
						if err := l.Ack(ctx); err != nil {
							t.Error(err)
						}
					}
				}
			}()
		}
		wg.Wait()
		if len(seen) != n {
			t.Fatalf("the workers leased %d of %d jobs", len(seen), n)
		}
		for id, c := range seen {
			if c != 1 {
				t.Fatalf("job %s was leased %d times within its visibility", id, c)
			}
		}
	})
	t.Run("Queues", func(t *testing.T) {
		q := open(t, 10)
		q1, q2 := name("queue"), name("queue")
		enqueue(t, q, bus.Job{ID: "j", Queue: q1})
		if ls := lease(t, q, q2, bus.LeaseOptions{Max: 1, Visibility: long, Wait: vis}); len(ls) != 0 {
			t.Fatal("a job was leased from another queue")
		}
		if ls := leaseSoon(t, q, q1, 1, long); len(ls) != 1 || ls[0].Job().Queue != q1 {
			t.Fatal("the job was not leased from its queue")
		}
	})
	t.Run("Wait", func(t *testing.T) {
		q := open(t, 10)
		queue := name("queue")
		got := make(chan []bus.Leased, 1)
		start := time.Now()
		go func() {
			ls, err := q.Lease(context.Background(), queue, bus.LeaseOptions{Max: 1, Visibility: long, Wait: 15 * time.Second})
			if err != nil {
				t.Error(err)
			}
			got <- ls
		}()
		time.Sleep(500 * time.Millisecond)
		enqueue(t, q, bus.Job{ID: "late", Queue: queue})
		ls := <-got
		if len(ls) != 1 {
			t.Fatalf("a waiting Lease returned %d jobs", len(ls))
		}
		if waited := time.Since(start); waited > 10*time.Second {
			t.Fatalf("a waiting Lease took %v to see a job enqueued after 0.5s", waited)
		}
	})
	t.Run("Max", func(t *testing.T) {
		q := open(t, 10)
		queue := name("queue")
		for i := 0; i < 5; i++ {
			enqueue(t, q, bus.Job{ID: fmt.Sprintf("m%d", i), Queue: queue})
		}
		time.Sleep(200 * time.Millisecond)
		if ls := leaseSoon(t, q, queue, 2, long); len(ls) > 2 || len(ls) == 0 {
			t.Fatalf("a Lease of at most 2 returned %d", len(ls))
		}
	})
}
