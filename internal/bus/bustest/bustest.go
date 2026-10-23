// Package bustest is the conformance suites every bus.Stream and bus.Queue passes: run
// RunStream and RunQueue against an implementation.
package bustest

import (
	"bytes"
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

// StreamOptions says how a stream behaves beyond what every stream must.
type StreamOptions struct {
	// Concurrent is how many deliveries, of different keys, the stream hands one member at once:
	// at least this many are in flight before any is answered. Zero checks nothing.
	Concurrent int
}

type delivered struct {
	topic    string
	key, seq int64
}

// collector records the events a subscription handled, and how many times each.
type collector struct {
	mu   sync.Mutex
	seen map[delivered]int
	body map[delivered][]byte
}

func newCollector() *collector {
	return &collector{seen: map[delivered]int{}, body: map[delivered][]byte{}}
}

func (c *collector) handle(_ context.Context, e bus.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := delivered{e.Topic, e.Key, e.Seq}
	c.seen[d]++
	c.body[d] = append([]byte(nil), e.Body...)
	return nil
}

func (c *collector) has(want []bus.Event) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range want {
		if c.seen[delivered{e.Topic, e.Key, e.Seq}] == 0 {
			return false
		}
	}
	return true
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}

// subscribe runs h on topic's group until the test ends.
func subscribe(t *testing.T, s bus.Stream, topic, group string, h bus.Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Subscribe(ctx, topic, group, h) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Subscribe %s/%s: %v", topic, group, err)
			}
		case <-time.After(30 * time.Second):
			t.Errorf("Subscribe %s/%s did not return after its context ended", topic, group)
		}
	})
}

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

func events(topic string, keys, perKey int) []bus.Event {
	var out []bus.Event
	for k := 1; k <= keys; k++ {
		for s := 1; s <= perKey; s++ {
			out = append(out, bus.Event{Topic: topic, Key: int64(k * 1_000_003), Seq: int64(s), Body: []byte(fmt.Sprintf("%d:%d", k, s))})
		}
	}
	return out
}

func publish(t *testing.T, s bus.Stream, es []bus.Event) {
	t.Helper()
	if err := s.Publish(context.Background(), es...); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func createGroup(t *testing.T, s bus.Stream, topic, group string) {
	t.Helper()
	if err := s.CreateGroup(context.Background(), topic, group); err != nil {
		t.Fatalf("CreateGroup %s/%s: %v", topic, group, err)
	}
}

// RunStream runs the stream suite on the stream open makes.
func RunStream(t *testing.T, open func(t *testing.T) bus.Stream, o StreamOptions) {
	t.Run("Delivery", func(t *testing.T) {
		s := open(t)
		topic, group := name("topic"), name("group")
		createGroup(t, s, topic, group)
		es := events(topic, 7, 8)
		publish(t, s, es)
		c := newCollector()
		subscribe(t, s, topic, group, c.handle)
		eventually(t, "every event", func() bool { return c.has(es) })
		for _, e := range es {
			if got := c.body[delivered{e.Topic, e.Key, e.Seq}]; !bytes.Equal(got, e.Body) {
				t.Fatalf("%d/%d's body = %q, want %q", e.Key, e.Seq, got, e.Body)
			}
		}
	})
	t.Run("FanOut", func(t *testing.T) {
		s := open(t)
		topic, g1, g2 := name("topic"), name("group"), name("group")
		createGroup(t, s, topic, g1)
		createGroup(t, s, topic, g2)
		c1, c2 := newCollector(), newCollector()
		subscribe(t, s, topic, g1, c1.handle)
		subscribe(t, s, topic, g2, c2.handle)
		es := events(topic, 4, 5)
		publish(t, s, es)
		eventually(t, "every event in each group", func() bool { return c1.has(es) && c2.has(es) })
	})
	t.Run("Shared", func(t *testing.T) {
		s := open(t)
		topic, group := name("topic"), name("group")
		createGroup(t, s, topic, group)
		c := newCollector()
		var a, b atomic.Int64
		subscribe(t, s, topic, group, func(ctx context.Context, e bus.Event) error { a.Add(1); return c.handle(ctx, e) })
		subscribe(t, s, topic, group, func(ctx context.Context, e bus.Event) error { b.Add(1); return c.handle(ctx, e) })
		es := events(topic, 20, 5)
		publish(t, s, es)
		eventually(t, "every event, shared by two members", func() bool { return c.has(es) })
		if a.Load()+b.Load() < int64(len(es)) {
			t.Fatalf("the members handled %d and %d of %d", a.Load(), b.Load(), len(es))
		}
	})
	t.Run("Redelivery", func(t *testing.T) {
		s := open(t)
		topic, group := name("topic"), name("group")
		createGroup(t, s, topic, group)
		var mu sync.Mutex
		failed := map[delivered]bool{}
		c := newCollector()
		subscribe(t, s, topic, group, func(ctx context.Context, e bus.Event) error {
			d := delivered{e.Topic, e.Key, e.Seq}
			mu.Lock()
			first := !failed[d]
			failed[d] = true
			mu.Unlock()
			if first {
				return errors.New("not yet")
			}
			return c.handle(ctx, e)
		})
		es := events(topic, 3, 3)
		publish(t, s, es)
		eventually(t, "every event, after each failed once", func() bool { return c.has(es) })
	})
	t.Run("AfterCreate", func(t *testing.T) {
		s := open(t)
		topic, group := name("topic"), name("group")
		publish(t, s, events(topic, 2, 2))
		createGroup(t, s, topic, group)
		later := []bus.Event{{Topic: topic, Key: 99, Seq: 1, Body: []byte("later")}}
		publish(t, s, later)
		c := newCollector()
		subscribe(t, s, topic, group, c.handle)
		eventually(t, "the event published after the group was made", func() bool { return c.has(later) })
	})
	t.Run("Resume", func(t *testing.T) {
		s := open(t)
		topic, group := name("topic"), name("group")
		createGroup(t, s, topic, group)
		first := events(topic, 3, 3)
		c := newCollector()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Subscribe(ctx, topic, group, c.handle) }()
		publish(t, s, first)
		eventually(t, "the first events", func() bool { return c.has(first) })
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		second := []bus.Event{{Topic: topic, Key: 7, Seq: 9, Body: []byte("after")}}
		publish(t, s, second)
		c2 := newCollector()
		subscribe(t, s, topic, group, c2.handle)
		eventually(t, "the event published while no member ran", func() bool { return c2.has(second) })
	})
	t.Run("Topics", func(t *testing.T) {
		s := open(t)
		t1, t2, group := name("topic"), name("topic"), name("group")
		createGroup(t, s, t1, group)
		createGroup(t, s, t2, group)
		c1, c2 := newCollector(), newCollector()
		subscribe(t, s, t1, group, c1.handle)
		subscribe(t, s, t2, group, c2.handle)
		e1, e2 := events(t1, 2, 2), events(t2, 3, 1)
		publish(t, s, e1)
		publish(t, s, e2)
		eventually(t, "each topic's events", func() bool { return c1.has(e1) && c2.has(e2) })
		time.Sleep(200 * time.Millisecond)
		if c1.count() != len(e1) || c2.count() != len(e2) {
			t.Fatalf("a topic's group saw another's events: %d and %d distinct", c1.count(), c2.count())
		}
	})
	t.Run("LargeBody", func(t *testing.T) {
		s := open(t)
		topic, group := name("topic"), name("group")
		createGroup(t, s, topic, group)
		body := bytes.Repeat([]byte("0123456789abcdef"), 4096)
		es := []bus.Event{{Topic: topic, Key: 1, Seq: 1, Body: body}}
		publish(t, s, es)
		c := newCollector()
		subscribe(t, s, topic, group, c.handle)
		eventually(t, "the large event", func() bool { return c.has(es) })
		if got := c.body[delivered{topic, 1, 1}]; !bytes.Equal(got, body) {
			t.Fatalf("the large body came back as %d bytes", len(got))
		}
	})
	t.Run("Concurrent", func(t *testing.T) {
		if o.Concurrent < 2 {
			t.Skip("the stream hands a member one delivery at a time")
		}
		s := open(t)
		topic, group := name("topic"), name("group")
		createGroup(t, s, topic, group)
		var inFlight, peak atomic.Int64
		release := make(chan struct{})
		c := newCollector()
		subscribe(t, s, topic, group, func(ctx context.Context, e bus.Event) error {
			n := inFlight.Add(1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
			select {
			case <-release:
			case <-time.After(patience):
			}
			inFlight.Add(-1)
			return c.handle(ctx, e)
		})
		es := events(topic, 64, 1)
		publish(t, s, es)
		eventually(t, fmt.Sprintf("%d deliveries in flight at once", o.Concurrent), func() bool { return peak.Load() >= int64(o.Concurrent) })
		close(release)
		eventually(t, "every event", func() bool { return c.has(es) })
	})
	t.Run("Prompt", func(t *testing.T) {
		s := open(t)
		topic, group := name("topic"), name("group")
		createGroup(t, s, topic, group)
		c := newCollector()
		subscribe(t, s, topic, group, c.handle)
		// The member waits a while for events, as one does between bursts.
		time.Sleep(1500 * time.Millisecond)
		es := []bus.Event{{Topic: topic, Key: 5, Seq: 1, Body: []byte("now")}}
		start := time.Now()
		publish(t, s, es)
		eventually(t, "the event", func() bool { return c.has(es) })
		if took := time.Since(start); took > 5*time.Second {
			t.Fatalf("a waiting member handled an event %v after it was published", took)
		}
	})
	t.Run("ConcurrentPublishers", func(t *testing.T) {
		s := open(t)
		topic, group := name("topic"), name("group")
		createGroup(t, s, topic, group)
		c := newCollector()
		subscribe(t, s, topic, group, c.handle)
		var all []bus.Event
		var wg sync.WaitGroup
		for p := 0; p < 4; p++ {
			es := events(topic, 5, 5)
			for i := range es {
				es[i].Key += int64(p)
			}
			all = append(all, es...)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, e := range es {
					if err := s.Publish(context.Background(), e); err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
		wg.Wait()
		eventually(t, "every concurrently published event", func() bool { return c.has(all) })
	})
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
