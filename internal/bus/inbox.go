package bus

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"
)

// An Inbox is a queue of a live request's own, for its replies: anything, anywhere, enqueues a job
// on its address (Job.Queue), and the request takes them as they come (docs/state.md, "Waiting").
// It ends with the request: what is enqueued on it after it closes is dropped, as no one waits for
// it.
type Inbox interface {
	// Address is the queue to enqueue its jobs on.
	Address() string
	// Next waits for its next job until ctx ends.
	Next(ctx context.Context) (Job, error)
	// Close ends it.
	Close() error
}

// inboxPrefix begins every inbox's address, and its process's queue's name.
const inboxPrefix = "inbox."

// InboxJob is j as its backend carries it when j.Queue is an inbox's address: on the inbox's
// process's queue, keyed by its number there, its ID — which the backend drops a duplicate of —
// the inbox's own, and due now: a reply waits for nothing. ok is false for a job on no inbox.
func InboxJob(j Job) (Job, bool) {
	queue, n, ok := InboxQueue(j.Queue)
	if !ok {
		return j, false
	}
	j.Queue, j.Key, j.NotBefore = queue, n, time.Time{}
	if j.ID != "" {
		j.ID += "@" + strconv.FormatInt(n, 10)
	}
	return j, true
}

// InboxQueue is the queue and number an inbox's address names: its process's queue, and its
// number there; ok is false for an address that is no inbox's.
func InboxQueue(address string) (queue string, key int64, ok bool) {
	if !strings.HasPrefix(address, inboxPrefix) {
		return "", 0, false
	}
	i := strings.LastIndexByte(address, '.')
	if i < len(inboxPrefix) {
		return "", 0, false
	}
	n, err := strconv.ParseInt(address[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return address[:i], n, true
}

// IsInboxQueue reports whether queue is a process's queue of inboxes, which its backend keeps only
// while the process lives.
func IsInboxQueue(queue string) bool {
	return strings.HasPrefix(queue, inboxPrefix) && len(queue) > len(inboxPrefix) && !strings.Contains(queue[len(inboxPrefix):], ".")
}

// Inboxes are a process's inboxes, carried on one queue of the process's own: a backend's Inbox
// opens one. While any is open it leases the process's queue — waiting on it, as a lease does — and
// hands each job to the inbox its key names, dropping one for an inbox that closed.
type Inboxes struct {
	q       Queue
	prepare func(ctx context.Context, queue string) error
	drop    func(ctx context.Context, queue string) error
	queue   string

	mu      sync.Mutex
	boxes   map[int64]*inbox
	cancel  context.CancelFunc // ends the router, while one runs
	stopped chan struct{}
	closed  bool
}

// NewInboxes are the inboxes of a process on q. prepare makes the process's queue, if it is not
// there, before an inbox's address is handed out — so nothing enqueued on it is lost — each time
// the first inbox opens after none was: a backend may have removed a queue left unleased; drop
// removes it as the process closes them (Close).
func NewInboxes(q Queue, prepare, drop func(ctx context.Context, queue string) error) *Inboxes {
	var b [8]byte
	_, _ = crand.Read(b[:])
	return &Inboxes{q: q, prepare: prepare, drop: drop, queue: inboxPrefix + hex.EncodeToString(b[:]), boxes: map[int64]*inbox{}}
}

// Queue is the process's queue of inboxes.
func (in *Inboxes) Queue() string { return in.queue }

// ErrClosed reports an inbox opened, or waited on, after it or its process's inboxes closed.
var ErrClosed = errors.New("bus: the inbox is closed")

// Open opens an inbox.
func (in *Inboxes) Open(ctx context.Context) (Inbox, error) {
	in.mu.Lock()
	routing := in.cancel != nil
	in.mu.Unlock()
	if !routing {
		if err := in.prepare(ctx, in.queue); err != nil {
			return nil, err
		}
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.closed {
		return nil, ErrClosed
	}
	var n int64
	for n == 0 || in.boxes[n] != nil {
		n = rand.Int64N(1<<62) + 1
	}
	b := &inbox{in: in, n: n, ready: make(chan struct{}, 1)}
	in.boxes[n] = b
	if in.cancel == nil {
		rctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		in.cancel, in.stopped = cancel, make(chan struct{})
		go in.route(rctx, in.stopped)
	}
	return b, nil
}

// Close closes every inbox, and removes the process's queue.
func (in *Inboxes) Close(ctx context.Context) error {
	in.mu.Lock()
	in.closed = true
	for _, b := range in.boxes {
		b.end()
	}
	in.boxes = map[int64]*inbox{}
	stopped := in.stop()
	in.mu.Unlock()
	if stopped != nil {
		<-stopped
	}
	return in.drop(ctx, in.queue)
}

// stop ends the router, if one runs; in.mu is held. It returns what closes once it has ended.
func (in *Inboxes) stop() chan struct{} {
	if in.cancel == nil {
		return nil
	}
	in.cancel()
	stopped := in.stopped
	in.cancel, in.stopped = nil, nil
	return stopped
}

// routeWait is how long one lease of the process's queue waits for a job.
const routeWait = 20 * time.Second

// route leases the process's queue until ctx ends, handing each job to its inbox.
func (in *Inboxes) route(ctx context.Context, stopped chan struct{}) {
	defer close(stopped)
	for backoff := 100 * time.Millisecond; ctx.Err() == nil; {
		got, err := in.q.Lease(ctx, in.queue, LeaseOptions{Max: 64, Visibility: time.Minute, Wait: routeWait})
		for _, l := range got {
			in.deliver(l.Job())
			if err := l.Ack(context.WithoutCancel(ctx)); err != nil && ctx.Err() == nil {
				slog.Warn("bus: acknowledging an inbox's job", "queue", in.queue, "err", err)
			}
		}
		if err == nil || ctx.Err() != nil {
			backoff = 100 * time.Millisecond
			continue
		}
		slog.Warn("bus: leasing the process's inboxes", "queue", in.queue, "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 10*time.Second)
	}
}

func (in *Inboxes) deliver(j Job) {
	in.mu.Lock()
	b := in.boxes[j.Key]
	in.mu.Unlock()
	if b != nil {
		b.put(j)
	}
}

func (in *Inboxes) remove(n int64) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if _, ok := in.boxes[n]; !ok {
		return
	}
	delete(in.boxes, n)
	if len(in.boxes) == 0 {
		in.stop() // no one waits on the queue: a job enqueued meanwhile waits for the next inbox
	}
}

type inbox struct {
	in    *Inboxes
	n     int64
	ready chan struct{}

	mu     sync.Mutex
	jobs   []Job
	closed bool
}

func (b *inbox) Address() string { return b.in.queue + "." + strconv.FormatInt(b.n, 10) }

func (b *inbox) put(j Job) {
	b.mu.Lock()
	if !b.closed {
		b.jobs = append(b.jobs, j)
	}
	b.mu.Unlock()
	select {
	case b.ready <- struct{}{}:
	default:
	}
}

func (b *inbox) Next(ctx context.Context) (Job, error) {
	for {
		b.mu.Lock()
		switch {
		case len(b.jobs) > 0:
			j := b.jobs[0]
			b.jobs = b.jobs[1:]
			b.mu.Unlock()
			return j, nil
		case b.closed:
			b.mu.Unlock()
			return Job{}, ErrClosed
		}
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return Job{}, ctx.Err()
		case <-b.ready:
		}
	}
}

func (b *inbox) end() {
	b.mu.Lock()
	b.closed, b.jobs = true, nil
	b.mu.Unlock()
	select {
	case b.ready <- struct{}{}:
	default:
	}
}

func (b *inbox) Close() error {
	b.end()
	b.in.remove(b.n)
	return nil
}

// Reply enqueues a job of body on each inbox of addresses: a fan-out of one change, named by id, to
// the live requests waiting for it. A reply of an id to an inbox while one of it waits there is
// dropped where the backend can tell.
func Reply(ctx context.Context, q Queue, addresses []string, id string, body []byte) error {
	if len(addresses) == 0 {
		return nil
	}
	jobs := make([]Job, len(addresses))
	for i, a := range addresses {
		if _, _, ok := InboxQueue(a); !ok {
			return fmt.Errorf("bus: %q is no inbox's address", a)
		}
		jobs[i] = Job{ID: id, Queue: a, Body: body}
	}
	return q.Enqueue(ctx, jobs...)
}
