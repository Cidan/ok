package bus

import (
	"context"
	"strconv"
	"sync"
	"time"
)

// LocalQueue is a Queue within one process: for tests, and for roles that share a process with all
// the replicas they have. A job's ID drops a job enqueued while another with it waits; a lease's
// end is told (ErrLeaseLost); a job past MaxAttempts leases is a dead letter, kept in Dead.
type LocalQueue struct {
	// MaxAttempts is how many leases a job gets before it is a dead letter. Default 10.
	MaxAttempts int

	mu     sync.Mutex
	queues map[string][]*localJob
	dead   []Job
	// woken is closed, and made anew, whenever a job is enqueued or becomes visible again.
	woken chan struct{}

	inboxes     *Inboxes
	inboxesOnce sync.Once
}

// localJob is a job on a LocalQueue: visible once its time passes, held by the lease whose
// generation it carries.
type localJob struct {
	job      Job
	visible  time.Time
	attempts int
	lease    int64 // the generation of the lease that holds it; 0 for none
	done     bool
}

var leaseGeneration struct {
	sync.Mutex
	n int64
}

func nextGeneration() int64 {
	leaseGeneration.Lock()
	defer leaseGeneration.Unlock()
	leaseGeneration.n++
	return leaseGeneration.n
}

// NewLocalQueue makes a queue within the process.
func NewLocalQueue() *LocalQueue {
	return &LocalQueue{queues: map[string][]*localJob{}, woken: make(chan struct{})}
}

func (q *LocalQueue) maxAttempts() int {
	if q.MaxAttempts <= 0 {
		return 10
	}
	return q.MaxAttempts
}

// wake tells every waiting Lease to look again. q.mu is held.
func (q *LocalQueue) wake() {
	close(q.woken)
	q.woken = make(chan struct{})
}

// Inbox opens an inbox, carried on a queue of the LocalQueue's own.
func (q *LocalQueue) Inbox(ctx context.Context) (Inbox, error) { return q.inboxesOf().Open(ctx) }

func (q *LocalQueue) inboxesOf() *Inboxes {
	q.inboxesOnce.Do(func() {
		q.inboxes = NewInboxes(q, func(context.Context, string) error { return nil }, func(_ context.Context, queue string) error {
			q.mu.Lock()
			delete(q.queues, queue)
			q.mu.Unlock()
			return nil
		})
	})
	return q.inboxes
}

// CloseInboxes closes its inboxes.
func (q *LocalQueue) CloseInboxes(ctx context.Context) error { return q.inboxesOf().Close(ctx) }

func (q *LocalQueue) Enqueue(_ context.Context, jobs ...Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	for _, j := range jobs {
		j, _ = InboxJob(j)
		waiting := false
		for _, have := range q.queues[j.Queue] {
			if j.ID != "" && have.job.ID == j.ID && !have.done && (have.lease == 0 || !have.visible.After(now)) {
				waiting = true
				break
			}
		}
		if waiting {
			continue
		}
		visible := now
		if j.NotBefore.After(now) {
			visible = j.NotBefore
		}
		q.queues[j.Queue] = append(q.queues[j.Queue], &localJob{job: j, visible: visible})
	}
	q.wake()
	return nil
}

func (q *LocalQueue) Lease(ctx context.Context, queue string, o LeaseOptions) ([]Leased, error) {
	deadline := time.Now().Add(o.Wait)
	for {
		q.mu.Lock()
		now := time.Now()
		var out []Leased
		next := time.Time{}
		kept := q.queues[queue][:0]
		for _, lj := range q.queues[queue] {
			if lj.done {
				continue
			}
			kept = append(kept, lj)
			if len(out) >= max(o.Max, 1) {
				continue
			}
			if lj.visible.After(now) {
				if next.IsZero() || lj.visible.Before(next) {
					next = lj.visible
				}
				continue
			}
			if lj.attempts >= q.maxAttempts() {
				lj.done = true
				q.dead = append(q.dead, lj.job)
				continue
			}
			lj.attempts++
			lj.lease = nextGeneration()
			lj.visible = now.Add(o.Visibility)
			out = append(out, &localLeased{q: q, lj: lj, gen: lj.lease, attempt: lj.attempts})
		}
		q.queues[queue] = kept
		woken := q.woken
		q.mu.Unlock()
		left := time.Until(deadline)
		if len(out) > 0 || left <= 0 {
			return out, ctx.Err()
		}
		wait := left
		if !next.IsZero() {
			wait = min(wait, time.Until(next))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-woken:
		case <-time.After(max(wait, time.Millisecond)):
		}
	}
}

// Dead are the jobs that ran out of attempts.
func (q *LocalQueue) Dead() []Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]Job(nil), q.dead...)
}

// localLeased is a lease of a LocalQueue's job.
type localLeased struct {
	q       *LocalQueue
	lj      *localJob
	gen     int64
	attempt int
}

func (l *localLeased) Job() Job     { return l.lj.job }
func (l *localLeased) Attempt() int { return l.attempt }

func (l *localLeased) Handle() []byte {
	return EncodeHandle(l.lj.job.Queue, strconv.FormatInt(l.gen, 10))
}

// Resume finds the job the handle's lease is of, in its queue; a lease that ended resumes as one
// whose every call reports ErrLeaseLost.
func (q *LocalQueue) Resume(handle []byte) (Leased, error) {
	parts, err := DecodeHandle(handle, 2)
	if err != nil {
		return nil, err
	}
	gen, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, errBadHandle
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, lj := range q.queues[parts[0]] {
		if lj.lease == gen {
			return &localLeased{q: q, lj: lj, gen: gen}, nil
		}
	}
	return &localLeased{q: q, lj: &localJob{job: Job{Queue: parts[0]}, done: true}, gen: gen}, nil
}

// held reports whether the lease still holds its job: its generation is the job's, and its
// visibility has not passed. q.mu is held.
func (l *localLeased) held() bool {
	return !l.lj.done && l.lj.lease == l.gen && time.Now().Before(l.lj.visible)
}

func (l *localLeased) Extend(_ context.Context, d time.Duration) error {
	l.q.mu.Lock()
	defer l.q.mu.Unlock()
	if !l.held() {
		return ErrLeaseLost
	}
	l.lj.visible = time.Now().Add(d)
	return nil
}

func (l *localLeased) Ack(_ context.Context) error {
	l.q.mu.Lock()
	defer l.q.mu.Unlock()
	if !l.held() {
		return ErrLeaseLost
	}
	l.lj.done = true
	return nil
}

func (l *localLeased) Nack(_ context.Context, delay time.Duration) error {
	l.q.mu.Lock()
	defer l.q.mu.Unlock()
	if !l.held() {
		return ErrLeaseLost
	}
	if l.lj.attempts >= l.q.maxAttempts() {
		l.lj.done = true
		l.q.dead = append(l.q.dead, l.lj.job)
		return nil
	}
	l.lj.lease = 0
	l.lj.visible = time.Now().Add(delay)
	l.q.wake()
	return nil
}
