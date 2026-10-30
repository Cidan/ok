// Package bus is how ok's roles hand out work (docs/hosted.md, "Events and the bus"; docs/state.md),
// behind one interface with an implementation for each environment ok runs in: a Queue carries
// work — each job leased to one worker at a time, invisible to the others until its lease ends,
// and redelivered until acknowledged, up to the queue's attempts, after which it is a dead letter.
// A job names work; the worker takes the work's lease in the database before doing it, so a lost
// or duplicated job costs a retry, never a double execution. Its inboxes carry the replies to the
// live requests waiting on a change (Inbox).
//
// Every implementation passes bustest's suite.
package bus

import (
	"context"
	"errors"
	"time"
)

// Event is one event of a log another service writes, as its followers read it (internal/bus/follow).
type Event struct {
	// Topic is the log it is of: "scm.events", "directory.repositories".
	Topic string
	// Key is what it is about — a repository, an account — and Seq its place in that key's log.
	Key int64
	Seq int64
	// Body is the event, encoded by the log's source.
	Body []byte
}

// Job is one piece of work.
type Job struct {
	// ID identifies the job: one enqueued while another with its ID is waiting is dropped where
	// the backend can tell.
	ID string
	// Queue is the queue it is on: "scm.compact", "automation.dispatch.linux-x86".
	Queue string
	// Key is what it is about — a repository, an organization — for metrics and fairness.
	Key int64
	// NotBefore delays its first delivery; zero is now.
	NotBefore time.Time
	Body      []byte
}

// LeaseOptions shapes a Lease.
type LeaseOptions struct {
	// Max is the most jobs to lease; at least one.
	Max int
	// Visibility is how long the leased jobs stay invisible to other workers.
	Visibility time.Duration
	// Wait is how long to wait for a job when none is ready; zero returns at once.
	Wait time.Duration
}

// Queue carries work to workers.
type Queue interface {
	// Enqueue adds jobs.
	Enqueue(ctx context.Context, jobs ...Job) error
	// Lease takes up to o.Max ready jobs of queue, each invisible to other workers until its
	// lease ends; none is not an error.
	Lease(ctx context.Context, queue string, o LeaseOptions) ([]Leased, error)
	// Resume is the lease a handle names (Leased.Handle), as any process may hold it: its Extend,
	// Ack, and Nack act on the lease as its holder's would, though the holder may be another
	// process, or gone. Its Job names only the job's queue and ID, and its Attempt is 0. A lease
	// that ended may not report ErrLeaseLost to one resumed: whatever the job names keeps its own
	// record of who holds it.
	Resume(handle []byte) (Leased, error)
	// Inbox opens an inbox for a live request: a queue its replies are enqueued on, by address,
	// until it closes (Inbox).
	Inbox(ctx context.Context) (Inbox, error)
	// CloseInboxes closes the process's inboxes, and removes the queue that carried them: as the
	// process stops.
	CloseInboxes(ctx context.Context) error
}

// Leased is a job a worker holds.
type Leased interface {
	Job() Job
	// Handle names the lease for Queue.Resume: a job held as long as the work it names runs, kept
	// by whichever process that work reports to.
	Handle() []byte
	// Attempt counts the leases the job has had, this one included.
	Attempt() int
	// Extend keeps the job invisible for d more from now.
	Extend(ctx context.Context, d time.Duration) error
	// Ack finishes the job: it is not delivered again.
	Ack(ctx context.Context) error
	// Nack returns the job, to be delivered again after delay — or, past the queue's attempts,
	// kept as a dead letter.
	Nack(ctx context.Context, delay time.Duration) error
}

// ErrLeaseLost reports an Extend, Ack, or Nack of a lease that has ended: the job's visibility
// passed and another worker may hold it.
var ErrLeaseLost = errors.New("bus: the lease has ended")
