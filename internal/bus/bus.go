// Package bus is how ok's roles hear of what happened and hand out work (docs/hosted.md, "Events
// and the bus"), behind two interfaces with an implementation for each environment ok runs in:
//
//   - A Stream carries events: every event of a topic reaches one subscriber of each group, at
//     least once, in no promised order. Events are first rows of their repository's log in the
//     database that caused them; the stream only delivers them, and a consumer applies each once
//     by comparing its sequence with its cursor.
//   - A Queue carries work: each job is leased to one worker at a time, invisible to the others
//     until its lease ends, and redelivered until acknowledged — up to the queue's attempts, after
//     which it is a dead letter. A job names work; the worker takes the work's lease in the
//     database before doing it, so a lost or duplicated job costs a retry, never a double
//     execution.
//
// Every implementation passes bustest's suites.
package bus

import (
	"context"
	"errors"
	"time"
)

// Event is one event of a topic.
type Event struct {
	// Topic is the stream it is on: "scm.events", "directory.events".
	Topic string
	// Key is what it is about — a repository, an account — and Seq its place in that key's log.
	Key int64
	Seq int64
	// Body is the event, encoded by whoever published it.
	Body []byte
}

// Handler handles one delivered event. Returning nil acknowledges it; an error has it delivered
// again later.
type Handler func(ctx context.Context, e Event) error

// Stream carries events from publishers to every group of subscribers.
type Stream interface {
	// Publish sends events; it returns once the stream holds them all.
	Publish(ctx context.Context, events ...Event) error
	// CreateGroup makes group on topic, if it does not exist: a group receives the events
	// published after it was made.
	CreateGroup(ctx context.Context, topic, group string) error
	// Subscribe delivers topic's events to h as a member of group (made if it does not exist)
	// until ctx ends, sharing them with the group's other members. It returns ctx's error, or the
	// error that stopped it.
	Subscribe(ctx context.Context, topic, group string, h Handler) error
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
}

// Leased is a job a worker holds.
type Leased interface {
	Job() Job
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
