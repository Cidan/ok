package work

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/bus"
)

// Every mark is a job (docs/hosted.md, "Work without leaders"): the transaction that marks a piece
// of work enqueues a job naming it on the bus — on its kind's queue — before it commits, so a mark
// is never without its job, and a worker's lease takes the job and the work's row lease after it.
// A job of a transaction that does not commit names work no mark made, and costs a look.
//
// A worker that takes a job before the transaction that enqueued it has committed waits for it in
// the database: the mark holds the cells a take and a release write (a row lock on PostgreSQL, the
// mark's read on Spanner), so whatever a worker does with the row after leasing the job is ordered
// after the mark's commit, or its abort.

// Jobs is where a role's keyed work goes as jobs: its queue, and the role's name, which its kinds'
// queues are named under (work.<prefix>.<kind>).
type Jobs struct {
	Queue  bus.Queue
	Prefix string
}

// QueueOf is a kind's queue.
func (j Jobs) QueueOf(kind string) string { return "work." + j.Prefix + "." + kind }

// job is a job naming key's work of kind, due at at (zero: now), under a fresh token.
func (j Jobs) job(kind string, key int64, at time.Time) bus.Job {
	return j.jobWith(kind, key, at, newToken())
}

// jobWith is a job naming key's work of kind under token: a holder's lease names the token of the
// job it holds the work under, so a copy of that job is known as its own.
func (j Jobs) jobWith(kind string, key int64, at time.Time, token string) bus.Job {
	var id [12]byte
	rand.Read(id[:])
	return bus.Job{ID: hex.EncodeToString(id[:]), Queue: j.QueueOf(kind), Key: key, NotBefore: at,
		Body: append(binary.BigEndian.AppendUint64(nil, uint64(key)), token...)}
}

// Enqueue enqueues the jobs of marks made outside any transaction of a registered database's —
// a mark's row written in a transaction of its own, which enqueues them itself.
func (j Jobs) Enqueue(ctx context.Context, marks ...Mark) error {
	if len(marks) == 0 {
		return nil
	}
	jobs := make([]bus.Job, len(marks))
	for i, m := range marks {
		jobs[i] = j.job(m.Kind, m.Key, m.At)
	}
	if err := j.Queue.Enqueue(ctx, jobs...); err != nil {
		return fmt.Errorf("work: enqueueing marks' jobs: %w", err)
	}
	return nil
}

// Mark is a piece of work marked: its kind and key, due at At (zero: now).
type Mark struct {
	Kind string
	Key  int64
	At   time.Time
}

func newToken() string {
	var b [8]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// parseJob is the key and token a job names.
func parseJob(j bus.Job) (key int64, token string, ok bool) {
	if len(j.Body) < 8 {
		return 0, "", false
	}
	return int64(binary.BigEndian.Uint64(j.Body)), string(j.Body[8:]), true
}

// databases are the registered databases' registrations, by their handles.
var databases sync.Map // any → *registrations

// registrations are a database's jobs as each opener registered them: its jobs are those of the
// latest standing — a role's replicas that share a database handle, as a test's do, each register
// their own bus, and one that stops withdraws its own — or, none standing, the last withdrawn.
type registrations struct {
	mu   sync.Mutex
	list []*Jobs
	last Jobs
}

func (r *registrations) current() Jobs {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n := len(r.list); n > 0 {
		return *r.list[n-1]
	}
	return r.last
}

// local guards the making of an unregistered database's jobs.
var local sync.Mutex

// Register makes db's marks enqueue their jobs on j until withdraw is called: a role registers its
// database's handle (a *pgxpool.Pool, a *spanner.Client) as it opens it, before anything marks
// work there, and withdraws it before it closes j's bus.
func Register(db any, j Jobs) (withdraw func()) {
	v, _ := databases.LoadOrStore(db, &registrations{})
	r := v.(*registrations)
	reg := &j
	r.mu.Lock()
	r.list = append(r.list, reg)
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if i := slices.Index(r.list, reg); i >= 0 {
			r.list = slices.Delete(r.list, i, i+1)
			r.last = j
		}
	}
}

// RegisterStore registers the database a store keeps its work in (Register).
func RegisterStore(s Store, j Jobs) (withdraw func(), err error) {
	d, ok := s.(interface{ Database() any })
	if !ok {
		return nil, fmt.Errorf("work: a store of %T names no database to register", s)
	}
	return Register(d.Database(), j), nil
}

// JobsOf is the jobs of db's marks: those it is registered with now, or, for one never registered —
// a test's, a role run within one process — a queue of the process's own, the database's alone.
func JobsOf(db any) Jobs {
	if v, ok := databases.Load(db); ok {
		return v.(*registrations).current()
	}
	local.Lock()
	defer local.Unlock()
	if v, ok := databases.Load(db); ok {
		return v.(*registrations).current()
	}
	j := Jobs{Queue: bus.NewLocalQueue(), Prefix: "local"}
	databases.Store(db, &registrations{list: []*Jobs{&j}})
	return j
}

// attempts are the marks of the transactions' attempts under way, by their transactions.
var attempts sync.Map // any → *attempt

type attempt struct {
	jobs  Jobs
	mu    sync.Mutex
	marks []Mark
	steps []bus.Job
}

// Begin begins recording the marks of an attempt of a transaction, tx, on db: a database's Txn
// calls it as each attempt begins, Flush as it is about to commit, and End as it ends.
func Begin(db, tx any) { attempts.Store(tx, &attempt{jobs: JobsOf(db)}) }

// End ends tx's attempt.
func End(tx any) { attempts.Delete(tx) }

// errNoAttempt refuses a mark in a transaction no Txn began: its job would never be enqueued.
var errNoAttempt = errors.New("work: a mark in a transaction no Txn began, whose job no one would enqueue")

// Marked records, in tx's attempt, key's work of kind marked, due at at (zero: now): its job is
// enqueued as the attempt is about to commit.
func Marked(tx any, kind string, key int64, at time.Time) error {
	a, ok := attempts.Load(tx)
	if !ok {
		return errNoAttempt
	}
	at0 := a.(*attempt)
	at0.mu.Lock()
	at0.marks = append(at0.marks, Mark{Kind: kind, Key: key, At: at})
	at0.mu.Unlock()
	return nil
}

// stepJob is a job of kind's queue naming a step about key, carrying body after when the job was
// made, due at at (zero: now).
// StepJob is a step's job, as Consume takes it, due at at (zero: now): for a job another service
// enqueues on this one's queue, outside any transaction of this one's.
func (j Jobs) StepJob(kind string, key int64, at time.Time, body []byte) bus.Job {
	return j.stepJob(kind, key, at, body)
}

func (j Jobs) stepJob(kind string, key int64, at time.Time, body []byte) bus.Job {
	var id [12]byte
	rand.Read(id[:])
	// The body follows when the job was made, which its step judges a missing write by.
	b := append(binary.BigEndian.AppendUint64(nil, uint64(time.Now().UnixMilli())), body...)
	return bus.Job{ID: hex.EncodeToString(id[:]), Queue: j.QueueOf(kind), Key: key, NotBefore: at, Body: b}
}

// Step enqueues a step outside any transaction (Pend's are a transaction's): what a move's arrival,
// or an upgrade, finds left to do.
func (j Jobs) Step(ctx context.Context, kind string, key int64, at time.Time, body []byte) error {
	if err := j.Queue.Enqueue(ctx, j.stepJob(kind, key, at, body)); err != nil {
		return fmt.Errorf("work: enqueueing a step's job: %w", err)
	}
	return nil
}

// Pend records, in tx's attempt, a step to take after it commits — sequencing what it left pending,
// relaying what it logged — as a job of kind's queue on the attempt's database's bus, naming key and
// carrying body, due at at (zero: now): enqueued as the attempt is about to commit, so the step is
// never without its job. Its consumer (Consume) finds what the attempt wrote, or, not yet, looks
// again; a job of an attempt that did not commit finds nothing.
func Pend(tx any, kind string, key int64, at time.Time, body []byte) error {
	a, ok := attempts.Load(tx)
	if !ok {
		return errNoAttempt
	}
	at0 := a.(*attempt)
	at0.mu.Lock()
	defer at0.mu.Unlock()
	at0.steps = append(at0.steps, at0.jobs.stepJob(kind, key, at, body))
	return nil
}

// Flush enqueues the jobs of tx's attempt's marks, each once, and of its steps: the attempt's Txn
// calls it last, before it commits. A failure fails the attempt: a mark, or a step, is never
// committed without its job.
func Flush(ctx context.Context, tx any) error {
	a, ok := attempts.Load(tx)
	if !ok {
		return nil
	}
	at := a.(*attempt)
	at.mu.Lock()
	marks, steps := dedupe(at.marks), at.steps
	at.marks, at.steps = nil, nil
	at.mu.Unlock()
	if err := at.jobs.Enqueue(ctx, marks...); err != nil {
		return err
	}
	if len(steps) == 0 {
		return nil
	}
	if err := at.jobs.Queue.Enqueue(ctx, steps...); err != nil {
		return fmt.Errorf("work: enqueueing steps' jobs: %w", err)
	}
	return nil
}

// dedupe is marks with each piece of work's once, at the soonest it was marked due.
func dedupe(marks []Mark) []Mark {
	if len(marks) < 2 {
		return marks
	}
	type id struct {
		kind string
		key  int64
	}
	at := map[id]int{}
	var out []Mark
	for _, m := range marks {
		k := id{m.Kind, m.Key}
		i, seen := at[k]
		switch {
		case !seen:
			at[k] = len(out)
			out = append(out, m)
		case m.At.IsZero() || !out[i].At.IsZero() && m.At.Before(out[i].At):
			out[i].At = m.At
		}
	}
	return out
}
