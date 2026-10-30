// Package wait is how a live request waits for something to change (docs/state.md, "Waiting"). It
// opens an inbox (bus.Queue.Inbox) and registers it, in the database of each service that changes
// what it waits for, under the subjects it waits on; it reads what it waits for after it registers,
// so a change between the two is read, not missed. A transaction that changes a subject pends a
// step naming it (Changed), which any worker of the service takes (Run): it replies to every inbox
// registered under the subject. A registration lapses unless its request renews it, and a step due
// as it lapses removes it: a request whose process died leaves nothing behind.
package wait

import (
	"context"
	"encoding/hex"
	"errors"
	"hash/fnv"
	"log/slog"
	"strconv"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/work"
)

// Registrar registers a waiter with a service: its Store, or the service's Waits, called from
// another (Remote).
type Registrar interface {
	// Register registers inbox under each of subjects until lapses, and pends the step that
	// removes them once they have lapsed (LapseKind), in one transaction.
	Register(ctx context.Context, inbox string, subjects []string, lapses time.Time) error
	// Unregister removes inbox's registrations.
	Unregister(ctx context.Context, inbox string) error
}

// Store is a service's registrations, in its database.
type Store interface {
	Registrar
	// Waiting are the inboxes registered under any of subjects that have not lapsed at now, each
	// once.
	Waiting(ctx context.Context, subjects []string, now time.Time) ([]string, error)
	// Lapse removes inbox's registrations that have lapsed at now.
	Lapse(ctx context.Context, inbox string, now time.Time) error
	// Jobs are the service's (its keyed work's).
	Jobs() work.Jobs
}

// The steps of a service's waits: a subject changed, and an inbox's registrations lapsed.
const (
	ChangedKind = "wait.changed"
	LapseKind   = "wait.lapse"
)

// Changed is the step a transaction pends as it changes subject: its kind, key, and body
// (PendStep). A transaction that does not commit leaves no step.
func Changed(subject string) (kind string, key int64, body []byte) {
	return ChangedKind, Key(subject), []byte(subject)
}

// Key is a subject's key, or an inbox's, for its steps' jobs.
func Key(s string) int64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return int64(h.Sum64() >> 1)
}

// Run replies to the inboxes waiting on each subject a step names, and removes registrations that
// lapsed, as their steps come, until ctx ends: every replica of the service runs it.
func Run(ctx context.Context, s Store, q bus.Queue, log *slog.Logger) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		work.Consume(ctx, s, LapseKind, 2, time.Second, func(ctx context.Context, _ int64, body []byte, _ time.Duration) error {
			return s.Lapse(ctx, string(body), time.Now())
		}, log)
	}()
	work.Consume(ctx, s, ChangedKind, 8, time.Second, func(ctx context.Context, _ int64, body []byte, _ time.Duration) error {
		subject := string(body)
		inboxes, err := s.Waiting(ctx, []string{subject}, time.Now())
		if err != nil {
			return err
		}
		return bus.Reply(ctx, q, inboxes, "", body)
	}, log)
	<-done
}

// Lease is how long a registration lasts unrenewed; a waiter renews it every third of it.
var Lease = 2 * time.Minute

// Waiter is a live request's wait: its inbox, registered under its subjects with each store, until
// it closes.
type Waiter struct {
	in      bus.Inbox
	regs    []Reg
	renewed time.Time
}

// Reg is subjects to register with a service.
type Reg struct {
	Store    Registrar
	Subjects []string
}

// Open opens an inbox on q and registers it as each of regs says.
func Open(ctx context.Context, q bus.Queue, regs ...Reg) (*Waiter, error) {
	in, err := q.Inbox(ctx)
	if err != nil {
		return nil, err
	}
	w := &Waiter{in: in, regs: regs}
	if err := w.register(ctx); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

func (w *Waiter) register(ctx context.Context) error {
	lapses := time.Now().Add(Lease)
	for _, r := range w.regs {
		if err := r.Store.Register(ctx, w.in.Address(), r.Subjects, lapses); err != nil {
			return err
		}
	}
	w.renewed = time.Now()
	return nil
}

// Add registers more subjects, to wait on from now.
func (w *Waiter) Add(ctx context.Context, r Reg) error {
	w.regs = append(w.regs, r)
	return r.Store.Register(ctx, w.in.Address(), r.Subjects, time.Now().Add(Lease))
}

// Next waits until a subject it waits on changes, renewing its registrations meanwhile, and
// returns the subjects changed: every reply already in its inbox, so one change read now answers
// them all.
func (w *Waiter) Next(ctx context.Context) ([]string, error) {
	for {
		wait, cancel := context.WithTimeout(ctx, max(time.Until(w.renewed.Add(Lease/3)), 0))
		j, err := w.in.Next(wait)
		cancel()
		if err == nil {
			changed := []string{string(j.Body)}
			for {
				now, cancel := context.WithTimeout(ctx, time.Millisecond)
				more, err := w.in.Next(now)
				cancel()
				if err != nil {
					return changed, nil
				}
				changed = append(changed, string(more.Body))
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, bus.ErrClosed) {
			return nil, err
		}
		if err := w.register(ctx); err != nil {
			return nil, err
		}
	}
}

// Close ends its registrations and closes its inbox.
func (w *Waiter) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, r := range w.regs {
		if err := r.Store.Unregister(ctx, w.in.Address()); err != nil {
			slog.Warn("wait: ending a registration", "err", err)
		}
	}
	w.in.Close()
}

// CLSubject is a CL's: what changes what its status shows, in ok control (its review, its record),
// in ok automation (its pre and post), and — for its landed commit, CommitSubject — in the
// deployer. ok control's watchers of the CL register on it with each.
func CLSubject(repo, number int64) string {
	return "cl/" + strconv.FormatInt(repo, 10) + "/" + strconv.FormatInt(number, 10)
}

// CommitSubject is a landed commit's, in the deployer: its deployments.
func CommitSubject(repo int64, commit []byte) string {
	return "commit/" + strconv.FormatInt(repo, 10) + "/" + hex.EncodeToString(commit)
}

// HeadSubject is a repository's main, as ok control's change feed applies its moves: what decides
// whether an approved CL whose pre passed can land.
func HeadSubject(repo int64) string { return "head/" + strconv.FormatInt(repo, 10) }

// PathSubject is a path of a repository's main: a move of main's head that changes the path, or
// a file directly under it, changes it (ok control's head step), for the watchers of CLs that
// change it.
func PathSubject(repo int64, path string) string {
	return "path/" + strconv.FormatInt(repo, 10) + "/" + path
}

// GoverningSubject is what governs every CL of a repository as main's head has it — its settings,
// its OWNERS files, its groups-and-ACLs tree — which a move of main's head that changes any of it
// changes.
func GoverningSubject(repo int64) string { return "governing/" + strconv.FormatInt(repo, 10) }

// RunSubject is a run's, in ok automation: its status, as it is queued, runs, and ends, and its log
// as it grows.
func RunSubject(id int64) string { return "run/" + strconv.FormatInt(id, 10) }

// DeploymentSubject is a deployment's, in the deployer: its status, stages, and log.
func DeploymentSubject(id int64) string { return "deployment/" + strconv.FormatInt(id, 10) }

// ServicesSubject is the deployer's services': what the Services pages show — a deployment, or a
// change in what the cluster runs of a service.
const ServicesSubject = "services"
