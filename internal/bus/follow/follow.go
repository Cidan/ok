// Package follow applies a log another service writes — the SCM's events, the directory's — once
// per event, in each key's order, by a cursor per key (docs/state.md, "Events between services").
//
// The log's relay, as it relays a page of a key's events, enqueues on each follower's queue a job
// naming the key (Relay). The follower's worker that takes the job marks the key's work in the
// follower's own database (internal/work), and its keyed work applies the key's events from its
// cursor to the log's end — one worker a key at a time, and the holder of a key marked again while
// it applies goes round again before it lets go, so nothing written meanwhile waits. A follower
// registers with the log's source as it starts — the relay enqueues for those registered — and the
// first time it follows a log, catches up at once every key its cursors are behind, once for its
// database (work.Once).
package follow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/work"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Log is where a topic's events come from.
type Log interface {
	// Read returns up to a page of key's events after seq after, in order; none at the end of the
	// log.
	Read(ctx context.Context, key, after int64) ([]bus.Event, error)
	// Heads calls fn with how far each key's log reaches, a page of keys at a time, by key.
	Heads(ctx context.Context, fn func(heads []Head) error) error
}

// Head is how far a key's log reaches: the seq of its last event.
type Head struct{ Key, Seq int64 }

// Applier is a follower's: its cursors, and how it applies an event.
type Applier interface {
	// Cursors are the seqs of the last events of keys applied; a key with none is 0 or absent.
	Cursors(ctx context.Context, keys []int64) (map[int64]int64, error)
	// Apply applies e, the event after its key's cursor, and moves the cursor to e.Seq — doing
	// nothing if the cursor is at e.Seq or past it already. It may answer ErrBusy.
	Apply(ctx context.Context, e bus.Event) error
}

// BatchApplier is an Applier that applies a run of a key's events at once: the follower hands it
// each page of the key's log it reads, from the event after its cursor. ApplyBatch applies the
// events after the key's cursor and moves the cursor past the last, skipping those at or before it,
// as Apply does one; it may answer ErrBusy.
type BatchApplier interface {
	Applier
	ApplyBatch(ctx context.Context, evs []bus.Event) error
}

// ErrBusy is an Applier's answer when something else of its service holds the key now: the key's
// work is done again a moment later.
var ErrBusy = errors.New("follow: the key is held")

// Name is a follower's name as its log's source registers it: its role's, the caller's role
// (authn.Caller.Role) when it registers.
func Name(role okpb.Caller) string {
	return strings.ToLower(strings.TrimPrefix(role.String(), "CALLER_"))
}

// relayKind is a follower's step kind of a topic's relays, and workKind the keyed work it marks.
func relayKind(topic string) string { return "relay." + topic }
func workKind(topic string) string  { return "follow." + topic }

// Relay enqueues, on the queue of each of followers — services by name — a job naming each key of
// events whose log grew, one job of a key and its last seq: a log's relay, as it relays them.
func Relay(ctx context.Context, q bus.Queue, topic string, followers []string, events []bus.Event) error {
	last := map[int64]int64{}
	var keys []int64
	for _, e := range events {
		if _, ok := last[e.Key]; !ok {
			keys = append(keys, e.Key)
		}
		last[e.Key] = max(last[e.Key], e.Seq)
	}
	var jobs []bus.Job
	for _, f := range followers {
		fj := work.Jobs{Queue: q, Prefix: f}
		for _, k := range keys {
			j := fj.StepJob(relayKind(topic), k, time.Time{}, nil)
			j.ID = fmt.Sprintf("%d/%d", k, last[k])
			jobs = append(jobs, j)
		}
	}
	if len(jobs) == 0 {
		return nil
	}
	if err := q.Enqueue(ctx, jobs...); err != nil {
		return fmt.Errorf("follow: relaying %s: %w", topic, err)
	}
	return nil
}

// Follower follows one topic for its service.
type Follower struct {
	// Topic is the log's, and Log where its events are read.
	Topic string
	Log   Log
	// Apply applies them.
	Apply Applier
	// Work is the service's keyed work: one piece a key, of kind follow.<topic>, whose jobs are on
	// the service's queue — where the relay's jobs come too, by the service's name (its Jobs'
	// Prefix).
	Work work.Store
	// Register records the service as one of the log's followers with its source: until it has,
	// the relay enqueues nothing for it.
	Register func(ctx context.Context) error
	// Workers bounds the keys this replica applies at once. Default sixteen.
	Workers int
	Logger  *slog.Logger
}

// busyRetry is how soon a key's work is done again after its Applier found the key held.
const busyRetry = 200 * time.Millisecond

// Run follows until ctx ends.
func (f *Follower) Run(ctx context.Context) error {
	log := f.Logger
	if log == nil {
		log = slog.Default()
	}
	workers := f.Workers
	if workers <= 0 {
		workers = 16
	}
	for backoff := time.Second; ; backoff = min(2*backoff, 30*time.Second) {
		err := f.Register(ctx)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		log.Warn("follow: registering with the log's source", "topic", f.Topic, "err", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
	}
	done := make(chan struct{}, 3)
	go func() {
		defer func() { done <- struct{}{} }()
		// Due now, not a release's turnover later as work.Once has it: what was relayed before the
		// follower registered waits for nothing else.
		kind := "caught-up." + f.Topic
		if p, err := f.Work.Peek(ctx, kind, 0); err == nil && !p.Exists {
			err = f.Work.Mark(ctx, kind, 0)
			if err != nil && ctx.Err() == nil {
				log.Warn("follow: marking the catch-up", "topic", f.Topic, "err", err)
			}
		}
		if err := work.Once(ctx, f.Work, kind, f.catchUp, log); err != nil && ctx.Err() == nil {
			log.Warn("follow: catching up", "topic", f.Topic, "err", err)
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		work.Consume(ctx, f.Work, relayKind(f.Topic), workers, time.Second, func(ctx context.Context, key int64, _ []byte, _ time.Duration) error {
			return f.Work.Mark(ctx, workKind(f.Topic), key)
		}, log)
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		r := &work.Runner{Kind: workKind(f.Topic), Store: f.Work, Workers: workers, RetryAfter: time.Second, Log: log, Do: f.through}
		r.Run(ctx)
	}()
	for range 3 {
		<-done
	}
	return nil
}

// catchUp marks every key whose log reaches past its cursor: what was relayed before the follower
// registered, or that it did not follow.
func (f *Follower) catchUp(ctx context.Context) error {
	return f.Log.Heads(ctx, func(heads []Head) error {
		keys := make([]int64, len(heads))
		for i, h := range heads {
			keys[i] = h.Key
		}
		cursors, err := f.Apply.Cursors(ctx, keys)
		if err != nil {
			return err
		}
		for _, h := range heads {
			if cursors[h.Key] < h.Seq {
				if err := f.Work.Mark(ctx, workKind(f.Topic), h.Key); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// through applies key's events from its cursor to its log's end, in order: a page of its log at a
// time to a BatchApplier, an event at a time to an Applier.
func (f *Follower) through(ctx context.Context, key int64) error {
	cursors, err := f.Apply.Cursors(ctx, []int64{key})
	if err != nil {
		return err
	}
	at := cursors[key]
	batch, batches := f.Apply.(BatchApplier)
	for {
		evs, err := f.Log.Read(ctx, key, at)
		if err != nil {
			return fmt.Errorf("follow: reading key %d's log after %d: %w", key, at, err)
		}
		if len(evs) == 0 {
			return nil
		}
		for i, e := range evs {
			if e.Seq != at+int64(i)+1 {
				return fmt.Errorf("follow: key %d's log has %d after %d", key, e.Seq, at+int64(i))
			}
		}
		if batches {
			err = batch.ApplyBatch(ctx, evs)
		} else {
			for _, e := range evs {
				if err = f.Apply.Apply(ctx, e); err != nil {
					break
				}
			}
		}
		if errors.Is(err, ErrBusy) {
			return f.Work.MarkAt(ctx, workKind(f.Topic), key, time.Now().Add(busyRetry))
		}
		if err != nil {
			return err
		}
		at = evs[len(evs)-1].Seq
	}
}
