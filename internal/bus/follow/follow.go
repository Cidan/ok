// Package follow applies a topic of the bus's stream (internal/bus) once per event, in each key's
// order, by a cursor per key (docs/hosted.md, "Consumers"). The stream delivers at least once, in
// no promised order: an event at or before its key's cursor is a duplicate, acknowledged; one
// after a gap waits for the gap to be filled from the log the stream was relayed from. As it
// starts, a follower catches up every key whose log has gone past its cursor — what was published
// before its group was made, or while it was away longer than the stream keeps events — beside
// following the stream, a page of keys at a time.
package follow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Cidan/ok/internal/bus"
)

// Log is where a topic's events come from.
type Log interface {
	// Read returns up to a page of key's events after seq after, in order; fewer than a page at
	// the end of the log.
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
	// nothing if the cursor is at e.Seq or past it already, as another of the group's members
	// may have moved it. It may answer ErrBusy.
	Apply(ctx context.Context, e bus.Event) error
}

// BatchApplier is an Applier that applies a run of a key's events at once: the follower hands it
// each page of the key's log it reads, from the event after its cursor, and applies an event the
// stream brings through its key's log, its page with it. ApplyBatch applies the events after the
// key's cursor and moves the cursor past the last, skipping those at or before it, as Apply does
// one; it may answer ErrBusy.
type BatchApplier interface {
	Applier
	ApplyBatch(ctx context.Context, evs []bus.Event) error
}

// ErrBusy is an Applier's answer when another of the group's members is applying the key's events
// now: catching up leaves the key to it, and an event the stream brings waits for it a while
// (Follower.BusyWait), then is refused, to come again later.
var ErrBusy = errors.New("follow: another member is applying the key's events")

// catchUpAtOnce bounds how many keys a follower catches up at once.
const catchUpAtOnce = 8

// Follower follows one topic as a member of a group.
type Follower struct {
	Stream bus.Stream
	Topic  string
	Group  string
	Log    Log
	Apply  Applier
	// BusyWait bounds how long an event the stream brings waits for a key another member is
	// applying before it is refused, to come again later. Default five seconds.
	BusyWait time.Duration
}

// Run follows until ctx ends, returning ctx's error, or the error that stopped it — after which
// running it again resumes from the cursors.
func (f *Follower) Run(ctx context.Context) error {
	// The group first: what is published from here on reaches it, and the catching up beside the
	// stream reads what was published before.
	if err := f.Stream.CreateGroup(ctx, f.Topic, f.Group); err != nil {
		return fmt.Errorf("follow: making the group: %w", err)
	}
	run, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	caughtUp := make(chan struct{})
	go func() {
		defer close(caughtUp)
		if err := f.catchUp(run); err != nil && run.Err() == nil {
			stop(fmt.Errorf("follow: catching up: %w", err))
		}
	}()
	err := f.Stream.Subscribe(run, f.Topic, f.Group, f.deliver)
	stop(nil)
	<-caughtUp
	if cause := context.Cause(run); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return err
}

// deliver applies what the stream brings, after its key's events before it, waiting a while for a
// key another member is applying: the stream's own redelivery is seconds away.
func (f *Follower) deliver(ctx context.Context, e bus.Event) error {
	busyWait := f.BusyWait
	if busyWait <= 0 {
		busyWait = 5 * time.Second
	}
	deadline := time.Now().Add(busyWait)
	_, batches := f.Apply.(BatchApplier)
	for wait := 10 * time.Millisecond; ; wait = min(2*wait, 500*time.Millisecond) {
		var err error
		if batches {
			err = f.through(ctx, e.Key, e.Seq)
		} else if err = f.through(ctx, e.Key, e.Seq-1); err == nil {
			err = f.Apply.Apply(ctx, e)
		}
		if !errors.Is(err, ErrBusy) || time.Now().Add(wait).After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// catchUp applies every key's events from its cursor through its log's head, a page of keys at a
// time: one read of the page's cursors, then the keys behind, a few at once.
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
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(catchUpAtOnce)
		for _, h := range heads {
			if cursors[h.Key] >= h.Seq {
				continue
			}
			g.Go(func() error {
				if err := f.through(gctx, h.Key, h.Seq); !errors.Is(err, ErrBusy) {
					return err
				}
				return nil // the member applying it brings it through
			})
		}
		return g.Wait()
	})
}

// through applies key's events from its cursor through seq, in order: a page of its log at a time
// to a BatchApplier, the whole page though it goes past seq.
func (f *Follower) through(ctx context.Context, key, seq int64) error {
	cursors, err := f.Apply.Cursors(ctx, []int64{key})
	if err != nil {
		return err
	}
	at := cursors[key]
	batch, batches := f.Apply.(BatchApplier)
	for at < seq {
		evs, err := f.Log.Read(ctx, key, at)
		if err != nil {
			return fmt.Errorf("follow: reading key %d's log after %d: %w", key, at, err)
		}
		if len(evs) == 0 {
			return fmt.Errorf("follow: key %d's log ends at %d, before %d", key, at, seq)
		}
		if batches {
			for i, e := range evs {
				if e.Seq != at+int64(i)+1 {
					return fmt.Errorf("follow: key %d's log has %d after %d", key, e.Seq, at+int64(i))
				}
			}
			if err := batch.ApplyBatch(ctx, evs); err != nil {
				return err
			}
			at = evs[len(evs)-1].Seq
			continue
		}
		for _, e := range evs {
			if e.Seq > seq {
				return nil
			}
			if e.Seq != at+1 {
				return fmt.Errorf("follow: key %d's log has %d after %d", key, e.Seq, at)
			}
			if err := f.Apply.Apply(ctx, e); err != nil {
				return err
			}
			at = e.Seq
		}
	}
	return nil
}
