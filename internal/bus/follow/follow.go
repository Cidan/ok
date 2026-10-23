// Package follow applies a topic of the bus's stream (internal/bus) once per event, in each key's
// order, by a cursor per key (docs/hosted.md, "Consumers"). The stream delivers at least once, in
// no promised order: an event at or before its key's cursor is a duplicate, acknowledged; one
// after a gap waits for the gap to be filled from the log the stream was relayed from. As it
// starts, a follower catches up every key whose log has gone past its cursor — what was published
// before its group was made, or while it was away longer than the stream keeps events.
package follow

import (
	"context"
	"fmt"

	"github.com/Cidan/ok/internal/bus"
)

// Log is where a topic's events come from.
type Log interface {
	// Read returns up to a page of key's events after seq after, in order; fewer than a page at
	// the end of the log.
	Read(ctx context.Context, key, after int64) ([]bus.Event, error)
	// Heads calls fn with each key's last seq — now, or (asOf > 0) as of asOf in a legacy feed
	// that numbered every key's events together — by key.
	Heads(ctx context.Context, asOf int64, fn func(key, seq int64) error) error
}

// Applier is a follower's: its cursors, and how it applies an event.
type Applier interface {
	// Cursor is the seq of the last of key's events applied (0: none).
	Cursor(ctx context.Context, key int64) (int64, error)
	// Apply applies e, the event after its key's cursor, and moves the cursor to e.Seq — doing
	// nothing if the cursor is at e.Seq or past it already, as another of the group's members
	// may have moved it.
	Apply(ctx context.Context, e bus.Event) error
}

// Legacy is an Applier that followed a feed that numbered every key's events together, which it
// leaves for its keys' own cursors once.
type Legacy interface {
	// LegacyCursor is the last place in the legacy feed it applied (0: it never followed one, or
	// left it).
	LegacyCursor(ctx context.Context) (int64, error)
	// Seed sets key's cursor to seq, if it has none.
	Seed(ctx context.Context, key, seq int64) error
	// LeaveLegacy forgets the legacy cursor.
	LeaveLegacy(ctx context.Context) error
}

// Follower follows one topic as a member of a group.
type Follower struct {
	Stream bus.Stream
	Topic  string
	Group  string
	Log    Log
	Apply  Applier
}

// Run follows until ctx ends, returning ctx's error, or the error that stopped it — after which
// running it again resumes from the cursors.
func (f *Follower) Run(ctx context.Context) error {
	// The group first: what is published from here on reaches it, and the catching up below
	// reads what was published before.
	if err := f.Stream.CreateGroup(ctx, f.Topic, f.Group); err != nil {
		return fmt.Errorf("follow: making the group: %w", err)
	}
	if err := f.leaveLegacy(ctx); err != nil {
		return err
	}
	if err := f.Log.Heads(ctx, 0, func(key, seq int64) error { return f.through(ctx, key, seq) }); err != nil {
		return fmt.Errorf("follow: catching up: %w", err)
	}
	return f.Stream.Subscribe(ctx, f.Topic, f.Group, func(ctx context.Context, e bus.Event) error {
		if err := f.through(ctx, e.Key, e.Seq-1); err != nil {
			return err
		}
		return f.Apply.Apply(ctx, e)
	})
}

// leaveLegacy seeds each key's cursor from the legacy feed's, once.
func (f *Follower) leaveLegacy(ctx context.Context) error {
	l, ok := f.Apply.(Legacy)
	if !ok {
		return nil
	}
	at, err := l.LegacyCursor(ctx)
	if err != nil || at == 0 {
		return err
	}
	if err := f.Log.Heads(ctx, at, func(key, seq int64) error { return l.Seed(ctx, key, seq) }); err != nil {
		return fmt.Errorf("follow: leaving the legacy feed: %w", err)
	}
	return l.LeaveLegacy(ctx)
}

// through applies key's events from its cursor through seq, in order.
func (f *Follower) through(ctx context.Context, key, seq int64) error {
	at, err := f.Apply.Cursor(ctx, key)
	if err != nil {
		return err
	}
	for at < seq {
		evs, err := f.Log.Read(ctx, key, at)
		if err != nil {
			return fmt.Errorf("follow: reading key %d's log after %d: %w", key, at, err)
		}
		if len(evs) == 0 {
			return fmt.Errorf("follow: key %d's log ends at %d, before %d", key, at, seq)
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
