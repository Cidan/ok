package feed

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/work"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Directory follows the directory's events (dirfeed) for what automation acts on: a repository's
// purge, after which it forgets the repository whole — runs, their logs, plans, submissions.
type Directory struct {
	store  *store.Store
	stream bus.Stream
	events dirfeed.Events
	log    *slog.Logger
	holder string
}

// NewDirectory builds a follower of the directory's events on stream.
func NewDirectory(s *store.Store, stream bus.Stream, events dirfeed.Events, log *slog.Logger) *Directory {
	return &Directory{store: s, stream: stream, events: events, log: log, holder: work.NewHolder()}
}

// Run follows the directory's events until ctx ends, starting again after a failure.
func (d *Directory) Run(ctx context.Context) error {
	fl := &follow.Follower{Stream: d.stream, Topic: dirfeed.Topic, Group: "automation", Log: dirfeed.Log{Events: d.events}, Apply: directoryApplier{d}}
	for {
		if err := fl.Run(ctx); ctx.Err() == nil {
			d.log.Warn("feed: stopped following the directory's events", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

// directoryApplier applies a directory event with the repository claimed, as the SCM's events
// are (feedClaim), so a purge never runs beside one.
type directoryApplier struct{ *Directory }

func (a directoryApplier) Cursors(ctx context.Context, repos []int64) (map[int64]int64, error) {
	return a.store.DirectoryCursors(ctx, repos)
}

func (a directoryApplier) Apply(ctx context.Context, e bus.Event) error {
	held, release, ok, err := work.Claim(ctx, a.store.Work(), feedClaim, e.Key, a.holder, feedLease)
	if err != nil {
		return err
	}
	if !ok {
		return follow.ErrBusy
	}
	defer release()
	cursor, err := a.store.DirectoryCursor(held, e.Key)
	if err != nil || e.Seq <= cursor {
		return err
	}
	ev, err := dirfeed.Decode(e)
	if err != nil {
		return err
	}
	if ev.GetKind() == identitypb.DirectoryEvent_KIND_PURGED {
		if err := a.store.PurgeRepository(held, e.Key); err != nil {
			return fmt.Errorf("purging repository %d: %w", e.Key, err)
		}
		a.log.Info("feed: forgot a purged repository", "repository", e.Key)
	} else if owner := ev.GetRepository().GetOwnerId(); owner != "" {
		// Its account, which its runs are admitted and charged under (docs/hosted.md, "Hosted
		// automation, as built"): a transfer changes it.
		if err := a.store.SetOwner(held, e.Key, owner); err != nil {
			return fmt.Errorf("recording repository %d's account: %w", e.Key, err)
		}
	}
	return a.store.SetDirectoryCursor(held, e.Key, e.Seq)
}
