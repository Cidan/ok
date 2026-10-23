package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Follow applies the SCM's events to the store until ctx ends, as the bus's stream brings them
// and each repository's log fills in: each repository's creation, and each move of its main. Every replica follows, sharing the repositories (the stream's group, deploy); an
// event is applied once, however often it comes.
func Follow(ctx context.Context, s *store.Store, stream bus.Stream, events scmpb.EventsClient, log *slog.Logger) error {
	f := &follow.Follower{Stream: stream, Topic: scmfeed.Topic, Group: "deploy", Log: scmfeed.Log{Events: events}, Apply: applier{s}}
	for {
		if err := f.Run(ctx); ctx.Err() == nil {
			log.Warn("deploy: stopped following the SCM's events", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

// applier is the store as a follow.Applier.
type applier struct{ *store.Store }

func (a applier) Apply(ctx context.Context, e bus.Event) error {
	ev, err := scmfeed.Decode(e)
	if err != nil {
		return err
	}
	return a.Store.Apply(ctx, FeedEvent(ev))
}

// FeedEvent is what the store keeps of an event of the change feed.
func FeedEvent(ev *scmpb.Event) store.FeedEvent {
	out := store.FeedEvent{Seq: ev.GetRepositorySeq(), Repository: ev.GetRepositoryId()}
	switch k := ev.GetKind().(type) {
	case *scmpb.Event_RepositoryCreated:
		out.Created = true
	case *scmpb.Event_HeadMoved:
		out.Head, out.HeadSeq = k.HeadMoved.GetNewHead(), k.HeadMoved.GetHeadSeq()
	}
	return out
}

// FollowDirectory applies the directory's events to the store until ctx ends (dirfeed): each
// repository's full name as it is made, renamed, or transferred. Every replica follows, sharing the
// repositories, as Follow does.
func FollowDirectory(ctx context.Context, s *store.Store, stream bus.Stream, events dirfeed.Events, log *slog.Logger) error {
	f := &follow.Follower{Stream: stream, Topic: dirfeed.Topic, Group: "deploy", Log: dirfeed.Log{Events: events}, Apply: directoryApplier{s}}
	for {
		if err := f.Run(ctx); ctx.Err() == nil {
			log.Warn("deploy: stopped following the directory's events", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

// directoryApplier is the store as the directory's follow.Applier.
type directoryApplier struct{ s *store.Store }

func (a directoryApplier) Cursors(ctx context.Context, keys []int64) (map[int64]int64, error) {
	return a.s.DirectoryCursors(ctx, keys)
}

func (a directoryApplier) Apply(ctx context.Context, e bus.Event) error {
	ev, err := dirfeed.Decode(e)
	if err != nil {
		return err
	}
	return a.s.ApplyDirectory(ctx, store.DirectoryEvent{Seq: ev.GetSeq(), Repository: ev.GetRepositoryId(), FullName: ev.GetRepository().GetFullName(),
		Owner:  ev.GetRepository().GetOwnerId(),
		Purged: ev.GetKind() == identitypb.DirectoryEvent_KIND_PURGED})
}
