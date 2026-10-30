package engine

import (
	"context"
	"log/slog"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Follow applies the SCM's events to the store until ctx ends (internal/bus/follow): each
// repository's creation, and each move of its main. Every replica follows, a repository's events
// applied by one at a time; an event is applied once, however often its repository is named.
func Follow(ctx context.Context, s *store.Store, events scmpb.EventsClient, log *slog.Logger) error {
	return (&follow.Follower{Topic: scmfeed.Topic, Log: scmfeed.Log{Events: events}, Apply: applier{s},
		Work: s.Work(), Register: scmfeed.Register(events), Logger: log}).Run(ctx)
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
// repository's full name as it is made, renamed, or transferred, as Follow does.
func FollowDirectory(ctx context.Context, s *store.Store, events dirfeed.Events, log *slog.Logger) error {
	return (&follow.Follower{Topic: dirfeed.Topic, Log: dirfeed.Log{Events: events}, Apply: directoryApplier{s},
		Work: s.Work(), Register: dirfeed.Register(events), Logger: log}).Run(ctx)
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
