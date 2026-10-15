package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/lock"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Follow applies the SCM's change feed to the store until ctx ends: each repository's
// creation and renaming, and each move of its main. One replica follows at a time (the feed
// lock, its follow ended if the lock is lost); an event is applied once, however often it is
// read.
func Follow(ctx context.Context, s *store.Store, events scmpb.EventsClient, log *slog.Logger) error {
	for {
		err := s.LeadFeed(ctx, func(ctx context.Context) error { return follow(ctx, s, events) })
		if !errors.Is(err, lock.ErrNotAcquired) && ctx.Err() == nil {
			log.Warn("deploy: stopped following the change feed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

func follow(ctx context.Context, s *store.Store, events scmpb.EventsClient) error {
	cursor, err := s.Cursor(ctx)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := events.Watch(ctx, &scmpb.WatchRequest{AfterSeq: cursor})
	if err != nil {
		return err
	}
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.Apply(ctx, FeedEvent(ev)); err != nil {
			return fmt.Errorf("event %d: %w", ev.GetSeq(), err)
		}
	}
}

// FeedEvent is what the store keeps of an event of the change feed.
func FeedEvent(ev *scmpb.Event) store.FeedEvent {
	out := store.FeedEvent{Seq: ev.GetSeq(), Repository: ev.GetRepositoryId()}
	switch k := ev.GetKind().(type) {
	case *scmpb.Event_RepositoryCreated:
		out.Name, out.Created = k.RepositoryCreated.GetName(), true
	case *scmpb.Event_RepositoryRenamed:
		out.Name = k.RepositoryRenamed.GetName()
	case *scmpb.Event_HeadMoved:
		out.Head, out.HeadSeq = k.HeadMoved.GetNewHead(), k.HeadMoved.GetHeadSeq()
	}
	return out
}
