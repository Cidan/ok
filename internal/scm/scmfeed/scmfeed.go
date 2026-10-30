// Package scmfeed is the SCM's events as their followers read them (internal/bus/follow): the
// topic the SCM's relay publishes them on, each repository's log through the SCM's Events service,
// and an event's body.
package scmfeed

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Topic is the bus's stream the SCM's events are on, each keyed by its repository and numbered by
// its place in that repository's log, its body an ok.scm.v1.Event.
const Topic = "scm.events"

// Log is the SCM's events' log, a follow.Log.
type Log struct{ Events scmpb.EventsClient }

// Register is a follower's registration with the SCM (follow.Follower.Register).
func Register(events scmpb.EventsClient) func(context.Context) error {
	return func(ctx context.Context) error {
		_, err := events.Follow(ctx, &scmpb.FollowRequest{})
		return err
	}
}

// Read returns a page of a repository's events after a place in its log.
func (l Log) Read(ctx context.Context, repo, after int64) ([]bus.Event, error) {
	resp, err := l.Events.Read(ctx, &scmpb.ReadEventsRequest{RepositoryId: repo, AfterSeq: after})
	if err != nil {
		return nil, err
	}
	out := make([]bus.Event, len(resp.GetEvents()))
	for i, ev := range resp.GetEvents() {
		if out[i], err = Encode(ev); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Heads calls fn with how far each repository's log reaches, a page of repositories at a time, by
// repository.
func (l Log) Heads(ctx context.Context, fn func([]follow.Head) error) error {
	var after int64
	for {
		resp, err := l.Events.Heads(ctx, &scmpb.HeadsRequest{AfterRepositoryId: after})
		if err != nil {
			return err
		}
		if len(resp.GetHeads()) == 0 {
			return nil
		}
		page := make([]follow.Head, len(resp.GetHeads()))
		for i, h := range resp.GetHeads() {
			page[i] = follow.Head{Key: h.GetRepositoryId(), Seq: h.GetRepositorySeq()}
		}
		if err := fn(page); err != nil {
			return err
		}
		after = page[len(page)-1].Key
	}
}

// Encode is an event as the stream carries it.
func Encode(ev *scmpb.Event) (bus.Event, error) {
	body, err := proto.Marshal(ev)
	if err != nil {
		return bus.Event{}, err
	}
	return bus.Event{Topic: Topic, Key: ev.GetRepositoryId(), Seq: ev.GetRepositorySeq(), Body: body}, nil
}

// Decode is the event a delivery carries.
func Decode(e bus.Event) (*scmpb.Event, error) {
	ev := &scmpb.Event{}
	if err := proto.Unmarshal(e.Body, ev); err != nil {
		return nil, fmt.Errorf("event %d of repository %d: %w", e.Seq, e.Key, err)
	}
	if ev.GetRepositoryId() != e.Key || ev.GetRepositorySeq() != e.Seq {
		return nil, fmt.Errorf("event %d of repository %d carries event %d of repository %d", e.Seq, e.Key, ev.GetRepositorySeq(), ev.GetRepositoryId())
	}
	return ev, nil
}

// Kind names an event's kind, for metrics.
func Kind(ev *scmpb.Event) string {
	switch ev.GetKind().(type) {
	case *scmpb.Event_Uploaded:
		return "uploaded"
	case *scmpb.Event_Transitioned:
		return "transitioned"
	case *scmpb.Event_Landed:
		return "landed"
	case *scmpb.Event_HeadMoved:
		return "head_moved"
	case *scmpb.Event_RepositoryCreated:
		return "repository_created"
	}
	return "other"
}
