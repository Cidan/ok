// Package scmfeed is the SCM's events as their followers read them (internal/bus/follow): the
// topic the SCM's relay publishes them on, each repository's log through the SCM's Events service,
// and an event's body.
package scmfeed

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/bus"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Topic is the bus's stream the SCM's events are on, each keyed by its repository and numbered by
// its place in that repository's log, its body an ok.scm.v1.Event.
const Topic = "scm.events"

// Log is the SCM's events' log, a follow.Log.
type Log struct{ Events scmpb.EventsClient }

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

// Heads calls fn with how far each repository's log reaches — now, or as of a place in the SCM's
// feed — by repository.
func (l Log) Heads(ctx context.Context, asOf int64, fn func(repo, seq int64) error) error {
	var after int64
	for {
		resp, err := l.Events.Heads(ctx, &scmpb.HeadsRequest{AfterRepositoryId: after, AsOfSeq: asOf})
		if err != nil {
			return err
		}
		for _, h := range resp.GetHeads() {
			if err := fn(h.GetRepositoryId(), h.GetRepositorySeq()); err != nil {
				return err
			}
			after = h.GetRepositoryId()
		}
		if len(resp.GetHeads()) == 0 {
			return nil
		}
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
