// Package dirfeed is the directory's events as their followers read them (internal/bus/follow):
// the topic ok identity's relay publishes them on, each repository's log through identity's
// DirectoryEvents service, and an event's body (docs/hosted.md, "Tenancy, as built").
package dirfeed

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Topic is the bus's stream the directory's events are on, each keyed by its repository and
// numbered by its place in that repository's log, its body an ok.identity.v1.DirectoryEvent.
const Topic = "directory.repositories"

// RetentionPeriod is how long a deleted repository can be restored, after which its cell purges
// it; CreatingTimeout how long an entry may wait for its cell to make its storage before it is
// abandoned, and its storage purged.
const (
	RetentionPeriod = 30 * 24 * time.Hour
	CreatingTimeout = time.Hour
)

// Events is the part of identity's DirectoryEvents service a follower reads.
type Events interface {
	Read(ctx context.Context, in *identitypb.ReadDirectoryEventsRequest, opts ...grpc.CallOption) (*identitypb.ReadDirectoryEventsResponse, error)
	Heads(ctx context.Context, in *identitypb.DirectoryHeadsRequest, opts ...grpc.CallOption) (*identitypb.DirectoryHeadsResponse, error)
	Follow(ctx context.Context, in *identitypb.FollowDirectoryRequest, opts ...grpc.CallOption) (*identitypb.FollowDirectoryResponse, error)
}

// Register is a follower's registration with the directory (follow.Follower.Register).
func Register(events Events) func(context.Context) error {
	return func(ctx context.Context) error {
		_, err := events.Follow(ctx, &identitypb.FollowDirectoryRequest{})
		return err
	}
}

// Log is the directory's events' log, a follow.Log.
type Log struct{ Events Events }

// Read returns a page of a repository's events after a place in its log.
func (l Log) Read(ctx context.Context, repo, after int64) ([]bus.Event, error) {
	resp, err := l.Events.Read(ctx, &identitypb.ReadDirectoryEventsRequest{RepositoryId: repo, AfterSeq: after})
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
		resp, err := l.Events.Heads(ctx, &identitypb.DirectoryHeadsRequest{AfterRepositoryId: after})
		if err != nil {
			return err
		}
		if len(resp.GetHeads()) == 0 {
			return nil
		}
		page := make([]follow.Head, len(resp.GetHeads()))
		for i, h := range resp.GetHeads() {
			page[i] = follow.Head{Key: h.GetRepositoryId(), Seq: h.GetSeq()}
		}
		if err := fn(page); err != nil {
			return err
		}
		after = page[len(page)-1].Key
	}
}

// Encode is an event as the stream carries it.
func Encode(ev *identitypb.DirectoryEvent) (bus.Event, error) {
	body, err := proto.Marshal(ev)
	if err != nil {
		return bus.Event{}, err
	}
	return bus.Event{Topic: Topic, Key: ev.GetRepositoryId(), Seq: ev.GetSeq(), Body: body}, nil
}

// Decode is the event a delivery carries.
func Decode(e bus.Event) (*identitypb.DirectoryEvent, error) {
	ev := &identitypb.DirectoryEvent{}
	if err := proto.Unmarshal(e.Body, ev); err != nil {
		return nil, fmt.Errorf("directory event %d of repository %d: %w", e.Seq, e.Key, err)
	}
	if ev.GetRepositoryId() != e.Key || ev.GetSeq() != e.Seq {
		return nil, fmt.Errorf("directory event %d of repository %d carries event %d of repository %d", e.Seq, e.Key, ev.GetSeq(), ev.GetRepositoryId())
	}
	return ev, nil
}
