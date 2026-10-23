package store

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/scm/store/backend"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// eventHub wakes the relay when an event commits. Each replica listens on eventChannel (the
// backend's Listen), which every event-writing transaction notifies at commit; each
// notification closes the current channel and opens the next.
type eventHub struct {
	start sync.Once
	mu    sync.Mutex
	ch    chan struct{}
	// ctx ends the listening connection (Store.Close), and done closes once it has.
	ctx       context.Context
	stop      context.CancelFunc
	listening bool
	done      chan struct{}
}

func (h *eventHub) wait() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ch == nil {
		h.ch = make(chan struct{})
	}
	return h.ch
}

func (h *eventHub) signal() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ch != nil {
		close(h.ch)
	}
	h.ch = make(chan struct{})
}

// startListening starts the listening connection, once.
func (s *Store) startListening() {
	s.events.start.Do(func() {
		s.events.mu.Lock()
		s.events.listening = true
		s.events.mu.Unlock()
		go func() {
			defer close(s.events.done)
			s.listen()
		}()
	})
}

// Close stops the store's listening connection, if it started, and waits for it to end.
func (s *Store) Close() {
	s.events.stop()
	s.events.mu.Lock()
	listening := s.events.listening
	s.events.mu.Unlock()
	if listening {
		<-s.events.done
	}
}

// listen listens until the store closes, reconnecting after a failure: on eventChannel, for
// event watchers, and on contentChannel, for the content indexes. Every (re)connection signals
// and marks every index stale, so a watcher re-reads anything committed while the connection
// was down, and an index refreshes.
func (s *Store) listen() {
	ctx := s.events.ctx
	ready := func() {
		s.events.signal()
		s.allIndexesChanged()
	}
	deliver := func(channel, payload string) {
		if channel == contentChannel {
			if repo, err := strconv.ParseInt(payload, 10, 64); err == nil {
				s.indexChanged(repo)
			}
			return
		}
		s.events.signal()
	}
	for backoff := time.Second; ctx.Err() == nil; {
		start := time.Now()
		err := s.b.Listen(ctx, []string{eventChannel, contentChannel}, ready, deliver)
		if ctx.Err() != nil {
			return
		}
		slog.Warn("ok scm: event listener", slog.Any("err", err))
		if time.Since(start) > time.Minute {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

// eventPage is how many events one read returns.
const eventPage = 1000

// decodeEvents decodes events as the log holds them. An event written before repositories had
// logs of their own carries its place in its repository's log in its row alone.
func decodeEvents(rows []backend.Event) ([]*scmpb.Event, error) {
	evs := make([]*scmpb.Event, 0, len(rows))
	for _, r := range rows {
		ev := &scmpb.Event{}
		if err := proto.Unmarshal(r.Body, ev); err != nil {
			return nil, fmt.Errorf("event %d of repository %d: %w", r.RepositorySeq, r.Repository, err)
		}
		ev.RepositoryId, ev.RepositorySeq = r.Repository, r.RepositorySeq
		evs = append(evs, ev)
	}
	return evs, nil
}

// ReadEvents returns up to limit of a repository's events after place after in its log, in order
// (limit 0, or past eventPage: eventPage).
func (s *Store) ReadEvents(ctx context.Context, repo, after int64, limit int) ([]*scmpb.Event, error) {
	if after < 0 {
		return nil, invalid("a place in a log is not negative")
	}
	if limit <= 0 || limit > eventPage {
		limit = eventPage
	}
	if err := s.repositoryExists(ctx, s.b, repo); err != nil {
		return nil, err
	}
	rows, err := s.b.RepositoryEvents(ctx, repo, after, limit)
	if err != nil {
		return nil, err
	}
	return decodeEvents(rows)
}

// LogHead is how far a repository's log reaches: the place of its last event.
type LogHead struct {
	Repository, Seq int64
}

// LogHeads lists, for up to limit repositories with IDs above after (limit 0, or past eventPage:
// eventPage), by ID, the last place in each one's log — now, or (asOf > 0) as of the feed's
// sequence number asOf, for a follower moving from the feed to the stream. A repository with no
// event by then is left out.
func (s *Store) LogHeads(ctx context.Context, asOf, after int64, limit int) ([]LogHead, error) {
	if asOf < 0 || after < 0 {
		return nil, invalid("a place in the feed, and a repository ID, are not negative")
	}
	if limit <= 0 || limit > eventPage {
		limit = eventPage
	}
	rows, err := s.b.LogHeads(ctx, asOf, after, limit)
	if err != nil {
		return nil, err
	}
	out := make([]LogHead, len(rows))
	for i, r := range rows {
		out[i] = LogHead{Repository: r[0], Seq: r[1]}
	}
	return out, nil
}

// EventTopic is the bus's stream the relay publishes the SCM's events on (scmfeed.Topic).
const EventTopic = scmfeed.Topic

// relayPage is how many events one relay round publishes.
const relayPage = 500

// Relay publishes to stream every event no relay has yet, until ctx ends: at least once each, as
// they commit, any number of relays beside it (docs/hosted.md, "The per-repository log").
func (s *Store) Relay(ctx context.Context, stream bus.Stream) error {
	s.startListening()
	for {
		wake := s.events.wait()
		n, err := s.relayOnce(ctx, stream)
		if err != nil && ctx.Err() == nil {
			slog.Warn("ok scm: relaying events", slog.Any("err", err))
		}
		if n == relayPage && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-time.After(relayPoll):
		}
	}
}

// relayPoll is how often a caught-up relay looks for events without a notification.
const relayPoll = time.Second

// relayOnce publishes one page of pending events.
func (s *Store) relayOnce(ctx context.Context, stream bus.Stream) (int, error) {
	return s.b.RelayEvents(ctx, relayPage, func(rows []backend.Event) error {
		evs, err := decodeEvents(rows)
		if err != nil {
			return err
		}
		out := make([]bus.Event, len(evs))
		for i, ev := range evs {
			if out[i], err = scmfeed.Encode(ev); err != nil {
				return err
			}
		}
		return stream.Publish(ctx, out...)
	})
}
