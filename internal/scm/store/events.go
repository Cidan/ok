package store

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"slices"
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
	s.marksMade()
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
		s.allSegmentsChanged()
	}
	deliver := func(channel, payload string) {
		switch channel {
		case contentChannel:
			if sc, ok := backend.ParseScope(payload); ok {
				s.indexChanged(sc)
			}
		case segmentChannel:
			if sc, ok := backend.ParseScope(payload); ok {
				s.segmentsChanged(sc)
			}
		default:
			s.events.signal()
		}
	}
	for backoff := time.Second; ctx.Err() == nil; {
		start := time.Now()
		err := s.signals.Listen(ctx, []string{eventChannel, contentChannel, segmentChannel}, ready, deliver)
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
	if err := s.repositoryExists(ctx, s.ops(ctx), repo); err != nil {
		return nil, err
	}
	rows, err := s.repositoryEvents(ctx, repo, after, limit)
	if err != nil {
		return nil, err
	}
	return decodeEvents(rows)
}

// eventKey is an event's key in an event segment: its place in its repository's log.
func eventKey(seq int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(seq)) }

// repositoryEvents are up to limit of a repository's events after place after, in order: the
// tail's, then — unless the tail's run on from after without a gap through limit of them, which
// nothing else can come between — the segments', the manifest read after the tail.
func (s *Store) repositoryEvents(ctx context.Context, repo, after int64, limit int) ([]backend.Event, error) {
	tail, err := s.ops(ctx).RepositoryEvents(ctx, repo, after, limit)
	if err != nil {
		return nil, err
	}
	if len(tail) == limit && tail[len(tail)-1].RepositorySeq == after+int64(limit) {
		return tail, nil
	}
	readers, err := s.segmentReaders(ctx, repoScope(repo), segEvents, true)
	if err != nil || len(readers) == 0 {
		return tail, err
	}
	have := make(map[int64]bool, len(tail))
	out := slices.Clone(tail)
	for _, e := range tail {
		have[e.RepositorySeq] = true
	}
	n := 0
	err = scanSegments(ctx, readers, nil, eventKey(after+1), nil, false, func(k, v []byte) bool {
		if len(k) == 8 {
			if seq := int64(binary.BigEndian.Uint64(k)); !have[seq] {
				out = append(out, backend.Event{Repository: repo, RepositorySeq: seq, Body: bytes.Clone(v)})
			}
		}
		n++
		return n < limit
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(out, func(a, b backend.Event) int { return cmp.Compare(a.RepositorySeq, b.RepositorySeq) })
	return out[:min(limit, len(out))], nil
}

// LogHead is how far a repository's log reaches: the place of its last event.
type LogHead struct {
	Repository, Seq int64
}

// LogHeads lists, for up to limit repositories with IDs above after (limit 0, or past eventPage:
// eventPage), by ID, the last place in each one's log. A repository with no event is left out.
func (s *Store) LogHeads(ctx context.Context, after int64, limit int) ([]LogHead, error) {
	if after < 0 {
		return nil, invalid("a repository ID is not negative")
	}
	if limit <= 0 || limit > eventPage {
		limit = eventPage
	}
	rows, err := s.ops(ctx).LogHeads(ctx, after, limit)
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
// they commit, any number of relays beside it (docs/hosted.md, "The per-repository log"). Beside
// it, it runs the sequencing passes that writers stopped before running leave (sweep).
func (s *Store) Relay(ctx context.Context, stream bus.Stream) error {
	s.startListening()
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		s.sweep(ctx)
	}()
	defer func() { <-swept }()
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
