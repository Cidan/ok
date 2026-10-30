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
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Close waits for the marks in flight to be made.
func (s *Store) Close() { s.marksMade() }

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

// relayKind is the step that publishes the events a transaction placed: a job it enqueued as it
// was about to commit, naming its repository and the places it gave (pendRelay).
const relayKind = "relay"

// commitWithin is how long a step's job waits for the transaction that enqueued it to commit
// before it takes the transaction as one that never did.
const commitWithin = time.Minute

// pendRelay pends, in tx, the relay of the events it places in repo, first through last.
func pendRelay(ctx context.Context, tx *txn, repo, first, last int64) error {
	return tx.PendStep(ctx, relayKind, repo, time.Time{}, relayBody(first, last))
}

func relayBody(first, last int64) []byte {
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, uint64(first)), uint64(last))
}

// Relay relays every event its transaction placed, as their relay steps' jobs come, until ctx
// ends: on q, a job for each of the log's followers naming each repository whose log grew
// (follow.Relay), at least once each, any number of replicas beside it (docs/state.md, "Events
// between services"). Beside it, it runs the sequencing passes writers stopped before running
// (sequenceStep), and, once, takes up what a release before steps left (relayLeftovers).
func (s *Store) Relay(ctx context.Context, q bus.Queue) error {
	w := s.Work()
	var wg sync.WaitGroup
	wg.Go(func() { work.Consume(ctx, w, relayKind, 4, time.Second, s.relayStep(q), nil) })
	// The writers waiting on what the store's transactions changed — a sequencing pass's end — hear
	// of it (internal/wait), on this replica's bus.
	wg.Go(func() { wait.Run(ctx, s.b.Waits(), q, slog.Default()) })
	if s.b.SequencesAfterCommit() {
		wg.Go(func() { work.Consume(ctx, w, sequenceKind, 2, time.Second, s.sequenceStep, nil) })
	}
	wg.Go(func() {
		if err := work.Once(ctx, s.Work(), relayUpgrade, func(ctx context.Context) error { return s.relayLeftovers(ctx, q) }, nil); err != nil && ctx.Err() == nil {
			slog.Warn("ok scm: taking up what a release before left to relay", slog.Any("err", err))
		}
	})
	wg.Wait()
	return ctx.Err()
}

// relayStep publishes a repository's pending events, done once those the job's transaction placed
// are published — by it, or by another relay, which holds them meanwhile. A job whose
// transaction's events are not placed may beat its commit: looked at again soon, until it is a
// minute old. A job of no events (first and last 0: a move's arrival) is done once none of the
// repository's is pending.
func (s *Store) relayStep(q bus.Queue) work.Step {
	return func(ctx context.Context, repo int64, body []byte, age time.Duration) error {
		if len(body) != 16 {
			return nil
		}
		first, last := int64(binary.BigEndian.Uint64(body)), int64(binary.BigEndian.Uint64(body[8:]))
		written, relayed, err := s.b.EventsRelayed(ctx, repo, first, last)
		switch {
		case err != nil:
			return err
		case !written && age < commitWithin:
			return work.Later{After: min(max(age, 20*time.Millisecond), 2*time.Second)}
		case !written || relayed:
			return nil
		}
		for {
			n, err := s.relayOnce(ctx, q, repo)
			if err != nil {
				return err
			}
			if n < relayPage {
				break
			}
		}
		if _, relayed, err = s.b.EventsRelayed(ctx, repo, first, last); err != nil || relayed {
			return err
		}
		return work.Later{After: time.Second} // another relay holds them
	}
}

// relayUpgrade takes up, once, what a release before steps left: rows its writers left pending,
// and events its relay had not published (relayLeftovers).
const relayUpgrade = "relay-upgrade"

// relayLeftovers sequences every repository's pending rows and relays every pending event.
func (s *Store) relayLeftovers(ctx context.Context, q bus.Queue) error {
	if err := s.sweepOnce(ctx, time.Now()); err != nil {
		return err
	}
	for {
		n, err := s.relayOnce(ctx, q, 0)
		if err != nil || n < relayPage {
			return err
		}
	}
}

// relayOnce relays one page of repo's pending events — of every repository's, for 0 — to the
// followers registered before it takes them: one registered after catches up, as it first starts,
// past every event placed before it registered (follow.Follower). The followers are read before
// the page is taken, which on PostgreSQL holds its connection.
func (s *Store) relayOnce(ctx context.Context, q bus.Queue, repo int64) (int, error) {
	followers, err := s.b.Followers(ctx, scmfeed.Topic)
	if err != nil {
		return 0, err
	}
	return s.b.RelayEvents(ctx, repo, relayPage, func(rows []backend.Event) error {
		out := make([]bus.Event, len(rows))
		for i, r := range rows {
			out[i] = bus.Event{Topic: scmfeed.Topic, Key: r.Repository, Seq: r.RepositorySeq}
		}
		return follow.Relay(ctx, q, scmfeed.Topic, followers, out)
	})
}

// AddFollower records a role as one of the SCM's events' followers (Events.Follow).
func (s *Store) AddFollower(ctx context.Context, name string) error {
	return s.b.AddFollower(ctx, scmfeed.Topic, name)
}
