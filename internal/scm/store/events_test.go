package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/bus"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// busy makes events in a repository: two CLs uploaded, sent for review, and landed — eight.
func (e *clEnv) busy() {
	e.t.Helper()
	a := e.commit(alice, newChange(), e.head(), "a", map[string]string{"a": "1"})
	b := e.commit(alice, newChange(), a, "b", map[string]string{"a": "1", "b": "1"})
	res := e.mustUpload(alice, ReadinessUnchanged, a, b)
	e.transition(res[0].Number, RequestReview)
	e.transition(res[1].Number, RequestReview)
	e.mustLand(LandItem{Number: res[0].Number, Patchset: 1}, LandItem{Number: res[1].Number, Patchset: 1})
}

// Each repository's events are numbered in its own log, from 1 with no gap.
func TestRepositoryEventLogs(t *testing.T) {
	ctx := context.Background()
	a := newCLEnv(t)
	b := &clEnv{t: t, s: a.s, repo: newRepo(t, a.s).ID}
	a.busy()
	b.busy()
	a.busy()
	for _, e := range []*clEnv{a, b} {
		evs, err := e.s.ReadEvents(ctx, e.repo, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		want := 1 + 8 // the repository's creation, and each busy round's eight
		if e == a {
			want += 8
		}
		if len(evs) != want {
			t.Fatalf("repository %d's log has %d events, want %d", e.repo, len(evs), want)
		}
		for i, ev := range evs {
			if ev.GetRepositoryId() != e.repo || ev.GetRepositorySeq() != int64(i+1) || ev.GetKind() == nil {
				t.Fatalf("event %d of repository %d's log: %v", i+1, e.repo, ev)
			}
		}
		page, err := e.s.ReadEvents(ctx, e.repo, 3, 2)
		if err != nil || len(page) != 2 || page[0].GetRepositorySeq() != 4 || page[1].GetRepositorySeq() != 5 {
			t.Fatalf("the page after 3: %v, %v", page, err)
		}
		if end, err := e.s.ReadEvents(ctx, e.repo, int64(want), 0); err != nil || len(end) != 0 {
			t.Fatalf("the page after the last: %v, %v", end, err)
		}
	}
	if _, err := a.s.ReadEvents(ctx, 1<<40, 0, 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("an unknown repository's log: %v", err)
	}
	if _, err := a.s.ReadEvents(ctx, a.repo, -1, 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("a negative place: %v", err)
	}
}

// stream is a bus.Stream that records what is published, failing while fail is set.
type stream struct {
	mu   sync.Mutex
	got  []bus.Event
	fail error
}

func (s *stream) Publish(_ context.Context, events ...bus.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	s.got = append(s.got, events...)
	return nil
}

func (*stream) CreateGroup(context.Context, string, string) error { return nil }
func (*stream) Subscribe(context.Context, string, string, bus.Handler) error {
	return errors.ErrUnsupported
}

// published are the events of repo the stream got, by place in its log, each checked against what
// it carries; dup is set when any came twice.
func (s *stream) published(t *testing.T, repo int64) (seqs map[int64]bool, dup bool) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	seqs = map[int64]bool{}
	for _, e := range s.got {
		ev := &scmpb.Event{}
		if err := proto.Unmarshal(e.Body, ev); err != nil {
			t.Fatal(err)
		}
		if e.Topic != EventTopic || e.Key != ev.GetRepositoryId() || e.Seq != ev.GetRepositorySeq() || ev.GetKind() == nil {
			t.Fatalf("a published event %+v carries %v", e, ev)
		}
		if e.Key != repo {
			continue
		}
		dup = dup || seqs[e.Seq]
		seqs[e.Seq] = true
	}
	return seqs, dup
}

// drain relays until no event is pending.
func drain(t *testing.T, s *Store, st bus.Stream) {
	t.Helper()
	for {
		n, err := s.relayOnce(context.Background(), st)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
	}
}

// The relay publishes each event once, as it was written; a publish that fails leaves its events
// to the next.
func TestRelay(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	st := &stream{}
	drain(t, e.s, st)
	e.busy()
	st.fail = errors.New("the stream is down")
	if _, err := e.s.relayOnce(ctx, st); !errors.Is(err, st.fail) {
		t.Fatalf("a failed publish: %v", err)
	}
	st.fail = nil
	drain(t, e.s, st)
	seqs, dup := st.published(t, e.repo)
	if len(seqs) != 9 || dup {
		t.Fatalf("published %v (twice: %v), want repository %d's nine events once", seqs, dup, e.repo)
	}
	for s := int64(1); s <= 9; s++ {
		if !seqs[s] {
			t.Errorf("event %d was not published", s)
		}
	}
	if n, err := e.s.relayOnce(ctx, st); err != nil || n != 0 {
		t.Errorf("a relay with nothing pending: %d, %v", n, err)
	}
}

// Relays beside one another publish every event once between them, as the events are written.
func TestRelaysTogether(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	e := newCLEnv(t)
	other := &clEnv{t: t, s: reopen(t, e.s), repo: e.repo}
	st := &stream{}
	var wg sync.WaitGroup
	relays, stop := context.WithCancel(ctx)
	for _, s := range []*Store{e.s, other.s, e.s, other.s} {
		wg.Go(func() { s.Relay(relays, st) })
	}
	for range 3 {
		e.busy()
	}
	want := int64(1 + 3*8)
	for {
		seqs, dup := st.published(t, e.repo)
		if dup {
			t.Fatal("an event was published twice")
		}
		if int64(len(seqs)) == want {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("published %d of %d events", len(seqs), want)
		case <-time.After(50 * time.Millisecond):
		}
	}
	stop()
	wg.Wait()
}
