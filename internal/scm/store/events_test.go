package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/work"
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

// testFollower is the follower the test environments register, whose relays stream records.
const testFollower = "x"

// stream is a bus.Queue that records the relay's jobs for testFollower, failing while fail is set.
type stream struct {
	bus.Queue
	mu   sync.Mutex
	got  []bus.Job
	fail error
}

func (s *stream) Enqueue(_ context.Context, jobs ...bus.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	for _, j := range jobs {
		if j.Queue == (work.Jobs{Prefix: testFollower}).QueueOf("relay."+scmfeed.Topic) {
			s.got = append(s.got, j)
		}
	}
	return nil
}

// published are the events of repo the relay relayed, by place in its log: each job names a key
// through its page's last event, so a key's jobs, in order, cover its events after the one before
// through theirs. dup is set when a page was relayed twice: two jobs through one event.
func (s *stream) published(t *testing.T, repo int64) (seqs map[int64]bool, dup bool) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var lasts []int64
	seen := map[int64]bool{}
	for _, j := range s.got {
		var key, last int64
		if _, err := fmt.Sscanf(j.ID, "%d/%d", &key, &last); err != nil || key != j.Key {
			t.Fatalf("a relay's job %+v: %v", j, err)
		}
		if key != repo {
			continue
		}
		dup = dup || seen[last]
		seen[last] = true
		lasts = append(lasts, last)
	}
	slices.Sort(lasts)
	seqs = map[int64]bool{}
	for i := int64(1); len(lasts) > 0 && i <= lasts[len(lasts)-1]; i++ {
		seqs[i] = true
	}
	return seqs, dup
}

// drain relays until no event is pending.
func drain(t *testing.T, s *Store, st bus.Queue) {
	t.Helper()
	for {
		n, err := s.relayOnce(context.Background(), st, 0)
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
	if _, err := e.s.relayOnce(ctx, st, 0); !errors.Is(err, st.fail) {
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
	if n, err := e.s.relayOnce(ctx, st, 0); err != nil || n != 0 {
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

// A relay step's job that beats its transaction's commit — its events not placed yet — is looked at
// again soon, until it is a minute old; one whose events another relay published is done, and one
// whose events wait is done once it published them.
func TestRelayStep(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	st := &stream{}
	drain(t, e.s, st)
	step := e.s.relayStep(st)
	head := int64(len(e.checkLog()))
	var later work.Later
	if err := step(ctx, e.repo, relayBody(head+1, head+2), 0); !errors.As(err, &later) || later.After <= 0 || later.After > 2*time.Second {
		t.Fatalf("a step whose transaction has not committed: %v", err)
	}
	if err := step(ctx, e.repo, relayBody(head+1, head+2), commitWithin); err != nil {
		t.Fatalf("a step whose transaction never committed, a minute on: %v", err)
	}
	if err := step(ctx, e.repo, relayBody(1, head), 0); err != nil {
		t.Fatalf("a step whose events are published: %v", err)
	}
	e.busy()
	after := int64(len(e.checkLog()))
	if err := step(ctx, e.repo, relayBody(head+1, after), 0); err != nil {
		t.Fatalf("a step whose events wait: %v", err)
	}
	seqs, dup := st.published(t, e.repo)
	if int64(len(seqs)) != after || dup {
		t.Fatalf("published %d events of %d (twice: %v)", len(seqs), after, dup)
	}
	if n, err := e.s.relayOnce(ctx, st, 0); err != nil || n != 0 {
		t.Errorf("events left pending: %d, %v", n, err)
	}
}

// Every event a write places reaches the stream by its relay step's job alone: no relay looks for
// events, and one with none to relay reads nothing.
func TestRelayedByJobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	e := newCLEnv(t)
	st := &stream{}
	drain(t, e.s, st)
	relays, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan struct{})
	go func() { defer close(done); reopen(t, e.s).Relay(relays, st) }()
	e.busy()
	want := int64(len(e.checkLog()))
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
		case <-time.After(20 * time.Millisecond):
		}
	}
	stop()
	<-done
}

// The relay enqueues for the followers registered as it relays: none before one registers — its
// events are relayed all the same — and, after, for what it relays from then on.
func TestRelayToFollowers(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	e := &clEnv{t: t, s: s, repo: newRepo(t, s).ID}
	st := &stream{}
	e.busy()
	drain(t, s, st)
	if seqs, _ := st.published(t, e.repo); len(seqs) != 0 {
		t.Fatalf("relayed %v with no follower", seqs)
	}
	if n, err := s.relayOnce(ctx, st, 0); err != nil || n != 0 {
		t.Fatalf("with no follower, events left pending: %d, %v", n, err)
	}
	before := int64(len(e.checkLog()))
	if err := s.AddFollower(ctx, testFollower); err != nil {
		t.Fatal(err)
	}
	e.busy()
	drain(t, s, st)
	after := int64(len(e.checkLog()))
	if seqs, dup := st.published(t, e.repo); int64(len(seqs)) != after || dup || after <= before {
		t.Fatalf("relayed through %d (twice: %v), want through %d, past %d", len(seqs), dup, after, before)
	}
}
