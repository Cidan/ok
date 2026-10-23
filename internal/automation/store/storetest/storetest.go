// Package storetest is the conformance suite every backend of automation's store
// (internal/automation/store) passes: run it with Run, giving it a new, empty backend for each
// test.
package storetest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/bus"
)

// env makes a test's stores, each over a new backend open makes and a bucket of its own, its
// Listen running for the test's life.
type env struct {
	open     func(t *testing.T) store.Backend
	backends map[*store.Store]store.Backend
	blobs    map[*store.Store]blob.Store
	// signals are the test's replicas', each store's among them.
	signals bus.Signals
}

func (e *env) newStore(t *testing.T, cfg store.Config) *store.Store {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	blobs, err := blob.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := e.open(t)
	cfg.Signals = e.signals
	s := store.New(b, blobs, cfg)
	e.backends[s] = b
	e.blobs[s] = blobs
	go s.Listen(ctx)
	return s
}

// Run runs the suite, each test with stores over new, empty backends that open makes.
func Run(t *testing.T, open func(t *testing.T) store.Backend) {
	for _, tc := range []struct {
		name string
		fn   func(t *testing.T, e *env)
	}{
		{"Start", testStart},
		{"StartRefuses", testStartRefuses},
		{"LeaseIsFIFOAndExclusive", testLeaseIsFIFOAndExclusive},
		{"LeaseWaits", testLeaseWaits},
		{"Heartbeat", testHeartbeat},
		{"LeaseKeys", testLeaseKeys},
		{"LogAndFinish", testLogAndFinish},
		{"LogFull", testLogFull},
		{"LogRaces", testLogRaces},
		{"LogGatheredAsRead", testLogGatheredAsRead},
		{"LogLeftSegment", testLogLeftSegment},
		{"ReadLogFollows", testReadLogFollows},
		{"Cancel", testCancel},
		{"SweepLapsedLeases", testSweepLapsedLeases},
		{"Rerun", testRerun},
		{"List", testList},
		{"Release", testRelease},
		{"Push", testPush},
		{"CommitsTheBuildCacheKeeps", testCommitsTheBuildCacheKeeps},
		{"Plan", testPlan},
		{"PlanFails", testPlanFails},
		{"PlanSupersedes", testPlanSupersedes},
		{"PlanRefuses", testPlanRefuses},
		{"FeedCursor", testFeedCursor},
		{"QueueWork", testQueueWork},
		{"PostRunLostMarksTheQueue", testPostRunLostMarksTheQueue},
		{"EndAndResumePre", testEndAndResumePre},
		{"Submit", testSubmit},
		{"Attempts", testAttempts},
		{"StackPlannedAtItsTop", testStackPlannedAtItsTop},
		{"DiscardAndFail", testDiscardAndFail},
		{"OneActiveAttempt", testOneActiveAttempt},
		{"FirstLanding", testFirstLanding},
		{"AdoptedLandingAtOnce", testAdoptedLandingAtOnce},
		{"QueueState", testQueueState},
		{"CarryFromPre", testCarryFromPre},
		{"Purge", testPurge},
		{"Moves", testMoves},
		{"FairShare", testFairShare},
		{"ConcurrentClaims", testConcurrentClaims},
		{"Flood", testFlood},
		{"Pools", testPools},
		{"Usage", testUsage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, &env{open: open, backends: map[*store.Store]store.Backend{}, blobs: map[*store.Store]blob.Store{}, signals: bus.NewLocal()})
		})
	}
}

const self = "automation.ok.robot.example.com"

func commit() []byte {
	b := make([]byte, 32)
	rand.Read(b)
	return b
}

func build(targets ...string) store.Job { return store.Job{Kind: store.KindBuild, Targets: targets} }

func start(t *testing.T, s *store.Store, repo int64, job store.Job) *store.Run {
	t.Helper()
	r, err := s.Start(context.Background(), store.StartRequest{RepositoryID: repo, CommitID: commit(), Job: job,
		Origin: store.Origin{Phase: store.PhaseManual}, RequestedBy: "alice.human.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func lease(t *testing.T, s *store.Store, worker string) *store.Run {
	t.Helper()
	r, err := s.Lease(context.Background(), worker, "", 0)
	if err != nil || r == nil {
		t.Fatalf("lease for %s: %v, %v", worker, r, err)
	}
	return r
}

func testStart(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	c := commit()
	r, err := s.Start(ctx, store.StartRequest{RepositoryID: 7, CommitID: c,
		Job:    store.Job{Kind: store.KindTest, Targets: []string{"//a:test"}, Args: []string{"-test.run", "X"}},
		Origin: store.Origin{Phase: store.PhasePre, CLNumber: 12, Patchset: 3, Job: "tests"}, RequestedBy: "alice.human.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != store.StatusQueued || got.RepositoryID != 7 || !bytes.Equal(got.CommitID, c) || got.Attempt != 1 ||
		got.Origin != (store.Origin{Phase: store.PhasePre, CLNumber: 12, Patchset: 3, Job: "tests"}) ||
		!slices.Equal(got.Job.Args, []string{"-test.run", "X"}) || got.TimeoutSeconds != int32(store.DefaultConfig.DefaultTimeout/time.Second) {
		t.Errorf("run %+v", got)
	}
	if _, err := s.Get(ctx, r.ID+1000); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a missing run: %v", err)
	}
}

func testStartRefuses(t *testing.T, e *env) {
	s := e.newStore(t, store.DefaultConfig)
	ok := store.StartRequest{RepositoryID: 1, CommitID: commit(), Job: build("//a:b"), Origin: store.Origin{Phase: store.PhaseManual}, RequestedBy: "a"}
	for _, tc := range []struct {
		name string
		edit func(*store.StartRequest)
	}{
		{"no repository", func(r *store.StartRequest) { r.RepositoryID = 0 }},
		{"a short commit", func(r *store.StartRequest) { r.CommitID = r.CommitID[:20] }},
		{"no requester", func(r *store.StartRequest) { r.RequestedBy = "" }},
		{"an unknown kind", func(r *store.StartRequest) { r.Job.Kind = 9 }},
		{"a build of nothing", func(r *store.StartRequest) { r.Job = build() }},
		{"a build with arguments", func(r *store.StartRequest) { r.Job.Args = []string{"x"} }},
		{"a label that is not one", func(r *store.StartRequest) { r.Job = build("a:b") }},
		{"arguments to two tests", func(r *store.StartRequest) {
			r.Job = store.Job{Kind: store.KindTest, Targets: []string{"//a:t", "//b:t"}, Args: []string{"-v"}}
		}},
		{"a run of two targets", func(r *store.StartRequest) {
			r.Job = store.Job{Kind: store.KindRun, Targets: []string{"//a:b", "//a:c"}}
		}},
		{"a lint with arguments", func(r *store.StartRequest) { r.Job = store.Job{Kind: store.KindLint, Args: []string{"-x"}} }},
		{"pre without its CL", func(r *store.StartRequest) { r.Origin = store.Origin{Phase: store.PhasePre} }},
		{"an unknown phase", func(r *store.StartRequest) { r.Origin = store.Origin{Phase: 7} }},
		{"a timeout past the most", func(r *store.StartRequest) { r.Timeout = 48 * time.Hour }},
	} {
		req := ok
		req.Job.Targets = slices.Clone(ok.Job.Targets)
		tc.edit(&req)
		var inv *store.InvalidError
		if _, err := s.Start(context.Background(), req); !errors.As(err, &inv) {
			t.Errorf("%s: %v, want invalid", tc.name, err)
		}
	}
}

// Workers take the oldest queued run first, and no run twice.
func testLeaseIsFIFOAndExclusive(t *testing.T, e *env) {
	s := e.newStore(t, store.DefaultConfig)
	var want []int64
	for i := range 6 {
		want = append(want, start(t, s, 1, build(fmt.Sprintf("//a:%d", i))).ID)
	}
	first := lease(t, s, "w0")
	if first.ID != want[0] || first.Status != store.StatusRunning || first.Worker != "w0" || first.StartedAt.IsZero() {
		t.Fatalf("first lease: %+v, want run %d", first, want[0])
	}
	var mu sync.Mutex
	var got []int64
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.Lease(context.Background(), fmt.Sprintf("w%d", i+1), "", 0)
			if err != nil {
				t.Error(err)
				return
			}
			if r != nil {
				mu.Lock()
				got = append(got, r.ID)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	rest := slices.Clone(want[1:])
	slices.Sort(got)
	slices.Sort(rest)
	if !slices.Equal(got, rest) {
		t.Errorf("leased %v, want each of %v once", got, rest)
	}
}

// A waiting worker is woken by a queued run's notification, well before the poll.
func testLeaseWaits(t *testing.T, e *env) {
	cfg := store.DefaultConfig
	cfg.Poll = time.Minute
	s := e.newStore(t, cfg)
	time.Sleep(200 * time.Millisecond) // the listener is up
	got := make(chan *store.Run, 1)
	go func() {
		r, err := s.Lease(context.Background(), "w", "", 30*time.Second)
		if err != nil {
			t.Error(err)
		}
		got <- r
	}()
	time.Sleep(300 * time.Millisecond)
	want := start(t, s, 1, build("//a:b"))
	select {
	case r := <-got:
		if r == nil || r.ID != want.ID {
			t.Errorf("leased %+v, want run %d", r, want.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the waiting worker was not woken")
	}
	// With nothing queued, a wait ends empty.
	if r, err := s.Lease(context.Background(), "w", "", 300*time.Millisecond); r != nil || err != nil {
		t.Errorf("an empty queue: %+v, %v", r, err)
	}
}

func testHeartbeat(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	r := start(t, s, 1, build("//a:b"))
	lease(t, s, "w")
	if stop, reason, err := s.Heartbeat(ctx, r.ID, "w"); stop != store.StopNone || err != nil {
		t.Fatalf("a held lease: stop %v %q %v", stop, reason, err)
	}
	if stop, reason, _ := s.Heartbeat(ctx, r.ID, "other"); stop != store.StopLeaseLost || reason != store.ErrLeaseLost.Error() {
		t.Errorf("another worker's heartbeat: stop %v %q", stop, reason)
	}
	if _, err := s.Cancel(ctx, r.ID, "not needed", ""); err != nil {
		t.Fatal(err)
	}
	if stop, reason, _ := s.Heartbeat(ctx, r.ID, "w"); stop != store.StopCanceled || reason != "not needed" {
		t.Errorf("a canceled run: stop %v %q", stop, reason)
	}
	// A run past its timeout must stop.
	short, err := s.Start(ctx, store.StartRequest{RepositoryID: 1, CommitID: commit(), Job: build("//a:c"),
		Origin: store.Origin{Phase: store.PhaseManual}, RequestedBy: "a", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	lease(t, s, "w2")
	time.Sleep(1200 * time.Millisecond)
	if stop, reason, _ := s.Heartbeat(ctx, short.ID, "w2"); stop != store.StopTimedOut || !strings.Contains(reason, "timeout of 1s") {
		t.Errorf("an overdue run: stop %v %q", stop, reason)
	}
}

func testLogAndFinish(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	r := start(t, s, 1, build("//a:b"))
	if _, err := s.AppendLog(ctx, r.ID, "w", 0, []byte("x")); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("a log for a queued run: %v", err)
	}
	lease(t, s, "w")
	for _, step := range []struct {
		offset int64
		data   string
		size   int64
	}{
		{0, "hello ", 6},
		{6, "world\n", 12},
		{6, "world\n", 12},    // resent after a lost answer: taken once
		{9, "ld\nmore\n", 17}, // overlapping: only the new bytes
		{17, "", 17},
	} {
		size, err := s.AppendLog(ctx, r.ID, "w", step.offset, []byte(step.data))
		if err != nil || size != step.size {
			t.Fatalf("append %q at %d: %d, %v; want %d", step.data, step.offset, size, err, step.size)
		}
	}
	var inv *store.InvalidError
	if _, err := s.AppendLog(ctx, r.ID, "w", 40, []byte("x")); !errors.As(err, &inv) {
		t.Errorf("a gap: %v", err)
	}
	if _, err := s.AppendLog(ctx, r.ID, "other", 17, []byte("x")); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("another worker's log: %v", err)
	}
	read := func(offset int64) (string, bool) {
		var b strings.Builder
		var end bool
		if err := s.ReadLog(ctx, r.ID, offset, false, func(_ int64, data []byte, e bool) error {
			b.Write(data)
			end = end || e
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return b.String(), end
	}
	if got, end := read(0); got != "hello world\nmore\n" || end {
		t.Errorf("the running log: %q, end %v", got, end)
	}
	// A running run's log is its appends' segments in the bucket: three that each moved it.
	if live, err := e.blobs[s].List(ctx, "live/"); err != nil || len(live) != 3 {
		t.Errorf("the running log's segments: %v, %v", live, err)
	}
	if err := s.Finish(ctx, r.ID, "other", store.StatusPassed, 0, "", nil); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("another worker's finish: %v", err)
	}
	if err := s.Finish(ctx, r.ID, "w", store.StatusFailed, 3, "bazel exited 3", nil); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != store.StatusFailed || done.ExitCode != 3 || done.Reason != "bazel exited 3" || !done.LogStored ||
		done.FinishedAt.IsZero() || done.LogSize != 17 {
		t.Errorf("finished run %+v", done)
	}
	if live, err := e.blobs[s].List(ctx, "live/"); err != nil || len(live) != 0 {
		t.Errorf("segments left behind: %v, %v", live, err)
	}
	if got, end := read(0); got != "hello world\nmore\n" || !end {
		t.Errorf("the stored log: %q, end %v", got, end)
	}
	if got, end := read(6); got != "world\nmore\n" || !end {
		t.Errorf("the stored log from 6: %q, end %v", got, end)
	}
	if got, end := read(17); got != "" || !end {
		t.Errorf("the stored log from its end: %q, end %v", got, end)
	}
	if _, err := s.AppendLog(ctx, r.ID, "w", 17, []byte("late")); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("a log after finishing: %v", err)
	}
}

// A log that reaches the most a run keeps ends with a note saying so, at exactly the most.
func testLogFull(t *testing.T, e *env) {
	ctx := context.Background()
	cfg := store.DefaultConfig
	cfg.MaxLog = 1 << 20
	s := e.newStore(t, cfg)
	r := start(t, s, 1, build("//a:b"))
	lease(t, s, "w")
	big := bytes.Repeat([]byte("y"), 700<<10)
	if _, err := s.AppendLog(ctx, r.ID, "w", 0, big); err != nil {
		t.Fatal(err)
	}
	size, err := s.AppendLog(ctx, r.ID, "w", int64(len(big)), big)
	if err != nil || size != cfg.MaxLog {
		t.Fatalf("the append past the most: %d, %v; want %d", size, err, cfg.MaxLog)
	}
	if _, err := s.AppendLog(ctx, r.ID, "w", size, []byte("z")); !errors.Is(err, store.ErrLogFull) {
		t.Errorf("an append to a full log: %v", err)
	}
	if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if err := s.ReadLog(ctx, r.ID, 0, false, func(_ int64, d []byte, _ bool) error { b.Write(d); return nil }); err != nil {
		t.Fatal(err)
	}
	if int64(b.Len()) != cfg.MaxLog || !strings.HasSuffix(b.String(), "at the 1 MiB a run keeps]\n") {
		t.Errorf("the full log: %d bytes, ending %q", b.Len(), b.String()[b.Len()-60:])
	}
}

// Appends racing at one offset — a request and its resends, longer as more is written — make
// the log the worker's bytes, once each, however they interleave.
func testLogRaces(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	r := start(t, s, 1, build("//a:b"))
	lease(t, s, "w")
	var log []byte
	for i := 0; len(log) < 20_000; i++ {
		log = fmt.Appendf(log, "line %d\n", i)
	}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			// Each sends from where it last saw the log end, a different length each time.
			for at, n := int64(0), 1; at < int64(len(log)); n++ {
				end := min(int64(len(log)), at+int64(1+(n*37+g*101)%900))
				size, err := s.AppendLog(ctx, r.ID, "w", at, log[at:end])
				if err != nil {
					t.Errorf("an append at %d: %v", at, err)
					return
				}
				at = size
			}
		})
	}
	wg.Wait()
	var got bytes.Buffer
	if err := s.ReadLog(ctx, r.ID, 0, false, func(_ int64, d []byte, _ bool) error { got.Write(d); return nil }); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), log) {
		t.Fatalf("the log after racing appends: %d bytes, want the worker's %d", got.Len(), len(log))
	}
	if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	got.Reset()
	if err := s.ReadLog(ctx, r.ID, 0, false, func(_ int64, d []byte, _ bool) error { got.Write(d); return nil }); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), log) {
		t.Fatalf("the stored log: %d bytes, want the worker's %d", got.Len(), len(log))
	}
}

// hookedBlobs is a bucket that runs a hook, once, before a range read of a key under a prefix, and
// fails a put of a key under another, once.
type hookedBlobs struct {
	blob.Store
	mu        sync.Mutex
	getPrefix string
	beforeGet func()
	failPut   string
}

func (h *hookedBlobs) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	h.mu.Lock()
	hook := h.beforeGet
	if hook != nil && strings.HasPrefix(key, h.getPrefix) {
		h.beforeGet = nil
	} else {
		hook = nil
	}
	h.mu.Unlock()
	if hook != nil {
		hook()
	}
	return h.Store.GetRange(ctx, key, offset, length)
}

func (h *hookedBlobs) Put(ctx context.Context, key string, r io.Reader, size int64, opts ...blob.PutOption) error {
	h.mu.Lock()
	fail := h.failPut != "" && strings.HasPrefix(key, h.failPut)
	if fail {
		h.failPut = ""
	}
	h.mu.Unlock()
	if fail {
		return errors.New("the bucket refused it")
	}
	return h.Store.Put(ctx, key, r, size, opts...)
}

// A log another replica gathers into the run's object while this one reads its segments — the
// segments deleted under the reader — is read whole: the reader finds the rest in the object. And a
// gathering that finds another finished it first, the segments gone as it read them, ends without
// an error.
func testLogGatheredAsRead(t *testing.T, e *env) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := e.open(t)
	plain, err := blob.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	hooked := &hookedBlobs{Store: plain}
	cfg := store.DefaultConfig
	cfg.Signals = e.signals
	s := store.New(b, hooked, cfg)
	other := store.New(b, plain, cfg)
	go s.Listen(ctx)
	go other.Listen(ctx)
	// Another replica's sweep gathers every finished run's log.
	gatherElsewhere := func() {
		if err := other.Sweep(ctx); err != nil {
			t.Errorf("the other replica's sweep: %v", err)
		}
	}
	logged := func() (int64, string) {
		t.Helper()
		r := start(t, s, 1, build("//a:b"))
		lease(t, s, "w")
		var want strings.Builder
		for i := range 20 {
			line := fmt.Sprintf("line %d\n", i)
			if _, err := s.AppendLog(ctx, r.ID, "w", int64(want.Len()), []byte(line)); err != nil {
				t.Fatal(err)
			}
			want.WriteString(line)
		}
		return r.ID, want.String()
	}
	read := func(id int64) string {
		t.Helper()
		var got strings.Builder
		if err := s.ReadLog(ctx, id, 0, false, func(_ int64, d []byte, _ bool) error { got.Write(d); return nil }); err != nil {
			t.Fatalf("reading run %d's log: %v", id, err)
		}
		return got.String()
	}

	// Two gatherings at once: this replica's, as the run finishes, and another's.
	id, want := logged()
	hooked.mu.Lock()
	hooked.getPrefix, hooked.beforeGet = "live/", gatherElsewhere
	hooked.mu.Unlock()
	if err := s.Finish(ctx, id, "w", store.StatusPassed, 0, "", nil); err != nil {
		t.Errorf("finishing as another replica gathered the log: %v", err)
	}
	if r, _ := s.Get(ctx, id); !r.LogStored {
		t.Errorf("the log is not stored: %+v", r)
	}
	if got := read(id); got != want {
		t.Errorf("the log gathered twice at once: %q, want %q", got, want)
	}

	// A reader of a finished log not yet gathered, gathered as it reads.
	id, want = logged()
	hooked.mu.Lock()
	hooked.failPut = "runs/"
	hooked.mu.Unlock()
	if err := s.Finish(ctx, id, "w", store.StatusPassed, 0, "", nil); err == nil {
		t.Fatal("the gathering the bucket refused succeeded")
	}
	hooked.mu.Lock()
	hooked.getPrefix, hooked.beforeGet = "live/", gatherElsewhere
	hooked.mu.Unlock()
	if got := read(id); got != want {
		t.Errorf("the log read as it was gathered: %q, want %q", got, want)
	}
}

// A segment a request wrote where the log ends, and could not then move the log past — it lost a
// race — holds the worker's bytes: the next append there takes it as it is, and sends what it has
// beyond the segment after it.
func testLogLeftSegment(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	r := start(t, s, 1, build("//a:b"))
	lease(t, s, "w")
	if _, err := s.AppendLog(ctx, r.ID, "w", 0, []byte("one\n")); err != nil {
		t.Fatal(err)
	}
	run, err := s.Get(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	// A segment is live/<run>/<the lease's start, in microseconds>/<its offset, in hex> (logs.go).
	leave := func(at int64, data string) {
		t.Helper()
		key := fmt.Sprintf("live/%d/%d/%016x", r.ID, run.StartedAt.UnixMicro(), at)
		if err := e.blobs[s].Put(ctx, key, strings.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
	}
	leave(4, "two\nthr")
	if size, err := s.AppendLog(ctx, r.ID, "w", 4, []byte("two\n")); err != nil || size != 11 {
		t.Fatalf("an append where a longer segment was left: %d, %v; want 11", size, err)
	}
	if size, err := s.AppendLog(ctx, r.ID, "w", 11, []byte("ee\n")); err != nil || size != 14 {
		t.Fatalf("the append after it: %d, %v; want 14", size, err)
	}
	leave(14, "fo")
	if size, err := s.AppendLog(ctx, r.ID, "w", 14, []byte("four\n")); err != nil || size != 19 {
		t.Fatalf("an append where a shorter segment was left: %d, %v; want 19", size, err)
	}
	read := func() string {
		t.Helper()
		var b strings.Builder
		if err := s.ReadLog(ctx, r.ID, 0, false, func(_ int64, d []byte, _ bool) error { b.Write(d); return nil }); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	if got := read(); got != "one\ntwo\nthree\nfour\n" {
		t.Fatalf("the log: %q", got)
	}
	if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	if got := read(); got != "one\ntwo\nthree\nfour\n" {
		t.Fatalf("the stored log: %q", got)
	}
}

// A follower sees what is written as it is written, and the end once the run finishes.
func testReadLogFollows(t *testing.T, e *env) {
	ctx := context.Background()
	cfg := store.DefaultConfig
	cfg.Poll = time.Minute
	s := e.newStore(t, cfg)
	time.Sleep(200 * time.Millisecond)
	r := start(t, s, 1, build("//a:b"))
	lease(t, s, "w")
	type got struct {
		data string
		end  bool
	}
	out := make(chan got, 16)
	go func() {
		err := s.ReadLog(ctx, r.ID, 0, true, func(_ int64, d []byte, end bool) error {
			out <- got{string(d), end}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		close(out)
	}()
	next := func() got {
		select {
		case g := <-out:
			return g
		case <-time.After(10 * time.Second):
			t.Fatal("the follower saw nothing")
			return got{}
		}
	}
	s.AppendLog(ctx, r.ID, "w", 0, []byte("one\n"))
	if g := next(); g.data != "one\n" || g.end {
		t.Errorf("first: %+v", g)
	}
	s.AppendLog(ctx, r.ID, "w", 4, []byte("two\n"))
	if g := next(); g.data != "two\n" || g.end {
		t.Errorf("second: %+v", g)
	}
	if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	var tail strings.Builder
	var end bool
	for g := range out {
		tail.WriteString(g.data)
		end = end || g.end
	}
	if tail.String() != "" || !end {
		t.Errorf("after finishing: %q, end %v", tail.String(), end)
	}
}

func testCancel(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	queued := start(t, s, 1, build("//a:b"))
	c, err := s.Cancel(ctx, queued.ID, "superseded", "prn_ada")
	if err != nil || c.Status != store.StatusCanceled || c.Reason != "superseded" || c.CanceledBy != "prn_ada" || !c.LogStored || c.FinishedAt.IsZero() {
		t.Fatalf("a queued run: %+v, %v", c, err)
	}
	if r, _ := s.Lease(ctx, "w", "", 0); r != nil {
		t.Errorf("a canceled run was leased: %+v", r)
	}
	running := start(t, s, 1, build("//a:c"))
	lease(t, s, "w")
	c, err = s.Cancel(ctx, running.ID, "", "prn_bob")
	if err != nil || c.Status != store.StatusRunning || c.CanceledBy != "prn_bob" {
		t.Fatalf("a running run is canceled by its worker: %+v, %v", c, err)
	}
	if stop, reason, _ := s.Heartbeat(ctx, running.ID, "w"); stop != store.StopCanceled || reason != "canceled" {
		t.Errorf("the worker is told: %v %q", stop, reason)
	}
	if err := s.Finish(ctx, running.ID, "w", store.StatusCanceled, 0, "canceled", nil); err != nil {
		t.Fatal(err)
	}
	c, err = s.Cancel(ctx, running.ID, "again", "prn_ada")
	if err != nil || c.Status != store.StatusCanceled || c.Reason != "canceled" || c.CanceledBy != "prn_bob" {
		t.Errorf("a finished run is left as it is: %+v, %v", c, err)
	}
	var b strings.Builder
	if err := s.ReadLog(ctx, queued.ID, 0, true, func(_ int64, d []byte, end bool) error {
		if !end {
			t.Error("a never-run log does not end")
		}
		b.Write(d)
		return nil
	}); err != nil || b.Len() != 0 {
		t.Errorf("a never-run log: %q, %v", b.String(), err)
	}
}

// A lapsed lease runs its run again once, with a fresh log; lapsed again, the run errs; a
// canceled run whose lease lapses ends canceled.
func testSweepLapsedLeases(t *testing.T, e *env) {
	ctx := context.Background()
	// A lease lasts three heartbeats; only Sweep acts on one that lapsed, so waiting past it is
	// lapsing it.
	cfg := store.DefaultConfig
	cfg.Heartbeat = 100 * time.Millisecond
	s := e.newStore(t, cfg)
	lapse := func(int64) { time.Sleep(3*cfg.Heartbeat + 200*time.Millisecond) }
	r := start(t, s, 1, build("//a:b"))
	lease(t, s, "w1")
	if _, err := s.AppendLog(ctx, r.ID, "w1", 0, []byte("partial")); err != nil {
		t.Fatal(err)
	}
	lapse(r.ID)
	if err := s.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	again, _ := s.Get(ctx, r.ID)
	if again.Status != store.StatusQueued || again.Attempt != 2 || again.Worker != "" || again.LogSize != 0 ||
		!strings.Contains(again.Reason, "w1 was lost") {
		t.Fatalf("after one lapse: %+v", again)
	}
	if again2 := lease(t, s, "w2"); again2.ID != r.ID {
		t.Fatalf("the requeued run was not leased again: %+v", again2)
	}
	if size, err := s.AppendLog(ctx, r.ID, "w2", 0, []byte("fresh")); err != nil || size != 5 {
		t.Errorf("the second attempt's log: %d, %v", size, err)
	}
	lapse(r.ID)
	if err := s.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	lost, _ := s.Get(ctx, r.ID)
	if lost.Status != store.StatusError || !strings.Contains(lost.Reason, "last attempt") || !lost.LogStored {
		t.Errorf("after two lapses: %+v", lost)
	}
	// Its log is its last lease's alone, each lease's segments gone with the log stored.
	var log strings.Builder
	if err := s.ReadLog(ctx, r.ID, 0, false, func(_ int64, d []byte, _ bool) error { log.Write(d); return nil }); err != nil || log.String() != "fresh" {
		t.Errorf("the lost run's log: %q, %v", log.String(), err)
	}
	if live, err := e.blobs[s].List(ctx, "live/"); err != nil || len(live) != 0 {
		t.Errorf("segments left behind: %v, %v", live, err)
	}
	c := start(t, s, 1, build("//a:c"))
	lease(t, s, "w3")
	if _, err := s.Cancel(ctx, c.ID, "stop it", ""); err != nil {
		t.Fatal(err)
	}
	lapse(c.ID)
	if err := s.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get(ctx, c.ID); got.Status != store.StatusCanceled || got.Reason != "stop it" {
		t.Errorf("a canceled run's lapse: %+v", got)
	}
}

func testRerun(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	r, err := s.Start(ctx, store.StartRequest{RepositoryID: 3, CommitID: commit(), Job: store.Job{Kind: store.KindLint, Targets: []string{"//a:a"}},
		Origin: store.Origin{Phase: store.PhasePre, CLNumber: 4, Patchset: 1, Job: "lint"}, RequestedBy: "alice.human.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	var conflict *store.ConflictError
	if _, err := s.Rerun(ctx, r.ID, "bob.human.example.com"); !errors.As(err, &conflict) {
		t.Errorf("a queued run's rerun: %v", err)
	}
	lease(t, s, "w")
	if err := s.Finish(ctx, r.ID, "w", store.StatusFailed, 1, "", nil); err != nil {
		t.Fatal(err)
	}
	again, err := s.Rerun(ctx, r.ID, "bob.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if again.RerunOf != r.ID || again.Status != store.StatusQueued || again.RequestedBy != "bob.human.example.com" ||
		!bytes.Equal(again.CommitID, r.CommitID) || again.Origin != r.Origin || !slices.Equal(again.Job.Targets, r.Job.Targets) {
		t.Errorf("the rerun %+v of %+v", again, r)
	}
}

func testList(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	c := commit()
	var ids []int64
	for i := range 5 {
		r, err := s.Start(ctx, store.StartRequest{RepositoryID: 9, CommitID: c, Job: build(fmt.Sprintf("//a:%d", i)),
			Origin: store.Origin{Phase: store.PhasePre, CLNumber: 2, Patchset: int32(1 + i%2)}, RequestedBy: "a"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.ID)
	}
	start(t, s, 9, build("//other:x"))
	idsOf := func(rs []*store.Run) []int64 {
		var out []int64
		for _, r := range rs {
			out = append(out, r.ID)
		}
		return out
	}
	all, err := s.List(ctx, store.ListQuery{RepositoryID: 9, CLNumber: 2})
	if err != nil || !slices.Equal(idsOf(all), []int64{ids[4], ids[3], ids[2], ids[1], ids[0]}) {
		t.Errorf("a CL's runs: %v, %v", idsOf(all), err)
	}
	two, _ := s.List(ctx, store.ListQuery{RepositoryID: 9, CLNumber: 2, Patchset: 2})
	if !slices.Equal(idsOf(two), []int64{ids[3], ids[1]}) {
		t.Errorf("patchset 2's runs: %v", idsOf(two))
	}
	page, _ := s.List(ctx, store.ListQuery{RepositoryID: 9, CommitID: c, Limit: 2, BeforeID: ids[3]})
	if !slices.Equal(idsOf(page), []int64{ids[2], ids[1]}) {
		t.Errorf("a commit's runs, paged: %v", idsOf(page))
	}
	var inv *store.InvalidError
	if _, err := s.List(ctx, store.ListQuery{RepositoryID: 9}); !errors.As(err, &inv) {
		t.Errorf("neither a CL nor a commit: %v", err)
	}
}

// A released run goes back to the queue without spending an attempt, its log cleared; one
// canceled meanwhile ends canceled; only the lease's holder releases it.
func testRelease(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	r := start(t, s, 1, build("//a:b"))
	lease(t, s, "w1")
	if _, err := s.AppendLog(ctx, r.ID, "w1", 0, []byte("half")); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, r.ID, "w2", ""); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("another worker's release: %v", err)
	}
	if err := s.Release(ctx, r.ID, "w1", "w1 is shutting down"); err != nil {
		t.Fatal(err)
	}
	back, _ := s.Get(ctx, r.ID)
	if back.Status != store.StatusQueued || back.Attempt != 1 || back.Worker != "" || back.LogSize != 0 || !back.StartedAt.IsZero() ||
		back.Reason != "w1 is shutting down" {
		t.Fatalf("released: %+v", back)
	}
	if again := lease(t, s, "w2"); again.ID != r.ID {
		t.Fatalf("the released run was not leased again: %+v", again)
	}
	if size, err := s.AppendLog(ctx, r.ID, "w2", 0, []byte("whole")); err != nil || size != 5 {
		t.Errorf("the new holder's log: %d, %v", size, err)
	}
	if _, err := s.Cancel(ctx, r.ID, "not wanted", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, r.ID, "w2", ""); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get(ctx, r.ID); got.Status != store.StatusCanceled || got.Reason != "not wanted" {
		t.Errorf("a canceled run's release: %+v", got)
	}
}

// A push is a deployment's: it names one oci_push, and when it passes it records what it
// pushed, which Get and List carry; no one runs it again.
func testPush(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	push := store.Job{Kind: store.KindPush, Targets: []string{"//web:push"}}
	deploy := store.Origin{Phase: store.PhaseDeploy, Job: "//.ok/workflow/deploy.yaml:deploy:web"}
	req := func(job store.Job, origin store.Origin) store.StartRequest {
		return store.StartRequest{RepositoryID: 1, CommitID: commit(), Job: job, Origin: origin, RequestedBy: "deploy.ok.robot.example.com"}
	}
	var inv *store.InvalidError
	for name, r := range map[string]store.StartRequest{
		"a push outside a deployment": req(push, store.Origin{Phase: store.PhaseManual}),
		"a deployment's build":        req(build("//web:web"), deploy),
		"a push of two":               req(store.Job{Kind: store.KindPush, Targets: []string{"//a:push", "//b:push"}}, deploy),
		"a push with arguments":       req(store.Job{Kind: store.KindPush, Targets: []string{"//a:push"}, Args: []string{"x"}}, deploy),
		"a deployment with a CL":      req(push, store.Origin{Phase: store.PhaseDeploy, Job: "d", CLNumber: 3}),
		"a deployment with no deploy": req(push, store.Origin{Phase: store.PhaseDeploy}),
	} {
		if _, err := s.Start(ctx, r); !errors.As(err, &inv) {
			t.Errorf("%s: %v", name, err)
		}
	}
	started, err := s.Start(ctx, req(push, deploy))
	if err != nil {
		t.Fatal(err)
	}
	r := lease(t, s, "w")
	if r.ID != started.ID {
		t.Fatalf("leased %d, want the push %d", r.ID, started.ID)
	}
	good := store.Pushed{Target: "//web:push", Registry: "registry.example.com", Repository: "apps/web",
		Digest: "sha256:" + strings.Repeat("ab", 32), Tags: []string{"latest", "c0ffee"}}
	for name, pushed := range map[string][]store.Pushed{
		"nothing pushed":      nil,
		"another target":      {{Target: "//other:push", Registry: good.Registry, Repository: good.Repository, Digest: good.Digest}},
		"a digest that isn't": {{Target: good.Target, Registry: good.Registry, Repository: good.Repository, Digest: "sha256:xyz"}},
		"two images":          {good, good},
	} {
		if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", nil, pushed...); !errors.As(err, &inv) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", nil, good); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Pushed) != 1 || got.Pushed[0].Digest != good.Digest || !slices.Equal(got.Pushed[0].Tags, good.Tags) {
		t.Errorf("Get: %+v", got.Pushed)
	}
	listed, err := s.List(ctx, store.ListQuery{RepositoryID: 1, CommitID: r.CommitID})
	if err != nil || len(listed) != 1 || len(listed[0].Pushed) != 1 {
		t.Errorf("List: %+v (%v)", listed, err)
	}
	var conflict *store.ConflictError
	if _, err := s.Rerun(ctx, r.ID, "alice.human.example.com"); !errors.As(err, &conflict) {
		t.Errorf("rerunning a push: %v", err)
	}
	// Only a push carries what it pushed.
	other := start(t, s, 1, build("//web:web"))
	lease(t, s, "w")
	if err := s.Finish(ctx, other.ID, "w", store.StatusPassed, 0, "", nil, good); !errors.As(err, &inv) {
		t.Errorf("a build carrying a pushed image: %v", err)
	}
}

// The build cache sweep's live commits: each repository's newest commits on main, and the
// commits of runs in flight or just finished.
func testCommitsTheBuildCacheKeeps(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	var mains [][]byte
	for seq := range 6 {
		c := commit()
		mains = append(mains, c)
		if err := s.RecordMainCommit(ctx, 5, int64(seq), c); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.MainCommits(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[5]) != 4 || !bytes.Equal(got[5][0], mains[5]) || !bytes.Equal(got[5][3], mains[2]) {
		t.Fatalf("main's newest 4: %x", got[5])
	}
	queued := start(t, s, 6, build("//a:a"))
	done := start(t, s, 6, build("//b:b"))
	// done finishes now: a run finished before since is not kept.
	if _, err := s.Cancel(ctx, done.ID, "done", ""); err != nil {
		t.Fatal(err)
	}
	runs, err := s.RunCommits(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(runs[6]) != 1 || !bytes.Equal(runs[6][0], queued.CommitID) {
		t.Fatalf("runs' commits: %x", runs[6])
	}
	// A run finished since since is kept.
	if runs, err := s.RunCommits(ctx, time.Now().Add(-time.Hour)); err != nil || len(runs[6]) != 2 {
		t.Fatalf("runs' commits, the finished one's too: %x, %v", runs[6], err)
	}
}

// plan records a patchset's plan on repository 9, CL 4.
func plan(t *testing.T, s *store.Store, patchset int32) (*store.Plan, []byte) {
	t.Helper()
	c := commit()
	p, created, err := s.CreatePlan(context.Background(), store.PlanRequest{RepositoryID: 9, CLNumber: 4, Patchset: patchset,
		CommitID: c, WorkflowsCommit: commit(), Context: []byte(`{"phase":"pre","cl":4}`), RequestedBy: "alice.human.example.com"})
	if err != nil || !created {
		t.Fatalf("planning patchset %d: %v (created %v)", patchset, err, created)
	}
	return p, c
}

func pre(t *testing.T, s *store.Store, patchset int32) *store.Pre {
	t.Helper()
	p, err := s.GetPre(context.Background(), 9, 4, patchset)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

var twoJobs = &store.PlanResult{
	Jobs: []store.PlannedJob{
		{Job: "build", Spec: build("//a:a"), Why: "every target the change affects must build"},
		{Job: "test //a:test", Spec: store.Job{Kind: store.KindTest, Targets: []string{"//a:test"}}, Why: "the test is affected"},
	},
	Skipped: []store.SkippedJob{{Job: "//.ok/workflow/ci.yaml:pre:bench", Target: "//b:bench", Why: "its target is not affected"}},
}

func testPlan(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	if p := pre(t, s, 1); p.Verdict != store.VerdictNone || p.Plan != nil {
		t.Fatalf("before planning: %+v", p)
	}
	p, c := plan(t, s, 1)

	// A patchset is planned once, however often it is announced.
	again, created, err := s.CreatePlan(ctx, store.PlanRequest{RepositoryID: 9, CLNumber: 4, Patchset: 1, CommitID: commit(),
		WorkflowsCommit: commit(), Context: []byte(`{}`), RequestedBy: "alice.human.example.com"})
	if err != nil || created || again.ID != p.ID || !bytes.Equal(again.CommitID, c) {
		t.Fatalf("planning it again: %+v, %v, %v", again, created, err)
	}

	// Its planning run is queued, carrying the plan, and the worker gets the job context.
	r := lease(t, s, "w")
	if r.Job.Kind != store.KindPlan || r.PlanID != p.ID || r.Origin.Phase != store.PhasePre || r.Origin.CLNumber != 4 || r.Origin.Job != "plan" ||
		!bytes.Equal(r.CommitID, c) || r.Why != "patchset 1 was uploaded" {
		t.Fatalf("the planning run: %+v", r)
	}
	if got, err := s.GetPlan(ctx, r.PlanID); err != nil || !strings.Contains(string(got.Context), `"cl": 4`) && !strings.Contains(string(got.Context), `"cl":4`) {
		t.Fatalf("its plan: %+v, %v", got, err)
	}
	if v := pre(t, s, 1).Verdict; v != store.VerdictPending {
		t.Errorf("while planning: %d", v)
	}

	// A planning run that passes carries its plan; nothing else does.
	if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", nil); err == nil {
		t.Fatal("a planning run passed without its plan")
	}
	if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", &store.PlanResult{Jobs: []store.PlannedJob{{Job: "x", Spec: store.Job{Kind: store.KindPlan}}}}); err == nil {
		t.Fatal("a plan queued a planning run")
	}
	if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", twoJobs); err != nil {
		t.Fatal(err)
	}
	got := pre(t, s, 1)
	if got.Verdict != store.VerdictPending || got.PlanRun.Status != store.StatusPassed || len(got.Jobs) != 2 ||
		got.Jobs[0].Origin.Job != "build" || got.Jobs[1].Origin.Job != "test //a:test" || got.Jobs[1].Why != "the test is affected" ||
		len(got.Plan.Skipped) != 1 || got.Plan.Skipped[0].Target != "//b:bench" || !got.Plan.Planned {
		t.Fatalf("after planning: %+v", got)
	}
	for _, j := range got.Jobs {
		if j.PlanID != p.ID || j.Origin.Patchset != 1 || j.Status != store.StatusQueued || !bytes.Equal(j.CommitID, c) {
			t.Errorf("a planned job: %+v", j)
		}
	}
	// A planned patchset is not planned again.
	if _, err := s.Rerun(ctx, r.ID, "alice.human.example.com"); err == nil {
		t.Error("a planned patchset's planning run ran again")
	}

	// A job that fails fails pre, whatever is still running; its rerun is what counts.
	b, tst := lease(t, s, "w"), lease(t, s, "w")
	if err := s.Finish(ctx, b.ID, "w", store.StatusFailed, 1, "", nil); err != nil {
		t.Fatal(err)
	}
	if v := pre(t, s, 1).Verdict; v != store.VerdictFailed {
		t.Errorf("with a failed job and one running: %d", v)
	}
	again2, err := s.Rerun(ctx, b.ID, "alice.human.example.com")
	if err != nil || again2.PlanID != p.ID || again2.Why != b.Why || again2.Origin.Job != "build" {
		t.Fatalf("rerunning the build: %+v, %v", again2, err)
	}
	if v := pre(t, s, 1).Verdict; v != store.VerdictPending {
		t.Errorf("with the build rerun: %d", v)
	}
	if err := s.Finish(ctx, tst.ID, "w", store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	b2 := lease(t, s, "w")
	if err := s.Finish(ctx, b2.ID, "w", store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	got = pre(t, s, 1)
	if got.Verdict != store.VerdictPassed || len(got.Jobs) != 2 || got.Jobs[0].ID != b2.ID {
		t.Errorf("with every job passed: %+v", got)
	}
}

func testPlanFails(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	plan(t, s, 1)
	r := lease(t, s, "w")
	if err := s.Finish(ctx, r.ID, "w", store.StatusFailed, 1, "the build graph: a cycle", nil); err != nil {
		t.Fatal(err)
	}
	if p := pre(t, s, 1); p.Verdict != store.VerdictFailed || len(p.Jobs) != 0 {
		t.Fatalf("a failed plan: %+v", p)
	}
	// An unplanned patchset's planning run may run again.
	if _, err := s.Rerun(ctx, r.ID, "alice.human.example.com"); err != nil {
		t.Fatal(err)
	}
	if p := pre(t, s, 1); p.Verdict != store.VerdictPending || p.PlanRun.RerunOf != r.ID {
		t.Fatalf("planning again: %+v", p)
	}
}

// A new patchset supersedes the last: its pre runs that have not finished are canceled, and a
// planning run that passes after that queues its jobs canceled.
func testPlanSupersedes(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	plan(t, s, 1)
	r := lease(t, s, "w")
	if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", twoJobs); err != nil {
		t.Fatal(err)
	}
	running := lease(t, s, "w") // the build runs; the test stays queued
	manual := start(t, s, 9, build("//a:a"))

	plan(t, s, 2)
	old := pre(t, s, 1)
	if old.Verdict != store.VerdictFailed {
		t.Errorf("patchset 1 after patchset 2: %d", old.Verdict)
	}
	for _, j := range old.Jobs {
		switch j.ID {
		case running.ID:
			if stop, reason, _ := s.Heartbeat(ctx, j.ID, "w"); stop != store.StopCanceled || reason != "patchset 2 superseded it" {
				t.Errorf("the running build: %d %q", stop, reason)
			}
		default:
			if j.Status != store.StatusCanceled || j.Reason != "patchset 2 superseded it" {
				t.Errorf("the queued test: %+v", j)
			}
		}
	}
	if m, _ := s.Get(ctx, manual.ID); m.Status != store.StatusQueued {
		t.Errorf("a manual run was canceled: %+v", m)
	}

	// Patchset 2's planning run is queued; patchset 3's supersedes it while it runs, and it
	// passes anyway: its jobs are recorded canceled.
	var two *store.Run
	for two == nil || two.Job.Kind != store.KindPlan {
		two = lease(t, s, "w2")
	}
	plan(t, s, 3)
	if err := s.Finish(ctx, two.ID, "w2", store.StatusPassed, 0, "", twoJobs); err != nil {
		t.Fatal(err)
	}
	p2 := pre(t, s, 2)
	if len(p2.Jobs) != 2 || p2.Verdict != store.VerdictFailed {
		t.Fatalf("patchset 2: %+v", p2)
	}
	for _, j := range p2.Jobs {
		if j.Status != store.StatusCanceled || j.Reason != "patchset 3 superseded it" {
			t.Errorf("a superseded plan's job: %+v", j)
		}
	}
}

func testPlanRefuses(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	var inv *store.InvalidError
	for name, req := range map[string]store.PlanRequest{
		"no patchset":   {RepositoryID: 1, CLNumber: 1, CommitID: commit(), WorkflowsCommit: commit(), Context: []byte(`{}`), RequestedBy: "a"},
		"short commit":  {RepositoryID: 1, CLNumber: 1, Patchset: 1, CommitID: []byte{1}, WorkflowsCommit: commit(), Context: []byte(`{}`), RequestedBy: "a"},
		"no context":    {RepositoryID: 1, CLNumber: 1, Patchset: 1, CommitID: commit(), WorkflowsCommit: commit(), Context: []byte(`{`), RequestedBy: "a"},
		"no one asking": {RepositoryID: 1, CLNumber: 1, Patchset: 1, CommitID: commit(), WorkflowsCommit: commit(), Context: []byte(`{}`)},
	} {
		if _, _, err := s.CreatePlan(ctx, req); !errors.As(err, &inv) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// Only automation plans.
	_, err := s.Start(ctx, store.StartRequest{RepositoryID: 1, CommitID: commit(), Job: store.Job{Kind: store.KindPlan},
		Origin: store.Origin{Phase: store.PhasePre, CLNumber: 1, Patchset: 1}, RequestedBy: "a"})
	if !errors.As(err, &inv) {
		t.Errorf("starting a planning run: %v", err)
	}
	// A plan's jobs each have an identity.
	plan(t, s, 1)
	r := lease(t, s, "w")
	dup := &store.PlanResult{Jobs: []store.PlannedJob{{Job: "build", Spec: build("//a:a")}, {Job: "build", Spec: build("//b:b")}}}
	if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", dup); !errors.As(err, &inv) {
		t.Errorf("a plan with a job twice: %v", err)
	}
}

// A repository's landing queue is keyed work, marked by what the queue acts on — a submission
// here — taken by one holder at a time with its mark cleared, and let go.
func testQueueWork(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	w := s.Work()
	submit(t, s, 7, store.SubmittedCL{Number: 1, Patchset: 1})
	due, err := w.Due(ctx, store.QueueWork, 10)
	if err != nil || len(due) != 1 || due[0] != 7 {
		t.Fatalf("due after a submission: %v, %v", due, err)
	}
	if ok, err := w.Take(ctx, store.QueueWork, 7, "a", time.Minute); err != nil || !ok {
		t.Fatalf("taking it: %v, %v", ok, err)
	}
	if ok, _ := w.Take(ctx, store.QueueWork, 7, "b", time.Minute); ok {
		t.Fatal("a second holder took it")
	}
	if due, _ := w.Due(ctx, store.QueueWork, 10); len(due) != 0 {
		t.Errorf("due while held: %v", due)
	}
	if again, err := w.Release(ctx, store.QueueWork, 7, "a"); err != nil || again {
		t.Errorf("releasing it: %v, %v", again, err)
	}
}

// A post run lost with its worker on its last attempt ends in error, and its repository's landing
// queue is marked to act on it, as it is for a post run that finishes.
func testPostRunLostMarksTheQueue(t *testing.T, e *env) {
	ctx := context.Background()
	cfg := store.DefaultConfig
	cfg.Heartbeat, cfg.Attempts = 100*time.Millisecond, 1
	s := e.newStore(t, cfg)
	sub := submit(t, s, 7, store.SubmittedCL{Number: 1, Patchset: 1})
	if _, err := s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: sub.ID, Base: commit(), BaseSeq: 1,
		Candidates: []store.NewCandidate{{SubmissionID: sub.ID, CLNumber: 1, Patchset: 1, Commit: commit(), Base: commit(), Own: true,
			WorkflowsCommit: commit(), Context: postContext(t, 1)}}}); err != nil {
		t.Fatal(err)
	}
	plan := lease(t, s, "w") // the attempt's planning run, post's
	if plan.Origin.Phase != store.PhasePost {
		t.Fatalf("leased %+v, want the post plan's run", plan)
	}
	// The queue has acted on everything so far.
	w := s.Work()
	if ok, err := w.Take(ctx, store.QueueWork, 7, "queue", time.Minute); err != nil || !ok {
		t.Fatalf("taking the queue's work: %v, %v", ok, err)
	}
	if _, err := w.Release(ctx, store.QueueWork, 7, "queue"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3*cfg.Heartbeat + 200*time.Millisecond)
	if err := s.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Get(ctx, plan.ID); r.Status != store.StatusError {
		t.Fatalf("the lost post run: %+v", r)
	}
	if due, err := w.Due(ctx, store.QueueWork, 10); err != nil || !slices.Contains(due, 7) {
		t.Errorf("the queue's work due after a post run was lost: %v, %v; want repository 7's", due, err)
	}
}

// Each repository's cursor is its own and never moves back.
func testFeedCursor(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	if c, err := s.Cursor(ctx, 1); err != nil || c != 0 {
		t.Fatalf("a new cursor: %d, %v", c, err)
	}
	for _, seq := range []int64{5, 9, 7} {
		if err := s.SetCursor(ctx, 1, seq); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SetCursor(ctx, 2, 3); err != nil {
		t.Fatal(err)
	}
	for repo, want := range map[int64]int64{1: 9, 2: 3, 3: 0} {
		if c, err := s.Cursor(ctx, repo); err != nil || c != want {
			t.Errorf("repository %d's cursor: %d, %v; want %d", repo, c, err, want)
		}
	}
}

// A CL abandoned has its unfinished pre runs canceled; restored, its pre resumes.
func testEndAndResumePre(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	plan(t, s, 1)
	r := lease(t, s, "w")
	if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", twoJobs); err != nil {
		t.Fatal(err)
	}
	running := lease(t, s, "w") // the build runs; the test stays queued

	if err := s.EndPre(ctx, 9, 4, "CL 4 was abandoned"); err != nil {
		t.Fatal(err)
	}
	p := pre(t, s, 1)
	if p.Verdict != store.VerdictFailed || p.Jobs[1].Status != store.StatusCanceled || p.Jobs[1].Reason != "CL 4 was abandoned" {
		t.Fatalf("after abandoning: %+v", p)
	}

	// Restored before the build's worker heard the stop: the build keeps running, and the
	// test is queued again.
	n, err := s.ResumePre(ctx, 9, 4, 1, "alice.human.example.com")
	if err != nil || n != 1 {
		t.Fatalf("resuming: %d, %v", n, err)
	}
	if stop, _, _ := s.Heartbeat(ctx, running.ID, "w"); stop != store.StopNone {
		t.Errorf("the running build is still told to stop: %d", stop)
	}
	p = pre(t, s, 1)
	if p.Verdict != store.VerdictPending || p.Jobs[1].Status != store.StatusQueued || p.Jobs[1].RerunOf == 0 || p.Jobs[1].Why != "the test is affected" {
		t.Fatalf("after restoring: %+v", p)
	}
	// Nothing canceled, nothing resumed.
	if n, err := s.ResumePre(ctx, 9, 4, 1, "alice.human.example.com"); err != nil || n != 0 {
		t.Errorf("resuming again: %d, %v", n, err)
	}

	// A CL abandoned while planning resumes its planning.
	plan(t, s, 2)
	if err := s.EndPre(ctx, 9, 4, "CL 4 was abandoned"); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ResumePre(ctx, 9, 4, 2, "alice.human.example.com"); err != nil || n != 1 {
		t.Fatalf("resuming the planning: %d, %v", n, err)
	}
	if p := pre(t, s, 2); p.Verdict != store.VerdictPending || p.PlanRun.Status != store.StatusQueued {
		t.Errorf("patchset 2 after restoring: %+v", p.PlanRun)
	}
}

const submitter = "alice.human.example.com"

func submit(t *testing.T, s *store.Store, repo int64, cls ...store.SubmittedCL) *store.Submission {
	t.Helper()
	sub, err := s.Submit(context.Background(), store.SubmitRequest{RepositoryID: repo, CLs: cls, SubmittedBy: submitter})
	if err != nil {
		t.Fatal(err)
	}
	return sub
}

func postContext(t *testing.T, cl int64) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"phase": "post", "cl": cl, "patchset": 1})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func testSubmit(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	var inv *store.InvalidError
	for name, req := range map[string]store.SubmitRequest{
		"no repository": {CLs: []store.SubmittedCL{{Number: 1, Patchset: 1}}, SubmittedBy: submitter},
		"no CLs":        {RepositoryID: 1, SubmittedBy: submitter},
		"a CL twice":    {RepositoryID: 1, CLs: []store.SubmittedCL{{Number: 1, Patchset: 1}, {Number: 1, Patchset: 1}}, SubmittedBy: submitter},
		"no patchset":   {RepositoryID: 1, CLs: []store.SubmittedCL{{Number: 1}}, SubmittedBy: submitter},
		"no one":        {RepositoryID: 1, CLs: []store.SubmittedCL{{Number: 1, Patchset: 1}}},
	} {
		if _, err := s.Submit(ctx, req); !errors.As(err, &inv) {
			t.Errorf("%s: %v", name, err)
		}
	}

	stack := submit(t, s, 1, store.SubmittedCL{Number: 5, Patchset: 2}, store.SubmittedCL{Number: 6, Patchset: 1})
	if stack.Status != store.SubmissionActive || len(stack.CLs) != 2 || stack.CLs[0].Number != 5 || stack.CLs[1].Patchset != 1 || stack.SubmittedBy != submitter {
		t.Fatalf("the stack: %+v", stack)
	}
	// A CL is in one active submission at a time.
	var conflict *store.ConflictError
	if _, err := s.Submit(ctx, store.SubmitRequest{RepositoryID: 1, CLs: []store.SubmittedCL{{Number: 6, Patchset: 1}}, SubmittedBy: submitter}); !errors.As(err, &conflict) ||
		!strings.Contains(conflict.Message, "CL 6 is already submitting") {
		t.Fatalf("submitting a CL twice: %v", err)
	}
	// Another repository's CL 6 is another CL.
	submit(t, s, 2, store.SubmittedCL{Number: 6, Patchset: 1})

	canceled, err := s.CancelSubmission(ctx, stack.ID, "not now", "prn_ada")
	if err != nil || canceled.Status != store.SubmissionCanceled || canceled.Reason != "not now" || canceled.CanceledBy != "prn_ada" || canceled.FinishedAt.IsZero() {
		t.Fatalf("canceling: %+v, %v", canceled, err)
	}
	again := submit(t, s, 1, store.SubmittedCL{Number: 6, Patchset: 2})
	if list, err := s.ListSubmissions(ctx, 1, 6, 0); err != nil || len(list) != 2 || list[0].ID != again.ID || list[1].ID != stack.ID {
		t.Fatalf("CL 6's submissions: %+v, %v", list, err)
	}
	if got, err := s.GetSubmissions(ctx, []int64{stack.ID, again.ID, 9999}); err != nil || len(got) != 2 || got[stack.ID].Status != store.SubmissionCanceled {
		t.Fatalf("getting submissions: %v, %v", got, err)
	}

	// A new patchset cancels the submissions of earlier ones; leaving review cancels every one.
	if n, err := s.CancelSubmissionsOf(ctx, 1, 6, 2, "patchset 2 was uploaded"); err != nil || n != 0 {
		t.Errorf("a patchset no older: %d, %v", n, err)
	}
	if n, err := s.CancelSubmissionsOf(ctx, 1, 6, 3, "patchset 3 was uploaded"); err != nil || n != 1 {
		t.Errorf("an older patchset: %d, %v", n, err)
	}
	if got, _ := s.GetSubmission(ctx, again.ID); got.Status != store.SubmissionCanceled || got.Reason != "patchset 3 was uploaded" {
		t.Errorf("after a new patchset: %+v", got)
	}
}

// An attempt's post: its planning runs go first, its jobs wait for the queue to release it,
// and it ends discarded or landed.
func testAttempts(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	pre := start(t, s, 1, build("//a:a")) // a run queued before, at the default priority
	sub := submit(t, s, 1, store.SubmittedCL{Number: 5, Patchset: 1})
	base, stone, cand := commit(), commit(), commit()
	id, err := s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: sub.ID, Base: base, BaseSeq: 7, Chain: []int64{99},
		Why: "CL 5 was submitted", Candidates: []store.NewCandidate{
			{SubmissionID: 99, CLNumber: 4, Patchset: 1, Commit: stone, Base: base},
			{SubmissionID: sub.ID, CLNumber: 5, Patchset: 1, Commit: cand, Base: stone, Own: true, WorkflowsCommit: base, Context: postContext(t, 5),
				Touched: []string{"lib/lib.go"}},
		}})
	if err != nil {
		t.Fatal(err)
	}

	// Post goes first: its planning run is leased before the older pre run.
	plan := lease(t, s, "w")
	if plan.Job.Kind != store.KindPlan || plan.Origin.Phase != store.PhasePost || plan.Origin.CLNumber != 5 || !bytes.Equal(plan.CommitID, cand) ||
		plan.RequestedBy != submitter || plan.Why != "CL 5 was submitted" {
		t.Fatalf("the planning run: %+v", plan)
	}
	if p, err := s.GetPlan(ctx, plan.PlanID); err != nil || !slices.Equal(p.Touched, []string{"lib/lib.go"}) {
		t.Errorf("the post plan's footprint: %v (%v)", p.Touched, err)
	}
	if err := s.ReleaseAttempt(ctx, id); err == nil {
		t.Fatal("an attempt was released before it was planned")
	}
	result := &store.PlanResult{
		Jobs:     []store.PlannedJob{{Job: "build", Spec: build("//lib:lib"), Why: "every target the change affects must build"}},
		Affected: []string{"//lib:lib"}, Upstream: []string{"//base:base"},
	}
	if err := s.Finish(ctx, plan.ID, "w", store.StatusPassed, 0, "", result); err != nil {
		t.Fatal(err)
	}
	// Its jobs wait: the pre run is next.
	if r := lease(t, s, "w"); r.ID != pre.ID {
		t.Fatalf("leased %+v, want the pre run", r)
	}
	attempts, err := s.Attempts(ctx, sub.ID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts: %v, %v", attempts, err)
	}
	a := attempts[0]
	own := a.Own()
	if a.BaseSeq != 7 || a.Released || len(a.Chain) != 1 || a.Chain[0] != 99 || len(a.Candidates) != 2 || len(own) != 1 ||
		!own[0].Plan.Planned || own[0].PlanRun.Status != store.StatusPassed || len(own[0].Jobs) != 0 ||
		own[0].Plan.Affected[0] != "//lib:lib" || own[0].Plan.Upstream[0] != "//base:base" || own[0].Plan.Jobs[0].Job != "build" {
		t.Fatalf("the planned attempt: %+v; %+v", a, own[0])
	}

	// Released, its jobs run on the candidate, ahead of pre.
	start(t, s, 1, build("//b:b"))
	if err := s.ReleaseAttempt(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseAttempt(ctx, id); err == nil {
		t.Error("an attempt was released twice")
	}
	job := lease(t, s, "w")
	if job.Origin.Phase != store.PhasePost || job.Origin.Job != "build" || !bytes.Equal(job.CommitID, cand) || job.PlanID != own[0].PlanID ||
		job.Why != "every target the change affects must build" {
		t.Fatalf("the post job: %+v", job)
	}
	// A post run is its submission's: no one cancels or reruns it alone.
	if _, err := s.Cancel(ctx, job.ID, "no", ""); err == nil {
		t.Error("a post run was canceled alone")
	}
	if err := s.Finish(ctx, job.ID, "w", store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Rerun(ctx, job.ID, submitter); err == nil {
		t.Error("a post run was rerun alone")
	}
	if _, err := s.Start(ctx, store.StartRequest{RepositoryID: 1, CommitID: commit(), Job: build("//a:a"),
		Origin: store.Origin{Phase: store.PhasePost, CLNumber: 5, Patchset: 1}, RequestedBy: submitter}); err == nil {
		t.Error("a post run was started by hand")
	}

	// In use, the attempt's candidates stay; reused on the same base.
	if inUse, err := s.CandidatesInUse(ctx, 1); err != nil || !inUse[string(stone)] || !inUse[string(cand)] {
		t.Errorf("candidates in use: %v, %v", inUse, err)
	}
	if c, ok, err := s.ReusableCandidate(ctx, 1, base, 4, 1); err != nil || !ok || !bytes.Equal(c, stone) {
		t.Errorf("a reusable candidate: %x, %v, %v", c, ok, err)
	}
	if _, ok, _ := s.ReusableCandidate(ctx, 1, base, 4, 2); ok {
		t.Error("another patchset's candidate was reused")
	}

	// Landed: the submission, the attempt, main's history, and each CL's commit.
	landed := commit()
	if err := s.RecordLanding(ctx, sub.ID, id, []store.LandedCL{{CLNumber: 5, Commit: landed}}, 7); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetSubmission(ctx, sub.ID)
	if got.Status != store.SubmissionLanded || !bytes.Equal(got.CLs[0].LandedCommit, landed) {
		t.Fatalf("the landed submission: %+v", got)
	}
	if inUse, _ := s.CandidatesInUse(ctx, 1); len(inUse) != 0 {
		t.Errorf("a landed attempt's candidates are still in use: %v", inUse)
	}
	st, err := s.QueueState(ctx, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Submissions) != 0 || len(st.Main) != 1 || st.Main[0].Seq != 8 || st.Main[0].SubmissionID != sub.ID ||
		st.Main[0].CLNumber != 5 || st.Others[sub.ID] == nil || st.Landings[sub.ID] == nil || st.Landings[sub.ID].ID != id {
		t.Errorf("the queue after landing: %+v", st)
	}
	if err := s.RecordLanding(ctx, sub.ID, id, nil, store.MainUnknown); err == nil {
		t.Error("a submission landed twice")
	}
}

// A repository's first landing is main's history from its first place, by its submission, and
// so is a landing whose place the queue does not know, whichever of it and the SCM's event of
// main's move is recorded first.
// An adopted landing and the SCM's event of it, recorded at once, take turns (LockMain): each
// waits while another transaction holds the repository's main, and either way main's commit has
// its submission — which, at once without the lock, each could miss.
// A stack's attempt plans post once, at its top (docs/automation.md, "post"): every CL of the
// submission is its own, and lands; the top alone is planned. A top with no plan, or a plan on
// another submission's candidate, is refused.
func testStackPlannedAtItsTop(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	sub := submit(t, s, 1, store.SubmittedCL{Number: 5, Patchset: 1}, store.SubmittedCL{Number: 6, Patchset: 2})
	base, low, top := commit(), commit(), commit()
	var inv *store.InvalidError
	for name, cands := range map[string][]store.NewCandidate{
		"an unplanned top": {
			{SubmissionID: sub.ID, CLNumber: 5, Patchset: 1, Commit: low, Base: base, Own: true},
			{SubmissionID: sub.ID, CLNumber: 6, Patchset: 2, Commit: top, Base: low, Own: true},
		},
		"a planned chain candidate": {
			{SubmissionID: 99, CLNumber: 4, Patchset: 1, Commit: low, Base: base, WorkflowsCommit: base, Context: postContext(t, 4)},
			{SubmissionID: sub.ID, CLNumber: 6, Patchset: 2, Commit: top, Base: low, Own: true, WorkflowsCommit: base, Context: postContext(t, 6)},
		},
	} {
		if _, err := s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: sub.ID, Base: base, Candidates: cands}); !errors.As(err, &inv) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: sub.ID, Base: base, Why: "the stack was submitted", Candidates: []store.NewCandidate{
		{SubmissionID: sub.ID, CLNumber: 5, Patchset: 1, Commit: low, Base: base, Own: true},
		{SubmissionID: sub.ID, CLNumber: 6, Patchset: 2, Commit: top, Base: low, Own: true, WorkflowsCommit: base, Context: postContext(t, 6),
			Touched: []string{"a/a.go", "b/b.go"}},
	}}); err != nil {
		t.Fatal(err)
	}
	plan := lease(t, s, "w")
	if plan.Job.Kind != store.KindPlan || plan.Origin.CLNumber != 6 || !bytes.Equal(plan.CommitID, top) {
		t.Fatalf("the planning run: %+v", plan)
	}
	if r, err := s.Lease(ctx, "w", "", 50*time.Millisecond); err != nil || r != nil {
		t.Fatalf("a second planning run: %+v, %v", r, err)
	}
	attempts, err := s.Attempts(ctx, sub.ID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts: %v, %v", attempts, err)
	}
	a := attempts[0]
	own, planned := a.Own(), a.Planned()
	if len(own) != 2 || own[0].CLNumber != 5 || own[0].PlanID != 0 || own[1].CLNumber != 6 || len(planned) != 1 || planned[0].CLNumber != 6 {
		t.Fatalf("own %+v, planned %+v", own, planned)
	}
	if p, err := s.GetPlan(ctx, planned[0].PlanID); err != nil || !slices.Equal(p.Touched, []string{"a/a.go", "b/b.go"}) {
		t.Errorf("the top's plan's footprint: %v (%v)", p.Touched, err)
	}
}

func testAdoptedLandingAtOnce(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	b := e.backends[s]
	adopt := func(repo int64) (*store.Submission, int64, []byte) {
		t.Helper()
		sub := submit(t, s, repo, store.SubmittedCL{Number: 1, Patchset: 1})
		id, err := s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: sub.ID, Base: commit(), BaseSeq: 0,
			Candidates: []store.NewCandidate{{SubmissionID: sub.ID, CLNumber: 1, Patchset: 1, Commit: commit(), Base: commit(), Own: true,
				WorkflowsCommit: commit(), Context: postContext(t, 1)}}})
		if err != nil {
			t.Fatal(err)
		}
		return sub, id, commit()
	}
	both := func(repo int64, sub *store.Submission, attempt int64, c []byte) (landing, event chan error) {
		landing, event = make(chan error, 1), make(chan error, 1)
		go func() {
			landing <- s.RecordLanding(ctx, sub.ID, attempt, []store.LandedCL{{CLNumber: 1, Commit: c}}, store.MainUnknown)
		}()
		go func() { event <- s.RecordMainCommit(ctx, repo, 1, c) }()
		return landing, event
	}
	submitted := func(repo int64) int64 {
		t.Helper()
		st, err := s.QueueState(ctx, repo, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(st.Main) != 1 {
			t.Fatalf("repository %d's main: %+v", repo, st.Main)
		}
		return st.Main[0].SubmissionID
	}

	// Each waits for a transaction holding repository 100's main.
	sub, attempt, c := adopt(100)
	held, release := make(chan struct{}), make(chan struct{})
	holder := make(chan error, 1)
	go func() {
		holder <- b.Txn(ctx, func(tx store.Ops) error {
			if err := tx.LockMain(ctx, 100); err != nil {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	landing, event := both(100, sub, attempt, c)
	select {
	case err := <-landing:
		close(release)
		t.Fatalf("the landing recorded while another held main: %v", err)
	case err := <-event:
		close(release)
		t.Fatalf("the event recorded while another held main: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	if err := errors.Join(<-holder, <-landing, <-event); err != nil {
		t.Fatal(err)
	}
	if got := submitted(100); got != sub.ID {
		t.Errorf("repository 100's main records submission %d, want %d", got, sub.ID)
	}

	// Many at once, each repository's main its submission's.
	var wg sync.WaitGroup
	subs := map[int64]int64{}
	for repo := int64(101); repo <= 140; repo++ {
		sub, attempt, c := adopt(repo)
		subs[repo] = sub.ID
		wg.Go(func() {
			landing, event := both(repo, sub, attempt, c)
			if err := errors.Join(<-landing, <-event); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	for repo, want := range subs {
		if got := submitted(repo); got != want {
			t.Errorf("repository %d's main records submission %d, want %d", repo, got, want)
		}
	}
}

func testFirstLanding(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	land := func(repo int64, fromSeq int64) (*store.Submission, []byte) {
		t.Helper()
		sub := submit(t, s, repo, store.SubmittedCL{Number: 1, Patchset: 1})
		cand := commit()
		id, err := s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: sub.ID, Base: commit(), BaseSeq: 0,
			Candidates: []store.NewCandidate{{SubmissionID: sub.ID, CLNumber: 1, Patchset: 1, Commit: cand, Base: commit(), Own: true,
				WorkflowsCommit: commit(), Context: postContext(t, 1)}}})
		if err != nil {
			t.Fatal(err)
		}
		landed := commit()
		if err := s.RecordLanding(ctx, sub.ID, id, []store.LandedCL{{CLNumber: 1, Commit: landed}}, fromSeq); err != nil {
			t.Fatal(err)
		}
		return sub, landed
	}
	mainAt := func(repo int64) []store.MainCommit {
		t.Helper()
		st, err := s.QueueState(ctx, repo, 0)
		if err != nil {
			t.Fatal(err)
		}
		return st.Main
	}
	check := func(what string, repo int64, sub *store.Submission, landed []byte) {
		t.Helper()
		m := mainAt(repo)
		if len(m) != 1 || m[0].Seq != 1 || !bytes.Equal(m[0].Commit, landed) || m[0].SubmissionID != sub.ID || m[0].CLNumber != 1 {
			t.Errorf("%s: main %+v, want the submission %d's CL 1 at 1", what, m, sub.ID)
		}
	}

	// The queue landed on a main with nothing on it, then the SCM's event came.
	sub, landed := land(1, 0)
	check("the first landing", 1, sub, landed)
	if err := s.RecordMainCommit(ctx, 1, 1, landed); err != nil {
		t.Fatal(err)
	}
	check("the first landing, its event recorded", 1, sub, landed)

	// Adopted, the queue not knowing where: the SCM's event recorded after it, then before it.
	sub, landed = land(2, store.MainUnknown)
	if m := mainAt(2); len(m) != 0 {
		t.Errorf("an adopted landing placed on main before its event: %+v", m)
	}
	if err := s.RecordMainCommit(ctx, 2, 1, landed); err != nil {
		t.Fatal(err)
	}
	check("an adopted landing, its event after", 2, sub, landed)
	early := commit()
	if err := s.RecordMainCommit(ctx, 3, 1, early); err != nil {
		t.Fatal(err)
	}
	sub3 := submit(t, s, 3, store.SubmittedCL{Number: 1, Patchset: 1})
	cand := commit()
	id, err := s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: sub3.ID, Base: commit(), BaseSeq: 0,
		Candidates: []store.NewCandidate{{SubmissionID: sub3.ID, CLNumber: 1, Patchset: 1, Commit: cand, Base: commit(), Own: true,
			WorkflowsCommit: commit(), Context: postContext(t, 1)}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordLanding(ctx, sub3.ID, id, []store.LandedCL{{CLNumber: 1, Commit: early}}, store.MainUnknown); err != nil {
		t.Fatal(err)
	}
	check("an adopted landing, its event before", 3, sub3, early)
}

func testDiscardAndFail(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	sub := submit(t, s, 1, store.SubmittedCL{Number: 5, Patchset: 1})
	cand := commit()
	attempt := func() int64 {
		t.Helper()
		id, err := s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: sub.ID, Base: commit(), BaseSeq: 1,
			Candidates: []store.NewCandidate{{SubmissionID: sub.ID, CLNumber: 5, Patchset: 1, Commit: cand, Base: commit(), Own: true,
				WorkflowsCommit: commit(), Context: postContext(t, 5)}}})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	first := attempt()
	running := lease(t, s, "w") // its planning run
	if err := s.DiscardAttempt(ctx, first, "CL 4, which it was tested on, did not land"); err != nil {
		t.Fatal(err)
	}
	if stop, reason, _ := s.Heartbeat(ctx, running.ID, "w"); stop != store.StopCanceled || reason != "CL 4, which it was tested on, did not land" {
		t.Errorf("the discarded attempt's running plan: %d %q", stop, reason)
	}
	second := attempt()
	if err := s.FailSubmission(ctx, sub.ID, "CL 5's test //a:test failed"); err != nil {
		t.Fatal(err)
	}
	attempts, _ := s.Attempts(ctx, sub.ID)
	if attempts[0].Status != store.AttemptDiscarded || attempts[1].ID != second || attempts[1].Status != store.AttemptFailed ||
		attempts[1].Own()[0].PlanRun.Status != store.StatusCanceled {
		t.Errorf("after failing: %+v %+v", attempts[0], attempts[1])
	}
	if got, _ := s.GetSubmission(ctx, sub.ID); got.Status != store.SubmissionFailed || got.Reason != "CL 5's test //a:test failed" {
		t.Errorf("the failed submission: %+v", got)
	}
	var conflict *store.ConflictError
	if _, err := s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: sub.ID, Base: commit(), BaseSeq: 1}); !errors.As(err, &conflict) {
		t.Errorf("an attempt of a failed submission: %v", err)
	}
}

// A submission has one active attempt at a time: a second, even from two queues at once, is
// refused, so its post never runs twice; once the first is discarded, another may be made.
func testOneActiveAttempt(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	sub := submit(t, s, 1, store.SubmittedCL{Number: 5, Patchset: 1})
	create := func() (int64, error) {
		return s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: sub.ID, Base: commit(), BaseSeq: 1,
			Candidates: []store.NewCandidate{{SubmissionID: sub.ID, CLNumber: 5, Patchset: 1, Commit: commit(), Base: commit(), Own: true,
				WorkflowsCommit: commit(), Context: postContext(t, 5)}}})
	}
	const queues = 8
	ids := make(chan int64, queues)
	errs := make(chan error, queues)
	var wg sync.WaitGroup
	for range queues {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := create()
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	var made []int64
	for id := range ids {
		made = append(made, id)
	}
	if len(made) != 1 {
		t.Fatalf("%d attempts made at once, want 1: %v", len(made), made)
	}
	for err := range errs {
		var conflict *store.ConflictError
		if !errors.As(err, &conflict) || !strings.Contains(err.Error(), "already has an active attempt") {
			t.Errorf("a second attempt: %v, want a conflict", err)
		}
	}
	if err := s.DiscardAttempt(ctx, made[0], "main moved"); err != nil {
		t.Fatal(err)
	}
	if _, err := create(); err != nil {
		t.Errorf("an attempt once the first was discarded: %v", err)
	}
}

// The queue's view: the active submissions in order, what their chains name, and main's
// history as the feed saw it.
func testQueueState(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	one := submit(t, s, 3, store.SubmittedCL{Number: 1, Patchset: 1})
	two := submit(t, s, 3, store.SubmittedCL{Number: 2, Patchset: 1}, store.SubmittedCL{Number: 3, Patchset: 1})
	if _, err := s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: two.ID, Base: commit(), BaseSeq: 4, Chain: []int64{one.ID}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelSubmission(ctx, one.ID, "no", ""); err != nil {
		t.Fatal(err)
	}
	c5, c6 := commit(), commit()
	for seq, c := range map[int64][]byte{5: c5, 6: c6} {
		if err := s.RecordMainCommit(ctx, 3, seq, c); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RecordLandedChange(ctx, 3, c6, 3); err != nil {
		t.Fatal(err)
	}
	st, err := s.QueueState(ctx, 3, 9)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Submissions) != 1 || st.Submissions[0].ID != two.ID || len(st.Submissions[0].Attempts) != 1 || st.From != 4 ||
		len(st.Main) != 2 || st.Main[1].CLNumber != 3 || st.Main[1].SubmissionID != 0 ||
		st.Others[one.ID] == nil || st.Others[one.ID].Status != store.SubmissionCanceled || !bytes.Equal(st.LandedCLs[3], c6) {
		t.Fatalf("the queue: %+v", st)
	}
	if due, err := s.Work().Due(ctx, store.QueueWork, 10); err != nil || !slices.Contains(due, 3) {
		t.Errorf("the queue's work due: %v, %v; want repository 3's", due, err)
	}
}

// Post carries a pass from pre for each job its plan says nothing changed for since pre: the
// same built-in job, or a workflow job that ran the same Bazel command. A job pre failed, never
// ran, or that something changed for since runs on the candidate.
func testCarryFromPre(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	test := func(target string, args ...string) store.Job {
		return store.Job{Kind: store.KindTest, Targets: []string{target}, Args: args}
	}

	// Pre on CL 4's patchset 1: everything passes but the flaky test.
	prePlan, preCommit := plan(t, s, 1)
	if err := s.Finish(ctx, lease(t, s, "w").ID, "w", store.StatusPassed, 0, "", &store.PlanResult{Jobs: []store.PlannedJob{
		{Job: "build", Spec: build("//lib:lib", "//app:app")},
		{Job: "test //lib:test", Spec: test("//lib:test")},
		{Job: "test //lib:flaky", Spec: test("//lib:flaky")},
		{Job: "lint", Spec: store.Job{Kind: store.KindLint}},
		{Job: "//.ok/workflow/ci.yaml:pre:check", Spec: test("//lib:check", "-v")},
	}}); err != nil {
		t.Fatal(err)
	}
	preRuns := map[string]int64{}
	for range 5 {
		r := lease(t, s, "w")
		status := store.StatusPassed
		if r.Origin.Job == "test //lib:flaky" {
			status = store.StatusFailed
		}
		if err := s.Finish(ctx, r.ID, "w", status, 0, "", nil); err != nil {
			t.Fatal(err)
		}
		preRuns[r.Origin.Job] = r.ID
	}

	sub := submit(t, s, 9, store.SubmittedCL{Number: 4, Patchset: 1})
	cand := commit()
	attempt, err := s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: sub.ID, Base: commit(), BaseSeq: 1,
		Candidates: []store.NewCandidate{{SubmissionID: sub.ID, CLNumber: 4, Patchset: 1, Commit: cand, Base: commit(), Own: true,
			WorkflowsCommit: commit(), Context: postContext(t, 4)}}})
	if err != nil {
		t.Fatal(err)
	}
	planning := lease(t, s, "w")
	if p, err := s.GetPlan(ctx, planning.PlanID); err != nil || !bytes.Equal(p.PreCommit, preCommit) || prePlan.PreCommit != nil {
		t.Fatalf("the post plan's pre commit: %x, want %x (%v)", p.PreCommit, preCommit, err)
	}
	const check = "//.ok/workflow/ci.yaml:post:check"
	if err := s.Finish(ctx, planning.ID, "w", store.StatusPassed, 0, "", &store.PlanResult{Jobs: []store.PlannedJob{
		{Job: "build", Spec: build("//lib:lib"), UnchangedSincePre: true},
		{Job: "build since pre", Spec: build("//app:app")},
		{Job: "test //lib:test", Spec: test("//lib:test"), UnchangedSincePre: true},
		{Job: "test //lib:flaky", Spec: test("//lib:flaky"), UnchangedSincePre: true},
		{Job: "test //lib:new", Spec: test("//lib:new"), UnchangedSincePre: true},
		{Job: "lint", Spec: store.Job{Kind: store.KindLint}},
		{Job: check, Spec: test("//lib:check", "-v"), UnchangedSincePre: true},
	}, Affected: []string{"//lib:lib"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}

	attempts, err := s.Attempts(ctx, sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	carried := map[string]int64{"build": preRuns["build"], "test //lib:test": preRuns["test //lib:test"], check: preRuns["//.ok/workflow/ci.yaml:pre:check"]}
	queued := 0
	for _, j := range attempts[0].Own()[0].Jobs {
		from, ok := carried[j.Origin.Job]
		switch {
		case ok && (j.Status != store.StatusPassed || j.CarriedFrom != from || !bytes.Equal(j.CommitID, cand) || !j.LogStored || j.Worker != ""):
			t.Errorf("carried %s: %+v, want a pass from run %d", j.Origin.Job, j, from)
		case !ok && (j.Status != store.StatusQueued || j.CarriedFrom != 0):
			t.Errorf("%s was not run: %+v", j.Origin.Job, j)
		case !ok:
			queued++
		}
	}
	if queued != 4 {
		t.Errorf("%d jobs queued, want 4", queued)
	}
	var log strings.Builder
	build := attempts[0].Own()[0].Jobs[0]
	if err := s.ReadLog(ctx, build.ID, 0, false, func(_ int64, data []byte, _ bool) error { log.Write(data); return nil }); err != nil ||
		!strings.Contains(log.String(), fmt.Sprintf("not run: it passed in pre as run %d, on patchset 1's commit %.10x", preRuns["build"], preCommit)) {
		t.Errorf("the carried build's log: %q (%v)", log.String(), err)
	}
	if st, reason, _ := attemptVerdict(t, s, sub.ID); st != "testing" {
		t.Errorf("the attempt with jobs left: %s %s", st, reason)
	}

	// A CL without pre has nothing to compare with.
	other := submit(t, s, 9, store.SubmittedCL{Number: 8, Patchset: 1})
	if _, err := s.CreateAttempt(ctx, store.NewAttempt{SubmissionID: other.ID, Base: commit(), BaseSeq: 1,
		Candidates: []store.NewCandidate{{SubmissionID: other.ID, CLNumber: 8, Patchset: 1, Commit: commit(), Base: commit(), Own: true,
			WorkflowsCommit: commit(), Context: postContext(t, 8)}}}); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		lease(t, s, "w")
	}
	if p, err := s.GetPlan(ctx, lease(t, s, "w").PlanID); err != nil || p.PreCommit != nil {
		t.Errorf("a post plan without pre: %+v, %v", p, err)
	}
}

// attemptVerdict says whether a submission's latest attempt's jobs are all finished.
func attemptVerdict(t *testing.T, s *store.Store, sub int64) (string, string, error) {
	t.Helper()
	attempts, err := s.Attempts(context.Background(), sub)
	if err != nil {
		return "", "", err
	}
	for _, j := range attempts[len(attempts)-1].Own()[0].Jobs {
		if !j.Status.Finished() {
			return "testing", j.Origin.Job, nil
		}
	}
	return "finished", "", nil
}

// A repository the directory purged is forgotten whole: its runs' logs — gathered, and a lease's
// segments — and its runs and submissions, however often it is purged; another repository keeps
// all of its own.
func testPurge(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	finished := start(t, s, 1, build("//a:b"))
	lease(t, s, "w")
	if _, err := s.AppendLog(ctx, finished.ID, "w", 0, []byte("done\n")); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, finished.ID, "w", store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	running := start(t, s, 1, build("//a:c"))
	lease(t, s, "w")
	if _, err := s.AppendLog(ctx, running.ID, "w", 0, []byte("going\n")); err != nil {
		t.Fatal(err)
	}
	other := start(t, s, 2, build("//a:b"))
	if _, err := s.Submit(ctx, store.SubmitRequest{RepositoryID: 1, CLs: []store.SubmittedCL{{Number: 1, Patchset: 1}}, SubmittedBy: submitter}); err != nil {
		t.Fatal(err)
	}
	objects := func(prefix string) int {
		t.Helper()
		os, err := e.blobs[s].List(ctx, prefix)
		if err != nil {
			t.Fatal(err)
		}
		return len(os)
	}
	if objects(fmt.Sprintf("runs/%d", finished.ID)) != 1 || objects(fmt.Sprintf("live/%d/", running.ID)) == 0 {
		t.Fatalf("the logs before the purge: %d gathered, %d segments", objects(fmt.Sprintf("runs/%d", finished.ID)), objects(fmt.Sprintf("live/%d/", running.ID)))
	}
	for i := 0; i < 2; i++ {
		if err := s.PurgeRepository(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []int64{finished.ID, running.ID} {
		if _, err := s.Get(ctx, id); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("run %d after the purge: %v", id, err)
		}
	}
	if n := objects(fmt.Sprintf("runs/%d", finished.ID)) + objects(fmt.Sprintf("live/%d/", running.ID)); n != 0 {
		t.Errorf("log objects after the purge: %d", n)
	}
	if subs, err := s.ListSubmissions(ctx, 1, 1, 10); err != nil || len(subs) != 0 {
		t.Errorf("submissions after the purge: %v, %v", subs, err)
	}
	if _, err := s.Get(ctx, other.ID); err != nil {
		t.Errorf("another repository's run: %v", err)
	}
	if err := s.SetDirectoryCursor(ctx, 1, 4); err != nil {
		t.Fatal(err)
	}
	if c, err := s.DirectoryCursor(ctx, 1); err != nil || c != 4 {
		t.Errorf("the directory's cursor = %d, %v", c, err)
	}
	if c, _ := s.Cursor(ctx, 1); c != 0 {
		t.Errorf("the SCM's cursor moved with the directory's: %d", c)
	}
}
