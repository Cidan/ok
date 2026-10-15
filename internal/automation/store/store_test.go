package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/testdb"
)

const self = "automation.ok.robot.example.com"

// newStore opens a store on a database of this test's own, as
// automation's principal, and its own bucket. Listen runs for the test's life.
func newStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s3 := testdb.S3(t)
	blobs, err := blob.NewS3(blob.Config{Endpoint: s3.Endpoint, Bucket: testdb.Bucket(t), AccessKey: s3.AccessKey, SecretKey: s3.SecretKey, Region: s3.Region})
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, pool, blobs, cfg)
	if err != nil {
		t.Fatal(err)
	}
	go s.Listen(ctx)
	return s
}

func commit() []byte {
	b := make([]byte, 32)
	rand.Read(b)
	return b
}

func build(targets ...string) Job { return Job{Kind: KindBuild, Targets: targets} }

func start(t *testing.T, s *Store, repo int64, job Job) *Run {
	t.Helper()
	r, err := s.Start(context.Background(), StartRequest{RepositoryID: repo, CommitID: commit(), Job: job,
		Origin: Origin{Phase: PhaseManual}, RequestedBy: "alice.human.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func lease(t *testing.T, s *Store, worker string) *Run {
	t.Helper()
	r, err := s.Lease(context.Background(), worker, 0)
	if err != nil || r == nil {
		t.Fatalf("lease for %s: %v, %v", worker, r, err)
	}
	return r
}

func TestStart(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	c := commit()
	r, err := s.Start(ctx, StartRequest{RepositoryID: 7, CommitID: c,
		Job:    Job{Kind: KindTest, Targets: []string{"//a:test"}, Args: []string{"-test.run", "X"}},
		Origin: Origin{Phase: PhasePre, CLNumber: 12, Patchset: 3, Job: "tests"}, RequestedBy: "alice.human.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusQueued || got.RepositoryID != 7 || !bytes.Equal(got.CommitID, c) || got.Attempt != 1 ||
		got.Origin != (Origin{Phase: PhasePre, CLNumber: 12, Patchset: 3, Job: "tests"}) ||
		!slices.Equal(got.Job.Args, []string{"-test.run", "X"}) || got.TimeoutSeconds != int32(DefaultConfig.DefaultTimeout/time.Second) {
		t.Errorf("run %+v", got)
	}
	if _, err := s.Get(ctx, r.ID+1000); !errors.Is(err, ErrNotFound) {
		t.Errorf("a missing run: %v", err)
	}
}

func TestStartRefuses(t *testing.T) {
	s := newStore(t, DefaultConfig)
	ok := StartRequest{RepositoryID: 1, CommitID: commit(), Job: build("//a:b"), Origin: Origin{Phase: PhaseManual}, RequestedBy: "a"}
	for _, tc := range []struct {
		name string
		edit func(*StartRequest)
	}{
		{"no repository", func(r *StartRequest) { r.RepositoryID = 0 }},
		{"a short commit", func(r *StartRequest) { r.CommitID = r.CommitID[:20] }},
		{"no requester", func(r *StartRequest) { r.RequestedBy = "" }},
		{"an unknown kind", func(r *StartRequest) { r.Job.Kind = 9 }},
		{"a build of nothing", func(r *StartRequest) { r.Job = build() }},
		{"a build with arguments", func(r *StartRequest) { r.Job.Args = []string{"x"} }},
		{"a label that is not one", func(r *StartRequest) { r.Job = build("a:b") }},
		{"arguments to two tests", func(r *StartRequest) {
			r.Job = Job{Kind: KindTest, Targets: []string{"//a:t", "//b:t"}, Args: []string{"-v"}}
		}},
		{"a run of two targets", func(r *StartRequest) { r.Job = Job{Kind: KindRun, Targets: []string{"//a:b", "//a:c"}} }},
		{"a lint with arguments", func(r *StartRequest) { r.Job = Job{Kind: KindLint, Args: []string{"-x"}} }},
		{"pre without its CL", func(r *StartRequest) { r.Origin = Origin{Phase: PhasePre} }},
		{"an unknown phase", func(r *StartRequest) { r.Origin = Origin{Phase: 7} }},
		{"a timeout past the most", func(r *StartRequest) { r.Timeout = 48 * time.Hour }},
	} {
		req := ok
		req.Job.Targets = slices.Clone(ok.Job.Targets)
		tc.edit(&req)
		var inv *InvalidError
		if _, err := s.Start(context.Background(), req); !errors.As(err, &inv) {
			t.Errorf("%s: %v, want invalid", tc.name, err)
		}
	}
}

// Workers take the oldest queued run first, and no run twice.
func TestLeaseIsFIFOAndExclusive(t *testing.T) {
	s := newStore(t, DefaultConfig)
	var want []int64
	for i := range 6 {
		want = append(want, start(t, s, 1, build(fmt.Sprintf("//a:%d", i))).ID)
	}
	first := lease(t, s, "w0")
	if first.ID != want[0] || first.Status != StatusRunning || first.Worker != "w0" || first.StartedAt.IsZero() {
		t.Fatalf("first lease: %+v, want run %d", first, want[0])
	}
	var mu sync.Mutex
	var got []int64
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.Lease(context.Background(), fmt.Sprintf("w%d", i+1), 0)
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
	slices.Sort(got)
	if !slices.Equal(got, want[1:]) {
		t.Errorf("leased %v, want each of %v once", got, want[1:])
	}
}

// A waiting worker is woken by a queued run's notification, well before the poll.
func TestLeaseWaits(t *testing.T) {
	cfg := DefaultConfig
	cfg.Poll = time.Minute
	s := newStore(t, cfg)
	time.Sleep(200 * time.Millisecond) // the listener is up
	got := make(chan *Run, 1)
	go func() {
		r, err := s.Lease(context.Background(), "w", 30*time.Second)
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
	if r, err := s.Lease(context.Background(), "w", 300*time.Millisecond); r != nil || err != nil {
		t.Errorf("an empty queue: %+v, %v", r, err)
	}
}

func TestHeartbeat(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	r := start(t, s, 1, build("//a:b"))
	lease(t, s, "w")
	if stop, reason, err := s.Heartbeat(ctx, r.ID, "w"); stop != StopNone || err != nil {
		t.Fatalf("a held lease: stop %v %q %v", stop, reason, err)
	}
	if stop, reason, _ := s.Heartbeat(ctx, r.ID, "other"); stop != StopLeaseLost || reason != ErrLeaseLost.Error() {
		t.Errorf("another worker's heartbeat: stop %v %q", stop, reason)
	}
	if _, err := s.Cancel(ctx, r.ID, "not needed"); err != nil {
		t.Fatal(err)
	}
	if stop, reason, _ := s.Heartbeat(ctx, r.ID, "w"); stop != StopCanceled || reason != "not needed" {
		t.Errorf("a canceled run: stop %v %q", stop, reason)
	}
	// A run past its timeout must stop.
	short, err := s.Start(ctx, StartRequest{RepositoryID: 1, CommitID: commit(), Job: build("//a:c"),
		Origin: Origin{Phase: PhaseManual}, RequestedBy: "a", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	lease(t, s, "w2")
	time.Sleep(1200 * time.Millisecond)
	if stop, reason, _ := s.Heartbeat(ctx, short.ID, "w2"); stop != StopTimedOut || !strings.Contains(reason, "timeout of 1s") {
		t.Errorf("an overdue run: stop %v %q", stop, reason)
	}
}

func TestLogAndFinish(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	r := start(t, s, 1, build("//a:b"))
	if _, err := s.AppendLog(ctx, r.ID, "w", 0, []byte("x")); !errors.Is(err, ErrLeaseLost) {
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
	var inv *InvalidError
	if _, err := s.AppendLog(ctx, r.ID, "w", 40, []byte("x")); !errors.As(err, &inv) {
		t.Errorf("a gap: %v", err)
	}
	if _, err := s.AppendLog(ctx, r.ID, "other", 17, []byte("x")); !errors.Is(err, ErrLeaseLost) {
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
	if err := s.Finish(ctx, r.ID, "other", StatusPassed, 0, "", nil); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("another worker's finish: %v", err)
	}
	if err := s.Finish(ctx, r.ID, "w", StatusFailed, 3, "bazel exited 3", nil); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != StatusFailed || done.ExitCode != 3 || done.Reason != "bazel exited 3" || !done.LogStored ||
		done.FinishedAt.IsZero() || done.LogSize != 17 {
		t.Errorf("finished run %+v", done)
	}
	var chunks int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM log_chunk WHERE run_id = $1`, r.ID).Scan(&chunks); err != nil || chunks != 0 {
		t.Errorf("chunks left behind: %d, %v", chunks, err)
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
	if _, err := s.AppendLog(ctx, r.ID, "w", 17, []byte("late")); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("a log after finishing: %v", err)
	}
}

// A log that reaches the most a run keeps ends with a note saying so, at exactly the most.
func TestLogFull(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfig
	cfg.MaxLog = 1 << 20
	s := newStore(t, cfg)
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
	if _, err := s.AppendLog(ctx, r.ID, "w", size, []byte("z")); !errors.Is(err, ErrLogFull) {
		t.Errorf("an append to a full log: %v", err)
	}
	if err := s.Finish(ctx, r.ID, "w", StatusPassed, 0, "", nil); err != nil {
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

// A follower sees what is written as it is written, and the end once the run finishes.
func TestReadLogFollows(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfig
	cfg.Poll = time.Minute
	s := newStore(t, cfg)
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
	if err := s.Finish(ctx, r.ID, "w", StatusPassed, 0, "", nil); err != nil {
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

func TestCancel(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	queued := start(t, s, 1, build("//a:b"))
	c, err := s.Cancel(ctx, queued.ID, "superseded")
	if err != nil || c.Status != StatusCanceled || c.Reason != "superseded" || !c.LogStored || c.FinishedAt.IsZero() {
		t.Fatalf("a queued run: %+v, %v", c, err)
	}
	if r, _ := s.Lease(ctx, "w", 0); r != nil {
		t.Errorf("a canceled run was leased: %+v", r)
	}
	running := start(t, s, 1, build("//a:c"))
	lease(t, s, "w")
	c, err = s.Cancel(ctx, running.ID, "")
	if err != nil || c.Status != StatusRunning {
		t.Fatalf("a running run is canceled by its worker: %+v, %v", c, err)
	}
	if stop, reason, _ := s.Heartbeat(ctx, running.ID, "w"); stop != StopCanceled || reason != "canceled" {
		t.Errorf("the worker is told: %v %q", stop, reason)
	}
	if err := s.Finish(ctx, running.ID, "w", StatusCanceled, 0, "canceled", nil); err != nil {
		t.Fatal(err)
	}
	c, err = s.Cancel(ctx, running.ID, "again")
	if err != nil || c.Status != StatusCanceled || c.Reason != "canceled" {
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
func TestSweepLapsedLeases(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	lapse := func(id int64) {
		if _, err := s.pool.Exec(ctx, `UPDATE run SET lease_expires = now() - interval '1 second' WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
	}
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
	if again.Status != StatusQueued || again.Attempt != 2 || again.Worker != "" || again.LogSize != 0 ||
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
	if lost.Status != StatusError || !strings.Contains(lost.Reason, "last attempt") || !lost.LogStored {
		t.Errorf("after two lapses: %+v", lost)
	}
	c := start(t, s, 1, build("//a:c"))
	lease(t, s, "w3")
	if _, err := s.Cancel(ctx, c.ID, "stop it"); err != nil {
		t.Fatal(err)
	}
	lapse(c.ID)
	if err := s.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get(ctx, c.ID); got.Status != StatusCanceled || got.Reason != "stop it" {
		t.Errorf("a canceled run's lapse: %+v", got)
	}
}

func TestRerun(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	r, err := s.Start(ctx, StartRequest{RepositoryID: 3, CommitID: commit(), Job: Job{Kind: KindLint, Targets: []string{"//a:a"}},
		Origin: Origin{Phase: PhasePre, CLNumber: 4, Patchset: 1, Job: "lint"}, RequestedBy: "alice.human.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	var conflict *ConflictError
	if _, err := s.Rerun(ctx, r.ID, "bob.human.example.com"); !errors.As(err, &conflict) {
		t.Errorf("a queued run's rerun: %v", err)
	}
	lease(t, s, "w")
	if err := s.Finish(ctx, r.ID, "w", StatusFailed, 1, "", nil); err != nil {
		t.Fatal(err)
	}
	again, err := s.Rerun(ctx, r.ID, "bob.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if again.RerunOf != r.ID || again.Status != StatusQueued || again.RequestedBy != "bob.human.example.com" ||
		!bytes.Equal(again.CommitID, r.CommitID) || again.Origin != r.Origin || !slices.Equal(again.Job.Targets, r.Job.Targets) {
		t.Errorf("the rerun %+v of %+v", again, r)
	}
}

func TestList(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	c := commit()
	var ids []int64
	for i := range 5 {
		r, err := s.Start(ctx, StartRequest{RepositoryID: 9, CommitID: c, Job: build(fmt.Sprintf("//a:%d", i)),
			Origin: Origin{Phase: PhasePre, CLNumber: 2, Patchset: int32(1 + i%2)}, RequestedBy: "a"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.ID)
	}
	start(t, s, 9, build("//other:x"))
	idsOf := func(rs []*Run) []int64 {
		var out []int64
		for _, r := range rs {
			out = append(out, r.ID)
		}
		return out
	}
	all, err := s.List(ctx, ListQuery{RepositoryID: 9, CLNumber: 2})
	if err != nil || !slices.Equal(idsOf(all), []int64{ids[4], ids[3], ids[2], ids[1], ids[0]}) {
		t.Errorf("a CL's runs: %v, %v", idsOf(all), err)
	}
	two, _ := s.List(ctx, ListQuery{RepositoryID: 9, CLNumber: 2, Patchset: 2})
	if !slices.Equal(idsOf(two), []int64{ids[3], ids[1]}) {
		t.Errorf("patchset 2's runs: %v", idsOf(two))
	}
	page, _ := s.List(ctx, ListQuery{RepositoryID: 9, CommitID: c, Limit: 2, BeforeID: ids[3]})
	if !slices.Equal(idsOf(page), []int64{ids[2], ids[1]}) {
		t.Errorf("a commit's runs, paged: %v", idsOf(page))
	}
	var inv *InvalidError
	if _, err := s.List(ctx, ListQuery{RepositoryID: 9}); !errors.As(err, &inv) {
		t.Errorf("neither a CL nor a commit: %v", err)
	}
}

// A released run goes back to the queue without spending an attempt, its log cleared; one
// canceled meanwhile ends canceled; only the lease's holder releases it.
func TestRelease(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	r := start(t, s, 1, build("//a:b"))
	lease(t, s, "w1")
	if _, err := s.AppendLog(ctx, r.ID, "w1", 0, []byte("half")); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, r.ID, "w2", ""); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("another worker's release: %v", err)
	}
	if err := s.Release(ctx, r.ID, "w1", "w1 is shutting down"); err != nil {
		t.Fatal(err)
	}
	back, _ := s.Get(ctx, r.ID)
	if back.Status != StatusQueued || back.Attempt != 1 || back.Worker != "" || back.LogSize != 0 || !back.StartedAt.IsZero() ||
		back.Reason != "w1 is shutting down" {
		t.Fatalf("released: %+v", back)
	}
	if again := lease(t, s, "w2"); again.ID != r.ID {
		t.Fatalf("the released run was not leased again: %+v", again)
	}
	if size, err := s.AppendLog(ctx, r.ID, "w2", 0, []byte("whole")); err != nil || size != 5 {
		t.Errorf("the new holder's log: %d, %v", size, err)
	}
	if _, err := s.Cancel(ctx, r.ID, "not wanted"); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, r.ID, "w2", ""); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get(ctx, r.ID); got.Status != StatusCanceled || got.Reason != "not wanted" {
		t.Errorf("a canceled run's release: %+v", got)
	}
}

// A push is a deployment's: it names one oci_push, and when it passes it records what it
// pushed, which Get and List carry; no one runs it again.
func TestPush(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	push := Job{Kind: KindPush, Targets: []string{"//web:push"}}
	deploy := Origin{Phase: PhaseDeploy, Job: "//.ok/workflow/deploy.yaml:deploy:web"}
	req := func(job Job, origin Origin) StartRequest {
		return StartRequest{RepositoryID: 1, CommitID: commit(), Job: job, Origin: origin, RequestedBy: "deploy.ok.robot.example.com"}
	}
	var inv *InvalidError
	for name, r := range map[string]StartRequest{
		"a push outside a deployment": req(push, Origin{Phase: PhaseManual}),
		"a deployment's build":        req(build("//web:web"), deploy),
		"a push of two":               req(Job{Kind: KindPush, Targets: []string{"//a:push", "//b:push"}}, deploy),
		"a push with arguments":       req(Job{Kind: KindPush, Targets: []string{"//a:push"}, Args: []string{"x"}}, deploy),
		"a deployment with a CL":      req(push, Origin{Phase: PhaseDeploy, Job: "d", CLNumber: 3}),
		"a deployment with no deploy": req(push, Origin{Phase: PhaseDeploy}),
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
	good := Pushed{Target: "//web:push", Registry: "registry.example.com", Repository: "apps/web",
		Digest: "sha256:" + strings.Repeat("ab", 32), Tags: []string{"latest", "c0ffee"}}
	for name, pushed := range map[string][]Pushed{
		"nothing pushed":      nil,
		"another target":      {{Target: "//other:push", Registry: good.Registry, Repository: good.Repository, Digest: good.Digest}},
		"a digest that isn't": {{Target: good.Target, Registry: good.Registry, Repository: good.Repository, Digest: "sha256:xyz"}},
		"two images":          {good, good},
	} {
		if err := s.Finish(ctx, r.ID, "w", StatusPassed, 0, "", nil, pushed...); !errors.As(err, &inv) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := s.Finish(ctx, r.ID, "w", StatusPassed, 0, "", nil, good); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Pushed) != 1 || got.Pushed[0].Digest != good.Digest || !slices.Equal(got.Pushed[0].Tags, good.Tags) {
		t.Errorf("Get: %+v", got.Pushed)
	}
	listed, err := s.List(ctx, ListQuery{RepositoryID: 1, CommitID: r.CommitID})
	if err != nil || len(listed) != 1 || len(listed[0].Pushed) != 1 {
		t.Errorf("List: %+v (%v)", listed, err)
	}
	var conflict *ConflictError
	if _, err := s.Rerun(ctx, r.ID, "alice.human.example.com"); !errors.As(err, &conflict) {
		t.Errorf("rerunning a push: %v", err)
	}
	// Only a push carries what it pushed.
	other := start(t, s, 1, build("//web:web"))
	lease(t, s, "w")
	if err := s.Finish(ctx, other.ID, "w", StatusPassed, 0, "", nil, good); !errors.As(err, &inv) {
		t.Errorf("a build carrying a pushed image: %v", err)
	}
}

// The build cache sweep's live commits: each repository's newest commits on main, and the
// commits of runs in flight or just finished.
func TestCommitsTheBuildCacheKeeps(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, DefaultConfig)
	var mains [][]byte
	for seq := range 6 {
		c := commit()
		mains = append(mains, c)
		if _, err := s.pool.Exec(ctx, `INSERT INTO main_commit (repository_id, seq, commit_id) VALUES (5, $1, $2)`, seq, c); err != nil {
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
	if _, err := s.pool.Exec(ctx, `UPDATE run SET status = $2, finished_at = now() - interval '3 hours' WHERE id = $1`, done.ID, StatusPassed); err != nil {
		t.Fatal(err)
	}
	runs, err := s.RunCommits(ctx, time.Now().Add(-2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(runs[6]) != 1 || !bytes.Equal(runs[6][0], queued.CommitID) {
		t.Fatalf("runs' commits: %x", runs[6])
	}
}
