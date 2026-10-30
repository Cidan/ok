package storetest

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/bus"
)

// strictQueue drops a job whose ID it was ever given — as NATS does within its window — so that no
// path of the store's relies on enqueueing a job's ID again: a job put off is a copy of its own
// (bus.PutOff). A job of no ID, as a reply to an inbox is, NATS drops none of.
type strictQueue struct {
	bus.Queue
	mu   sync.Mutex
	seen map[string]bool
}

func (q *strictQueue) Enqueue(ctx context.Context, jobs ...bus.Job) error {
	q.mu.Lock()
	var fresh []bus.Job
	for _, j := range jobs {
		if j.ID == "" {
			fresh = append(fresh, j)
			continue
		}
		if !q.seen[j.Queue+"\x00"+j.ID] {
			q.seen[j.Queue+"\x00"+j.ID] = true
			fresh = append(fresh, j)
		}
	}
	q.mu.Unlock()
	return q.Queue.Enqueue(ctx, fresh...)
}

// failingQueue is a dispatch queue whose enqueues fail while fail is set.
type failingQueue struct {
	bus.Queue
	fail atomic.Bool
}

func (q *failingQueue) Enqueue(ctx context.Context, jobs ...bus.Job) error {
	if q.fail.Load() {
		return errors.New("the bus is down")
	}
	return q.Queue.Enqueue(ctx, jobs...)
}

// dispatchJob is a job dispatching a run, as the store makes one: the run's ID and when it was made.
func dispatchJob(id, run int64) bus.Job {
	body := binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, uint64(run)), uint64(time.Now().UnixMilli()))
	return bus.Job{ID: "copy-" + strings.Repeat("0", int(id)), Queue: "automation.dispatch.default", Key: 1, Body: body}
}

// A run is queued only with its job: one whose job the bus refuses is not queued at all, its
// request failing. A job naming a run that is not there — of a transaction that did not commit —
// and a copy of a run's job other than the one that dispatches it are dropped where they are
// leased, holding up nothing.
func testDispatchWithTheRun(t *testing.T, e *env) {
	ctx := context.Background()
	failing := &failingQueue{Queue: e.queue}
	cfg := store.DefaultConfig
	cfg.Dispatch = failing
	s := e.newStore(t, cfg)

	failing.fail.Store(true)
	refused := commit()
	if _, err := s.Start(ctx, store.StartRequest{RepositoryID: 1, CommitID: refused, Job: build("//a:a"), Origin: store.Origin{Phase: store.PhaseManual},
		RequestedBy: "alice.human.example.com"}); err == nil {
		t.Fatal("a run was queued though its job was refused")
	}
	failing.fail.Store(false)
	if runs, err := s.List(ctx, store.ListQuery{RepositoryID: 1, CommitID: refused, Limit: 10}); err != nil || len(runs) != 0 {
		t.Fatalf("a run whose job was refused is recorded: %v, %v", runs, err)
	}

	r := start(t, s, 1, build("//a:b"))
	// A copy of its job, and a job of a run never recorded, both older than any transaction's commit.
	old := func(id, run int64) bus.Job {
		j := dispatchJob(id, run)
		j.Body = binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, uint64(run)), uint64(time.Now().Add(-2*time.Minute).UnixMilli()))
		return j
	}
	stale, orphan := old(1, r.ID), old(2, r.ID+1)
	if err := e.queue.Enqueue(ctx, stale, orphan); err != nil {
		t.Fatal(err)
	}
	got := lease(t, s, "w")
	if got.ID != r.ID {
		t.Fatalf("leased %d, want %d", got.ID, r.ID)
	}
	if again, err := s.Lease(ctx, "w2", "", 0); err != nil || again != nil {
		t.Fatalf("a stale copy or an orphan was leased: %v, %v", again, err)
	}
	if ls, err := e.queue.Lease(ctx, "automation.dispatch.default", bus.LeaseOptions{Max: 10, Visibility: time.Minute}); err != nil || len(ls) != 0 {
		t.Fatalf("the queue keeps %d jobs, %v; want the copy and the orphan dropped", len(ls), err)
	}
}

// A run's worker that stops heartbeating lets its job's delivery lapse: the bus delivers it again,
// and the lease that takes it takes the run over — its next attempt, with a fresh log. Lapsed
// again, on its last attempt, the run ends in error, its log stored by its log's job; a canceled
// run whose worker is lost ends canceled. Nothing but the bus's redelivery acts on a lapse.
func testLapsedLeases(t *testing.T, e *env) {
	ctx := context.Background()
	cfg := store.DefaultConfig
	cfg.Heartbeat = 100 * time.Millisecond
	s := e.newStore(t, cfg)
	lapse := func() { time.Sleep(3*cfg.Heartbeat + 200*time.Millisecond) }
	r := start(t, s, 1, build("//a:b"))
	lease(t, s, "w1")
	if _, err := s.AppendLog(ctx, r.ID, "w1", 0, []byte("partial")); err != nil {
		t.Fatal(err)
	}
	// Heartbeats keep it: past three of their spans, it is no one else's.
	for range 6 {
		time.Sleep(cfg.Heartbeat)
		if stop, _, err := s.Heartbeat(ctx, r.ID, "w1"); err != nil || stop != store.StopNone {
			t.Fatalf("a heartbeat: %v, %v", stop, err)
		}
	}
	if other, err := s.Lease(ctx, "w9", "", 0); err != nil || other != nil {
		t.Fatalf("a run its worker heartbeats was leased again: %+v, %v", other, err)
	}
	lapse()
	again := lease(t, s, "w2")
	if again.ID != r.ID || again.Attempt != 2 || again.Worker != "w2" || again.LogSize != 0 {
		t.Fatalf("after one lapse: %+v", again)
	}
	if stop, _, err := s.Heartbeat(ctx, r.ID, "w1"); err != nil || stop != store.StopLeaseLost {
		t.Errorf("the lost worker's heartbeat: %v, %v", stop, err)
	}
	if size, err := s.AppendLog(ctx, r.ID, "w2", 0, []byte("fresh")); err != nil || size != 5 {
		t.Errorf("the second attempt's log: %d, %v", size, err)
	}
	lapse()
	if other, err := s.Lease(ctx, "w3", "", time.Second); err != nil || other != nil {
		t.Fatalf("a run lapsed on its last attempt was leased: %+v, %v", other, err)
	}
	if err := s.StoreReadyLogs(ctx); err != nil {
		t.Fatal(err)
	}
	lost, _ := s.Get(ctx, r.ID)
	if lost.Status != store.StatusError || !strings.Contains(lost.Reason, "last attempt") || !lost.LogStored {
		t.Errorf("after two lapses: %+v", lost)
	}
	var log strings.Builder
	if err := s.ReadLog(ctx, r.ID, 0, false, func(_ int64, d []byte, _ bool) error { log.Write(d); return nil }); err != nil || log.String() != "fresh" {
		t.Errorf("the lost run's log: %q, %v", log.String(), err)
	}
	if live, err := e.blobs[s].List(ctx, "live/"); err != nil || len(live) != 0 {
		t.Errorf("segments left behind: %v, %v", live, err)
	}
	c := start(t, s, 1, build("//a:c"))
	lease(t, s, "w4")
	if _, err := s.Cancel(ctx, c.ID, "stop it", ""); err != nil {
		t.Fatal(err)
	}
	lapse()
	if other, err := s.Lease(ctx, "w5", "", time.Second); err != nil || other != nil {
		t.Fatalf("a canceled run whose worker was lost was leased: %+v, %v", other, err)
	}
	if got, _ := s.Get(ctx, c.ID); got.Status != store.StatusCanceled || got.Reason != "stop it" {
		t.Errorf("a canceled run's lapse: %+v", got)
	}
}

// A job of a run queued again — released by its worker, or its lease lapsed — is enqueued before the
// transaction that names it commits: leased first, while the run still runs under its old job, it
// is put off, not dropped as a stale copy, and dispatches the run once the transaction commits. The
// run has no other job: dropped, it was queued for good with none.
func testRequeueJobBeforeItsCommit(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	r := start(t, s, 1, build("//a:b"))
	lease(t, s, "w1")
	// The requeue's job, leased before its transaction commits.
	next := dispatchJob(3, r.ID)
	if err := e.queue.Enqueue(ctx, next); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Lease(ctx, "w2", "", 0); err != nil || got != nil {
		t.Fatalf("a job its run does not name yet was leased: %+v, %v", got, err)
	}
	// The transaction commits.
	if err := e.backends[s].Txn(ctx, func(tx store.Ops) error {
		return tx.RequeueRun(ctx, r.ID, next.ID, "released by its worker", false)
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Lease(ctx, "w2", "", 5*time.Second)
	if err != nil || got == nil || got.ID != r.ID || got.Worker != "w2" {
		t.Fatalf("the requeued run, once its transaction committed: %+v, %v", got, err)
	}
}

// A run released by its worker is dispatched again by a job of its own, and leased by the next
// worker at once; the delivery it ran under, acknowledged, never comes back.
func testReleaseDispatches(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	r := start(t, s, 1, build("//a:b"))
	first := lease(t, s, "w1")
	if err := s.Release(ctx, first.ID, "w1", "shutting down"); err != nil {
		t.Fatal(err)
	}
	next := lease(t, s, "w2")
	if next.ID != r.ID || next.Attempt != 1 || next.Worker != "w2" {
		t.Fatalf("the released run: %+v", next)
	}
	if err := s.Finish(ctx, next.ID, "w2", store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	if other, err := s.Lease(ctx, "w3", "", time.Second); err != nil || other != nil {
		t.Fatalf("a finished run's delivery came back: %+v, %v", other, err)
	}
}

// Queued runs an older release recorded, which no job dispatches, are dispatched as a replica of
// this one starts.
func testDispatchUndispatched(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	old, err := e.backends[s].InsertRun(ctx, store.NewRun{RepositoryID: 1, CommitID: commit(), Job: build("//a:old"),
		Origin: store.Origin{Phase: store.PhaseManual}, RequestedBy: "alice.human.example.com", Status: store.StatusQueued, TimeoutSeconds: 60})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Lease(ctx, "w", "", 0); err != nil || got != nil {
		t.Fatalf("an undispatched run was leased before the replica started: %+v, %v", got, err)
	}
	if err := s.Started(ctx); err != nil {
		t.Fatal(err)
	}
	if got := lease(t, s, "w"); got.ID != old.ID {
		t.Fatalf("leased %d, want the older release's run %d", got.ID, old.ID)
	}
}
