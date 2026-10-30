package store

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/workflow"
)

// Dispatch (docs/hosted.md, "Hosted automation, as built"): every queued run is a job on its pool's
// dispatch queue on the bus, enqueued before the transaction that queued it commits, and a worker's
// lease takes the next job and claims the run it names, a transaction no other claim meets — the
// queue hands each job to one consumer, so workers added to a pool add as many consumers, with
// nothing they all contend on. Runs are taken in the order they were queued.
//
// The bus's lease on the job's delivery is the run's lease while it runs: the run records the
// delivery's handle, each heartbeat extends it through whichever replica it reaches, and the
// run's outcome acknowledges it. A worker that stops heartbeating lets the delivery lapse, and
// the bus delivers the job again: the run is taken over by the worker whose lease takes it —
// another attempt, or, past its attempts, an error. Nothing sweeps the database for what lapsed.
//
// A job is a pointer, and the run's row the truth. Each run names the one job that dispatches it
// now: a delivery of any other — a copy of an attempt that did not commit, a job a release
// replaced — is acknowledged and dropped where it is leased, as is the job of a run that is no
// longer queued, or running under it. A job whose run is not there yet, or names another job — its
// transaction, which queued the run or queued it again, still committing — is returned to be
// delivered again, until it is older than notThereFor: a release's or a takeover's job is enqueued
// before the transaction that names it commits, and dropped as a stale copy, the run, which no
// other job dispatches, was queued for good with none. An account
// at its concurrent quota, or past its month's seconds, has its run's job enqueued again a while
// later (Config.QuotaWait, MonthWait), holding up no other account's: the queue's own schedule, not
// a sweep, brings it back.

// notThereFor is how long a job may name a run that is not there before it is dropped: its
// transaction did not commit.
const notThereFor = time.Minute

// dispatchVisibility is how long a leased job stays invisible while its run is claimed: the
// claim's heartbeats extend it from then on.
const dispatchVisibility = 30 * time.Second

// dispatchQueue is a pool's queue.
func dispatchQueue(pool string) string { return "automation.dispatch." + poolName(pool) }

// poolName is a pool as dispatch names it: "" is the default one.
func poolName(p string) string {
	if p == "" {
		return workflow.DefaultPool
	}
	return p
}

var (
	dispatched = metrics.NewCounter("ok_automation_dispatched_total",
		"Jobs enqueued to dispatch runs, by why: queued, a quota's wait, or a move's arrival.", "why")
	claims = metrics.NewCounter("ok_automation_claims_total",
		"Leased jobs, by outcome: claimed, taken over (its worker lost), ended (lost past its attempts, or canceled), gone (its run not queued under it), not there (yet), waiting (its account at its quota), moving (its repository frozen), or failed.", "outcome")
)

// dispatchJob is a job dispatching a run: its ID, which the run names, and what it is about.
type dispatchJob struct {
	id   string
	run  int64
	repo int64
	pool string
}

// newDispatchJob is a fresh job's ID.
func newDispatchJob() string {
	var b [12]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// jobBody is a dispatch job's body: its run's ID, and when the job was made (Unix milliseconds).
func jobBody(run int64, at time.Time) []byte {
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, uint64(run)), uint64(at.UnixMilli()))
}

func parseJobBody(b []byte) (run int64, at time.Time, ok bool) {
	if len(b) != 16 {
		return 0, time.Time{}, false
	}
	return int64(binary.BigEndian.Uint64(b)), time.UnixMilli(int64(binary.BigEndian.Uint64(b[8:]))), true
}

// busJob is a dispatch job as the bus carries it, due at notBefore (zero: now).
func busJob(d dispatchJob, notBefore time.Time) bus.Job {
	return bus.Job{ID: d.id, Queue: dispatchQueue(d.pool), Key: d.repo, Body: jobBody(d.run, time.Now()), NotBefore: notBefore}
}

// enqueue enqueues the jobs dispatching runs.
func (s *Store) enqueue(ctx context.Context, ds []dispatchJob) error {
	if len(ds) == 0 {
		return nil
	}
	jobs := make([]bus.Job, len(ds))
	for i, d := range ds {
		jobs[i] = busJob(d, time.Time{})
	}
	if err := s.queue.Enqueue(ctx, jobs...); err != nil {
		return fmt.Errorf("dispatching runs: %w", err)
	}
	dispatched.With("queued").Add(uint64(len(ds)))
	return nil
}

// acknowledge acknowledges deliveries runs ran under, now that their outcomes are recorded. One it
// misses is delivered again, and dropped: its run no longer runs under it.
func (s *Store) acknowledge(ctx context.Context, handles [][]byte) {
	for _, h := range handles {
		l, err := s.queue.Resume(h)
		if err == nil {
			err = l.Ack(context.WithoutCancel(ctx))
		}
		if err != nil && !errors.Is(err, bus.ErrLeaseLost) {
			slog.Warn("automation: acknowledging a run's delivery", "err", err)
		}
	}
}

// extend keeps a run's delivery leased for ttl more, through whichever replica the heartbeat
// reached. A failure is logged: the delivery lapses, if it does, and the run is taken over.
func (s *Store) extend(ctx context.Context, handle []byte, ttl time.Duration) {
	if len(handle) == 0 {
		return
	}
	l, err := s.queue.Resume(handle)
	if err == nil {
		err = l.Extend(ctx, ttl)
	}
	if err != nil && !errors.Is(err, bus.ErrLeaseLost) {
		slog.Warn("automation: extending a run's delivery", "err", err)
	}
}

// Lease assigns a queued run of a pool to a worker: the next job of the pool's queue — waiting up
// to wait for one — whose run it claims; a job whose run it cannot claim is dropped, or put off,
// and the next taken, until none is ready. It returns nil when none came.
func (s *Store) Lease(ctx context.Context, worker, pool string, wait time.Duration) (*Run, error) {
	if worker == "" {
		return nil, invalid("a worker names itself")
	}
	pool = poolName(pool)
	deadline := time.Now().Add(wait)
	for {
		ls, err := s.queue.Lease(ctx, dispatchQueue(pool), bus.LeaseOptions{Max: 1, Visibility: dispatchVisibility, Wait: max(time.Until(deadline), 0)})
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("leasing from %s: %w", dispatchQueue(pool), err)
		}
		if len(ls) == 0 {
			return nil, nil
		}
		run, err := s.claimJob(ctx, ls[0], worker, pool)
		if err != nil || run != nil {
			return run, err
		}
	}
}

// claimJob claims the run a leased job names for a worker, under the job's delivery: the run
// queued, or one whose worker was lost, running. Nil when the job is a stale copy, its run not
// there yet, its account at its quota, or its repository frozen; on an error the job is returned,
// for another lease.
func (s *Store) claimJob(ctx context.Context, l bus.Leased, worker, pool string) (*Run, error) {
	drop := func(outcome string) {
		claims.With(outcome).Inc()
		if err := l.Ack(context.WithoutCancel(ctx)); err != nil && !errors.Is(err, bus.ErrLeaseLost) {
			slog.Warn("automation: acknowledging a dispatch job", "job", l.Job().ID, "err", err)
		}
	}
	later := func(outcome string, delay time.Duration) {
		claims.With(outcome).Inc()
		if err := bus.PutOff(ctx, s.queue, l, delay); err != nil {
			slog.Warn("automation: putting a dispatch job off", "job", l.Job().ID, "err", err)
		}
	}
	fail := func(err error) (*Run, error) {
		later("failed", bus.Backoff(bus.Copies(l.Job().ID), time.Second, time.Minute))
		return nil, err
	}
	id, made, ok := parseJobBody(l.Job().Body)
	if !ok {
		drop("gone")
		return nil, nil
	}
	ctx = move.Untagged(ctx)
	run, err := s.b.Run(ctx, id)
	switch {
	case errors.Is(err, ErrNotFound) && time.Since(made) < notThereFor:
		// Its transaction commits in moments: looked at again soon, and less often as it waits.
		later("not_there", bus.Backoff(bus.Copies(l.Job().ID), 20*time.Millisecond, 2*time.Second))
		return nil, nil
	case errors.Is(err, ErrNotFound):
		drop("gone")
		return nil, nil
	case err != nil:
		return fail(err)
	case poolName(run.Job.Pool) != pool:
		drop("gone")
		return nil, nil
	case run.DispatchJob != bus.JobBase(l.Job().ID) && time.Since(made) < notThereFor:
		// Its run queued again under it in a transaction still committing, or a stale copy, dropped
		// once older than any commit.
		later("not_yet", bus.Backoff(bus.Copies(l.Job().ID), 20*time.Millisecond, 2*time.Second))
		return nil, nil
	case run.DispatchJob != bus.JobBase(l.Job().ID):
		drop("gone")
		return nil, nil
	case run.Status == StatusRunning:
		return s.takeOver(ctx, l, run, worker, pool)
	case run.Status != StatusQueued:
		drop("gone")
		return nil, nil
	}
	account, err := s.b.OwnerOf(ctx, run.RepositoryID)
	if err != nil {
		return fail(err)
	}
	quotas, err := s.quotas(ctx, []string{account})
	if err != nil {
		return fail(err)
	}
	q := quotas[account]
	for tries := 0; ; tries++ {
		slot, admitted, wait, err := s.admit(ctx, account, q)
		if err != nil {
			return fail(err)
		}
		if !admitted {
			s.wait(ctx, l, run, wait)
			return nil, nil
		}
		claimed, err := s.claimRun(ctx, id, bus.JobBase(l.Job().ID), pool, worker, account, slot, l.Handle())
		switch {
		case errors.Is(err, ErrSlotTaken) && tries+1 < slotTries:
			continue // another claim took the slot first: another
		case errors.Is(err, ErrSlotTaken):
			s.wait(ctx, l, run, s.cfg.QuotaWait)
			return nil, nil
		case errors.Is(err, errGiveBack):
			// Dispatched again where its repository arrives, or here once the move is called off.
			drop("moving")
			return nil, nil
		case err != nil:
			return fail(err)
		case claimed == nil:
			drop("gone")
			return nil, nil
		}
		claims.With("claimed").Inc()
		s.extend(ctx, l.Handle(), s.leaseTTL())
		return claimed, nil
	}
}

// wait puts a run off while its account is at its quota: a copy of its job, due after delay
// (bus.PutOff).
func (s *Store) wait(ctx context.Context, l bus.Leased, run *Run, delay time.Duration) {
	dispatched.With("quota").Inc()
	claims.With("waiting").Inc()
	if err := bus.PutOff(ctx, s.queue, l, delay); err != nil {
		slog.Warn("automation: putting a dispatch job off", "job", l.Job().ID, "run", run.ID, "err", err)
	}
}

// takeOver acts on a running run whose job was delivered again: its worker let the delivery
// lapse. A run its worker was asked to stop ends canceled; one with an attempt left is queued
// again under the same job, and claimed for this lease's worker as any; one on its last attempt
// ends in error.
func (s *Store) takeOver(ctx context.Context, l bus.Leased, run *Run, worker, pool string) (*Run, error) {
	var requeued, gone bool
	err := s.txn(ctx, func(tx Ops) error {
		requeued, gone = false, false
		cur, err := tx.LockRun(ctx, run.ID)
		if err != nil {
			return err
		}
		if cur.Status != StatusRunning || cur.DispatchJob != bus.JobBase(l.Job().ID) {
			gone = true
			return nil
		}
		stopping, stopReason, err := tx.StopRequest(ctx, run.ID)
		if err != nil {
			return err
		}
		switch {
		case stopping:
			if err := tx.FinishRun(ctx, run.ID, StatusCanceled, 0, stopReason); err != nil {
				return err
			}
		case cur.Attempt < s.cfg.Attempts:
			// The lost attempt's log goes; the next writes its own from the start.
			requeued = true
			t := tx.(txOps)
			t.touched.runs[run.ID] = true
			return t.Ops.RequeueRun(ctx, run.ID, bus.JobBase(l.Job().ID), "its worker "+cur.Worker+" was lost; it runs again", true)
		default:
			if err := tx.FinishRun(ctx, run.ID, StatusError, 0, "its worker "+cur.Worker+" was lost, on its last attempt"); err != nil {
				return err
			}
		}
		if err := finishedPost(ctx, tx, cur.RepositoryID, cur.Origin.Phase); err != nil {
			return err
		}
		return changed(ctx, tx, run.ID)
	})
	switch {
	case move.IsFrozen(err) || moved(err):
		claims.With("moving").Inc()
		_ = l.Ack(context.WithoutCancel(ctx))
		return nil, nil
	case err != nil:
		claims.With("failed").Inc()
		if perr := bus.PutOff(ctx, s.queue, l, bus.Backoff(bus.Copies(l.Job().ID), time.Second, time.Minute)); perr != nil {
			slog.Warn("automation: putting a dispatch job off", "job", l.Job().ID, "err", perr)
		}
		return nil, err
	case gone:
		claims.With("gone").Inc()
		_ = l.Ack(context.WithoutCancel(ctx))
		return nil, nil
	case !requeued:
		claims.With("ended").Inc()
		_ = l.Ack(context.WithoutCancel(ctx))
		return nil, nil
	}
	claims.With("taken_over").Inc()
	return s.claimJob(ctx, l, worker, pool)
}

// admit is whether an account may start another run, the slot of its it would hold (-1 for an
// account with no concurrent quota), and, refused, how long its run waits.
func (s *Store) admit(ctx context.Context, account string, q Quota) (int64, bool, time.Duration, error) {
	if q.Seconds > 0 {
		seconds, err := s.b.MonthSeconds(ctx, MonthOf(time.Now()), []string{account})
		if err != nil {
			return 0, false, 0, err
		}
		if seconds[account]+q.SecondsElsewhere >= q.Seconds {
			return 0, false, s.cfg.MonthWait, nil
		}
	}
	if q.Concurrent <= 0 {
		return -1, true, 0, nil
	}
	slot, ok, err := s.freeSlot(ctx, account, q)
	return slot, ok, s.cfg.QuotaWait, err
}

// dispatchRuns dispatches queued runs, each with a job of its own.
func (s *Store) dispatchRuns(ctx context.Context, qs []Queued, why string) error {
	if len(qs) == 0 {
		return nil
	}
	ds := make([]dispatchJob, len(qs))
	jobs := make(map[int64]string, len(qs))
	for i, q := range qs {
		ds[i] = dispatchJob{id: newDispatchJob(), run: q.ID, repo: q.RepositoryID, pool: q.Pool}
		jobs[q.ID] = ds[i].id
	}
	err := s.b.Txn(ctx, func(tx Ops) error {
		if err := tx.SetDispatchJobs(ctx, jobs); err != nil {
			return err
		}
		return s.enqueue(ctx, ds)
	})
	if err == nil {
		dispatched.With(why).Add(uint64(len(ds)))
	}
	return err
}

// dispatchQueued dispatches repositories' queued runs: they arrived here, or a move of theirs was
// called off.
func (s *Store) dispatchQueued(ctx context.Context, repos []int64) error {
	qs, err := s.b.QueuedRuns(ctx, repos)
	if err != nil {
		return err
	}
	return s.dispatchRuns(ctx, qs, "arrived")
}

// DispatchUndispatched dispatches the queued runs an older release queued, which no job names:
// once, as a replica starts.
func (s *Store) DispatchUndispatched(ctx context.Context) error {
	qs, err := s.b.UndispatchedRuns(ctx)
	if err != nil {
		return err
	}
	return s.dispatchRuns(ctx, qs, "upgraded")
}
