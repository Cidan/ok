package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/wait"
)

// A run's log is in the logs bucket (automation.logs), never in the database. While a run is
// leased, each append is an object of its own, a segment, under the lease:
// live/<run>/<claim>/<start>, claim the microsecond the lease began and start the segment's offset
// in the log, in hex. The run's log_size says how much of what is there is the log: an append
// writes its segment, then moves log_size past it — set only if the lease and the size are as the
// append read them — so a reader that sees a size finds every segment before it. Once the run
// finishes, its log is gathered into one object, runs/<run>, and its segments deleted.
//
// A segment is written only where the log ends, and only if nothing is there (blob.IfAbsent). The
// worker sends the same bytes at an offset however often it sends them, so a segment written by a
// request that could not then move the size — it lost a race with its own resend — still holds the
// log's bytes, and the next append at its offset takes it as it is.

// logKey is a finished run's log object in the bucket.
func logKey(id int64) string { return "runs/" + strconv.FormatInt(id, 10) }

// liveDir holds every segment of a run's leases.
func liveDir(id int64) string { return "live/" + strconv.FormatInt(id, 10) + "/" }

// leaseDir holds the segments of a run's current lease.
func leaseDir(run *Run) string {
	return liveDir(run.ID) + strconv.FormatInt(run.StartedAt.UnixMicro(), 10) + "/"
}

// segmentKey is the segment of a run's current lease that starts at start.
func segmentKey(run *Run, start int64) string { return leaseDir(run) + fmt.Sprintf("%016x", start) }

// truncatedNote ends a log that reached the most a run keeps.
func (s *Store) truncatedNote() string {
	return fmt.Sprintf("\n[ok: the log stops here, at the %d MiB a run keeps]\n", s.cfg.MaxLog>>20)
}

// carriedNote is the log of a run that never ran: it carries run from's pass in pre, on a
// patchset's commit.
func carriedNote(from int64, patchset int32, commit []byte) string {
	return fmt.Sprintf("ok: not run: it passed in pre as run %d, on patchset %d's commit %.10x, and nothing it reads differs on this candidate\n",
		from, patchset, commit)
}

// carriedLog is a carried run's log, made from the run it carries, as when it was recorded.
func (s *Store) carriedLog(ctx context.Context, run *Run) ([]byte, error) {
	from, err := s.b.Run(ctx, run.CarriedFrom)
	if err != nil {
		return nil, fmt.Errorf("run %d's carried pass, run %d: %w", run.ID, run.CarriedFrom, err)
	}
	return []byte(carriedNote(from.ID, run.Origin.Patchset, from.CommitID)), nil
}

// appendTries bounds how often an append looks again at a log that moved under it.
const appendTries = 64

// AppendLog adds data at offset to a leased run's log and returns the log's size. A
// request resent after a lost answer is taken once (the bytes it repeats are skipped), and
// one past the end is refused. A log that reaches the most a run keeps ends with a note
// saying so, and refuses more with ErrLogFull.
func (s *Store) AppendLog(ctx context.Context, id int64, worker string, offset int64, data []byte) (int64, error) {
	if offset < 0 {
		return 0, invalid("a log offset is not negative")
	}
	for range appendTries {
		run, err := s.b.Run(ctx, id)
		if errors.Is(err, ErrNotFound) {
			return 0, ErrLeaseLost
		}
		if err != nil {
			return 0, err
		}
		if run.Status != StatusRunning || run.Worker != worker {
			return 0, ErrLeaseLost
		}
		size := run.LogSize
		if offset > size {
			return 0, invalid("the log is %d bytes; a write at %d leaves a gap", size, offset)
		}
		// Only what extends the log is new.
		rest := data[min(int64(len(data)), size-offset):]
		if len(rest) == 0 {
			return size, nil
		}
		if size >= s.cfg.MaxLog {
			return 0, ErrLogFull
		}
		// A log is either within the note's room of full, or full: the append that would
		// pass the room takes what fits and the note, which fills it exactly.
		note := s.truncatedNote()
		if room := s.cfg.MaxLog - int64(len(note)) - size; int64(len(rest)) > room {
			rest = append(append([]byte{}, rest[:room]...), note...)
		}
		n, err := s.putSegment(ctx, run, size, rest)
		if err != nil {
			return 0, err
		}
		moved, err := s.advanceLog(ctx, run, size, size+n)
		if err != nil {
			return 0, err
		}
		if moved && (size+n >= offset+int64(len(data)) || size+n >= s.cfg.MaxLog) {
			return size + n, nil
		}
		// Not moved: another append moved the log, or the lease ended. Moved short: the
		// segment there was a shorter one an earlier request left, and the rest goes after it.
		// Either way, look again.
	}
	return 0, fmt.Errorf("the log of run %d kept moving under an append", id)
}

// putSegment writes data at start of a run's log, unless a segment is there already, and
// returns how many bytes of the log the segment there holds.
func (s *Store) putSegment(ctx context.Context, run *Run, start int64, data []byte) (int64, error) {
	key := segmentKey(run, start)
	err := s.blobs.Put(ctx, key, bytes.NewReader(data), int64(len(data)), blob.IfAbsent())
	if errors.Is(err, blob.ErrExists) {
		obj, err := s.blobs.Stat(ctx, key)
		if err != nil {
			return 0, fmt.Errorf("the log of run %d: %w", run.ID, err)
		}
		return obj.Size, nil
	}
	if err != nil {
		return 0, fmt.Errorf("the log of run %d: %w", run.ID, err)
	}
	return int64(len(data)), nil
}

// advanceLog moves a run's log size from from to to, waking its readers, if the run is still the
// lease the append read and its log is still from long; it reports whether it did.
func (s *Store) advanceLog(ctx context.Context, run *Run, from, to int64) (bool, error) {
	var moved bool
	err := s.txn(ctx, func(tx Ops) error {
		if err := tx.Gate(ctx, run.RepositoryID); err != nil {
			return err
		}
		var err error
		if moved, err = tx.AdvanceLog(ctx, run.ID, run.Worker, run.StartedAt, from, to); err != nil || !moved {
			return err
		}
		return changed(ctx, tx, run.ID)
	})
	return moved, err
}

// ReadLog emits a run's log from offset: what there is, then — with follow — what is
// written, until the run finishes. end is set on the last emit of a finished run's log.
func (s *Store) ReadLog(ctx context.Context, id, offset int64, follow bool, emit func(offset int64, data []byte, end bool) error) error {
	if offset < 0 {
		return invalid("a log offset is not negative")
	}
	var segs segments
	var waiter *wait.Waiter
	defer func() {
		if waiter != nil {
			waiter.Close()
		}
	}()
	short := 0
	for {
		// A follower waits on the run's changes from before its read (wait.Open), so a write between
		// the read and the wait still wakes it.
		if follow && waiter == nil {
			w, err := wait.Open(ctx, s.queue, wait.Reg{Store: s.b.Waits(), Subjects: []string{wait.RunSubject(id)}})
			if err != nil {
				return err
			}
			waiter = w
		}
		run, err := s.b.Run(ctx, id)
		if err != nil {
			return err
		}
		if offset > run.LogSize {
			return invalid("the log is %d bytes; %d is past its end", run.LogSize, offset)
		}
		if run.LogStored {
			return s.readStored(ctx, run, offset, emit)
		}
		if run.CarriedFrom != 0 {
			note, err := s.carriedLog(ctx, run)
			if err != nil {
				return err
			}
			return emit(offset, note[min(offset, int64(len(note))):], true)
		}
		finished := run.Status.Finished()
		emitted := false
		offset, err = s.readLive(ctx, run, offset, &segs, func(at int64, data []byte) error {
			emitted = true
			return emit(at, data, finished && at+int64(len(data)) == run.LogSize)
		})
		if err != nil {
			return err
		}
		switch {
		case finished && offset == run.LogSize:
			if !emitted {
				return emit(offset, nil, true)
			}
			return nil
		case offset < run.LogSize:
			// Bytes the size counts were not where they were looked for: the run finished and its
			// log was gathered into its object as it was read — the object written before the
			// segments go — or, read as the run ran, it finished since. Look again.
			if finished {
				gathered, err := s.gathered(ctx, run)
				if err != nil {
					return err
				}
				if gathered {
					return s.readStored(ctx, run, offset, emit)
				}
			}
			if short++; short == shortReads {
				return fmt.Errorf("the log of run %d lacks bytes %d to %d where they should be", id, offset, run.LogSize)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(shortWait):
			}
			continue
		case !follow:
			return nil
		}
		if _, err := waiter.Next(ctx); err != nil {
			return err
		}
	}
}

// shortReads bounds how often a read looks again for bytes it did not find where they should be,
// shortWait apart: a log gathered as it is read is found in its object at once.
const (
	shortReads = 50
	shortWait  = 20 * time.Millisecond
)

// readChunk is how much of a log one emit carries at most.
const readChunk = 256 << 10

// readStored emits a stored log from offset, from the bucket.
func (s *Store) readStored(ctx context.Context, run *Run, offset int64, emit func(int64, []byte, bool) error) error {
	if offset == run.LogSize {
		return emit(offset, nil, true)
	}
	for offset < run.LogSize {
		n := min(readChunk, run.LogSize-offset)
		data, err := s.blobs.GetRange(ctx, logKey(run.ID), offset, n)
		if err != nil {
			return fmt.Errorf("the log of run %d: %w", run.ID, err)
		}
		if err := emit(offset, data, offset+n == run.LogSize); err != nil {
			return err
		}
		offset += n
	}
	return nil
}

// segment is one of a lease's segments: its key, and the bytes of the log it holds.
type segment struct {
	key        string
	start, end int64
}

// segments are a lease's segments as listed so far, by start.
type segments struct {
	dir  string
	list []segment
	last string // the last key listed: the next listing starts after it
}

// refresh lists the lease's segments written since the last listing.
func (g *segments) refresh(ctx context.Context, blobs blob.Store) error {
	for {
		objs, next, err := blobs.ListPage(ctx, g.dir, g.last, 1000)
		if err != nil {
			return err
		}
		for _, o := range objs {
			g.last = o.Key
			if start, err := strconv.ParseInt(strings.TrimPrefix(o.Key, g.dir), 16, 64); err == nil {
				g.list = append(g.list, segment{key: o.Key, start: start, end: start + o.Size})
			}
		}
		if next == "" {
			return nil
		}
	}
}

// at is the segment that holds offset o, if one does. Segments never overlap: each starts where
// the log ended when it was written, and the next where it ends.
func (g *segments) at(o int64) (segment, bool) {
	i := sort.Search(len(g.list), func(i int) bool { return g.list[i].start > o }) - 1
	if i >= 0 && o < g.list[i].end {
		return g.list[i], true
	}
	return segment{}, false
}

// readLive emits a run's log that is not yet stored from offset, as far as it reaches up to the
// run's size, from its lease's segments, and returns where it stopped. It stops short of the size
// only when bytes the size counts are not where they were looked for: they went to the bucket's
// object for the run, or the lease is another.
func (s *Store) readLive(ctx context.Context, run *Run, offset int64, g *segments, emit func(at int64, data []byte) error) (int64, error) {
	if dir := leaseDir(run); g.dir != dir {
		*g = segments{dir: dir}
	}
	listed := false
	for offset < run.LogSize {
		if sg, ok := g.at(offset); ok {
			end := min(sg.end, run.LogSize, offset+readChunk)
			data, err := s.blobs.GetRange(ctx, sg.key, offset-sg.start, end-offset)
			if errors.Is(err, blob.ErrNotFound) {
				return offset, nil // gathered into the run's object as it was read
			}
			if err != nil {
				return offset, fmt.Errorf("the log of run %d: %w", run.ID, err)
			}
			if err := emit(offset, data); err != nil {
				return offset, err
			}
			offset = end
			continue
		}
		if !listed {
			if err := g.refresh(ctx, s.blobs); err != nil {
				return offset, fmt.Errorf("the log of run %d: %w", run.ID, err)
			}
			listed = true
			continue
		}
		return offset, nil
	}
	return offset, nil
}

// gathered reports whether a finished run's whole log is in its object in the bucket, which a
// move to the bucket writes before it deletes the segments and marks the log stored.
func (s *Store) gathered(ctx context.Context, run *Run) (bool, error) {
	obj, err := s.blobs.Stat(ctx, logKey(run.ID))
	if errors.Is(err, blob.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("the log of run %d: %w", run.ID, err)
	}
	return obj.Size == run.LogSize, nil
}

// storeLog gathers a finished run's log into its object in the bucket: the object is written,
// then the segments of each of the run's leases deleted, then the run marked stored — each step
// safe to repeat, so a move that fails is the sweep's to finish. A reader finds the segments, or
// the object. An empty log is stored without an object.
func (s *Store) storeLog(ctx context.Context, id int64) error {
	run, err := s.b.Run(ctx, id)
	if err != nil {
		return err
	}
	// Not while its repository moves, nor once it moved: its log goes with it, as it is, and is
	// stored where it arrives.
	ctx = move.WithRepository(ctx, run.RepositoryID)
	if err := s.b.Txn(ctx, func(Ops) error { return nil }); err != nil {
		return err
	}
	if run.LogStored || !run.Status.Finished() {
		return nil
	}
	if run.LogSize > 0 {
		done, err := s.gathered(ctx, run)
		if err != nil {
			return err
		}
		if !done {
			var log bytes.Buffer
			if run.CarriedFrom != 0 {
				note, err := s.carriedLog(ctx, run)
				if err != nil {
					return err
				}
				log.Write(note)
			} else {
				at, err := s.readLive(ctx, run, 0, &segments{}, func(_ int64, data []byte) error { log.Write(data); return nil })
				if err != nil {
					return err
				}
				if at != run.LogSize {
					// The segments went as they were read: another gathering finished first, its
					// object written before them.
					if done, err = s.gathered(ctx, run); err != nil {
						return err
					}
					if !done {
						return fmt.Errorf("the log of run %d is missing bytes %d to %d", id, at, run.LogSize)
					}
				}
			}
			if !done {
				if int64(log.Len()) != run.LogSize {
					return fmt.Errorf("the log of run %d has %d of its %d bytes", id, log.Len(), run.LogSize)
				}
				if err := s.blobs.Put(ctx, logKey(id), bytes.NewReader(log.Bytes()), run.LogSize); err != nil {
					return fmt.Errorf("store the log of run %d: %w", id, err)
				}
			}
		}
	}
	segs, err := s.blobs.List(ctx, liveDir(id))
	if err != nil {
		return fmt.Errorf("the segments of run %d's log: %w", id, err)
	}
	if len(segs) > 0 {
		keys := make([]string, len(segs))
		for i, o := range segs {
			keys[i] = o.Key
		}
		if err := s.blobs.Delete(ctx, keys...); err != nil {
			return fmt.Errorf("delete the segments of run %d's log: %w", id, err)
		}
	}
	return s.b.MarkLogStored(ctx, id)
}

// logQueue carries the jobs that store finished runs' logs: one a run, enqueued before the
// transaction that finished it commits, done as it finishes and again by whichever replica leases
// it should that fail — the queue retries it, and nothing sweeps the database for logs left behind.
const logQueue = "automation.logs"

func logJob(id int64) bus.Job {
	return bus.Job{ID: "log-" + strconv.FormatInt(id, 10), Queue: logQueue, Key: id, Body: jobBody(id, time.Now())}
}

// enqueueLogs enqueues the jobs storing runs' logs.
func (s *Store) enqueueLogs(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	jobs := make([]bus.Job, len(ids))
	for i, id := range ids {
		jobs[i] = logJob(id)
	}
	if err := s.queue.Enqueue(ctx, jobs...); err != nil {
		return fmt.Errorf("enqueueing runs' logs: %w", err)
	}
	return nil
}

// StoreLogs stores finished runs' logs as their jobs come, until ctx ends: every replica runs it.
func (s *Store) StoreLogs(ctx context.Context) error {
	for ctx.Err() == nil {
		if _, err := s.storeLogs(ctx, 30*time.Second); err != nil && ctx.Err() == nil {
			slog.Warn("automation: leasing log jobs", "err", err)
			time.Sleep(time.Second)
		}
	}
	return nil
}

// StoreReadyLogs stores the logs whose jobs are ready now, and returns.
func (s *Store) StoreReadyLogs(ctx context.Context) error {
	for {
		n, err := s.storeLogs(ctx, 0)
		if err != nil || n == 0 {
			return err
		}
	}
}

// storeLogs stores the logs of a batch of log jobs, waiting up to wait for one; it returns how many
// it leased. A log of a repository moving, or moved, goes with it, as it is; a failure is tried
// again later.
func (s *Store) storeLogs(ctx context.Context, wait time.Duration) (int, error) {
	ls, err := s.queue.Lease(ctx, logQueue, bus.LeaseOptions{Max: 8, Visibility: 2 * time.Minute, Wait: wait})
	if err != nil {
		return 0, err
	}
	for _, l := range ls {
		id, made, ok := parseJobBody(l.Job().Body)
		if !ok {
			_ = l.Ack(ctx)
			continue
		}
		copies := bus.Copies(l.Job().ID)
		var err error
		run, rerr := s.b.Run(ctx, id)
		switch {
		case (errors.Is(rerr, ErrNotFound) || rerr == nil && !run.Status.Finished()) && time.Since(made) < notThereFor:
			// The transaction that finished it, which enqueued the job, commits in moments.
			err = bus.PutOff(ctx, s.queue, l, bus.Backoff(copies, 20*time.Millisecond, 2*time.Second))
		case errors.Is(rerr, ErrNotFound) || rerr == nil && !run.Status.Finished():
			err = l.Ack(ctx) // its transaction never committed, or the run is gone
		case rerr != nil:
			err = bus.PutOff(ctx, s.queue, l, bus.Backoff(copies, time.Second, 5*time.Minute))
		default:
			switch serr := s.storeLog(ctx, id); {
			case serr == nil || errors.Is(serr, ErrNotFound) || move.IsFrozen(serr) || moved(serr):
				err = l.Ack(ctx)
			default:
				slog.Warn("automation: storing a run's log", "run", id, "copies", copies, "err", serr)
				err = bus.PutOff(ctx, s.queue, l, bus.Backoff(copies, time.Second, 5*time.Minute))
			}
		}
		if err != nil && !errors.Is(err, bus.ErrLeaseLost) && ctx.Err() == nil {
			slog.Warn("automation: finishing a log job", "run", id, "err", err)
		}
	}
	return len(ls), nil
}

// enqueueUnstoredLogs enqueues the jobs of the finished runs whose logs are not stored, of repos,
// or of every repository for none: a move's arrival, and, once as a replica starts, the logs an
// older release left.
func (s *Store) enqueueUnstoredLogs(ctx context.Context, repos []int64) error {
	ids, err := s.b.UnstoredLogs(ctx, repos, 1<<20)
	if err != nil {
		return err
	}
	for start := 0; start < len(ids); start += 1000 {
		if err := s.enqueueLogs(ctx, ids[start:min(start+1000, len(ids))]); err != nil {
			return err
		}
	}
	return nil
}

// Started does what a release before dispatch left for this one: its queued runs dispatched, and
// its finished runs' logs stored — once, once that release has stopped (work.Once).
func (s *Store) Started(ctx context.Context) error {
	return errors.Join(s.DispatchUndispatched(ctx), s.enqueueUnstoredLogs(ctx, nil))
}

// moved reports a refusal because the repository moved to another cell.
func moved(err error) bool {
	_, ok := cells.MovedOf(err)
	return ok
}
