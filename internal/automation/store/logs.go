package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// logKey is a finished run's log object in the bucket.
func logKey(id int64) string { return "runs/" + strconv.FormatInt(id, 10) }

// truncatedNote ends a log that reached the most a run keeps.
func (s *Store) truncatedNote() string {
	return fmt.Sprintf("\n[ok: the log stops here, at the %d MiB a run keeps]\n", s.cfg.MaxLog>>20)
}

// AppendLog adds data at offset to a leased run's log and returns the log's size. A
// request resent after a lost answer is taken once (the bytes it repeats are skipped), and
// one past the end is refused. A log that reaches the most a run keeps ends with a note
// saying so, and refuses more with ErrLogFull.
func (s *Store) AppendLog(ctx context.Context, id int64, worker string, offset int64, data []byte) (int64, error) {
	if offset < 0 {
		return 0, invalid("a log offset is not negative")
	}
	var size int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var status Status
		var holder *string
		if err := tx.QueryRow(ctx, `SELECT status, worker, log_size FROM run WHERE id = $1 FOR UPDATE`, id).
			Scan(&status, &holder, &size); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if status != StatusRunning || holder == nil || *holder != worker {
			return ErrLeaseLost
		}
		if offset > size {
			return invalid("the log is %d bytes; a write at %d leaves a gap", size, offset)
		}
		// Only what extends the log is new.
		data = data[min(int64(len(data)), size-offset):]
		if len(data) == 0 {
			return nil
		}
		if size >= s.cfg.MaxLog {
			return ErrLogFull
		}
		// A log is either within the note's room of full, or full: the append that would
		// pass the room takes what fits and the note, which fills it exactly.
		note := s.truncatedNote()
		if room := s.cfg.MaxLog - int64(len(note)) - size; int64(len(data)) > room {
			data = append(append([]byte{}, data[:room]...), note...)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO log_chunk (run_id, at, data) VALUES ($1, $2, $3)`, id, size, data); err != nil {
			return err
		}
		size += int64(len(data))
		if _, err := tx.Exec(ctx, `UPDATE run SET log_size = $2 WHERE id = $1`, id, size); err != nil {
			return err
		}
		_, err := notify(ctx, tx, chanLog, id)
		return err
	})
	return size, err
}

// logChunk is a part of a log: its bytes at its offset.
type logChunk struct {
	at   int64
	data []byte
}

// ReadLog emits a run's log from offset: what there is, then — with follow — what is
// written, until the run finishes. end is set on the last emit of a finished run's log.
func (s *Store) ReadLog(ctx context.Context, id, offset int64, follow bool, emit func(offset int64, data []byte, end bool) error) error {
	if offset < 0 {
		return invalid("a log offset is not negative")
	}
	for {
		// Watching starts before the read, so a write between the read and the wait still
		// wakes it.
		woken := s.notify.watch(chanLog, strconv.FormatInt(id, 10))
		done := s.notify.watch(chanDone, strconv.FormatInt(id, 10))
		run, err := s.Get(ctx, id)
		if err != nil {
			return err
		}
		if offset > run.LogSize {
			return invalid("the log is %d bytes; %d is past its end", run.LogSize, offset)
		}
		if run.LogStored {
			return s.readStored(ctx, run, offset, emit)
		}
		chunks, err := s.chunksFrom(ctx, id, offset)
		if err != nil {
			return err
		}
		for _, c := range chunks {
			if c.at > offset {
				break // the chunks moved to the bucket as they were read: read again
			}
			data := c.data[offset-c.at:]
			offset += int64(len(data))
			if err := emit(offset-int64(len(data)), data, run.Status.Finished() && offset == run.LogSize); err != nil {
				return err
			}
		}
		if run.Status.Finished() && offset == run.LogSize {
			if len(chunks) == 0 {
				return emit(offset, nil, true)
			}
			return nil
		}
		if run.Status.Finished() {
			continue // the rest moved to the bucket meanwhile
		}
		if !follow {
			return nil
		}
		if err := s.notify.waitAny(ctx, s.cfg.Poll, woken, done); err != nil {
			return err
		}
	}
}

// chunksFrom returns the log's chunks that reach past offset, in order.
func (s *Store) chunksFrom(ctx context.Context, id, offset int64) ([]logChunk, error) {
	rows, err := s.pool.Query(ctx, `SELECT at, data FROM log_chunk
		WHERE run_id = $1 AND at + length(data) > $2 ORDER BY at`, id, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []logChunk
	for rows.Next() {
		var c logChunk
		if err := rows.Scan(&c.at, &c.data); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// readChunk is how much of a stored log one emit carries.
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

// storeLog moves a finished run's log from the database to the bucket: the object is
// written, then the run marked stored and its chunks deleted in one transaction, so a reader
// sees one or the other. An empty log is stored without an object.
func (s *Store) storeLog(ctx context.Context, id int64) error {
	run, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if run.LogStored || !run.Status.Finished() {
		return nil
	}
	if run.LogSize > 0 {
		chunks, err := s.chunksFrom(ctx, id, 0)
		if err != nil {
			return err
		}
		var log bytes.Buffer
		for _, c := range chunks {
			if c.at != int64(log.Len()) {
				return fmt.Errorf("the log of run %d is missing bytes %d to %d", id, log.Len(), c.at)
			}
			log.Write(c.data)
		}
		if int64(log.Len()) != run.LogSize {
			return fmt.Errorf("the log of run %d has %d of its %d bytes", id, log.Len(), run.LogSize)
		}
		if err := s.blobs.Put(ctx, logKey(id), bytes.NewReader(log.Bytes()), run.LogSize); err != nil {
			return fmt.Errorf("store the log of run %d: %w", id, err)
		}
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE run SET log_stored = true WHERE id = $1`, id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM log_chunk WHERE run_id = $1`, id)
		return err
	})
}

// sweepLogs moves the logs of finished runs a failed move left in the database.
func (s *Store) sweepLogs(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, `SELECT id FROM run WHERE status >= $1 AND NOT log_stored ORDER BY id LIMIT 100`, StatusPassed)
	if err != nil {
		return err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return err
	}
	var errs []error
	for _, id := range ids {
		if err := s.storeLog(ctx, id); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Sweep does the work no request does: it acts on lapsed leases and moves the logs a
// failed move left behind. Every replica may run it; each step is safe to repeat.
func (s *Store) Sweep(ctx context.Context) error {
	return errors.Join(s.sweepLeases(ctx), s.sweepLogs(ctx))
}
