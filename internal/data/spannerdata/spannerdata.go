// Package spannerdata is internal/data's Spanner side (docs/hosted.md, "Spanner"): the fenced
// transactions, keyed work, and follow cursors every role's Spanner backend does the same way, in
// GoogleSQL. A read-write transaction locks what it reads against writers until it commits and
// Spanner aborts one of two that meet — the client running it again — so each operation reads its
// rows and then writes them, and needs no lock of its own.
package spannerdata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannertxn"
	"github.com/Cidan/ok/internal/lock/spannerlock"
	"github.com/Cidan/ok/internal/work"
)

// Reader is what reads go through: a read-write transaction, a read-only one, or a client's
// single read.
type Reader interface {
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
}

// Txn runs fn in a read-write transaction on c — again, when Spanner aborts it — checking the
// fence ctx carries first (CheckFence). fn holds no effects outside the transaction.
func Txn(ctx context.Context, c *spanner.Client, fn func(context.Context, *spanner.ReadWriteTransaction) error) error {
	_, err := spannertxn.ReadWrite(ctx, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		if err := CheckFence(ctx, tx); err != nil {
			return err
		}
		return fn(ctx, tx)
	})
	return err
}

// CheckFence checks, in tx, the fence ctx carries, if any (data.Fence): a lock's lease
// (spannerlock), or a piece of keyed work's.
func CheckFence(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
	f, ok := data.FenceOf(ctx)
	if !ok {
		return nil
	}
	if f.Lock {
		return spannerlock.CheckFence(ctx, tx, f)
	}
	return CheckWorkFence(ctx, tx, f)
}

// one runs a query expected to answer at most one row, scanning it into dest; false for none.
func one(ctx context.Context, r Reader, stmt spanner.Statement, dest ...any) (bool, error) {
	it := r.Query(ctx, stmt)
	defer it.Stop()
	row, err := it.Next()
	if errors.Is(err, iterator.Done) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, row.Columns(dest...)
}

func update(ctx context.Context, tx *spanner.ReadWriteTransaction, sql string, params map[string]any) (int64, error) {
	return tx.Update(ctx, spanner.Statement{SQL: sql, Params: params})
}

// ─── Keyed work ───

// WorkDDL makes the table a role keeps its keyed work in (internal/work), as pgdata.WorkSchema
// does on PostgreSQL.
const WorkDDL = `CREATE TABLE work (
    kind        STRING(MAX) NOT NULL,
    key         INT64       NOT NULL,
    dirty       BOOL        NOT NULL,
    due_at      TIMESTAMP,
    holder      STRING(MAX),
    lease_until TIMESTAMP,
) PRIMARY KEY (kind, key)`

// workRow is a piece of work's row as the operations read it, judged by the database's clock.
type workRow struct {
	dirty, held, dueNow, dueLater bool
	holder                        spanner.NullString
}

func readWork(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64) (workRow, bool, error) {
	var w workRow
	ok, err := one(ctx, tx, spanner.Statement{SQL: `SELECT dirty,
			holder IS NOT NULL AND lease_until >= CURRENT_TIMESTAMP(),
			due_at IS NULL OR due_at <= CURRENT_TIMESTAMP(),
			IFNULL(due_at > CURRENT_TIMESTAMP(), FALSE),
			holder
		FROM work WHERE kind = @kind AND key = @key`, Params: map[string]any{"kind": kind, "key": key}},
		&w.dirty, &w.held, &w.dueNow, &w.dueLater, &w.holder)
	return w, ok, err
}

func micros(d time.Duration) int64 { return d.Microseconds() }

// MarkWork marks key's work of kind dirty, due now.
func MarkWork(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64) error {
	w, ok, err := readWork(ctx, tx, kind, key)
	if err != nil {
		return err
	}
	p := map[string]any{"kind": kind, "key": key}
	switch {
	case ok && w.dirty && w.dueNow:
		// Marked due now already: written again, the row would be every marker's to wait on — a
		// repository's every upload marks its compaction. The read's lock holds the mark until
		// this transaction commits: the work cannot take it off meanwhile.
	case ok:
		_, err = update(ctx, tx, `UPDATE work SET dirty = TRUE, due_at = NULL WHERE kind = @kind AND key = @key`, p)
	default:
		_, err = update(ctx, tx, `INSERT INTO work (kind, key, dirty) VALUES (@kind, @key, TRUE)`, p)
	}
	return err
}

// MarkWorkAt marks key's work of kind dirty, due at at — or sooner, if it is marked so already.
func MarkWorkAt(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64, at time.Time) error {
	w, ok, err := readWork(ctx, tx, kind, key)
	if err != nil {
		return err
	}
	p := map[string]any{"kind": kind, "key": key, "at": at}
	switch {
	case !ok:
		_, err = update(ctx, tx, `INSERT INTO work (kind, key, dirty, due_at) VALUES (@kind, @key, TRUE, @at)`, p)
	case !w.dirty:
		_, err = update(ctx, tx, `UPDATE work SET dirty = TRUE, due_at = @at WHERE kind = @kind AND key = @key`, p)
	default:
		// Marked already: due now stays due now, and a later mark keeps the sooner of the two.
		_, err = update(ctx, tx, `UPDATE work SET due_at = IF(due_at IS NULL, NULL, LEAST(due_at, @at)) WHERE kind = @kind AND key = @key`, p)
	}
	return err
}

// MarkWorkIdle marks key's work of kind dirty, due now — making its row if it has none — if it is
// not marked and no one holds it, or its holder's lease lapsed, as pgdata.MarkWorkIdle does.
func MarkWorkIdle(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64) error {
	w, ok, err := readWork(ctx, tx, kind, key)
	if err != nil {
		return err
	}
	p := map[string]any{"kind": kind, "key": key}
	switch {
	case !ok:
		_, err = update(ctx, tx, `INSERT INTO work (kind, key, dirty) VALUES (@kind, @key, TRUE)`, p)
	case !w.dirty && !w.held:
		_, err = update(ctx, tx, `UPDATE work SET dirty = TRUE, due_at = NULL WHERE kind = @kind AND key = @key`, p)
	}
	return err
}

// TakeWork leases key's work to holder for ttl, clearing its mark, if no one holds it or its
// holder's lease lapsed — making its row if it has none; it reports whether it did.
func TakeWork(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	w, ok, err := readWork(ctx, tx, kind, key)
	if err != nil || ok && w.held {
		return false, err
	}
	p := map[string]any{"kind": kind, "key": key, "holder": holder, "ttl": micros(ttl)}
	if ok {
		_, err = update(ctx, tx, `UPDATE work SET holder = @holder, lease_until = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl MICROSECOND),
			dirty = FALSE, due_at = NULL WHERE kind = @kind AND key = @key`, p)
	} else {
		_, err = update(ctx, tx, `INSERT INTO work (kind, key, dirty, holder, lease_until)
			VALUES (@kind, @key, FALSE, @holder, TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl MICROSECOND))`, p)
	}
	return err == nil, err
}

// RenewWork extends holder's lease on key's work by ttl from now; it reports false if the lease is
// another's.
func RenewWork(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	n, err := update(ctx, tx, `UPDATE work SET lease_until = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl MICROSECOND)
		WHERE kind = @kind AND key = @key AND holder = @holder`, map[string]any{"kind": kind, "key": key, "holder": holder, "ttl": micros(ttl)})
	return n > 0, err
}

// ReleaseWork ends holder's lease on key's work — unless the work was marked meanwhile, due now,
// when it clears the mark and keeps the lease, reporting again — as pgdata.ReleaseWork does.
func ReleaseWork(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64, holder string) (again bool, err error) {
	w, ok, err := readWork(ctx, tx, kind, key)
	if err != nil || !ok || w.holder.StringVal != holder || !w.holder.Valid {
		return false, err
	}
	p := map[string]any{"kind": kind, "key": key}
	if w.dirty && w.dueNow {
		_, err = update(ctx, tx, `UPDATE work SET dirty = FALSE, due_at = NULL WHERE kind = @kind AND key = @key`, p)
		return err == nil, err
	}
	// A mark due later stays, for whoever takes it then.
	_, err = update(ctx, tx, `UPDATE work SET holder = NULL, lease_until = NULL, dirty = (dirty AND IFNULL(due_at > CURRENT_TIMESTAMP(), FALSE)),
		due_at = IF(dirty AND IFNULL(due_at > CURRENT_TIMESTAMP(), FALSE), due_at, NULL) WHERE kind = @kind AND key = @key`, p)
	return false, err
}

// DueWork are up to limit keys of kind's work that is marked, due, and held by no one, or whose
// holder's lease lapsed, by key.
func DueWork(ctx context.Context, r Reader, kind string, limit int) ([]int64, error) {
	it := r.Query(ctx, spanner.Statement{SQL: `SELECT key FROM work WHERE kind = @kind
		AND ((dirty AND holder IS NULL AND (due_at IS NULL OR due_at <= CURRENT_TIMESTAMP())) OR lease_until < CURRENT_TIMESTAMP())
		ORDER BY key LIMIT @limit`, Params: map[string]any{"kind": kind, "limit": int64(limit)}})
	var out []int64
	err := it.Do(func(row *spanner.Row) error {
		var key int64
		if err := row.Columns(&key); err != nil {
			return err
		}
		out = append(out, key)
		return nil
	})
	return out, err
}

// CheckWorkFence fails tx with data.ErrFenced unless f's work is still leased to f's holder,
// reading its row so a takeover waits for or is aborted by tx.
func CheckWorkFence(ctx context.Context, tx *spanner.ReadWriteTransaction, f data.Fence) error {
	var held bool
	ok, err := one(ctx, tx, spanner.Statement{SQL: `SELECT holder = @holder AND lease_until >= CURRENT_TIMESTAMP() FROM work
		WHERE kind = @kind AND key = @key`, Params: map[string]any{"kind": f.Kind, "key": f.Key, "holder": f.Holder}}, &held)
	if err != nil {
		return err
	}
	if !ok || !held {
		return fmt.Errorf("%w: %s", data.ErrFenced, f)
	}
	return nil
}

// Work is keyed work's store (internal/work) over the work table of the database c reaches, each
// operation a transaction of its own.
func Work(c *spanner.Client) work.Store { return workStore{c} }

type workStore struct{ c *spanner.Client }

func (w workStore) txn(ctx context.Context, fn func(context.Context, *spanner.ReadWriteTransaction) error) error {
	_, err := spannertxn.ReadWrite(ctx, w.c, fn)
	return err
}

func (w workStore) Mark(ctx context.Context, kind string, key int64) error {
	return w.txn(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error { return MarkWork(ctx, tx, kind, key) })
}

func (w workStore) MarkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return w.txn(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		return MarkWorkAt(ctx, tx, kind, key, at)
	})
}

func (w workStore) Take(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	var ok bool
	err := w.txn(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		ok, err = TakeWork(ctx, tx, kind, key, holder, ttl)
		return err
	})
	return ok, err
}

func (w workStore) Renew(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	var ok bool
	err := w.txn(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		ok, err = RenewWork(ctx, tx, kind, key, holder, ttl)
		return err
	})
	return ok, err
}

func (w workStore) Release(ctx context.Context, kind string, key int64, holder string) (bool, error) {
	var again bool
	err := w.txn(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		again, err = ReleaseWork(ctx, tx, kind, key, holder)
		return err
	})
	return again, err
}

func (w workStore) Due(ctx context.Context, kind string, limit int) ([]int64, error) {
	return DueWork(ctx, w.c.Single(), kind, limit)
}

// ─── Follow cursors ───

// CursorsDDL makes the table a role that follows topics of the bus's stream keeps its place in each
// key's log in (internal/bus/follow), as pgdata.CursorsSchema does on PostgreSQL.
const CursorsDDL = `CREATE TABLE follow_cursor (
    topic STRING(MAX) NOT NULL,
    key   INT64       NOT NULL,
    seq   INT64       NOT NULL,
) PRIMARY KEY (topic, key)`

// Cursor is the last seq of key's log on topic applied (0: none).
func Cursor(ctx context.Context, r Reader, topic string, key int64) (int64, error) {
	var seq int64
	_, err := one(ctx, r, spanner.Statement{SQL: `SELECT seq FROM follow_cursor WHERE topic = @topic AND key = @key`,
		Params: map[string]any{"topic": topic, "key": key}}, &seq)
	return seq, err
}

// Cursors are the last seqs of keys' logs on topic applied; a key with none is absent.
func Cursors(ctx context.Context, r Reader, topic string, keys []int64) (map[int64]int64, error) {
	it := r.Query(ctx, spanner.Statement{SQL: `SELECT key, seq FROM follow_cursor WHERE topic = @topic AND key IN UNNEST(@keys)`,
		Params: map[string]any{"topic": topic, "keys": keys}})
	out := map[int64]int64{}
	err := it.Do(func(row *spanner.Row) error {
		var key, seq int64
		if err := row.Columns(&key, &seq); err != nil {
			return err
		}
		out[key] = seq
		return nil
	})
	return out, err
}

// LockCursor is Cursor, held until tx ends: it reads key's row — so two transactions applying key's
// events meet, and Spanner runs one after the other — and makes it, at 0, if there is none.
func LockCursor(ctx context.Context, tx *spanner.ReadWriteTransaction, topic string, key int64) (int64, error) {
	var seq int64
	ok, err := one(ctx, tx, spanner.Statement{SQL: `SELECT seq FROM follow_cursor WHERE topic = @topic AND key = @key`,
		Params: map[string]any{"topic": topic, "key": key}}, &seq)
	if err != nil || ok {
		return seq, err
	}
	_, err = update(ctx, tx, `INSERT INTO follow_cursor (topic, key, seq) VALUES (@topic, @key, 0)`, map[string]any{"topic": topic, "key": key})
	return 0, err
}

// SetCursor records seq as the last of key's log applied.
func SetCursor(ctx context.Context, tx *spanner.ReadWriteTransaction, topic string, key, seq int64) error {
	if _, err := LockCursor(ctx, tx, topic, key); err != nil {
		return err
	}
	_, err := update(ctx, tx, `UPDATE follow_cursor SET seq = @seq WHERE topic = @topic AND key = @key`,
		map[string]any{"topic": topic, "key": key, "seq": seq})
	return err
}

// AdvanceCursor moves key's cursor to seq, never back.
func AdvanceCursor(ctx context.Context, tx *spanner.ReadWriteTransaction, topic string, key, seq int64) error {
	cur, err := LockCursor(ctx, tx, topic, key)
	if err != nil || cur >= seq {
		return err
	}
	_, err = update(ctx, tx, `UPDATE follow_cursor SET seq = @seq WHERE topic = @topic AND key = @key`,
		map[string]any{"topic": topic, "key": key, "seq": seq})
	return err
}
