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
	dueAt                         spanner.NullTime
}

func readWork(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64) (workRow, bool, error) {
	var w workRow
	ok, err := one(ctx, tx, spanner.Statement{SQL: `SELECT dirty,
			holder IS NOT NULL AND lease_until >= CURRENT_TIMESTAMP(),
			due_at IS NULL OR due_at <= CURRENT_TIMESTAMP(),
			IFNULL(due_at > CURRENT_TIMESTAMP(), FALSE),
			holder, due_at
		FROM work WHERE kind = @kind AND key = @key`, Params: map[string]any{"kind": kind, "key": key}},
		&w.dirty, &w.held, &w.dueNow, &w.dueLater, &w.holder, &w.dueAt)
	return w, ok, err
}

// readMark reads, in tx, what a mark decides by — whether key's work is marked, and due now — and
// those cells alone: not its holder's or lease's, which a renewal writes, so marks never hold its
// holder's renewals back. A take and a release write the mark's cells, so a mark is ordered against
// them still.
func readMark(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64) (workRow, bool, error) {
	var w workRow
	ok, err := one(ctx, tx, spanner.Statement{SQL: `SELECT dirty, due_at IS NULL OR due_at <= CURRENT_TIMESTAMP()
		FROM work WHERE kind = @kind AND key = @key`, Params: map[string]any{"kind": kind, "key": key}}, &w.dirty, &w.dueNow)
	return w, ok, err
}

func micros(d time.Duration) int64 { return d.Microseconds() }

// MarkWork marks key's work of kind dirty, due now, and records the mark's job in tx's attempt
// (work.Marked), enqueued before it commits. The mark's read holds its cells, which a take and a
// release write, until the transaction ends: every take and release of the work is ordered after
// it.
func MarkWork(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64) error {
	w, ok, err := readMark(ctx, tx, kind, key)
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
	if err != nil {
		return err
	}
	return work.Marked(tx, kind, key, time.Time{})
}

// MarkWorkAt marks key's work of kind dirty, due at at — or sooner, if it is marked so already —
// and records the mark's job, due at at, as MarkWork does.
func MarkWorkAt(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64, at time.Time) error {
	w, ok, err := readMark(ctx, tx, kind, key)
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
	if err != nil {
		return err
	}
	return work.Marked(tx, kind, key, at)
}

// PeekWork reads who holds key's work, without a lock.
func PeekWork(ctx context.Context, r Reader, kind string, key int64) (work.Peeked, error) {
	var p work.Peeked
	var holder spanner.NullString
	var left int64
	ok, err := one(ctx, r, spanner.Statement{SQL: `SELECT dirty, holder, holder IS NOT NULL AND lease_until >= CURRENT_TIMESTAMP(),
			IF(lease_until > CURRENT_TIMESTAMP(), TIMESTAMP_DIFF(lease_until, CURRENT_TIMESTAMP(), MICROSECOND), 0)
		FROM work WHERE kind = @kind AND key = @key`, Params: map[string]any{"kind": kind, "key": key}}, &p.Marked, &holder, &p.Held, &left)
	if err != nil || !ok {
		return work.Peeked{}, err
	}
	p.Exists, p.Holder, p.Left = true, holder.StringVal, time.Duration(left)*time.Microsecond
	return p, nil
}

// PendStep records a step to take once tx commits (work.Pend), as pgdata.PendStep does.
func PendStep(tx *spanner.ReadWriteTransaction, kind string, key int64, at time.Time, body []byte) error {
	return work.Pend(tx, kind, key, at, body)
}

// MarkedWork are up to limit pieces of kind's work after key after, in key order, marked or held by
// a holder whose lease lapsed, as pgdata.MarkedWork are.
func MarkedWork(ctx context.Context, r Reader, kind string, after int64, limit int) ([]work.Mark, error) {
	it := r.Query(ctx, spanner.Statement{SQL: `SELECT key, IF(dirty AND due_at > CURRENT_TIMESTAMP(), due_at, NULL) FROM work
		WHERE kind = @kind AND key > @after AND (dirty OR (holder IS NOT NULL AND lease_until < CURRENT_TIMESTAMP()))
		ORDER BY key LIMIT @limit`, Params: map[string]any{"kind": kind, "after": after, "limit": int64(limit)}})
	var out []work.Mark
	err := it.Do(func(row *spanner.Row) error {
		m := work.Mark{Kind: kind}
		var at spanner.NullTime
		if err := row.Columns(&m.Key, &at); err != nil {
			return err
		}
		m.At = at.Time
		out = append(out, m)
		return nil
	})
	return out, err
}

// TakeWork leases key's work to holder for ttl, if no one holds it or its holder's lease lapsed —
// making its row if it has none — clearing its mark if it is due now; a mark due later stays. The
// work is due if it was marked due now, or its holder's lease lapsed before it let go. Its read
// waits for a transaction marking the work to end: the mark holds what it reads.
func TakeWork(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64, holder string, ttl time.Duration) (work.Taken, error) {
	w, ok, err := readWork(ctx, tx, kind, key)
	if err != nil || ok && w.held {
		return work.Taken{}, err
	}
	p := map[string]any{"kind": kind, "key": key, "holder": holder, "ttl": micros(ttl)}
	if !ok {
		_, err = update(ctx, tx, `INSERT INTO work (kind, key, dirty, holder, lease_until)
			VALUES (@kind, @key, FALSE, @holder, TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl MICROSECOND))`, p)
		return work.Taken{Took: err == nil}, err
	}
	_, err = update(ctx, tx, `UPDATE work SET holder = @holder, lease_until = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl MICROSECOND),
			dirty = (dirty AND IFNULL(due_at > CURRENT_TIMESTAMP(), FALSE)),
			due_at = IF(dirty AND IFNULL(due_at > CURRENT_TIMESTAMP(), FALSE), due_at, NULL)
		WHERE kind = @kind AND key = @key`, p)
	if err != nil {
		return work.Taken{}, err
	}
	t := work.Taken{Took: true, Due: w.dirty && w.dueNow || w.holder.Valid}
	if w.dirty && w.dueLater {
		t.Later = w.dueAt.Time
	}
	return t, nil
}

// RenewWork extends holder's lease on key's work by ttl from now; it reports false if the lease is
// another's.
func RenewWork(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	n, err := update(ctx, tx, `UPDATE work SET lease_until = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl MICROSECOND)
		WHERE kind = @kind AND key = @key AND holder = @holder`, map[string]any{"kind": kind, "key": key, "holder": holder, "ttl": micros(ttl)})
	return n > 0, err
}

// LetWork ends holder's lease on key's work and leaves its mark as it is, a job brought at once for
// a mark due now, as pgdata.LetWork does.
func LetWork(ctx context.Context, tx *spanner.ReadWriteTransaction, kind string, key int64, holder string) error {
	w, ok, err := readWork(ctx, tx, kind, key)
	if err != nil || !ok || !w.holder.Valid || w.holder.StringVal != holder {
		return err
	}
	if _, err := update(ctx, tx, `UPDATE work SET holder = NULL, lease_until = NULL WHERE kind = @kind AND key = @key`,
		map[string]any{"kind": kind, "key": key}); err != nil {
		return err
	}
	if w.dirty && w.dueNow {
		return work.Marked(tx, kind, key, time.Time{})
	}
	return nil
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

// CheckWorkFence fails tx with data.ErrFenced unless f's work is held by f's holder, reading that
// cell alone, so a takeover, which writes it, waits for tx or aborts it. It reads no other: Spanner
// locks cells, and a renewal writes lease_until, which a fenced transaction reading it would keep
// from its holder's own renewals — every one of a compaction's batches, each renewal waiting on all
// of them. A lease lapsed and not taken over still fences: no one else writes.
func CheckWorkFence(ctx context.Context, tx *spanner.ReadWriteTransaction, f data.Fence) error {
	var held bool
	ok, err := one(ctx, tx, spanner.Statement{SQL: `SELECT holder = @holder FROM work WHERE kind = @kind AND key = @key`,
		Params: map[string]any{"kind": f.Kind, "key": f.Key, "holder": f.Holder}}, &held)
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

func (w workStore) Peek(ctx context.Context, kind string, key int64) (work.Peeked, error) {
	return PeekWork(ctx, w.c.Single(), kind, key)
}

func (w workStore) Take(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (work.Taken, error) {
	var t work.Taken
	err := w.txn(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		t, err = TakeWork(ctx, tx, kind, key, holder, ttl)
		return err
	})
	return t, err
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

func (w workStore) Let(ctx context.Context, kind string, key int64, holder string) error {
	return w.txn(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		return LetWork(ctx, tx, kind, key, holder)
	})
}

func (w workStore) Marked(ctx context.Context, kind string, after int64, limit int) ([]work.Mark, error) {
	return MarkedWork(ctx, w.c.Single(), kind, after, limit)
}

func (w workStore) Jobs() work.Jobs { return work.JobsOf(w.c) }

// Database is the client the store's work is in (work.RegisterStore).
func (w workStore) Database() any { return w.c }

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

// FollowersDDL makes the table a log's source keeps its followers in (internal/bus/follow), as
// pgdata.FollowersSchema does on PostgreSQL.
const FollowersDDL = `CREATE TABLE follower (
    topic       STRING(MAX) NOT NULL,
    name        STRING(MAX) NOT NULL,
    followed_at TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (topic, name)`

// AddFollower records name as one of topic's followers, once, in tx.
func AddFollower(ctx context.Context, tx *spanner.ReadWriteTransaction, topic, name string) error {
	_, err := update(ctx, tx, `INSERT OR IGNORE INTO follower (topic, name, followed_at) VALUES (@topic, @name, PENDING_COMMIT_TIMESTAMP())`,
		map[string]any{"topic": topic, "name": name})
	return err
}

// Followers are topic's followers, by name.
func Followers(ctx context.Context, r Reader, topic string) ([]string, error) {
	var out []string
	err := r.Query(ctx, spanner.Statement{SQL: `SELECT name FROM follower WHERE topic = @topic ORDER BY name`,
		Params: map[string]any{"topic": topic}}).Do(func(row *spanner.Row) error {
		var n string
		if err := row.Columns(&n); err != nil {
			return err
		}
		out = append(out, n)
		return nil
	})
	return out, err
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
