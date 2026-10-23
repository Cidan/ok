package spannerdata

import (
	"context"
	"errors"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannertxn"
)

// Ops runs a backend's statements: in Tx, the read-write transaction the operation is in, or — with
// none — each write in a read-write transaction of its own and each read in a read-only one, as a
// PostgreSQL backend's operations run in its transaction or on its pool. Writes are DML, which a
// transaction reads back, as PostgreSQL's transactions read their own writes.
type Ops struct {
	Client *spanner.Client
	Tx     *spanner.ReadWriteTransaction
	// Snapshot, when set, is a read-only transaction every read is in: a backend's Snapshot,
	// which writes nothing.
	Snapshot *spanner.ReadOnlyTransaction
	// Gate, when set, runs first in each write's transaction of its own: a move's gate, which
	// refuses a write about a repository whose network moves (move.SpannerGate). A backend's Txn
	// runs it first in its transaction.
	Gate func(ctx context.Context, tx *spanner.ReadWriteTransaction) error
}

// ErrReadOnly is a write in a read-only snapshot.
var ErrReadOnly = errors.New("spannerdata: a write in a read-only snapshot")

// Write runs fn in the operation's transaction, or in one of its own.
func (o Ops) Write(ctx context.Context, fn func(tx *spanner.ReadWriteTransaction) error) error {
	if o.Tx != nil {
		return fn(o.Tx)
	}
	if o.Snapshot != nil {
		return ErrReadOnly
	}
	_, err := spannertxn.ReadWrite(ctx, o.Client, func(tctx context.Context, tx *spanner.ReadWriteTransaction) error {
		if o.Gate != nil {
			if err := o.Gate(ctx, tx); err != nil {
				return err
			}
		}
		return fn(tx)
	})
	return err
}

// Read runs fn over the operation's transaction or snapshot, or over a read-only transaction of
// its own: what fn reads, it reads at one moment.
func (o Ops) Read(fn func(r Reader) error) error {
	if o.Tx != nil {
		return fn(o.Tx)
	}
	if o.Snapshot != nil {
		return fn(o.Snapshot)
	}
	ro := o.Client.ReadOnlyTransaction()
	defer ro.Close()
	return fn(ro)
}

// Exec runs a DML statement, returning the rows it changed.
func (o Ops) Exec(ctx context.Context, sql string, params map[string]any) (int64, error) {
	var n int64
	err := o.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		n, err = tx.Update(ctx, spanner.Statement{SQL: sql, Params: params})
		return err
	})
	return n, err
}

// Returning runs a DML statement with THEN RETURN, calling fn with each row it returns; reset runs
// before each attempt, as a transaction of its own may run again.
func (o Ops) Returning(ctx context.Context, sql string, params map[string]any, reset func(), fn func(*spanner.Row) error) error {
	return o.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		reset()
		return tx.Query(ctx, spanner.Statement{SQL: sql, Params: params}).Do(fn)
	})
}

// Each runs a query, calling fn with each row.
func Each(ctx context.Context, r Reader, sql string, params map[string]any, fn func(*spanner.Row) error) error {
	spannertxn.Counted(r)
	return r.Query(ctx, spanner.Statement{SQL: sql, Params: params}).Do(fn)
}

// First runs a query, scanning its first row into dest; false when it has none.
func First(ctx context.Context, r Reader, sql string, params map[string]any, dest ...any) (bool, error) {
	spannertxn.Counted(r)
	return one(ctx, r, spanner.Statement{SQL: sql, Params: params}, dest...)
}

// Update runs a DML statement in tx, returning the rows it changed.
func Update(ctx context.Context, tx *spanner.ReadWriteTransaction, sql string, params map[string]any) (int64, error) {
	spannertxn.Counted(tx)
	return tx.Update(ctx, spanner.Statement{SQL: sql, Params: params})
}
