// Package spannerlock is the lock library's (internal/lock) Spanner side: its DB, and the locks a
// read-write transaction holds, over a database's locks table (DDL). A read-write transaction
// locks what it reads against writers and what it writes against everyone until it commits, so a
// lease is taken by reading its row and writing it, a transaction's lock is held by writing its
// row, and a fence is held by reading its row; Spanner aborts one of two transactions that meet,
// and the client runs it again.
package spannerlock

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannertxn"
	"github.com/Cidan/ok/internal/lock"
)

// DDL makes the locks table, if it is not there.
const DDL = `CREATE TABLE IF NOT EXISTS locks (
    bucket       STRING(MAX) NOT NULL,
    id           STRING(MAX) NOT NULL,
    owner        STRING(MAX) NOT NULL,
    refresh_id   STRING(MAX) NOT NULL,
    last_refresh TIMESTAMP   NOT NULL,
) PRIMARY KEY (bucket, id)`

// lapsed is true of a row unrefreshed for the lease, by the database's clock.
const lapsed = `last_refresh < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lease MICROSECOND)`

// DB is a lock.DB over the locks table of the database client reaches.
func DB(client *spanner.Client) lock.DB { return db{client} }

type db struct{ c *spanner.Client }

func leaseParam() int64 { return lock.Lease.Microseconds() }

func (d db) Take(ctx context.Context, bucket, id, owner, refreshID string) (bool, error) {
	var took bool
	_, err := spannertxn.ReadWrite(ctx, d.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		took = false
		free, exists, err := rowFree(ctx, tx, bucket, id)
		if err != nil || exists && !free {
			return err
		}
		sql := `INSERT INTO locks (bucket, id, owner, refresh_id, last_refresh) VALUES (@b, @i, @o, @r, CURRENT_TIMESTAMP())`
		if exists {
			sql = `UPDATE locks SET owner = @o, refresh_id = @r, last_refresh = CURRENT_TIMESTAMP() WHERE bucket = @b AND id = @i`
		}
		if _, err := tx.Update(ctx, spanner.Statement{SQL: sql, Params: map[string]any{"b": bucket, "i": id, "o": owner, "r": refreshID}}); err != nil {
			return err
		}
		took = true
		return nil
	})
	return took, err
}

// rowFree reads a lock's row in tx: whether it is free — unrefreshed for the lease — and whether
// there is one.
func rowFree(ctx context.Context, tx *spanner.ReadWriteTransaction, bucket, id string) (free, exists bool, err error) {
	it := tx.Query(ctx, spanner.Statement{SQL: `SELECT ` + lapsed + ` FROM locks WHERE bucket = @b AND id = @i`,
		Params: map[string]any{"b": bucket, "i": id, "lease": leaseParam()}})
	defer it.Stop()
	row, err := it.Next()
	if errors.Is(err, iterator.Done) {
		return true, false, nil
	}
	if err != nil {
		return false, false, err
	}
	err = row.Columns(&free)
	return free, true, err
}

func (d db) Refresh(ctx context.Context, bucket, id, refreshID string) (bool, error) {
	var n int64
	_, err := spannertxn.ReadWrite(ctx, d.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		n, err = tx.Update(ctx, spanner.Statement{SQL: `UPDATE locks SET last_refresh = CURRENT_TIMESTAMP()
			WHERE bucket = @b AND id = @i AND refresh_id = @r`, Params: map[string]any{"b": bucket, "i": id, "r": refreshID}})
		return err
	})
	return n > 0, err
}

func (d db) Release(ctx context.Context, bucket, id, owner, refreshID string) (bool, error) {
	var n int64
	_, err := spannertxn.ReadWrite(ctx, d.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		n, err = tx.Update(ctx, spanner.Statement{SQL: `DELETE FROM locks WHERE bucket = @b AND id = @i AND owner = @o
			AND (@r = '' OR refresh_id = @r)`, Params: map[string]any{"b": bucket, "i": id, "o": owner, "r": refreshID}})
		return err
	})
	return n > 0, err
}

// InTx holds the lock id in bucket for the rest of tx, as lock.InTx does on PostgreSQL: its row —
// made, if it has none — written, so another transaction that holds it, or a lease taken meanwhile,
// waits for or is aborted by this one. A lease already held fails tx with lock.ErrHeld.
func InTx(ctx context.Context, tx *spanner.ReadWriteTransaction, bucket, id string) error {
	free, exists, err := rowFree(ctx, tx, bucket, id)
	if err != nil {
		return err
	}
	if !exists {
		_, err := tx.Update(ctx, spanner.Statement{SQL: `INSERT INTO locks (bucket, id, owner, refresh_id, last_refresh)
			VALUES (@b, @i, '', '', TIMESTAMP '0001-01-01 00:00:00+00')`, Params: map[string]any{"b": bucket, "i": id}})
		return err
	}
	if !free {
		// A row InTx made is never refreshed, so a row refreshed within the lease is a lease's.
		return lock.ErrHeld
	}
	// Written to lock it: Spanner locks a row a transaction writes until it commits.
	_, err = tx.Update(ctx, spanner.Statement{SQL: `UPDATE locks SET owner = owner WHERE bucket = @b AND id = @i`,
		Params: map[string]any{"b": bucket, "i": id}})
	return err
}

// CheckFence fails tx with data.ErrFenced unless f's lock is still held by the lease f names — its
// row carries f's refresh ID — reading that cell, so a takeover, which writes it, waits for or is
// aborted by tx. It reads no other: Spanner locks cells, and a refresh writes last_refresh, which
// a fenced transaction holding it would keep from the holder's own refreshes until the lease ran
// out, losing the lock. A lease lapsed and not taken over still fences: no one else writes.
func CheckFence(ctx context.Context, tx *spanner.ReadWriteTransaction, f data.Fence) error {
	it := tx.Query(ctx, spanner.Statement{SQL: `SELECT refresh_id = @r FROM locks WHERE bucket = @b AND id = @i`,
		Params: map[string]any{"b": f.Bucket, "i": f.ID, "r": f.Holder}})
	defer it.Stop()
	row, err := it.Next()
	if errors.Is(err, iterator.Done) {
		return fmt.Errorf("%w: %s", data.ErrFenced, f)
	}
	if err != nil {
		return err
	}
	var held bool
	if err := row.Columns(&held); err != nil {
		return err
	}
	if !held {
		return fmt.Errorf("%w: %s", data.ErrFenced, f)
	}
	return nil
}
