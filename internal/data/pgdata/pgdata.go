// Package pgdata is internal/data's PostgreSQL side: the transactions, snapshots, and listening every
// role's PostgreSQL backend does the same way.
package pgdata

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/work"
)

// Retryable reports a transaction PostgreSQL aborted to break a deadlock (40P01) or a
// serialization conflict (40001), which running again may commit.
func Retryable(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && (pg.Code == "40P01" || pg.Code == "40001")
}

// Txn runs fn in a read-write transaction on pool, again (data.Retry) when PostgreSQL aborts it to
// break a deadlock or a conflict. fn holds no effects outside the transaction but the jobs of the
// work it marks, enqueued before it commits (work.Flush).
func Txn(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	return data.Retry(ctx, Retryable, func() error {
		return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return Attempt(ctx, pool, tx, func() error { return fn(tx) }) })
	})
}

// Attempt runs fn in an attempt of tx, a transaction on pool, recording the work its operations
// mark — through tx, or anything that wraps it and reaches its connection (Conn) — and enqueueing
// its jobs before it commits: a backend that begins its own transactions runs each attempt's work
// through it.
func Attempt(ctx context.Context, pool *pgxpool.Pool, tx pgx.Tx, fn func() error) error {
	work.Begin(pool, tx.Conn())
	defer work.End(tx.Conn())
	if err := fn(); err != nil {
		return err
	}
	return work.Flush(ctx, tx.Conn())
}

// attemptOf is what a mark made through q is recorded under: the connection of the transaction q
// is, or wraps.
func attemptOf(q any) any {
	if c, ok := q.(interface{ Conn() *pgx.Conn }); ok {
		return c.Conn()
	}
	return q
}

// poolOf is the pool q is, or a pool's wrapper names — a mark through it is a transaction of its
// own.
func poolOf(q any) (*pgxpool.Pool, bool) {
	switch p := q.(type) {
	case *pgxpool.Pool:
		return p, true
	case interface{ PgxPool() *pgxpool.Pool }:
		return p.PgxPool(), true
	}
	return nil, false
}

// Snapshot runs fn in a read-only, repeatable-read transaction on pool: it sees the database as of
// one moment throughout.
func Snapshot(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}
