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
)

// Retryable reports a transaction PostgreSQL aborted to break a deadlock (40P01) or a
// serialization conflict (40001), which running again may commit.
func Retryable(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && (pg.Code == "40P01" || pg.Code == "40001")
}

// Txn runs fn in a read-write transaction on pool, again (data.Retry) when PostgreSQL aborts it to
// break a deadlock or a conflict. fn holds no effects outside the transaction.
func Txn(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	return data.Retry(ctx, Retryable, func() error { return pgx.BeginFunc(ctx, pool, fn) })
}

// Snapshot runs fn in a read-only, repeatable-read transaction on pool: it sees the database as of
// one moment throughout.
func Snapshot(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

// Listen holds a connection of its own, apart from pool's, listening on channels, and hands each
// notification on them to deliver until ctx ends or the connection fails, returning why. ready is
// called once it listens: nothing notified after is missed until Listen returns, so a caller that
// listens again re-reads whatever it waits on.
func Listen(ctx context.Context, pool *pgxpool.Pool, channels []string, ready func(), deliver func(channel, payload string)) error {
	conn, err := pgx.ConnectConfig(ctx, pool.Config().ConnConfig.Copy())
	if err != nil {
		return err
	}
	defer conn.Close(context.WithoutCancel(ctx))
	for _, ch := range channels {
		if _, err := conn.Exec(ctx, `LISTEN `+pgx.Identifier{ch}.Sanitize()); err != nil {
			return err
		}
	}
	ready()
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		deliver(n.Channel, n.Payload)
	}
}
