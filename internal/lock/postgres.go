package lock

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/data"
)

// Schema makes the locks table on PostgreSQL; a role that takes locks includes it in its
// migrations.
const Schema = `CREATE TABLE IF NOT EXISTS locks (
    bucket       text NOT NULL,
    id           text NOT NULL,
    owner        text NOT NULL,
    refresh_id   text NOT NULL,
    last_refresh timestamptz NOT NULL,
    PRIMARY KEY (bucket, id)
);`

// Postgres is a DB over the locks table of the PostgreSQL database pool reaches.
func Postgres(pool *pgxpool.Pool) DB { return postgresDB{pool} }

type postgresDB struct{ pool *pgxpool.Pool }

// Take locks the row FOR UPDATE before taking it over, which waits for a fenced transaction's FOR KEY
// SHARE (CheckFence): the upsert's own row lock, FOR NO KEY UPDATE, would not.
func (p postgresDB) Take(ctx context.Context, bucket, id, owner, refreshID string) (bool, error) {
	took := false
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT 1 FROM locks WHERE bucket = $1 AND id = $2 FOR UPDATE`, bucket, id); err != nil {
			return err
		}
		err := tx.QueryRow(ctx, `INSERT INTO locks (bucket, id, owner, refresh_id, last_refresh)
			VALUES ($1, $2, $3, $4, now())
			ON CONFLICT (bucket, id) DO UPDATE SET owner = EXCLUDED.owner, refresh_id = EXCLUDED.refresh_id,
				last_refresh = EXCLUDED.last_refresh
			WHERE locks.last_refresh < now() - make_interval(secs => $5)
			RETURNING refresh_id`, bucket, id, owner, refreshID, Lease.Seconds()).Scan(new(string))
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		took = err == nil
		return err
	})
	return took, err
}

func (p postgresDB) Refresh(ctx context.Context, bucket, id, refreshID string) (bool, error) {
	tag, err := p.pool.Exec(ctx, `UPDATE locks SET last_refresh = now() WHERE bucket = $1 AND id = $2 AND refresh_id = $3`,
		bucket, id, refreshID)
	return tag.RowsAffected() > 0, err
}

func (p postgresDB) Release(ctx context.Context, bucket, id, owner, refreshID string) (bool, error) {
	tag, err := p.pool.Exec(ctx, `DELETE FROM locks WHERE bucket = $1 AND id = $2 AND owner = $3 AND ($4 = '' OR refresh_id = $4)`,
		bucket, id, owner, refreshID)
	return tag.RowsAffected() > 0, err
}

// Ensure makes the locks table in the schema pool reaches, if it is not there: what a role's
// migrations need before they run, under a lock. Two replicas making it at once are both done.
func Ensure(ctx context.Context, pool *pgxpool.Pool) error {
	for range 3 {
		_, err := pool.Exec(ctx, Schema)
		// CREATE TABLE IF NOT EXISTS fails in all but one of several at once: unique_violation on
		// the catalog, duplicate_table, or duplicate_object (its row type). The next try finds the
		// table.
		var pg *pgconn.PgError
		if errors.As(err, &pg) && (pg.Code == "23505" || pg.Code == "42P07" || pg.Code == "42710") {
			continue
		}
		return err
	}
	return errors.New("lock: the locks table was made and unmade three times over")
}

// Querier is what InTx locks through on PostgreSQL: a transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// InTx holds the lock id in bucket for the rest of the PostgreSQL transaction q is: its row — made,
// if it has none — locked until the transaction commits or rolls back, waiting for another
// transaction that holds it. Nothing refreshes it and nothing within the transaction can lose it.
// A lease taken meanwhile waits for the transaction to end; one already held fails the transaction
// with ErrHeld. The row stays, free to the next holder of either kind.
func InTx(ctx context.Context, q Querier, bucket, id string) error {
	if _, err := q.Exec(ctx, `INSERT INTO locks (bucket, id, owner, refresh_id, last_refresh) VALUES ($1, $2, '', '', '-infinity')
		ON CONFLICT (bucket, id) DO NOTHING`, bucket, id); err != nil {
		return err
	}
	var leased bool
	if err := q.QueryRow(ctx, `SELECT owner <> '' AND last_refresh >= now() - make_interval(secs => $3) FROM locks
		WHERE bucket = $1 AND id = $2 FOR UPDATE`, bucket, id, Lease.Seconds()).Scan(&leased); err != nil {
		return err
	}
	if leased {
		return ErrHeld
	}
	return nil
}

// CheckFence fails the PostgreSQL transaction q is with data.ErrFenced unless f's lock is still
// held by the lease f names — its row carries f's refresh ID — and holds the row against a
// takeover until the transaction ends: FOR KEY SHARE, which a takeover's FOR UPDATE waits for and
// the holder's own refreshes, plain updates, do not — a fenced transaction holding them off would
// lose the lock once its lease ran out. A lease lapsed and not taken over still fences: no one
// else writes.
func CheckFence(ctx context.Context, q Querier, f data.Fence) error {
	var held bool
	err := q.QueryRow(ctx, `SELECT refresh_id = $3 FROM locks
		WHERE bucket = $1 AND id = $2 FOR KEY SHARE`, f.Bucket, f.ID, f.Holder).Scan(&held)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !held {
		return fmt.Errorf("%w: %s", data.ErrFenced, f)
	}
	return err
}
