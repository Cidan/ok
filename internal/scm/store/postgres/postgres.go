// Package postgres is the SCM's database (internal/scm/store/backend) on PostgreSQL, in the
// schema scm.
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are the SCM's schema.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Backend is the SCM's database on PostgreSQL.
type Backend struct {
	queries
	pool  *pgxpool.Pool
	locks *lock.Lock
}

var _ backend.Backend = (*Backend)(nil)

// Open brings the database behind pool to the SCM's schema and returns a backend over it.
func Open(ctx context.Context, pool *pgxpool.Pool) (*Backend, error) {
	if _, err := migrate.Apply(ctx, pool, "scm", Migrations); err != nil {
		return nil, err
	}
	return &Backend{queries: queries{pool}, pool: pool, locks: lock.New(pool)}, nil
}

// Pool is the backend's pool: for tests, which inspect and age rows.
func (b *Backend) Pool() *pgxpool.Pool { return b.pool }

func (b *Backend) Txn(ctx context.Context, fn func(backend.Ops) error) error {
	return pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error { return fn(queries{tx}) })
}

// Retryable reports a deadlock victim or a serialization failure.
func (b *Backend) Retryable(err error) bool {
	return pgdata.Retryable(err)
}

// Snapshot runs fn in a read-only, repeatable-read transaction.
func (b *Backend) Snapshot(ctx context.Context, fn func(backend.Ops) error) error {
	return pgdata.Snapshot(ctx, b.pool, func(tx pgx.Tx) error { return fn(queries{tx}) })
}

func (b *Backend) Lead(ctx context.Context, role string, fn lock.LockFn) error {
	return b.locks.LockFn(ctx, "leaders", role, 0, fn)
}

// Listen holds a connection of its own, apart from the pool, listening on channels.
func (b *Backend) Listen(ctx context.Context, channels []string, ready func(), deliver func(channel, payload string)) error {
	return pgdata.Listen(ctx, b.pool, channels, ready, deliver)
}

func (b *Backend) Ping(ctx context.Context) error { return b.pool.Ping(ctx) }

// RelayEvents takes pending events with FOR UPDATE SKIP LOCKED, so relays beside it take others, and
// deletes them in the transaction that took them once publish has: a relay that fails or dies
// before it commits leaves them pending.
func (b *Backend) RelayEvents(ctx context.Context, limit int, publish func([]backend.Event) error) (int, error) {
	var n int
	err := pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		evs, err := events(tx.Query(ctx, `WITH taken AS (
				SELECT repository_id, repository_seq FROM event_pending
				ORDER BY repository_id, repository_seq LIMIT $1 FOR UPDATE SKIP LOCKED)
			SELECT e.repository_id, e.repository_seq, e.body
			FROM taken t JOIN event e ON e.repository_id = t.repository_id AND e.repository_seq = t.repository_seq
			ORDER BY e.repository_id, e.repository_seq`, limit))
		if err != nil || len(evs) == 0 {
			return err
		}
		if err := publish(evs); err != nil {
			return err
		}
		repos, seqs := make([]int64, len(evs)), make([]int64, len(evs))
		for i, e := range evs {
			repos[i], seqs[i] = e.Repository, e.RepositorySeq
		}
		if _, err := tx.Exec(ctx, `DELETE FROM event_pending p USING unnest($1::bigint[], $2::bigint[]) AS u(r, s)
			WHERE p.repository_id = u.r AND p.repository_seq = u.s`, repos, seqs); err != nil {
			return err
		}
		n = len(evs)
		return nil
	})
	return n, err
}

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// queries are the backend's operations over a pool or a transaction.
type queries struct{ q querier }

func (x queries) Notify(ctx context.Context, channel, payload string) error {
	_, err := x.q.Exec(ctx, `SELECT pg_notify($1, $2)`, channel, payload)
	return err
}

// ids converts IDs to the [][]byte pgx sends as bytea[].
func ids(in []encoding.ID) [][]byte {
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = in[i][:]
	}
	return out
}

func toID(b []byte) (encoding.ID, error) {
	if len(b) != encoding.IDLen {
		return encoding.ID{}, fmt.Errorf("stored ID of %d bytes", len(b))
	}
	return encoding.ID(b), nil
}

func unique(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}

// idList reads a query's rows of one ID each.
func idList(rows pgx.Rows, err error) ([]encoding.ID, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []encoding.ID
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		id, err := toID(b)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// idSet reads a query's rows of one ID each, as a set.
func idSet(rows pgx.Rows, err error) (map[encoding.ID]bool, error) {
	list, err := idList(rows, err)
	if err != nil {
		return nil, err
	}
	out := make(map[encoding.ID]bool, len(list))
	for _, id := range list {
		out[id] = true
	}
	return out, nil
}

// bodies reads a query's rows of an ID and a body each.
func bodies(rows pgx.Rows, err error) (map[encoding.ID][]byte, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[encoding.ID][]byte{}
	for rows.Next() {
		var idb, body []byte
		if err := rows.Scan(&idb, &body); err != nil {
			return nil, err
		}
		id, err := toID(idb)
		if err != nil {
			return nil, err
		}
		out[id] = body
	}
	return out, rows.Err()
}

// objects reads a query's rows of an ID and a body each, as objects of kind, in order.
func objects(rows pgx.Rows, err error, kind encoding.Kind) ([]backend.Object, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Object
	for rows.Next() {
		var idb, body []byte
		if err := rows.Scan(&idb, &body); err != nil {
			return nil, err
		}
		id, err := toID(idb)
		if err != nil {
			return nil, err
		}
		out = append(out, backend.Object{ID: id, Kind: kind, Body: body})
	}
	return out, rows.Err()
}

func int64s(rows pgx.Rows, err error) ([]int64, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// timeOrZero is t, or the zero time for NULL.
func timeOrZero(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
