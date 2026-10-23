package pgdata

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// CursorsSchema makes the table a role that follows topics of the bus's stream keeps its place in
// each key's log in (internal/bus/follow); a role that follows includes it in its migrations.
const CursorsSchema = `CREATE TABLE follow_cursor (
    topic text   NOT NULL,
    key   bigint NOT NULL,
    seq   bigint NOT NULL,
    PRIMARY KEY (topic, key)
);`

// Querier is a pool or a transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Cursor is the last seq of key's log on topic applied (0: none).
func Cursor(ctx context.Context, q Querier, topic string, key int64) (int64, error) {
	var seq int64
	err := q.QueryRow(ctx, `SELECT seq FROM follow_cursor WHERE topic = $1 AND key = $2`, topic, key).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

// LockCursor is Cursor, held until the transaction q is ends: it makes key's row, at 0, if there is
// none, so two transactions applying key's first event wait on one another.
func LockCursor(ctx context.Context, q Querier, topic string, key int64) (int64, error) {
	if _, err := q.Exec(ctx, `INSERT INTO follow_cursor (topic, key, seq) VALUES ($1, $2, 0) ON CONFLICT DO NOTHING`, topic, key); err != nil {
		return 0, err
	}
	var seq int64
	err := q.QueryRow(ctx, `SELECT seq FROM follow_cursor WHERE topic = $1 AND key = $2 FOR UPDATE`, topic, key).Scan(&seq)
	return seq, err
}

// SetCursor records seq as the last of key's log applied.
func SetCursor(ctx context.Context, q Querier, topic string, key, seq int64) error {
	_, err := q.Exec(ctx, `INSERT INTO follow_cursor (topic, key, seq) VALUES ($1, $2, $3)
		ON CONFLICT (topic, key) DO UPDATE SET seq = EXCLUDED.seq`, topic, key, seq)
	return err
}

// AdvanceCursor moves key's cursor to seq, never back.
func AdvanceCursor(ctx context.Context, q Querier, topic string, key, seq int64) error {
	_, err := q.Exec(ctx, `INSERT INTO follow_cursor (topic, key, seq) VALUES ($1, $2, $3)
		ON CONFLICT (topic, key) DO UPDATE SET seq = greatest(follow_cursor.seq, EXCLUDED.seq)`, topic, key, seq)
	return err
}

// SeedCursor sets key's cursor to seq, if it has none.
func SeedCursor(ctx context.Context, q Querier, topic string, key, seq int64) error {
	_, err := q.Exec(ctx, `INSERT INTO follow_cursor (topic, key, seq) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, topic, key, seq)
	return err
}
