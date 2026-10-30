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

// Cursors are the last seqs of keys' logs on topic applied; a key with none is absent.
func Cursors(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, topic string, keys []int64) (map[int64]int64, error) {
	rows, err := q.Query(ctx, `SELECT key, seq FROM follow_cursor WHERE topic = $1 AND key = ANY($2)`, topic, keys)
	if err != nil {
		return nil, err
	}
	out := map[int64]int64{}
	var key, seq int64
	_, err = pgx.ForEachRow(rows, []any{&key, &seq}, func() error {
		out[key] = seq
		return nil
	})
	return out, err
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

// FollowersSchema makes the table a log's source keeps its followers in (internal/bus/follow): the
// roles its relay enqueues for, each as it registered.
const FollowersSchema = `CREATE TABLE follower (
    topic       text        NOT NULL,
    name        text        NOT NULL,
    followed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, name)
);`

// AddFollower records name as one of topic's followers, once.
func AddFollower(ctx context.Context, q Querier, topic, name string) error {
	_, err := q.Exec(ctx, `INSERT INTO follower (topic, name) VALUES ($1, $2) ON CONFLICT DO NOTHING`, topic, name)
	return err
}

// Followers are topic's followers, by name.
func Followers(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, topic string) ([]string, error) {
	rows, err := q.Query(ctx, `SELECT name FROM follower WHERE topic = $1 ORDER BY name`, topic)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
