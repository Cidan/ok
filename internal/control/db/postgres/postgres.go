// Package postgres is ok control's store (internal/control/db) on PostgreSQL, in the schema
// control.
package postgres

import (
	"context"
	"embed"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/migrate"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are ok control's schema.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Store is ok control's store on PostgreSQL.
type Store struct {
	queries
	pool  *pgxpool.Pool
	locks *lock.Lock
}

var _ db.Store = (*Store)(nil)

// Open brings the database behind pool to ok control's schema and returns its store.
func Open(ctx context.Context, pool *pgxpool.Pool) (*Store, error) {
	if _, err := migrate.Apply(ctx, pool, "control", Migrations); err != nil {
		return nil, err
	}
	return &Store{queries: queries{pool}, pool: pool, locks: lock.New(pool)}, nil
}

// Pool is the underlying pool.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Txn runs fn in a transaction, again if PostgreSQL aborts it to break a deadlock or a
// serialization conflict.
func (s *Store) Txn(ctx context.Context, fn func(db.Ops) error) error {
	return pgdata.Txn(ctx, s.pool, func(tx pgx.Tx) error { return fn(queries{tx}) })
}

// querier runs queries, inside a transaction or not.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// queries are the store's operations over a pool or a transaction.
type queries struct{ q querier }

const clColumns = `repository_id, number, change_id, author, status, coalesce(parent_number, 0), current_patchset,
	description, landed_commit, coalesce(submitted_by, ''), approved, created_at, updated_at, rolls_back`

func scanCL(row pgx.Row) (db.ChangeList, error) {
	var c db.ChangeList
	err := row.Scan(&c.Repo, &c.Number, &c.ChangeID, &c.Author, &c.Status, &c.ParentNumber, &c.CurrentPatchset,
		&c.Description, &c.LandedCommit, &c.SubmittedBy, &c.Approved, &c.CreatedAt, &c.UpdatedAt, &c.RollsBack)
	return c, err
}

func collectCLs(rows pgx.Rows, err error) ([]db.ChangeList, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []db.ChangeList
	for rows.Next() {
		c, err := scanCL(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (x queries) UpsertChangeList(ctx context.Context, c db.ChangeList) (bool, error) {
	var parent *int64
	if c.ParentNumber != 0 {
		parent = &c.ParentNumber
	}
	var submitted *string
	if c.SubmittedBy != "" {
		submitted = &c.SubmittedBy
	}
	rollsBack := c.RollsBack
	if rollsBack == nil {
		rollsBack = []int64{}
	}
	tag, err := x.q.Exec(ctx, `INSERT INTO changelist (repository_id, number, change_id, author, status, parent_number,
			current_patchset, description, landed_commit, submitted_by, created_at, updated_at, rolls_back)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (repository_id, number) DO UPDATE SET
			status = EXCLUDED.status, parent_number = EXCLUDED.parent_number,
			current_patchset = EXCLUDED.current_patchset, description = EXCLUDED.description,
			landed_commit = EXCLUDED.landed_commit, submitted_by = EXCLUDED.submitted_by,
			updated_at = EXCLUDED.updated_at
		WHERE changelist.updated_at <= EXCLUDED.updated_at`,
		c.Repo, c.Number, c.ChangeID, c.Author, c.Status, parent, c.CurrentPatchset, c.Description,
		c.LandedCommit, submitted, c.CreatedAt, c.UpdatedAt, rollsBack)
	return tag.RowsAffected() > 0, err
}

func (x queries) RollbacksOf(ctx context.Context, repo, number int64) ([]db.ChangeList, error) {
	return collectCLs(x.q.Query(ctx, `SELECT `+clColumns+` FROM changelist
		WHERE repository_id = $1 AND rolls_back @> ARRAY[$2::bigint] ORDER BY number`, repo, number))
}

func (x queries) GetChangeList(ctx context.Context, repo, number int64) (db.ChangeList, bool, error) {
	return oneCL(x.q.QueryRow(ctx, `SELECT `+clColumns+` FROM changelist WHERE repository_id = $1 AND number = $2`, repo, number))
}

func (x queries) LockChangeList(ctx context.Context, repo, number int64) (db.ChangeList, bool, error) {
	return oneCL(x.q.QueryRow(ctx, `SELECT `+clColumns+` FROM changelist WHERE repository_id = $1 AND number = $2 FOR UPDATE`, repo, number))
}

func oneCL(row pgx.Row) (db.ChangeList, bool, error) {
	c, err := scanCL(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.ChangeList{}, false, nil
	}
	return c, err == nil, err
}

func (x queries) ListChangeLists(ctx context.Context, repo int64, f db.Filter, viewer string, limit int) ([]db.ChangeList, error) {
	where := `repository_id = $1`
	args := []any{repo}
	switch f {
	case db.FilterOpen:
		where += ` AND (status = 2 OR (status = 1 AND author = $2))`
		args = append(args, viewer)
	case db.FilterDrafts:
		where += ` AND status = 1`
	case db.FilterNeedsReview:
		where += ` AND status = 2 AND EXISTS (SELECT 1 FROM reviewer r WHERE r.repository_id = changelist.repository_id
			AND r.number = changelist.number AND r.principal = $2)
			AND NOT EXISTS (SELECT 1 FROM label_grant g WHERE g.repository_id = changelist.repository_id
			AND g.number = changelist.number AND g.principal = $2 AND g.granted)`
		args = append(args, viewer)
	case db.FilterMerged:
		where += ` AND status = 3`
	case db.FilterAbandoned:
		where += ` AND status = 4`
	}
	args = append(args, limit)
	return collectCLs(x.q.Query(ctx, `SELECT `+clColumns+` FROM changelist WHERE `+where+
		` ORDER BY updated_at DESC, number DESC LIMIT $`+strconv.Itoa(len(args)), args...))
}

func (x queries) ActiveChangeLists(ctx context.Context, repo int64) ([]db.ChangeList, error) {
	return collectCLs(x.q.Query(ctx, `SELECT `+clColumns+` FROM changelist WHERE repository_id = $1 AND status IN (1, 2) ORDER BY number`, repo))
}

func (x queries) Stack(ctx context.Context, repo, number int64) ([]db.ChangeList, error) {
	anc, err := collectCLs(x.q.Query(ctx, `WITH RECURSIVE up(number, depth) AS (
			SELECT $2::bigint, 0
			UNION ALL
			SELECT c.parent_number, up.depth + 1 FROM up JOIN changelist c ON c.repository_id = $1 AND c.number = up.number
			WHERE c.parent_number IS NOT NULL
		)
		SELECT `+clColumns+` FROM changelist JOIN up USING (number) WHERE repository_id = $1 ORDER BY up.depth DESC`, repo, number))
	if err != nil {
		return nil, err
	}
	desc, err := collectCLs(x.q.Query(ctx, `WITH RECURSIVE down(number, depth) AS (
			SELECT number, 1 FROM changelist WHERE repository_id = $1 AND parent_number = $2
			UNION ALL
			SELECT c.number, down.depth + 1 FROM down JOIN changelist c ON c.repository_id = $1 AND c.parent_number = down.number
		)
		SELECT `+clColumns+` FROM changelist JOIN down USING (number) WHERE repository_id = $1 ORDER BY down.depth, number`, repo, number))
	if err != nil {
		return nil, err
	}
	return append(anc, desc...), nil
}

func (x queries) LandedBy(ctx context.Context, repo int64, commits [][]byte) (map[string]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT landed_commit, number FROM changelist WHERE repository_id = $1 AND landed_commit = ANY($2::bytea[])`, repo, commits)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var c []byte
		var n int64
		if err := rows.Scan(&c, &n); err != nil {
			return nil, err
		}
		out[string(c)] = n
	}
	return out, rows.Err()
}

func (x queries) SetApproved(ctx context.Context, repo, number int64, approved bool) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE changelist SET approved = $3 WHERE repository_id = $1 AND number = $2 AND approved <> $3`, repo, number, approved)
	return tag.RowsAffected() > 0, err
}

func (x queries) LegacyCursor(ctx context.Context) (int64, error) {
	var seq int64
	err := x.q.QueryRow(ctx, `SELECT coalesce((SELECT seq FROM feed_cursor), 0)`).Scan(&seq)
	return seq, err
}

func (x queries) ForgetLegacyCursor(ctx context.Context) error {
	_, err := x.q.Exec(ctx, `DELETE FROM feed_cursor`)
	return err
}

func (x queries) FollowCursor(ctx context.Context, topic string, key int64) (int64, error) {
	return pgdata.Cursor(ctx, x.q, topic, key)
}

func (x queries) LockFollowCursor(ctx context.Context, topic string, key int64) (int64, error) {
	return pgdata.LockCursor(ctx, x.q, topic, key)
}

func (x queries) SetFollowCursor(ctx context.Context, topic string, key, seq int64) error {
	return pgdata.SetCursor(ctx, x.q, topic, key, seq)
}

func (x queries) SeedFollowCursor(ctx context.Context, topic string, key, seq int64) error {
	return pgdata.SeedCursor(ctx, x.q, topic, key, seq)
}

// CreateRepository leaves a repository already recorded as it is: a conflict clause naming only
// the ID would let the second of two concurrent inserts fail on the name instead.
func (x queries) CreateRepository(ctx context.Context, r db.Repository) error {
	_, err := x.q.Exec(ctx, `INSERT INTO repository (id, name, created_by, created_at, head_seq, updated_at)
		VALUES ($1, $2, $3, $4, $5, $4) ON CONFLICT DO NOTHING`, r.ID, r.Name, r.CreatedBy, r.CreatedAt, r.HeadSeq)
	return err
}

func (x queries) RenameRepository(ctx context.Context, id int64, name string) error {
	_, err := x.q.Exec(ctx, `UPDATE repository SET name = $2 WHERE id = $1`, id, name)
	return err
}

func (x queries) MoveHead(ctx context.Context, id, headSeq int64, at time.Time) error {
	_, err := x.q.Exec(ctx, `UPDATE repository SET head_seq = $2, updated_at = $3 WHERE id = $1 AND head_seq < $2`, id, headSeq, at)
	return err
}

func (x queries) RepositoryByName(ctx context.Context, name string) (db.Repository, bool, error) {
	var r db.Repository
	err := x.q.QueryRow(ctx, `SELECT id, name, created_by, created_at, head_seq, updated_at FROM repository WHERE name = $1`, name).
		Scan(&r.ID, &r.Name, &r.CreatedBy, &r.CreatedAt, &r.HeadSeq, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.Repository{}, false, nil
	}
	return r, err == nil, err
}

func (x queries) ListRepositories(ctx context.Context, principal, keyword string, offset, limit int) ([]db.Repository, error) {
	pattern := "%" + strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(strings.ToLower(keyword)) + "%"
	rows, err := x.q.Query(ctx, `SELECT r.id, r.name, r.created_by, r.created_at, r.head_seq, r.updated_at, f.principal IS NOT NULL
		FROM repository r LEFT JOIN favorite f ON f.repository_id = r.id AND f.principal = $1
		WHERE r.name LIKE $2
		ORDER BY f.principal IS NOT NULL DESC, r.updated_at DESC, r.id
		OFFSET $3 LIMIT $4`, principal, pattern, offset, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []db.Repository
	for rows.Next() {
		var r db.Repository
		if err := rows.Scan(&r.ID, &r.Name, &r.CreatedBy, &r.CreatedAt, &r.HeadSeq, &r.UpdatedAt, &r.Favorite); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
