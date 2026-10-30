// Package postgres is ok secrets' store backend (internal/secrets/store) on PostgreSQL, in the
// schema secrets.
package postgres

import (
	"context"
	"embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/work"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are ok secrets' schema.
var Migrations = migrate.MustLoad(migrations, "migrations")

// moveTables are the tables of rows about repositories, which a move copies (docs/hosted.md,
// "Cells, as built"): every one but the work and the gates.
var moveTables = []string{"repository_key", "secret_version", "unseal", "approval_grant", "audit"}

var moveTableSet = func() map[string]bool {
	m := map[string]bool{}
	for _, t := range moveTables {
		m[t] = true
	}
	return m
}()

// Backend is ok secrets' store backend on PostgreSQL.
type Backend struct {
	queries
	pool  *pgxpool.Pool
	moves *move.PostgresRows
}

var _ store.Backend = (*Backend)(nil)

// Open brings the database behind pool to ok secrets' schema and returns a backend over it.
func Open(ctx context.Context, pool *pgxpool.Pool) (*Backend, error) {
	if _, err := migrate.Apply(ctx, pool, "secrets", Migrations); err != nil {
		return nil, err
	}
	return &Backend{queries: queries{move.GatedPool{Pool: pool, Tables: moveTableSet}}, pool: pool,
		moves: &move.PostgresRows{Pool: pool, Tables: moveTables, Where: func(string) string { return "repository_id = ANY(@repos)" }}}, nil
}

// Txn runs fn in a transaction, again if PostgreSQL aborts it to break a deadlock or a
// serialization conflict.
func (b *Backend) Txn(ctx context.Context, fn func(store.Ops) error) error {
	return pgdata.Txn(ctx, b.pool, func(tx pgx.Tx) error {
		// What ctx's lease guards is written only while its holder holds it (data.Fence).
		if err := pgdata.CheckFence(ctx, tx); err != nil {
			return err
		}
		if err := move.PostgresGate(ctx, tx); err != nil {
			return err
		}
		return fn(queries{&move.CheckedTx{Tx: tx, Tables: moveTableSet}})
	})
}

func (b *Backend) Work() work.Store { return pgdata.Work(b.pool) }

func (b *Backend) Moves() move.Rows { return b.moves }

// querier is what the operations run on: the gated pool, or a transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// queries are the backend's operations, in a transaction or each on its own.
type queries struct{ q querier }

// foreignKeyViolation is PostgreSQL's error for a row naming one that is not there, or a row
// deleted that one still names.
func foreignKeyViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23503"
}

// ─── Repository keys ───

func (x queries) RepositoryKeys(ctx context.Context, repo int64) ([]store.RepositoryKey, error) {
	rows, err := x.q.Query(ctx, `SELECT repository_id, version, root_id, wrapped, created_at FROM repository_key
		WHERE repository_id = $1 ORDER BY version`, repo)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.RepositoryKey, error) {
		var k store.RepositoryKey
		err := r.Scan(&k.Repository, &k.Version, &k.Root, &k.Wrapped, &k.CreatedAt)
		return k, err
	})
}

func (x queries) InsertRepositoryKey(ctx context.Context, k store.RepositoryKey) (bool, error) {
	tag, err := x.q.Exec(ctx, `INSERT INTO repository_key (repository_id, version, root_id, wrapped) VALUES ($1, $2, $3, $4)
		ON CONFLICT (repository_id, version) DO NOTHING`, k.Repository, k.Version, k.Root, k.Wrapped)
	return tag.RowsAffected() == 1, err
}

func (x queries) MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return pgdata.MarkWorkAt(ctx, x.q, kind, key, at)
}

func (x queries) UpdateRepositoryKey(ctx context.Context, repo, version int64, root string, wrapped []byte) error {
	_, err := x.q.Exec(ctx, `UPDATE repository_key SET root_id = $3, wrapped = $4
		WHERE repository_id = $1 AND version = $2 AND wrapped IS NOT NULL`, repo, version, root, wrapped)
	return err
}

func (x queries) ShredRepositoryKeys(ctx context.Context, repo int64) error {
	_, err := x.q.Exec(ctx, `UPDATE repository_key SET wrapped = NULL WHERE repository_id = $1`, repo)
	return err
}

func (x queries) DeleteRepositoryKey(ctx context.Context, repo, version int64) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM repository_key WHERE repository_id = $1 AND version = $2
		AND NOT EXISTS (SELECT 1 FROM secret_version WHERE repository_id = $1 AND key_version = $2)`, repo, version)
	if foreignKeyViolation(err) {
		return false, nil // a version written under it since the check
	}
	return tag.RowsAffected() == 1, err
}

// ─── Versions ───

const versionColumns = `repository_id, path, digest, key_version, wrapped_key, sealed, store, written_by, written_at,
	unreferenced_at, destroyed_at, destroyed_by`

func scanVersion(r pgx.Row) (store.Version, error) {
	var v store.Version
	var digest []byte
	err := r.Scan(&v.Repository, &v.Path, &digest, &v.KeyVersion, &v.WrappedKey, &v.Sealed, &v.Store, &v.WrittenBy, &v.WrittenAt,
		&v.UnreferencedAt, &v.DestroyedAt, &v.DestroyedBy)
	copy(v.Digest[:], digest)
	return v, err
}

func (x queries) InsertVersion(ctx context.Context, v store.Version) (bool, error) {
	tag, err := x.q.Exec(ctx, `INSERT INTO secret_version (repository_id, path, digest, key_version, wrapped_key, sealed, store, written_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (repository_id, path, digest) DO NOTHING`,
		v.Repository, v.Path, v.Digest[:], v.KeyVersion, v.WrappedKey, v.Sealed, v.Store, v.WrittenBy)
	if foreignKeyViolation(err) {
		return false, store.ErrNoKey
	}
	return tag.RowsAffected() == 1, err
}

func (x queries) Version(ctx context.Context, repo int64, path string, digest secrets.Digest) (store.Version, bool, error) {
	v, err := scanVersion(x.q.QueryRow(ctx, `SELECT `+versionColumns+` FROM secret_version
		WHERE repository_id = $1 AND path = $2 AND digest = $3`, repo, path, digest[:]))
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Version{}, false, nil
	}
	return v, err == nil, err
}

func (x queries) Versions(ctx context.Context, repo int64, after store.VersionKey, limit int) ([]store.Version, error) {
	rows, err := x.q.Query(ctx, `SELECT `+versionColumns+` FROM secret_version
		WHERE repository_id = $1 AND (path, digest) > ($2, $3) ORDER BY path, digest LIMIT $4`, repo, after.Path, after.Digest[:], limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.Version, error) { return scanVersion(r) })
}

func (x queries) SetUnreferenced(ctx context.Context, repo int64, path string, digest secrets.Digest, at *time.Time) error {
	_, err := x.q.Exec(ctx, `UPDATE secret_version SET unreferenced_at = $4 WHERE repository_id = $1 AND path = $2 AND digest = $3`,
		repo, path, digest[:], at)
	return err
}

func (x queries) DestroyVersion(ctx context.Context, repo int64, path string, digest secrets.Digest, by string) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE secret_version SET wrapped_key = NULL, sealed = NULL, destroyed_at = now(), destroyed_by = $4
		WHERE repository_id = $1 AND path = $2 AND digest = $3 AND destroyed_at IS NULL`, repo, path, digest[:], by)
	return tag.RowsAffected() == 1, err
}

func (x queries) RewrapVersion(ctx context.Context, repo int64, path string, digest secrets.Digest, keyVersion int64, wrapped []byte) error {
	// A version destroyed meanwhile keeps no data key, whatever the rewrap read of it.
	_, err := x.q.Exec(ctx, `UPDATE secret_version SET key_version = $4, wrapped_key = CASE WHEN destroyed_at IS NULL THEN $5::bytea END
		WHERE repository_id = $1 AND path = $2 AND digest = $3`, repo, path, digest[:], keyVersion, wrapped)
	if foreignKeyViolation(err) {
		return store.ErrNoKey
	}
	return err
}

func (x queries) MoveVersion(ctx context.Context, repo int64, path string, digest secrets.Digest, from, to string, sealed []byte) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE secret_version SET store = $5, sealed = $6
		WHERE repository_id = $1 AND path = $2 AND digest = $3 AND store = $4 AND destroyed_at IS NULL`, repo, path, digest[:], from, to, sealed)
	return tag.RowsAffected() == 1, err
}

// ─── Unseals ───

func (x queries) InsertUnseals(ctx context.Context, us []store.Unseal, ttl time.Duration) error {
	for _, u := range us {
		if _, err := x.q.Exec(ctx, `INSERT INTO unseal (repository_id, id, path, principal_id, expires_at)
			VALUES ($1, $2, $3, $4, now() + make_interval(secs => $5))`, u.Repository, u.ID, u.Path, u.Principal, ttl.Seconds()); err != nil {
			return err
		}
	}
	return nil
}

func (x queries) UseUnseal(ctx context.Context, repo int64, path, principal string) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE unseal SET used_at = now() WHERE repository_id = $1 AND id = (
			SELECT id FROM unseal WHERE repository_id = $1 AND principal_id = $2 AND path = $3 AND used_at IS NULL AND expires_at > now()
			ORDER BY expires_at, id LIMIT 1 FOR UPDATE SKIP LOCKED)`, repo, principal, path)
	return tag.RowsAffected() == 1, err
}

func (x queries) Unseals(ctx context.Context, repo int64, principal string) ([]store.Unseal, error) {
	rows, err := x.q.Query(ctx, `SELECT repository_id, id, path, principal_id, expires_at, used_at FROM unseal
		WHERE repository_id = $1 AND principal_id = $2 AND used_at IS NULL AND expires_at > now() ORDER BY expires_at, id`, repo, principal)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.Unseal, error) {
		var u store.Unseal
		err := r.Scan(&u.Repository, &u.ID, &u.Path, &u.Principal, &u.ExpiresAt, &u.UsedAt)
		return u, err
	})
}

func (x queries) GrantApproval(ctx context.Context, repo int64, approval string) (bool, error) {
	tag, err := x.q.Exec(ctx, `INSERT INTO approval_grant (repository_id, approval_id) VALUES ($1, $2)
		ON CONFLICT (repository_id, approval_id) DO NOTHING`, repo, approval)
	return tag.RowsAffected() == 1, err
}

func (x queries) DeleteUnseals(ctx context.Context, repo int64, before time.Time, limit int) (int64, error) {
	return pgdata.DeleteRows(ctx, x.q, "unseal", "repository_id = @repo AND expires_at < @before",
		pgx.NamedArgs{"repo": repo, "before": before}, limit)
}

// ─── Audit ───

func (x queries) InsertAudit(ctx context.Context, e store.AuditEntry) error {
	_, err := x.q.Exec(ctx, `INSERT INTO audit (repository_id, id, actor_id, for_id, op, path, digest, outcome)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, e.Repository, e.ID, e.Actor, e.For, e.Op, e.Path, e.Digest, e.Outcome)
	return err
}

func (x queries) Audit(ctx context.Context, repo, after int64, limit int) ([]store.AuditEntry, error) {
	rows, err := x.q.Query(ctx, `SELECT repository_id, id, decided_at, actor_id, for_id, op, path, digest, outcome FROM audit a
		WHERE repository_id = $1 AND ($2::bigint = 0 OR (decided_at, id) > (SELECT decided_at, id FROM audit WHERE repository_id = $1 AND id = $2))
		ORDER BY decided_at, id LIMIT $3`, repo, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.AuditEntry, error) {
		var e store.AuditEntry
		err := r.Scan(&e.Repository, &e.ID, &e.At, &e.Actor, &e.For, &e.Op, &e.Path, &e.Digest, &e.Outcome)
		return e, err
	})
}

// ─── Repositories ───

func (x queries) Repositories(ctx context.Context, after int64, limit int) ([]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT DISTINCT repository_id FROM repository_key WHERE repository_id > $1
		ORDER BY repository_id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func (x queries) DropRepository(ctx context.Context, repo int64, limit int) (int64, error) {
	var n int64
	for _, table := range []string{"secret_version", "unseal", "approval_grant", "audit", "repository_key"} {
		if n >= int64(limit) {
			break
		}
		d, err := pgdata.DeleteRows(ctx, x.q, table, "repository_id = @repo", pgx.NamedArgs{"repo": repo}, limit-int(n))
		if err != nil {
			return n, err
		}
		n += d
	}
	return n, nil
}
