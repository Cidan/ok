// Package spannerstore is ok secrets' store backend (internal/secrets/store) on Spanner, in a
// database of its own. Every write is DML, so a transaction reads what it wrote, as on
// PostgreSQL; an operation outside a transaction is a transaction of its own.
package spannerstore

import (
	"context"
	"embed"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/work"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are ok secrets' schema on Spanner.
var Migrations = migrate.MustLoad(migrations, "migrations")

// moveTables are the tables of rows about repositories, which a move copies: every one but the
// work and the gates.
var moveTables = []string{"repository_key", "secret_version", "unseal", "approval_grant", "audit"}

// Backend is ok secrets' store backend on Spanner.
type Backend struct {
	queries
	moves *move.SpannerRows
}

var _ store.Backend = (*Backend)(nil)

// Open brings the database client reaches to ok secrets' schema, through admin, and returns a
// backend over it.
func Open(ctx context.Context, admin *database.DatabaseAdminClient, client *spanner.Client) (*Backend, error) {
	if _, err := spannerdata.Migrate(ctx, admin, client, Migrations); err != nil {
		return nil, err
	}
	return &Backend{queries: queries{spannerdata.Ops{Client: client, Gate: move.SpannerGate}},
		moves: &move.SpannerRows{Client: client, Tables: moveTables, Where: func(string) string { return "repository_id IN UNNEST(@repos)" }}}, nil
}

func (b *Backend) Txn(ctx context.Context, fn func(store.Ops) error) error {
	return spannerdata.Txn(ctx, b.Client, func(_ context.Context, tx *spanner.ReadWriteTransaction) error {
		if err := move.SpannerGate(ctx, tx); err != nil {
			return err
		}
		return fn(queries{spannerdata.Ops{Client: b.Client, Tx: tx}})
	})
}

func (b *Backend) Work() work.Store { return spannerdata.Work(b.Client) }

func (b *Backend) Moves() move.Rows { return b.moves }

// queries are the backend's operations, in a transaction or each in one of its own.
type queries struct{ spannerdata.Ops }

// foreignKeyViolation is Spanner's error for a row naming one that is not there, or a row deleted
// that one still names.
func foreignKeyViolation(err error) bool {
	return spanner.ErrCode(err) == codes.FailedPrecondition && strings.Contains(spanner.ErrDesc(err), "Foreign key")
}

func nullTime(t *time.Time) spanner.NullTime {
	if t == nil {
		return spanner.NullTime{}
	}
	return spanner.NullTime{Time: *t, Valid: true}
}

func timePtr(t spanner.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	return &t.Time
}

// ─── Repository keys ───

func (x queries) RepositoryKeys(ctx context.Context, repo int64) ([]store.RepositoryKey, error) {
	var out []store.RepositoryKey
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT repository_id, version, root_id, wrapped, created_at FROM repository_key
			WHERE repository_id = @repo ORDER BY version`, map[string]any{"repo": repo}, func(row *spanner.Row) error {
			var k store.RepositoryKey
			if err := row.Columns(&k.Repository, &k.Version, &k.Root, &k.Wrapped, &k.CreatedAt); err != nil {
				return err
			}
			out = append(out, k)
			return nil
		})
	})
	return out, err
}

func (x queries) InsertRepositoryKey(ctx context.Context, k store.RepositoryKey) (bool, error) {
	n, err := x.Exec(ctx, `INSERT OR IGNORE INTO repository_key (repository_id, version, root_id, wrapped, created_at)
		VALUES (@repo, @version, @root, @wrapped, CURRENT_TIMESTAMP())`,
		map[string]any{"repo": k.Repository, "version": k.Version, "root": k.Root, "wrapped": k.Wrapped})
	return n == 1, err
}

func (x queries) MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.MarkWorkAt(ctx, tx, kind, key, at) })
}

func (x queries) UpdateRepositoryKey(ctx context.Context, repo, version int64, root string, wrapped []byte) error {
	_, err := x.Exec(ctx, `UPDATE repository_key SET root_id = @root, wrapped = @wrapped
		WHERE repository_id = @repo AND version = @version AND wrapped IS NOT NULL`,
		map[string]any{"repo": repo, "version": version, "root": root, "wrapped": wrapped})
	return err
}

func (x queries) ShredRepositoryKeys(ctx context.Context, repo int64) error {
	_, err := x.Exec(ctx, `UPDATE repository_key SET wrapped = NULL WHERE repository_id = @repo`, map[string]any{"repo": repo})
	return err
}

func (x queries) DeleteRepositoryKey(ctx context.Context, repo, version int64) (bool, error) {
	n, err := x.Exec(ctx, `DELETE FROM repository_key WHERE repository_id = @repo AND version = @version
		AND NOT EXISTS (SELECT 1 FROM secret_version WHERE repository_id = @repo AND key_version = @version)`,
		map[string]any{"repo": repo, "version": version})
	if foreignKeyViolation(err) {
		return false, nil // a version written under it since the check
	}
	return n == 1, err
}

// ─── Versions ───

const versionColumns = `repository_id, path, digest, key_version, wrapped_key, sealed, store, written_by, written_at,
	unreferenced_at, destroyed_at, destroyed_by`

func scanVersion(row *spanner.Row) (store.Version, error) {
	var v store.Version
	var digest []byte
	var unreferenced, destroyed spanner.NullTime
	err := row.Columns(&v.Repository, &v.Path, &digest, &v.KeyVersion, &v.WrappedKey, &v.Sealed, &v.Store, &v.WrittenBy, &v.WrittenAt,
		&unreferenced, &destroyed, &v.DestroyedBy)
	copy(v.Digest[:], digest)
	v.UnreferencedAt, v.DestroyedAt = timePtr(unreferenced), timePtr(destroyed)
	return v, err
}

func (x queries) InsertVersion(ctx context.Context, v store.Version) (bool, error) {
	n, err := x.Exec(ctx, `INSERT OR IGNORE INTO secret_version (repository_id, path, digest, key_version, wrapped_key, sealed, store,
			written_by, written_at, destroyed_by)
		VALUES (@repo, @path, @digest, @version, @wrapped, @sealed, @store, @by, CURRENT_TIMESTAMP(), '')`,
		map[string]any{"repo": v.Repository, "path": v.Path, "digest": v.Digest[:], "version": v.KeyVersion, "wrapped": v.WrappedKey,
			"sealed": v.Sealed, "store": v.Store, "by": v.WrittenBy})
	if foreignKeyViolation(err) {
		return false, store.ErrNoKey
	}
	return n == 1, err
}

func (x queries) Version(ctx context.Context, repo int64, path string, digest secrets.Digest) (store.Version, bool, error) {
	var v store.Version
	var found bool
	err := x.Read(func(r spannerdata.Reader) error {
		found = false
		return spannerdata.Each(ctx, r, `SELECT `+versionColumns+` FROM secret_version
			WHERE repository_id = @repo AND path = @path AND digest = @digest`,
			map[string]any{"repo": repo, "path": path, "digest": digest[:]}, func(row *spanner.Row) error {
				var err error
				v, err = scanVersion(row)
				found = err == nil
				return err
			})
	})
	return v, found, err
}

func (x queries) Versions(ctx context.Context, repo int64, after store.VersionKey, limit int) ([]store.Version, error) {
	var out []store.Version
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT `+versionColumns+` FROM secret_version
			WHERE repository_id = @repo AND (path > @path OR (path = @path AND digest > @digest)) ORDER BY path, digest LIMIT @limit`,
			map[string]any{"repo": repo, "path": after.Path, "digest": after.Digest[:], "limit": int64(limit)}, func(row *spanner.Row) error {
				v, err := scanVersion(row)
				if err != nil {
					return err
				}
				out = append(out, v)
				return nil
			})
	})
	return out, err
}

func (x queries) SetUnreferenced(ctx context.Context, repo int64, path string, digest secrets.Digest, at *time.Time) error {
	_, err := x.Exec(ctx, `UPDATE secret_version SET unreferenced_at = @at WHERE repository_id = @repo AND path = @path AND digest = @digest`,
		map[string]any{"repo": repo, "path": path, "digest": digest[:], "at": nullTime(at)})
	return err
}

func (x queries) DestroyVersion(ctx context.Context, repo int64, path string, digest secrets.Digest, by string) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE secret_version SET wrapped_key = NULL, sealed = NULL, destroyed_at = CURRENT_TIMESTAMP(), destroyed_by = @by
		WHERE repository_id = @repo AND path = @path AND digest = @digest AND destroyed_at IS NULL`,
		map[string]any{"repo": repo, "path": path, "digest": digest[:], "by": by})
	return n == 1, err
}

func (x queries) RewrapVersion(ctx context.Context, repo int64, path string, digest secrets.Digest, keyVersion int64, wrapped []byte) error {
	// A version destroyed meanwhile keeps no data key, whatever the rewrap read of it.
	_, err := x.Exec(ctx, `UPDATE secret_version SET key_version = @version, wrapped_key = IF(destroyed_at IS NULL, @wrapped, NULL)
		WHERE repository_id = @repo AND path = @path AND digest = @digest`,
		map[string]any{"repo": repo, "path": path, "digest": digest[:], "version": keyVersion, "wrapped": wrapped})
	if foreignKeyViolation(err) {
		return store.ErrNoKey
	}
	return err
}

func (x queries) MoveVersion(ctx context.Context, repo int64, path string, digest secrets.Digest, from, to string, sealed []byte) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE secret_version SET store = @to, sealed = @sealed
		WHERE repository_id = @repo AND path = @path AND digest = @digest AND store = @from AND destroyed_at IS NULL`,
		map[string]any{"repo": repo, "path": path, "digest": digest[:], "from": from, "to": to, "sealed": sealed})
	return n == 1, err
}

// ─── Unseals ───

func (x queries) InsertUnseals(ctx context.Context, us []store.Unseal, ttl time.Duration) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		for _, u := range us {
			if _, err := spannerdata.Update(ctx, tx, `INSERT INTO unseal (repository_id, id, path, principal_id, expires_at)
				VALUES (@repo, @id, @path, @principal, TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl MICROSECOND))`,
				map[string]any{"repo": u.Repository, "id": u.ID, "path": u.Path, "principal": u.Principal, "ttl": ttl.Microseconds()}); err != nil {
				return err
			}
		}
		return nil
	})
}

func (x queries) UseUnseal(ctx context.Context, repo int64, path, principal string) (bool, error) {
	var used bool
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var id int64
		ok, err := spannerdata.First(ctx, tx, `SELECT id FROM unseal WHERE repository_id = @repo AND principal_id = @principal AND path = @path
			AND used_at IS NULL AND expires_at > CURRENT_TIMESTAMP() ORDER BY expires_at, id LIMIT 1`,
			map[string]any{"repo": repo, "principal": principal, "path": path}, &id)
		if err != nil || !ok {
			used = false
			return err
		}
		n, err := spannerdata.Update(ctx, tx, `UPDATE unseal SET used_at = CURRENT_TIMESTAMP() WHERE repository_id = @repo AND id = @id`,
			map[string]any{"repo": repo, "id": id})
		used = n == 1
		return err
	})
	return used, err
}

func (x queries) Unseals(ctx context.Context, repo int64, principal string) ([]store.Unseal, error) {
	var out []store.Unseal
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT repository_id, id, path, principal_id, expires_at, used_at FROM unseal
			WHERE repository_id = @repo AND principal_id = @principal AND used_at IS NULL AND expires_at > CURRENT_TIMESTAMP()
			ORDER BY expires_at, id`, map[string]any{"repo": repo, "principal": principal}, func(row *spanner.Row) error {
			var u store.Unseal
			var used spanner.NullTime
			if err := row.Columns(&u.Repository, &u.ID, &u.Path, &u.Principal, &u.ExpiresAt, &used); err != nil {
				return err
			}
			u.UsedAt = timePtr(used)
			out = append(out, u)
			return nil
		})
	})
	return out, err
}

func (x queries) GrantApproval(ctx context.Context, repo int64, approval string) (bool, error) {
	n, err := x.Exec(ctx, `INSERT OR IGNORE INTO approval_grant (repository_id, approval_id, granted_at) VALUES (@repo, @approval, CURRENT_TIMESTAMP())`,
		map[string]any{"repo": repo, "approval": approval})
	return n == 1, err
}

func (x queries) DeleteUnseals(ctx context.Context, repo int64, before time.Time, limit int) (int64, error) {
	var n int64
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		n = 0
		var ms []*spanner.Mutation
		if err := spannerdata.Each(ctx, tx, `SELECT id FROM unseal WHERE repository_id = @repo AND expires_at < @before LIMIT @limit`,
			map[string]any{"repo": repo, "before": before, "limit": int64(limit)}, func(row *spanner.Row) error {
				var id int64
				if err := row.Columns(&id); err != nil {
					return err
				}
				ms = append(ms, spanner.Delete("unseal", spanner.Key{repo, id}))
				return nil
			}); err != nil {
			return err
		}
		n = int64(len(ms))
		return tx.BufferWrite(ms)
	})
	return n, err
}

// ─── Audit ───

func (x queries) InsertAudit(ctx context.Context, e store.AuditEntry) error {
	_, err := x.Exec(ctx, `INSERT INTO audit (repository_id, id, decided_at, actor_id, for_id, op, path, digest, outcome)
		VALUES (@repo, @id, CURRENT_TIMESTAMP(), @actor, @for, @op, @path, @digest, @outcome)`,
		map[string]any{"repo": e.Repository, "id": e.ID, "actor": e.Actor, "for": e.For, "op": e.Op, "path": e.Path, "digest": e.Digest,
			"outcome": e.Outcome})
	return err
}

func (x queries) Audit(ctx context.Context, repo, after int64, limit int) ([]store.AuditEntry, error) {
	var out []store.AuditEntry
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		p := map[string]any{"repo": repo, "after": after, "limit": int64(limit), "at": time.Time{}}
		if after != 0 {
			var at time.Time
			ok, err := spannerdata.First(ctx, r, `SELECT decided_at FROM audit WHERE repository_id = @repo AND id = @after`, p, &at)
			if err != nil || !ok {
				return err
			}
			p["at"] = at
		}
		return spannerdata.Each(ctx, r, `SELECT repository_id, id, decided_at, actor_id, for_id, op, path, digest, outcome FROM audit
			WHERE repository_id = @repo AND (@after = 0 OR decided_at > @at OR (decided_at = @at AND id > @after))
			ORDER BY decided_at, id LIMIT @limit`, p, func(row *spanner.Row) error {
			var e store.AuditEntry
			if err := row.Columns(&e.Repository, &e.ID, &e.At, &e.Actor, &e.For, &e.Op, &e.Path, &e.Digest, &e.Outcome); err != nil {
				return err
			}
			out = append(out, e)
			return nil
		})
	})
	return out, err
}

// ─── Repositories ───

func (x queries) Repositories(ctx context.Context, after int64, limit int) ([]int64, error) {
	var out []int64
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT DISTINCT repository_id FROM repository_key WHERE repository_id > @after
			ORDER BY repository_id LIMIT @limit`, map[string]any{"after": after, "limit": int64(limit)}, func(row *spanner.Row) error {
			var id int64
			if err := row.Columns(&id); err != nil {
				return err
			}
			out = append(out, id)
			return nil
		})
	})
	return out, err
}

// dropKeys are each table's primary key below the repository, as DropRepository reads and
// deletes them: children before the keys their rows name.
var dropKeys = []struct {
	table, columns string
	key            func(repo int64, row *spanner.Row) (spanner.Key, error)
}{
	{"secret_version", "path, digest", func(repo int64, row *spanner.Row) (spanner.Key, error) {
		var path string
		var digest []byte
		err := row.Columns(&path, &digest)
		return spanner.Key{repo, path, digest}, err
	}},
	{"unseal", "id", int64Key},
	{"approval_grant", "approval_id", func(repo int64, row *spanner.Row) (spanner.Key, error) {
		var id string
		err := row.Columns(&id)
		return spanner.Key{repo, id}, err
	}},
	{"audit", "id", int64Key},
	{"repository_key", "version", int64Key},
}

func int64Key(repo int64, row *spanner.Row) (spanner.Key, error) {
	var id int64
	err := row.Columns(&id)
	return spanner.Key{repo, id}, err
}

func (x queries) DropRepository(ctx context.Context, repo int64, limit int) (int64, error) {
	var n int64
	for _, t := range dropKeys {
		if n >= int64(limit) {
			break
		}
		var d int64
		err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
			var ms []*spanner.Mutation
			if err := spannerdata.Each(ctx, tx, `SELECT `+t.columns+` FROM `+t.table+` WHERE repository_id = @repo LIMIT @limit`,
				map[string]any{"repo": repo, "limit": int64(limit) - n}, func(row *spanner.Row) error {
					k, err := t.key(repo, row)
					if err != nil {
						return err
					}
					ms = append(ms, spanner.Delete(t.table, k))
					return nil
				}); err != nil {
				return err
			}
			d = int64(len(ms))
			return tx.BufferWrite(ms)
		})
		if err != nil {
			return n, err
		}
		n += d
	}
	return n, nil
}
