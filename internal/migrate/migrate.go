// Package migrate brings a role's PostgreSQL database to its schema: ordered SQL files, each
// applied once, in a transaction of its own, recorded in the database. Roles starting together
// apply them once, under an advisory lock; a database ahead of the binary — a migration it does not
// know — is refused, never undone.
package migrate

import (
	"context"
	"fmt"
	"hash/fnv"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Migration is one step of a schema.
type Migration struct {
	Version int
	Name    string
	SQL     string
}

var fileName = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// Load reads a schema's migrations from fsys: every file NNNN_name.sql at its root, by number,
// which must run 1, 2, 3, … with no gap and no number twice.
func Load(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	var out []Migration
	for _, e := range entries {
		m := fileName.FindStringSubmatch(e.Name())
		if e.IsDir() || m == nil {
			continue
		}
		data, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, err
		}
		v, _ := strconv.Atoi(m[1])
		out = append(out, Migration{Version: v, Name: m[2], SQL: string(data)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i, m := range out {
		if m.Version != i+1 {
			return nil, fmt.Errorf("migration %04d_%s: versions run 1, 2, 3, … with no gap or repeat", m.Version, m.Name)
		}
	}
	return out, nil
}

// MustLoad is Load for an embedded schema known to be well formed.
func MustLoad(fsys fs.FS, dir string) []Migration {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic(err)
	}
	ms, err := Load(sub)
	if err != nil {
		panic(fmt.Sprintf("migrate: %s: %v", path.Clean(dir), err))
	}
	return ms
}

// Apply brings the database pool reaches to schema's migrations, recording them in
// <schema>_migrations. It returns the versions it applied.
func Apply(ctx context.Context, pool *pgxpool.Pool, schema string, migrations []Migration) ([]int, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	h := fnv.New64a()
	h.Write([]byte("ok-migrate:" + schema))
	key := int64(h.Sum64())
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", key); err != nil {
		return nil, fmt.Errorf("migrate %s: taking the lock: %w", schema, err)
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", key)

	table := pgx.Identifier{schema + "_migrations"}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+table+` (
		version    integer PRIMARY KEY,
		name       text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return nil, fmt.Errorf("migrate %s: %w", schema, err)
	}
	applied, err := appliedVersions(ctx, conn, schema, migrations)
	if err != nil {
		return nil, err
	}
	var did []int
	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO "+table+" (version, name) VALUES ($1, $2)", m.Version, m.Name)
			return err
		})
		if err != nil {
			return did, fmt.Errorf("migrate %s: %04d_%s: %w", schema, m.Version, m.Name, err)
		}
		did = append(did, m.Version)
	}
	return did, nil
}

// Pending are schema's migrations the database pool reaches has not applied, in order, applying
// nothing.
func Pending(ctx context.Context, pool *pgxpool.Pool, schema string, migrations []Migration) ([]Migration, error) {
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", pgx.Identifier{schema + "_migrations"}.Sanitize()).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return migrations, nil
	}
	applied, err := appliedVersions(ctx, pool, schema, migrations)
	if err != nil {
		return nil, err
	}
	var out []Migration
	for _, m := range migrations {
		if !applied[m.Version] {
			out = append(out, m)
		}
	}
	return out, nil
}

// querier is a pool or a connection.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// appliedVersions are the versions the database recorded, refusing one this binary does not know.
func appliedVersions(ctx context.Context, q querier, schema string, migrations []Migration) (map[int]bool, error) {
	applied := map[int]bool{}
	rows, err := q.Query(ctx, "SELECT version FROM "+pgx.Identifier{schema + "_migrations"}.Sanitize())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return nil, err
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for v := range applied {
		if v > len(migrations) {
			return nil, fmt.Errorf("migrate %s: the database has migration %d, which this binary does not know: it is older than the database", schema, v)
		}
	}
	return applied, nil
}
