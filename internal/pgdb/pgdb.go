// Package pgdb opens a role's PostgreSQL pool: the installation's one database, with the role's
// tables in a PostgreSQL schema of its own (identity, scm, control, …), so roles sharing the
// database never share a table — each has its own locks, its own migrations, and its own names.
package pgdb

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var schemaName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// Open connects to the database url names, the role's tables in schema, which it makes if it is
// not there; maxConns bounds the pool (0 for the driver's default). Making the schema needs the
// CREATE privilege on the database, once; an installation that grants none makes the schemas
// beforehand.
func Open(ctx context.Context, url, schema string, maxConns int32) (*pgxpool.Pool, error) {
	if !schemaName.MatchString(schema) {
		return nil, fmt.Errorf("pgdb: %q is not a schema name", schema)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("the database URL: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, schema).Scan(&exists); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgdb: the database: %w", err)
	}
	if !exists {
		if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{schema}.Sanitize()); err != nil {
			pool.Close()
			return nil, fmt.Errorf("pgdb: making the schema %s: %w", schema, err)
		}
	}
	return pool, nil
}
