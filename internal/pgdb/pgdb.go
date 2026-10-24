// Package pgdb opens a role's PostgreSQL pool: the installation's one database, with the role's
// tables in a PostgreSQL schema of its own (identity, scm, control, …), so roles sharing the
// database never share a table — each has its own locks, its own migrations, and its own names.
package pgdb

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var schemaName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// Open connects to the database url names, the role's tables in schema, which it makes if it is
// not there; maxConns bounds the pool (0 for the driver's default). Making the schema needs the
// CREATE privilege on the database, once; an installation that grants none makes the schemas
// beforehand.
func Open(ctx context.Context, url, schema string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("the database URL: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	return open(ctx, cfg, schema)
}

// OpenConfig is Open over a parsed configuration: one a caller tunes or traces first (a test's).
func OpenConfig(ctx context.Context, cfg *pgxpool.Config, schema string) (*pgxpool.Pool, error) {
	return open(ctx, cfg, schema)
}

// Beside connects to the database pool reaches, as pool does, its tables in schema instead: what
// several roles share, beside each role's own (the bus).
func Beside(ctx context.Context, pool *pgxpool.Pool, schema string) (*pgxpool.Pool, error) {
	cfg := pool.Config()
	cfg.ConnConfig.RuntimeParams = maps.Clone(cfg.ConnConfig.RuntimeParams)
	return open(ctx, cfg, schema)
}

func open(ctx context.Context, cfg *pgxpool.Config, schema string) (*pgxpool.Pool, error) {
	if !schemaName.MatchString(schema) {
		return nil, fmt.Errorf("pgdb: %q is not a schema name", schema)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	// ok's days — a touch's, a quota's — are the database's current_date, and every role takes
	// them for UTC's: the session's time zone says so, whatever the server's is.
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
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
		if err := makeSchema(ctx, pool, schema); err != nil {
			pool.Close()
			return nil, fmt.Errorf("pgdb: making the schema %s: %w", schema, err)
		}
	}
	return pool, nil
}

// makeSchema makes a schema if it is not there. CREATE SCHEMA IF NOT EXISTS fails in all but one of
// several that run at once — roles starting together on a new database — with unique_violation on
// the catalog or duplicate_schema; the next try finds the schema.
func makeSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	for range 3 {
		_, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{schema}.Sanitize())
		var pg *pgconn.PgError
		if errors.As(err, &pg) && (pg.Code == "23505" || pg.Code == "42P06") {
			continue
		}
		return err
	}
	return errors.New("the schema was made and unmade three times over")
}
