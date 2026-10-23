package spannerdata

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/lock/spannerlock"
	"github.com/Cidan/ok/internal/migrate"
)

// migrationsDDL records the migrations a database has, as migrate's <schema>_migrations does on
// PostgreSQL: each Spanner database is one role's.
const migrationsDDL = `CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INT64       NOT NULL,
    name       STRING(MAX) NOT NULL,
    applied_at TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (version)`

// migrateWait is how long Migrate waits for another replica's migrations to finish.
const migrateWait = 10 * time.Minute

// Statements are a migration's DDL statements: its file's, its comments (-- to the end of a line)
// gone, split at each ';' — neither read inside a quoted string or identifier.
func Statements(sql string) []string {
	var out []string
	var b strings.Builder
	var quote rune // the quote a string or identifier is open in; 0 outside one
	runes := []rune(sql)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case quote != 0:
			b.WriteRune(r)
			if r == '\\' && i+1 < len(runes) {
				i++
				b.WriteRune(runes[i])
			} else if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"' || r == '`':
			quote = r
			b.WriteRune(r)
		case r == '-' && i+1 < len(runes) && runes[i+1] == '-':
			for i < len(runes) && runes[i] != '\n' {
				i++
			}
			b.WriteRune('\n')
		case r == ';':
			if st := strings.TrimSpace(b.String()); st != "" {
				out = append(out, st)
			}
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	if st := strings.TrimSpace(b.String()); st != "" {
		out = append(out, st)
	}
	return out
}

// Migrate brings the database c reaches to migrations — files of GoogleSQL DDL, read as migrate
// reads PostgreSQL's — and returns the versions it applied. Each is one schema change, applied
// once, its version recorded in schema_migrations, under the database's migrate lock, whose table
// Migrate makes first. A schema change's operation is named for its version: a replica that died
// after starting one waits for it, rather than applying it twice. A database ahead of the binary —
// a migration it does not know — is refused, never undone.
func Migrate(ctx context.Context, admin *database.DatabaseAdminClient, c *spanner.Client, migrations []migrate.Migration) ([]int, error) {
	db := c.DatabaseName()
	// A schema change costs seconds even when it changes nothing, so a database that has the
	// tables — every one but a new one — is not asked for them again.
	have, err := haveTables(ctx, c, "locks", "schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("migrate %s: %w", db, err)
	}
	if !have {
		if err := ddl(ctx, admin, db, "", []string{spannerlock.DDL, migrationsDDL}); err != nil {
			return nil, fmt.Errorf("migrate %s: the locks and migrations tables: %w", db, err)
		}
	}
	locks := lock.NewOn(spannerlock.DB(c))
	defer locks.WaitForRefreshers()
	var did []int
	err = locks.LockFn(ctx, "migrate", "schema", migrateWait, func(ctx context.Context) error {
		applied, err := appliedVersions(ctx, c, len(migrations))
		if err != nil {
			return err
		}
		for _, m := range migrations {
			if applied[m.Version] {
				continue
			}
			if err := ddl(ctx, admin, db, fmt.Sprintf("ok_schema_v%04d", m.Version), Statements(m.SQL)); err != nil {
				return fmt.Errorf("%04d_%s: %w", m.Version, m.Name, err)
			}
			if _, err := c.Apply(ctx, []*spanner.Mutation{record(m)}); err != nil {
				return fmt.Errorf("%04d_%s: recording it: %w", m.Version, m.Name, err)
			}
			did = append(did, m.Version)
		}
		return nil
	})
	if err != nil {
		return did, fmt.Errorf("migrate %s: %w", db, err)
	}
	return did, nil
}

// record records a migration in schema_migrations.
func record(m migrate.Migration) *spanner.Mutation {
	return spanner.InsertOrUpdate("schema_migrations", []string{"version", "name", "applied_at"},
		[]any{int64(m.Version), m.Name, spanner.CommitTimestamp})
}

// Schema is the DDL Migrate leaves a new database with — the locks and migrations tables, then
// each migration's statements, in order — for a database made with its schema at once, as
// CreateDatabase's extra statements; Applied then records its migrations, so Migrate finds none
// to apply. On Spanner Omni a database made so takes under a second, where Migrate's schema
// changes on a new one take tens of seconds: what tests make a database for each case with.
func Schema(migrations []migrate.Migration) []string {
	out := []string{spannerlock.DDL, migrationsDDL}
	for _, m := range migrations {
		out = append(out, Statements(m.SQL)...)
	}
	return out
}

// Applied records migrations as applied, as Migrate does once it has applied each: for a database
// made with Schema.
func Applied(ctx context.Context, c *spanner.Client, migrations []migrate.Migration) error {
	if len(migrations) == 0 {
		return nil
	}
	ms := make([]*spanner.Mutation, 0, len(migrations))
	for _, m := range migrations {
		ms = append(ms, record(m))
	}
	_, err := c.Apply(ctx, ms)
	return err
}

// ddl applies a schema change and waits for it: under operation id, when one is given, so that a
// change asked again waits for the first rather than applying it twice. A change another replica's
// makes refuse — two at once — is asked again.
func ddl(ctx context.Context, admin *database.DatabaseAdminClient, db, id string, statements []string) error {
	if len(statements) == 0 {
		return nil
	}
	for attempt := 1; ; attempt++ {
		op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: db, Statements: statements, OperationId: id})
		if spanner.ErrCode(err) == codes.AlreadyExists && id != "" {
			return waitDDL(ctx, admin.UpdateDatabaseDdlOperation(db+"/operations/"+id))
		}
		if err == nil {
			err = waitDDL(ctx, op)
		}
		if c := spanner.ErrCode(err); (c == codes.Aborted || c == codes.FailedPrecondition) && attempt < 5 && id == "" {
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
			continue
		}
		return err
	}
}

// waitDDL waits for a schema change, asking after it at most a second apart: the client's Wait
// backs off to a minute between asks, outwaiting a change of seconds many times over.
func waitDDL(ctx context.Context, op *database.UpdateDatabaseDdlOperation) error {
	for interval := 50 * time.Millisecond; ; interval = min(interval*2, time.Second) {
		if err := op.Poll(ctx); err != nil || op.Done() {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// haveTables reports whether the database has every one of tables.
func haveTables(ctx context.Context, c *spanner.Client, tables ...string) (bool, error) {
	var n int64
	err := c.Single().Query(ctx, spanner.Statement{SQL: `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = '' AND table_name IN UNNEST(@tables)`, Params: map[string]any{"tables": tables}}).
		Do(func(r *spanner.Row) error { return r.Columns(&n) })
	return n == int64(len(tables)), err
}

// appliedVersions are the migrations the database has, refusing one this binary does not know.
func appliedVersions(ctx context.Context, c *spanner.Client, known int) (map[int]bool, error) {
	applied := map[int]bool{}
	it := c.Single().Query(ctx, spanner.Statement{SQL: `SELECT version FROM schema_migrations`})
	err := it.Do(func(row *spanner.Row) error {
		var v int64
		if err := row.Columns(&v); err != nil {
			return err
		}
		applied[int(v)] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	for v := range applied {
		if v > known {
			return nil, fmt.Errorf("the database has migration %d, which this binary does not know: it is older than the database", v)
		}
	}
	return applied, nil
}
