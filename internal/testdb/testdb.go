// Package testdb gives each test a PostgreSQL database and an S3 bucket of its own, so tests run
// in parallel and change what they like without seeing one another. The servers are the
// environment's, named as PostgreSQL's and AWS's own tools name them: PGHOST, PGPORT, PGUSER, and
// PGPASSWORD (a role that may create databases), and AWS_ENDPOINT_URL_S3, AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KEY, and AWS_REGION. A test with no server is skipped — unless
// OK_TEST_INTEGRATION is "require", as CI sets it, which fails it instead.
package testdb

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
)

var counter atomic.Int64

// missing skips or fails a test whose server the environment does not name.
func missing(t testing.TB, what string) {
	t.Helper()
	if os.Getenv("OK_TEST_INTEGRATION") == "require" {
		t.Fatalf("testdb: no %s server, and OK_TEST_INTEGRATION=require", what)
	}
	t.Skipf("testdb: no %s server in the environment", what)
}

// postgresURL is the URL of a database on the environment's server, as its role.
func postgresURL(database string) string {
	q := url.Values{}
	q.Set("host", os.Getenv("PGHOST"))
	if p := os.Getenv("PGPORT"); p != "" {
		q.Set("port", p)
	}
	q.Set("user", os.Getenv("PGUSER"))
	if pw := os.Getenv("PGPASSWORD"); pw != "" {
		q.Set("password", pw)
	}
	mode := os.Getenv("PGSSLMODE")
	if mode == "" {
		mode = "disable"
	}
	q.Set("sslmode", mode)
	return (&url.URL{Scheme: "postgres", Path: "/" + database, RawQuery: q.Encode()}).String()
}

// Postgres returns the URL of a new, empty database for this test alone, owned by the
// environment's role; it is dropped when the test ends.
func Postgres(t testing.TB) string {
	t.Helper()
	if os.Getenv("PGHOST") == "" || os.Getenv("PGUSER") == "" {
		missing(t, "PostgreSQL")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, postgresURL("postgres"))
	if err != nil {
		t.Fatalf("testdb: connecting to PostgreSQL: %v", err)
	}
	defer admin.Close(ctx)
	name := fmt.Sprintf("test_%d_%d", os.Getpid(), counter.Add(1))
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+ident(name)+" TEMPLATE template0"); err != nil {
		t.Fatalf("testdb: creating database %s: %v", name, err)
	}
	t.Cleanup(func() {
		admin, err := pgx.Connect(ctx, postgresURL("postgres"))
		if err != nil {
			t.Errorf("testdb: connecting to drop %s: %v", name, err)
			return
		}
		defer admin.Close(ctx)
		if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+ident(name)+" WITH (FORCE)"); err != nil {
			t.Errorf("testdb: dropping database %s: %v", name, err)
		}
	})
	return postgresURL(name)
}

func ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
