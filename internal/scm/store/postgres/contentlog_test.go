package postgres_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/scm/store/postgres"
	"github.com/Cidan/ok/internal/testdb"
)

// A content log carried over a restore without its transaction IDs is neither read nor cleaned: a
// place past the database's snapshot — a fold's, kept in the dump — is refused by both the reads and
// the fold's cleanup, which deletes nothing, and so is a row of a transaction the database has not
// reached.
func TestARestoredContentLogIsNeitherReadNorDeleted(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Pool(t)
	b, err := postgres.Open(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	sc := backend.RepositoryScope(7)
	if err := b.InsertContentIndex(ctx, sc, 1); err != nil {
		t.Fatal(err)
	}
	if err := b.InsertContentLog(ctx, sc, []byte("written after the restore")); err != nil {
		t.Fatal(err)
	}
	if logs, _, err := b.ContentLogSince(ctx, sc, nil); err != nil || len(logs) != 1 {
		t.Fatalf("reading the log: %d rows, %v", len(logs), err)
	}

	ahead := backend.LogPlace(fmt.Sprintf("%d:%d:", uint64(1)<<40+3, uint64(1)<<40+3))
	if _, _, err := b.ContentLogSince(ctx, sc, ahead); err == nil || !strings.Contains(err.Error(), "past this database's snapshot") {
		t.Errorf("reading on from a place ahead: %v", err)
	}
	if _, current, _, _, err := b.ContentLogCurrent(ctx, sc, 1, ahead); err == nil || current {
		t.Errorf("reading the current log on from a place ahead: %v, %v", current, err)
	}
	if n, err := b.DeleteFoldedContentLogs(ctx, sc, ahead, 100); err == nil || n != 0 {
		t.Errorf("cleaning up after a fold's place ahead: deleted %d, %v", n, err)
	}
	if logs, _, err := b.ContentLogSince(ctx, sc, nil); err != nil || len(logs) != 1 {
		t.Fatalf("after the refused cleanup: %d rows, %v", len(logs), err)
	}

	if _, err := pool.Exec(ctx, `UPDATE content_log SET xid = (pg_current_xact_id()::text::bigint + 1000000)::text::xid8`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.ContentLogSince(ctx, sc, nil); err == nil || !strings.Contains(err.Error(), "has not reached") {
		t.Errorf("reading a log stamped by a transaction not yet reached: %v", err)
	}
}
