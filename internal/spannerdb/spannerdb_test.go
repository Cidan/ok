package spannerdb_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/spannerdb"
	"github.com/Cidan/ok/internal/testdb"
)

// Two replicas starting together make a role's database once, each opening it; a third finds it
// made, and reads what the first wrote.
func TestOpen(t *testing.T) {
	ctx := context.Background()
	conn, inst := testdb.SpannerConnection(t)
	cfg := &config.SpannerDatabase{Instance: inst, Prefix: fmt.Sprintf("t%d", os.Getpid()%1_000_000)}
	if conn.Omni != "" {
		cfg.Omni = &config.SpannerOmni{Address: conn.Omni, PlainText: conn.PlainText}
	}
	admin := testdb.SpannerAdmin(t)
	t.Cleanup(func() {
		_ = admin.DropDatabase(context.Background(), &databasepb.DropDatabaseRequest{Database: cfg.DatabaseName("deploy")})
	})
	dbs := make(chan *spannerdb.DB, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			db, err := spannerdb.Open(ctx, cfg, "deploy")
			errs <- err
			dbs <- db
		}()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("opening at once: %v", err)
		}
		db := <-dbs
		t.Cleanup(db.Close)
	}
	first, err := spannerdb.Open(ctx, cfg, "deploy")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(first.Close)
	op, err := first.Admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: cfg.DatabaseName("deploy"),
		Statements: []string{"CREATE TABLE t (x INT64 NOT NULL) PRIMARY KEY (x)"}})
	if err == nil {
		err = op.Wait(ctx)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Client.Apply(ctx, []*spanner.Mutation{spanner.Insert("t", []string{"x"}, []any{int64(7)})}); err != nil {
		t.Fatal(err)
	}
	again, err := spannerdb.Open(ctx, cfg, "deploy")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(again.Close)
	row, err := again.Client.Single().ReadRow(ctx, "t", spanner.Key{int64(7)}, []string{"x"})
	if err != nil {
		t.Fatalf("the database opened again: %v", err)
	}
	var x int64
	if err := row.Columns(&x); err != nil || x != 7 {
		t.Errorf("read %d, %v", x, err)
	}
}
