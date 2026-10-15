package pgdb_test

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/blob"
	idstore "github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/pgdb"
	scmstore "github.com/Cidan/ok/internal/scm/store"
	"github.com/Cidan/ok/internal/testdb"
)

// Roles sharing one database keep their tables apart: identity's and the SCM's both have a locks
// table and their own migrations, and each opens over the other's.
func TestRolesShareADatabase(t *testing.T) {
	ctx := context.Background()
	url := testdb.Postgres(t)
	names := identity.MustNames("example.com")

	idPool, err := pgdb.Open(ctx, url, "identity", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer idPool.Close()
	if _, err := idstore.Open(ctx, idPool, names); err != nil {
		t.Fatalf("identity: %v", err)
	}
	scmPool, err := pgdb.Open(ctx, url, "scm", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer scmPool.Close()
	s3 := testdb.S3(t)
	blobs, err := blobOf(s3, testdb.Bucket(t))
	if err != nil {
		t.Fatal(err)
	}
	st, err := scmstore.Open(ctx, scmPool, blobs, scmstore.Config{Names: names})
	if err != nil {
		t.Fatalf("the SCM: %v", err)
	}
	defer st.Close()

	for schema, table := range map[string]string{"identity": "principals", "scm": "repository"} {
		var n int
		if err := idPool.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname = $1 AND tablename IN ($2, 'locks')`, schema, table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Errorf("schema %s has %d of %s and locks", schema, n, table)
		}
	}
	// Opening again changes nothing.
	again, err := pgdb.Open(ctx, url, "identity", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if _, err := idstore.Open(ctx, again, names); err != nil {
		t.Errorf("identity again: %v", err)
	}
	if _, err := pgdb.Open(ctx, url, "Bad; DROP", 0); err == nil {
		t.Error("a schema name that is no identifier was accepted")
	}
}

func blobOf(s3 testdb.S3Server, bucket string) (*blob.S3, error) {
	return blob.NewS3(blob.Config{Endpoint: s3.Endpoint, Bucket: bucket, AccessKey: s3.AccessKey, SecretKey: s3.SecretKey, Region: s3.Region})
}
