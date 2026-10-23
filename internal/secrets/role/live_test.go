package role_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/id/idtest"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/secrets/role"
	"github.com/Cidan/ok/internal/secrets/secretstore"
	"github.com/Cidan/ok/internal/secrets/secretstore/gcpsm"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testgcp"
)

// ok secrets' store on a real Google Cloud (internal/testgcp), opened as an installation's
// configuration says: a repository's keys wrapped by a Cloud KMS root, its values kept in Secret
// Manager, sealed — written, opened, moved to ok secrets' own database and back, its key rotated
// and its data keys rewrapped, destroyed — and nothing left in Secret Manager.
func TestOnGCPLive(t *testing.T) {
	root, _ := testgcp.KMS(t)
	project := testgcp.SecretProject(t)
	ctx := context.Background()
	cfg, err := config.Parse([]byte("domain: example.com\npublic_url: http://127.0.0.1:1\nroles: " + idtest.Roles + "\nsecrets:\n  root: " + root +
		"\n  stores:\n    gcp: {kind: gcp-secret-manager, project: " + project + "}\n"))
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgdb.Open(ctx, testdb.Postgres(t), role.Schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s, done, err := role.OpenStore(ctx, cfg, role.Options{Pool: pool})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(done)
	c, err := secretmanager.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	sm := gcpsm.New(c, project, nil)

	// A repository of this run's own: Secret Manager is shared.
	repo, path := time.Now().UnixNano(), "secret/prod/db-password"
	const value = "hunter2-the-real-one"
	d, _, err := s.Write(ctx, repo, path, []byte(value), "ada-id", "gcp")
	if err != nil {
		t.Fatal(err)
	}
	key := secretstore.Key(repo, path, d)
	t.Cleanup(func() { _ = sm.Delete(context.Background(), []string{key}) })
	open := func(when string) {
		t.Helper()
		if _, v, err := s.Open(ctx, repo, path, d); err != nil || string(v) != value {
			t.Fatalf("%s: %q, %v", when, v, err)
		}
	}
	open("written")
	sealed, err := sm.Get(ctx, key)
	if err != nil || len(sealed) == 0 || bytes.Contains(sealed, []byte(value)) {
		t.Fatalf("Secret Manager holds %d bytes (%v), the value among them: %v", len(sealed), err, bytes.Contains(sealed, []byte(value)))
	}

	if n, err := s.Transfer(ctx, repo, ""); err != nil || n != 1 {
		t.Fatalf("into ok secrets' database: %d, %v", n, err)
	}
	if _, err := sm.Get(ctx, key); !errors.Is(err, secretstore.ErrNotFound) {
		t.Errorf("Secret Manager after the transfer out: %v", err)
	}
	open("in ok secrets' database")
	if n, err := s.Transfer(ctx, repo, "gcp"); err != nil || n != 1 {
		t.Fatalf("back into Secret Manager: %d, %v", n, err)
	}
	open("back in Secret Manager")

	if v, err := s.Rotate(ctx, repo); err != nil || v != 2 {
		t.Fatalf("rotating: %d, %v", v, err)
	}
	if res, err := s.Rewrap(ctx, repo); err != nil || res.Versions != 1 {
		t.Fatalf("rewrapping: %+v, %v", res, err)
	}
	open("rewrapped under key version 2")

	if ok, err := s.Destroy(ctx, repo, path, d, "ada-id"); err != nil || !ok {
		t.Fatalf("destroying: %v, %v", ok, err)
	}
	if _, _, err := s.Open(ctx, repo, path, d); !errors.Is(err, store.ErrDestroyed) {
		t.Errorf("a destroyed version opened: %v", err)
	}
	if _, err := sm.Get(ctx, key); !errors.Is(err, secretstore.ErrNotFound) {
		t.Errorf("Secret Manager after the destroy: %v", err)
	}
}
