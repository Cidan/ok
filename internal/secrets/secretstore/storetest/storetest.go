// Package storetest is the conformance suite every external secret store passes
// (internal/secrets/secretstore).
package storetest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/secrets/secretstore"
)

// Run checks a store: a put is kept, once; the same bytes again are no error and other bytes a
// conflict; a key not kept is not found; a delete deletes, and passes over what is not held; keys
// of every path the tree allows, in any repository, are kept apart; and a value of 60,000 bytes,
// which every store holds, is kept whole.
func Run(t *testing.T, s secretstore.Store) {
	ctx := context.Background()
	var d1, d2 secrets.Digest
	d1[0], d2[0] = 1, 2
	k1, k2 := secretstore.Key(7, "secret/prod/db-password", d1), secretstore.Key(-3, "secret/a-b_c.d/E9", d2)
	if _, err := s.Get(ctx, k1); !errors.Is(err, secretstore.ErrNotFound) {
		t.Fatalf("a key never kept: %v", err)
	}
	sealed := []byte("\x00ciphertext\xff and its tag")
	if err := s.Put(ctx, k1, sealed); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, k1, sealed); err != nil {
		t.Errorf("the same bytes again: %v", err)
	}
	if err := s.Put(ctx, k1, []byte("other")); !errors.Is(err, secretstore.ErrConflict) {
		t.Errorf("other bytes under a kept key: %v", err)
	}
	if got, err := s.Get(ctx, k1); err != nil || !bytes.Equal(got, sealed) {
		t.Fatalf("got %q, %v", got, err)
	}
	if err := s.Put(ctx, k2, []byte{}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, k2); err != nil || len(got) != 0 {
		t.Errorf("an empty value: %q, %v", got, err)
	}
	if err := s.Delete(ctx, []string{k1, secretstore.Key(1, "secret/never", d1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, k1); !errors.Is(err, secretstore.ErrNotFound) {
		t.Errorf("a deleted key: %v", err)
	}
	if _, err := s.Get(ctx, k2); err != nil {
		t.Errorf("a key not deleted: %v", err)
	}
	if err := s.Delete(ctx, []string{k1}); err != nil {
		t.Errorf("deleting again: %v", err)
	}
	big := bytes.Repeat([]byte("sealed bytes "), 60000/13)
	k3 := secretstore.Key(7, "secret/big", d2)
	if err := s.Put(ctx, k3, big); err != nil {
		t.Fatalf("a value of %d bytes: %v", len(big), err)
	}
	if got, err := s.Get(ctx, k3); err != nil || !bytes.Equal(got, big) {
		t.Errorf("a value of %d bytes came back as %d, %v", len(big), len(got), err)
	}
}
