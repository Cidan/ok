package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
)

// An approval waits for its person's answer: theirs alone, once, until it expires.
func testApprovals(t *testing.T, s *store.Store) {
	ctx := context.Background()
	sync(t, s, &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true), person("2", "bea", true)}}})
	ada, _ := s.Principal(ctx, "ada.human.example.com")
	bea, _ := s.Principal(ctx, "bea.human.example.com")
	a, err := s.StartApproval(ctx, ada.ID, "secrets/unseal/7/secret/prod/db", "Unseal secret/prod/db", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Approval(ctx, a.ID)
	if err != nil || got.PrincipalID != ada.ID || got.Scope != a.Scope || got.Description != a.Description || got.Decision != "" || got.Decided != nil ||
		!got.Expires.After(got.Created) {
		t.Fatalf("the approval: %+v, %v", got, err)
	}
	if err := s.DecideApproval(ctx, a.ID, bea.ID, true); !errors.Is(err, store.ErrNoApproval) {
		t.Errorf("another person deciding: %v", err)
	}
	if err := s.DecideApproval(ctx, a.ID, ada.ID, true); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Approval(ctx, a.ID); got.Decision != store.ApprovalApproved || got.Decided == nil {
		t.Errorf("approved: %+v", got)
	}
	if err := s.DecideApproval(ctx, a.ID, ada.ID, false); !errors.Is(err, store.ErrNoApproval) {
		t.Errorf("deciding twice: %v", err)
	}
	d, err := s.StartApproval(ctx, ada.ID, "x", "x", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DecideApproval(ctx, d.ID, ada.ID, false); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Approval(ctx, d.ID); got.Decision != store.ApprovalDeclined {
		t.Errorf("declined: %+v", got)
	}
	expired, err := s.StartApproval(ctx, ada.ID, "x", "x", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if err := s.DecideApproval(ctx, expired.ID, ada.ID, true); !errors.Is(err, store.ErrNoApproval) {
		t.Errorf("deciding an expired approval: %v", err)
	}
	if _, err := s.Approval(ctx, "forged"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a forged approval: %v", err)
	}
}

// An organization names its own root — a key in a KMS, never a keyring — and forgets it.
func testSecretsRoots(t *testing.T, s *store.Store) {
	ctx := context.Background()
	const root = "gcp-kms:projects/acme-prod/locations/us/keyRings/ok/cryptoKeys/root"
	if got, err := s.SecretsRoot(ctx, "org_1"); err != nil || got != "" {
		t.Fatalf("no root: %q, %v", got, err)
	}
	if err := s.SetSecretsRoot(ctx, "org_1", root, "ada"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSecretsRoot(ctx, "org_1", root, "ada"); err != nil {
		t.Fatalf("naming it again: %v", err)
	}
	if got, err := s.SecretsRoot(ctx, "org_1"); err != nil || got != root {
		t.Errorf("the root: %q, %v", got, err)
	}
	if got, _ := s.SecretsRoot(ctx, "org_2"); got != "" {
		t.Errorf("another organization's: %q", got)
	}
	if err := s.SetSecretsRoot(ctx, "org_1", "keyring:/etc/ok/keyring", "ada"); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a keyring: %v", err)
	}
	if err := s.SetSecretsRoot(ctx, "org_1", "", "ada"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.SecretsRoot(ctx, "org_1"); got != "" {
		t.Errorf("forgotten: %q", got)
	}
}
