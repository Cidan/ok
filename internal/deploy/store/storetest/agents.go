package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/deploy/store"
)

// A repository's owner is the directory's, a rename that names none keeping it; and an agent's
// session is a replica's until another claims it, renewed and released only by its holder.
func testAgents(t *testing.T, s *store.Store) {
	ctx := context.Background()
	if owner, err := s.RepositoryOwner(ctx, 9); err != nil || owner != "" {
		t.Errorf("an unknown repository's owner: %q, %v", owner, err)
	}
	for _, ev := range []store.DirectoryEvent{
		{Seq: 1, Repository: 9, FullName: "acme/api", Owner: "org_acme"},
		{Seq: 2, Repository: 9, FullName: "acme/api2"},
	} {
		if err := s.ApplyDirectory(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	if owner, err := s.RepositoryOwner(ctx, 9); err != nil || owner != "org_acme" {
		t.Errorf("the owner after a rename naming none: %q, %v", owner, err)
	}
	if err := s.ApplyDirectory(ctx, store.DirectoryEvent{Seq: 3, Repository: 9, FullName: "globex/api", Owner: "org_globex"}); err != nil {
		t.Fatal(err)
	}
	if owner, _ := s.RepositoryOwner(ctx, 9); owner != "org_globex" {
		t.Errorf("the owner after a transfer: %q", owner)
	}

	at := time.Unix(1_800_000_000, 0).UTC()
	if _, ok, err := s.AgentSession(ctx, "org_acme"); err != nil || ok {
		t.Errorf("no session: %v, %v", ok, err)
	}
	if err := s.ClaimAgentSession(ctx, store.AgentSession{Account: "org_acme", Holder: "r1", Address: "10.0.0.1:8472", LeaseUntil: at,
		ConnectedAt: at}); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.RenewAgentSession(ctx, "org_acme", "r1", at.Add(time.Minute)); err != nil || !ok {
		t.Errorf("its holder renewing: %v, %v", ok, err)
	}
	if err := s.ClaimAgentSession(ctx, store.AgentSession{Account: "org_acme", Holder: "r2", Address: "10.0.0.2:8472", LeaseUntil: at,
		ConnectedAt: at.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.RenewAgentSession(ctx, "org_acme", "r1", at.Add(2*time.Minute)); err != nil || ok {
		t.Errorf("a replaced holder renewing: %v, %v", ok, err)
	}
	if err := s.ReleaseAgentSession(ctx, "org_acme", "r1"); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.AgentSession(ctx, "org_acme")
	if err != nil || !ok || got.Holder != "r2" || got.Address != "10.0.0.2:8472" || !got.LeaseUntil.Equal(at) {
		t.Errorf("the session after another's release: %+v, %v, %v", got, ok, err)
	}
	if err := s.ReleaseAgentSession(ctx, "org_acme", "r2"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.AgentSession(ctx, "org_acme"); ok {
		t.Error("a released session")
	}
}
