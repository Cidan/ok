package storetest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/Cidan/ok/internal/automation/store"
)

// Each lease mints a key its worker alone is given: the run is the key's while it runs under that
// lease, and no other key's — not another lease's of it, nor any once it finishes.
func testLeaseKeys(t *testing.T, e *env) {
	ctx := context.Background()
	s := e.newStore(t, store.DefaultConfig)
	r := start(t, s, 1, build("//a:b"))
	other := start(t, s, 1, build("//a:c"))
	l := lease(t, s, "w")
	m := lease(t, s, "w2")
	if l.ID != r.ID || m.ID != other.ID || len(l.LeaseKey) != 32 || bytes.Equal(l.LeaseKey, m.LeaseKey) {
		t.Fatalf("leases %d and %d, keys %x and %x", l.ID, m.ID, l.LeaseKey, m.LeaseKey)
	}
	if got, err := s.RunOfLease(ctx, r.ID, l.LeaseKey); err != nil || got.ID != r.ID {
		t.Fatalf("the run under its lease's key: %+v, %v", got, err)
	}
	if got, _ := s.Get(ctx, r.ID); got.LeaseKey != nil {
		t.Error("a run read again carries its key")
	}
	for name, key := range map[string][]byte{"another run's": m.LeaseKey, "a forged": make([]byte, 32)} {
		if _, err := s.RunOfLease(ctx, r.ID, key); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("%s key: %v", name, err)
		}
	}
	if err := s.Finish(ctx, r.ID, "w", store.StatusPassed, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RunOfLease(ctx, r.ID, l.LeaseKey); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a finished run's key: %v", err)
	}
}
