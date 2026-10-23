package server

import (
	"context"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/scm/store"
	scmpostgres "github.com/Cidan/ok/internal/scm/store/postgres"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()
	pool := testdb.Pool(t)
	b, err := scmpostgres.Open(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.New(b, testobjects.Store(t), store.Config{Names: identity.MustNames("example.com")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

// The GC node's sweep marks a repository never collected, and the node runs its pass, recorded: it
// is due no more.
func TestGC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s := testStore(t)
	r, err := s.CreateRepository(ctx, data.NewID(), data.NewID(), "ada.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	n := NewGC(New(s, nil, nil, nil), time.Now().UTC().Hour())
	if err := n.sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if due, err := s.Work().Due(ctx, store.GCWork, 10); err != nil || len(due) != 1 || due[0] != r.ID {
		t.Fatalf("marked after the sweep: %v, %v", due, err)
	}
	done := make(chan error, 1)
	go func() { done <- n.Start(ctx) }()
	for {
		due, err := s.GCDue(ctx, r.ID, time.Now(), n.hour)
		if err != nil {
			t.Fatal(err)
		}
		if !due {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the repository's pass never ran")
		case <-time.After(50 * time.Millisecond):
		}
	}
	// Not due, its collection deletes the retired packs it has past their grace: none.
	if err := n.repository(ctx, r.ID); err != nil {
		t.Errorf("collecting a repository not due a pass: %v", err)
	}
	cancel()
	<-done
}
