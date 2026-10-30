package server

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/scm/store"
	scmpostgres "github.com/Cidan/ok/internal/scm/store/postgres"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
	"github.com/Cidan/ok/internal/work/worktest"
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

// The GC node's one-time booking books a repository never collected for now, and the node runs its
// pass, recorded — it is due no more — and books its next for the next day's slot.
func TestGC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s := testStore(t)
	r, err := s.CreateRepository(ctx, data.NewID(), data.NewID(), "ada.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	n := NewGC(New(s, nil, &expiring{}, nil), time.Now().UTC().Hour())
	if err := n.schedule(ctx); err != nil {
		t.Fatal(err)
	}
	if got := worktest.Ready(t, s.Work(), store.GCWork); !slices.Equal(got, []int64{r.ID}) {
		t.Fatalf("the collection's jobs after the booking: %v", got)
	}
	if err := s.Work().Mark(ctx, store.GCWork, r.ID); err != nil {
		t.Fatal(err)
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
	// Its next collection is booked for the next day's slot.
	want := store.NextGCSlot(time.Now(), n.hour)
	for {
		got, err := s.Work().Take(ctx, store.GCWork, r.ID, "look", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if got.Took {
			if err := s.Work().Let(ctx, store.GCWork, r.ID, "look"); err != nil {
				t.Fatal(err)
			}
			if !got.Due && got.Later.Sub(want).Abs() < time.Minute {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the next collection never booked for %v: %+v", want, got)
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
