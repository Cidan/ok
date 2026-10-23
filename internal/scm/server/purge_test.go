package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/scm/service"
	"github.com/Cidan/ok/internal/scm/store"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// expiring answers what the directory says of expired repositories, and records the purges.
type expiring struct {
	service.Tenancy
	mu      sync.Mutex
	expired map[int64]identitypb.Expiry_Kind
	listed  map[int64]identitypb.Expiry_Kind // listed, though the directory says otherwise when asked
	purged  []int64
}

func (e *expiring) ListExpiredRepositories(context.Context, *identitypb.ListExpiredRepositoriesRequest, ...grpc.CallOption) (*identitypb.ListExpiredRepositoriesResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := &identitypb.ListExpiredRepositoriesResponse{}
	for _, m := range []map[int64]identitypb.Expiry_Kind{e.expired, e.listed} {
		for id, k := range m {
			out.Repositories = append(out.Repositories, &identitypb.Expiry{RepositoryId: id, Kind: k})
		}
	}
	return out, nil
}

func (e *expiring) GetExpiry(_ context.Context, in *identitypb.RepositoryRef, _ ...grpc.CallOption) (*identitypb.Expiry, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	k, ok := e.expired[in.GetId()]
	if !ok {
		k = identitypb.Expiry_KIND_NONE
	}
	return &identitypb.Expiry{RepositoryId: in.GetId(), Kind: k}, nil
}

func (e *expiring) PurgeRepository(_ context.Context, in *identitypb.RepositoryRef, _ ...grpc.CallOption) (*identitypb.Expiry, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	k := e.expired[in.GetId()]
	delete(e.expired, in.GetId())
	e.purged = append(e.purged, in.GetId())
	return &identitypb.Expiry{RepositoryId: in.GetId(), Kind: k}, nil
}

// The purge node marks what the directory lists as expired; for each, as the directory says when
// asked, it deletes the storage, then has the entry purged — a deleted repository, and an
// abandoned creation alike, even one whose storage was never made; a repository the directory no
// longer says has expired keeps its storage; done again, a purge is the same.
func TestPurge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s := testStore(t)
	made := func() int64 {
		t.Helper()
		r, err := s.CreateRepository(ctx, data.NewID(), data.NewID(), "ada.human.example.com")
		if err != nil {
			t.Fatal(err)
		}
		return r.ID
	}
	deleted, creating, kept := made(), made(), made()
	neverMade := data.NewID()
	dir := &expiring{
		expired: map[int64]identitypb.Expiry_Kind{deleted: identitypb.Expiry_KIND_DELETED, creating: identitypb.Expiry_KIND_CREATING,
			neverMade: identitypb.Expiry_KIND_CREATING},
		listed: map[int64]identitypb.Expiry_Kind{kept: identitypb.Expiry_KIND_DELETED},
	}
	n := NewPurge(New(s, nil, dir, nil))
	if err := n.sweep(ctx); err != nil {
		t.Fatal(err)
	}
	due, err := s.Work().Due(ctx, store.PurgeWork, 10)
	if err != nil || len(due) != 4 {
		t.Fatalf("marked after the sweep: %v, %v", due, err)
	}
	for _, id := range due {
		if err := n.repository(ctx, id); err != nil {
			t.Fatalf("purging %d: %v", id, err)
		}
	}
	for _, id := range []int64{deleted, creating} {
		if _, err := s.GetRepository(ctx, id); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("repository %d's storage after its purge: %v", id, err)
		}
	}
	if _, err := s.GetRepository(ctx, kept); err != nil {
		t.Errorf("a repository the directory says now has not expired: %v", err)
	}
	if len(dir.purged) != 3 {
		t.Errorf("entries purged: %v", dir.purged)
	}
	// Done again — a crash after the storage went, before the entry did — it finishes.
	dir.expired[deleted] = identitypb.Expiry_KIND_DELETED
	if err := n.repository(ctx, deleted); err != nil || len(dir.purged) != 4 {
		t.Errorf("a purge done again: %v, %v", err, dir.purged)
	}
}
