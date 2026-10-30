package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/id/dirfeed"
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
	expires map[int64]time.Time              // when one not expired will
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
	out := &identitypb.Expiry{RepositoryId: in.GetId(), Kind: k}
	if at, ok := e.expires[in.GetId()]; ok {
		out.ExpiresAt = timestamppb.New(at)
	}
	return out, nil
}

func (e *expiring) PurgeRepository(_ context.Context, in *identitypb.RepositoryRef, _ ...grpc.CallOption) (*identitypb.Expiry, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	k := e.expired[in.GetId()]
	delete(e.expired, in.GetId())
	e.purged = append(e.purged, in.GetId())
	return &identitypb.Expiry{RepositoryId: in.GetId(), Kind: k}, nil
}

// A repository's purge is booked as it is made, for its making's timeout, and as the directory's
// deletion of it is followed, for its retention's end — folded into the sooner, the purge looks
// again when the directory says it will expire; the purge, as the directory says when asked,
// deletes the storage, then has the entry purged — a deleted repository, and an abandoned creation
// alike, even one whose storage was never made; a repository the directory no longer says has
// expired keeps its storage; done again, a purge is the same.
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
	}
	n := NewPurge(New(s, nil, dir, nil), nil)
	booked := func(repo int64) time.Time {
		t.Helper()
		got, err := s.Work().Take(ctx, store.PurgeWork, repo, "look", time.Minute)
		if err != nil || !got.Took {
			t.Fatalf("taking repository %d's purge: %+v, %v", repo, got, err)
		}
		if err := s.Work().Let(ctx, store.PurgeWork, repo, "look"); err != nil {
			t.Fatal(err)
		}
		return got.Later
	}
	if at := booked(kept); time.Until(at) < 59*time.Minute || time.Until(at) > 62*time.Minute {
		t.Errorf("a new repository's purge booked for %v; want its making's timeout on", at)
	}
	// The directory's deletion of it, followed, books its purge for its retention's end.
	at := time.Now().Add(-time.Hour)
	ev, err := dirfeed.Encode(&identitypb.DirectoryEvent{RepositoryId: kept, Seq: 1, At: timestamppb.New(at), Kind: identitypb.DirectoryEvent_KIND_DELETED})
	if err != nil {
		t.Fatal(err)
	}
	if err := (directoryApplier{n}).Apply(ctx, ev); err != nil {
		t.Fatal(err)
	}
	// Folded into the sooner booking, its making's timeout: the purge then, finding it not
	// expired, books itself for when the directory says it will.
	if got := booked(kept); time.Until(got) > 62*time.Minute {
		t.Errorf("a deleted repository's purge booked for %v; want the sooner of the two", got)
	}
	// Its look at its making's timeout, now: the mark taken, as its job's runner takes it.
	if err := s.Work().MarkAt(ctx, store.PurgeWork, kept, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Work().Take(ctx, store.PurgeWork, kept, "runner", time.Minute); err != nil || !got.Due {
		t.Fatalf("taking the look: %+v, %v", got, err)
	}
	dir.expires = map[int64]time.Time{kept: at.Add(dirfeed.RetentionPeriod)}
	if err := n.repository(ctx, kept); err != nil {
		t.Fatal(err)
	}
	if again, err := s.Work().Release(ctx, store.PurgeWork, kept, "runner"); err != nil || again {
		t.Fatalf("releasing the look: %v, %v", again, err)
	}
	if got := booked(kept); got.Sub(at.Add(dirfeed.RetentionPeriod)).Abs() > 2*time.Minute {
		t.Errorf("a deleted repository's purge booked for %v; want %v", got, at.Add(dirfeed.RetentionPeriod))
	}
	if c, err := s.FollowCursor(ctx, dirfeed.Topic, kept); err != nil || c != 1 {
		t.Errorf("the directory's cursor: %d, %v", c, err)
	}
	for _, id := range []int64{deleted, creating, neverMade, kept} {
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
