package service_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/id/store"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Expired repositories are the SCM's: it lists them, asks of each, and purges it — a deleted one's
// purge in its log; a person may do none of it, and nothing not expired is purged.
func TestExpiredRepositories(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	now := time.Now()
	e.store.SetClock(func() time.Time { return now })
	made := func(name string) *identitypb.RepositoryEntry {
		t.Helper()
		r, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "bea", Name: name, ForPersonId: e.beaID})
		if err != nil {
			t.Fatal(err)
		}
		if r, err = e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: r.Id}); err != nil {
			t.Fatal(err)
		}
		return r
	}
	gone, kept := made("gone"), made("kept")
	if _, err := e.bea.DeleteRepository(ctx, &identitypb.RepositoryRef{Id: gone.Id}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.ListExpiredRepositories(ctx, &identitypb.ListExpiredRepositoriesRequest{}); code(err) != codes.PermissionDenied {
		t.Errorf("a person listing expired repositories: %v", err)
	}
	if _, err := e.bea.PurgeRepository(ctx, &identitypb.RepositoryRef{Id: gone.Id}); code(err) != codes.PermissionDenied {
		t.Errorf("a person purging: %v", err)
	}
	if ex, err := e.scm.GetExpiry(ctx, &identitypb.RepositoryRef{Id: gone.Id}); err != nil || ex.Kind != identitypb.Expiry_KIND_NONE {
		t.Errorf("a repository deleted within the retention period = %v, %v", ex, err)
	}
	now = now.Add(store.RetentionPeriod + time.Hour)
	ls, err := e.scm.ListExpiredRepositories(ctx, &identitypb.ListExpiredRepositoriesRequest{})
	if err != nil || len(ls.Repositories) != 1 || ls.Repositories[0].RepositoryId != gone.Id || ls.Repositories[0].Kind != identitypb.Expiry_KIND_DELETED {
		t.Fatalf("ListExpiredRepositories = %v, %v", ls, err)
	}
	if ex, err := e.scm.PurgeRepository(ctx, &identitypb.RepositoryRef{Id: kept.Id}); err != nil || ex.Kind != identitypb.Expiry_KIND_NONE {
		t.Errorf("purging a repository that has not expired = %v, %v", ex, err)
	}
	if _, err := e.scm.GetRepositoryEntry(ctx, &identitypb.RepositoryRef{Id: kept.Id}); err != nil {
		t.Errorf("a repository that has not expired, after a purge asked of it: %v", err)
	}
	if ex, err := e.scm.PurgeRepository(ctx, &identitypb.RepositoryRef{Id: gone.Id}); err != nil || ex.Kind != identitypb.Expiry_KIND_DELETED {
		t.Fatalf("PurgeRepository = %v, %v", ex, err)
	}
	evs, err := identitypb.NewDirectoryEventsClient(e.as(control)).Read(ctx, &identitypb.ReadDirectoryEventsRequest{RepositoryId: gone.Id})
	if err != nil || len(evs.Events) == 0 || evs.Events[len(evs.Events)-1].Kind != identitypb.DirectoryEvent_KIND_PURGED {
		t.Errorf("the purged repository's log = %v, %v", evs, err)
	}
}
