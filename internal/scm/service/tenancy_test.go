package service

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/data"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// tenancyDir is ok identity's Tenancy as the SCM's tests need it: the installation's organization,
// example; a person's own repositories, under their handle; a name once per owner; entries made,
// activated, abandoned, adopted, resolved, and named. A person is "id-" and their name, as the
// tests' directory says.
type tenancyDir struct {
	mu     sync.Mutex
	byID   map[int64]*identitypb.RepositoryEntry
	grants map[int64]map[string]string // a repository's roles, by the principal's ID
	// expired are the repositories whose time is up, as the directory says.
	expired map[int64]identitypb.Expiry_Kind
	// slow: each creation reserved has expired before its storage is made.
	slow bool
}

func newTenancy() *tenancyDir {
	return &tenancyDir{byID: map[int64]*identitypb.RepositoryEntry{}, grants: map[int64]map[string]string{}, expired: map[int64]identitypb.Expiry_Kind{}}
}

const installation = "example"

func fullName(owner, name string) string {
	if owner == installation {
		return name
	}
	return owner + "/" + name
}

// handleOf is the handle of the person an ID names; "" for any other.
func handleOf(id string) string {
	name, ok := strings.CutPrefix(id, "id-")
	if !ok || unknown(name) {
		return ""
	}
	h, ok := strings.CutSuffix(name, ".human.example.com")
	if !ok {
		return ""
	}
	return h
}

func (t *tenancyDir) named(owner, name string) *identitypb.RepositoryEntry {
	for _, e := range t.byID {
		if e.Owner == owner && e.Name == name {
			return e
		}
	}
	return nil
}

func (t *tenancyDir) ReserveRepository(_ context.Context, in *identitypb.ReserveRepositoryRequest, _ ...grpc.CallOption) (*identitypb.RepositoryEntry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	person := handleOf(in.GetForPersonId())
	if person == "" {
		return nil, status.Errorf(codes.PermissionDenied, "%s is no active person", in.GetForPersonId())
	}
	owner := in.GetOwner()
	if owner == "" {
		owner = installation
	}
	if owner != installation && owner != person {
		return nil, status.Errorf(codes.PermissionDenied, "a person makes repositories of their own, not %s's", owner)
	}
	if held := t.named(owner, in.GetName()); held != nil {
		if held.State == "creating" && held.CreatedById == in.GetForPersonId() {
			return held, nil
		}
		return nil, status.Errorf(codes.AlreadyExists, "%s is taken", fullName(owner, in.GetName()))
	}
	id := data.NewID()
	visibility := "private"
	if owner == installation {
		visibility = "internal"
	}
	e := &identitypb.RepositoryEntry{Id: id, NetworkId: data.NewID(), Owner: owner, Name: in.GetName(), FullName: fullName(owner, in.GetName()),
		State: "creating", CreatedById: in.GetForPersonId(), Visibility: visibility, Propose: "read", ParentId: in.GetParentId()}
	t.byID[id] = e
	if t.slow {
		t.expired[id] = identitypb.Expiry_KIND_CREATING
	}
	return e, nil
}

func (t *tenancyDir) ActivateRepository(_ context.Context, in *identitypb.RepositoryRef, _ ...grpc.CallOption) (*identitypb.RepositoryEntry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.byID[in.GetId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no repository has the ID %d", in.GetId())
	}
	if t.expired[in.GetId()] == identitypb.Expiry_KIND_CREATING {
		return nil, status.Error(codes.FailedPrecondition, "the repository's creation took too long, and it is being removed")
	}
	e.State = "active"
	return e, nil
}

func (t *tenancyDir) ListExpiredRepositories(context.Context, *identitypb.ListExpiredRepositoriesRequest, ...grpc.CallOption) (*identitypb.ListExpiredRepositoriesResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := &identitypb.ListExpiredRepositoriesResponse{}
	for id, k := range t.expired {
		out.Repositories = append(out.Repositories, &identitypb.Expiry{RepositoryId: id, Kind: k})
	}
	return out, nil
}

func (t *tenancyDir) GetExpiry(_ context.Context, in *identitypb.RepositoryRef, _ ...grpc.CallOption) (*identitypb.Expiry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k, ok := t.expired[in.GetId()]
	if !ok {
		k = identitypb.Expiry_KIND_NONE
	}
	return &identitypb.Expiry{RepositoryId: in.GetId(), Kind: k}, nil
}

func (t *tenancyDir) PurgeRepository(_ context.Context, in *identitypb.RepositoryRef, _ ...grpc.CallOption) (*identitypb.Expiry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k, ok := t.expired[in.GetId()]
	if !ok {
		return &identitypb.Expiry{RepositoryId: in.GetId(), Kind: identitypb.Expiry_KIND_NONE}, nil
	}
	delete(t.expired, in.GetId())
	delete(t.byID, in.GetId())
	return &identitypb.Expiry{RepositoryId: in.GetId(), Kind: k}, nil
}

// expire says a repository's time is up.
func (t *tenancyDir) expire(id int64, k identitypb.Expiry_Kind) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expired[id] = k
}

func (t *tenancyDir) AbandonRepository(_ context.Context, in *identitypb.RepositoryRef, _ ...grpc.CallOption) (*identitypb.AbandonRepositoryResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.byID, in.GetId())
	return &identitypb.AbandonRepositoryResponse{}, nil
}

func (t *tenancyDir) ResolveRepository(_ context.Context, in *identitypb.ResolveRepositoryRequest, _ ...grpc.CallOption) (*identitypb.RepositoryEntry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	owner := in.GetOwner()
	if owner == "" {
		owner = installation
	}
	if e := t.named(owner, in.GetName()); e != nil && e.State != "creating" {
		return e, nil
	}
	return nil, status.Errorf(codes.NotFound, "no repository is called %s", fullName(owner, in.GetName()))
}

func (t *tenancyDir) GetRepositoryEntry(_ context.Context, in *identitypb.RepositoryRef, _ ...grpc.CallOption) (*identitypb.RepositoryEntry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.byID[in.GetId()]; ok {
		return e, nil
	}
	return nil, status.Errorf(codes.NotFound, "no repository has the ID %d", in.GetId())
}

// Facts answers as identity's Authorization does for an installation: its organization's
// repositories are its people's, who may change them; a person owns their own.
func (t *tenancyDir) Facts(_ context.Context, in *identitypb.FactsRequest, _ ...grpc.CallOption) (*identitypb.FactsResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.byID[in.GetRepositoryId()]
	if !ok || e.State == "creating" {
		return nil, status.Errorf(codes.NotFound, "no repository has the ID %d", in.GetRepositoryId())
	}
	org := e.Owner == installation
	owner := "org-" + installation
	if !org {
		owner = "id-" + e.Owner + ".human.example.com"
	}
	out := &identitypb.FactsResponse{
		Repository: &identitypb.FactsResponse_Repository{Id: e.Id, Visibility: e.Visibility, Propose: e.Propose, Deleted: e.State == "deleted",
			OwnerId: owner, OwnedByOrg: org, BaseRole: "none"},
		Caller: &identitypb.FactsResponse_Caller{Membership: "none"},
	}
	if org {
		out.Repository.BaseRole = "write"
	}
	if strings.HasSuffix(in.GetCallerId(), ".robot.example.com") {
		// A robot of an organization: a member of nothing, its grants alone.
		out.Caller.Active = true
		if role, ok := t.grants[e.Id][in.GetCallerId()]; ok {
			out.Caller.Granted = []string{role}
		}
		return out, nil
	}
	if handleOf(in.GetCallerId()) == "" {
		return out, nil
	}
	out.Caller.Active = true
	if org {
		out.Caller.Membership, out.Caller.Internal = "member", true
	}
	if role, ok := t.grants[e.Id][in.GetCallerId()]; ok {
		out.Caller.Granted = []string{role}
	}
	return out, nil
}

// set changes a repository's visibility, or who proposes, as its admin would.
func (t *tenancyDir) set(id int64, visibility, propose string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.byID[id]
	if visibility != "" {
		e.Visibility = visibility
	}
	if propose != "" {
		e.Propose = propose
	}
}

// grant grants a principal a role on a repository.
func (t *tenancyDir) grant(id int64, principal, role string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.grants[id] == nil {
		t.grants[id] = map[string]string{}
	}
	t.grants[id][principal] = role
}

// rename renames a repository in the directory, as identity's Tenancy would.
func (t *tenancyDir) rename(id int64, name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.byID[id]
	e.Name, e.FullName = name, fullName(e.Owner, name)
}

// A repository's full name is the directory's, kept repoNameTTL: a rename shows once it passes, and
// a repository the directory has no entry of has no name.
func TestRepoNames(t *testing.T) {
	ctx := context.Background()
	ten := newTenancy()
	e, err := ten.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Name: "app", ForPersonId: "id-ada.human.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	n := newRepoNames(ten)
	now := time.Unix(1_000_000, 0)
	n.now = func() time.Time { return now }
	if got, err := n.name(ctx, e.Id); err != nil || got != "app" {
		t.Fatalf("name = %q, %v", got, err)
	}
	ten.rename(e.Id, "service")
	if got, _ := n.name(ctx, e.Id); got != "app" {
		t.Errorf("within the TTL: %q", got)
	}
	now = now.Add(repoNameTTL)
	if got, _ := n.name(ctx, e.Id); got != "service" {
		t.Errorf("past the TTL: %q", got)
	}
	if got, err := n.name(ctx, 42); err != nil || got != "" {
		t.Errorf("a repository the directory has no entry of: %q, %v", got, err)
	}
}
