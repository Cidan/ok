package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/scm/store"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// The directory names repositories (docs/hosted.md, "Tenancy, as built"): the SCM makes one under
// the entry it reserves in ok identity, finds one by its full name through it, and shows the full
// name it has now.

// Tenancy is the part of ok identity's Tenancy service the SCM asks; identitypb's client is one.
type Tenancy interface {
	ReserveRepository(ctx context.Context, in *identitypb.ReserveRepositoryRequest, opts ...grpc.CallOption) (*identitypb.RepositoryEntry, error)
	ActivateRepository(ctx context.Context, in *identitypb.RepositoryRef, opts ...grpc.CallOption) (*identitypb.RepositoryEntry, error)
	AbandonRepository(ctx context.Context, in *identitypb.RepositoryRef, opts ...grpc.CallOption) (*identitypb.AbandonRepositoryResponse, error)
	ResolveRepository(ctx context.Context, in *identitypb.ResolveRepositoryRequest, opts ...grpc.CallOption) (*identitypb.RepositoryEntry, error)
	GetRepositoryEntry(ctx context.Context, in *identitypb.RepositoryRef, opts ...grpc.CallOption) (*identitypb.RepositoryEntry, error)
	ListExpiredRepositories(ctx context.Context, in *identitypb.ListExpiredRepositoriesRequest, opts ...grpc.CallOption) (*identitypb.ListExpiredRepositoriesResponse, error)
	GetExpiry(ctx context.Context, in *identitypb.RepositoryRef, opts ...grpc.CallOption) (*identitypb.Expiry, error)
	PurgeRepository(ctx context.Context, in *identitypb.RepositoryRef, opts ...grpc.CallOption) (*identitypb.Expiry, error)
}

// repoNameTTL is how long the SCM shows a repository's full name before it asks again: a rename
// shows within it.
const repoNameTTL = 30 * time.Second

// repoNames are repositories' full names and upstreams, kept repoNameTTL. They are safe for
// concurrent use.
type repoNames struct {
	dir Tenancy
	now func() time.Time

	mu sync.Mutex
	m  map[int64]repoName
}

type repoName struct {
	full   string
	parent int64 // a fork's upstream, as the directory names it; 0 for none
	at     time.Time
}

func newRepoNames(dir Tenancy) *repoNames {
	return &repoNames{dir: dir, now: time.Now, m: map[int64]repoName{}}
}

// name is a repository's full name now; "" for one the directory has no entry of.
func (n *repoNames) name(ctx context.Context, id int64) (string, error) {
	e, err := n.entry(ctx, id)
	return e.full, err
}

// entry is a repository's full name and upstream now.
func (n *repoNames) entry(ctx context.Context, id int64) (repoName, error) {
	n.mu.Lock()
	e, ok := n.m[id]
	n.mu.Unlock()
	if ok && n.now().Sub(e.at) < repoNameTTL {
		return e, nil
	}
	got, err := n.dir.GetRepositoryEntry(ctx, &identitypb.RepositoryRef{Id: id})
	switch {
	case status.Code(err) == codes.NotFound:
		n.put(&identitypb.RepositoryEntry{Id: id})
		return repoName{}, nil
	case err != nil:
		return repoName{}, err
	}
	n.put(got)
	return repoName{full: got.GetFullName(), parent: got.GetParentId()}, nil
}

func (n *repoNames) put(e *identitypb.RepositoryEntry) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.m) > 100_000 {
		clear(n.m)
	}
	n.m[e.GetId()] = repoName{full: e.GetFullName(), parent: e.GetParentId(), at: n.now()}
}

// create makes the repository a full name names for person, by stable ID: the directory reserves
// its entry — refusing a name taken, or an owner the person may not make repositories of — then
// the SCM makes its storage under the entry's ID and network, and the directory activates it. An
// entry whose storage could not be made is abandoned. Each step may be asked again: a creation a
// stopped replica left half done resumes — the directory answers the person's entry still being
// made, and storage made already under its ID is taken as made.
func (k *core) create(ctx context.Context, person, full string) (*scmpb.Repository, error) {
	owner, name, err := tenancy.ParseFullName(full)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	e, err := k.dir.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: owner, Name: name, ForPersonId: person})
	if err != nil {
		return nil, err
	}
	if cell := e.GetCell(); k.elsewhere(cell) {
		return k.forward(ctx, cell, full, person)
	}
	return k.make(ctx, e, func() (store.Repository, error) {
		return k.store.CreateRepository(ctx, e.GetId(), e.GetNetworkId(), person)
	})
}

// forward has the SCM of the cell the directory placed a repository in make it, for person.
func (k *core) forward(ctx context.Context, cell, full, person string) (*scmpb.Repository, error) {
	if k.peers == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "the directory placed %s in cell %s, which this SCM does not reach", full, cell)
	}
	c, err := k.peers.SCM(ctx, cell)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "reaching cell %s's SCM: %v", cell, err)
	}
	return c.MakeRepository(ctx, &scmpb.MakeRepositoryRequest{FullName: full, ForPersonId: person})
}

// Cells is how another cell's SCM asks this one (docs/hosted.md, "Cells, as built").
type Cells struct {
	scmpb.UnimplementedCellsServer
	c *core
}

// MakeRepository makes a repository another cell's SCM reserved for a person and the directory
// placed here: reserved again for the same person — which the directory answers with the entry
// still being made — then made, as a creation asked again is.
func (s *Cells) MakeRepository(ctx context.Context, req *scmpb.MakeRepositoryRequest) (*scmpb.Repository, error) {
	k := s.c
	owner, name, err := tenancy.ParseFullName(req.GetFullName())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	person := req.GetForPersonId()
	e, err := k.dir.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: owner, Name: name, ForPersonId: person})
	if err != nil {
		return nil, err
	}
	if k.elsewhere(e.GetCell()) {
		return nil, cells.Moved(e.GetId(), e.GetCell())
	}
	return k.make(ctx, e, func() (store.Repository, error) {
		return k.store.CreateRepository(ctx, e.GetId(), e.GetNetworkId(), person)
	})
}

// make makes the storage of an entry the directory reserved, then has the directory activate it.
// Storage made already — a creation asked again — is taken as made; storage that could not be made
// abandons the entry; a creation that took too long, which the directory refuses to activate,
// deletes what it made.
func (k *core) make(ctx context.Context, e *identitypb.RepositoryEntry, makeStorage func() (store.Repository, error)) (*scmpb.Repository, error) {
	id := e.GetId()
	r, err := makeStorage()
	if errors.Is(err, store.ErrAlreadyExists) {
		r, err = k.store.GetRepository(ctx, id)
	}
	if err != nil {
		if _, aerr := k.dir.AbandonRepository(context.WithoutCancel(ctx), &identitypb.RepositoryRef{Id: id}); aerr != nil {
			slog.Warn("scm: abandoning an entry whose storage was not made", "repository", id, "err", aerr)
		}
		return nil, toStatus(err)
	}
	if e, err = k.dir.ActivateRepository(context.WithoutCancel(ctx), &identitypb.RepositoryRef{Id: id}); err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			// The creation took too long: the entry is the purge's to remove, and what was made goes now.
			if derr := k.store.DeleteRepository(context.WithoutCancel(ctx), id); derr != nil && !errors.Is(derr, store.ErrNotFound) {
				slog.Warn("scm: deleting an expired creation's storage", "repository", id, "err", derr)
			}
			return nil, err
		}
		return nil, status.Errorf(codes.Unavailable, "repository %d is made, but the directory did not activate it: %v", id, err)
	}
	k.repoNames.put(e)
	return k.repositoryOut(ctx, r)
}

// fork makes a fork of the repository parent names, called full, for person — tok, a personal
// token they presented, narrowing what they may fork (nil from ok control, which decided). The
// directory reserves the fork's entry under its upstream, deciding the person may fork it and make
// repositories of the fork's owner; the SCM makes its storage by reference to the upstream's; the
// directory activates it. Asked again, it resumes as a creation does.
func (k *core) fork(ctx context.Context, person string, tok *authz.Token, parent, full string) (*scmpb.Repository, error) {
	powner, pname, err := tenancy.ParseFullName(parent)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "no repository is called %s", parent)
	}
	up, err := k.dir.ResolveRepository(ctx, &identitypb.ResolveRepositoryRequest{Owner: powner, Name: pname, ForPersonId: person})
	if err != nil {
		return nil, err
	}
	if err := k.allow(ctx, person, tok, up.GetId(), parent, authz.Fork); err != nil {
		return nil, err
	}
	owner, name, err := tenancy.ParseFullName(full)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	e, err := k.dir.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: owner, Name: name, ParentId: up.GetId(), ForPersonId: person})
	if err != nil {
		return nil, err
	}
	return k.make(ctx, e, func() (store.Repository, error) { return k.store.Fork(ctx, e.GetId(), up.GetId(), person) })
}

// allow refuses person an action on a repository, named name to them, that Authorize refuses —
// as though there were none, if they cannot see it.
func (k *core) allow(ctx context.Context, person string, tok *authz.Token, repo int64, name string, a authz.Action) error {
	d, err := k.access.checker.Decide(ctx, person, tok, repo, a)
	if err != nil {
		return status.Errorf(codes.Unavailable, "deciding who may %s repository %d: %v", a, repo, err)
	}
	switch {
	case !d.Visible:
		return status.Errorf(codes.NotFound, "no repository is called %s", name)
	case !d.Allowed:
		return status.Error(codes.PermissionDenied, upperFirst(d.Why))
	}
	return nil
}

// syncFork brings a fork its upstream's newer work, for person: the upstream the directory names
// now, which they must be able to read. Writing to the fork, Authorize decided before (the
// method's action) or ok control did.
func (k *core) syncFork(ctx context.Context, person string, tok *authz.Token, fork int64) (*scmpb.SyncForkResponse, error) {
	e, err := k.dir.GetRepositoryEntry(ctx, &identitypb.RepositoryRef{Id: fork})
	if err != nil {
		return nil, err
	}
	k.repoNames.put(e)
	if e.GetParentId() == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "%s is no fork, or stands alone now", e.GetFullName())
	}
	up, err := k.repoNames.name(ctx, e.GetParentId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "naming repository %d's upstream: %v", fork, err)
	}
	if err := k.allow(ctx, person, tok, e.GetParentId(), up, authz.ReadCode); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Errorf(codes.FailedPrecondition, "%s's upstream is not one you may read", e.GetFullName())
		}
		return nil, err
	}
	r, err := k.store.Sync(ctx, fork, e.GetParentId(), person)
	if err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.SyncForkResponse{Moved: r.Moved, Head: r.Head[:], HeadSeq: r.Seq}, nil
}

// resolve is the repository a full name names — or one it had — as the caller may see it: a
// person sees what Authorize lets them, ok's roles every repository. One another cell holds is
// refused, naming the cell.
func (k *core) resolve(ctx context.Context, full string) (store.Repository, error) {
	owner, name, err := tenancy.ParseFullName(full)
	if err != nil {
		return store.Repository{}, status.Errorf(codes.NotFound, "no repository is called %s", full)
	}
	req := &identitypb.ResolveRepositoryRequest{Owner: owner, Name: name}
	c, ok := authn.FromContext(ctx)
	if ok && c.Human {
		req.ForPersonId = c.ID
	}
	e, err := k.dir.ResolveRepository(ctx, req)
	if err != nil {
		return store.Repository{}, err
	}
	if ok && !c.IsRole() && (!c.Human || c.Token != nil) {
		// The directory decided for the SCM, which asked: a robot, or the token a person presented,
		// is decided here.
		d, err := k.access.checker.Decide(ctx, c.ID, c.Token, e.GetId(), authz.ReadCode)
		if err != nil {
			return store.Repository{}, status.Errorf(codes.Unavailable, "deciding who may read repository %d: %v", e.GetId(), err)
		}
		if !d.Visible {
			return store.Repository{}, status.Errorf(codes.NotFound, "no repository is called %s", full)
		}
	}
	if k.elsewhere(e.GetCell()) {
		return store.Repository{}, cells.Moved(e.GetId(), e.GetCell())
	}
	k.repoNames.put(e)
	r, err := k.store.GetRepository(ctx, e.GetId())
	return r, toStatus(err)
}
