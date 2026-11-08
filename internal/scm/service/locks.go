package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/scm/store"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Locks serves locks of paths (docs/scm.md, "Locks").
type Locks struct {
	scmpb.UnimplementedLocksServer
	c *core
}

func lockOut(l store.Lock) *scmpb.FileLock {
	return &scmpb.FileLock{Path: l.Path, HolderId: l.Holder, Holder: l.HolderName, TakenAt: timestamppb.New(l.TakenAt),
		ExpiresAt: timestamppb.New(l.Expires())}
}

func locksOut(ls []store.Lock) []*scmpb.FileLock {
	out := make([]*scmpb.FileLock, len(ls))
	for i, l := range ls {
		out[i] = lockOut(l)
	}
	return out
}

func (s *Locks) Lock(ctx context.Context, req *scmpb.LockRequest) (*scmpb.LockResponse, error) {
	principal, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	ls, err := s.c.store.LockPaths(ctx, req.GetRepositoryId(), principal, req.GetPaths())
	if err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.LockResponse{Locks: locksOut(ls)}, nil
}

func (s *Locks) Unlock(ctx context.Context, req *scmpb.UnlockRequest) (*scmpb.UnlockResponse, error) {
	principal, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	paths, err := s.c.store.UnlockPaths(ctx, req.GetRepositoryId(), principal, req.GetPaths())
	if err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.UnlockResponse{Unlocked: paths}, nil
}

func (s *Locks) BreakLocks(ctx context.Context, req *scmpb.BreakLocksRequest) (*scmpb.BreakLocksResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	ls, err := s.c.store.BreakLocks(ctx, req.GetRepositoryId(), req.GetPaths())
	if err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.BreakLocksResponse{Broken: locksOut(ls)}, nil
}

func (s *Locks) ListLocks(ctx context.Context, req *scmpb.ListLocksRequest) (*scmpb.ListLocksResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	ls, err := s.c.store.Locks(ctx, req.GetRepositoryId(), req.GetPathPrefix())
	if err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.ListLocksResponse{Locks: locksOut(ls)}, nil
}

// lockRejection is a refused lock's status.
func lockRejection(r *store.LockRejection) error {
	return withDetails(codes.FailedPrecondition, r.Message, &scmpb.LockRejection{Held: lockOut(r.Held), Message: r.Message})
}
