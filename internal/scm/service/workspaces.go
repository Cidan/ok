package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Workspaces is okfs's leases and checkpoints (docs/okfs.md): every call acts on the
// caller's own workspaces, as the operation log's calls act on the caller's own log.
type Workspaces struct {
	scmpb.UnimplementedWorkspacesServer
	c *core
}

// maxCheckpoints bounds a page of ListCheckpoints.
const maxCheckpoints = 1000

func leaseIn(l *scmpb.Lease) (store.Lease, error) {
	if l == nil {
		return store.Lease{}, status.Error(codes.InvalidArgument, "a lease is required")
	}
	return store.Lease{Repo: l.GetRepositoryId(), Workspace: l.GetWorkspace(), Machine: l.GetMachine(), Epoch: l.GetEpoch()}, nil
}

func leaseOut(l store.Lease) *scmpb.Lease {
	return &scmpb.Lease{RepositoryId: l.Repo, Workspace: l.Workspace, Machine: l.Machine, Epoch: l.Epoch, ExpiresAt: timestamppb.New(l.ExpiresAt)}
}

func checkpointOut(c *store.Checkpoint) *scmpb.Checkpoint {
	if c == nil {
		return nil
	}
	return &scmpb.Checkpoint{
		Id: c.ID, RepositoryId: c.Repo, Workspace: c.Workspace, Machine: c.Machine, ParentId: c.ParentID,
		TreeIds: idsOut(c.Trees), BaseCommitId: c.BaseCommit[:], Fork: c.Fork, CreatedAt: timestamppb.New(c.CreatedAt),
	}
}

func (w *Workspaces) Attach(ctx context.Context, req *scmpb.AttachRequest) (*scmpb.AttachResponse, error) {
	p, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	a, err := w.c.store.Attach(ctx, p, req.GetRepositoryId(), req.GetWorkspace(), req.GetMachine(), req.GetTakeover(), w.c.now())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.AttachResponse{Granted: a.Granted, Holder: a.Holder, Latest: checkpointOut(a.Latest)}
	if a.Granted {
		out.Lease = leaseOut(a.Lease)
	} else {
		out.HolderExpiresAt = timestamppb.New(a.HolderExpires)
	}
	return out, nil
}

func (w *Workspaces) Renew(ctx context.Context, req *scmpb.RenewRequest) (*scmpb.RenewResponse, error) {
	p, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	l, err := leaseIn(req.GetLease())
	if err != nil {
		return nil, err
	}
	renewed, takeoverBy, err := w.c.store.Renew(ctx, p, l, w.c.now())
	if err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.RenewResponse{Lease: leaseOut(renewed), TakeoverBy: takeoverBy}, nil
}

func (w *Workspaces) Release(ctx context.Context, req *scmpb.ReleaseRequest) (*scmpb.ReleaseResponse, error) {
	p, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	l, err := leaseIn(req.GetLease())
	if err != nil {
		return nil, err
	}
	if err := w.c.store.Release(ctx, p, l); err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.ReleaseResponse{}, nil
}

func (w *Workspaces) RecordCheckpoint(ctx context.Context, req *scmpb.RecordCheckpointRequest) (*scmpb.Checkpoint, error) {
	p, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	l, err := leaseIn(req.GetLease())
	if err != nil {
		return nil, err
	}
	trees := make([]encoding.ID, len(req.GetTreeIds()))
	for i, t := range req.GetTreeIds() {
		if trees[i], err = idIn(t, "tree_ids"); err != nil {
			return nil, err
		}
	}
	base, err := idIn(req.GetBaseCommitId(), "base_commit_id")
	if err != nil {
		return nil, err
	}
	c, err := w.c.store.RecordCheckpoint(ctx, p, l, req.GetParentId(), trees, base)
	if err != nil {
		return nil, toStatus(err)
	}
	return checkpointOut(&c), nil
}

func (w *Workspaces) GetCheckpoint(ctx context.Context, req *scmpb.GetCheckpointRequest) (*scmpb.Checkpoint, error) {
	p, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	c, err := w.c.store.GetCheckpoint(ctx, p, req.GetRepositoryId(), req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return checkpointOut(c), nil
}

func (w *Workspaces) ListCheckpoints(ctx context.Context, req *scmpb.ListCheckpointsRequest) (*scmpb.ListCheckpointsResponse, error) {
	p, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	limit := int(req.GetLimit())
	switch {
	case limit <= 0:
		limit = 50
	case limit > maxCheckpoints:
		limit = maxCheckpoints
	}
	cs, err := w.c.store.ListCheckpoints(ctx, p, req.GetRepositoryId(), req.GetWorkspace(), req.GetBeforeId(), limit)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.ListCheckpointsResponse{}
	for i := range cs {
		out.Checkpoints = append(out.Checkpoints, checkpointOut(&cs[i]))
	}
	return out, nil
}

func (w *Workspaces) ListWorkspaces(ctx context.Context, req *scmpb.ListWorkspacesRequest) (*scmpb.ListWorkspacesResponse, error) {
	p, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	ws, err := w.c.store.ListWorkspaces(ctx, p, req.GetRepositoryId())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.ListWorkspacesResponse{}
	for _, s := range ws {
		out.Workspaces = append(out.Workspaces, workspaceStateOut(s))
	}
	return out, nil
}

func (w *Workspaces) CreateWorkspace(ctx context.Context, req *scmpb.CreateWorkspaceRequest) (*scmpb.WorkspaceState, error) {
	p, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	s, err := w.c.store.CreateWorkspace(ctx, p, req.GetRepositoryId(), req.GetWorkspace())
	if err != nil {
		return nil, toStatus(err)
	}
	return workspaceStateOut(s), nil
}

func workspaceStateOut(s store.WorkspaceState) *scmpb.WorkspaceState {
	st := &scmpb.WorkspaceState{Workspace: s.Name, Holder: s.Holder, Epoch: s.Epoch, TakeoverBy: s.TakeoverBy, Latest: checkpointOut(s.Latest), OwnLog: s.OwnLog}
	if !s.ExpiresAt.IsZero() {
		st.ExpiresAt = timestamppb.New(s.ExpiresAt)
	}
	return st
}
