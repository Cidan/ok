package service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"

	"github.com/Cidan/ok/internal/id/store"
)

// Inboxes (docs/hosted.md, "ok control at scale, as built"): ok control's cells set each CL's entries;
// people list theirs, across every cell.

// SetInbox sets a CL's every entry, at a version.
func (s *Server) SetInbox(ctx context.Context, req *identitypb.SetInboxRequest) (*identitypb.SetInboxResponse, error) {
	entries := make([]store.InboxEntry, 0, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		entries = append(entries, store.InboxEntry{Principal: e.GetPrincipalId(), Repository: e.GetRepositoryId(), Number: e.GetNumber(),
			Relation: store.InboxRelation(e.GetRelation()), Attention: e.GetAttention(), RepositoryName: e.GetRepository(), Title: e.GetTitle(),
			Status: e.GetStatus(), UpdatedAt: e.GetUpdatedAt().AsTime()})
	}
	applied, err := s.cfg.Store.SetInbox(ctx, req.GetRepositoryId(), req.GetNumber(), req.GetVersion(), entries)
	if err != nil {
		return nil, tenancyError(err)
	}
	return &identitypb.SetInboxResponse{Applied: applied}, nil
}

// ListInbox lists a person's entries of repositories they may still see.
func (s *Server) ListInbox(ctx context.Context, req *identitypb.ListInboxRequest) (*identitypb.ListInboxResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 50
	}
	limit = min(limit, 200)
	entries, err := s.cfg.Store.Inbox(ctx, p.ID, store.InboxRelation(req.GetRelation()), req.GetClosed(), limit)
	if err != nil {
		return nil, grpcError(err)
	}
	seen := map[int64]bool{}
	out := &identitypb.ListInboxResponse{}
	for _, e := range entries {
		visible, ok := seen[e.Repository]
		if !ok {
			if visible, err = s.mayRead(ctx, e.Repository, p.ID); err != nil {
				return nil, err
			}
			seen[e.Repository] = visible
		}
		if !visible {
			continue
		}
		out.Entries = append(out.Entries, &identitypb.InboxEntry{PrincipalId: e.Principal, RepositoryId: e.Repository, Number: e.Number,
			Relation: identitypb.InboxEntry_Relation(e.Relation), Attention: e.Attention, Repository: e.RepositoryName, Title: e.Title,
			Status: e.Status, UpdatedAt: timestamppb.New(e.UpdatedAt)})
	}
	return out, nil
}

// mayRead reports whether a person may see a repository, active as it is.
func (s *Server) mayRead(ctx context.Context, repo int64, person string) (bool, error) {
	r, err := s.cfg.Store.Repository(ctx, repo)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, grpcError(err)
	}
	if r.State != store.Active {
		return false, nil
	}
	if _, err := s.readerRole(ctx, r, person); status.Code(err) == codes.NotFound {
		return false, nil
	} else if err != nil {
		return false, grpcError(err)
	}
	return true, nil
}
