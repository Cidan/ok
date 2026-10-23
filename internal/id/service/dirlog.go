package service

import (
	"context"

	"google.golang.org/protobuf/proto"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Read returns up to a page of a repository's events after a place in its log.
func (s *Server) Read(ctx context.Context, req *identitypb.ReadDirectoryEventsRequest) (*identitypb.ReadDirectoryEventsResponse, error) {
	rows, err := s.cfg.Store.DirectoryEvents(ctx, req.GetRepositoryId(), req.GetAfterSeq())
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.ReadDirectoryEventsResponse{}
	for _, r := range rows {
		ev := &identitypb.DirectoryEvent{}
		if err := proto.Unmarshal(r.Body, ev); err != nil {
			return nil, grpcError(err)
		}
		out.Events = append(out.Events, ev)
	}
	return out, nil
}

// Heads returns how far each repository's log reaches, a page of repositories at a time.
func (s *Server) Heads(ctx context.Context, req *identitypb.DirectoryHeadsRequest) (*identitypb.DirectoryHeadsResponse, error) {
	hs, err := s.cfg.Store.DirectoryHeads(ctx, req.GetAfterRepositoryId())
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.DirectoryHeadsResponse{}
	for _, h := range hs {
		out.Heads = append(out.Heads, &identitypb.DirectoryHeadsResponse_Head{RepositoryId: h.Repository, Seq: h.Seq})
	}
	return out, nil
}
