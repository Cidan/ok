package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"

	"github.com/Cidan/ok/internal/id/store"
)

// Search finds repositories, people, and organizations by part of their names (docs/hosted.md, "ok
// control at scale, as built"): repositories as the caller, or the person ok control names, may see
// them — active ones, a deleted one's admins finding it where they restore it — and every active
// person and organization, as GetPrincipal shows them.
func (s *Server) Search(ctx context.Context, req *identitypb.SearchRequest) (*identitypb.SearchResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 10
	}
	limit = min(limit, 50)
	rs, err := s.cfg.Store.SearchRepositories(ctx, req.GetQuery())
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.SearchResponse{}
	for _, r := range rs {
		if len(out.Repositories) == limit {
			break
		}
		if r.State != store.Active {
			continue
		}
		role, err := s.readerRole(ctx, r, req.GetForPersonId())
		if status.Code(err) == codes.NotFound {
			continue
		}
		if err != nil {
			return nil, grpcError(err)
		}
		e, err := s.entryOut(ctx, r, role, false)
		if err != nil {
			return nil, grpcError(err)
		}
		out.Repositories = append(out.Repositories, e)
	}
	ps, err := s.cfg.Store.SearchPrincipals(ctx, req.GetQuery())
	if err != nil {
		return nil, grpcError(err)
	}
	for _, p := range ps {
		handle, ok := s.names.HandleOf(p.Name)
		if !ok {
			continue
		}
		hit := &identitypb.SearchHit{Id: p.ID, Handle: handle, DisplayName: p.DisplayName}
		switch {
		case p.Kind == store.Org && len(out.Organizations) < limit:
			out.Organizations = append(out.Organizations, hit)
		case p.Kind == store.Human && len(out.People) < limit:
			out.People = append(out.People, hit)
		}
	}
	return out, nil
}
