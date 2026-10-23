package service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/scm/codesearch"
	"github.com/Cidan/ok/internal/scm/store"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Search is a repository's code search (docs/hosted.md, "ok control at scale, as built").
type Search struct {
	scmpb.UnimplementedSearchServer
	c *core
}

// Code searches main's files, as the latest index of them says.
func (s *Search) Code(ctx context.Context, req *scmpb.CodeSearchRequest) (*scmpb.CodeSearchResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100
	}
	q := codesearch.Query{Pattern: req.GetPattern(), Regexp: req.GetRegexp(), CaseSensitive: req.GetCaseSensitive(), Path: req.GetPath(),
		Limit: min(limit, 1000)}
	res, commit, stale, err := s.c.store.SearchCode(ctx, req.GetRepositoryId(), q)
	switch {
	case errors.Is(err, codesearch.ErrQuery):
		return nil, status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, store.ErrNoCodeIndex):
		return nil, status.Error(codes.Unavailable, err.Error())
	case err != nil:
		return nil, toStatus(err)
	}
	out := &scmpb.CodeSearchResponse{CommitId: commit[:], Stale: stale, Candidates: int32(res.Candidates), Truncated: res.Truncated}
	for _, m := range res.Matches {
		out.Matches = append(out.Matches, &scmpb.CodeMatch{Path: m.Path, Line: int32(m.Line), Text: m.Text, Start: int32(m.Start), End: int32(m.End)})
	}
	return out, nil
}
