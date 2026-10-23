package store

import (
	"context"

	"github.com/Cidan/ok/internal/scm/store/backend"
)

// A fork network (docs/hosted.md, "Fork networks, as built") holds what its repositories share:
// packs, dictionaries, and where each chunk is (its chunk index, cindex.go). A repository is in one
// for good, and keeps for itself everything that says what it holds.

// network is the network repo is in. It never changes, so a replica learns it once.
func (s *Store) network(ctx context.Context, repo int64) (backend.Network, error) {
	if n, ok := s.networks.Load(repo); ok {
		return n.(backend.Network), nil
	}
	r, ok, err := s.ops(ctx).Repository(ctx, repo)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, notFound("repository %d", repo)
	}
	s.networks.Store(repo, r.Network)
	return r.Network, nil
}

// networkOf is repo's network's scope.
func (s *Store) networkOf(ctx context.Context, repo int64) (backend.Network, backend.Scope, error) {
	net, err := s.network(ctx, repo)
	return net, backend.NetworkScope(net), err
}
