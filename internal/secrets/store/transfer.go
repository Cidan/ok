package store

import (
	"context"
	"fmt"

	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/secrets/secretstore"
	"github.com/Cidan/ok/internal/workflow"
)

// Transfer moves the sealed value of every version of a repository's secrets, not destroyed, that
// another store keeps into to — an external store's name, or default, ok secrets' own database
// (docs/secrets.md, "Stores"): each copied there first, then its row recording the new store, then
// the old store's copy deleted. A version destroyed or moved meanwhile is passed over. It returns
// how many it moved. Run it before a change of the repository's settings' store lands, and again
// after, for what was written meanwhile.
func (s *Store) Transfer(ctx context.Context, repo int64, to string) (int, error) {
	if to == "" {
		to = workflow.DefaultSecretStore
	}
	dest, err := s.cfg.Stores.For(to)
	if err != nil {
		return 0, err
	}
	ctx = move.WithRepository(ctx, repo)
	moved := 0
	var after VersionKey
	for {
		vs, err := s.b.Versions(ctx, repo, after, 500)
		if err != nil {
			return moved, err
		}
		for _, v := range vs {
			if v.Destroyed() || v.Store == to {
				continue
			}
			ok, err := s.transferOne(ctx, v, to, dest)
			if err != nil {
				return moved, fmt.Errorf("%s@%s: %w", v.Path, v.Digest.Short(), err)
			}
			if ok {
				moved++
			}
		}
		if len(vs) < 500 {
			return moved, nil
		}
		after = VersionKey{Path: vs[len(vs)-1].Path, Digest: vs[len(vs)-1].Digest}
	}
}

// transferOne moves one version's sealed value into to, whose store is dest (nil: ok secrets'
// own database).
func (s *Store) transferOne(ctx context.Context, v Version, to string, dest secretstore.Store) (bool, error) {
	key := secretstore.Key(v.Repository, v.Path, v.Digest)
	sealed := v.Sealed
	var src secretstore.Store
	if v.Store != workflow.DefaultSecretStore {
		var err error
		if src, err = s.cfg.Stores.For(v.Store); err != nil {
			return false, err
		}
		if sealed, err = src.Get(ctx, key); err != nil {
			return false, fmt.Errorf("reading it from the %s store: %w", v.Store, err)
		}
	}
	var kept []byte
	if dest != nil {
		if err := dest.Put(ctx, key, sealed); err != nil {
			return false, fmt.Errorf("keeping it in the %s store: %w", to, err)
		}
	} else {
		kept = sealed
	}
	ok, err := s.b.MoveVersion(ctx, v.Repository, v.Path, v.Digest, v.Store, to, kept)
	if err != nil {
		return false, err
	}
	if !ok {
		// Destroyed, or moved by another, meanwhile: the copy is kept only if its row says so.
		now, found, err := s.b.Version(ctx, v.Repository, v.Path, v.Digest)
		if err != nil {
			return false, err
		}
		if dest != nil && (!found || now.Destroyed() || now.Store != to) {
			if err := dest.Delete(ctx, []string{key}); err != nil {
				return false, fmt.Errorf("deleting its copy from the %s store: %w", to, err)
			}
		}
		return false, nil
	}
	if src != nil {
		// Its row names the new store: the old copy is no longer read, and deleting it fails only
		// to free its space.
		if err := src.Delete(ctx, []string{key}); err != nil {
			return true, fmt.Errorf("deleting its old copy from the %s store: %w", v.Store, err)
		}
	}
	return true, nil
}
