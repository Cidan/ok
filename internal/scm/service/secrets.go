package service

import (
	"context"
	"errors"

	"github.com/Cidan/ok/internal/scm/store"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
)

// SecretReferences serves ok secrets the pointers a repository still holds (docs/secrets.md,
// "The SCM").
type SecretReferences struct {
	scmpb.UnimplementedSecretReferencesServer
	c *core
}

func (s *SecretReferences) ListSecretReferences(ctx context.Context, req *scmpb.ListSecretReferencesRequest) (*scmpb.ListSecretReferencesResponse, error) {
	refs, err := s.c.store.SecretReferences(ctx, req.GetRepositoryId())
	if errors.Is(err, store.ErrNotFound) {
		// Deleted, a repository's row stays until its purge: one the SCM holds no more is gone.
		return &scmpb.ListSecretReferencesResponse{State: scmpb.ListSecretReferencesResponse_STATE_PURGED}, nil
	}
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.ListSecretReferencesResponse{State: scmpb.ListSecretReferencesResponse_STATE_ACTIVE}
	for _, r := range refs {
		out.Pointers = append(out.Pointers, &scmpb.SecretPointer{Path: r.Path, Digest: r.Digest[:]})
	}
	return out, nil
}

// SecretVersions is ok secrets' Versions service, which upload asks (store.Secrets).
type SecretVersions struct{ Client secretspb.VersionsClient }

func (v SecretVersions) Kept(ctx context.Context, repo int64, pointers []store.SecretPointer) ([]bool, error) {
	req := &secretspb.CheckPointersRequest{RepositoryId: repo}
	for _, p := range pointers {
		req.Pointers = append(req.Pointers, &secretspb.Pointer{Path: p.Path, Digest: p.Digest[:]})
	}
	r, err := v.Client.CheckPointers(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(r.GetKept()) != len(pointers) {
		return nil, errors.New("ok secrets answered for another number of pointers")
	}
	return r.GetKept(), nil
}

func (v SecretVersions) Writers(ctx context.Context, repo int64, principal string, folders []string) ([]bool, error) {
	r, err := v.Client.CheckWriters(ctx, &secretspb.CheckWritersRequest{RepositoryId: repo, PrincipalId: principal, Folders: folders})
	if err != nil {
		return nil, err
	}
	if len(r.GetWriter()) != len(folders) {
		return nil, errors.New("ok secrets answered for another number of folders")
	}
	return r.GetWriter(), nil
}
