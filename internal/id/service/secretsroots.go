package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Organizations' own roots for their repositories' secrets (docs/secrets.md, "Keys").

// SetSecretsRoot names an organization's own root: its owners'.
func (s *Server) SetSecretsRoot(ctx context.Context, req *identitypb.SetSecretsRootRequest) (*identitypb.SecretsRoot, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.requireOwner(ctx, o, p, "name its secrets' root"); err != nil {
		return nil, grpcError(err)
	}
	if err := s.cfg.Store.SetSecretsRoot(ctx, o.ID, req.GetRoot(), p.ID); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "organization.secrets_root", map[string]any{"organization": o.Handle(), "root": req.GetRoot()})
	return &identitypb.SecretsRoot{Owner: o.Handle(), Root: req.GetRoot()}, nil
}

// GetSecretsRoot reads an organization's own root: its members'.
func (s *Server) GetSecretsRoot(ctx context.Context, req *identitypb.GetSecretsRootRequest) (*identitypb.SecretsRoot, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	o, err := s.organization(ctx, req.GetHandle())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.requireMember(ctx, o, p); err != nil {
		return nil, grpcError(err)
	}
	root, err := s.cfg.Store.SecretsRoot(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.SecretsRoot{Owner: o.Handle(), Root: root}, nil
}

// GetRepositorySecretsRoot is a repository's owner's own root: ok secrets'.
func (s *Server) GetRepositorySecretsRoot(ctx context.Context, req *identitypb.GetRepositorySecretsRootRequest) (*identitypb.SecretsRoot, error) {
	r, err := s.cfg.Store.Repository(ctx, req.GetRepositoryId())
	if err != nil {
		return nil, grpcError(err)
	}
	if r.Owner == "" {
		return nil, status.Errorf(codes.Internal, "repository %d has no owner", r.ID)
	}
	root, err := s.cfg.Store.SecretsRoot(ctx, r.Owner)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.SecretsRoot{Owner: r.Owner, Root: root}, nil
}
