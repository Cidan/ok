package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Personal tokens (docs/hosted.md, "Robots and tokens per organization"). A token reaches
// repositories and nothing else: identity takes one only on the methods tokenMethods names —
// reading the directory, and what Tenancy does to a repository, which Authorize narrows to what the
// token allows — so a token never mints a token, a credential, or an organization's members.

// tokenMethods are the methods a caller presenting a personal token may call.
var tokenMethods = map[string]bool{
	"/ok.identity.v1.Directory/GetPrincipal":        true,
	"/ok.identity.v1.Directory/ListPrincipals":      true,
	"/ok.identity.v1.Directory/ListRetiredNames":    true,
	"/ok.identity.v1.Directory/GetSet":              true,
	"/ok.identity.v1.Directory/ListSets":            true,
	"/ok.identity.v1.Directory/ExpandSet":           true,
	"/ok.identity.v1.Directory/CheckMembership":     true,
	"/ok.identity.v1.Tenancy/GetOrganization":       true,
	"/ok.identity.v1.Tenancy/ResolveRepository":     true,
	"/ok.identity.v1.Tenancy/GetRepositoryEntry":    true,
	"/ok.identity.v1.Tenancy/ListRepositoryEntries": true,
	"/ok.identity.v1.Tenancy/UpdateRepository":      true,
	"/ok.identity.v1.Tenancy/RenameRepository":      true,
	"/ok.identity.v1.Tenancy/TransferRepository":    true,
	"/ok.identity.v1.Tenancy/DeleteRepository":      true,
	"/ok.identity.v1.Tenancy/RestoreRepository":     true,
	"/ok.identity.v1.Tenancy/ListGrants":            true,
	"/ok.identity.v1.Tenancy/SetGrant":              true,
	"/ok.identity.v1.Tenancy/RemoveGrant":           true,
}

// tokenGate registers services whose every method refuses a caller presenting a personal token,
// unless tokenMethods names it.
type tokenGate struct{ reg grpc.ServiceRegistrar }

func (g tokenGate) RegisterService(desc *grpc.ServiceDesc, impl any) {
	d := *desc
	d.Methods = make([]grpc.MethodDesc, len(desc.Methods))
	for i, m := range desc.Methods {
		method := "/" + desc.ServiceName + "/" + m.MethodName
		inner := m.Handler
		m.Handler = func(srv any, ctx context.Context, dec func(any) error, chain grpc.UnaryServerInterceptor) (any, error) {
			wrapped := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
				checked := func(ctx context.Context, req any) (any, error) {
					if err := refuseToken(ctx, method); err != nil {
						return nil, err
					}
					return h(ctx, req)
				}
				if chain == nil {
					return checked(ctx, req)
				}
				return chain(ctx, req, info, checked)
			}
			return inner(srv, ctx, dec, wrapped)
		}
		d.Methods[i] = m
	}
	d.Streams = make([]grpc.StreamDesc, len(desc.Streams))
	for i, st := range desc.Streams {
		method := "/" + desc.ServiceName + "/" + st.StreamName
		inner := st.Handler
		st.Handler = func(srv any, ss grpc.ServerStream) error {
			if err := refuseToken(ss.Context(), method); err != nil {
				return err
			}
			return inner(srv, ss)
		}
		d.Streams[i] = st
	}
	g.reg.RegisterService(&d, impl)
}

func refuseToken(ctx context.Context, method string) error {
	if c, ok := authn.FromContext(ctx); ok && (c.Via == authn.ViaPersonalToken || c.Via == authn.ViaInstallationToken) && !tokenMethods[method] {
		return status.Error(codes.PermissionDenied, "a token reaches repositories, and nothing else: sign in for this")
	}
	return nil
}

// tokenOf is what the personal token a person presented allows, when the request acts for them.
func tokenOf(ctx context.Context, person string) *authz.Token {
	if c, ok := authn.FromContext(ctx); ok && c.ID == person {
		return c.Token
	}
	return nil
}

func (s *Server) tokenOut(ctx context.Context, t store.Token) (*identitypb.PersonalToken, error) {
	out := &identitypb.PersonalToken{Id: t.ID, Label: t.Label, Role: t.Role.String(), Created: timestamppb.New(t.Created),
		Expires: timestamppb.New(t.Expires), LastUsed: ts(t.LastUsed), Revoked: ts(t.Revoked)}
	for _, id := range t.Repositories {
		name := fmt.Sprintf("#%d", id)
		if r, err := s.cfg.Store.Repository(ctx, id); err == nil {
			if e, err := s.cfg.Store.Entry(ctx, r); err == nil {
				name = e.GetFullName()
			}
		} else if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		out.Repositories = append(out.Repositories, name)
	}
	return out, nil
}

// person is the person a Tokens request is from: themselves, never ok's roles.
func (s *Server) person(ctx context.Context) (store.Principal, error) {
	c, err := callerOf(ctx)
	if err != nil {
		return store.Principal{}, err
	}
	if !c.Human {
		return store.Principal{}, status.Error(codes.PermissionDenied, "personal tokens are a person's")
	}
	return s.actor(ctx, "")
}

func (s *Server) CreateToken(ctx context.Context, req *identitypb.CreateTokenRequest) (*identitypb.CreateTokenResponse, error) {
	p, err := s.person(ctx)
	if err != nil {
		return nil, err
	}
	role := tenancy.Read
	if req.GetRole() != "" {
		if role, err = tenancy.ParseRole(req.GetRole()); err != nil {
			return nil, invalid(err)
		}
	}
	days := req.GetLifetimeDays()
	if days == 0 {
		days = 90
	}
	if days < 1 || days > 366 {
		return nil, status.Error(codes.InvalidArgument, "a token lives 1 to 366 days")
	}
	var repos []int64
	for _, full := range req.GetRepositories() {
		full = strings.TrimSpace(full)
		r, _, err := s.cfg.Store.ResolveFullName(ctx, full)
		if err == nil {
			var f authz.Facts
			if f, err = s.facts(ctx, p.ID, r); err == nil && !authz.Decide(f, authz.ReadCode).Allowed {
				err = store.ErrNotFound
			}
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "no repository is called %s", full)
		}
		if err != nil {
			return nil, grpcError(err)
		}
		repos = append(repos, r.ID)
	}
	secret, t, err := s.cfg.Store.CreateToken(ctx, store.NewToken{Principal: p.ID, Label: req.GetLabel(), Role: role, Repositories: repos,
		Lifetime: time.Duration(days) * 24 * time.Hour})
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "token.create", map[string]any{"token": t.ID, "role": role.String(), "repositories": repos})
	out, err := s.tokenOut(ctx, t)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.CreateTokenResponse{Token: out, Secret: secret}, nil
}

func (s *Server) ListTokens(ctx context.Context, _ *identitypb.ListTokensRequest) (*identitypb.ListTokensResponse, error) {
	p, err := s.person(ctx)
	if err != nil {
		return nil, err
	}
	ts, err := s.cfg.Store.Tokens(ctx, p.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.ListTokensResponse{}
	for _, t := range ts {
		o, err := s.tokenOut(ctx, t)
		if err != nil {
			return nil, grpcError(err)
		}
		out.Tokens = append(out.Tokens, o)
	}
	return out, nil
}

func (s *Server) RevokeToken(ctx context.Context, req *identitypb.RevokeTokenRequest) (*identitypb.RevokeTokenResponse, error) {
	p, err := s.person(ctx)
	if err != nil {
		return nil, err
	}
	revoked, err := s.cfg.Store.RevokeToken(ctx, p.ID, req.GetId())
	if err != nil {
		return nil, grpcError(err)
	}
	if revoked {
		s.audit(ctx, p.Name, "token.revoke", map[string]any{"token": req.GetId()})
	}
	return &identitypb.RevokeTokenResponse{Revoked: revoked}, nil
}
