package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Apps (docs/hosted.md, "Robots and tokens per organization"): an organization's robots with a
// public key, installed in organizations with a role, signing in with a JWT their key signs for an
// hour's installation token.

// maxAssertion bounds an app's JWT: issued now, and expiring within ten minutes of it.
const maxAssertion = 10 * time.Minute

// assertionLeeway is the clock skew an app's JWT is allowed.
const assertionLeeway = time.Minute

// verifyAssertion verifies an app's JWT — its key's signature, ES256 or EdDSA; its iss the app; its
// aud ok's public URL; issued now, expiring within ten minutes — and answers the app, whose robot
// stands.
func (s *Server) verifyAssertion(ctx context.Context, raw string) (store.App, error) {
	refuse := func(why string) (store.App, error) {
		return store.App{}, status.Error(codes.Unauthenticated, "the app's assertion: "+why)
	}
	tok, err := jwt.ParseSigned(raw, []jose.SignatureAlgorithm{jose.ES256, jose.EdDSA})
	if err != nil {
		return refuse("not a JWT signed with ES256 or EdDSA")
	}
	var unverified jwt.Claims
	if err := tok.UnsafeClaimsWithoutVerification(&unverified); err != nil || !strings.HasPrefix(unverified.Issuer, "app_") {
		return refuse("its iss names no app")
	}
	app, err := s.cfg.Store.App(ctx, unverified.Issuer)
	if errors.Is(err, store.ErrNotFound) {
		return refuse("no app is " + unverified.Issuer)
	}
	if err != nil {
		return store.App{}, grpcError(err)
	}
	key, err := store.ParseAppKey(app.PublicKey)
	if err != nil {
		return store.App{}, grpcError(err)
	}
	var c jwt.Claims
	if err := tok.Claims(key, &c); err != nil {
		return refuse("its signature is not the app's key's")
	}
	now := time.Now()
	if err := c.ValidateWithLeeway(jwt.Expected{Issuer: app.ID, AnyAudience: jwt.Audience{s.audience()}, Time: now}, assertionLeeway); err != nil {
		return refuse(err.Error())
	}
	if c.IssuedAt == nil || c.Expiry == nil || c.Expiry.Time().Sub(c.IssuedAt.Time()) > maxAssertion {
		return refuse(fmt.Sprintf("it is issued and expires within %s", maxAssertion))
	}
	robot, err := s.cfg.Store.PrincipalByID(ctx, app.Robot)
	if err != nil {
		return store.App{}, grpcError(err)
	}
	if !robot.Active() {
		return refuse("the app's robot does not stand")
	}
	return app, nil
}

// audience is what an app's JWT names as its aud: ok's public URL.
func (s *Server) audience() string { return strings.TrimRight(s.cfg.PublicURL, "/") }

func (s *Server) appOut(ctx context.Context, a store.App) (*identitypb.AppInfo, error) {
	robot, err := s.cfg.Store.PrincipalByID(ctx, a.Robot)
	if err != nil {
		return nil, grpcError(err)
	}
	o, err := s.cfg.Store.Organization(ctx, a.Organization)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.AppInfo{Id: a.ID, Robot: principalPB(robot), Organization: o.Handle(), Public: a.Public, PublicKey: a.PublicKey,
		Created: timestamppb.New(a.Created)}, nil
}

func (s *Server) installationOut(ctx context.Context, i store.Installation) (*identitypb.AppInstallation, error) {
	o, err := s.cfg.Store.Organization(ctx, i.Organization)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.AppInstallation{Id: i.ID, AppId: i.App, Organization: o.Handle(), Role: i.Role.String(), Created: timestamppb.New(i.Created)}
	for _, id := range i.Repositories {
		r, err := s.cfg.Store.Repository(ctx, id)
		if err != nil {
			return nil, grpcError(err)
		}
		e, err := s.cfg.Store.Entry(ctx, r)
		if err != nil {
			return nil, grpcError(err)
		}
		out.Repositories = append(out.Repositories, e.GetFullName())
	}
	return out, nil
}

// appOwned is an app, which the person asking owns the maker of.
func (s *Server) appOwned(ctx context.Context, id, forPerson, what string) (store.App, store.Principal, error) {
	p, err := s.actor(ctx, forPerson)
	if err != nil {
		return store.App{}, store.Principal{}, err
	}
	a, err := s.cfg.Store.App(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return store.App{}, store.Principal{}, status.Errorf(codes.NotFound, "no app is %s", id)
	}
	if err != nil {
		return store.App{}, store.Principal{}, grpcError(err)
	}
	o, err := s.cfg.Store.Organization(ctx, a.Organization)
	if err != nil {
		return store.App{}, store.Principal{}, grpcError(err)
	}
	if err := s.requireOwner(ctx, o, p, what); err != nil {
		return store.App{}, store.Principal{}, grpcError(err)
	}
	return a, p, nil
}

func (s *Server) CreateApp(ctx context.Context, req *identitypb.CreateAppRequest) (*identitypb.AppInfo, error) {
	o, p, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "make its apps")
	if err != nil {
		return nil, err
	}
	a, _, err := s.cfg.Store.CreateApp(ctx, o.ID, req.GetName(), req.GetPublic(), req.GetPublicKey())
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "app.create", map[string]any{"organization": o.Handle(), "app": a.ID, "public": a.Public})
	return s.appOut(ctx, a)
}

func (s *Server) ListApps(ctx context.Context, req *identitypb.ListAppsRequest) (*identitypb.ListAppsResponse, error) {
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
	apps, err := s.cfg.Store.Apps(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.ListAppsResponse{}
	for _, a := range apps {
		ai, err := s.appOut(ctx, a)
		if err != nil {
			return nil, err
		}
		out.Apps = append(out.Apps, ai)
	}
	return out, nil
}

func (s *Server) SetAppKey(ctx context.Context, req *identitypb.SetAppKeyRequest) (*identitypb.AppInfo, error) {
	a, p, err := s.appOwned(ctx, req.GetId(), req.GetForPersonId(), "replace its apps' keys")
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Store.SetAppKey(ctx, a.ID, req.GetPublicKey()); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "app.key", map[string]any{"app": a.ID})
	a.PublicKey = req.GetPublicKey()
	return s.appOut(ctx, a)
}

func (s *Server) DeleteApp(ctx context.Context, req *identitypb.AppRef) (*identitypb.DeleteAppResponse, error) {
	a, p, err := s.appOwned(ctx, req.GetId(), req.GetForPersonId(), "delete its apps")
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Store.DeleteApp(ctx, a.ID); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "app.delete", map[string]any{"app": a.ID})
	return &identitypb.DeleteAppResponse{}, nil
}

func (s *Server) InstallApp(ctx context.Context, req *identitypb.InstallAppRequest) (*identitypb.AppInstallation, error) {
	o, p, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "install apps")
	if err != nil {
		return nil, err
	}
	role, err := tenancy.ParseRole(req.GetRole())
	if err != nil {
		return nil, invalid(err)
	}
	if _, err := s.cfg.Store.App(ctx, req.GetAppId()); errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "no app is %s", req.GetAppId())
	} else if err != nil {
		return nil, grpcError(err)
	}
	var repos []int64
	for _, full := range req.GetRepositories() {
		r, _, err := s.cfg.Store.ResolveFullName(ctx, strings.TrimSpace(full))
		if errors.Is(err, store.ErrNotFound) || (err == nil && r.Owner != o.ID) {
			return nil, status.Errorf(codes.NotFound, "%s has no repository called %s", o.Handle(), full)
		}
		if err != nil {
			return nil, grpcError(err)
		}
		repos = append(repos, r.ID)
	}
	i, err := s.cfg.Store.Install(ctx, req.GetAppId(), o.ID, role, repos, p.ID)
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "app.install", map[string]any{"organization": o.Handle(), "app": i.App, "installation": i.ID, "role": role.String(),
		"repositories": repos})
	return s.installationOut(ctx, i)
}

func (s *Server) ListInstallations(ctx context.Context, req *identitypb.ListInstallationsRequest) (*identitypb.ListInstallationsResponse, error) {
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
	is, err := s.cfg.Store.Installations(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	return s.installationsOut(ctx, is)
}

func (s *Server) installationsOut(ctx context.Context, is []store.Installation) (*identitypb.ListInstallationsResponse, error) {
	out := &identitypb.ListInstallationsResponse{}
	for _, i := range is {
		o, err := s.installationOut(ctx, i)
		if err != nil {
			return nil, err
		}
		out.Installations = append(out.Installations, o)
	}
	return out, nil
}

func (s *Server) UninstallApp(ctx context.Context, req *identitypb.UninstallAppRequest) (*identitypb.UninstallAppResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	i, err := s.cfg.Store.AppInstallation(ctx, req.GetId())
	if errors.Is(err, store.ErrNotFound) {
		return &identitypb.UninstallAppResponse{}, nil
	}
	if err != nil {
		return nil, grpcError(err)
	}
	o, err := s.cfg.Store.Organization(ctx, i.Organization)
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.requireOwner(ctx, o, p, "uninstall apps"); err != nil {
		return nil, grpcError(err)
	}
	removed, err := s.cfg.Store.Uninstall(ctx, i.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	if removed {
		s.audit(ctx, p.Name, "app.uninstall", map[string]any{"organization": o.Handle(), "app": i.App, "installation": i.ID})
	}
	return &identitypb.UninstallAppResponse{Uninstalled: removed}, nil
}

// ─── AppTokens: the app itself ───

func (s *Server) ListAppInstallations(ctx context.Context, req *identitypb.AppAssertion) (*identitypb.ListInstallationsResponse, error) {
	a, err := s.verifyAssertion(ctx, req.GetJwt())
	if err != nil {
		return nil, err
	}
	is, err := s.cfg.Store.InstallationsOfApp(ctx, a.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	return s.installationsOut(ctx, is)
}

func (s *Server) CreateInstallationToken(ctx context.Context, req *identitypb.CreateInstallationTokenRequest) (*identitypb.CreateInstallationTokenResponse, error) {
	a, err := s.verifyAssertion(ctx, req.GetJwt())
	if err != nil {
		return nil, err
	}
	i, err := s.cfg.Store.AppInstallation(ctx, req.GetInstallationId())
	if errors.Is(err, store.ErrNotFound) || (err == nil && i.App != a.ID) {
		return nil, status.Errorf(codes.NotFound, "the app has no installation %s", req.GetInstallationId())
	}
	if err != nil {
		return nil, grpcError(err)
	}
	secret, t, err := s.cfg.Store.CreateInstallationToken(ctx, i.ID)
	if err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, a.ID, "app.token", map[string]any{"app": a.ID, "installation": i.ID, "token": t.ID})
	out, err := s.installationOut(ctx, i)
	if err != nil {
		return nil, err
	}
	return &identitypb.CreateInstallationTokenResponse{Token: secret, Expires: timestamppb.New(t.Expires), Installation: out}, nil
}
