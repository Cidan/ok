// Package service is ok identity's gRPC services over its store and CA: the directory, credential
// and session verification for the other roles, the CA's roots, device login, a principal's own
// credentials, administration, and applying the groups-and-ACLs tree.
package service

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/serve/authn"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Syncer runs the sync from the system of record.
type Syncer interface {
	Sync(ctx context.Context) (*directory.Plan, error)
	Source() string
}

// Config is what the services need.
type Config struct {
	Store *store.Store
	CA    *ca.CA
	// Syncer runs the sync; nil when the installation has no system of record yet.
	Syncer Syncer
	// PublicURL is where people reach ok's web pages: https://ok.example.com.
	PublicURL string
	// BootstrapAdmins are the handles that administer ok before any tree is applied.
	BootstrapAdmins []string
	// SessionIdle is how long a browser session lives unused.
	SessionIdle time.Duration
}

// Server implements every identity service.
type Server struct {
	identitypb.UnimplementedDirectoryServer
	identitypb.UnimplementedVerifyServer
	identitypb.UnimplementedCAServer
	identitypb.UnimplementedDeviceLoginServer
	identitypb.UnimplementedCredentialsServer
	identitypb.UnimplementedAdminServer
	identitypb.UnimplementedTreeServer

	cfg   Config
	names identity.Names
	apply sync.Mutex // one tree apply at a time
}

// New returns the services over cfg.
func New(cfg Config) *Server {
	if cfg.SessionIdle == 0 {
		cfg.SessionIdle = store.DefaultSessionIdle
	}
	return &Server{cfg: cfg, names: cfg.Store.Names()}
}

// Register registers every service on a gRPC server.
func (s *Server) Register(r grpc.ServiceRegistrar) {
	identitypb.RegisterDirectoryServer(r, s)
	identitypb.RegisterVerifyServer(r, s)
	identitypb.RegisterCAServer(r, s)
	identitypb.RegisterDeviceLoginServer(r, s)
	identitypb.RegisterCredentialsServer(r, s)
	identitypb.RegisterAdminServer(r, s)
	identitypb.RegisterTreeServer(r, s)
}

// grpcError maps the store's and the model's errors to gRPC statuses.
func grpcError(err error) error {
	var ie *identity.Error
	switch {
	case err == nil:
		return nil
	case status.Code(err) != codes.Unknown:
		return err
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrTaken), errors.Is(err, store.ErrKeyEnrolled):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, store.ErrStale):
		return status.Error(codes.Aborted, err.Error())
	case errors.As(err, &ie):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	}
	slog.Error("identity: internal error", "err", err)
	return status.Error(codes.Internal, err.Error())
}

func kindPB(k store.Kind) identitypb.Kind {
	if k == store.Human {
		return identitypb.Kind_KIND_HUMAN
	}
	return identitypb.Kind_KIND_ROBOT
}

func statusPB(s directory.Status) identitypb.Status {
	switch s {
	case directory.Suspended:
		return identitypb.Status_STATUS_SUSPENDED
	case directory.Deleted:
		return identitypb.Status_STATUS_DELETED
	}
	return identitypb.Status_STATUS_ACTIVE
}

func principalPB(p store.Principal) *identitypb.Principal {
	return &identitypb.Principal{
		Id: p.ID, Name: p.Name, Kind: kindPB(p.Kind), Status: statusPB(p.Status), Email: p.Email,
		DisplayName: p.DisplayName, ExternalId: p.ExternalID, Created: timestamppb.New(p.Created),
	}
}

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func credentialPB(c store.Credential) *identitypb.Credential {
	return &identitypb.Credential{Id: c.ID, Principal: c.Principal.Name, Label: c.Label, Created: ts(c.Created), LastIssued: ts(c.LastIssued), Revoked: ts(c.Revoked)}
}

// tree is the applied tree with the synced groups, as the model holds it. A synced group the tree
// names that the system of record no longer holds counts as empty, so the tree still loads.
func (s *Server) tree(ctx context.Context) (*identity.Tree, store.Applied, error) {
	sets, applied, err := s.cfg.Store.Tree(ctx)
	if err != nil {
		return nil, applied, err
	}
	synced, err := s.cfg.Store.SyncedGroups(ctx)
	if err != nil {
		return nil, applied, err
	}
	if len(sets) == 0 {
		return nil, applied, nil
	}
	model := make([]identity.Set, 0, len(sets))
	for _, ts := range sets {
		model = append(model, ts.Set)
		for _, m := range ts.Members {
			if k, ok := s.names.KindOf(m); ok && k == identity.GroupMember {
				if _, ok := synced[m]; !ok {
					synced[m] = nil // gone from the system of record: an empty group
				}
			}
		}
	}
	for _, ts := range sets {
		delete(synced, s.names.OfSet(ts.Set))
	}
	t, err := identity.New(s.names, model, identity.WithSynced(synced))
	return t, applied, err
}

// callerOf is the request's caller, which the services' callers guarantee there is.
func callerOf(ctx context.Context) (authn.Caller, error) {
	c, ok := authn.FromContext(ctx)
	if !ok {
		return authn.Caller{}, status.Error(codes.Unauthenticated, "sign in")
	}
	return c, nil
}

// requireAdmin refuses a caller who is not in admins.acl.<domain> — or, before any tree is
// applied, not among the bootstrap admins.
func (s *Server) requireAdmin(ctx context.Context) (authn.Caller, error) {
	c, err := callerOf(ctx)
	if err != nil {
		return c, err
	}
	ok, why, err := s.isAdmin(ctx, c.Principal)
	if err != nil {
		return c, grpcError(err)
	}
	if !ok {
		return c, status.Error(codes.PermissionDenied, why)
	}
	return c, nil
}

// isAdmin reports whether a principal administers ok, and why not when it does not.
func (s *Server) isAdmin(ctx context.Context, principal string) (bool, string, error) {
	t, _, err := s.tree(ctx)
	if err != nil {
		return false, "", err
	}
	admins := s.names.Of(identity.ACL, identity.Root)
	if t == nil {
		for _, h := range s.cfg.BootstrapAdmins {
			if s.names.Human(h) == principal {
				return true, "", nil
			}
		}
		return false, fmt.Sprintf("%s is not a bootstrap administrator, and no tree declares %s yet", principal, admins), nil
	}
	if _, ok := t.Contains(admins, principal); !ok {
		return false, fmt.Sprintf("%s is not in %s", principal, admins), nil
	}
	return true, "", nil
}

func (s *Server) ListAdmins(ctx context.Context, _ *identitypb.ListAdminsRequest) (*identitypb.ListAdminsResponse, error) {
	t, _, err := s.tree(ctx)
	if err != nil {
		return nil, grpcError(err)
	}
	var all []string
	if t == nil {
		for _, h := range s.cfg.BootstrapAdmins {
			all = append(all, s.names.Human(h))
		}
	} else if all, _, err = s.expand(ctx, s.names.Of(identity.ACL, identity.Root)); err != nil {
		return nil, grpcError(err)
	}
	active, err := s.activeOnly(ctx, all)
	if err != nil {
		return nil, grpcError(err)
	}
	sort.Strings(active)
	return &identitypb.ListAdminsResponse{Principals: active}, nil
}

func (s *Server) CheckAdmin(ctx context.Context, req *identitypb.CheckAdminRequest) (*identitypb.CheckAdminResponse, error) {
	active, err := s.activeOnly(ctx, []string{req.GetPrincipal()})
	if err != nil {
		return nil, grpcError(err)
	}
	if len(active) == 0 {
		return &identitypb.CheckAdminResponse{}, nil
	}
	ok, _, err := s.isAdmin(ctx, req.GetPrincipal())
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.CheckAdminResponse{Admin: ok}, nil
}

func (s *Server) audit(ctx context.Context, actor, action string, detail map[string]any) {
	if err := s.cfg.Store.Audit(ctx, actor, action, detail); err != nil {
		slog.Error("identity: audit", "action", action, "err", err)
	}
}

// ---- Directory ----

func (s *Server) GetPrincipal(ctx context.Context, req *identitypb.GetPrincipalRequest) (*identitypb.GetPrincipalResponse, error) {
	var p store.Principal
	var err error
	switch k := req.Key.(type) {
	case *identitypb.GetPrincipalRequest_Name:
		p, err = s.cfg.Store.Principal(ctx, k.Name)
	case *identitypb.GetPrincipalRequest_Id:
		p, err = s.cfg.Store.PrincipalByID(ctx, k.Id)
	default:
		return nil, status.Error(codes.InvalidArgument, "name a principal, or give its ID")
	}
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.GetPrincipalResponse{Principal: principalPB(p)}, nil
}

func (s *Server) ListPrincipals(ctx context.Context, req *identitypb.ListPrincipalsRequest) (*identitypb.ListPrincipalsResponse, error) {
	size := int(req.GetPageSize())
	if size <= 0 || size > 1000 {
		size = 100
	}
	f := store.Filter{Query: req.GetQuery(), IncludeInactive: req.GetIncludeInactive(), After: req.GetPageToken(), Limit: size + 1}
	switch req.GetKind() {
	case identitypb.Kind_KIND_HUMAN:
		f.Kind = store.Human
	case identitypb.Kind_KIND_ROBOT:
		f.Kind = store.Robot
	}
	ps, err := s.cfg.Store.Principals(ctx, f)
	if err != nil {
		return nil, grpcError(err)
	}
	resp := &identitypb.ListPrincipalsResponse{}
	if len(ps) > size {
		ps = ps[:size]
		resp.NextPageToken = ps[size-1].Name
	}
	for _, p := range ps {
		resp.Principals = append(resp.Principals, principalPB(p))
	}
	return resp, nil
}

func (s *Server) ListRetiredNames(ctx context.Context, _ *identitypb.ListRetiredNamesRequest) (*identitypb.ListRetiredNamesResponse, error) {
	rs, err := s.cfg.Store.RetiredNames(ctx)
	if err != nil {
		return nil, grpcError(err)
	}
	resp := &identitypb.ListRetiredNamesResponse{}
	for _, r := range rs {
		resp.Names = append(resp.Names, &identitypb.RetiredName{Name: r.Name, Retired: timestamppb.New(r.Retired), Reason: r.Reason})
	}
	return resp, nil
}

// sets are every set — the tree's and the synced groups — by name.
func (s *Server) sets(ctx context.Context) (map[string]*identitypb.Set, error) {
	treeSets, _, err := s.cfg.Store.Tree(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]*identitypb.Set{}
	for _, ts := range treeSets {
		kind := identitypb.SetKind_SET_KIND_GROUP
		if ts.Kind == identity.ACL {
			kind = identitypb.SetKind_SET_KIND_ACL
		}
		name := s.names.OfSet(ts.Set)
		out[name] = &identitypb.Set{Name: name, Kind: kind, Source: identitypb.SetSource_SET_SOURCE_TREE, Owner: ts.Owner, Path: ts.Path, Members: ts.Members}
	}
	synced, err := s.cfg.Store.SyncedGroups(ctx)
	if err != nil {
		return nil, err
	}
	admins := s.names.Of(identity.ACL, identity.Root)
	for name, ms := range synced {
		if _, ok := out[name]; !ok {
			out[name] = &identitypb.Set{Name: name, Kind: identitypb.SetKind_SET_KIND_GROUP, Source: identitypb.SetSource_SET_SOURCE_SYNCED, Owner: admins, Members: ms}
		}
	}
	return out, nil
}

func (s *Server) GetSet(ctx context.Context, req *identitypb.GetSetRequest) (*identitypb.GetSetResponse, error) {
	all, err := s.sets(ctx)
	if err != nil {
		return nil, grpcError(err)
	}
	set, ok := all[req.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no set is named %s", req.GetName())
	}
	return &identitypb.GetSetResponse{Set: set}, nil
}

func (s *Server) ListSets(ctx context.Context, _ *identitypb.ListSetsRequest) (*identitypb.ListSetsResponse, error) {
	all, err := s.sets(ctx)
	if err != nil {
		return nil, grpcError(err)
	}
	resp := &identitypb.ListSetsResponse{}
	for _, set := range all {
		resp.Sets = append(resp.Sets, set)
	}
	sort.Slice(resp.Sets, func(i, j int) bool { return resp.Sets[i].Name < resp.Sets[j].Name })
	return resp, nil
}

// model is the tree with the synced groups, or — before a tree is applied — the synced groups
// alone, as a tree's members would see them.
func (s *Server) model(ctx context.Context) (*identity.Tree, map[string][]string, error) {
	t, _, err := s.tree(ctx)
	if err != nil {
		return nil, nil, err
	}
	synced, err := s.cfg.Store.SyncedGroups(ctx)
	return t, synced, err
}

// expand is every principal in a set, through the sets it holds.
func (s *Server) expand(ctx context.Context, name string) ([]string, bool, error) {
	t, synced, err := s.model(ctx)
	if err != nil {
		return nil, false, err
	}
	if t != nil {
		if _, ok := t.Set(name); ok || t.Synced(name) {
			return t.Expand(name), true, nil
		}
	}
	if _, ok := synced[name]; ok {
		// A synced group the tree does not name: expand it over the synced groups alone.
		seen := map[string]bool{}
		var out []string
		var walk func(string)
		walk = func(n string) {
			for _, m := range synced[n] {
				if seen[m] {
					continue
				}
				seen[m] = true
				if _, ok := synced[m]; ok {
					walk(m)
				} else {
					out = append(out, m)
				}
			}
		}
		walk(name)
		sort.Strings(out)
		return out, true, nil
	}
	return nil, false, nil
}

// activeOnly keeps the names of active principals.
func (s *Server) activeOnly(ctx context.Context, names []string) ([]string, error) {
	var out []string
	for _, n := range names {
		p, err := s.cfg.Store.Principal(ctx, n)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if p.Active() {
			out = append(out, n)
		}
	}
	return out, nil
}

func (s *Server) ExpandSet(ctx context.Context, req *identitypb.ExpandSetRequest) (*identitypb.ExpandSetResponse, error) {
	ms, ok, err := s.expand(ctx, req.GetName())
	if err != nil {
		return nil, grpcError(err)
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no set is named %s", req.GetName())
	}
	active, err := s.activeOnly(ctx, ms)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.ExpandSetResponse{Principals: active}, nil
}

func (s *Server) CheckMembership(ctx context.Context, req *identitypb.CheckMembershipRequest) (*identitypb.CheckMembershipResponse, error) {
	active, err := s.activeOnly(ctx, []string{req.GetPrincipal()})
	if err != nil {
		return nil, grpcError(err)
	}
	if len(active) == 0 {
		return &identitypb.CheckMembershipResponse{}, nil
	}
	t, _, err := s.model(ctx)
	if err != nil {
		return nil, grpcError(err)
	}
	if t != nil {
		if chain, ok := t.Contains(req.GetSet(), req.GetPrincipal()); ok {
			return &identitypb.CheckMembershipResponse{Member: true, Chain: chain}, nil
		}
		return &identitypb.CheckMembershipResponse{}, nil
	}
	ms, _, err := s.expand(ctx, req.GetSet())
	if err != nil {
		return nil, grpcError(err)
	}
	for _, m := range ms {
		if m == req.GetPrincipal() {
			return &identitypb.CheckMembershipResponse{Member: true, Chain: []string{req.GetSet(), m}}, nil
		}
	}
	return &identitypb.CheckMembershipResponse{}, nil
}

// ---- Verify ----

func (s *Server) GetCredential(ctx context.Context, req *identitypb.GetCredentialRequest) (*identitypb.GetCredentialResponse, error) {
	c, err := s.cfg.Store.Credential(ctx, req.GetId())
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.GetCredentialResponse{Principal: principalPB(c.Principal), Active: c.Standing()}, nil
}

func (s *Server) GetSession(ctx context.Context, req *identitypb.GetSessionRequest) (*identitypb.GetSessionResponse, error) {
	p, err := s.cfg.Store.Session(ctx, req.GetId(), s.cfg.SessionIdle)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.GetSessionResponse{Principal: principalPB(p)}, nil
}

// ---- CA ----

func (s *Server) GetRoots(context.Context, *identitypb.GetRootsRequest) (*identitypb.GetRootsResponse, error) {
	roots, err := s.cfg.CA.Roots()
	if err != nil {
		return nil, grpcError(err)
	}
	resp := &identitypb.GetRootsResponse{}
	for _, r := range roots {
		resp.Certificates = append(resp.Certificates, r.Raw)
	}
	return resp, nil
}

// ---- DeviceLogin ----

// parseKey reads a credential's key: PKIX DER, ECDSA P-256.
func parseKey(der []byte) (*ecdsa.PublicKey, error) {
	k, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "the public key is not PKIX DER")
	}
	pub, ok := k.(*ecdsa.PublicKey)
	if !ok || pub.Curve != elliptic.P256() {
		return nil, status.Error(codes.InvalidArgument, "a credential's key is ECDSA P-256")
	}
	return pub, nil
}

// checkLabel refuses a label a page could not show plainly.
func checkLabel(label string) error {
	if label == "" || len(label) > 100 {
		return status.Error(codes.InvalidArgument, "a label names the machine, in 1 to 100 characters")
	}
	for _, r := range label {
		if !unicode.IsPrint(r) {
			return status.Error(codes.InvalidArgument, "a label is printable text")
		}
	}
	return nil
}

func (s *Server) Start(ctx context.Context, req *identitypb.StartRequest) (*identitypb.StartResponse, error) {
	if _, err := parseKey(req.GetPublicKey()); err != nil {
		return nil, err
	}
	if err := checkLabel(req.GetLabel()); err != nil {
		return nil, err
	}
	addr := ""
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		addr = p.Addr.String()
	}
	device, code, expires, err := s.cfg.Store.StartDeviceLogin(ctx, req.GetPublicKey(), req.GetLabel(), addr)
	if err != nil {
		return nil, grpcError(err)
	}
	uri := strings.TrimSuffix(s.cfg.PublicURL, "/") + "/id/device"
	return &identitypb.StartResponse{
		DeviceCode:              device,
		UserCode:                code,
		VerificationUri:         uri,
		VerificationUriComplete: uri + "?code=" + code,
		ExpiresIn:               int32(time.Until(expires).Seconds()),
		Interval:                int32(store.PollInterval.Seconds()),
	}, nil
}

func (s *Server) Poll(ctx context.Context, req *identitypb.PollRequest) (*identitypb.PollResponse, error) {
	state, d, err := s.cfg.Store.PollDeviceLogin(ctx, req.GetDeviceCode())
	if err != nil {
		return nil, grpcError(err)
	}
	switch state {
	case store.PollPending:
		return &identitypb.PollResponse{State: identitypb.PollState_POLL_STATE_PENDING}, nil
	case store.PollSlowDown:
		return &identitypb.PollResponse{State: identitypb.PollState_POLL_STATE_SLOW_DOWN}, nil
	case store.PollDenied:
		return &identitypb.PollResponse{State: identitypb.PollState_POLL_STATE_DENIED}, nil
	case store.PollExpired:
		return &identitypb.PollResponse{State: identitypb.PollState_POLL_STATE_EXPIRED}, nil
	}
	p, err := s.cfg.Store.PrincipalByID(ctx, d.PrincipalID)
	if err != nil {
		return nil, grpcError(err)
	}
	if !p.Active() {
		return &identitypb.PollResponse{State: identitypb.PollState_POLL_STATE_DENIED}, nil
	}
	c, err := s.cfg.Store.EnrollKey(ctx, p.ID, d.Label, d.PublicKey)
	if err != nil {
		return nil, grpcError(err)
	}
	cert, err := s.issue(ctx, c)
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Store.CompleteDeviceLogin(ctx, req.GetDeviceCode(), c.ID); errors.Is(err, store.ErrNotFound) {
		return &identitypb.PollResponse{State: identitypb.PollState_POLL_STATE_EXPIRED}, nil
	} else if err != nil {
		return nil, grpcError(err)
	}
	s.audit(ctx, p.Name, "device login", map[string]any{"credential": c.ID, "label": c.Label, "from": d.ClientAddress})
	return &identitypb.PollResponse{State: identitypb.PollState_POLL_STATE_APPROVED, Certificate: cert, Principal: p.Name}, nil
}

// issue issues a certificate for a standing credential and records it.
func (s *Server) issue(ctx context.Context, c store.Credential) (*identitypb.Certificate, error) {
	if !c.Standing() {
		return nil, status.Error(codes.PermissionDenied, "the credential is revoked, or its principal does not stand")
	}
	pub, err := parseKey(c.PublicKey)
	if err != nil {
		return nil, err
	}
	chain, notAfter, err := s.cfg.CA.Issue(pub, c.Principal.Name, c.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.cfg.Store.Issued(ctx, c.ID, notAfter); err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.Certificate{Credential: c.ID, Chain: chain, NotAfter: timestamppb.New(notAfter)}, nil
}

// ---- Credentials ----

func (s *Server) Renew(ctx context.Context, _ *identitypb.RenewRequest) (*identitypb.RenewResponse, error) {
	caller, err := callerOf(ctx)
	if err != nil {
		return nil, err
	}
	if caller.Credential == "" {
		return nil, status.Error(codes.FailedPrecondition, "renew with the credential to renew: this call was not made with one")
	}
	c, err := s.cfg.Store.Credential(ctx, caller.Credential)
	if err != nil {
		return nil, grpcError(err)
	}
	cert, err := s.issue(ctx, c)
	if err != nil {
		return nil, err
	}
	return &identitypb.RenewResponse{Certificate: cert}, nil
}

func (s *Server) List(ctx context.Context, _ *identitypb.ListCredentialsRequest) (*identitypb.ListCredentialsResponse, error) {
	caller, err := callerOf(ctx)
	if err != nil {
		return nil, err
	}
	cs, err := s.cfg.Store.CredentialsOf(ctx, caller.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	resp := &identitypb.ListCredentialsResponse{}
	for _, c := range cs {
		resp.Credentials = append(resp.Credentials, credentialPB(c))
	}
	return resp, nil
}

func (s *Server) Revoke(ctx context.Context, req *identitypb.RevokeRequest) (*identitypb.RevokeResponse, error) {
	caller, err := callerOf(ctx)
	if err != nil {
		return nil, err
	}
	c, err := s.cfg.Store.Credential(ctx, req.GetId())
	if err != nil || c.Principal.ID != caller.ID {
		return nil, status.Errorf(codes.NotFound, "you have no credential %s", req.GetId())
	}
	if err := s.cfg.Store.Revoke(ctx, c.ID); err != nil {
		return nil, grpcError(err)
	}
	s.audit(ctx, caller.Principal, "revoke credential", map[string]any{"credential": c.ID})
	return &identitypb.RevokeResponse{}, nil
}

func (s *Server) Certify(ctx context.Context, req *identitypb.CertifyRequest) (*identitypb.CertifyResponse, error) {
	caller, err := callerOf(ctx)
	if err != nil {
		return nil, err
	}
	if caller.Human || caller.Via != authn.ViaToken {
		return nil, status.Error(codes.PermissionDenied, "a robot certifies a key with its issuer's token; people sign in with ok auth login")
	}
	if _, err := parseKey(req.GetPublicKey()); err != nil {
		return nil, err
	}
	if err := checkLabel(req.GetLabel()); err != nil {
		return nil, err
	}
	c, err := s.cfg.Store.EnrollKey(ctx, caller.ID, req.GetLabel(), req.GetPublicKey())
	if err != nil {
		return nil, grpcError(err)
	}
	cert, err := s.issue(ctx, c)
	if err != nil {
		return nil, err
	}
	s.audit(ctx, caller.Principal, "certify", map[string]any{"credential": c.ID, "label": c.Label})
	return &identitypb.CertifyResponse{Certificate: cert}, nil
}

// ---- Admin ----

func waitingPB(ws []directory.Waiting) []*identitypb.Waiting {
	var out []*identitypb.Waiting
	for _, w := range ws {
		out = append(out, &identitypb.Waiting{ExternalId: w.ExternalID, Group: w.Group, Reason: w.Reason})
	}
	return out
}

func (s *Server) Sync(ctx context.Context, _ *identitypb.SyncRequest) (*identitypb.SyncResponse, error) {
	caller, err := s.requireAdmin(ctx)
	if err != nil {
		return nil, err
	}
	if s.cfg.Syncer == nil {
		return nil, status.Error(codes.FailedPrecondition, "the installation names no system of record")
	}
	plan, err := s.cfg.Syncer.Sync(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "the sync: %v", err)
	}
	s.audit(ctx, caller.Principal, "sync", map[string]any{"humans": len(plan.Humans), "groups": len(plan.Groups)})
	resp := &identitypb.SyncResponse{Waiting: waitingPB(plan.Waiting), Notes: plan.Notes}
	for _, c := range plan.Humans {
		resp.Changes = append(resp.Changes, c.String())
	}
	for _, c := range plan.Groups {
		resp.Changes = append(resp.Changes, c.String())
	}
	return resp, nil
}

func (s *Server) GetSyncStatus(ctx context.Context, _ *identitypb.GetSyncStatusRequest) (*identitypb.GetSyncStatusResponse, error) {
	if _, err := s.requireAdmin(ctx); err != nil {
		return nil, err
	}
	st, err := s.cfg.Store.SyncStatus(ctx)
	if err != nil {
		return nil, grpcError(err)
	}
	resp := &identitypb.GetSyncStatusResponse{LastRun: ts(st.LastRun), Error: st.Error, Waiting: waitingPB(st.Waiting), Notes: st.Notes}
	if s.cfg.Syncer != nil {
		resp.Source = s.cfg.Syncer.Source()
	}
	return resp, nil
}

func (s *Server) SetName(ctx context.Context, req *identitypb.SetNameRequest) (*identitypb.SetNameResponse, error) {
	caller, err := s.requireAdmin(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetExternalId() == "" {
		return nil, status.Error(codes.InvalidArgument, "name the person or group by its external ID")
	}
	if err := s.cfg.Store.SetName(ctx, req.GetExternalId(), req.GetGroup(), req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.audit(ctx, caller.Principal, "set name", map[string]any{"external_id": req.GetExternalId(), "group": req.GetGroup(), "name": req.GetName()})
	return &identitypb.SetNameResponse{}, nil
}

// robotName is a robot's full name from what precedes .robot.<domain>.
func (s *Server) robotName(short string) (string, error) {
	full := s.names.Robot(short)
	if k, ok := s.names.KindOf(full); !ok || k != identity.Robot {
		return "", status.Errorf(codes.InvalidArgument, "%q is not a robot's name: one or two lowercase DNS labels, as ci or deploy.ok", short)
	}
	return full, nil
}

func (s *Server) CreateRobot(ctx context.Context, req *identitypb.CreateRobotRequest) (*identitypb.CreateRobotResponse, error) {
	caller, err := s.requireAdmin(ctx)
	if err != nil {
		return nil, err
	}
	name, err := s.robotName(req.GetName())
	if err != nil {
		return nil, err
	}
	p, err := s.cfg.Store.CreateRobot(ctx, name)
	if err != nil {
		return nil, grpcError(err)
	}
	s.audit(ctx, caller.Principal, "create robot", map[string]any{"robot": name})
	return &identitypb.CreateRobotResponse{Principal: principalPB(p)}, nil
}

func (s *Server) DeleteRobot(ctx context.Context, req *identitypb.DeleteRobotRequest) (*identitypb.DeleteRobotResponse, error) {
	caller, err := s.requireAdmin(ctx)
	if err != nil {
		return nil, err
	}
	name, err := s.robotName(req.GetName())
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Store.DeleteRobot(ctx, name); err != nil {
		return nil, grpcError(err)
	}
	s.audit(ctx, caller.Principal, "delete robot", map[string]any{"robot": name})
	return &identitypb.DeleteRobotResponse{}, nil
}

func (s *Server) ListRobots(ctx context.Context, _ *identitypb.ListRobotsRequest) (*identitypb.ListRobotsResponse, error) {
	if _, err := s.requireAdmin(ctx); err != nil {
		return nil, err
	}
	robots, err := s.cfg.Store.Robots(ctx)
	if err != nil {
		return nil, grpcError(err)
	}
	resp := &identitypb.ListRobotsResponse{}
	for _, r := range robots {
		rb := &identitypb.Robot{Principal: principalPB(r.Principal)}
		for _, b := range r.Bindings {
			rb.Bindings = append(rb.Bindings, &identitypb.Binding{Issuer: b.Issuer, Subject: b.Subject})
		}
		resp.Robots = append(resp.Robots, rb)
	}
	return resp, nil
}

func (s *Server) Bind(ctx context.Context, req *identitypb.BindRequest) (*identitypb.BindResponse, error) {
	caller, err := s.requireAdmin(ctx)
	if err != nil {
		return nil, err
	}
	name, err := s.robotName(req.GetRobot())
	if err != nil {
		return nil, err
	}
	b := store.Binding{Issuer: req.GetBinding().GetIssuer(), Subject: req.GetBinding().GetSubject()}
	if err := s.cfg.Store.Bind(ctx, name, b); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, grpcError(err)
		}
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	s.audit(ctx, caller.Principal, "bind robot", map[string]any{"robot": name, "issuer": b.Issuer, "subject": b.Subject})
	return &identitypb.BindResponse{}, nil
}

func (s *Server) Unbind(ctx context.Context, req *identitypb.UnbindRequest) (*identitypb.UnbindResponse, error) {
	caller, err := s.requireAdmin(ctx)
	if err != nil {
		return nil, err
	}
	name, err := s.robotName(req.GetRobot())
	if err != nil {
		return nil, err
	}
	b := store.Binding{Issuer: req.GetBinding().GetIssuer(), Subject: req.GetBinding().GetSubject()}
	if err := s.cfg.Store.Unbind(ctx, name, b); err != nil {
		return nil, grpcError(err)
	}
	s.audit(ctx, caller.Principal, "unbind robot", map[string]any{"robot": name, "issuer": b.Issuer, "subject": b.Subject})
	return &identitypb.UnbindResponse{}, nil
}

func (s *Server) CertifyRobot(ctx context.Context, req *identitypb.CertifyRobotRequest) (*identitypb.CertifyRobotResponse, error) {
	caller, err := s.requireAdmin(ctx)
	if err != nil {
		return nil, err
	}
	name, err := s.robotName(req.GetRobot())
	if err != nil {
		return nil, err
	}
	if _, err := parseKey(req.GetPublicKey()); err != nil {
		return nil, err
	}
	if err := checkLabel(req.GetLabel()); err != nil {
		return nil, err
	}
	p, err := s.cfg.Store.Principal(ctx, name)
	if err != nil {
		return nil, grpcError(err)
	}
	if p.Kind != store.Robot || !p.Active() {
		return nil, status.Errorf(codes.FailedPrecondition, "%s is not an active robot", name)
	}
	c, err := s.cfg.Store.EnrollKey(ctx, p.ID, req.GetLabel(), req.GetPublicKey())
	if err != nil {
		return nil, grpcError(err)
	}
	cert, err := s.issue(ctx, c)
	if err != nil {
		return nil, err
	}
	s.audit(ctx, caller.Principal, "certify robot", map[string]any{"robot": name, "credential": c.ID})
	return &identitypb.CertifyRobotResponse{Certificate: cert}, nil
}

// CertifyLocal enrolls a robot's key and certifies it, for one of ok's roles running in the same
// process as identity (ok serve), which no token need prove: the process is identity.
func (s *Server) CertifyLocal(ctx context.Context, robot string, publicKey []byte, label string) (*identitypb.Certificate, error) {
	name, err := s.robotName(robot)
	if err != nil {
		return nil, err
	}
	if _, err := parseKey(publicKey); err != nil {
		return nil, err
	}
	if err := checkLabel(label); err != nil {
		return nil, err
	}
	p, err := s.cfg.Store.Principal(ctx, name)
	if err != nil {
		return nil, grpcError(err)
	}
	if p.Kind != store.Robot || !p.Active() {
		return nil, status.Errorf(codes.FailedPrecondition, "%s is not an active robot", name)
	}
	c, err := s.cfg.Store.EnrollKey(ctx, p.ID, label, publicKey)
	if err != nil {
		return nil, grpcError(err)
	}
	cert, err := s.issue(ctx, c)
	if err != nil {
		return nil, err
	}
	s.audit(ctx, name, "certify locally", map[string]any{"credential": c.ID, "label": label})
	return cert, nil
}

// ReissueLocal certifies a credential CertifyLocal made again, before its certificate expires.
func (s *Server) ReissueLocal(ctx context.Context, credentialID string) (*identitypb.Certificate, error) {
	c, err := s.cfg.Store.Credential(ctx, credentialID)
	if err != nil {
		return nil, grpcError(err)
	}
	return s.issue(ctx, c)
}

func (s *Server) RevokeCredential(ctx context.Context, req *identitypb.RevokeCredentialRequest) (*identitypb.RevokeCredentialResponse, error) {
	caller, err := s.requireAdmin(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Store.Revoke(ctx, req.GetId()); err != nil {
		return nil, grpcError(err)
	}
	s.audit(ctx, caller.Principal, "revoke credential", map[string]any{"credential": req.GetId()})
	return &identitypb.RevokeCredentialResponse{}, nil
}

func (s *Server) ListCredentials(ctx context.Context, req *identitypb.AdminListCredentialsRequest) (*identitypb.AdminListCredentialsResponse, error) {
	if _, err := s.requireAdmin(ctx); err != nil {
		return nil, err
	}
	p, err := s.cfg.Store.Principal(ctx, req.GetPrincipal())
	if err != nil {
		return nil, grpcError(err)
	}
	cs, err := s.cfg.Store.CredentialsOf(ctx, p.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	resp := &identitypb.AdminListCredentialsResponse{}
	for _, c := range cs {
		resp.Credentials = append(resp.Credentials, credentialPB(c))
	}
	return resp, nil
}

// ---- Tree ----

// planTree loads the files as a tree, checks the change from the applied one, and returns it with
// what it changes.
func (s *Server) planTree(ctx context.Context, files []*identitypb.File) (*identity.Tree, store.Applied, []string, error) {
	fs := map[string][]byte{}
	for _, f := range files {
		fs[f.GetPath()] = f.GetContent()
	}
	synced, err := s.cfg.Store.SyncedGroups(ctx)
	if err != nil {
		return nil, store.Applied{}, nil, err
	}
	after, err := identity.LoadFiles(s.names, fs, identity.WithSynced(synced))
	if err != nil {
		return nil, store.Applied{}, nil, err
	}
	before, applied, err := s.tree(ctx)
	if err != nil {
		return nil, applied, nil, err
	}
	if err := identity.Check(before, after); err != nil {
		return nil, applied, nil, err
	}
	var changes []string
	for _, c := range identity.Diff(before, after) {
		changes = append(changes, c.String())
	}
	return after, applied, changes, nil
}

func (s *Server) Plan(ctx context.Context, req *identitypb.PlanRequest) (*identitypb.PlanResponse, error) {
	_, _, changes, err := s.planTree(ctx, req.GetFiles())
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.PlanResponse{Changes: changes}, nil
}

func (s *Server) Apply(ctx context.Context, req *identitypb.ApplyRequest) (*identitypb.ApplyResponse, error) {
	if req.GetCommit() == "" {
		return nil, status.Error(codes.InvalidArgument, "name the commit the tree is at")
	}
	s.apply.Lock()
	defer s.apply.Unlock()
	after, applied, changes, err := s.planTree(ctx, req.GetFiles())
	if err != nil {
		return nil, grpcError(err)
	}
	if applied.Commit == req.GetCommit() && applied.Sequence == req.GetSequence() {
		return &identitypb.ApplyResponse{}, nil
	}
	if err := s.cfg.Store.ReplaceTree(ctx, applied, req.GetCommit(), req.GetSequence(), after); err != nil {
		return nil, grpcError(err)
	}
	caller, _ := authn.FromContext(ctx)
	s.audit(ctx, caller.Principal, "apply tree", map[string]any{"commit": req.GetCommit(), "sequence": req.GetSequence(), "changes": changes})
	return &identitypb.ApplyResponse{Changes: changes}, nil
}

func (s *Server) GetApplied(ctx context.Context, _ *identitypb.GetAppliedRequest) (*identitypb.GetAppliedResponse, error) {
	_, applied, err := s.cfg.Store.Tree(ctx)
	if err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.GetAppliedResponse{Commit: applied.Commit, Sequence: applied.Sequence, Applied: ts(applied.At)}, nil
}

// ---- the identity role's own resolver ----

// Directory is the authn.Directory of the identity role itself, which reads its store directly.
type Directory struct {
	Store *store.Store
	Idle  time.Duration
}

func toAuthn(p store.Principal, active bool) authn.Principal {
	return authn.Principal{Name: p.Name, ID: p.ID, Human: p.Kind == store.Human, Active: active}
}

func mapNotFound(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return authn.ErrNotFound
	}
	return err
}

func (d Directory) Principal(ctx context.Context, name string) (authn.Principal, error) {
	p, err := d.Store.Principal(ctx, name)
	if err != nil {
		return authn.Principal{}, mapNotFound(err)
	}
	return toAuthn(p, p.Active()), nil
}

func (d Directory) Credential(ctx context.Context, id string) (authn.Principal, error) {
	c, err := d.Store.Credential(ctx, id)
	if err != nil {
		return authn.Principal{}, mapNotFound(err)
	}
	return toAuthn(c.Principal, c.Standing()), nil
}

func (d Directory) Session(ctx context.Context, id string) (authn.Principal, error) {
	idle := d.Idle
	if idle == 0 {
		idle = store.DefaultSessionIdle
	}
	p, err := d.Store.Session(ctx, id, idle)
	if err != nil {
		return authn.Principal{}, mapNotFound(err)
	}
	return toAuthn(p, true), nil
}
