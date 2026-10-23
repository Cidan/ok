package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/directory/ldapdir"
	"github.com/Cidan/ok/internal/id/enterprise"
	"github.com/Cidan/ok/internal/id/store"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// An enterprise organization's identity provider (docs/hosted.md, "Enterprise organizations"):
// its owners' to set, read, sync, and remove.

// settingsIn is a request's settings as stored, and its secrets.
func settingsIn(in *identitypb.IdentityProviderSettings) (enterprise.Settings, enterprise.Secrets) {
	var set enterprise.Settings
	var sec enterprise.Secrets
	if g := in.GetGoogle(); g != nil {
		set.Google = &enterprise.Google{Subject: g.GetSubject(), Customer: g.GetCustomer(), GroupQuery: g.GetGroupQuery()}
		sec.GoogleKey = g.GetServiceAccountKey()
	}
	if e := in.GetEntra(); e != nil {
		set.Entra = &enterprise.Entra{TenantID: e.GetTenantId(), ClientID: e.GetClientId(), GroupFilter: e.GetGroupFilter(), IncludeGuests: e.GetIncludeGuests()}
		sec.EntraClientSecret = e.GetClientSecret()
	}
	if l := in.GetLdap(); l != nil {
		p, g := l.GetPeople(), l.GetGroups()
		set.LDAP = &enterprise.LDAP{URL: l.GetUrl(), StartTLS: l.GetStartTls(), CA: l.GetCa(), BindDN: l.GetBindDn(),
			People: ldapdir.People{BaseDN: p.GetBaseDn(), Filter: p.GetFilter(), DisabledFilter: p.GetDisabledFilter(), IDAttribute: p.GetIdAttribute(),
				HandleAttribute: p.GetHandleAttribute(), EmailAttribute: p.GetEmailAttribute(), NameAttribute: p.GetNameAttribute()},
			Groups: ldapdir.Groups{BaseDN: g.GetBaseDn(), Filter: g.GetFilter(), IDAttribute: g.GetIdAttribute(), NameAttribute: g.GetNameAttribute(),
				MemberAttribute: g.GetMemberAttribute()}}
		sec.LDAPBindPassword = l.GetBindPassword()
	}
	if in.GetScim() != nil {
		set.SCIM = &enterprise.SCIM{}
	}
	if si := in.GetSignIn(); si != nil {
		set.SignIn = &enterprise.SignIn{Issuer: si.GetIssuer(), ClientID: si.GetClientId(), Match: si.GetMatch(), Claim: si.GetClaim()}
		sec.OIDCClientSecret = si.GetClientSecret()
	}
	return set, sec
}

// settingsOut is a provider's settings as answered: no secret.
func settingsOut(set enterprise.Settings, interval time.Duration) *identitypb.IdentityProviderSettings {
	out := &identitypb.IdentityProviderSettings{IntervalSecs: int64(interval / time.Second)}
	if g := set.Google; g != nil {
		out.Google = &identitypb.GoogleProvider{Subject: g.Subject, Customer: g.Customer, GroupQuery: g.GroupQuery}
	}
	if e := set.Entra; e != nil {
		out.Entra = &identitypb.EntraProvider{TenantId: e.TenantID, ClientId: e.ClientID, GroupFilter: e.GroupFilter, IncludeGuests: e.IncludeGuests}
	}
	if l := set.LDAP; l != nil {
		p, g := l.People, l.Groups
		out.Ldap = &identitypb.LDAPProvider{Url: l.URL, StartTls: l.StartTLS, Ca: l.CA, BindDn: l.BindDN,
			People: &identitypb.LDAPPeople{BaseDn: p.BaseDN, Filter: p.Filter, DisabledFilter: p.DisabledFilter, IdAttribute: p.IDAttribute,
				HandleAttribute: p.HandleAttribute, EmailAttribute: p.EmailAttribute, NameAttribute: p.NameAttribute},
			Groups: &identitypb.LDAPGroups{BaseDn: g.BaseDN, Filter: g.Filter, IdAttribute: g.IDAttribute, NameAttribute: g.NameAttribute,
				MemberAttribute: g.MemberAttribute}}
	}
	if set.SCIM != nil {
		out.Scim = &identitypb.SCIMProvider{}
	}
	if si := set.SignIn; si != nil {
		out.SignIn = &identitypb.OIDCSignIn{Issuer: si.Issuer, ClientId: si.ClientID, Match: si.Match, Claim: si.Claim}
	}
	return out
}

// keep fills the secrets a request leaves empty with those set before.
func keep(sec, before enterprise.Secrets) enterprise.Secrets {
	if len(sec.GoogleKey) == 0 {
		sec.GoogleKey = before.GoogleKey
	}
	if sec.EntraClientSecret == "" {
		sec.EntraClientSecret = before.EntraClientSecret
	}
	if sec.LDAPBindPassword == "" {
		sec.LDAPBindPassword = before.LDAPBindPassword
	}
	if sec.OIDCClientSecret == "" {
		sec.OIDCClientSecret = before.OIDCClientSecret
	}
	return sec
}

// enterpriseOn refuses where identity holds no sealing key: an organization's secrets are never
// stored in the clear.
func (s *Server) enterpriseOn() error {
	if s.cfg.Enterprise == nil {
		return status.Error(codes.FailedPrecondition, "this ok keeps no organization's identity provider: it has no sealing key (identity.sealing_key_file)")
	}
	return nil
}

func (s *Server) providerInfo(ctx context.Context, o store.Organization) (*identitypb.IdentityProviderInfo, error) {
	p, err := s.cfg.Store.IdentityProviderOf(ctx, o.ID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "%s has no identity provider", o.Handle())
	}
	if err != nil {
		return nil, grpcError(err)
	}
	var set enterprise.Settings
	if err := json.Unmarshal(p.Settings, &set); err != nil {
		return nil, grpcError(err)
	}
	base := strings.TrimRight(s.cfg.PublicURL, "/")
	out := &identitypb.IdentityProviderInfo{Settings: settingsOut(set, p.Interval), LastError: p.LastError, Notes: p.Notes}
	if !p.LastRun.IsZero() {
		out.LastRun = timestamppb.New(p.LastRun)
	}
	for _, w := range p.Waiting {
		out.Waiting = append(out.Waiting, fmt.Sprintf("%s: %s", w.ExternalID, w.Reason))
	}
	if set.SCIM != nil {
		out.ScimUrl = base + "/id/scim/v2/orgs/" + url.PathEscape(o.Handle())
	}
	if set.SignIn != nil {
		out.SignInUrl = base + "/id/login/org/" + url.PathEscape(o.Handle())
	}
	return out, nil
}

func (s *Server) SetIdentityProvider(ctx context.Context, req *identitypb.SetIdentityProviderRequest) (*identitypb.IdentityProviderInfo, error) {
	if err := s.enterpriseOn(); err != nil {
		return nil, err
	}
	o, p, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "set its identity provider")
	if err != nil {
		return nil, err
	}
	if o.ID == s.cfg.Installation {
		return nil, status.Error(codes.FailedPrecondition, "the installation's identity provider is its configuration's (identity.system_of_record)")
	}
	set, sec := settingsIn(req.GetSettings())
	if before, err := s.cfg.Store.IdentityProviderOf(ctx, o.ID); err == nil {
		_, old, err := enterprise.Open(s.cfg.Enterprise.Sealer, before)
		if err != nil {
			return nil, grpcError(err)
		}
		sec = keep(sec, old)
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, grpcError(err)
	}
	if err := set.Check(sec); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	source, _ := set.Source()
	raw, err := json.Marshal(set)
	if err != nil {
		return nil, grpcError(err)
	}
	sealed, err := enterprise.Seal(s.cfg.Enterprise.Sealer, o.ID, sec)
	if err != nil {
		return nil, grpcError(err)
	}
	interval := time.Duration(req.GetSettings().GetIntervalSecs()) * time.Second
	if err := s.cfg.Store.SetIdentityProvider(ctx, store.IdentityProvider{Organization: o.ID, Source: source, Settings: raw, Sealed: sealed,
		Interval: interval}); err != nil {
		return nil, tenancyError(err)
	}
	s.audit(ctx, p.Name, "identity_provider.set", map[string]any{"organization": o.Handle(), "source": source, "sign_in": set.SignIn != nil})
	return s.providerInfo(ctx, o)
}

func (s *Server) GetIdentityProvider(ctx context.Context, req *identitypb.IdentityProviderRequest) (*identitypb.IdentityProviderInfo, error) {
	o, _, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "read its identity provider")
	if err != nil {
		return nil, err
	}
	return s.providerInfo(ctx, o)
}

func (s *Server) SyncIdentityProvider(ctx context.Context, req *identitypb.IdentityProviderRequest) (*identitypb.IdentityProviderInfo, error) {
	if err := s.enterpriseOn(); err != nil {
		return nil, err
	}
	o, p, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "sync its identity provider")
	if err != nil {
		return nil, err
	}
	if _, err := s.cfg.Store.IdentityProviderOf(ctx, o.ID); errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "%s has no identity provider", o.Handle())
	}
	_, syncErr := s.cfg.Enterprise.Sync(ctx, o.ID)
	s.audit(ctx, p.Name, "identity_provider.sync", map[string]any{"organization": o.Handle(), "ok": syncErr == nil})
	// How it went is in what the provider answers: its last run and error.
	return s.providerInfo(ctx, o)
}

func (s *Server) RemoveIdentityProvider(ctx context.Context, req *identitypb.IdentityProviderRequest) (*identitypb.RemoveIdentityProviderResponse, error) {
	o, p, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "remove its identity provider")
	if err != nil {
		return nil, err
	}
	removed, err := s.cfg.Store.RemoveIdentityProvider(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	if removed {
		s.audit(ctx, p.Name, "identity_provider.remove", map[string]any{"organization": o.Handle()})
	}
	return &identitypb.RemoveIdentityProviderResponse{Removed: removed}, nil
}

func (s *Server) ResetSCIMToken(ctx context.Context, req *identitypb.IdentityProviderRequest) (*identitypb.ResetSCIMTokenResponse, error) {
	o, p, err := s.ownedOrganization(ctx, req.GetHandle(), req.GetForPersonId(), "reset its SCIM token")
	if err != nil {
		return nil, err
	}
	info, err := s.providerInfo(ctx, o)
	if err != nil {
		return nil, err
	}
	if info.ScimUrl == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "%s's identity provider is not SCIM", o.Handle())
	}
	tok, err := s.cfg.Store.SetSCIMToken(ctx, o.ID)
	if err != nil {
		return nil, grpcError(err)
	}
	s.audit(ctx, p.Name, "identity_provider.scim_token", map[string]any{"organization": o.Handle()})
	return &identitypb.ResetSCIMTokenResponse{Token: tok, ScimUrl: info.ScimUrl}, nil
}
