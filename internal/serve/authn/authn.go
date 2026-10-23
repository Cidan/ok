// Package authn resolves who each request to ok's services is for, and holds every request to the
// callers its service declares (ok.v1.callers). A caller is resolved, in order, from:
//
//   - a development principal (ok-dev-principal), only on a server set up for development, which
//     listens on loopback alone;
//   - a header an authenticating proxy in front of ok writes, when the installation names one;
//   - a device credential: the certificate ok's identity CA issued and a proof signed by its key
//     (package credential);
//   - a bearer token from an identity provider the installation federates with: a Kubernetes
//     ServiceAccount's, or a CI system's OIDC token;
//   - a browser session's cookie.
//
// Each is checked against the directory — the principal active, the credential not revoked, the
// session alive — and the answers are cached briefly. A request no method attributes has no caller,
// and only a service open to anyone takes it. The directory not answering fails the request.
package authn

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/credential"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// The metadata keys a request names its caller with, besides the credential's.
const (
	DevPrincipalKey  = "ok-dev-principal"
	authorizationKey = "authorization"
	cookieKey        = "cookie"
)

// SessionCookie is the cookie a browser session is kept in.
const SessionCookie = "ok_session"

// How a caller was resolved.
const (
	ViaDev        = "dev"
	ViaHeader     = "header"
	ViaCredential = "credential"
	ViaToken      = "token"
	// ViaPersonalToken is a person's personal token (okp_…), which narrows them (Caller.Token).
	ViaPersonalToken = "personal-token"
	// ViaInstallationToken is an app's installation token (oki_…): the app's robot, narrowed to
	// its installation (Caller.Token).
	ViaInstallationToken = "installation-token"
	ViaSession           = "session"
)

// Caller is the principal a request is for.
type Caller struct {
	// Principal is its name: ada.human.example.com.
	Principal string
	// ID is the principal's stable ID, as the directory has it.
	ID string
	// Human is true for a person, false for a robot.
	Human bool
	// Via is how the caller was resolved.
	Via string
	// Credential is the device credential's ID, for a caller resolved from one.
	Credential string
	// Token is what the personal token a caller presented allows, which narrows them (Authorize);
	// nil for none.
	Token *authz.Token
	// Role is which of ok's roles the caller is (Config.Roles); CALLER_UNSPECIFIED for anyone else —
	// a person, or a robot of the installation's or an organization's, whom Authorize decides.
	Role okpb.Caller
}

// IsRole reports whether the caller is one of ok's roles, which act for a person they checked.
func (c Caller) IsRole() bool { return c.Role != okpb.Caller_CALLER_UNSPECIFIED }

type callerKey struct{}

// NewContext returns ctx carrying c.
func NewContext(ctx context.Context, c Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, c)
}

// FromContext returns the caller resolved for ctx's request, if any.
func FromContext(ctx context.Context) (Caller, bool) {
	c, ok := ctx.Value(callerKey{}).(Caller)
	return c, ok
}

// Principal is what the directory says of a principal.
type Principal struct {
	Name  string
	ID    string
	Human bool
	// Org: an organization, which owns repositories and acts as no one — never a caller.
	Org    bool
	Active bool
	// Scope is what a personal token allows, for a principal resolved from one.
	Scope *authz.Token
}

// PersonalTokenPrefix starts every personal token, and InstallationTokenPrefix every app's
// installation token.
const (
	PersonalTokenPrefix     = "okp_"
	InstallationTokenPrefix = "oki_"
)

// ErrNotFound is the directory's answer for what it does not hold.
var ErrNotFound = errors.New("not found")

// Directory says whether principals, credentials, and sessions stand.
type Directory interface {
	// Principal returns the principal with a name.
	Principal(ctx context.Context, name string) (Principal, error)
	// PrincipalByID returns the principal with a stable ID, under its current name; Active is
	// false for one suspended or deleted.
	PrincipalByID(ctx context.Context, id string) (Principal, error)
	// Credential returns the principal a device credential (by ID) belongs to; Active is false
	// when either the principal or the credential no longer stands.
	Credential(ctx context.Context, id string) (Principal, error)
	// Session returns the principal a browser session belongs to, while it is alive.
	Session(ctx context.Context, id string) (Principal, error)
	// Token returns the person a live personal token is theirs, its Scope what it allows.
	Token(ctx context.Context, secret string) (Principal, error)
}

// TokenVerifier verifies a federated bearer token and names the principal it is for.
type TokenVerifier interface {
	Verify(ctx context.Context, token string) (principal string, err error)
}

// Roles are the robots ok's roles run as.
type Roles map[okpb.Caller]string

// Config is how a server resolves callers.
type Config struct {
	Directory Directory
	// Credentials verifies device credentials; nil accepts none.
	Credentials *credential.Verifier
	// Tokens verifies federated bearer tokens; nil accepts none.
	Tokens TokenVerifier
	// TrustedHeader, when set, is a header an authenticating proxy in front of ok writes with the
	// principal's name (and strips from every request it forwards).
	TrustedHeader string
	// Dev accepts a development principal: tests and local development, on loopback only.
	Dev bool
	// Roles are the robots ok's roles run as.
	Roles Roles
}

// Directory answers are cached: a principal or credential that stops standing stops resolving
// within PositiveTTL, and one that starts within NegativeTTL.
const (
	PositiveTTL   = 30 * time.Second
	NegativeTTL   = 5 * time.Second
	LookupTimeout = 5 * time.Second
)

type entry struct {
	p       Principal
	ok      bool
	expires time.Time
}

// Resolver resolves callers and enforces services' callers.
type Resolver struct {
	cfg   Config
	now   func() time.Time
	group singleflight.Group

	mu    sync.Mutex
	cache map[string]entry
}

// NewResolver returns a resolver over cfg.
func NewResolver(cfg Config) *Resolver {
	return &Resolver{cfg: cfg, now: time.Now, cache: map[string]entry{}}
}

// lookup asks the directory, through the cache, under key.
func (r *Resolver) lookup(ctx context.Context, key string, ask func(context.Context) (Principal, error)) (Principal, bool, error) {
	r.mu.Lock()
	e, hit := r.cache[key]
	r.mu.Unlock()
	if hit && r.now().Before(e.expires) {
		return e.p, e.ok, nil
	}
	ch := r.group.DoChan(key, func() (any, error) {
		lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), LookupTimeout)
		defer cancel()
		p, err := ask(lctx)
		var e entry
		switch {
		case errors.Is(err, ErrNotFound) || status.Code(err) == codes.NotFound:
			e = entry{expires: r.now().Add(NegativeTTL)}
		case err != nil:
			return nil, err
		case p.Active && !p.Org:
			e = entry{p: p, ok: true, expires: r.now().Add(PositiveTTL)}
		default:
			e = entry{expires: r.now().Add(NegativeTTL)}
		}
		r.mu.Lock()
		r.cache[key] = e
		r.mu.Unlock()
		return e, nil
	})
	select {
	case <-ctx.Done():
		return Principal{}, false, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return Principal{}, false, res.Err
		}
		e := res.Val.(entry)
		return e.p, e.ok, nil
	}
}

// ResolveName returns the active principal with a name, through the cache.
func (r *Resolver) ResolveName(ctx context.Context, name string) (Principal, bool, error) {
	return r.lookup(ctx, "name:"+name, func(ctx context.Context) (Principal, error) {
		p, err := r.cfg.Directory.Principal(ctx, name)
		if err == nil && p.Name != name {
			return Principal{}, ErrNotFound
		}
		return p, err
	})
}

// errUnauthenticated is a credential the request presents that does not hold: a request that
// tried to say who it is and failed is refused, never served as no one.
type errUnauthenticated struct{ why string }

func (e errUnauthenticated) Error() string { return e.why }

// Resolve returns the caller md names for a request to method. ok is false when md names none;
// err is a status error when a credential it presents does not hold, or the directory cannot say.
func (r *Resolver) Resolve(ctx context.Context, md metadata.MD, method string) (Caller, bool, error) {
	c, ok, err := r.resolve(ctx, md, method)
	var u errUnauthenticated
	switch {
	case errors.As(err, &u):
		return Caller{}, false, status.Error(codes.Unauthenticated, u.why)
	case err != nil && ctx.Err() != nil:
		return Caller{}, false, status.FromContextError(ctx.Err()).Err()
	case err != nil:
		return Caller{}, false, status.Error(codes.Unavailable, "resolving the caller: "+err.Error())
	}
	if ok && !c.Human && c.Token == nil {
		for k, name := range r.cfg.Roles {
			if name != "" && name == c.Principal {
				c.Role = k
			}
		}
	}
	return c, ok, nil
}

func (r *Resolver) resolve(ctx context.Context, md metadata.MD, method string) (Caller, bool, error) {
	if name := single(md, DevPrincipalKey); name != "" {
		if !r.cfg.Dev {
			return Caller{}, false, errUnauthenticated{"a development principal is accepted only by a development server"}
		}
		return r.byName(ctx, name, ViaDev)
	}
	if h := r.cfg.TrustedHeader; h != "" {
		if name := single(md, strings.ToLower(h)); name != "" {
			return r.byName(ctx, name, ViaHeader)
		}
	}
	if certs := md.Get(credential.CertificateKey); len(certs) > 0 {
		if r.cfg.Credentials == nil || len(certs) != 1 {
			return Caller{}, false, errUnauthenticated{"this server takes no device credential"}
		}
		v, err := r.cfg.Credentials.Verify(method, []byte(certs[0]), single(md, credential.ProofKey))
		if err != nil {
			return Caller{}, false, errUnauthenticated{err.Error()}
		}
		p, ok, err := r.lookup(ctx, "credential:"+v.Credential, func(ctx context.Context) (Principal, error) {
			return r.cfg.Directory.Credential(ctx, v.Credential)
		})
		if err != nil {
			return Caller{}, false, err
		}
		if ok && p.Name != v.Principal {
			// A certificate issued under a name an installation's move gave up names the principal
			// still: the old name is an alias of theirs, and a retired name is never anyone else's.
			q, found, err := r.lookup(ctx, "alias:"+v.Principal, func(ctx context.Context) (Principal, error) {
				return r.cfg.Directory.Principal(ctx, v.Principal)
			})
			if err != nil {
				return Caller{}, false, err
			}
			ok = found && q.ID == p.ID
		}
		if !ok {
			return Caller{}, false, errUnauthenticated{"the device credential is revoked, or its principal is not active"}
		}
		return Caller{Principal: p.Name, ID: p.ID, Human: p.Human, Via: ViaCredential, Credential: v.Credential}, true, nil
	}
	if auth := single(md, authorizationKey); auth != "" {
		token, ok := strings.CutPrefix(auth, "Bearer ")
		token = strings.TrimSpace(token)
		personal, installation := strings.HasPrefix(token, PersonalTokenPrefix), strings.HasPrefix(token, InstallationTokenPrefix)
		if ok && (personal || installation) {
			// Cached by the secret's hash: no secret is kept as a key.
			h := sha256.Sum256([]byte(token))
			p, found, err := r.lookup(ctx, "token:"+hex.EncodeToString(h[:]), func(ctx context.Context) (Principal, error) {
				return r.cfg.Directory.Token(ctx, token)
			})
			if err != nil {
				return Caller{}, false, err
			}
			switch {
			case personal && found && p.Human && p.Scope != nil:
				return Caller{Principal: p.Name, ID: p.ID, Human: true, Via: ViaPersonalToken, Token: p.Scope}, true, nil
			case installation && found && !p.Human && !p.Org && p.Scope != nil && p.Scope.Owner != "":
				return Caller{Principal: p.Name, ID: p.ID, Via: ViaInstallationToken, Token: p.Scope}, true, nil
			}
			return Caller{}, false, errUnauthenticated{"the token is revoked, expired, or its holder no longer stands"}
		}
		if !ok || r.cfg.Tokens == nil {
			return Caller{}, false, errUnauthenticated{"this server takes no such authorization"}
		}
		name, err := r.cfg.Tokens.Verify(ctx, strings.TrimSpace(token))
		if err != nil {
			return Caller{}, false, errUnauthenticated{"the bearer token: " + err.Error()}
		}
		return r.byName(ctx, name, ViaToken)
	}
	if id := sessionCookie(md.Get(cookieKey)); id != "" {
		p, ok, err := r.lookup(ctx, "session:"+id, func(ctx context.Context) (Principal, error) {
			return r.cfg.Directory.Session(ctx, id)
		})
		if err != nil {
			return Caller{}, false, err
		}
		if !ok {
			return Caller{}, false, nil // an expired session is no one: the browser signs in again
		}
		return Caller{Principal: p.Name, ID: p.ID, Human: p.Human, Via: ViaSession}, true, nil
	}
	return Caller{}, false, nil
}

// byName resolves a caller something trusted named.
func (r *Resolver) byName(ctx context.Context, name, via string) (Caller, bool, error) {
	p, ok, err := r.ResolveName(ctx, name)
	if err != nil {
		return Caller{}, false, err
	}
	if !ok {
		return Caller{}, false, errUnauthenticated{fmt.Sprintf("%s is not an active principal", name)}
	}
	return Caller{Principal: p.Name, ID: p.ID, Human: p.Human, Via: via}, true, nil
}

// sessionCookie finds the session cookie among Cookie headers.
func sessionCookie(headers []string) string {
	for _, h := range headers {
		cs, err := http.ParseCookie(h)
		if err != nil {
			continue
		}
		for _, c := range cs {
			if c.Name == SessionCookie && c.Value != "" {
				return c.Value
			}
		}
	}
	return ""
}

// Allowed reports whether a caller (ok false: none) may call a service that admits callers.
func (r *Resolver) Allowed(callers []okpb.Caller, c Caller, ok bool) bool {
	for _, k := range callers {
		switch {
		case k == okpb.Caller_CALLER_ANYONE:
			return true
		case !ok:
		case k == okpb.Caller_CALLER_PEOPLE && c.Human, k == okpb.Caller_CALLER_ROBOTS && !c.Human:
			return true
		case r.cfg.Roles[k] != "" && r.cfg.Roles[k] == c.Principal:
			return true
		}
	}
	return false
}

// open are services outside ok's protocols that every server serves to anyone.
var open = map[string]bool{
	"grpc.health.v1.Health":                    true,
	"grpc.reflection.v1.ServerReflection":      true,
	"grpc.reflection.v1alpha.ServerReflection": true,
}

// foreign are services outside ok's protocols that ok serves to some callers: Bazel's remote cache,
// which ok automation serves to workers (docs/hosted.md, "Hosted automation, as built").
var foreign = map[string][]okpb.Caller{
	"build.bazel.remote.execution.v2.ContentAddressableStorage": {okpb.Caller_CALLER_WORKER},
	"build.bazel.remote.execution.v2.ActionCache":               {okpb.Caller_CALLER_WORKER},
	"build.bazel.remote.execution.v2.Capabilities":              {okpb.Caller_CALLER_WORKER},
	"google.bytestream.ByteStream":                              {okpb.Caller_CALLER_WORKER},
}

var callersCache sync.Map // service full name -> []okpb.Caller

// Callers are the callers a service (by its full name) declares; a service declaring none admits
// no one.
func Callers(service string) []okpb.Caller {
	if v, ok := callersCache.Load(service); ok {
		return v.([]okpb.Caller)
	}
	var out []okpb.Caller
	if open[service] {
		out = []okpb.Caller{okpb.Caller_CALLER_ANYONE}
	} else if c, ok := foreign[service]; ok {
		out = c
	} else if d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(service)); err == nil {
		if sd, ok := d.(protoreflect.ServiceDescriptor); ok {
			if opts, ok := sd.Options().(*descriptorpb.ServiceOptions); ok && opts != nil {
				out = proto.GetExtension(opts, okpb.E_Callers).([]okpb.Caller)
			}
		}
	}
	callersCache.Store(service, out)
	return out
}

// serviceOf is the service of a full method: /ok.scm.v1.Store/Read is ok.scm.v1.Store.
func serviceOf(method string) string {
	s := strings.TrimPrefix(method, "/")
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		return s[:i]
	}
	return s
}

// admit resolves a request's caller and holds it to its service's callers.
func (r *Resolver) admit(ctx context.Context, method string) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	c, ok, err := r.Resolve(ctx, md, method)
	if err != nil {
		return nil, err
	}
	callers := Callers(serviceOf(method))
	if !r.Allowed(callers, c, ok) {
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "%s needs a caller: sign in", serviceOf(method))
		}
		return nil, status.Errorf(codes.PermissionDenied, "%s may not call %s", c.Principal, serviceOf(method))
	}
	if ok {
		ctx = NewContext(ctx, c)
	}
	return ctx, nil
}

// UnaryInterceptor resolves each unary request's caller, admits it or refuses it, and attaches it
// to the request's context.
func (r *Resolver) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := r.admit(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor is UnaryInterceptor for streams.
func (r *Resolver) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := r.admit(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &stream{ServerStream: ss, ctx: ctx})
	}
}

type stream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *stream) Context() context.Context { return s.ctx }

// Middleware resolves the caller of a web request — its session, a development principal, or a
// trusted header — and attaches it to the request's context; the handlers decide what no caller
// may see.
func (r *Resolver) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		md := metadata.MD{}
		for k, vs := range req.Header {
			k = strings.ToLower(k)
			if strings.HasSuffix(k, "-bin") {
				for _, v := range vs {
					if b, err := base64.StdEncoding.DecodeString(v); err == nil {
						md.Append(k, string(b))
					}
				}
				continue
			}
			md.Append(k, vs...)
		}
		c, ok, err := r.Resolve(req.Context(), md, req.URL.Path)
		switch {
		case status.Code(err) == codes.Unauthenticated:
			http.Error(w, status.Convert(err).Message(), http.StatusUnauthorized)
			return
		case err != nil:
			http.Error(w, status.Convert(err).Message(), http.StatusServiceUnavailable)
			return
		case ok:
			req = req.WithContext(NewContext(req.Context(), c))
		}
		next.ServeHTTP(w, req)
	})
}

// single returns md's one value for key, or "" when it has none or several.
func single(md metadata.MD, key string) string {
	v := md.Get(key)
	if len(v) != 1 {
		return ""
	}
	return strings.TrimSpace(v[0])
}
