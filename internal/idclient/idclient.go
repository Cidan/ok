// Package idclient is how ok's roles other than identity use it: their callers resolved through
// its Verify and Directory services (Directory, an authn.Directory), credentials checked against
// the CA certificates it publishes (Roots), and their own credential — a key they hold, certified
// with their issuer's token and renewed before it expires (Workload).
package idclient

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/client/auth"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Callers are each role's caller, as services declare them, by the role's name in the
// configuration.
var Callers = map[string]okpb.Caller{
	"identity": okpb.Caller_CALLER_IDENTITY, "scm": okpb.Caller_CALLER_SCM, "control": okpb.Caller_CALLER_CONTROL,
	"automation": okpb.Caller_CALLER_AUTOMATION, "worker": okpb.Caller_CALLER_WORKER, "deploy": okpb.Caller_CALLER_DEPLOY,
	"edge": okpb.Caller_CALLER_EDGE, "secrets": okpb.Caller_CALLER_SECRETS,
}

// Roles are the robots the installation's roles run as, for an authn.Resolver.
func Roles(cfg *config.Config) authn.Roles {
	roles := authn.Roles{}
	for role, short := range cfg.Roles.All() {
		roles[Callers[role]] = cfg.RobotName(short)
	}
	return roles
}

// Options are the dial options that reach a role: TLS as the installation's services say, or
// cleartext on the network they trust.
func Options(s config.Services) (dial.Options, error) {
	if !s.TLS {
		return dial.Options{Insecure: true}, nil
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if s.CAFile != "" {
		pem, err := os.ReadFile(s.CAFile)
		if err != nil {
			return dial.Options{}, fmt.Errorf("services.ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return dial.Options{}, fmt.Errorf("services.ca_file: %s holds no certificate", s.CAFile)
		}
		tc.RootCAs = pool
	}
	return dial.Options{TLS: tc}, nil
}

// Directory resolves a role's callers through identity: an authn.Directory.
type Directory struct {
	verify identitypb.VerifyClient
	dir    identitypb.DirectoryClient
}

// NewDirectory asks identity over conn, which calls as the role.
func NewDirectory(conn grpc.ClientConnInterface) Directory {
	return Directory{verify: identitypb.NewVerifyClient(conn), dir: identitypb.NewDirectoryClient(conn)}
}

var _ authn.Directory = Directory{}

func toPrincipal(p *identitypb.Principal, active bool) authn.Principal {
	return authn.Principal{Name: p.GetName(), ID: p.GetId(), Human: p.GetKind() == identitypb.Kind_KIND_HUMAN, Org: p.GetKind() == identitypb.Kind_KIND_ORG,
		Active: active}
}

func notFound(err error) error {
	if status.Code(err) == codes.NotFound {
		return authn.ErrNotFound
	}
	return err
}

func (d Directory) Principal(ctx context.Context, name string) (authn.Principal, error) {
	resp, err := d.dir.GetPrincipal(ctx, &identitypb.GetPrincipalRequest{Key: &identitypb.GetPrincipalRequest_Name{Name: name}})
	if err != nil {
		return authn.Principal{}, notFound(err)
	}
	return toPrincipal(resp.GetPrincipal(), resp.GetPrincipal().GetStatus() == identitypb.Status_STATUS_ACTIVE), nil
}

func (d Directory) PrincipalByID(ctx context.Context, id string) (authn.Principal, error) {
	resp, err := d.dir.GetPrincipal(ctx, &identitypb.GetPrincipalRequest{Key: &identitypb.GetPrincipalRequest_Id{Id: id}})
	if err != nil {
		return authn.Principal{}, notFound(err)
	}
	return toPrincipal(resp.GetPrincipal(), resp.GetPrincipal().GetStatus() == identitypb.Status_STATUS_ACTIVE), nil
}

func (d Directory) Credential(ctx context.Context, id string) (authn.Principal, error) {
	resp, err := d.verify.GetCredential(ctx, &identitypb.GetCredentialRequest{Id: id})
	if err != nil {
		return authn.Principal{}, notFound(err)
	}
	return toPrincipal(resp.GetPrincipal(), resp.GetActive()), nil
}

func (d Directory) Session(ctx context.Context, id string) (authn.Principal, error) {
	resp, err := d.verify.GetSession(ctx, &identitypb.GetSessionRequest{Id: id})
	if err != nil {
		return authn.Principal{}, notFound(err)
	}
	return toPrincipal(resp.GetPrincipal(), true), nil
}

// Token returns whose a live token is — a person's personal token, an app's installation token —
// and what it allows.
func (d Directory) Token(ctx context.Context, secret string) (authn.Principal, error) {
	resp, err := d.verify.GetToken(ctx, &identitypb.GetTokenRequest{Token: secret})
	if err != nil {
		return authn.Principal{}, notFound(err)
	}
	r, err := tenancy.ParseRole(resp.GetRole())
	if err != nil {
		return authn.Principal{}, err
	}
	p := toPrincipal(resp.GetPrincipal(), true)
	p.Scope = &authz.Token{Role: r, Repositories: resp.GetRepositoryIds(), Owner: resp.GetOwnerId()}
	return p, nil
}

// rootsEvery is how often Roots reads the CA's certificates again: a rotation's new certificate is
// trusted within it.
const rootsEvery = 5 * time.Minute

// Roots are the identity CA's certificates, as identity publishes them: read at start, and again
// every few minutes while the role runs (Start). A read that fails keeps the last ones. A role
// that cannot reach identity as it starts — identity in the same process, not serving yet — starts
// without them, verifying no credential, and reads them every few seconds until identity answers.
type Roots struct {
	ca    identitypb.CAClient
	local func() *x509.CertPool // identity's own, in the same process
	owner string

	mu   sync.Mutex
	pool *x509.CertPool
}

var _ node.Application = (*Roots)(nil)

// NewRoots reads the CA's certificates from identity over conn, for the role named owner (scm,
// control, …), whose node it is.
func NewRoots(ctx context.Context, conn grpc.ClientConnInterface, owner string) (*Roots, error) {
	r := &Roots{ca: identitypb.NewCAClient(conn), owner: owner}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.read(readCtx); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		slog.Info("identity client: identity's CA certificates are not readable yet; reading them again until they are", "role", owner, "err", err)
	}
	return r, nil
}

// LocalRoots are the CA's certificates of identity in the same process (ok serve), for the role
// named owner: read from identity directly, as they are now.
func LocalRoots(ca func() *x509.CertPool, owner string) *Roots {
	return &Roots{local: ca, owner: owner}
}

// rootsRetry is how often Roots tries again while it has no certificates.
const rootsRetry = 2 * time.Second

func (r *Roots) read(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := r.ca.GetRoots(ctx, &identitypb.GetRootsRequest{})
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	for _, der := range resp.GetCertificates() {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return err
		}
		pool.AddCert(cert)
	}
	if len(resp.GetCertificates()) == 0 {
		return errors.New("identity published no CA certificate")
	}
	r.mu.Lock()
	r.pool = pool
	r.mu.Unlock()
	return nil
}

// Pool is the certificates now, for a credential.Verifier: empty until they are read.
func (r *Roots) Pool() *x509.CertPool {
	if r.local != nil {
		return r.local()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pool == nil {
		return x509.NewCertPool()
	}
	return r.pool
}

func (r *Roots) has() bool {
	if r.local != nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pool != nil
}

func (r *Roots) Name() string                 { return "ok/" + r.owner + "/identity-roots" }
func (r *Roots) Dependencies() []node.Node    { return nil }
func (r *Roots) Setup(context.Context) error  { return nil }
func (r *Roots) Healthy(context.Context) bool { return true }

// Start reads the certificates again every few minutes until ctx ends — every few seconds while it
// has none.
func (r *Roots) Start(ctx context.Context) error {
	if r.local != nil {
		<-ctx.Done()
		return nil
	}
	for {
		wait := rootsEvery
		if !r.has() {
			wait = rootsRetry
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
			if err := r.read(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("identity client: reading the CA's certificates", "role", r.owner, "err", err)
			}
		}
	}
}

// renewWithin is how near expiry a workload credential is renewed.
const renewWithin = 6 * time.Hour

// Workload is a role's credential, certified with its issuer's token: a dial.Credential. It makes
// a key when it first certifies, renews the certificate before it expires, and certifies a new key
// with a fresh token when the credential has lapsed or was revoked.
type Workload struct {
	certify identitypb.CredentialsClient // calls with the token
	base    dial.Options
	addr    string
	label   string
	now     func() time.Time

	mu   sync.Mutex
	key  *ecdsa.PrivateKey
	cert *x509.Certificate
}

// NewWorkload is the credential of a role whose token is in tokenFile (re-read for each
// certification: a projected token is renewed in place), reaching identity at addr with base.
func NewWorkload(addr string, base dial.Options, tokenFile, label string) (*Workload, error) {
	o := base
	o.Token = func(context.Context) (string, error) {
		b, err := os.ReadFile(tokenFile)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}
	conn, err := dial.Dial(addr, o)
	if err != nil {
		return nil, err
	}
	return &Workload{certify: identitypb.NewCredentialsClient(conn), base: base, addr: addr, label: label, now: time.Now}, nil
}

// Principal is who the credential is, once it has certified.
func (w *Workload) Principal() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cert == nil {
		return ""
	}
	p, _ := credential.Principal(w.cert)
	return p
}

// Current is the certificate and key to call with.
func (w *Workload) Current(ctx context.Context) ([]byte, crypto.Signer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	switch {
	case w.cert == nil || !now.Before(w.cert.NotAfter.Add(-time.Minute)):
		if err := w.certifyNew(ctx); err != nil {
			return nil, nil, err
		}
	case w.cert.NotAfter.Sub(now) < renewWithin:
		if err := w.renew(ctx); err != nil {
			if status.Code(err) == codes.PermissionDenied || status.Code(err) == codes.Unauthenticated {
				// Revoked: certify a new key.
				if err := w.certifyNew(ctx); err != nil {
					return nil, nil, err
				}
			} else {
				slog.Warn("identity client: renewing the workload credential; it is still good", "err", err, "until", w.cert.NotAfter)
			}
		}
	}
	return w.cert.Raw, w.key, nil
}

// certifyNew makes a key and certifies it with the token; w.mu is held.
func (w *Workload) certifyNew(ctx context.Context) error {
	key, err := credential.NewKey()
	if err != nil {
		return err
	}
	spki, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return err
	}
	resp, err := w.certify.Certify(ctx, &identitypb.CertifyRequest{PublicKey: spki, Label: w.label})
	if err != nil {
		return fmt.Errorf("certifying the workload's key with its token: %w", err)
	}
	cert, err := x509.ParseCertificate(resp.GetCertificate().GetChain()[0])
	if err != nil {
		return err
	}
	w.key, w.cert = key, cert
	return nil
}

// renew renews the certificate with itself; w.mu is held.
func (w *Workload) renew(ctx context.Context) error {
	o := w.base
	o.Credential = dial.Static{Cert: w.cert.Raw, Key: w.key}
	conn, err := dial.Dial(w.addr, o)
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := identitypb.NewCredentialsClient(conn).Renew(ctx, &identitypb.RenewRequest{})
	if err != nil {
		return err
	}
	cert, err := x509.ParseCertificate(resp.GetCertificate().GetChain()[0])
	if err != nil {
		return err
	}
	w.cert = cert
	return nil
}

// Credential is a role's credential as its configuration names it: certified with a token, or an
// administrator's in a directory.
func Credential(cfg *config.Config, base dial.Options) (dial.Credential, error) {
	switch {
	case cfg.Workload.TokenFile != "":
		if cfg.Services.Identity == "" {
			return nil, errors.New("services.identity is where the role certifies its key")
		}
		return NewWorkload(cfg.Services.Identity, base, cfg.Workload.TokenFile, label())
	case cfg.Workload.CredentialDir != "":
		return auth.Load(cfg.Workload.CredentialDir)
	}
	return nil, errors.New("workload names how the role proves itself: a token_file, or a credential_dir")
}

// label names the process: its pod's name, or its host's.
func label() string {
	if h := os.Getenv("HOSTNAME"); h != "" {
		return h
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "a role"
}

// LocalCertifier is identity in the same process (ok serve): it certifies a role's key without a
// token.
type LocalCertifier interface {
	CertifyLocal(ctx context.Context, robot string, publicKey []byte, label string) (*identitypb.Certificate, error)
	ReissueLocal(ctx context.Context, credentialID string) (*identitypb.Certificate, error)
}

// Local is a role's credential certified by identity in the same process: a dial.Credential. It
// makes a key when first used, and has its certificate reissued before it expires.
type Local struct {
	identity LocalCertifier
	robot    string
	label    string
	now      func() time.Time

	mu   sync.Mutex
	key  *ecdsa.PrivateKey
	cert *x509.Certificate
	id   string
}

// NewLocal is the credential of the robot robot (scm.ok), certified by identity in this process.
func NewLocal(identity LocalCertifier, robot string) *Local {
	return &Local{identity: identity, robot: robot, label: label(), now: time.Now}
}

// Current is the certificate and key to call with.
func (l *Local) Current(ctx context.Context) ([]byte, crypto.Signer, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	switch {
	case l.cert == nil || !now.Before(l.cert.NotAfter.Add(-time.Minute)):
		key, err := credential.NewKey()
		if err != nil {
			return nil, nil, err
		}
		spki, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
		if err != nil {
			return nil, nil, err
		}
		c, err := l.identity.CertifyLocal(ctx, l.robot, spki, l.label)
		if err != nil {
			return nil, nil, fmt.Errorf("certifying %s's key: %w", l.robot, err)
		}
		if err := l.take(c); err != nil {
			return nil, nil, err
		}
		l.key = key
	case l.cert.NotAfter.Sub(now) < renewWithin:
		c, err := l.identity.ReissueLocal(ctx, l.id)
		if err == nil {
			err = l.take(c)
		}
		if err != nil {
			slog.Warn("identity client: reissuing a local credential; it is still good", "robot", l.robot, "err", err, "until", l.cert.NotAfter)
		}
	}
	return l.cert.Raw, l.key, nil
}

// take holds a certificate identity issued; l.mu is held.
func (l *Local) take(c *identitypb.Certificate) error {
	cert, err := x509.ParseCertificate(c.GetChain()[0])
	if err != nil {
		return err
	}
	l.cert, l.id = cert, c.GetCredential()
	return nil
}
