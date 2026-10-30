package authn

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/serve/inproc"
	_ "github.com/Cidan/ok/internal/serve/serving/echotest"
	"github.com/Cidan/ok/internal/tenancy"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// directory is a directory in memory.
type directory struct {
	principals  map[string]Principal
	credentials map[string]string // credential ID -> principal
	revoked     map[string]bool
	sessions    map[string]string // id -> principal
	pats        map[string]string // personal token -> principal
	aliases     map[string]string // a name a move gave up -> the name that took its place
	down        bool
	asked       int
}

func (d *directory) Principal(_ context.Context, name string) (Principal, error) {
	d.asked++
	if d.down {
		return Principal{}, errors.New("the directory is down")
	}
	p, ok := d.principals[name]
	if !ok {
		if to, aliased := d.aliases[name]; aliased {
			return d.principals[to], nil
		}
		return Principal{}, ErrNotFound
	}
	return p, nil
}

func (d *directory) PrincipalByID(_ context.Context, id string) (Principal, error) {
	d.asked++
	for _, p := range d.principals {
		if p.ID == id {
			return p, nil
		}
	}
	return Principal{}, ErrNotFound
}

func (d *directory) Credential(ctx context.Context, id string) (Principal, error) {
	name, ok := d.credentials[id]
	if !ok {
		return Principal{}, ErrNotFound
	}
	p, err := d.Principal(ctx, name)
	p.Active = p.Active && !d.revoked[id]
	return p, err
}

func (d *directory) Session(ctx context.Context, id string) (Principal, error) {
	name, ok := d.sessions[id]
	if !ok {
		return Principal{}, ErrNotFound
	}
	return d.Principal(ctx, name)
}

func (d *directory) Token(ctx context.Context, secret string) (Principal, error) {
	name, ok := d.pats[secret]
	if !ok {
		return Principal{}, ErrNotFound
	}
	p, err := d.Principal(ctx, name)
	if err == nil {
		p.Scope = &authz.Token{Role: tenancy.Read, Repositories: []int64{7}}
	}
	return p, err
}

type tokens map[string]string

func (tk tokens) Verify(_ context.Context, token string) (string, error) {
	if name, ok := tk[token]; ok {
		return name, nil
	}
	return "", errors.New("not a token anyone issued")
}

// ca issues device credentials.
type ca struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newCA(t *testing.T) *ca {
	t.Helper()
	key, _ := credential.NewKey()
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "CA"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &ca{cert, key}
}

func (c *ca) pool(context.Context) (*x509.CertPool, error) {
	p := x509.NewCertPool()
	p.AddCert(c.cert)
	return p, nil
}

func (c *ca) issue(t *testing.T, principal string, serial int64) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()
	key, _ := credential.NewKey()
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs: []*url.URL{{Scheme: "ok", Opaque: "principal:" + principal}, {Scheme: "ok", Opaque: fmt.Sprintf("credential:%x", serial)}}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	return der, key
}

const echoMethod = "/ok.echo.v1.Echo/Say"

func setup(t *testing.T, dev bool) (*Resolver, *directory, *ca) {
	t.Helper()
	d := &directory{
		principals: map[string]Principal{
			"ada.human.example.com":        {Name: "ada.human.example.com", ID: "p1", Human: true, Active: true},
			"gone.human.example.com":       {Name: "gone.human.example.com", ID: "p2", Human: true},
			"control.ok.robot.example.com": {Name: "control.ok.robot.example.com", ID: "p3", Active: true},
			"ci.ci.robot.example.com":      {Name: "ci.ci.robot.example.com", ID: "p4", Active: true},
			"acme.org.example.com":         {Name: "acme.org.example.com", ID: "p5", Org: true, Active: true},
			"ci.acme.robot.example.com":    {Name: "ci.acme.robot.example.com", ID: "p6", Active: true},
			"bea.human.example.com":        {Name: "bea.human.example.com", ID: "p7", Human: true, Active: true},
		},
		credentials: map[string]string{"10": "ada.human.example.com", "11": "ada.human.example.com", "12": "gone.human.example.com",
			"13": "ci.acme.robot.example.com", "14": "bea.human.example.com"},
		aliases:  map[string]string{"ci.robot.example.com": "ci.acme.robot.example.com"},
		revoked:  map[string]bool{"11": true},
		sessions: map[string]string{"s1": "ada.human.example.com", "s2": "gone.human.example.com"},
		pats:     map[string]string{"okp_ada": "ada.human.example.com", "okp_gone": "gone.human.example.com"},
	}
	c := newCA(t)
	r := NewResolver(Config{
		Directory:     d,
		Credentials:   credential.NewVerifier(c.pool),
		Tokens:        tokens{"good": "ci.ci.robot.example.com"},
		TrustedHeader: "X-Auth-Principal",
		Dev:           dev,
		Roles:         Roles{okpb.Caller_CALLER_CONTROL: "control.ok.robot.example.com"},
		InProcess:     map[string]Principal{"control.ok.robot.example.com": {Name: "control.ok.robot.example.com", ID: "p3", Active: true}},
	})
	return r, d, c
}

// A call from within the process is the role it names, with no question of the directory; the
// same name from any other peer — another machine, the REST gateway's in-memory listener, no
// peer at all — is refused, never served as no one, and so is a name that is no role's.
func TestInProcess(t *testing.T) {
	r, d, _ := setup(t, false)
	from := func(a net.Addr) context.Context {
		return peer.NewContext(context.Background(), &peer.Peer{Addr: a})
	}
	md := metadata.Pairs(InProcessKey, "control.ok.robot.example.com")
	asked := d.asked
	got, ok, err := r.Resolve(from(inproc.Addr{}), md, echoMethod)
	if err != nil || !ok || got.Principal != "control.ok.robot.example.com" || got.ID != "p3" || got.Role != okpb.Caller_CALLER_CONTROL ||
		got.Human || got.Via != ViaInProcess || d.asked != asked {
		t.Errorf("a role within the process: %+v, %v, %v; the directory asked %d times", got, ok, err, d.asked-asked)
	}
	lis := bufconn.Listen(1 << 10)
	defer lis.Close()
	for name, ctx := range map[string]context.Context{
		"another machine":            from(&net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 4000}),
		"another in-memory listener": from(lis.Addr()),
		"no peer":                    context.Background(),
	} {
		if _, ok, err := r.Resolve(ctx, md, echoMethod); status.Code(err) != codes.Unauthenticated || ok {
			t.Errorf("%s naming a role: %v, %v", name, ok, err)
		}
	}
	if _, ok, err := r.Resolve(from(inproc.Addr{}), metadata.Pairs(InProcessKey, "ada.human.example.com"), echoMethod); status.Code(err) != codes.Unauthenticated || ok {
		t.Errorf("within the process, naming someone no role is: %v, %v", ok, err)
	}
}

func withCredential(t *testing.T, c *ca, principal string, serial int64) metadata.MD {
	t.Helper()
	der, key := c.issue(t, principal, serial)
	proof, err := credential.Prove(key, echoMethod, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return metadata.Pairs(credential.CertificateKey, string(der), credential.ProofKey, proof)
}

func TestResolve(t *testing.T) {
	r, _, c := setup(t, false)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		md   metadata.MD
		want string // the principal, "" for none
		code codes.Code
	}{
		{"nothing", metadata.MD{}, "", codes.OK},
		{"a credential", withCredential(t, c, "ada.human.example.com", 0x10), "ada.human.example.com", codes.OK},
		{"a revoked credential", withCredential(t, c, "ada.human.example.com", 0x11), "", codes.Unauthenticated},
		{"an inactive principal's credential", withCredential(t, c, "gone.human.example.com", 0x12), "", codes.Unauthenticated},
		{"a credential the directory does not hold", withCredential(t, c, "ada.human.example.com", 0x99), "", codes.Unauthenticated},
		{"a credential naming another principal", withCredential(t, c, "eve.human.example.com", 0x10), "", codes.Unauthenticated},
		{"a credential under a name a move gave up", withCredential(t, c, "ci.robot.example.com", 0x13), "ci.acme.robot.example.com", codes.OK},
		{"a credential under a renamed person's old name", withCredential(t, c, "old-bea.human.example.com", 0x14), "", codes.Unauthenticated},
		{"a credential under another principal's alias", withCredential(t, c, "ci.robot.example.com", 0x10), "", codes.Unauthenticated},
		{"a token", metadata.Pairs("authorization", "Bearer good"), "ci.ci.robot.example.com", codes.OK},
		{"a bad token", metadata.Pairs("authorization", "Bearer bad"), "", codes.Unauthenticated},
		{"a personal token", metadata.Pairs("authorization", "Bearer okp_ada"), "ada.human.example.com", codes.OK},
		{"a personal token there is none of", metadata.Pairs("authorization", "Bearer okp_nobody"), "", codes.Unauthenticated},
		{"an inactive person's personal token", metadata.Pairs("authorization", "Bearer okp_gone"), "", codes.Unauthenticated},
		{"a session", metadata.Pairs("cookie", "a=b; ok_session=s1"), "ada.human.example.com", codes.OK},
		{"an inactive principal's session", metadata.Pairs("cookie", "ok_session=s2"), "", codes.OK},
		{"an unknown session", metadata.Pairs("cookie", "ok_session=nope"), "", codes.OK},
		{"the trusted header", metadata.Pairs("x-auth-principal", "ada.human.example.com"), "ada.human.example.com", codes.OK},
		{"the trusted header naming an organization", metadata.Pairs("x-auth-principal", "acme.org.example.com"), "", codes.Unauthenticated},
		{"a development principal, to a production server", metadata.Pairs(DevPrincipalKey, "ada.human.example.com"), "", codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := r.Resolve(ctx, tc.md, echoMethod)
			if status.Code(err) != tc.code {
				t.Fatalf("Resolve: %v, want %v", err, tc.code)
			}
			if (tc.want != "") != ok || got.Principal != tc.want {
				t.Errorf("Resolve = %+v, %v; want %q", got, ok, tc.want)
			}
		})
	}
	// A credential's proof is accepted once.
	md := withCredential(t, c, "ada.human.example.com", 0x10)
	if _, ok, err := r.Resolve(ctx, md, echoMethod); err != nil || !ok {
		t.Fatalf("a credential's proof: %v, %v", ok, err)
	}
	if _, ok, err := r.Resolve(ctx, md, echoMethod); status.Code(err) != codes.Unauthenticated || ok {
		t.Errorf("the proof again: %v, %v", ok, err)
	}
	// A personal token's caller carries what it allows, and how they came.
	if got, ok, err := r.Resolve(ctx, metadata.Pairs("authorization", "Bearer okp_ada"), echoMethod); err != nil || !ok ||
		got.Via != ViaPersonalToken || got.Token == nil || got.Token.Role != tenancy.Read || len(got.Token.Repositories) != 1 {
		t.Errorf("a personal token's caller: %+v, %v, %v", got, ok, err)
	}
	dev, _, _ := setup(t, true)
	if got, ok, err := dev.Resolve(ctx, metadata.Pairs(DevPrincipalKey, "ada.human.example.com"), echoMethod); err != nil || !ok || !got.Human || got.Via != ViaDev {
		t.Errorf("a development principal, to a development server: %+v, %v, %v", got, ok, err)
	}
	// An organization acts as no one.
	if got, ok, err := dev.Resolve(ctx, metadata.Pairs(DevPrincipalKey, "acme.org.example.com"), echoMethod); status.Code(err) != codes.Unauthenticated || ok {
		t.Errorf("an organization as a development principal: %+v, %v, %v", got, ok, err)
	}
}

// The directory is asked each time a caller is resolved — what it said is kept by no replica — and a
// directory that cannot answer refuses the caller as unavailable.
func TestDirectoryAskedEachTimeAndOutage(t *testing.T) {
	r, d, _ := setup(t, false)
	ctx := context.Background()
	md := metadata.Pairs("x-auth-principal", "ada.human.example.com")
	for range 3 {
		if _, ok, err := r.Resolve(ctx, md, echoMethod); err != nil || !ok {
			t.Fatal(err)
		}
	}
	if d.asked != 3 {
		t.Errorf("the directory was asked %d times, want each of three", d.asked)
	}
	d.down = true
	if _, _, err := r.Resolve(ctx, md, echoMethod); status.Code(err) != codes.Unavailable {
		t.Errorf("a directory that cannot answer: %v", err)
	}
}

func TestCallers(t *testing.T) {
	r, _, _ := setup(t, false)
	callers := Callers("ok.echo.v1.Echo")
	if len(callers) != 2 {
		t.Fatalf("Callers = %v", callers)
	}
	for _, tc := range []struct {
		c    Caller
		ok   bool
		want bool
	}{
		{Caller{}, false, false},
		{Caller{Principal: "ada.human.example.com", Human: true}, true, true},
		{Caller{Principal: "control.ok.robot.example.com"}, true, true},
		{Caller{Principal: "ci.ci.robot.example.com"}, true, false},
	} {
		if got := r.Allowed(callers, tc.c, tc.ok); got != tc.want {
			t.Errorf("Allowed(%+v) = %v", tc.c, got)
		}
	}
	if got := Callers("ok.nothing.v1.Undeclared"); len(got) != 0 {
		t.Errorf("an undeclared service admits %v", got)
	}
	if !r.Allowed(Callers("grpc.health.v1.Health"), Caller{}, false) {
		t.Error("health is not open")
	}
	// Bazel's remote cache, outside ok's protocols, is workers' alone.
	for _, svc := range []string{"build.bazel.remote.execution.v2.ActionCache", "google.bytestream.ByteStream"} {
		if got := Callers(svc); len(got) != 1 || got[0] != okpb.Caller_CALLER_WORKER {
			t.Errorf("%s admits %v, want workers alone", svc, got)
		}
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-auth-principal", "ci.ci.robot.example.com"))
	if _, err := r.admit(ctx, echoMethod); status.Code(err) != codes.PermissionDenied {
		t.Errorf("a robot that may not call: %v", err)
	}
	if _, err := r.admit(context.Background(), echoMethod); status.Code(err) != codes.Unauthenticated {
		t.Errorf("no caller: %v", err)
	}
}

func TestMiddleware(t *testing.T) {
	r, _, _ := setup(t, false)
	var got Caller
	h := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { got, _ = FromContext(req.Context()) }))
	req := httptest.NewRequest("GET", "/x", nil)
	req.AddCookie(&http.Cookie{Name: SessionCookie, Value: "s1"})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if got.Principal != "ada.human.example.com" || got.Via != ViaSession {
		t.Errorf("a session's request: %+v (%d)", got, w.Code)
	}
	req = httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("Authorization", "Bearer bad")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("a bad token's request: %d", w.Code)
	}
}
