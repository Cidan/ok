package idclient_test

import (
	"context"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/federation/fedtest"
	"github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/testdb"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

const scmRobot = "scm.ok.robot.example.com"

// A role certifies a key with its ServiceAccount's token, calls identity with the credential,
// renews it, and certifies anew once it is revoked or lapses.
func TestWorkload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	iss := fedtest.New(t)
	cfg, err := config.Parse([]byte(`
domain: example.com
public_url: https://ok.example.com
roles: {scm: scm.ok}
identity:
  ca_dir: ` + filepath.Join(dir, "ca") + `
  federation: [{issuer: "` + iss.URL + `", audience: ok}]
  robots:
    - name: scm.ok
      bindings: [{issuer: "` + iss.URL + `", subject: "system:serviceaccount:ok:scm"}]
`))
	if err != nil {
		t.Fatal(err)
	}
	pool := testdb.Pool(t)
	r, err := role.Build(ctx, cfg, role.Options{Pool: pool})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, r.Root, serve.WithAuthn(r.Resolver), serve.WithListener(lis)) }()
	defer func() { cancel(); <-done }()
	addr := lis.Addr().String()

	tokenFile := filepath.Join(dir, "token")
	os.WriteFile(tokenFile, []byte(iss.Token(t, map[string]any{"aud": "ok", "sub": "system:serviceaccount:ok:scm"})+"\n"), 0o600)
	w, err := idclient.NewWorkload(addr, dial.Options{}, tokenFile, "scm-0")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := dial.Dial(addr, dial.Options{Credential: w})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	d := idclient.NewDirectory(conn)
	p, err := d.Principal(ctx, scmRobot)
	if err != nil || p.Human || !p.Active || w.Principal() != scmRobot {
		t.Fatalf("Principal = %+v, %v (the workload is %q)", p, err, w.Principal())
	}
	if _, err := d.Principal(ctx, "nobody.human.example.com"); err == nil {
		t.Error("a principal that does not exist")
	}
	roots, err := idclient.NewRoots(ctx, conn, "test")
	if err != nil {
		t.Fatal(err)
	}
	certDER, key, _ := w.Current(ctx)
	first, _ := x509.ParseCertificate(certDER)
	id, _ := credential.ID(first)
	proof, _ := credential.Prove(key, "/m", time.Now())
	if v, err := credential.NewVerifier(roots.Pool).Verify("/m", certDER, proof); err != nil || v.Principal != scmRobot {
		t.Errorf("verifying the workload's credential against the roots: %+v, %v", v, err)
	}
	if c, err := d.Credential(ctx, id); err != nil || !c.Active || c.Name != scmRobot {
		t.Errorf("Credential = %+v, %v", c, err)
	}

	// A credential certifies nothing: only a token does.
	k2, _ := credential.NewKey()
	spki2, _ := x509.MarshalPKIXPublicKey(&k2.PublicKey)
	if _, err := identitypb.NewCredentialsClient(conn).Certify(ctx, &identitypb.CertifyRequest{PublicKey: spki2, Label: "x"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("Certify with a credential: %v", err)
	}

	// Near expiry, it renews: the same credential, a new certificate.
	idclient.SetNow(w, first.NotAfter.Add(-time.Hour))
	renewedDER, _, err := w.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	renewed, _ := x509.ParseCertificate(renewedDER)
	if rid, _ := credential.ID(renewed); rid != id || renewed.SerialNumber.Cmp(first.SerialNumber) == 0 {
		t.Errorf("renewed: credential %s (was %s), serial %v (was %v)", rid, id, renewed.SerialNumber, first.SerialNumber)
	}

	// Revoked, it certifies a new key with its token.
	if err := r.Store.Revoke(ctx, id); err != nil {
		t.Fatal(err)
	}
	idclient.SetNow(w, renewed.NotAfter.Add(-time.Hour))
	againDER, _, err := w.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := x509.ParseCertificate(againDER)
	if aid, _ := credential.ID(again); aid == id {
		t.Error("a revoked credential was renewed")
	}

	// Lapsed, likewise.
	idclient.SetNow(w, again.NotAfter.Add(time.Hour))
	lastDER, _, err := w.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	last, _ := x509.ParseCertificate(lastDER)
	if lid, _ := credential.ID(last); lid == id {
		t.Error("a lapsed credential was kept")
	}

	// A token for another subject certifies nothing.
	os.WriteFile(tokenFile, []byte(iss.Token(t, map[string]any{"aud": "ok", "sub": "system:serviceaccount:ok:other"})), 0o600)
	idclient.SetNow(w, last.NotAfter.Add(time.Hour))
	if _, _, err := w.Current(ctx); err == nil {
		t.Error("an unbound subject's token certified a key")
	}
}

// A role in identity's process: its key certified by identity directly, its certificate reissued
// before it expires, verified against the CA's certificates; and roots read while identity does not
// answer start empty, verifying nothing.
func TestLocal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(`
domain: example.com
public_url: https://ok.example.com
roles: {scm: scm.ok}
identity:
  ca_dir: ` + filepath.Join(dir, "ca") + `
`))
	if err != nil {
		t.Fatal(err)
	}
	pool := testdb.Pool(t)
	r, err := role.Build(ctx, cfg, role.Options{Pool: pool})
	if err != nil {
		t.Fatal(err)
	}

	// Before identity serves, roots read nothing, and nothing verifies against them.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	early, err := dial.Dial(addr, dial.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer early.Close()
	lis.Close()
	roots, err := idclient.NewRoots(ctx, early, "scm")
	if err != nil {
		t.Fatal(err)
	}

	l := idclient.NewLocal(r.Services, "scm.ok")
	certDER, key, err := l.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	proof, _ := credential.Prove(key, "/m", time.Now())
	if _, err := credential.NewVerifier(roots.Pool).Verify("/m", certDER, proof); err == nil {
		t.Error("a credential verified against roots never read")
	}

	// Identity serving, roots read: the local credential is the robot's.
	lis, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, r.Root, serve.WithAuthn(r.Resolver), serve.WithListener(lis)) }()
	defer func() { cancel(); <-done }()
	ready, err := dial.Dial(addr, dial.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer ready.Close()
	if roots, err = idclient.NewRoots(ctx, ready, "scm"); err != nil {
		t.Fatal(err)
	}
	proof, _ = credential.Prove(key, "/m", time.Now())
	v, err := credential.NewVerifier(roots.Pool).Verify("/m", certDER, proof)
	if err != nil || v.Principal != scmRobot {
		t.Fatalf("the local credential: %+v, %v", v, err)
	}
	conn, err := dial.Dial(addr, dial.Options{Credential: l})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if p, err := idclient.NewDirectory(conn).Principal(ctx, scmRobot); err != nil || p.Human {
		t.Errorf("calling identity with it: %+v, %v", p, err)
	}

	// Near expiry, it is reissued: the same credential, a new certificate, the same key.
	first, _ := x509.ParseCertificate(certDER)
	idclient.SetLocalNow(l, first.NotAfter.Add(-time.Hour))
	again, key2, err := l.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := x509.ParseCertificate(again)
	id1, _ := credential.ID(first)
	id2, _ := credential.ID(second)
	if id1 != id2 || second.SerialNumber.Cmp(first.SerialNumber) == 0 || key2 != key {
		t.Errorf("reissued: credential %s → %s, serial %v → %v", id1, id2, first.SerialNumber, second.SerialNumber)
	}
}
