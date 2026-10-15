package role_test

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

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/federation/fedtest"
	idrole "github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/testdb"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// The SCM role, beside identity in one database: it certifies its key with its ServiceAccount's
// token, resolves callers through identity, and serves a person who signed in with the device
// login.
func TestSCMWithIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "people.yaml"), []byte("people:\n  - {id: \"1\", email: ada@example.com, name: Ada}\n"), 0o644)
	iss := fedtest.New(t)
	cfg, err := config.Parse([]byte(`
domain: example.com
public_url: https://ok.example.com
roles: {identity: identity.ok, scm: scm.ok, control: control.ok}
identity:
  ca_dir: ` + filepath.Join(dir, "ca") + `
  system_of_record: {static: {file: ` + filepath.Join(dir, "people.yaml") + `}}
  federation: [{issuer: "` + iss.URL + `", audience: ok}]
  robots:
    - name: scm.ok
      bindings: [{issuer: "` + iss.URL + `", subject: "system:serviceaccount:ok:scm"}]
`))
	if err != nil {
		t.Fatal(err)
	}
	url := testdb.Postgres(t)
	idPool, err := pgdb.Open(ctx, url, idrole.Schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer idPool.Close()
	id, err := idrole.Build(ctx, cfg, idrole.Options{Pool: idPool})
	if err != nil {
		t.Fatal(err)
	}
	idLis, _ := net.Listen("tcp", "127.0.0.1:0")
	idDone := make(chan error, 1)
	go func() { idDone <- serve.Run(ctx, id.Root, serve.WithAuthn(id.Resolver), serve.WithListener(idLis)) }()
	defer func() { <-idDone }()
	idAddr := idLis.Addr().String()
	for i := 0; ; i++ {
		if _, err := id.Store.Principal(ctx, "ada.human.example.com"); err == nil {
			break
		}
		if i == 100 {
			t.Fatal("the sync never made ada")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The SCM's credential, certified with its token.
	tokenFile := filepath.Join(dir, "token")
	os.WriteFile(tokenFile, []byte(iss.Token(t, map[string]any{"aud": "ok", "sub": "system:serviceaccount:ok:scm"})), 0o600)
	w, err := idclient.NewWorkload(idAddr, dial.Options{}, tokenFile, "scm-0")
	if err != nil {
		t.Fatal(err)
	}
	idConn, err := dial.Dial(idAddr, dial.Options{Credential: w})
	if err != nil {
		t.Fatal(err)
	}
	defer idConn.Close()
	scmPool, err := pgdb.Open(ctx, url, role.Schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer scmPool.Close()
	s3 := testdb.S3(t)
	blobs, err := blob.NewS3(blob.Config{Endpoint: s3.Endpoint, Bucket: testdb.Bucket(t), AccessKey: s3.AccessKey, SecretKey: s3.SecretKey, Region: s3.Region})
	if err != nil {
		t.Fatal(err)
	}
	scm, err := role.Build(ctx, cfg, role.Options{Pool: scmPool, Blobs: blobs, Identity: idConn})
	if err != nil {
		t.Fatal(err)
	}
	defer scm.Close()
	scmLis, _ := net.Listen("tcp", "127.0.0.1:0")
	scmDone := make(chan error, 1)
	go func() { scmDone <- serve.Run(ctx, scm.Root, serve.WithAuthn(scm.Resolver), serve.WithListener(scmLis)) }()
	defer func() { cancel(); <-scmDone }()
	scmAddr := scmLis.Addr().String()

	// ada signs a machine in.
	key, _ := credential.NewKey()
	spki, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	anon, err := dial.Dial(idAddr, dial.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer anon.Close()
	login := identitypb.NewDeviceLoginClient(anon)
	start, err := login.Start(ctx, &identitypb.StartRequest{PublicKey: spki, Label: "ada's laptop"})
	if err != nil {
		t.Fatal(err)
	}
	ada, _ := id.Store.Principal(ctx, "ada.human.example.com")
	if err := id.Store.DecideDeviceLogin(ctx, start.GetUserCode(), ada.ID, true); err != nil {
		t.Fatal(err)
	}
	poll, err := login.Poll(ctx, &identitypb.PollRequest{DeviceCode: start.GetDeviceCode()})
	if err != nil || poll.GetState() != identitypb.PollState_POLL_STATE_APPROVED {
		t.Fatalf("Poll = %v, %v", poll, err)
	}

	// ada's credential reaches the SCM; no credential does not.
	adaConn, err := dial.Dial(scmAddr, dial.Options{Credential: dial.Static{Cert: poll.GetCertificate().GetChain()[0], Key: key}})
	if err != nil {
		t.Fatal(err)
	}
	defer adaConn.Close()
	repos := scmpb.NewRepositoriesClient(adaConn)
	r, err := repos.CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: "app"})
	if err != nil || r.GetCreatedBy() != "ada.human.example.com" {
		t.Fatalf("CreateRepository = %v, %v", r, err)
	}
	nobody, err := dial.Dial(scmAddr, dial.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer nobody.Close()
	if _, err := scmpb.NewRepositoriesClient(nobody).GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: "app"}}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("no credential: %v", err)
	}
	// The SCM's own robot is none of Review's callers: only ok control calls it.
	selfConn, err := dial.Dial(scmAddr, dial.Options{Credential: w})
	if err != nil {
		t.Fatal(err)
	}
	defer selfConn.Close()
	if _, err := scmpb.NewReviewClient(selfConn).CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: "x", Actor: "ada.human.example.com"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("the SCM calling Review: %v", err)
	}
}

// With -dev and no identity, the SCM trusts the principal a caller names, on loopback: what the
// jj client's tests run against.
func TestSCMDevWithoutIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg, err := config.Parse([]byte("domain: example.com\npublic_url: http://127.0.0.1:1\nroles: {control: control.ok}\n"))
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgdb.Open(ctx, testdb.Postgres(t), role.Schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s3 := testdb.S3(t)
	blobs, err := blob.NewS3(blob.Config{Endpoint: s3.Endpoint, Bucket: testdb.Bucket(t), AccessKey: s3.AccessKey, SecretKey: s3.SecretKey, Region: s3.Region})
	if err != nil {
		t.Fatal(err)
	}
	scm, err := role.Build(ctx, cfg, role.Options{Dev: true, Pool: pool, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	defer scm.Close()
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, scm.Root, serve.WithAuthn(scm.Resolver), serve.WithListener(lis)) }()
	defer func() { cancel(); <-done }()
	as := func(p string) scmpb.RepositoriesClient {
		conn, err := dial.Dial(lis.Addr().String(), dial.Options{DevPrincipal: p})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return scmpb.NewRepositoriesClient(conn)
	}
	r, err := as("ada.human.example.com").CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: "dev"})
	if err != nil || r.GetCreatedBy() != "ada.human.example.com" {
		t.Fatalf("CreateRepository = %v, %v", r, err)
	}
	if _, err := as("not a name").GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: "dev"}}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("a name that is no principal's: %v", err)
	}
	review := func(p string) error {
		conn, _ := dial.Dial(lis.Addr().String(), dial.Options{DevPrincipal: p})
		defer conn.Close()
		_, err := scmpb.NewReviewClient(conn).CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: "by-control", Actor: "ada.human.example.com"})
		return err
	}
	if err := review("control.ok.robot.example.com"); err != nil {
		t.Errorf("ok control calling Review: %v", err)
	}
	if err := review("ada.human.example.com"); status.Code(err) != codes.PermissionDenied {
		t.Errorf("a person calling Review: %v", err)
	}
}
