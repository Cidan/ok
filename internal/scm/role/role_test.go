package role_test

import (
	"context"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/pgbus"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/federation/fedtest"
	"github.com/Cidan/ok/internal/id/idtest"
	idrole "github.com/Cidan/ok/internal/id/role"
	idstore "github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/scm/store"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
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
	blobs := testobjects.Store(t)
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
	if _, err := scmpb.NewReviewClient(selfConn).CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: "x", ActorId: "prn_ada"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("the SCM calling Review: %v", err)
	}
}

// In development, beside a development identity, the SCM names itself to it and trusts the
// principal a caller names, on loopback: what the jj client's tests run against.
func TestSCMDev(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	id := idtest.Start(t, "ada")
	cfg, err := config.Parse([]byte("domain: example.com\npublic_url: http://127.0.0.1:1\nroles: " + idtest.Roles + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgdb.Open(ctx, testdb.Postgres(t), role.Schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	blobs := testobjects.Store(t)
	scm, err := role.Build(ctx, cfg, role.Options{Dev: true, Pool: pool, Blobs: blobs, Identity: id.As("scm.ok.robot.example.com")})
	if err != nil {
		t.Fatal(err)
	}
	defer scm.Close()
	adaID := id.ID("ada.human.example.com")
	// The SCM publishes its events on the bus the configuration names: PostgreSQL's, beside its
	// own tables.
	busPool, err := pgdb.Beside(ctx, pool, pgbus.Schema)
	if err != nil {
		t.Fatal(err)
	}
	defer busPool.Close()
	b, err := pgbus.Open(ctx, busPool, pgbus.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateGroup(ctx, store.EventTopic, "test"); err != nil {
		t.Fatal(err)
	}
	published := make(chan *scmpb.Event, 16)
	go b.Subscribe(ctx, store.EventTopic, "test", func(_ context.Context, e bus.Event) error {
		ev := &scmpb.Event{}
		if err := proto.Unmarshal(e.Body, ev); err != nil || e.Key != ev.GetRepositoryId() || e.Seq != ev.GetRepositorySeq() {
			t.Errorf("a published event %+v: %v, %v", e, ev, err)
		}
		published <- ev
		return nil
	})
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
	if err != nil || r.GetName() != "dev" || r.GetCreatedBy() != "ada.human.example.com" || r.GetCreatedById() != adaID {
		t.Fatalf("CreateRepository = %v, %v", r, err)
	}
	if got, err := as("ada.human.example.com").GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: "dev"}}); err != nil || got.GetId() != r.GetId() {
		t.Errorf("by its name = %v, %v", got, err)
	}
	select {
	case ev := <-published:
		if ev.GetRepositoryId() != r.GetId() || ev.GetRepositorySeq() != 1 || ev.GetRepositoryCreated().GetCreatedById() != adaID {
			t.Errorf("the published event: %v", ev)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the repository's creation was never published")
	}
	if _, err := as("not a name").GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: "dev"}}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("a name that is no principal's: %v", err)
	}
	review := func(p string) error {
		conn, _ := dial.Dial(lis.Addr().String(), dial.Options{DevPrincipal: p})
		defer conn.Close()
		_, err := scmpb.NewReviewClient(conn).CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: "by-" + p, ActorId: adaID})
		return err
	}
	if err := review("control.ok.robot.example.com"); err != nil {
		t.Errorf("ok control calling Review: %v", err)
	}
	if err := review("ada.human.example.com"); status.Code(err) != codes.PermissionDenied {
		t.Errorf("a person calling Review: %v", err)
	}
}

// Two cells, one directory: the directory places each new repository in the open cell holding the
// fewest, an SCM asked for one placed in the other cell has that cell's SCM make it, and each SCM
// refuses the other's repositories, naming the cell that holds them (docs/hosted.md, "Cells, as
// built").
func TestSCMCells(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lis := map[string]net.Listener{}
	var list []config.IdentityCell
	for _, name := range []string{"c1", "c2"} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lis[name] = l
		list = append(list, config.IdentityCell{Name: name, SCM: l.Addr().String()})
	}
	id := idtest.StartWithCells(t, list, "ada")
	var dones []chan error
	defer func() {
		cancel()
		for _, d := range dones {
			<-d
		}
	}()
	for _, name := range []string{"c1", "c2"} {
		cfg, err := config.Parse([]byte("domain: example.com\npublic_url: http://127.0.0.1:1\nroles: " + idtest.Roles + "\ncell: {name: " + name + "}\n"))
		if err != nil {
			t.Fatal(err)
		}
		pool, err := pgdb.Open(ctx, testdb.Postgres(t), role.Schema, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		scm, err := role.Build(ctx, cfg, role.Options{Dev: true, Pool: pool, Blobs: testobjects.Store(t), Identity: id.As("scm.ok.robot.example.com")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(scm.Close)
		done := make(chan error, 1)
		dones = append(dones, done)
		go func() { done <- serve.Run(ctx, scm.Root, serve.WithAuthn(scm.Resolver), serve.WithListener(lis[name])) }()
	}
	at := func(cell string, who ...string) scmpb.RepositoriesClient {
		p := "ada.human.example.com"
		if len(who) > 0 {
			p = who[0]
		}
		conn, err := dial.Dial(lis[cell].Addr().String(), dial.Options{DevPrincipal: p})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return scmpb.NewRepositoriesClient(conn)
	}
	// Four repositories, all asked of c1's SCM: placed in turn, the fewest first.
	for i, want := range []string{"c1", "c2", "c1", "c2"} {
		name := fmt.Sprintf("app-%d", i)
		r, err := at("c1").CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: name})
		if err != nil {
			t.Fatalf("making %s: %v", name, err)
		}
		e, err := id.Store.Repository(ctx, r.GetId())
		if err != nil || e.Cell != want || e.State != idstore.Active {
			t.Fatalf("%s's entry: %+v, %v; want it active in %s", name, e, err, want)
		}
		other := map[string]string{"c1": "c2", "c2": "c1"}[want]
		byID := &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: r.GetId()}}
		if got, err := at(want).GetRepository(ctx, byID); err != nil || got.GetId() != r.GetId() {
			t.Errorf("%s from its cell %s: %v, %v", name, want, got, err)
		}
		byName := &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: name}}
		for _, who := range []string{"ada.human.example.com", "control.ok.robot.example.com"} {
			for how, req := range map[string]*scmpb.GetRepositoryRequest{"by ID": byID, "by name": byName} {
				_, err = at(other, who).GetRepository(ctx, req)
				if m, ok := cells.MovedOf(err); !ok || m.GetCell() != want || m.GetRepositoryId() != r.GetId() {
					t.Errorf("%s %s from cell %s, as %s: %v; want it refused, naming %s", name, how, other, who, err, want)
				}
			}
		}
	}
}
