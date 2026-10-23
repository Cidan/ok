package role_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/control/role"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/federation/fedtest"
	idrole "github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/pgdb"
	scmrole "github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
	controlpb "github.com/Cidan/ok/proto/ok/control/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

const (
	ada = "ada.human.example.com"
	bea = "bea.human.example.com"
)

// serveRole runs a role's root on a loopback port until the test ends.
func serveRole(t *testing.T, ctx context.Context, root node.Node, opts ...serve.Option) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, root, append(opts, serve.WithListener(lis))...) }()
	t.Cleanup(func() { cancel(); <-done })
	return lis.Addr().String()
}

// ok control beside identity and the SCM, in one database: it certifies its key with its
// ServiceAccount's token, follows the SCM's change feed, shows its pages to a signed-in person and
// sends anyone else to sign in, creates repositories on the SCM for the person who asks, and
// serves the ChangeLists service.
func TestControl(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "people.yaml"), []byte("people:\n  - {id: \"1\", email: ada@example.com, name: Ada}\n  - {id: \"2\", email: bea@example.com, name: Bea}\n"), 0o644)
	iss := fedtest.New(t)
	cfg, err := config.Parse([]byte(`
domain: example.com
installation: Example ok
public_url: http://127.0.0.1:1
roles: {identity: identity.ok, scm: scm.ok, control: control.ok}
identity:
  ca_dir: ` + filepath.Join(dir, "ca") + `
  bootstrap_admins: [bea]
  system_of_record: {static: {file: ` + filepath.Join(dir, "people.yaml") + `}}
  federation: [{issuer: "` + iss.URL + `", audience: ok}]
  robots:
    - name: scm.ok
      bindings: [{issuer: "` + iss.URL + `", subject: "system:serviceaccount:ok:scm"}]
    - name: control.ok
      bindings: [{issuer: "` + iss.URL + `", subject: "system:serviceaccount:ok:control"}]
`))
	if err != nil {
		t.Fatal(err)
	}
	dbURL := testdb.Postgres(t)
	open := func(schema string) *pgxpool.Pool {
		p, err := pgdb.Open(ctx, dbURL, schema, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}

	// ok identity, and its sync making ada and bea.
	id, err := idrole.Build(ctx, cfg, idrole.Options{Pool: open(idrole.Schema)})
	if err != nil {
		t.Fatal(err)
	}
	idAddr := serveRole(t, ctx, id.Root, serve.WithAuthn(id.Resolver))
	for i := 0; ; i++ {
		if _, err := id.Store.Principal(ctx, bea); err == nil {
			break
		}
		if i == 100 {
			t.Fatal("the sync never made the people")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// A role's credential, certified with its token, and a connection to a role with it.
	as := func(sa string) *idclient.Workload {
		tok := filepath.Join(dir, sa+".token")
		os.WriteFile(tok, []byte(iss.Token(t, map[string]any{"aud": "ok", "sub": "system:serviceaccount:ok:" + sa})), 0o600)
		w, err := idclient.NewWorkload(idAddr, dial.Options{}, tok, sa)
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	conn := func(addr string, w *idclient.Workload) *grpc.ClientConn {
		c, err := dial.Dial(addr, dial.Options{Credential: w})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}

	// The SCM.
	scmCred := as("scm")
	blobs := testobjects.Store(t)
	scm, err := scmrole.Build(ctx, cfg, scmrole.Options{Pool: open(scmrole.Schema), Blobs: blobs, Identity: conn(idAddr, scmCred)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scm.Close)
	scmAddr := serveRole(t, ctx, scm.Root, serve.WithAuthn(scm.Resolver))

	// ok control, as control.ok.
	ctlCred := as("control")
	ctl, err := role.Build(ctx, cfg, role.Options{Dev: true, Pool: open(role.Schema), Identity: conn(idAddr, ctlCred), SCM: conn(scmAddr, ctlCred)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ctl.Close)
	ctlAddr := serveRole(t, ctx, ctl.Root, serve.WithAuthn(ctl.Resolver))
	base := "http://" + ctlAddr
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	get := func(path, who string) (int, string, http.Header) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, base+path, nil)
		if who != "" {
			req.Header.Set(authn.DevPrincipalKey, who)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b), resp.Header
	}

	// No one is sent to sign in; a person sees the finder, under the installation's name.
	if code, _, h := get("/", ""); code != http.StatusSeeOther || h.Get("Location") != "/id/login?next=%2F" {
		t.Errorf("no one: %d, %s", code, h.Get("Location"))
	}
	if code, body, _ := get("/", ada); code != http.StatusOK || !strings.Contains(body, "<title>Repositories · Example ok</title>") {
		t.Fatalf("ada's home: %d\n%.400s", code, body)
	}

	// ada creates a repository through control, which asks the SCM as itself for her.
	form := url.Values{"name": {"app"}}
	req, _ := http.NewRequest(http.MethodPost, base+"/repositories/new", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set(authn.DevPrincipalKey, ada)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/app" {
		t.Fatalf("creating a repository: %d, %s", resp.StatusCode, resp.Header.Get("Location"))
	}
	repo, err := scm.Store.GetRepositoryByName(ctx, "app")
	if err != nil || repo.CreatedBy != ada {
		t.Fatalf("the SCM's repository: %+v, %v", repo, err)
	}

	// The change feed brings it to the finder.
	for i := 0; ; i++ {
		_, body, _ := get("/repositories/list", ada)
		if strings.Contains(body, `href="/app"`) {
			break
		}
		if i == 100 {
			t.Fatalf("the finder never listed it:\n%.600s", body)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if code, body, _ := get("/app", ada); code != http.StatusOK || !strings.Contains(body, "app · Example ok") {
		t.Errorf("the repository's page: %d\n%.400s", code, body)
	}

	// The ChangeLists service serves people, and no one else.
	cl, err := dial.Dial(ctlAddr, dial.Options{DevPrincipal: ada})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	_, err = controlpb.NewChangeListsClient(cl).Status(ctx, &controlpb.CLRef{Repository: "app", Number: 1})
	if status.Code(err) != codes.NotFound {
		t.Errorf("a CL that does not exist: %v", err)
	}
	robot := metadata.AppendToOutgoingContext(ctx, authn.DevPrincipalKey, "scm.ok.robot.example.com")
	plain, err := dial.Dial(ctlAddr, dial.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { plain.Close() })
	if _, err := controlpb.NewChangeListsClient(plain).Status(robot, &controlpb.CLRef{Repository: "app", Number: 1}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("a robot calling ChangeLists: %v", err)
	}

	// Who administers comes from identity: bea, a bootstrap administrator, administers app; ada
	// created it.
	repoPB := &scmpb.Repository{Name: repo.Name, CreatedBy: repo.CreatedBy}
	if !ctl.Review.IsAdmin(ctx, repoPB, bea) || !ctl.Review.IsAdmin(ctx, repoPB, ada) || ctl.Review.IsAdmin(ctx, repoPB, "cal.human.example.com") {
		t.Errorf("IsAdmin: bea %v, ada %v", ctl.Review.IsAdmin(ctx, repoPB, bea), ctl.Review.IsAdmin(ctx, repoPB, ada))
	}
}
