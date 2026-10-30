package role_test

import (
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus/bustest"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	autorole "github.com/Cidan/ok/internal/automation/role"
	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/deploy/kubesim"
	"github.com/Cidan/ok/internal/deploy/role"
	"github.com/Cidan/ok/internal/deploy/roll"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/ca"
	idrole "github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/pgdb"
	scmrole "github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/testbazel"
	"github.com/Cidan/ok/internal/testdb/roledb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
	"github.com/Cidan/ok/internal/testjj"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

const (
	ada      = "ada.human.example.com"
	control  = "control.ok.robot.example.com"
	deployer = "deploy.ok.robot.example.com"
	scmBot   = "scm.ok.robot.example.com"
	autoBot  = "automation.ok.robot.example.com"
)

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

func as(t *testing.T, addr, principal string) *grpc.ClientConn {
	t.Helper()
	c, err := dial.Dial(addr, dial.Options{DevPrincipal: principal})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func bucket(t *testing.T) blob.Store {
	t.Helper()
	return testobjects.Store(t)
}

// fixture is the tree's repository: a Bazel workspace of Starlark rules alone whose deploy runs one
// migration — a script speaking the migrate protocol, which says what database its environment
// names — and the groups-and-ACLs tree, at identity/.
var fixture = map[string]string{
	"MODULE.bazel":        "module(name = \"infra\")\n",
	"BUILD.bazel":         "",
	".ok/repository.yaml": "build: bazel\nidentity: identity\n",
	".ok/workflow/deploy.yaml": `deploy:
  data:
    migrate: [//db:migrate]
`,
	"rules.bzl": `def _script_impl(ctx):
    out = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(out, "#!/bin/sh\n" + ctx.attr.script, is_executable = True)
    return [DefaultInfo(executable = out)]

script_binary = rule(implementation = _script_impl, executable = True, attrs = {"script": attr.string()})
`,
	"db/BUILD.bazel": `load("//:rules.bzl", "script_binary")

script_binary(
    name = "migrate",
    script = """echo "migrating $APP_DATABASE_URL" >&2
echo '{"database":"app","steps":[{"description":"0001_create","sql":"CREATE TABLE t (id int)"}],"executed":true}'
""",
)
`,
	"identity/acl.yaml":       "members: [ada.human.example.com]\n",
	"identity/eng/acl.yaml":   "members: [ada.human.example.com]\n",
	"identity/eng/group.yaml": "members: [ada.human.example.com]\n",
}

// The deployer on its own: following main from the SCM's change feed, planning with real Bazel in a
// real checkout, running the deploy's migration through the migrate protocol with the environment
// the configuration gives it, and applying the groups-and-ACLs tree to ok identity — then a change
// to the tree applying it again, and Deployments serving ok control.
func TestDeployer(t *testing.T) {
	jj := testjj.Path(t)
	bazel := testbazel.Bazel(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "people.yaml"), []byte("people:\n  - {id: \"1\", email: ada@example.com, name: Ada}\n"), 0o644)
	state := t.TempDir()
	cfg, err := config.Parse([]byte(`
domain: example.com
public_url: http://127.0.0.1:1
roles: {identity: identity.ok, scm: scm.ok, control: control.ok, automation: automation.ok, worker: worker.ok, deploy: deploy.ok}
identity:
  ca_dir: ` + filepath.Join(dir, "ca") + `
  tree_repository: infra
  system_of_record: {static: {file: ` + filepath.Join(dir, "people.yaml") + `}}
deploy:
  state: ` + state + `
  checkout: real
  self: {namespace: ok, deployment: ok-deploy}
`))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Bus = bustest.Config(t)
	// The roles' databases (roledb, as OK_TEST_DATABASE names them): on Spanner each role opens its
	// own as cfg says, and no pool.
	dbs := roledb.New(t)
	dbs.Configure(cfg)
	open := func(schema string) *pgxpool.Pool {
		if dbs.OnSpanner() {
			return nil
		}
		p, err := pgdb.Open(ctx, dbs.URL(), schema, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}
	id, err := idrole.Build(ctx, cfg, idrole.Options{Dev: true, Pool: open(idrole.Schema)})
	if err != nil {
		t.Fatal(err)
	}
	idAddr := serveRole(t, ctx, id.Root, serve.WithAuthn(id.Resolver))
	var adaID string // ada's stable ID, as ok identity minted it
	for i := 0; ; i++ {
		if p, err := id.Store.Principal(ctx, ada); err == nil {
			adaID = p.ID
			break
		}
		if i == 200 {
			t.Fatal("the sync never made ada")
		}
		time.Sleep(50 * time.Millisecond)
	}
	scm, err := scmrole.Build(ctx, cfg, scmrole.Options{Dev: true, Pool: open(scmrole.Schema), Blobs: bucket(t), Identity: as(t, idAddr, scmBot)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scm.Close)
	scmAddr := serveRole(t, ctx, scm.Root, serve.WithAuthn(scm.Resolver))
	auto, err := autorole.Build(ctx, cfg, autorole.Options{Dev: true, Pool: open(autorole.Schema), Logs: bucket(t),
		Identity: as(t, idAddr, autoBot), SCM: as(t, scmAddr, autoBot)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(auto.Close)
	autoAddr := serveRole(t, ctx, auto.Root, serve.WithAuthn(auto.Resolver))

	// ada makes the tree's repository, and it lands.
	home := t.TempDir()
	jjRun := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command(jj, append([]string{"--no-pager", "--color=never", "--config", "ok.endpoint=http://" + scmAddr}, args...)...)
		cmd.Dir = filepath.Join(home, dir)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "OK_DEV_PRINCIPAL=" + ada, "JJ_USER=ada", "JJ_EMAIL=ada@example.com"}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("jj %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	write := func(files map[string]string) {
		for p, c := range files {
			full := filepath.Join(home, "infra", filepath.FromSlash(p))
			os.MkdirAll(filepath.Dir(full), 0o755)
			if err := os.WriteFile(full, []byte(c), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	scmAda := as(t, scmAddr, ada)
	land := func(msg string) []byte {
		t.Helper()
		jjRun("infra", "describe", "-m", msg)
		m := regexp.MustCompile(`CL (\d+) (?:created|patchset)`).FindStringSubmatch(jjRun("infra", "cl", "--ready"))
		if m == nil {
			t.Fatal("jj cl uploaded nothing")
		}
		n, _ := strconv.ParseInt(m[1], 10, 64)
		repos := scmpb.NewRepositoriesClient(scmAda)
		r, err := repos.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: "infra"}})
		if err != nil {
			t.Fatal(err)
		}
		c, err := scmpb.NewChangeListsClient(scmAda).GetChangeList(ctx, &scmpb.GetChangeListRequest{RepositoryId: r.GetId(), Number: n})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := scmpb.NewLandingClient(as(t, scmAddr, autoBot)).Land(ctx, &scmpb.LandRequest{RepositoryId: r.GetId(), ActorId: adaID, ExpectedHead: r.GetHead(),
			Items: []*scmpb.LandRequest_Item{{Number: n, Patchset: c.GetCurrentPatchset()}}}); err != nil {
			t.Fatalf("landing CL %d: %v", n, err)
		}
		r, _ = repos.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: r.GetId()}})
		jjRun("infra", "sync")
		jjRun("infra", "new", "main@ok")
		return r.GetHead()
	}
	os.MkdirAll(home, 0o755)
	jjRun(".", "create", "infra", "infra", "--real")
	write(fixture)
	first := land("the tree's repository")
	repo, err := scmpb.NewRepositoriesClient(scmAda).GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: "infra"}})
	if err != nil {
		t.Fatal(err)
	}

	// The deployer, as its robot, in a real checkout.
	deployerSCM := as(t, scmAddr, deployer)
	src := checkout.SCM(scmpb.NewStoreClient(deployerSCM))
	co, err := checkout.NewReal(filepath.Join(state, "repo"), src)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { exec.Command(bazel, "--output_base="+filepath.Join(state, "bazel"), "shutdown").Run() })
	cluster := kubesim.New(t)
	d, err := role.Build(ctx, cfg, role.Options{Dev: true, Color: roll.Blue, Checkout: co, Source: src, Bazel: bazel,
		Env:  []string{"PATH=" + os.Getenv("PATH"), "HOME=" + filepath.Join(state, "home"), "APP_DATABASE_URL=postgres://app.example.com/app"},
		Pool: open(role.Schema), Kube: cluster.Kube, Identity: as(t, idAddr, deployer), SCM: deployerSCM, Automation: as(t, autoAddr, deployer),
		Look: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	deployAddr := serveRole(t, ctx, d.Root, serve.WithAuthn(d.Resolver))
	deploys := deploypb.NewDeploymentsClient(as(t, deployAddr, control))
	tree := identitypb.NewTreeClient(as(t, idAddr, deployer))

	settled := func(what string, cond func(map[string]*deploypb.Deployment) bool) map[string]*deploypb.Deployment {
		t.Helper()
		deadline := time.Now().Add(5 * time.Minute)
		for {
			resp, err := deploys.ListDeployments(ctx, &deploypb.ListDeploymentsRequest{RepositoryId: repo.GetId(), Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			byName := map[string]*deploypb.Deployment{}
			for _, dep := range resp.GetDeployments() {
				if old := byName[dep.GetName()]; old == nil || dep.GetCreatedAt().AsTime().After(old.GetCreatedAt().AsTime()) {
					byName[dep.GetName()] = dep
				}
			}
			if cond(byName) {
				return byName
			}
			if time.Now().After(deadline) {
				t.Fatalf("never: %s: %v", what, resp.GetDeployments())
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	done := func(names ...string) func(map[string]*deploypb.Deployment) bool {
		return func(m map[string]*deploypb.Deployment) bool {
			for _, n := range names {
				if dep := m[n]; dep == nil || dep.GetStatus() <= deploypb.Deployment_STATUS_RUNNING {
					return false
				}
			}
			return true
		}
	}
	got := settled("the first plan's deployments", done("data", "identity"))
	for _, n := range []string{"data", "identity"} {
		dep, err := deploys.GetDeployment(ctx, &deploypb.GetDeploymentRequest{Id: got[n].GetId()})
		if err != nil {
			t.Fatal(err)
		}
		if dep.GetStatus() != deploypb.Deployment_STATUS_SUCCEEDED {
			var log []string
			for _, ev := range dep.GetEvents() {
				log = append(log, ev.GetMessage())
			}
			t.Fatalf("%s: %s (%s)\n%s\nmigrations %v", n, dep.GetStatus(), dep.GetReason(), strings.Join(log, "\n"), dep.GetMigrations())
		}
	}
	m := got["data"].GetMigrations()
	if len(m) != 1 || m[0].GetStatus() != deploypb.Migration_STATUS_APPLIED || !strings.Contains(m[0].GetOutput(), "migrating postgres://app.example.com/app") ||
		m[0].GetPlan().GetDatabase() != "app" || len(m[0].GetPlan().GetSteps()) != 1 {
		t.Errorf("data's migration: %v", m)
	}
	applied, err := tree.GetApplied(ctx, &identitypb.GetAppliedRequest{})
	if err != nil || applied.GetCommit() != hex.EncodeToString(first) {
		t.Fatalf("the applied tree: %v (%v), want %x", applied, err, first)
	}
	if set, err := identitypb.NewDirectoryClient(as(t, idAddr, ada)).GetSet(ctx, &identitypb.GetSetRequest{Name: "eng.acl.example.com"}); err != nil {
		t.Errorf("eng.acl.example.com after the apply: %v, %v", set, err)
	}

	// A change to the tree alone: identity applies it, and data's migration does not run again.
	write(map[string]string{"identity/ops/acl.yaml": "members: [ada.human.example.com]\n"})
	second := land("an ops ACL")
	got = settled("the tree's second deployment", func(m map[string]*deploypb.Deployment) bool {
		return done("identity")(m) && m["identity"].GetId() != got["identity"].GetId()
	})
	if got["identity"].GetStatus() != deploypb.Deployment_STATUS_SUCCEEDED {
		t.Fatalf("the second apply: %v", got["identity"])
	}
	if applied, err = tree.GetApplied(ctx, &identitypb.GetAppliedRequest{}); err != nil || applied.GetCommit() != hex.EncodeToString(second) {
		t.Errorf("the applied tree after the change: %v (%v)", applied, err)
	}
	all, err := deploys.ListDeployments(ctx, &deploypb.ListDeploymentsRequest{RepositoryId: repo.GetId(), Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	datas := 0
	for _, dep := range all.GetDeployments() {
		if dep.GetName() == "data" {
			datas++
		}
	}
	if datas != 1 {
		t.Errorf("data deployed %d times; the second change does not affect it", datas)
	}
}
