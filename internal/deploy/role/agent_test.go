package role_test

import (
	"context"
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

	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/client-go/rest"

	autorole "github.com/Cidan/ok/internal/automation/role"
	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/deploy/agent"
	"github.com/Cidan/ok/internal/deploy/engine"
	"github.com/Cidan/ok/internal/deploy/kubesim"
	"github.com/Cidan/ok/internal/deploy/role"
	"github.com/Cidan/ok/internal/deploy/roll"
	"github.com/Cidan/ok/internal/id/ca"
	idrole "github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/pgdb"
	scmrole "github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/secrets/keys/keystest"
	secretsrole "github.com/Cidan/ok/internal/secrets/role"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/testbazel"
	"github.com/Cidan/ok/internal/testdb/roledb"
	"github.com/Cidan/ok/internal/testjj"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
)

// agentSecrets is how an organization's repository keeps a secret: its settings, and //secret's
// acl.yaml, which gives acme's ops team the folder and its deploy the folder's secrets.
var agentSecrets = map[string]string{
	".ok/repository.yaml": "build: bazel\nsecrets: {}\n",
	"secret/acl.yaml":     "readers: [\"@acme/ops\"]\nwriters: [\"@acme/ops\"]\ndeploys: [\"//.ok/workflow/deploy.yaml:deploy:data\"]\n",
}

// agentFixture is the rest of it: a deploy running one migration, which says what database its
// environment names and reads the secret the deploy's stanza names.
var agentFixture = map[string]string{
	"MODULE.bazel": "module(name = \"shop\")\n",
	"BUILD.bazel":  "",
	".ok/workflow/deploy.yaml": `deploy:
  data:
    migrate: [//db:migrate]
    secrets: [//secret/db-password]
`,
	"rules.bzl": fixture["rules.bzl"],
	"db/BUILD.bazel": `load("//:rules.bzl", "script_binary")

script_binary(
    name = "migrate",
    script = """echo "migrating $APP_DATABASE_URL" >&2
echo "as $(cat "$OK_SECRETS_DIR/db-password")" >&2
echo '{"database":"app","steps":[{"description":"0001_create","sql":"CREATE TABLE t (id int)"}],"executed":true}'
""",
)
`,
}

const secretsBot = "secrets.ok.robot.example.com"

// A hosted deployer sends an organization's deployments to its deploy agent (docs/hosted.md,
// "Deploy agents, as built"): before the agent connects, the deployment waits, saying so; once it
// connects, the deployment's migration runs at the agent — checked out from the SCM as the agent's
// robot, built with real Bazel, in the agent's own environment, the deployer's never seen, with the
// secret its deploy names (docs/secrets.md, "Readers that are not people"), which ok secrets gave
// the deployer for that commit of main, never in what the migration printed — and the deployment
// succeeds.
func TestDeployThroughAnAgent(t *testing.T) {
	jj := testjj.Path(t)
	bazel := testbazel.Bazel(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	cfg, err := config.Parse([]byte(`
domain: example.com
public_url: http://127.0.0.1:1
roles: {identity: identity.ok, scm: scm.ok, control: control.ok, automation: automation.ok, worker: worker.ok, deploy: deploy.ok, secrets: secrets.ok}
tenancy: {mode: hosted}
secrets: {root: "` + keystest.Keyring(t, 1) + `"}
identity:
  ca_dir: ` + filepath.Join(dir, "ca") + `
deploy:
  state: ` + state + `
  checkout: real
  self: {namespace: ok, deployment: ok-deploy}
`))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Bus = bustest.Config(t)
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
	// A hosted service's people sign up: ada has.
	adaP, err := id.Store.CreateAccount(ctx, "ada", "ada@example.com", "github", "gh-ada")
	if err != nil {
		t.Fatal(err)
	}
	adaID := adaP.ID
	// ok secrets listens first: the SCM asks it about uploads' secrets, and it asks the SCM in turn.
	secLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	secAddr := secLis.Addr().String()
	scm, err := scmrole.Build(ctx, cfg, scmrole.Options{Dev: true, Pool: open(scmrole.Schema), Blobs: bucket(t), Identity: as(t, idAddr, scmBot),
		Secrets: as(t, secAddr, scmBot)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scm.Close)
	scmAddr := serveRole(t, ctx, scm.Root, serve.WithAuthn(scm.Resolver))
	sec, err := secretsrole.Build(ctx, cfg, secretsrole.Options{Dev: true, Pool: open(secretsrole.Schema), Identity: as(t, idAddr, secretsBot),
		SCM: as(t, scmAddr, secretsBot)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sec.Close)
	go func() { _ = serve.Run(ctx, sec.Root, serve.WithAuthn(sec.Resolver), serve.WithListener(secLis)) }()
	auto, err := autorole.Build(ctx, cfg, autorole.Options{Dev: true, Pool: open(autorole.Schema), Logs: bucket(t),
		Identity: as(t, idAddr, autoBot), SCM: as(t, scmAddr, autoBot)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(auto.Close)
	autoAddr := serveRole(t, ctx, auto.Root, serve.WithAuthn(auto.Resolver))

	// ada makes acme, its robot agent — designated its deploy agent, and granted read on its
	// repository — and acme/shop.
	tenancy := identitypb.NewTenancyClient(as(t, idAddr, ada))
	if _, err := tenancy.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	robot, err := tenancy.CreateOrganizationRobot(ctx, &identitypb.CreateOrganizationRobotRequest{Handle: "acme", Name: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := identitypb.NewDeployAgentsClient(as(t, idAddr, ada)).SetDeployAgent(ctx,
		&identitypb.SetDeployAgentRequest{Handle: "acme", RobotId: robot.GetPrincipal().GetId()}); err != nil {
		t.Fatal(err)
	}
	// acme's ops team, ada in it, which reads and writes the shop's secrets.
	ops, err := tenancy.CreateTeam(ctx, &identitypb.CreateTeamRequest{Handle: "acme", Team: "ops"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tenancy.AddTeamMember(ctx, &identitypb.TeamMemberRequest{Team: ops.GetName(), PrincipalId: adaID}); err != nil {
		t.Fatal(err)
	}
	scmAda := as(t, scmAddr, ada)
	repos := scmpb.NewRepositoriesClient(scmAda)
	repo, err := repos.CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: "acme/shop"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tenancy.SetGrant(ctx, &identitypb.SetGrantRequest{Id: repo.GetId(),
		Grant: &identitypb.RepositoryGrant{Subject: robot.GetPrincipal().GetId(), Role: "read"}}); err != nil {
		t.Fatal(err)
	}
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
	writeFiles := func(files map[string]string) {
		t.Helper()
		for p, c := range files {
			full := filepath.Join(home, "shop", filepath.FromSlash(p))
			os.MkdirAll(filepath.Dir(full), 0o755)
			if err := os.WriteFile(full, []byte(c), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	land := func(msg string) {
		t.Helper()
		jjRun("shop", "describe", "-m", msg)
		m := regexp.MustCompile(`CL (\d+) (?:created|patchset)`).FindStringSubmatch(jjRun("shop", "cl", "--ready"))
		if m == nil {
			t.Fatal("jj cl uploaded nothing")
		}
		n, _ := strconv.ParseInt(m[1], 10, 64)
		c, err := scmpb.NewChangeListsClient(scmAda).GetChangeList(ctx, &scmpb.GetChangeListRequest{RepositoryId: repo.GetId(), Number: n})
		if err != nil {
			t.Fatal(err)
		}
		head, err := repos.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo.GetId()}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := scmpb.NewLandingClient(as(t, scmAddr, autoBot)).Land(ctx, &scmpb.LandRequest{RepositoryId: repo.GetId(), ActorId: adaID,
			ExpectedHead: head.GetHead(), Items: []*scmpb.LandRequest_Item{{Number: n, Patchset: c.GetCurrentPatchset()}}}); err != nil {
			t.Fatalf("landing CL %d: %v", n, err)
		}
	}
	jjRun(".", "clone", "acme/shop", "shop", "--real")
	// The shop keeps secrets; then ada, of acme's ops, writes its database's password, and its
	// pointer lands with the deploy that names it.
	writeFiles(agentSecrets)
	land("the shop keeps secrets")
	const password = "hunter2-the-shops-password"
	written, err := secretspb.NewSecretsClient(as(t, secAddr, ada)).WriteSecret(ctx, &secretspb.WriteSecretRequest{RepositoryId: repo.GetId(),
		Path: "secret/db-password", Value: []byte(password)})
	if err != nil {
		t.Fatalf("writing the password: %v", err)
	}
	jjRun("shop", "sync")
	jjRun("shop", "new", "main@ok")
	writeFiles(agentFixture)
	writeFiles(map[string]string{"secret/db-password": string(written.GetPointer())})
	land("the shop's database")

	// The deployer, whose own environment names another database, and its own cluster.
	deployerSCM := as(t, scmAddr, deployer)
	src := checkout.SCM(scmpb.NewStoreClient(deployerSCM))
	co, err := checkout.NewReal(filepath.Join(state, "repo"), src)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { exec.Command(bazel, "--output_base="+filepath.Join(state, "bazel"), "shutdown").Run() })
	d, err := role.Build(ctx, cfg, role.Options{Dev: true, Color: roll.Blue, Checkout: co, Source: src, Bazel: bazel,
		Env:  []string{"PATH=" + os.Getenv("PATH"), "HOME=" + filepath.Join(state, "home"), "APP_DATABASE_URL=postgres://deployer.example.com/app"},
		Pool: open(role.Schema), Kube: kubesim.New(t).Kube, Identity: as(t, idAddr, deployer), SCM: deployerSCM, Automation: as(t, autoAddr, deployer),
		Secrets:  as(t, secAddr, deployer),
		Interval: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	deployAddr := serveRole(t, ctx, d.Root, serve.WithAuthn(d.Resolver))
	deploys := deploypb.NewDeploymentsClient(as(t, deployAddr, control))
	data := func() *deploypb.Deployment {
		t.Helper()
		resp, err := deploys.ListDeployments(ctx, &deploypb.ListDeploymentsRequest{RepositoryId: repo.GetId(), Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		for _, dep := range resp.GetDeployments() {
			if dep.GetName() == "data" {
				full, err := deploys.GetDeployment(ctx, &deploypb.GetDeploymentRequest{Id: dep.GetId()})
				if err != nil {
					t.Fatal(err)
				}
				return full
			}
		}
		return nil
	}
	logOf := func(dep *deploypb.Deployment) string {
		var out []string
		for _, ev := range dep.GetEvents() {
			out = append(out, ev.GetMessage())
		}
		return strings.Join(out, "\n")
	}
	waitFor := func(what string, cond func(*deploypb.Deployment) bool) *deploypb.Deployment {
		t.Helper()
		deadline := time.Now().Add(5 * time.Minute)
		for {
			dep := data()
			if dep != nil && cond(dep) {
				return dep
			}
			if time.Now().After(deadline) {
				if dep != nil {
					t.Fatalf("never: %s: %s (%s)\n%s", what, dep.GetStatus(), dep.GetReason(), logOf(dep))
				}
				t.Fatalf("never: %s: no deployment", what)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	waitFor("the deployment to wait for acme's agent", func(dep *deploypb.Deployment) bool {
		return strings.Contains(logOf(dep), "waiting: the organization's deploy agent is not connected")
	})
	if dep := data(); dep.GetStatus() != deploypb.Deployment_STATUS_RUNNING {
		t.Fatalf("the deployment while acme's agent is away: %s (%s)", dep.GetStatus(), dep.GetReason())
	}

	// acme's agent connects, as its robot, its own environment naming its database.
	agentName := robot.GetPrincipal().GetName()
	agentSCM := as(t, scmAddr, agentName)
	agentState := t.TempDir()
	agentSrc := checkout.SCM(scmpb.NewStoreClient(agentSCM))
	agentCo, err := checkout.NewReal(filepath.Join(agentState, "repo"), agentSrc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { exec.Command(bazel, "--output_base="+filepath.Join(agentState, "bazel"), "shutdown").Run() })
	migrations := &agent.Migrations{Identity: identitypb.NewDeployAgentsClient(as(t, idAddr, agentName)), Checkout: agentCo,
		SCM: struct {
			scmpb.RepositoriesClient
			scmpb.BrowseClient
		}{scmpb.NewRepositoriesClient(agentSCM), scmpb.NewBrowseClient(agentSCM)},
		Bazel: &engine.Bazel{Path: bazel, OutputBase: filepath.Join(agentState, "bazel"), Checkout: agentCo, Source: agentSrc,
			Env: []string{"PATH=" + os.Getenv("PATH"), "HOME=" + filepath.Join(agentState, "home"), "APP_DATABASE_URL=postgres://acme.internal/shop"}}}
	a := &agent.Agent{Kube: &rest.Config{Host: "https://127.0.0.1:1"}, Migrate: migrations.Run}
	actx, disconnect := context.WithCancel(ctx)
	t.Cleanup(disconnect)
	stream, err := deploypb.NewAgentsClient(as(t, deployAddr, agentName)).Connect(actx)
	if err != nil {
		t.Fatal(err)
	}
	go a.Serve(actx, stream)

	dep := waitFor("the deployment to finish", func(dep *deploypb.Deployment) bool { return dep.GetStatus() > deploypb.Deployment_STATUS_RUNNING })
	if dep.GetStatus() != deploypb.Deployment_STATUS_SUCCEEDED {
		t.Fatalf("the deployment: %s (%s)\n%s", dep.GetStatus(), dep.GetReason(), logOf(dep))
	}
	ms := dep.GetMigrations()
	if len(ms) != 1 || ms[0].GetStatus() != deploypb.Migration_STATUS_APPLIED || !strings.Contains(ms[0].GetOutput(), "migrating postgres://acme.internal/shop") ||
		strings.Contains(ms[0].GetOutput(), "deployer.example.com") || ms[0].GetPlan().GetDatabase() != "app" {
		t.Errorf("the migration: %v", ms)
	}
	if out := ms[0].GetOutput(); !strings.Contains(out, "as [secret //secret/db-password]") || strings.Contains(out, password) {
		t.Errorf("the migration's output, which read the password: %q", out)
	}
	if !strings.Contains(logOf(dep), "its migrations read 1 secret(s): //secret/db-password") {
		t.Errorf("the log does not say the migration read the password:\n%s", logOf(dep))
	}
	if !strings.Contains(logOf(dep), "applying //db:migrate, in its cluster by its organization's deploy agent") {
		t.Errorf("the log does not say the agent ran it:\n%s", logOf(dep))
	}

	// The agent runs no commit that is not on main — a CL's, unlanded — and no other
	// organization's repository.
	jjRun("shop", "sync")
	jjRun("shop", "new", "main@ok", "-m", "unreviewed")
	if err := os.WriteFile(filepath.Join(home, "shop", "db", "extra.txt"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`CL (\d+) (?:created|patchset)`).FindStringSubmatch(jjRun("shop", "cl"))
	if m == nil {
		t.Fatal("jj cl uploaded nothing")
	}
	n2, _ := strconv.ParseInt(m[1], 10, 64)
	c2, err := scmpb.NewChangeListsClient(scmAda).GetChangeList(ctx, &scmpb.GetChangeListRequest{RepositoryId: repo.GetId(), Number: n2})
	if err != nil {
		t.Fatal(err)
	}
	unlanded := c2.GetPatchsets()[len(c2.GetPatchsets())-1].GetCommitId()
	if res := migrations.Run(ctx, &deploypb.MigrationRequest{RepositoryId: repo.GetId(), CommitId: unlanded, Target: "//db:migrate"}); !strings.Contains(res.GetFailed(), "is not on acme/shop's main") {
		t.Errorf("a commit not on main: %v", res)
	}
	mine, err := repos.CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: "ada/notes"})
	if err != nil {
		t.Fatal(err)
	}
	if res := migrations.Run(ctx, &deploypb.MigrationRequest{RepositoryId: mine.GetId(), CommitId: mine.GetHead(), Target: "//db:migrate"}); !strings.Contains(res.GetFailed(), "not this agent's organization's") {
		t.Errorf("another's repository: %v", res)
	}
}
