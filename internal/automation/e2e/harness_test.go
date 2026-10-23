// Package e2e runs ok automation end to end: ok identity, the SCM, and ok automation in process,
// each as its robot; workers in process running real Bazel on real checkouts of commits ok's jj
// put in the SCM; and ok control's part — submitting — played by the test, as control's robot.
//
// It needs PostgreSQL and S3 (internal/testdb), Bazel and bubblewrap (internal/testbazel), and ok's
// jj (internal/testjj). The fixtures are Bazel workspaces of Starlark rules
// alone, so Bazel downloads nothing.
package e2e

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

	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/work"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/automation/role"
	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/automation/worker"
	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/ca"
	idrole "github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/pgdb"
	scmrole "github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/secrets/keys/keystest"
	secretsrole "github.com/Cidan/ok/internal/secrets/role"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/testbazel"
	"github.com/Cidan/ok/internal/testdb/roledb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
	"github.com/Cidan/ok/internal/testjj"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
)

const (
	ada        = "ada.human.example.com"
	control    = "control.ok.robot.example.com"
	automation = "automation.ok.robot.example.com"
	workerBot  = "worker.ok.robot.example.com"
	scmBot     = "scm.ok.robot.example.com"
	secretsBot = "secrets.ok.robot.example.com"
	deployBot  = "deploy.ok.robot.example.com"
)

// env is one test's ok: its roles' addresses, and clients as the roles that call them.
type env struct {
	t    *testing.T
	ctx  context.Context
	scm  string // the SCM's address, host:port
	auto string // ok automation's
	id   string // ok identity's
	// secrets is ok secrets' address, when the test runs it (startWithSecrets): "" otherwise.
	secrets string
	adaID   string // ada's stable ID, as ok identity minted it
	runs    automationpb.RunsClient
	// automation is ok automation's store, which a test reaches past its services: to hold a
	// repository's landing queue.
	automation *store.Store
	jj         string
	bazel      string
	bwrap      string
	// state holds the workers' slots, output bases, and kept graphs, as one machine's disk.
	state string
}

// serveRole runs a role's root on a loopback port until the test ends.
func serveRole(t *testing.T, ctx context.Context, root node.Node, opts ...serve.Option) string {
	t.Helper()
	return serveOn(t, ctx, listen(t), root, opts...)
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return lis
}

// serveOn runs a role's root on lis until the test ends.
func serveOn(t *testing.T, ctx context.Context, lis net.Listener, root node.Node, opts ...serve.Option) string {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, root, append(opts, serve.WithListener(lis))...) }()
	t.Cleanup(func() { cancel(); <-done })
	return lis.Addr().String()
}

// as is a connection to a role on loopback, naming principal: development mode's caller.
func as(t *testing.T, addr, principal string) *grpc.ClientConn {
	t.Helper()
	c, err := dial.Dial(addr, dial.Options{DevPrincipal: principal})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// bucket is a new bucket of the test's S3 server.
func bucket(t *testing.T) blob.Store {
	t.Helper()
	return testobjects.Store(t)
}

// start runs ok identity, the SCM, and ok automation for a test.
func start(t *testing.T, opts ...func(*config.Config)) *env {
	t.Helper()
	return startAll(t, false, opts...)
}

// startWithSecrets runs ok secrets beside them, under a keyring (docs/secrets.md), its workers
// reaching it; ok identity's tree declares ops (ada).
func startWithSecrets(t *testing.T, opts ...func(*config.Config)) *env {
	t.Helper()
	return startAll(t, true, opts...)
}

func startAll(t *testing.T, withSecrets bool, opts ...func(*config.Config)) *env {
	t.Helper()
	jj := testjj.Path(t)
	e := &env{t: t, jj: jj, bazel: testbazel.Bazel(t), bwrap: testbazel.Bwrap(t), state: t.TempDir()}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	e.ctx = ctx
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "people.yaml"), []byte("people:\n  - {id: \"1\", email: ada@example.com, name: Ada}\n"), 0o644)
	text := `
domain: example.com
public_url: http://127.0.0.1:1
roles: {identity: identity.ok, scm: scm.ok, control: control.ok, automation: automation.ok, worker: worker.ok, deploy: deploy.ok, secrets: secrets.ok}
identity:
  ca_dir: ` + filepath.Join(dir, "ca") + `
  system_of_record: {static: {file: ` + filepath.Join(dir, "people.yaml") + `}}
`
	if withSecrets {
		text += "secrets: {root: \"" + keystest.Keyring(t, 1) + "\"}\n"
	}
	cfg, err := config.Parse([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Bus = bustest.Config(t)
	for _, o := range opts {
		o(cfg)
	}
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
	e.id = idAddr
	for i := 0; ; i++ {
		if p, err := id.Store.Principal(ctx, ada); err == nil {
			e.adaID = p.ID
			break
		}
		if i == 200 {
			t.Fatal("the sync never made ada")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// ok secrets listens first: the SCM asks it about uploads' secrets, and it asks the SCM and ok
	// automation in turn.
	var secLis net.Listener
	var scmSecrets grpc.ClientConnInterface
	if withSecrets {
		secLis = listen(t)
		scmSecrets = as(t, secLis.Addr().String(), scmBot)
	}
	scm, err := scmrole.Build(ctx, cfg, scmrole.Options{Dev: true, Pool: open(scmrole.Schema), Blobs: bucket(t), Identity: as(t, idAddr, scmBot),
		Secrets: scmSecrets})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scm.Close)
	e.scm = serveRole(t, ctx, scm.Root, serve.WithAuthn(scm.Resolver))
	auto, err := role.Build(ctx, cfg, role.Options{Dev: true, Pool: open(role.Schema), Logs: bucket(t),
		Identity: as(t, idAddr, automation), SCM: as(t, e.scm, automation)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(auto.Close)
	e.automation = auto.Store
	e.auto = serveRole(t, ctx, auto.Root, serve.WithAuthn(auto.Resolver))
	e.runs = automationpb.NewRunsClient(as(t, e.auto, control))
	if withSecrets {
		sec, err := secretsrole.Build(ctx, cfg, secretsrole.Options{Dev: true, Pool: open(secretsrole.Schema), Identity: as(t, idAddr, secretsBot),
			SCM: as(t, e.scm, secretsBot), Automation: as(t, e.auto, secretsBot)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(sec.Close)
		e.secrets = serveOn(t, ctx, secLis, sec.Root, serve.WithAuthn(sec.Resolver))
		file := func(path, members string) *identitypb.File {
			return &identitypb.File{Path: path, Content: []byte("members: [" + members + "]\n")}
		}
		if _, err := identitypb.NewTreeClient(as(t, idAddr, deployBot)).Apply(ctx, &identitypb.ApplyRequest{Commit: "seed", Sequence: 1,
			Files: []*identitypb.File{file("acl.yaml", ada), file("ops/acl.yaml", ada)}}); err != nil {
			t.Fatal(err)
		}
	}
	return e
}

// runningWorker is a worker a test started: stop gives its runs back and ends it; ready says
// whether ok automation has answered it lately, which its health reports.
type runningWorker struct {
	stop    func()
	contact *worker.Contact
}

func (w *runningWorker) ready() bool { return w.contact.Within(time.Now(), worker.ReadyWithin) }

// startWorker runs a worker with slots until the test ends, or it is stopped: real checkouts under
// the test's worker state, Bazel and bubblewrap as found, taking runs from automation at auto.
func (e *env) startWorker(name string, slots int) *runningWorker {
	return e.startWorkerAt(name, slots, e.auto)
}

func (e *env) startWorkerAt(name string, slots int, auto string) *runningWorker {
	t := e.t
	t.Helper()
	scm := as(t, e.scm, workerBot)
	browse := scmpb.NewBrowseClient(scm)
	src := checkout.SCM(browse, scmpb.NewStoreClient(scm))
	var checkouts, bases []checkout.Checkout
	for i := 1; i <= slots; i++ {
		for _, suffix := range []string{"", "-base"} {
			co, err := checkout.NewReal(filepath.Join(e.state, name, "slots", strconv.Itoa(i)+suffix), src)
			if err != nil {
				t.Fatal(err)
			}
			if suffix == "" {
				checkouts = append(checkouts, co)
			} else {
				bases = append(bases, co)
			}
		}
	}
	home := filepath.Join(e.state, "home")
	os.MkdirAll(home, 0o755)
	// A secret as a worker's configuration names one, so every run's Bazel works as an
	// installation's does: out of sight of it (worker.HideSecrets), one server a slot's runs share.
	secret := filepath.Join(e.state, name, "secrets", "token")
	os.MkdirAll(filepath.Dir(secret), 0o700)
	os.WriteFile(secret, []byte("a worker's token\n"), 0o600)
	cfg := worker.Config{
		Name:        name,
		Work:        automationpb.NewWorkClient(as(t, auto, workerBot)),
		Automation:  as(t, auto, workerBot),
		Checkouts:   checkouts,
		Bases:       bases,
		Source:      src,
		Browse:      browse,
		Bazel:       e.bazel,
		Bwrap:       e.bwrap,
		Secrets:     []string{filepath.Dir(secret)},
		Names:       identity.MustNames("example.com"),
		Directory:   identitypb.NewDirectoryClient(as(t, e.id, workerBot)),
		Env:         []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "LANG=C.UTF-8", "TZ=UTC"},
		Scratch:     filepath.Join(e.state, name, "scratch"),
		OutputBases: filepath.Join(e.state, name, "bazel"),
		Graphs:      filepath.Join(e.state, name, "graphs"),
		Grace:       10 * time.Second,
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Contact:     &worker.Contact{},
	}
	if e.secrets != "" {
		cfg.RunSecrets = secretspb.NewRunSecretsClient(as(t, e.secrets, workerBot))
	}
	ctx, cancel := context.WithCancel(e.ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx, cfg) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-done; err != nil {
			t.Errorf("worker %s: %v", name, err)
		}
	}
	t.Cleanup(func() {
		stop()
		// Each slot's Bazel servers go with the worker.
		for i := 1; i <= slots; i++ {
			for _, suffix := range []string{"", "-base"} {
				exec.Command(e.bazel, "--output_base="+filepath.Join(cfg.OutputBases, strconv.Itoa(i)+suffix), "shutdown").Run()
			}
		}
	})
	return &runningWorker{stop: stop, contact: cfg.Contact}
}

// person is someone with a home of their own, uploading with jj.
type person struct {
	e         *env
	home      string
	principal string
}

func (e *env) person(principal string) *person {
	return &person{e: e, home: e.t.TempDir(), principal: principal}
}

func (p *person) dir(d string) string { return filepath.Join(p.home, d) }

// jj runs jj in dir — a clone or create a real checkout — requiring it to succeed.
func (p *person) jj(dir string, args ...string) string {
	p.e.t.Helper()
	if len(args) > 0 && (args[0] == "clone" || args[0] == "create") {
		args = append(args[:len(args):len(args)], "--real")
	}
	handle := strings.SplitN(p.principal, ".", 2)[0]
	os.MkdirAll(p.dir(dir), 0o755)
	cmd := exec.Command(p.e.jj, append([]string{"--no-pager", "--color=never", "--config", "ok.endpoint=http://" + p.e.scm}, args...)...)
	cmd.Dir = p.dir(dir)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + p.home,
		"OK_DEV_PRINCIPAL=" + p.principal,
		"JJ_USER=" + handle,
		"JJ_EMAIL=" + handle + "@example.com",
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		p.e.t.Fatalf("jj %v as %s: %v\n%s", args, p.principal, err, out)
	}
	return string(out)
}

// write writes files under dir; nil removes one.
func write(t *testing.T, dir string, files map[string]*string) {
	t.Helper()
	for rel, body := range files {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if body == nil {
			os.Remove(path)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(*body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func ptr(s string) *string { return &s }

// uploaded reads the CL numbers jj cl printed, in order.
func uploaded(out string) []int64 {
	var ns []int64
	for _, m := range regexp.MustCompile(`CL (\d+) (?:created|patchset \d+|unchanged)`).FindAllStringSubmatch(out, -1) {
		n, _ := strconv.ParseInt(m[1], 10, 64)
		ns = append(ns, n)
	}
	return ns
}

// repository is a repository's ID, by name.
func (e *env) repository(name string) int64 {
	e.t.Helper()
	r, err := scmpb.NewRepositoriesClient(as(e.t, e.scm, ada)).GetRepository(e.ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: name}})
	if err != nil {
		e.t.Fatal(err)
	}
	return r.GetId()
}

// cl is a CL as the SCM has it.
func (e *env) cl(repo, number int64) *scmpb.ChangeList {
	e.t.Helper()
	c, err := scmpb.NewChangeListsClient(as(e.t, e.scm, ada)).GetChangeList(e.ctx, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: number})
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

// parentsOf reads a commit's parents, as ada.
func (e *env) parentsOf(repo int64, commit []byte) [][]byte {
	e.t.Helper()
	resp, err := scmpb.NewBrowseClient(as(e.t, e.scm, ada)).GetCommits(e.ctx, &scmpb.GetCommitsRequest{RepositoryId: repo,
		CommitIds: [][]byte{commit}})
	if err != nil || len(resp.GetCommits()) != 1 {
		e.t.Fatalf("reading commit %x: %v, %v", commit, resp, err)
	}
	return resp.GetCommits()[0].GetParents()
}

// land lands a CL's patchset on main, as the landing queue does once post passes: a test's base,
// landed without post.
func (e *env) land(repo, cl int64, ps int32) {
	e.t.Helper()
	conn := as(e.t, e.scm, automation)
	r, err := scmpb.NewRepositoriesClient(conn).GetRepository(e.ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}})
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := scmpb.NewLandingClient(conn).Land(e.ctx, &scmpb.LandRequest{RepositoryId: repo, ActorId: e.adaID, ExpectedHead: r.GetHead(),
		Items: []*scmpb.LandRequest_Item{{Number: cl, Patchset: ps}}}); err != nil {
		e.t.Fatalf("landing CL %d: %v", cl, err)
	}
}

// logOf is a run's whole log.
func (e *env) logOf(id int64) string {
	e.t.Helper()
	stream, err := e.runs.ReadLog(e.ctx, &automationpb.ReadLogRequest{Id: id})
	if err != nil {
		e.t.Fatal(err)
	}
	var b strings.Builder
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return b.String()
		}
		if err != nil {
			e.t.Fatal(err)
		}
		b.Write(chunk.GetData())
	}
}

// pre is a patchset's pre, as control reads it.
func (e *env) pre(repo, cl int64, ps int32) *automationpb.Pre {
	e.t.Helper()
	p, err := e.runs.GetPre(e.ctx, &automationpb.GetPreRequest{RepositoryId: repo, ClNumber: cl, Patchset: ps})
	if err != nil {
		e.t.Fatal(err)
	}
	return p
}

// settled waits for a patchset's pre to pass or fail and every run of it to finish.
func (e *env) settled(repo, cl int64, ps int32) *automationpb.Pre {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Minute)
	for {
		p := e.pre(repo, cl, ps)
		done := p.GetPlan().GetStatus() >= automationpb.Run_STATUS_PASSED
		for _, j := range p.GetJobs() {
			done = done && j.GetStatus() >= automationpb.Run_STATUS_PASSED
		}
		if v := p.GetVerdict(); done && (v == automationpb.Pre_VERDICT_PASSED || v == automationpb.Pre_VERDICT_FAILED) {
			return p
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("CL %d patchset %d's pre did not settle: %v", cl, ps, p)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// describe is a pre and every run's log, for a failure's message.
func (e *env) describe(p *automationpb.Pre) string {
	var b strings.Builder
	b.WriteString(p.GetVerdict().String() + "; plan " + p.GetPlan().GetStatus().String() + " (" + p.GetPlan().GetReason() + ")\n" + e.logOf(p.GetPlan().GetId()) + "\n")
	for _, j := range p.GetJobs() {
		b.WriteString("job " + j.GetOrigin().GetJob() + ": " + j.GetStatus().String() + " (" + j.GetReason() + ")\n" + e.logOf(j.GetId()) + "\n")
	}
	return b.String()
}

// holdQueue holds a repository's landing queue, as a replica running it does: ok automation's
// queue does nothing for it until let go, and runs again within a few seconds after.
func (e *env) holdQueue(repo int64) (letGo func()) {
	e.t.Helper()
	_, release, ok, err := work.Claim(e.ctx, e.automation.Work(), store.QueueWork, repo, "the test", 3*time.Second)
	if err != nil || !ok {
		e.t.Fatalf("holding repository %d's queue: %v, %v", repo, ok, err)
	}
	return release
}
