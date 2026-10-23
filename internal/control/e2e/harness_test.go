// Package e2e runs ok control end to end: ok identity, the SCM, ok automation, and ok control in
// process, each as its own robot, served by ok's runtime — pages and the ChangeLists service on one
// port, each request's person resolved as ok resolves them — with ok's jj as the people uploading.
// A test plays automation's worker, and the deployer (deploy_test.go). Every CL flow in
// docs/changelists.md is driven through control's pages.
//
// It needs PostgreSQL and S3 (internal/testdb) and ok's jj (internal/testjj).
package e2e

import (
	"context"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/url"
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
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/role"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/mail/mailtest"
	idrole "github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/id/seal"
	scmrole "github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/testdb/roledb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
	"github.com/Cidan/ok/internal/testjj"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

const (
	control    = "control.ok.robot.example.com"
	scmSelf    = "scm.ok.robot.example.com"
	automation = "automation.ok.robot.example.com"
	worker     = "worker.ok.robot.example.com"
	deployBot  = "deploy.ok.robot.example.com"
	alice      = "alice.human.example.com"
	bob        = "bob.human.example.com"
	carol      = "carol.human.example.com"
	dana       = "dana.human.example.com" // an administrator: administers every repository
)

// reviewers is the one ACL ok identity's tree holds besides the root's: reviewers.acl.example.com,
// holding bob.
const reviewers = "reviewers.acl.example.com"

// retiredGroup is a group the tree once declared and then deleted, so its name is retired.
const retiredGroup = "old.group.example.com"

// treeRepository is the repository whose tree is ok's (identity_test.go).
const treeRepository = "org"

// env is one test's ok: identity, the SCM, automation, and control.
type env struct {
	t        *testing.T
	jj       string
	endpoint string // the SCM, http://127.0.0.1:<port>
	// control is ok control as it is deployed: its pages and its ChangeLists service on one port —
	// what ok's jj calls (ok.web-url).
	control string
	db      db.Store
	client  *http.Client
	// work is ok automation's side for workers, which a test plays; stopAutomation takes
	// automation away.
	work           automationpb.WorkClient
	stopAutomation func()
	// deployer is ok's deployer, which a test plays (deploy_test.go).
	deployer *deployer
	// dir is ok identity's directory, as ok control asks it.
	dir identitypb.DirectoryClient
	// idAddr is where ok identity serves; mail is the server it mails notifications through.
	idAddr string
	mail   *mailtest.Server
}

// tenancy is ok identity's Tenancy as a person calls it.
func (e *env) tenancy(person string) identitypb.TenancyClient {
	return identitypb.NewTenancyClient(as(e.t, e.idAddr, person))
}

// id is a principal's stable ID, as ok identity minted it: what pages carry in their forms, and
// ok control names principals by to the other roles.
func (e *env) id(name string) string {
	e.t.Helper()
	resp, err := e.dir.GetPrincipal(context.Background(), &identitypb.GetPrincipalRequest{Key: &identitypb.GetPrincipalRequest_Name{Name: name}})
	if err != nil {
		e.t.Fatalf("the ID of %s: %v", name, err)
	}
	return resp.GetPrincipal().GetId()
}

// serveRole runs a role's root on lis, or a loopback port, until the test ends or the returned stop
// is called.
func serveRole(t *testing.T, ctx context.Context, lis net.Listener, root node.Node, opts ...serve.Option) (addr string, stop func()) {
	t.Helper()
	if lis == nil {
		var err error
		if lis, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, root, append(opts, serve.WithListener(lis))...) }()
	stopped := false
	stop = func() {
		if !stopped {
			stopped = true
			cancel()
			<-done
		}
	}
	t.Cleanup(stop)
	return lis.Addr().String(), stop
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

func bucket(t *testing.T) blob.Store {
	t.Helper()
	return testobjects.Store(t)
}

func members(names ...string) string {
	if len(names) == 0 {
		return "members: []\n"
	}
	return "members:\n  - " + strings.Join(names, "\n  - ") + "\n"
}

// newEnv is a test's ok, with a stand-in worker that passes every run automation queues: pre
// passes on each patchset, with a plan that queues no jobs.
func newEnv(t *testing.T) *env {
	t.Helper()
	e := newEnvWithoutWorker(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go e.passEverything(ctx)
	return e
}

// passEverything plays a worker that passes every run it takes, until ctx ends.
func (e *env) passEverything(ctx context.Context) {
	for ctx.Err() == nil {
		resp, err := e.work.Lease(ctx, &automationpb.LeaseRequest{Worker: "stand-in", WaitSeconds: 1})
		if err != nil || resp.GetRun() == nil {
			if err != nil {
				time.Sleep(50 * time.Millisecond)
			}
			continue
		}
		req := &automationpb.FinishRequest{RunId: resp.GetRun().GetId(), Worker: "stand-in", Status: automationpb.Run_STATUS_PASSED}
		if resp.GetRun().GetJob().GetKind() == automationpb.Job_KIND_PLAN {
			req.Plan = &automationpb.Plan{}
		}
		_, _ = e.work.Finish(ctx, req)
	}
}

// submitted posts a CL's Submit (action "submit" or "submit-stack") as principal and waits for
// the landing queue to land it — the stand-in worker passes its post — returning its page.
func (e *env) submitted(principal, path, action string) page {
	e.t.Helper()
	p := e.post(principal, path+"/"+action, nil)
	if n := notice(p); n != "" {
		e.t.Fatalf("submitting %s: %s", path, n)
	}
	e.waitFor(path+" to land", func() bool { return e.landed(path) })
	return e.get(principal, path)
}

// landed reports whether the CL at path (clPath's form) has landed, as ok control records it — and
// so with main moved: the SCM lands a CL and moves main in one transaction.
func (e *env) landed(path string) bool {
	e.t.Helper()
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	n, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if len(parts) != 4 || parts[2] != "cl" || err != nil {
		e.t.Fatalf("%s is not a CL's path", path)
	}
	// The installation's organization's repositories' full names are their names alone.
	repo := parts[1]
	if parts[0] != "example" {
		repo = parts[0] + "/" + parts[1]
	}
	cl, ok, err := dbChangeList(e, repo, n)
	if err != nil {
		e.t.Fatal(err)
	}
	return ok && cl.Status == db.StatusSubmitted
}

// prePassed waits for pre to pass on a CL's current patchset: submit waits for it.
func (e *env) prePassed(repo string, n int64) {
	e.t.Helper()
	e.waitFor(fmt.Sprintf("CL %d's pre to pass", n), func() bool {
		_, verdict, _, _ := checks(e.t, e.get(alice, clPath(repo, n)+"/checks"))
		return verdict == "Passed"
	})
}

// testConfig is a test's ok configuration, ok control's public URL base: its CA, and its people —
// alice, bob, carol, and dana — under a directory of its own.
func testConfig(t *testing.T, base string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	var people strings.Builder
	people.WriteString("people:\n")
	for i, h := range []string{"alice", "bob", "carol", "dana"} {
		fmt.Fprintf(&people, "  - {id: \"%d\", email: %s@example.com, name: %s}\n", i+1, h, strings.ToUpper(h[:1])+h[1:])
	}
	os.WriteFile(filepath.Join(dir, "people.yaml"), []byte(people.String()), 0o644)
	cfg, err := config.Parse([]byte(`
domain: example.com
public_url: ` + base + `
roles: {identity: identity.ok, scm: scm.ok, control: control.ok, automation: automation.ok, worker: worker.ok, deploy: deploy.ok}
identity:
  ca_dir: ` + filepath.Join(dir, "ca") + `
  bootstrap_admins: [dana]
  tree_repository: ` + treeRepository + `
  system_of_record: {static: {file: ` + filepath.Join(dir, "people.yaml") + `}}
`))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Bus = bustest.Config(t)
	return cfg
}

// opener makes a test's roles' databases (roledb, as OK_TEST_DATABASE names them), pointing cfg at
// them, and opens pools on them, each with a role's schema, closed when the test ends: pools that
// fail the test when a goroutine holding a connection asks for another (testdb.NoNestedConns), as
// do the bus's pools the roles open beside them. On Spanner it opens none: each role opens its
// database as cfg says.
func opener(t *testing.T, cfg *config.Config) func(schema string) *pgxpool.Pool {
	dbs := roledb.New(t)
	dbs.Configure(cfg)
	return func(schema string) *pgxpool.Pool {
		t.Helper()
		return dbs.Pool(t, schema, 0)
	}
}

// startIdentity serves ok identity, its people synced, and a tree applied — admins.acl holding
// dana, and the reviewers ACL holding bob — after one that declared the group old, now retired.
// It returns where it serves.
func startIdentity(t *testing.T, ctx context.Context, cfg *config.Config, open func(string) *pgxpool.Pool) string {
	t.Helper()
	id, err := idrole.Build(ctx, cfg, idrole.Options{Dev: true, Pool: open(idrole.Schema), MailLook: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	idAddr, _ := serveRole(t, ctx, nil, id.Root, serve.WithAuthn(id.Resolver))
	seedIdentity(t, ctx, id, idAddr)
	return idAddr
}

// seedIdentity waits for identity's people to be synced, then applies its seed tree.
func seedIdentity(t *testing.T, ctx context.Context, id *idrole.Role, idAddr string) {
	t.Helper()
	for i := 0; ; i++ {
		if _, err := id.Store.Principal(ctx, dana); err == nil {
			break
		}
		if i == 200 {
			t.Fatal("the sync never made the people")
		}
		time.Sleep(50 * time.Millisecond)
	}
	trees := identitypb.NewTreeClient(as(t, idAddr, deployBot))
	for i, files := range []map[string]string{
		{"acl.yaml": members(dana), "reviewers/acl.yaml": members(bob), "old/group.yaml": members(carol)},
		{"acl.yaml": members(dana), "reviewers/acl.yaml": members(bob)},
	} {
		req := &identitypb.ApplyRequest{Commit: fmt.Sprintf("seed%d", i+1), Sequence: int64(i + 1)}
		for p, c := range files {
			req.Files = append(req.Files, &identitypb.File{Path: p, Content: []byte(c)})
		}
		if _, err := trees.Apply(ctx, req); err != nil {
			t.Fatalf("applying the seed tree: %v", err)
		}
	}
}

// newEnvWithoutWorker is a test's ok whose worker the test plays itself.
func newEnvWithoutWorker(t *testing.T) *env {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// ok control's address is its public URL: the links it gives, and the origin its forms and
	// ChangeLists service accept.
	controlLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + controlLis.Addr().String()
	cfg := testConfig(t, base)
	// The directory mails notifications through a mail server of the test's own.
	mail := mailtest.New(t)
	cfg.Identity.Mail = &config.Mail{SMTP: config.SMTP{Address: mail.Addr, From: "ok <no-reply@example.com>"}}
	// Webhooks, whose endpoints in a test are on loopback.
	key := filepath.Join(t.TempDir(), "sealing.key")
	if err := os.WriteFile(key, []byte(seal.NewKey()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.Control.SealingKeyFile, cfg.Control.Webhooks.AllowPrivateNetworks = key, true
	res := &resources{cfg: cfg, open: opener(t, cfg), blobs: bucket(t), logs: bucket(t), seed: true, mail: mail}
	res.deployer, res.deployAddr = startDeployer(t)
	e, _ := startStack(t, ctx, res, controlLis)
	return e
}

// resources are what a test's roles are built on, which a test may stop and build again on
// (movein_test.go): the configuration, the databases, the buckets, the deployer's stand-in, and,
// once built, the addresses each role serves at.
type resources struct {
	cfg             *config.Config
	open            func(string) *pgxpool.Pool
	blobs, logs     blob.Store
	deployer        *deployer
	deployAddr      string
	seed            bool // apply the seed tree once identity serves
	mail            *mailtest.Server
	idAddr, scmAddr string
	autoAddr        string
}

// listen listens at addr — again, after the role that served there stopped — or anywhere.
func listen(t *testing.T, addr string) net.Listener {
	t.Helper()
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	for i := 0; ; i++ {
		lis, err := net.Listen("tcp", addr)
		if err == nil {
			return lis
		}
		if i == 100 {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startStack builds and serves identity, the SCM, ok automation, and ok control on res, ok control
// on controlLis, and returns the test's env, and what stops them all.
func startStack(t *testing.T, ctx context.Context, res *resources, controlLis net.Listener) (*env, func()) {
	t.Helper()
	jj := testjj.Path(t)
	cfg, open := res.cfg, res.open
	var stops []func()
	stopAll := func() {
		for i := len(stops) - 1; i >= 0; i-- {
			stops[i]()
		}
	}
	id, err := idrole.Build(ctx, cfg, idrole.Options{Dev: true, Pool: open(idrole.Schema), MailLook: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	idAddr, stopID := serveRole(t, ctx, listen(t, res.idAddr), id.Root, serve.WithAuthn(id.Resolver))
	stops = append(stops, stopID)
	res.idAddr = idAddr
	if res.seed {
		seedIdentity(t, ctx, id, idAddr)
		res.seed = false
	}

	// The SCM, and ok automation following its change feed.
	scm, err := scmrole.Build(ctx, cfg, scmrole.Options{Dev: true, Pool: open(scmrole.Schema), Blobs: res.blobs, Identity: as(t, idAddr, scmSelf)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scm.Close)
	scmAddr, stopSCM := serveRole(t, ctx, listen(t, res.scmAddr), scm.Root, serve.WithAuthn(scm.Resolver))
	stops = append(stops, stopSCM)
	res.scmAddr = scmAddr
	auto, err := autorole.Build(ctx, cfg, autorole.Options{Dev: true, Pool: open(autorole.Schema), Logs: res.logs,
		Identity: as(t, idAddr, automation), SCM: as(t, scmAddr, automation)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(auto.Close)
	autoAddr, stopAutomation := serveRole(t, ctx, listen(t, res.autoAddr), auto.Root, serve.WithAuthn(auto.Resolver))
	stops = append(stops, stopAutomation)
	res.autoAddr = autoAddr

	// ok control, as its robot, with the deployer's stand-in.
	ctl, err := role.Build(ctx, cfg, role.Options{Dev: true, Pool: open(role.Schema), Identity: as(t, idAddr, control), SCM: as(t, scmAddr, control),
		Automation: as(t, autoAddr, control), Deploy: as(t, res.deployAddr, control), Poll: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ctl.Close)
	_, stopControl := serveRole(t, ctx, controlLis, ctl.Root, serve.WithAuthn(ctl.Resolver))
	stops = append(stops, stopControl)
	base := "http://" + controlLis.Addr().String()
	return &env{
		mail: res.mail,
		t:    t, jj: jj, endpoint: "http://" + scmAddr, control: base, db: ctl.DB, deployer: res.deployer, dir: identitypb.NewDirectoryClient(as(t, idAddr, control)),
		idAddr: idAddr, work: automationpb.NewWorkClient(as(t, autoAddr, worker)), stopAutomation: stopAutomation,
		client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}, stopAll
}

// page is one response: its status, its body with HTML entities decoded, and its headers.
type page struct {
	status int
	body   string
	header http.Header
}

func (p page) has(s string) bool { return strings.Contains(p.body, s) }

// do makes a request of ok control as principal (none: no one signed in).
func (e *env) do(principal, method, path string, form url.Values, header map[string]string) page {
	e.t.Helper()
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequest(method, e.control+path, body)
	if err != nil {
		e.t.Fatal(err)
	}
	if principal != "" {
		req.Header.Set(authn.DevPrincipalKey, principal)
	}
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		e.t.Fatal(err)
	}
	return page{status: resp.StatusCode, body: html.UnescapeString(string(b)), header: resp.Header}
}

// get fetches a page as principal, requiring it to succeed.
func (e *env) get(principal, path string) page {
	e.t.Helper()
	p := e.do(principal, http.MethodGet, path, nil, nil)
	if p.status != http.StatusOK {
		e.t.Fatalf("GET %s as %s: %d\n%s", path, principal, p.status, excerpt(p.body))
	}
	return p
}

// post posts a form as principal, requiring the page it answers with to render.
func (e *env) post(principal, path string, form url.Values) page {
	e.t.Helper()
	if form == nil {
		form = url.Values{}
	}
	p := e.do(principal, http.MethodPost, path, form, nil)
	if p.status != http.StatusOK && p.status != http.StatusSeeOther && p.status != http.StatusNoContent {
		e.t.Fatalf("POST %s as %s: %d\n%s", path, principal, p.status, excerpt(p.body))
	}
	return p
}

// excerpt is a page's text, tags stripped, for failure messages.
func excerpt(body string) string {
	text := regexp.MustCompile(`(?s)<script.*?</script>|<style.*?</style>|<[^>]+>`).ReplaceAllString(body, " ")
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 4000 {
		text = text[:4000] + "…"
	}
	return text
}

// notice is the notice banner a CL action answered with ("" when none).
func notice(p page) string {
	m := regexp.MustCompile(`(?s)<div class="mb-4 rounded-lg border border-amber-700/50[^"]*">(.*?)</div>`).FindStringSubmatch(p.body)
	if m == nil {
		return ""
	}
	return strings.TrimSpace(m[1])
}

// submitTitle is the tooltip of the page's disabled submit button with the given label ("" when
// that button is enabled or absent).
func submitTitle(p page, label string) string {
	m := regexp.MustCompile(`<button type="button" disabled title="([^"]*)"[^>]*>` + regexp.QuoteMeta(label)).FindStringSubmatch(p.body)
	if m == nil {
		return ""
	}
	return m[1]
}

// canSubmit reports whether the page offers an enabled submit form posting to action.
func canSubmit(p page, action string) bool {
	return regexp.MustCompile(`<form method="post" action="[^"]*/` + regexp.QuoteMeta(action) + `"`).MatchString(p.body)
}

// waitFor polls until cond holds, for what the change feed applies asynchronously.
func (e *env) waitFor(what string, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// person is someone at a workstation, uploading with ok's jj.
type person struct {
	e         *env
	home      string
	principal string
}

func (e *env) person(principal string) *person {
	return &person{e: e, home: e.t.TempDir(), principal: principal}
}

// realCheckout makes a clone or create a real checkout: the suite tests ok's services, not
// virtual workspaces (docs/okfs.md).
func realCheckout(args []string) []string {
	if len(args) > 0 && (args[0] == "clone" || args[0] == "create") {
		return append(args[:len(args):len(args)], "--real")
	}
	return args
}

// command is jj as the person runs it in dir.
func (p *person) command(dir string, args ...string) *exec.Cmd {
	handle, _, _ := strings.Cut(p.principal, ".")
	cwd := filepath.Join(p.home, dir)
	if err := os.MkdirAll(cwd, 0o755); err != nil {
		p.e.t.Fatal(err)
	}
	cmd := exec.Command(p.e.jj, append([]string{"--no-pager", "--color=never", "--config", "ok.endpoint=" + p.e.endpoint,
		"--config", "ok.web-url=" + p.e.control}, args...)...)
	cmd.Dir = cwd
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + p.home,
		"OK_DEV_PRINCIPAL=" + p.principal,
		"JJ_USER=" + handle,
		"JJ_EMAIL=" + handle + "@example.com",
	}
	return cmd
}

func (p *person) run(dir string, args ...string) (string, error) {
	out, err := p.command(dir, realCheckout(args)...).CombinedOutput()
	return string(out), err
}

// jj runs jj in dir, requiring it to succeed.
func (p *person) jj(dir string, args ...string) string {
	p.e.t.Helper()
	out, err := p.run(dir, args...)
	if err != nil {
		p.e.t.Fatalf("jj %v as %s: %v\n%s", args, p.principal, err, out)
	}
	return out
}

func (p *person) write(file, contents string) {
	p.e.t.Helper()
	path := filepath.Join(p.home, file)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		p.e.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		p.e.t.Fatal(err)
	}
}

// uploaded reads the CL numbers jj cl printed, in order.
func uploaded(out string) []int64 {
	var ns []int64
	for _, m := range regexp.MustCompile(`CL (\d+) (?:created|patchset \d+|unchanged)`).FindAllStringSubmatch(out, -1) {
		n, _ := strconv.ParseInt(m[1], 10, 64)
		ns = append(ns, n)
	}
	return ns
}

// clPath is a CL's page, of a repository of the installation's organization, example.
func clPath(repo string, n int64) string { return fmt.Sprintf("/example/%s/cl/%d", repo, n) }

// hasEvent reports whether a CL's change log has an entry of kind.
func (e *env) hasEvent(repo string, n int64, kind db.EventKind) bool {
	e.t.Helper()
	r, ok, err := e.db.RepositoryByName(context.Background(), repo)
	if err != nil || !ok {
		return false
	}
	evs, err := e.db.Events(context.Background(), r.ID, n)
	if err != nil {
		e.t.Fatal(err)
	}
	for _, ev := range evs {
		if ev.Kind == kind {
			return true
		}
	}
	return false
}
