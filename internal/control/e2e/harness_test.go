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

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	autorole "github.com/Cidan/ok/internal/automation/role"
	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/role"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/ca"
	idrole "github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/pgdb"
	scmrole "github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/testdb"
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
	db      *db.DB
	client  *http.Client
	// work is ok automation's side for workers, which a test plays; stopAutomation takes
	// automation away.
	work           automationpb.WorkClient
	stopAutomation func()
	// deployer is ok's deployer, which a test plays (deploy_test.go).
	deployer *deployer
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
	s3 := testdb.S3(t)
	b, err := blob.NewS3(blob.Config{Endpoint: s3.Endpoint, Bucket: testdb.Bucket(t), AccessKey: s3.AccessKey, SecretKey: s3.SecretKey, Region: s3.Region})
	if err != nil {
		t.Fatal(err)
	}
	return b
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
	if len(parts) != 3 || err != nil {
		e.t.Fatalf("%s is not a CL's path", path)
	}
	cl, ok, err := dbChangeList(e, parts[0], n)
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

// newEnvWithoutWorker is a test's ok whose worker the test plays itself.
func newEnvWithoutWorker(t *testing.T) *env {
	t.Helper()
	jj := testjj.Path(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
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
	// ok control's address is its public URL: the links it gives, and the origin its forms and
	// ChangeLists service accept.
	controlLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + controlLis.Addr().String()
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
	url := testdb.Postgres(t)
	open := func(schema string) *pgxpool.Pool {
		p, err := pgdb.Open(ctx, url, schema, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}

	// ok identity, its people synced, and a tree applied — admins.acl holding dana, and the
	// reviewers ACL holding bob — after one that declared the group old, now retired.
	id, err := idrole.Build(ctx, cfg, idrole.Options{Dev: true, Pool: open(idrole.Schema)})
	if err != nil {
		t.Fatal(err)
	}
	idAddr, _ := serveRole(t, ctx, nil, id.Root, serve.WithAuthn(id.Resolver))
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

	// The SCM, and ok automation following its change feed.
	scm, err := scmrole.Build(ctx, cfg, scmrole.Options{Dev: true, Pool: open(scmrole.Schema), Blobs: bucket(t), Identity: as(t, idAddr, scmSelf)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scm.Close)
	scmAddr, _ := serveRole(t, ctx, nil, scm.Root, serve.WithAuthn(scm.Resolver))
	auto, err := autorole.Build(ctx, cfg, autorole.Options{Dev: true, Pool: open(autorole.Schema), Logs: bucket(t),
		Identity: as(t, idAddr, automation), SCM: as(t, scmAddr, automation)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(auto.Close)
	autoAddr, stopAutomation := serveRole(t, ctx, nil, auto.Root, serve.WithAuthn(auto.Resolver))

	// ok control, as its robot, with the deployer's stand-in.
	dep, deployAddr := startDeployer(t)
	ctl, err := role.Build(ctx, cfg, role.Options{Dev: true, Pool: open(role.Schema), Identity: as(t, idAddr, control), SCM: as(t, scmAddr, control),
		Automation: as(t, autoAddr, control), Deploy: as(t, deployAddr, control), Poll: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ctl.Close)
	serveRole(t, ctx, controlLis, ctl.Root, serve.WithAuthn(ctl.Resolver))
	d, err := db.Open(ctx, open(role.Schema))
	if err != nil {
		t.Fatal(err)
	}
	return &env{
		t: t, jj: jj, endpoint: "http://" + scmAddr, control: base, db: d, deployer: dep,
		work: automationpb.NewWorkClient(as(t, autoAddr, worker)), stopAutomation: stopAutomation,
		client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
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
	handle := people.Handle(p.principal)
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

func clPath(repo string, n int64) string { return fmt.Sprintf("/%s/cl/%d", repo, n) }

// hasEvent reports whether a CL's change log has an entry of kind.
func (e *env) hasEvent(repo string, n int64, kind db.EventKind) bool {
	e.t.Helper()
	var id int64
	err := e.db.Q().QueryRow(context.Background(), `SELECT r.id FROM repository r WHERE r.name = $1`, repo).Scan(&id)
	if err != nil {
		return false
	}
	evs, err := db.Events(context.Background(), e.db.Q(), id, n)
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
