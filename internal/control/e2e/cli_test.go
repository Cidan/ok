package e2e

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/serve/authn"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// steered plays a worker the test steers: it passes every run, but fails the pre plan of the CL
// told to fail, writing a log first.
type steered struct {
	e      *env
	mu     sync.Mutex
	failCL int64
}

func (w *steered) fail(cl int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failCL = cl
}

func (w *steered) run(ctx context.Context) {
	for ctx.Err() == nil {
		resp, err := w.e.work.Lease(ctx, &automationpb.LeaseRequest{Worker: "steered", WaitSeconds: 1})
		if err != nil || resp.GetRun() == nil {
			continue
		}
		r := resp.GetRun()
		w.mu.Lock()
		fail := w.failCL != 0 && r.GetOrigin().GetClNumber() == w.failCL && r.GetOrigin().GetPhase() == automationpb.Origin_PHASE_PRE
		w.mu.Unlock()
		req := &automationpb.FinishRequest{RunId: r.GetId(), Worker: "steered", Status: automationpb.Run_STATUS_PASSED}
		if fail {
			if stream, err := w.e.work.AppendLog(ctx); err == nil {
				_ = stream.Send(&automationpb.AppendLogRequest{RunId: r.GetId(), Worker: "steered", Data: []byte("planning\n--- FAIL: TestNope\nFAIL\n")})
				_, _ = stream.CloseAndRecv()
			}
			req.Status, req.ExitCode, req.Reason = automationpb.Run_STATUS_FAILED, 1, "exit status 1"
		} else if r.GetJob().GetKind() == automationpb.Job_KIND_PLAN {
			req.Plan = &automationpb.Plan{}
		}
		_, _ = w.e.work.Finish(ctx, req)
	}
}

// try runs jj in dir and returns what it printed and its exit code.
func (p *person) try(dir string, args ...string) (string, int) {
	p.e.t.Helper()
	out, err := p.run(dir, args...)
	var exit *exec.ExitError
	switch {
	case err == nil:
		return out, 0
	case errors.As(err, &exit):
		return out, exit.ExitCode()
	}
	p.e.t.Fatalf("jj %v: %v", args, err)
	return "", 0
}

// expect runs jj in dir, requiring the exit code want.
func (p *person) expect(want int, dir string, args ...string) string {
	p.e.t.Helper()
	out, code := p.try(dir, args...)
	if code != want {
		p.e.t.Fatalf("jj %v as %s exited %d, want %d:\n%s", args, p.principal, code, want, out)
	}
	return out
}

// background starts jj in dir; wait returns what it printed and its exit code.
func (p *person) background(dir string, args ...string) (wait func() (string, int)) {
	p.e.t.Helper()
	cmd := p.command(dir, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		p.e.t.Fatal(err)
	}
	return func() (string, int) {
		err := cmd.Wait()
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return out.String(), exit.ExitCode()
		}
		if err != nil {
			p.e.t.Fatal(err)
		}
		return out.String(), 0
	}
}

// statusJSON reads a CL's status as jj prints it with --json: ok control's REST form.
func statusJSON(t *testing.T, out string) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &v); err != nil {
		t.Fatalf("not one JSON object: %v\n%s", err, out)
	}
	return v
}

func field(v map[string]any, path ...string) any {
	var cur any = v
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[p]
	}
	return cur
}

// The command line carries CLs through their whole lifecycle against ok control as it is
// deployed: status and following it, review, comments and threads, submit to landed and
// deployed, a deployment's retry, a failing pre with its log and a rerun, abandon and restore,
// and who may do what — and the same service answers JSON over HTTP, refusing other sites.
func TestCommandLineLifecycle(t *testing.T) {
	e := newEnvWithoutWorker(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := &steered{e: e}
	go w.run(ctx)
	e.post(alice, "/repositories/new", url.Values{"name": {"svc"}})
	svc, ok, err := e.db.RepositoryByName(ctx, "svc")
	if err != nil || !ok {
		t.Fatalf("the svc repository: %v %v", ok, err)
	}
	repoID := svc.ID
	a, b, c := e.person(alice), e.person(bob), e.person(carol)
	a.jj("", "clone", "svc")

	// main: bob owns lib/. Nothing governs OWNERS' first upload, so its author approves it.
	a.write("svc/lib/OWNERS", "user: bob\n")
	a.write("svc/lib/lib.go", "package lib\n\nfunc One() int { return 1 }\n")
	a.jj("svc", "describe", "-m", "add lib")
	first := uploaded(a.jj("svc", "cl", "--ready"))[0]
	a.expect(0, "svc", "cl", "review", itoa(first), "--lgtm", "--approve")
	a.expect(0, "svc", "cl", "status", "-f", "--until", "submittable", itoa(first))
	out := a.expect(0, "svc", "cl", "submit", itoa(first), "-f", "--until", "landed")
	if !strings.Contains(out, "Landed as") {
		t.Fatalf("submit -f --until landed:\n%s", out)
	}
	a.jj("svc", "sync")

	// A draft runs no pre until it is sent for review: it waits on its author. Run by hand
	// (jj cl pre), its pre runs, and it waits on its author again.
	a.jj("svc", "new", "main@ok", "-m", "two from lib")
	a.write("svc/lib/lib.go", "package lib\n\nfunc One() int { return 1 }\n\nfunc Two() int { return 2 }\n")
	n := uploaded(a.jj("svc", "cl"))[0]
	cl := itoa(n)
	if out := a.expect(2, "svc", "cl", "status", "-f"); !strings.Contains(out, "cl/"+cl+"  draft") || !strings.Contains(out, "pre runs once it is sent for review") ||
		!strings.Contains(out, "[waiting on its author: alice]") {
		t.Fatalf("following a draft:\n%s", out)
	}
	if st := statusJSON(t, a.expect(0, "svc", "cl", "status", "--json")); field(st, "pre", "verdict") != "VERDICT_NONE" {
		t.Fatalf("a draft's pre: %v", st)
	}
	a.expect(0, "svc", "cl", "pre")
	if out := a.expect(2, "svc", "cl", "status", "-f"); !strings.Contains(out, "cl/"+cl+"  draft") || !strings.Contains(out, "[waiting on its author: alice]") {
		t.Fatalf("following a draft whose pre was run by hand:\n%s", out)
	}
	st := statusJSON(t, a.expect(0, "svc", "cl", "status", "--json"))
	if field(st, "stage") != "STAGE_DRAFT" || field(st, "waitingOn") != "WAITING_ON_AUTHOR" || field(st, "pre", "verdict") != "VERDICT_PASSED" || field(st, "cl", "number") != cl {
		t.Fatalf("a draft's JSON, its pre run by hand: %v", st)
	}

	// Sent for review: it waits on bob, lib's owner.
	if out, code := a.try("svc", "cl", "--ready", "--reviewer", "bob"); code == 0 || !strings.Contains(out, "jj cl ready") {
		t.Errorf("--ready with nothing new to upload points at jj cl ready: %d\n%s", code, out)
	}
	if out := a.expect(0, "svc", "cl", "ready", "--reviewer", "bob", "-m", "please look"); !strings.Contains(out, "in review") {
		t.Fatalf("jj cl ready:\n%s", out)
	}
	if out := a.expect(2, "svc", "cl", "status", "-f", cl); !strings.Contains(out, "in review") || !strings.Contains(out, "[waiting on reviewers: bob]") {
		t.Fatalf("following a CL in review:\n%s", out)
	}
	c.jj("", "clone", "svc")
	if out, code := c.try("svc", "cl", "submit", cl); code == 0 || !strings.Contains(out, "Only the author or a repository admin can submit this changelist") {
		t.Errorf("carol submitting alice's CL: %d\n%s", code, out)
	}

	// bob reviews from his own clone: a thread on a line, then his LGTM and Approval.
	b.jj("", "clone", "svc")
	b.expect(0, "svc", "cl", "comment", "--cl", cl, "-m", "name it Two?", "--file", "lib/lib.go", "--line", "5")
	st = statusJSON(t, a.expect(0, "svc", "cl", "status", "--json", cl))
	threads, _ := field(st, "review", "unresolved").([]any)
	if len(threads) != 1 {
		t.Fatalf("bob's thread: %v", field(st, "review"))
	}
	thread, _ := threads[0].(map[string]any)["id"].(string)
	if out := a.expect(0, "svc", "cl", "status", cl); !strings.Contains(out, "unresolved thread "+thread+" by bob on lib/lib.go:5: name it Two?") {
		t.Fatalf("status shows the thread:\n%s", out)
	}
	b.expect(0, "svc", "cl", "review", cl, "--lgtm", "--approve", "-m", "ship it")
	if out := a.expect(2, "svc", "cl", "status", "-f", cl); !strings.Contains(out, "1 unresolved thread resolved") {
		t.Fatalf("approved, but for the thread:\n%s", out)
	}
	a.expect(0, "svc", "cl", "resolve", "--cl", cl, thread)
	a.expect(0, "svc", "cl", "status", "-f", "--until", "submittable", cl)

	// Submit, following it until it lands.
	if out := a.expect(0, "svc", "cl", "submit", cl, "-f", "--until", "landed"); !strings.Contains(out, "Landed as") {
		t.Fatalf("submit:\n%s", out)
	}
	landed, _ := hex.DecodeString(field(statusJSON(t, a.expect(0, "svc", "cl", "status", "--json", cl)), "cl", "landedCommit").(string))

	// Its deployments: following waits while one rolls, and ends once it succeeded.
	dep := &deploypb.Deployment{Id: 1, RepositoryId: repoID, Deploy: "//lib/.ok/workflow/d.yaml:deploy:lib", Name: "lib",
		Status: deploypb.Deployment_STATUS_RUNNING, Stage: deploypb.Deployment_STAGE_ROLL, CreatedAt: timestamppb.New(time.Now().Add(-time.Minute))}
	e.deployer.add(dep, landed)
	wait := a.background("svc", "cl", "status", "-f", "--until", "deployed", cl)
	e.waitFor("the follow to see the roll", func() bool {
		return strings.Contains(a.jj("svc", "cl", "status", cl), "deploying 0/1: lib: roll")
	})
	time.Sleep(300 * time.Millisecond)
	e.deployer.mu.Lock()
	dep.Status, dep.Stage = deploypb.Deployment_STATUS_SUCCEEDED, deploypb.Deployment_STAGE_DONE
	e.deployer.mu.Unlock()
	if out, code := wait(); code != 0 || !strings.Contains(out, "cl/"+cl+"  deploying") || !strings.Contains(out, "cl/"+cl+"  deployed") {
		t.Fatalf("following to deployed: %d\n%s", code, out)
	}
	// The same, as JSON: each line ok control's REST form.
	lines := strings.Split(strings.TrimSpace(a.expect(0, "svc", "cl", "status", "-f", "--json", cl)), "\n")
	if last := statusJSON(t, lines[len(lines)-1]); field(last, "stage") != "STAGE_DEPLOYED" {
		t.Fatalf("following as JSON: %v", lines)
	}

	// A later deployment of the commit failed: retried from the command line by its author.
	failed := &deploypb.Deployment{Id: 2, RepositoryId: repoID, Deploy: "//lib/.ok/workflow/d.yaml:deploy:lib", Name: "lib",
		Status: deploypb.Deployment_STATUS_FAILED, Stage: deploypb.Deployment_STAGE_ROLL, Reason: "a pod restarted", CreatedAt: timestamppb.Now()}
	e.deployer.add(failed, landed)
	if out := a.expect(1, "svc", "cl", "status", "-f", cl); !strings.Contains(out, "deploy failed") || !strings.Contains(out, "deployment failed: lib — a pod restarted") {
		t.Fatalf("a failed deployment:\n%s", out)
	}
	if out, code := c.try("svc", "cl", "deployment", "--cl", cl, "retry", "2"); code == 0 || !strings.Contains(out, "can retry this deployment") {
		t.Errorf("carol retrying: %d\n%s", code, out)
	}
	a.expect(0, "svc", "cl", "deployment", "--cl", cl, "retry", "2")
	if got := e.deployer.actions(); len(got) != 1 || got[0] != "retry 2 for "+e.id(alice) {
		t.Errorf("the deployer was asked: %v", got)
	}

	// A CL whose pre fails: following ends with the failure and its log; a rerun passes it.
	a.jj("svc", "new", "main@ok", "-m", "three from lib")
	a.write("svc/lib/three.go", "package lib\n\nfunc Three() int { return 3 }\n")
	w.fail(n + 1)
	bad := uploaded(a.jj("svc", "cl", "--ready"))[0]
	if bad != n+1 {
		t.Fatalf("CL %d, want %d", bad, n+1)
	}
	out = a.expect(1, "svc", "cl", "status", "-f")
	if !strings.Contains(out, "pre failed") || !strings.Contains(out, "  │ --- FAIL: TestNope") || !strings.Contains(out, "exit status 1") {
		t.Fatalf("a failing pre:\n%s", out)
	}
	run := field(statusJSON(t, a.expect(0, "svc", "cl", "status", "--json")), "failure", "run").(string)
	if log := a.expect(0, "svc", "cl", "log", run); log != "planning\n--- FAIL: TestNope\nFAIL\n" {
		t.Errorf("the run's log: %q", log)
	}
	if log := a.expect(0, "svc", "cl", "log", run, "--tail", "5"); log != "FAIL\n" {
		t.Errorf("the log's tail: %q", log)
	}
	w.fail(0)
	a.expect(0, "svc", "cl", "rerun")
	a.expect(0, "svc", "cl", "status", "-f", "--until", "pre")

	// Abandon and restore.
	if out := a.expect(0, "svc", "cl", "abandon", "-m", "not now"); !strings.Contains(out, "abandoned") {
		t.Fatalf("abandon:\n%s", out)
	}
	a.expect(1, "svc", "cl", "status", "-f")
	if out := a.expect(0, "svc", "cl", "restore"); !strings.Contains(out, "in review") {
		t.Fatalf("restore:\n%s", out)
	}

	// No such CL, and a change with none.
	if out := a.expect(4, "svc", "cl", "status", "999"); !strings.Contains(out, "no such changelist") {
		t.Errorf("no such CL:\n%s", out)
	}
	a.jj("svc", "new", "main@ok", "-m", "never uploaded")
	a.write("svc/x.txt", "x\n")
	if out := a.expect(4, "svc", "cl", "status", "-f"); !strings.Contains(out, "has no CL") {
		t.Errorf("a change with no CL:\n%s", out)
	}

	// The service as JSON over HTTP: a person's, never another site's.
	rest := func(principal string, header map[string]string) (int, string) {
		req, _ := http.NewRequest(http.MethodPost, e.control+"/ok.control.v1.ChangeLists/Status", strings.NewReader(`{"repository":"svc","number":`+cl+`}`))
		req.Header.Set("Content-Type", "application/json")
		if principal != "" {
			req.Header.Set(authn.DevPrincipalKey, principal)
		}
		for k, v := range header {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body bytes.Buffer
		body.ReadFrom(resp.Body)
		return resp.StatusCode, body.String()
	}
	if code, body := rest(carol, nil); code != 200 || !strings.Contains(body, `"stage":"STAGE_DEPLOY`) {
		t.Errorf("REST, as a person: %d %s", code, body)
	}
	if code, body := rest(carol, map[string]string{"Sec-Fetch-Site": "cross-site"}); code != 403 || !strings.Contains(body, "another site") {
		t.Errorf("REST, from another site: %d %s", code, body)
	}
	if code, _ := rest("", nil); code != 401 {
		t.Errorf("REST, as no one: %d", code)
	}
	if code, body := rest(carol, map[string]string{"Origin": e.control}); code != 200 {
		t.Errorf("REST, from ok's own page: %d %s", code, body)
	}
	// The site still answers beside the service, on the same port.
	req, _ := http.NewRequest(http.MethodGet, e.control+"/example/svc/cl/"+cl, nil)
	req.Header.Set(authn.DevPrincipalKey, carol)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var page bytes.Buffer
	page.ReadFrom(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(page.String(), "two from lib") {
		t.Errorf("the site beside the service: %d", resp.StatusCode)
	}
}
