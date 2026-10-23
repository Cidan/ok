package e2e

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// workerFixture is the fixture with a test that fails, and one that runs until stopped when asked
// to (--slow): pre, which the upload plans, runs it quickly.
func workerFixture() map[string]*string {
	f := fixture()
	f["fails/BUILD.bazel"] = ptr(`load("//:rules.bzl", "script_test")

script_test(
    name = "fails_test",
    script = "echo a deliberate failure; exit 3\n",
)
`)
	f["slow/BUILD.bazel"] = ptr(`load("//:rules.bzl", "script_test")

script_test(
    name = "slow_test",
    script = "if [ \"$1\" = --slow ]; then echo waiting; sleep 600; fi\n",
)
`)
	return f
}

// TestWorker runs workers on runs started by hand: every job kind, a failure, a build of two
// targets one of which is missing, the sandboxed run, a second patchset's checkout, cancel,
// release on shutdown, readiness — none without automation, ready once it answered — and a
// worker of two slots running two runs at once, each in its own checkout, giving both back on
// shutdown.
func TestWorker(t *testing.T) {
	e := start(t)
	a := e.person(ada)
	a.jj(".", "create", "svc", "svc")
	write(t, a.dir("svc"), workerFixture())
	a.jj("svc", "describe", "-m", "a service")
	cl := uploaded(a.jj("svc", "cl"))[0]
	repo := e.repository("svc")
	first := e.cl(repo, cl).GetPatchsets()[0].GetCommitId()

	// A worker that cannot reach ok automation is not ready: it takes no runs.
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nowhere := closed.Addr().String()
	closed.Close()
	lost := e.startWorkerAt("lost", 1, nowhere)
	time.Sleep(3 * time.Second)
	if lost.ready() {
		t.Fatal("a worker that cannot reach ok automation is ready")
	}
	lost.stop()

	w1 := e.startWorker("w1", 1)
	startRun := func(commit []byte, job *automationpb.Job) int64 {
		t.Helper()
		r, err := e.runs.StartRun(e.ctx, &automationpb.StartRunRequest{RepositoryId: repo, CommitId: commit, Job: job,
			Origin: &automationpb.Origin{Phase: automationpb.Origin_PHASE_MANUAL}, ForPersonId: e.adaID})
		if err != nil {
			t.Fatal(err)
		}
		return r.GetId()
	}
	get := func(id int64) *automationpb.Run {
		t.Helper()
		r, err := e.runs.GetRun(e.ctx, &automationpb.GetRunRequest{Id: id})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	waitFor := func(what string, limit time.Duration, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(limit)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	expect := func(what string, id int64, want automationpb.Run_Status, logHas ...string) *automationpb.Run {
		t.Helper()
		var r *automationpb.Run
		waitFor(fmt.Sprintf("run %d to finish", id), 5*time.Minute, func() bool {
			r = get(id)
			return r.GetStatus() >= automationpb.Run_STATUS_PASSED
		})
		log := e.logOf(id)
		if r.GetStatus() != want {
			t.Fatalf("%s: %s (%s), want %s\n%s", what, r.GetStatus(), r.GetReason(), want, log)
		}
		for _, s := range logHas {
			if !strings.Contains(log, s) {
				t.Errorf("%s: the log lacks %q:\n%s", what, s, log)
			}
		}
		return r
	}
	job := func(kind automationpb.Job_Kind, targets ...string) *automationpb.Job {
		return &automationpb.Job{Kind: kind, Targets: targets}
	}

	// A build: the commit written to the slot's checkout, and Bazel building the target.
	r := expect("a build", startRun(first, job(automationpb.Job_KIND_BUILD, "//lib")), automationpb.Run_STATUS_PASSED,
		"ok: checking out", "written; ", "ok: $ bazel ", "build ", "ok: the run passed")
	if r.GetWorker() != "w1" || r.GetExitCode() != 0 {
		t.Errorf("the build: %v", r)
	}
	if !w1.ready() {
		t.Error("w1 took a run from ok automation but is not ready")
	}

	// Tests: one that passes, and one that fails, its exit and the command in the run's reason.
	expect("a passing test", startRun(first, job(automationpb.Job_KIND_TEST, "//app:app_test")), automationpb.Run_STATUS_PASSED)
	r = expect("a failing test", startRun(first, job(automationpb.Job_KIND_TEST, "//fails:fails_test")), automationpb.Run_STATUS_FAILED)
	if r.GetExitCode() == 0 || !strings.Contains(r.GetReason(), "-- //fails:fails_test exited with status") {
		t.Errorf("the failing test's run: %v", r)
	}

	// A build of several targets builds every one, and fails for the one that is missing.
	expect("a build of two targets, one missing", startRun(first, job(automationpb.Job_KIND_BUILD, "//nope:nope", "//lib")),
		automationpb.Run_STATUS_FAILED, "//nope")

	// A run job's binary runs in the sandbox: it writes no checkout and reaches no network. A
	// manual run has no job context, so the tool, which reads one, fails — and says why.
	expect("a run with no job context", startRun(first, &automationpb.Job{Kind: automationpb.Job_KIND_RUN, Targets: []string{"//app:tool"}}),
		automationpb.Run_STATUS_FAILED)

	// A second patchset breaks the test: each run sees its own commit's files, the checkout moving
	// between them.
	write(t, a.dir("svc"), map[string]*string{"lib/a.txt": ptr("goodbye\n")})
	a.jj("svc", "cl")
	second := e.cl(repo, cl).GetPatchsets()[1].GetCommitId()
	expect("a test on the second patchset", startRun(second, job(automationpb.Job_KIND_TEST, "//app:app_test")), automationpb.Run_STATUS_FAILED)
	expect("the test, back on the first", startRun(first, job(automationpb.Job_KIND_TEST, "//app:app_test")), automationpb.Run_STATUS_PASSED, "written; 1 files")

	// A run canceled while it runs is stopped, Bazel and its test with it.
	running := func(id int64, worker string) func() bool {
		return func() bool {
			r := get(id)
			return r.GetStatus() == automationpb.Run_STATUS_RUNNING && r.GetWorker() == worker && strings.Contains(e.logOf(id), "//slow:slow_test")
		}
	}
	slow := &automationpb.Job{Kind: automationpb.Job_KIND_TEST, Targets: []string{"//slow:slow_test"}, Args: []string{"--slow"}}
	id := startRun(first, slow)
	waitFor("the slow test to run", 2*time.Minute, running(id, "w1"))
	time.Sleep(2 * time.Second) // into the test itself
	if _, err := e.runs.CancelRun(e.ctx, &automationpb.CancelRunRequest{Id: id, Reason: "not needed", ForPersonId: e.adaID}); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	expect("a canceled run", id, automationpb.Run_STATUS_CANCELED, "ok: the run was canceled: not needed")
	if took := time.Since(began); took > time.Minute {
		t.Errorf("the canceled run took %s to stop", took)
	}

	// A worker shutting down gives its run back, which another worker then takes.
	id = startRun(first, slow)
	waitFor("the slow test to run on w1", 2*time.Minute, running(id, "w1"))
	w1.stop()
	r = get(id)
	if r.GetStatus() != automationpb.Run_STATUS_QUEUED || r.GetAttempt() != 1 || !strings.Contains(r.GetReason(), "w1 is shutting down") {
		t.Fatalf("after w1 shut down: %v", r)
	}
	w2 := e.startWorker("w2", 1)
	waitFor("the slow test to run on w2", 2*time.Minute, running(id, "w2"))
	if _, err := e.runs.CancelRun(e.ctx, &automationpb.CancelRunRequest{Id: id, ForPersonId: e.adaID}); err != nil {
		t.Fatal(err)
	}
	expect("the released run, canceled", id, automationpb.Run_STATUS_CANCELED)
	w2.stop()

	// A worker with two slots runs two runs at once, each in a checkout of its own: beside a slow
	// test on the first patchset, a test on the second reads the second's files; two slow tests
	// run together.
	e.startWorker("w3", 2)
	inSlot := func(id int64) func() bool {
		return func() bool { return running(id, "w3/1")() || running(id, "w3/2")() }
	}
	one := startRun(first, slow)
	waitFor("a slow test to run on w3", 2*time.Minute, inSlot(one))
	beside := expect("a test beside it", startRun(second, job(automationpb.Job_KIND_TEST, "//app:app_test")), automationpb.Run_STATUS_FAILED)
	if beside.GetWorker() == get(one).GetWorker() || !strings.HasPrefix(beside.GetWorker(), "w3/") {
		t.Errorf("the test beside the slow one ran on %q, the slow one on %q", beside.GetWorker(), get(one).GetWorker())
	}
	two := startRun(first, slow)
	waitFor("two slow tests to run on w3 at once", 2*time.Minute, func() bool { return inSlot(one)() && inSlot(two)() })
	if get(one).GetWorker() == get(two).GetWorker() {
		t.Errorf("two runs at once in one slot: %q", get(one).GetWorker())
	}
	for _, id := range []int64{one, two} {
		if _, err := e.runs.CancelRun(e.ctx, &automationpb.CancelRunRequest{Id: id, ForPersonId: e.adaID}); err != nil {
			t.Fatal(err)
		}
		expect("a slow test, canceled", id, automationpb.Run_STATUS_CANCELED)
	}
}
