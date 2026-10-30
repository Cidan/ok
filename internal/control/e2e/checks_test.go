package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"

	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// checkRow is one run on the Checks tab, as its region renders it.
type checkRow struct {
	status, name string
	action       string // "Cancel" or "Rerun"
	enabled      bool   // the action posts; else it is disabled
	title        string // a disabled action's reason
	logHref      string
}

var checkRowRE = regexp.MustCompile(`(?s)<span class="[^"]*rounded-md[^"]*">\s*([A-Za-z ]+?)\s*</span>\s*</span>\s*<div class="min-w-0 flex-1 leading-6"><div class="flex min-w-0 items-center gap-2"><span class="truncate font-mono text-zinc-200">([^<]+)</span>.*?<a href="([^"]+)"[^>]*>Log</a>\s*(?:<button type="button" hx-post="[^"]+"[^>]*>([A-Za-z]+)</button>|<button type="button" disabled title="([^"]*)"[^>]*>([A-Za-z]+)</button>)`)

// checks reads the Checks tab's runs, its verdict, whether it refreshes itself, and its
// unaffected workflow jobs, from a page or the region's own fragment.
func checks(t *testing.T, p page) (rows []checkRow, verdict string, live bool, skipped []string) {
	t.Helper()
	i := strings.Index(p.body, `id="cl-checks"`)
	if i < 0 {
		t.Fatalf("no Checks region:\n%s", excerpt(p.body))
	}
	body := p.body[i:]
	open := body[:strings.IndexByte(body, '>')]
	live = strings.Contains(open, `hx-trigger="live" data-live-events="/example/svc/cl/`)
	if m := regexp.MustCompile(`(?s)<h3>Pre</h3>.*?<span class="[^"]*rounded-md[^"]*">\s*([A-Za-z ]+?)\s*</span>`).FindStringSubmatch(body); m != nil {
		verdict = m[1]
	}
	for _, m := range checkRowRE.FindAllStringSubmatch(body, -1) {
		r := checkRow{status: m[1], name: m[2], logHref: m[3]}
		if m[4] != "" {
			r.action, r.enabled = m[4], true
		} else {
			r.action, r.title = m[6], m[5]
		}
		rows = append(rows, r)
	}
	for _, m := range regexp.MustCompile(`(?s)Not Run\s*</span>\s*</span>\s*<div class="min-w-0 flex-1 leading-6"><div class="flex min-w-0 items-center gap-2"><span class="truncate font-mono text-zinc-200">([^<]+)</span>`).FindAllStringSubmatch(body, -1) {
		skipped = append(skipped, m[1])
	}
	return rows, verdict, live, skipped
}

func rowNamed(t *testing.T, rows []checkRow, name string) checkRow {
	t.Helper()
	for _, r := range rows {
		if r.name == name {
			return r
		}
	}
	t.Fatalf("no check %q in %+v", name, rows)
	return checkRow{}
}

// TestChecks drives a CL's Checks tab through ok control's pages, over a real ok
// automation that plans each patchset from the SCM's change feed; the test plays the worker
// that runs what automation queues.
func TestChecks(t *testing.T) {
	ctx := context.Background()
	e := newEnvWithoutWorker(t)
	e.post(alice, "/repositories/new", url.Values{"name": {"svc"}})
	a := e.person(alice)
	a.jj("", "clone", "svc")
	a.write("svc/lib/lib.go", "package lib\n")
	a.jj("svc", "describe", "-m", "add lib")
	n := uploaded(a.jj("svc", "cl", "--ready"))[0]
	path := clPath("svc", n)

	// Automation plans the patchset from the feed: the tab shows its plan queued, and refreshes
	// itself. Whoever may land on the repository may act on it: in an installation, every member.
	var rows []checkRow
	var verdict string
	var live bool
	e.waitFor("the patchset's plan on the Checks tab", func() bool {
		rows, verdict, live, _ = checks(t, e.get(alice, path))
		return len(rows) == 1
	})
	if plan := rows[0]; plan.name != "plan" || plan.status != "Queued" || plan.action != "Cancel" || !plan.enabled || verdict != "Running" || !live {
		t.Fatalf("the planned patchset: %+v, %q, live %v", rows, verdict, live)
	}
	rows, _, _, _ = checks(t, e.get(bob, path))
	if r := rows[0]; !r.enabled || r.action != "Cancel" {
		t.Errorf("the plan, to another member, who may land: %+v", r)
	}

	// The worker takes the planning run, logs, and finishes it with a plan: a build, a test, and
	// a workflow job left to main.
	lease := func() *automationpb.Run {
		t.Helper()
		resp, err := e.work.Lease(ctx, &automationpb.LeaseRequest{Worker: "w", WaitSeconds: 10})
		if err != nil || resp.GetRun() == nil {
			t.Fatalf("leasing a run: %v, %v", resp, err)
		}
		return resp.GetRun()
	}
	appendLog := func(r *automationpb.Run, offset int64, text string) {
		t.Helper()
		stream, err := e.work.AppendLog(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&automationpb.AppendLogRequest{RunId: r.GetId(), Worker: "w", Offset: offset, Data: []byte(text)}); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.CloseAndRecv(); err != nil {
			t.Fatal(err)
		}
	}
	finish := func(r *automationpb.Run, st automationpb.Run_Status, plan *automationpb.Plan) {
		t.Helper()
		if _, err := e.work.Finish(ctx, &automationpb.FinishRequest{RunId: r.GetId(), Worker: "w", Status: st, Plan: plan}); err != nil {
			t.Fatal(err)
		}
	}
	plan := lease()
	appendLog(plan, 0, "planning pre\n")
	finish(plan, automationpb.Run_STATUS_PASSED, &automationpb.Plan{
		Jobs: []*automationpb.PlannedJob{
			{Job: "build", Why: "every target the change affects must build", Spec: &automationpb.Job{Kind: automationpb.Job_KIND_BUILD, Targets: []string{"//lib:lib"}}},
			{Job: "test //lib:test", Why: "the test is affected", Spec: &automationpb.Job{Kind: automationpb.Job_KIND_TEST, Targets: []string{"//lib:test"}}},
		},
		Skipped: []*automationpb.SkippedJob{{Job: "//.ok/workflow/ci.yaml:pre:docs", Target: "//docs:check", Why: "its target is not affected"}},
	})
	checksPath := path + "/checks?ps=1"
	tab := e.get(alice, checksPath)
	rows, verdict, live, skipped := checks(t, tab)
	// Pre's runs are grouped by kind — a test is named by its target in its group — each group
	// tallying its runs, and the unaffected workflow jobs are a group too.
	for _, want := range []string{`data-check-group="pre-1:plan"`, `data-check-group="pre-1:build"`, `data-check-group="pre-1:test"`,
		`data-check-group="pre-1:unaffected"`, "1 Queued", "Unaffected Workflow Jobs"} {
		if !tab.has(want) {
			t.Errorf("the grouped checks lack %s: %s", want, excerpt(tab.body))
		}
	}
	if len(rows) != 3 || rowNamed(t, rows, "plan").status != "Passed" || rowNamed(t, rows, "build").status != "Queued" ||
		rowNamed(t, rows, "//lib:test").status != "Queued" || verdict != "Running" || !live {
		t.Fatalf("the planned jobs: %+v, %q, live %v", rows, verdict, live)
	}
	if r := rowNamed(t, rows, "plan"); r.action != "Rerun" || r.enabled || r.title != "The patchset is planned: rerun its jobs instead" {
		t.Errorf("the passed plan's action: %+v", r)
	}
	if len(skipped) != 1 || skipped[0] != "docs" {
		t.Errorf("the unaffected workflow jobs: %v", skipped)
	}

	// Review is satisfied, but pre gates submit: it is still running.
	e.post(alice, path+"/reply", grant("LGTM", "Approval"))
	p := e.get(alice, path)
	if got := submitTitle(p, "Submit"); got != "Checks are still running on patchset 1" || canSubmit(p, "submit") {
		t.Errorf("submit while pre runs: %q", got)
	}
	if n := notice(e.post(alice, path+"/submit", nil)); n != "Checks are still running on patchset 1" {
		t.Errorf("submitting while pre runs: %q", n)
	}

	// The author cancels the build, which fails pre, and reruns it.
	build := rowNamed(t, rows, "build")
	rows, verdict, _, _ = checks(t, e.post(alice, build.logHref+"/cancel", nil))
	if r := rowNamed(t, rows, "build"); r.status != "Canceled" || r.action != "Rerun" || verdict != "Failed" {
		t.Fatalf("after canceling the build: %+v, %q", rows, verdict)
	}
	if got := submitTitle(e.get(alice, path), "Submit"); got != "Checks failed on patchset 1: build was canceled" {
		t.Errorf("submit with a canceled build: %q", got)
	}
	rows, verdict, _, _ = checks(t, e.post(alice, build.logHref+"/rerun", nil))
	if r := rowNamed(t, rows, "build"); r.status != "Queued" || r.logHref == build.logHref || verdict != "Running" {
		t.Fatalf("after rerunning the build: %+v, %q", rows, verdict)
	}
	rebuilt := rowNamed(t, rows, "build").logHref
	// Another member, who may land on the repository, may cancel it too (and does not here).
	test := rowNamed(t, rows, "//lib:test")
	if rows, _, _, _ := checks(t, e.get(bob, path)); !rowNamed(t, rows, "//lib:test").enabled {
		t.Errorf("alice's test, to another member: %+v", rowNamed(t, rows, "//lib:test"))
	}

	// A run's page: its status and its log, which grows while it runs, then its raw bytes.
	testRun := lease()
	if want := "test //lib:test"; testRun.GetOrigin().GetJob() != want {
		t.Fatalf("leased %q, want %q", testRun.GetOrigin().GetJob(), want)
	}
	first := "=== RUN   TestLib\n\x1b[32mok\x1b[0m\n"
	appendLog(testRun, 0, first)
	p = e.get(alice, test.logHref)
	if !p.has("Running") || !p.has("=== RUN   TestLib\nok\n") || !p.has(`hx-get="`+test.logHref+`/log?offset=`) || !p.has(`hx-get="`+test.logHref+`/status"`) {
		t.Fatalf("the running test's page:\n%s", excerpt(p.body))
	}
	offset := regexp.MustCompile(`/log\?offset=(\d+)`).FindStringSubmatch(p.body)[1]
	appendLog(testRun, int64(len(first)), "--- PASS: TestLib\n")
	finish(testRun, automationpb.Run_STATUS_PASSED, nil)
	chunk := e.get(alice, test.logHref+"/log?offset="+offset)
	if !chunk.has("--- PASS: TestLib") || chunk.has("hx-trigger") {
		t.Errorf("the log's end: %q", chunk.body)
	}
	if st := e.get(alice, test.logHref+"/status"); !st.has("Passed") || st.has("hx-trigger") {
		t.Errorf("the finished run's status:\n%s", excerpt(st.body))
	}
	raw := e.do(alice, http.MethodGet, test.logHref+"/raw", nil, nil)
	if raw.status != http.StatusOK || !strings.HasPrefix(raw.header.Get("Content-Type"), "text/plain") || raw.header.Get("X-Content-Type-Options") != "nosniff" ||
		!strings.Contains(raw.body, "--- PASS: TestLib") {
		t.Errorf("the raw log: %d %v\n%s", raw.status, raw.header, raw.body)
	}

	// The rebuilt build passes, and so does pre: the tab stops refreshing.
	finish(lease(), automationpb.Run_STATUS_PASSED, nil)
	rows, verdict, live, _ = checks(t, e.get(alice, checksPath))
	if verdict != "Passed" || live {
		t.Errorf("with every job passed: %+v, %q, live %v", rows, verdict, live)
	}
	// Pre passed: Submit sends the change to the landing queue, which runs post on its candidate
	// and lands it once post passes.
	if !canSubmit(e.get(alice, path), "submit") {
		t.Fatalf("with pre passed: %q", submitTitle(e.get(alice, path), "Submit"))
	}
	p = e.post(alice, path+"/submit", nil)
	if !chip(p, "Submitting") || !p.has(path+"/cancel-submit") || !strings.HasPrefix(submitTitle(p, "Submit"), "Submitting: ") ||
		!logged(p, "alice", "sent it to the landing queue") {
		t.Fatalf("a submitting CL: %s", excerpt(p.body))
	}
	if b := e.get(bob, path); b.has(path+"/cancel-submit") || !chip(b, "Submitting") {
		t.Errorf("a submitting CL, to someone else: %s", excerpt(b.body))
	}
	postOf := func(p page) (status, detail string, rows []checkRow, attempts int) {
		t.Helper()
		i := strings.Index(p.body, "<h3>Post</h3>")
		if i < 0 {
			t.Fatalf("no Post panel:\n%s", excerpt(p.body))
		}
		body := p.body[i:]
		end := strings.Index(body, "</section>")
		if m := regexp.MustCompile(`(?s)^.*?rounded-md[^"]*">\s*([A-Za-z ]+?)\s*</span>`).FindStringSubmatch(body[:end]); m != nil {
			status = m[1]
		}
		if m := regexp.MustCompile(`px-4 py-2.5 text-sm text-zinc-300">([^<]*)</div>`).FindStringSubmatch(body[:end]); m != nil {
			detail = m[1]
		}
		for _, m := range checkRowRE.FindAllStringSubmatch(body[:end], -1) {
			rows = append(rows, checkRow{status: m[1], name: m[2], logHref: m[3], action: m[4] + m[6], enabled: m[4] != "", title: m[5]})
		}
		attempts = strings.Count(body[:end], ">Attempt ")
		return status, detail, rows, attempts
	}
	// take plays the worker until it leases the run want picks, passing every other run it takes.
	take := func(want func(*automationpb.Run) bool) *automationpb.Run {
		t.Helper()
		for i := 0; i < 20; i++ {
			r := lease()
			if want(r) {
				return r
			}
			var pl *automationpb.Plan
			if r.GetJob().GetKind() == automationpb.Job_KIND_PLAN {
				pl = &automationpb.Plan{}
			}
			finish(r, automationpb.Run_STATUS_PASSED, pl)
		}
		t.Fatal("the run never came")
		return nil
	}
	postPlan := func(cl int64) func(*automationpb.Run) bool {
		return func(r *automationpb.Run) bool {
			return r.GetOrigin().GetPhase() == automationpb.Origin_PHASE_POST && r.GetJob().GetKind() == automationpb.Job_KIND_PLAN &&
				r.GetOrigin().GetClNumber() == cl
		}
	}
	postJob := func(cl int64) func(*automationpb.Run) bool {
		return func(r *automationpb.Run) bool {
			return r.GetOrigin().GetPhase() == automationpb.Origin_PHASE_POST && r.GetJob().GetKind() != automationpb.Job_KIND_PLAN &&
				r.GetOrigin().GetClNumber() == cl
		}
	}
	testJob := &automationpb.Plan{Jobs: []*automationpb.PlannedJob{{Job: "test //lib:test", Why: "the test is affected",
		Spec: &automationpb.Job{Kind: automationpb.Job_KIND_TEST, Targets: []string{"//lib:test"}}}}}
	// Nothing the build reads changed since pre, where it passed: post carries that pass, and its
	// log is the pre run's.
	finish(take(postPlan(n)), automationpb.Run_STATUS_PASSED, &automationpb.Plan{Jobs: []*automationpb.PlannedJob{
		{Job: "build", Why: "every target the change affects must build", UnchangedSincePre: true,
			Spec: &automationpb.Job{Kind: automationpb.Job_KIND_BUILD, Targets: []string{"//lib:lib"}}},
		testJob.Jobs[0],
	}})
	job := take(postJob(n))
	if job.GetOrigin().GetJob() != "test //lib:test" {
		t.Fatalf("post ran %q, which pre's pass carries", job.GetOrigin().GetJob())
	}
	// The job runs, and the queue notes it is testing: two writes, which the tab shows as each lands.
	e.waitFor("the post job on the Checks tab", func() bool {
		_, detail, rows, _ := postOf(e.get(alice, checksPath))
		return len(rows) == 3 && rows[2].status == "Running" && detail == "Testing"
	})
	tab = e.get(alice, checksPath)
	status, detail, prow, attempts := postOf(tab)
	if status != "Submitting" || detail != "Testing" || attempts != 1 || prow[0].name != "plan" || prow[2].name != "//lib:test" ||
		prow[2].enabled || prow[2].title != "A post run is its submission's: cancel the submission" {
		t.Errorf("post while it tests: %q %q %+v, %d attempts", status, detail, prow, attempts)
	}
	if b := prow[1]; b.name != "build" || b.status != "Passed" || b.logHref != rebuilt || !tab.has("In Pre") {
		t.Errorf("post's build, carried from pre's %s: %+v", rebuilt, b)
	}
	finish(job, automationpb.Run_STATUS_PASSED, nil)
	e.waitFor("the CL to land", func() bool { return e.landed(path) })
	p = e.get(alice, path)
	if status, _, _, attempts := postOf(p); status != "Landed" || attempts != 1 || !logged(p, "alice", "submitted to main as") ||
		p.has(path+"/cancel-submit") {
		t.Errorf("the landed CL: %q, %d attempts\n%s", status, attempts, excerpt(p.body))
	}

	// A post that fails: the CL stays open, saying why; submitted again and canceled, it says so.
	a.jj("svc", "sync")
	a.jj("svc", "new", "main@ok", "-m", "two")
	a.write("svc/lib/two.go", "package lib\n")
	two := uploaded(a.jj("svc", "cl", "--ready"))[0]
	twoPath := clPath("svc", two)
	e.post(alice, twoPath+"/reply", grant("LGTM", "Approval"))
	e.waitFor("pre to pass on the second CL", func() bool {
		if r, err := e.work.Lease(ctx, &automationpb.LeaseRequest{Worker: "w", WaitSeconds: 1}); err == nil && r.GetRun() != nil {
			var pl *automationpb.Plan
			if r.GetRun().GetJob().GetKind() == automationpb.Job_KIND_PLAN {
				pl = &automationpb.Plan{}
			}
			finish(r.GetRun(), automationpb.Run_STATUS_PASSED, pl)
		}
		_, verdict, _, _ := checks(t, e.get(alice, twoPath+"/checks"))
		return verdict == "Passed"
	})
	if n := notice(e.post(alice, twoPath+"/submit", nil)); n != "" {
		t.Fatalf("submitting the second CL: %s", n)
	}
	finish(take(postPlan(two)), automationpb.Run_STATUS_PASSED, testJob)
	failing := take(postJob(two))
	if _, err := e.work.Finish(ctx, &automationpb.FinishRequest{RunId: failing.GetId(), Worker: "w", Status: automationpb.Run_STATUS_FAILED,
		ExitCode: 1, Reason: "bazel test //lib:test exited with status 1"}); err != nil {
		t.Fatal(err)
	}
	e.waitFor("the submission to fail", func() bool {
		status, _, _, _ := postOf(e.get(alice, twoPath+"/checks"))
		return status == "Failed"
	})
	p = e.get(alice, twoPath)
	if status, detail, _, _ := postOf(p); chip(p, "Submitting") || !chip(p, "Approved") || !canSubmit(p, "submit") ||
		detail != fmt.Sprintf("CL %d's test //lib:test failed: bazel test //lib:test exited with status 1", two) || status != "Failed" ||
		!logged(p, "ok", "could not land it") {
		t.Errorf("a CL whose post failed: %q %q\n%s", status, detail, excerpt(p.body))
	}
	if n := notice(e.post(alice, twoPath+"/submit", nil)); n != "" {
		t.Fatalf("submitting it again: %s", n)
	}
	if n := notice(e.post(bob, twoPath+"/cancel-submit", nil)); n != "Only the author or a repository admin, who may land on it, can cancel the submission of this changelist" {
		t.Errorf("bob canceling alice's submission: %q", n)
	}
	p = e.post(alice, twoPath+"/cancel-submit", nil)
	if status, detail, _, _ := postOf(p); chip(p, "Submitting") || status != "Canceled" || detail != "Canceled by alice" ||
		!logged(p, "ok", "stopped submitting it") {
		t.Errorf("a canceled submission: %q %q\n%s", status, detail, excerpt(p.body))
	}

	// A run is found only under its own CL.
	b := e.person(bob)
	b.jj("", "clone", "svc")
	b.write("svc/other.txt", "other\n")
	b.jj("svc", "describe", "-m", "another change")
	other := uploaded(b.jj("svc", "cl"))[0]
	if p := e.do(alice, http.MethodGet, strings.Replace(test.logHref, path, clPath("svc", other), 1), nil, nil); p.status != http.StatusNotFound {
		t.Errorf("a run under another CL: %d", p.status)
	}
	// A draft's tab says it runs no pre, and offers whoever may land — every member — to run it
	// by hand, which runs it.
	if p := e.get(alice, clPath("svc", other)+"/checks"); !p.has("A draft runs no pre until it is sent for review.") || !p.has("Run pre") {
		t.Errorf("a draft's Checks tab, to a member not its author:\n%s", p.body)
	}
	if p := e.do(bob, http.MethodPost, clPath("svc", other)+"/run-pre", url.Values{}, map[string]string{"HX-Request": "true", "HX-Target": "cl-checks"}); p.status != http.StatusOK ||
		p.has("A draft runs no pre") {
		t.Errorf("running a draft's pre by hand: %d\n%s", p.status, p.body)
	}

	// ok automation gone: the tab says so, and the page still renders.
	e.stopAutomation()
	p = e.get(alice, path)
	if _, verdict, _, _ := checks(t, p); verdict != "Unavailable" || !p.has("ok could not read this patchset's checks") ||
		!p.has("ok could not read this change's submissions") {
		t.Errorf("with automation gone: %q", verdict)
	}
}
