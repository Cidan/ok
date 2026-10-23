package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/serve/metrics"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// postFixture is the fixture with a package nothing else depends on (//other, its own test), and
// a post job running the tool on the change it lands.
func postFixture() map[string]*string {
	f := fixture()
	f[".ok/workflow/ci.yaml"] = ptr(`post:
  later:
    target: //app:tool
    args: ["--cl={cl}", "--patchset={patchset}"]
`)
	f["other/BUILD.bazel"] = ptr(`load("//:rules.bzl", "cat", "script_test")

cat(
    name = "other",
    srcs = ["b.txt"],
)

script_test(
    name = "other_test",
    data = [":other"],
    script = "grep -q answer \"$TEST_SRCDIR/_main/other/other.txt\"\n",
)
`)
	f["other/b.txt"] = ptr("the answer is 42\n")
	return f
}

// TestPost drives the landing queue as ok control does: submissions handed to automation, post
// run on each one's candidate by a worker running real Bazel, and main moving only on what passed —
// independent submissions landing without running again, a conflicting one tested on top of the
// one ahead of it, a failure ahead rebasing what speculated on it, a path conflict failing the
// later submission, a stack landing whole, a new patchset canceling a submission, a change whose
// pre passed landing with every post job carried from pre though main moved under it, and an
// imported repository's change landing with no job. One worker, so the queue's decisions repeat.
func TestPost(t *testing.T) {
	e := start(t)
	a := e.person(ada)
	a.jj(".", "create", "post", "post")
	write(t, a.dir("post"), postFixture())
	a.jj("post", "describe", "-m", "the fixture")
	base := uploaded(a.jj("post", "cl", "--ready"))[0]
	repo := e.repository("post")
	e.land(repo, base, 1)
	a.jj("post", "sync")
	w1 := e.startWorker("w1", 1)
	fix := postFixture()

	change := func(msg string, files map[string]*string) int64 {
		t.Helper()
		a.jj("post", "new", "main@ok", "-m", msg)
		write(t, a.dir("post"), files)
		return uploaded(a.jj("post", "cl", "--ready"))[0]
	}
	submit := func(numbers ...int64) int64 {
		t.Helper()
		req := &automationpb.SubmitRequest{RepositoryId: repo, ForPersonId: e.adaID}
		for _, n := range numbers {
			req.Cls = append(req.Cls, &automationpb.SubmitRequest_CL{Number: n, Patchset: e.cl(repo, n).GetCurrentPatchset()})
		}
		sub, err := e.runs.Submit(e.ctx, req)
		if err != nil {
			t.Fatalf("submitting %v: %v", numbers, err)
		}
		return sub.GetId()
	}
	describe := func(s *automationpb.Submission) string {
		var b strings.Builder
		fmt.Fprintf(&b, "submission %d: %s (%s) note %q\n", s.GetId(), s.GetStatus(), s.GetReason(), s.GetNote())
		for _, at := range s.GetAttempts() {
			fmt.Fprintf(&b, "  attempt %d: %s %s on %v (%s)\n", at.GetId(), at.GetStatus(), at.GetVerdict(), at.GetChain(), at.GetReason())
			for _, c := range at.GetCandidates() {
				fmt.Fprintf(&b, "    CL %d plan %s (%s)\n%s", c.GetClNumber(), c.GetPlan().GetStatus(), c.GetPlan().GetReason(), e.logOf(c.GetPlan().GetId()))
				for _, j := range c.GetJobs() {
					fmt.Fprintf(&b, "    job %s: %s (%s)\n%s", j.GetOrigin().GetJob(), j.GetStatus(), j.GetReason(), e.logOf(j.GetId()))
				}
			}
		}
		return b.String()
	}
	finished := func(id int64) *automationpb.Submission {
		t.Helper()
		deadline := time.Now().Add(10 * time.Minute)
		for {
			s, err := e.runs.GetSubmission(e.ctx, &automationpb.GetSubmissionRequest{Id: id})
			if err != nil {
				t.Fatal(err)
			}
			if s.GetStatus() != automationpb.Submission_STATUS_ACTIVE {
				return s
			}
			if time.Now().After(deadline) {
				t.Fatalf("submission %d did not finish:\n%s", id, describe(s))
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	landed := func(what string, id int64) *automationpb.Submission {
		t.Helper()
		s := finished(id)
		if s.GetStatus() != automationpb.Submission_STATUS_LANDED {
			t.Fatalf("%s did not land:\n%s", what, describe(s))
		}
		for _, c := range s.GetCls() {
			got := e.cl(repo, c.GetNumber())
			if got.GetStatus() != scmpb.ChangeListStatus_CHANGE_LIST_STATUS_SUBMITTED || string(got.GetLandedCommit()) != string(c.GetLandedCommit()) {
				t.Errorf("%s: CL %d in the SCM: %v", what, c.GetNumber(), got)
			}
		}
		return s
	}
	landedOn := func(s *automationpb.Submission) *automationpb.Attempt {
		for _, at := range s.GetAttempts() {
			if at.GetStatus() == automationpb.Attempt_STATUS_LANDED {
				return at
			}
		}
		return nil
	}
	chainOf := func(at *automationpb.Attempt) []int64 {
		var out []int64
		for _, c := range at.GetChain() {
			out = append(out, c.GetSubmissionId())
		}
		return out
	}

	// Round one: A changes the library; B, the package nothing depends on; C, a package built on
	// the library.
	ca := change("hello, A", map[string]*string{"lib/a.txt": ptr("hello, A\n")})
	cb := change("another answer", map[string]*string{"other/b.txt": ptr("the answer is still 42\n")})
	cc := change("a tool, described", map[string]*string{"app/BUILD.bazel": ptr("# The app: a test and a tool.\n" + *fix["app/BUILD.bazel"])})
	sa, sb, sc := submit(ca), submit(cb), submit(cc)

	// A and B are independent: each is tested once, on main, and lands.
	subA, subB := landed("A", sa), landed("B", sb)
	for what, s := range map[string]*automationpb.Submission{"A": subA, "B": subB} {
		if len(s.GetAttempts()) != 1 || len(chainOf(landedOn(s))) != 0 {
			t.Errorf("%s was tested more than once, or speculatively:\n%s", what, describe(s))
		}
	}
	// A's post ran its workflow's post job, told it runs for A.
	var later *automationpb.Run
	for _, j := range landedOn(subA).GetCandidates()[0].GetJobs() {
		if j.GetOrigin().GetJob() == "//.ok/workflow/ci.yaml:post:later" {
			later = j
		}
	}
	if later == nil || later.GetStatus() != automationpb.Run_STATUS_PASSED || later.GetOrigin().GetPhase() != automationpb.Origin_PHASE_POST ||
		!strings.Contains(e.logOf(later.GetId()), fmt.Sprintf("the tool ran: --cl=%d --patchset=1", ca)) {
		t.Errorf("A's post job:\n%s", describe(subA))
	}
	// C conflicts with A: it was tested on top of A, and that result landed.
	subC := landed("C", sc)
	if on := chainOf(landedOn(subC)); len(on) != 1 || on[0] != sa {
		t.Errorf("C did not land on its speculation on A:\n%s", describe(subC))
	}

	// Round two: E breaks the library's test; F changes the package built on it; G and H change
	// the same file.
	a.jj("post", "sync")
	ce := change("goodbye", map[string]*string{"lib/a.txt": ptr("goodbye\n")})
	cf := change("a tool, described again", map[string]*string{"app/BUILD.bazel": ptr("# The app, again.\n" + *fix["app/BUILD.bazel"])})
	cg := change("the answer, explained", map[string]*string{"other/b.txt": ptr("the answer is 42: six times seven\n")})
	ch := change("the answer, doubted", map[string]*string{"other/b.txt": ptr("the answer is 42, or is it?\n")})
	se, sf, sg, sh := submit(ce), submit(cf), submit(cg), submit(ch)

	subE := finished(se)
	if subE.GetStatus() != automationpb.Submission_STATUS_FAILED || !strings.Contains(subE.GetReason(), fmt.Sprintf("CL %d's test //app:app_test failed", ce)) {
		t.Errorf("E:\n%s", describe(subE))
	}
	// F speculated on E; E failed, so F was tested again without it, and landed.
	subF := landed("F", sf)
	speculated := false
	for _, at := range subF.GetAttempts() {
		if on := chainOf(at); len(on) == 1 && on[0] == se && at.GetStatus() == automationpb.Attempt_STATUS_DISCARDED {
			speculated = true
		}
	}
	if !speculated || len(chainOf(landedOn(subF))) != 0 {
		t.Errorf("F:\n%s", describe(subF))
	}
	// G lands; H, which changes the same file, cannot.
	landed("G", sg)
	subH := finished(sh)
	if subH.GetStatus() != automationpb.Submission_STATUS_FAILED || !strings.Contains(subH.GetReason(), fmt.Sprintf("CL %d conflicts with main in other/b.txt", ch)) {
		t.Errorf("H:\n%s", describe(subH))
	}

	// Round three: a stack lands whole; a submission a new patchset supersedes is canceled.
	a.jj("post", "sync")
	a.jj("post", "new", "main@ok", "-m", "more")
	write(t, a.dir("post"), map[string]*string{"lib/more.txt": ptr("more\n")})
	a.jj("post", "new", "-m", "even more")
	write(t, a.dir("post"), map[string]*string{"lib/even.txt": ptr("even more\n")})
	stack := uploaded(a.jj("post", "cl", "--ready"))
	if len(stack) != 2 {
		t.Fatalf("the stack: %v", stack)
	}
	sstack := submit(stack...)
	cx := change("the answer, restated", map[string]*string{"other/b.txt": ptr("the answer is six times seven\n")})
	sx := submit(cx)
	write(t, a.dir("post"), map[string]*string{"other/b.txt": ptr("the answer is seven times six\n")})
	a.jj("post", "cl")
	subX := finished(sx)
	if subX.GetStatus() != automationpb.Submission_STATUS_CANCELED || subX.GetReason() != fmt.Sprintf("patchset 2 of CL %d was uploaded while it was submitting", cx) {
		t.Errorf("X:\n%s", describe(subX))
	}
	subS := landed("the stack", sstack)
	// Post is planned once, at the stack's top, over both CLs' paths; the CL under it lands with
	// it and runs nothing of its own.
	if on := landedOn(subS); len(on.GetCandidates()) != 2 || on.GetCandidates()[0].GetPlan() != nil || len(on.GetCandidates()[0].GetJobs()) != 0 ||
		on.GetCandidates()[1].GetPlan().GetStatus() != automationpb.Run_STATUS_PASSED {
		t.Errorf("the stack's post was not planned once, at its top:\n%s", describe(subS))
	} else if log := e.logOf(on.GetCandidates()[1].GetPlan().GetId()); !strings.Contains(log, "the change touches 2 paths") {
		t.Errorf("the stack's post did not plan both CLs' paths:\n%s", log)
	}

	// Round four: a change whose pre passed, with main moving under it before it is submitted —
	// but not under anything it builds on. Post carries pre's pass for every job, and runs none.
	a.jj("post", "sync")
	ck := change("the answer, at last", map[string]*string{"other/b.txt": ptr("the answer is 42, at last\n")})
	preK := e.settled(repo, ck, 1)
	if preK.GetVerdict() != automationpb.Pre_VERDICT_PASSED {
		t.Fatalf("K's pre: %s", e.describe(preK))
	}
	landed("a change under it", submit(change("hello, under", map[string]*string{"lib/under.txt": ptr("under\n")})))
	subK := landed("K", submit(ck))
	on := landedOn(subK).GetCandidates()[0]
	preRuns := map[string]int64{}
	for _, j := range preK.GetJobs() {
		preRuns[j.GetOrigin().GetJob()] = j.GetId()
	}
	var carried []string
	for _, j := range on.GetJobs() {
		if from := preRuns[j.GetOrigin().GetJob()]; from == 0 || j.GetCarriedFrom() != from || j.GetStatus() != automationpb.Run_STATUS_PASSED {
			t.Errorf("K's post %s: carried from %d, want pre's %d (%s)", j.GetOrigin().GetJob(), j.GetCarriedFrom(), from, j.GetStatus())
		}
		carried = append(carried, j.GetOrigin().GetJob())
	}
	// //other's library is built by its test, its one root: nothing else to build.
	if want := []string{"test //other:other_test", "lint"}; strings.Join(carried, ",") != strings.Join(want, ",") ||
		!strings.Contains(e.logOf(on.GetPlan().GetId()), "since pre, on patchset 1's commit") {
		t.Errorf("K's post carried %v, want %v:\n%s", carried, want, describe(subK))
	}

	// Round four, in a batch: two independent changes whose pre passed, both planned while the
	// queue was held — with no worker until both were prepared, so neither could pass alone — are
	// released in one pass, pass at once on pre's results, and land in one landing: one
	// transaction in the SCM, main moved once, each submission recorded landed.
	a.jj("post", "sync")
	n1 := change("a first note", map[string]*string{"notes/one.txt": ptr("one\n")})
	n2 := change("a second note", map[string]*string{"notes/two.txt": ptr("two\n")})
	for _, n := range []int64{n1, n2} {
		if p := e.settled(repo, n, 1); p.GetVerdict() != automationpb.Pre_VERDICT_PASSED {
			t.Fatalf("CL %d's pre: %s", n, e.describe(p))
		}
	}
	w1.stop()
	s1, s2 := submit(n1), submit(n2)
	attempted := func(id int64, planned bool) bool {
		s, err := e.runs.GetSubmission(e.ctx, &automationpb.GetSubmissionRequest{Id: id})
		if err != nil {
			t.Fatal(err)
		}
		for _, at := range s.GetAttempts() {
			if at.GetStatus() != automationpb.Attempt_STATUS_ACTIVE {
				continue
			}
			if !planned {
				return true
			}
			for _, c := range at.GetCandidates() {
				if c.GetPlan().GetStatus() != automationpb.Run_STATUS_PASSED {
					return false
				}
			}
			return len(at.GetCandidates()) > 0
		}
		return false
	}
	waitFor := func(what string, done func() bool) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Minute); !done(); time.Sleep(200 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("waiting for %s", what)
			}
		}
	}
	waitFor("both prepared", func() bool { return attempted(s1, false) && attempted(s2, false) })
	letGo := e.holdQueue(repo)
	e.startWorker("w2", 1)
	waitFor("both planned", func() bool { return attempted(s1, true) && attempted(s2, true) })
	before := []float64{metrics.Default().Read("ok_automation_landings_total").Value,
		metrics.Default().Read("ok_automation_landed_submissions_total").Value}
	letGo()
	b1, b2 := landed("the first note", s1), landed("the second note", s2)
	if got := []float64{metrics.Default().Read("ok_automation_landings_total").Value - before[0],
		metrics.Default().Read("ok_automation_landed_submissions_total").Value - before[1]}; got[0] != 1 || got[1] != 2 {
		t.Errorf("the two landed in %v landings, %v submissions: want one landing of both", got[0], got[1])
	}
	one, two := b1.GetCls()[0].GetLandedCommit(), b2.GetCls()[0].GetLandedCommit()
	if parents := e.parentsOf(repo, two); len(parents) != 1 || string(parents[0]) != string(one) {
		t.Errorf("the second note landed on %x, not on the first's %x", parents, one)
	}

	// Round five, in an imported repository: its change lands with nothing to run, whatever its
	// files hold.
	a.jj(".", "create", "imp", "imp")
	write(t, a.dir("imp"), map[string]*string{".ok/repository.yaml": ptr("build: none\n"), "BUILD.bazel": ptr("not starlark (\n")})
	a.jj("imp", "describe", "-m", "imported as it was")
	first := uploaded(a.jj("imp", "cl", "--ready"))[0]
	repo = e.repository("imp")
	subI := landed("an imported repository's change", submit(first))
	if c := landedOn(subI).GetCandidates(); len(c) != 1 || c[0].GetPlan().GetStatus() != automationpb.Run_STATUS_PASSED || len(c[0].GetJobs()) != 0 ||
		!strings.Contains(e.logOf(c[0].GetPlan().GetId()), "the repository is imported") {
		t.Errorf("an imported repository's post:\n%s", describe(subI))
	}
}
