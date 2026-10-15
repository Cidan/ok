package e2e

import (
	"fmt"
	"strings"
	"testing"

	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// fixture is a Bazel workspace of Starlark rules alone: a library (//lib, a file), a test that
// reads it (passing while it says hello), a tool a workflow job runs in the sandbox — checking it
// cannot write the checkout or reach a network, can write its own TMPDIR, and is told its job
// context — and a formatter whose check fails on a file saying UNFORMATTED.
func fixture() map[string]*string {
	return map[string]*string{
		"MODULE.bazel": ptr(`module(name = "fixture")` + "\n"),
		".ok/repository.yaml": ptr(`build: bazel
bazel:
  lint:
    format: //tools/format:format
`),
		".ok/workflow/ci.yaml": ptr(`pre:
  sandbox:
    target: //app:tool
    args: ["--cl={cl}", "{changed_files}"]
`),
		"rules.bzl": ptr(`def _cat_impl(ctx):
    out = ctx.actions.declare_file(ctx.label.name + ".txt")
    ctx.actions.run_shell(
        inputs = ctx.files.srcs,
        outputs = [out],
        command = "cat %s > %s" % (" ".join([f.path for f in ctx.files.srcs]), out.path),
    )
    return [DefaultInfo(files = depset([out]))]

cat = rule(implementation = _cat_impl, attrs = {"srcs": attr.label_list(allow_files = True)})

def _script_impl(ctx):
    out = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(out, "#!/bin/sh\n" + ctx.attr.script, is_executable = True)
    return [DefaultInfo(executable = out, runfiles = ctx.runfiles(files = ctx.files.data))]

_attrs = {"script": attr.string(), "data": attr.label_list(allow_files = True)}

script_test = rule(implementation = _script_impl, test = True, attrs = _attrs)

script_binary = rule(implementation = _script_impl, executable = True, attrs = _attrs)
`),
		"BUILD.bazel": ptr(""),
		"lib/BUILD.bazel": ptr(`load("//:rules.bzl", "cat")

cat(
    name = "lib",
    srcs = ["a.txt"],
    visibility = ["//visibility:public"],
)
`),
		"lib/a.txt": ptr("hello\n"),
		"app/BUILD.bazel": ptr(`load("//:rules.bzl", "script_binary", "script_test")

script_test(
    name = "app_test",
    data = ["//lib"],
    script = "grep -q hello \"$TEST_SRCDIR/_main/lib/lib.txt\"\n",
)

script_binary(
    name = "tool",
    data = ["//lib"],
    script = """set -e
test -f "$OK_CONTEXT"
grep -q '"cl"' "$OK_CONTEXT"
if touch "$BUILD_WORKSPACE_DIRECTORY/written" 2>/dev/null; then echo "the tool wrote the checkout"; exit 1; fi
touch "$TMPDIR/scratch"
if [ "$(grep -c : /proc/net/dev)" != 1 ]; then echo "the tool has a network:"; cat /proc/net/dev; exit 1; fi
echo "the tool ran: $*"
""",
)
`),
		"tools/format/BUILD.bazel": ptr(`load("//:rules.bzl", "script_binary")

script_binary(
    name = "format.check",
    script = """for f in "$@"; do
  if grep -q UNFORMATTED "$BUILD_WORKSPACE_DIRECTORY/$f"; then echo "$f is not formatted"; exit 1; fi
done
echo "checked $# files"
""",
)
`),
	}
}

// jobs are a pre's jobs, by name.
func jobs(p *automationpb.Pre) map[string]*automationpb.Run {
	out := map[string]*automationpb.Run{}
	for _, j := range p.GetJobs() {
		out[j.GetOrigin().GetJob()] = j
	}
	return out
}

// newRepository makes a repository of the fixture, landed on main, and a checkout of it for ada.
func (e *env) newRepository(name string) (*person, int64) {
	e.t.Helper()
	a := e.person(ada)
	a.jj(".", "create", name, name)
	write(e.t, a.dir(name), fixture())
	a.jj(name, "describe", "-m", "the fixture")
	base := uploaded(a.jj(name, "cl", "--ready"))[0]
	repo := e.repository(name)
	e.land(repo, base, 1)
	a.jj(name, "sync")
	return a, repo
}

// TestPre drives pre as ok runs it: automation follows the SCM's change feed and plans each
// patchset uploaded — what it affects in the Bazel graph, the built-in jobs, and main's workflow
// jobs — on a worker running real Bazel, runs what it planned, and says whether pre passed.
func TestPre(t *testing.T) {
	e := start(t)
	a, repo := e.newRepository("pre")
	e.startWorker("w1", 1)

	// A change to the library: its build, the test that reads it, the formatter over the file,
	// and the workflow job whose tool reads it, in the sandbox.
	a.jj("pre", "new", "main@ok", "-m", "hello, world")
	write(t, a.dir("pre"), map[string]*string{"lib/a.txt": ptr("hello, world\n")})
	cl := uploaded(a.jj("pre", "cl"))[0]
	p := e.settled(repo, cl, 1)
	js := jobs(p)
	if p.GetVerdict() != automationpb.Pre_VERDICT_PASSED {
		t.Fatalf("a good change's pre: %s", e.describe(p))
	}
	for _, want := range []string{"build", "test //app:app_test", "lint", "//.ok/workflow/ci.yaml:pre:sandbox"} {
		if js[want] == nil {
			t.Errorf("pre did not run %s: %s", want, e.describe(p))
		}
	}
	if log := e.logOf(js["//.ok/workflow/ci.yaml:pre:sandbox"].GetId()); !strings.Contains(log, fmt.Sprintf("the tool ran: --cl=%d lib/a.txt", cl)) {
		t.Errorf("the workflow job's log:\n%s", log)
	}
	if log := e.logOf(js["lint"].GetId()); !strings.Contains(log, "checked 1 files") {
		t.Errorf("the lint's log:\n%s", log)
	}
	if log := e.logOf(p.GetPlan().GetId()); !strings.Contains(log, "the change affects") {
		t.Errorf("the plan's log:\n%s", log)
	}

	// Its next patchset breaks the test: pre fails, on that job.
	write(t, a.dir("pre"), map[string]*string{"lib/a.txt": ptr("goodbye\n")})
	a.jj("pre", "cl")
	p = e.settled(repo, cl, 2)
	if p.GetVerdict() != automationpb.Pre_VERDICT_FAILED || jobs(p)["test //app:app_test"].GetStatus() != automationpb.Run_STATUS_FAILED ||
		jobs(p)["build"].GetStatus() != automationpb.Run_STATUS_PASSED {
		t.Fatalf("a breaking patchset's pre: %s", e.describe(p))
	}

	// A file no package holds, unformatted: only the lint runs, and it fails; the workflow job,
	// whose tool the change does not affect, is left to main.
	a.jj("pre", "new", "main@ok", "-m", "notes")
	write(t, a.dir("pre"), map[string]*string{"docs/notes.md": ptr("UNFORMATTED\n")})
	cl = uploaded(a.jj("pre", "cl"))[0]
	p = e.settled(repo, cl, 1)
	js = jobs(p)
	if p.GetVerdict() != automationpb.Pre_VERDICT_FAILED || len(js) != 1 || js["lint"].GetStatus() != automationpb.Run_STATUS_FAILED {
		t.Fatalf("an unformatted file's pre: %s", e.describe(p))
	}
	if log := e.logOf(js["lint"].GetId()); !strings.Contains(log, "docs/notes.md is not formatted") {
		t.Errorf("the lint's log:\n%s", log)
	}

	// A workflow edit naming a target that does not exist fails the plan.
	a.jj("pre", "new", "main@ok", "-m", "a missing target")
	write(t, a.dir("pre"), map[string]*string{".ok/workflow/ci.yaml": ptr("pre:\n  nope:\n    target: //nope:nope\n")})
	cl = uploaded(a.jj("pre", "cl"))[0]
	p = e.settled(repo, cl, 1)
	if p.GetVerdict() != automationpb.Pre_VERDICT_FAILED || !strings.Contains(p.GetPlan().GetReason(), "target //nope:nope does not exist at this change") {
		t.Fatalf("a workflow edit naming a missing target: %s", e.describe(p))
	}

	// An OWNERS file only its one owner could approve fails the plan: no change to those files
	// could ever be approved. Naming everyone (*) with set allowself passes.
	a.jj("pre", "new", "main@ok", "-m", "a lone owner")
	write(t, a.dir("pre"), map[string]*string{"lib/OWNERS": ptr("user: ada\n")})
	cl = uploaded(a.jj("pre", "cl"))[0]
	p = e.settled(repo, cl, 1)
	if p.GetVerdict() != automationpb.Pre_VERDICT_FAILED || !strings.Contains(p.GetPlan().GetReason(), "ada alone owns these files") {
		t.Fatalf("an unapprovable OWNERS file: %s", e.describe(p))
	}
	write(t, a.dir("pre"), map[string]*string{"lib/OWNERS": ptr("user: ada\nset allowself\n")})
	a.jj("pre", "cl")
	if p = e.settled(repo, cl, 2); p.GetVerdict() != automationpb.Pre_VERDICT_PASSED ||
		!strings.Contains(e.logOf(p.GetPlan().GetId()), "every set can be approved by its own people") {
		t.Fatalf("an approvable OWNERS file: %s", e.describe(p))
	}

	// A BUILD file that does not parse leaves no graph: the plan fails, saying so.
	a.jj("pre", "new", "main@ok", "-m", "broken")
	write(t, a.dir("pre"), map[string]*string{"app/BUILD.bazel": ptr("script_test(\n")})
	cl = uploaded(a.jj("pre", "cl"))[0]
	p = e.settled(repo, cl, 1)
	if p.GetVerdict() != automationpb.Pre_VERDICT_FAILED || !strings.Contains(p.GetPlan().GetReason(), "the change leaves no build graph Bazel can load") {
		t.Fatalf("a broken BUILD file: %s", e.describe(p))
	}
}
