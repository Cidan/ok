package plan

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/workflow"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// The change: lib/lib.go modified, docs/old.md renamed to docs/new.md, gone/gone.go deleted.
func change() *workflow.Context {
	return &workflow.Context{
		Change: workflow.Change{Phase: workflow.Pre, CL: 7, Patchset: 2, Commit: "c0", Base: "b0"},
		Files: []workflow.ChangedFile{
			{Path: "lib/lib.go", Status: workflow.Modified},
			{Path: "docs/new.md", Status: workflow.Renamed, OldPath: "docs/old.md"},
			{Path: "gone/gone.go", Status: workflow.Deleted},
		},
	}
}

// What the Bazel graph says the change affects: //lib and //gone directly, //app and its test through
// deps; //base is upstream.
func affected() *Affected {
	return &Affected{
		Changed: []string{"docs/new.md", "docs/old.md", "gone/gone.go", "lib/lib.go"},
		Affected: []Target{
			{Label: "//app:app", Kind: "go_binary", Binary: true, Root: true, Because: []Step{{Label: "//app:app", Via: "deps"}, {Label: "//lib:lib", File: "lib/lib.go"}}},
			{Label: "//app:test", Kind: "go_test", Test: true, Root: true, Because: []Step{{Label: "//app:test", Via: "deps"}, {Label: "//lib:lib", File: "lib/lib.go"}}},
			{Label: "//gone:gone", Kind: "go_library", Direct: true, Root: true, Because: []Step{{Label: "//gone:gone", File: "gone/gone.go"}}},
			{Label: "//lib:lib", Kind: "go_library", Direct: true, Because: []Step{{Label: "//lib:lib", File: "lib/lib.go"}}},
		},
		Tests:     []string{"//app:test"},
		Upstream:  []string{"//base:base"},
		LintTools: []string{"//tools/lint:linters.bzl"},
	}
}

var mainWorkflows = map[string][]byte{
	// The root's: governs docs/, and everything without closer workflows.
	".ok/workflow/root.yaml": []byte("pre:\n  docs:\n    target: //docs:check\n    args: [\"{changed_files:docs/**}\"]\npost:\n  late:\n    target: //app:app\n"),
	// lib's: governs lib/lib.go.
	"lib/.ok/workflow/lib.yaml": []byte("pre:\n  e2e:\n    target: //app:test\n    args: [-test.v, \"--cl={cl}\"]\n"),
	// base's: selected because the change builds on //base, but its target is not affected.
	"base/.ok/workflow/base.yaml": []byte("pre:\n  bench:\n    target: //base:bench\n"),
	// far's: nothing of the change is under far/, but its job's target is affected.
	"far/.ok/workflow/far.yaml": []byte("pre:\n  smoke:\n    target: //app:app\n    args: [--smoke, \"{changed_files:lib/**}\"]\n"),
}

func described(labels ...Description) *Described {
	return &Described{Targets: labels}
}

var allTargets = described(
	Description{Label: "//app:app", Kind: "go_binary", Binary: true},
	Description{Label: "//app:test", Kind: "go_test", Test: true},
	Description{Label: "//base:bench", Kind: "go_test", Test: true},
	Description{Label: "//docs:check", Kind: "go_binary", Binary: true},
)

func jobs(pl *automationpb.Plan) map[string]*automationpb.PlannedJob {
	out := map[string]*automationpb.PlannedJob{}
	for _, j := range pl.GetJobs() {
		out[j.GetJob()] = j
	}
	return out
}

func TestPlan(t *testing.T) {
	p := New(Input{Context: change(), Affected: affected(), Main: mainWorkflows})
	if want := []string{"//app:app", "//app:test", "//base:bench", "//docs:check"}; !reflect.DeepEqual(p.Targets(), want) {
		t.Fatalf("targets %v, want %v", p.Targets(), want)
	}
	pl, problems := p.Plan(allTargets)
	if len(problems) != 0 {
		t.Fatalf("problems: %v", problems)
	}
	js := jobs(pl)
	var ids []string
	for _, j := range pl.GetJobs() {
		ids = append(ids, j.GetJob())
	}
	want := []string{
		"build", "test //app:test", "lint",
		"//lib/.ok/workflow/lib.yaml:pre:e2e",
		"//far/.ok/workflow/far.yaml:pre:smoke",
	}
	if !reflect.DeepEqual(ids, want) {
		t.Errorf("jobs %v, want %v", ids, want)
	}

	// The build builds the affected roots that are not tests; each test is its own job.
	if b := js["build"].GetSpec(); b.GetKind() != automationpb.Job_KIND_BUILD || !reflect.DeepEqual(b.GetTargets(), []string{"//app:app", "//gone:gone"}) {
		t.Errorf("build: %v", b)
	}
	if j := js["test //app:test"]; j.GetSpec().GetKind() != automationpb.Job_KIND_TEST || j.GetWhy() != "the test is affected: //app:test —deps→ //lib:lib, which holds lib/lib.go" {
		t.Errorf("the test: %v", j)
	}
	// Lint names the target that held the deleted file, and no files: its worker reads the
	// change's, which the why counts.
	if l := js["lint"]; !reflect.DeepEqual(l.GetSpec().GetTargets(), []string{"//gone:gone"}) ||
		l.GetWhy() != "the change's 2 files, and the 1 target that held what it removes" {
		t.Errorf("lint: %v", l)
	}

	// A workflow job's kind follows its target's; its args stay as written, for the worker to
	// expand.
	if e := js["//lib/.ok/workflow/lib.yaml:pre:e2e"]; e.GetSpec().GetKind() != automationpb.Job_KIND_TEST ||
		!reflect.DeepEqual(e.GetSpec().GetArgs(), []string{"-test.v", "--cl={cl}"}) || !strings.Contains(e.GetWhy(), "lib/lib.go changed, and //lib's workflows govern it") {
		t.Errorf("e2e: %v", e)
	}
	if s := js["//far/.ok/workflow/far.yaml:pre:smoke"]; s.GetSpec().GetKind() != automationpb.Job_KIND_RUN ||
		!reflect.DeepEqual(s.GetSpec().GetArgs(), []string{"--smoke", "{changed_files:lib/**}"}) || !strings.HasPrefix(s.GetWhy(), "its target is affected") {
		t.Errorf("smoke: %v", s)
	}
	// post jobs are not pre's.
	if _, ok := js["//.ok/workflow/root.yaml:post:late"]; ok {
		t.Error("a post job was planned for pre")
	}
}

// TestPlanGraduates: a change that makes an imported repository a Bazel workspace is planned
// as any other, but from its own workflow files — every one checked — not main's, which nothing
// checked while it was imported; and its lint is the whole repository's, none of which was
// ever linted.
func TestPlanGraduates(t *testing.T) {
	imported := map[string][]byte{".ok/workflow/old.yaml": []byte("pre:\n  nope:\n    target: //nope:nope\n")}
	p := New(Input{Context: change(), Affected: affected(), Main: imported, Change: mainWorkflows, Standing: workflow.Graduates})
	if want := []string{"//app:app", "//app:test", "//base:bench", "//docs:check"}; !reflect.DeepEqual(p.Targets(), want) {
		t.Errorf("targets %v, want %v: every workflow job at the change", p.Targets(), want)
	}
	pl, problems := p.Plan(allTargets)
	if len(problems) != 0 {
		t.Fatalf("problems: %v", problems)
	}
	js := jobs(pl)
	if js["build"] == nil || js["test //app:test"] == nil || js["//lib/.ok/workflow/lib.yaml:pre:e2e"] == nil {
		t.Errorf("the build, the test, and the change's workflow job were not planned: %v", pl.GetJobs())
	}
	// A workflow file the change does not touch is checked like the rest.
	broken := map[string][]byte{"far/.ok/workflow/far.yaml": []byte("pre:\n  smoke:\n    target: //nope:nope\n")}
	if _, problems := New(Input{Context: change(), Affected: affected(), Change: broken, Standing: workflow.Graduates}).Plan(allTargets); len(problems) == 0 ||
		!strings.Contains(strings.Join(problems, "\n"), "far/.ok/workflow/far.yaml:2: job smoke: target //nope:nope does not exist at this change") {
		t.Errorf("an untouched broken workflow file: %v", problems)
	}
	if l := js["lint"].GetSpec(); l.GetKind() != automationpb.Job_KIND_LINT || !reflect.DeepEqual(l.GetTargets(), []string{"//..."}) {
		t.Errorf("lint: %v, want the whole repository's", l)
	}
	if why := js["lint"].GetWhy(); !strings.Contains(why, "every file in it is linted") {
		t.Errorf("lint's why: %s", why)
	}
}

func TestPlanSkipped(t *testing.T) {
	a := affected()
	p := New(Input{Context: change(), Affected: a, Main: mainWorkflows})
	pl, problems := p.Plan(allTargets)
	if len(problems) != 0 {
		t.Fatalf("problems: %v", problems)
	}
	skipped := map[string]*automationpb.SkippedJob{}
	for _, s := range pl.GetSkipped() {
		skipped[s.GetJob()] = s
	}
	if s := skipped["//base/.ok/workflow/base.yaml:pre:bench"]; s == nil || s.GetTarget() != "//base:bench" ||
		!strings.Contains(s.GetWhy(), "the change builds on //base:base") || !strings.Contains(s.GetWhy(), "its result on main stands") {
		t.Errorf("bench: %v", s)
	}
	if s := skipped["//.ok/workflow/root.yaml:pre:docs"]; s == nil || !strings.Contains(s.GetWhy(), "docs/new.md changed, and //'s workflows govern it") {
		t.Errorf("the docs job's target is not affected, but it was not skipped: %v", s)
	}
	if _, ok := jobs(pl)["//.ok/workflow/root.yaml:pre:docs"]; ok {
		t.Error("the docs job both ran and was skipped")
	}
}

func TestPlanProblems(t *testing.T) {
	// A graph with problems fails the plan before the graph describes anything.
	bad := affected()
	bad.Problems = []string{`lib/build.yaml: kind "go_libary" is unknown`}
	p := New(Input{Context: change(), Affected: bad, Main: mainWorkflows})
	if _, problems := p.Plan(&Described{}); len(problems) != 1 || !strings.Contains(problems[0], "the build graph: lib/build.yaml") {
		t.Errorf("an invalid graph: %v", problems)
	}

	// A job on main whose target the change removes, and one with args its target cannot take.
	main := map[string][]byte{
		"lib/.ok/workflow/lib.yaml": []byte("pre:\n  e2e:\n    target: //app:test\n  gen:\n    target: //lib:lib\n    args: [-x]\n"),
	}
	p = New(Input{Context: change(), Affected: affected(), Main: main})
	_, problems := p.Plan(described(Description{Label: "//lib:lib", Kind: "go_library"}))
	got := strings.Join(problems, "\n")
	for _, want := range []string{
		"lib/.ok/workflow/lib.yaml:2: job e2e (on main): target //app:test does not exist at this change",
		"lib/.ok/workflow/lib.yaml:4: job gen (on main): target //lib:lib is a go_library, which builds and takes no args",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("problems lack %q:\n%s", want, got)
		}
	}
}

func TestPlanChecksEditedWorkflows(t *testing.T) {
	c := change()
	c.Files = append(c.Files,
		workflow.ChangedFile{Path: "lib/.ok/workflow/lib.yaml", Status: workflow.Modified},
		workflow.ChangedFile{Path: ".ok/workflow/common.yaml", Status: workflow.Deleted},
	)
	at := map[string][]byte{
		// Edited: its target does not exist at the change.
		"lib/.ok/workflow/lib.yaml": []byte("pre:\n  e2e:\n    target: //lib:nope\n"),
		// Not edited, but it includes the file the change deletes.
		"app/.ok/workflow/app.yaml": []byte("include:\n  - //.ok/workflow/common.yaml\npre:\n  a:\n    target: //app:app\n"),
		// Neither: not checked.
		"far/.ok/workflow/far.yaml": []byte("pre:\n  f:\n    target: //far:gone\n"),
	}
	p := New(Input{Context: c, Affected: affected(), Main: map[string][]byte{}, Change: at})
	if want := []string{"//app:app", "//lib:nope"}; !reflect.DeepEqual(p.Targets(), want) {
		t.Errorf("targets %v, want %v", p.Targets(), want)
	}
	_, problems := p.Plan(described(Description{Label: "//app:app", Kind: "go_binary", Binary: true}))
	got := strings.Join(problems, "\n")
	for _, want := range []string{
		"app/.ok/workflow/app.yaml:2: include //.ok/workflow/common.yaml does not exist",
		"lib/.ok/workflow/lib.yaml:2: job e2e: target //lib:nope does not exist at this change",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("problems lack %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "far") {
		t.Errorf("a workflow file the change does not touch was checked:\n%s", got)
	}
}

// A deploy a change adds or edits must name existing targets of the right kinds — migrations
// binaries, images oci_push targets — and no workload may have two deploys.
func TestPlanChecksDeploys(t *testing.T) {
	c := change()
	c.Files = append(c.Files, workflow.ChangedFile{Path: "app/.ok/workflow/app.yaml", Status: workflow.Added})
	at := map[string][]byte{
		"app/.ok/workflow/app.yaml": []byte(`deploy:
  app:
    migrate: [//app/schema:apply, //app:lib]
    images:
      app: //app:push
      helper: //app:missing
      sidecar: //app:image
    workload: {namespace: app, deployment: app}
  data:
    migrate: [//app:bucket-apply, //identity:apply]
`),
		// Not edited, but it rolls the same workload.
		"web/.ok/workflow/web.yaml": []byte("deploy:\n  app-too:\n    images: {app: //web:push}\n    workload: {namespace: app, deployment: app}\n"),
	}
	p := New(Input{Context: c, Affected: affected(), Main: map[string][]byte{}, Change: at})
	if want := []string{"//app/schema:apply", "//app:bucket-apply", "//app:image", "//app:lib", "//app:missing", "//app:push", "//identity:apply"}; !reflect.DeepEqual(p.Targets(), want) {
		t.Errorf("targets %v, want %v", p.Targets(), want)
	}
	_, problems := p.Plan(described(
		Description{Label: "//app/schema:apply", Kind: "go_binary", Binary: true},
		Description{Label: "//app:lib", Kind: "go_library"},
		Description{Label: "//app:bucket-apply", Kind: "go_binary", Binary: true},
		Description{Label: "//app:push", Kind: "oci_push"},
		Description{Label: "//app:image", Kind: "oci_image"},
		Description{Label: "//identity:apply", Kind: "sh_binary", Binary: true},
	))
	got := strings.Join(problems, "\n")
	for _, want := range []string{
		"web/.ok/workflow/web.yaml:4: deploy app-too rolls deployment app/app, which deploy app (//app/.ok/workflow/app.yaml:deploy:app) already rolls",
		"app/.ok/workflow/app.yaml:3: deploy app: migration //app:lib is a go_library, not a binary that speaks the migrate protocol",
		"app/.ok/workflow/app.yaml:6: deploy app: container helper's image //app:missing does not exist at this change",
		"app/.ok/workflow/app.yaml:7: deploy app: container sidecar's image //app:image is a oci_image, not oci_push",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("problems lack %q:\n%s", want, got)
		}
	}
	if len(problems) != 4 {
		t.Errorf("%d problems:\n%s", len(problems), got)
	}
}

func TestPlanNothingAffected(t *testing.T) {
	// A change to a file no package holds builds and tests nothing, and lints the file.
	c := &workflow.Context{Change: workflow.Change{Phase: workflow.Pre, CL: 1, Patchset: 1}, Files: []workflow.ChangedFile{{Path: "README.md", Status: workflow.Modified}}}
	p := New(Input{Context: c, Affected: &Affected{Changed: []string{"README.md"}, LintTools: []string{"//tools/lint:linters.bzl"}}})
	pl, problems := p.Plan(&Described{})
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if got := jobs(pl); len(got) != 1 || got["lint"] == nil {
		t.Errorf("jobs %v", pl.GetJobs())
	}
}

// A repository that names no linters gets no lint job.
func TestPlanNoLinters(t *testing.T) {
	a := affected()
	a.LintTools = nil
	pl, problems := New(Input{Context: change(), Affected: a, Main: mainWorkflows}).Plan(allTargets)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if js := jobs(pl); js["lint"] != nil || js["build"] == nil {
		t.Errorf("jobs without linters: %v", pl.GetJobs())
	}
}

// Post selects main's post jobs, never pre's, and leaves workflow edits to pre, which already
// checked them.
func TestPlanPost(t *testing.T) {
	c := change()
	c.Phase = workflow.Post
	c.Files = append(c.Files, workflow.ChangedFile{Path: "lib/.ok/workflow/lib.yaml", Status: workflow.Modified})
	at := map[string][]byte{"lib/.ok/workflow/lib.yaml": []byte("pre:\n  e2e:\n    target: //lib:nope\n")}
	p := New(Input{Context: c, Affected: affected(), Main: mainWorkflows, Change: at})
	if want := []string{"//app:app"}; !reflect.DeepEqual(p.Targets(), want) {
		t.Errorf("targets %v, want %v", p.Targets(), want)
	}
	pl, problems := p.Plan(described(Description{Label: "//app:app", Kind: "go_binary", Binary: true}))
	if len(problems) != 0 {
		t.Fatalf("problems: %v", problems)
	}
	var ids []string
	for _, j := range pl.GetJobs() {
		ids = append(ids, j.GetJob())
	}
	if want := []string{"build", "test //app:test", "lint", "//.ok/workflow/root.yaml:post:late"}; !reflect.DeepEqual(ids, want) {
		t.Errorf("jobs %v, want %v", ids, want)
	}
	if late := jobs(pl)["//.ok/workflow/root.yaml:post:late"]; late.GetSpec().GetKind() != automationpb.Job_KIND_RUN ||
		!strings.Contains(late.GetWhy(), "govern it; its target is affected: //app:app") {
		t.Errorf("the post job: %v", late)
	}
}

// Post marks each job whose inputs nothing changed between the commit pre ran on and the
// candidate: a pass of it in pre holds. A run job and a job whose arguments name the change
// never are; the build splits into what pre built on the same inputs and what changed.
func TestPlanPostSincePre(t *testing.T) {
	post := func() *workflow.Context {
		c := change()
		c.Phase = workflow.Post
		return c
	}
	main := map[string][]byte{".ok/workflow/root.yaml": []byte("post:\n" +
		"  same:\n    target: //app:test\n" +
		"  named:\n    target: //app:test\n    args: [\"--cl={cl}\"]\n" +
		"  late:\n    target: //app:app\n")}
	targets := described(Description{Label: "//app:app", Kind: "go_binary", Binary: true}, Description{Label: "//app:test", Kind: "go_test", Test: true})
	withTools := func() *Affected {
		a := affected()
		a.LintTools = []string{"//tools/lint:linters.bzl", "//tools/format:format"}
		return a
	}
	plan := func(a *Affected, since *Affected) (map[string]*automationpb.PlannedJob, []string) {
		t.Helper()
		p := New(Input{Context: post(), Affected: a, Main: main, SincePre: since})
		pl, problems := p.Plan(targets)
		if len(problems) != 0 {
			t.Fatalf("problems: %v", problems)
		}
		return jobs(pl), p.Notes()
	}
	unchanged := func(js map[string]*automationpb.PlannedJob) []string {
		var out []string
		for id, j := range js {
			if j.GetUnchangedSincePre() {
				out = append(out, id)
			}
		}
		sort.Strings(out)
		return out
	}
	const same, named, late = "//.ok/workflow/root.yaml:post:same", "//.ok/workflow/root.yaml:post:named", "//.ok/workflow/root.yaml:post:late"

	// No pre to compare with: everything runs.
	if js, _ := plan(withTools(), nil); len(unchanged(js)) != 0 {
		t.Errorf("without pre: unchanged %v", unchanged(js))
	}
	// The same tree: everything but the run job and the job naming the change holds.
	js, _ := plan(withTools(), &Affected{})
	if want := []string{same, "build", "lint", "test //app:test"}; !reflect.DeepEqual(unchanged(js), want) {
		t.Errorf("the same tree: unchanged %v, want %v", unchanged(js), want)
	}
	if b := js["build"]; !reflect.DeepEqual(b.GetSpec().GetTargets(), []string{"//app:app", "//gone:gone"}) ||
		!strings.HasSuffix(b.GetWhy(), "and nothing they build on changed since pre") || js[BuildSincePre] != nil {
		t.Errorf("the build of the same tree: %v; %v", b, js[BuildSincePre])
	}
	if js[named].GetUnchangedSincePre() || js[late].GetUnchangedSincePre() {
		t.Errorf("a job naming the change, or a run, held: %v %v", js[named], js[late])
	}
	// What main changed since pre affects //gone: its root builds again, on its own; the rest
	// held, but the lint, which analyzes //gone, the target that held a file the change removes.
	js, _ = plan(withTools(), &Affected{Changed: []string{"gone/other.go"}, Affected: []Target{{Label: "//gone:gone", Direct: true}}})
	if want := []string{same, "build", "test //app:test"}; !reflect.DeepEqual(unchanged(js), want) {
		t.Errorf("//gone changed: unchanged %v, want %v", unchanged(js), want)
	}
	if b, r := js["build"], js[BuildSincePre]; !reflect.DeepEqual(b.GetSpec().GetTargets(), []string{"//app:app"}) ||
		r == nil || !reflect.DeepEqual(r.GetSpec().GetTargets(), []string{"//gone:gone"}) || r.GetUnchangedSincePre() ||
		!strings.HasSuffix(r.GetWhy(), "building 1 target builds those that changed since pre") {
		t.Errorf("the split build: %v; %v", b, r)
	}
	// A change to a linter — the formatter's target, or anything in a lint aspect's package, which
	// no query sees — and only the lint runs.
	for what, since := range map[string]*Affected{
		"the formatter": {Changed: []string{"tools/format/main.go"}, Affected: []Target{{Label: "//tools/format:format", Direct: true}}},
		"an aspect":     {Changed: []string{"tools/lint/golangci.bzl"}},
	} {
		js, _ = plan(withTools(), since)
		if js["lint"].GetUnchangedSincePre() || !js["build"].GetUnchangedSincePre() {
			t.Errorf("%s changed: %v %v", what, js["lint"], js["build"])
		}
	}

	// The repository's settings, and a graph with problems, change everything.
	for what, since := range map[string]*Affected{
		workflow.SettingsFile: {Changed: []string{workflow.SettingsFile}},
		"problems":            {Problems: []string{"//x: no such target"}},
	} {
		js, notes := plan(withTools(), since)
		if len(unchanged(js)) != 0 || len(notes) != 1 || !strings.HasSuffix(notes[0], "every job runs") {
			t.Errorf("%s: unchanged %v, notes %v", what, unchanged(js), notes)
		}
	}
}
