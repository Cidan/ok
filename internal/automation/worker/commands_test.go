package worker

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/buildsys/bazel"
	"github.com/Cidan/ok/internal/workflow"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// The change: a.go modified, docs/old.md renamed to docs/new.md, gone.go deleted.
func jobContext() *workflow.Context {
	return &workflow.Context{
		Change: workflow.Change{Phase: workflow.Pre, CL: 7, Patchset: 2, Commit: "c0", Base: "b0"},
		Files: []workflow.ChangedFile{
			{Path: "a/a.go", Status: workflow.Modified},
			{Path: "docs/new.md", Status: workflow.Renamed, OldPath: "docs/old.md"},
			{Path: "gone.go", Status: workflow.Deleted},
		},
	}
}

// d is a driver whose commands are short enough to read: bazel, the verb, and what follows the
// flags every command shares.
var d = &bazel.Driver{Bazel: "bazel"}

func short(argv []string) string {
	i := slices.Index(argv, "--lockfile_mode=off")
	return argv[1] + " " + strings.Join(argv[i+1:], " ")
}

func shorts(cmds [][]string) []string {
	var out []string
	for _, c := range cmds {
		out = append(out, short(c))
	}
	return out
}

func linted(aspects []string, format string) *workflow.Settings {
	s := &workflow.Settings{Build: workflow.BuildBazel}
	s.Bazel.Lint.Aspects, s.Bazel.Lint.Format = aspects, format
	return s
}

// Each job kind is the Bazel commands it names; with a job context, a test's and a run's arguments
// expand from it, and a lint checks the files the change touches that exist, with the rules that
// read them.
func TestCommands(t *testing.T) {
	sb := []string{"/usr/bin/bwrap", "--unshare-net"}
	both := linted([]string{"//tools/lint:linters.bzl%eslint"}, "//tools/format:format")
	for name, tc := range map[string]struct {
		in   jobInput
		want []string
	}{
		"build": {jobInput{job: &automationpb.Job{Kind: automationpb.Job_KIND_BUILD, Targets: []string{"//a:a", "//b:b"}}},
			[]string{"build --keep_going -- //a:a //b:b"}},
		"tests, each its own command": {jobInput{job: &automationpb.Job{Kind: automationpb.Job_KIND_TEST, Targets: []string{"//a:test", "//b:test"}, Args: []string{"-test.v"}}},
			[]string{"test --test_output=errors --test_arg=-test.v -- //a:test", "test --test_output=errors --test_arg=-test.v -- //b:test"}},
		"run, in the sandbox": {jobInput{job: &automationpb.Job{Kind: automationpb.Job_KIND_RUN, Targets: []string{"//a:bin"}, Args: []string{"{deleted_files}"}}, jc: jobContext(), sandbox: sb},
			[]string{"run --run_under=/usr/bin/bwrap --unshare-net -- //a:bin docs/old.md gone.go"}},
		"push, tagged with the commit": {jobInput{job: &automationpb.Job{Kind: automationpb.Job_KIND_PUSH, Targets: []string{"//a:push"}}, commit: []byte{0xab, 0xcd}},
			[]string{"run -- //a:push --tag abcd"}},
		"test expanding": {jobInput{job: &automationpb.Job{Kind: automationpb.Job_KIND_TEST, Targets: []string{"//a:test"}, Args: []string{"--cl={cl}", "{changed_files:**/*.go}"}}, jc: jobContext()},
			[]string{"test --test_output=errors --test_arg=--cl=7 --test_arg=a/a.go -- //a:test"}},
		"lint: aspects over the rules that read the files, the formatter over the files": {
			jobInput{job: &automationpb.Job{Kind: automationpb.Job_KIND_LINT, Targets: []string{"//gone:gone"}}, jc: jobContext(), settings: both, owners: []string{"//a:a", "//docs:docs"}, sandbox: sb},
			[]string{"build --keep_going --aspects=//tools/lint:linters.bzl%eslint --@aspect_rules_lint//lint:fail_on_violation --output_groups=rules_lint_human -- //a:a //docs:docs //gone:gone",
				"run --run_under=/usr/bin/bwrap --unshare-net -- //tools/format:format.check a/a.go docs/new.md"}},
		"lint: a formatter alone": {
			jobInput{job: &automationpb.Job{Kind: automationpb.Job_KIND_LINT}, jc: jobContext(), settings: linted(nil, "//tools/format:format"), owners: []string{"//a:a"}},
			[]string{"run -- //tools/format:format.check a/a.go docs/new.md"}},
		"lint: aspects alone, and no rule reads the files": {
			jobInput{job: &automationpb.Job{Kind: automationpb.Job_KIND_LINT}, jc: jobContext(), settings: linted([]string{"//l:l.bzl%x"}, "")},
			nil},
		"lint of the whole repository": {
			jobInput{job: &automationpb.Job{Kind: automationpb.Job_KIND_LINT, Targets: []string{"//..."}}, jc: jobContext(), settings: both, tree: []string{"a/a.go", "main.go"}},
			[]string{"build --keep_going --aspects=//tools/lint:linters.bzl%eslint --@aspect_rules_lint//lint:fail_on_violation --output_groups=rules_lint_human -- //...",
				"run -- //tools/format:format.check a/a.go main.go"}},
		"a manual lint, of its targets": {
			jobInput{job: &automationpb.Job{Kind: automationpb.Job_KIND_LINT, Targets: []string{"//a:a"}}, settings: both},
			[]string{"build --keep_going --aspects=//tools/lint:linters.bzl%eslint --@aspect_rules_lint//lint:fail_on_violation --output_groups=rules_lint_human -- //a:a"}},
		"a manual test takes its arguments as they are": {
			jobInput{job: &automationpb.Job{Kind: automationpb.Job_KIND_TEST, Targets: []string{"//a:test"}, Args: []string{"{literal}"}}},
			[]string{"test --test_output=errors --test_arg={literal} -- //a:test"}},
	} {
		tc.in.driver, tc.in.dir = d, t.TempDir()
		got, err := commands(tc.in)
		if err != nil || !reflect.DeepEqual(shorts(got), tc.want) {
			t.Errorf("%s: %q (%v)\nwant %q", name, shorts(got), err, tc.want)
		}
	}

	// A whole repository's files are checked in batches.
	var tree []string
	for i := range 2*formatBatch + 1 {
		tree = append(tree, fmt.Sprintf("f%d.go", i))
	}
	got, err := commands(jobInput{job: &automationpb.Job{Kind: automationpb.Job_KIND_LINT, Targets: []string{"//..."}}, settings: linted(nil, "//f:f"), tree: tree, driver: d, dir: t.TempDir()})
	if err != nil || len(got) != 3 || !slices.Contains(got[2], tree[len(tree)-1]) {
		t.Errorf("batches: %d commands (%v)", len(got), err)
	}

	for name, in := range map[string]jobInput{
		"an unknown placeholder": {job: &automationpb.Job{Kind: automationpb.Job_KIND_TEST, Targets: []string{"//a:test"}, Args: []string{"{nope}"}}, jc: jobContext()},
		"a run of two targets":   {job: &automationpb.Job{Kind: automationpb.Job_KIND_RUN, Targets: []string{"//a:a", "//b:b"}}},
		"a build of nothing":     {job: &automationpb.Job{Kind: automationpb.Job_KIND_BUILD}},
		"a push of two":          {job: &automationpb.Job{Kind: automationpb.Job_KIND_PUSH, Targets: []string{"//a:push", "//b:push"}}},
		"a lint with no linters": {job: &automationpb.Job{Kind: automationpb.Job_KIND_LINT, Targets: []string{"//a:a"}}, settings: linted(nil, "")},
	} {
		in.driver, in.dir = d, t.TempDir()
		if _, err := commands(in); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// The sandbox sees every file, writes only its own directory, and has no network; a run with a job
// context is told where it is.
func TestSandbox(t *testing.T) {
	got := strings.Join(sandbox("/usr/bin/bwrap", nil, "/s/run-1", "/c/ok-context.json"), " ")
	for _, want := range []string{"/usr/bin/bwrap --ro-bind / /", "--proc /proc", "--bind /s/run-1 /s/run-1", "--setenv TMPDIR /s/run-1", "--unshare-net", "--unshare-pid", "--die-with-parent", "--setenv OK_CONTEXT /c/ok-context.json"} {
		if !strings.Contains(got, want) {
			t.Errorf("sandbox %q lacks %q", got, want)
		}
	}
	if strings.Contains(strings.Join(sandbox("/usr/bin/bwrap", nil, "/s", ""), " "), "OK_CONTEXT") {
		t.Error("a run without a job context was told of one")
	}
	// Where a fresh /proc is refused, the pod's is bound, and the sandbox is otherwise the same.
	bound := strings.Join(sandbox("/usr/bin/bwrap", ProcBound, "/s", ""), " ")
	if !strings.Contains(bound, "--ro-bind /proc /proc") || strings.Contains(bound, "--proc /proc") || !strings.Contains(bound, "--unshare-pid") {
		t.Errorf("the sandbox with the pod's /proc: %q", bound)
	}
}
