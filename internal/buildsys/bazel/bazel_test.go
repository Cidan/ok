package bazel

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/buildsys"
)

// commit is a variant of the sample workspace (testdata/regen.sh): its checkout, and the graph
// real Bazel answered for it.
func commit(t *testing.T, variant string) Commit {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "sample", variant+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	g, err := ParseGraph(f)
	if err != nil {
		t.Fatalf("ParseGraph(%s): %v", variant, err)
	}
	return Commit{Files: os.DirFS(filepath.Join("testdata", "sample", variant)), Graph: g}
}

// summary is an affected target as a line: its label, direct or not, root or not, and its chain.
func summary(a *buildsys.Affected) []string {
	var out []string
	for _, t := range a.Affected {
		var chain []string
		for _, s := range t.Because {
			switch {
			case s.File != "":
				chain = append(chain, s.Label+" holds "+s.File)
			case s.Via != "":
				chain = append(chain, s.Label+" "+s.Via)
			default:
				chain = append(chain, s.Label)
			}
		}
		line := t.Label + " (" + t.Kind
		if t.Direct {
			line += ", direct"
		}
		if t.Root {
			line += ", root"
		}
		out = append(out, line+"): "+strings.Join(chain, " → "))
	}
	return out
}

func TestAffected(t *testing.T) {
	base := commit(t, "base")
	for _, tc := range []struct {
		variant  string
		touched  []string
		want     []string
		tests    []string
		upstream []string
		global   string
	}{
		{
			variant: "src-edit", touched: []string{"lib/a.txt"},
			want: []string{
				"//app:joined (cat): //app:joined deps → //lib:files holds lib/a.txt",
				"//app:joined_test (check_test, root): //app:joined_test deps → //app:joined deps → //lib:files holds lib/a.txt",
				"//lib:files (filegroup, direct): //lib:files holds lib/a.txt",
			},
			tests:    []string{"//app:joined_test"},
			upstream: []string{"//lib:gen"},
		},
		{
			// The rules' definition changed, in defs.bzl: every rule of its classes, and nothing
			// the edit cannot reach — not lib's filegroup and genrule.
			variant: "bzl-edit", touched: []string{"defs.bzl"},
			want: []string{
				"//app:joined (cat, direct): //app:joined",
				"//app:joined_test (check_test, direct, root): //app:joined_test",
				"//app:other (cat, direct, root): //app:other",
			},
			tests:    []string{"//app:joined_test"},
			upstream: []string{"//lib:files", "//lib:gen"},
		},
		{
			// The genrule's command changed: it and what is built from it, and not the filegroup
			// declared beside it in the same BUILD file.
			variant: "attr-edit", touched: []string{"lib/BUILD.bazel"},
			want: []string{
				"//app:joined (cat): //app:joined deps → //lib:gen holds lib/BUILD.bazel",
				"//app:joined_test (check_test, root): //app:joined_test deps → //app:joined deps → //lib:gen holds lib/BUILD.bazel",
				"//lib:gen (genrule, direct): //lib:gen holds lib/BUILD.bazel",
			},
			tests:    []string{"//app:joined_test"},
			upstream: []string{"//lib:files"},
		},
		{
			variant: "new-target", touched: []string{"app/BUILD.bazel"},
			want:     []string{"//app:extra (cat, direct, root): //app:extra holds app/BUILD.bazel"},
			upstream: []string{"//app:joined", "//lib:files", "//lib:gen"},
		},
		{
			variant: "deleted-src", touched: []string{"app/c.txt"},
			want: []string{"//app:other (cat, direct, root): //app:other holds app/c.txt"},
		},
		{
			variant: "module-edit", touched: []string{"MODULE.bazel"},
			want: []string{
				"//app:joined (cat, direct): //app:joined holds MODULE.bazel",
				"//app:joined_test (check_test, direct, root): //app:joined_test holds MODULE.bazel",
				"//app:other (cat, direct, root): //app:other holds MODULE.bazel",
				"//lib:files (filegroup, direct): //lib:files holds MODULE.bazel",
				"//lib:gen (genrule, direct): //lib:gen holds MODULE.bazel",
			},
			tests:  []string{"//app:joined_test"},
			global: "MODULE.bazel",
		},
		{variant: "readme-edit", touched: []string{"docs/README.md"}},
		{variant: "base"},
	} {
		t.Run(tc.variant, func(t *testing.T) {
			a, err := Affected(base, commit(t, tc.variant), tc.touched)
			if err != nil {
				t.Fatal(err)
			}
			if got := summary(a); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("affected:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(tc.want, "\n  "))
			}
			if tc.tests == nil {
				tc.tests = []string{}
			}
			if tc.upstream == nil {
				tc.upstream = []string{}
			}
			if !reflect.DeepEqual(a.Tests, tc.tests) {
				t.Errorf("tests = %v, want %v", a.Tests, tc.tests)
			}
			if !reflect.DeepEqual(a.Upstream, tc.upstream) {
				t.Errorf("upstream = %v, want %v", a.Upstream, tc.upstream)
			}
			if a.Global != tc.global {
				t.Errorf("global = %q, want %q", a.Global, tc.global)
			}
		})
	}
}

// A toolchain is resolved at analysis, out of the query's sight: one affected makes every target
// affected.
func TestAffectedToolchain(t *testing.T) {
	g := func(attr string) *Graph {
		gr, err := ParseGraph(strings.NewReader(`{"type":"RULE","rule":{"name":"//tc:cc","ruleClass":"toolchain","attribute":[{"name":"$internal_attr_hash","stringValue":"` + attr + `"}]}}
{"type":"RULE","rule":{"name":"//app:bin","ruleClass":"cc_binary","attribute":[{"name":"$internal_attr_hash","stringValue":"same"}]}}
`))
		if err != nil {
			t.Fatal(err)
		}
		return gr
	}
	a, err := Affected(Commit{Graph: g("one")}, Commit{Graph: g("two")}, []string{"tc/BUILD.bazel"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"//app:bin (cc_binary, direct, root): //app:bin the toolchain //tc:cc changed",
		"//tc:cc (toolchain, direct, root): //tc:cc the toolchain //tc:cc changed",
	}
	if got := summary(a); !reflect.DeepEqual(got, want) || a.Global != "//tc:cc" {
		t.Errorf("affected = %v, global %q", got, a.Global)
	}
}

// A rule reading a generated file depends on the rule that generates it, through the file.
func TestAffectedThroughGeneratedFile(t *testing.T) {
	g := func(attr string) *Graph {
		gr, err := ParseGraph(strings.NewReader(`{"type":"RULE","rule":{"name":"//a:gen","ruleClass":"genrule","attribute":[{"name":"$internal_attr_hash","stringValue":"` + attr + `"}]}}
{"type":"GENERATED_FILE","generatedFile":{"name":"//a:out.txt","generatingRule":"//a:gen"}}
{"type":"RULE","rule":{"name":"//b:use","ruleClass":"filegroup","attribute":[{"name":"$internal_attr_hash","stringValue":"same"}],"ruleInput":["//a:out.txt"]}}
`))
		if err != nil {
			t.Fatal(err)
		}
		return gr
	}
	a, err := Affected(Commit{Graph: g("one")}, Commit{Graph: g("two")}, []string{"a/BUILD.bazel"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"//a:gen (genrule, direct): //a:gen holds a/BUILD.bazel",
		"//b:use (filegroup, root): //b:use deps → //a:out.txt deps → //a:gen holds a/BUILD.bazel",
	}
	if got := summary(a); !reflect.DeepEqual(got, want) {
		t.Errorf("affected:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

func TestParseGraphRefuses(t *testing.T) {
	for name, in := range map[string]string{
		"no attribute hash": `{"type":"RULE","rule":{"name":"//a:a","ruleClass":"genrule","attribute":[]}}`,
		"unknown type":      `{"type":"SOMETHING","something":{}}`,
		"twice":             `{"type":"SOURCE_FILE","sourceFile":{"name":"//a:f"}}` + "\n" + `{"type":"SOURCE_FILE","sourceFile":{"name":"//a:f"}}`,
		"not json":          `{"type":`,
	} {
		if _, err := ParseGraph(strings.NewReader(in)); err == nil {
			t.Errorf("%s: ParseGraph accepted %q", name, in)
		}
	}
}

func TestDescribe(t *testing.T) {
	d := Describe(commit(t, "base").Graph, []string{"//app:joined_test", "//lib:a.txt", "//lib:gen", "//nope:x"})
	want := &buildsys.Described{
		Targets: []buildsys.Description{
			{Label: "//app:joined_test", Kind: "check_test", Test: true},
			{Label: "//lib:a.txt", Kind: "source file"},
			{Label: "//lib:gen", Kind: "genrule"},
		},
		Problems: []string{"target //nope:x does not exist at the change"},
	}
	if !reflect.DeepEqual(d, want) {
		t.Errorf("Describe = %+v, want %+v", d, want)
	}
}

func TestDriverCommands(t *testing.T) {
	d := &Driver{Bazel: "bazel", OutputBase: "/state/slot/1/bazel", Flags: []string{"--remote_cache=grpcs://cache.example.com"}}
	dir := t.TempDir()
	got := d.Build(dir, []string{"//a:a", "//b:b"})
	want := []string{"bazel", "--output_base=/state/slot/1/bazel", "build", "--symlink_prefix=/", "--color=no", "--curses=no",
		"--sandbox_default_allow_network=false", "--lockfile_mode=off", "--remote_cache=grpcs://cache.example.com", "--keep_going", "--", "//a:a", "//b:b"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Build = %q", got)
	}
	os.WriteFile(filepath.Join(dir, "MODULE.bazel.lock"), []byte("{}"), 0o644)
	got = d.Test(dir, "//a:test", []string{"-test.run", "TestX"})
	want = []string{"bazel", "--output_base=/state/slot/1/bazel", "test", "--symlink_prefix=/", "--color=no", "--curses=no",
		"--sandbox_default_allow_network=false", "--lockfile_mode=error", "--remote_cache=grpcs://cache.example.com",
		"--test_arg=-test.run", "--test_arg=TestX", "--", "//a:test"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Test = %q", got)
	}
	common := []string{"--symlink_prefix=/", "--color=no", "--curses=no", "--sandbox_default_allow_network=false", "--lockfile_mode=error", "--remote_cache=grpcs://cache.example.com"}
	cmd := func(verb string, rest ...string) []string {
		return append(append([]string{"bazel", "--output_base=/state/slot/1/bazel", verb}, common...), rest...)
	}
	for name, tc := range map[string]struct{ got, want []string }{
		"Run": {d.Run(dir, "//a:bin", []string{"x", "y"}, []string{"/usr/bin/bwrap", "--unshare-net"}),
			cmd("run", "--run_under=/usr/bin/bwrap --unshare-net", "--", "//a:bin", "x", "y")},
		"Lint": {d.Lint(dir, []string{"//tools/lint:linters.bzl%eslint", "//tools/lint:linters.bzl%ruff"}, []string{"//a:a"}),
			cmd("build", "--keep_going", "--aspects=//tools/lint:linters.bzl%eslint,//tools/lint:linters.bzl%ruff",
				"--@aspect_rules_lint//lint:fail_on_violation", "--output_groups=rules_lint_human", "--", "//a:a")},
		"FormatCheck": {d.FormatCheck(dir, "//tools/format:format", []string{"a/x.go"}, []string{"/usr/bin/bwrap"}),
			cmd("run", "--run_under=/usr/bin/bwrap", "--", "//tools/format:format.check", "a/x.go")},
		"Push": {d.Push(dir, "//a:push", "c0ffee"), cmd("run", "--", "//a:push", "--tag", "c0ffee")},
	} {
		if !reflect.DeepEqual(tc.got, tc.want) {
			t.Errorf("%s = %q\nwant %q", name, tc.got, tc.want)
		}
	}
}

// Graph runs the query as QueryArgs says, in the checkout, and reads its answer.
func TestDriverGraph(t *testing.T) {
	dir := t.TempDir()
	answer, err := filepath.Abs(filepath.Join("testdata", "sample", "base.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	args := filepath.Join(dir, "args")
	fake := filepath.Join(dir, "bazel")
	os.WriteFile(fake, []byte("#!/bin/sh\necho \"$PWD $*\" > "+args+"\ncat "+answer+"\n"), 0o755)
	checkout := t.TempDir()
	g, err := (&Driver{Bazel: fake, OutputBase: "/ob"}).Graph(context.Background(), checkout)
	if err != nil {
		t.Fatal(err)
	}
	if g.Len() != 52 {
		t.Errorf("the graph holds %d targets, want 52", g.Len())
	}
	data, _ := os.ReadFile(args)
	want := checkout + " --output_base=/ob " + strings.Join(QueryArgs, " ") + " --lockfile_mode=off\n"
	if string(data) != want {
		t.Errorf("bazel ran as %q, want %q", data, want)
	}

	os.WriteFile(fake, []byte("#!/bin/sh\necho 'ERROR: no such package' >&2\nexit 7\n"), 0o755)
	if _, err := (&Driver{Bazel: fake}).Graph(context.Background(), checkout); err == nil || !strings.Contains(err.Error(), "no such package") {
		t.Errorf("a failed query: %v", err)
	}
}
