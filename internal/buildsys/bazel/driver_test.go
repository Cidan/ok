package bazel

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/testbazel"
)

func TestOwnersAndAttrs(t *testing.T) {
	g := commit(t, "base").Graph
	for paths, want := range map[string][]string{
		"lib/a.txt":           {"//lib:files"},
		"lib/b.txt":           {"//lib:gen"},
		"app/c.txt,lib/a.txt": {"//app:other", "//lib:files"},
		"docs/README.md":      nil,
	} {
		if got := g.Owners(strings.Split(paths, ",")); !reflect.DeepEqual(got, want) {
			t.Errorf("Owners(%s) = %v, want %v", paths, got, want)
		}
	}
	if v, ok := g.Attr("//lib:gen", "cmd"); !ok || v != "cp $< $@" {
		t.Errorf("Attr(//lib:gen, cmd) = %q, %v", v, ok)
	}
	if _, ok := g.Attr("//lib:gen", "$internal_attr_hash"); ok {
		t.Error("a synthetic attribute is an attribute")
	}
	if _, ok := g.Attr("//nope:nope", "name"); ok {
		t.Error("a target that does not exist has attributes")
	}
}

// run runs a command the driver made, in dir, and returns its exit status and what it printed.
func run(t *testing.T, dir string, argv []string) (int, string) {
	t.Helper()
	var out bytes.Buffer
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode(), out.String()
	}
	if err != nil {
		t.Fatalf("%v: %v", argv, err)
	}
	return 0, out.String()
}

// The driver's commands, run by real Bazel on a copy of the sample workspace — read-only, as a
// worker's presentation is: its graph, a build, a test, what a target builds, and a binary run in
// a sandbox without network.
func TestDriverOnBazel(t *testing.T) {
	bazel := testbazel.Bazel(t)
	bwrap := testbazel.Bwrap(t)
	dir := filepath.Join(t.TempDir(), "ws")
	if err := os.CopyFS(dir, os.DirFS(filepath.Join("testdata", "sample", "base"))); err != nil {
		t.Fatal(err)
	}
	filepath.WalkDir(dir, func(p string, _ os.DirEntry, _ error) error { return os.Chmod(p, 0o555) })
	t.Cleanup(func() {
		filepath.WalkDir(dir, func(p string, _ os.DirEntry, _ error) error { return os.Chmod(p, 0o755) })
	})
	ob := t.TempDir()
	d := &Driver{Bazel: bazel, OutputBase: ob, Env: os.Environ()}
	t.Cleanup(func() { exec.Command(bazel, "--output_base="+ob, "shutdown").Run() })
	ctx := context.Background()

	g, err := d.Graph(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if desc := Describe(g, []string{"//app:joined_test", "//app:joined"}); len(desc.Problems) != 0 || !desc.Targets[1].Test {
		t.Errorf("describe: %+v", desc)
	}
	if code, out := run(t, dir, d.Build(dir, []string{"//app:joined", "//app:other"})); code != 0 {
		t.Fatalf("build: %d\n%s", code, out)
	}
	if code, out := run(t, dir, d.Test(dir, "//app:joined_test", nil)); code != 0 {
		t.Fatalf("test: %d\n%s", code, out)
	}
	files, err := d.OutputFiles(ctx, dir, "//app:joined")
	if err != nil || len(files) != 1 || !strings.HasSuffix(files[0], "/app/joined.txt") {
		t.Fatalf("output files: %v, %v", files, err)
	}
	if data, err := os.ReadFile(files[0]); err != nil || len(data) == 0 {
		t.Errorf("%s: %q, %v", files[0], data, err)
	}
	tmp := t.TempDir()
	sandbox := []string{bwrap, "--ro-bind", "/", "/", "--dev", "/dev", "--proc", "/proc", "--bind", tmp, tmp,
		"--setenv", "TMPDIR", tmp, "--unshare-net", "--die-with-parent"}
	if code, out := run(t, dir, d.Run(dir, "//app:joined_test", []string{"an-arg"}, sandbox)); code != 0 {
		t.Fatalf("run in the sandbox: %d\n%s", code, out)
	}
	if _, err := os.Stat(filepath.Join(dir, "bazel-bin")); !os.IsNotExist(err) {
		t.Errorf("the checkout gained bazel-bin: %v", err)
	}
	// A build that fails says so.
	if code, _ := run(t, dir, d.Build(dir, []string{"//app:nope"})); code == 0 {
		t.Error("building a target that does not exist succeeded")
	}
}
