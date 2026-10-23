package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/buildsys/bazel"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/testbazel"
	"github.com/Cidan/ok/migrate"
)

// mapSource is a repository's commits, each a map of files, as the SCM serves them.
type mapSource map[byte]map[string]string

func (m mapSource) Entries(_ context.Context, _ int64, commit []byte) ([]checkout.Entry, error) {
	var out []checkout.Entry
	for p, c := range m[commit[0]] {
		out = append(out, checkout.Entry{Path: p, Kind: checkout.File, ID: sha256.Sum256([]byte(c)), Size: int64(len(c)), Executable: strings.HasPrefix(c, "#!")})
	}
	return out, nil
}

func (m mapSource) ReadFiles(_ context.Context, _ int64, ids []checkout.ID, write func(checkout.ID, []byte, bool) error) error {
	for _, id := range ids {
		found := false
		for _, files := range m {
			for _, c := range files {
				if !found && checkout.ID(sha256.Sum256([]byte(c))) == id {
					found = true
					if err := write(id, []byte(c), true); err != nil {
						return err
					}
				}
			}
		}
		if !found {
			return errors.New("no such file")
		}
	}
	return nil
}

func commitOf(b byte) []byte {
	c := make([]byte, 32)
	c[0] = b
	return c
}

// The deployer's Build on real Bazel, in a real checkout: what a change affects across two commits
// (the earlier one's graph queried with the checkout moved there and back), what a target is, and
// a migration speaking the migrate protocol — refusing a gated hazard, then applying it allowed.
func TestBazelBuild(t *testing.T) {
	bin := testbazel.Bazel(t)
	base := map[string]string{
		"MODULE.bazel":        "module(name = \"fixture\")\n",
		"BUILD.bazel":         "",
		".ok/repository.yaml": "build: bazel\n",
		"rules.bzl": `def _cat_impl(ctx):
    out = ctx.actions.declare_file(ctx.label.name + ".txt")
    ctx.actions.run_shell(inputs = ctx.files.srcs, outputs = [out], command = "cat %s > %s" % (" ".join([f.path for f in ctx.files.srcs]), out.path))
    return [DefaultInfo(files = depset([out]))]

cat = rule(implementation = _cat_impl, attrs = {"srcs": attr.label_list(allow_files = True)})

def _script_impl(ctx):
    out = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(out, "#!/bin/sh\n" + ctx.attr.script, is_executable = True)
    return [DefaultInfo(executable = out)]

script_binary = rule(implementation = _script_impl, executable = True, attrs = {"script": attr.string()})
`,
		"lib/BUILD.bazel": "load(\"//:rules.bzl\", \"cat\")\n\ncat(\n    name = \"lib\",\n    srcs = [\"a.txt\"],\n)\n",
		"lib/a.txt":       "one\n",
		"db/BUILD.bazel": `load("//:rules.bzl", "script_binary")

script_binary(
    name = "migrate",
    script = """allowed=no
for a in "$@"; do [ "$a" = DELETES_DATA ] && allowed=yes; done
echo "migrating" >&2
if [ -n "$OK_SECRETS_DIR" ]; then echo "the password is $(cat "$OK_SECRETS_DIR/prod/db")" >&2; fi
if [ $allowed = no ]; then
  echo '{"database":"app","steps":[{"description":"0002_drop","hazards":[{"class":"DELETES_DATA","gated":true}]}],"refused":["DELETES_DATA"],"executed":false,"error":"the plan carries gated hazards that were not allowed: DELETES_DATA"}'
  exit 2
fi
echo '{"database":"app","steps":[{"description":"0002_drop"}],"executed":true}'
""",
)
`,
	}
	change := map[string]string{}
	for p, c := range base {
		change[p] = c
	}
	change["lib/a.txt"] = "two\n"
	src := mapSource{1: base, 2: change}
	co, err := checkout.NewReal(filepath.Join(t.TempDir(), "co"), src)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := co.Sync(ctx, 1, commitOf(2), nil); err != nil {
		t.Fatal(err)
	}
	ob := t.TempDir()
	t.Cleanup(func() { exec.Command(bin, "--output_base="+ob, "shutdown").Run() })
	b := &Bazel{Path: bin, OutputBase: ob, Env: os.Environ(), Graphs: bazel.Graphs{Dir: t.TempDir(), Keep: 8}, Checkout: co, Source: src}

	a, err := b.Affected(ctx, 1, commitOf(1), commitOf(2), []string{"lib/a.txt"})
	if err != nil {
		t.Fatal(err)
	}
	var labels []string
	for _, tg := range a.Affected {
		labels = append(labels, tg.Label)
	}
	if !slices.Contains(labels, "//lib:lib") || slices.Contains(labels, "//db:migrate") || len(a.Problems) != 0 {
		t.Errorf("affected %v (problems %v)", labels, a.Problems)
	}
	if data, err := os.ReadFile(filepath.Join(co.Dir(), "lib", "a.txt")); err != nil || string(data) != "two\n" {
		t.Errorf("the checkout after querying the base: %q, %v", data, err)
	}
	if _, ok := b.Graphs.Get("1-" + hex.EncodeToString(commitOf(1))); !ok {
		t.Error("the base's graph was not kept")
	}

	described, err := b.Describe(ctx, 1, commitOf(2), []string{"//db:migrate", "//nope:nope"})
	if err != nil {
		t.Fatal(err)
	}
	if d, ok := described["//db:migrate"]; !ok || !d.Binary || d.Kind != "script_binary" {
		t.Errorf("//db:migrate: %+v", d)
	}
	if _, ok := described["//nope:nope"]; ok {
		t.Error("a target that does not exist was described")
	}

	refused, err := b.Migrate(ctx, "//db:migrate", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(refused.Refused, []string{migrate.DeletesData}) || !strings.Contains(refused.Failed, "gated hazards") ||
		!strings.Contains(refused.Output, "migrating") || !strings.Contains(string(refused.Report), `"0002_drop"`) {
		t.Errorf("refused: %+v", refused)
	}
	// Its secrets are files for it alone, and nothing it printed holds them.
	values := map[string][]byte{"secret/prod/db": []byte("hunter2-correct")}
	applied, err := b.Migrate(ctx, "//db:migrate", []string{migrate.DeletesData}, values)
	if err != nil || applied.Failed != "" || len(applied.Refused) != 0 || !strings.Contains(string(applied.Report), `"executed":true`) {
		t.Errorf("applied: %+v (%v)", applied, err)
	}
	if !strings.Contains(applied.Output, "the password is [secret //secret/prod/db]") || strings.Contains(applied.Output, "hunter2") {
		t.Errorf("the migration's output: %q", applied.Output)
	}
	if left, _ := filepath.Glob("/dev/shm/ok-secrets-*"); len(left) != 0 {
		t.Errorf("secrets left behind: %v", left)
	}
}
