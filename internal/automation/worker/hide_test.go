package worker

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/testbazel"
)

// What Bazel runs, under a slot's prefix, reads none of the worker's secrets — a file, a
// directory's contents — nor another slot's checkout or another run's scratch, and still reads
// and writes its own run's scratch and everything else the worker sees.
func TestHideSecrets(t *testing.T) {
	bwrap := testbazel.Bwrap(t)
	root := t.TempDir()
	write := func(p, s string) {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	token := filepath.Join(root, "secrets", "token")
	credential := filepath.Join(root, "state", "okfs", "config", "credential", "key.pem")
	other := filepath.Join(root, "state", "slots", "2", "src.go")
	theirs := filepath.Join(root, "state", "scratch", "run-8", "ok-context.json")
	mine := filepath.Join(root, "state", "scratch", "run-7")
	public := filepath.Join(root, "public.txt")
	// The token as Kubernetes projects one: a link to the volume's own file, beside it.
	write(filepath.Join(root, "secrets", "..data", "token"), "TOKEN")
	if err := os.Symlink("..data/token", token); err != nil {
		t.Fatal(err)
	}
	write(credential, "KEY")
	write(other, "OTHER")
	write(theirs, "THEIRS")
	write(filepath.Join(mine, "ok-context.json"), "MINE")
	write(public, "PUBLIC")

	cfg := Config{Bwrap: bwrap, Scratch: filepath.Join(root, "state", "scratch"),
		Secrets: []string{token, filepath.Join(root, "state", "okfs"), filepath.Join(root, "missing")}}
	cfg.others = []string{filepath.Join(root, "state", "slots", "2")}
	script := `for f in "$@"; do printf '%s=' "$f"; cat "$f" 2>/dev/null; echo; done; ls -A "` + filepath.Join(root, "state", "okfs") + `"; echo written > "` + mine + `/out" && echo wrote`
	argv := append(cfg.hide(mine), "sh", "-c", script, "sh", token, credential, other, theirs, filepath.Join(mine, "ok-context.json"), public)
	out, err := exec.Command(argv[0], argv[1:]...).CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	got := string(out)
	for _, want := range []string{token + "=\n", credential + "=\n", other + "=\n", theirs + "=\n", filepath.Join(mine, "ok-context.json") + "=MINE\n", public + "=PUBLIC\n", "wrote"} {
		if !strings.Contains(got, want) {
			t.Errorf("under the prefix, %q lacks %q", got, want)
		}
	}
	if !strings.Contains(got, filepath.Join(root, "secrets", "..data", "token")+"=\n") {
		argv := append(cfg.hide(mine), "cat", filepath.Join(root, "secrets", "..data", "token"))
		if out, _ := exec.Command(argv[0], argv[1:]...).CombinedOutput(); strings.Contains(string(out), "TOKEN") {
			t.Errorf("the token's own file, beside its link, is in sight")
		}
	}
	for _, leaked := range []string{"TOKEN", "KEY", "OTHER", "THEIRS"} {
		if strings.Contains(got, leaked) {
			t.Errorf("a secret leaked: %q", got)
		}
	}
	if b, err := os.ReadFile(filepath.Join(mine, "out")); err != nil || string(b) != "written\n" {
		t.Errorf("the run's own scratch, written under the prefix: %q %v", b, err)
	}
	if cfg2 := (Config{Bwrap: bwrap}); cfg2.hide(mine) != nil {
		t.Error("a worker with no secrets hides something")
	}
}
