package load

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// scripts/chaos.sh, against a kubectl that records what it is asked: it deletes running pods of ok's
// server and deployer, drains a zone's every node and uncordons them all, and restarts the bus.
func TestChaos(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("no bash, which the script and its stub kubectl run in")
	}
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	stub := filepath.Join(dir, "kubectl")
	os.WriteFile(stub, []byte(`#!`+bash+`
echo "$*" >> `+calls+`
case "$*" in
  *"get pods"*) printf 'pod/ok-server-a\npod/ok-server-b\n' ;;
  *"get nodes -o jsonpath"*) echo "us-central1-a us-central1-b us-central1-b" ;;
  *"get nodes -l"*) printf 'node/n1\nnode/n2\n' ;;
esac
`), 0o755)
	cmd := exec.Command(bash, "../../scripts/chaos.sh", "-d", "4", "-i", "1")
	cmd.Env = append(os.Environ(), "KUBECTL="+stub)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("chaos.sh: %v\n%s", err, out)
	}
	data, _ := os.ReadFile(calls)
	got := string(data)
	for _, want := range []string{
		"-n ok delete pod/ok-server-",
		"cordon node/n1", "drain node/n1 --ignore-daemonsets", "cordon node/n2", "drain node/n2",
		"uncordon node/n1", "uncordon node/n2",
		"-n nats rollout restart statefulset/nats",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("chaos.sh never ran kubectl %q:\n%s", want, got)
		}
	}
	if strings.Count(got, "uncordon node/n1") != 1 {
		t.Errorf("node/n1 uncordoned not once:\n%s", got)
	}
	for _, want := range []string{"chaos: deleting pod/", "chaos: draining zone us-central1-", "chaos: restarting the bus, nats/nats", "chaos: done after 4s"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("chaos.sh's log lacks %q:\n%s", want, out)
		}
	}
}
