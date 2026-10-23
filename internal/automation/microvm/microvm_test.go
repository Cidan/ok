//go:build linux

package microvm

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// tools are the microVM's: from OK_TEST_MICROVM_KERNEL, _HYPERVISOR, and _VIRTIOFSD; the test is
// skipped without them unless OK_TEST_MICROVM=require.
func tools(t *testing.T) Config {
	t.Helper()
	cfg := Config{Kernel: os.Getenv("OK_TEST_MICROVM_KERNEL"), Hypervisor: os.Getenv("OK_TEST_MICROVM_HYPERVISOR"),
		Virtiofsd: os.Getenv("OK_TEST_MICROVM_VIRTIOFSD"), CPUs: 1, MemoryMiB: 512, System: DefaultSystem}
	if cfg.Kernel == "" || cfg.Hypervisor == "" || cfg.Virtiofsd == "" {
		if os.Getenv("OK_TEST_MICROVM") == "require" {
			t.Fatal("OK_TEST_MICROVM=require, and OK_TEST_MICROVM_KERNEL, _HYPERVISOR, or _VIRTIOFSD is unset")
		}
		t.Skip("no microVM tools (OK_TEST_MICROVM_KERNEL, _HYPERVISOR, _VIRTIOFSD)")
	}
	return cfg
}

var (
	built    string
	buildErr error
	buildOne sync.Once
)

// okBinary is ok, built static: the guest's init.
func okBinary(t *testing.T) string {
	t.Helper()
	buildOne.Do(func() {
		dir, err := os.MkdirTemp("", "okvm-init")
		if err != nil {
			buildErr = err
			return
		}
		built = filepath.Join(dir, "ok")
		cmd := exec.Command("go", "build", "-o", built, "github.com/Cidan/ok/cmd/ok")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("building ok: %v: %s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return built
}

// run runs a command in the guest, as ok worker vm-exec does.
func run(t *testing.T, vm *VM, dir string, argv ...string) (string, string, int) {
	t.Helper()
	wd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(wd)
	var stdout, stderr bytes.Buffer
	status, err := RunInGuest(vm.Socket(), argv, &stdout, &stderr)
	if err != nil {
		t.Fatalf("%v: %v", argv, err)
	}
	return stdout.String(), stderr.String(), status
}

// A run's microVM sees what is shared with it and nothing else — read-only where it is shared so,
// the host refusing its writes — runs its commands as the worker, reaches out only through the
// host's proxy and only where it allows, and stops its commands when asked.
func TestMicroVM(t *testing.T) {
	cfg := tools(t)
	cfg.Init = okBinary(t)
	cfg.Root = t.TempDir()
	if err := Prepare(cfg, os.Getuid(), os.Getgid()); err != nil {
		t.Fatal(err)
	}
	host := t.TempDir()
	ro, rw, secret := filepath.Join(host, "checkout"), filepath.Join(host, "rw"), filepath.Join(host, "secret")
	for _, d := range []string{ro, rw, secret} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(ro, "BUILD"), []byte("committed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secret, "token"), []byte("hunter2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// An HTTP server on the host, which the guest may reach through the proxy, and one it may not.
	allowed, refused := serve(t, "allowed"), serve(t, "refused")
	var mu sync.Mutex
	var refusals []string
	ctx := context.Background()
	start := time.Now()
	vm, err := Start(ctx, cfg, Options{
		Shares: []Share{{Path: ro, ReadOnly: true}, {Path: rw}},
		Egress: &Egress{Allowed: []string{allowed}, Refused: func(hp string) { mu.Lock(); refusals = append(refusals, hp); mu.Unlock() }},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Stop()
	t.Logf("booted in %s", time.Since(start).Round(time.Millisecond))

	if out, _, status := run(t, vm, ro, "cat", "BUILD"); out != "committed\n" || status != 0 {
		t.Errorf("reading the checkout: %q, %d", out, status)
	}
	if out, _, _ := run(t, vm, ro, "pwd"); out != ro+"\n" {
		t.Errorf("the command's directory: %q, want %q", out, ro)
	}
	if out, _, _ := run(t, vm, ro, "id", "-u"); strings.TrimSpace(out) != strconv.Itoa(os.Getuid()) {
		t.Errorf("the command's user: %q, want the worker's %d", out, os.Getuid())
	}
	if _, _, status := run(t, vm, ro, "sh", "-c", "exit 3"); status != 3 {
		t.Errorf("an exit status: %d, want 3", status)
	}
	// The checkout is read-only, refused by the host as by the guest's mount: even a remount as
	// read-write could not write it.
	if _, _, status := run(t, vm, ro, "sh", "-c", "echo x > BUILD"); status == 0 {
		t.Error("the guest wrote its read-only checkout")
	}
	if b, _ := os.ReadFile(filepath.Join(ro, "BUILD")); string(b) != "committed\n" {
		t.Errorf("the checkout after the guest's write: %q", b)
	}
	if _, _, status := run(t, vm, rw, "sh", "-c", "echo built > out"); status != 0 {
		t.Error("the guest could not write its output base")
	}
	if b, err := os.ReadFile(filepath.Join(rw, "out")); err != nil || string(b) != "built\n" {
		t.Errorf("what the guest wrote, on the host: %q, %v", b, err)
	}
	// What the host does not share is not there.
	if out, _, status := run(t, vm, ro, "cat", filepath.Join(secret, "token")); status == 0 || strings.Contains(out, "hunter2") {
		t.Errorf("the guest read the host's secret: %q, %d", out, status)
	}
	for _, p := range []string{"/etc/shadow", os.Getenv("HOME")} {
		if _, _, status := run(t, vm, ro, "ls", p); p != "" && status == 0 {
			t.Errorf("the guest sees the host's %s", p)
		}
	}
	if out, _, _ := run(t, vm, ro, "ls", "/sys/class/net"); out != "lo\n" {
		t.Errorf("the guest's network interfaces: %q, want lo alone", out)
	}
	// Out only through the proxy, only where it allows.
	proxy := fmt.Sprintf("http://127.0.0.1:%d", ProxyPort)
	if out, errOut, status := run(t, vm, ro, "curl", "-sS", "--max-time", "10", "-p", "-x", proxy, "http://"+allowed+"/"); out != "allowed" || status != 0 {
		t.Errorf("an allowed host: %q, %q, %d", out, errOut, status)
	}
	if out, _, status := run(t, vm, ro, "curl", "-sS", "--max-time", "10", "-p", "-x", proxy, "http://"+refused+"/"); status == 0 || out == "refused" {
		t.Errorf("a host no rule allows: %q, %d", out, status)
	}
	if out, _, status := run(t, vm, ro, "curl", "-sS", "--max-time", "5", "--noproxy", "*", "http://"+allowed+"/"); status == 0 || out == "allowed" {
		t.Errorf("around the proxy: %q, %d", out, status)
	}
	mu.Lock()
	if len(refusals) != 1 || refusals[0] != refused {
		t.Errorf("refusals: %v, want %s", refusals, refused)
	}
	mu.Unlock()
	// What a command leaves running does not hold its end.
	begun := time.Now()
	if _, _, status := run(t, vm, ro, "sh", "-c", "sleep 60 & echo started"); status != 0 || time.Since(begun) > 10*time.Second {
		t.Errorf("a command leaving a process behind: %d, after %s", status, time.Since(begun))
	}
	// A command stopped: its group terminated.
	wd, _ := os.Getwd()
	os.Chdir(ro)
	done := make(chan int, 1)
	c, err := DialGuest(vm.Socket(), ExecPort)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeLine(c, Request{Argv: []string{"sleep", "100"}, Dir: ro, Env: os.Environ()}); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			kind, payload, err := ReadFrame(c)
			if err != nil {
				done <- 999
				return
			}
			if kind == FrameExit {
				s, _ := ExitStatus(payload)
				done <- s
				return
			}
		}
	}()
	time.Sleep(300 * time.Millisecond)
	if _, err := c.Write([]byte{Terminate}); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-done:
		if s != -15 {
			t.Errorf("a terminated command's status: %d, want -15", s)
		}
	case <-time.After(10 * time.Second):
		t.Error("a terminated command did not stop")
	}
	c.Close()
	os.Chdir(wd)
}

// serve serves body over HTTP on the host's loopback, returning its address.
func serve(t *testing.T, body string) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) })}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
	return l.Addr().String()
}
