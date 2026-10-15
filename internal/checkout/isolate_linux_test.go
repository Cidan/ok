package checkout

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// A root process keeps every range of ids it has, each mapped to itself.
func TestParseIdentityMap(t *testing.T) {
	got, err := parseIdentityMap(strings.NewReader("         0     100000      65536\n 65536 200000 10\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := []syscall.SysProcIDMap{{ContainerID: 0, HostID: 0, Size: 65536}, {ContainerID: 65536, HostID: 65536, Size: 10}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for _, bad := range []string{"", "0 1\n", "a b c\n"} {
		if _, err := parseIdentityMap(strings.NewReader(bad)); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}

// Root keeps its ids, and setgroups(2) as allowed as it is now: a sandbox that denies it (a build system's,
// for a root caller) gets a namespace that denies it too. Anyone else maps their id to root, with
// setgroups denied.
func TestNamespaceMaps(t *testing.T) {
	for _, tc := range []struct {
		name               string
		euid, egid         int
		setgroups          string
		wantUID, wantGID   syscall.SysProcIDMap
		wantAllowSetgroups bool
	}{
		{"a pod's root", 0, 0, "allow\n", syscall.SysProcIDMap{ContainerID: 0, HostID: 0, Size: 65536}, syscall.SysProcIDMap{ContainerID: 0, HostID: 0, Size: 65536}, true},
		{"root in a sandbox that denies setgroups", 0, 0, "deny\n", syscall.SysProcIDMap{ContainerID: 0, HostID: 0, Size: 65536}, syscall.SysProcIDMap{ContainerID: 0, HostID: 0, Size: 65536}, false},
		{"a user", 1000, 100, "allow\n", syscall.SysProcIDMap{ContainerID: 0, HostID: 1000, Size: 1}, syscall.SysProcIDMap{ContainerID: 0, HostID: 100, Size: 1}, false},
	} {
		uids, gids, allow, err := namespaceMaps(tc.euid, tc.egid, "0 100000 65536\n", "0 100000 65536\n", tc.setgroups)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if len(uids) != 1 || uids[0] != tc.wantUID || len(gids) != 1 || gids[0] != tc.wantGID || allow != tc.wantAllowSetgroups {
			t.Errorf("%s: %+v %+v %v", tc.name, uids, gids, allow)
		}
	}
}

// isolateHelper marks the test binary run again by TestIsolateWithoutPrivilegeEscalation.
const isolateHelper = "OK_TEST_ISOLATE_HELPER"

// The copy Isolate starts is root of its namespace, able to mount, even where privilege
// escalation is not allowed — as Kubernetes runs a container with allowPrivilegeEscalation:
// false, under which no exec gains a capability.
func TestIsolateWithoutPrivilegeEscalation(t *testing.T) {
	if dir := os.Getenv(isolateHelper); dir != "" {
		if os.Getenv(isolatedEnv) == "" {
			if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
				t.Fatal(err)
			}
		}
		if err := Isolate(); err != nil {
			fmt.Println("isolate:", err)
			os.Exit(2)
		}
		if err := unix.Mount("tmpfs", dir, "tmpfs", 0, ""); err != nil {
			fmt.Println("mount:", err)
			os.Exit(3)
		}
		fmt.Println("isolated, and mounted")
		os.Exit(0)
	}
	// A sandbox whose /proc numbers an outer PID namespace's processes cannot be given a clone's
	// maps at all — they are written at the number the clone returns, which there names another
	// process, or none — and Isolate takes its other way there, which no_new_privs forbids.
	if self, err := os.Readlink("/proc/self"); err != nil || self != strconv.Itoa(os.Getpid()) {
		t.Skipf("this sandbox's /proc numbers another PID namespace's processes (/proc/self is %q, this process %d)", self, os.Getpid())
	}
	probe := exec.Command("/proc/self/exe", "-test.run=^$")
	probe.SysProcAttr = &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWUSER,
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Geteuid(), Size: 1}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getegid(), Size: 1}}}
	if err := probe.Run(); err != nil {
		if os.Getenv("OK_TEST_INTEGRATION") == "require" {
			t.Fatalf("OK_TEST_INTEGRATION=require, and this host makes no user namespace: %v; %s", err, UserNamespaces())
		}
		t.Skipf("this host makes no user namespace: %v", err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestIsolateWithoutPrivilegeEscalation$", "-test.v")
	cmd.Env = append(os.Environ(), isolateHelper+"="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "isolated, and mounted") {
		t.Fatalf("isolating without privilege escalation: %v\n%s", err, out)
	}
}
