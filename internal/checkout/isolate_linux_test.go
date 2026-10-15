package checkout

import (
	"strings"
	"syscall"
	"testing"
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
