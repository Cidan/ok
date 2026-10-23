package checkout

import (
	"errors"
	"os"
	"strings"
	"testing"
)

// A role with virtual checkouts runs only in a user namespace of its own, where it may make its
// mounts private; anywhere else it says how to start it.
func TestIsolated(t *testing.T) {
	private := func() error { return nil }
	refused := func() error { return errors.New("operation not permitted") }
	for _, tc := range []struct {
		name        string
		ns          string
		makePrivate func() error
		restriction string
		want        []string // "" for none
	}{
		{"its own namespaces", "user:[4026532001]", private, "", nil},
		{"the host's user namespace", initialUserNamespace, private, "", []string{"the host's", Launch, "check out real"}},
		{"the host's, where AppArmor restricts", initialUserNamespace, private, "AppArmor restricts it",
			[]string{"the host's", Launch, "AppArmor restricts it"}},
		{"its own user namespace, no capability over its mounts", "user:[4026532001]", refused, "",
			[]string{"operation not permitted", "no capability over its mount namespace", Launch}},
	} {
		called := false
		err := isolated(tc.ns, func() error { called = true; return tc.makePrivate() }, tc.restriction)
		switch {
		case tc.want == nil && err != nil:
			t.Errorf("%s: %v", tc.name, err)
		case tc.want != nil && err == nil:
			t.Errorf("%s: no error", tc.name)
		}
		for _, w := range tc.want {
			if err != nil && !strings.Contains(err.Error(), w) {
				t.Errorf("%s: %q lacks %q", tc.name, err, w)
			}
		}
		if tc.ns == initialUserNamespace && called {
			t.Errorf("%s: it changed the host's mounts", tc.name)
		}
	}
}

// In the host's user namespace — as an ordinary test runs — the check refuses, and changes nothing.
func TestRequireIsolatedOnTheHost(t *testing.T) {
	ns, err := os.Readlink("/proc/self/ns/user")
	if err != nil {
		t.Fatal(err)
	}
	if ns != initialUserNamespace {
		t.Skipf("this test runs in a user namespace of its own (%s), whose mounts it must not change", ns)
	}
	if err := RequireIsolated(); err == nil || !strings.Contains(err.Error(), Launch) {
		t.Fatalf("RequireIsolated in the host's user namespace: %v", err)
	}
}
