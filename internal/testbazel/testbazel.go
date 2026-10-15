// Package testbazel finds Bazel for a test that runs it: $OK_BAZEL, else bazel or bazelisk on the
// PATH. A test finding none is skipped — unless OK_TEST_BAZEL is "require", as CI sets it where
// Bazel is installed, which fails it instead. Bazel resolves a module's dependencies over the
// network; the tests that run it use none.
package testbazel

import (
	"os"
	"os/exec"
	"testing"
)

// Bazel is the path of Bazel, or bazelisk, for t.
func Bazel(t testing.TB) string {
	t.Helper()
	if p := os.Getenv("OK_BAZEL"); p != "" {
		return p
	}
	for _, name := range []string{"bazel", "bazelisk"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	if os.Getenv("OK_TEST_BAZEL") == "require" {
		t.Fatal("OK_TEST_BAZEL=require, and no Bazel: set OK_BAZEL, or put bazelisk or bazel on the PATH")
	}
	t.Skip("no Bazel: set OK_BAZEL, or put bazelisk or bazel on the PATH")
	return ""
}

// Bwrap is the path of bubblewrap, for a test that sandboxes what it runs; like Bazel, its absence
// skips the test unless OK_TEST_BAZEL is "require".
func Bwrap(t testing.TB) string {
	t.Helper()
	if p, err := exec.LookPath("bwrap"); err == nil {
		return p
	}
	if os.Getenv("OK_TEST_BAZEL") == "require" {
		t.Fatal("OK_TEST_BAZEL=require, and no bwrap on the PATH")
	}
	t.Skip("no bwrap on the PATH")
	return ""
}
