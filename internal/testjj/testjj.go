// Package testjj finds ok's jj for a test that runs it as people do: $OK_JJ, the Cargo workspace's
// jj built (cargo build -p ok-jj). A test finding none is skipped — unless OK_TEST_JJ is "require",
// as CI sets it where jj is built, which fails it instead.
package testjj

import (
	"os"
	"testing"
)

// Path is ok's jj, for t.
func Path(t testing.TB) string {
	t.Helper()
	if p := os.Getenv("OK_JJ"); p != "" {
		return p
	}
	if os.Getenv("OK_TEST_JJ") == "require" {
		t.Fatal("OK_TEST_JJ=require, and OK_JJ names no jj")
	}
	t.Skip("OK_JJ names no jj: build the Cargo workspace's (cargo build -p ok-jj)")
	return ""
}
