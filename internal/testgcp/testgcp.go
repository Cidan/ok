// Package testgcp says what a test reaches of a real Google Cloud, by Application Default
// Credentials: Cloud KMS keys (OK_TEST_GCP_KMS_ROOT, OK_TEST_GCP_KMS_OTHER — each a key's resource
// name) and a project's Secret Manager (OK_TEST_GCP_SECRET_PROJECT). A test finding none is skipped
// — unless OK_TEST_GCP is "require", which fails it instead.
package testgcp

import (
	"os"
	"testing"
)

// KMS are two Cloud KMS keys, as roots name them: gcp-kms:<resource name>.
func KMS(t testing.TB) (root, other string) {
	t.Helper()
	root, other = os.Getenv("OK_TEST_GCP_KMS_ROOT"), os.Getenv("OK_TEST_GCP_KMS_OTHER")
	if root == "" || other == "" {
		skip(t, "OK_TEST_GCP_KMS_ROOT and OK_TEST_GCP_KMS_OTHER name no Cloud KMS keys")
	}
	return "gcp-kms:" + root, "gcp-kms:" + other
}

// SecretProject is the project whose Secret Manager a test keeps secrets in.
func SecretProject(t testing.TB) string {
	t.Helper()
	p := os.Getenv("OK_TEST_GCP_SECRET_PROJECT")
	if p == "" {
		skip(t, "OK_TEST_GCP_SECRET_PROJECT names no project")
	}
	return p
}

func skip(t testing.TB, why string) {
	t.Helper()
	if os.Getenv("OK_TEST_GCP") == "require" {
		t.Fatal("OK_TEST_GCP=require, and " + why)
	}
	t.Skip(why)
}
