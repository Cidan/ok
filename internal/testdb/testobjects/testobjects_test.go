package testobjects_test

import (
	"testing"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/blob/blobtest"
	"github.com/Cidan/ok/internal/testdb/testobjects"
)

// The emulator serves a GCS client an ok configuration builds — through its endpoint, with no
// credentials — as GCS does: the conformance suite passes over it. (Its signed URLs are not
// fetched: the emulator serves plain HTTP and checks no signature.)
func TestGCSEmulator(t *testing.T) {
	t.Setenv("OK_TEST_OBJECTS", "gcs")
	blobtest.Run(t, func(t *testing.T) blob.Store { return testobjects.Store(t) },
		blobtest.Options{Signs: true, Classes: true, Nested: true})
}

func TestFS(t *testing.T) {
	t.Setenv("OK_TEST_OBJECTS", "fs")
	blobtest.Run(t, func(t *testing.T) blob.Store { return testobjects.Store(t) }, blobtest.Options{Nested: true})
}
