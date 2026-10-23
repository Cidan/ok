package config

import (
	"context"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/blob"
)

// An object store is opened by its type: S3 by default, GCS, or a directory; anything else, and a
// store missing what it needs, is refused, naming the field.
func TestObjectsOpen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Objects{Type: "fs", Dir: dir}.Open(ctx, "scm.objects", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*blob.FS); !ok {
		t.Fatalf("type fs opened a %T", s)
	}
	s, err = Objects{Endpoint: "http://127.0.0.1:1", Bucket: "b"}.Open(ctx, "scm.objects", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*blob.S3); !ok {
		t.Fatalf("no type opened a %T, want S3", s)
	}
	for _, c := range []struct {
		o    Objects
		want string
	}{
		{Objects{Type: "azure"}, `scm.objects.type: "azure"`},
		{Objects{Type: "fs"}, "scm.objects.dir"},
		{Objects{Type: "gcs"}, "scm.objects: blob: no bucket"},
		{Objects{Type: "s3", Bucket: "b"}, "scm.objects: blob: endpoint"},
	} {
		if _, err := c.o.Open(ctx, "scm.objects", false); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%+v: %v, want %q", c.o, err, c.want)
		}
	}
}
