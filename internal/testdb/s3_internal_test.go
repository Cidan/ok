package testdb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

// A write that lands once its bucket is emptied — one its client gave up on, which the server
// finished — is emptied too, and the bucket deleted.
func TestBucketEmptiedOfALateWrite(t *testing.T) {
	s := S3(t)
	c := s.client(t)
	ctx := context.Background()
	name := fmt.Sprintf("test-%d-%d", os.Getpid(), counter.Add(1))
	if err := c.MakeBucket(ctx, name, minio.MakeBucketOptions{Region: s.Region}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutObject(ctx, name, "repos/7/packs/a", strings.NewReader("a"), 1, minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	late := true
	emptyAndDelete(t, ctx, c, name, func() {
		if late {
			late = false
			if _, err := c.PutObject(ctx, name, "repos/7/segments/7/late", strings.NewReader("x"), 1, minio.PutObjectOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	})
	if ok, err := c.BucketExists(ctx, name); err != nil || ok {
		t.Errorf("the bucket after its cleanup: exists %v, %v", ok, err)
	}
}
