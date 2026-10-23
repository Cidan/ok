package testdb

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Server is the environment's S3 server, addressed path-style, with a credential that may do
// anything.
type S3Server struct {
	Endpoint  string // its URL: http://127.0.0.1:9000
	AccessKey string
	SecretKey string
	Region    string
}

// S3 returns the environment's S3 server.
func S3(t testing.TB) S3Server {
	t.Helper()
	s := S3Server{
		Endpoint:  os.Getenv("AWS_ENDPOINT_URL_S3"),
		AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		Region:    os.Getenv("AWS_REGION"),
	}
	if s.Endpoint == "" || s.AccessKey == "" || s.SecretKey == "" {
		missing(t, "S3")
	}
	if s.Region == "" {
		s.Region = "us-east-1"
	}
	return s
}

func (s S3Server) client(t testing.TB) *minio.Client {
	t.Helper()
	u, err := url.Parse(s.Endpoint)
	if err != nil {
		t.Fatalf("testdb: the S3 endpoint %q: %v", s.Endpoint, err)
	}
	c, err := minio.New(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(s.AccessKey, s.SecretKey, ""),
		Secure:       u.Scheme == "https",
		Region:       s.Region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Bucket creates a bucket for this test alone and returns its name; it is emptied and deleted
// when the test ends.
func Bucket(t testing.TB) string {
	t.Helper()
	s := S3(t)
	c := s.client(t)
	ctx := context.Background()
	name := fmt.Sprintf("test-%d-%d", os.Getpid(), counter.Add(1))
	if err := c.MakeBucket(ctx, name, minio.MakeBucketOptions{Region: s.Region}); err != nil {
		t.Fatalf("testdb: creating bucket %s: %v", name, err)
	}
	t.Cleanup(func() { emptyAndDelete(t, ctx, c, name, nil) })
	return name
}

// emptyFor is how long emptyAndDelete empties a bucket that writes still land in.
const emptyFor = 30 * time.Second

// emptyAndDelete empties a bucket and deletes it. A write its client gave up on — a role's, stopped
// mid-write as its test ended — the server may still finish once the bucket is listed, as
// versitygw does: while the bucket is not empty, it is emptied again, for up to emptyFor. between,
// when set, runs after each emptying, before the deletion: tests.
func emptyAndDelete(t testing.TB, ctx context.Context, c *minio.Client, name string, between func()) {
	t.Helper()
	deadline := time.Now().Add(emptyFor)
	for {
		objects := c.ListObjects(ctx, name, minio.ListObjectsOptions{Recursive: true, WithVersions: true})
		for err := range c.RemoveObjects(ctx, name, objects, minio.RemoveObjectsOptions{}) {
			t.Errorf("testdb: emptying bucket %s: %s: %v", name, err.ObjectName, err.Err)
		}
		if between != nil {
			between()
		}
		err := c.RemoveBucket(ctx, name)
		if err == nil {
			return
		}
		if minio.ToErrorResponse(err).Code != "BucketNotEmpty" || time.Now().After(deadline) {
			t.Errorf("testdb: deleting bucket %s: %v", name, err)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
