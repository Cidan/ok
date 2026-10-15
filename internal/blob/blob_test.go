package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/testdb"
)

// testStore connects to a new bucket on the S3 server the test is given (internal/testdb).
func testStore(t *testing.T) *S3 {
	t.Helper()
	srv := testdb.S3(t)
	s, err := NewS3(Config{
		Endpoint:  srv.Endpoint,
		Bucket:    testdb.Bucket(t),
		AccessKey: srv.AccessKey,
		SecretKey: srv.SecretKey,
		Region:    srv.Region,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestS3(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("blobtest/%d/", time.Now().UnixNano())
	data := bytes.Repeat([]byte("0123456789"), 1000)
	key := prefix + "a"
	if err := s.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		objs, _ := s.List(ctx, prefix)
		for _, o := range objs {
			s.Delete(ctx, o.Key)
		}
	})
	got, err := s.GetRange(ctx, key, 1234, 100)
	if err != nil || !bytes.Equal(got, data[1234:1334]) {
		t.Fatalf("GetRange = %q, %v", got, err)
	}
	if got, err := s.GetRange(ctx, key, 5, 0); err != nil || len(got) != 0 {
		t.Fatalf("empty GetRange = %q, %v", got, err)
	}
	var buf bytes.Buffer
	if n, err := s.Download(ctx, key, &buf); err != nil || n != int64(len(data)) || !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("Download = %d, %v", n, err)
	}
	if err := s.Put(ctx, prefix+"b", bytes.NewReader([]byte("b")), 1); err != nil {
		t.Fatal(err)
	}
	objs, err := s.List(ctx, prefix)
	if err != nil || len(objs) != 2 || objs[0].Key != key || objs[0].Size != int64(len(data)) {
		t.Fatalf("List = %+v, %v", objs, err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRange(ctx, key, 0, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRange after Delete = %v, want ErrNotFound", err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("deleting a missing key: %v", err)
	}
}

// Every request is counted by bucket, operation, and outcome, with its time and bytes.
func TestMetrics(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	r := metrics.Default()
	count := func(op, result string) float64 { return r.Read("ok_blob_requests_total", s.bucket, op, result).Value }
	payload := bytes.Repeat([]byte("x"), 5000)
	if err := s.Put(ctx, "k", bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRange(ctx, "k", 10, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRange(ctx, "missing", 0, 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a missing key: %v", err)
	}
	var buf bytes.Buffer
	if _, err := s.Download(ctx, "k", &buf); err != nil {
		t.Fatal(err)
	}
	if _, err := s.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		op, result string
		want       float64
	}{
		{"put", "ok", 1}, {"get_range", "ok", 1}, {"get_range", "not_found", 1},
		{"download", "ok", 1}, {"list", "ok", 1}, {"delete", "ok", 1},
	} {
		if got := count(c.op, c.result); got != c.want {
			t.Errorf("%s %s: %v, want %v", c.op, c.result, got, c.want)
		}
	}
	for op, want := range map[string]float64{"put": 5000, "get_range": 100, "download": 5000} {
		if got := r.Read("ok_blob_request_bytes", s.bucket, op); got.Count != 1 || got.Sum != want {
			t.Errorf("%s bytes: %+v, want one of %v", op, got, want)
		}
	}
	if got := r.Read("ok_blob_request_seconds", s.bucket, "put"); got.Count != 1 {
		t.Errorf("put's time: %+v", got)
	}
}

// EnsureBucket makes a bucket that does not exist, and leaves one that does.
func TestEnsureBucket(t *testing.T) {
	srv := testdb.S3(t)
	ctx := context.Background()
	s, err := NewS3(Config{
		Endpoint: srv.Endpoint, Bucket: fmt.Sprintf("ensure-%d", time.Now().UnixNano()),
		AccessKey: srv.AccessKey, SecretKey: srv.SecretKey, Region: srv.Region,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "k", bytes.NewReader([]byte("x")), 1); err == nil {
		t.Fatal("a put into a bucket that does not exist succeeded")
	}
	for range 2 {
		if err := s.EnsureBucket(ctx, srv.Region); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put(ctx, "k", bytes.NewReader([]byte("x")), 1); err != nil {
		t.Fatalf("a put after EnsureBucket: %v", err)
	}
	t.Cleanup(func() {
		s.Delete(context.Background(), "k")
		s.client.RemoveBucket(context.Background(), s.bucket)
	})
}
