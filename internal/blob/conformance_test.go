package blob_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/blob/blobtest"
	"github.com/Cidan/ok/internal/testdb"
)

func TestS3Conformance(t *testing.T) {
	blobtest.Run(t, func(t *testing.T) blob.Store {
		srv := testdb.S3(t)
		s, err := blob.NewS3(blob.Config{
			Endpoint: srv.Endpoint, Bucket: testdb.Bucket(t),
			AccessKey: srv.AccessKey, SecretKey: srv.SecretKey, Region: srv.Region,
		})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}, blobtest.Options{Signs: true, Fetch: true, Classes: true})
}

var gcsBuckets atomic.Int64

// gcsStore is a GCS store over a bucket of its own on an in-process fake of GCS, signing URLs with
// a key made for the test.
func gcsStore(t *testing.T) *blob.GCS {
	t.Helper()
	srv, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	bucket := fmt.Sprintf("test-%d", gcsBuckets.Add(1))
	srv.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucket})
	return blob.NewGCSFromClient(srv.Client(), bucket).WithSigner("ok-test@example.iam.gserviceaccount.com", testKey(t))
}

func testKey(t *testing.T) []byte {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
}

func TestGCSConformance(t *testing.T) {
	blobtest.Run(t, func(t *testing.T) blob.Store { return gcsStore(t) },
		blobtest.Options{Signs: true, Classes: true, Nested: true})
}

// flaky is a transport that answers every other request with body, status 503, in place of the
// store's answer: a store's every request fails once, for a moment.
type flaky struct {
	base http.RoundTripper
	body string
	n    atomic.Int64
}

func (f *flaky) RoundTrip(r *http.Request) (*http.Response, error) {
	if f.n.Add(1)%2 == 1 {
		if r.Body != nil {
			_, _ = io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable", Proto: "HTTP/1.1",
			ProtoMajor: 1, ProtoMinor: 1, Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(f.body)), Request: r}, nil
	}
	return f.base.RoundTrip(r)
}

// Through GCS failing every other request for a moment (503) — writes, unconditional and staged,
// copies, deletes, reads, and listings — every request is made again, and the store holds to its
// contract.
func TestGCSConformanceThroughFailures(t *testing.T) {
	blobtest.Run(t, func(t *testing.T) blob.Store {
		srv, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(srv.Stop)
		bucket := fmt.Sprintf("test-%d", gcsBuckets.Add(1))
		srv.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucket})
		client, err := storage.NewClient(t.Context(), option.WithCredentials(&google.Credentials{}),
			option.WithHTTPClient(&http.Client{Transport: &flaky{base: srv.HTTPClient().Transport,
				body: `{"error":{"code":503,"message":"Backend Error"}}`}}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.Close() })
		return blob.NewGCSFromClient(client, bucket).WithSigner("ok-test@example.iam.gserviceaccount.com", testKey(t))
	}, blobtest.Options{Signs: true, Classes: true, Nested: true})
}

// Through S3 failing every other request for a moment (503), every request is made again — a body
// that cannot be rewound held, for minio to send again — and the store holds to its contract.
func TestS3ConformanceThroughFailures(t *testing.T) {
	blobtest.Run(t, func(t *testing.T) blob.Store {
		srv := testdb.S3(t)
		s, err := blob.NewS3(blob.Config{
			Endpoint: srv.Endpoint, Bucket: testdb.Bucket(t),
			AccessKey: srv.AccessKey, SecretKey: srv.SecretKey, Region: srv.Region,
			Transport: &flaky{base: http.DefaultTransport.(*http.Transport).Clone(),
				body: `<?xml version="1.0" encoding="UTF-8"?><Error><Code>SlowDown</Code><Message>Reduce your rate.</Message></Error>`},
		})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}, blobtest.Options{Signs: true, Classes: true})
}

// A GCS URL is signed with GCS's V4 scheme, as the signer, for the key, until the TTL passes.
func TestGCSSignedURL(t *testing.T) {
	s := gcsStore(t)
	raw, err := s.SignGet(context.Background(), "repos/1/packs/ab", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if !strings.HasSuffix(u.Path, "/repos/1/packs/ab") || q.Get("X-Goog-Algorithm") != "GOOG4-RSA-SHA256" ||
		!strings.HasPrefix(q.Get("X-Goog-Credential"), "ok-test@example.iam.gserviceaccount.com/") ||
		(q.Get("X-Goog-Expires") != "600" && q.Get("X-Goog-Expires") != "599") || q.Get("X-Goog-Signature") == "" {
		t.Fatalf("the signed URL: %s", raw)
	}
}

func TestFSConformance(t *testing.T) {
	blobtest.Run(t, func(t *testing.T) blob.Store {
		s, err := blob.NewFS(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return s
	}, blobtest.Options{Classes: true, Nested: true})
}

// A key that would leave the directory, or name its scratch space, is refused.
func TestFSKeys(t *testing.T) {
	s, err := blob.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, k := range []string{"", "../x", "a/../../x", "/abs", "a//b", "a/", "a/./b", ".", ".."} {
		if err := s.Put(ctx, k, strings.NewReader("x"), 1); err == nil {
			t.Errorf("Put %q succeeded", k)
		}
	}
}
