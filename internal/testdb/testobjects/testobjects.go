// Package testobjects is the object store a test runs on, as OK_TEST_OBJECTS names it: s3 (the
// default) — a bucket of the test's own on the environment's S3 server (internal/testdb) — gcs, a
// bucket of its own on a GCS emulator this process runs on its loopback, or fs, a directory of its
// own. A test that keeps objects takes its store from here, so the suites run on each (docs/hosted.md,
// "Testing").
package testobjects

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/testdb"
)

// Location is where a test's objects are, as an ok configuration's objects section names a store.
type Location struct {
	Type      string // s3, gcs, or fs
	Endpoint  string // s3, gcs
	Bucket    string // s3, gcs
	Region    string // s3
	AccessKey string // s3
	SecretKey string // s3
	Dir       string // fs
}

// Kind is the store OK_TEST_OBJECTS names: s3 unless it says gcs or fs.
func Kind(t testing.TB) string {
	t.Helper()
	switch k := os.Getenv("OK_TEST_OBJECTS"); k {
	case "", "s3":
		return "s3"
	case "gcs", "fs":
		return k
	default:
		t.Fatalf("testobjects: OK_TEST_OBJECTS=%q is none of s3, gcs, and fs", k)
		return ""
	}
}

// New makes a store for this test alone, of the kind OK_TEST_OBJECTS names, and returns where it is;
// it goes when the test ends.
func New(t testing.TB) Location {
	t.Helper()
	switch Kind(t) {
	case "gcs":
		srv := emulator(t)
		return Location{Type: "gcs", Endpoint: srv.Endpoint(), Bucket: srv.Bucket(t)}
	case "fs":
		return Location{Type: "fs", Dir: t.TempDir()}
	}
	s3 := testdb.S3(t)
	return Location{Type: "s3", Endpoint: s3.Endpoint, Bucket: testdb.Bucket(t), Region: s3.Region,
		AccessKey: s3.AccessKey, SecretKey: s3.SecretKey}
}

// Open is a client of the store, built as an ok configuration's would be. A GCS store signs URLs
// as a service account with a key made for this process, as one with a key file does.
func (l Location) Open(t testing.TB) blob.Store {
	t.Helper()
	var s blob.Store
	var err error
	switch l.Type {
	case "gcs":
		var g *blob.GCS
		if g, err = blob.NewGCS(context.Background(), blob.GCSConfig{Bucket: l.Bucket, Endpoint: l.Endpoint}); err == nil {
			t.Cleanup(func() { g.Close() })
			s = g.WithSigner(signer, signingKey(t))
		}
	case "fs":
		s, err = blob.NewFS(l.Dir)
	default:
		s, err = blob.NewS3(blob.Config{Endpoint: l.Endpoint, Bucket: l.Bucket, Region: l.Region,
			AccessKey: l.AccessKey, SecretKey: l.SecretKey})
	}
	if err != nil {
		t.Fatalf("testobjects: opening the %s store: %v", l.Type, err)
	}
	if d := Latency(t); d > 0 {
		return NewSlow(s, d)
	}
	return s
}

// Store makes a store for this test alone (New) and opens it.
func Store(t testing.TB) blob.Store {
	t.Helper()
	return New(t).Open(t)
}

// YAML is the store as an ok configuration's objects section, flow style; secret writes a secret's
// value to a file and returns its path, for the S3 keys.
func (l Location) YAML(secret func(name, value string) string) string {
	switch l.Type {
	case "gcs":
		return fmt.Sprintf("{type: gcs, endpoint: %q, bucket: %q}", l.Endpoint, l.Bucket)
	case "fs":
		return fmt.Sprintf("{type: fs, dir: %q}", l.Dir)
	}
	return fmt.Sprintf("{type: s3, endpoint: %q, bucket: %q, region: %q, access_key_file: %q, secret_key_file: %q}",
		l.Endpoint, l.Bucket, l.Region, secret(l.Bucket+"-access-key", l.AccessKey), secret(l.Bucket+"-secret-key", l.SecretKey))
}

// GCSServer is a GCS emulator on a loopback port, in memory: the JSON API at Endpoint, and objects'
// XML URLs at its own address, as GCS serves them at storage.googleapis.com.
type GCSServer struct {
	fake    *fakestorage.Server
	http    *http.Server
	addr    string
	buckets atomic.Int64
}

// StartGCS starts a GCS emulator listening at addr (127.0.0.1:0 for any port), with buckets made.
func StartGCS(addr string, buckets ...string) (*GCSServer, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	host := lis.Addr().String()
	fake, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		NoListener: true, Scheme: "http", PublicHost: host, ExternalURL: "http://" + host,
	})
	if err != nil {
		lis.Close()
		return nil, err
	}
	s := &GCSServer{fake: fake, http: &http.Server{Handler: fake.HTTPHandler()}, addr: host}
	for _, b := range buckets {
		fake.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: b})
	}
	go s.http.Serve(lis)
	return s, nil
}

// Addr is the emulator's host:port, what STORAGE_EMULATOR_HOST names.
func (s *GCSServer) Addr() string { return s.addr }

// Endpoint is the emulator's JSON API, what a GCS client's endpoint names.
func (s *GCSServer) Endpoint() string { return "http://" + s.addr + "/storage/v1/" }

// Bucket creates a bucket for this test alone and returns its name; its objects go when the test ends.
func (s *GCSServer) Bucket(t testing.TB) string {
	t.Helper()
	name := fmt.Sprintf("test-%d-%d", os.Getpid(), s.buckets.Add(1))
	s.fake.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: name})
	t.Cleanup(func() {
		objs, _, err := s.fake.ListObjects(name, "", "", false)
		if err != nil {
			t.Errorf("testobjects: listing bucket %s: %v", name, err)
			return
		}
		for _, o := range objs {
			s.fake.Backend().DeleteObject(name, o.Name)
		}
		s.fake.Backend().DeleteBucket(name)
	})
	return name
}

// Close stops the emulator.
func (s *GCSServer) Close() error {
	err := s.http.Close()
	s.fake.Stop()
	return err
}

// signer is the service account a GCS store on the emulator signs URLs as.
const signer = "ok-test@example.iam.gserviceaccount.com"

var (
	once   sync.Once
	shared *GCSServer
	failed error

	keyOnce sync.Once
	key     []byte
	keyErr  error
)

// signingKey is the signer's PEM private key, made once per process.
func signingKey(t testing.TB) []byte {
	t.Helper()
	keyOnce.Do(func() {
		var k *rsa.PrivateKey
		if k, keyErr = rsa.GenerateKey(rand.Reader, 2048); keyErr == nil {
			key = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
		}
	})
	if keyErr != nil {
		t.Fatalf("testobjects: making a signing key: %v", keyErr)
	}
	return key
}

// emulator is the GCS emulator this process runs, started on first use. A GCS client finds it
// through STORAGE_EMULATOR_HOST, which it sets — as it would for any emulator — so a client an ok
// configuration builds, with no credentials, does not look for Google's.
func emulator(t testing.TB) *GCSServer {
	t.Helper()
	once.Do(func() {
		if shared, failed = StartGCS("127.0.0.1:0"); failed == nil {
			failed = os.Setenv("STORAGE_EMULATOR_HOST", shared.Addr())
		}
	})
	if failed != nil {
		t.Fatalf("testobjects: starting the GCS emulator: %v", failed)
	}
	return shared
}
