// Package blob is ok's object storage: a bucket on any S3-compatible store — AWS S3, MinIO, Ceph,
// Cloudflare R2, GCS's XML API. The SCM keeps file contents in its bucket, as packs under
// repos/<repository id>/packs/<pack id hex>; automation keeps finished runs' logs in its bucket,
// as runs/<run id>.
package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/Cidan/ok/internal/serve/metrics"
)

// Every request to object storage, by bucket, operation (put, get_range, download, delete, list),
// and outcome (ok, not_found, error), with its time and the bytes it moved.
var (
	requests = metrics.NewCounter("ok_blob_requests_total",
		"Requests to object storage, by bucket, operation, and outcome.", "bucket", "op", "result")
	requestSeconds = metrics.NewHistogram("ok_blob_request_seconds",
		"How long each request to object storage took, its body read in full.", metrics.Seconds, "bucket", "op")
	requestBytes = metrics.NewHistogram("ok_blob_request_bytes",
		"Bytes each put wrote or each get read.", metrics.Bytes, "bucket", "op")
)

// observe records one request: its outcome, its time, and, for a put or a get, its bytes.
func (s *S3) observe(op string, start time.Time, n int64, err error) {
	result := "ok"
	switch {
	case errors.Is(err, ErrNotFound):
		result = "not_found"
	case err != nil:
		result = "error"
	}
	requests.With(s.bucket, op, result).Inc()
	requestSeconds.With(s.bucket, op).Since(start)
	if err == nil && (op == "put" || op == "get_range" || op == "download") {
		requestBytes.With(s.bucket, op).Observe(float64(n))
	}
}

// ErrNotFound reports a key with no object.
var ErrNotFound = errors.New("blob: not found")

// Object describes a stored object.
type Object struct {
	Key      string
	Size     int64
	Modified time.Time
}

// Store is the object storage ok's roles need.
type Store interface {
	// Put stores size bytes from r at key.
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	// GetRange returns length bytes of key starting at offset.
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	// Download copies the whole object at key to w and returns its size.
	Download(ctx context.Context, key string, w io.Writer) (int64, error)
	// Delete removes key. Deleting a missing key succeeds.
	Delete(ctx context.Context, key string) error
	// List returns every object whose key starts with prefix.
	List(ctx context.Context, prefix string) ([]Object, error)
}

// Config locates the bucket. Endpoint is a full URL whose scheme selects TLS. Without an access
// key, credentials come from AWS's chain: its environment variables, its credentials file, or the
// instance's or pod's role.
type Config struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
}

// S3 is a Store over an S3-compatible bucket.
type S3 struct {
	client *minio.Client
	bucket string
}

// NewS3 builds a Store for cfg. It performs no network I/O.
func NewS3(cfg Config) (*S3, error) {
	u, err := url.Parse(strings.TrimSpace(cfg.Endpoint))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("blob: endpoint %q must be an http(s) URL", cfg.Endpoint)
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("blob: no bucket")
	}
	creds := credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")
	if cfg.AccessKey == "" {
		// AWS's chain: its environment variables, its credentials file, then the instance's or
		// pod's role.
		creds = credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvAWS{},
			&credentials.FileAWSCredentials{},
			&credentials.IAM{},
		})
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  creds,
		Secure: u.Scheme == "https",
		Region: cfg.Region,
		// AWS's virtual-hosted buckets where the endpoint is AWS's, paths elsewhere (MinIO, Ceph,
		// a bucket behind one host).
		BucketLookup: minio.BucketLookupAuto,
	})
	if err != nil {
		return nil, fmt.Errorf("blob: %w", err)
	}
	return &S3{client: client, bucket: cfg.Bucket}, nil
}

// EnsureBucket makes the bucket if it does not exist: for local development, where nothing else
// makes it.
func (s *S3) EnsureBucket(ctx context.Context, region string) error {
	ok, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("blob: bucket %s: %w", s.bucket, err)
	}
	if ok {
		return nil
	}
	if err := s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{Region: region}); err != nil {
		return fmt.Errorf("blob: making bucket %s: %w", s.bucket, err)
	}
	return nil
}

func (s *S3) wrap(op, key string, err error) error {
	if minio.ToErrorResponse(err).Code == minio.NoSuchKey {
		return fmt.Errorf("blob: %s %s: %w", op, key, ErrNotFound)
	}
	return fmt.Errorf("blob: %s %s: %w", op, key, err)
}

func (s *S3) Put(ctx context.Context, key string, r io.Reader, size int64) (err error) {
	defer func(start time.Time) { s.observe("put", start, size, err) }(time.Now())
	_, err = s.client.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return s.wrap("put", key, err)
	}
	return nil
}

func (s *S3) GetRange(ctx context.Context, key string, offset, length int64) (_ []byte, err error) {
	if length == 0 {
		return []byte{}, nil
	}
	defer func(start time.Time) { s.observe("get_range", start, length, err) }(time.Now())
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(offset, offset+length-1); err != nil {
		return nil, err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return nil, s.wrap("get", key, err)
	}
	defer obj.Close()
	buf := make([]byte, length)
	if _, err := io.ReadFull(obj, buf); err != nil {
		return nil, s.wrap("read", key, err)
	}
	return buf, nil
}

func (s *S3) Download(ctx context.Context, key string, w io.Writer) (n int64, err error) {
	defer func(start time.Time) { s.observe("download", start, n, err) }(time.Now())
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return 0, s.wrap("get", key, err)
	}
	defer obj.Close()
	n, err = io.Copy(w, obj)
	if err != nil {
		return n, s.wrap("read", key, err)
	}
	return n, nil
}

func (s *S3) Delete(ctx context.Context, key string) (err error) {
	defer func(start time.Time) { s.observe("delete", start, 0, err) }(time.Now())
	if err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return s.wrap("delete", key, err)
	}
	return nil
}

// DeleteMany removes keys, many to a request (S3's multi-object delete, up to 1,000 keys each).
// Deleting a missing key succeeds; every key that could not be deleted is in the error.
func (s *S3) DeleteMany(ctx context.Context, keys []string) (err error) {
	defer func(start time.Time) { s.observe("delete_many", start, 0, err) }(time.Now())
	objects := make(chan minio.ObjectInfo)
	go func() {
		defer close(objects)
		for _, k := range keys {
			select {
			case objects <- minio.ObjectInfo{Key: k}:
			case <-ctx.Done():
				return
			}
		}
	}()
	for e := range s.client.RemoveObjects(ctx, s.bucket, objects, minio.RemoveObjectsOptions{}) {
		if e.Err != nil {
			err = errors.Join(err, s.wrap("delete", e.ObjectName, e.Err))
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	return err
}

func (s *S3) List(ctx context.Context, prefix string) (_ []Object, err error) {
	defer func(start time.Time) { s.observe("list", start, 0, err) }(time.Now())
	var out []Object
	for info := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if info.Err != nil {
			return nil, s.wrap("list", prefix, info.Err)
		}
		out = append(out, Object{Key: info.Key, Size: info.Size, Modified: info.LastModified})
	}
	return out, nil
}
