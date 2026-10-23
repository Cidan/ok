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
)

// Config locates an S3 bucket. Endpoint is a full URL whose scheme selects TLS. Without an access
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

func (s *S3) Name() string { return s.bucket }

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
	switch minio.ToErrorResponse(err).Code {
	case minio.NoSuchKey:
		return fmt.Errorf("blob: %s %s: %w", op, key, ErrNotFound)
	case "PreconditionFailed":
		return fmt.Errorf("blob: %s %s: %w", op, key, ErrExists)
	}
	return fmt.Errorf("blob: %s %s: %w", op, key, err)
}

// s3Classes are S3's names for each Class.
var s3Classes = map[Class]string{Standard: "", Infrequent: "STANDARD_IA", Archive: "GLACIER_IR"}

func (s *S3) Put(ctx context.Context, key string, r io.Reader, size int64, opts ...PutOption) (err error) {
	defer func(start time.Time) { observe(s.bucket, "put", start, size, err) }(time.Now())
	o := putOpts(opts)
	po := minio.PutObjectOptions{
		ContentType:  "application/octet-stream",
		StorageClass: s3Classes[o.class],
	}
	if o.ifAbsent {
		po.SetMatchETagExcept("*")
	}
	if _, err = s.client.PutObject(ctx, s.bucket, key, r, size, po); err != nil {
		return s.wrap("put", key, err)
	}
	return nil
}

func (s *S3) GetRange(ctx context.Context, key string, offset, length int64) (_ []byte, err error) {
	if length == 0 {
		return []byte{}, nil
	}
	defer func(start time.Time) { observe(s.bucket, "get_range", start, length, err) }(time.Now())
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
	defer func(start time.Time) { observe(s.bucket, "download", start, n, err) }(time.Now())
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

func (s *S3) Stat(ctx context.Context, key string) (_ Object, err error) {
	defer func(start time.Time) { observe(s.bucket, "stat", start, 0, err) }(time.Now())
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return Object{}, s.wrap("stat", key, err)
	}
	return Object{Key: info.Key, Size: info.Size, Modified: info.LastModified}, nil
}

// Delete removes keys: one with one request, more with S3's multi-object delete, up to 1,000 keys
// a request.
func (s *S3) Delete(ctx context.Context, keys ...string) (err error) {
	defer func(start time.Time) { observe(s.bucket, "delete", start, 0, err) }(time.Now())
	if len(keys) == 1 {
		if err := s.client.RemoveObject(ctx, s.bucket, keys[0], minio.RemoveObjectOptions{}); err != nil {
			return s.wrap("delete", keys[0], err)
		}
		return nil
	}
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
	objs, _, err := s.list(ctx, prefix, "", 0)
	return objs, err
}

func (s *S3) ListPage(ctx context.Context, prefix, after string, limit int) ([]Object, string, error) {
	return s.list(ctx, prefix, after, limit)
}

// list lists from after (exclusive), stopping after limit objects (0: every one).
func (s *S3) list(ctx context.Context, prefix, after string, limit int) (_ []Object, next string, err error) {
	defer func(start time.Time) { observe(s.bucket, "list", start, 0, err) }(time.Now())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true, StartAfter: after}
	if limit > 0 && limit < 1000 {
		opts.MaxKeys = limit + 1
	}
	var out []Object
	for info := range s.client.ListObjects(ctx, s.bucket, opts) {
		if info.Err != nil {
			return nil, "", s.wrap("list", prefix, info.Err)
		}
		if info.Key <= after {
			continue
		}
		if limit > 0 && len(out) == limit {
			return out, out[len(out)-1].Key, nil
		}
		out = append(out, Object{Key: info.Key, Size: info.Size, Modified: info.LastModified})
	}
	return out, "", nil
}

// copyLimit is the largest object one S3 copy request copies; past it, the copy is multipart.
const copyLimit = 5 << 30

func (s *S3) Copy(ctx context.Context, src, dst string) (err error) {
	defer func(start time.Time) { observe(s.bucket, "copy", start, 0, err) }(time.Now())
	info, err := s.client.StatObject(ctx, s.bucket, src, minio.StatObjectOptions{})
	if err != nil {
		return s.wrap("copy", src, err)
	}
	d := minio.CopyDestOptions{Bucket: s.bucket, Object: dst}
	sr := minio.CopySrcOptions{Bucket: s.bucket, Object: src}
	if info.Size > copyLimit {
		_, err = s.client.ComposeObject(ctx, d, sr)
	} else {
		_, err = s.client.CopyObject(ctx, d, sr)
	}
	if err != nil {
		return s.wrap("copy", src, err)
	}
	return nil
}

func (s *S3) SignGet(ctx context.Context, key string, ttl time.Duration) (_ string, err error) {
	defer func(start time.Time) { observe(s.bucket, "sign", start, 0, err) }(time.Now())
	u, err := s.client.PresignedGetObject(ctx, s.bucket, key, ttl, nil)
	if err != nil {
		return "", s.wrap("sign", key, err)
	}
	return u.String(), nil
}
