package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSConfig locates a Google Cloud Storage bucket. Without a credentials file, credentials come
// from Google's application default chain: GOOGLE_APPLICATION_CREDENTIALS, the gcloud user's, or
// the workload's own identity (GKE's Workload Identity, a VM's service account).
type GCSConfig struct {
	Bucket string
	// CredentialsFile is a service account's JSON key.
	CredentialsFile string
	// Endpoint replaces Google's, for an emulator; STORAGE_EMULATOR_HOST does too.
	Endpoint string
}

// GCS is a Store over a Google Cloud Storage bucket, through GCS's own API: conditional writes by
// generation, signed URLs (with the credentials' key, or IAM's signBlob for a workload identity),
// and server-side copies.
type GCS struct {
	client *storage.Client
	bucket *storage.BucketHandle
	name   string
	own    bool
	// signer signs URLs instead of what the credentials offer, when set.
	signerID  string
	signerKey []byte
}

// NewGCS builds a Store for cfg. It performs no network I/O.
func NewGCS(ctx context.Context, cfg GCSConfig) (*GCS, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("blob: no bucket")
	}
	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithAuthCredentialsFile(option.ServiceAccount, cfg.CredentialsFile))
	}
	if cfg.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(cfg.Endpoint))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("blob: %w", err)
	}
	g := NewGCSFromClient(client, cfg.Bucket)
	g.own = true
	return g, nil
}

// NewGCSFromClient is a Store over bucket through client, which the caller keeps: tests, with an
// in-process server's client.
func NewGCSFromClient(client *storage.Client, bucket string) *GCS {
	return &GCS{client: client, bucket: retrying(client.Bucket(bucket)), name: bucket}
}

// retrying has every request of b that fails for a moment — a 429, a 5xx, a dropped connection —
// made again, a while after, up to gcsAttempts times: writes too, whose writers write what any of
// them may (Store.Put). GCS's own policy makes again only what it can tell is idempotent, which an
// unconditional write is not: staging's publish failed on one 503 on its pack.
func retrying(b *storage.BucketHandle) *storage.BucketHandle {
	return b.Retryer(storage.WithPolicy(storage.RetryAlways), storage.WithMaxAttempts(gcsAttempts),
		storage.WithBackoff(gax.Backoff{Initial: 100 * time.Millisecond, Max: 3 * time.Second, Multiplier: 2}))
}

// gcsAttempts bounds the attempts at a request: about six seconds of backing off.
const gcsAttempts = 6

// WithSigner signs URLs as the service account id with its PEM private key, instead of with what
// the client's credentials offer: for a client whose credentials hold no key and cannot reach IAM
// (tests).
func (g *GCS) WithSigner(id string, pemKey []byte) *GCS {
	g.signerID, g.signerKey = id, pemKey
	return g
}

// Close releases the client, if NewGCS made it.
func (g *GCS) Close() error {
	if g.own {
		return g.client.Close()
	}
	return nil
}

func (g *GCS) Name() string { return g.name }

func (g *GCS) wrap(op, key string, err error) error {
	if errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("blob: %s %s: %w", op, key, ErrNotFound)
	}
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		switch ge.Code {
		case http.StatusPreconditionFailed:
			return fmt.Errorf("blob: %s %s: %w", op, key, ErrExists)
		case http.StatusNotFound:
			return fmt.Errorf("blob: %s %s: %w", op, key, ErrNotFound)
		}
	}
	return fmt.Errorf("blob: %s %s: %w", op, key, err)
}

// gcsClasses are GCS's names for each Class.
var gcsClasses = map[Class]string{Standard: "", Infrequent: "NEARLINE", Archive: "ARCHIVE"}

func (g *GCS) Put(ctx context.Context, key string, r io.Reader, size int64, opts ...PutOption) (err error) {
	defer func(start time.Time) { observe(g.name, "put", start, size, err) }(time.Now())
	o := putOpts(opts)
	obj := g.bucket.Object(key)
	if o.ifAbsent {
		obj = obj.If(storage.Conditions{DoesNotExist: true})
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := obj.NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	w.StorageClass = gcsClasses[o.class]
	// The writer buffers a chunk to retry with: a small object's own size, not the default 16 MiB
	// each of many uploads at once would hold.
	w.ChunkSize = min(gcsChunkSize, int(max(size, 1)))
	if err := copyN(w, r, size); err != nil {
		// The abort must reach the upload before Close does: canceling the context aborts it
		// from another goroutine, which a Close could beat, committing what was written so far.
		// CloseWithError aborts it here and now; Close then waits for the upload to end.
		cancel()
		_ = w.CloseWithError(err)
		_ = w.Close()
		return g.wrap("put", key, err)
	}
	if err := w.Close(); err != nil {
		return g.wrap("put", key, err)
	}
	return nil
}

func (g *GCS) GetRange(ctx context.Context, key string, offset, length int64) (_ []byte, err error) {
	if length == 0 {
		return []byte{}, nil
	}
	defer func(start time.Time) { observe(g.name, "get_range", start, length, err) }(time.Now())
	rd, err := g.bucket.Object(key).NewRangeReader(ctx, offset, length)
	if err != nil {
		return nil, g.wrap("get", key, err)
	}
	defer rd.Close()
	buf := make([]byte, length)
	if _, err := io.ReadFull(rd, buf); err != nil {
		return nil, g.wrap("read", key, err)
	}
	return buf, nil
}

func (g *GCS) Download(ctx context.Context, key string, w io.Writer) (n int64, err error) {
	defer func(start time.Time) { observe(g.name, "download", start, n, err) }(time.Now())
	rd, err := g.bucket.Object(key).NewReader(ctx)
	if err != nil {
		return 0, g.wrap("get", key, err)
	}
	defer rd.Close()
	n, err = io.Copy(w, rd)
	if err != nil {
		return n, g.wrap("read", key, err)
	}
	return n, nil
}

func (g *GCS) Stat(ctx context.Context, key string) (_ Object, err error) {
	defer func(start time.Time) { observe(g.name, "stat", start, 0, err) }(time.Now())
	a, err := g.bucket.Object(key).Attrs(ctx)
	if err != nil {
		return Object{}, g.wrap("stat", key, err)
	}
	return Object{Key: a.Name, Size: a.Size, Modified: a.Updated}, nil
}

// deleteParallel bounds how many deletes run at once: GCS has no multi-object delete.
const deleteParallel = 16

func (g *GCS) Delete(ctx context.Context, keys ...string) (err error) {
	defer func(start time.Time) { observe(g.name, "delete", start, 0, err) }(time.Now())
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		errs error
		sem  = make(chan struct{}, deleteParallel)
	)
	for _, k := range keys {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return errors.Join(errs, ctx.Err())
		}
		wg.Add(1)
		go func(k string) {
			defer func() { <-sem; wg.Done() }()
			if err := g.bucket.Object(k).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				mu.Lock()
				errs = errors.Join(errs, g.wrap("delete", k, err))
				mu.Unlock()
			}
		}(k)
	}
	wg.Wait()
	return errs
}

func (g *GCS) List(ctx context.Context, prefix string) ([]Object, error) {
	objs, _, err := g.list(ctx, prefix, "", 0)
	return objs, err
}

func (g *GCS) ListPage(ctx context.Context, prefix, after string, limit int) ([]Object, string, error) {
	return g.list(ctx, prefix, after, limit)
}

func (g *GCS) list(ctx context.Context, prefix, after string, limit int) (_ []Object, next string, err error) {
	defer func(start time.Time) { observe(g.name, "list", start, 0, err) }(time.Now())
	q := &storage.Query{Prefix: prefix}
	if after != "" {
		// StartOffset is inclusive; the smallest key after after is after itself and a NUL byte.
		q.StartOffset = after + "\x00"
	}
	if err := q.SetAttrSelection([]string{"Name", "Size", "Updated"}); err != nil {
		return nil, "", err
	}
	it := g.bucket.Objects(ctx, q)
	var out []Object
	for {
		a, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out, "", nil
		}
		if err != nil {
			return nil, "", g.wrap("list", prefix, err)
		}
		if a.Name <= after {
			continue
		}
		if limit > 0 && len(out) == limit {
			return out, out[len(out)-1].Key, nil
		}
		out = append(out, Object{Key: a.Name, Size: a.Size, Modified: a.Updated})
	}
}

func (g *GCS) Copy(ctx context.Context, src, dst string) (err error) {
	defer func(start time.Time) { observe(g.name, "copy", start, 0, err) }(time.Now())
	if _, err := g.bucket.Object(dst).CopierFrom(g.bucket.Object(src)).Run(ctx); err != nil {
		return g.wrap("copy", src, err)
	}
	return nil
}

func (g *GCS) SignGet(ctx context.Context, key string, ttl time.Duration) (_ string, err error) {
	defer func(start time.Time) { observe(g.name, "sign", start, 0, err) }(time.Now())
	u, err := g.bucket.SignedURL(key, &storage.SignedURLOptions{
		Method:         http.MethodGet,
		Expires:        time.Now().Add(ttl),
		Scheme:         storage.SigningSchemeV4,
		GoogleAccessID: g.signerID,
		PrivateKey:     g.signerKey,
	})
	if err != nil {
		return "", g.wrap("sign", key, err)
	}
	return u, nil
}

// gcsChunkSize is the most an upload buffers to retry a request with: the client's default.
const gcsChunkSize = 16 << 20

// Stage uploads the object at tmp as it is written, and Commit copies it to its key within the
// bucket — a rewrite that moves no bytes through this process — then deletes tmp.
func (g *GCS) Stage(ctx context.Context, tmp string) (Staged, error) {
	ctx, cancel := context.WithCancel(ctx)
	w := g.bucket.Object(tmp).NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	w.ChunkSize = gcsChunkSize
	return &gcsStaged{g: g, tmp: tmp, w: w, cancel: cancel}, nil
}

type gcsStaged struct {
	g      *GCS
	tmp    string
	w      *storage.Writer
	cancel context.CancelFunc
	n      int64
	done   bool
}

func (s *gcsStaged) Write(p []byte) (int, error) {
	n, err := s.w.Write(p)
	s.n += int64(n)
	return n, err
}

func (s *gcsStaged) Commit(ctx context.Context, key string) (err error) {
	defer func(start time.Time) { observe(s.g.name, "put", start, s.n, err) }(time.Now())
	if s.done {
		return errors.New("blob: a staged object committed after it ended")
	}
	s.done = true
	defer s.cancel()
	if err := s.w.Close(); err != nil {
		return s.g.wrap("put", s.tmp, err)
	}
	// tmp goes whether the copy succeeds or not; one this delete misses, the caller's sweep takes.
	defer s.g.bucket.Object(s.tmp).Delete(context.WithoutCancel(ctx))
	if _, err := s.g.bucket.Object(key).CopierFrom(s.g.bucket.Object(s.tmp)).Run(ctx); err != nil {
		return s.g.wrap("put", key, err)
	}
	return nil
}

func (s *gcsStaged) Abort() {
	if s.done {
		return
	}
	s.done = true
	// As Put's: the abort reaches the upload before Close, which would otherwise commit it.
	s.cancel()
	_ = s.w.CloseWithError(errAborted)
	_ = s.w.Close()
}
