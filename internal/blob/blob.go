// Package blob is ok's object storage, one interface over three stores: any S3-compatible bucket
// (AWS S3, MinIO, Ceph, Cloudflare R2), a Google Cloud Storage bucket through its own API, and a
// directory on local disk. The SCM keeps file contents in its store, as packs under
// repos/<repository id>/packs/<pack id hex>; automation keeps finished runs' logs in its store, as
// runs/<run id>.
package blob

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/Cidan/ok/internal/serve/metrics"
)

// Every request to object storage, by bucket, operation (put, get_range, download, stat, delete,
// list, copy, sign), and outcome (ok, not_found, exists, error), with its time and the bytes it
// moved.
var (
	requests = metrics.NewCounter("ok_blob_requests_total",
		"Requests to object storage, by bucket, operation, and outcome.", "bucket", "op", "result")
	requestSeconds = metrics.NewHistogram("ok_blob_request_seconds",
		"How long each request to object storage took, its body read in full.", metrics.Seconds, "bucket", "op")
	requestBytes = metrics.NewHistogram("ok_blob_request_bytes",
		"Bytes each put wrote or each get read.", metrics.Bytes, "bucket", "op")
)

// observe records one request: its outcome, its time, and, for a put or a get, its bytes.
func observe(bucket, op string, start time.Time, n int64, err error) {
	result := "ok"
	switch {
	case errors.Is(err, ErrNotFound):
		result = "not_found"
	case errors.Is(err, ErrExists):
		result = "exists"
	case err != nil:
		result = "error"
	}
	requests.With(bucket, op, result).Inc()
	requestSeconds.With(bucket, op).Since(start)
	if err == nil && (op == "put" || op == "get_range" || op == "download") {
		requestBytes.With(bucket, op).Observe(float64(n))
	}
}

var (
	// ErrNotFound reports a key with no object.
	ErrNotFound = errors.New("blob: not found")
	// ErrExists reports a put IfAbsent of a key that has an object.
	ErrExists = errors.New("blob: already exists")
	// ErrUnsupported reports an operation the store cannot do (a directory cannot sign a URL).
	ErrUnsupported = errors.New("blob: not supported by this store")
)

// Object describes a stored object.
type Object struct {
	Key      string
	Size     int64
	Modified time.Time
}

// Class is how an object is stored: how often it is read decides what keeping it costs. Every
// class reads at once; a store that has no such classes keeps every object the same.
type Class int

const (
	// Standard is for what is read often.
	Standard Class = iota
	// Infrequent is for what is read rarely: S3's STANDARD_IA, GCS's NEARLINE.
	Infrequent
	// Archive is for what is almost never read: S3's GLACIER_IR, GCS's ARCHIVE.
	Archive
)

// PutOption shapes a Put.
type PutOption func(*putOptions)

type putOptions struct {
	ifAbsent bool
	class    Class
}

// IfAbsent makes a Put fail with ErrExists, writing nothing, when key has an object.
func IfAbsent() PutOption { return func(o *putOptions) { o.ifAbsent = true } }

// WithClass stores the object in class c.
func WithClass(c Class) PutOption { return func(o *putOptions) { o.class = c } }

func putOpts(opts []PutOption) putOptions {
	var o putOptions
	for _, f := range opts {
		f(&o)
	}
	return o
}

// Staged is an object being written before its key is known (Store.Stage).
type Staged interface {
	io.Writer
	// Commit stores what was written at key, replacing what is there: a key named by a hash over
	// a random nonce, which names nothing else. Staging ends with it, whether it succeeds or not.
	Commit(ctx context.Context, key string) error
	// Abort discards what was written. After Commit it does nothing.
	Abort()
}

// errAborted ends a staged upload that was aborted.
var errAborted = errors.New("blob: the staged object was aborted")

// Store is the object storage ok's roles need. Every implementation passes blobtest's suite.
type Store interface {
	// Name is the bucket's name (a directory's path), for errors and metrics.
	Name() string
	// Put stores size bytes from r at key, replacing what is there unless IfAbsent. It stores
	// nothing unless it read all size bytes: a reader that fails before its last byte — or in place
	// of it, as one that checks what it read does — leaves key as it was. A put that fails for a
	// moment — the store's 429 or 5xx, a dropped connection — is made again, so a key's writers
	// each write what any of them may: the same bytes, as at a key named by its content, or bytes
	// any of which is right, as a cache's entries are.
	Put(ctx context.Context, key string, r io.Reader, size int64, opts ...PutOption) error
	// Stage starts an object whose key is known only once it is written — a pack or a segment,
	// named by the hash of its bytes — uploaded as it is written, under tmp until Commit names it.
	// tmp is a key the caller's sweep of what no row names deletes, should the process stop before
	// it commits or aborts; a directory keeps it in its own temporary space instead.
	Stage(ctx context.Context, tmp string) (Staged, error)
	// GetRange returns length bytes of key starting at offset; a range past the object's end is
	// an error.
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	// Download copies the whole object at key to w and returns its size.
	Download(ctx context.Context, key string, w io.Writer) (int64, error)
	// Stat describes the object at key.
	Stat(ctx context.Context, key string) (Object, error)
	// Delete removes keys, many to a request where the store can. Deleting a missing key
	// succeeds; every key that could not be deleted is in the error.
	Delete(ctx context.Context, keys ...string) error
	// List returns every object whose key starts with prefix, in key order.
	List(ctx context.Context, prefix string) ([]Object, error)
	// ListPage returns up to limit objects whose key starts with prefix and sorts after after, in
	// key order, and the key to pass as after for the next page ("" when there is none).
	ListPage(ctx context.Context, prefix, after string, limit int) ([]Object, string, error)
	// Copy copies the object at src to dst within the store, without moving its bytes through
	// this process where the store can.
	Copy(ctx context.Context, src, dst string) error
	// SignGet returns a URL from which anyone holding it may GET the object at key — whole, or a
	// range with a Range header — until ttl passes. ErrUnsupported where the store has no URLs.
	SignGet(ctx context.Context, key string, ttl time.Duration) (string, error)
}

// page cuts a sorted listing to the objects after after, at most limit of them.
func page(objs []Object, after string, limit int) ([]Object, string) {
	i := 0
	for i < len(objs) && objs[i].Key <= after {
		i++
	}
	objs = objs[i:]
	if limit > 0 && len(objs) > limit {
		objs = objs[:limit]
		return objs, objs[len(objs)-1].Key
	}
	return objs, ""
}

// copyN copies exactly size bytes from r to w, failing if r holds fewer or more.
func copyN(w io.Writer, r io.Reader, size int64) error {
	n, err := io.Copy(w, io.LimitReader(r, size+1))
	if err != nil {
		return err
	}
	if n != size {
		return errors.New("blob: the reader held a different number of bytes than the size given")
	}
	return nil
}
