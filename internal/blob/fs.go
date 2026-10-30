package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// FS is a Store over a directory, for development, tests, and an installation on one machine. A
// key's every component but the last is a directory named for it with ".d" appended, and its last
// a file with ".obj" appended — so "p/a" (p.d/a.obj) and "p/a/b" (p.d/a.d/b.obj), which an object
// store holds side by side, never contend for one name, and a listing walks only the directory
// its prefix names. Writes are atomic: a file is written in tmpDir and renamed (or, IfAbsent,
// linked) into place, so a reader never sees half an object. They are durable before Put returns:
// the file's data is synced, then the directory that names it, and every directory made for it —
// a caller records an object's key in a database once Put returns, and a power loss must not take
// the name back.
type FS struct {
	root string
}

const (
	// tmpDir is where puts are written before they take their place; no key's path is in it.
	tmpDir = ".tmp"
	dirExt = ".d"
	objExt = ".obj"
)

// NewFS is a Store over the directory root, which it makes if it does not exist.
func NewFS(root string) (*FS, error) {
	if root == "" {
		return nil, fmt.Errorf("blob: no directory")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("blob: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(abs, tmpDir), 0o700); err != nil {
		return nil, fmt.Errorf("blob: %w", err)
	}
	return &FS{root: abs}, nil
}

func (f *FS) Name() string { return f.root }

// file is key's path, refused unless key is a clean relative path.
func (f *FS) file(key string) (string, error) {
	if key == "" || strings.HasSuffix(key, "/") || path.Clean("/"+key) != "/"+key {
		return "", fmt.Errorf("blob: key %q is not a clean relative path", key)
	}
	parts := strings.Split(key, "/")
	for i, p := range parts {
		if p == "." || p == ".." {
			return "", fmt.Errorf("blob: key %q is not a clean relative path", key)
		}
		if i < len(parts)-1 {
			parts[i] = p + dirExt
		} else {
			parts[i] = p + objExt
		}
	}
	return filepath.Join(append([]string{f.root}, parts...)...), nil
}

// dir is the directory holding every key that starts with prefix's components up to its last
// slash: "p/a/" and "p/a/x" are under p.d/a.d, "p" under the root.
func (f *FS) dir(prefix string) string {
	i := strings.LastIndex(prefix, "/")
	if i < 0 {
		return f.root
	}
	parts := strings.Split(prefix[:i], "/")
	for j := range parts {
		parts[j] += dirExt
	}
	return filepath.Join(append([]string{f.root}, parts...)...)
}

// key is the key of the file at rel (a path under the root), and whether it is an object's.
func key(rel string) (string, bool) {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	for i, p := range parts {
		ext := dirExt
		if i == len(parts)-1 {
			ext = objExt
		}
		if !strings.HasSuffix(p, ext) {
			return "", false
		}
		parts[i] = strings.TrimSuffix(p, ext)
	}
	return strings.Join(parts, "/"), true
}

func (f *FS) wrap(op, key string, err error) error {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("blob: %s %s: %w", op, key, ErrNotFound)
	case errors.Is(err, fs.ErrExist):
		return fmt.Errorf("blob: %s %s: %w", op, key, ErrExists)
	}
	return fmt.Errorf("blob: %s %s: %w", op, key, err)
}

func (f *FS) Put(ctx context.Context, key string, r io.Reader, size int64, opts ...PutOption) (err error) {
	defer func(start time.Time) { observe(f.root, "put", start, size, err) }(time.Now())
	dst, err := f.file(key)
	if err != nil {
		return err
	}
	o := putOpts(opts)
	if o.ifAbsent {
		if _, err := os.Lstat(dst); err == nil {
			return f.wrap("put", key, fs.ErrExist)
		}
	}
	tmp, err := os.CreateTemp(filepath.Join(f.root, tmpDir), "put-*")
	if err != nil {
		return f.wrap("put", key, err)
	}
	defer os.Remove(tmp.Name())
	if err := copyN(tmp, r, size); err != nil {
		tmp.Close()
		return f.wrap("put", key, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return f.wrap("put", key, err)
	}
	if err := tmp.Close(); err != nil {
		return f.wrap("put", key, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.mkdirs(filepath.Dir(dst)); err != nil {
		return f.wrap("put", key, err)
	}
	if o.ifAbsent {
		// A link fails if the name is taken: the check and the write are one step.
		if err := os.Link(tmp.Name(), dst); err != nil {
			return f.wrap("put", key, err)
		}
	} else if err := os.Rename(tmp.Name(), dst); err != nil {
		return f.wrap("put", key, err)
	}
	return wrapNil(f.wrap, "put", key, syncDir(filepath.Dir(dst)))
}

// mkdirs makes dir and the directories above it under the root that are missing, each made
// durable: synced into its parent before the next is made in it.
func (f *FS) mkdirs(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		return nil
	}
	parent := filepath.Dir(dir)
	if dir != f.root && parent != dir {
		if err := f.mkdirs(parent); err != nil {
			return err
		}
	}
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	return syncDir(parent)
}

// syncDir syncs a directory, making the names it holds durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func wrapNil(wrap func(op, key string, err error) error, op, key string, err error) error {
	if err == nil {
		return nil
	}
	return wrap(op, key, err)
}

func (f *FS) GetRange(ctx context.Context, key string, offset, length int64) (_ []byte, err error) {
	if length == 0 {
		return []byte{}, nil
	}
	defer func(start time.Time) { observe(f.root, "get_range", start, length, err) }(time.Now())
	p, err := f.file(key)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(p)
	if err != nil {
		return nil, f.wrap("get", key, err)
	}
	defer file.Close()
	buf := make([]byte, length)
	if _, err := file.ReadAt(buf, offset); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, f.wrap("read", key, err)
	}
	return buf, nil
}

func (f *FS) Download(ctx context.Context, key string, w io.Writer) (n int64, err error) {
	defer func(start time.Time) { observe(f.root, "download", start, n, err) }(time.Now())
	p, err := f.file(key)
	if err != nil {
		return 0, err
	}
	file, err := os.Open(p)
	if err != nil {
		return 0, f.wrap("get", key, err)
	}
	defer file.Close()
	n, err = io.Copy(w, file)
	if err != nil {
		return n, f.wrap("read", key, err)
	}
	return n, nil
}

func (f *FS) Stat(ctx context.Context, key string) (_ Object, err error) {
	defer func(start time.Time) { observe(f.root, "stat", start, 0, err) }(time.Now())
	p, err := f.file(key)
	if err != nil {
		return Object{}, err
	}
	fi, err := os.Stat(p)
	if err != nil {
		return Object{}, f.wrap("stat", key, err)
	}
	if fi.IsDir() {
		return Object{}, f.wrap("stat", key, fs.ErrNotExist)
	}
	return Object{Key: key, Size: fi.Size(), Modified: fi.ModTime()}, nil
}

func (f *FS) Delete(ctx context.Context, keys ...string) (err error) {
	defer func(start time.Time) { observe(f.root, "delete", start, 0, err) }(time.Now())
	for _, k := range keys {
		p, perr := f.file(k)
		if perr != nil {
			err = errors.Join(err, perr)
			continue
		}
		if rerr := os.Remove(p); rerr != nil && !errors.Is(rerr, fs.ErrNotExist) {
			err = errors.Join(err, f.wrap("delete", k, rerr))
		}
	}
	return err
}

func (f *FS) List(ctx context.Context, prefix string) (_ []Object, err error) {
	defer func(start time.Time) { observe(f.root, "list", start, 0, err) }(time.Now())
	return f.walk(prefix)
}

func (f *FS) ListPage(ctx context.Context, prefix, after string, limit int) (_ []Object, next string, err error) {
	defer func(start time.Time) { observe(f.root, "list", start, 0, err) }(time.Now())
	objs, err := f.walk(prefix)
	if err != nil {
		return nil, "", err
	}
	objs, next = page(objs, after, limit)
	return objs, next, nil
}

// walk lists every object whose key starts with prefix, in key order.
func (f *FS) walk(prefix string) ([]Object, error) {
	var out []Object
	err := filepath.WalkDir(f.dir(prefix), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			if d.Name() == tmpDir && filepath.Dir(p) == f.root {
				return fs.SkipDir
			}
			return nil
		}
		rel, rerr := filepath.Rel(f.root, p)
		if rerr != nil {
			return rerr
		}
		key, ok := key(rel)
		if !ok || !strings.HasPrefix(key, prefix) {
			return nil
		}
		fi, ierr := d.Info()
		if ierr != nil {
			if errors.Is(ierr, fs.ErrNotExist) {
				return nil // deleted while listing
			}
			return ierr
		}
		out = append(out, Object{Key: key, Size: fi.Size(), Modified: fi.ModTime()})
		return nil
	})
	if err != nil {
		return nil, f.wrap("list", prefix, err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (f *FS) Copy(ctx context.Context, src, dst string) (err error) {
	defer func(start time.Time) { observe(f.root, "copy", start, 0, err) }(time.Now())
	p, err := f.file(src)
	if err != nil {
		return err
	}
	file, err := os.Open(p)
	if err != nil {
		return f.wrap("copy", src, err)
	}
	defer file.Close()
	fi, err := file.Stat()
	if err != nil {
		return f.wrap("copy", src, err)
	}
	return f.Put(ctx, dst, file, fi.Size())
}

// SignGet is unsupported: a directory has no URLs.
func (f *FS) SignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	return "", fmt.Errorf("blob: sign %s: %w", key, ErrUnsupported)
}

// Stage writes the object in the directory's temporary space, and Commit renames it into place,
// as Put does; tmp is not needed.
func (f *FS) Stage(ctx context.Context, tmp string) (Staged, error) {
	t, err := os.CreateTemp(filepath.Join(f.root, tmpDir), "stage-*")
	if err != nil {
		return nil, f.wrap("stage", tmp, err)
	}
	return &fsStaged{f: f, tmp: t}, nil
}

type fsStaged struct {
	f    *FS
	tmp  *os.File
	n    int64
	done bool
}

func (s *fsStaged) Write(p []byte) (int, error) {
	n, err := s.tmp.Write(p)
	s.n += int64(n)
	return n, err
}

func (s *fsStaged) Commit(ctx context.Context, key string) (err error) {
	defer func(start time.Time) { observe(s.f.root, "put", start, s.n, err) }(time.Now())
	if s.done {
		return errors.New("blob: a staged object committed after it ended")
	}
	s.done = true
	defer os.Remove(s.tmp.Name())
	dst, err := s.f.file(key)
	if err != nil {
		s.tmp.Close()
		return err
	}
	if err := s.tmp.Sync(); err != nil {
		s.tmp.Close()
		return s.f.wrap("put", key, err)
	}
	if err := s.tmp.Close(); err != nil {
		return s.f.wrap("put", key, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.f.mkdirs(filepath.Dir(dst)); err != nil {
		return s.f.wrap("put", key, err)
	}
	if err := os.Rename(s.tmp.Name(), dst); err != nil {
		return s.f.wrap("put", key, err)
	}
	return wrapNil(s.f.wrap, "put", key, syncDir(filepath.Dir(dst)))
}

func (s *fsStaged) Abort() {
	if s.done {
		return
	}
	s.done = true
	s.tmp.Close()
	os.Remove(s.tmp.Name())
}
