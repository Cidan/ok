package checkout

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"path"
	"strings"
	"time"
)

// FilesAt are the files at paths in a commit — those of them that exist — read from the SCM, as a
// file system holding them and the directories above them.
func FilesAt(ctx context.Context, src Source, repo int64, commit []byte, paths []string) (fs.FS, error) {
	entries, err := src.Entries(ctx, repo, commit)
	if err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, p := range paths {
		want[p] = true
	}
	files := memFS{}
	byID := map[ID][]string{}
	var ids []ID
	for _, e := range entries {
		if !want[e.Path] {
			continue
		}
		switch e.Kind {
		case Symlink:
			files[e.Path] = []byte(e.Target)
		case File:
			if _, ok := byID[e.ID]; !ok {
				ids = append(ids, e.ID)
			}
			byID[e.ID] = append(byID[e.ID], e.Path)
		}
	}
	if len(ids) == 0 {
		return files, nil
	}
	parts := map[ID][]byte{}
	err = src.ReadFiles(ctx, repo, ids, func(id ID, data []byte, last bool) error {
		parts[id] = append(parts[id], data...)
		if last {
			for _, p := range byID[id] {
				files[p] = parts[id]
			}
		}
		return nil
	})
	return files, err
}

// memFS is files by path, and the directories above them: what filesAt read.
type memFS map[string][]byte

func (m memFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	if data, ok := m[name]; ok {
		return &memFile{info: memInfo{name: path.Base(name), size: int64(len(data))}, r: bytes.NewReader(data)}, nil
	}
	for p := range m {
		if name == "." || strings.HasPrefix(p, name+"/") {
			return &memFile{info: memInfo{name: path.Base(name), dir: true}, r: bytes.NewReader(nil)}, nil
		}
	}
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}

type memFile struct {
	info memInfo
	r    *bytes.Reader
}

func (f *memFile) Stat() (fs.FileInfo, error) { return f.info, nil }
func (f *memFile) Close() error               { return nil }
func (f *memFile) Read(b []byte) (int, error) {
	if f.info.dir {
		return 0, &fs.PathError{Op: "read", Path: f.info.name, Err: errors.New("is a directory")}
	}
	return f.r.Read(b)
}

type memInfo struct {
	name string
	size int64
	dir  bool
}

func (i memInfo) Name() string { return i.name }
func (i memInfo) Size() int64  { return i.size }
func (i memInfo) Mode() fs.FileMode {
	if i.dir {
		return fs.ModeDir | 0o555
	}
	return 0o444
}
func (i memInfo) ModTime() time.Time { return time.Time{} }
func (i memInfo) IsDir() bool        { return i.dir }
func (i memInfo) Sys() any           { return nil }
