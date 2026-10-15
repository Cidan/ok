package checkout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

// Real writes each commit's files to its directory, from the SCM: for a service that cannot mount
// a presentation (no /dev/fuse). A Sync writes only what differs from the commit it wrote last,
// which it remembers in a file beside the directory, and leaves every file read-only. It is
// slower to start than a presentation — a commit's files are written before anything reads them
// — and the result is the same.
type Real struct {
	dir   string
	state string
	src   Source
	// shown are the files the last Sync put at the root beside the commit's.
	shown []string
}

// leaf is a written leaf, as the state file remembers it.
type leaf struct {
	Kind       Kind   `json:"kind"`
	ID         ID     `json:"id"`
	Executable bool   `json:"executable,omitempty"`
	Target     string `json:"target,omitempty"`
}

// NewReal returns a Real writing at dir, an absolute path: an empty directory, or one a Real
// wrote before. It remembers what it wrote in a file beside dir.
func NewReal(dir string, src Source) (*Real, error) {
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("a checkout's directory %q is an absolute path", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Real{dir: dir, state: filepath.Join(filepath.Dir(dir), "."+filepath.Base(dir)+".checkout.json"), src: src}, nil
}

func (r *Real) Dir() string { return r.dir }

// Sync writes the commit's files, removes what the commit does not have, and writes files at the
// root beside them.
func (r *Real) Sync(ctx context.Context, repo int64, commit []byte, files map[string][]byte) (Stats, error) {
	entries, err := r.src.Entries(ctx, repo, commit)
	if err != nil {
		return Stats{}, err
	}
	want := map[string]leaf{}
	for _, e := range entries {
		if !fs.ValidPath(e.Path) || e.Path == "." {
			return Stats{}, fmt.Errorf("the commit names %q, which is no path in a checkout", e.Path)
		}
		want[e.Path] = leaf{Kind: e.Kind, ID: e.ID, Executable: e.Executable, Target: e.Target}
	}
	for name := range files {
		if _, clash := want[name]; clash || strings.Contains(name, "/") {
			return Stats{}, fmt.Errorf("a file shown beside the commit, %q, is one of its own or not at its root", name)
		}
	}
	have, ok := r.load()
	if !ok {
		// Without a record of what is there, nothing there is trusted.
		if err := removeAll(r.dir); err != nil {
			return Stats{}, err
		}
		have = map[string]leaf{}
	}
	for _, name := range r.shown {
		if err := removeFile(filepath.Join(r.dir, name)); err != nil {
			return Stats{}, err
		}
	}
	r.shown = nil
	// What stays: the same leaf at the same path. The rest of what is there goes first, deepest
	// first, so a file can become a directory and back.
	var gone []string
	for p, l := range have {
		if w, ok := want[p]; !ok || w != l {
			gone = append(gone, p)
		}
	}
	slices.SortFunc(gone, func(a, b string) int { return strings.Count(b, "/") - strings.Count(a, "/") })
	for _, p := range gone {
		if err := removeFile(filepath.Join(r.dir, filepath.FromSlash(p))); err != nil {
			return Stats{}, err
		}
		delete(have, p)
		pruneDirs(r.dir, path.Dir(p))
	}
	// Recorded as it changes, so a Sync broken off leaves a true record of what is there.
	if err := r.save(have); err != nil {
		return Stats{}, err
	}
	byID := map[ID][]string{}
	var ids []ID
	for p, l := range want {
		if _, ok := have[p]; ok {
			continue
		}
		switch l.Kind {
		case Symlink:
			full := filepath.Join(r.dir, filepath.FromSlash(p))
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				return Stats{}, err
			}
			if err := os.Symlink(l.Target, full); err != nil {
				return Stats{}, err
			}
			have[p] = l
		case File:
			if _, ok := byID[l.ID]; !ok {
				ids = append(ids, l.ID)
			}
			byID[l.ID] = append(byID[l.ID], p)
		}
	}
	stats := Stats{Written: true}
	parts := map[ID][]byte{}
	err = r.src.ReadFiles(ctx, repo, ids, func(id ID, data []byte, last bool) error {
		parts[id] = append(parts[id], data...)
		if !last {
			return nil
		}
		for _, p := range byID[id] {
			l := want[p]
			if err := writeLeaf(filepath.Join(r.dir, filepath.FromSlash(p)), parts[id], l.Executable); err != nil {
				return err
			}
			have[p] = l
			stats.ReadAhead++
			stats.ReadAheadBytes += int64(len(parts[id]))
		}
		delete(parts, id)
		return nil
	})
	if serr := r.save(have); err == nil {
		err = serr
	}
	if err != nil {
		return Stats{}, err
	}
	for name, data := range files {
		if err := writeLeaf(filepath.Join(r.dir, name), data, false); err != nil {
			return Stats{}, err
		}
		r.shown = append(r.shown, name)
	}
	return stats, nil
}

// load reads what the directory holds, as the last Sync recorded it.
func (r *Real) load() (map[string]leaf, bool) {
	data, err := os.ReadFile(r.state)
	if err != nil {
		return nil, false
	}
	var out map[string]leaf
	if json.Unmarshal(data, &out) != nil {
		return nil, false
	}
	return out, true
}

func (r *Real) save(have map[string]leaf) error {
	data, err := json.Marshal(have)
	if err != nil {
		return err
	}
	tmp := r.state + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, r.state)
}

// writeLeaf writes a file whole and read-only, its parent directories made.
func writeLeaf(full string, data []byte, executable bool) error {
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	mode := os.FileMode(0o444)
	if executable {
		mode = 0o555
	}
	tmp := full + ".ok-checkout"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Chmod(tmp, mode); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, full)
}

// removeFile removes a file or symlink, or a directory and what it holds; nothing there is
// fine.
func removeFile(full string) error {
	info, err := os.Lstat(full)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return err
	case info.IsDir():
		return removeAll(full)
	}
	return os.Remove(full)
}

// removeAll empties dir, making read-only directories writable as it goes.
func removeAll(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		full := filepath.Join(dir, e.Name())
		if e.IsDir() {
			_ = os.Chmod(full, 0o755)
		}
		if err := os.RemoveAll(full); err != nil {
			return err
		}
	}
	return nil
}

// pruneDirs removes the directory at rel, under root, and those above it, while they are empty.
func pruneDirs(root, rel string) {
	for rel != "." && rel != "/" && rel != "" {
		if os.Remove(filepath.Join(root, filepath.FromSlash(rel))) != nil {
			return
		}
		rel = path.Dir(rel)
	}
}
