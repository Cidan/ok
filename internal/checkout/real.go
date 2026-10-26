package checkout

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Cidan/ok/internal/scm/encoding"
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
	Kind       Kind
	ID         ID
	Executable bool
	Target     string
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
	return &Real{dir: dir, state: filepath.Join(filepath.Dir(dir), "."+filepath.Base(dir)+".checkout"), src: src}, nil
}

func (r *Real) Dir() string { return r.dir }

// Sync writes the commit's files, removes what the commit does not have, and writes files at the
// root beside them. From the tree it wrote last, it reads only what the commit changes from it
// (Source.Changes); without one, every leaf of the commit, and what is there that the commit does
// not have goes.
func (r *Real) Sync(ctx context.Context, repo int64, commit []byte, files map[string][]byte) (Stats, error) {
	have, from, ok := r.load()
	if !ok {
		// Without a record of what is there, nothing there is trusted.
		if err := removeAll(r.dir); err != nil {
			return Stats{}, err
		}
		have, from = map[string]leaf{}, ID{}
	}
	tree, changes, whole, err := r.src.Changes(ctx, repo, from, commit)
	if err != nil {
		return Stats{}, err
	}
	// want is each path's leaf after the Sync where it may differ from have's: nil for none.
	want := make(map[string]*leaf, len(changes))
	for _, c := range changes {
		if !fs.ValidPath(c.Path) || c.Path == "." {
			return Stats{}, fmt.Errorf("the commit names %q, which is no path in a checkout", c.Path)
		}
		if c.New == nil {
			want[c.Path] = nil
			continue
		}
		want[c.Path] = &leaf{Kind: c.New.Kind, ID: c.New.ID, Executable: c.New.Executable, Target: c.New.Target}
	}
	if whole {
		for p := range have {
			if _, ok := want[p]; !ok {
				want[p] = nil
			}
		}
	}
	for name := range files {
		w, changed := want[name]
		_, held := have[name]
		if changed && w != nil || !changed && held || strings.Contains(name, "/") {
			return Stats{}, fmt.Errorf("a file shown beside the commit, %q, is one of its own or not at its root", name)
		}
	}
	for _, name := range r.shown {
		if err := removeFile(filepath.Join(r.dir, name)); err != nil {
			return Stats{}, err
		}
	}
	r.shown = nil
	// A file replacing one at its path is read as a delta against the one there, while it is, if
	// it is what the record says (bases).
	bases := r.bases(have, want)
	// What stays: the same leaf at the same path. The rest of what is there goes first, deepest
	// first, so a file can become a directory and back.
	var gone []string
	for p, w := range want {
		if h, ok := have[p]; ok && (w == nil || *w != h) {
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
	// Recorded as it changes, so a Sync broken off leaves a true record of what is there — and of
	// no tree, until it is all there.
	if err := r.save(have, ID{}); err != nil {
		return Stats{}, err
	}
	byID := map[ID][]string{}
	var ids []ID
	for p, w := range want {
		if w == nil {
			continue
		}
		if _, ok := have[p]; ok {
			continue
		}
		switch w.Kind {
		case Symlink:
			full := filepath.Join(r.dir, filepath.FromSlash(p))
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				return Stats{}, err
			}
			if err := os.Symlink(w.Target, full); err != nil {
				return Stats{}, err
			}
			have[p] = *w
		case File:
			if _, ok := byID[w.ID]; !ok {
				ids = append(ids, w.ID)
			}
			byID[w.ID] = append(byID[w.ID], p)
		}
	}
	stats := Stats{Written: true}
	parts := map[ID][]byte{}
	reads := make([]Read, len(ids))
	for i, id := range ids {
		reads[i] = Read{ID: id}
		if b, ok := bases[id]; ok {
			reads[i].Base, reads[i].BaseData = b.id, b.data
		}
	}
	err = r.src.ReadFilesFrom(ctx, repo, reads, func(id ID, data []byte, last bool) error {
		parts[id] = append(parts[id], data...)
		if !last {
			return nil
		}
		for _, p := range byID[id] {
			l := *want[p]
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
	if err != nil {
		if serr := r.save(have, ID{}); serr != nil {
			return Stats{}, errors.Join(err, serr)
		}
		return Stats{}, err
	}
	if err := r.save(have, tree); err != nil {
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

// base is a version of a file the checkout holds, read from it.
type base struct {
	id   ID
	data []byte
}

// bases are, for each file want has where have has another file at the same path, that file's
// bytes, by the new file's ID — those of at most maxDeltaLength whose bytes are what the record
// says, read before the file is replaced.
func (r *Real) bases(have map[string]leaf, want map[string]*leaf) map[ID]base {
	out := map[ID]base{}
	for p, w := range want {
		h, ok := have[p]
		if w == nil || !ok || w.Kind != File || h.Kind != File || h.ID == w.ID {
			continue
		}
		if _, ok := out[w.ID]; ok {
			continue
		}
		full := filepath.Join(r.dir, filepath.FromSlash(p))
		info, err := os.Lstat(full)
		if err != nil || !info.Mode().IsRegular() || info.Size() > maxDeltaLength {
			continue
		}
		data, err := os.ReadFile(full)
		if err != nil {
			continue
		}
		hs := encoding.NewHasher(encoding.KindFile)
		hs.Write(data)
		if hs.Sum() != encoding.ID(h.ID) {
			continue // changed since it was written: no base
		}
		out[w.ID] = base{id: h.ID, data: data}
	}
	return out
}

// The state file: "OKCHKST1", the tree the directory holds exactly (zero while a Sync is under
// way, or broken off), the count of leaves (uvarint), and each leaf sorted by path — its path's
// length (uvarint) and path, its kind, a flags byte (1: executable), then a file's ID or a
// symlink's target's length (uvarint) and target.
const stateMagic = "OKCHKST1"

// load reads what the directory holds, and the tree it holds exactly, as the last Sync recorded.
func (r *Real) load() (map[string]leaf, ID, bool) {
	b, err := os.ReadFile(r.state)
	if err != nil || len(b) < len(stateMagic)+len(ID{}) || string(b[:len(stateMagic)]) != stateMagic {
		return nil, ID{}, false
	}
	b = b[len(stateMagic):]
	var tree ID
	copy(tree[:], b)
	b = b[len(tree):]
	next := func() (uint64, bool) {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, false
		}
		b = b[n:]
		return v, true
	}
	count, ok := next()
	if !ok || count > uint64(len(b)) {
		return nil, ID{}, false
	}
	out := make(map[string]leaf, count)
	for range count {
		n, ok := next()
		if !ok || n > uint64(len(b)) || len(b) < int(n)+2 {
			return nil, ID{}, false
		}
		p := string(b[:n])
		l := leaf{Kind: Kind(b[n]), Executable: b[n+1]&1 != 0}
		b = b[n+2:]
		switch l.Kind {
		case File:
			if len(b) < len(l.ID) {
				return nil, ID{}, false
			}
			copy(l.ID[:], b)
			b = b[len(l.ID):]
		case Symlink:
			t, ok := next()
			if !ok || t > uint64(len(b)) {
				return nil, ID{}, false
			}
			l.Target = string(b[:t])
			b = b[t:]
		default:
			return nil, ID{}, false
		}
		out[p] = l
	}
	if len(b) != 0 {
		return nil, ID{}, false
	}
	return out, tree, true
}

// save records what the directory holds and, when it holds a tree exactly, that tree.
func (r *Real) save(have map[string]leaf, tree ID) error {
	paths := make([]string, 0, len(have))
	n := len(stateMagic) + len(tree) + binary.MaxVarintLen64
	for p, l := range have {
		paths = append(paths, p)
		n += len(p) + len(l.Target) + 2*binary.MaxVarintLen64 + 2 + len(l.ID)
	}
	slices.Sort(paths)
	out := make([]byte, 0, n)
	out = append(out, stateMagic...)
	out = append(out, tree[:]...)
	out = binary.AppendUvarint(out, uint64(len(paths)))
	for _, p := range paths {
		l := have[p]
		out = binary.AppendUvarint(out, uint64(len(p)))
		out = append(out, p...)
		var flags byte
		if l.Executable {
			flags = 1
		}
		out = append(out, byte(l.Kind), flags)
		switch l.Kind {
		case File:
			out = append(out, l.ID[:]...)
		case Symlink:
			out = binary.AppendUvarint(out, uint64(len(l.Target)))
			out = append(out, l.Target...)
		}
	}
	tmp := r.state + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
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
