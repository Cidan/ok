package checkout

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// fakeSource serves commits named by their first byte: each a tree of files ("x" prefix:
// executable), and symlinks ("->" prefix).
type fakeSource struct {
	commits map[byte]map[string]string
	reads   int
	// bases are the reads that named a base, and compared whether each Changes compared trees.
	bases    []Read
	compared []bool
}

// id is a content's file ID, as the SCM names it.
func id(content string) ID {
	return ID(encoding.Hash(append([]byte{byte(encoding.KindFile)}, content...)))
}

func (f *fakeSource) Entries(_ context.Context, _ int64, commit []byte) ([]Entry, error) {
	tree, ok := f.commits[commit[0]]
	if !ok {
		return nil, &CommitError{Message: "it has conflicts"}
	}
	var out []Entry
	for p, c := range tree {
		switch {
		case len(c) > 2 && c[:2] == "->":
			out = append(out, Entry{Path: p, Kind: Symlink, Target: c[2:]})
		default:
			out = append(out, Entry{Path: p, Kind: File, ID: id(c), Executable: len(c) > 0 && c[0] == 'x', Size: int64(len(c))})
		}
	}
	return out, nil
}

// Changes compares commits' leaves: a commit's tree is its byte, and from a tree of none of its
// commits it lists the commit whole.
func (f *fakeSource) Changes(ctx context.Context, repo int64, from ID, commit []byte) (ID, []Change, bool, error) {
	entries, err := f.Entries(ctx, repo, commit)
	if err != nil {
		return ID{}, nil, false, err
	}
	tree := ID{commit[0]}
	var old map[string]Entry
	if _, known := f.commits[from[0]]; known && from != (ID{}) && from == (ID{from[0]}) {
		was, err := f.Entries(ctx, repo, from[:1])
		if err != nil {
			return ID{}, nil, false, err
		}
		old = map[string]Entry{}
		for _, e := range was {
			old[e.Path] = e
		}
	}
	f.compared = append(f.compared, old != nil)
	var out []Change
	for _, e := range entries {
		if o, ok := old[e.Path]; !ok || o != e {
			out = append(out, Change{Path: e.Path, New: &e})
		}
		delete(old, e.Path)
	}
	for p := range old {
		out = append(out, Change{Path: p})
	}
	return tree, out, old == nil, nil
}

func (f *fakeSource) ReadFilesFrom(ctx context.Context, repo int64, reads []Read, write func(ID, []byte, bool) error) error {
	ids := make([]ID, len(reads))
	for i, r := range reads {
		ids[i] = r.ID
		if r.Base != (ID{}) {
			f.bases = append(f.bases, r)
		}
	}
	return f.ReadFiles(ctx, repo, ids, write)
}

func (f *fakeSource) ReadFiles(_ context.Context, _ int64, ids []ID, write func(ID, []byte, bool) error) error {
	for _, want := range ids {
		found := false
		for _, tree := range f.commits {
			for _, c := range tree {
				if id(c) == want && !found {
					found = true
					f.reads++
					if err := write(want, []byte(c[:len(c)/2]), false); err != nil {
						return err
					}
					if err := write(want, []byte(c[len(c)/2:]), true); err != nil {
						return err
					}
				}
			}
		}
		if !found {
			return errors.New("no such file")
		}
	}
	return nil
}

// tree is what dir holds: each file's content (and "+x" when executable), each symlink's target.
func tree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || p == dir || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		info, _ := os.Lstat(p)
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, _ := os.Readlink(p)
			out[rel] = "->" + target
		default:
			data, _ := os.ReadFile(p)
			s := string(data)
			if info.Mode().Perm()&0o111 != 0 {
				s += " +x"
			}
			if info.Mode().Perm()&0o222 != 0 {
				s += " writable"
			}
			out[rel] = s
		}
		return nil
	})
	return out
}

func TestReal(t *testing.T) {
	src := &fakeSource{commits: map[byte]map[string]string{
		1: {"a.txt": "one", "lib/b.go": "package b", "bin/run": "x#!/bin/sh", "link": "->a.txt", "dir/f": "f"},
		2: {"a.txt": "two", "lib/b.go": "package b", "bin/run": "x#!/bin/sh", "dir": "now a file", "lib/new/c": "c"},
		3: {"dir/g": "g"},
	}}
	dir := filepath.Join(t.TempDir(), "co")
	r, err := NewReal(dir, src)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	equal := func(want map[string]string) {
		t.Helper()
		got := tree(t, dir)
		if len(got) != len(want) {
			t.Errorf("tree %v, want %v", got, want)
			return
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("%s: %q, want %q (tree %v)", k, got[k], v, got)
			}
		}
	}

	stats, err := r.Sync(ctx, 1, []byte{1}, map[string][]byte{"ok-context.json": []byte("{}")})
	if err != nil {
		t.Fatal(err)
	}
	equal(map[string]string{"a.txt": "one", "lib/b.go": "package b", "bin/run": "x#!/bin/sh +x", "link": "->a.txt", "dir/f": "f", "ok-context.json": "{}"})
	if stats.ReadAhead != 4 {
		t.Errorf("first sync wrote %d files", stats.ReadAhead)
	}

	// Only what differs is read; a directory becomes a file; the shown file goes.
	src.reads = 0
	if _, err := r.Sync(ctx, 1, []byte{2}, nil); err != nil {
		t.Fatal(err)
	}
	equal(map[string]string{"a.txt": "two", "lib/b.go": "package b", "bin/run": "x#!/bin/sh +x", "dir": "now a file", "lib/new/c": "c"})
	if src.reads != 3 {
		t.Errorf("the second sync read %d files, want 3 (a.txt, dir, lib/new/c)", src.reads)
	}
	// The file replacing one at its path is read against it, as it was on disk; the rest against none.
	if len(src.bases) != 1 || src.bases[0].ID != id("two") || src.bases[0].Base != id("one") || string(src.bases[0].BaseData) != "one" {
		t.Errorf("the second sync's bases: %+v; want a.txt against its last version", src.bases)
	}
	// The first listed the commit whole; the second compared it with the tree written.
	if len(src.compared) != 2 || src.compared[0] || !src.compared[1] {
		t.Errorf("syncs compared trees: %v; want the second alone", src.compared)
	}

	// A file becomes a directory again; emptied directories go.
	if _, err := r.Sync(ctx, 1, []byte{3}, nil); err != nil {
		t.Fatal(err)
	}
	equal(map[string]string{"dir/g": "g"})
	entries, _ := os.ReadDir(dir)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if len(names) != 1 || names[0] != "dir" {
		t.Errorf("left at the root: %v", names)
	}

	// A commit with conflicts has no checkout.
	var ce *CommitError
	if _, err := r.Sync(ctx, 1, []byte{9}, nil); !errors.As(err, &ce) {
		t.Errorf("a conflicted commit: %v", err)
	}

	// Without the record of what it wrote, a Real starts over.
	os.Remove(filepath.Join(filepath.Dir(dir), ".co.checkout"))
	os.WriteFile(filepath.Join(dir, "stray"), []byte("x"), 0o644)
	r2, _ := NewReal(dir, src)
	if _, err := r2.Sync(ctx, 1, []byte{1}, nil); err != nil {
		t.Fatal(err)
	}
	equal(map[string]string{"a.txt": "one", "lib/b.go": "package b", "bin/run": "x#!/bin/sh +x", "link": "->a.txt", "dir/f": "f"})

	// A shown file may not shadow one of the commit's.
	if _, err := r2.Sync(ctx, 1, []byte{1}, map[string][]byte{"a.txt": nil}); err == nil {
		t.Error("a shown file replaced the commit's own")
	}
}
