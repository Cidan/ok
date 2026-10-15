package store

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// memTrees is a treeStore in memory.
type memTrees map[encoding.ID]*encoding.Tree

func (m memTrees) readTree(id encoding.ID) (*encoding.Tree, error) {
	if id == encoding.EmptyTreeID {
		return &encoding.Tree{}, nil
	}
	t, ok := m[id]
	if !ok {
		return nil, fmt.Errorf("no tree %s", id)
	}
	return t, nil
}

func (m memTrees) writeTree(t *encoding.Tree) (encoding.ID, error) {
	if len(t.Entries) == 0 {
		return encoding.EmptyTreeID, nil
	}
	id, err := encoding.IDOf(t)
	if err != nil {
		return id, err
	}
	m[id] = t
	return id, nil
}

// leaf describes a flat tree's leaf: file contents, "x:" for an executable file, "l:" for
// a symlink.
func leafValue(desc string) encoding.TreeValue {
	switch {
	case strings.HasPrefix(desc, "l:"):
		return encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: encoding.Hash(append([]byte{byte(encoding.KindSymlink)}, desc[2:]...))}
	case strings.HasPrefix(desc, "x:"):
		return encoding.TreeValue{Kind: encoding.TreeValueFile, ID: encoding.Hash(append([]byte{byte(encoding.KindFile)}, desc[2:]...)), Executable: true}
	}
	return encoding.TreeValue{Kind: encoding.TreeValueFile, ID: encoding.Hash(append([]byte{byte(encoding.KindFile)}, desc...))}
}

// build writes a tree holding the flat paths.
func (m memTrees) build(t *testing.T, flat map[string]string) encoding.ID {
	t.Helper()
	sub := map[string]map[string]string{}
	var tree encoding.Tree
	for p, desc := range flat {
		name, rest, nested := strings.Cut(p, "/")
		if !nested {
			tree.Entries = append(tree.Entries, encoding.TreeEntry{Name: name, Value: leafValue(desc)})
			continue
		}
		if sub[name] == nil {
			sub[name] = map[string]string{}
		}
		sub[name][rest] = desc
	}
	for name, f := range sub {
		tree.Entries = append(tree.Entries, encoding.TreeEntry{Name: name, Value: encoding.TreeValue{Kind: encoding.TreeValueTree, ID: m.build(t, f)}})
	}
	sort.Slice(tree.Entries, func(i, j int) bool { return tree.Entries[i].Name < tree.Entries[j].Name })
	id, err := m.writeTree(&tree)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// flatten lists a tree's leaves as path → value.
func (m memTrees) flatten(t *testing.T, id encoding.ID, dir string, out map[string]encoding.TreeValue) map[string]encoding.TreeValue {
	t.Helper()
	if out == nil {
		out = map[string]encoding.TreeValue{}
	}
	tree, err := m.readTree(id)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range tree.Entries {
		p := joinPath(dir, e.Name)
		if e.Value.Kind == encoding.TreeValueTree {
			if e.Value.ID == encoding.EmptyTreeID {
				t.Fatalf("%s: an empty subtree survived", p)
			}
			m.flatten(t, e.Value.ID, p, out)
		} else {
			out[p] = e.Value
		}
	}
	return out
}

func TestSplice(t *testing.T) {
	for _, tc := range []struct {
		name      string
		p, c, h   map[string]string
		want      map[string]string // nil when refused
		conflicts []string
	}{{
		name: "main untouched: the CL's tree",
		p:    map[string]string{"a": "1"},
		c:    map[string]string{"a": "2", "b/c": "3"},
		h:    map[string]string{"a": "1"},
		want: map[string]string{"a": "2", "b/c": "3"},
	}, {
		name: "disjoint changes",
		p:    map[string]string{"a/x": "1", "b/y": "1"},
		c:    map[string]string{"a/x": "2", "b/y": "1"},
		h:    map[string]string{"a/x": "1", "b/y": "9", "d/z": "new"},
		want: map[string]string{"a/x": "2", "b/y": "9", "d/z": "new"},
	}, {
		name:      "both changed one file",
		p:         map[string]string{"a/x": "1", "k": "k"},
		c:         map[string]string{"a/x": "2", "k": "k"},
		h:         map[string]string{"a/x": "3", "k": "k"},
		conflicts: []string{"a/x"},
	}, {
		name: "main already carries some of the change",
		p:    map[string]string{"a": "1", "b": "1"},
		c:    map[string]string{"a": "2", "b": "2"},
		h:    map[string]string{"a": "2", "b": "1", "z": "z"},
		want: map[string]string{"a": "2", "b": "2", "z": "z"},
	}, {
		name: "a deletion empties a directory, which vanishes",
		p:    map[string]string{"d/x": "1", "k": "k"},
		c:    map[string]string{"k": "k"},
		h:    map[string]string{"d/x": "1", "k": "k", "m": "m"},
		want: map[string]string{"k": "k", "m": "m"},
	}, {
		name:      "the CL deletes what main modified",
		p:         map[string]string{"d/x": "1"},
		c:         map[string]string{"k": "k"},
		h:         map[string]string{"d/x": "2"},
		conflicts: []string{"d/x"},
	}, {
		name:      "the CL modifies what main deleted",
		p:         map[string]string{"d/x": "1", "k": "k"},
		c:         map[string]string{"d/x": "2", "k": "k"},
		h:         map[string]string{"k": "k"},
		conflicts: []string{"d/x"},
	}, {
		name:      "the CL adds a file where main added a directory",
		p:         map[string]string{"k": "k"},
		c:         map[string]string{"k": "k", "a/b": "file"},
		h:         map[string]string{"k": "k", "a/b/c": "in dir"},
		conflicts: []string{"a/b"},
	}, {
		name:      "the CL adds a directory where main added a file",
		p:         map[string]string{"k": "k"},
		c:         map[string]string{"k": "k", "a/b/c": "in dir"},
		h:         map[string]string{"k": "k", "a/b": "file"},
		conflicts: []string{"a/b"},
	}, {
		name:      "an executable bit is part of a path's value",
		p:         map[string]string{"s": "1"},
		c:         map[string]string{"s": "x:1"},
		h:         map[string]string{"s": "2"},
		conflicts: []string{"s"},
	}, {
		name: "the CL turns a file into a directory main left alone",
		p:    map[string]string{"a": "1", "k": "k"},
		c:    map[string]string{"a/x": "1", "k": "k"},
		h:    map[string]string{"a": "1", "k": "2"},
		want: map[string]string{"a/x": "1", "k": "2"},
	}, {
		name: "a file and a symlink at one path differ",
		p:    map[string]string{"s": "1", "k": "k"},
		c:    map[string]string{"s": "l:1", "k": "k"},
		h:    map[string]string{"s": "1", "k": "2"},
		want: map[string]string{"s": "l:1", "k": "2"},
	}, {
		name: "everything the CL does is already on main",
		p:    map[string]string{"a": "1"},
		c:    map[string]string{"a": "2"},
		h:    map[string]string{"a": "2", "z": "z"},
		want: map[string]string{"a": "2", "z": "z"},
	}, {
		name:      "every conflict is named",
		p:         map[string]string{"a": "1", "b/c": "1", "d": "1"},
		c:         map[string]string{"a": "2", "b/c": "2", "d": "2"},
		h:         map[string]string{"a": "3", "b/c": "3", "d": "1"},
		conflicts: []string{"a", "b/c"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			m := memTrees{}
			sp := &splicer{ts: m}
			r, err := sp.splice(m.build(t, tc.p), m.build(t, tc.c), m.build(t, tc.h), "")
			if err != nil {
				t.Fatal(err)
			}
			if got := sp.conflictPaths(); !reflect.DeepEqual(got, tc.conflicts) {
				t.Fatalf("conflicts %v, want %v", got, tc.conflicts)
			}
			if tc.want == nil {
				return
			}
			want := map[string]encoding.TreeValue{}
			for p, d := range tc.want {
				want[p] = leafValue(d)
			}
			if got := m.flatten(t, r, "", nil); !reflect.DeepEqual(got, want) {
				t.Fatalf("landed %v, want %v", got, want)
			}
			if r != m.build(t, tc.want) {
				t.Fatal("the landed tree is not canonical")
			}
		})
	}
}

// TestSpliceOnlyReadsWhatDiffers checks the cost follows the change: subtrees whose IDs
// match are never read.
func TestSpliceOnlyReadsWhatDiffers(t *testing.T) {
	m := memTrees{}
	big := map[string]string{}
	for i := range 200 {
		big[fmt.Sprintf("big/d%03d/f", i)] = fmt.Sprint(i)
	}
	p := copyFlat(big, map[string]string{"small/x": "1"})
	c := copyFlat(big, map[string]string{"small/x": "2"})
	h := copyFlat(big, map[string]string{"small/x": "1", "other": "o"})
	pid, cid, hid := m.build(t, p), m.build(t, c), m.build(t, h)
	counting := &countingTrees{memTrees: m}
	sp := &splicer{ts: counting}
	if _, err := sp.splice(pid, cid, hid, ""); err != nil {
		t.Fatal(err)
	}
	if counting.reads > 6 {
		t.Fatalf("read %d trees for a one-file change", counting.reads)
	}
}

type countingTrees struct {
	memTrees
	reads int
}

func (c *countingTrees) readTree(id encoding.ID) (*encoding.Tree, error) {
	c.reads++
	return c.memTrees.readTree(id)
}

func copyFlat(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

func TestDiffTrees(t *testing.T) {
	m := memTrees{}
	a := m.build(t, map[string]string{"a": "1", "d/x": "1", "d/y": "1", "f": "file", "s": "1"})
	b := m.build(t, map[string]string{"a": "1", "d/x": "2", "f/g": "now a dir", "n/new": "n", "s": "x:1"})
	var got []string
	if err := diffTrees(m, a, b, "", func(ch leafChange) error {
		got = append(got, fmt.Sprintf("%s %v→%v", ch.Path, ch.Before != nil, ch.After != nil))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"d/x true→true", "d/y true→false", "f true→false", "f/g false→true", "n/new false→true", "s true→true"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("diff\n got %v\nwant %v", got, want)
	}
}

// batchedTrees is a memTrees that reads many trees at once, counting its reads.
type batchedTrees struct {
	memTrees
	singles int
	batches [][]encoding.ID
}

func (b *batchedTrees) readTree(id encoding.ID) (*encoding.Tree, error) {
	b.singles++
	return b.memTrees.readTree(id)
}

func (b *batchedTrees) readTrees(ids []encoding.ID) (map[encoding.ID]*encoding.Tree, error) {
	b.batches = append(b.batches, ids)
	out := map[encoding.ID]*encoding.Tree{}
	for _, id := range ids {
		t, err := b.memTrees.readTree(id)
		if err != nil {
			return nil, err
		}
		out[id] = t
	}
	return out, nil
}

// wide is a tree of 10 directories of 10 directories of one file each, and a file at the root.
func wide(t *testing.T, m memTrees, change string) encoding.ID {
	flat := map[string]string{"README": "r"}
	for a := range 10 {
		for b := range 10 {
			flat[fmt.Sprintf("d%d/e%d/f", a, b)] = fmt.Sprintf("%d%d", a, b)
		}
	}
	if change != "" {
		flat[change] = "changed"
	}
	return m.build(t, flat)
}

// Over a store that reads many trees at once, a diff reads each level of the trees in one read
// and no tree alone, and reports what the one-at-a-time walk does.
func TestDiffTreesBatched(t *testing.T) {
	m := memTrees{}
	before, after := wide(t, m, ""), wide(t, m, "d3/e4/f")
	for name, tc := range map[string]struct {
		a, b  encoding.ID
		sizes string // each batch's size
	}{
		"everything added": {encoding.EmptyTreeID, after, "[1 10 100]"},
		"one file changed": {before, after, "[2 2 2]"},
	} {
		var want, got []string
		collect := func(into *[]string) func(leafChange) error {
			return func(ch leafChange) error {
				*into = append(*into, fmt.Sprintf("%s %v→%v", ch.Path, ch.Before != nil, ch.After != nil))
				return nil
			}
		}
		if err := diffTrees(m, tc.a, tc.b, "", collect(&want)); err != nil {
			t.Fatal(err)
		}
		bt := &batchedTrees{memTrees: m}
		if err := diffTrees(bt, tc.a, tc.b, "", collect(&got)); err != nil {
			t.Fatal(err)
		}
		var sizes []int
		for _, b := range bt.batches {
			sizes = append(sizes, len(b))
		}
		if !reflect.DeepEqual(got, want) || bt.singles != 0 || fmt.Sprint(sizes) != tc.sizes {
			t.Errorf("%s: %d changes (want %d), %d single reads, batches %v (want %s)", name, len(got), len(want), bt.singles, sizes, tc.sizes)
		}
	}
}

// A walk's prefetch reads a level of the trees in one read, descending only where it is told.
func TestPrefetchWalk(t *testing.T) {
	m := memTrees{}
	root := wide(t, m, "")
	bt := &batchedTrees{memTrees: m}
	trees, err := prefetchWalk(bt, root, func(dir string) bool { return dir == "d3" || strings.HasPrefix(dir, "d3/") })
	if err != nil {
		t.Fatal(err)
	}
	var sizes []int
	for _, b := range bt.batches {
		sizes = append(sizes, len(b))
	}
	if fmt.Sprint(sizes) != "[1 1 10]" || len(trees) != 13 {
		t.Errorf("batches %v, %d trees", sizes, len(trees))
	}
}
