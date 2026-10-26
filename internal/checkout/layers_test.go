package checkout

import (
	"slices"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// held is a source holding one layer of objects for repository 1.
func held(t *testing.T, objs ...encoding.Object) *scm {
	t.Helper()
	l := &decodedLayer{id: ID{9}, objs: map[layerKey][]byte{}}
	for _, o := range objs {
		c, err := encoding.Encode(o)
		if err != nil {
			t.Fatal(err)
		}
		l.objs[layerKey{o.Kind(), ID(encoding.Hash(c))}] = c[1:]
	}
	s := &scm{}
	s.layers.put(1, l)
	return s
}

func objID(t *testing.T, o encoding.Object) ID {
	t.Helper()
	id, err := encoding.IDOf(o)
	if err != nil {
		t.Fatal(err)
	}
	return ID(id)
}

// Two trees the held layers hold compare leaf by leaf, walking only subtrees whose IDs differ: a
// file edited, added, and gone; a file made executable; a symlink retargeted; a file becoming a
// directory and a directory a file. A tree no held layer holds cannot be compared from.
func TestDiffFromHeldLayers(t *testing.T) {
	f := func(name string, id ID, exec bool) encoding.TreeEntry {
		return encoding.TreeEntry{Name: name, Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: encoding.ID(id), Executable: exec}}
	}
	d := func(name string, id ID) encoding.TreeEntry {
		return encoding.TreeEntry{Name: name, Value: encoding.TreeValue{Kind: encoding.TreeValueTree, ID: encoding.ID(id)}}
	}
	l := func(name string, id ID) encoding.TreeEntry {
		return encoding.TreeEntry{Name: name, Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: encoding.ID(id)}}
	}
	tree := func(entries ...encoding.TreeEntry) *encoding.Tree {
		slices.SortFunc(entries, func(a, b encoding.TreeEntry) int { return strings.Compare(a.Name, b.Name) })
		return &encoding.Tree{Entries: entries}
	}
	one, two, three := id("one"), id("two"), id("three")
	toA, toB := &encoding.Symlink{Target: "a"}, &encoding.Symlink{Target: "b"}
	same := tree(f("kept", one, false))
	becomes := tree(f("inner", two, false))
	oldT := tree(f("edited", one, false), f("exec", one, false), f("gone", one, false), d("dir", objID(t, becomes)),
		f("file", one, false), l("link", objID(t, toA)), d("same", objID(t, same)))
	newDir := tree(f("x", three, false))
	newT := tree(f("added", three, false), f("dir", two, false), f("edited", two, false), f("exec", one, true),
		d("file", objID(t, newDir)), l("link", objID(t, toB)), d("same", objID(t, same)))
	s := held(t, same, becomes, oldT, newDir, newT, toA, toB)
	co := &checkoutOf{tree: objID(t, newT)}
	var got []string
	if err := s.diff(1, co, objID(t, oldT), objID(t, newT), "", func(c Change) {
		what := c.Path + " gone"
		if c.New != nil {
			what = c.Path + " " + c.New.Target
			if c.New.Kind == File {
				what = c.Path + " file"
			}
		}
		got = append(got, what)
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	want := []string{"added file", "dir file", "dir/inner gone", "edited file", "exec file", "file gone", "file/x file", "gone gone", "link b"}
	if !slices.Equal(got, want) {
		t.Errorf("changes %q\nwant    %q", got, want)
	}
	if err := s.diff(1, co, ID{1}, objID(t, newT), "", func(Change) {}); err != errNoTree {
		t.Errorf("from a tree no layer holds: %v", err)
	}
}
