package copies

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// repo is an in-memory object store building trees from path → leaf maps.
type repo struct {
	trees map[encoding.ID]*encoding.Tree
	files map[encoding.ID][]byte
}

func newRepo() *repo {
	return &repo{trees: map[encoding.ID]*encoding.Tree{}, files: map[encoding.ID][]byte{}}
}

func (r *repo) file(contents string) encoding.ID {
	id, _ := encoding.IDOf(&encoding.File{Contents: []byte(contents)})
	r.files[id] = []byte(contents)
	return id
}

func symlinkID(target string) encoding.ID {
	id, _ := encoding.IDOf(&encoding.Symlink{Target: target})
	return id
}

// tree builds nested trees from paths to values ("f:<contents>", "x:<contents>" for an
// executable file, "l:<target>" for a symlink).
func (r *repo) tree(t *testing.T, leaves map[string]string) encoding.ID {
	t.Helper()
	type node struct {
		leaves map[string]encoding.TreeValue
		dirs   map[string]map[string]string
	}
	n := node{leaves: map[string]encoding.TreeValue{}, dirs: map[string]map[string]string{}}
	for p, v := range leaves {
		if dir, rest, ok := strings.Cut(p, "/"); ok {
			if n.dirs[dir] == nil {
				n.dirs[dir] = map[string]string{}
			}
			n.dirs[dir][rest] = v
			continue
		}
		kind, body, _ := strings.Cut(v, ":")
		switch kind {
		case "f", "x":
			n.leaves[p] = encoding.TreeValue{Kind: encoding.TreeValueFile, ID: r.file(body), Executable: kind == "x"}
		case "c":
			copyID := encoding.ID{7}
			n.leaves[p] = encoding.TreeValue{Kind: encoding.TreeValueFile, ID: r.file(body), CopyID: &copyID}
		case "l":
			n.leaves[p] = encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: symlinkID(body)}
		default:
			t.Fatalf("bad leaf %q", v)
		}
	}
	for dir, sub := range n.dirs {
		n.leaves[dir] = encoding.TreeValue{Kind: encoding.TreeValueTree, ID: r.tree(t, sub)}
	}
	tree := &encoding.Tree{}
	for name, v := range n.leaves {
		tree.Entries = append(tree.Entries, encoding.TreeEntry{Name: name, Value: v})
	}
	sort.Slice(tree.Entries, func(i, j int) bool { return tree.Entries[i].Name < tree.Entries[j].Name })
	id, err := encoding.IDOf(tree)
	if err != nil {
		t.Fatal(err)
	}
	if len(tree.Entries) > 0 {
		r.trees[id] = tree
	}
	return id
}

func (r *repo) readTree(_ context.Context, id encoding.ID) (*encoding.Tree, error) {
	t, ok := r.trees[id]
	if !ok {
		return nil, fmt.Errorf("no tree %s", id)
	}
	return t, nil
}

func (r *repo) readFile(_ context.Context, id encoding.ID) ([]byte, error) {
	b, ok := r.files[id]
	if !ok {
		return nil, fmt.Errorf("no file %s", id)
	}
	return b, nil
}

func (r *repo) track(t *testing.T, before, after map[string]string) []string {
	t.Helper()
	ctx := context.Background()
	changes, err := Changes(ctx, r.readTree, r.tree(t, before), r.tree(t, after))
	if err != nil {
		t.Fatal(err)
	}
	matches, err := Track(ctx, changes, r.readFile)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, m := range matches {
		verb := "rename"
		if m.Copy {
			verb = "copy"
		}
		out = append(out, fmt.Sprintf("%s %s -> %s", verb, m.Source.Path, m.Dest.Path))
	}
	return out
}

func TestChanges(t *testing.T) {
	r := newRepo()
	before := r.tree(t, map[string]string{
		"same":        "f:same",
		"edited":      "f:one",
		"chmod":       "f:mode",
		"recopied":    "f:body",
		"gone":        "f:gone",
		"dir/a":       "f:a",
		"dir/b":       "f:b",
		"becomes/dir": "f:inner",
		"link":        "l:target",
	})
	after := r.tree(t, map[string]string{
		"same":        "f:same",
		"edited":      "f:two",
		"chmod":       "x:mode",
		"recopied":    "c:body",
		"new":         "f:new",
		"dir":         "f:dir is now a file",
		"becomes/dir": "f:inner",
		"link":        "l:elsewhere",
	})
	changes, err := Changes(context.Background(), r.readTree, before, after)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range changes {
		got = append(got, fmt.Sprintf("%d %s %t", c.Kind, c.Path, c.Symlink))
	}
	want := []string{
		fmt.Sprintf("%d chmod false", Modification),
		fmt.Sprintf("%d dir/a false", Deletion),
		fmt.Sprintf("%d dir/b false", Deletion),
		fmt.Sprintf("%d dir false", Addition),
		fmt.Sprintf("%d edited false", Modification),
		fmt.Sprintf("%d gone false", Deletion),
		fmt.Sprintf("%d link true", Modification),
		fmt.Sprintf("%d new false", Addition),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes:\n got %q\nwant %q", got, want)
	}
}

func text(n int, tag string) string {
	var b strings.Builder
	for i := range n {
		fmt.Fprintf(&b, "%s line %d\n", tag, i)
	}
	return b.String()
}

func TestExactRename(t *testing.T) {
	r := newRepo()
	got := r.track(t, map[string]string{"a/x.txt": "f:" + text(5, "x"), "b/keep": "f:k"}, map[string]string{"c/x.txt": "f:" + text(5, "x"), "b/keep": "f:k"})
	if want := []string{"rename a/x.txt -> c/x.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// With identical contents at several deleted paths, the one sharing the destination's
// basename wins.
func TestExactRenamePrefersBasename(t *testing.T) {
	r := newRepo()
	body := "f:" + text(3, "same")
	got := r.track(t, map[string]string{"a/one": body, "b/two": body}, map[string]string{"c/two": body})
	if want := []string{"rename b/two -> c/two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSimilarityRename(t *testing.T) {
	r := newRepo()
	base := text(10, "body")
	edited := base + "one more line\n"
	got := r.track(t, map[string]string{"old.go": "f:" + base}, map[string]string{"new.go": "f:" + edited})
	if want := []string{"rename old.go -> new.go"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	// Mostly rewritten: under 50% similar, so no rename.
	got = r.track(t, map[string]string{"old.go": "f:" + base}, map[string]string{"new.go": "f:" + text(2, "body") + text(20, "other")})
	if len(got) != 0 {
		t.Fatalf("got %q, want no matches", got)
	}
}

func TestCopyFromModifiedFile(t *testing.T) {
	r := newRepo()
	base := text(10, "shared")
	got := r.track(t,
		map[string]string{"orig.go": "f:" + base},
		map[string]string{"orig.go": "f:" + base + "edit\n", "copy.go": "f:" + base + "edit\nmore\n"},
	)
	if want := []string{"copy orig.go -> copy.go"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// An unmodified file is not a copy source (copies come from the set of modified files).
func TestCopyNeedsModifiedSource(t *testing.T) {
	r := newRepo()
	base := text(10, "shared")
	got := r.track(t, map[string]string{"orig.go": "f:" + base}, map[string]string{"orig.go": "f:" + base, "copy.go": "f:" + base})
	if len(got) != 0 {
		t.Fatalf("got %q, want no matches", got)
	}
}

func TestBinaryMatchesOnlyExactly(t *testing.T) {
	r := newRepo()
	bin := "f:\x00" + text(10, "bin")
	got := r.track(t, map[string]string{"a.bin": bin}, map[string]string{"b.bin": bin + "x\n"})
	if len(got) != 0 {
		t.Fatalf("got %q, want no matches", got)
	}
	got = r.track(t, map[string]string{"a.bin": bin}, map[string]string{"b.bin": bin})
	if want := []string{"rename a.bin -> b.bin"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestEmptyFileIsNeverADestination(t *testing.T) {
	r := newRepo()
	got := r.track(t, map[string]string{"a": "f:"}, map[string]string{"b": "f:"})
	if len(got) != 0 {
		t.Fatalf("got %q, want no matches", got)
	}
}

func TestSymlinkRenamesExactly(t *testing.T) {
	r := newRepo()
	got := r.track(t, map[string]string{"l1": "l:../target"}, map[string]string{"l2": "l:../target"})
	if want := []string{"rename l1 -> l2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	got = r.track(t, map[string]string{"l1": "l:../target"}, map[string]string{"l2": "l:../targets"})
	if len(got) != 0 {
		t.Fatalf("got %q, want no matches", got)
	}
}

// 40 deletions × 30 additions exceed the limit: identity still matches, similarity does not.
func TestLimitSkipsSimilarity(t *testing.T) {
	r := newRepo()
	before, after := map[string]string{}, map[string]string{}
	for i := range 40 {
		before[fmt.Sprintf("old/%02d", i)] = "f:" + text(10, fmt.Sprintf("file%d", i))
	}
	for i := range 29 {
		after[fmt.Sprintf("new/%02d", i)] = "f:" + text(10, fmt.Sprintf("file%d", i)) + "edited\n"
	}
	after["new/exact"] = before["old/39"]
	got := r.track(t, before, after)
	if want := []string{"rename old/39 -> new/exact"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSimilarity(t *testing.T) {
	for _, tc := range []struct {
		old, cur string
		want     float32
	}{
		{"a\nb\nc\nd\n", "a\nb\nc\nd\n", 1},
		{"a\nb\nc\nd\n", "a\nb\n", 0.5},
		{"a\nb\n", "a\nb\nc\nd\n", 0.5},
		{"a\nb\n", "c\nd\n", 0},
	} {
		if got := Similarity([]byte(tc.old), []byte(tc.cur)); got != tc.want {
			t.Errorf("Similarity(%q, %q) = %v, want %v", tc.old, tc.cur, got, tc.want)
		}
	}
}
