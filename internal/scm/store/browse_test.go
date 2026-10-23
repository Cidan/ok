package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/scm/encoding"
)

// browseEnv is a repository whose main is three landed commits:
//
//	c1: a/x.txt "1 2 3", b.txt, link -> a/x.txt
//	c2: a/x.txt "1 two 3"
//	c3: b.txt renamed to c.txt, a/y.txt a copy of a/x.txt with a line added, a/x.txt
//	    changed again
//
// and one open CL off main, p, changing a/x.txt's last line.
type browseEnv struct {
	*clEnv
	c1, c2, c3, p encoding.ID
}

const bText = "alpha\nbeta\ngamma\ndelta\nepsilon\nzeta\neta\ntheta\n"

func newBrowseEnv(t *testing.T) *browseEnv {
	e := &browseEnv{clEnv: newCLEnv(t)}
	land := func(parent encoding.ID, desc string, build func(b *builder) encoding.ID) encoding.ID {
		t.Helper()
		b := newBuilder(t)
		sig := encoding.Signature{Name: "alice", Email: "alice@example.com", Time: encoding.Timestamp{Millis: 1_790_000_000_000 + int64(len(desc))}}
		id := b.add(&encoding.Commit{
			Parents: []encoding.ID{parent}, RootTree: encoding.Merge[encoding.ID]{build(b)}, ConflictLabels: encoding.Merge[string]{""},
			ChangeID: newChange(), Description: desc, Author: sig, Committer: sig,
		})
		mustPublish(t, e.s, e.repo, alice, b, nil)
		n := e.open(id)
		e.mustLand(LandItem{Number: n, Patchset: 1})
		if e.head() != id {
			t.Fatalf("%s did not land by fast-forward", desc)
		}
		return id
	}
	e.c1 = land(encoding.RootCommitID, "one", func(b *builder) encoding.ID {
		return b.tree(
			treeEntry("a", b.tree(fileEntry("x.txt", b.file([]byte("1\n2\n3\n"))))),
			fileEntry("b.txt", b.file([]byte(bText))),
			encoding.TreeEntry{Name: "link", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: b.symlink("a/x.txt")}},
		)
	})
	e.c2 = land(e.c1, "two", func(b *builder) encoding.ID {
		return b.tree(
			treeEntry("a", b.tree(fileEntry("x.txt", b.file([]byte("1\ntwo\n3\n"))))),
			fileEntry("b.txt", b.file([]byte(bText))),
			encoding.TreeEntry{Name: "link", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: b.symlink("a/x.txt")}},
		)
	})
	e.c3 = land(e.c2, "three", func(b *builder) encoding.ID {
		return b.tree(
			treeEntry("a", b.tree(
				fileEntry("x.txt", b.file([]byte("1\ntwo\n3\nfour\n"))),
				fileEntry("y.txt", b.file([]byte("1\ntwo\n3\nfour\nfive\n"))),
			)),
			fileEntry("c.txt", b.file([]byte(bText))),
			encoding.TreeEntry{Name: "link", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: b.symlink("a/x.txt")}},
		)
	})
	b := newBuilder(t)
	sig := encoding.Signature{Name: "bob", Email: "bob@example.com", Time: encoding.Timestamp{Millis: 1_790_000_100_000}}
	e.p = b.add(&encoding.Commit{
		Parents: []encoding.ID{e.c3}, RootTree: encoding.Merge[encoding.ID]{b.tree(
			treeEntry("a", b.tree(
				fileEntry("x.txt", b.file([]byte("1\ntwo\n3\nFOUR\n"))),
				fileEntry("y.txt", b.file([]byte("1\ntwo\n3\nfour\nfive\n"))),
			)),
			fileEntry("c.txt", b.file([]byte(bText))),
			encoding.TreeEntry{Name: "link", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: b.symlink("a/x.txt")}},
		)}, ConflictLabels: encoding.Merge[string]{""},
		ChangeID: newChange(), Description: "patch", Author: sig, Committer: sig,
	})
	mustPublish(t, e.s, e.repo, bob, b, nil)
	e.mustUpload(bob, Ready, e.p)
	return e
}

func (e *browseEnv) tree(t *testing.T, commit encoding.ID) encoding.ID {
	t.Helper()
	tree, err := e.s.resolvedTree(context.Background(), e.repo, commit)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

func commitIDs(cs []CommitInfo) []encoding.ID {
	var out []encoding.ID
	for _, c := range cs {
		out = append(out, c.ID)
	}
	return out
}

func TestBrowseCommitsAndLog(t *testing.T) {
	ctx := context.Background()
	e := newBrowseEnv(t)

	got, err := e.s.GetCommits(ctx, e.repo, []encoding.ID{e.c2, e.c1})
	if err != nil || len(got) != 2 || got[0].ID != e.c2 || got[0].Commit.Description != "two" || got[1].Commit.Parents[0] != encoding.RootCommitID {
		t.Fatalf("GetCommits: %+v, %v", got, err)
	}
	if _, err := e.s.GetCommits(ctx, e.repo, []encoding.ID{encoding.RootCommitID}); !errors.Is(err, ErrInvalid) {
		t.Errorf("the root commit: %v", err)
	}
	if _, err := e.s.GetCommits(ctx, e.repo, []encoding.ID{{9}}); !errors.Is(err, ErrNotFound) {
		t.Errorf("an unknown commit: %v", err)
	}

	for _, tc := range []struct {
		name  string
		from  *encoding.ID
		paths []string
		want  []encoding.ID
	}{
		{"all of main", nil, nil, []encoding.ID{e.c3, e.c2, e.c1}},
		{"from a commit", &e.c2, nil, []encoding.ID{e.c2, e.c1}},
		{"a directory", nil, []string{"a"}, []encoding.ID{e.c3, e.c2, e.c1}},
		{"a directory with a trailing slash", nil, []string{"a/"}, []encoding.ID{e.c3, e.c2, e.c1}},
		{"a file", nil, []string{"b.txt"}, []encoding.ID{e.c3, e.c1}},
		{"a renamed file's new path", nil, []string{"c.txt"}, []encoding.ID{e.c3}},
		{"a file added later", nil, []string{"a/y.txt"}, []encoding.ID{e.c3}},
		{"a string some path starts with is not a path", nil, []string{"a/x"}, nil},
		{"two paths, from a commit", &e.c2, []string{"link", "a/x.txt"}, []encoding.ID{e.c2, e.c1}},
	} {
		page, err := e.s.Log(ctx, e.repo, tc.from, tc.paths, 0, 0, 0)
		if err != nil || page.Older != 0 || page.Newer != 0 || !reflect.DeepEqual(commitIDs(page.Commits), tc.want) {
			t.Errorf("%s: %v older %d newer %d, %v; want %v", tc.name, commitIDs(page.Commits), page.Older, page.Newer, err, tc.want)
		}
	}

	// Paging older, then back newer, by the cursors each page returns.
	first, err := e.s.Log(ctx, e.repo, nil, nil, 2, 0, 0)
	if err != nil || !reflect.DeepEqual(commitIDs(first.Commits), []encoding.ID{e.c3, e.c2}) || first.Older == 0 || first.Newer != 0 {
		t.Fatalf("first page: %v older %d newer %d, %v", commitIDs(first.Commits), first.Older, first.Newer, err)
	}
	second, err := e.s.Log(ctx, e.repo, nil, nil, 2, first.Older, 0)
	if err != nil || !reflect.DeepEqual(commitIDs(second.Commits), []encoding.ID{e.c1}) || second.Older != 0 || second.Newer == 0 {
		t.Fatalf("second page: %v older %d newer %d, %v", commitIDs(second.Commits), second.Older, second.Newer, err)
	}
	back, err := e.s.Log(ctx, e.repo, nil, nil, 2, 0, second.Newer)
	if err != nil || !reflect.DeepEqual(commitIDs(back.Commits), []encoding.ID{e.c3, e.c2}) || back.Newer != 0 || back.Older == 0 {
		t.Fatalf("back to the newest: %v older %d newer %d, %v", commitIDs(back.Commits), back.Older, back.Newer, err)
	}
	one, err := e.s.Log(ctx, e.repo, nil, nil, 1, 0, 0)
	if err != nil || len(one.Commits) != 1 {
		t.Fatal(one, err)
	}
	middle, err := e.s.Log(ctx, e.repo, nil, nil, 1, one.Older, 0)
	if err != nil || !reflect.DeepEqual(commitIDs(middle.Commits), []encoding.ID{e.c2}) || middle.Older == 0 || middle.Newer == 0 {
		t.Fatalf("a middle page: %v older %d newer %d, %v", commitIDs(middle.Commits), middle.Older, middle.Newer, err)
	}
	filtered, err := e.s.Log(ctx, e.repo, nil, []string{"b.txt"}, 1, 0, 0)
	if err != nil || !reflect.DeepEqual(commitIDs(filtered.Commits), []encoding.ID{e.c3}) || filtered.Older == 0 {
		t.Fatalf("a filtered first page: %v older %d, %v", commitIDs(filtered.Commits), filtered.Older, err)
	}
	filtered, err = e.s.Log(ctx, e.repo, nil, []string{"b.txt"}, 1, filtered.Older, 0)
	if err != nil || !reflect.DeepEqual(commitIDs(filtered.Commits), []encoding.ID{e.c1}) || filtered.Older != 0 || filtered.Newer == 0 {
		t.Fatalf("a filtered page skips commits that did not touch the path: %v older %d newer %d, %v", commitIDs(filtered.Commits), filtered.Older, filtered.Newer, err)
	}

	if _, err := e.s.Log(ctx, e.repo, &e.p, nil, 0, 0, 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("Log from a commit off main: %v", err)
	}
	if _, err := e.s.Log(ctx, e.repo, nil, []string{"../etc"}, 0, 0, 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("a bad path: %v", err)
	}
	if _, err := e.s.Log(ctx, e.repo, nil, nil, 0, 2, 1); !errors.Is(err, ErrInvalid) {
		t.Errorf("both directions at once: %v", err)
	}
	if _, err := e.s.Log(ctx, 1<<40, nil, nil, 0, 0, 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("an unknown repository: %v", err)
	}
	empty := newRepo(t, e.s)
	if page, err := e.s.Log(ctx, empty.ID, nil, nil, 0, 0, 0); err != nil || len(page.Commits) != 0 || page.Older != 0 {
		t.Errorf("a new repository's log: %+v %v", page, err)
	}
}

func TestBrowseTreesAndFiles(t *testing.T) {
	ctx := context.Background()
	e := newBrowseEnv(t)

	_, root, err := e.s.ReadTree(ctx, e.repo, e.c3, "", true)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, en := range root {
		names = append(names, en.Name)
		switch en.Name {
		case "c.txt":
			if en.Value.Kind != encoding.TreeValueFile || en.Value.Size != uint64(len(bText)) {
				t.Errorf("c.txt: %+v", en.Value)
			}
		case "link":
			if en.Value.Kind != encoding.TreeValueSymlink || en.Value.SymlinkTarget != "a/x.txt" {
				t.Errorf("link: %+v", en.Value)
			}
		case "a":
			if en.Value.Kind != encoding.TreeValueTree {
				t.Errorf("a: %+v", en.Value)
			}
		}
	}
	if strings.Join(names, " ") != "a c.txt link" {
		t.Errorf("root entries %v", names)
	}
	// Each entry's last commit: the newest on main that changed it, directories included.
	last := map[string]encoding.ID{}
	for _, en := range root {
		if en.LastCommit != nil {
			last[en.Name] = *en.LastCommit
		}
	}
	if want := map[string]encoding.ID{"a": e.c3, "c.txt": e.c3, "link": e.c1}; !reflect.DeepEqual(last, want) {
		t.Errorf("last commits at c3: %v, want %v", last, want)
	}
	_, atC2, err := e.s.ReadTree(ctx, e.repo, e.c2, "", true)
	if err != nil {
		t.Fatal(err)
	}
	for _, en := range atC2 {
		want := map[string]encoding.ID{"a": e.c2, "b.txt": e.c1, "link": e.c1}[en.Name]
		if en.LastCommit == nil || *en.LastCommit != want {
			t.Errorf("at c2, %s last changed at %v, want %s", en.Name, en.LastCommit, want)
		}
	}
	_, offMain, err := e.s.ReadTree(ctx, e.repo, e.p, "", true)
	if err != nil || offMain[0].LastCommit != nil {
		t.Errorf("a commit off main has no history to name: %+v, %v", offMain, err)
	}
	_, sub, err := e.s.ReadTree(ctx, e.repo, e.c3, "/a/", true)
	if err != nil || len(sub) != 2 || sub[1].Name != "y.txt" || *sub[0].LastCommit != e.c3 || *sub[1].LastCommit != e.c3 {
		t.Errorf("a/: %+v, %v", sub, err)
	}
	for _, bad := range []string{"nope", "c.txt"} {
		if _, _, err := e.s.ReadTree(ctx, e.repo, e.c3, bad, false); !errors.Is(err, ErrNotFound) {
			t.Errorf("ReadTree %s: %v", bad, err)
		}
	}
	if _, _, err := e.s.ReadTree(ctx, e.repo, encoding.RootCommitID, "", true); err != nil {
		t.Errorf("the root commit's tree: %v", err)
	}

	read := func(p string, off, n uint64) (*BrowseValue, []byte, error) {
		var v *BrowseValue
		var data bytes.Buffer
		err := e.s.ReadFileAt(ctx, e.repo, e.c3, p, off, n, func(bv *BrowseValue, d []byte) error {
			if bv != nil {
				v = bv
			}
			data.Write(d)
			return nil
		})
		return v, data.Bytes(), err
	}
	if v, data, err := read("c.txt", 0, 0); err != nil || string(data) != bText || v.Size != uint64(len(bText)) {
		t.Errorf("c.txt: %+v %q %v", v, data, err)
	}
	if _, data, err := read("c.txt", 6, 4); err != nil || string(data) != "beta" {
		t.Errorf("a range of c.txt: %q %v", data, err)
	}
	if v, data, err := read("link", 0, 0); err != nil || v.SymlinkTarget != "a/x.txt" || len(data) != 0 {
		t.Errorf("link: %+v %q %v", v, data, err)
	}
	if _, _, err := read("a", 0, 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("a directory: %v", err)
	}
	if _, _, err := read("c.txt", 100, 10); !errors.Is(err, ErrInvalid) {
		t.Errorf("an offset past the end: %v", err)
	}
	if _, data, err := read("c.txt", uint64(len(bText))-6, 100); err != nil || string(data) != "theta\n" {
		t.Errorf("a length past the end reads to the end: %q %v", data, err)
	}

	// Export: every leaf in path order, each file followed by its bytes.
	var got []string
	var cur *ExportEntry
	var body bytes.Buffer
	flush := func() {
		if cur != nil {
			got = append(got, cur.Path+"="+body.String())
		}
		body.Reset()
	}
	if err := e.s.Export(ctx, e.repo, e.c3, nil, true, func(en *ExportEntry, d []byte) error {
		if en != nil {
			flush()
			cur = en
			if en.Value.Kind == encoding.TreeValueSymlink {
				body.WriteString("->" + en.Value.SymlinkTarget)
			}
		}
		body.Write(d)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	flush()
	want := []string{"a/x.txt=1\ntwo\n3\nfour\n", "a/y.txt=1\ntwo\n3\nfour\nfive\n", "c.txt=" + bText, "link=->a/x.txt"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("export:\n%q\nwant\n%q", got, want)
	}
	var paths []string
	if err := e.s.Export(ctx, e.repo, e.c3, []string{"a/y.txt", "link"}, true, func(en *ExportEntry, _ []byte) error {
		if en != nil {
			paths = append(paths, en.Path)
		}
		return nil
	}); err != nil || strings.Join(paths, " ") != "a/y.txt link" {
		t.Errorf("export under prefixes: %v %v", paths, err)
	}
	// Entries alone: the same entries, with sizes and symlink targets, and no contents.
	var entries []string
	if err := e.s.Export(ctx, e.repo, e.c3, nil, false, func(en *ExportEntry, d []byte) error {
		if d != nil {
			t.Errorf("contents with entries only: %q", d)
		}
		if en != nil {
			entries = append(entries, fmt.Sprintf("%s:%d:%s", en.Path, en.Value.Size, en.Value.SymlinkTarget))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	x, y := "1\ntwo\n3\nfour\n", "1\ntwo\n3\nfour\nfive\n"
	wantEntries := []string{fmt.Sprintf("a/x.txt:%d:", len(x)), fmt.Sprintf("a/y.txt:%d:", len(y)), fmt.Sprintf("c.txt:%d:", len(bText)), "link:0:a/x.txt"}
	if !reflect.DeepEqual(entries, wantEntries) {
		t.Errorf("entries only:\n%q\nwant\n%q", entries, wantEntries)
	}
}

func TestBrowseDiffs(t *testing.T) {
	ctx := context.Background()
	e := newBrowseEnv(t)
	t1, t2, t3 := e.tree(t, e.c1), e.tree(t, e.c2), e.tree(t, e.c3)

	summary := func(changes []PathChange) string {
		var out []string
		for _, c := range changes {
			s := map[PathChangeKind]string{PathAdded: "A", PathDeleted: "D", PathModified: "M", PathRenamed: "R", PathCopied: "C", PathTypeChanged: "T"}[c.Kind] + " " + c.Path
			if c.OldPath != "" {
				s += " <- " + c.OldPath
			}
			out = append(out, s)
		}
		return strings.Join(out, "; ")
	}
	for _, tc := range []struct {
		name     string
		from, to encoding.ID
		prefixes []string
		renames  bool
		want     string
	}{
		{"a modification", t1, t2, nil, true, "M a/x.txt"},
		{"without rename detection", t2, t3, nil, false, "M a/x.txt; A a/y.txt; D b.txt; A c.txt"},
		{"a rename and a copy", t2, t3, nil, true, "M a/x.txt; C a/y.txt <- a/x.txt; R c.txt <- b.txt"},
		{"under a prefix", t2, t3, []string{"a", "a/x.txt"}, false, "M a/x.txt; A a/y.txt"},
		{"from the empty tree", encoding.ID{}, t1, nil, true, "A a/x.txt; A b.txt; A link"},
		{"nothing", t3, t3, nil, true, ""},
	} {
		got, err := e.s.DiffTrees(ctx, e.repo, tc.from, tc.to, tc.prefixes, tc.renames, 0)
		if err != nil || summary(got) != tc.want {
			t.Errorf("%s: %q, %v; want %q", tc.name, summary(got), err, tc.want)
		}
	}
	strict, err := e.s.DiffTrees(ctx, e.repo, t2, t3, nil, true, 100)
	if err != nil || summary(strict) != "M a/x.txt; A a/y.txt; R c.txt <- b.txt" {
		t.Errorf("at 100%% similarity only the identical rename remains: %q %v", summary(strict), err)
	}
	full, err := e.s.DiffTrees(ctx, e.repo, t2, t3, nil, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range full {
		if c.Path == "c.txt" && (c.Before == nil || c.After == nil || c.Before.Size != uint64(len(bText))) {
			t.Errorf("the rename's values: %+v %+v", c.Before, c.After)
		}
	}

	// File diffs: hunks with context, zero context, an added file, and a binary file.
	var x2, x3 encoding.ID
	for _, c := range full {
		if c.Path == "a/x.txt" {
			x2, x3 = c.Before.ID, c.After.ID
		}
	}
	b := newBuilder(t)
	bin := b.file([]byte("\x00\x01binary"))
	long := b.file([]byte("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n"))
	longer := b.file([]byte("1\nTWO\n3\n4\n5\n6\n7\n8\n9\n10\nELEVEN\n12"))
	b.commit(b.tree(fileEntry("bin", bin), fileEntry("long", long), fileEntry("longer", longer)), "files", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	diffs, err := e.s.DiffFiles(ctx, e.repo, []FilePair{
		{Old: &x2, New: &x3},
		{New: &x3},
		{Old: &x2, New: &bin},
		{Old: &long, New: &longer},
	}, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	render := func(d FileDiff) string {
		var out []string
		for _, h := range d.Hunks {
			s := ""
			for _, l := range h.Lines {
				s += map[DiffOp]string{DiffContext: " ", DiffAdd: "+", DiffDelete: "-"}[l.Op] + string(l.Text)
				if l.NoNewline {
					s += "$"
				}
				s += "|"
			}
			out = append(out, fmt.Sprintf("%d,%d %d,%d %s", h.OldStart, h.OldLines, h.NewStart, h.NewLines, s))
		}
		return strings.Join(out, " / ")
	}
	if got := render(diffs[0]); got != "1,3 1,4  1| two| 3|+four|" {
		t.Errorf("a modification: %q", got)
	}
	if got := render(diffs[1]); got != "0,0 1,4 +1|+two|+3|+four|" {
		t.Errorf("an added file: %q", got)
	}
	if !diffs[2].Binary || diffs[2].Hunks != nil {
		t.Errorf("a binary file: %+v", diffs[2])
	}
	// Changes eight lines apart: two hunks with three lines of context. The last line
	// lost its newline, so it changed too.
	if got := render(diffs[3]); got != "1,5 1,5  1|-2|+TWO| 3| 4| 5| / 8,5 8,5  8| 9| 10|-11|-12|+ELEVEN|+12$|" {
		t.Errorf("two hunks: %q", got)
	}
	// Each diff counts the lines it adds and removes, a binary one none.
	for i, want := range [][2]int{{1, 0}, {4, 0}, {0, 0}, {3, 3}} {
		if d := diffs[i]; d.Additions != want[0] || d.Deletions != want[1] {
			t.Errorf("diff %d counts +%d −%d, want +%d −%d", i, d.Additions, d.Deletions, want[0], want[1])
		}
	}
	zero, err := e.s.DiffFiles(ctx, e.repo, []FilePair{{Old: &long, New: &longer}}, 0, false)
	if err != nil || len(zero[0].Hunks) != 2 {
		t.Fatalf("zero context: %+v %v", zero, err)
	}
	if h := zero[0].Hunks[0]; h.OldStart != 2 || h.OldLines != 1 || h.NewStart != 2 || h.NewLines != 1 || len(h.Lines) != 2 {
		t.Errorf("a zero-context hunk: %+v", h)
	}
}

func TestBrowseAnnotateAndAncestry(t *testing.T) {
	ctx := context.Background()
	e := newBrowseEnv(t)

	blame := func(commit encoding.ID, p string) []AnnotateRange {
		t.Helper()
		r, err := e.s.Annotate(ctx, e.repo, commit, p)
		if err != nil {
			t.Fatalf("annotate %s: %v", p, err)
		}
		return r
	}
	if got, want := blame(e.c3, "a/x.txt"), []AnnotateRange{{1, 1, e.c1}, {2, 1, e.c2}, {3, 1, e.c1}, {4, 1, e.c3}}; !reflect.DeepEqual(got, want) {
		t.Errorf("a/x.txt on main:\n%+v\nwant\n%+v", got, want)
	}
	if got, want := blame(e.c2, "a/x.txt"), []AnnotateRange{{1, 1, e.c1}, {2, 1, e.c2}, {3, 1, e.c1}}; !reflect.DeepEqual(got, want) {
		t.Errorf("a/x.txt at c2: %+v", got)
	}
	// A patchset off main: its own line, then main's history.
	if got, want := blame(e.p, "a/x.txt"), []AnnotateRange{{1, 1, e.c1}, {2, 1, e.c2}, {3, 1, e.c1}, {4, 1, e.p}}; !reflect.DeepEqual(got, want) {
		t.Errorf("a/x.txt at the patchset: %+v", got)
	}
	// A renamed file's history starts at the rename: paths are followed, not renames.
	if got := blame(e.c3, "c.txt"); len(got) != 1 || got[0].Commit != e.c3 || got[0].LineCount != 8 {
		t.Errorf("c.txt: %+v", got)
	}
	if _, err := e.s.Annotate(ctx, e.repo, e.c3, "a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("annotating a directory: %v", err)
	}

	for _, tc := range []struct {
		a, d encoding.ID
		want bool
	}{
		{e.c1, e.c3, true},
		{e.c3, e.c1, false},
		{e.c2, e.c2, true},
		{encoding.RootCommitID, e.c1, true},
		{e.c1, e.p, true},
		{e.p, e.c3, false},
	} {
		got, err := e.s.IsAncestor(ctx, e.repo, tc.a, tc.d)
		if err != nil || got != tc.want {
			t.Errorf("IsAncestor(%s, %s) = %v, %v; want %v", tc.a, tc.d, got, err, tc.want)
		}
	}
	if _, err := e.s.IsAncestor(ctx, e.repo, encoding.ID{7}, e.c1); !errors.Is(err, ErrNotFound) {
		t.Errorf("an unknown commit: %v", err)
	}
}

// A repository is made under the ID and network the directory chose, once: names are the
// directory's.
func TestCreateRepository(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	for _, ids := range [][2]int64{{0, 1}, {1, 0}} {
		if _, err := s.CreateRepository(ctx, ids[0], ids[1], "alice.human.example.com"); !errors.Is(err, ErrInvalid) {
			t.Errorf("made under %v: %v", ids, err)
		}
	}
	id, net := data.NewID(), data.NewID()
	r, err := s.CreateRepository(ctx, id, net, "alice.human.example.com")
	if err != nil || r.ID != id || int64(r.Network) != net || r.Head != encoding.RootCommitID || r.CreatedBy != "alice.human.example.com" {
		t.Fatalf("CreateRepository = %+v, %v", r, err)
	}
	if _, err := s.CreateRepository(ctx, id, data.NewID(), "bob.human.example.com"); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("an ID made twice: %v, want ErrAlreadyExists", err)
	}
	if got, err := s.GetRepository(ctx, id); err != nil || got.ID != id {
		t.Errorf("the repository by its ID: %+v, %v", got, err)
	}
}

// ReadFilesAt reads many paths of a commit at once, in their order: files with their heads,
// directories and symlinks with their values, and nothing for a path the commit does not hold or
// that runs through a file.
func TestReadFilesAt(t *testing.T) {
	e := newBrowseEnv(t)
	ctx := context.Background()
	paths := []string{"a/x.txt", "a/OWNERS", "OWNERS", "a", "link", "c.txt", "c.txt/below", "", "a/y.txt", "a/x.txt"}
	got, err := e.s.ReadFilesAt(ctx, e.repo, e.c3, paths, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(paths) {
		t.Fatalf("%d results for %d paths", len(got), len(paths))
	}
	for i, f := range got {
		if f.Path != paths[i] {
			t.Fatalf("result %d is %q, want %q", i, f.Path, paths[i])
		}
	}
	check := func(i int, kind encoding.TreeValueKind, size uint64, data string) {
		t.Helper()
		f := got[i]
		if f.Value == nil || f.Value.Kind != kind || (kind == encoding.TreeValueFile && f.Value.Size != size) || string(f.Data) != data {
			t.Fatalf("%q: %+v, data %q", f.Path, f.Value, f.Data)
		}
	}
	check(0, encoding.TreeValueFile, 13, "1\ntwo")
	for _, i := range []int{1, 2, 6} {
		if got[i].Value != nil || got[i].Data != nil {
			t.Fatalf("%q: %+v, want nothing", got[i].Path, got[i].Value)
		}
	}
	check(3, encoding.TreeValueTree, 0, "")
	if got[4].Value == nil || got[4].Value.Kind != encoding.TreeValueSymlink || got[4].Value.SymlinkTarget != "a/x.txt" {
		t.Fatalf("link: %+v", got[4].Value)
	}
	check(5, encoding.TreeValueFile, uint64(len(bText)), "alpha")
	check(7, encoding.TreeValueTree, 0, "")
	check(8, encoding.TreeValueFile, 18, "1\ntwo")
	check(9, encoding.TreeValueFile, 13, "1\ntwo")

	// A limit of 0 reads values alone; a large one, whole files.
	got, err = e.s.ReadFilesAt(ctx, e.repo, e.c3, []string{"a/x.txt", "c.txt"}, 0)
	if err != nil || got[0].Value == nil || got[0].Data != nil || got[1].Data != nil {
		t.Fatalf("limit 0: %+v, %v", got, err)
	}
	got, err = e.s.ReadFilesAt(ctx, e.repo, e.c3, []string{"c.txt"}, MaxReadFilesAtLimit)
	if err != nil || string(got[0].Data) != bText {
		t.Fatalf("whole: %q, %v", got[0].Data, err)
	}
	// Refusals: a path that is not clean, a limit over the bound, more than the total bound.
	for name, call := range map[string]func() error{
		"unclean path": func() error { _, err := e.s.ReadFilesAt(ctx, e.repo, e.c3, []string{"a/../b"}, 1); return err },
		"limit": func() error {
			_, err := e.s.ReadFilesAt(ctx, e.repo, e.c3, []string{"a"}, MaxReadFilesAtLimit+1)
			return err
		},
		"total": func() error {
			_, err := e.s.ReadFilesAt(ctx, e.repo, e.c3, make([]string, MaxReadFilesAtBytes/MaxReadFilesAtLimit+1), MaxReadFilesAtLimit)
			return err
		},
	} {
		if err := call(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v, want invalid", name, err)
		}
	}
	// A commit the repository does not hold.
	if _, err := e.s.ReadFilesAt(ctx, e.repo, encoding.ID{9}, []string{"a"}, 1); err == nil {
		t.Fatal("an unknown commit read")
	}
}
