package review

import (
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/Cidan/ok/internal/control/db"
)

func TestMapLine(t *testing.T) {
	// A 20-line file edited four times: line 2 modified; 3 lines inserted after line 5; lines 9-10
	// removed; line 15 replaced by two lines.
	multi := []Hunk{{2, 1, 2, 1}, {5, 0, 6, 3}, {9, 2, 11, 0}, {15, 1, 16, 2}}

	tests := []struct {
		name      string
		hunks     []Hunk
		line      int64
		wantLine  int64
		wantExact bool
	}{
		{name: "identical file", hunks: nil, line: 7, wantLine: 7, wantExact: true},
		{name: "unchanged region above the only hunk", hunks: []Hunk{{10, 2, 10, 3}}, line: 5, wantLine: 5, wantExact: true},
		{name: "unchanged region below a growing hunk shifts down", hunks: []Hunk{{10, 2, 10, 3}}, line: 20, wantLine: 21, wantExact: true},
		{name: "the line right after a hunk", hunks: []Hunk{{10, 2, 10, 3}}, line: 12, wantLine: 13, wantExact: true},
		{name: "insertion above shifts down", hunks: []Hunk{{3, 0, 4, 2}}, line: 4, wantLine: 6, wantExact: true},
		{name: "insertion just below leaves the line", hunks: []Hunk{{3, 0, 4, 2}}, line: 3, wantLine: 3, wantExact: true},
		{name: "deletion above shifts up", hunks: []Hunk{{3, 2, 2, 0}}, line: 5, wantLine: 3, wantExact: true},
		{name: "modified line maps into its replacement", hunks: []Hunk{{10, 3, 10, 3}}, line: 11, wantLine: 11},
		{name: "shorter replacement clamps to its last line", hunks: []Hunk{{10, 3, 20, 1}}, line: 12, wantLine: 20},
		{name: "longer replacement keeps the offset", hunks: []Hunk{{10, 2, 10, 5}}, line: 11, wantLine: 11},
		{name: "removed line lands just above the removal", hunks: []Hunk{{5, 2, 4, 0}}, line: 6, wantLine: 4},
		{name: "removal at the top lands on the first line", hunks: []Hunk{{1, 2, 0, 0}}, line: 1, wantLine: 1},
		{name: "first line after an insertion at the top", hunks: []Hunk{{0, 0, 1, 2}}, line: 1, wantLine: 3, wantExact: true},
		{name: "first line modified", hunks: []Hunk{{1, 1, 1, 1}}, line: 1, wantLine: 1},
		{name: "last line removed", hunks: []Hunk{{10, 1, 9, 0}}, line: 10, wantLine: 9},
		{name: "last line with lines appended after it", hunks: []Hunk{{10, 0, 11, 2}}, line: 10, wantLine: 10, wantExact: true},
		{name: "every line of an emptied file", hunks: []Hunk{{1, 3, 0, 0}}, line: 2, wantLine: 1},
		{name: "multiple hunks: above them all", hunks: multi, line: 1, wantLine: 1, wantExact: true},
		{name: "multiple hunks: inside the first", hunks: multi, line: 2, wantLine: 2},
		{name: "multiple hunks: between the first two", hunks: multi, line: 4, wantLine: 4, wantExact: true},
		{name: "multiple hunks: just above an insertion", hunks: multi, line: 5, wantLine: 5, wantExact: true},
		{name: "multiple hunks: below the insertion", hunks: multi, line: 6, wantLine: 9, wantExact: true},
		{name: "multiple hunks: inside the removal", hunks: multi, line: 10, wantLine: 11},
		{name: "multiple hunks: below the removal", hunks: multi, line: 12, wantLine: 13, wantExact: true},
		{name: "multiple hunks: just above the last", hunks: multi, line: 14, wantLine: 15, wantExact: true},
		{name: "multiple hunks: inside the last", hunks: multi, line: 15, wantLine: 16},
		{name: "multiple hunks: below them all", hunks: multi, line: 20, wantLine: 22, wantExact: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, exact := mapLine(tt.hunks, tt.line)
			if got != tt.wantLine || exact != tt.wantExact {
				t.Fatalf("mapLine(%v, %d) = (%d, %v), want (%d, %v)", tt.hunks, tt.line, got, exact, tt.wantLine, tt.wantExact)
			}
		})
	}
}

func TestPortPosition(t *testing.T) {
	changes := map[string]FileChange{
		"edit.go":      {OldPath: "edit.go", NewPath: "edit.go", Hunks: []Hunk{{3, 0, 4, 2}}},
		"added.go":     {OldPath: "added.go", NewPath: "added.go", Hunks: []Hunk{{10, 0, 11, 5}}},
		"old.go":       {OldPath: "old.go", NewPath: "new.go", Hunks: []Hunk{{1, 1, 1, 1}}},
		"moved.go":     {OldPath: "moved.go", NewPath: "dir/moved.go"},
		"gone.go":      {OldPath: "gone.go"},
		"img.png":      {OldPath: "img.png", NewPath: "img.png", Binary: true},
		"emptied.go":   {OldPath: "emptied.go", NewPath: "emptied.go", Hunks: []Hunk{{1, 4, 0, 0}}},
		"filled.go":    {OldPath: "filled.go", NewPath: "filled.go", Hunks: []Hunk{{0, 0, 1, 3}}},
		"chmod.sh":     {OldPath: "chmod.sh", NewPath: "chmod.sh"},
		"retyped.link": {OldPath: "retyped.link", NewPath: "retyped.link", Hunks: []Hunk{{0, 0, 1, 1}, {1, 1, 0, 0}}},
	}
	tests := []struct {
		name string
		path string
		line int64
		want PortedAnchor
	}{
		{name: "unchanged file keeps its position", path: "same.go", line: 12, want: PortedAnchor{"same.go", 12, PortExact}},
		{name: "line shifted by an insertion", path: "edit.go", line: 5, want: PortedAnchor{"edit.go", 7, PortExact}},
		{name: "line of a file the change adds, edited between patchsets", path: "added.go", line: 12, want: PortedAnchor{"added.go", 17, PortExact}},
		{name: "renamed file, modified line", path: "old.go", line: 1, want: PortedAnchor{"new.go", 1, PortLineChanged}},
		{name: "renamed file, unchanged line", path: "old.go", line: 4, want: PortedAnchor{"new.go", 4, PortExact}},
		{name: "pure rename keeps the line", path: "moved.go", line: 9, want: PortedAnchor{"dir/moved.go", 9, PortExact}},
		{name: "mode-only change keeps the line", path: "chmod.sh", line: 2, want: PortedAnchor{"chmod.sh", 2, PortExact}},
		{name: "deleted file", path: "gone.go", line: 3, want: PortedAnchor{"gone.go", 0, PortFileDeleted}},
		{name: "emptied file", path: "emptied.go", line: 3, want: PortedAnchor{"emptied.go", 1, PortLineChanged}},
		{name: "file-level thread follows a rename", path: "old.go", line: 0, want: PortedAnchor{"new.go", 0, PortExact}},
		{name: "file-level thread on a previously empty file", path: "filled.go", line: 0, want: PortedAnchor{"filled.go", 0, PortExact}},
		{name: "file-level thread on a deleted file", path: "gone.go", line: 0, want: PortedAnchor{"gone.go", 0, PortFileDeleted}},
		{name: "binary change loses the line", path: "img.png", line: 4, want: PortedAnchor{"img.png", 0, PortUnknown}},
		{name: "file-level thread on a binary change", path: "img.png", line: 0, want: PortedAnchor{"img.png", 0, PortExact}},
		{name: "type change replaces the content", path: "retyped.link", line: 1, want: PortedAnchor{"retyped.link", 1, PortLineChanged}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := portPosition(changes, tt.path, tt.line); got != tt.want {
				t.Fatalf("portPosition(%q, %d) = %+v, want %+v", tt.path, tt.line, got, tt.want)
			}
		})
	}
}

func TestContextWindows(t *testing.T) {
	right := func(n int64) LineAnchor { return LineAnchor{Side: db.SidePatchset, Line: n} }
	left := func(n int64) LineAnchor { return LineAnchor{Side: db.SideBase, Line: n} }
	// Two hunks: base 5-12 ↔ revision 5-14, and base 40-47 ↔ revision 42-49. Between them base lines
	// 13-39 are revision lines 15-41 (a shift of 2).
	spans := []DiffSpan{{5, 12, 5, 14}, {40, 47, 42, 49}}

	tests := []struct {
		name    string
		spans   []DiffSpan
		anchors []LineAnchor
		margin  int64
		want    []ContextWindow
	}{
		{name: "unchanged file", anchors: []LineAnchor{right(10)}, margin: 3, want: []ContextWindow{{0, 7, 13, 0}}},
		{name: "clipped at the top of the file", anchors: []LineAnchor{right(2)}, margin: 3, want: []ContextWindow{{0, 1, 5, 0}}},
		{name: "anchors the hunks already show", spans: spans, anchors: []LineAnchor{right(10), left(12), left(40), right(49)}, margin: 3},
		{name: "revision line between hunks", spans: spans, anchors: []LineAnchor{right(20)}, margin: 3, want: []ContextWindow{{1, 17, 23, 2}}},
		{name: "base line maps through the gap's shift", spans: spans, anchors: []LineAnchor{left(20)}, margin: 3, want: []ContextWindow{{1, 19, 25, 2}}},
		{name: "overlapping windows merge", spans: spans, anchors: []LineAnchor{right(20), left(20)}, margin: 3, want: []ContextWindow{{1, 17, 25, 2}}},
		{name: "touching windows merge", spans: spans, anchors: []LineAnchor{right(27), right(20)}, margin: 3, want: []ContextWindow{{1, 17, 30, 2}}},
		{name: "distant windows stay apart", spans: spans, anchors: []LineAnchor{right(20), right(30)}, margin: 2, want: []ContextWindow{{1, 18, 22, 2}, {1, 28, 32, 2}}},
		{name: "clipped to the end of the previous hunk", spans: spans, anchors: []LineAnchor{right(16)}, margin: 3, want: []ContextWindow{{1, 15, 19, 2}}},
		{name: "clipped to the start of the next hunk", spans: spans, anchors: []LineAnchor{right(40)}, margin: 3, want: []ContextWindow{{1, 37, 41, 2}}},
		{name: "before the first hunk", spans: spans, anchors: []LineAnchor{right(2)}, margin: 3, want: []ContextWindow{{0, 1, 4, 0}}},
		{name: "after the last hunk", spans: spans, anchors: []LineAnchor{left(58)}, margin: 3, want: []ContextWindow{{2, 57, 63, 2}}},
		{name: "sorted across gaps", spans: spans, anchors: []LineAnchor{right(60), right(2)}, margin: 3, want: []ContextWindow{{0, 1, 4, 0}, {2, 57, 63, 2}}},
		{name: "after a hunk with no base lines", spans: []DiffSpan{{0, 0, 1, 3}}, anchors: []LineAnchor{left(5)}, margin: 3, want: []ContextWindow{{1, 5, 11, 3}}},
		{name: "after a hunk with no revision lines", spans: []DiffSpan{{1, 2, 0, 0}}, anchors: []LineAnchor{right(3)}, margin: 3, want: []ContextWindow{{1, 1, 6, -2}}},
		{name: "no margin", spans: spans, anchors: []LineAnchor{right(20)}, margin: 0, want: []ContextWindow{{1, 20, 20, 2}}},
		{name: "line-less anchor", spans: spans, anchors: []LineAnchor{right(0)}, margin: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ContextWindows(tt.spans, tt.anchors, tt.margin)
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ContextWindows = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// fakeDiffer serves canned tree diffs and records every call.
type fakeDiffer struct {
	changes  map[[2]string][]FileChange
	patches  map[[2]string]map[string]patchFile
	fail     map[[2]string]bool
	listed   map[[2]string]int
	patched  map[[2]string][]string
	patchErr map[[2]string]bool
}

func (f *fakeDiffer) nameStatus(from, to string) ([]FileChange, error) {
	k := [2]string{from, to}
	f.listed[k]++
	if f.fail[k] {
		return nil, errors.New("git diff failed")
	}
	return f.changes[k], nil
}

func (f *fakeDiffer) patch(from, to string, pathspec []string) (map[string]patchFile, error) {
	k := [2]string{from, to}
	f.patched[k] = append(f.patched[k], pathspec...)
	if f.patchErr[k] {
		return nil, errors.New("git diff failed")
	}
	return f.patches[k], nil
}

func TestPortThreads(t *testing.T) {
	// Patchsets 1 and 2 sit on base b1; patchset 3 was rebased onto b2; patchset 4 is newer than the
	// viewed patchset 3.
	revs := []revision{
		{Patchset: 1, Commit: "c1", Base: "b1"},
		{Patchset: 2, Commit: "c2", Base: "b1"},
		{Patchset: 3, Commit: "c3", Base: "b2"},
		{Patchset: 4, Commit: "c4", Base: "b2"},
	}
	right, left := db.SidePatchset, db.SideBase
	thread := func(root int64, ps int32, side db.Side, path string, line int32, resolved bool) Thread {
		return Thread{RootID: root, Patchset: ps, Side: side, FilePath: path, Line: line, Resolved: resolved}
	}
	threads := []Thread{
		thread(1, 1, right, "edit.go", 5, false),
		thread(2, 2, right, "old.go", 1, false),
		thread(3, 1, right, "edit.go", 5, true),
		thread(4, 3, right, "edit.go", 5, false),
		thread(5, 4, right, "edit.go", 5, false),
		thread(6, 1, left, "renamed.go", 3, false),
		thread(7, 2, left, "stay.go", 8, false),
		thread(8, 1, right, "gone.go", 2, false),
		thread(9, 2, right, "old.go", 0, false),
	}
	newDiffer := func() *fakeDiffer {
		return &fakeDiffer{
			changes: map[[2]string][]FileChange{
				{"b1", "c1"}: {{OldPath: "orig.go", NewPath: "renamed.go"}},
				{"c1", "c3"}: {{OldPath: "edit.go", NewPath: "edit.go"}, {OldPath: "gone.go"}, {NewPath: "fresh.go"}},
				{"c2", "c3"}: {{OldPath: "old.go", NewPath: "new.go"}, {OldPath: "unrelated.go", NewPath: "unrelated.go"}},
				{"b1", "b2"}: {{OldPath: "orig.go", NewPath: "orig.go"}},
			},
			patches: map[[2]string]map[string]patchFile{
				{"c1", "c3"}: {"edit.go": {hunks: []Hunk{{3, 0, 4, 2}}}},
				{"c2", "c3"}: {"old.go": {hunks: []Hunk{{1, 1, 1, 1}}}},
				{"b1", "b2"}: {"orig.go": {hunks: []Hunk{{1, 0, 2, 1}}}},
			},
			fail:     map[[2]string]bool{},
			patchErr: map[[2]string]bool{},
			listed:   map[[2]string]int{},
			patched:  map[[2]string][]string{},
		}
	}

	t.Run("ports unresolved threads from earlier patchsets", func(t *testing.T) {
		d := newDiffer()
		got := portThreads(d, revs, revs[2], threads)
		want := map[int64]PortedAnchor{
			1: {Path: "edit.go", Line: 7, State: PortExact},
			2: {Path: "new.go", Line: 1, State: PortLineChanged},
			6: {Path: "orig.go", Line: 4, State: PortExact},
			7: {Path: "stay.go", Line: 8, State: PortExact},
			8: {Path: "gone.go", State: PortFileDeleted},
			9: {Path: "new.go", State: PortExact},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("portThreads =\n%+v\nwant\n%+v", got, want)
		}
		for pair, n := range d.listed {
			if n != 1 {
				t.Errorf("listed %v %d times, want once", pair, n)
			}
		}
		wantListed := [][2]string{{"b1", "b2"}, {"b1", "c1"}, {"b1", "c2"}, {"c1", "c3"}, {"c2", "c3"}}
		var listed [][2]string
		for pair := range d.listed {
			listed = append(listed, pair)
		}
		sort.Slice(listed, func(i, j int) bool { return listed[i][0]+listed[i][1] < listed[j][0]+listed[j][1] })
		if !reflect.DeepEqual(listed, wantListed) {
			t.Fatalf("listed pairs = %v, want %v", listed, wantListed)
		}
		wantPatched := map[[2]string][]string{
			{"c1", "c3"}: {"edit.go"},
			{"c2", "c3"}: {"old.go", "new.go"},
			{"b1", "b2"}: {"orig.go"},
		}
		if !reflect.DeepEqual(d.patched, wantPatched) {
			t.Fatalf("patched = %v, want %v", d.patched, wantPatched)
		}
	})

	t.Run("an unchanged base maps base lines one to one", func(t *testing.T) {
		d := newDiffer()
		got := portThreads(d, revs, revs[1], []Thread{thread(6, 1, left, "renamed.go", 3, false)})
		want := map[int64]PortedAnchor{6: {Path: "orig.go", Line: 3, State: PortExact}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("portThreads = %+v, want %+v", got, want)
		}
		if len(d.patched) != 0 {
			t.Fatalf("patched %v, want no patch for identical bases", d.patched)
		}
	})

	t.Run("a failed diff degrades its threads, not the rest", func(t *testing.T) {
		d := newDiffer()
		d.fail[[2]string{"c2", "c3"}] = true
		d.patchErr[[2]string{"c1", "c3"}] = true
		d.fail[[2]string{"b1", "c2"}] = true
		got := portThreads(d, revs, revs[2], threads)
		want := map[int64]PortedAnchor{
			1: {Path: "edit.go", State: PortUnknown},
			2: {Path: "old.go", State: PortUnknown},
			6: {Path: "orig.go", Line: 4, State: PortExact},
			7: {Path: "stay.go", State: PortUnknown},
			8: {Path: "gone.go", State: PortUnknown},
			9: {Path: "old.go", State: PortUnknown},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("portThreads =\n%+v\nwant\n%+v", got, want)
		}
	})
}
