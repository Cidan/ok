package feed

import (
	"fmt"
	"slices"
	"testing"

	"github.com/Cidan/ok/internal/wait"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// A move of main changes the paths it changed and the directories they are directly in — a
// rename's both — or, when it changed what governs every CL or more than maxMovedPaths, what
// governs them all.
func TestChangedSubjects(t *testing.T) {
	ch := func(p string) *scmpb.PathChange { return &scmpb.PathChange{Path: p} }
	path := func(p string) string { return wait.PathSubject(7, p) }
	governing := []string{wait.GoverningSubject(7)}
	many := make([]*scmpb.PathChange, maxMovedPaths)
	for i := range many {
		many[i] = ch(fmt.Sprintf("d/f%d", i))
	}
	for _, tc := range []struct {
		name    string
		changes []*scmpb.PathChange
		treeDir string
		want    []string
	}{
		{"files", []*scmpb.PathChange{ch("a/b/x.go"), ch("README")}, "", []string{path("a/b/x.go"), path("a/b"), path("README"), path("")}},
		{"a rename", []*scmpb.PathChange{{Path: "c/new", OldPath: "d/old", Kind: scmpb.PathChange_KIND_RENAMED}}, "", []string{path("c/new"), path("c"), path("d/old"), path("d")}},
		{"an OWNERS file", []*scmpb.PathChange{ch("a/x.go"), ch("a/OWNERS")}, "", governing},
		{"an OWNERS import", []*scmpb.PathChange{ch("team_OWNERS")}, "", governing},
		{"settings", []*scmpb.PathChange{ch(".ok/repository.yaml")}, "", governing},
		{"the groups-and-ACLs tree", []*scmpb.PathChange{ch("identity/groups/ops.yaml")}, "identity", governing},
		{"beside the tree", []*scmpb.PathChange{ch("identity-docs/x")}, "identity", []string{path("identity-docs/x"), path("identity-docs")}},
		{"too many", many, "", governing},
	} {
		got := changedSubjects(7, tc.changes, tc.treeDir)
		slices.Sort(got)
		want := slices.Clone(tc.want)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s: %v, want %v", tc.name, got, want)
		}
	}
}
