package api

import (
	"fmt"
	"slices"
	"testing"

	"github.com/Cidan/ok/internal/wait"
)

// A watch registers on its CL and the unlanded CLs of its stack below it; while the CL is active,
// also on what governs every CL, each path it changes, and each directory it adds a file to — or on
// every move of main, for a CL that changes more than maxWatchedPaths.
func TestWatched(t *testing.T) {
	cl := func(n int64) string { return wait.CLSubject(7, n) }
	path := func(p string) string { return wait.PathSubject(7, p) }
	many := make([]string, maxWatchedPaths+1)
	for i := range many {
		many[i] = fmt.Sprintf("d/f%d", i)
	}
	for _, tc := range []struct {
		name string
		bc   blockedCache
		want []string
	}{
		{"landed", blockedCache{repo: 7, files: []string{"a/x"}}, []string{cl(3)}},
		{"a stack's", blockedCache{repo: 7, ancestors: []int64{2, 1}}, []string{cl(3), cl(2), cl(1)}},
		{"active", blockedCache{repo: 7, active: true, files: []string{"a/b/x", "README", "c/new"}, added: []string{"c/new", "TOP"}},
			[]string{cl(3), wait.GoverningSubject(7), path("a/b/x"), path("README"), path("c/new"), path("c"), path("")}},
		{"too large", blockedCache{repo: 7, active: true, files: many}, []string{cl(3), wait.HeadSubject(7)}},
	} {
		got := watched(&tc.bc, 3)
		slices.Sort(got)
		want := slices.Clone(tc.want)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s: %v, want %v", tc.name, got, want)
		}
	}
}

// A CL that has not landed registers on nothing with the deployer. Landed, it registers on its
// commit and the repository's plans — a plan with nothing to deploy for it deploys it, and makes no
// deployment that would say so — and on each deployment covering it, a newer commit's when it
// landed below a plan's head.
func TestDeploySubjects(t *testing.T) {
	if got := DeploySubjects(7, nil, nil); len(got) != 0 {
		t.Errorf("an unlanded CL registers on %v", got)
	}
	landed := []byte{0xab, 0xcd}
	want := []string{wait.CommitSubject(7, landed), wait.PlanSubject(7)}
	if got := DeploySubjects(7, landed, nil); !slices.Equal(got, want) {
		t.Errorf("landed, unplanned: %v, want %v", got, want)
	}
	want = append(want, wait.DeploymentSubject(3), wait.DeploymentSubject(4))
	if got := DeploySubjects(7, landed, []int64{3, 4}); !slices.Equal(got, want) {
		t.Errorf("covered by two deployments: %v, want %v", got, want)
	}
}
