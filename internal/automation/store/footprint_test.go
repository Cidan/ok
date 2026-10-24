package store

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// A footprint within the budget is kept as it is; past it, its paths become their directories, a
// level at a time, until they fit — every path still under one of them — and its targets every
// target.
func TestBoundFootprints(t *testing.T) {
	small := []string{"a/b/c.go", "a/d.go", "README.md"}
	if got := BoundPaths(small); !slices.Equal(got, small) {
		t.Errorf("a small footprint: %v", got)
	}
	var big []string
	for i := range 20_000 {
		big = append(big, fmt.Sprintf("vendor/mod%03d/pkg%02d/file%05d.go", i%300, i%40, i))
	}
	big = append(big, "README.md")
	got := BoundPaths(big)
	if size(got) > FootprintBudget || len(got) == 0 {
		t.Fatalf("bounded to %d bytes (%d entries)", size(got), len(got))
	}
	for _, p := range big {
		covered := false
		for _, g := range got {
			if g == p || g == "/" || (strings.HasSuffix(g, "/") && strings.HasPrefix(p, g)) {
				covered = true
				break
			}
		}
		if !covered {
			t.Fatalf("%s is under none of %v", p, got[:min(5, len(got))])
		}
	}
	if !slices.Contains(got, "README.md") {
		t.Errorf("a shallow path was coarsened with the deep ones: %v", got[:min(5, len(got))])
	}
	var flat []string
	for i := range 10_000 {
		flat = append(flat, fmt.Sprintf("file%05d-with-a-long-name.go", i))
	}
	if got := BoundPaths(flat); !slices.Equal(got, []string{"/"}) {
		t.Errorf("a footprint of the root's files: %v", got[:min(5, len(got))])
	}
	var labels []string
	for i := range 5_000 {
		labels = append(labels, fmt.Sprintf("//pkg%04d:target", i))
	}
	if got := BoundTargets(labels); !slices.Equal(got, []string{AllTargets}) {
		t.Errorf("many targets: %v", got[:min(5, len(got))])
	}
	if got := BoundTargets(labels[:10]); !slices.Equal(got, labels[:10]) {
		t.Errorf("a few targets: %v", got)
	}
}
