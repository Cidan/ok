package store

import (
	"path"
	"slices"
	"strings"
)

// A post plan's footprint — the paths its change touches, the targets it affects, and what they
// build on — is kept in its row, and at monorepo scale a change's can be any size: an import, a
// vendored tree, a change to a BUILD file every target reads. So what is kept is bounded, coarser
// past FootprintBudget, never missing anything (docs/automation.md, "The landing queue"): paths
// become their directories, a directory written with a trailing "/" for everything under it ("/"
// for everything); targets become AllTargets. A coarse footprint conflicts with more, and lands
// more alone, as a change that large does anyway.

// FootprintBudget bounds the bytes of a footprint's paths, and of its targets of each kind.
const FootprintBudget = 64 << 10

// AllTargets stands for every target: a footprint whose targets are past the budget.
const AllTargets = "//..."

// BoundPaths is paths within FootprintBudget: as they are, or coarsened a level at a time — each
// path deepest first replaced by its directory, as a subtree — until they fit.
func BoundPaths(paths []string) []string {
	if size(paths) <= FootprintBudget {
		return paths
	}
	out := slices.Clone(paths)
	for depth := maxDepth(out); size(out) > FootprintBudget; depth-- {
		if depth <= 0 {
			return []string{"/"}
		}
		seen := map[string]bool{}
		next := out[:0]
		for _, p := range out {
			if depthOf(p) >= depth {
				p = subtree(path.Dir(strings.TrimSuffix(p, "/")))
			}
			if !seen[p] {
				seen[p] = true
				next = append(next, p)
			}
		}
		out = next
	}
	slices.Sort(out)
	return out
}

// BoundTargets is labels within FootprintBudget: as they are, or AllTargets.
func BoundTargets(labels []string) []string {
	if size(labels) <= FootprintBudget {
		return labels
	}
	return []string{AllTargets}
}

// subtree is a directory's subtree: "dir/", or "/" for the root.
func subtree(dir string) string {
	if dir == "." || dir == "" {
		return "/"
	}
	return dir + "/"
}

// depthOf is how many components a path or subtree has.
func depthOf(p string) int {
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return 0
	}
	return strings.Count(p, "/") + 1
}

func maxDepth(paths []string) int {
	d := 0
	for _, p := range paths {
		d = max(d, depthOf(p))
	}
	return d
}

// size is the bytes of strings as a JSON array of them holds them, near enough.
func size(ss []string) int {
	n := 0
	for _, s := range ss {
		n += len(s) + 3
	}
	return n
}
