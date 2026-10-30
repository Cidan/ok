package queue

import (
	gopath "path"

	"github.com/Cidan/ok/internal/workflow"
)

// footprintIndex finds, among the footprints it holds, those that may conflict with another —
// PathConflict or Conflict — a superset of them, which those checks then decide: an inverted
// index of the footprints' paths, packages, and targets, so that a footprint is checked against
// those it shares something with, not every one the queue holds (docs/automation.md, "The
// landing queue"). Each footprint is held under a key its caller names it by: a submission's
// place in the queue, a commit's place on main.
type footprintIndex struct {
	// pathAt holds footprints by each file and subtree they touch, and pathUnder by every
	// directory above one too, to the repository's root (""); packageAt and packageUnder hold
	// them so by the package directories of the targets they reach.
	pathAt, pathUnder       map[string][]int
	packageAt, packageUnder map[string][]int
	affected, reach         map[string][]int
	// every are the footprints that may conflict with any other: they touch the repository's
	// settings, or affect or reach every target. all is every footprint held.
	every, all []int
}

func newFootprintIndex() *footprintIndex {
	return &footprintIndex{pathAt: map[string][]int{}, pathUnder: map[string][]int{}, packageAt: map[string][]int{},
		packageUnder: map[string][]int{}, affected: map[string][]int{}, reach: map[string][]int{}}
}

// everywhere reports whether a footprint may conflict with any other (footprintIndex.every).
func (f *Footprint) everywhere() bool {
	return f.all || f.reachAll || f.touches(workflow.SettingsFile)
}

// lineage calls fn with p and every directory above it, to the repository's root (""): by the
// path's slashes, which within reads, and by path.Dir, which ancestorIn and under read.
func lineage(p string, fn func(string)) {
	fn(p)
	for i := len(p) - 1; i > 0; i-- {
		if p[i] == '/' {
			fn(p[:i])
		}
	}
	for d := gopath.Dir(p); d != "." && d != "/" && d != p; d = gopath.Dir(d) {
		fn(d)
	}
	fn("")
}

// add holds f under key.
func (x *footprintIndex) add(key int, f *Footprint) {
	x.all = append(x.all, key)
	if f.everywhere() {
		x.every = append(x.every, key)
	}
	holdPaths := func(at, under map[string][]int, paths ...map[string]bool) {
		seenAt, seenUnder := map[string]bool{}, map[string]bool{}
		for _, ps := range paths {
			for p := range ps {
				if !seenAt[p] {
					seenAt[p] = true
					at[p] = append(at[p], key)
				}
				lineage(p, func(a string) {
					if !seenUnder[a] {
						seenUnder[a] = true
						under[a] = append(under[a], key)
					}
				})
			}
		}
	}
	holdPaths(x.pathAt, x.pathUnder, f.files, f.subtrees)
	holdPaths(x.packageAt, x.packageUnder, f.packages)
	for t := range f.affected {
		x.affected[t] = append(x.affected[t], key)
	}
	for t := range f.reach {
		x.reach[t] = append(x.reach[t], key)
	}
}

// candidates calls visit with the key of every footprint held that may conflict with f — some
// more than once — so that any that PathConflict or Conflict says conflicts, in either order,
// is among them. A footprint g shares with f, for each way the checks find a conflict:
//   - a path one touches, at, above, or under one the other touches (PathConflict; and the files
//     and subtrees touchesReach finds in packages, either way round);
//   - a target one affects that the other affects or reaches (affects, either way round);
//   - or one of them may conflict with any other (everywhere).
func (x *footprintIndex) candidates(f *Footprint, visit func(int)) {
	each := func(keys []int) {
		for _, k := range keys {
			visit(k)
		}
	}
	if f.everywhere() {
		each(x.all)
		return
	}
	each(x.every)
	// Held at or under p, or at a directory above it.
	related := func(at, under map[string][]int, p string) {
		each(under[p])
		lineage(p, func(a string) { each(at[a]) })
	}
	for _, ps := range []map[string]bool{f.files, f.subtrees} {
		for p := range ps {
			related(x.pathAt, x.pathUnder, p)       // their paths and f's
			related(x.packageAt, x.packageUnder, p) // f's paths in packages they reach
		}
	}
	for pkg := range f.packages {
		related(x.pathAt, x.pathUnder, pkg) // their paths in packages f reaches
	}
	for t := range f.reach {
		each(x.affected[t])
	}
	for t := range f.affected {
		each(x.reach[t])
	}
}
