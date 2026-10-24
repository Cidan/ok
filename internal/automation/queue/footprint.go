package queue

import (
	gopath "path"
	"strings"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/workflow"
)

// Footprint is what a change can alter and what that rests on: the targets it affects, the
// targets they build on (upstream), and the paths it touches — the Bazel graph's answer for
// the change on the commit under test. A large change's is kept coarse (store.BoundPaths,
// store.BoundTargets): subtrees it may touch anything under, and every target.
type Footprint struct {
	affected map[string]bool
	reach    map[string]bool // affected and upstream
	packages map[string]bool // the package directories of reach
	files    map[string]bool
	subtrees map[string]bool // directories it may touch anything under ("": everything)
	all      bool            // it may affect every target
	reachAll bool            // it reaches every target
}

func newFootprint() *Footprint {
	return &Footprint{affected: map[string]bool{}, reach: map[string]bool{}, packages: map[string]bool{}, files: map[string]bool{},
		subtrees: map[string]bool{}}
}

// NewFootprint builds a change's footprint from its affected targets, their upstream, and the
// paths it touches, as its post plan found them.
func NewFootprint(affected, upstream, files []string) *Footprint {
	f := newFootprint()
	for _, l := range affected {
		if l == store.AllTargets {
			f.all, f.reachAll = true, true
		}
		f.affected[l] = true
	}
	for _, l := range append(append([]string(nil), affected...), upstream...) {
		if l == store.AllTargets {
			f.reachAll = true
		}
		f.reach[l] = true
		f.packages[packageOf(l)] = true
	}
	for _, p := range files {
		if strings.HasSuffix(p, "/") {
			f.subtrees[strings.TrimSuffix(strings.TrimSuffix(p, "/"), "/")] = true
			continue
		}
		f.files[p] = true
	}
	return f
}

// Merge is the footprint of several changes landing together (a stack).
func Merge(fps ...*Footprint) *Footprint {
	out := newFootprint()
	for _, f := range fps {
		for _, m := range []struct{ from, to map[string]bool }{
			{f.affected, out.affected}, {f.reach, out.reach}, {f.packages, out.packages}, {f.files, out.files}, {f.subtrees, out.subtrees},
		} {
			for k := range m.from {
				m.to[k] = true
			}
		}
		out.all, out.reachAll = out.all || f.all, out.reachAll || f.reachAll
	}
	return out
}

// Conflict reports whether one change's results can depend on another's having landed
// (docs/automation.md, "The landing queue"): a target either affects is one the other
// affects or builds on — so an edge either adds, removes, or changes shows up on both sides —
// or either touches a file in a package the other reaches, which covers a package one of them
// deletes, whose targets no longer exist to be affected — or either touches the repository's
// settings, which decide how every file is checked, those outside every package too, and whether
// any is (an imported repository's). Two changes that do not conflict keep their results whatever order
// they land in.
func Conflict(a, b *Footprint) bool {
	return a.touches(workflow.SettingsFile) || b.touches(workflow.SettingsFile) ||
		affects(a, b) || affects(b, a) || touchesReach(a, b) || touchesReach(b, a)
}

// affects reports whether a target a affects is one b affects or builds on.
func affects(a, b *Footprint) bool {
	switch {
	case a.all:
		return len(b.reach) > 0
	case b.reachAll:
		return len(a.affected) > 0
	}
	return meets(a.affected, b.reach)
}

// touchesReach reports whether a touches a file in a package b reaches.
func touchesReach(a, b *Footprint) bool {
	if b.reachAll {
		return len(a.files) > 0 || len(a.subtrees) > 0
	}
	if under(a.files, b.packages) {
		return true
	}
	for s := range a.subtrees {
		for pkg := range b.packages {
			if within(pkg, s) || within(s, pkg) {
				return true
			}
		}
	}
	return false
}

// touches reports whether a change may touch a path: it does, or a subtree it touches holds it.
func (f *Footprint) touches(p string) bool {
	if f.files[p] {
		return true
	}
	for s := range f.subtrees {
		if within(p, s) {
			return true
		}
	}
	return false
}

// PathConflict reports whether two changes touch a path in common, or one touches a path under
// another's (a file where the other adds a directory, or the reverse): the SCM lands at most
// one of them. A subtree conflicts with anything under it, and with a file above it.
func PathConflict(a, b *Footprint) bool {
	for p := range a.files {
		if b.files[p] || ancestorIn(p, b.files) || b.covers(p) {
			return true
		}
	}
	for p := range b.files {
		if ancestorIn(p, a.files) || a.covers(p) {
			return true
		}
	}
	for s := range a.subtrees {
		if b.covers(s) || b.holdsUnder(s) || (s != "" && ancestorIn(s, b.files)) {
			return true
		}
	}
	for s := range b.subtrees {
		if a.holdsUnder(s) || (s != "" && ancestorIn(s, a.files)) {
			return true
		}
	}
	return false
}

// covers reports whether a subtree of f holds p.
func (f *Footprint) covers(p string) bool {
	for s := range f.subtrees {
		if within(p, s) {
			return true
		}
	}
	return false
}

// holdsUnder reports whether f touches a path or subtree within s.
func (f *Footprint) holdsUnder(s string) bool {
	for p := range f.files {
		if within(p, s) {
			return true
		}
	}
	for t := range f.subtrees {
		if within(t, s) {
			return true
		}
	}
	return false
}

// within reports whether p is dir or under it ("": everything is).
func within(p, dir string) bool {
	return dir == "" || p == dir || strings.HasPrefix(p, dir+"/")
}

// meets reports whether two sets share a member.
func meets(a, b map[string]bool) bool {
	if len(a) > len(b) {
		a, b = b, a
	}
	for k := range a {
		if b[k] {
			return true
		}
	}
	return false
}

// under reports whether a path of files lies in one of packages' directories.
func under(files, packages map[string]bool) bool {
	if len(packages) == 0 {
		return false
	}
	for p := range files {
		for d := gopath.Dir(p); ; d = gopath.Dir(d) {
			if d == "." {
				d = ""
			}
			if packages[d] {
				return true
			}
			if d == "" {
				break
			}
		}
	}
	return false
}

// ancestorIn reports whether a proper ancestor directory of p is in set.
func ancestorIn(p string, set map[string]bool) bool {
	for d := gopath.Dir(p); d != "." && d != "/"; d = gopath.Dir(d) {
		if set[d] {
			return true
		}
	}
	return false
}

// packageOf is a label's package directory.
func packageOf(label string) string {
	pkg, _, _ := strings.Cut(strings.TrimPrefix(label, "//"), ":")
	return pkg
}
