package queue

import (
	gopath "path"
	"strings"

	"github.com/Cidan/ok/internal/workflow"
)

// Footprint is what a change can alter and what that rests on: the targets it affects, the
// targets they build on (upstream), and the paths it touches — the Bazel graph's answer for
// the change on the commit under test.
type Footprint struct {
	affected map[string]bool
	reach    map[string]bool // affected and upstream
	packages map[string]bool // the package directories of reach
	files    map[string]bool
}

// NewFootprint builds a change's footprint from its affected targets, their upstream, and the
// paths it touches, as its post plan found them.
func NewFootprint(affected, upstream, files []string) *Footprint {
	f := &Footprint{affected: map[string]bool{}, reach: map[string]bool{}, packages: map[string]bool{}, files: map[string]bool{}}
	for _, l := range affected {
		f.affected[l] = true
	}
	for _, l := range append(append([]string(nil), affected...), upstream...) {
		f.reach[l] = true
		f.packages[packageOf(l)] = true
	}
	for _, p := range files {
		f.files[p] = true
	}
	return f
}

// Merge is the footprint of several changes landing together (a stack).
func Merge(fps ...*Footprint) *Footprint {
	out := &Footprint{affected: map[string]bool{}, reach: map[string]bool{}, packages: map[string]bool{}, files: map[string]bool{}}
	for _, f := range fps {
		for _, m := range []struct{ from, to map[string]bool }{
			{f.affected, out.affected}, {f.reach, out.reach}, {f.packages, out.packages}, {f.files, out.files},
		} {
			for k := range m.from {
				m.to[k] = true
			}
		}
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
	return a.files[workflow.SettingsFile] || b.files[workflow.SettingsFile] ||
		meets(a.affected, b.reach) || meets(b.affected, a.reach) || under(a.files, b.packages) || under(b.files, a.packages)
}

// PathConflict reports whether two changes touch a path in common, or one touches a path under
// another's (a file where the other adds a directory, or the reverse): the SCM lands at most
// one of them.
func PathConflict(a, b *Footprint) bool {
	for p := range a.files {
		if b.files[p] || ancestorIn(p, b.files) {
			return true
		}
	}
	for p := range b.files {
		if ancestorIn(p, a.files) {
			return true
		}
	}
	return false
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
