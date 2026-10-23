package owners

import (
	"cmp"
	"fmt"
	"path"
	"slices"
	"strings"
)

// Validate checks an OWNERS file as a change makes it — its content data, at path, read against
// the change's tree through src — and returns every problem, by line: what does not parse; imports
// that name no file, go round, or reach a file with problems; users knownUser does not know and
// ACLs knownACL does not; an OWNERS file that names no directory-level owner, which would leave
// files no per-file rule matches unapprovable (nothing above the nearest OWNERS file can fill the
// gap); and a `set noparent` rule whose globs — the same, in the same order — no per-file rule
// gives owners.
func Validate(p string, data []byte, src Source, knownUser, knownACL func(name string) bool) []Problem {
	f, problems := Parse(p, data)
	add := func(line int, format string, a ...any) {
		problems = append(problems, Problem{Path: p, Line: line, Msg: fmt.Sprintf(format, a...)})
	}
	check := func(o Owner, line int) {
		switch {
		case o.Kind == User && !knownUser(o.Name):
			add(line, "user: %s names no person", o.Name)
		case o.Kind == ACL && !knownACL(o.Name):
			if strings.Contains(o.Name, "/") {
				add(line, "acl: @%s names no team", o.Name)
			} else {
				add(line, "acl: %s names no ACL the groups-and-ACLs tree declares", o.Name)
			}
		}
	}
	for _, o := range f.Owners {
		check(o.Value, o.Line)
	}
	for _, rule := range f.Rules {
		for _, o := range rule.Owners {
			check(o, rule.Line)
		}
	}

	// Imports are followed with the file as the change makes it, so one that comes back to it is
	// seen going round. A failure is reported where it is: a target that does not exist, or a
	// cycle, at the importing line; a problem in a file an import reaches, in that file.
	r := NewResolver(overlay{src: src, path: p, data: data})
	for _, imp := range f.Imports {
		var err error
		if imp.Include {
			_, err = r.expand(imp.Path, []string{p})
		} else {
			_, err = r.dirOwners(imp.Path, []string{p})
		}
		if err != nil {
			problems = append(problems, at(err, p, imp.Line)...)
		}
	}
	for _, rule := range f.Rules {
		if rule.Import != nil {
			if _, err := r.dirOwners(rule.Import.Path, nil); err != nil {
				problems = append(problems, at(err, p, rule.Line)...)
			}
		}
	}
	if path.Base(p) == "OWNERS" && len(f.Owners) == 0 {
		if owners, err := r.dirOwners(p, nil); err == nil && len(owners) == 0 {
			add(0, "no directory-level owners: files no per-file rule matches could never be approved (add a user:, an acl:, *, or an import that gives owners)")
		}
	}
	for _, rule := range f.Rules {
		if !rule.NoParent {
			continue
		}
		// A rule with owners, or an import of them, gives these files owners.
		given := false
		for _, other := range f.Rules {
			given = given || slices.Equal(other.Globs, rule.Globs) && (len(other.Owners) > 0 || other.Import != nil)
		}
		if !given {
			add(rule.Line, "%s: set noparent, and no per-file rule gives these files owners, so no change to them could be approved", rule.Label())
		}
	}
	slices.SortStableFunc(problems, func(a, b Problem) int { return cmp.Compare(a.Line, b.Line) })
	return slices.Compact(problems)
}

// overlay is a source with one file as a change makes it.
type overlay struct {
	src  Source
	path string
	data []byte
}

func (o overlay) Read(p string) ([]byte, bool, error) {
	if p == o.path {
		return o.data, true, nil
	}
	return o.src.Read(p)
}

// globKey is a rule's globs, as written, as a key: rules naming the same globs are one set.
func globKey(globs []string) string { return strings.Join(globs, "\x00") }
