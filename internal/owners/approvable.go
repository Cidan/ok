package owners

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/Cidan/ok/internal/identity"
)

// People is what Approvable knows of an installation's people.
type People struct {
	// ACL returns the handles of the people an ACL holds, through the sets it holds (robots
	// approve nothing, so they are not among them), and whether the tree declares it.
	ACL func(name string) (handles []string, declared bool)
	// Estate is how many people the installation is known to have, at least: all a `*` owner
	// can be counted as.
	Estate int
}

// Approvable checks that every owner set the OWNERS files at paths can make could be approved
// by its own people. An LGTM must come from someone other than the change's author unless every
// set governing it allows self, so a set that one person owns alone, without set allowself,
// makes that person's own changes unapprovable, and a set no person owns makes every change so.
// Each file's directory-level set is checked, and each per-file set — the rules naming the same
// globs, in the same order, together, with the directory-level owners unless one of them is set
// noparent; those an included file brings are checked as the including file has them, and
// reported against it. A set is refused when it:
//
//   - names no person: no user, no ACL holding a person, and no *;
//   - has exactly one person, without set allowself;
//   - is * without set allowself where fewer than two people are known;
//   - names an ACL the tree does not declare.
//
// A file that cannot be read — it does not exist, does not parse, or an import fails — is a
// problem too.
func Approvable(src Source, paths []string, people People) []Problem {
	r := NewResolver(src)
	var problems []Problem
	for _, p := range paths {
		e, err := r.governed(p)
		if err != nil {
			problems = append(problems, at(err, p, 0)...)
			continue
		}
		problems = append(problems, approvable(p, 0, "directory-level owners", e.owners, e.allowSelf, people)...)
		var order []string
		groups := map[string][]resolvedRule{}
		for _, rule := range e.rules {
			k := globKey(rule.Globs)
			if _, ok := groups[k]; !ok {
				order = append(order, k)
			}
			groups[k] = append(groups[k], rule)
		}
		for _, k := range order {
			rules := groups[k]
			owners := map[Owner]bool{}
			noParent := false
			for _, rule := range rules {
				noParent = noParent || rule.NoParent
				for o := range rule.owners {
					owners[o] = true
				}
			}
			if !noParent {
				for o := range e.owners {
					owners[o] = true
				}
			}
			// A set of an included file's rules is p's, as p includes them: its problems are p's.
			line := 0
			if rules[0].file == p {
				line = rules[0].Line
			}
			problems = append(problems, approvable(p, line, rules[0].Label(), owners, e.allowSelf, people)...)
		}
	}
	slices.SortStableFunc(problems, func(a, b Problem) int {
		return cmp.Or(cmp.Compare(a.Path, b.Path), cmp.Compare(a.Line, b.Line), cmp.Compare(a.Msg, b.Msg))
	})
	return slices.Compact(problems)
}

// approvable checks one owner set.
func approvable(p string, line int, label string, owners map[Owner]bool, allowSelf bool, people People) []Problem {
	var problems []Problem
	add := func(format string, a ...any) {
		problems = append(problems, Problem{Path: p, Line: line, Msg: label + ": " + fmt.Sprintf(format, a...)})
	}
	persons := map[string]bool{}
	anyone := false
	for _, o := range sortedOwners(owners) {
		switch o.Kind {
		case User:
			persons[o.Name] = true
		case ACL:
			handles, declared := people.ACL(o.Name)
			if !declared {
				add("acl: %s names no ACL the groups-and-ACLs tree declares", o.Name)
				continue
			}
			for _, h := range handles {
				persons[h] = true
			}
		case Anyone:
			anyone = true
		}
	}
	names := slices.Sorted(func(yield func(string) bool) {
		for h := range persons {
			if !yield(h) {
				return
			}
		}
	})
	switch {
	case len(names) == 0 && !anyone:
		add("no person owns these files, so no change to them could be approved (name a person, an ACL with people in it, or *)")
	case allowSelf:
	case anyone && len(names) < 2 && people.Estate < 2:
		add("* without set allowself, and fewer than two people are known: a change by the only one could never be approved (set allowself)")
	case !anyone && len(names) == 1:
		add("%s alone owns these files, without set allowself, so %s's own changes to them could never be approved (add a second person, or set allowself)", names[0], names[0])
	}
	return problems
}

// HumanHandle is the handle of a person's name (<handle>.human.<domain>), and whether the name is
// a person's.
func HumanHandle(names identity.Names, name string) (string, bool) {
	if k, ok := names.KindOf(name); !ok || k != identity.Human {
		return "", false
	}
	return strings.TrimSuffix(name, ".human."+names.Domain()), true
}
