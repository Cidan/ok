// Package owners implements OWNERS files: who may approve changes to the files beneath a
// directory. The format is the one Chromium and Gerrit's find-owners made common — owner lines,
// per-file rules, set options, and imports — naming ok's people by handle and its ACLs by name.
// Owners are found nearest-only: the first OWNERS file above a changed file governs it alone.
//
// The package parses files (Parse), resolves which owner set governs each of a change's files
// (Resolver), validates an OWNERS file as an upload edits it (Validate), and checks that every set
// can be approved by its own people (Approvable).
package owners

import (
	"fmt"
	"path"
	"slices"
	"strings"
)

// Kind is what an owner line names.
type Kind int

const (
	// User is a person, by handle: ada is ada.human.<domain>.
	User Kind = iota + 1
	// ACL is every person in an ACL of the groups-and-ACLs tree, through the sets it holds:
	// eng is eng.acl.<domain>.
	ACL
	// Anyone is `*`: anyone may approve.
	Anyone
)

// Owner is one owner: a user, an ACL, or anyone.
type Owner struct {
	Kind Kind
	Name string // a handle or an ACL's name; "" for Anyone
}

func (o Owner) String() string {
	switch o.Kind {
	case User:
		return "user: " + o.Name
	case ACL:
		return "acl: " + o.Name
	}
	return "*"
}

// File is a parsed OWNERS file.
type File struct {
	Path string // repository-relative
	// Owners are its directory-level owner lines, in order, each once.
	Owners []Located[Owner]
	// AllowSelf is `set allowself`: an author's own LGTM and Approval count where they may grant
	// them.
	AllowSelf bool
	// Imports are its `file:` and `include` lines.
	Imports []Import
	// Rules are its per-file rules, in order.
	Rules []Rule
}

// Located is a value and the line it is on.
type Located[T any] struct {
	Value T
	Line  int
}

// Import is a `file:` line (the target's directory-level owners) or an `include` line
// (everything the target holds: its owners, per-file rules, and allowself).
type Import struct {
	Path    string // the target, repository-relative
	Include bool
	Line    int
}

// Rule is one per-file line: files matching any of its globs get its owners, its import's
// directory-level owners, or — with NoParent — only the owners per-file rules give them.
type Rule struct {
	Globs    []string
	Owners   []Owner
	Import   *Import
	NoParent bool
	Line     int
	matchers []matcher
}

// Label names the rule as people read it: "per-file *.go,docs/**".
func (r Rule) Label() string { return "per-file " + strings.Join(r.Globs, ",") }

// matches reports whether a path, relative to the governing OWNERS file's directory, is one of
// the rule's files.
func (r Rule) matches(rel string) bool {
	for _, m := range r.matchers {
		if m.match(rel) {
			return true
		}
	}
	return false
}

// Problem is something wrong with an OWNERS file, at a line (0 when it is the file's as a whole).
type Problem struct {
	Path string
	Line int
	Msg  string
}

func (p Problem) Error() string {
	if p.Line == 0 {
		return p.Path + ": " + p.Msg
	}
	return fmt.Sprintf("%s:%d: %s", p.Path, p.Line, p.Msg)
}

// Problems are several problems as one error: every line of a file that does not parse.
type Problems []Problem

func (ps Problems) Error() string {
	lines := make([]string, len(ps))
	for i, p := range ps {
		lines[i] = p.Error()
	}
	return strings.Join(lines, "\n")
}

// IsOwnersFile reports whether a path names an OWNERS file: OWNERS, which governs its directory,
// or <prefix>_OWNERS or OWNERS_<suffix>, which only imports name — the prefix or suffix ASCII
// letters, digits, _, and -.
func IsOwnersFile(p string) bool {
	base := path.Base(p)
	if base == "OWNERS" {
		return true
	}
	if pre, ok := strings.CutSuffix(base, "_OWNERS"); ok && namepart(pre) {
		return true
	}
	suf, ok := strings.CutPrefix(base, "OWNERS_")
	return ok && namepart(suf)
}

// namePart reports whether s can be an OWNERS file name's prefix or suffix.
func namepart(s string) bool {
	for _, r := range s {
		if !('a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || '0' <= r && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return s != ""
}

// Parse reads an OWNERS file at a repository-relative path. It returns what it understood and a
// problem for every line it did not.
func Parse(p string, data []byte) (*File, []Problem) {
	f := &File{Path: p}
	var problems []Problem
	bad := func(line int, format string, a ...any) {
		problems = append(problems, Problem{Path: p, Line: line, Msg: fmt.Sprintf(format, a...)})
	}
	seen := map[Owner]bool{}
	for i, raw := range strings.Split(string(data), "\n") {
		n := i + 1
		line, _, _ := strings.Cut(raw, "#")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if rest, ok := strings.CutPrefix(line, "per-file"); ok && rest != "" && (rest[0] == ' ' || rest[0] == '\t') {
			r, err := parseRule(p, strings.TrimSpace(rest), n)
			if err != "" {
				bad(n, "%s", err)
				continue
			}
			f.Rules = append(f.Rules, r)
			continue
		}
		if words := strings.Fields(line); words[0] == "set" {
			switch strings.Join(words[1:], " ") {
			case "noparent":
				// Owners are found nearest-only, so no file inherits its parents' anyway.
			case "allowself":
				f.AllowSelf = true
			default:
				bad(n, "unknown option %q (set noparent, or set allowself)", strings.Join(words[1:], " "))
			}
			continue
		}
		if imp, ok, err := parseImport(p, line, n); ok {
			if err != "" {
				bad(n, "%s", err)
				continue
			}
			f.Imports = append(f.Imports, imp)
			continue
		}
		o, err := parseOwner(line)
		if err != "" {
			bad(n, "%s", err)
			continue
		}
		if !seen[o] {
			seen[o] = true
			f.Owners = append(f.Owners, Located[Owner]{o, n})
		}
	}
	return f, problems
}

// parseImport reads a `file: <path>` or `include <path>` line, if line is one.
func parseImport(from, line string, n int) (Import, bool, string) {
	var target string
	include := false
	if rest, ok := strings.CutPrefix(line, "file:"); ok {
		target = strings.TrimSpace(rest)
	} else if rest, ok := strings.CutPrefix(line, "include"); ok && rest != "" && (rest[0] == ' ' || rest[0] == '\t') {
		target, include = strings.TrimSpace(rest), true
	} else {
		return Import{}, false, ""
	}
	p, err := importPath(from, target)
	return Import{Path: p, Include: include, Line: n}, true, err
}

// importPath resolves an import's target: //dir/OWNERS or /dir/OWNERS from the repository's root,
// anything else from the importing file's directory. It must stay in the repository and name an
// OWNERS file.
func importPath(from, target string) (string, string) {
	switch {
	case target == "":
		return "", "an import names no file"
	case strings.ContainsAny(target, " \t"):
		return "", fmt.Sprintf("%q is not one path", target)
	case strings.Contains(target, ":"):
		return "", fmt.Sprintf("%q names another repository or branch; imports are of this repository's files", target)
	}
	var p string
	switch {
	case strings.HasPrefix(target, "//"):
		p = path.Clean(target[2:])
	case strings.HasPrefix(target, "/"):
		p = path.Clean(target[1:])
	default:
		p = path.Join(path.Dir(from), target)
	}
	if p == "." || p == ".." || strings.HasPrefix(p, "../") || strings.HasPrefix(p, "/") {
		return "", fmt.Sprintf("%q is outside the repository", target)
	}
	if !IsOwnersFile(p) {
		return "", fmt.Sprintf("%q is not an OWNERS file (OWNERS, <prefix>_OWNERS, or OWNERS_<suffix>)", target)
	}
	return p, ""
}

// parseOwner reads one owner: user: <handle>, acl: <name>, or *.
func parseOwner(tok string) (Owner, string) {
	tok = strings.TrimSpace(tok)
	if tok == "*" {
		return Owner{Kind: Anyone}, ""
	}
	if name, ok := strings.CutPrefix(tok, "user:"); ok {
		name = strings.ToLower(strings.TrimSpace(name))
		if !handle(name) {
			return Owner{}, fmt.Sprintf("%q is not a handle", strings.TrimSpace(tok[len("user:"):]))
		}
		return Owner{Kind: User, Name: name}, ""
	}
	if name, ok := strings.CutPrefix(tok, "acl:"); ok {
		// A name the tree could not declare parses, and validation finds no such ACL.
		name = strings.ToLower(strings.TrimSpace(name))
		if !handle(name) {
			return Owner{}, fmt.Sprintf("%q is not an ACL's name", strings.TrimSpace(tok[len("acl:"):]))
		}
		return Owner{Kind: ACL, Name: name}, ""
	}
	return Owner{}, fmt.Sprintf("%q is not an owner (user: <handle>, acl: <name>, or *), a per-file rule, a set option, or an import", tok)
}

// handle reports whether s can be a person's handle, or an ACL's name: a letter or digit, then
// letters, digits, and ._-.
func handle(s string) bool {
	for i, r := range s {
		alnum := 'a' <= r && r <= 'z' || '0' <= r && r <= '9'
		if !alnum && (i == 0 || r != '.' && r != '_' && r != '-') {
			return false
		}
	}
	return s != ""
}

// parseRule reads a per-file rule's body: <globs>=<owners>, <globs>=set noparent, or
// <globs>=file: <path>.
func parseRule(from, body string, n int) (Rule, string) {
	globs, rhs, ok := cutTopLevel(body, '=')
	if !ok {
		// An = inside an unclosed [ or { is still the rule's: the glob is what is wrong.
		if globs, rhs, ok = strings.Cut(body, "="); !ok {
			return Rule{}, fmt.Sprintf("per-file %s: a per-file rule is <globs>=<owners>", body)
		}
	}
	r := Rule{Line: n}
	for _, g := range splitTopLevel(globs, ',') {
		g = strings.TrimSpace(g)
		if g == "" {
			return Rule{}, "per-file: an empty glob"
		}
		m, err := compileGlob(g)
		if err != nil {
			return Rule{}, fmt.Sprintf("per-file: glob %q: %v", g, err)
		}
		r.Globs = append(r.Globs, g)
		r.matchers = append(r.matchers, m)
	}
	rhs = strings.TrimSpace(rhs)
	if strings.Join(strings.Fields(rhs), " ") == "set noparent" {
		r.NoParent = true
		return r, ""
	}
	if imp, isImport, err := parseImport(from, rhs, n); isImport {
		if err != "" {
			return Rule{}, err
		}
		if imp.Include {
			return Rule{}, "per-file: a rule imports with file:, not include"
		}
		r.Import = &imp
		return r, ""
	}
	seen := map[Owner]bool{}
	for _, tok := range strings.Split(rhs, ",") {
		o, err := parseOwner(tok)
		if err != "" {
			return Rule{}, "per-file: " + err
		}
		if !seen[o] {
			seen[o] = true
			r.Owners = append(r.Owners, o)
		}
	}
	return r, ""
}

// cutTopLevel cuts s at the first sep outside {…} and […].
func cutTopLevel(s string, sep byte) (string, string, bool) {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '{', '[':
			depth++
		case '}', ']':
			if depth > 0 {
				depth--
			}
		case sep:
			if depth == 0 {
				return s[:i], s[i+1:], true
			}
		}
	}
	return s, "", false
}

// splitTopLevel splits s at every sep outside {…} and […].
func splitTopLevel(s string, sep byte) []string {
	var out []string
	for {
		before, after, ok := cutTopLevel(s, sep)
		out = append(out, before)
		if !ok {
			return out
		}
		s = after
	}
}

// sortedOwners is a set of owners, by kind and then name.
func sortedOwners(set map[Owner]bool) []Owner {
	out := make([]Owner, 0, len(set))
	for o := range set {
		out = append(out, o)
	}
	slices.SortFunc(out, func(a, b Owner) int {
		if a.Kind != b.Kind {
			return int(a.Kind) - int(b.Kind)
		}
		return strings.Compare(a.Name, b.Name)
	})
	return out
}
