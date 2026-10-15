package workflow

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Status is how a change touches a file.
type Status string

const (
	Added    Status = "added"
	Modified Status = "modified"
	Deleted  Status = "deleted"
	Renamed  Status = "renamed"
	Copied   Status = "copied"
)

// ChangedFile is one file a change touches.
type ChangedFile struct {
	Path    string `json:"path"`
	Status  Status `json:"status"`
	OldPath string `json:"old_path,omitempty"` // a rename's or copy's source
}

// Repository names the repository a job runs in.
type Repository struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// ContextFile is the job context's name in the job's checkout; the job's OK_CONTEXT variable
// names its path.
const ContextFile = "ok-context.json"

// ContextEnv is the variable that names the job context's file to a run job's binary.
const ContextEnv = "OK_CONTEXT"

// Change is the change a job runs for, as ok records it: everything of the job context but
// the changed files, which the worker that runs the job reads from the SCM — a change's files
// can be any number, so they never travel with a job.
type Change struct {
	Phase      Phase      `json:"phase"`
	Repository Repository `json:"repository"`
	CL         int64      `json:"cl"`
	Patchset   int32      `json:"patchset"`
	Author     string     `json:"author"` // the CL's author, a principal
	// Commit is the commit under test — the patchset, or the candidate — and Base the commit
	// it changes, both in hex.
	Commit string `json:"commit"`
	Base   string `json:"base"`
}

// Context is the job context: what ok tells a job about the change it runs for — the change
// and its files. The worker writes it, as JSON, into the job's checkout as ok-context.json.
type Context struct {
	Change
	Files []ChangedFile `json:"changed_files"`
}

// Existing is every changed file that exists at the commit under test.
func (c *Context) Existing() []string {
	var out []string
	for _, f := range c.Files {
		if f.Status != Deleted {
			out = append(out, f.Path)
		}
	}
	return out
}

// Removed is every path the change removes: the deleted files, and the old path of each
// rename.
func (c *Context) Removed() []string {
	var out []string
	for _, f := range c.Files {
		switch f.Status {
		case Deleted:
			out = append(out, f.Path)
		case Renamed:
			out = append(out, f.OldPath)
		}
	}
	return out
}

// Touched is every path the change touches, as OWNERS count them: both paths of a rename,
// the old path of a deletion, and the new path of everything else.
func (c *Context) Touched() []string {
	var out []string
	for _, f := range c.Files {
		out = append(out, f.Path)
		if f.Status == Renamed {
			out = append(out, f.OldPath)
		}
	}
	return out
}

// The placeholders a job's arguments may carry. A list placeholder is a whole argument and
// expands to one argument per path; a scalar one may sit inside an argument.
var (
	listPlaceholders   = map[string]bool{"changed_files": true, "deleted_files": true}
	scalarPlaceholders = map[string]bool{"cl": true, "patchset": true, "commit": true, "base": true, "phase": true}
)

// argPart is one piece of an argument: literal text, or a placeholder with its optional glob.
type argPart struct {
	literal string
	name    string // "" for a literal
	glob    *regexp.Regexp
}

// parseArg splits an argument into literal text and placeholders: `{name}` or
// `{name:glob}`, with `{{` and `}}` for literal braces.
func parseArg(arg string) ([]argPart, error) {
	var parts []argPart
	var lit strings.Builder
	flush := func() {
		if lit.Len() > 0 {
			parts = append(parts, argPart{literal: lit.String()})
			lit.Reset()
		}
	}
	for i := 0; i < len(arg); i++ {
		switch c := arg[i]; c {
		case '{':
			if i+1 < len(arg) && arg[i+1] == '{' {
				lit.WriteByte('{')
				i++
				continue
			}
			end := placeholderEnd(arg, i)
			if end < 0 {
				return nil, errors.New("a { opens a placeholder that is never closed (write {{ for a literal {)")
			}
			body := arg[i+1 : end]
			name, glob, hasGlob := strings.Cut(body, ":")
			p := argPart{name: name}
			switch {
			case listPlaceholders[name]:
				if hasGlob {
					if glob == "" {
						return nil, fmt.Errorf("{%s:} has an empty glob", name)
					}
					re, err := compileGlob(glob)
					if err != nil {
						return nil, fmt.Errorf("{%s}: %v", body, err)
					}
					p.glob = re
				}
			case scalarPlaceholders[name]:
				if hasGlob {
					return nil, fmt.Errorf("{%s} takes no glob; only changed_files and deleted_files do", name)
				}
			default:
				return nil, fmt.Errorf("unknown placeholder {%s} (the placeholders are changed_files, deleted_files, cl, patchset, commit, base, and phase)", name)
			}
			flush()
			parts = append(parts, p)
			i = end
		case '}':
			if i+1 < len(arg) && arg[i+1] == '}' {
				lit.WriteByte('}')
				i++
				continue
			}
			return nil, errors.New("a } closes no placeholder (write }} for a literal })")
		default:
			lit.WriteByte(c)
		}
	}
	flush()
	for _, p := range parts {
		if listPlaceholders[p.name] && len(parts) > 1 {
			return nil, fmt.Errorf("{%s} expands to one argument per path, so it must be the whole argument", p.name)
		}
	}
	return parts, nil
}

// placeholderEnd returns the index of the } that closes the placeholder opened at start, or
// -1: a glob's own {a,b} alternations and \-escapes are skipped over.
func placeholderEnd(arg string, start int) int {
	depth := 0
	for j := start + 1; j < len(arg); j++ {
		switch arg[j] {
		case '\\':
			j++
		case '{':
			depth++
		case '}':
			if depth == 0 {
				return j
			}
			depth--
		}
	}
	return -1
}

// HasPlaceholders reports whether any of a job's arguments carries a placeholder: what such a
// job runs depends on the change it runs for — its phase, its commit — and not only on its
// target. An argument that does not parse counts as one.
func HasPlaceholders(args []string) bool {
	for _, a := range args {
		parts, err := parseArg(a)
		if err != nil {
			return true
		}
		for _, p := range parts {
			if p.name != "" {
				return true
			}
		}
	}
	return false
}

// Expand expands the placeholders in a job's arguments from its context. A list placeholder
// expands to one argument per path, in the context's order, and to none when nothing matches.
func Expand(args []string, c *Context) ([]string, error) {
	var out []string
	for _, a := range args {
		parts, err := parseArg(a)
		if err != nil {
			return nil, fmt.Errorf("argument %q: %v", a, err)
		}
		if len(parts) == 1 && listPlaceholders[parts[0].name] {
			paths := c.Existing()
			if parts[0].name == "deleted_files" {
				paths = c.Removed()
			}
			for _, p := range paths {
				if parts[0].glob == nil || parts[0].glob.MatchString(p) {
					out = append(out, p)
				}
			}
			continue
		}
		var b strings.Builder
		for _, p := range parts {
			switch p.name {
			case "":
				b.WriteString(p.literal)
			case "cl":
				b.WriteString(strconv.FormatInt(c.CL, 10))
			case "patchset":
				b.WriteString(strconv.FormatInt(int64(c.Patchset), 10))
			case "commit":
				b.WriteString(c.Commit)
			case "base":
				b.WriteString(c.Base)
			case "phase":
				b.WriteString(string(c.Phase))
			}
		}
		out = append(out, b.String())
	}
	return out, nil
}
