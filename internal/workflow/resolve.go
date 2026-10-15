package workflow

import (
	"fmt"
	gopath "path"
	"sort"
	"strings"
)

// Source reads a file from the tree workflows are resolved against: main, for selecting a
// change's jobs, or a commit's tree, for validating it. path is repository-relative; ok is
// false when there is no file there.
type Source interface {
	Read(path string) (data []byte, ok bool, err error)
}

// includedFrom checks that an include points strictly upward: the included file's directory
// declares automation for a proper ancestor of the including file's, so includes cannot
// cycle and a directory never pulls in a sibling's or a descendant's jobs.
func includedFrom(from, target string) error {
	fromDir, targetDir := DeclaringDir(from), DeclaringDir(target)
	if targetDir == fromDir || !isAncestor(targetDir, fromDir) {
		return fmt.Errorf("include //%s is not in an ancestor of %s: includes point strictly upward", target, dirName(fromDir))
	}
	return nil
}

// isAncestor reports whether directory a is b or above it ("" is the root).
func isAncestor(a, b string) bool {
	return a == "" || a == b || strings.HasPrefix(b, a+"/")
}

func dirName(d string) string {
	return "//" + d
}

// Validate checks one workflow file's content — as a CL edits it, read against that CL's
// tree through src — for what can be checked without the build graph: its syntax, and that
// each include resolves to a workflow file above it that is itself valid. It returns every
// problem found: the edited file's first, in line order, then those of the files it
// includes, carrying their paths. Whether each job's target exists, and can back the job,
// needs Bazel, and is checked in pre.
func Validate(path string, data []byte, src Source) []Error {
	f, errs := Parse(path, data)
	seen := map[string]bool{path: true}
	var included []Error
	for _, inc := range f.Includes {
		own, more := includeProblems(path, inc, src, seen)
		errs = append(errs, own...)
		included = append(included, more...)
	}
	sort.SliceStable(errs, func(i, j int) bool { return errs[i].Line < errs[j].Line })
	return append(errs, included...)
}

// includeProblems checks one include of the file from: that it points upward and exists
// (problems of from), then that the included file — and, in turn, what it includes — is
// valid (problems of those files). Each file is checked once.
func includeProblems(from string, inc Include, src Source, seen map[string]bool) (own, included []Error) {
	if err := includedFrom(from, inc.Path); err != nil {
		return []Error{{Path: from, Line: inc.Line, Msg: err.Error()}}, nil
	}
	if seen[inc.Path] {
		return nil, nil
	}
	seen[inc.Path] = true
	data, ok, err := src.Read(inc.Path)
	switch {
	case err != nil:
		return []Error{{Path: from, Line: inc.Line, Msg: fmt.Sprintf("reading include //%s: %v", inc.Path, err)}}, nil
	case !ok:
		return []Error{{Path: from, Line: inc.Line, Msg: fmt.Sprintf("include //%s does not exist", inc.Path)}}, nil
	}
	f, errs := Parse(inc.Path, data)
	var deeper []Error
	for _, next := range f.Includes {
		o, more := includeProblems(inc.Path, next, src, seen)
		errs = append(errs, o...)
		deeper = append(deeper, more...)
	}
	sort.SliceStable(errs, func(i, j int) bool { return errs[i].Line < errs[j].Line })
	return nil, append(errs, deeper...)
}

// Set is a repository's workflow files at one revision — main's, when selecting a change's
// jobs — parsed, by declaring directory.
type Set struct {
	files map[string]*File
	errs  []Error
	dirs  map[string][]string // declaring directory → its workflow files, sorted
}

// NewSet parses a repository's workflow files, given by repository-relative path. Paths that
// are not workflow files are ignored. The problems of files that do not parse are kept
// (Problems); what does parse still counts.
func NewSet(files map[string][]byte) *Set {
	s := &Set{files: map[string]*File{}, dirs: map[string][]string{}}
	paths := make([]string, 0, len(files))
	for p := range files {
		if IsWorkflowFile(p) {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	for _, p := range paths {
		f, errs := Parse(p, files[p])
		s.files[p] = f
		s.errs = append(s.errs, errs...)
		d := DeclaringDir(p)
		s.dirs[d] = append(s.dirs[d], p)
	}
	return s
}

// Problems are the problems of the set's files that do not parse, and of includes that do
// not resolve.
func (s *Set) Problems() []Error {
	out := append([]Error(nil), s.errs...)
	for _, p := range s.sortedPaths() {
		for _, inc := range s.files[p].Includes {
			if err := s.checkInclude(p, inc); err != nil {
				out = append(out, *err)
			}
		}
	}
	return out
}

func (s *Set) sortedPaths() []string {
	out := make([]string, 0, len(s.files))
	for p := range s.files {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func (s *Set) checkInclude(from string, inc Include) *Error {
	if err := includedFrom(from, inc.Path); err != nil {
		return &Error{Path: from, Line: inc.Line, Msg: err.Error()}
	}
	if s.files[inc.Path] == nil {
		return &Error{Path: from, Line: inc.Line, Msg: fmt.Sprintf("include //%s does not exist", inc.Path)}
	}
	return nil
}

// Governing returns the workflow directory that governs a directory: the closest at or
// above it that declares workflow files. Resolution stops there, so its workflows replace
// those above it. ok is false when none does.
func (s *Set) Governing(dir string) (string, bool) {
	for {
		if len(s.dirs[dir]) > 0 {
			return dir, true
		}
		if dir == "" {
			return "", false
		}
		dir = gopath.Dir(dir)
		if dir == "." {
			dir = ""
		}
	}
}

// Jobs returns the jobs of a workflow directory: those of each of its files and,
// transitively, of what they include, each once, in a stable order.
func (s *Set) Jobs(dir string) []Job {
	seen := map[string]bool{}
	var out []Job
	var visit func(path string)
	visit = func(path string) {
		f := s.files[path]
		if f == nil || seen[path] {
			return
		}
		seen[path] = true
		out = append(out, f.Jobs...)
		for _, inc := range f.Includes {
			if s.checkInclude(path, inc) == nil {
				visit(inc.Path)
			}
		}
	}
	for _, p := range s.dirs[dir] {
		visit(p)
	}
	return out
}

// Deploys returns every deploy of every workflow file, in a stable order. Deploys are not
// selected through directories or includes: each counts once, wherever it is declared.
func (s *Set) Deploys() []Deploy {
	var out []Deploy
	for _, p := range s.sortedPaths() {
		out = append(out, s.files[p].Deploys...)
	}
	return out
}

// DeployProblems are the deploys that name a workload another deploy already names — two
// deploys of one workload would fight — each reported where it is declared.
func (s *Set) DeployProblems() []Error {
	first := map[string]Deploy{}
	var out []Error
	for _, d := range s.Deploys() {
		if d.Workload == nil {
			continue
		}
		key := d.Workload.String()
		if f, ok := first[key]; ok {
			out = append(out, Error{Path: d.File, Line: d.Workload.Line,
				Msg: fmt.Sprintf("deploy %s rolls %s, which deploy %s (%s) already rolls: a workload has one deploy", d.Name, key, f.Name, f.ID())})
			continue
		}
		first[key] = d
	}
	return out
}

// All returns every job of every workflow file, in a stable order.
func (s *Set) All() []Job {
	var out []Job
	for _, p := range s.sortedPaths() {
		out = append(out, s.files[p].Jobs...)
	}
	return out
}

// Includers returns every workflow file that includes, directly or transitively, one of
// paths: the files a change to those paths can break without editing them.
func (s *Set) Includers(paths []string) []string {
	target := map[string]bool{}
	for _, p := range paths {
		target[p] = true
	}
	var out []string
	for _, p := range s.sortedPaths() {
		if s.reaches(p, target, map[string]bool{}) {
			out = append(out, p)
		}
	}
	return out
}

func (s *Set) reaches(p string, target, seen map[string]bool) bool {
	if seen[p] {
		return false
	}
	seen[p] = true
	f := s.files[p]
	if f == nil {
		return false
	}
	for _, inc := range f.Includes {
		if target[inc.Path] || s.reaches(inc.Path, target, seen) {
			return true
		}
	}
	return false
}
