package owners

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
)

// Source reads a repository's files at the revision OWNERS are read from — the base branch's
// head, never the change's. ok is false where there is no file.
type Source interface {
	Read(path string) (data []byte, ok bool, err error)
}

// Set is the owners of some of a change's files: who governs them, and who may approve them.
type Set struct {
	// File is the OWNERS file that governs them; "" for files no OWNERS file governs, which are
	// unrestricted: owned by anyone, their author too.
	File string
	// Rules are the per-file rules of File (and what it includes) the files match, by label.
	Rules []string
	// Owners are who may approve them, by kind and then name.
	Owners []Owner
	// AllowSelf is whether the author's own grants count.
	AllowSelf bool
	// Files are the files, as given.
	Files []string
}

// Unrestricted reports whether no OWNERS file governs the set's files.
func (s *Set) Unrestricted() bool { return s.File == "" }

// Resolver finds the owners of files, reading each OWNERS file once.
type Resolver struct {
	src    Source
	files  map[string]*parsed
	govern map[string]string // a directory -> the OWNERS file governing it ("" for none)
	full   map[string]*fullResult
}

type parsed struct {
	f        *File
	problems []Problem
	missing  bool
}

// expanded is an OWNERS file in full: its directory-level owners (its own, and what its imports
// give), its per-file rules (its own, and its includes', each with its owners), and allowself (its
// own, or an include's).
type expanded struct {
	owners    map[Owner]bool
	rules     []resolvedRule
	allowSelf bool
}

// resolvedRule is a per-file rule, the file it is written in, and its owners: its own, and its
// import's directory-level owners.
type resolvedRule struct {
	Rule
	file   string
	owners map[Owner]bool
}

// NewResolver reads OWNERS files from src.
func NewResolver(src Source) *Resolver {
	return &Resolver{src: src, files: map[string]*parsed{}, govern: map[string]string{}, full: map[string]*fullResult{}}
}

type fullResult struct {
	e   *expanded
	err error
}

// load reads and parses an OWNERS file once.
func (r *Resolver) load(p string) (*parsed, error) {
	if pf, ok := r.files[p]; ok {
		return pf, nil
	}
	data, ok, err := r.src.Read(p)
	if err != nil {
		return nil, err
	}
	pf := &parsed{missing: !ok}
	if ok {
		pf.f, pf.problems = Parse(p, data)
	}
	r.files[p] = pf
	return pf, nil
}

// file is an OWNERS file that exists and parses; stack is the chain of files importing it, in
// which it appearing again is a cycle.
func (r *Resolver) file(p string, stack []string) (*File, error) {
	if i := slices.Index(stack, p); i >= 0 {
		return nil, fmt.Errorf("imports go round: %s", strings.Join(append(slices.Clone(stack[i:]), p), " → "))
	}
	pf, err := r.load(p)
	if err != nil {
		return nil, err
	}
	if pf.missing {
		return nil, fmt.Errorf("%s does not exist", p)
	}
	if len(pf.problems) > 0 {
		return nil, Problems(pf.problems)
	}
	return pf.f, nil
}

// governing is the OWNERS file nearest a directory, at it or above: "" when there is none.
func (r *Resolver) governing(dir string) (string, error) {
	if g, ok := r.govern[dir]; ok {
		return g, nil
	}
	p := path.Join(dir, "OWNERS")
	pf, err := r.load(p)
	if err != nil {
		return "", err
	}
	g := ""
	switch {
	case !pf.missing:
		g = p
	case dir != "" && dir != ".":
		parent := path.Dir(dir)
		if parent == "." {
			parent = ""
		}
		if g, err = r.governing(parent); err != nil {
			return "", err
		}
	}
	r.govern[dir] = g
	return g, nil
}

// dirOwners are an OWNERS file's directory-level owners: its owner lines, and the directory-level
// owners of every file it imports, file: or include.
func (r *Resolver) dirOwners(p string, stack []string) (map[Owner]bool, error) {
	f, err := r.file(p, stack)
	if err != nil {
		return nil, err
	}
	owners := map[Owner]bool{}
	for _, o := range f.Owners {
		owners[o.Value] = true
	}
	for _, imp := range f.Imports {
		sub, err := r.dirOwners(imp.Path, append(slices.Clone(stack), p))
		if err != nil {
			return nil, at(err, p, imp.Line)
		}
		for o := range sub {
			owners[o] = true
		}
	}
	return owners, nil
}

// expand is an OWNERS file in full. A per-file rule's import gives the target's directory-level
// owners, followed afresh: they do not depend on any per-file rule, so a rule importing its own
// file is no cycle. Anything that cannot be read is an error: owners fail closed.
func (r *Resolver) expand(p string, stack []string) (*expanded, error) {
	f, err := r.file(p, stack)
	if err != nil {
		return nil, err
	}
	owners, err := r.dirOwners(p, stack)
	if err != nil {
		return nil, err
	}
	e := &expanded{owners: owners, allowSelf: f.AllowSelf}
	for _, rule := range f.Rules {
		rr := resolvedRule{Rule: rule, file: p, owners: map[Owner]bool{}}
		for _, o := range rule.Owners {
			rr.owners[o] = true
		}
		if rule.Import != nil {
			sub, err := r.dirOwners(rule.Import.Path, nil)
			if err != nil {
				return nil, at(err, p, rule.Line)
			}
			for o := range sub {
				rr.owners[o] = true
			}
		}
		e.rules = append(e.rules, rr)
	}
	for _, imp := range f.Imports {
		if !imp.Include {
			continue
		}
		sub, err := r.expand(imp.Path, append(slices.Clone(stack), p))
		if err != nil {
			return nil, at(err, p, imp.Line)
		}
		e.rules = append(e.rules, sub.rules...)
		e.allowSelf = e.allowSelf || sub.allowSelf
	}
	return e, nil
}

// governed is a governing OWNERS file in full, expanded once.
func (r *Resolver) governed(p string) (*expanded, error) {
	if res, ok := r.full[p]; ok {
		return res.e, res.err
	}
	e, err := r.expand(p, nil)
	r.full[p] = &fullResult{e, err}
	return e, err
}

// at is an import's failure as problems: where they are found — the innermost file whose own
// lines fail, whatever imports lead there — or, for an import that names no file or goes round,
// the importing line.
func at(err error, p string, line int) Problems {
	var ps Problems
	if errors.As(err, &ps) {
		return ps
	}
	return Problems{{Path: p, Line: line, Msg: err.Error()}}
}

// Resolve groups files — repository-relative paths, a deleted file's old path, both of a rename's —
// by the owner set governing each: the nearest OWNERS file at or above its directory, and the
// per-file rules of it (and of what it includes) the file matches, relative to its directory. A
// file matching a `set noparent` rule is owned by the per-file rules it matches alone; any other,
// by the file's directory-level owners too. Sets are ordered by OWNERS file, then rules; an OWNERS
// file that cannot be read — a syntax error, a broken import — is an error, so approval fails
// closed.
func (r *Resolver) Resolve(files []string) ([]*Set, error) {
	var sets []*Set
	byKey := map[string]*Set{}
	for _, f := range files {
		dir := path.Dir(f)
		if dir == "." {
			dir = ""
		}
		gov, err := r.governing(dir)
		if err != nil {
			return nil, err
		}
		key, set := "", (*Set)(nil)
		if gov == "" {
			set = &Set{Owners: []Owner{{Kind: Anyone}}, AllowSelf: true}
		} else {
			e, err := r.governed(gov)
			if err != nil {
				return nil, err
			}
			rel := strings.TrimPrefix(f, path.Dir(gov)+"/")
			if path.Dir(gov) == "." {
				rel = f
			}
			owners := map[Owner]bool{}
			noParent := false
			var labels, ids []string
			for _, rule := range e.rules {
				if !rule.matches(rel) {
					continue
				}
				noParent = noParent || rule.NoParent
				labels = append(labels, rule.Label())
				ids = append(ids, fmt.Sprintf("%s:%d", rule.file, rule.Line))
				for o := range rule.owners {
					owners[o] = true
				}
			}
			if !noParent {
				for o := range e.owners {
					owners[o] = true
				}
			}
			key = gov + "\x00" + strings.Join(ids, "\x00")
			set = &Set{File: gov, Rules: labels, Owners: sortedOwners(owners), AllowSelf: e.allowSelf}
		}
		if s, ok := byKey[key]; ok {
			s.Files = append(s.Files, f)
			continue
		}
		set.Files = []string{f}
		byKey[key] = set
		sets = append(sets, set)
	}
	slices.SortStableFunc(sets, func(a, b *Set) int {
		if c := strings.Compare(a.File, b.File); c != 0 {
			return c
		}
		return strings.Compare(strings.Join(a.Rules, ","), strings.Join(b.Rules, ","))
	})
	return sets, nil
}
