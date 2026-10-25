package identity

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// declaration is an acl.yaml or a group.yaml.
type declaration struct {
	Members *[]string `yaml:"members"`
}

// Load reads the tree rooted at fsys's root and validates it under names: LoadFiles over every
// file in it.
func Load(names Names, fsys fs.FS, opts ...Option) (*Tree, error) {
	files := map[string][]byte{}
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p != "." && strings.HasPrefix(d.Name(), ".") {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		var data []byte
		if d.Name() == ACL.File() || d.Name() == Group.File() {
			if data, err = fs.ReadFile(fsys, p); err != nil {
				return err
			}
		}
		files[p] = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	return LoadFiles(names, files, opts...)
}

// LoadFiles reads a tree given as its files — each by its slash-separated path from the tree's
// root, with its contents (only acl.yaml's and group.yaml's are read) — and validates it under
// names (New).
// Each folder's acl.yaml and group.yaml declare its sets, and its subfolders are sets beneath it.
// Paths with a part starting with "." are skipped (.ok/); the root's other files are its own
// (its documentation), and any other file elsewhere is refused, as is a folder that
// is not a lowercase DNS label. The error is an *Error listing every problem.
func LoadFiles(names Names, files map[string][]byte, opts ...Option) (*Tree, error) {
	sets, problems := declarations(files)
	if len(problems) > 0 {
		return nil, &Error{Problems: problems}
	}
	return New(names, sets, opts...)
}

// Referenced are the full names of the sets files declare and of everything those list, sorted:
// what a tree of them may need of the groups the system of record syncs, fetched before
// LoadFiles validates it. Files that do not parse add nothing.
func Referenced(names Names, files map[string][]byte) []string {
	sets, _ := declarations(files)
	seen := map[string]bool{}
	for _, s := range sets {
		seen[names.OfSet(s)] = true
		for _, m := range s.Members {
			seen[m] = true
		}
	}
	return sortedKeys(seen)
}

// declarations are the sets files declare, and what is wrong with them, each a sentence.
func declarations(files map[string][]byte) ([]Set, []string) {
	var sets []Set
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	badFolder := map[string]bool{}
	for _, p := range paths {
		parts := strings.Split(p, "/")
		hidden := false
		for _, part := range parts {
			hidden = hidden || strings.HasPrefix(part, ".")
		}
		if hidden {
			continue
		}
		bad := ""
		for i := range parts[:len(parts)-1] {
			if !ValidLabel(parts[i]) {
				bad = strings.Join(parts[:i+1], "/")
				break
			}
		}
		if bad != "" {
			if !badFolder[bad] {
				badFolder[bad] = true
				add("%s/: a folder is a set's name, a lowercase DNS label", bad)
			}
			continue
		}
		folder := path.Dir(p)
		if folder == "." {
			folder = ""
		}
		var kind Kind
		switch path.Base(p) {
		case ACL.File():
			kind = ACL
		case Group.File():
			kind = Group
		default:
			if folder != "" {
				add("%s: a folder in the tree holds only %s, %s, and folders", p, ACL.File(), Group.File())
			}
			continue
		}
		members, err := parseDeclaration(files[p])
		if err != nil {
			add("%s: %v", p, err)
			continue
		}
		name := Root
		if folder != "" {
			name = path.Base(folder)
		}
		sets = append(sets, Set{Kind: kind, Name: name, Path: folder, Members: members})
	}
	return sets, problems
}

// parseDeclaration reads an acl.yaml or group.yaml: one key, members, a list of names.
func parseDeclaration(data []byte) ([]string, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var d declaration
	if err := dec.Decode(&d); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("declares nothing; a set's members are a list under members (members: [] for none)")
		}
		return nil, err
	}
	var extra declaration
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("holds more than one YAML document")
	}
	if d.Members == nil {
		return nil, errors.New("declares no members; a set's members are a list under members (members: [] for none)")
	}
	return append([]string{}, *d.Members...), nil
}
