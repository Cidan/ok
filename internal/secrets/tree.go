package secrets

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/owners"
)

// MaxACLFile bounds an acl.yaml the tree's readers read.
const MaxACLFile = 64 << 10

// Error is everything wrong with a tree, each problem a sentence.
type Error struct {
	Problems []string
}

func (e *Error) Error() string {
	if len(e.Problems) == 1 {
		return e.Problems[0]
	}
	return fmt.Sprintf("%d problems:\n  %s", len(e.Problems), strings.Join(e.Problems, "\n  "))
}

// AsError returns err's *Error, if it is one.
func AsError(err error) (*Error, bool) {
	var e *Error
	ok := errors.As(err, &e)
	return e, ok
}

// Unseal is what a person proves to unseal a folder's secret (docs/secrets.md, "Unsealing").
type Unseal string

const (
	// UnsealDevice is the default: the person's device credential, which every request carries.
	UnsealDevice Unseal = "device"
	// UnsealPasskey asks for the person's passkey, each unseal.
	UnsealPasskey Unseal = "passkey"
)

// Access is what a folder's acl.yaml grants: whose members read its secrets, and whose write them;
// what a person proves to unseal one; and which of the repository's deploys and post jobs read
// them. None implies another, and none approves a change — OWNERS do.
type Access struct {
	// Readers and Writers are sets by their directory names: ACLs, and in a hosted service an
	// organization's teams.
	Readers []string
	Writers []string
	Unseal  Unseal
	// Deploys and Runs are deploys and post jobs by their identities (workflow.Deploy.ID,
	// workflow.Job.ID): each reads the folder's secrets its stanza names, at commits of main.
	Deploys []string
	Runs    []string
}

// GrantsDeploy reports whether the folder's secrets are given to a deploy.
func (a Access) GrantsDeploy(id string) bool { return slices.Contains(a.Deploys, id) }

// GrantsRun reports whether the folder's secrets are given to a post job.
func (a Access) GrantsRun(id string) bool { return slices.Contains(a.Runs, id) }

// declaration is an acl.yaml as written.
type declaration struct {
	Readers *[]string `yaml:"readers"`
	Writers *[]string `yaml:"writers"`
	Unseal  string    `yaml:"unseal"`
	Deploys []string  `yaml:"deploys"`
	Runs    []string  `yaml:"runs"`
}

// ParseACL reads an acl.yaml: one YAML document with two keys, readers and writers, each a list
// of sets — an ACL (prod-db, or prod-db.acl.example.com), or in a hosted service an
// organization's team (@acme/prod-db) — each once in its list ([] for none); unseal, device or
// passkey; and deploys and runs, the deploys' and post jobs' identities given its secrets. Sets
// come back as their directory names.
func ParseACL(names identity.Names, data []byte) (Access, error) {
	if len(data) > MaxACLFile {
		return Access{}, fmt.Errorf("is larger than %d bytes", MaxACLFile)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var d declaration
	if err := dec.Decode(&d); err != nil {
		if errors.Is(err, io.EOF) {
			return Access{}, errors.New("declares nothing; an acl.yaml lists the sets whose members read the folder's secrets under readers, and those whose members write them under writers")
		}
		return Access{}, err
	}
	var extra declaration
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return Access{}, errors.New("holds more than one YAML document")
	}
	var problems []string
	list := func(key string, forms *[]string) []string {
		if forms == nil {
			problems = append(problems, fmt.Sprintf("has no %s; list the sets (an ACL, or @org/team) whose members may, or %s: [] for none", key, key))
			return nil
		}
		out := []string{}
		seen := map[string]bool{}
		for _, f := range *forms {
			name, ok := names.ParseSet(f)
			kind, _ := names.KindOf(name)
			switch {
			case !ok:
				problems = append(problems, fmt.Sprintf("%s: %q names no set: a secret is given to the members of an ACL or a team, never to a person or a robot by name", key, f))
				continue
			case kind == identity.GroupMember:
				problems = append(problems, fmt.Sprintf("%s: %s is a group: give the secret to an ACL that holds it", key, f))
				continue
			case seen[name]:
				problems = append(problems, fmt.Sprintf("%s: %s is named twice", key, f))
				continue
			}
			seen[name] = true
			out = append(out, name)
		}
		return out
	}
	a := Access{Readers: list("readers", d.Readers), Writers: list("writers", d.Writers), Unseal: UnsealDevice}
	switch Unseal(d.Unseal) {
	case "", UnsealDevice:
	case UnsealPasskey:
		a.Unseal = UnsealPasskey
	default:
		problems = append(problems, fmt.Sprintf("unseal: %q is neither device nor passkey", d.Unseal))
	}
	grants := func(key string, ids []string, valid func(string) bool, form string) []string {
		seen := map[string]bool{}
		for _, id := range ids {
			switch {
			case !valid(id):
				problems = append(problems, fmt.Sprintf("%s: %q is not %s", key, id, form))
			case seen[id]:
				problems = append(problems, fmt.Sprintf("%s: %s is named twice", key, id))
			}
			seen[id] = true
		}
		return append([]string(nil), ids...)
	}
	a.Deploys = grants("deploys", d.Deploys, ValidDeploy, "a deploy's identity, //<dir>/.ok/workflow/<file>.yaml:deploy:<name>")
	a.Runs = grants("runs", d.Runs, ValidRun, "a post job's identity, //<dir>/.ok/workflow/<file>.yaml:post:<job>")
	if len(problems) > 0 {
		return Access{}, errors.New(strings.Join(problems, "; "))
	}
	return a, nil
}

// EntryKind is what a tree entry is.
type EntryKind int

const (
	File EntryKind = iota + 1
	Executable
	Symlink
)

// Entry is one leaf of a commit's //secret tree: a file (with its contents, which a reader reads
// up to MaxACLFile for an acl.yaml, and a pointer's length for a secret) or a symlink.
type Entry struct {
	// Path is the repository path: "secret/prod/db-password".
	Path     string
	Kind     EntryKind
	Contents []byte
}

// Tree is a valid //secret tree.
type Tree struct {
	// Folders maps each folder ("secret", "secret/prod") to what its acl.yaml grants.
	Folders map[string]Access
	// Secrets maps each secret's path to its pointer's digest.
	Secrets map[string]Digest
}

// Load validates a commit's //secret tree, given as its leaves (every entry under secret/), and
// returns it: every folder — the root and every folder beneath — has an acl.yaml naming its
// readers and writers, every OWNERS-config file is a regular file (OWNERS validation reads it),
// every other entry is a regular, non-executable file holding a pointer, and every name below
// secret/ is a valid one. The error is an *Error listing every problem.
func Load(names identity.Names, entries []Entry) (*Tree, error) {
	t := &Tree{Folders: map[string]Access{}, Secrets: map[string]Digest{}}
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	sorted := append([]Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })
	folders := map[string]bool{}
	badName := map[string]bool{}
	for _, e := range sorted {
		rest, ok := strings.CutPrefix(e.Path, Dir+"/")
		if e.Path == Dir {
			add("%s: is a file; //%s is a folder", e.Path, Dir)
			continue
		}
		if !ok {
			add("%s: is not in //%s", e.Path, Dir)
			continue
		}
		parts := strings.Split(rest, "/")
		folder := Dir
		valid := true
		for i, part := range parts {
			last := i == len(parts)-1
			if !(last && (part == ACLFile || owners.IsOwnersFile(folder+"/"+part))) && !ValidName(part) {
				p := folder + "/" + part
				if !badName[p] {
					badName[p] = true
					add("%s: %q is not a valid name ([A-Za-z0-9][A-Za-z0-9._-]*, at most %d bytes; no dotfiles)", p, part, MaxNameLen)
				}
				valid = false
				break
			}
			if !last {
				folders[folder] = true
				folder += "/" + part
			}
		}
		if !valid {
			continue
		}
		folders[folder] = true
		if path.Base(e.Path) == ACLFile {
			if e.Kind != File {
				add("%s: an %s is a regular file", e.Path, ACLFile)
				continue
			}
			access, err := ParseACL(names, e.Contents)
			if err != nil {
				add("%s: %v", e.Path, err)
				continue
			}
			t.Folders[path.Dir(e.Path)] = access
			continue
		}
		if IsOwners(e.Path) {
			if e.Kind != File {
				add("%s: an OWNERS file is a regular file", e.Path)
			}
			continue
		}
		switch e.Kind {
		case Symlink:
			add("%s: is a symlink; every file in //%s but an %s or an OWNERS file is a secret, kept as its pointer", e.Path, Dir, ACLFile)
			continue
		case Executable:
			add("%s: is executable; a secret is a regular file (jj file chmod n %s)", e.Path, e.Path)
			continue
		}
		d, err := ParsePointer(e.Contents)
		if err != nil {
			add("%s: holds a value, not a pointer: a secret's value never enters the repository (write it in a virtual workspace, which keeps it in ok secrets)", e.Path)
			continue
		}
		t.Secrets[e.Path] = d
	}
	list := make([]string, 0, len(folders))
	for f := range folders {
		list = append(list, f)
	}
	sort.Strings(list)
	for _, f := range list {
		if _, ok := t.Folders[f]; !ok && !hasProblemFor(problems, f+"/"+ACLFile) {
			add("%s/: has no %s; every folder of //%s names the sets that read and write its secrets", f, ACLFile, Dir)
		}
	}
	if len(problems) > 0 {
		return nil, &Error{Problems: problems}
	}
	return t, nil
}

// hasProblemFor reports whether a problem was already reported for a path.
func hasProblemFor(problems []string, p string) bool {
	for _, pr := range problems {
		if strings.HasPrefix(pr, p+":") {
			return true
		}
	}
	return false
}
