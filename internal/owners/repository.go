package owners

import (
	"errors"
	"io/fs"
	"path"
	"strings"

	"github.com/Cidan/ok/internal/identity"
)

// Roster is what an approvability check knows of an installation's people: who each ACL holds,
// and who else there is.
type Roster interface {
	// ACL returns the handles of the people an ACL holds, through the sets it holds (robots approve
	// nothing, so they are not among them), and whether it exists.
	ACL(name string) (handles []string, declared bool, err error)
	// People are the handles of the people known besides those OWNERS files name.
	People() ([]string, error)
}

// TreeRoster is a groups-and-ACLs tree's roster: its ACLs, and the people its sets hold.
func TreeRoster(names identity.Names, t *identity.Tree) Roster { return treeRoster{names, t} }

type treeRoster struct {
	names identity.Names
	t     *identity.Tree
}

func (r treeRoster) ACL(acl string) ([]string, bool, error) {
	full := r.names.Of(identity.ACL, acl)
	if _, ok := r.t.Set(full); !ok {
		return nil, false, nil
	}
	var handles []string
	for _, p := range r.t.Expand(full) {
		if h, ok := HumanHandle(r.names, p); ok {
			handles = append(handles, h)
		}
	}
	return handles, true, nil
}

func (r treeRoster) People() ([]string, error) {
	var out []string
	for _, s := range r.t.Sets() {
		for _, p := range r.t.Expand(r.names.OfSet(s)) {
			if h, ok := HumanHandle(r.names, p); ok {
				out = append(out, h)
			}
		}
	}
	return out, nil
}

// EmptyRoster knows no ACL and no one: a repository checked without a tree.
var EmptyRoster Roster = emptyRoster{}

type emptyRoster struct{}

func (emptyRoster) ACL(string) ([]string, bool, error) { return nil, false, nil }
func (emptyRoster) People() ([]string, error)          { return nil, nil }

// LoadTreeRoster is the roster of the tree in fsys at treeDir, or EmptyRoster when treeDir is "".
func LoadTreeRoster(fsys fs.FS, names identity.Names, treeDir string) (Roster, error) {
	if treeDir == "" {
		return EmptyRoster, nil
	}
	sub, err := fs.Sub(fsys, treeDir)
	if err != nil {
		return nil, err
	}
	t, err := identity.Load(names, sub)
	if err != nil {
		return nil, err
	}
	return TreeRoster(names, t), nil
}

// CheckRepository checks that every OWNERS file in a repository's files can be approved by its own
// people (Approvable), the ACLs and people resolved by roster, everyone known being the roster's
// people and those the user: lines name. It returns the problems and how many OWNERS files there
// are; an error is one the roster or the files gave.
func CheckRepository(fsys fs.FS, roster Roster) ([]Problem, int, error) {
	estate := map[string]bool{}
	known, err := roster.People()
	if err != nil {
		return nil, 0, err
	}
	for _, h := range known {
		estate[h] = true
	}
	var configs []string
	err = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && p != "." && strings.HasPrefix(d.Name(), ".") {
			return fs.SkipDir
		}
		if d.IsDir() || d.Name() != "OWNERS" {
			return nil
		}
		configs = append(configs, p)
		data, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		f, _ := Parse(p, data)
		if f == nil {
			return nil
		}
		for _, o := range f.Owners {
			if o.Value.Kind == User {
				estate[o.Value.Name] = true
			}
		}
		for _, r := range f.Rules {
			for _, o := range r.Owners {
				if o.Kind == User {
					estate[o.Name] = true
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	var aclErr error
	people := People{Estate: len(estate), ACL: func(name string) ([]string, bool) {
		handles, ok, err := roster.ACL(name)
		if err != nil && aclErr == nil {
			aclErr = err
		}
		return handles, ok
	}}
	problems := Approvable(fsSource{fsys}, configs, people)
	if aclErr != nil {
		return nil, 0, aclErr
	}
	return problems, len(configs), nil
}

// Touches reports whether a change touching paths can change what CheckRepository finds: an
// OWNERS-config file, the tree at treeDir, or the repository's settings, which say where the tree
// is (settingsFile).
func Touches(paths []string, treeDir, settingsFile string) bool {
	for _, p := range paths {
		if IsOwnersFile(p) || p == settingsFile || (treeDir != "" && (p == treeDir || strings.HasPrefix(p, treeDir+"/"))) {
			return true
		}
	}
	return false
}

// fsSource reads a repository's files from a file system.
type fsSource struct{ fsys fs.FS }

func (s fsSource) Read(p string) ([]byte, bool, error) {
	data, err := fs.ReadFile(s.fsys, path.Clean(p))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	return data, err == nil, err
}
