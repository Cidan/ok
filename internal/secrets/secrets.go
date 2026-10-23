// Package secrets is the model of a repository's secrets (docs/secrets.md): the //secret tree,
// whose folders' acl.yaml files name the sets that read and write their secrets, whose OWNERS
// files approve changes to it as anywhere else, and whose every other file is a secret's pointer;
// pointers and the commitments they name; the key hierarchy's associated data; and the files a
// deploy's migrations and a post job read their secrets from. Pure Go with no I/O: the SCM
// enforces the tree with it and ok secrets decides access with it, so the two agree by
// construction. ok's jj implements pointers, commitments, and names again in Rust (jj/secret),
// held to the same golden vectors (testdata/vectors.json).
package secrets

import (
	"path"
	"regexp"
	"strings"

	"github.com/Cidan/ok/internal/owners"
)

// Dir is the folder, at a repository's root, that holds its secrets: //secret.
const Dir = "secret"

// ACLFile is the file in each of the tree's folders naming the sets that may use its secrets.
const ACLFile = "acl.yaml"

// MaxValue is the largest value a secret holds.
const MaxValue = 1 << 20

// MaxNameLen bounds a folder's or a secret's name, as a file system does.
const MaxNameLen = 255

// nameRE is a folder's or a secret's name: no dotfiles, so an editor's swap and backup files are
// never secrets, and nothing a shell or a URL must quote.
var nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// ValidName reports whether s may name a folder or a secret in the tree.
func ValidName(s string) bool { return len(s) <= MaxNameLen && nameRE.MatchString(s) }

// InTree reports whether a slash-separated repository path is in the //secret tree: the folder
// itself, or anything beneath it.
func InTree(p string) bool { return p == Dir || strings.HasPrefix(p, Dir+"/") }

// IsSecret reports whether a repository path is a secret's: a file in the tree that is neither an
// acl.yaml nor an OWNERS-config file. Its name is not checked (ValidPath does).
func IsSecret(p string) bool {
	return strings.HasPrefix(p, Dir+"/") && path.Base(p) != ACLFile && !owners.IsOwnersFile(p)
}

// IsOwners reports whether a repository path is an OWNERS-config file in the tree: the tree's
// approval list, as anywhere else in the repository, and no secret.
func IsOwners(p string) bool { return strings.HasPrefix(p, Dir+"/") && owners.IsOwnersFile(p) }

// IsACL reports whether a repository path is one of the tree's acl.yaml files.
func IsACL(p string) bool { return strings.HasPrefix(p, Dir+"/") && path.Base(p) == ACLFile }

// Folder is the folder a path in the tree is in: "secret/prod" for "secret/prod/db-password".
func Folder(p string) string { return path.Dir(p) }

// ValidPath reports whether p may be a secret's path: in the tree, every part below secret/ a
// valid name, and the last neither acl.yaml nor an OWNERS-config file's name.
func ValidPath(p string) bool {
	rest, ok := strings.CutPrefix(p, Dir+"/")
	if !ok || !IsSecret(p) {
		return false
	}
	for _, part := range strings.Split(rest, "/") {
		if !ValidName(part) {
			return false
		}
	}
	return true
}

// Ref is a secret's path as a stanza or a person writes it from the repository's root:
// //secret/prod/db-password.
func Ref(p string) string { return "//" + p }

// ParseRef reads a secret's path written from the repository's root (//secret/…), or as the
// repository path itself; ok is false for anything that is no secret's valid path.
func ParseRef(s string) (string, bool) {
	p := strings.TrimPrefix(s, "//")
	return p, ValidPath(p)
}

// grantRE is a deploy's or a post job's identity, as an acl.yaml's deploys and runs name them:
// the workflow file that declares it, then its phase and name (workflow.Job.ID, workflow.Deploy.ID)
// — //<dir>/.ok/workflow/<file>.yaml:deploy:<name>.
var grantRE = regexp.MustCompile(`^//((?:[A-Za-z0-9][A-Za-z0-9._-]*/)*)\.ok/workflow/[A-Za-z0-9][A-Za-z0-9._-]*\.yaml:(deploy|post):[a-z0-9][a-z0-9_.-]{0,63}$`)

// ValidDeploy reports whether id is a deploy's identity, as an acl.yaml's deploys name one.
func ValidDeploy(id string) bool { return validGrant(id, "deploy") }

// ValidRun reports whether id is a post job's identity, as an acl.yaml's runs name one.
func ValidRun(id string) bool { return validGrant(id, "post") }

func validGrant(id, phase string) bool {
	m := grantRE.FindStringSubmatch(id)
	if m == nil || m[2] != phase {
		return false
	}
	for _, part := range strings.Split(strings.TrimSuffix(m[1], "/"), "/") {
		if part == "." || part == ".." {
			return false
		}
	}
	return true
}

// Parent is the folder above a folder of the tree, or "" for the tree's root.
func Parent(folder string) string {
	if folder == Dir {
		return ""
	}
	return path.Dir(folder)
}
