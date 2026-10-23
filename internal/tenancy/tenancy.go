// Package tenancy is the vocabulary of ok's tenancy (docs/hosted.md, "Tenancy, as built"): the
// roles a principal holds on a repository, a repository's visibility, an organization's role
// within it, and the rules for handles and repository names, which the directory, Authorize, and
// the SCM share.
package tenancy

import (
	"fmt"
	"regexp"
	"strings"
)

// Role is a principal's role on a repository, each holding every one before it.
type Role int

const (
	None Role = iota
	Read
	Triage
	Write
	Maintain
	Admin
)

var roleNames = []string{"none", "read", "triage", "write", "maintain", "admin"}

func (r Role) String() string {
	if r < None || r > Admin {
		return fmt.Sprintf("Role(%d)", int(r))
	}
	return roleNames[r]
}

// ParseRole is the role a name names.
func ParseRole(s string) (Role, error) {
	for i, n := range roleNames {
		if n == s {
			return Role(i), nil
		}
	}
	return None, fmt.Errorf("%q is no role (none, read, triage, write, maintain, or admin)", s)
}

// Visibility is who reads a repository without a grant.
type Visibility int

const (
	// Private: only those granted.
	Private Visibility = iota
	// Internal: every member of the owning organization's enterprise, or of the organization
	// when it has none.
	Internal
	// Public: anyone, signed in or not.
	Public
)

var visibilityNames = []string{"private", "internal", "public"}

func (v Visibility) String() string {
	if v < Private || v > Public {
		return fmt.Sprintf("Visibility(%d)", int(v))
	}
	return visibilityNames[v]
}

// ParseVisibility is the visibility a name names.
func ParseVisibility(s string) (Visibility, error) {
	for i, n := range visibilityNames {
		if n == s {
			return Visibility(i), nil
		}
	}
	return Private, fmt.Errorf("%q is no visibility (private, internal, or public)", s)
}

// Membership is a principal's place in an organization.
type Membership int

const (
	// NotMember: none.
	NotMember Membership = iota
	// Member: the organization's base role on its repositories, and its teams' grants.
	Member
	// Owner: admin of every repository the organization owns, and of the organization.
	Owner
)

var membershipNames = []string{"none", "member", "owner"}

func (m Membership) String() string {
	if m < NotMember || m > Owner {
		return fmt.Sprintf("Membership(%d)", int(m))
	}
	return membershipNames[m]
}

// ParseMembership is the membership a name names.
func ParseMembership(s string) (Membership, error) {
	for i, n := range membershipNames {
		if n == s && i != int(NotMember) {
			return Membership(i), nil
		}
	}
	return NotMember, fmt.Errorf("%q is no membership (member, or owner)", s)
}

// handle is 1–39 lowercase letters, digits, and inner single hyphens.
var handle = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9]|-[a-z0-9]){0,38}$`)

// reserved are handles and repository names ok's own pages and APIs use at the top of its URL
// space (/<owner>/<repo>, and an installation's /<repo>).
var reserved = map[string]bool{
	"about": true, "account": true, "admin": true, "api": true, "apps": true, "assets": true, "dashboard": true,
	"explore": true, "favicon.svg": true, "healthz": true, "help": true, "id": true, "login": true, "logout": true,
	"new": true, "notifications": true, "ok": true, "orgs": true, "readyz": true, "repositories": true,
	"search": true, "services": true, "settings": true, "signup": true, "static": true, "users": true,
}

// ValidHandle refuses what no person or organization may be called: anything but 1–39 lowercase
// letters, digits, and inner hyphens, never two at once, or a reserved word.
func ValidHandle(h string) error {
	if !handle.MatchString(h) {
		return fmt.Errorf("handle %q: use 1-39 lowercase letters, digits, and hyphens, neither first, last, nor two in a row", h)
	}
	if reserved[h] {
		return fmt.Errorf("handle %q is reserved for ok's own pages", h)
	}
	return nil
}

// repositoryName is 1–64 lowercase letters, digits, '.', '_', and '-', starting and ending with a
// letter or digit.
var repositoryName = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,62}[a-z0-9])?$`)

// ValidRepositoryName refuses what no repository may be named.
func ValidRepositoryName(name string) error {
	if !repositoryName.MatchString(name) {
		return fmt.Errorf("repository name %q: use 1-64 lowercase letters, digits, '.', '_', '-', starting and ending with a letter or digit", name)
	}
	if reserved[name] {
		return fmt.Errorf("repository name %q is reserved for ok's own pages", name)
	}
	return nil
}

// ParseFullName is the owner's handle and the name a repository's full name holds: acme/app is
// acme's app; app alone is the installation's organization's (owner ""). It refuses what no full
// name looks like, not names a reserved word: a repository made before they were reserved keeps its.
func ParseFullName(full string) (owner, name string, err error) {
	owner, name, slash := strings.Cut(full, "/")
	if !slash {
		owner, name = "", full
	} else if !handle.MatchString(owner) {
		return "", "", fmt.Errorf("%q is no repository's full name: %q is no handle", full, owner)
	}
	if !repositoryName.MatchString(name) {
		return "", "", fmt.Errorf("%q is no repository's full name", full)
	}
	return owner, name, nil
}

// FullName is a repository's full name, as ok shows it: its name alone for the installation's
// organization's (installation true), else its owner's handle and its name.
func FullName(owner, name string, installation bool) string {
	if installation {
		return name
	}
	return owner + "/" + name
}

// URLPath is a repository's path on ok's site, /owner/name, from its full name: a name alone is the
// installation's organization's, whose handle is installation.
func URLPath(full, installation string) string {
	if strings.Contains(full, "/") || installation == "" {
		return "/" + full
	}
	return "/" + installation + "/" + full
}
