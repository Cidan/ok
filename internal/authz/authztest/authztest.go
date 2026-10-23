// Package authztest is the permission matrix (docs/hosted.md, "Tenancy, as built", "Done when"):
// every kind of caller of an organization's repository, the role each has at each visibility, and
// what each action needs, written out apart from authz's rules, so that authz and each role's API
// are held to one table.
package authztest

import (
	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/tenancy"
)

// The matrix's repository: RepositoryID, owned by the organization Owner, whose members read, and
// on which proposing needs read.
const (
	RepositoryID = 7
	Owner        = "prn_acme"
)

// Repository is the matrix's repository at a visibility.
func Repository(v tenancy.Visibility) authz.Repository {
	return authz.Repository{ID: RepositoryID, Visibility: v, Propose: tenancy.Read, Owner: Owner, OwnedByOrg: true,
		Org: authz.Org{BaseRole: tenancy.Read}}
}

// Caller is a kind of caller: the facts ok identity answers for it (and the token it presents), and
// its role on the matrix's repository at each visibility — private, internal, public.
type Caller struct {
	Name   string
	Caller authz.Caller
	Roles  [3]tenancy.Role
}

const (
	none     = tenancy.None
	read     = tenancy.Read
	triage   = tenancy.Triage
	write    = tenancy.Write
	maintain = tenancy.Maintain
	admin    = tenancy.Admin
)

// Callers are every kind of caller, each with an ID of its own. ok's own roles are not among them:
// each role's API lets them through before Authorize decides anything.
var Callers = []Caller{
	{"anonymous", authz.Caller{}, [3]tenancy.Role{none, none, read}},
	{"a stranger", authz.Caller{ID: "prn_stranger", Active: true}, [3]tenancy.Role{none, none, read}},
	{"a suspended owner", authz.Caller{ID: "prn_suspended", Membership: tenancy.Owner}, [3]tenancy.Role{none, none, none}},
	{"a member", authz.Caller{ID: "prn_member", Active: true, Membership: tenancy.Member}, [3]tenancy.Role{read, read, read}},
	{"a team's member", authz.Caller{ID: "prn_team", Active: true, Membership: tenancy.Member, Granted: []tenancy.Role{maintain}},
		[3]tenancy.Role{maintain, maintain, maintain}},
	{"an outside collaborator", authz.Caller{ID: "prn_collaborator", Active: true, Granted: []tenancy.Role{triage}},
		[3]tenancy.Role{triage, triage, triage}},
	{"a collaborator granted twice", authz.Caller{ID: "prn_twice", Active: true, Granted: []tenancy.Role{read, write}},
		[3]tenancy.Role{write, write, write}},
	{"the enterprise's", authz.Caller{ID: "prn_enterprise", Active: true, Internal: true}, [3]tenancy.Role{none, read, read}},
	{"an owner", authz.Caller{ID: "prn_owner", Active: true, Membership: tenancy.Owner}, [3]tenancy.Role{admin, admin, admin}},
	{"an owner's read token", authz.Caller{ID: "prn_owner_read", Active: true, Membership: tenancy.Owner,
		Token: &authz.Token{Role: read, Repositories: []int64{RepositoryID}}}, [3]tenancy.Role{read, read, read}},
	{"an owner's token for every repository", authz.Caller{ID: "prn_owner_every", Active: true, Membership: tenancy.Owner,
		Token: &authz.Token{Role: write}}, [3]tenancy.Role{write, write, write}},
	{"a token for another repository", authz.Caller{ID: "prn_owner_other", Active: true, Membership: tenancy.Owner,
		Token: &authz.Token{Role: admin, Repositories: []int64{RepositoryID + 1}}}, [3]tenancy.Role{none, none, none}},
	{"an app installed with write, by its installation token", authz.Caller{ID: "prn_app", Active: true, Granted: []tenancy.Role{write},
		Token: &authz.Token{Role: write, Owner: Owner}}, [3]tenancy.Role{write, write, write}},
	{"an app's installation token for another organization", authz.Caller{ID: "prn_app_elsewhere", Active: true, Granted: []tenancy.Role{write},
		Token: &authz.Token{Role: write, Owner: "prn_globex"}}, [3]tenancy.Role{none, none, none}},
	{"an app not installed here, by its installation token", authz.Caller{ID: "prn_app_absent", Active: true,
		Token: &authz.Token{Role: write, Owner: Owner}}, [3]tenancy.Role{none, none, read}},
}

// Needs are what each action needs on a repository standing, its proposing needing read.
var Needs = map[authz.Action]tenancy.Role{
	authz.ReadCode: read, authz.Fork: read, authz.Propose: read, authz.Triage: triage, authz.Land: write,
	authz.Maintain: maintain, authz.Administer: admin,
}

// Allowed is whether a kind of caller may do an action to the matrix's repository, standing, at a
// visibility: what it needs, but no one signed in does anything but read, no one restores what
// stands, and the organization lets no private repository be forked.
func Allowed(c Caller, v tenancy.Visibility, a authz.Action) bool {
	switch {
	case a == authz.Restore:
		return false
	case c.Caller.ID == "" && a != authz.ReadCode:
		return false
	case a == authz.Fork && v == tenancy.Private:
		return false
	}
	return c.Roles[v] >= Needs[a]
}

// Visible is whether a kind of caller sees the matrix's repository at a visibility.
func Visible(c Caller, v tenancy.Visibility) bool { return c.Roles[v] >= read }
