// Package authz decides what a caller may do to a repository (docs/hosted.md, "Tenancy, as built",
// "Roles and actions"): Decide is a pure function of the facts — the repository, its owner, and the
// caller's place — which ok identity gathers and each role keeps a few seconds (Checker).
package authz

import (
	"fmt"
	"slices"

	"github.com/Cidan/ok/internal/tenancy"
)

// Action is what a caller does to a repository.
type Action int

const (
	// ReadCode reads its code, CLs, runs, logs, and deployments, and clones it.
	ReadCode Action = iota + 1
	// Fork makes a fork of it.
	Fork
	// Propose uploads a CL, and reviews and comments on CLs.
	Propose
	// Triage sets labels and reviewers, and closes and reopens CLs.
	Triage
	// Land submits a CL to land, reruns and cancels checks, and acts on deployments.
	Land
	// Maintain changes its settings, but its visibility, name, and grants.
	Maintain
	// Administer changes its visibility, name, owner, and grants, and deletes it.
	Administer
	// Restore restores it once deleted.
	Restore
)

var actionNames = map[Action]string{ReadCode: "read", Fork: "fork", Propose: "propose", Triage: "triage", Land: "land",
	Maintain: "maintain", Administer: "administer", Restore: "restore"}

func (a Action) String() string {
	if n, ok := actionNames[a]; ok {
		return n
	}
	return fmt.Sprintf("Action(%d)", int(a))
}

// Actions are every action, in order.
var Actions = []Action{ReadCode, Fork, Propose, Triage, Land, Maintain, Administer, Restore}

// State is where a repository stands.
type State int

const (
	// Active: it stands.
	Active State = iota
	// Deleted: soft-deleted; only its admins see it, to restore it.
	Deleted
)

// Repository is what decides about the repository itself.
type Repository struct {
	ID         int64
	Visibility tenancy.Visibility
	Propose    tenancy.Role // Read or Write
	State      State
	// Owner is the owning person's or organization's ID; OwnedByOrg says which.
	Owner      string
	OwnedByOrg bool
	// Org is the owning organization's settings, when OwnedByOrg.
	Org Org
	// Cell is the cell holding it (docs/hosted.md, "Cells, as built").
	Cell string
}

// Org is what of the owning organization decides.
type Org struct {
	BaseRole     tenancy.Role
	PrivateForks bool
}

// Caller is who asks, and their place in the repository's world.
type Caller struct {
	// ID is the caller's stable ID; "" for no one signed in.
	ID string
	// Active is false for a suspended or deleted caller, who may do nothing.
	Active bool
	// Membership is the caller's in the owning organization.
	Membership tenancy.Membership
	// Granted are the roles granted the caller on the repository: directly, and through every set
	// — team, group, ACL — the caller is in.
	Granted []tenancy.Role
	// Internal: the caller reads the repository's internal visibility — a member of the owning
	// organization's enterprise, or of the organization when it has none.
	Internal bool
	// Token narrows the caller, when the request came with one; nil for none.
	Token *Token
}

// Token is what a token lets its holder do: at most Role, on the repositories it names (none: every
// repository its holder may reach) — and, when Owner is set, only on that account's repositories:
// an app's installation token reaches the installing organization's alone, wherever else the app
// is installed.
type Token struct {
	Role         tenancy.Role
	Repositories []int64
	Owner        string
}

// Facts are everything Decide reads.
type Facts struct {
	Repository Repository
	Caller     Caller
}

// Decision is what Decide decides.
type Decision struct {
	// Allowed: the caller may do the action.
	Allowed bool
	// Visible: the caller sees the repository at all. A refusal of an action on a repository the
	// caller cannot see answers as though it did not exist.
	Visible bool
	// Role is the caller's role on the repository, a token's narrowing applied.
	Role tenancy.Role
	// Why says why not, when not allowed.
	Why string
	// Cell is the cell holding the repository, when it is visible: a role refuses what another
	// cell holds.
	Cell string
}

// RoleOf is the caller's role on the repository: the highest of what owning it, belonging to its
// organization, the roles granted, and its visibility give — none for one who is not active — and,
// with a token, no more than the token allows, and none on a repository it does not name.
func RoleOf(f Facts) tenancy.Role {
	c, r := f.Caller, f.Repository
	if c.ID != "" && !c.Active {
		return tenancy.None
	}
	role := tenancy.None
	raise := func(to tenancy.Role) {
		if to > role {
			role = to
		}
	}
	if c.ID != "" {
		switch {
		case !r.OwnedByOrg && c.ID == r.Owner:
			raise(tenancy.Admin)
		case r.OwnedByOrg && c.Membership == tenancy.Owner:
			raise(tenancy.Admin)
		case r.OwnedByOrg && c.Membership == tenancy.Member:
			raise(r.Org.BaseRole)
		}
		for _, g := range c.Granted {
			raise(g)
		}
		if r.Visibility == tenancy.Internal && c.Internal {
			raise(tenancy.Read)
		}
	}
	if r.Visibility == tenancy.Public {
		raise(tenancy.Read)
	}
	if t := c.Token; t != nil {
		if len(t.Repositories) > 0 && !slices.Contains(t.Repositories, r.ID) || t.Owner != "" && t.Owner != r.Owner {
			return tenancy.None
		}
		if role > t.Role {
			role = t.Role
		}
	}
	return role
}

// Needs is the role an action needs on a repository.
func Needs(a Action, r Repository) tenancy.Role {
	switch a {
	case ReadCode, Fork:
		return tenancy.Read
	case Propose:
		if r.Propose == tenancy.Write {
			return tenancy.Write
		}
		return tenancy.Read
	case Triage:
		return tenancy.Triage
	case Land:
		return tenancy.Write
	case Maintain:
		return tenancy.Maintain
	}
	return tenancy.Admin
}

// Decide decides whether the caller may do an action to the repository, naming its cell when the
// caller sees it.
func Decide(f Facts, a Action) Decision {
	d := decide(f, a)
	if d.Visible {
		d.Cell = f.Repository.Cell
	}
	return d
}

func decide(f Facts, a Action) Decision {
	role := RoleOf(f)
	d := Decision{Role: role, Visible: role >= tenancy.Read}
	r := f.Repository
	if r.State == Deleted {
		// A deleted repository is its admins' alone, to restore.
		d.Visible = role == tenancy.Admin
		if a != Restore {
			d.Why = "the repository is deleted"
			return d
		}
	} else if a == Restore {
		d.Why = "the repository is not deleted"
		return d
	}
	if f.Caller.ID == "" && a != ReadCode {
		// No one signed in reads a public repository, and does nothing else.
		d.Why = fmt.Sprintf("to %s, sign in", a)
		return d
	}
	need := Needs(a, r)
	if role < need {
		d.Why = fmt.Sprintf("to %s needs %s, and the caller is %s", a, need, role)
		return d
	}
	if a == Fork && r.Visibility == tenancy.Private && r.OwnedByOrg && !r.Org.PrivateForks {
		d.Why = "the organization does not let its private repositories be forked"
		return d
	}
	d.Allowed = true
	return d
}
