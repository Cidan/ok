package identity

import (
	"fmt"
	"sort"
	"strings"
)

// Change is how one set differs between two trees.
type Change struct {
	Set  string // the set's full name
	Kind Kind
	// Created is a set only the later tree has; Deleted, one only the earlier has.
	Created, Deleted bool
	// Added and Removed are the members it gained and lost, sorted. A created set's members are
	// all added; a deleted set's are not listed.
	Added, Removed []string
	// FromOwner and FromPath are its owner and folder before; ToOwner and ToPath, after. Each is
	// "" where the set is absent (and FromPath and ToPath are "" for the root).
	FromOwner, ToOwner string
	FromPath, ToPath   string
}

// Moved reports whether a set in both trees is in another folder after.
func (c Change) Moved() bool { return !c.Created && !c.Deleted && c.FromPath != c.ToPath }

// Reowned reports whether a set in both trees has another owner after.
func (c Change) Reowned() bool { return !c.Created && !c.Deleted && c.FromOwner != c.ToOwner }

// String describes the change, as a plan's step.
func (c Change) String() string {
	switch {
	case c.Created:
		s := fmt.Sprintf("create %s (in %s), owned by %s", c.Set, displayPath(c.ToPath), c.ToOwner)
		if len(c.Added) == 0 {
			return s + ", with no members"
		}
		return s + ", with members " + strings.Join(c.Added, ", ")
	case c.Deleted:
		return fmt.Sprintf("delete %s (in %s, owned by %s); its name is retired", c.Set, displayPath(c.FromPath), c.FromOwner)
	}
	var parts []string
	if c.Moved() {
		parts = append(parts, fmt.Sprintf("move from %s to %s", displayPath(c.FromPath), displayPath(c.ToPath)))
	}
	if c.Reowned() {
		parts = append(parts, fmt.Sprintf("owner %s → %s", c.FromOwner, c.ToOwner))
	}
	if len(c.Added) > 0 {
		parts = append(parts, "add "+strings.Join(c.Added, ", "))
	}
	if len(c.Removed) > 0 {
		parts = append(parts, "remove "+strings.Join(c.Removed, ", "))
	}
	return c.Set + ": " + strings.Join(parts, "; ")
}

// Diff returns how after differs from before, set by set, ordered by full name. before may be
// nil: the tree's genesis, where every set is created. A reordering of members is no change.
func Diff(before, after *Tree) []Change {
	if before == nil {
		before = &Tree{naming: after.naming, sets: map[string]*Set{}, owner: map[string]string{}, synced: after.synced}
	}
	names := map[string]bool{}
	for n := range before.sets {
		names[n] = true
	}
	for n := range after.sets {
		names[n] = true
	}
	var out []Change
	for _, n := range sortedKeys(names) {
		b, inBefore := before.sets[n]
		a, inAfter := after.sets[n]
		c := Change{Set: n}
		switch {
		case !inBefore:
			c.Kind, c.Created = a.Kind, true
			c.Added = sortedCopy(a.Members)
			c.ToOwner, c.ToPath = after.owner[n], a.Path
		case !inAfter:
			c.Kind, c.Deleted = b.Kind, true
			c.FromOwner, c.FromPath = before.owner[n], b.Path
		default:
			c.Kind = a.Kind
			c.FromOwner, c.FromPath = before.owner[n], b.Path
			c.ToOwner, c.ToPath = after.owner[n], a.Path
			c.Added = minus(a.Members, b.Members)
			c.Removed = minus(b.Members, a.Members)
			if len(c.Added) == 0 && len(c.Removed) == 0 && !c.Moved() && !c.Reowned() {
				continue
			}
		}
		out = append(out, c)
	}
	return out
}

// Check validates the change from before to after beyond each tree's own validity: an ACL that
// owns anything is not deleted, since what remains in its folder or beneath it would silently
// fall to the next ACL up. before may be nil (genesis). The error is an *Error.
func Check(before, after *Tree) error {
	if before == nil {
		return nil
	}
	var problems []string
	for _, n := range before.names {
		s := before.sets[n]
		if s.Kind != ACL {
			continue
		}
		if _, ok := after.sets[n]; ok {
			continue
		}
		var left []string
		for _, m := range after.names {
			if under(after.sets[m].Path, s.Path) {
				left = append(left, m)
			}
		}
		if len(left) > 0 {
			problems = append(problems, fmt.Sprintf("%s is deleted, but %s would remain in its folder or beneath it and fall to the next ACL up: delete or move what it owns in the same change", n, strings.Join(left, ", ")))
		}
	}
	if len(problems) > 0 {
		return &Error{Problems: problems}
	}
	return nil
}

// Action is what a part of a change does to a set.
type Action string

const (
	// Create is a new set.
	Create Action = "create"
	// Delete is a set removed.
	Delete Action = "delete"
	// Members is a set's members changed.
	Members Action = "change the members of"
	// MoveOut is a set moved away from its owner (the old owner's half of a move).
	MoveOut Action = "move out"
	// MoveIn is a set moved to a new owner (the new owner's half of a move).
	MoveIn Action = "move in"
	// Handover is a set taken over, where it is, by a new ACL its owner owns.
	Handover Action = "hand over"
)

// Part is one approval a change needs: a member of the ACL By approves the Action on Set.
type Part struct {
	Set    string
	Action Action
	By     string
	// To is, for a Handover, the ACL that takes the set over.
	To string
	// Accepted reports whether an approver may approve the part; Approver is one who may.
	Accepted bool
	Approver string
	// Eligible are the principals whose approval accepts the part, given what the change's
	// accepted parts grant: By's members.
	Eligible []string
}

// What describes what the part does: "create eng.acl.example.com", "hand eng.group.example.com
// to eng.acl.example.com".
func (p Part) What() string {
	switch p.Action {
	case MoveOut:
		return "move " + p.Set + " out of " + p.By
	case MoveIn:
		return "move " + p.Set + " into " + p.By
	case Handover:
		return "hand " + p.Set + " to " + p.To
	}
	return string(p.Action) + " " + p.Set
}

func (p Part) String() string {
	s := p.What() + ": a member of " + p.By
	if p.Accepted {
		s += " (" + p.Approver + ")"
	}
	return s
}

// Decision is what approving a change takes.
type Decision struct {
	// Genesis reports that there was no tree before: the change that creates it is approved by
	// the repository's owners, and no part is accepted by membership.
	Genesis bool
	// Parts are the approvals the change needs, ordered by set and then as listed for each.
	Parts []Part
	// Approved reports whether every part is accepted. It is false for the genesis.
	Approved bool
}

// Refused returns the parts not accepted.
func (d *Decision) Refused() []Part {
	var out []Part
	for _, p := range d.Parts {
		if !p.Accepted {
			out = append(out, p)
		}
	}
	return out
}

// partFor is a part and the set whose members its acceptance makes count.
type partFor struct {
	Part
	grants string
}

// Authorize decides whether approvers — the principals that approved a change — may approve the
// change from before to after. Each part of the change needs a member of one ACL:
//
//   - creating a set, or changing its members: its owner after;
//   - deleting a set: its owner before;
//   - moving a set to another owner: its old owner, and its new owner;
//   - a new ACL taking a set over (the set did not move): its old owner, which owns the new ACL.
//
// The change is evaluated as a whole: starting from the memberships before, every part some
// approver may approve is accepted, the memberships accepted parts create (a created set's, and
// a set's new members) count from then on, and so on until nothing more is accepted. Removals
// take no authority away within the change. before is nil for the genesis.
func Authorize(before, after *Tree, approvers []string) *Decision {
	changes := Diff(before, after)
	var parts []partFor
	for _, c := range changes {
		switch {
		case c.Created:
			parts = append(parts, partFor{Part{Set: c.Set, Action: Create, By: c.ToOwner}, c.Set})
			continue
		case c.Deleted:
			parts = append(parts, partFor{Part{Set: c.Set, Action: Delete, By: c.FromOwner}, ""})
			continue
		}
		if c.Reowned() && c.Moved() {
			parts = append(parts,
				partFor{Part{Set: c.Set, Action: MoveOut, By: c.FromOwner}, ""},
				partFor{Part{Set: c.Set, Action: MoveIn, By: c.ToOwner}, ""})
		}
		if c.Reowned() && !c.Moved() {
			parts = append(parts, partFor{Part{Set: c.Set, Action: Handover, By: c.FromOwner, To: c.ToOwner}, ""})
		}
		if len(c.Added) > 0 || len(c.Removed) > 0 {
			parts = append(parts, partFor{Part{Set: c.Set, Action: Members, By: c.ToOwner}, c.Set})
		}
	}
	d := &Decision{Genesis: before == nil}
	if d.Genesis {
		for _, p := range parts {
			p.Eligible = after.Expand(p.By)
			d.Parts = append(d.Parts, p.Part)
		}
		return d
	}

	// Authority: each set's members before, and — once the part that sets them is accepted —
	// after.
	granted := map[string]bool{}
	members := func(n string) ([]string, bool) {
		var ms []string
		b, inBefore := before.sets[n]
		if inBefore {
			ms = append(ms, b.Members...)
		}
		a, inAfter := after.sets[n]
		if inAfter && granted[n] {
			ms = append(ms, a.Members...)
		}
		if !inBefore && !inAfter {
			// A synced group is the system of record's: the change does not alter it.
			if synced, ok := after.synced[n]; ok {
				return synced, true
			}
		}
		return ms, inBefore || (inAfter && granted[n])
	}
	sortedApprovers := sortedCopy(approvers)
	for progress := true; progress; {
		progress = false
		for i := range parts {
			p := &parts[i]
			if p.Accepted {
				continue
			}
			in := map[string]bool{}
			for _, m := range expand(members, p.By) {
				in[m] = true
			}
			for _, a := range sortedApprovers {
				if in[a] {
					p.Accepted, p.Approver = true, a
					if p.grants != "" {
						granted[p.grants] = true
					}
					progress = true
					break
				}
			}
		}
	}
	d.Approved = true
	for _, p := range parts {
		p.Eligible = expand(members, p.By)
		d.Parts = append(d.Parts, p.Part)
		if !p.Accepted {
			d.Approved = false
		}
	}
	return d
}

func sortedCopy(s []string) []string {
	out := append([]string{}, s...)
	sort.Strings(out)
	return out
}

// minus is the elements of a not in b, sorted.
func minus(a, b []string) []string {
	in := map[string]bool{}
	for _, x := range b {
		in[x] = true
	}
	var out []string
	for _, x := range a {
		if !in[x] {
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}
