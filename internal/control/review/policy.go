// Package review is ok control's review process (docs/changelists.md): the
// approval evaluation over OWNERS (docs/owners.md), reviewer suggestions, comment
// threads and their porting across patchsets, and the actions people take on a CL —
// lifecycle transitions and submit through the SCM, and reviewers, grants, and comments
// in ok control's own database.
package review

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/owners"
)

// OwnerReq is one owner set's approval requirement on a CL: the governing OWNERS (and
// matched per-file rules), the changed files it governs, and who satisfies it.
type OwnerReq struct {
	Set        *owners.Set
	Satisfied  bool
	Implicit   bool     // the author is a listed owner, which satisfies the set by itself
	ApprovedBy []string // reviewers whose Approval grant counts for this set
}

// Status is a CL's review evaluation against its current patchset — Critique's scoring: an
// LGTM, every owner set's Approval, and no unresolved comments.
type Status struct {
	LGTMBy          []string // reviewers whose LGTM grant counts, by stable ID
	LGTMSatisfied   bool
	SelfLGTMAllowed bool // every governing OWNERS allows self, so the author's own LGTM counts
	Owners          []OwnerReq
	OwnersErr       error // OWNERS could not be resolved; Approval fails closed
	Unresolved      int64 // unresolved review threads, from every patchset
	// Rollback: the CL is a rollback, reviewed expedited — Owners govern only the files its
	// author edited by hand, and its author's own LGTM never counts.
	Rollback bool
	// Identity are the approvals the CL's change to the groups-and-ACLs tree needs, decided by the ACLs that
	// own what it changes instead of OWNERS (docs/identity.md §9); IdentityErr is why they
	// cannot be: the change is no valid tree, or ok identity would not apply it. Approval fails
	// closed on it.
	Identity    []IdentityReq
	IdentityErr error
	// owned are who the owner sets name.
	owned owned
}

// owned are who owner sets name, by stable ID: each ACL's members, by the ACL's full name, and the
// person each user: line's handle names now.
type owned struct {
	acls  map[string]map[string]bool
	users map[string]string // handle -> the person's ID; absent: no such person
}

// ACLName is the full name of the ACL an acl: owner names: acl: admins is admins.acl.<domain>.
func ACLName(o owners.Owner) string {
	if name, ok := people.Names().ParseSet(o.Name); ok {
		return name
	}
	return people.Names().Of(identity.ACL, o.Name)
}

// ApprovalSatisfied reports whether every owner set is satisfied, and every part of a change to
// the groups-and-ACLs tree approved.
func (s *Status) ApprovalSatisfied() bool {
	if s.OwnersErr != nil || s.IdentityErr != nil {
		return false
	}
	for _, o := range s.Owners {
		if !o.Satisfied {
			return false
		}
	}
	for _, r := range s.Identity {
		if !r.Accepted {
			return false
		}
	}
	return true
}

// Satisfied reports whether the CL meets every review requirement: the submit gate, and
// the approved projection.
func (s *Status) Satisfied() bool {
	return s.LGTMSatisfied && s.ApprovalSatisfied() && s.Unresolved == 0
}

// BlockedReason names the unmet review requirements ("" when met).
func (s *Status) BlockedReason() string {
	var missing []string
	if !s.LGTMSatisfied {
		missing = append(missing, "LGTM")
	}
	if s.OwnersErr != nil {
		missing = append(missing, "valid OWNERS ("+s.OwnersErr.Error()+")")
	} else {
		for _, o := range s.Owners {
			if !o.Satisfied {
				missing = append(missing, "Approval from "+OwnersLabel(o.Set))
			}
		}
	}
	if s.IdentityErr != nil {
		missing = append(missing, "a change to the groups-and-ACLs tree that can apply ("+s.IdentityErr.Error()+")")
	}
	for _, r := range s.Identity {
		if !r.Accepted {
			missing = append(missing, "Approval from a member of "+r.ACL+" ("+r.What+")")
		}
	}
	if s.Unresolved > 0 {
		missing = append(missing, "no unresolved comments ("+strconv.FormatInt(s.Unresolved, 10)+" unresolved)")
	}
	if len(missing) == 0 {
		return ""
	}
	return "Needs " + strings.Join(missing, ", ")
}

// CanApprove reports whether an Approval grant by principal (a stable ID) would count toward any
// owner set, or any part of a change to the groups-and-ACLs tree.
func (s *Status) CanApprove(ctx context.Context, principal, author string) bool {
	for _, o := range s.Owners {
		if grantCounts(o.Set, principal, author, s.owned) {
			return true
		}
	}
	if len(s.Identity) == 0 {
		return false
	}
	name := people.Name(ctx, principal)
	for _, r := range s.Identity {
		if slices.Contains(r.Eligible, name) {
			return true
		}
	}
	return false
}

// OwnersLabel names an owner set for display: "//services/api/OWNERS", with matched
// per-file rules appended, or "anyone (no OWNERS)" when no OWNERS governs the files.
func OwnersLabel(s *owners.Set) string {
	if s.File == "" {
		return "anyone (no OWNERS)"
	}
	label := "//" + s.File
	if len(s.Rules) > 0 {
		label += " (" + strings.Join(s.Rules, ", ") + ")"
	}
	return label
}

// ownerMatches reports whether principal (a stable ID) is covered by owner o: a person by
// handle, or a member of an ACL (docs/identity.md §9).
func ownerMatches(o owners.Owner, principal string, ow owned) bool {
	switch o.Kind {
	case owners.Anyone:
		return true
	case owners.User:
		id, ok := ow.users[o.Name]
		return ok && id == principal
	case owners.ACL:
		return ow.acls[ACLName(o)][principal]
	default:
		return false
	}
}

// authorIsListed reports whether the author is named in the set — by a user: line, or as a member
// of an acl: line's ACL — the implicit-approval case. "*" does not make the author an implicit
// approver.
func authorIsListed(s *owners.Set, author string, ow owned) bool {
	return slices.ContainsFunc(s.Owners, func(o owners.Owner) bool {
		return (o.Kind == owners.User || o.Kind == owners.ACL) && ownerMatches(o, author, ow)
	})
}

// AllowsSelf reports whether the author's own grants count for set s: its OWNERS allow
// self, or no OWNERS governs the files at all — OWNERS are opt-in, so an ungoverned file
// is unrestricted.
func AllowsSelf(s *owners.Set) bool {
	return s.AllowSelf || s.File == ""
}

// grantCounts reports whether an Approval by principal satisfies set s: the principal
// must be covered by the set, and the author's own grant counts only where the set
// allows self.
func grantCounts(s *owners.Set, principal, author string, ow owned) bool {
	if principal == author && !AllowsSelf(s) {
		return false
	}
	return slices.ContainsFunc(s.Owners, func(o owners.Owner) bool { return ownerMatches(o, principal, ow) })
}

// grant is one currently granted label.
type grant struct {
	label     db.Label
	principal string // a stable ID
}

// evaluate applies the review policy to resolved owner sets and the current grants:
//   - LGTM counts from anyone but the author; the author's own LGTM counts only when
//     every owner set allows self — never when OWNERS are unresolved.
//   - Each owner set is satisfied implicitly when the author is a listed (user:) owner,
//     or by an Approval from someone the set covers — the author's own only where the
//     set allows self.
//   - Every review thread must be resolved.
//   - A rollback's sets are those of the files its author edited by hand (none, for the
//     inverse change the SCM computed), and its author's LGTM never counts.
func evaluate(sets []*owners.Set, ownersErr error, grants []grant, author string, unresolved int64, rollback bool, ow owned) *Status {
	st := &Status{OwnersErr: ownersErr, Unresolved: unresolved, Rollback: rollback, owned: ow}
	if ownersErr == nil {
		for _, s := range sets {
			st.Owners = append(st.Owners, OwnerReq{Set: s})
		}
		st.SelfLGTMAllowed = !rollback && !slices.ContainsFunc(sets, func(s *owners.Set) bool { return !AllowsSelf(s) })
	}
	for _, g := range grants {
		switch g.label {
		case db.LabelLGTM:
			if g.principal != author || st.SelfLGTMAllowed {
				st.LGTMBy = append(st.LGTMBy, g.principal)
			}
		case db.LabelApproval:
			for i := range st.Owners {
				if grantCounts(st.Owners[i].Set, g.principal, author, ow) {
					st.Owners[i].ApprovedBy = append(st.Owners[i].ApprovedBy, g.principal)
				}
			}
		}
	}
	st.LGTMSatisfied = len(st.LGTMBy) > 0
	for i := range st.Owners {
		o := &st.Owners[i]
		o.Implicit = authorIsListed(o.Set, author, ow)
		o.Satisfied = o.Implicit || len(o.ApprovedBy) > 0
	}
	return st
}

// grantsOf keeps the current grants of the given rows.
func grantsOf(rows []db.Grant) []grant {
	var out []grant
	for _, r := range rows {
		if r.Granted {
			out = append(out, grant{label: r.Label, principal: r.Principal})
		}
	}
	return out
}
