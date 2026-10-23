package review

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/owners"
)

const (
	alice = "alice.human.example.com" // the CL author
	bob   = "bob.human.example.com"
	carol = "carol.human.example.com"
)

func users(names ...string) []owners.Owner {
	out := make([]owners.Owner, 0, len(names))
	for _, n := range names {
		if n == "*" {
			out = append(out, owners.Owner{Kind: owners.Anyone})
		} else {
			out = append(out, owners.Owner{Kind: owners.User, Name: n})
		}
	}
	return out
}

func set(config string, allowSelf bool, names ...string) *owners.Set {
	return &owners.Set{File: config, Owners: users(names...), AllowSelf: allowSelf, Files: []string{config + ".file"}}
}

// named are who the tests' OWNERS handles name: each person's ID, which is their name here (no
// resolver is configured).
var named = owned{users: map[string]string{"alice": alice, "bob": bob, "carol": carol}}

// withACLs are named, and ACLs' members.
func withACLs(acls map[string]map[string]bool) owned { return owned{acls: acls, users: named.users} }

func lgtm(p string) grant     { return grant{label: db.LabelLGTM, principal: p} }
func approval(p string) grant { return grant{label: db.LabelApproval, principal: p} }

func TestEvaluate(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sets       []*owners.Set
		ownersErr  error
		grants     []grant
		unresolved int64
		wantLGTM   bool
		wantSelf   bool   // SelfLGTMAllowed
		wantSets   []bool // per-set Satisfied
		wantOK     bool   // Satisfied()
	}{
		{
			name: "no grants", sets: []*owners.Set{set("a/OWNERS", false, "bob")},
			wantSets: []bool{false},
		},
		{
			name: "LGTM + owner approval", sets: []*owners.Set{set("a/OWNERS", false, "bob")},
			grants: []grant{lgtm(carol), approval(bob)}, wantLGTM: true, wantSets: []bool{true}, wantOK: true,
		},
		{
			name: "author LGTM does not count", sets: []*owners.Set{set("a/OWNERS", false, "bob")},
			grants: []grant{lgtm(alice), approval(bob)}, wantSets: []bool{true},
		},
		{
			name:   "author LGTM counts when every OWNERS allows self",
			sets:   []*owners.Set{set("a/OWNERS", true, "bob"), set("b/OWNERS", true, "carol")},
			grants: []grant{lgtm(alice), approval(bob), approval(carol)}, wantLGTM: true, wantSelf: true,
			wantSets: []bool{true, true}, wantOK: true,
		},
		{
			name:   "one OWNERS without allowself blocks author LGTM",
			sets:   []*owners.Set{set("a/OWNERS", true, "bob"), set("b/OWNERS", false, "carol")},
			grants: []grant{lgtm(alice)}, wantSets: []bool{false, false},
		},
		{
			name: "no OWNERS anywhere lets the author LGTM and approve", sets: []*owners.Set{set("", false, "*")},
			grants: []grant{lgtm(alice), approval(alice)}, wantLGTM: true, wantSelf: true, wantSets: []bool{true}, wantOK: true,
		},
		{
			name:   "an OWNERS-governed set without allowself blocks self LGTM even beside ungoverned files",
			sets:   []*owners.Set{set("", false, "*"), set("a/OWNERS", false, "bob")},
			grants: []grant{lgtm(alice), approval(alice)}, wantSets: []bool{true, false},
		},
		{
			name: "non-owner approval does not count", sets: []*owners.Set{set("a/OWNERS", false, "bob")},
			grants: []grant{lgtm(bob), approval(carol)}, wantLGTM: true, wantSets: []bool{false},
		},
		{
			name: "listed author satisfies implicitly", sets: []*owners.Set{set("a/OWNERS", false, "alice", "bob")},
			grants: []grant{lgtm(bob)}, wantLGTM: true, wantSets: []bool{true}, wantOK: true,
		},
		{
			name: "anyone approves under *, but not the author", sets: []*owners.Set{set("a/OWNERS", false, "*")},
			grants: []grant{approval(alice)}, wantSets: []bool{false},
		},
		{
			name: "* approved by another user", sets: []*owners.Set{set("a/OWNERS", false, "*")},
			grants: []grant{lgtm(bob), approval(carol)}, wantLGTM: true, wantSets: []bool{true}, wantOK: true,
		},
		{
			name: "allowself lets the author approve under *", sets: []*owners.Set{set("a/OWNERS", true, "*")},
			grants: []grant{lgtm(alice), approval(alice)}, wantLGTM: true, wantSelf: true, wantSets: []bool{true}, wantOK: true,
		},
		{
			name:   "one approver covers several sets",
			sets:   []*owners.Set{set("a/OWNERS", false, "bob"), set("b/OWNERS", false, "bob", "carol")},
			grants: []grant{lgtm(carol), approval(bob)}, wantLGTM: true, wantSets: []bool{true, true}, wantOK: true,
		},
		{
			name:   "each set needs its own owner",
			sets:   []*owners.Set{set("a/OWNERS", false, "bob"), set("b/OWNERS", false, "carol")},
			grants: []grant{lgtm(carol), approval(bob)}, wantLGTM: true, wantSets: []bool{true, false},
		},
		{
			name: "no eligible owners is unsatisfiable", sets: []*owners.Set{set("a/OWNERS", false)},
			grants: []grant{lgtm(bob), approval(bob)}, wantLGTM: true, wantSets: []bool{false},
		},
		{
			name: "unresolvable OWNERS fails closed", ownersErr: errors.New("a/OWNERS:3: invalid line"),
			grants: []grant{lgtm(bob), approval(bob)}, wantLGTM: true,
		},
		{
			name: "no changed files is ungoverned: self LGTM allowed", grants: []grant{lgtm(alice)}, wantLGTM: true, wantSelf: true, wantOK: true,
		},
		{
			name: "unresolvable OWNERS never allows self LGTM", ownersErr: errors.New("a/OWNERS:3: invalid line"),
			grants: []grant{lgtm(alice)},
		},
		{
			name: "an unresolved comment blocks an otherwise approved CL", sets: []*owners.Set{set("a/OWNERS", false, "bob")},
			grants: []grant{lgtm(carol), approval(bob)}, unresolved: 1, wantLGTM: true, wantSets: []bool{true},
		},
		{
			name: "unresolved comments block even a self-approved ungoverned CL", sets: []*owners.Set{set("", false, "*")},
			grants: []grant{lgtm(alice), approval(alice)}, unresolved: 3, wantLGTM: true, wantSelf: true, wantSets: []bool{true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := evaluate(tc.sets, tc.ownersErr, tc.grants, alice, tc.unresolved, false, named)
			if st.LGTMSatisfied != tc.wantLGTM {
				t.Errorf("LGTMSatisfied = %v, want %v", st.LGTMSatisfied, tc.wantLGTM)
			}
			if st.SelfLGTMAllowed != tc.wantSelf {
				t.Errorf("SelfLGTMAllowed = %v, want %v", st.SelfLGTMAllowed, tc.wantSelf)
			}
			got := make([]bool, 0, len(st.Owners))
			for _, o := range st.Owners {
				got = append(got, o.Satisfied)
			}
			if !slices.Equal(got, tc.wantSets) && (len(got) > 0 || len(tc.wantSets) > 0) {
				t.Errorf("owner sets satisfied = %v, want %v", got, tc.wantSets)
			}
			if st.Satisfied() != tc.wantOK {
				t.Errorf("Satisfied() = %v, want %v (reason %q)", st.Satisfied(), tc.wantOK, st.BlockedReason())
			}
			if tc.wantOK != (st.BlockedReason() == "") {
				t.Errorf("BlockedReason() = %q inconsistent with Satisfied() = %v", st.BlockedReason(), tc.wantOK)
			}
		})
	}
}

func TestCanApprove(t *testing.T) {
	st := evaluate([]*owners.Set{set("a/OWNERS", false, "bob"), set("b/OWNERS", false, "*")}, nil, nil, alice, 0, false, named)
	if !st.CanApprove(context.Background(), bob, alice) || !st.CanApprove(context.Background(), carol, alice) {
		t.Error("bob (owner) and carol (under *) should be able to approve")
	}
	if st.CanApprove(context.Background(), alice, alice) {
		t.Error("the author must not approve OWNERS-governed files without allowself")
	}
	if !evaluate([]*owners.Set{set("", false, "*")}, nil, nil, alice, 0, false, named).CanApprove(context.Background(), alice, alice) {
		t.Error("the author may approve files no OWNERS governs")
	}
}

func TestBlockedReasonNamesOwners(t *testing.T) {
	st := evaluate([]*owners.Set{set("a/OWNERS", false, "bob"), set("", false, "*")}, nil, []grant{lgtm(bob)}, alice, 2, false, named)
	want := "Needs Approval from //a/OWNERS, Approval from anyone (no OWNERS), no unresolved comments (2 unresolved)"
	if got := st.BlockedReason(); got != want {
		t.Errorf("BlockedReason() = %q, want %q", got, want)
	}
}

func TestOwnersNameHumansByHandle(t *testing.T) {
	st := evaluate([]*owners.Set{set("a/OWNERS", false, "bob")}, nil, []grant{approval("bob.robot.example.com")}, alice, 0, false, named)
	if st.Owners[0].Satisfied {
		t.Error("a robot whose name starts like an owner's handle counts as the owner")
	}
}

// A rollback's review is expedited: with no file edited by hand there is no owner set, so an
// LGTM from anyone but its author and no unresolved comments are all it needs — and its
// author's LGTM never counts, even where every OWNERS allows self.
func TestEvaluateRollback(t *testing.T) {
	st := evaluate(nil, nil, []grant{lgtm(alice)}, alice, 0, true, named)
	if !st.Rollback || st.SelfLGTMAllowed || st.LGTMSatisfied || !st.ApprovalSatisfied() || st.Satisfied() {
		t.Errorf("the author's own LGTM on a rollback: %+v", st)
	}
	if got := st.BlockedReason(); got != "Needs LGTM" {
		t.Errorf("blocked: %q", got)
	}
	st = evaluate(nil, nil, []grant{lgtm(bob)}, alice, 0, true, named)
	if !st.Satisfied() {
		t.Errorf("a rollback with another's LGTM: %+v", st)
	}
	if evaluate(nil, nil, []grant{lgtm(bob)}, alice, 1, true, named).Satisfied() {
		t.Error("a rollback with an unresolved comment is satisfied")
	}
	// A file edited by hand needs its owners' Approval, and self never counts for LGTM.
	st = evaluate([]*owners.Set{set("a/OWNERS", true, "alice", "carol")}, nil, []grant{lgtm(bob)}, alice, 0, true, named)
	if st.SelfLGTMAllowed || !st.Satisfied() || !st.Owners[0].Implicit {
		t.Errorf("a hand edit its author owns: %+v", st)
	}
	st = evaluate([]*owners.Set{set("a/OWNERS", false, "carol")}, nil, []grant{lgtm(bob)}, alice, 0, true, named)
	if st.Satisfied() || st.BlockedReason() != "Needs Approval from //a/OWNERS" {
		t.Errorf("a hand edit without its owners' Approval: %q", st.BlockedReason())
	}
}

func TestEvaluateACLOwners(t *testing.T) {
	sets := []*owners.Set{{File: "auth/OWNERS", Owners: []owners.Owner{{Kind: owners.ACL, Name: "admins"}}, Files: []string{"auth/x.yaml"}}}
	acls := withACLs(map[string]map[string]bool{"admins.acl.example.com": {bob: true, alice: true}})
	// carol, no member, approves: it does not count.
	st := evaluate(sets, nil, []grant{lgtm(carol), approval(carol)}, alice, 0, false, withACLs(map[string]map[string]bool{"admins.acl.example.com": {bob: true}}))
	if st.ApprovalSatisfied() || st.CanApprove(context.Background(), carol, alice) || !st.CanApprove(context.Background(), bob, alice) {
		t.Errorf("carol's approval: %+v", st.Owners)
	}
	// bob, a member, approves.
	st = evaluate(sets, nil, []grant{lgtm(alice), approval(bob)}, carol, 0, false, acls)
	if !st.Satisfied() || st.Owners[0].ApprovedBy[0] != bob {
		t.Errorf("bob's approval: %+v", st.Owners)
	}
	// alice, the author, is a member: implicit.
	st = evaluate(sets, nil, []grant{lgtm(carol)}, alice, 0, false, acls)
	if !st.Owners[0].Implicit || !st.Satisfied() {
		t.Errorf("the author a member: %+v", st.Owners)
	}
	// An ACL not resolved matches nobody.
	if st := evaluate(sets, nil, []grant{lgtm(alice), approval(bob)}, carol, 0, false, named); st.ApprovalSatisfied() {
		t.Error("an unresolved ACL matched")
	}
	if ACLName(owners.Owner{Kind: owners.ACL, Name: "eng"}) != "eng.acl.example.com" {
		t.Error("ACLName")
	}
}
