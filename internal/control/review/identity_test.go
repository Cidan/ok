package review

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/identity"
)

const robot = "deploy.ok.robot.example.com"

func decl(names ...string) []byte {
	if len(names) == 0 {
		return []byte("members: []\n")
	}
	return []byte("members:\n  - " + strings.Join(names, "\n  - ") + "\n")
}

// tree is a tree of the root's ACL (carol), and team's ACL (bob) and group (alice), with
// edits applied: a path mapped to nil is removed.
func tree(t *testing.T, edits map[string][]byte) *identity.Tree {
	t.Helper()
	files := map[string][]byte{
		"acl.yaml":        decl(carol),
		"build.yaml":      nil,
		"team/acl.yaml":   decl(bob),
		"team/group.yaml": decl(alice),
	}
	for p, c := range edits {
		if c == nil {
			delete(files, p)
			continue
		}
		files[p] = c
	}
	tr, err := identity.LoadFiles(identity.MustNames("example.com"), files)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestEvaluateTree(t *testing.T) {
	before := tree(t, nil)
	change := tree(t, map[string][]byte{"team/group.yaml": decl(alice, carol)})

	// alice, the author, is in no owning ACL: the change needs team.acl's member, bob.
	reqs := evaluateTree("identity", before, change, nil, nil, alice)
	if len(reqs) != 1 {
		t.Fatalf("reqs %+v", reqs)
	}
	r := reqs[0]
	if r.Accepted || r.Implicit || r.ACL != "team.acl.example.com" || r.ACLPath != "team" ||
		r.What != "change the members of team.group.example.com" || !slices.Equal(r.Eligible, []string{bob}) {
		t.Errorf("unapproved: %+v", r)
	}
	st := &Status{Identity: reqs}
	if st.ApprovalSatisfied() || !strings.Contains(st.BlockedReason(), "Approval from a member of team.acl.example.com (change the members of team.group.example.com)") {
		t.Errorf("blocked: %q", st.BlockedReason())
	}
	if !st.CanApprove(context.Background(), bob, alice) || st.CanApprove(context.Background(), carol, alice) {
		t.Errorf("CanApprove: bob %v, carol %v", st.CanApprove(context.Background(), bob, alice), st.CanApprove(context.Background(), carol, alice))
	}

	// bob approves.
	reqs = evaluateTree("identity", before, change, nil, []grant{approval(bob), approval(carol), lgtm(carol)}, alice)
	if r := reqs[0]; !r.Accepted || r.Implicit || !slices.Equal(r.ApprovedBy, []string{bob}) {
		t.Errorf("bob's approval: %+v", r)
	}
	if st := (&Status{Identity: reqs}); !st.ApprovalSatisfied() {
		t.Errorf("approved: %q", st.BlockedReason())
	}

	// bob is the author: his membership approves it.
	if r := evaluateTree("identity", before, change, nil, nil, bob)[0]; !r.Accepted || !r.Implicit {
		t.Errorf("the author a member: %+v", r)
	}
}

func TestEvaluateTreeChains(t *testing.T) {
	before := tree(t, nil)
	// carol, an admin, adds herself to team.acl and changes team's group in one change.
	change := tree(t, map[string][]byte{"team/acl.yaml": decl(bob, carol), "team/group.yaml": decl(alice, carol)})
	reqs := evaluateTree("identity", before, change, nil, nil, carol)
	if len(reqs) != 2 {
		t.Fatalf("reqs %+v", reqs)
	}
	for _, r := range reqs {
		if !r.Accepted || !r.Implicit {
			t.Errorf("carol's self-grant chain: %+v", r)
		}
	}
	// alice cannot add herself to the root's ACL.
	reqs = evaluateTree("identity", before, tree(t, map[string][]byte{"acl.yaml": decl(carol, alice)}), nil, nil, alice)
	if reqs[0].Accepted {
		t.Errorf("alice escalated: %+v", reqs[0])
	}
}

func TestEvaluateTreeOtherFiles(t *testing.T) {
	before := tree(t, nil)
	// A change to the tree's build.yaml needs the root's ACL.
	reqs := evaluateTree("identity", before, before, []string{"identity/build.yaml"}, nil, bob)
	if len(reqs) != 1 {
		t.Fatalf("reqs %+v", reqs)
	}
	r := reqs[0]
	if r.Accepted || r.ACL != "admins.acl.example.com" || r.ACLPath != "" || r.What != "change identity/build.yaml" || !slices.Equal(r.Eligible, []string{carol}) {
		t.Errorf("other files: %+v", r)
	}
	if r := evaluateTree("identity", before, before, []string{"identity/build.yaml"}, []grant{approval(carol)}, bob)[0]; !r.Accepted {
		t.Errorf("carol's approval: %+v", r)
	}
}

func TestSplitTree(t *testing.T) {
	tr, rest := splitTree("identity", []string{"identity/acl.yaml", "identity/team/group.yaml", "identityx/a", "svc/identity/acl.yaml", "README"})
	if !slices.Equal(tr, []string{"identity/acl.yaml", "identity/team/group.yaml"}) || !slices.Equal(rest, []string{"identityx/a", "svc/identity/acl.yaml", "README"}) {
		t.Errorf("split %v / %v", tr, rest)
	}
}

// fakeDirectory is ok identity, for applies.
type fakeDirectory struct {
	principals map[string]bool
	retired    map[string]bool
	err        error
}

func (f fakeDirectory) IsPrincipal(_ context.Context, name string) (bool, error) {
	return f.principals[name], f.err
}

func (f fakeDirectory) RetiredSetNames(context.Context) (map[string]bool, error) {
	return f.retired, nil
}

func (f fakeDirectory) Sets(context.Context, []string) (map[string][]string, error) { return nil, nil }

func TestApplies(t *testing.T) {
	ctx := context.Background()
	dir := fakeDirectory{principals: map[string]bool{alice: true, bob: true, carol: true, robot: true}, retired: map[string]bool{"old.group.example.com": true}}
	before := tree(t, nil)
	if err := applies(ctx, dir, before, tree(t, map[string][]byte{"team/group.yaml": decl(alice, robot)})); err != nil {
		t.Errorf("a change ok identity applies: %v", err)
	}
	for name, tc := range map[string]struct {
		after map[string][]byte
		want  string
	}{
		"no principal":   {map[string][]byte{"team/group.yaml": decl(alice, "ghost.human.example.com")}, "ghost.human.example.com (in team.group.example.com) is no principal"},
		"a retired name": {map[string][]byte{"old/group.yaml": decl(alice)}, "old.group.example.com was deleted, and its name is retired"},
		"an owning ACL":  {map[string][]byte{"team/acl.yaml": nil}, "team.acl.example.com is deleted, but team.group.example.com would remain"},
	} {
		t.Run(name, func(t *testing.T) {
			err := applies(ctx, dir, before, tree(t, tc.after))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("applies: %v; want %q", err, tc.want)
			}
		})
	}
	down := fakeDirectory{err: errors.New("unavailable")}
	if err := applies(ctx, down, before, before); err == nil || !strings.Contains(err.Error(), "asking ok identity") {
		t.Errorf("ok identity down: %v", err)
	}
	st := &Status{IdentityErr: errors.New("ok identity would not apply it: x")}
	if st.ApprovalSatisfied() || !strings.Contains(st.BlockedReason(), "a change to the groups-and-ACLs tree that can apply (ok identity would not apply it: x)") {
		t.Errorf("an identity error: %q", st.BlockedReason())
	}
}
