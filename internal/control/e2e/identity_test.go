package e2e

import (
	"net/url"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/control/db"
)

// In the repository that holds ok's groups-and-ACLs tree, once main has one, its files are approved
// by the ACLs that own what a change changes, not OWNERS (docs/identity.md): the author's
// membership counts, an approver's must be the owning ACL's, the root's ACL approves the tree's
// other files, and a change ok identity would not apply cannot be approved at all.
func TestIdentityApprovals(t *testing.T) {
	e := newEnv(t)
	e.post(alice, "/repositories/new", url.Values{"name": {"org"}})
	a, b, c := e.person(alice), e.person(bob), e.person(carol)
	a.jj("", "clone", "org")

	// The first tree: there is none on main, so OWNERS approve it (here, none: anyone).
	a.write("org/.ok/repository.yaml", "build: none\nidentity: identity\n")
	a.write("org/identity/acl.yaml", members(alice))
	a.write("org/identity/README.md", "The groups and ACLs.\n")
	a.write("org/identity/eng/acl.yaml", members(bob))
	a.write("org/identity/eng/group.yaml", members(carol))
	a.jj("org", "describe", "-m", "the tree")
	n := uploaded(a.jj("org", "cl", "--ready"))[0]
	if p := e.get(alice, clPath("org", n)); p.has("parts of the groups-and-ACLs tree's change") {
		t.Errorf("the first tree is approved by ownership: %s", excerpt(p.body))
	}
	e.post(alice, clPath("org", n)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("org", n)
	e.submitted(alice, clPath("org", n), "submit")

	// bob, eng.acl's member, changes eng's group: his own membership approves it.
	b.jj("", "clone", "org")
	b.write("org/identity/eng/group.yaml", members(carol, dana))
	b.jj("org", "describe", "-m", "dana joins eng")
	nb := uploaded(b.jj("org", "cl", "--ready"))[0]
	e.post(bob, clPath("org", nb)+"/reply", grant("LGTM"))
	e.waitFor("bob's change to be approved", func() bool { return e.hasEvent("org", nb, db.EventApproved) })
	if p := e.get(bob, clPath("org", nb)); !p.has("Satisfied — the author is a member") || !p.has("1 of 1 parts of the groups-and-ACLs tree's change") || p.has("owner sets") {
		t.Errorf("bob's change: %s", excerpt(p.body))
	}

	// carol, in no owning ACL, changes it too: eng.acl's member must approve.
	c.jj("", "clone", "org")
	c.write("org/identity/eng/group.yaml", members(carol, alice))
	c.jj("org", "describe", "-m", "alice joins eng")
	nc := uploaded(c.jj("org", "cl", "--ready"))[0]
	path := clPath("org", nc)
	e.post(carol, path+"/reply", grant("LGTM", "Approval"))
	p := e.get(carol, path)
	if got := submitTitle(p, "Submit"); !strings.Contains(got, "Approval from a member of eng.acl.example.com (change the members of eng.group.example.com)") {
		t.Errorf("carol's change: submit %q", got)
	}
	if !p.has("eng.acl.example.com") || !p.has("/org/tree/main/identity/eng/acl.yaml") || !p.has("0 of 1 parts of the groups-and-ACLs tree's change") {
		t.Errorf("who can approve carol's change: %s", excerpt(p.body))
	}
	e.post(bob, path+"/reply", grant("Approval"))
	e.waitFor("bob's approval of carol's change", func() bool { return e.hasEvent("org", nc, db.EventApproved) })

	// An approval from someone not in the owning ACL does not count — until a landing puts them
	// in it, which re-derives the approval of every active CL.
	c.jj("org", "new", "main@ok", "-m", "bob joins eng's group")
	c.write("org/identity/eng/group.yaml", members(carol, bob))
	nd := uploaded(c.jj("org", "cl", "--ready"))[0]
	e.post(dana, clPath("org", nd)+"/reply", grant("LGTM", "Approval"))
	if got := submitTitle(e.get(carol, clPath("org", nd)), "Submit"); !strings.Contains(got, "Approval from a member of eng.acl.example.com") {
		t.Errorf("dana's approval counted before she is eng's: submit %q", got)
	}
	a.jj("org", "sync")
	a.jj("org", "new", "main@ok", "-m", "dana joins eng's ACL")
	a.write("org/identity/eng/acl.yaml", members(bob, dana))
	na := uploaded(a.jj("org", "cl", "--ready"))[0]
	e.post(alice, clPath("org", na)+"/reply", grant("LGTM"))
	e.prePassed("org", na)
	e.submitted(alice, clPath("org", na), "submit")
	e.waitFor("dana's approval to count once she is eng's", func() bool { return e.hasEvent("org", nd, db.EventApproved) })

	// carol adds herself to the root's ACL: only its member, alice, may approve that.
	c.jj("org", "new", "main@ok", "-m", "carol is an admin")
	c.write("org/identity/acl.yaml", members(alice, carol))
	nr := uploaded(c.jj("org", "cl", "--ready"))[0]
	e.post(bob, clPath("org", nr)+"/reply", grant("LGTM", "Approval"))
	if got := submitTitle(e.get(carol, clPath("org", nr)), "Submit"); !strings.Contains(got, "Approval from a member of admins.acl.example.com") {
		t.Errorf("carol's self-promotion: submit %q", got)
	}

	// The tree's other files need the root's ACL too.
	b.jj("org", "new", "main@ok", "-m", "readme")
	b.write("org/identity/README.md", "The groups and ACLs, by folder.\n")
	nf := uploaded(b.jj("org", "cl", "--ready"))[0]
	if got := submitTitle(e.get(bob, clPath("org", nf)), "Submit"); !strings.Contains(got, "Approval from a member of admins.acl.example.com (change identity/README.md)") {
		t.Errorf("the tree's README: submit %q", got)
	}

	// A change ok identity would not apply cannot be approved: a member that is no principal, and a
	// retired name.
	b.jj("org", "new", "main@ok", "-m", "a ghost")
	b.write("org/identity/eng/acl.yaml", members(bob, "ghost.human.example.com"))
	b.write("org/identity/old/group.yaml", members(bob))
	ng := uploaded(b.jj("org", "cl", "--ready"))[0]
	p = e.get(bob, clPath("org", ng))
	for _, want := range []string{
		"This change to the groups-and-ACLs tree can't be approved: ok identity would not apply it",
		"ghost.human.example.com (in eng.acl.example.com) is no principal",
		"old.group.example.com was deleted, and its name is retired",
	} {
		if !p.has(want) {
			t.Errorf("a change ok identity would not apply lacks %q: %s", want, excerpt(p.body))
		}
	}
	if got := submitTitle(p, "Submit"); !strings.Contains(got, "a change to the groups-and-ACLs tree that can apply") {
		t.Errorf("its submit: %q", got)
	}
	if !p.has("a change to the groups-and-ACLs tree that can&#39;t apply") && !p.has("a change to the groups-and-ACLs tree that can't apply") {
		t.Errorf("its Approval requirement: %s", excerpt(p.body))
	}
}
