package e2e

import (
	"net/url"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/control/db"
)

// An OWNERS file may name an ACL: its members' Approval counts, upload refuses an ACL that does
// not exist, and the review dialog suggests the ACL's members.
func TestACLOwners(t *testing.T) {
	e := newEnv(t)
	e.post(alice, "/repositories/new", url.Values{"name": {"acls"}})
	a, c := e.person(alice), e.person(carol)
	a.jj("", "clone", "acls")

	// An ACL that does not exist is refused at upload.
	a.write("acls/svc/OWNERS", "acl: nobody\n")
	a.write("acls/svc/x.txt", "x\n")
	a.jj("acls", "describe", "-m", "bad owners")
	if out, err := a.run("acls", "cl"); err == nil || !strings.Contains(out, "acl: nobody names no ACL") {
		t.Fatalf("an OWNERS naming no ACL uploaded: %v\n%s", err, out)
	}

	a.write("acls/svc/OWNERS", "acl: reviewers\n")
	a.jj("acls", "describe", "-m", "owners by ACL")
	n := uploaded(a.jj("acls", "cl", "--ready"))[0]
	e.post(alice, clPath("acls", n)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("acls", n)
	e.submitted(alice, clPath("acls", n), "submit")

	// carol changes svc/: her own Approval does not count, bob's — a member — does.
	c.jj("", "clone", "acls")
	c.write("acls/svc/x.txt", "y\n")
	c.jj("acls", "describe", "-m", "y")
	nc := uploaded(c.jj("acls", "cl", "--ready"))[0]
	path := clPath("acls", nc)
	e.post(carol, path+"/reply", grant("Approval"))
	e.post(alice, path+"/reply", grant("LGTM"))
	p := e.get(carol, path)
	if got := submitTitle(p, "Submit"); !strings.Contains(got, "Approval from //svc/OWNERS") {
		t.Errorf("carol's own approval counted: submit %q", got)
	}
	if !p.has("acl: reviewers") {
		t.Errorf("who can approve lacks the ACL: %s", excerpt(p.body))
	}
	e.post(bob, path+"/reply", grant("Approval"))
	e.waitFor("bob's approval", func() bool { return e.hasEvent("acls", nc, db.EventApproved) })
}
