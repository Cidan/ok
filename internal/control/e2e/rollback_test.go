package e2e

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/control/db"
)

// rollback presses Roll Back on a CL as principal, through htmx, and returns the rollback CL's
// path, which the response navigates to.
func (e *env) rollback(principal, path, reason string) string {
	e.t.Helper()
	p := e.do(principal, http.MethodPost, path+"/rollback", url.Values{"reason": {reason}},
		map[string]string{"HX-Request": "true", "HX-Target": "div#repo-main"})
	var loc struct{ Path, Target, Swap string }
	if err := json.Unmarshal([]byte(p.header.Get("HX-Location")), &loc); err != nil || loc.Target != "#repo-main" || !strings.Contains(loc.Path, "/cl/") {
		e.t.Fatalf("rolling back %s: %d, HX-Location %q (%v)\n%s", path, p.status, p.header.Get("HX-Location"), err, excerpt(p.body))
	}
	return loc.Path
}

// file is a file's contents on main, and whether it is there.
func (e *env) file(repo, path string) (string, bool) {
	e.t.Helper()
	p := e.do(alice, http.MethodGet, "/"+repo+"/raw/main/"+path, nil, nil)
	return p.body, p.status == http.StatusOK
}

// TestRollbacks rolls landed CLs back from their pages. A rollback is a CL of whoever asked,
// reviewed expedited — an LGTM from anyone but its author, no owner's Approval — and landed like
// any; a stack rolls back whole; a path main changed since is left, and rolled back by hand in a
// CL stacked on the rollback, reviewed like any change.
func TestRollbacks(t *testing.T) {
	e := newEnv(t)
	e.post(alice, "/repositories/new", url.Values{"name": {"svc"}})
	a, b, c := e.person(alice), e.person(bob), e.person(carol)
	a.jj("", "clone", "svc")

	// main: lib/, which bob owns.
	a.write("svc/lib/OWNERS", "user: bob\n")
	a.write("svc/lib/lib.go", "package lib\n\nfunc One() int { return 1 }\n")
	a.jj("svc", "describe", "-m", "add lib")
	n := uploaded(a.jj("svc", "cl", "--ready"))[0]
	e.post(alice, clPath("svc", n)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("svc", n)
	e.submitted(alice, clPath("svc", n), "submit")

	// bob's change to lib, reviewed by alice, lands.
	b.jj("", "clone", "svc")
	b.write("svc/lib/lib.go", "package lib\n\nfunc One() int { return 2 }\n")
	b.write("svc/lib/extra.go", "package lib\n")
	b.jj("svc", "describe", "-m", "lib: one is two")
	n2 := uploaded(b.jj("svc", "cl"))[0]
	p2 := clPath("svc", n2)
	e.post(bob, p2+"/request-review", url.Values{"reviewer": {alice}})
	e.post(alice, p2+"/reply", grant("LGTM"))
	e.prePassed("svc", n2)
	e.submitted(bob, p2, "submit")

	// Anyone may roll back a merged CL, saying why; an open one is not rolled back.
	if !e.get(carol, p2).has(p2 + "/rollback") {
		t.Fatalf("a merged CL offers no Roll Back")
	}
	if got := notice(e.post(carol, p2+"/rollback", url.Values{"reason": {" "}})); got != "Say why CL 2 is rolled back" {
		t.Errorf("a rollback without a reason: %q", got)
	}
	p3 := e.rollback(carol, p2, "it broke the build")
	if p3 != clPath("svc", 3) {
		t.Fatalf("the rollback is %s", p3)
	}

	// The rollback: carol's, open, marked, reviewed by bob and alice, and needing only an LGTM.
	p := e.get(carol, p3)
	if !chip(p, "Rollback") || !chip(p, "Open") || !p.has("Roll back CL 2: lib: one is two") || !p.has("it broke the build") ||
		!p.has("Not needed: a rollback") || !logged(p, "carol", "added reviewer bob") || !logged(p, "carol", "added reviewer alice") {
		t.Fatalf("the rollback CL: %s", excerpt(p.body))
	}
	if got := submitTitle(p, "Submit"); !strings.Contains(got, "LGTM") || strings.Contains(got, "Approval") {
		t.Errorf("the rollback's submit: %q", got)
	}
	if !chip(e.get(carol, "/svc/cls"), "Rollback") {
		t.Errorf("the finder does not mark the rollback")
	}

	// The rolled-back CL names it, and offers no second one while it is open.
	p = e.get(alice, p2)
	if !p.has("Rolled Back") || !p.has(`href="`+p3+`"`) || p.has(p2+"/rollback") || !logged(p, "carol", "rolled the change back in") {
		t.Fatalf("the rolled-back CL: %s", excerpt(p.body))
	}
	if got := notice(e.post(dana, p2+"/rollback", url.Values{"reason": {"again"}})); !strings.Contains(got, "CL 3 already rolls it back") {
		t.Errorf("a second rollback: %q", got)
	}

	// Its author's LGTM does not count; dana's does, without lib's owner. It lands like any CL.
	e.post(carol, p3+"/reply", grant("LGTM"))
	if strings.Contains(submitTitle(e.get(carol, p3), "Submit"), "Approval") || e.hasEvent("svc", 3, db.EventApproved) {
		t.Fatalf("the rollback's author approved it")
	}
	e.post(dana, p3+"/reply", grant("LGTM"))
	e.waitFor("the rollback's approval", func() bool { return e.hasEvent("svc", 3, db.EventApproved) })
	e.prePassed("svc", 3)
	e.submitted(carol, p3, "submit")
	if got, ok := e.file("svc", "lib/lib.go"); !ok || got != "package lib\n\nfunc One() int { return 1 }\n" {
		t.Errorf("lib/lib.go after the rollback: %q", got)
	}
	if _, ok := e.file("svc", "lib/extra.go"); ok {
		t.Errorf("lib/extra.go survived the rollback")
	}

	// Rolling back the rollback, without htmx, redirects to it.
	p = e.do(bob, http.MethodPost, p3+"/rollback", url.Values{"reason": {"it was fine"}}, nil)
	if p.status != http.StatusSeeOther || p.header.Get("Location") != clPath("svc", 4) {
		t.Fatalf("rolling back the rollback: %d %q", p.status, p.header.Get("Location"))
	}
	if p := e.get(bob, clPath("svc", 4)); !p.has("Roll back CL 3: Roll back CL 2: lib: one is two") || !chip(p, "Rollback") {
		t.Errorf("the rollback of the rollback: %s", excerpt(p.body))
	}
	e.post(bob, clPath("svc", 4)+"/abandon", url.Values{})

	// A stack rolls back whole; main changed lib/lib.go since, so it is left.
	b.jj("svc", "sync")
	b.jj("svc", "new", "main@ok", "-m", "lib: two")
	b.write("svc/lib/two.go", "package lib\n")
	b.jj("svc", "new", "-m", "lib: three")
	b.write("svc/lib/three.go", "package lib\n")
	b.write("svc/lib/lib.go", "package lib\n\nfunc One() int { return 3 }\n")
	stack := uploaded(b.jj("svc", "cl", "--ready", "-r", "@-", "-r", "@"))
	n5, n6 := stack[0], stack[1]
	for _, n := range stack {
		e.post(alice, clPath("svc", n)+"/reply", grant("LGTM"))
		e.prePassed("svc", n)
	}
	e.submitted(bob, clPath("svc", n6), "submit-stack")
	a.jj("svc", "sync")
	a.jj("svc", "new", "main@ok", "-m", "lib: one is four")
	a.write("svc/lib/lib.go", "package lib\n\nfunc One() int { return 4 }\n")
	n7 := uploaded(a.jj("svc", "cl", "--ready"))[0]
	e.post(bob, clPath("svc", n7)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("svc", n7)
	e.submitted(alice, clPath("svc", n7), "submit")

	p8 := e.rollback(carol, clPath("svc", n5), "two was wrong")
	p = e.get(carol, p8)
	for _, want := range []string{"Roll back CLs 5 and 6: lib: two", `href="` + clPath("svc", n6) + `"`, "lib/lib.go", "jj cl get 8", "jj new cl/8@ok"} {
		if !p.has(want) {
			t.Fatalf("the stack's rollback lacks %q: %s", want, excerpt(p.body))
		}
	}
	for _, rolled := range []int64{n5, n6} {
		if p := e.get(alice, clPath("svc", rolled)); !p.has("Rolled Back") || !p.has(`href="`+p8+`"`) {
			t.Errorf("CL %d does not name its rollback", rolled)
		}
	}

	// carol rolls lib/lib.go back by hand in a CL stacked on the rollback: that one needs lib's
	// owner's Approval, the rollback only an LGTM; Submit Stack lands both.
	c.jj("", "clone", "svc")
	c.jj("svc", "cl", "get", "8")
	c.jj("svc", "new", "cl/8@ok", "-m", "lib: one is one again, by hand")
	c.write("svc/lib/lib.go", "package lib\n\nfunc One() int { return 1 }\n")
	n9 := uploaded(c.jj("svc", "cl", "--ready"))[0]
	if p := e.get(carol, clPath("svc", n9)); !p.has("0 of 1 owner sets") || p.has("Not needed: a rollback") || chip(p, "Rollback") {
		t.Errorf("the hand edit's review: %s", excerpt(p.body))
	}
	e.post(dana, p8+"/reply", grant("LGTM"))
	e.post(dana, clPath("svc", n9)+"/reply", grant("LGTM"))
	e.post(bob, clPath("svc", n9)+"/reply", grant("Approval"))
	e.prePassed("svc", 8)
	e.prePassed("svc", n9)
	e.submitted(carol, clPath("svc", n9), "submit-stack")
	if got, _ := e.file("svc", "lib/lib.go"); got != "package lib\n\nfunc One() int { return 1 }\n" {
		t.Errorf("lib/lib.go after the stack's rollback: %q", got)
	}
	for _, gone := range []string{"lib/two.go", "lib/three.go"} {
		if _, ok := e.file("svc", gone); ok {
			t.Errorf("%s survived the stack's rollback", gone)
		}
	}

	// A patchset its author edits on a rollback CL is theirs: what differs from the inverse
	// change needs its owners' Approval.
	p10 := e.rollback(carol, clPath("svc", n9), "by hand was wrong")
	c.jj("svc", "sync")
	c.jj("svc", "cl", "get", "10")
	c.jj("svc", "--ignore-immutable", "edit", "cl/10@ok")
	c.write("svc/lib/lib.go", "package lib\n\nfunc One() int { return 5 }\n")
	if out := c.jj("svc", "--ignore-immutable", "cl"); !strings.Contains(out, "CL 10 patchset 2") {
		t.Fatalf("a hand-edited patchset of the rollback: %s", out)
	}
	e.waitFor("the rollback's second patchset", func() bool { return logged(e.get(carol, p10), "carol", "uploaded patchset 2") })
	if got := submitTitle(e.get(carol, p10), "Submit"); !strings.Contains(got, "Approval from //lib/OWNERS") {
		t.Errorf("an edited rollback's submit: %q", got)
	}
	if !e.get(carol, p10).has("for the files edited by hand") {
		t.Errorf("an edited rollback's Approval row")
	}
}
