package e2e

import (
	"net/http"
	"net/url"
	"testing"
)

// Forks from ok control: a person forks a repository from its page into their own name; the fork
// says what it is a fork of, holds what its upstream holds, and — a fork of an internal repository
// being internal, as its upstream is — the organization's other people read it, with no Sync, as
// they may not write to it; once the upstream lands more, its page's Sync brings the fork the
// upstream's work. A name taken is refused.
func TestForks(t *testing.T) {
	e := newEnv(t)
	e.post(dana, "/repositories/new", url.Values{"name": {"svc"}})
	b := e.person(bob)
	b.jj("", "clone", "svc")
	land := func(file, message string) {
		t.Helper()
		b.jj("svc", "new", "main@ok")
		b.write("svc/"+file, "package main\n")
		b.jj("svc", "describe", "-m", message)
		n := uploaded(b.jj("svc", "cl", "--ready"))[0]
		path := clPath("svc", n)
		e.post(alice, path+"/reply", grant("LGTM", "Approval"))
		e.prePassed("svc", n)
		e.submitted(bob, path, "submit")
	}
	land("serve/main.go", "add serve")

	if p := e.get(alice, "/example/svc"); !p.has(`href="/example/svc/fork"`) || p.has("Sync Fork") {
		t.Fatalf("the upstream's page: a Fork link, and no Sync: %s", excerpt(p.body))
	}
	form := e.get(alice, "/example/svc/fork")
	if !form.has("Fork svc") || !form.has(`action="/example/svc/fork"`) || !form.has(`value="svc"`) {
		t.Fatalf("the fork form: %s", excerpt(form.body))
	}
	p := e.do(alice, http.MethodPost, "/example/svc/fork", url.Values{"owner": {"alice"}, "name": {"svc"}}, nil)
	if p.status != http.StatusSeeOther || p.header.Get("Location") != "/alice/svc" {
		t.Fatalf("forking: %d to %q\n%s", p.status, p.header.Get("Location"), excerpt(p.body))
	}
	fork := e.get(alice, "/alice/svc")
	if !fork.has("Fork of") || !fork.has(`href="/example/svc"`) || !fork.has(">serve<") || !fork.has("Sync Fork") {
		t.Fatalf("the fork's page: %s", excerpt(fork.body))
	}
	if p := e.get(carol, "/alice/svc"); !p.has("Fork of") || p.has("Sync Fork") {
		t.Errorf("alice's fork, to another of the organization's people: %s", excerpt(p.body))
	}
	if p := e.post(alice, "/example/svc/fork", url.Values{"owner": {"alice"}, "name": {"svc"}}); !p.has("already exists") {
		t.Errorf("forking into a name taken: %s", excerpt(p.body))
	}

	// The upstream lands more; the fork takes it when synced.
	land("later.go", "add later")
	if e.get(alice, "/alice/svc").has(">later.go<") {
		t.Fatal("the fork has the upstream's later work before a sync")
	}
	if p := e.do(alice, http.MethodPost, "/alice/svc/action/sync-fork", url.Values{}, nil); p.status != http.StatusSeeOther {
		t.Fatalf("syncing the fork: %d\n%s", p.status, excerpt(p.body))
	}
	e.waitFor("the fork's main to follow its upstream's", func() bool { return e.get(alice, "/alice/svc").has(">later.go<") })
}
