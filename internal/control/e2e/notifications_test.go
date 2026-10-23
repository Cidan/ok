package e2e

import (
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestNotifications drives notifications through ok control's outbox, the directory, and its mail
// (docs/hosted.md, "ok control at scale, as built"): a review asked of bob notifies him and mails
// him, by default; his comment and his grant notify alice, unmailed until she chooses to be; and
// she marks them read.
func TestNotifications(t *testing.T) {
	e := newEnv(t)
	e.post(alice, "/repositories/new", url.Values{"name": {"notes"}})
	a := e.person(alice)
	a.jj("", "clone", "notes")
	a.write("notes/notes.md", "hello\n")
	a.jj("notes", "describe", "-m", "a note for bob")
	cl := uploaded(a.jj("notes", "cl"))[0]
	path := clPath("notes", cl)
	listed := func(who, text string) bool { return strings.Contains(e.get(who, "/notifications").body, text) }

	e.post(alice, path+"/request-review", url.Values{"reviewer": {e.id(bob)}, "message": {"please look"}})
	e.waitFor("bob notified of the review", func() bool { return listed(bob, "asked you to review it") })
	m := e.mail.Next(t)
	if !slices.Equal(m.To, []string{"bob@example.com"}) || !strings.Contains(m.Subject, "a note for bob") ||
		!strings.Contains(m.Body, "alice asked you to review it.") || !strings.Contains(m.Body, e.control+path) {
		t.Errorf("bob's mail: %+v", m)
	}
	if p := e.get(bob, "/"); !strings.Contains(p.body, `id="notifications-link"`) || !strings.Contains(p.body, ">1</span>") {
		t.Errorf("bob's home's unread count: %s", excerpt(p.body))
	}

	// bob comments, then grants: alice is notified of each, mailed of neither.
	e.post(bob, path+"/reply", url.Values{"comment": {"why a note?"}})
	e.post(bob, path+"/reply", grant("LGTM"))
	ok := false
	for range 100 {
		p := e.get(alice, "/notifications").body
		if ok = strings.Contains(p, "commented: why a note?") && strings.Contains(p, "granted LGTM on patchset 1"); ok {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("alice was not notified of the comment and the grant: %s", excerpt(e.get(alice, "/notifications").body))
	}
	if listed(bob, "commented") {
		t.Error("bob was notified of his own comment")
	}

	// alice has comments mailed: bob's next is.
	e.post(alice, "/notifications/preferences", url.Values{"mail": {"3"}})
	if p := e.get(alice, "/notifications"); !strings.Contains(p.body, `value="3" checked`) || strings.Contains(p.body, `value="1" checked`) {
		t.Errorf("alice's preferences: %s", excerpt(p.body))
	}
	e.post(bob, path+"/reply", url.Values{"comment": {"still here"}})
	m = e.mail.Next(t)
	if !slices.Equal(m.To, []string{"alice@example.com"}) || !strings.Contains(m.Body, "bob commented: still here.") {
		t.Errorf("alice's mail: %+v", m)
	}
	if n := len(e.mail.Messages()); n != 2 {
		t.Errorf("%d messages mailed, want bob's review and alice's comment", n)
	}

	e.post(alice, "/notifications/read", nil)
	if p := e.get(alice, "/notifications"); !strings.Contains(p.body, "Nothing unread.") {
		t.Errorf("alice's notifications once read: %s", excerpt(p.body))
	}
}
