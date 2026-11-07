package e2e

import (
	"net/url"
	"strings"
	"testing"
)

// TestInbox drives a person's inbox through ok control's outbox and the directory (docs/hosted.md,
// "ok control at scale, as built"): a review requested of bob waits on him on his home, until he
// replies on the CL's current patchset; its author sees it too; and a CL closed leaves both.
func TestInbox(t *testing.T) {
	e := newEnv(t)
	e.post(alice, "/repositories/new", url.Values{"name": {"inbox"}})
	a := e.person(alice)
	a.jj("", "clone", "inbox")
	a.write("inbox/notes.md", "hello\n")
	a.jj("inbox", "describe", "-m", "a note for bob")
	cl := uploaded(a.jj("inbox", "cl"))[0]
	path := clPath("inbox", cl)
	// The CL's row in who's inbox — the dashboard's lists, not its notifications, which link to the CL
	// as long as they are kept — and whether it waits on them.
	waiting := func(who string) (bool, bool) {
		body := e.get(who, "/").body
		for at := 0; ; {
			i := strings.Index(body[at:], `<a href="`+path+`"`)
			if i < 0 {
				return false, false
			}
			tag := body[at+i : at+i+strings.Index(body[at+i:], ">")]
			if strings.Contains(tag, "data-inbox-item") {
				return true, strings.Contains(tag, `data-attention="true"`)
			}
			at += i + len(tag)
		}
	}
	e.waitFor("the draft in alice's inbox", func() bool { listed, _ := waiting(alice); return listed })
	if listed, _ := waiting(bob); listed {
		t.Error("a draft no one reviews is in bob's inbox")
	}

	e.post(alice, path+"/request-review", url.Values{"reviewer": {e.id(bob)}, "message": {"please look"}})
	e.waitFor("the review waiting on bob", func() bool { listed, attn := waiting(bob); return listed && attn })
	if p := e.get(bob, "/"); !strings.Contains(p.body, "a note for bob") || !strings.Contains(p.body, "Needs Review") {
		t.Errorf("bob's inbox row: %s", excerpt(p.body))
	}

	// bob replies on the current patchset: it no longer waits on him.
	e.post(bob, path+"/reply", grant("LGTM"))
	e.waitFor("the review no longer waiting on bob", func() bool { listed, attn := waiting(bob); return listed && !attn })

	// Abandoned, it leaves both inboxes.
	e.post(alice, path+"/abandon", url.Values{"reason": {"not needed"}})
	e.waitFor("the abandoned CL gone from both inboxes", func() bool {
		la, _ := waiting(alice)
		lb, _ := waiting(bob)
		return !la && !lb
	})
}
