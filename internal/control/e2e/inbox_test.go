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
	waiting := func(who string) (bool, bool) {
		p := e.get(who, "/")
		at := strings.Index(p.body, `href="`+path+`"`)
		if at < 0 {
			return false, false
		}
		row := p.body[at:min(len(p.body), at+400)]
		return true, strings.Contains(row, `data-attention="true"`)
	}
	e.waitFor("the draft in alice's inbox", func() bool { listed, _ := waiting(alice); return listed })
	if listed, _ := waiting(bob); listed {
		t.Error("a draft no one reviews is in bob's inbox")
	}

	e.post(alice, path+"/request-review", url.Values{"reviewer": {e.id(bob)}, "message": {"please look"}})
	e.waitFor("the review waiting on bob", func() bool { listed, attn := waiting(bob); return listed && attn })
	if p := e.get(bob, "/"); !strings.Contains(p.body, "a note for bob") || !strings.Contains(p.body, "reviewer · open") {
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
