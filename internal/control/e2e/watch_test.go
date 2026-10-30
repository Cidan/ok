package e2e

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	controlpb "github.com/Cidan/ok/proto/ok/control/v1"
)

// watcher is a watch of a CL's status as a person holds one: each status it sends, in order.
type watcher struct {
	t    *testing.T
	what string
	ch   chan *controlpb.CLStatus
	last *controlpb.CLStatus
}

func (e *env) watch(principal, repo string, n int64) *watcher {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	e.t.Cleanup(cancel)
	ctl := controlpb.NewChangeListsClient(as(e.t, strings.TrimPrefix(e.control, "http://"), principal))
	stream, err := ctl.Watch(ctx, &controlpb.CLRef{Repository: repo, Number: n})
	if err != nil {
		e.t.Fatal(err)
	}
	w := &watcher{t: e.t, what: fmt.Sprintf("%s's watch of CL %d", principal, n), ch: make(chan *controlpb.CLStatus, 64)}
	go func() {
		defer close(w.ch)
		for {
			st, err := stream.Recv()
			if err != nil {
				return
			}
			w.ch <- st
		}
	}()
	return w
}

// until waits for a status the watch sends that cond holds of.
func (w *watcher) until(what string, cond func(*controlpb.CLStatus) bool) {
	w.t.Helper()
	deadline := time.After(time.Minute)
	for {
		if w.last != nil && cond(w.last) {
			return
		}
		select {
		case st, ok := <-w.ch:
			if !ok {
				w.t.Fatalf("%s ended waiting for %s; last: %v", w.what, what, w.last)
			}
			w.last = st
		case <-deadline:
			w.t.Fatalf("%s never sent %s; last: %v", w.what, what, w.last)
		}
	}
}

func blocked(st *controlpb.CLStatus) string { return st.GetReview().GetBlocked() }

// A watch of a CL hears what changes its status, though neither changes the CL: a landing that
// conflicts with it — a move of main that changes a file it changes — and the CL below it in its
// stack, abandoned; and a landing that changes an OWNERS file governing it, which costs it an
// approval (the feed's head step wakes every watch for what governs every CL, and re-derives every
// open CL's approval).
func TestWatchHearsWhatChangesItsStatus(t *testing.T) {
	e := newEnv(t)
	e.post(alice, "/repositories/new", url.Values{"name": {"watch"}})
	a := e.person(alice)
	b := e.person(bob)
	a.jj("", "clone", "watch")
	a.write("watch/README.md", "one\n")
	a.write("watch/core/OWNERS", "user: bob\n")
	a.write("watch/core/core.go", "package core\n")
	a.write("watch/docs/guide.md", "# Guide\n")
	a.jj("watch", "describe", "-m", "base")
	n := uploaded(a.jj("watch", "cl", "--ready"))[0]
	e.post(alice, clPath("watch", n)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("watch", n)
	e.submitted(alice, clPath("watch", n), "submit")
	a.jj("watch", "sync")

	// bob's README change, approved, its pre passed: nothing keeps it from submitting.
	b.jj("", "clone", "watch")
	b.write("watch/README.md", "one, bob's way\n")
	b.jj("watch", "describe", "-m", "bob's readme")
	nb := uploaded(b.jj("watch", "cl", "--ready"))[0]
	e.post(alice, clPath("watch", nb)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("watch", nb)
	conflicting := e.watch(bob, "watch", nb)
	conflicting.until("bob's CL submittable", func(st *controlpb.CLStatus) bool {
		return st.GetReview().GetApproved() && st.GetPre().GetVerdict() == controlpb.Checks_VERDICT_PASSED && blocked(st) == ""
	})

	// A stack of two, approved, its pre passed at its top: the top waits for the bottom.
	a.jj("watch", "new", "main@ok", "-m", "guide")
	a.write("watch/docs/guide.md", "# Guide\n\nMore.\n")
	a.jj("watch", "new", "-m", "core")
	a.write("watch/core/core.go", "package core\n\nconst V = 1\n")
	stack := uploaded(a.jj("watch", "cl", "--ready"))
	if len(stack) != 2 {
		t.Fatalf("a stack: %v", stack)
	}
	for _, s := range stack {
		e.post(bob, clPath("watch", s)+"/reply", grant("LGTM", "Approval"))
	}
	e.prePassed("watch", stack[1])
	top := e.watch(alice, "watch", stack[1])
	parentFirst := fmt.Sprintf("Submit the parent changelist (CL %d) first", stack[0])
	top.until("the parent first", func(st *controlpb.CLStatus) bool { return strings.HasPrefix(blocked(st), parentFirst) })

	// alice's README change lands: bob's CL conflicts with main, which its watch hears.
	a.jj("watch", "new", "main@ok", "-m", "alice's readme")
	a.write("watch/README.md", "one, two\n")
	na := uploaded(a.jj("watch", "cl", "--ready"))[0]
	e.post(bob, clPath("watch", na)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("watch", na)
	e.submitted(alice, clPath("watch", na), "submit")
	conflicting.until("the conflict", func(st *controlpb.CLStatus) bool {
		return strings.Contains(blocked(st), "conflicts with main in README.md")
	})

	// The stack's bottom abandoned: its top's watch hears it.
	e.post(alice, clPath("watch", stack[0])+"/abandon", url.Values{})
	top.until("the parent abandoned", func(st *controlpb.CLStatus) bool {
		return strings.HasPrefix(blocked(st), fmt.Sprintf("Parent CL %d is abandoned", stack[0]))
	})

	// A landing that moves core to carol: the approval bob gave core's change as its owner no longer
	// counts, which a watch of it hears though the landing changed no file it changes.
	a.jj("watch", "new", "main@ok", "-m", "core change")
	a.write("watch/core/core.go", "package core\n\nconst W = 2\n")
	nc := uploaded(a.jj("watch", "cl", "--ready"))[0]
	e.post(bob, clPath("watch", nc)+"/reply", grant("LGTM", "Approval"))
	owned := e.watch(alice, "watch", nc)
	owned.until("core's change approved", func(st *controlpb.CLStatus) bool { return st.GetReview().GetApproved() })
	a.jj("watch", "new", "main@ok", "-m", "carol owns core")
	a.write("watch/core/OWNERS", "user: carol\n")
	no := uploaded(a.jj("watch", "cl", "--ready"))[0]
	e.post(bob, clPath("watch", no)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("watch", no)
	e.submitted(alice, clPath("watch", no), "submit")
	owned.until("the approval lost", func(st *controlpb.CLStatus) bool { return !st.GetReview().GetApproved() })
}
