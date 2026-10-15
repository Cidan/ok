package e2e

import (
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/control/db"
)

// chip reports whether a page has a status chip reading label.
func chip(p page, label string) bool {
	return regexp.MustCompile(`<span class="[^"]*">\s*` + regexp.QuoteMeta(label) + `\s*</span>`).MatchString(p.body)
}

// logged reports whether a CL page's change log has an entry by who with action.
func logged(p page, who, action string) bool {
	return regexp.MustCompile(`>` + regexp.QuoteMeta(who) + `</span>\s*<span class="text-zinc-500">` + regexp.QuoteMeta(action) + `</span>`).MatchString(p.body)
}

// grant is a Reply form granting (and so, for labels left out, revoking) labels.
func grant(labels ...string) url.Values {
	f := url.Values{"label": {"LGTM", "Approval"}}
	for _, l := range labels {
		f.Set("grant:"+l, "on")
	}
	return f
}

func TestRepositoriesAndBrowsing(t *testing.T) {
	e := newEnv(t)

	// Creating in the web makes the person the repository's creator.
	p := e.post(alice, "/repositories/new", url.Values{"name": {"browse"}})
	if p.status != http.StatusSeeOther || p.header.Get("Location") != "/browse" {
		t.Fatalf("create: %d to %q", p.status, p.header.Get("Location"))
	}
	if p := e.post(bob, "/repositories/new", url.Values{"name": {"browse"}}); !p.has("A repository named browse already exists") {
		t.Errorf("a name taken: %s", excerpt(p.body))
	}
	if p := e.post(bob, "/repositories/new", url.Values{"name": {"assets"}}); !p.has("reserved for ok's own pages") {
		t.Errorf("a reserved name: %s", excerpt(p.body))
	}
	// Every stylesheet, script, and icon a page references is served, as what it is.
	for _, page := range []string{"/", "/browse"} {
		body := e.get(alice, page).body
		refs := regexp.MustCompile(`(?:href|src)="(/(?:assets/[^"]+|favicon\.svg))"`).FindAllStringSubmatch(body, -1)
		if len(refs) < 5 {
			t.Fatalf("%s references %d assets: %s", page, len(refs), excerpt(body))
		}
		for _, m := range refs {
			p := e.do("", http.MethodGet, m[1], nil, nil)
			ct := p.header.Get("Content-Type")
			want := map[string]string{".css": "text/css", ".js": "text/javascript", ".svg": "image/svg+xml"}[filepath.Ext(m[1])]
			if p.status != http.StatusOK || want == "" || !strings.HasPrefix(ct, want) {
				t.Errorf("%s: %d %q", m[1], p.status, ct)
			}
		}
	}
	if p := e.do("", http.MethodGet, "/assets/fonts/ibm-plex-sans/ibm-plex-sans-latin-400-normal.woff2", nil, nil); p.status != http.StatusOK {
		t.Errorf("a font: %d", p.status)
	}

	home := e.get(alice, "/browse")
	if !home.has("Created by alice") || !home.has("Nothing has landed on main yet") || !home.has("jj clone browse") {
		t.Fatalf("a new repository: %s", excerpt(home.body))
	}

	// A first change, landed.
	a := e.person(alice)
	a.jj("", "clone", "browse")
	a.write("browse/README.md", "# Browse\n\nSee [the service](svc/main.go) and ![logo](img/logo.svg).\n")
	a.write("browse/svc/main.go", "package main\n\n/* a comment\n   spanning lines */\nfunc main() {}\n")
	a.write("browse/img/logo.svg", `<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>`)
	a.write("browse/page.html", "<script>alert(1)</script>\n")
	a.jj("browse", "describe", "-m", "a first change")
	n := uploaded(a.jj("browse", "cl", "--ready"))[0]
	e.post(alice, clPath("browse", n)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("browse", n)
	e.submitted(alice, clPath("browse", n), "submit")

	// The repository view: entries with their last commits, the README rendered with its
	// links resolved, the latest commit.
	home = e.get(bob, "/browse")
	for _, want := range []string{">svc<", ">README.md<", "a first change", `href="/browse/tree/main/svc/main.go"`, `src="/browse/raw/main/img/logo.svg"`, ">1</span> commit"} {
		if !home.has(want) {
			t.Errorf("repository view lacks %s:\n%s", want, excerpt(home.body))
		}
	}
	dir := e.get(bob, "/browse/tree/main/svc")
	if !dir.has(">main.go<") {
		t.Errorf("a directory: %s", excerpt(dir.body))
	}
	file := e.get(bob, "/browse/tree/main/svc/main.go")
	if !file.has(`<span class="cm">   spanning lines */</span>`) || !file.has("Blame") || !file.has("History") {
		t.Errorf("a source file (highlighted across lines): %s", excerpt(file.body))
	}
	blame := e.get(bob, "/browse/blame/main/svc/main.go")
	if !blame.has("a first change") || !blame.has(">alice<") {
		t.Errorf("blame: %s", excerpt(blame.body))
	}

	// Below the root, the browser: the tree beside the pane, open down to the path and naming
	// it, and every other folder fetched when it is first opened.
	for _, p := range []page{dir, file, blame} {
		if !p.has("data-browser") || !p.has(`id="browse-main"`) || !p.has(`data-tree-dir="svc" open`) {
			t.Errorf("the browser: %s", excerpt(p.body))
		}
	}
	if !file.has(`data-path="svc/main.go" aria-current="page"`) || !blame.has(`data-browse-kind="blame"`) {
		t.Errorf("the browser's path: %s", excerpt(file.body))
	}
	if !dir.has(`hx-get="/browse/tree-children/main/img"`) {
		t.Errorf("a folder off the path is fetched on opening: %s", excerpt(dir.body))
	}
	if home.has("data-browser") {
		t.Errorf("the root is the landing, not the browser")
	}
	// A navigation within the browser swaps the pane alone; any other, the content region; a
	// history restore gets the whole document.
	nav := map[string]string{"HX-Request": "true"}
	pane := e.do(bob, http.MethodGet, "/browse/tree/main/svc", nil, map[string]string{"HX-Request": "true", "HX-Target": "div#browse-main"})
	if !strings.HasPrefix(pane.body, `<div data-browse-path="svc" data-browse-kind="dir"`) || pane.has("data-browser") {
		t.Errorf("the pane alone: %s", excerpt(pane.body))
	}
	if content := e.do(bob, http.MethodGet, "/browse/tree/main/svc", nil, nav); !strings.HasPrefix(content.body, "<div data-browser") {
		t.Errorf("the content region: %s", excerpt(content.body))
	}
	restore := e.do(bob, http.MethodGet, "/browse/tree/main/svc", nil, map[string]string{"HX-Request": "true", "HX-History-Restore-Request": "true", "HX-Target": "main#content"})
	if !strings.HasPrefix(strings.ToLower(restore.body), "<!doctype html>") || !restore.has("hx-history-elt") {
		t.Errorf("a history restore: %s", excerpt(restore.body))
	}
	children := e.do(bob, http.MethodGet, "/browse/tree-children/main/img", nil, nav)
	if !strings.HasPrefix(children.body, `<li role="treeitem" aria-level="2">`) || !children.has(`data-path="img/logo.svg"`) || children.has("data-browser") {
		t.Errorf("a folder's rows: %s", excerpt(children.body))
	}
	if p := e.do(bob, http.MethodGet, "/browse/tree-children/main/nowhere", nil, nav); p.status != http.StatusNotFound {
		t.Errorf("the rows of a folder that is not there: %d", p.status)
	}
	history := e.get(bob, "/browse/commits?path=svc")
	if !history.has("a first change") || !history.has("CL 1") {
		t.Errorf("history of a directory: %s", excerpt(history.body))
	}
	if p := e.get(bob, "/browse/commits?path=nowhere"); !p.has("No commits found") {
		t.Errorf("history of a path nothing changed: %s", excerpt(p.body))
	}
	m := regexp.MustCompile(`href="/browse/commit/([0-9a-f]{64})"`).FindStringSubmatch(history.body)
	if m == nil {
		t.Fatalf("no commit link in %s", excerpt(history.body))
	}
	commit := e.get(bob, "/browse/commit/"+m[1])
	for _, want := range []string{"a first change", "CL 1", "svc/main.go", "README.md", "+5"} {
		if !commit.has(want) {
			t.Errorf("the commit page lacks %s: %s", want, excerpt(commit.body))
		}
	}
	at := e.get(bob, "/browse/tree/"+m[1]+"/svc")
	if !at.has("Back to main") {
		t.Errorf("browsing at a commit: %s", excerpt(at.body))
	}

	// Every page is under the site's policy: no inline script or style, no framing.
	for _, path := range []string{"/", "/browse", "/browse/tree/main/svc", "/browse/cls"} {
		h := e.get(bob, path).header
		if csp := h.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'self'") || !strings.Contains(csp, "frame-ancestors 'none'") ||
			h.Get("X-Frame-Options") != "DENY" || h.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%s's security headers: %v", path, h)
		}
	}

	// Raw bytes: nothing a browser could run is served live.
	for _, tc := range []struct{ path, ctype string }{
		{"/browse/raw/main/page.html", "text/plain; charset=utf-8"},
		{"/browse/raw/main/img/logo.svg", "image/svg+xml"},
		{"/browse/raw/main/svc/main.go", "text/plain; charset=utf-8"},
	} {
		p := e.get(bob, tc.path)
		if got := p.header.Get("Content-Type"); got != tc.ctype {
			t.Errorf("%s served as %q, want %q", tc.path, got, tc.ctype)
		}
		if csp := p.header.Get("Content-Security-Policy"); !strings.Contains(csp, "sandbox") || p.header.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%s is not sandboxed: %q", tc.path, csp)
		}
	}

	// The finder: search, and favorites first.
	e.post(bob, "/repositories/new", url.Values{"name": {"zebra"}})
	list := e.get(bob, "/repositories/list?q=zeb")
	if !list.has(">zebra<") || list.has(">browse<") {
		t.Errorf("search: %s", excerpt(list.body))
	}
	if p := e.do(bob, http.MethodPost, "/browse/action/star", url.Values{}, nil); p.status != http.StatusNoContent {
		t.Fatalf("favorite: %d", p.status)
	}
	rows := e.get(bob, "/").body
	if strings.Index(rows, ">browse<") > strings.Index(rows, ">zebra<") {
		t.Error("a favorite is not listed first")
	}

	// Refusals.
	if p := e.do(bob, http.MethodGet, "/nowhere", nil, nil); p.status != http.StatusNotFound {
		t.Errorf("an unknown repository: %d", p.status)
	}
	if p := e.do("", http.MethodGet, "/browse", nil, nil); p.status != http.StatusSeeOther || p.header.Get("Location") != "/id/login?next=%2Fbrowse" {
		t.Errorf("a request from no one signed in: %d, to %q", p.status, p.header.Get("Location"))
	}
	if p := e.do(bob, http.MethodPost, "/browse/action/unstar", url.Values{}, map[string]string{"Sec-Fetch-Site": "cross-site"}); p.status != http.StatusForbidden {
		t.Errorf("a cross-site post: %d", p.status)
	}
}

func TestChangeListLifecycle(t *testing.T) {
	e := newEnv(t)
	e.post(alice, "/repositories/new", url.Values{"name": {"svc"}})
	a := e.person(alice)
	b := e.person(bob)
	a.jj("", "clone", "svc")

	// main: an OWNERS file making bob the owner of lib/.
	a.write("svc/lib/OWNERS", "user: bob\n")
	a.write("svc/lib/lib.go", "package lib\n\nfunc One() int { return 1 }\n")
	a.jj("svc", "describe", "-m", "add lib")
	n := uploaded(a.jj("svc", "cl", "--ready"))[0]
	e.post(alice, clPath("svc", n)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("svc", n)
	e.submitted(alice, clPath("svc", n), "submit")
	a.jj("svc", "sync")

	// A draft: visible, quiet, not submittable.
	a.jj("svc", "new", "main@ok", "-m", "two from lib")
	a.write("svc/lib/lib.go", "package lib\n\nfunc One() int { return 1 }\n\nfunc Two() int { return 2 }\n")
	cl := uploaded(a.jj("svc", "cl"))[0]
	path := clPath("svc", cl)
	// Submit is never hidden: greyed, with the reason, for everyone.
	p := e.get(bob, path)
	if !chip(p, "Draft") || submitTitle(p, "Submit") != "Request review first: a draft can't be submitted" || canSubmit(p, "submit") || p.has("/request-review") {
		t.Fatalf("a draft seen by someone else: %s", excerpt(p.body))
	}
	if n := strings.Count(p.body, "two from lib"); n != 2 {
		t.Errorf("the title shows as the heading and the message's first line, %d times in all", n)
	}
	p = e.get(alice, path)
	if got := submitTitle(p, "Submit"); got != "Request review first: a draft can't be submitted" || !p.has(path+"/request-review") {
		t.Errorf("a draft's author: submit %q", got)
	}
	// The review dialog offers bob, lib's owner, pre-selected.
	if !regexp.MustCompile(`name="reviewer" value="bob.human.example.com"[^>]*checked`).MatchString(p.body) || !p.has("//lib/OWNERS") {
		t.Errorf("the review dialog: %s", excerpt(p.body))
	}
	if p := e.do(carol, http.MethodGet, path+"/reviewers/search?q=car", nil, nil); !p.has(`data-id="carol.human.example.com"`) {
		t.Errorf("reviewer search: %s", excerpt(p.body))
	}
	if n := notice(e.post(carol, path+"/request-review", url.Values{})); n != "Only the author or a repository admin can request review on this changelist" {
		t.Errorf("someone else requesting review: %q", n)
	}

	// Request review, with bob and a message.
	p = e.post(alice, path+"/request-review", url.Values{"reviewer": {bob}, "message": {"please look"}})
	if !chip(p, "Open") || !logged(p, "alice", "requested review") || !logged(p, "alice", "added reviewer bob") || !p.has("please look") {
		t.Fatalf("request review: %s", excerpt(p.body))
	}
	reason := submitTitle(p, "Submit")
	if !strings.Contains(reason, "LGTM") || !strings.Contains(reason, "Approval from //lib/OWNERS") {
		t.Errorf("an open CL's submit: %q", reason)
	}

	// bob comments on a line: unresolved, it holds the CL back.
	p = e.post(bob, path+"/comment", url.Values{"ps": {"1"}, "file": {"lib/lib.go"}, "line": {"5"}, "side": {"right"}, "content": {"name it Two?"}})
	if !p.has("name it Two?") || !strings.Contains(submitTitle(e.get(alice, path), "Submit"), "no unresolved comments (1 unresolved)") {
		t.Fatalf("an inline comment: %s", excerpt(p.body))
	}
	root := regexp.MustCompile(`/comment/(\d+)/resolve`).FindStringSubmatch(p.body)
	if root == nil {
		t.Fatalf("no resolve target: %s", excerpt(p.body))
	}

	// A second patchset moves the line; the thread follows it.
	a.write("svc/lib/lib.go", "package lib\n\n// One is one.\nfunc One() int { return 1 }\n\nfunc Two() int { return 2 }\n")
	if got := uploaded(a.jj("svc", "cl")); len(got) != 1 || got[0] != cl {
		t.Fatalf("a new patchset: %v", got)
	}
	e.waitFor("the new patchset in the change log", func() bool { return logged(e.get(alice, path), "alice", "uploaded patchset 2") })
	p = e.get(bob, path+"?ps=2")
	if !p.has("name it Two?") || !p.has("From Patchset 1") {
		t.Errorf("the thread on patchset 2: %s", excerpt(p.body))
	}
	// Its file's diff, which the row loads when opened, holds it on the line it moved to.
	if f := e.get(bob, path+"/file?"+url.Values{"ps": {"2"}, "path": {"lib/lib.go"}}.Encode()); !f.has("name it Two?") || !f.has("From Patchset 1") || !f.has("cl-comment-add") {
		t.Errorf("the thread in patchset 2's diff: %s", excerpt(f.body))
	}
	e.post(alice, path+"/comment?"+url.Values{"parent": {root[1]}, "ps": {"2"}}.Encode(), url.Values{"content": {"it is"}})
	e.post(alice, path+"/comment/"+root[1]+"/resolve?ps=2", nil)

	// bob approves: the CL is approved, and says so.
	p = e.post(bob, path+"/reply", grant("LGTM", "Approval"))
	if !logged(p, "bob", "granted") {
		t.Errorf("bob's grants: %s", excerpt(p.body))
	}
	e.waitFor("approval", func() bool { return e.hasEvent("svc", cl, db.EventApproved) })
	if list := e.get(alice, "/svc/cls"); !chip(list, "Approved") {
		t.Errorf("the finder's approved CL: %s", excerpt(list.body))
	}
	if n := notice(e.post(carol, path+"/submit", nil)); n != "Only the author or a repository admin can submit this changelist" {
		t.Errorf("someone else submitting: %q", n)
	}
	e.prePassed("svc", cl)
	if !canSubmit(e.get(alice, path), "submit") {
		t.Fatalf("alice cannot submit: %q", submitTitle(e.get(alice, path), "Submit"))
	}

	// Submit: merged, on main, and in history with its CL.
	p = e.submitted(alice, path, "submit")
	if !logged(p, "alice", "sent it to the landing queue") || !logged(p, "alice", "submitted to main as") {
		t.Fatalf("submit: %s", excerpt(p.body))
	}
	if h := e.get(bob, "/svc/commits"); !h.has("two from lib") || !h.has("CL 2") {
		t.Errorf("history: %s", excerpt(h.body))
	}
	if n := notice(e.post(alice, path+"/reply", grant("LGTM"))); !strings.Contains(n, "submitted") {
		t.Errorf("a grant on a submitted CL: %q", n)
	}

	// Abandon and restore; an admin may, someone else may not.
	b.jj("", "clone", "svc")
	b.write("svc/notes.txt", "notes\n")
	b.jj("svc", "describe", "-m", "notes")
	nb := uploaded(b.jj("svc", "cl"))[0]
	bpath := clPath("svc", nb)
	if n := notice(e.post(carol, bpath+"/abandon", url.Values{})); n != "Only the author or a repository admin can abandon this changelist" {
		t.Errorf("someone else abandoning: %q", n)
	}
	p = e.post(bob, bpath+"/abandon", url.Values{"reason": {"not needed"}})
	if !chip(p, "Abandoned") || !p.has("not needed") || !regexp.MustCompile(`jj abandon [k-z]{32}`).MatchString(p.body) {
		t.Fatalf("abandon: %s", excerpt(p.body))
	}
	if p := e.post(dana, bpath+"/restore", nil); !chip(p, "Draft") || !logged(p, "dana", "restored the change as a draft") {
		t.Fatalf("an admin restoring: %s", excerpt(p.body))
	}
	// The repository's creator administers it too.
	if p := e.post(alice, bpath+"/abandon", url.Values{}); !chip(p, "Abandoned") || !logged(p, "alice", "abandoned the change") {
		t.Errorf("the repository's creator abandoning someone else's CL: %s", excerpt(p.body))
	}
	if n := notice(e.post(alice, bpath+"/draft", nil)); !strings.Contains(n, "only an open CL returns to draft") {
		t.Errorf("returning an abandoned CL to draft: %q", n)
	}
}

func TestStacksConflictsAndOwners(t *testing.T) {
	e := newEnv(t)
	e.post(alice, "/repositories/new", url.Values{"name": {"stack"}})
	a := e.person(alice)
	b := e.person(bob)
	a.jj("", "clone", "stack")
	a.write("stack/README.md", "one\n")
	a.write("stack/core/OWNERS", "user: bob\n")
	a.write("stack/core/core.go", "package core\n")
	a.jj("stack", "describe", "-m", "base")
	n := uploaded(a.jj("stack", "cl", "--ready"))[0]
	e.post(alice, clPath("stack", n)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("stack", n)
	e.submitted(alice, clPath("stack", n), "submit")
	a.jj("stack", "sync")

	// bob's change to README, based on main before the stack lands.
	b.jj("", "clone", "stack")
	b.write("stack/README.md", "one, bob's way\n")
	b.jj("stack", "describe", "-m", "bob's readme")
	nb := uploaded(b.jj("stack", "cl", "--ready"))[0]

	// A stack of two: README, then docs on top of it.
	a.jj("stack", "new", "main@ok", "-m", "readme")
	a.write("stack/README.md", "one, two\n")
	a.jj("stack", "new", "-m", "docs")
	a.write("stack/docs/guide.md", "# Guide\n")
	stack := uploaded(a.jj("stack", "cl", "--ready"))
	if len(stack) != 2 {
		t.Fatalf("a stack: %v", stack)
	}
	bottom, top := clPath("stack", stack[0]), clPath("stack", stack[1])
	for _, p := range []string{bottom, top} {
		e.post(bob, p+"/reply", grant("LGTM"))
		e.post(alice, p+"/reply", grant("Approval"))
	}
	e.prePassed("stack", stack[0])
	e.prePassed("stack", stack[1])
	p := e.get(alice, top)
	if got := submitTitle(p, "Submit"); got != "Submit the parent changelist (CL "+strings.TrimPrefix(bottom, "/stack/cl/")+") first, or submit the stack" {
		t.Errorf("the top of a stack alone: %q", got)
	}
	if !canSubmit(p, "submit-stack") || !p.has("Submit Stack (2)") {
		t.Fatalf("submit stack: %q", submitTitle(p, "Submit Stack (2)"))
	}
	if p := e.submitted(alice, top, "submit-stack"); !logged(p, "alice", "sent its stack to the landing queue") {
		t.Errorf("the stack's change log: %s", excerpt(p.body))
	}
	if !e.landed(bottom) {
		t.Errorf("the bottom of the landed stack did not land")
	}
	a.jj("stack", "sync")

	// bob's README change now conflicts with main.
	e.post(alice, clPath("stack", nb)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("stack", nb)
	if got := submitTitle(e.get(bob, clPath("stack", nb)), "Submit"); !strings.Contains(got, "conflicts with main in README.md") {
		t.Errorf("a path conflict: %q", got)
	}

	// An open CL bob approved as core's owner loses its approval when a landing moves core
	// to carol.
	a.jj("stack", "new", "main@ok", "-m", "core change")
	a.write("stack/core/core.go", "package core\n\nconst V = 1\n")
	nc := uploaded(a.jj("stack", "cl", "--ready"))[0]
	e.post(bob, clPath("stack", nc)+"/reply", grant("LGTM", "Approval"))
	e.waitFor("approval", func() bool { return e.hasEvent("stack", nc, db.EventApproved) })

	a.jj("stack", "new", "main@ok", "-m", "carol owns core")
	a.write("stack/core/OWNERS", "user: carol\n")
	no := uploaded(a.jj("stack", "cl", "--ready"))[0]
	e.post(bob, clPath("stack", no)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("stack", no)
	e.submitted(alice, clPath("stack", no), "submit")
	e.waitFor("the lost approval", func() bool { return e.hasEvent("stack", nc, db.EventApprovalLost) })
	if got := submitTitle(e.get(alice, clPath("stack", nc)), "Submit"); !strings.Contains(got, "Approval from //core/OWNERS") {
		t.Errorf("after the OWNERS change: %q", got)
	}

	// Reviewers: removing one revokes their grants.
	p = e.get(alice, clPath("stack", nc))
	if !p.has("reviewers/remove?principal=bob.human.example.com") {
		t.Fatalf("no way to remove bob: %s", excerpt(p.body))
	}
	p = e.post(alice, clPath("stack", nc)+"/reviewers/remove?principal=bob.human.example.com", nil)
	if !logged(p, "alice", "removed reviewer bob") || !strings.Contains(submitTitle(p, "Submit"), "LGTM") {
		t.Errorf("removing a reviewer revokes their grants: %q\n%s", submitTitle(p, "Submit"), excerpt(p.body))
	}
}
