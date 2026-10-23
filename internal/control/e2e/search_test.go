package e2e

import (
	"net/url"
	"strings"
	"testing"
)

// TestCodeSearch searches a repository's main through ok control (docs/hosted.md, "ok control at
// scale, as built"): once landed, its files are indexed — a search before asks for it — and found by
// a literal, a regular expression, and within a path, each line linking to main's file.
func TestCodeSearch(t *testing.T) {
	e := newEnv(t)
	e.post(alice, "/repositories/new", url.Values{"name": {"searched"}})
	a := e.person(alice)
	a.jj("", "clone", "searched")
	a.write("searched/api/limit.go", "package api\n\n// RateLimit limits a caller.\nfunc RateLimit(q string) {}\n")
	a.write("searched/web/index.html", "<h1>RateLimit dashboard</h1>\n")
	a.jj("searched", "describe", "-m", "add the rate limiter")
	n := uploaded(a.jj("searched", "cl", "--ready"))[0]
	path := clPath("searched", n)
	e.post(alice, path+"/reply", grant("LGTM", "Approval"))
	e.prePassed("searched", n)
	e.submitted(alice, path, "submit")
	base := "/example/searched/search?"
	search := func(q url.Values) string { return e.get(bob, base+q.Encode()).body }
	e.waitFor("main indexed", func() bool {
		return strings.Contains(search(url.Values{"q": {"ratelimit"}}), `data-search-line`)
	})
	p := search(url.Values{"q": {"ratelimit"}})
	if strings.Count(p, `data-search-file`) != 2 || strings.Count(p, `data-search-line`) != 3 || !strings.Contains(p, "/tree/") {
		t.Errorf("ratelimit, in any case: %s", excerpt(p))
	}
	if p := search(url.Values{"q": {`func \w+\(`}, "regexp": {"1"}}); strings.Count(p, `data-search-line`) != 1 || !strings.Contains(p, "#L4") {
		t.Errorf("a regular expression: %s", excerpt(p))
	}
	if p := search(url.Values{"q": {"RateLimit"}, "case": {"1"}, "path": {"*.html"}}); strings.Count(p, `data-search-line`) != 1 ||
		!strings.Contains(p, "web/index.html") {
		t.Errorf("within *.html: %s", excerpt(p))
	}
	if p := search(url.Values{"q": {"("}, "regexp": {"1"}}); !strings.Contains(p, "missing closing )") {
		t.Errorf("a regular expression that does not parse: %s", excerpt(p))
	}
	if p := search(url.Values{"q": {"nowhere at all"}}); !strings.Contains(p, "Nothing in main matches.") {
		t.Errorf("nothing found: %s", excerpt(p))
	}
}
