package e2e

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"testing"
)

var (
	fileRowRe = regexp.MustCompile(`<details id="diff-[0-9a-f]+" class="[^"]*"( open)?>`)
	rowNameRe = regexp.MustCompile(`<span class="min-w-0 truncate text-zinc-200">([^<]+)</span>`)
	nextRe    = regexp.MustCompile(`hx-get="([^"]*/files\?[^"]*)"`)
	totalsRe  = regexp.MustCompile(`files? changed</span>\s*<span class="text-emerald-400">\+(\d+)</span>`)
)

// fileRows are a file list page's rows, in order, and whether each starts open.
func fileRows(p page) (names []string, open []bool) {
	for _, m := range rowNameRe.FindAllStringSubmatch(p.body, -1) {
		names = append(names, m[1])
	}
	for _, m := range fileRowRe.FindAllStringSubmatch(p.body, -1) {
		open = append(open, m[1] != "")
	}
	return names, open
}

// nextPage is a file list page's next page ("" for the last).
func nextPage(p page) string {
	if m := nextRe.FindStringSubmatch(p.body); m != nil {
		return m[1]
	}
	return ""
}

// allRows follows a file list from its first page to its last, returning every row's name and
// how many rows each page held.
func allRows(t *testing.T, e *env, first page) (names []string, pages []int) {
	t.Helper()
	p := first
	for {
		n, _ := fileRows(p)
		names = append(names, n...)
		pages = append(pages, len(n))
		next := nextPage(p)
		if next == "" {
			return names, pages
		}
		if p = e.get(bob, next); p.status != http.StatusOK {
			t.Fatalf("%s: %d", next, p.status)
		}
	}
}

// TestFileLists shows a change of any size: a CL's and a commit's files list a page of rows at a
// time, each loading its diff — with its review threads — only when opened; a small change's rows
// start open, and only a small change's lines are totaled; and the approvers name a few of many
// files.
func TestFileLists(t *testing.T) {
	e := newEnv(t)
	e.post(dana, "/repositories/new", url.Values{"name": {"many"}})
	b := e.person(bob)
	b.jj("", "clone", "many")
	const files = 350
	want := make([]string, files)
	for i := range files {
		want[i] = fmt.Sprintf("pkg/f%03d.txt", i)
		b.write("many/"+want[i], fmt.Sprintf("line %d\n", i))
	}
	b.jj("many", "describe", "-m", "many files")
	n := uploaded(b.jj("many", "cl", "--ready"))[0]
	path := clPath("many", n)

	// The page holds the first 100 rows, none open and none with a diff, and no line totals; the
	// approvers' title names 20 of the files.
	p := e.get(bob, path)
	names, open := fileRows(p)
	if len(names) != 100 || names[0] != want[0] || names[99] != want[99] || slices.Contains(open, true) {
		t.Fatalf("the first page: %d rows, open %v", len(names), open)
	}
	if !p.has(">350</span> files changed") || totalsRe.MatchString(p.body) || p.has("cl-comment-add") {
		t.Errorf("the CL's stat or a diff on its page: %s", excerpt(p.body))
	}
	if !p.has("… and 330 more") || p.has(want[files-1]) {
		t.Errorf("the approvers' files: %s", excerpt(p.body))
	}
	if len(p.body) > 500_000 {
		t.Errorf("the CL's page is %d bytes", len(p.body))
	}

	// Every page after it, in order: 100, 100, 100, 50.
	all, pages := allRows(t, e, p)
	if strings.Join(all, " ") != strings.Join(want, " ") || fmt.Sprint(pages) != "[100 100 100 50]" {
		t.Errorf("the pages: %v, %d rows", pages, len(all))
	}

	// A row's diff loads alone, and a path the change does not touch is not found.
	if f := e.get(bob, path+"/file?"+url.Values{"ps": {"1"}, "path": {"pkg/f007.txt"}}.Encode()); f.status != http.StatusOK ||
		!strings.Contains(f.body, "line 7") || !strings.Contains(f.body, "cl-comment-add") || strings.Contains(f.body, "line 8") {
		t.Errorf("a file's diff: %d %s", f.status, excerpt(f.body))
	}
	if f := e.do(bob, http.MethodGet, path+"/file?"+url.Values{"ps": {"1"}, "path": {"nope.txt"}}.Encode(), nil, nil); f.status != http.StatusNotFound {
		t.Errorf("a file the change does not touch: %d", f.status)
	}

	// A comment on a file of the third page: that row starts open and counts it, and its diff
	// holds it.
	c := e.post(alice, path+"/comment", url.Values{"ps": {"1"}, "file": {"pkg/f250.txt"}, "line": {"1"}, "side": {"right"}, "content": {"why 250?"}})
	root := regexp.MustCompile(`/comment/(\d+)/resolve`).FindStringSubmatch(c.body)
	if root == nil {
		t.Fatalf("no resolve target: %s", excerpt(c.body))
	}
	third := e.get(bob, path+"/files?"+url.Values{"ps": {"1"}, "page": {"3"}}.Encode())
	names, open = fileRows(third)
	for i, name := range names {
		if (name == "pkg/f250.txt") != open[i] {
			t.Errorf("row %s open: %v", name, open[i])
		}
	}
	if !third.has("1 Comment") {
		t.Errorf("the commented row: %s", excerpt(third.body))
	}
	if f := e.get(bob, path+"/file?"+url.Values{"ps": {"1"}, "path": {"pkg/f250.txt"}}.Encode()); !f.has("why 250?") || !f.has("line 250") {
		t.Errorf("the commented file's diff: %s", excerpt(f.body))
	}
	e.post(alice, path+"/comment/"+root[1]+"/resolve?ps=1", nil)

	// Landed, the commit lists its files the same way.
	e.post(alice, path+"/reply", grant("LGTM", "Approval"))
	e.prePassed("many", n)
	e.submitted(bob, path, "submit")
	cl, _, err := dbChangeList(e, "many", n)
	if err != nil {
		t.Fatal(err)
	}
	commit := "/many/commit/" + hex.EncodeToString(cl.LandedCommit)
	p = e.get(bob, commit)
	if all, pages := allRows(t, e, p); strings.Join(all, " ") != strings.Join(want, " ") || fmt.Sprint(pages) != "[100 100 100 50]" {
		t.Errorf("the commit's pages: %v, %d rows", pages, len(all))
	}
	if !p.has(">350</span> files changed") || totalsRe.MatchString(p.body) || p.has("<table") {
		t.Errorf("the commit's stat or a diff on its page: %s", excerpt(p.body))
	}
	if f := e.get(bob, commit+"/file?"+url.Values{"path": {"pkg/f001.txt"}}.Encode()); !f.has("line 1") {
		t.Errorf("a commit's file: %s", excerpt(f.body))
	}

	// A small change: its rows start open, and its lines are totaled.
	b.jj("many", "sync")
	b.jj("many", "new", "main@ok", "-m", "one line")
	b.write("many/pkg/f000.txt", "line zero\n")
	small := clPath("many", uploaded(b.jj("many", "cl"))[0])
	p = e.get(bob, small)
	names, open = fileRows(p)
	if fmt.Sprint(names) != "[pkg/f000.txt]" || fmt.Sprint(open) != "[true]" {
		t.Errorf("a small change's rows: %v %v", names, open)
	}
	if m := totalsRe.FindStringSubmatch(p.body); m == nil || m[1] != "1" || !p.has(`<span class="text-red-400">−1</span>`) {
		t.Errorf("a small change's totals: %s", excerpt(p.body))
	}
}
