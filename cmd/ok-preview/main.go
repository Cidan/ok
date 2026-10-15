// Command ok-preview is a backend-free preview harness for ok control's web UI
// (internal/control/pages): it serves every page — the repository finder, a repository, a file,
// blame, history, a commit, ChangeLists, checks, deployments, and services — on mock data, so the
// templ + htmx + Tailwind frontend can be iterated on without ok control or the SCM.
// Run: ADDR=:8093 go run ./cmd/ok-preview
package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	pages "github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/pages/servicegroup"
)

const pageSize = 20

func mockRepos(n int) []pages.RepoItem {
	ages := []string{"just now", "2 hours ago", "yesterday", "3 days ago", "2 weeks ago", "5 months ago"}
	items := make([]pages.RepoItem, n)
	for i := 0; i < n; i++ {
		items[i] = pages.RepoItem{
			Name:         fmt.Sprintf("repo-%03d", i+1),
			Detail:       fmt.Sprintf("%d commits · created by ada", (i*7)%90+1),
			Link:         fmt.Sprintf("/repo-%03d", i+1),
			Starred:      i%7 == 0,
			UpdatedUnix:  int64(1_700_000_000 - i*37_000),
			UpdatedAgo:   ages[i%len(ages)],
			UpdatedTitle: "Sep 2026",
		}
	}
	return items
}

// mockChrome builds the shared repo header/toolbar model for the preview.
func mockChrome(name string, crumbs []pages.Crumb) pages.RepoChrome {
	return pages.RepoChrome{
		Name:         name,
		Subtitle:     "Created by ada",
		Link:         "/" + name,
		CommitsCount: 42,
		CommitsHref:  "/" + name + "/commits",
		ChangesHref:  "/" + name + "/cls",
		Clone:        pages.CloneURLs{Command: "jj clone " + name},
		Crumbs:       crumbs,
	}
}

var mockReadmeHTML = `<h1>first-repo</h1>
<p>A first repository on ok, served by <strong>ok</strong> and cloned with ok's
<code>jj</code>.</p>
<h2>Getting started</h2>
<p>Clone the repository and start hacking:</p>
<pre class="chroma"><code>jj clone first-repo</code></pre>
<ul><li>Windowed infinite scroll</li><li>Server-rendered markup</li><li>Dark, sleek, fast</li></ul>`

var mockSourceLines = []string{
	`<span class="kn">package</span> <span class="nx">main</span>`,
	``,
	`<span class="kn">import</span> <span class="s">&#34;fmt&#34;</span>`,
	``,
	`<span class="kd">func</span> <span class="nf">main</span>() {`,
	`	<span class="nx">fmt</span>.<span class="nf">Println</span>(<span class="s">&#34;hello, ok&#34;</span>)`,
	`	<span class="c1">// A line longer than any pane is wide, which scrolls inside its panel and never widens the page: ` + strings.Repeat("and on ", 40) + `</span>`,
	`}`,
}

func mockRepoView(name string, empty bool) pages.RepoView {
	chrome := mockChrome(name, []pages.Crumb{{Label: name}})
	if empty {
		return pages.RepoView{Chrome: chrome, IsEmpty: true}
	}
	entries := mockEntries(name, "")
	return pages.RepoView{
		Chrome:  chrome,
		Entries: entries,
		Latest:  pages.LatestCommit{Present: true, Author: "ada", Message: "Wire up the serving node and health checks", Href: "#", Age: "2 days ago", AgeTitle: "Sep 18, 2026"},
		Readme:  &pages.ReadmeView{Name: "README.md", HTML: mockReadmeHTML},
	}
}

// mockFS is the preview repository's tree: each directory's entries, a directory's name ending
// in "/". testdata is long, to scroll the browser's tree.
var mockFS = func() map[string][]string {
	fs := map[string][]string{
		"":                         {"cmd/", "internal/", "testdata/", "web/", ".gitignore", "README.md", "go.mod", "main.go"},
		"cmd":                      {"ok/", "tool/"},
		"cmd/ok":                   {"flags.go", "main.go"},
		"cmd/tool":                 {"main.go"},
		"internal":                 {"serving/", "store/", "doc.go"},
		"internal/serving":         {"webnode/", "OWNERS", "health.go", "node.go"},
		"internal/serving/webnode": {"doc.go", "webnode.go", "webnode_test.go"},
		"internal/store":           {"README.md", "store.go", "store_test.go"},
		"web":                      {"app.css", "app.js", "index.templ"},
	}
	for i := range 60 {
		fs["testdata"] = append(fs["testdata"], fmt.Sprintf("case_%02d.golden", i))
	}
	return fs
}()

// mockIsDir reports whether p is a directory of the preview repository.
func mockIsDir(p string) bool {
	_, ok := mockFS[p]
	return ok
}

// mockExists reports whether p is in the preview repository.
func mockExists(p string) bool {
	if mockIsDir(p) {
		return true
	}
	dir, name := path.Split(p)
	return slices.Contains(mockFS[strings.TrimSuffix(dir, "/")], name)
}

// mockEntries are a directory's listing rows, directories first.
func mockEntries(name, dir string) []pages.EntryItem {
	var items []pages.EntryItem
	for i, e := range mockFS[dir] {
		n, isDir := strings.CutSuffix(e, "/")
		items = append(items, pages.EntryItem{Name: n, IsDir: isDir, Href: "/" + name + "/tree/main/" + path.Join(dir, n),
			CommitMsg: mockCommitTitles[i%len(mockCommitTitles)], CommitHref: "/" + name + "/commit/c0ffee", Age: "2 days ago", AgeTitle: "Sep 18, 2026"})
	}
	return items
}

// mockTree is dir's rows in the browser's tree: open along the way to current (and current, a
// directory), otherwise fetched when first opened.
func mockTree(name, dir, current string) []pages.TreeNode {
	depth := 0
	if dir != "" {
		depth = strings.Count(dir, "/") + 1
	}
	var nodes []pages.TreeNode
	for _, e := range mockFS[dir] {
		n, isDir := strings.CutSuffix(e, "/")
		p := path.Join(dir, n)
		node := pages.TreeNode{Name: n, Path: p, IsDir: isDir, Depth: depth, Href: "/" + name + "/tree/main/" + p, Current: p == current}
		if isDir {
			node.ChildrenID = "tree-" + strings.NewReplacer("/", "-", ".", "-").Replace(p)
			if current == p || strings.HasPrefix(current, p+"/") {
				node.Open, node.Children = true, mockTree(name, p, current)
			} else {
				node.ChildrenHref = "/" + name + "/tree-children/main/" + p
			}
		}
		nodes = append(nodes, node)
	}
	return nodes
}

// mockBrowse is the browser at p: a directory, a file, or (blame) a file's blame.
func mockBrowse(name, p string, blame bool) pages.BrowseView {
	crumbs := []pages.Crumb{{Label: name, Href: "/" + name, Target: "#content"}}
	parts := strings.Split(p, "/")
	for i, part := range parts {
		c := pages.Crumb{Label: part}
		if i < len(parts)-1 {
			c.Href, c.Target, c.Swap = "/"+name+"/tree/main/"+strings.Join(parts[:i+1], "/"), "#browse-main", pages.PaneSwap
		}
		crumbs = append(crumbs, c)
	}
	v := pages.BrowseView{Chrome: mockChrome(name, crumbs), Tree: mockTree(name, "", p), Path: p}
	switch {
	case blame:
		v.Kind, v.Blame = pages.BrowseBlameKind, &pages.BrowseBlame{FileHref: "/" + name + "/tree/main/" + p}
		for i, line := range mockSourceLines {
			l := pages.BlameLine{Number: i + 1, HTML: line, First: i == 0 || i == 4}
			if i < 4 {
				l.CommitShort, l.CommitTitle, l.Author, l.Age = "9adc24208a1f", "Initial commit", "ada", "5 days ago"
			} else {
				l.CommitShort, l.CommitTitle, l.Author, l.Age = "c0ffee00c0ff", "Say hello to ok", "grace", "2 days ago"
			}
			l.CommitHref, l.AgeTitle = "/"+name+"/commit/"+l.CommitShort, "Sep 2026"
			v.Blame.Lines = append(v.Blame.Lines, l)
		}
	case mockIsDir(p):
		parent := path.Dir(p)
		if parent == "." {
			parent = ""
		}
		d := &pages.BrowseDir{Entries: mockEntries(name, p), ParentRoot: parent == "", ParentHref: "/" + name + "/tree/main/" + parent,
			Latest: pages.LatestCommit{Present: true, Author: "ada", Message: "Wire up the serving node and health checks", Href: "/" + name + "/commit/c0ffee", Age: "2 days ago", AgeTitle: "Sep 18, 2026"}}
		if d.ParentRoot {
			d.ParentHref = "/" + name
		}
		if slices.Contains(mockFS[p], "README.md") {
			d.Readme = &pages.ReadmeView{Name: "README.md", HTML: mockReadmeHTML}
		}
		v.Kind, v.Dir = pages.BrowseDirKind, d
	default:
		f := &pages.FileBlob{Name: path.Base(p), SizeHuman: "412 B", RawHref: "#", HistoryHref: "/" + name + "/commits?path=" + p, BlameHref: "/" + name + "/blame/main/" + p}
		switch {
		case strings.HasSuffix(p, ".md"):
			f.Mode, f.HTML = "markup", mockReadmeHTML
		case strings.HasSuffix(p, ".go"):
			// A long file, to scroll the pane.
			f.Mode, f.LexerName, f.SizeHuman = "source", "Go", "6.1 KiB"
			for range 30 {
				f.Lines = append(f.Lines, mockSourceLines...)
			}
		default:
			f.Mode, f.Lines = "source", []string{"ada", "grace"}
		}
		v.Kind, v.File = pages.BrowseFileKind, f
	}
	return v
}

var mockCommitTitles = []string{
	"Wire up the serving node and health checks",
	"Serving node graph + bootstrap",
	"Add multi-call CLI entrypoint",
	"Document the boot order",
	"Render markup server-side; drop the client md lib",
	"Initial commit",
}

func mockCommits(name string) pages.CommitsView {
	chrome := mockChrome(name, []pages.Crumb{{Label: name, Href: "/" + name}})
	ages := []string{"just now", "2 hours ago", "yesterday", "2 days ago", "5 days ago", "3 weeks ago"}
	rows := make([]pages.CommitRow, len(mockCommitTitles))
	for i, t := range mockCommitTitles {
		sha := fmt.Sprintf("%040x", 0xc0ffee+i*0x1d1d1d)
		row := pages.CommitRow{
			SHA: sha, ShortSHA: sha[:10],
			Href:       "/" + name + "/commit/" + sha,
			BrowseHref: "/" + name + "/tree/" + sha,
			Author:     "ada", Title: t,
			Age: ages[i%len(ages)], AgeTitle: "Sep 2026",
		}
		if i%2 == 0 {
			row.CL, row.CLHref = int64(12-i), fmt.Sprintf("/%s/cl/%d", name, 12-i)
		}
		rows[i] = row
	}
	return pages.CommitsView{
		Chrome: chrome, Commits: rows, Count: 42,
		NextHref: "/" + name + "/commits?page=o37",
	}
}

func mockCommitDiff(name string) pages.CommitDiffView {
	chrome := mockChrome(name, []pages.Crumb{{Label: name, Href: "/" + name}})
	sha := fmt.Sprintf("%040x", 0xc0ffee)
	added := func(l, r int, html string) pages.DiffLineRow {
		return pages.DiffLineRow{Type: "add", LeftIdx: l, RightIdx: r, Content: html}
	}
	del := func(l, r int, html string) pages.DiffLineRow {
		return pages.DiffLineRow{Type: "del", LeftIdx: l, RightIdx: r, Content: html}
	}
	same := func(l, r int, html string) pages.DiffLineRow {
		return pages.DiffLineRow{Type: "same", LeftIdx: l, RightIdx: r, Content: html}
	}
	hunk := func(html string) pages.DiffLineRow { return pages.DiffLineRow{Type: "tag", Content: html} }
	files := []pages.DiffFileView{
		{
			Name: "internal/serving/webnode/webnode.go", Anchor: "diff-a1", Status: "modified", Addition: 3, Deletion: 1,
			Sections: [][]pages.DiffLineRow{{
				hunk("@@ -17,7 +17,9 @@ func (n *WebNode) Serve(ctx context.Context) error {"),
				same(17, 17, `	<span class="nx">mux</span> := <span class="nx">http</span>.<span class="nf">NewServeMux</span>()`),
				same(18, 18, `	<span class="nx">mux</span>.<span class="nf">Handle</span>(<span class="s">&#34;/&#34;</span>, <span class="nx">n</span>.<span class="nx">router</span>)`),
				del(19, 0, `	<span class="k">return</span> <span class="nx">http</span>.<span class="nf">ListenAndServe</span>(<span class="s">&#34;<span class="removed-code">:3000</span>&#34;</span>, <span class="nx">mux</span>)`),
				added(0, 19, `	<span class="c1">// the runtime owns the listener; serve h2c over its net.Listener.</span>`),
				added(0, 20, `	<span class="k">return</span> <span class="nx">n</span>.<span class="nf">serve</span>(<span class="nx">ctx</span>, <span class="s">&#34;<span class="added-code">:8472</span>&#34;</span>, <span class="nx">mux</span>)`),
				same(20, 21, `}`),
			}},
		},
		{
			Name: "internal/serving/webnode/doc.go", Anchor: "diff-a2", Status: "added", Addition: 2, Deletion: 0,
			Sections: [][]pages.DiffLineRow{{
				hunk("@@ -0,0 +1,2 @@"),
				added(0, 1, `<span class="c1">// Package webnode is ok&#39;s web serving node.</span>`),
				added(0, 2, `<span class="kn">package</span> <span class="nx">webnode</span>`),
			}},
		},
		{Name: "assets/logo.png", Anchor: "diff-a3", Status: "modified", IsBinary: true},
	}
	return pages.CommitDiffView{
		Chrome: chrome, SHA: sha, ShortSHA: sha[:12], ChangeID: "qpvuntsmwlqt", CL: 3, CLHref: "/" + name + "/cl/3",
		Title:  "Serve the web node over the serving runtime instead of a raw HTTP listener",
		Body:   "the runtime owns the net.Listener and terminates h2c, so the node must\nhand its mux to the runtime rather than call ListenAndServe itself.",
		Author: "ada", Age: "2 days ago", AgeTitle: "Sep 18, 2026",
		Parents:  []pages.ParentRef{{ShortSHA: "9adc24208a", Href: "/" + name + "/commit/9adc24208a"}},
		NumFiles: 3, Addition: 5, Deletion: 1, TotalsKnown: true, Files: files,
	}
}

// mockCommitPage is the mock commit's page: its files as rows, each loading its diff when opened.
func mockCommitPage(name string) pages.CommitDiffView {
	v := mockCommitDiff(name)
	v.Files = asRows(v.Files, "/"+name+"/commit/"+v.SHA+"/file", url.Values{})
	return v
}

// asRows is a mock diff's files as a file list's rows: no diff, each loading its own from body.
func asRows(files []pages.DiffFileView, body string, q url.Values) []pages.DiffFileView {
	out := make([]pages.DiffFileView, len(files))
	for i, f := range files {
		n := len(f.Threads)
		for _, sec := range f.Sections {
			for _, ln := range sec {
				n += len(ln.Threads)
			}
		}
		fq := url.Values{"path": {f.Name}}
		for k, v := range q {
			fq[k] = v
		}
		row := pages.DiffFileView{Name: f.Name, OldName: f.OldName, Anchor: f.Anchor, Status: f.Status, Addition: f.Addition,
			Deletion: f.Deletion, IsBinary: f.IsBinary, Collapsed: f.Collapsed, TooLarge: f.TooLarge, OffDiff: f.OffDiff,
			ThreadCount: n, BodyHref: body + "?" + fq.Encode()}
		if row.OldName == "" {
			row.OldName = row.Name
		}
		row.Open = n > 0 || (len(files) <= 20 && !f.Collapsed)
		out[i] = row
	}
	return out
}

// fileNamed is the mock file at path ("" when there is none).
func fileNamed(files []pages.DiffFileView, path string) (pages.DiffFileView, bool) {
	for _, f := range files {
		if f.Name == path {
			return f, true
		}
	}
	return pages.DiffFileView{}, false
}

// manyFiles is how many files the large mock commit changes, and manyPage its rows to a page.
const (
	manyFiles = 250
	manyPage  = 100
)

// mockManyRows is page n of the large mock commit's rows, and its next page's URL.
func mockManyRows(name string, n int) ([]pages.DiffFileView, string) {
	base := "/" + name + "/commit/many"
	var rows []pages.DiffFileView
	for i := (n - 1) * manyPage; i < min(n*manyPage, manyFiles); i++ {
		p := fmt.Sprintf("third_party/go/example.com/mod%03d/mod.go", i)
		rows = append(rows, pages.DiffFileView{Name: p, OldName: p, Anchor: fmt.Sprintf("diff-m%03d", i), Status: "added",
			Addition: 10 + i%7, BodyHref: base + "/file?" + url.Values{"path": {p}}.Encode()})
	}
	next := ""
	if n*manyPage < manyFiles {
		next = fmt.Sprintf("%s/files?page=%d", base, n+1)
	}
	return rows, next
}

func mockCLList(name string) pages.CLListView {
	chrome := mockChrome(name, []pages.Crumb{{Label: name, Href: "/" + name}})
	mk := func(n int64, title, kind, label, age string) pages.CLItem {
		return pages.CLItem{
			Number: n, Title: title, Author: "ada", StatusKind: kind, StatusLabel: label,
			Age: age, AgeTitle: "Sep 2026", Href: fmt.Sprintf("/%s/cl/%d", name, n),
		}
	}
	rollback := func(it pages.CLItem) pages.CLItem {
		it.Rollback = true
		return it
	}
	base := "/" + name + "/cls"
	filter := func(label, f string, active bool) pages.CLFilter {
		href := base
		if f != "" {
			href += "?f=" + f
		}
		return pages.CLFilter{Label: label, Href: href, Active: active}
	}
	return pages.CLListView{
		Chrome: chrome,
		Filters: []pages.CLFilter{
			filter("Open", "", true), filter("Drafts", "drafts", false), filter("Needs My Review", "review", false),
			filter("Merged", "merged", false), filter("Abandoned", "abandoned", false), filter("All", "all", false),
		},
		Empty: "No open changes.",
		Items: []pages.CLItem{
			rollback(mk(12, "Roll back CLs 11 and 13: cache the manifest per request", "open", "Open", "2 minutes ago")),
			mk(9, "empty change", "draft", "Draft", "just now"),
			mk(3, "wire limiter into the gateway", "approved", "Approved", "1 hour ago"),
			mk(2, "add rate limiter", "open", "Open", "2 hours ago"),
		},
	}
}

// mockReviewDialog is the review dialog for a CL whose //internal/serving/OWNERS set still needs
// Approval: grace (pre-selected, recently active) or lin, plus recent people and the search.
func mockReviewDialog(name string, n int64, request bool) *pages.CLReviewDialog {
	base := fmt.Sprintf("/%s/cl/%d", name, n)
	d := &pages.CLReviewDialog{
		Title: "Add Reviewers", Action: "Add", Href: base + "/reviewers", SearchHref: base + "/reviewers/search",
		OwnerSets: []pages.CLSuggestSet{{
			Label: "//internal/serving/OWNERS (per-file *.go)",
			Candidates: []pages.CLCandidate{
				{Principal: "grace.human.example.com", Name: "grace", Selected: true},
				{Principal: "lin.human.example.com", Name: "lin"},
			},
		}},
		Recent: []pages.CLCandidate{{Principal: "sam.human.example.com", Name: "sam"}, {Principal: "priya.human.example.com", Name: "priya"}},
	}
	if request {
		d.Title, d.Action, d.Href = "Request Review", "Send", base+"/request-review"
	}
	return d
}

// mockCLView is the mock CL's page: its files as rows, each loading its diff and threads when
// opened.
func mockCLView(name string) pages.CLView {
	v := mockCLViewFull(name)
	v.Files = asRows(v.Files, fmt.Sprintf("/%s/cl/2/file", name), url.Values{"ps": {"2"}})
	return v
}

// mockCLViewFull is the mock CL with every file's diff and threads, as its rows load them.
func mockCLViewFull(name string) pages.CLView {
	chrome := mockChrome(name, []pages.Crumb{{Label: name, Href: "/" + name}})
	d := mockCommitDiff(name) // reuse the mock diff files
	rel := func(n int64, t string, cur bool) pages.CLRelated {
		return pages.CLRelated{Number: n, Title: t, Href: fmt.Sprintf("/%s/cl/%d", name, n), Current: cur}
	}
	base := fmt.Sprintf("/%s/cl/2", name)
	thread := pages.CommentThread{
		RootID: 1, FilePath: d.Files[0].Name, Side: "right", Line: 20, PatchSet: 2,
		Comments: []pages.CommentView{
			{ID: 1, Author: "grace", Body: "Should the port come from config instead of a literal?", Age: "1 hour ago", AgeTitle: "Sep 21, 2026"},
			{ID: 2, Author: "ada", Body: "the runtime injects it — this is the default only.", Age: "40 minutes ago", AgeTitle: "Sep 21, 2026"},
		},
		ReplyHref: base + "/comment", ResolveHref: base + "/resolve", UnresolveHref: base + "/unresolve",
	}
	outdated := pages.CommentThread{
		RootID: 3, FilePath: d.Files[1].Name, Side: "left", Line: 1, PatchSet: 1, Resolved: true, Outdated: true,
		Comments:  []pages.CommentView{{ID: 3, Author: "lin", Body: "Package doc reads well.", Age: "2 hours ago", AgeTitle: "Sep 21, 2026"}},
		ReplyHref: base + "/comment", ResolveHref: base + "/resolve", UnresolveHref: base + "/unresolve",
	}
	// Patchset 2 is viewed. Unresolved threads from patchset 1 are ported onto it: one whose line
	// moved down, one whose line was rewritten, one on a file patchset 2 leaves unchanged (shown with
	// the lines around it), and one on a file patchset 2 no longer has.
	psHref := func(n int64) string { return base + "?" + url.Values{"ps": {strconv.FormatInt(n, 10)}}.Encode() }
	thread.PatchSetHref, outdated.PatchSetHref = psHref(2), psHref(1)
	ported := func(root int64, file string, line int, port string, origLine int, author, body string) pages.CommentThread {
		return pages.CommentThread{
			RootID: root, FilePath: file, Side: "right", Line: line, PatchSet: 1, PatchSetHref: psHref(1),
			Port: port, OrigFilePath: file, OrigLine: origLine,
			Comments:  []pages.CommentView{{ID: root, Author: author, Body: body, Age: "3 hours ago", AgeTitle: "Sep 21, 2026"}},
			ReplyHref: base + "/comment", ResolveHref: base + "/resolve", UnresolveHref: base + "/unresolve",
		}
	}
	d.Files[0].Sections[0][4].Threads = []pages.CommentThread{
		ported(4, d.Files[0].Name, 19, "exact", 17, "lin", "Link the serving doc from this comment."),
	}
	d.Files[1].Sections[0][2].Threads = []pages.CommentThread{
		ported(5, d.Files[1].Name, 2, "changed", 2, "grace", "Name the package after the node it serves."),
	}
	same := func(n int, html string) pages.DiffLineRow {
		return pages.DiffLineRow{Type: "same", LeftIdx: n, RightIdx: n, Content: html}
	}
	serving := ported(6, "internal/serving/serving.go", 41, "exact", 41, "grace", "Does the assembly still need the web node first?")
	nextLine := same(42, `	<span class="nx">nodes</span> = <span class="nb">append</span>(<span class="nx">nodes</span>, <span class="nx">webnode</span>.<span class="nf">New</span>())`)
	commented := same(41, `	<span class="nx">nodes</span> := []<span class="nx">node</span>.<span class="nx">Node</span>{<span class="nx">bootstrap</span>.<span class="nf">New</span>()}`)
	commented.Threads = []pages.CommentThread{serving}
	d.Files = append(d.Files,
		pages.DiffFileView{
			Name: serving.FilePath, OldName: serving.FilePath, Anchor: "diff-a4", Status: "unmodified", OffDiff: true,
			Sections: [][]pages.DiffLineRow{{
				{Type: "tag", Content: "@@ -38,7 +38,7 @@"},
				same(38, `<span class="c1">// Assembly is the root node: the runtime sets up its dependencies first.</span>`),
				same(39, `<span class="kd">func</span> <span class="nf">Assembly</span>() <span class="nx">node</span>.<span class="nx">Node</span> {`),
				same(40, `	<span class="c1">// bootstrap runs Gitea&#39;s global init before any handler is mounted.</span>`),
				commented, nextLine,
				same(43, `	<span class="k">return</span> <span class="nx">node</span>.<span class="nf">Group</span>(<span class="nx">nodes</span>...)`),
				same(44, `}`),
			}},
		},
		pages.DiffFileView{
			Name: "internal/serving/legacy.go", OldName: "internal/serving/legacy.go", Anchor: "diff-a5", Status: "removed", OffDiff: true,
			Threads: []pages.CommentThread{
				ported(7, "internal/serving/legacy.go", 0, "deleted", 12, "lin", "Is anything still importing the legacy listener?"),
			},
		},
	)
	d.Files[0].Sections[0][5].Threads = []pages.CommentThread{thread}
	return pages.CLView{
		ShowSubmit: true, SubmitBlockedReason: "Needs Approval from //internal/serving/OWNERS (per-file *.go), no unresolved comments (4 unresolved)",
		SubmitHref: base + "/submit", ReplyHref: base + "/reply",
		AddReviewers: mockReviewDialog(name, 2, false), ReturnToDraftHref: base + "/draft", AbandonHref: base + "/abandon",
		ReplyLabels: []pages.CLReplyLabel{{Name: "LGTM", Granted: true}, {Name: "Approval"}},
		Checks:      mockChecks(name, 2),
		OwnerSets: []pages.CLOwnerSet{
			{
				Label: "//internal/serving/OWNERS (per-file *.go)", Href: "/" + name + "/tree/main/internal/serving/OWNERS",
				Owners: []string{"grace", "lin"}, NumFiles: 2, FilesTitle: "internal/serving/webnode/doc.go\ninternal/serving/webnode/webnode.go",
			},
			{
				Label: "//OWNERS", Href: "/" + name + "/tree/main/OWNERS", Owners: []string{"ada", "grace"},
				NumFiles: 1, FilesTitle: "assets/logo.png", Satisfied: true, Implicit: true, AllowSelf: true,
			},
		},
		CommentHref: base + "/comment", CommentCount: 6,
		AllThreads: []pages.CommentThread{
			d.Files[4].Threads[0], serving, outdated, d.Files[1].Sections[0][2].Threads[0], d.Files[0].Sections[0][4].Threads[0], thread,
		},

		Chrome: chrome, Number: 2, Title: "add rate limiter", StatusLabel: "Open", StatusKind: "open",
		Author: "ada", ChangeID: "zxcvbnmakqsw", CanReply: true,
		Age: "2 hours ago", AgeTitle: "Sep 21, 2026",
		Reviewers: []pages.CLReviewer{
			{Name: "grace", Labels: []string{"LGTM"}, RemoveHref: base + "/reviewers/remove?principal=grace.human.example.com"},
			{Name: "lin", Labels: []string{"LGTM"}, RemoveHref: base + "/reviewers/remove?principal=lin.human.example.com"},
		},
		SubmitReqs: []pages.CLSubmitReq{
			{Label: "LGTM", Satisfied: true, GrantedBy: []string{"grace", "lin"}},
			{Label: "Approval", Satisfied: false, Detail: "1 of 2 owner sets"},
			{Label: "Unresolved Comments", Satisfied: false, Detail: "4"},
			{Label: "Pre", Detail: "Failed"},
		},
		Message:         "Protects upstreams from bursty clients; configurable per-route.",
		ShowSubmitStack: true, StackSize: 2, SubmitStackHref: base + "/submit-stack",
		SubmitStackBlockedReason: "CL 2: needs Approval from //internal/serving/OWNERS (per-file *.go)",
		CommitShort:              "d41e0c7a9b", CurrentPatchSet: 2,
		NumFiles: d.NumFiles, Addition: d.Addition, Deletion: d.Deletion, TotalsKnown: true, Files: d.Files,
		PatchSets: []pages.CLPatchSet{
			{Number: 1, CommitShort: "c25b745123", Href: psHref(1)},
			{Number: 2, CommitShort: "d41e0c7a9b", Href: psHref(2), Current: true},
		},
		Related: []pages.CLRelated{
			rel(1, "serve the web node over the serving runtime", false),
			rel(2, "add rate limiter", true),
			rel(3, "wire limiter into the gateway", false),
		},
		Messages: []pages.CLMessage{
			{Author: "ada", Action: "uploaded patchset 1", PatchSet: 1, Age: "2 hours ago", AgeTitle: "Sep 21, 2026"},
			{Author: "ada", Action: "requested review", PatchSet: 1, Age: "2 hours ago", AgeTitle: "Sep 21, 2026"},
			{Author: "ada", Action: "added reviewer grace", PatchSet: 1, Age: "2 hours ago", AgeTitle: "Sep 21, 2026"},
			{Author: "grace", Action: "granted", Label: "LGTM", PatchSet: 1, Age: "1 hour ago", AgeTitle: "Sep 21, 2026"},
			{Author: "lin", Action: "revoked", Label: "Approval", Revoked: true, PatchSet: 1, Age: "50 minutes ago", AgeTitle: "Sep 21, 2026"},
			{Author: "lin", Action: "granted", Label: "LGTM", PatchSet: 1, Age: "45 minutes ago", AgeTitle: "Sep 21, 2026"},
			{Author: "grace", Action: "commented", Body: "Nice — ready once Approval lands.", PatchSet: 1, Age: "1 hour ago", AgeTitle: "Sep 21, 2026"},
			{Author: "ada", Action: "uploaded patchset 2", PatchSet: 2, Age: "30 minutes ago", AgeTitle: "Sep 21, 2026"},
		},
	}
}

// mockCLViewSubmitted is the bottom of the stack after submit: closed, requirements met, and the
// change log ending in the landed commit. Its Reply only comments.
func mockCLViewSubmitted(name string) pages.CLView {
	v := mockCLView(name)
	v.Number, v.Title, v.StatusLabel, v.StatusKind = 1, "serve the web node over the serving runtime", "Merged", "merged"
	v.ShowSubmit, v.SubmitBlockedReason, v.ReplyLabels = false, "", nil
	v.Checks.Live = false
	v.Checks.Post = mockPost(name, 1, "Landed")
	v.ShowSubmitStack = false
	v.AddReviewers, v.ReturnToDraftHref, v.AbandonHref = nil, "", ""
	v.RollbackHref = fmt.Sprintf("/%s/cl/1/rollback", name)
	v.Deploys = mockDeploys(name)
	for i := range v.Reviewers {
		v.Reviewers[i].RemoveHref = ""
	}
	v.SubmitReqs = []pages.CLSubmitReq{
		{Label: "LGTM", Satisfied: true, GrantedBy: []string{"grace"}},
		{Label: "Approval", Satisfied: true, Detail: "2 of 2 owner sets"},
		{Label: "Unresolved Comments", Satisfied: true, Detail: "None"},
		{Label: "Pre", Satisfied: true, Detail: "Passed"},
	}
	for i := range v.Related {
		v.Related[i].Current = v.Related[i].Number == 1
	}
	v.Messages = append(v.Messages,
		pages.CLMessage{Author: "ok", Action: "every review requirement is met", PatchSet: 1, Age: "12 minutes ago", AgeTitle: "Sep 21, 2026"},
		pages.CLMessage{
			Author: "ada", Action: "submitted to main as", Commit: "f26bb8f4a4",
			CommitHref: "/" + name + "/commit/f26bb8f4a44eef3f25dc88f1f15ab7681cc797b1",
			PatchSet:   1, Age: "10 minutes ago", AgeTitle: "Sep 21, 2026",
		})
	return v
}

// mockCLViewRolledBack is a merged CL an open rollback rolls back: its Rolled Back panel names the
// rollback, which it offers instead of Roll Back, and its change log says who rolled it back.
func mockCLViewRolledBack(name string) pages.CLView {
	v := mockCLViewSubmitted(name)
	v.Number, v.Title, v.RollbackHref = 11, "cache the manifest per request", ""
	v.Related = nil
	v.RolledBackBy = []pages.CLLink{{Number: 12, Title: "Roll back CLs 11 and 13: cache the manifest per request",
		Href: fmt.Sprintf("/%s/cl/12", name), StatusLabel: "Open", StatusKind: "open"}}
	v.Messages = append(v.Messages, pages.CLMessage{Author: "lin", Action: "rolled the change back in", CL: 12,
		CLHref: fmt.Sprintf("/%s/cl/12", name), PatchSet: 1, Age: "2 minutes ago", AgeTitle: "Sep 29, 2026"})
	return v
}

// mockCLViewRollback is a rollback CL: it rolls back a stack of two, left a path main changed
// since — with the commands that stack a CL on it to roll that back by hand — and needs only an
// LGTM from anyone but its author.
func mockCLViewRollback(name string) pages.CLView {
	v := mockCLViewBare(name)
	base := fmt.Sprintf("/%s/cl/12", name)
	v.Number, v.StatusLabel, v.StatusKind, v.Author = 12, "Open", "open", "lin"
	v.Title = "Roll back CLs 11 and 13: cache the manifest per request"
	v.Message = "The cache serves stale manifests after a push.\n\nThis rolls back CL 11, landed as 3f09a1c2e7, and the CLs landed above it in its stack:\n- CL 13, landed as 7ab2c90e14: invalidate the manifest cache on push"
	v.RequestReview, v.SubmitBlockedReason = nil, "Needs LGTM"
	v.SubmitReqs = []pages.CLSubmitReq{
		{Label: "LGTM"}, {Label: "Approval", Satisfied: true, Detail: "Not needed: a rollback"},
		{Label: "Unresolved Comments", Satisfied: true, Detail: "None"},
		{Label: "Pre", Satisfied: true, Detail: "Passed"},
	}
	v.ReplyLabels = nil
	v.IsRollback = true
	v.RollsBack = []pages.CLLink{
		{Number: 11, Title: "cache the manifest per request", Href: fmt.Sprintf("/%s/cl/11", name), StatusLabel: "Merged", StatusKind: "merged"},
		{Number: 13, Title: "invalidate the manifest cache on push", Href: fmt.Sprintf("/%s/cl/13", name), StatusLabel: "Merged", StatusKind: "merged"},
	}
	v.RollbackLeft = []string{"internal/serving/manifest.go"}
	v.GetCommand, v.StackCommand = "jj cl get 12", "jj new cl/12@ok"
	v.ReturnToDraftHref, v.AbandonHref = base+"/draft", base+"/abandon"
	v.Reviewers = []pages.CLReviewer{{Name: "ada"}, {Name: "grace"}}
	v.Messages = []pages.CLMessage{
		{Author: "lin", Action: "uploaded patchset 1", PatchSet: 1, Age: "2 minutes ago", AgeTitle: "Sep 29, 2026"},
		{Author: "lin", Action: "added reviewer ada", PatchSet: 1, Age: "2 minutes ago", AgeTitle: "Sep 29, 2026"},
		{Author: "lin", Action: "added reviewer grace", PatchSet: 1, Age: "2 minutes ago", AgeTitle: "Sep 29, 2026"},
	}
	return v
}

// mockDeployment is a deployment's page: 7 rolled back its canary, and offers Retry; 8 waits for
// an allowance of its migration's hazards, and offers Allow Hazards and Halt.
func mockDeployment(name string, id int64) pages.DeploymentView {
	chrome := mockChrome(name, []pages.Crumb{{Label: name, Href: "/" + name}})
	base := fmt.Sprintf("/%s/deploy/%d", name, id)
	v := pages.DeploymentView{
		Chrome: chrome, CLNumber: 1, CLHref: fmt.Sprintf("/%s/cl/1", name), ID: id,
		Commit: "f26bb8f4a4", CommitHref: "/" + name + "/commit/f26bb8f4a44eef3f25dc88f1f15ab7681cc797b1",
		Wave: "1 of plan 12", RequestedBy: "deploy.ok", StatusHref: base + "/status?cl=1",
		Started: "2026-09-29 12:01:04 UTC",
		Events: []pages.DeploymentEvent{
			{At: "6 minutes ago", AtTitle: "Sep 29, 2026", Message: "planned at f26bb8f4a44e: container web's image //web:push is affected: //web:push —image→ //web:image —binary→ //web:web —deps→ //lib/go/serve:serve, which holds libraries/go/serve/serving/serving.go"},
			{At: "6 minutes ago", AtTitle: "Sep 29, 2026", Message: "started"},
			{At: "5 minutes ago", AtTitle: "Sep 29, 2026", Message: "pushing container web's image, //web:push (run 4121)"},
			{At: "5 minutes ago", AtTitle: "Sep 29, 2026", Message: "pushed container web's image: registry.example.com/apps/web@sha256:3f09a1c2e7d44b1a9f0e6d2b8c5a7e4f1d3b9c0a2e8f6d4b7c1a5e9f3d2b8c6a"},
		},
	}
	switch id {
	case 8:
		v.Name, v.Deploy, v.Why = "data", "//data/.ok/workflow/data.yaml:deploy:data", "its migration //data/schema:apply is affected: //data/schema:apply —schema→ //data/schema:schema, which holds data/schema/data.sdl"
		v.StatusLabel, v.StatusKind, v.Stage = "Needs Allowance", "hazard", "Waiting for an Allowance"
		v.Live, v.AllowHref, v.HaltHref = true, base+"/allow?cl=1", base+"/halt?cl=1"
		v.Migrations = []pages.DeploymentMigration{{Target: "//data/schema:apply", StatusLabel: "Needs Allowance", StatusKind: "hazard",
			Refused: []string{"DELETES_DATA"}, Output: "database \"data\" on \"db\"\nplan: 2 step(s)\nrefusing to apply: the plan carries gated hazards",
			Steps: []pages.MigrationStep{
				{Description: "drop column public.events.legacy_id", SQL: "ALTER TABLE public.events DROP COLUMN legacy_id",
					Hazards: []pages.MigrationHazard{{Class: "DELETES_DATA", Detail: "drops public.events.legacy_id", Gated: true}, {Class: "ACCESS_EXCLUSIVE_LOCK", Detail: "locks public.events"}}},
				{Description: "create index public.events_at", SQL: "CREATE INDEX CONCURRENTLY events_at ON public.events (at)",
					Hazards: []pages.MigrationHazard{{Class: "INDEX_BUILD", Detail: "builds an index on public.events"}}},
			}}}
		v.Events = append(v.Events, pages.DeploymentEvent{At: "2 minutes ago", AtTitle: "Sep 29, 2026",
			Message: "//data/schema:apply's plan carries gated hazards (DELETES_DATA): it waits for a repository admin to allow them"})
	default:
		v.Name, v.Deploy, v.Why = "web", "//web/.ok/workflow/web.yaml:deploy:web", "container web's image //web:push is affected: //web:push —image→ //web:image —binary→ //web:web —deps→ //lib/go/serve:serve, which holds libraries/go/serve/serving/serving.go"
		v.Workload = "deployment web/web"
		v.StatusLabel, v.StatusKind = "Failed", "failed"
		v.Reason = "rolled back: canary: pod web-green-3: container web: CrashLoopBackOff"
		v.Roll, v.Finished, v.RetryHref = "Rolled back · green replacing blue · 3 replicas", "2026-09-29 12:04:31 UTC", base+"/retry?cl=1"
		v.Images = []pages.DeploymentImage{{Container: "web", Target: "//web:push", Run: 4121,
			Reference: "registry.example.com/apps/web@sha256:3f09a1c2e7d44b1a9f0e6d2b8c5a7e4f1d3b9c0a2e8f6d4b7c1a5e9f3d2b8c6a"}}
		v.Events = append(v.Events,
			pages.DeploymentEvent{At: "5 minutes ago", AtTitle: "Sep 29, 2026", Message: "canary: one green pod beside 3 blue"},
			pages.DeploymentEvent{At: "4 minutes ago", AtTitle: "Sep 29, 2026", Message: "failed: canary: pod web-green-3: container web: CrashLoopBackOff; rolling back"},
			pages.DeploymentEvent{At: "4 minutes ago", AtTitle: "Sep 29, 2026", Message: "rolled back: 3 blue pod(s) Ready"})
	}
	return v
}

// mockServices are the Services page's rows: one of each status.
// mockSummaries are the preview's services: ok's roles in one group, one of them deploying
// through its stages as time passes (to watch the pages refresh themselves); identity's two, one
// whose last deploy failed; a named group of one; and three of their own — drifted, never
// deployed, and one no longer declared.
func mockSummaries() []pages.ServiceSummary {
	sv := func(id int64, name, group, workload, label, kind, reason string) pages.ServiceSummary {
		s := pages.ServiceSummary{ID: id, Name: name, Group: group, Repository: "acme", RepositoryHref: "/acme", Workload: workload,
			Declared: true, StatusLabel: label, StatusKind: kind, Reason: reason, Found: true, Ready: 2, Replicas: 2,
			Version: "apps/" + name + "@674607bcb740", Commit: "8b20bf4dd75b", CommitHref: "/acme/commit/8b20bf4dd75b",
			CL: 11, CLHref: "/acme/cl/11", DeployedID: id, Age: "12 minutes ago", AgeTitle: "Sep 30, 2026"}
		if kind == "undeployed" {
			s.Version, s.Commit, s.CommitHref, s.CL, s.CLHref, s.DeployedID, s.Age, s.AgeTitle, s.Found = "", "", "", 0, "", 0, "", "", false
		}
		return s
	}
	scm := sv(2, "ok-scm", "ok", "deployment ok/ok-scm", "Deploying", "deploying", "Deployment 14: Canary")
	switch time.Now().Unix() / 5 % 3 {
	case 1:
		scm.Reason = "Deployment 14: The Rest"
	case 2:
		scm.StatusLabel, scm.StatusKind, scm.Reason, scm.DeployedID = "Healthy", "healthy", "2 of 2 pods Ready", 14
	}
	gone := sv(10, "legacy-app", "", "deployment legacy/app", "Degraded", "degraded", "1 of 2 pods Ready")
	gone.Declared, gone.Ready = false, 1
	return []pages.ServiceSummary{
		sv(1, "ok-control", "ok", "deployment ok/ok-control", "Healthy", "healthy", "2 of 2 pods Ready"),
		scm,
		sv(3, "ok-automation", "ok", "deployment ok/ok-automation", "Healthy", "healthy", "2 of 2 pods Ready"),
		sv(4, "ok-worker", "ok", "deployment ok/ok-worker", "Healthy", "healthy", "2 of 2 pods Ready"),
		sv(5, "ok-deploy", "ok", "deployment ok/ok-deploy", "Healthy", "healthy", "2 of 2 pods Ready"),
		sv(6, "ok-identity", "identity", "deployment ok/ok-identity", "Last Deploy Failed", "failed", "Deployment 12: rolled back: canary: pod ok-identity-7f9c: container identity: CrashLoopBackOff"),
		sv(7, "oidc-proxy", "identity", "deployment ok/oidc-proxy", "Healthy", "healthy", "2 of 2 pods Ready"),
		sv(8, "pgbouncer", "", "statefulset db/pgbouncer", "Drifted", "drifted", "Container pgbouncer runs apps/pgbouncer@badac5eedca0; ok deployed apps/pgbouncer@674607bcb740"),
		sv(9, "billing", "", "deployment billing/billing", "Not Deployed Yet", "undeployed", ""),
		gone,
		sv(11, "serve", "smoke", "deployment smoke/serve", "Healthy", "healthy", "2 of 2 pods Ready"),
	}
}

// mockDeployments are a service's recent deployments: deployed, one that failed before it, and
// one before that.
func mockDeployments(sv pages.ServiceSummary) []pages.ServiceDeployment {
	dep := func(n int64, label, kind, reason string) pages.ServiceDeployment {
		return pages.ServiceDeployment{ID: n, Service: sv.Name, Href: fmt.Sprintf("/acme/deploy/%d", n), StatusLabel: label, StatusKind: kind, Reason: reason,
			Commit: "8b20bf4dd75b", CommitHref: "/acme/commit/8b20bf4dd75b", CL: 11, CLHref: "/acme/cl/11", Age: "12 minutes ago", AgeTitle: "Sep 30, 2026"}
	}
	if sv.DeployedID == 0 {
		return nil
	}
	base := sv.ID * 10
	failed := "rolled back: canary: pod " + sv.Name + "-green-3: container " + sv.Name + ": CrashLoopBackOff"
	if sv.StatusKind == "failed" {
		// Its latest deployment failed; the one before is what runs.
		return []pages.ServiceDeployment{dep(base+3, "Failed", "failed", failed), dep(base+2, "Succeeded", "succeeded", ""), dep(base+1, "Succeeded", "succeeded", "")}
	}
	return []pages.ServiceDeployment{dep(base+3, "Succeeded", "succeeded", ""), dep(base+2, "Failed", "failed", failed), dep(base+1, "Succeeded", "succeeded", "")}
}

// mockService is a service's pane: its green copy live beside a blue copy draining, one pod on an
// image ok did not deploy.
func mockService(sv pages.ServiceSummary) pages.ServiceView {
	good := "registry.example.com/apps/" + sv.Name + "@sha256:674607bcb740114a2514ec3520eaa59f5e3199e9b364623042f8304bca02b019"
	other := "registry.example.com/apps/" + sv.Name + ":9eb1b330b7315e8253e1d92cd05d669e7c601317"
	recent := mockDeployments(sv)
	v := pages.ServiceView{
		ID: sv.ID, Name: sv.Name, Deploy: "//apps/" + sv.Name + "/.ok/workflow/deploy.yaml:deploy:" + sv.Name, Repository: sv.Repository, RepositoryHref: sv.RepositoryHref,
		Workload: sv.Workload, Declared: sv.Declared, StatusLabel: sv.StatusLabel, StatusKind: sv.StatusKind, Reason: sv.Reason,
		Found: sv.Found, Live: "green", Ready: sv.Ready, Replicas: sv.Replicas, Recent: recent,
	}
	if len(recent) == 0 {
		return v
	}
	v.Deployed, v.Latest = &recent[0], &recent[0]
	if recent[0].StatusKind == "failed" {
		v.Deployed = &recent[1]
	}
	pod := func(name, copyOf string, ready bool, image string, matches bool) pages.ServicePod {
		return pages.ServicePod{Name: name, Copy: copyOf, Ready: ready, Age: "11 minutes ago", AgeTitle: "Sep 30, 2026",
			Images: []pages.ServicePodImage{{Container: sv.Name, Image: image, Matches: matches}, {Container: "istio-proxy", Image: "docker.io/istio/proxyv2:1.31.1-distroless", Matches: true}}}
	}
	v.Images = []pages.ServiceImage{{Container: sv.Name, Deployed: good, Running: good, Matches: true}}
	v.Pods = []pages.ServicePod{
		pod(sv.Name+"-green-6f7d9c-2kq8z", "green", true, good, true),
		pod(sv.Name+"-green-6f7d9c-x7m4p", "green", true, good, true),
		func() pages.ServicePod {
			p := pod(sv.Name+"-5b8c4d-vv2lr", "blue", true, other, false)
			p.Terminating = true
			return p
		}(),
	}
	return v
}

// mockServiceGroup serves a group's page: the overview of a named group (name), or a service in
// its group (id).
func mockServiceGroup(w http.ResponseWriter, r *http.Request, name string, id int64) {
	groups := servicegroup.Of(mockSummaries())
	var g servicegroup.Group
	var ok bool
	if name != "" {
		if g, ok = servicegroup.FindNamed(groups, name); ok && !g.Overview() {
			id = g.Members[0].ID
		}
	} else {
		g, ok = servicegroup.Find(groups, id)
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	if id != 0 {
		sv, _ := g.Member(id)
		v := mockService(sv)
		pages.ServeServiceGroup(w, r, pages.ServiceGroupView{Group: g, Selected: id, Service: &v})
		return
	}
	var recent []pages.ServiceDeployment
	for _, sv := range g.Members {
		recent = append(recent, mockDeployments(sv)...)
	}
	slices.SortFunc(recent, func(a, b pages.ServiceDeployment) int { return int(b.ID - a.ID) })
	pages.ServeServiceGroup(w, r, pages.ServiceGroupView{Group: g, Recent: recent[:min(len(recent), 10)]})
}

// mockChecks is a patchset's pre, still running: its plan passed; the build passed, a test runs,
// lint failed, a workflow job was canceled; two workflow jobs are left to main.
func mockChecks(name string, cl int64) pages.CLChecks {
	base := fmt.Sprintf("/%s/cl/%d", name, cl)
	run := func(id int, c pages.CLCheck, action string) pages.CLCheck {
		c.LogHref = fmt.Sprintf("%s/run/%d", base, id)
		c.ActionLabel = action
		c.ActionHref = c.LogHref + "/" + strings.ToLower(action)
		return c
	}
	plan := run(5, pages.CLCheck{Kind: pages.CheckPlan, Name: "plan", Source: "Built In", Why: "patchset 2 was uploaded", StatusLabel: "Passed", StatusKind: "passed", Duration: "1s"}, "Rerun")
	plan.ActionHref, plan.ActionTitle = "", "The patchset is planned: rerun its jobs instead"
	runs := []pages.CLCheck{
		plan,
		run(6, pages.CLCheck{Kind: pages.CheckBuild, Name: "build", Source: "Built In", Why: "every target the change affects must build: building 3 targets builds them all", StatusLabel: "Passed", StatusKind: "passed", Duration: "1m20s"}, "Rerun"),
	}
	// Fourteen tests, more of them done as time passes (to watch the tab refresh itself): the
	// first passed, then running, then queued.
	done := 8 + int(time.Now().Unix()/4%6)
	for i := range 14 {
		c := pages.CLCheck{Kind: pages.CheckTest, Name: fmt.Sprintf("test //internal/pkg%02d:test", i), Source: "Built In",
			Why: fmt.Sprintf("the test is affected: //internal/pkg%02d:test —deps→ //internal/serving/webnode:webnode, which holds internal/serving/webnode/webnode.go", i)}
		action := "Rerun"
		switch {
		case i < done:
			c.StatusLabel, c.StatusKind, c.Duration = "Passed", "passed", fmt.Sprintf("%ds", 3+i)
		case i < done+2:
			c.StatusLabel, c.StatusKind, c.Duration, action = "Running", "running", "4s", "Cancel"
		default:
			c.StatusLabel, c.StatusKind, action = "Queued", "queued", "Cancel"
		}
		runs = append(runs, run(30+i, c, action))
	}
	runs = append(runs,
		run(8, pages.CLCheck{Kind: pages.CheckLint, Name: "lint", Source: "Built In", Why: "the change's 4 files", StatusLabel: "Failed", StatusKind: "failed", Reason: "bazel build exited with status 1", Duration: "12s"}, "Rerun"),
		run(9, pages.CLCheck{Kind: pages.CheckWorkflow, Name: "e2e", Source: "//internal/serving/.ok/workflow/serving.yaml", Why: "internal/serving/webnode/webnode.go changed, and //internal/serving's workflows govern it; its target is affected: //internal/serving/e2e:e2e —deps→ //internal/serving/webnode:webnode, which holds internal/serving/webnode/webnode.go", StatusLabel: "Canceled", StatusKind: "canceled", Reason: "canceled by ada"}, "Rerun"),
	)
	return pages.CLChecks{
		Patchset: 2, Verdict: "Failed", VerdictKind: "failed", Live: true, Planned: true, Href: base + "/checks?ps=2",
		Runs: runs,
		Skipped: []pages.CLSkippedCheck{
			{Name: "bench", Source: "//.ok/workflow/ci.yaml", Target: "//internal/ratelimit:bench", Why: "the change builds on //internal/ratelimit:ratelimit, and //'s workflows govern it; its target is not affected, so its result on main stands"},
			{Name: "docs", Source: "//.ok/workflow/ci.yaml", Target: "//docs:check", Why: "assets/logo.png changed, and //'s workflows govern it; its target is not affected, so its result on main stands"},
		},
	}
}

// mockDeploys is CL 1's Deployments tab: one failed, one waiting for an allowance, and one going
// through its stages as time passes (to watch the tab refresh itself).
func mockDeploys(name string) pages.CLDeploys {
	api := pages.CLDeployment{ID: 9, Name: "api", Workload: "deployment api/api", StatusLabel: "Running", StatusKind: "running", Stage: "Canary", Unfinished: true,
		Href: fmt.Sprintf("/%s/deploy/9?cl=1", name), Age: "just now", AgeTitle: "Sep 29, 2026"}
	switch time.Now().Unix() / 4 % 3 {
	case 1:
		api.Stage = "The Rest"
	case 2:
		api.StatusLabel, api.StatusKind, api.Stage, api.Unfinished = "Succeeded", "succeeded", "", false
	}
	return pages.CLDeploys{Landed: true, Planned: true, Href: fmt.Sprintf("/%s/cl/1/deployments", name), Items: []pages.CLDeployment{
		api,
		{ID: 8, Name: "data", StatusLabel: "Needs Allowance", StatusKind: "hazard", Stage: "Waiting for an Allowance", Unfinished: true,
			Href: fmt.Sprintf("/%s/deploy/8?cl=1", name), Age: "2 minutes ago", AgeTitle: "Sep 29, 2026"},
		{ID: 7, Name: "web", Workload: "deployment web/web", StatusLabel: "Failed", StatusKind: "failed",
			Reason: "rolled back: canary: pod web-green-3: container web: CrashLoopBackOff", Href: fmt.Sprintf("/%s/deploy/7?cl=1", name), Age: "4 minutes ago", AgeTitle: "Sep 29, 2026"},
	}}
}

// mockPost is a CL's post: submitting, testing on top of CL 1 after its first attempt was
// discarded; or, landed, canceled, or failed, as its status says.
func mockPost(name string, cl int64, status string) pages.CLPost {
	base := fmt.Sprintf("/%s/cl/%d", name, cl)
	disabled := func(id int, c pages.CLCheck) pages.CLCheck {
		c.LogHref = fmt.Sprintf("%s/run/%d", base, id)
		c.ActionLabel, c.ActionTitle = "Rerun", "A post run is its submission's: submit the change again"
		if c.StatusKind == "running" || c.StatusKind == "queued" {
			c.ActionLabel, c.ActionTitle = "Cancel", "A post run is its submission's: cancel the submission"
		}
		return c
	}
	p := pages.CLPost{Submitted: true, SubmittedBy: "ada", Age: "3 minutes ago", AgeTitle: "Sep 29, 2026",
		Runs: []pages.CLCheck{
			disabled(21, pages.CLCheck{Kind: pages.CheckPlan, Name: "plan", Source: "Built In", Why: "CL 2 was submitted: post runs on its candidate over main at f26bb8f4a4, speculating that CL 1 lands first", StatusLabel: "Passed", StatusKind: "passed", Duration: "2s"}),
			disabled(22, pages.CLCheck{Kind: pages.CheckBuild, Name: "build", Source: "Built In", Why: "every target the change affects must build: building 3 targets builds them all", StatusLabel: "Passed", StatusKind: "passed", Duration: "4s"}),
			disabled(23, pages.CLCheck{Kind: pages.CheckTest, Name: "test //internal/serving:test", Source: "Built In", Why: "the test is affected: //internal/serving:test —deps→ //internal/serving/webnode:webnode, which holds internal/serving/webnode/webnode.go", StatusLabel: "Running", StatusKind: "running", Duration: "12s"}),
			disabled(24, pages.CLCheck{Kind: pages.CheckWorkflow, Name: "e2e", Source: "//internal/serving/.ok/workflow/serving.yaml", Why: "internal/serving/webnode/webnode.go changed, and //internal/serving's workflows govern it; its target is affected: //internal/serving/e2e:e2e —deps→ //internal/serving/webnode:webnode, which holds internal/serving/webnode/webnode.go", StatusLabel: "Queued", StatusKind: "queued"}),
		},
		Attempts: []pages.CLAttempt{
			{Number: 2, Base: "f26bb8f4a4", OnTopOf: "CL 1", Status: "Testing", StatusKind: "running"},
			{Number: 1, Base: "f26bb8f4a4", Status: "Discarded", StatusKind: "none", Reason: "CL 1, ahead of it, changes what it changes: it is tested on top of it now"},
		},
	}
	switch status {
	case "Landed":
		p.Status, p.StatusKind = "Landed", "passed"
		for i := range p.Runs {
			p.Runs[i].StatusLabel, p.Runs[i].StatusKind, p.Runs[i].Duration = "Passed", "passed", "9s"
			p.Runs[i].ActionLabel, p.Runs[i].ActionTitle = "Rerun", "A post run is its submission's: submit the change again"
		}
		p.Attempts[0].Status, p.Attempts[0].StatusKind = "Landed", "passed"
	case "Canceled":
		p.Status, p.StatusKind, p.Detail = "Canceled", "canceled", "Canceled by ada"
		p.Runs = p.Runs[:1]
		p.Attempts = p.Attempts[1:]
		p.Attempts[0].Reason = "Canceled by ada"
	default:
		p.Status, p.StatusKind, p.Detail = "Submitting", "running", "Testing on top of CL 1"
	}
	return p
}

// mockRunView is a run's page: run 7, a test still running whose log grows; any other, a lint
// that failed.
func mockRunView(name string, cl int64, id int) pages.RunView {
	chrome := mockChrome(name, []pages.Crumb{{Label: name, Href: "/" + name}})
	checks := mockChecks(name, cl)
	check := checks.Runs[3]
	if id == 7 {
		check = checks.Runs[2]
	}
	base := fmt.Sprintf("/%s/cl/%d/run/%d", name, cl, id)
	v := pages.RunView{
		Chrome: chrome, CLNumber: cl, CLHref: fmt.Sprintf("/%s/cl/%d", name, cl), ID: int64(id), Check: check,
		Patchset: 2, Commit: "c0ffee4a4e", Worker: "ok-worker-78ff966f5-8bcxr", Queued: "Sep 29, 2026 06:46:05 UTC",
		Started: "Sep 29, 2026 06:46:06 UTC", StatusHref: base + "/status", RawHref: base + "/raw",
		Log: pages.RunLog{Text: "ok: run " + strconv.Itoa(id) + ", attempt 1, on ok-worker-78ff966f5-8bcxr\nok: why: " + check.Why + "\nok: checking out c0ffee4a4e\nok: checked out in 412ms: 948 files and 0 symlinks; 3 files (2210 bytes) read from the SCM, 0 checked, 0 paths removed\n"},
	}
	if id == 7 {
		v.Live = true
		v.Log.Text += "ok: $ bazel test //internal/serving:test\n=== RUN   TestServe\n"
		v.Log.NextHref = base + "/log?offset=1"
		return v
	}
	v.Finished = "Sep 29, 2026 06:46:18 UTC"
	v.Log.Text += "ok: $ bazel build --keep_going --aspects=//tools/lint:linters.bzl%golangci_lint --@aspect_rules_lint//lint:fail_on_violation --output_groups=rules_lint_human -- //internal/serving/webnode\ninternal/serving/webnode/webnode.go:41:2: should omit type int from declaration; it will be inferred from the right-hand side (ST1023)\nERROR: Build did NOT complete successfully\nok: the run failed: bazel build --keep_going --aspects=//tools/lint:linters.bzl%golangci_lint -- //internal/serving/webnode exited with status 1\n"
	return v
}

// mockCLViewSubmitting is an approved CL whose pre passed, in the landing queue: Submitting,
// with Cancel Submit, its post testing on top of CL 1.
func mockCLViewSubmitting(name string) pages.CLView {
	v := mockCLView(name)
	base := fmt.Sprintf("/%s/cl/7", name)
	v.Number, v.Title, v.StatusLabel, v.StatusKind = 7, "serve the web node's health over the serving runtime", "Approved", "approved"
	v.Submitting, v.CancelSubmitHref = true, base+"/cancel-submit"
	v.CanSubmit, v.SubmitBlockedReason = false, "Submitting: testing on top of CL 1"
	for i := range v.SubmitReqs {
		v.SubmitReqs[i].Satisfied = true
	}
	v.SubmitReqs[len(v.SubmitReqs)-1].Detail = "Passed"
	v.Checks.Verdict, v.Checks.VerdictKind, v.Checks.Href = "Passed", "passed", base+"/checks?ps=2"
	for i := range v.Checks.Runs {
		v.Checks.Runs[i].StatusLabel, v.Checks.Runs[i].StatusKind, v.Checks.Runs[i].Reason = "Passed", "passed", ""
	}
	v.Checks.Post = mockPost(name, 7, "Submitting")
	v.Messages = append(v.Messages, pages.CLMessage{Author: "ada", Action: "sent it to the landing queue", PatchSet: 2,
		Age: "3 minutes ago", AgeTitle: "Sep 29, 2026"})
	return v
}

// mockCLViewAbandoned is an abandoned CL: Restore leads the header, its Reply only comments, and
// the Abandoned panel shows how to drop it from a checkout.
func mockCLViewAbandoned(name string) pages.CLView {
	v := mockCLView(name)
	base := fmt.Sprintf("/%s/cl/5", name)
	v.Number, v.Title, v.StatusLabel, v.StatusKind = 5, "try a leaky-bucket limiter", "Abandoned", "abandoned"
	v.Checks = pages.CLChecks{Patchset: 2, Verdict: "Unavailable", VerdictKind: "unavailable", Href: base + "/checks?ps=2",
		Unavailable: "ok could not read this patchset's checks. Reload to try again.", Post: mockPost(name, 5, "Canceled")}
	v.ShowSubmit, v.SubmitBlockedReason, v.ReplyLabels = false, "", nil
	v.ShowSubmitStack = false
	v.AddReviewers, v.ReturnToDraftHref, v.AbandonHref = nil, "", ""
	v.RestoreHref = base + "/restore"
	v.DropCommand = "jj abandon ysmoyqrwkpzlnvtsxuoqmytlnvrwqyzp"
	for i := range v.Reviewers {
		v.Reviewers[i].RemoveHref = ""
	}
	v.Messages = append(v.Messages, pages.CLMessage{
		Author: "ada", Action: "abandoned the change", Body: "Superseded by the token bucket in CL #2.",
		PatchSet: 2, Age: "5 minutes ago", AgeTitle: "Sep 21, 2026",
	})
	return v
}

// mockCLViewBare is a fresh draft with no reviews, stack, checks, or changed files — every optional
// region empty, to check that panels keep a stable shape — offering Request Review.
func mockCLViewBare(name string) pages.CLView {
	chrome := mockChrome(name, []pages.Crumb{{Label: name, Href: "/" + name}})
	base := fmt.Sprintf("/%s/cl/9", name)
	return pages.CLView{
		Chrome: chrome, Number: 9, Title: "empty change", StatusLabel: "Draft", StatusKind: "draft",
		Author: "ada", ChangeID: "wtyqlnrsxmko", CanReply: true,
		Age: "just now", AgeTitle: "Sep 22, 2026",
		CommitShort: "0000000009", CurrentPatchSet: 1,
		PatchSets: []pages.CLPatchSet{{Number: 1, CommitShort: "0000000009", Current: true}},
		SubmitReqs: []pages.CLSubmitReq{
			{Label: "LGTM"}, {Label: "Approval", Detail: "0 of 0 owner sets"},
			{Label: "Unresolved Comments", Satisfied: true, Detail: "None"},
			{Label: "Pre", Detail: "Not Planned"},
		},
		ShowSubmit: true, SubmitBlockedReason: "Request review first: a draft can't be submitted",
		SubmitHref: base + "/submit", ReplyHref: base + "/reply",
		ReplyLabels:   []pages.CLReplyLabel{{Name: "LGTM"}, {Name: "Approval"}},
		RequestReview: mockReviewDialog(name, 9, true), AbandonHref: base + "/abandon",
		Checks: pages.CLChecks{Patchset: 1, Verdict: "Not Planned", VerdictKind: "none", Href: base + "/checks?ps=1"},
		Messages: []pages.CLMessage{
			{Author: "ada", Action: "uploaded patchset 1", PatchSet: 1, Age: "just now", AgeTitle: "Sep 22, 2026"},
		},
	}
}

// mockRepoRouter dispatches every /<repo>... preview path to a directory, file, blame, commit
// list, commit diff, CL finder, CL view, or empty repo view.
func mockRepoRouter(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	name, rest, _ := strings.Cut(p, "/")
	switch {
	case strings.HasPrefix(rest, "action/"):
		// Star/unstar toggle (POST): persist nothing in the preview.
		w.WriteHeader(http.StatusNoContent)
	case strings.HasPrefix(rest, "blame/main/"):
		p := strings.TrimPrefix(rest, "blame/main/")
		if !mockExists(p) || mockIsDir(p) {
			http.NotFound(w, r)
			return
		}
		pages.ServeBrowse(w, r, mockBrowse(name, p, true))
	case strings.HasPrefix(rest, "tree-children/main/"):
		dir := strings.TrimPrefix(rest, "tree-children/main/")
		if !mockIsDir(dir) {
			http.NotFound(w, r)
			return
		}
		pages.ServeTreeChildren(w, r, mockTree(name, dir, ""))
	case strings.HasPrefix(rest, "tree/main/") && rest != "tree/main/":
		p := strings.TrimPrefix(rest, "tree/main/")
		if !mockExists(p) {
			http.NotFound(w, r)
			return
		}
		pages.ServeBrowse(w, r, mockBrowse(name, p, false))
	case name == "empty-repo":
		pages.ServeRepoView(w, r, mockRepoView(name, true))
	case strings.HasPrefix(rest, "commits"):
		pages.ServeCommits(w, r, mockCommits(name))
	case rest == "commit/many":
		v := mockCommitPage(name)
		v.NumFiles, v.TotalsKnown = manyFiles, false
		v.Files, v.MoreFilesHref = mockManyRows(name, 1)
		pages.ServeCommitDiff(w, r, v)
	case rest == "commit/many/files":
		n, _ := strconv.Atoi(r.URL.Query().Get("page"))
		rows, next := mockManyRows(name, max(n, 1))
		pages.ServeFileRows(w, r, rows, next)
	case rest == "commit/many/file":
		f, _ := fileNamed(mockCommitDiff(name).Files, "internal/serving/webnode/doc.go")
		f.Name = r.URL.Query().Get("path")
		pages.ServeFileBody(w, r, f)
	case strings.HasPrefix(rest, "commit/") && strings.HasSuffix(rest, "/file"):
		f, ok := fileNamed(mockCommitDiff(name).Files, r.URL.Query().Get("path"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		pages.ServeFileBody(w, r, f)
	case strings.HasPrefix(rest, "commit/"):
		pages.ServeCommitDiff(w, r, mockCommitPage(name))
	case strings.HasPrefix(rest, "cl/") && strings.HasSuffix(rest, "/file"):
		v := mockCLViewFull(name)
		f, ok := fileNamed(v.Files, r.URL.Query().Get("path"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		pages.ServeCLFileBody(w, r, f, v.CommentHref, true)
	case strings.HasSuffix(rest, "/reviewers/search"):
		var matches []pages.CLCandidate
		q := strings.ToLower(r.URL.Query().Get("q"))
		for _, c := range []pages.CLCandidate{{Principal: "lin.human.example.com", Name: "lin"}, {Principal: "miles.human.example.com", Name: "miles"}, {Principal: "noor.human.example.com", Name: "noor"}} {
			if q != "" && strings.Contains(c.Name, q) {
				matches = append(matches, c)
			}
		}
		pages.ServeCLReviewerResults(w, r, q != "", matches)
	case rest == "cl/1/deployments":
		pages.ServeCLDeploys(w, r, mockDeploys(name))
	case strings.HasPrefix(rest, "cl/2/checks"):
		pages.ServeCLChecks(w, r, mockChecks(name, 2))
	case strings.HasPrefix(rest, "cl/2/run/"):
		idPart, tail, _ := strings.Cut(strings.TrimPrefix(rest, "cl/2/run/"), "/")
		id, _ := strconv.Atoi(idPart)
		v := mockRunView(name, 2, id)
		switch tail {
		case "status":
			pages.ServeRunStatus(w, r, v)
		case "log":
			// The log grows a line each read, for three reads, then the run ends.
			n, _ := strconv.Atoi(r.URL.Query().Get("offset"))
			l := pages.RunLog{Text: fmt.Sprintf("--- PASS: TestServe/case_%d (0.%02ds)\n", n, n*7)}
			if n < 3 {
				l.NextHref = fmt.Sprintf("/%s/cl/2/run/%d/log?offset=%d", name, id, n+1)
			} else {
				l.Text += "PASS\nok: the run passed\n"
			}
			pages.ServeRunLog(w, r, l)
		case "raw":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte(v.Log.Text))
		default:
			pages.ServeRunView(w, r, v)
		}
	case strings.HasPrefix(rest, "cl/7/checks"):
		v := mockCLViewSubmitting(name)
		pages.ServeCLChecks(w, r, v.Checks)
	case rest == "cl/7":
		pages.ServeCLView(w, r, mockCLViewSubmitting(name))
	case rest == "cl/9":
		pages.ServeCLView(w, r, mockCLViewBare(name))
	case rest == "cl/5":
		pages.ServeCLView(w, r, mockCLViewAbandoned(name))
	case rest == "cl/1":
		pages.ServeCLView(w, r, mockCLViewSubmitted(name))
	case strings.HasPrefix(rest, "deploy/"):
		id, _ := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(rest, "deploy/"), "/status"), 10, 64)
		if strings.HasSuffix(rest, "/status") {
			pages.ServeDeploymentStatus(w, r, mockDeployment(name, id))
			return
		}
		pages.ServeDeploymentView(w, r, mockDeployment(name, id))
	case rest == "cl/11":
		pages.ServeCLView(w, r, mockCLViewRolledBack(name))
	case rest == "cl/12":
		pages.ServeCLView(w, r, mockCLViewRollback(name))
	case strings.HasPrefix(rest, "cl/"):
		pages.ServeCLView(w, r, mockCLView(name))
	case strings.HasPrefix(rest, "cls"):
		pages.ServeCLList(w, r, mockCLList(name))
	default:
		pages.ServeRepoView(w, r, mockRepoView(name, false))
	}
}

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8093"
	}
	all := mockRepos(200)

	mux := http.NewServeMux()
	assets := pages.Assets()
	mux.Handle("/assets/", assets)
	mux.Handle("/favicon.svg", assets)
	mux.HandleFunc("/services", func(w http.ResponseWriter, r *http.Request) {
		pages.ServeServices(w, r, pages.ServicesView{Groups: servicegroup.Of(mockSummaries())})
	})
	mux.HandleFunc("/services/group/", func(w http.ResponseWriter, r *http.Request) {
		mockServiceGroup(w, r, strings.TrimPrefix(r.URL.Path, "/services/group/"), 0)
	})
	mux.HandleFunc("/services/", func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/services/"), 10, 64)
		mockServiceGroup(w, r, "", id)
	})

	filtered := func(q string) []pages.RepoItem {
		if q == "" {
			return all
		}
		q = strings.ToLower(q)
		out := []pages.RepoItem{}
		for _, it := range all {
			if strings.Contains(strings.ToLower(it.Name), q) {
				out = append(out, it)
			}
		}
		return out
	}
	pageOf := func(items []pages.RepoItem, p int) []pages.RepoItem {
		start := (p - 1) * pageSize
		if start >= len(items) {
			return nil
		}
		end := start + pageSize
		if end > len(items) {
			end = len(items)
		}
		return items[start:end]
	}
	rowsURL := func(q string) string {
		u := "/repositories/rows"
		if q != "" {
			u += "?q=" + url.QueryEscape(q)
		}
		return u
	}

	mux.HandleFunc("/repositories/rows", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page < 1 {
			page = 1
		}
		pages.ServeRepoRows(w, r, pageOf(filtered(q), page))
	})
	mux.HandleFunc("/repositories/list", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		items := filtered(q)
		pages.ServeHomeList(w, r, q, pageOf(items, 1), rowsURL(q), pageSize, len(items) > pageSize)
	})
	mux.HandleFunc("/repositories/new", func(w http.ResponseWriter, r *http.Request) {
		pages.ServeNewRepo(w, r, pages.NewRepoForm{}, "")
	})
	mux.HandleFunc("/repositories", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/", http.StatusFound)
	})
	// Repo view preview: /<repo>, /<repo>/tree/main/<file>, and /empty-repo for the
	// quick-start state.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			mockRepoRouter(w, r)
			return
		}
		q := r.URL.Query().Get("q")
		items := filtered(q)
		pages.ServeHome(w, r, "ada", q, pageOf(items, 1), rowsURL(q), pageSize, len(items) > pageSize)
	})

	log.Printf("ok-preview listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
