package web

import (
	"errors"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/control/db"
	pages "github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/render"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Limits on what a page reads to show.
const (
	maxViewBytes   = 2 << 20 // a file shown as source
	maxReadmeBytes = 1 << 20
	maxBlameBytes  = 1 << 20
	commitsPerPage = 30
)

// readmeNames are the files shown below a directory listing, in preference order.
var readmeNames = []string{"readme.md", "readme.markdown", "readme", "readme.txt"}

// tree shows a path at a revision — main, or a commit's full ID: the repository's root as its
// landing, and anything below it — a directory or a file — in the browser.
func (s *Server) tree(w http.ResponseWriter, r *http.Request) {
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	rev := chi.URLParam(r, "rev")
	if rev == "" {
		rev = "main"
	}
	at, ok := resolveRev(repo, rev)
	if !ok {
		pages.ServeError(w, r, http.StatusNotFound, "Not Found", "That is not a commit: name one by its full ID, or use main.")
		return
	}
	p := pathParam(r)
	if scm.IsRoot(at) {
		if p != "" {
			pages.ServeError(w, r, http.StatusNotFound, "Not Found", "Nothing has landed on main yet.")
			return
		}
		pages.ServeRepoView(w, r, pages.RepoView{Chrome: chrome(repo, rev, at, p), IsEmpty: true})
		return
	}
	resp, err := s.scm.Browse.ReadTree(r.Context(), &scmpb.ReadTreeRequest{RepositoryId: repo.GetId(), CommitId: at, Path: p, LastCommits: true})
	if status.Code(err) == codes.NotFound && p != "" {
		s.file(w, r, repo, rev, at, p)
		return
	}
	if err != nil {
		fail(w, r, err)
		return
	}
	if p == "" {
		s.landing(w, r, repo, rev, at, resp)
		return
	}
	s.directory(w, r, repo, rev, at, p, resp)
}

// landing shows a repository's root: its entries with each one's last commit, the latest
// commit, and its README.
func (s *Server) landing(w http.ResponseWriter, r *http.Request, repo *scmpb.Repository, rev string, at []byte, resp *scmpb.ReadTreeResponse) {
	entries, readme, latest, err := s.listing(r, repo, rev, at, "", resp)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeRepoView(w, r, pages.RepoView{Chrome: chrome(repo, rev, at, ""), Entries: entries, Readme: readme, Latest: latest})
}

// directory shows a directory below the root in the browser: the tree, open down to it, and in
// the pane its entries with each one's last commit, the latest commit, and its README.
func (s *Server) directory(w http.ResponseWriter, r *http.Request, repo *scmpb.Repository, rev string, at []byte, dir string, resp *scmpb.ReadTreeResponse) {
	entries, readme, latest, err := s.listing(r, repo, rev, at, dir, resp)
	if err != nil {
		fail(w, r, err)
		return
	}
	tree, err := s.browseTree(r, repo, rev, at, dir, true, resp.GetEntries())
	if err != nil {
		fail(w, r, err)
		return
	}
	parent := path.Dir(dir)
	if parent == "." {
		parent = ""
	}
	pages.ServeBrowse(w, r, pages.BrowseView{
		Chrome: chrome(repo, rev, at, dir),
		Tree:   tree,
		Path:   dir,
		Kind:   pages.BrowseDirKind,
		Dir: &pages.BrowseDir{Entries: entries, Readme: readme, Latest: latest,
			ParentHref: treeHref(repo.GetName(), rev, parent), ParentRoot: parent == ""},
	})
}

// sortEntries orders a directory's entries as every listing shows them: directories first,
// then by name.
func sortEntries(entries []*scmpb.TreeEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		di := entries[i].GetValue().GetType() == scmpb.EntryType_ENTRY_TYPE_TREE
		dj := entries[j].GetValue().GetType() == scmpb.EntryType_ENTRY_TYPE_TREE
		if di != dj {
			return di
		}
		return entries[i].GetName() < entries[j].GetName()
	})
}

// listing is a directory's rows — its entries with each one's last commit — its README, and
// the latest commit: the one browsed at the root, else the newest on main up to it that changed
// the directory.
func (s *Server) listing(r *http.Request, repo *scmpb.Repository, rev string, at []byte, dir string, resp *scmpb.ReadTreeResponse) ([]pages.EntryItem, *pages.ReadmeView, pages.LatestCommit, error) {
	ctx := r.Context()
	commits := map[string]*scmpb.Commit{}
	for _, c := range resp.GetCommits() {
		commits[string(c.GetId())] = c
	}
	entries := resp.GetEntries()
	sortEntries(entries)
	var items []pages.EntryItem
	var readme *scmpb.TreeEntry
	readmeRank := len(readmeNames)
	for _, e := range entries {
		p := path.Join(dir, e.GetName())
		item := pages.EntryItem{
			Name:   e.GetName(),
			IsDir:  e.GetValue().GetType() == scmpb.EntryType_ENTRY_TYPE_TREE,
			IsLink: e.GetValue().GetType() == scmpb.EntryType_ENTRY_TYPE_SYMLINK,
			Href:   treeHref(repo.GetName(), rev, p),
		}
		if c := commits[string(e.GetLastCommitId())]; c != nil {
			item.CommitMsg = commitTitle(c)
			item.CommitHref = commitHref(repo.GetName(), c.GetId())
			item.Age, item.AgeTitle = age(commitTime(c))
		}
		items = append(items, item)
		if e.GetValue().GetType() == scmpb.EntryType_ENTRY_TYPE_FILE {
			for rank, name := range readmeNames[:readmeRank] {
				if strings.ToLower(e.GetName()) == name {
					readme, readmeRank = e, rank
					break
				}
			}
		}
	}

	var lc pages.LatestCommit
	latest, err := s.latestCommit(r, repo, at, dir)
	if err != nil {
		return nil, nil, lc, err
	}
	if latest != nil {
		lc = pages.LatestCommit{Present: true, Author: commitAuthor(latest), Message: commitTitle(latest), Href: commitHref(repo.GetName(), latest.GetId())}
		lc.Age, lc.AgeTitle = age(commitTime(latest))
	}

	if readme == nil {
		return items, nil, lc, nil
	}
	p := path.Join(dir, readme.GetName())
	data, _, truncated, err := s.scm.ReadFile(ctx, repo.GetId(), at, p, maxReadmeBytes)
	if err != nil {
		return nil, nil, lc, err
	}
	if truncated || render.IsBinary(data) {
		return items, nil, lc, nil
	}
	html, err := s.markup(repo.GetName(), rev, p, data)
	if err != nil {
		return nil, nil, lc, err
	}
	return items, &pages.ReadmeView{Name: readme.GetName(), HTML: html}, lc, nil
}

// browseTree is the browser's tree for p: the root's entries, with every folder above p open —
// and p itself, a folder (isDir), whose entries the caller has read already. The folders on the
// way are read at once; every other folder's rows are fetched when it is first opened
// (treeChildren).
func (s *Server) browseTree(r *http.Request, repo *scmpb.Repository, rev string, at []byte, p string, isDir bool, entries []*scmpb.TreeEntry) ([]pages.TreeNode, error) {
	parts := strings.Split(p, "/")
	open := []string{""}
	for i := 1; i < len(parts); i++ {
		open = append(open, strings.Join(parts[:i], "/"))
	}
	listings := map[string][]*scmpb.TreeEntry{}
	if isDir {
		open = append(open, p)
		listings[p] = entries
	}
	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		first error
	)
	for _, dir := range open {
		if _, ok := listings[dir]; ok {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := s.scm.Browse.ReadTree(r.Context(), &scmpb.ReadTreeRequest{RepositoryId: repo.GetId(), CommitId: at, Path: dir})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if first == nil {
					first = err
				}
				return
			}
			listings[dir] = resp.GetEntries()
		}()
	}
	wg.Wait()
	if first != nil {
		return nil, first
	}
	return treeNodes(repo.GetName(), rev, "", p, listings), nil
}

// treeNodes are dir's rows in the browser's tree: open (its children with it) where listings
// holds the folder, and otherwise fetched from treeChildren when it is first opened. current is
// the path the browser shows.
func treeNodes(repo, rev, dir, current string, listings map[string][]*scmpb.TreeEntry) []pages.TreeNode {
	entries := listings[dir]
	sortEntries(entries)
	depth := 0
	if dir != "" {
		depth = strings.Count(dir, "/") + 1
	}
	nodes := make([]pages.TreeNode, 0, len(entries))
	for _, e := range entries {
		p := path.Join(dir, e.GetName())
		n := pages.TreeNode{
			Name:    e.GetName(),
			Path:    p,
			IsDir:   e.GetValue().GetType() == scmpb.EntryType_ENTRY_TYPE_TREE,
			IsLink:  e.GetValue().GetType() == scmpb.EntryType_ENTRY_TYPE_SYMLINK,
			Depth:   depth,
			Href:    treeHref(repo, rev, p),
			Current: p == current,
		}
		if n.IsDir {
			n.ChildrenID = treeChildrenID(p)
			if _, ok := listings[p]; ok {
				n.Open, n.Children = true, treeNodes(repo, rev, p, current, listings)
			} else {
				n.ChildrenHref = treeChildrenHref(repo, rev, p)
			}
		}
		nodes = append(nodes, n)
	}
	return nodes
}

// treeChildrenID names a folder's list in the tree, which its rows are fetched into: an id
// built from a hash of its path, since a path may hold anything an id may not.
func treeChildrenID(p string) string {
	h := fnv.New64a()
	h.Write([]byte(p))
	return "tree-" + strconv.FormatUint(h.Sum64(), 36)
}

// treeChildren is a folder's rows in the browser's tree: what the tree fetches the first time
// the folder is opened. Nothing else of the browser is rendered.
func (s *Server) treeChildren(w http.ResponseWriter, r *http.Request) {
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	rev := chi.URLParam(r, "rev")
	at, ok := resolveRev(repo, rev)
	if !ok || scm.IsRoot(at) {
		http.NotFound(w, r)
		return
	}
	dir := pathParam(r)
	resp, err := s.scm.Browse.ReadTree(r.Context(), &scmpb.ReadTreeRequest{RepositoryId: repo.GetId(), CommitId: at, Path: dir})
	if status.Code(err) == codes.NotFound {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeTreeChildren(w, r, treeNodes(repo.GetName(), rev, dir, "", map[string][]*scmpb.TreeEntry{dir: resp.GetEntries()}))
}

// latestCommit is the commit a directory page names as its latest.
func (s *Server) latestCommit(r *http.Request, repo *scmpb.Repository, at []byte, dir string) (*scmpb.Commit, error) {
	req := &scmpb.LogRequest{RepositoryId: repo.GetId(), From: at, PageSize: 1}
	if dir != "" {
		req.PathPrefixes = []string{dir}
	}
	resp, err := s.scm.Browse.Log(r.Context(), req)
	if status.Code(err) == codes.InvalidArgument {
		// A commit off main has no history on main: it is its own latest commit.
		c, err := s.scm.Browse.GetCommits(r.Context(), &scmpb.GetCommitsRequest{RepositoryId: repo.GetId(), CommitIds: [][]byte{at}})
		if err != nil {
			return nil, err
		}
		return c.GetCommits()[0], nil
	}
	if err != nil || len(resp.GetCommits()) == 0 {
		return nil, err
	}
	return resp.GetCommits()[0], nil
}

// markup renders a markdown file (or shows another text file as preformatted text), its
// relative links resolved against the repository at rev.
func (s *Server) markup(repo, rev, p string, data []byte) (string, error) {
	if !render.IsMarkdown(p) {
		return "<pre>" + render.HTML(render.Plain(string(data))) + "</pre>", nil
	}
	return render.Markdown(data, render.Links{
		Dir:  path.Dir(p),
		Page: func(target string) string { return treeHref(repo, rev, target) },
		Raw:  func(target string) string { return rawHref(repo, rev, target) },
	})
}

// file shows one file in the browser: rendered markdown, highlighted source, an image, or a
// way to download what can't be shown.
func (s *Server) file(w http.ResponseWriter, r *http.Request, repo *scmpb.Repository, rev string, at []byte, p string) {
	data, value, truncated, err := s.scm.ReadFile(r.Context(), repo.GetId(), at, p, maxViewBytes)
	if err != nil {
		fail(w, r, err)
		return
	}
	name := path.Base(p)
	f := pages.FileBlob{
		Name:        name,
		SizeHuman:   humanSize(value.GetSize()),
		RawHref:     rawHref(repo.GetName(), rev, p),
		HistoryHref: repoLink(repo.GetName()) + "/commits?path=" + url.QueryEscape(p),
	}
	switch {
	case value.GetType() == scmpb.EntryType_ENTRY_TYPE_SYMLINK:
		f.SizeHuman = "Symbolic link"
		f.Mode = "source"
		f.Lines = []string{render.HTML(render.Plain(value.GetSymlinkTarget()))}
		f.RawHref = ""
	case render.ImageType(name) != "":
		f.Mode, f.ImageSrc = "image", f.RawHref
	case truncated || render.IsBinary(data):
	case render.IsMarkdown(name):
		html, err := s.markup(repo.GetName(), rev, p, data)
		if err != nil {
			fail(w, r, err)
			return
		}
		f.Mode, f.HTML = "markup", html
		f.BlameHref = blameHref(repo.GetName(), rev, p)
	default:
		f.Mode = "source"
		f.LexerName = render.Language(name, data)
		for _, line := range render.Lines(name, data) {
			f.Lines = append(f.Lines, render.HTML(line))
		}
		f.BlameHref = blameHref(repo.GetName(), rev, p)
	}
	tree, err := s.browseTree(r, repo, rev, at, p, false, nil)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeBrowse(w, r, pages.BrowseView{Chrome: chrome(repo, rev, at, p), Tree: tree, Path: p, Kind: pages.BrowseFileKind, File: &f})
}

// raw serves a file's bytes. Anything a browser could run is served inert: text types as
// plain text, and every response under a sandboxing Content-Security-Policy.
func (s *Server) raw(w http.ResponseWriter, r *http.Request) {
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	at, ok := resolveRev(repo, chi.URLParam(r, "rev"))
	if !ok {
		pages.ServeError(w, r, http.StatusNotFound, "Not Found", "That is not a commit.")
		return
	}
	p := pathParam(r)
	stream, err := s.scm.Browse.ReadFileAt(r.Context(), &scmpb.ReadFileAtRequest{RepositoryId: repo.GetId(), CommitId: at, Path: p})
	if err != nil {
		fail(w, r, err)
		return
	}
	first, err := stream.Recv()
	if err != nil {
		fail(w, r, err)
		return
	}
	h := w.Header()
	h.Set("Content-Security-Policy", "default-src 'none'; img-src 'self'; style-src 'unsafe-inline'; sandbox")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Disposition", "inline; filename*=UTF-8''"+url.PathEscape(path.Base(p)))
	if first.GetValue().GetType() == scmpb.EntryType_ENTRY_TYPE_SYMLINK {
		h.Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, first.GetValue().GetSymlinkTarget())
		return
	}
	ct := render.ImageType(p)
	if ct == "" {
		sniffed := http.DetectContentType(first.GetData())
		switch {
		case strings.HasPrefix(sniffed, "text/"):
			ct = "text/plain; charset=utf-8"
		case strings.HasPrefix(sniffed, "image/"), strings.HasPrefix(sniffed, "audio/"), strings.HasPrefix(sniffed, "video/"),
			sniffed == "application/pdf", sniffed == "application/zip", sniffed == "application/x-gzip":
			ct = sniffed
		default:
			ct = "application/octet-stream"
		}
	}
	h.Set("Content-Type", ct)
	if _, err := w.Write(first.GetData()); err != nil {
		return
	}
	for {
		f, err := stream.Recv()
		if errors.Is(err, io.EOF) || err != nil {
			return
		}
		if _, err := w.Write(f.GetData()); err != nil {
			return
		}
	}
}

// blame shows, in the browser's pane, each line of a file beside the commit that last changed
// it.
func (s *Server) blame(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	rev := chi.URLParam(r, "rev")
	at, ok := resolveRev(repo, rev)
	if !ok {
		pages.ServeError(w, r, http.StatusNotFound, "Not Found", "That is not a commit.")
		return
	}
	p := pathParam(r)
	b := &pages.BrowseBlame{FileHref: treeHref(repo.GetName(), rev, p)}
	data, value, truncated, err := s.scm.ReadFile(ctx, repo.GetId(), at, p, maxBlameBytes)
	if err != nil {
		fail(w, r, err)
		return
	}
	tree, err := s.browseTree(r, repo, rev, at, p, false, nil)
	if err != nil {
		fail(w, r, err)
		return
	}
	v := pages.BrowseView{Chrome: chrome(repo, rev, at, p), Tree: tree, Path: p, Kind: pages.BrowseBlameKind, Blame: b}
	switch {
	case value.GetType() != scmpb.EntryType_ENTRY_TYPE_FILE:
		b.Message = "Only a file has a blame."
	case truncated:
		b.Message = "This file is too large to blame here."
	case render.IsBinary(data):
		b.Message = "A binary file has no lines to blame."
	}
	if b.Message != "" {
		pages.ServeBrowse(w, r, v)
		return
	}
	resp, err := s.scm.Browse.Annotate(ctx, &scmpb.AnnotateRequest{RepositoryId: repo.GetId(), CommitId: at, Path: p})
	if err != nil {
		fail(w, r, err)
		return
	}
	var ids [][]byte
	seen := map[string]bool{}
	for _, rg := range resp.GetRanges() {
		if id := rg.GetCommitId(); !scm.IsRoot(id) && !seen[string(id)] {
			seen[string(id)] = true
			ids = append(ids, id)
		}
	}
	commits := map[string]*scmpb.Commit{}
	if len(ids) > 0 {
		cs, err := s.scm.Browse.GetCommits(ctx, &scmpb.GetCommitsRequest{RepositoryId: repo.GetId(), CommitIds: ids})
		if err != nil {
			fail(w, r, err)
			return
		}
		for _, c := range cs.GetCommits() {
			commits[string(c.GetId())] = c
		}
	}
	lines := render.Lines(p, data)
	for _, rg := range resp.GetRanges() {
		c := commits[string(rg.GetCommitId())]
		for i := range int(rg.GetLineCount()) {
			n := int(rg.GetFirstLine()) + i
			l := pages.BlameLine{Number: n, First: i == 0}
			if n-1 < len(lines) {
				l.HTML = render.HTML(lines[n-1])
			}
			if c != nil {
				l.CommitShort = scm.Short(c.GetId())
				l.CommitTitle = commitTitle(c)
				l.CommitHref = commitHref(repo.GetName(), c.GetId())
				l.Author = commitAuthor(c)
				l.Age, l.AgeTitle = age(commitTime(c))
			}
			b.Lines = append(b.Lines, l)
		}
	}
	pages.ServeBrowse(w, r, v)
}

// commits is main's history, optionally only the commits that changed a path.
func (s *Server) commits(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	p := strings.Trim(r.URL.Query().Get("path"), "/")
	req := &scmpb.LogRequest{RepositoryId: repo.GetId(), PageSize: commitsPerPage, PageToken: r.URL.Query().Get("page")}
	if p != "" {
		req.PathPrefixes = []string{p}
	}
	resp, err := s.scm.Browse.Log(ctx, req)
	if err != nil {
		fail(w, r, err)
		return
	}
	v := pages.CommitsView{Chrome: chrome(repo, "main", repo.GetHead(), p), FilePath: p}
	if p == "" {
		v.Count = repo.GetHeadSeq()
	}
	ids := make([][]byte, len(resp.GetCommits()))
	for i, c := range resp.GetCommits() {
		ids[i] = c.GetId()
	}
	landed, err := db.LandedBy(ctx, s.db.Q(), repo.GetId(), ids)
	if err != nil {
		fail(w, r, err)
		return
	}
	for _, c := range resp.GetCommits() {
		row := pages.CommitRow{
			SHA:        scm.Hex(c.GetId()),
			ShortSHA:   scm.Short(c.GetId()),
			Href:       commitHref(repo.GetName(), c.GetId()),
			BrowseHref: treeHref(repo.GetName(), scm.Hex(c.GetId()), ""),
			Author:     commitAuthor(c),
			Title:      commitTitle(c),
		}
		row.Age, row.AgeTitle = age(commitTime(c))
		if n, ok := landed[string(c.GetId())]; ok {
			row.CL, row.CLHref = n, clHref(repo.GetName(), n)
		}
		v.Commits = append(v.Commits, row)
	}
	page := func(token string) string {
		if token == "" {
			return ""
		}
		q := url.Values{"page": {token}}
		if p != "" {
			q.Set("path", p)
		}
		return repoLink(repo.GetName()) + "/commits?" + q.Encode()
	}
	v.PrevHref, v.NextHref = page(resp.GetPrevPageToken()), page(resp.GetNextPageToken())
	pages.ServeCommits(w, r, v)
}

// commit shows one commit: its message and metadata, the CL it landed from, and its diff.
func (s *Server) commit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	id, ok := scm.ParseHex(chi.URLParam(r, "id"))
	if !ok {
		pages.ServeError(w, r, http.StatusNotFound, "Not Found", "That is not a commit ID.")
		return
	}
	cs, err := s.scm.Browse.GetCommits(ctx, &scmpb.GetCommitsRequest{RepositoryId: repo.GetId(), CommitIds: [][]byte{id}})
	if err != nil {
		fail(w, r, err)
		return
	}
	c := cs.GetCommits()[0]
	title, body, _ := strings.Cut(strings.TrimSpace(c.GetDescription()), "\n")
	v := pages.CommitDiffView{
		Chrome:   chrome(repo, scm.Hex(id), id, ""),
		SHA:      scm.Hex(id),
		ShortSHA: scm.Short(id),
		ChangeID: scm.ShortChangeID(c.GetChangeId()),
		Title:    commitTitle(c),
		Body:     strings.TrimSpace(body),
		Author:   commitAuthor(c),
	}
	if title == "" {
		v.Body = ""
	}
	v.Age, v.AgeTitle = age(commitTime(c))
	for _, parent := range c.GetParents() {
		if scm.IsRoot(parent) {
			continue
		}
		v.Parents = append(v.Parents, pages.ParentRef{ShortSHA: scm.Short(parent), Href: commitHref(repo.GetName(), parent)})
	}
	landed, err := db.LandedBy(ctx, s.db.Q(), repo.GetId(), [][]byte{id})
	if err != nil {
		fail(w, r, err)
		return
	}
	if n, ok := landed[string(id)]; ok {
		v.CL, v.CLHref = n, clHref(repo.GetName(), n)
	}
	if c.GetConflicted() {
		pages.ServeError(w, r, http.StatusBadRequest, "Can't Show That", "This commit has unresolved conflicts, which ok does not show.")
		return
	}
	changes, err := s.changes(ctx, repo.GetId(), c.GetParents()[0], id)
	if err != nil {
		fail(w, r, err)
		return
	}
	v.NumFiles = len(changes.changes)
	if v.Files, v.MoreFilesHref, err = s.rows(ctx, repo.GetId(), commitFileList(repo.GetName(), id, changes), 1); err != nil {
		fail(w, r, err)
		return
	}
	if v.Addition, v.Deletion, v.TotalsKnown, err = s.totals(ctx, repo.GetId(), changes); err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeCommitDiff(w, r, v)
}

// commitFileList is a commit's file list: what it changes against its first parent.
func commitFileList(repoName string, id []byte, cs *changeSet) *fileList {
	link := commitHref(repoName, id)
	return &fileList{
		cs:   cs,
		body: func(row pages.DiffFileView) string { return link + "/file?" + bodyQuery(url.Values{}, row) },
		page: func(n int) string { return link + "/files?" + url.Values{"page": {strconv.Itoa(n)}}.Encode() },
	}
}

// commitTarget is the commit a request names, its repository, its first parent, and what it
// changes against it; ok is false once the response is written.
func (s *Server) commitTarget(w http.ResponseWriter, r *http.Request) (repo *scmpb.Repository, id, parent []byte, cs *changeSet, ok bool) {
	ctx := r.Context()
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return nil, nil, nil, nil, false
	}
	id, valid := scm.ParseHex(chi.URLParam(r, "id"))
	if !valid {
		pages.ServeError(w, r, http.StatusNotFound, "Not Found", "That is not a commit ID.")
		return nil, nil, nil, nil, false
	}
	commits, err := s.scm.Browse.GetCommits(ctx, &scmpb.GetCommitsRequest{RepositoryId: repo.GetId(), CommitIds: [][]byte{id}})
	if err != nil {
		fail(w, r, err)
		return nil, nil, nil, nil, false
	}
	c := commits.GetCommits()[0]
	if c.GetConflicted() {
		pages.ServeError(w, r, http.StatusBadRequest, "Can't Show That", "This commit has unresolved conflicts, which ok does not show.")
		return nil, nil, nil, nil, false
	}
	parent = c.GetParents()[0]
	if cs, err = s.changes(ctx, repo.GetId(), parent, id); err != nil {
		fail(w, r, err)
		return nil, nil, nil, nil, false
	}
	return repo, id, parent, cs, true
}

// commitFiles is a page of a commit's file list after the first.
func (s *Server) commitFiles(w http.ResponseWriter, r *http.Request) {
	repo, id, _, cs, ok := s.commitTarget(w, r)
	if !ok {
		return
	}
	rows, next, err := s.rows(r.Context(), repo.GetId(), commitFileList(repo.GetName(), id, cs), pageNumber(r.URL.Query()))
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeFileRows(w, r, rows, next)
}

// commitFile is one file of a commit's diff: what its row loads when opened.
func (s *Server) commitFile(w http.ResponseWriter, r *http.Request) {
	repo, id, parent, cs, ok := s.commitTarget(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	c := findChange(cs, q.Get("path"), q.Get("old"))
	if c == nil {
		fail(w, r, review.ErrNotFound)
		return
	}
	d, err := s.renderDiff(r.Context(), repo.GetId(), parent, id, cs, []*scmpb.PathChange{c})
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeFileBody(w, r, d.files[0])
}
