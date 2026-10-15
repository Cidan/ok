package web

import (
	"container/list"
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"

	"github.com/go-enry/go-enry/v2"

	pages "github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/review"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// A diff's file list — a CL's patchset's, or a commit's — is every changed file as a row that
// loads its diff when it is opened, a page of rows at a time: a change of any size lists in
// bounded time, and a file's diff is read and highlighted only when someone looks at it.
const (
	// filePage is how many rows one page of a file list holds.
	filePage = 100
	// maxTotalFiles is the most files whose lines a file list totals: past it, counting would
	// diff every file, so each row counts its own lines alone.
	maxTotalFiles = 300
	// autoOpenFiles is the most files a file list opens from the start, each loading its diff as
	// it scrolls into view; a larger change's files open when clicked.
	autoOpenFiles = 20
	// changeCacheFiles bounds the changed paths the change-list cache holds, over its entries;
	// changeCacheEntries bounds the entries.
	changeCacheFiles   = 500_000
	changeCacheEntries = 64
)

// changeSet is what changed between two commits' trees: every changed path, renames and copies
// detected as the SCM detects them for jj, in the SCM's order.
type changeSet struct {
	fromTree, toTree []byte
	changes          []*scmpb.PathChange
}

// changes lists what changed from commit from to commit to. A file list reads it once for its
// first page and again for each later page and each file opened, so the listing is cached.
func (s *Server) changes(ctx context.Context, repo int64, from, to []byte) (*changeSet, error) {
	fromTree, err := s.scm.Tree(ctx, repo, from)
	if err != nil {
		return nil, err
	}
	toTree, err := s.scm.Tree(ctx, repo, to)
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%d %x %x", repo, fromTree, toTree)
	if cs := s.changeCache.get(key); cs != nil {
		return cs, nil
	}
	resp, err := s.scm.Browse.DiffTrees(ctx, &scmpb.DiffTreesRequest{
		RepositoryId: repo, FromTree: fromTree, ToTree: toTree, DetectRenames: true, RenameSimilarityPercent: review.RenameSimilarity,
	})
	if err != nil {
		return nil, err
	}
	cs := &changeSet{fromTree: fromTree, toTree: toTree, changes: resp.GetChanges()}
	s.changeCache.put(key, cs)
	return cs, nil
}

// changeCache holds the change lists read most recently, bounded by the paths they hold.
type changeCache struct {
	mu      sync.Mutex
	order   *list.List // of *cachedChanges, the most recent first
	entries map[string]*list.Element
	files   int
}

type cachedChanges struct {
	key string
	cs  *changeSet
}

func newChangeCache() *changeCache {
	return &changeCache{order: list.New(), entries: map[string]*list.Element{}}
}

func (c *changeCache) get(key string) *changeSet {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return nil
	}
	c.order.MoveToFront(e)
	return e.Value.(*cachedChanges).cs
}

func (c *changeCache) put(key string, cs *changeSet) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[key]; ok || len(cs.changes) > changeCacheFiles {
		return
	}
	c.entries[key] = c.order.PushFront(&cachedChanges{key: key, cs: cs})
	c.files += len(cs.changes)
	for c.files > changeCacheFiles || c.order.Len() > changeCacheEntries {
		last := c.order.Back()
		old := last.Value.(*cachedChanges)
		c.order.Remove(last)
		delete(c.entries, old.key)
		c.files -= len(old.cs.changes)
	}
}

// oldPath is a change's path before it: its source for a rename or copy, its own path when
// modified or deleted, and "" when added.
func oldPath(c *scmpb.PathChange) string {
	switch {
	case c.GetBefore() == nil:
		return ""
	case c.GetOldPath() != "":
		return c.GetOldPath()
	}
	return c.GetPath()
}

// fileRow is a changed file's row, without its diff: its status and paths, and whether it is
// generated or vendored, which shows no diff.
func fileRow(c *scmpb.PathChange) pages.DiffFileView {
	v := pages.DiffFileView{Name: c.GetPath(), OldName: c.GetPath(), Anchor: anchor(c.GetPath()), Status: statusName(c.GetKind())}
	if op := oldPath(c); op != "" && op != c.GetPath() {
		v.OldName = op
	}
	v.Collapsed = enry.IsVendor(c.GetPath()) || enry.IsGenerated(c.GetPath(), nil)
	return v
}

// countLines fills rows (one per change) with the lines each change adds and removes, as the SCM
// counts them without sending the lines; a symlink's target is one line.
func (s *Server) countLines(ctx context.Context, repo int64, changes []*scmpb.PathChange, rows []pages.DiffFileView) error {
	var pairs []*scmpb.DiffFilesRequest_Pair
	var at []int
	for i, c := range changes {
		pair := &scmpb.DiffFilesRequest_Pair{}
		if _, link := sideText(c.GetBefore()); link {
			rows[i].Deletion++
		} else {
			pair.OldFile = c.GetBefore().GetId()
		}
		if _, link := sideText(c.GetAfter()); link {
			rows[i].Addition++
		} else {
			pair.NewFile = c.GetAfter().GetId()
		}
		if len(pair.OldFile) > 0 || len(pair.NewFile) > 0 {
			pairs = append(pairs, pair)
			at = append(at, i)
		}
	}
	for start := 0; start < len(pairs); start += maxDiffPairs {
		end := min(start+maxDiffPairs, len(pairs))
		resp, err := s.scm.Browse.DiffFiles(ctx, &scmpb.DiffFilesRequest{RepositoryId: repo, Pairs: pairs[start:end], StatsOnly: true})
		if err != nil {
			return err
		}
		for k, d := range resp.GetDiffs() {
			r := &rows[at[start+k]]
			r.Addition += int(d.GetAdditions())
			r.Deletion += int(d.GetDeletions())
			r.IsBinary, r.TooLarge = d.GetBinary(), d.GetTooLarge()
		}
	}
	return nil
}

// fileList is a diff's file list, as its pages render it: the changes, the paths the CL's review
// threads land on that the diff does not list (none for a commit), and how many threads land on
// each row, by its path.
type fileList struct {
	cs      *changeSet
	offDiff []string
	threads map[string]int
	// body is the URL a row's diff loads from.
	body func(row pages.DiffFileView) string
	// page is the URL of a page of rows.
	page func(n int) string
}

// size is how many rows the list holds.
func (l *fileList) size() int { return len(l.cs.changes) + len(l.offDiff) }

// rows renders page n (from 1) of the list, and the URL of the next page ("" for the last).
func (s *Server) rows(ctx context.Context, repo int64, l *fileList, n int) ([]pages.DiffFileView, string, error) {
	first := (n - 1) * filePage
	last := min(first+filePage, l.size())
	var out []pages.DiffFileView
	if first < len(l.cs.changes) {
		changes := l.cs.changes[first:min(last, len(l.cs.changes))]
		out = make([]pages.DiffFileView, len(changes))
		for i, c := range changes {
			out[i] = fileRow(c)
		}
		if err := s.countLines(ctx, repo, changes, out); err != nil {
			return nil, "", err
		}
	}
	for i := max(first, len(l.cs.changes)); i < last; i++ {
		p := l.offDiff[i-len(l.cs.changes)]
		out = append(out, pages.DiffFileView{Name: p, OldName: p, Anchor: anchor(p), Status: "unmodified", OffDiff: true})
	}
	small := l.size() <= autoOpenFiles
	for i := range out {
		r := &out[i]
		r.ThreadCount = l.threads[r.Name]
		r.Open = r.ThreadCount > 0 || (small && !r.Collapsed)
		r.BodyHref = l.body(*r)
	}
	next := ""
	if last < l.size() {
		next = l.page(n + 1)
	}
	return out, next, nil
}

// totals are the lines a list's changes add and remove, when it has few enough files to count
// (known is false otherwise).
func (s *Server) totals(ctx context.Context, repo int64, cs *changeSet) (additions, deletions int, known bool, err error) {
	if len(cs.changes) > maxTotalFiles {
		return 0, 0, false, nil
	}
	rows := make([]pages.DiffFileView, len(cs.changes))
	if err := s.countLines(ctx, repo, cs.changes, rows); err != nil {
		return 0, 0, false, err
	}
	for _, r := range rows {
		additions += r.Addition
		deletions += r.Deletion
	}
	return additions, deletions, true, nil
}

// bodyQuery is where a row's diff loads from, under a page's link: the row's path and, for a
// rename or copy, its old path.
func bodyQuery(base url.Values, row pages.DiffFileView) string {
	q := url.Values{}
	for k, v := range base {
		q[k] = v
	}
	q.Set("path", row.Name)
	if row.OldName != row.Name {
		q.Set("old", row.OldName)
	}
	if row.OffDiff {
		q.Set("offdiff", "1")
	}
	return q.Encode()
}

// pageNumber is a request's ?page, from 1.
func pageNumber(q url.Values) int {
	n, err := strconv.Atoi(q.Get("page"))
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// findChange is the change a row stands for: its path, and its old path.
func findChange(cs *changeSet, path, old string) *scmpb.PathChange {
	for _, c := range cs.changes {
		if c.GetPath() != path {
			continue
		}
		if op := oldPath(c); old == "" || op == old {
			return c
		}
	}
	return nil
}
