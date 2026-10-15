package web

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/control/db"
	pages "github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/render"
	"github.com/Cidan/ok/internal/control/review"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Diff limits: how many file pairs one DiffFiles call names, how large a file is read whole to
// highlight, and how many files are read at once.
const (
	maxDiffPairs     = 1000
	maxHighlightFile = 1 << 20
	readParallelism  = 8
)

// diffFile is one file of a rendered diff and what the page needs beyond its view: its
// path on each side and the highlighted lines of each side ("" and nil when absent or not
// read).
type diffFile struct {
	view             pages.DiffFileView
	oldPath, newPath string
	oldLines         [][]render.Token
	newLines         [][]render.Token
}

// diffResult is a rendered diff of some of the files that changed between two commits.
type diffResult struct {
	files            []pages.DiffFileView
	sides            []diffFile // parallel to files
	fromTree, toTree []byte
}

func anchor(p string) string {
	h := fnv.New64a()
	h.Write([]byte(p))
	return fmt.Sprintf("diff-%016x", h.Sum64())
}

func statusName(k scmpb.PathChange_Kind) string {
	switch k {
	case scmpb.PathChange_KIND_ADDED:
		return "added"
	case scmpb.PathChange_KIND_DELETED:
		return "deleted"
	case scmpb.PathChange_KIND_RENAMED:
		return "renamed"
	case scmpb.PathChange_KIND_COPIED:
		return "copied"
	}
	return "modified"
}

// sideText is the text of one side of a change when it is not a regular file's: a
// symlink's target. ok is false for a file (or an absent side).
func sideText(v *scmpb.TreeValue) (string, bool) {
	if v.GetType() == scmpb.EntryType_ENTRY_TYPE_SYMLINK {
		return v.GetSymlinkTarget(), true
	}
	return "", false
}

// renderDiff renders the diffs of some of the files that changed from commit from to commit to
// (cs): each highlighted with its whole file for context and with word diffs on changed lines.
func (s *Server) renderDiff(ctx context.Context, repo int64, from, to []byte, cs *changeSet, changes []*scmpb.PathChange) (*diffResult, error) {
	out := &diffResult{fromTree: cs.fromTree, toTree: cs.toTree}
	files := make([]diffFile, len(changes))
	var pairs []*scmpb.DiffFilesRequest_Pair
	var paired []int
	for i, c := range changes {
		f := &files[i]
		f.newPath = c.GetPath()
		f.oldPath = c.GetPath()
		if c.GetOldPath() != "" {
			f.oldPath = c.GetOldPath()
		}
		if c.GetBefore() == nil {
			f.oldPath = ""
		}
		if c.GetAfter() == nil {
			f.newPath = ""
		}
		f.view = fileRow(c)
		_, oldLink := sideText(c.GetBefore())
		_, newLink := sideText(c.GetAfter())
		if oldLink || newLink {
			continue // a symlink side: diffed here, not by the SCM
		}
		pairs = append(pairs, &scmpb.DiffFilesRequest_Pair{OldFile: c.GetBefore().GetId(), NewFile: c.GetAfter().GetId()})
		paired = append(paired, i)
	}

	// The line diffs, from the SCM.
	diffs := make([]*scmpb.FileDiff, len(changes))
	for start := 0; start < len(pairs); start += maxDiffPairs {
		end := min(start+maxDiffPairs, len(pairs))
		resp, err := s.scm.Browse.DiffFiles(ctx, &scmpb.DiffFilesRequest{RepositoryId: repo, Pairs: pairs[start:end], ContextLines: 3})
		if err != nil {
			return nil, err
		}
		for k, d := range resp.GetDiffs() {
			diffs[paired[start+k]] = d
		}
	}

	// Each changed file's two versions, read whole to highlight in context.
	var wg sync.WaitGroup
	var mu sync.Mutex
	var readErr error
	sem := make(chan struct{}, readParallelism)
	read := func(commit []byte, p string, v *scmpb.TreeValue, into *[][]render.Token) {
		if v.GetType() != scmpb.EntryType_ENTRY_TYPE_FILE || v.GetSize() > maxHighlightFile {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			data, _, _, err := s.scm.ReadFile(ctx, repo, commit, p, 0)
			if err != nil {
				mu.Lock()
				readErr = errors.Join(readErr, err)
				mu.Unlock()
				return
			}
			*into = render.Lines(p, data)
		}()
	}
	for i, c := range changes {
		d := diffs[i]
		if d != nil && (d.GetBinary() || d.GetTooLarge() || files[i].view.Collapsed) {
			continue
		}
		if files[i].oldPath != "" {
			read(from, files[i].oldPath, c.GetBefore(), &files[i].oldLines)
		}
		if files[i].newPath != "" {
			read(to, files[i].newPath, c.GetAfter(), &files[i].newLines)
		}
	}
	wg.Wait()
	if readErr != nil {
		return nil, readErr
	}

	for i, c := range changes {
		f := &files[i]
		d := diffs[i]
		if d == nil {
			// A symlink side: the change replaces one text with the other.
			d = replacement(c, f)
		}
		f.view.IsBinary = d.GetBinary()
		f.view.TooLarge = d.GetTooLarge()
		for _, h := range d.GetHunks() {
			for _, l := range h.GetLines() {
				switch l.GetOp() {
				case scmpb.DiffLine_OP_ADD:
					f.view.Addition++
				case scmpb.DiffLine_OP_DELETE:
					f.view.Deletion++
				}
			}
		}
		if !f.view.IsBinary && !f.view.TooLarge && !f.view.Collapsed {
			for _, h := range d.GetHunks() {
				f.view.Sections = append(f.view.Sections, hunkRows(h, f))
			}
		}
		out.files = append(out.files, f.view)
		out.sides = append(out.sides, *f)
	}
	return out, nil
}

// replacement is the diff of a change a symlink takes part in: every line of the old side
// removed and every line of the new side added. A file side is read from the lines the
// change has already read; a file too large to read is shown as too large.
func replacement(c *scmpb.PathChange, f *diffFile) *scmpb.FileDiff {
	side := func(v *scmpb.TreeValue, lines [][]render.Token) ([]string, bool) {
		if v == nil {
			return nil, true
		}
		if t, ok := sideText(v); ok {
			return []string{t}, true
		}
		if lines == nil && v.GetSize() > 0 {
			return nil, false
		}
		out := make([]string, len(lines))
		for i, l := range lines {
			var b strings.Builder
			for _, t := range l {
				b.WriteString(t.Text)
			}
			out[i] = b.String()
		}
		return out, true
	}
	old, okOld := side(c.GetBefore(), f.oldLines)
	cur, okNew := side(c.GetAfter(), f.newLines)
	if !okOld || !okNew {
		return &scmpb.FileDiff{TooLarge: true}
	}
	if t, ok := sideText(c.GetBefore()); ok {
		f.oldLines = [][]render.Token{render.Plain(t)}
	}
	if t, ok := sideText(c.GetAfter()); ok {
		f.newLines = [][]render.Token{render.Plain(t)}
	}
	h := &scmpb.Hunk{OldStart: 1, OldLines: int32(len(old)), NewStart: 1, NewLines: int32(len(cur))}
	if len(old) == 0 {
		h.OldStart = 0
	}
	if len(cur) == 0 {
		h.NewStart = 0
	}
	for _, l := range old {
		h.Lines = append(h.Lines, &scmpb.DiffLine{Op: scmpb.DiffLine_OP_DELETE, Text: []byte(l)})
	}
	for _, l := range cur {
		h.Lines = append(h.Lines, &scmpb.DiffLine{Op: scmpb.DiffLine_OP_ADD, Text: []byte(l)})
	}
	return &scmpb.FileDiff{Hunks: []*scmpb.Hunk{h}}
}

// tokensAt is a line's highlighting from its whole file when that was read, else the line
// highlighted alone.
func tokensAt(lines [][]render.Token, n int, name string, text []byte) []render.Token {
	if n >= 1 && n <= len(lines) {
		return lines[n-1]
	}
	return render.LineTokens(name, string(text))
}

// hunkRows renders one hunk as diff rows: its header, then each line, with a removed run
// and the added run replacing it paired line by line for word diffs.
func hunkRows(h *scmpb.Hunk, f *diffFile) []pages.DiffLineRow {
	name := f.newPath
	if name == "" {
		name = f.oldPath
	}
	rows := []pages.DiffLineRow{{
		Type:    "tag",
		Content: fmt.Sprintf("@@ -%d,%d +%d,%d @@", h.GetOldStart(), h.GetOldLines(), h.GetNewStart(), h.GetNewLines()),
	}}
	o, n := int(h.GetOldStart()), int(h.GetNewStart())
	if h.GetOldLines() == 0 {
		o++
	}
	if h.GetNewLines() == 0 {
		n++
	}
	lines := h.GetLines()
	for i := 0; i < len(lines); {
		l := lines[i]
		if l.GetOp() == scmpb.DiffLine_OP_CONTEXT {
			rows = append(rows, pages.DiffLineRow{Type: "same", LeftIdx: o, RightIdx: n, Content: render.HTML(tokensAt(f.newLines, n, name, l.GetText()))})
			o, n, i = o+1, n+1, i+1
			continue
		}
		// A run of removed lines, then the run of added lines after it.
		var dels, adds []*scmpb.DiffLine
		for i < len(lines) && lines[i].GetOp() == scmpb.DiffLine_OP_DELETE {
			dels = append(dels, lines[i])
			i++
		}
		for i < len(lines) && lines[i].GetOp() == scmpb.DiffLine_OP_ADD {
			adds = append(adds, lines[i])
			i++
		}
		delHTML := make([]string, len(dels))
		addHTML := make([]string, len(adds))
		for k, d := range dels {
			delHTML[k] = render.HTML(tokensAt(f.oldLines, o+k, name, d.GetText()))
		}
		for k, a := range adds {
			addHTML[k] = render.HTML(tokensAt(f.newLines, n+k, name, a.GetText()))
		}
		for k := range min(len(dels), len(adds)) {
			if oh, nh, ok := render.WordDiff(tokensAt(f.oldLines, o+k, name, dels[k].GetText()), tokensAt(f.newLines, n+k, name, adds[k].GetText())); ok {
				delHTML[k], addHTML[k] = oh, nh
			}
		}
		for k := range dels {
			rows = append(rows, pages.DiffLineRow{Type: "del", LeftIdx: o + k, Content: delHTML[k]})
		}
		for k := range adds {
			rows = append(rows, pages.DiffLineRow{Type: "add", RightIdx: n + k, Content: addHTML[k]})
		}
		o, n = o+len(dels), n+len(adds)
	}
	return rows
}

// Thread placement: where a CL's review threads render on the viewed patchset's diff.

// threadContext is how many unchanged lines surround a thread's line when the diff's
// hunks don't show it, like the hunks' own context.
const threadContext = 3

// placedThread is a thread to place on the viewed patchset's diff: its side, its file —
// by the file's path in the patchset (byName: a thread written on the viewed patchset, or
// a patchset-side port) or by its path in the base (a base-side port) — and its line (0
// = the file as a whole). gone says the file no longer exists on the thread's side.
type placedThread struct {
	view   *pages.CommentThread
	side   string
	path   string
	byName bool
	line   int64
	gone   bool
}

// placer pins threads onto the viewed patchset's diff files, adding the context lines
// and off-diff files they need.
type placer struct {
	ctx      context.Context
	s        *Server
	repo     int64
	base, ps []byte
	d        *diffResult
}

func (p *placer) place(threads []placedThread) error {
	onRows := map[int][]placedThread{}
	onFile := map[int][]*pages.CommentThread{}
	for _, t := range threads {
		fi := p.fileIndex(t)
		if fi < 0 {
			var err error
			if fi, err = p.addOffDiffFile(t.path); err != nil {
				return err
			}
		}
		f := p.d.files[fi]
		t.view.FilePath = f.Name
		if t.line > 0 && !t.gone && showsSide(f, t.side) && hasBody(f) {
			onRows[fi] = append(onRows[fi], t)
		} else {
			onFile[fi] = append(onFile[fi], t.view)
		}
	}
	for fi, ts := range onRows {
		p.showLines(fi, ts)
		for _, t := range ts {
			if !attachToRow(&p.d.files[fi], t.side, t.line, *t.view) {
				t.view.Line = 0
				onFile[fi] = append(onFile[fi], t.view)
			}
		}
	}
	for fi, vs := range onFile {
		for _, v := range vs {
			p.d.files[fi].Threads = append(p.d.files[fi].Threads, *v)
		}
	}
	return nil
}

// fileIndex finds the diff file a thread belongs to (-1 when the diff doesn't show it).
func (p *placer) fileIndex(t placedThread) int {
	for i, f := range p.d.files {
		if (t.byName && f.Name == t.path) || (!t.byName && f.OldName == t.path) {
			return i
		}
	}
	return -1
}

// addOffDiffFile lists a file the diff doesn't show, for the threads that land on it, and
// returns its index. Comparing the file in the patchset and its base says why it isn't
// shown: it exists unchanged, neither has it (removed), or it changed but the diff was cut
// short before it.
func (p *placer) addOffDiffFile(filePath string) (int, error) {
	resp, err := p.s.scm.Browse.DiffTrees(p.ctx, &scmpb.DiffTreesRequest{RepositoryId: p.repo, FromTree: p.d.fromTree, ToTree: p.d.toTree, PathPrefixes: []string{filePath}})
	if err != nil {
		return 0, err
	}
	side := diffFile{view: pages.DiffFileView{Name: filePath, OldName: filePath, Anchor: anchor(filePath), OffDiff: true}}
	for _, c := range resp.GetChanges() {
		if c.GetPath() == filePath {
			side.view.Status = statusName(c.GetKind())
		}
	}
	if side.view.Status == "" {
		data, v, truncated, err := p.s.scm.ReadFile(p.ctx, p.repo, p.ps, filePath, maxHighlightFile)
		switch {
		case status.Code(err) == codes.NotFound || status.Code(err) == codes.InvalidArgument:
			side.view.Status = "removed"
		case err != nil:
			return 0, err
		case v.GetType() != scmpb.EntryType_ENTRY_TYPE_FILE || truncated || render.IsBinary(data):
			side.view.Status = "unmodified"
			side.view.IsBinary = true
		default:
			side.view.Status = "unmodified"
			side.newPath = filePath
			side.newLines = render.Lines(filePath, data)
		}
	}
	p.d.files = append(p.d.files, side.view)
	p.d.sides = append(p.d.sides, side)
	return len(p.d.files) - 1, nil
}

// showLines makes each thread's line visible in file fi's diff: a line outside its hunks
// gets the unchanged lines around it, read from the patchset. A deleted file needs none —
// its one hunk holds every base line.
func (p *placer) showLines(fi int, threads []placedThread) {
	f := &p.d.files[fi]
	lines := p.d.sides[fi].newLines
	if f.Status == "deleted" || lines == nil {
		return
	}
	spans := diffSpans(f.Sections)
	anchors := make([]review.LineAnchor, 0, len(threads))
	for _, t := range threads {
		a := review.LineAnchor{Side: db.SidePatchset, Line: t.line}
		if t.side == "left" {
			a.Side = db.SideBase
		}
		anchors = append(anchors, a)
	}
	windows := review.ContextWindows(spans, anchors, threadContext)
	if len(windows) == 0 {
		return
	}
	excerpts := map[int][][]pages.DiffLineRow{}
	for _, w := range windows {
		first, last := int(w.First), min(int(w.Last), len(lines))
		if first > last {
			continue
		}
		n := last - first + 1
		rows := []pages.DiffLineRow{{
			Type:    "tag",
			Content: fmt.Sprintf("@@ -%d,%d +%d,%d @@", first-int(w.Offset), n, first, n),
		}}
		for k := first; k <= last; k++ {
			rows = append(rows, pages.DiffLineRow{Type: "same", LeftIdx: k - int(w.Offset), RightIdx: k, Content: render.HTML(lines[k-1])})
		}
		excerpts[w.Before] = append(excerpts[w.Before], rows)
	}
	f.Sections = withContext(f.Sections, len(spans), excerpts)
}

// diffSpans is the lines each of a file's rendered hunks shows, in order.
func diffSpans(sections [][]pages.DiffLineRow) []review.DiffSpan {
	var spans []review.DiffSpan
	for _, sec := range sections {
		var s review.DiffSpan
		for _, ln := range sec {
			if ln.LeftIdx > 0 {
				if s.FirstLeft == 0 {
					s.FirstLeft = int64(ln.LeftIdx)
				}
				s.LastLeft = int64(ln.LeftIdx)
			}
			if ln.RightIdx > 0 {
				if s.FirstRight == 0 {
					s.FirstRight = int64(ln.RightIdx)
				}
				s.LastRight = int64(ln.RightIdx)
			}
		}
		if s.FirstLeft > 0 || s.FirstRight > 0 {
			spans = append(spans, s)
		}
	}
	return spans
}

// sectionShowsLines reports whether a rendered section shows any line of the file.
func sectionShowsLines(sec []pages.DiffLineRow) bool {
	for _, ln := range sec {
		if ln.LeftIdx > 0 || ln.RightIdx > 0 {
			return true
		}
	}
	return false
}

// withContext splices context excerpts into a file's sections: excerpts[g] go just before
// the g-th section that shows lines, and excerpts[spans] after the last one.
func withContext(sections [][]pages.DiffLineRow, spans int, excerpts map[int][][]pages.DiffLineRow) [][]pages.DiffLineRow {
	out := make([][]pages.DiffLineRow, 0, len(sections)+len(excerpts))
	if spans == 0 {
		out = append(out, excerpts[0]...)
	}
	span := 0
	for _, sec := range sections {
		if !sectionShowsLines(sec) {
			out = append(out, sec)
			continue
		}
		out = append(out, excerpts[span]...)
		out = append(out, sec)
		span++
		if span == spans {
			out = append(out, excerpts[span]...)
		}
	}
	return out
}

// attachToRow pins thread t under the row showing line on side of file f, reporting
// whether that row exists.
func attachToRow(f *pages.DiffFileView, side string, line int64, t pages.CommentThread) bool {
	for si := range f.Sections {
		rows := f.Sections[si]
		for ri := range rows {
			if (side == "right" && int64(rows[ri].RightIdx) == line) || (side == "left" && int64(rows[ri].LeftIdx) == line) {
				rows[ri].Threads = append(rows[ri].Threads, t)
				return true
			}
		}
	}
	return false
}

// showsSide reports whether file f's diff has lines on side.
func showsSide(f pages.DiffFileView, side string) bool {
	switch f.Status {
	case "removed":
		return false
	case "deleted":
		return side == "left"
	case "added":
		return side == "right"
	default:
		return true
	}
}

// hasBody reports whether file f renders diff rows at all (an off-diff file only when it
// is unchanged: its rows are the lines around its threads).
func hasBody(f pages.DiffFileView) bool {
	return !f.IsBinary && !f.TooLarge && !f.Collapsed && (!f.OffDiff || f.Status == "unmodified")
}
