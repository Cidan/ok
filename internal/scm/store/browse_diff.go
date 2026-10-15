package store

import (
	"context"
	"encoding/binary"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/Cidan/ok/internal/scm/copies"
	"github.com/Cidan/ok/internal/scm/diff"
	"github.com/Cidan/ok/internal/scm/encoding"
)

// PathChangeKind is how a path changed between two trees.
type PathChangeKind int

const (
	PathAdded PathChangeKind = iota + 1
	PathDeleted
	PathModified
	PathRenamed
	PathCopied
	PathTypeChanged
)

// PathChange is one changed leaf path between two trees. OldPath is a rename's or copy's
// source; Before is nil for an addition, After for a deletion.
type PathChange struct {
	Kind          PathChangeKind
	Path, OldPath string
	Before, After *BrowseValue
}

// DefaultSimilarity is the content similarity a rename or copy needs when the caller
// names none: jj's, 50%.
const DefaultSimilarity = 50

// DiffTrees reports the leaf paths that differ between two trees (the empty tree when
// from or to is zero), in path order, limited to paths under prefixes when any are given.
// With detectRenames, a deletion and an addition with the same copy history are a rename,
// and then the rest pair by content similarity at similarity percent: a deletion's match
// is a rename, a modified file's a copy.
func (s *Store) DiffTrees(ctx context.Context, repo int64, from, to encoding.ID, prefixes []string, detectRenames bool, similarity int) ([]PathChange, error) {
	prefixes, err := cleanPrefixes(prefixes)
	if err != nil {
		return nil, err
	}
	if similarity < 0 || similarity > 100 {
		return nil, invalid("rename similarity %d%% is outside 0-100", similarity)
	}
	if similarity == 0 {
		similarity = DefaultSimilarity
	}
	if from == (encoding.ID{}) {
		from = encoding.EmptyTreeID
	}
	if to == (encoding.ID{}) {
		to = encoding.EmptyTreeID
	}
	tr := treeReader{ctx: ctx, s: s, repo: repo}
	var leaves []leafChange
	collect := func(c leafChange) error {
		leaves = append(leaves, c)
		return nil
	}
	if len(prefixes) == 0 {
		if err := diffTrees(tr, from, to, "", collect); err != nil {
			return nil, err
		}
	} else {
		for _, p := range outermost(prefixes) {
			va, err := lookupPath(tr, from, p)
			if err != nil {
				return nil, err
			}
			vb, err := lookupPath(tr, to, p)
			if err != nil {
				return nil, err
			}
			la, sa := split(va)
			lb, sb := split(vb)
			if !sameValue(la, lb) {
				leaves = append(leaves, leafChange{Path: p, Before: la, After: lb})
			}
			if err := diffTrees(tr, sa, sb, p, collect); err != nil {
				return nil, err
			}
		}
	}

	out := make([]PathChange, 0, len(leaves))
	for _, l := range leaves {
		c := PathChange{Path: l.Path}
		if l.Before != nil {
			c.Before = &BrowseValue{TreeValue: *l.Before}
		}
		if l.After != nil {
			c.After = &BrowseValue{TreeValue: *l.After}
		}
		switch {
		case c.Before == nil:
			c.Kind = PathAdded
		case c.After == nil:
			c.Kind = PathDeleted
		case c.Before.Kind != c.After.Kind:
			c.Kind = PathTypeChanged
		default:
			c.Kind = PathModified
		}
		out = append(out, c)
	}
	if detectRenames {
		if out, err = s.detectRenames(ctx, repo, out, float32(similarity)/100); err != nil {
			return nil, err
		}
	}
	var vals []*BrowseValue
	for i := range out {
		if out[i].Before != nil {
			vals = append(vals, out[i].Before)
		}
		if out[i].After != nil {
			vals = append(vals, out[i].After)
		}
	}
	if err := s.fill(ctx, repo, vals); err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// outermost drops prefixes inside another, so no path is reported twice.
func outermost(prefixes []string) []string {
	sorted := append([]string(nil), prefixes...)
	sort.Strings(sorted)
	var out []string
	for _, p := range sorted {
		if len(out) > 0 {
			last := out[len(out)-1]
			if p == last || strings.HasPrefix(p, last+"/") {
				continue
			}
		}
		out = append(out, p)
	}
	return out
}

// detectRenames folds matched deletions and additions into renames, and marks additions
// copied from modified files.
func (s *Store) detectRenames(ctx context.Context, repo int64, changes []PathChange, pct float32) ([]PathChange, error) {
	gone := map[int]bool{}
	// By copy history: jj gives a renamed file's new path the old one's copy ID.
	byCopy := map[encoding.ID]int{}
	for i, c := range changes {
		if c.Kind == PathDeleted && c.Before.CopyID != nil {
			byCopy[*c.Before.CopyID] = i
		}
	}
	for i := range changes {
		c := &changes[i]
		if c.Kind != PathAdded || c.After.CopyID == nil {
			continue
		}
		d, ok := byCopy[*c.After.CopyID]
		if !ok || gone[d] {
			continue
		}
		c.Kind, c.OldPath, c.Before = PathRenamed, changes[d].Path, changes[d].Before
		gone[d] = true
	}
	// By content, as jj's git backend finds copy records.
	var tracked []copies.Change
	index := map[string]int{}
	for i, c := range changes {
		if gone[i] {
			continue
		}
		var ch copies.Change
		switch c.Kind {
		case PathAdded:
			ch = copies.Change{Kind: copies.Addition, Path: c.Path, ID: c.After.ID, Symlink: c.After.Kind == encoding.TreeValueSymlink}
		case PathDeleted:
			ch = copies.Change{Kind: copies.Deletion, Path: c.Path, ID: c.Before.ID, Symlink: c.Before.Kind == encoding.TreeValueSymlink}
		case PathModified:
			ch = copies.Change{Kind: copies.Modification, Path: c.Path, ID: c.After.ID, Symlink: c.After.Kind == encoding.TreeValueSymlink}
		default:
			continue
		}
		index[ch.Path] = i
		tracked = append(tracked, ch)
	}
	matches, err := copies.TrackAt(ctx, tracked, func(ctx context.Context, id encoding.ID) ([]byte, error) {
		return s.readFile(ctx, repo, id)
	}, pct)
	if err != nil {
		return nil, err
	}
	for _, m := range matches {
		d, ok := index[m.Dest.Path]
		if !ok {
			continue
		}
		src := index[m.Source.Path]
		c := &changes[d]
		c.OldPath = m.Source.Path
		if m.Copy {
			c.Kind = PathCopied
			before := *changes[src].After
			c.Before = &before
			continue
		}
		c.Kind = PathRenamed
		c.Before = changes[src].Before
		gone[src] = true
	}
	out := changes[:0:0]
	for i, c := range changes {
		if !gone[i] {
			out = append(out, c)
		}
	}
	return out, nil
}

// DiffOp is a diff line's operation.
type DiffOp int

const (
	DiffContext DiffOp = iota + 1
	DiffAdd
	DiffDelete
)

// DiffLine is one line of a hunk. NoNewline marks a file's last line when it has no
// terminating newline.
type DiffLine struct {
	Op        DiffOp
	Text      []byte
	NoNewline bool
}

// Hunk is a run of changes with its context. Starts are 1-based; a side with no lines
// starts at the line before (0 at the top), as unified diffs number them.
type Hunk struct {
	OldStart, OldLines int
	NewStart, NewLines int
	Lines              []DiffLine
}

// FileDiff is one file pair's diff.
type FileDiff struct {
	Binary, TooLarge     bool
	Hunks                []Hunk
	Additions, Deletions int // the lines the diff adds and removes
}

// FilePair names two versions of a file; a nil side is absent.
type FilePair struct {
	Old, New *encoding.ID
}

// Limits beyond which DiffFiles reports a file pair too large to diff, and how many pairs it
// diffs at once.
const (
	maxDiffBytes    = 8 << 20
	maxDiffLines    = 200_000
	diffFilesAtOnce = 8
)

// DiffFiles diffs file pairs line by line (the histogram algorithm), with contextLines
// of context around each run of changes, several pairs at once; with statsOnly, only each pair's
// counts, without hunks. A pair either side of which is binary is only marked so, and one too
// large to diff only marked too large. Every side's manifest is read with one query, which also
// confirms the repository holds it; a pair diffed before is answered from the diff cache, and the
// rest's contents are read a window at a time — at most diffWindowBytes, read together
// (contents) — before the window's pairs are diffed and cached.
func (s *Store) DiffFiles(ctx context.Context, repo int64, pairs []FilePair, contextLines int, statsOnly bool) ([]FileDiff, error) {
	if contextLines < 0 {
		return nil, invalid("context lines %d is negative", contextLines)
	}
	var sides []encoding.ID
	for _, p := range pairs {
		for _, id := range []*encoding.ID{p.Old, p.New} {
			if id != nil {
				sides = append(sides, *id)
			}
		}
	}
	ms, err := s.manifests(ctx, repo, sides)
	if err != nil {
		return nil, err
	}
	for _, id := range sides {
		if ms[id] == nil {
			return nil, notFound("file %s", id)
		}
	}
	out := make([]FileDiff, len(pairs))
	var pending []int
	for i, p := range pairs {
		if d, ok := s.diffs.get(diffKey(repo, p, contextLines, statsOnly)); ok {
			out[i] = d
			continue
		}
		pending = append(pending, i)
	}
	// readable is the side's file when its contents are to be read: present and small enough.
	readable := func(id *encoding.ID) (encoding.ID, bool) {
		if id == nil || ms[*id].size > maxDiffBytes {
			return encoding.ID{}, false
		}
		return *id, true
	}
	for start := 0; start < len(pending); {
		end := start
		var bytes uint64
		var want []encoding.ID
		for end < len(pending) {
			p := pairs[pending[end]]
			var need uint64
			var ids []encoding.ID
			for _, side := range []*encoding.ID{p.Old, p.New} {
				if id, ok := readable(side); ok {
					need += ms[id].size
					ids = append(ids, id)
				}
			}
			if end > start && bytes+need > s.reads.diffWindowBytes {
				break
			}
			bytes += need
			want = append(want, ids...)
			end++
		}
		files, err := s.contents(ctx, repo, ms, want)
		if err != nil {
			return nil, err
		}
		var wg sync.WaitGroup
		sem := make(chan struct{}, diffFilesAtOnce)
		for _, i := range pending[start:end] {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				d := diffPair(pairs[i], ms, files, contextLines, statsOnly)
				out[i] = d
				s.cacheDiff(repo, pairs[i], contextLines, statsOnly, d)
			}()
		}
		wg.Wait()
		start = end
	}
	return out, nil
}

// diffKey is a pair's key in the diff cache: its repository, its sides' IDs, and its context
// lines, or none for counts alone.
func diffKey(repo int64, p FilePair, contextLines int, statsOnly bool) string {
	b := binary.LittleEndian.AppendUint64(make([]byte, 0, 8+1+64+4), uint64(repo))
	var sides byte
	if p.Old != nil {
		sides |= 1
	}
	if p.New != nil {
		sides |= 2
	}
	b = append(b, sides)
	for _, id := range []*encoding.ID{p.Old, p.New} {
		if id != nil {
			b = append(b, id[:]...)
		} else {
			b = append(b, make([]byte, encoding.IDLen)...)
		}
	}
	mode := uint32(contextLines)
	if statsOnly {
		mode = math.MaxUint32
	}
	return string(binary.LittleEndian.AppendUint32(b, mode))
}

// Estimates of a cached diff's bookkeeping beyond its lines' text, for the diff cache's budget.
const (
	diffEntryBytes = 128
	diffHunkBytes  = 64
	diffLineBytes  = 48
)

// cacheDiff keeps d in the diff cache, its lines' text copied into one buffer of its own so the
// cache never holds the files it was diffed from; a full diff also answers counts alone.
func (s *Store) cacheDiff(repo int64, p FilePair, contextLines int, statsOnly bool, d FileDiff) {
	stats := FileDiff{Binary: d.Binary, TooLarge: d.TooLarge, Additions: d.Additions, Deletions: d.Deletions}
	s.diffs.put(diffKey(repo, p, 0, true), stats, diffEntryBytes)
	if statsOnly {
		return
	}
	text, lines := 0, 0
	for _, h := range d.Hunks {
		for _, l := range h.Lines {
			text += len(l.Text)
		}
		lines += len(h.Lines)
	}
	buf := make([]byte, 0, text)
	c := stats
	c.Hunks = make([]Hunk, len(d.Hunks))
	for i, h := range d.Hunks {
		ch := h
		ch.Lines = make([]DiffLine, len(h.Lines))
		for j, l := range h.Lines {
			at := len(buf)
			buf = append(buf, l.Text...)
			ch.Lines[j] = DiffLine{Op: l.Op, Text: buf[at:len(buf):len(buf)], NoNewline: l.NoNewline}
		}
		c.Hunks[i] = ch
	}
	s.diffs.put(diffKey(repo, p, contextLines, false), c, diffEntryBytes+text+len(d.Hunks)*diffHunkBytes+lines*diffLineBytes)
}

// diffPair diffs one file pair from its sides' manifests and the contents of those small enough
// to diff; with statsOnly, its counts alone.
func diffPair(p FilePair, ms map[encoding.ID]*manifest, files map[encoding.ID][]byte, contextLines int, statsOnly bool) FileDiff {
	var out FileDiff
	side := func(id *encoding.ID) ([]byte, bool) {
		if id == nil {
			return nil, false
		}
		if ms[*id].size > maxDiffBytes {
			return nil, true
		}
		return files[*id], false
	}
	old, oldBig := side(p.Old)
	cur, curBig := side(p.New)
	switch {
	case oldBig || curBig:
		out.TooLarge = true
		return out
	case copies.Binary(old) || copies.Binary(cur):
		out.Binary = true
		return out
	}
	before, after := diff.Lines(old), diff.Lines(cur)
	if len(before) > maxDiffLines || len(after) > maxDiffLines {
		out.TooLarge = true
		return out
	}
	d := diff.Compute(before, after)
	for _, r := range d.Hunks() {
		out.Deletions += r.BeforeEnd - r.BeforeStart
		out.Additions += r.AfterEnd - r.AfterStart
	}
	if !statsOnly {
		out.Hunks = hunks(before, after, d, contextLines)
	}
	return out
}

// hunks groups a diff's runs of changes, merging runs whose context would touch, into
// unified-diff hunks.
func hunks(before, after [][]byte, d diff.Diff, context int) []Hunk {
	runs := d.Hunks()
	if len(runs) == 0 {
		return nil
	}
	line := func(l []byte, op DiffOp) DiffLine {
		if n := len(l); n > 0 && l[n-1] == '\n' {
			return DiffLine{Op: op, Text: l[:n-1]}
		}
		return DiffLine{Op: op, Text: l, NoNewline: true}
	}
	var out []Hunk
	for i := 0; i < len(runs); {
		// The group of runs whose gaps are within twice the context.
		j := i
		for j+1 < len(runs) && runs[j+1].BeforeStart-runs[j].BeforeEnd <= 2*context {
			j++
		}
		first, last := runs[i], runs[j]
		lead := min(context, first.BeforeStart)
		trail := min(context, len(before)-last.BeforeEnd)
		oldStart := first.BeforeStart - lead
		newStart := first.AfterStart - lead
		oldEnd := last.BeforeEnd + trail
		newEnd := last.AfterEnd + trail
		h := Hunk{OldLines: oldEnd - oldStart, NewLines: newEnd - newStart}
		h.OldStart, h.NewStart = oldStart+1, newStart+1
		if h.OldLines == 0 {
			h.OldStart = oldStart
		}
		if h.NewLines == 0 {
			h.NewStart = newStart
		}
		o, n := oldStart, newStart
		for k := i; k <= j; k++ {
			r := runs[k]
			for o < r.BeforeStart {
				h.Lines = append(h.Lines, line(before[o], DiffContext))
				o, n = o+1, n+1
			}
			for ; o < r.BeforeEnd; o++ {
				h.Lines = append(h.Lines, line(before[o], DiffDelete))
			}
			for ; n < r.AfterEnd; n++ {
				h.Lines = append(h.Lines, line(after[n], DiffAdd))
			}
		}
		for o < oldEnd {
			h.Lines = append(h.Lines, line(before[o], DiffContext))
			o, n = o+1, n+1
		}
		out = append(out, h)
		i = j + 1
	}
	return out
}

// AnnotateRange attributes lines [FirstLine, FirstLine+LineCount) (1-based) of a file to
// the commit that last changed them.
type AnnotateRange struct {
	FirstLine, LineCount int
	Commit               encoding.ID
}

// fileAt reads the file at a path of a commit's tree, or reports it absent (a missing
// path, or one that is not a file).
func (s *Store) fileAt(ctx context.Context, repo int64, tree encoding.ID, p string) ([]byte, bool, error) {
	v, err := lookupPath(treeReader{ctx: ctx, s: s, repo: repo}, tree, p)
	if err != nil || v == nil || v.Kind != encoding.TreeValueFile {
		return nil, false, err
	}
	b, err := s.readFile(ctx, repo, v.ID)
	return b, err == nil, err
}

// Annotate attributes each line of a file at a commit to the commit that last changed
// it, following the commit's first parents back to main and then main's history of the
// path. Renames are not followed: a line is the path's.
func (s *Store) Annotate(ctx context.Context, repo int64, commit encoding.ID, p string) ([]AnnotateRange, error) {
	p, err := cleanPath(p)
	if err != nil {
		return nil, err
	}
	if p == "" {
		return nil, invalid("annotate needs a file path")
	}
	tree, err := s.resolvedTree(ctx, repo, commit)
	if err != nil {
		return nil, err
	}
	data, ok, err := s.fileAt(ctx, repo, tree, p)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, notFound("file %s at commit %s", p, commit)
	}
	if copies.Binary(data) {
		return nil, invalid("%s is binary", p)
	}
	lines := diff.Lines(data)
	blame := make([]encoding.ID, len(lines))
	// pending maps each line of the version being traced to the output line it is.
	pending := make([]int, len(lines))
	for i := range pending {
		pending[i] = i
	}
	cur := lines
	// attribute gives the lines of cur that its predecessor prev lacks to by, and moves
	// the rest onto prev's line numbers.
	attribute := func(prev [][]byte, by encoding.ID) {
		d := diff.Compute(prev, cur)
		var next []int
		pi := 0
		for ci := range cur {
			if d.Added[ci] {
				if pending[ci] >= 0 {
					blame[pending[ci]] = by
				}
				continue
			}
			for pi < len(prev) && d.Removed[pi] {
				next = append(next, -1)
				pi++
			}
			next = append(next, pending[ci])
			pi++
		}
		for pi < len(prev) {
			next = append(next, -1)
			pi++
		}
		pending, cur = next, prev
	}
	left := func() bool {
		for _, o := range pending {
			if o >= 0 {
				return true
			}
		}
		return false
	}
	// Off main: each commit's own changes to the path, back along first parents.
	at := commit
	for {
		on, err := onMain(ctx, s.pool, repo, at)
		if err != nil {
			return nil, err
		}
		if on {
			break
		}
		c, err := s.readCommit(ctx, repo, at)
		if err != nil {
			return nil, err
		}
		parent := c.Parents[0]
		ptree, err := s.resolvedTree(ctx, repo, parent)
		if err != nil {
			return nil, err
		}
		prev, _, err := s.fileAt(ctx, repo, ptree, p)
		if err != nil {
			return nil, err
		}
		attribute(diff.Lines(prev), at)
		at = parent
		if !left() {
			return ranges(blame), nil
		}
	}
	// On main: the commits that changed the path, newest first.
	if at != encoding.RootCommitID && left() {
		var gen int64
		if err := s.pool.QueryRow(ctx, `SELECT generation FROM commit WHERE repository_id = $1 AND id = $2`, repo, at[:]).Scan(&gen); err != nil {
			return nil, err
		}
		rows, err := s.pool.Query(ctx, `SELECT commit_id FROM commit_path WHERE repository_id = $1 AND path = $2 AND generation <= $3 ORDER BY generation DESC`, repo, p, gen)
		if err != nil {
			return nil, err
		}
		var touching []encoding.ID
		for rows.Next() {
			var b []byte
			if err := rows.Scan(&b); err != nil {
				rows.Close()
				return nil, err
			}
			id, err := toID(b)
			if err != nil {
				rows.Close()
				return nil, err
			}
			touching = append(touching, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		for _, m := range touching {
			if !left() {
				break
			}
			c, err := s.readCommit(ctx, repo, m)
			if err != nil {
				return nil, err
			}
			ptree, err := s.resolvedTree(ctx, repo, c.Parents[0])
			if err != nil {
				return nil, err
			}
			prev, _, err := s.fileAt(ctx, repo, ptree, p)
			if err != nil {
				return nil, err
			}
			attribute(diff.Lines(prev), m)
		}
	}
	return ranges(blame), nil
}

// ranges merges consecutive lines attributed to the same commit. A line no commit
// changed (on the root commit's tree, which is empty, there are none) keeps the zero ID.
func ranges(blame []encoding.ID) []AnnotateRange {
	var out []AnnotateRange
	for i, c := range blame {
		if n := len(out); n > 0 && out[n-1].Commit == c {
			out[n-1].LineCount++
			continue
		}
		out = append(out, AnnotateRange{FirstLine: i + 1, LineCount: 1, Commit: c})
	}
	return out
}
