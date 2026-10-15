package review

import (
	"log/slog"
	"sort"

	"github.com/Cidan/ok/internal/control/db"
)

// Porting (Gerrit's "ported comments") re-anchors an unresolved inline thread written on an earlier
// patchset onto the patchset being viewed, by following its position through what changed in
// between. A SideRight anchor — a line of the revision's file — follows the diff between the two
// revisions' commits; a SideLeft anchor — a line of the base file — follows the diff between their
// parent commits, which is empty unless the change was rebased. Files are followed across renames.
// Porting is a view computed at render time: a thread stays recorded against the patchset it was
// written on, so replying to or resolving a ported thread is the same as on its own patchset.

// PortState is how faithfully a ported thread's anchor survived the change between its patchset
// and the viewed one.
type PortState int

const (
	// PortExact — the anchored line (or file) is unchanged; the thread sits on the same content.
	PortExact PortState = iota
	// PortLineChanged — the anchored line was modified or removed; the thread sits at the nearest line.
	PortLineChanged
	// PortFileDeleted — the file no longer exists on the thread's side; Path is where it was last.
	PortFileDeleted
	// PortUnknown — lines can't be followed through the change (a binary change, or git failed).
	PortUnknown
)

// PortedAnchor is where a thread lands on the viewed patchset: Path is the file on the thread's diff
// side of that patchset — its parent commit's tree for SideLeft, its own commit's for SideRight — and
// Line the 1-based line there (0 = the file as a whole: a file-level thread, or no line survived).
type PortedAnchor struct {
	Path  string
	Line  int64
	State PortState
}

// Hunk is one change of a zero-context unified diff (`@@ -OldStart,OldLines +NewStart,NewLines @@`):
// OldLines lines from OldStart are replaced by NewLines lines from NewStart. A side with no lines
// names the line just before the change on that side (0 = the top of the file).
type Hunk struct {
	OldStart, OldLines, NewStart, NewLines int64
}

// FileChange is how one file changed between two trees: its path on each side (OldPath "" for an
// addition, NewPath "" for a deletion), whether git diffed it as binary, and its zero-context hunks
// in file order.
type FileChange struct {
	OldPath, NewPath string
	Binary           bool
	Hunks            []Hunk
}

// revision is one patchset: its commit and the base commit its diff is computed against,
// by hex ID.
type revision struct {
	Patchset     int32
	Commit, Base string
}

// treeDiffer runs the two diffs porting needs, between commits named by hex ID.
type treeDiffer interface {
	// nameStatus lists every file the diff from→to changes, following renames.
	nameStatus(from, to string) ([]FileChange, error)
	// patch is the zero-context diff from→to limited to the given paths, following
	// renames, keyed by old path.
	patch(from, to string, pathspec []string) (map[string]patchFile, error)
}

// patchFile is one file's part of a zero-context patch.
type patchFile struct {
	binary bool
	hunks  []Hunk
}

func portThreads(d treeDiffer, revs []revision, target revision, threads []Thread) map[int64]PortedAnchor {
	byPatchSet := make(map[int32]revision, len(revs))
	for _, r := range revs {
		byPatchSet[r.Patchset] = r
	}

	type treePair struct{ from, to string }
	type position struct {
		root int64
		path string
		line int64
	}
	listed := map[treePair][]FileChange{}
	listErr := map[treePair]error{}
	list := func(from, to string) ([]FileChange, error) {
		p := treePair{from, to}
		if _, done := listed[p]; !done {
			listed[p], listErr[p] = d.nameStatus(from, to)
			if listErr[p] != nil {
				slog.Warn("port review threads: list changes", "from", from, "to", to, "err", listErr[p])
			}
		}
		return listed[p], listErr[p]
	}

	ported := map[int64]PortedAnchor{}
	pending := map[treePair][]position{}
	for _, t := range threads {
		src, ok := byPatchSet[t.Patchset]
		if t.Resolved || t.FilePath == "" || !ok || t.Patchset >= target.Patchset {
			continue
		}
		pos := position{root: t.RootID, path: t.FilePath, line: int64(t.Line)}
		pair := treePair{src.Commit, target.Commit}
		if t.Side == db.SideBase {
			// The diff view names a file by its path in the revision; a left-side line lives in the
			// base tree, under the rename's source when the revision renamed the file.
			changes, err := list(src.Base, src.Commit)
			if err != nil {
				ported[t.RootID] = PortedAnchor{Path: t.FilePath, State: PortUnknown}
				continue
			}
			pos.path = renameSource(changes, t.FilePath)
			pair = treePair{src.Base, target.Base}
		}
		if pair.from == pair.to {
			ported[t.RootID] = PortedAnchor{Path: pos.path, Line: pos.line, State: PortExact}
			continue
		}
		pending[pair] = append(pending[pair], pos)
	}

	for pair, positions := range pending {
		paths := make([]string, 0, len(positions))
		for _, p := range positions {
			paths = append(paths, p.path)
		}
		all, err := list(pair.from, pair.to)
		var changes map[string]FileChange
		if err == nil {
			changes, err = withHunks(d, pair.from, pair.to, all, paths)
			if err != nil {
				slog.Warn("port review threads: diff", "from", pair.from, "to", pair.to, "err", err)
			}
		}
		for _, p := range positions {
			if err != nil {
				ported[p.root] = PortedAnchor{Path: p.path, State: PortUnknown}
				continue
			}
			ported[p.root] = portPosition(changes, p.path, p.line)
		}
	}
	return ported
}

// renameSource is path's path on the old side of changes: the rename's source when changes renamed a
// file to path, else path itself.
func renameSource(changes []FileChange, path string) string {
	for _, fc := range changes {
		if fc.NewPath == path && fc.OldPath != "" {
			return fc.OldPath
		}
	}
	return path
}

// withHunks narrows changes (every file the diff from→to changes) to the given old paths, keyed by
// old path, and fills in their hunks with one patch over just those files — both sides of a rename,
// so git pairs them again. A path changes doesn't list is unchanged and absent from the result.
func withHunks(d treeDiffer, from, to string, changes []FileChange, paths []string) (map[string]FileChange, error) {
	wanted := make(map[string]bool, len(paths))
	for _, p := range paths {
		wanted[p] = true
	}
	byOld := map[string]FileChange{}
	var pathspec []string
	for _, fc := range changes {
		if fc.OldPath == "" || !wanted[fc.OldPath] {
			continue
		}
		byOld[fc.OldPath] = fc
		if fc.NewPath == "" {
			continue
		}
		pathspec = append(pathspec, fc.OldPath)
		if fc.NewPath != fc.OldPath {
			pathspec = append(pathspec, fc.NewPath)
		}
	}
	if len(pathspec) == 0 {
		return byOld, nil
	}
	files, err := d.patch(from, to, pathspec)
	if err != nil {
		return nil, err
	}
	for path, fc := range byOld {
		if f, ok := files[path]; ok {
			fc.Binary, fc.Hunks = f.binary, f.hunks
			byOld[path] = fc
		}
	}
	return byOld, nil
}

// portPosition follows (path, line) on one side of a tree diff. changes holds the diff's changed files
// keyed by old path; a path it doesn't hold is unchanged. line 0 is a file-level anchor: only the path
// is followed.
func portPosition(changes map[string]FileChange, path string, line int64) PortedAnchor {
	fc, changed := changes[path]
	switch {
	case !changed:
		return PortedAnchor{Path: path, Line: line, State: PortExact}
	case fc.NewPath == "":
		return PortedAnchor{Path: path, State: PortFileDeleted}
	case line == 0:
		return PortedAnchor{Path: fc.NewPath, State: PortExact}
	case fc.Binary:
		return PortedAnchor{Path: fc.NewPath, State: PortUnknown}
	}
	mapped, exact := mapLine(fc.Hunks, line)
	state := PortExact
	if !exact {
		state = PortLineChanged
	}
	return PortedAnchor{Path: fc.NewPath, Line: mapped, State: state}
}

// mapLine follows a 1-based old-side line through a file's zero-context hunks (in file order).
// exact is false when the line itself was modified or removed: a modified line maps to the line at
// the same offset into its replacement, clamped to the replacement's last line; a removed line maps to
// the line just above the removal — a thread renders beneath its row, so it lands where the removed
// line was — or to the first line when the removal started at the top.
func mapLine(hunks []Hunk, line int64) (mapped int64, exact bool) {
	var shift int64
	for _, h := range hunks {
		if h.OldLines == 0 {
			if h.OldStart >= line {
				break
			}
			shift = h.NewStart + h.NewLines - 1 - h.OldStart
			continue
		}
		if line < h.OldStart {
			break
		}
		oldLast := h.OldStart + h.OldLines - 1
		if line > oldLast {
			newLast := h.NewStart + h.NewLines - 1
			if h.NewLines == 0 {
				newLast = h.NewStart
			}
			shift = newLast - oldLast
			continue
		}
		if h.NewLines == 0 {
			return max(h.NewStart, 1), false
		}
		return h.NewStart + min(line-h.OldStart, h.NewLines-1), false
	}
	return line + shift, true
}

// DiffSpan is the lines one rendered hunk of a file's diff shows: [FirstLeft, LastLeft] of the base
// file and [FirstRight, LastRight] of the revision's (0, 0 on a side it shows none of). Every span
// shows at least one line.
type DiffSpan struct {
	FirstLeft, LastLeft, FirstRight, LastRight int64
}

// LineAnchor is a 1-based line on one side of a diff.
type LineAnchor struct {
	Side db.Side
	Line int64
}

// ContextWindow is a run of unchanged lines to render so an anchor outside every hunk is visible: the
// revision's lines [First, Last], which are the base's lines [First-Offset, Last-Offset], shown just
// before span Before (len(spans) = after the last one).
type ContextWindow struct {
	Before      int
	First, Last int64
	Offset      int64
}

// ContextWindows plans the unchanged lines to render, margin lines either side, around each anchor
// that falls outside a file's rendered spans (its hunks, in file order) — Gerrit never hides a line a
// thread sits on. Windows are clipped to the gap between hunks and merged where they overlap or touch.
// The file's end is not known here, so the window after the last hunk is cut short by the reader.
func ContextWindows(spans []DiffSpan, anchors []LineAnchor, margin int64) []ContextWindow {
	// Gap g holds the revision's lines (fromR, toR] (toR < 0: to the end of the file), which are the
	// base's lines shifted by fromR-fromL.
	type gap struct{ fromL, fromR, toR int64 }
	gaps := make([]gap, 0, len(spans)+1)
	var posL, posR int64
	for _, s := range spans {
		startL, startR := s.FirstLeft-1, s.FirstRight-1
		switch {
		case s.FirstLeft == 0:
			startL = posL + startR - posR
		case s.FirstRight == 0:
			startR = posR + startL - posL
		}
		gaps = append(gaps, gap{fromL: posL, fromR: posR, toR: startR})
		posL, posR = startL, startR
		if s.LastLeft > 0 {
			posL = s.LastLeft
		}
		if s.LastRight > 0 {
			posR = s.LastRight
		}
	}
	gaps = append(gaps, gap{fromL: posL, fromR: posR, toR: -1})

	var windows []ContextWindow
	for _, a := range anchors {
		if a.Line < 1 || inSpan(spans, a) {
			continue
		}
		for g, gp := range gaps {
			offset := gp.fromR - gp.fromL
			r := a.Line
			if a.Side == db.SideBase {
				r += offset
			}
			if r <= gp.fromR || (gp.toR >= 0 && r > gp.toR) {
				continue
			}
			w := ContextWindow{Before: g, First: max(r-margin, gp.fromR+1), Last: r + margin, Offset: offset}
			if gp.toR >= 0 {
				w.Last = min(w.Last, gp.toR)
			}
			windows = append(windows, w)
			break
		}
	}

	sort.Slice(windows, func(i, j int) bool {
		if windows[i].Before != windows[j].Before {
			return windows[i].Before < windows[j].Before
		}
		return windows[i].First < windows[j].First
	})
	merged := windows[:0]
	for _, w := range windows {
		if n := len(merged); n > 0 && merged[n-1].Before == w.Before && w.First <= merged[n-1].Last+1 {
			merged[n-1].Last = max(merged[n-1].Last, w.Last)
			continue
		}
		merged = append(merged, w)
	}
	return merged
}

// inSpan reports whether a rendered span already shows the anchor's line.
func inSpan(spans []DiffSpan, a LineAnchor) bool {
	for _, s := range spans {
		first, last := s.FirstRight, s.LastRight
		if a.Side == db.SideBase {
			first, last = s.FirstLeft, s.LastLeft
		}
		if first > 0 && a.Line >= first && a.Line <= last {
			return true
		}
	}
	return false
}
