// Package copies finds renames and copies between two trees, the way jj's git backend
// does for Backend::get_copy_records (docs/scm.md, "The client"): it runs gix's
// rewrite tracker with copies drawn from modified files, a 50% similarity threshold for
// both renames and copies, and a limit of 1,000 source-destination pairs for similarity
// checks. This package reproduces that tracker's passes and tie-breaks over the SCM's
// trees, so the records an ok repository yields match what jj would compute itself.
package copies

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"

	"github.com/Cidan/ok/internal/scm/diff"
	"github.com/Cidan/ok/internal/scm/encoding"
)

const (
	// Percentage is the similarity a rename or copy needs (jj: 0.5 for both).
	Percentage float32 = 0.5
	// Limit bounds similarity checking: a pass whose sources × destinations exceed it
	// matches by identity only (jj: 1,000).
	Limit = 1000
)

// ChangeKind orders like gix's: deletions, then modifications, then additions.
type ChangeKind int

const (
	Deletion ChangeKind = iota
	Modification
	Addition
)

// Change is one changed leaf between the trees.
type Change struct {
	Kind ChangeKind
	Path string
	// ID is the leaf's file or symlink: its new value, or its old one for a deletion.
	ID      encoding.ID
	Symlink bool
}

// Match pairs a destination (an addition) with the source it was renamed or copied from.
type Match struct {
	Dest   Change
	Source Change
	// Copy is set for a copy from a modified file, rather than a rename from a deleted one.
	Copy bool
}

// Trees reads a tree by ID. The empty tree is handled here and never requested.
type Trees func(ctx context.Context, id encoding.ID) (*encoding.Tree, error)

// Files reads a file's contents by ID.
type Files func(ctx context.Context, id encoding.ID) ([]byte, error)

// leaf is a file or symlink as git sees it: copy IDs have no git counterpart, so they do
// not make two leaves differ.
type leaf struct {
	id         encoding.ID
	executable bool
	symlink    bool
}

func leafOf(v encoding.TreeValue) (leaf, bool) {
	switch v.Kind {
	case encoding.TreeValueFile:
		return leaf{id: v.ID, executable: v.Executable}, true
	case encoding.TreeValueSymlink:
		return leaf{id: v.ID, symlink: true}, true
	}
	return leaf{}, false
}

// Changes lists the leaf changes from tree before to tree after, descending only into
// subtrees whose IDs differ. A leaf replaced by a subtree (or the reverse) is a deletion
// and additions (or the reverse), as git's tree diff reports it.
func Changes(ctx context.Context, trees Trees, before, after encoding.ID) ([]Change, error) {
	var out []Change
	err := walk(ctx, trees, "", &before, &after, &out)
	return out, err
}

func readTree(ctx context.Context, trees Trees, id *encoding.ID) (*encoding.Tree, error) {
	if id == nil || *id == encoding.EmptyTreeID {
		return &encoding.Tree{}, nil
	}
	t, err := trees(ctx, *id)
	if err != nil {
		return nil, fmt.Errorf("tree %s: %w", *id, err)
	}
	return t, nil
}

func walk(ctx context.Context, trees Trees, dir string, before, after *encoding.ID, out *[]Change) error {
	a, err := readTree(ctx, trees, before)
	if err != nil {
		return err
	}
	b, err := readTree(ctx, trees, after)
	if err != nil {
		return err
	}
	i, j := 0, 0
	for i < len(a.Entries) || j < len(b.Entries) {
		var ea, eb *encoding.TreeEntry
		switch {
		case j == len(b.Entries) || (i < len(a.Entries) && a.Entries[i].Name < b.Entries[j].Name):
			ea, i = &a.Entries[i], i+1
		case i == len(a.Entries) || b.Entries[j].Name < a.Entries[i].Name:
			eb, j = &b.Entries[j], j+1
		default:
			ea, eb, i, j = &a.Entries[i], &b.Entries[j], i+1, j+1
		}
		var name string
		if ea != nil {
			name = ea.Name
		} else {
			name = eb.Name
		}
		p := name
		if dir != "" {
			p = dir + "/" + name
		}
		if err := change(ctx, trees, p, ea, eb, out); err != nil {
			return err
		}
	}
	return nil
}

func change(ctx context.Context, trees Trees, p string, ea, eb *encoding.TreeEntry, out *[]Change) error {
	aTree := ea != nil && ea.Value.Kind == encoding.TreeValueTree
	bTree := eb != nil && eb.Value.Kind == encoding.TreeValueTree
	switch {
	case aTree && bTree:
		if ea.Value.ID != eb.Value.ID {
			return walk(ctx, trees, p, &ea.Value.ID, &eb.Value.ID, out)
		}
		return nil
	case aTree:
		if err := walk(ctx, trees, p, &ea.Value.ID, nil, out); err != nil {
			return err
		}
		if eb != nil {
			l, _ := leafOf(eb.Value)
			*out = append(*out, Change{Kind: Addition, Path: p, ID: l.id, Symlink: l.symlink})
		}
		return nil
	case bTree:
		if ea != nil {
			l, _ := leafOf(ea.Value)
			*out = append(*out, Change{Kind: Deletion, Path: p, ID: l.id, Symlink: l.symlink})
		}
		return walk(ctx, trees, p, nil, &eb.Value.ID, out)
	}
	la, okA := leaf{}, false
	if ea != nil {
		la, okA = leafOf(ea.Value)
	}
	lb, okB := leaf{}, false
	if eb != nil {
		lb, okB = leafOf(eb.Value)
	}
	switch {
	case okA && okB:
		if la != lb {
			*out = append(*out, Change{Kind: Modification, Path: p, ID: lb.id, Symlink: lb.symlink})
		}
	case okA:
		*out = append(*out, Change{Kind: Deletion, Path: p, ID: la.id, Symlink: la.symlink})
	case okB:
		*out = append(*out, Change{Kind: Addition, Path: p, ID: lb.id, Symlink: lb.symlink})
	}
	return nil
}

// The empty file and symlink, which are never destinations (gix's track_empty: false).
var (
	emptyFile    = encoding.Hash([]byte{byte(encoding.KindFile)})
	emptySymlink = encoding.Hash([]byte{byte(encoding.KindSymlink)})
)

type sourceKind int

const (
	rename sourceKind = iota
	copying
)

type item struct {
	Change
	emitted bool
}

type tracker struct {
	ctx        context.Context
	items      []item
	files      Files
	cache      map[encoding.ID][]byte
	matches    []Match
	percentage float32
}

// Track pairs additions with the deletions they were renamed from, then with the
// modified files they were copied from, returning the matches in the order gix emits
// them.
func Track(ctx context.Context, changes []Change, files Files) ([]Match, error) {
	return TrackAt(ctx, changes, files, Percentage)
}

// TrackAt is Track with the similarity a rename or copy needs set to percentage (0 to 1)
// rather than jj's.
func TrackAt(ctx context.Context, changes []Change, files Files, percentage float32) ([]Match, error) {
	t := &tracker{ctx: ctx, files: files, cache: map[encoding.ID][]byte{}, percentage: percentage}
	var deletions, additions, modifications int
	hasWork := false
	for _, c := range changes {
		t.items = append(t.items, item{Change: c})
		switch c.Kind {
		case Deletion:
			deletions++
		case Modification:
			modifications++
		case Addition:
			additions++
		}
		if (deletions != 0 && additions != 0) || modifications+additions > 1 {
			hasWork = true
		}
	}
	if !hasWork {
		return nil, nil
	}
	sort.SliceStable(t.items, func(i, j int) bool {
		a, b := t.items[i], t.items[j]
		if c := bytes.Compare(a.ID[:], b.ID[:]); c != 0 {
			return c < 0
		}
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		return a.Kind < b.Kind
	})
	for _, kind := range []sourceKind{rename, copying} {
		if err := t.matchPairsOfKind(kind); err != nil {
			return nil, err
		}
	}
	return t.matches, nil
}

func (t *tracker) matchPairsOfKind(kind sourceKind) error {
	stop, err := t.matchPairs(nil, kind)
	if err != nil || stop {
		return err
	}
	src, dst := t.estimate(kind)
	if src*dst > Limit {
		return nil
	}
	pct := t.percentage
	_, err = t.matchPairs(&pct, kind)
	return err
}

// estimate counts the viable sources and destinations for a similarity pass.
func (t *tracker) estimate(kind sourceKind) (src, dst int) {
	for _, it := range t.items {
		if kind == rename && it.emitted {
			continue
		}
		switch it.Kind {
		case Addition:
			if kind == rename || !it.emitted {
				dst++
			}
		case Deletion:
			if kind == rename {
				src++
			}
		case Modification:
			if kind == copying {
				src++
			}
		}
	}
	return src, dst
}

// matchPairs matches each unemitted addition, by identity when pct is nil and by
// similarity otherwise. stop reports that the check budget ran out.
func (t *tracker) matchPairs(pct *float32, kind sourceKind) (stop bool, err error) {
	checks, maxChecks := 0, 0
	if len(t.items) >= 100_000 {
		maxChecks = Limit * Limit
	}
	for d := range t.items {
		dest := &t.items[d]
		if dest.emitted || dest.Kind != Addition || dest.ID == emptyFile || dest.ID == emptySymlink {
			continue
		}
		s, err := t.findMatch(d, pct, kind, &checks)
		if err != nil {
			return false, err
		}
		if maxChecks != 0 && checks > maxChecks {
			return true, nil
		}
		if s < 0 {
			continue
		}
		t.matches = append(t.matches, Match{Dest: dest.Change, Source: t.items[s].Change, Copy: kind == copying})
		dest.emitted = true
		t.items[s].emitted = true
	}
	return false, nil
}

func (t *tracker) isSource(src *item, kind sourceKind, dest *item) bool {
	if src.Symlink != dest.Symlink {
		return false
	}
	if kind == rename {
		return !src.emitted && src.Kind == Deletion
	}
	return src.Kind == Modification
}

func (t *tracker) findMatch(d int, pct *float32, kind sourceKind, checks *int) (int, error) {
	dest := &t.items[d]
	name := path.Base(dest.Path)
	if pct == nil || dest.Symlink {
		lo := sort.Search(len(t.items), func(i int) bool { return bytes.Compare(t.items[i].ID[:], dest.ID[:]) >= 0 })
		fallback := -1
		for s := lo; s < len(t.items) && t.items[s].ID == dest.ID; s++ {
			*checks++
			if s == d || !t.isSource(&t.items[s], kind, dest) {
				continue
			}
			if path.Base(t.items[s].Path) == name {
				return s, nil
			}
			if fallback < 0 {
				fallback = s
			}
		}
		return fallback, nil
	}
	best, bestSimilarity, bestSameName := -1, float32(0), false
	for s := range t.items {
		src := &t.items[s]
		if s == d || !t.isSource(src, kind, dest) {
			continue
		}
		*checks++
		old, err := t.read(src.ID)
		if err != nil {
			return 0, err
		}
		cur, err := t.read(dest.ID)
		if err != nil {
			return 0, err
		}
		if binary(old) || binary(cur) {
			continue
		}
		similarity := Similarity(old, cur)
		if !(similarity >= *pct) {
			continue
		}
		sameName := path.Base(src.Path) == name
		if best < 0 || similarity > bestSimilarity || (similarity == bestSimilarity && sameName && !bestSameName) {
			best, bestSimilarity, bestSameName = s, similarity, sameName
		}
	}
	return best, nil
}

func (t *tracker) read(id encoding.ID) ([]byte, error) {
	if b, ok := t.cache[id]; ok {
		return b, nil
	}
	b, err := t.files(t.ctx, id)
	if err != nil {
		return nil, fmt.Errorf("file %s: %w", id, err)
	}
	t.cache[id] = b
	return b, nil
}

// Binary reports whether contents are binary, by gix's test: a NUL byte in the first
// 8,000 bytes.
func Binary(b []byte) bool { return binary(b) }

// binary is gix's test: a NUL byte in the first 8,000 bytes.
func binary(b []byte) bool {
	return bytes.IndexByte(b[:min(len(b), 8000)], 0) >= 0
}

// Similarity is gix's score for two versions of a file: the bytes of the old version the
// line diff keeps, over the larger version's size.
func Similarity(old, cur []byte) float32 {
	before := diff.Lines(old)
	d := diff.Compute(before, diff.Lines(cur))
	removed := 0
	for i, r := range d.Removed {
		if r {
			removed += len(before[i])
		}
	}
	return float32(len(old)-removed) / float32(max(len(old), len(cur)))
}
