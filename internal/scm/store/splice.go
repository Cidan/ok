package store

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// treeStore reads trees and writes new ones. Landing and candidates write through a
// pending overlay, so a stack can splice onto a tree the same call just made.
type treeStore interface {
	readTree(id encoding.ID) (*encoding.Tree, error)
	writeTree(t *encoding.Tree) (encoding.ID, error)
}

// treeBatcher is a treeStore that reads many trees at once. A walk over one reads a level of
// the trees at a time rather than one tree to a round trip.
type treeBatcher interface {
	readTrees(ids []encoding.ID) (map[encoding.ID]*encoding.Tree, error)
}

// prefetched is a treeStore serving the trees it holds before asking its own.
type prefetched struct {
	treeStore
	trees map[encoding.ID]*encoding.Tree
}

func (p prefetched) readTree(id encoding.ID) (*encoding.Tree, error) {
	if t, ok := p.trees[id]; ok {
		return t, nil
	}
	return p.treeStore.readTree(id)
}

func (p prefetched) largeDiff(a, b encoding.ID) (*encoding.Tree, *encoding.Tree, bool, error) {
	if d, ok := p.treeStore.(largeTreeDiffer); ok {
		return d.largeDiff(a, b)
	}
	return nil, nil, false, nil
}

// largeTreeLooker is a treeStore that finds an entry of a large tree by its index (Store.lookupLarge):
// one leaf read, not the tree. large is false for a tree not large, which the caller reads whole.
type largeTreeLooker interface {
	lookupLarge(tree encoding.ID, name string) (v *encoding.TreeValue, large bool, err error)
}

func (p *pendingTrees) lookupLarge(tree encoding.ID, name string) (*encoding.TreeValue, bool, error) {
	if _, isNew := p.new[tree]; isNew {
		return nil, false, nil
	}
	return p.s.lookupLarge(p.ctx, p.repo, tree, name)
}

func (p prefetched) lookupLarge(tree encoding.ID, name string) (*encoding.TreeValue, bool, error) {
	if _, ok := p.trees[tree]; ok {
		return nil, false, nil
	}
	if l, ok := p.treeStore.(largeTreeLooker); ok {
		return l.lookupLarge(tree, name)
	}
	return nil, false, nil
}

// largeTreeDiffer is a treeStore that diffs two large trees by their chunks (docs/scm.md, "Large
// trees"): each one's entries in the leaves the other lacks. What differs between the two is among
// those, and nothing else — an entry is in one leaf of a tree, and a leaf both hold is the same
// entries — so a diff of them is the trees' diff, reading only the leaves around what changed.
type largeTreeDiffer interface {
	// largeDiff returns those entries of a and of b as trees, or ok false unless both are large.
	largeDiff(a, b encoding.ID) (ta, tb *encoding.Tree, ok bool, err error)
}

// largeTreeFinder is a treeBatcher that tells which trees are large, which a diff's prefetch leaves
// to largeTreeDiffer rather than reading whole.
type largeTreeFinder interface {
	// largeOf are the large trees among ids.
	largeOf(ids []encoding.ID) (map[encoding.ID]bool, error)
}

func (p *pendingTrees) largeOf(ids []encoding.ID) (map[encoding.ID]bool, error) {
	var stored []encoding.ID
	for _, id := range ids {
		if _, isNew := p.new[id]; !isNew && id != encoding.EmptyTreeID {
			stored = append(stored, id)
		}
	}
	large, err := p.s.LargeTrees(p.ctx, p.repo, stored)
	if err != nil {
		return nil, err
	}
	out := make(map[encoding.ID]bool, len(large))
	for id := range large {
		out[id] = true
	}
	return out, nil
}

func (p *pendingTrees) largeDiff(a, b encoding.ID) (*encoding.Tree, *encoding.Tree, bool, error) {
	for _, id := range []encoding.ID{a, b} {
		if _, isNew := p.new[id]; isNew || id == encoding.EmptyTreeID {
			return nil, nil, false, nil
		}
	}
	large, err := p.s.LargeTrees(p.ctx, p.repo, []encoding.ID{a, b})
	if err != nil {
		return nil, nil, false, err
	}
	la, okA := large[a]
	lb, okB := large[b]
	if !okA || !okB {
		return nil, nil, false, nil
	}
	in := func(e contentindex.Entry) map[encoding.ID]bool {
		out := make(map[encoding.ID]bool, len(e.Chunks))
		for _, c := range e.Chunks[1:] {
			out[c.ID] = true
		}
		return out
	}
	inA, inB := in(la), in(lb)
	var want []encoding.ID
	for _, side := range []struct {
		e     contentindex.Entry
		other map[encoding.ID]bool
	}{{la, inB}, {lb, inA}} {
		for _, c := range side.e.Chunks[1:] {
			if !side.other[c.ID] {
				want = append(want, c.ID)
			}
		}
	}
	data, err := p.s.chunkBytes(p.ctx, p.repo, want)
	if err != nil {
		return nil, nil, false, err
	}
	leaves := func(e contentindex.Entry, other map[encoding.ID]bool) (*encoding.Tree, error) {
		t := &encoding.Tree{}
		for _, c := range e.Chunks[1:] {
			if other[c.ID] {
				continue
			}
			b, ok := data[c.ID]
			if !ok {
				return nil, fmt.Errorf("a large tree's chunk %s: %w", c.ID, ErrNotFound)
			}
			got, err := encoding.TreeLeafEntries(b)
			if err != nil {
				return nil, err
			}
			t.Entries = append(t.Entries, got...)
		}
		return t, nil
	}
	ta, err := leaves(la, inB)
	if err != nil {
		return nil, nil, false, err
	}
	tb, err := leaves(lb, inA)
	if err != nil {
		return nil, nil, false, err
	}
	return ta, tb, true, nil
}

// pendingTrees is a treeStore over a repository's stored trees, holding the trees it
// writes in memory until insert stores them.
type pendingTrees struct {
	ctx   context.Context
	s     *Store
	repo  int64
	new   map[encoding.ID]*encoding.Tree
	order []encoding.ID
	body  map[encoding.ID][]byte
	// read are the stored trees it has read, for the request it serves: trees never change.
	read map[encoding.ID]*encoding.Tree
}

func (s *Store) pendingTrees(ctx context.Context, repo int64) *pendingTrees {
	return &pendingTrees{ctx: ctx, s: s, repo: repo, new: map[encoding.ID]*encoding.Tree{}, body: map[encoding.ID][]byte{},
		read: map[encoding.ID]*encoding.Tree{}}
}

func (p *pendingTrees) readTree(id encoding.ID) (*encoding.Tree, error) {
	if id == encoding.EmptyTreeID {
		return &encoding.Tree{}, nil
	}
	if t, ok := p.new[id]; ok {
		return t, nil
	}
	if t, ok := p.read[id]; ok {
		return t, nil
	}
	t, err := p.s.readTree(p.ctx, p.repo, id)
	if err == nil {
		p.read[id] = t
	}
	return t, err
}

func (p *pendingTrees) readTrees(ids []encoding.ID) (map[encoding.ID]*encoding.Tree, error) {
	out := make(map[encoding.ID]*encoding.Tree, len(ids))
	var stored []encoding.ID
	for _, id := range ids {
		if t, ok := p.new[id]; ok {
			out[id] = t
			continue
		}
		if t, ok := p.read[id]; ok {
			out[id] = t
			continue
		}
		if id == encoding.EmptyTreeID {
			out[id] = &encoding.Tree{}
			continue
		}
		stored = append(stored, id)
	}
	read, err := p.s.readTrees(p.ctx, p.repo, stored)
	if err != nil {
		return nil, err
	}
	for id, t := range read {
		out[id], p.read[id] = t, t
	}
	return out, nil
}

func (p *pendingTrees) writeTree(t *encoding.Tree) (encoding.ID, error) {
	if len(t.Entries) == 0 {
		return encoding.EmptyTreeID, nil
	}
	canonical, err := encoding.Encode(t)
	if err != nil {
		return encoding.ID{}, err
	}
	id := encoding.Hash(canonical)
	if _, ok := p.new[id]; !ok {
		p.new[id] = t
		p.body[id] = canonical[1:]
		p.order = append(p.order, id)
	}
	return id, nil
}

// insert stores the pending trees, keeping only what is reachable from roots (a landing
// that tried several splices wrote trees it no longer needs), and of them only those the
// repository's content index does not hold: a tree a publish stored is kept there, where the
// landing's pin or the candidate's refresh finds it.
func (p *pendingTrees) insert(ctx context.Context, tx *txn, roots []encoding.ID) error {
	idCol := p.kept(roots)
	if len(idCol) == 0 {
		return nil
	}
	sortIDs(idCol)
	held, err := p.s.indexHolds(ctx, tx, p.repo, encoding.KindTree, idCol)
	if err != nil {
		return err
	}
	objects := make([]backend.Object, 0, len(idCol))
	for _, id := range idCol {
		if !held[id] {
			objects = append(objects, backend.Object{ID: id, Kind: encoding.KindTree, Body: p.body[id]})
		}
	}
	if len(objects) == 0 {
		return nil
	}
	_, err = tx.InsertObjects(ctx, p.repo, objects)
	return err
}

// sameValue reports whether two tree values are the same path content: kind, ID,
// executable bit, and copy ID. nil is absent.
func sameValue(a, b *encoding.TreeValue) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Kind != b.Kind || a.ID != b.ID || a.Executable != b.Executable {
		return false
	}
	if a.CopyID == nil || b.CopyID == nil {
		return a.CopyID == nil && b.CopyID == nil
	}
	return *a.CopyID == *b.CopyID
}

// split separates an entry into its leaf (a file or symlink at the path itself) and its
// subtree (the empty tree when it is not a directory).
func split(v *encoding.TreeValue) (*encoding.TreeValue, encoding.ID) {
	if v == nil {
		return nil, encoding.EmptyTreeID
	}
	if v.Kind == encoding.TreeValueTree {
		return nil, v.ID
	}
	return v, encoding.EmptyTreeID
}

func joinPath(dir, name string) string {
	if dir == "" {
		return name
	}
	return dir + "/" + name
}

// entries indexes a tree's entries by name.
func entries(t *encoding.Tree) map[string]*encoding.TreeValue {
	out := make(map[string]*encoding.TreeValue, len(t.Entries))
	for i := range t.Entries {
		out[t.Entries[i].Name] = &t.Entries[i].Value
	}
	return out
}

// names is the sorted union of the trees' entry names.
func names(ts ...*encoding.Tree) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range ts {
		for _, e := range t.Entries {
			if !seen[e.Name] {
				seen[e.Name] = true
				out = append(out, e.Name)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// leafChange is one leaf path (a file or symlink) whose value differs between two trees.
// A nil side is absent.
type leafChange struct {
	Path          string
	Before, After *encoding.TreeValue
}

// diffTrees reports every leaf path whose value differs between trees a and b, in path
// order, descending only into subtrees whose IDs differ. A path that changes between a
// leaf and a directory reports the leaf's removal or addition and the directory's leaves.
// Over a store that reads many trees at once, every tree the diff reads is read first, a
// level at a time.
func diffTrees(ts treeStore, a, b encoding.ID, dir string, emit func(leafChange) error) error {
	if bt, ok := ts.(treeBatcher); ok && a != b {
		trees, err := prefetchDiff(bt, a, b)
		if err != nil {
			return err
		}
		ts = prefetched{treeStore: ts, trees: trees}
	}
	return walkDiff(ts, a, b, dir, emit)
}

// prefetchDiff reads every tree a diff from a to b reads: the two roots, then both sides of
// each directory that differs, a level of the trees to one read.
func prefetchDiff(bt treeBatcher, a, b encoding.ID) (map[encoding.ID]*encoding.Tree, error) {
	return prefetchDiffs(bt, [][2]encoding.ID{{a, b}})
}

// prefetchDiffs is prefetchDiff for several diffs at once, a level of all of them to one read.
func prefetchDiffs(bt treeBatcher, pairs [][2]encoding.ID) (map[encoding.ID]*encoding.Tree, error) {
	trees := map[encoding.ID]*encoding.Tree{encoding.EmptyTreeID: {}}
	for len(pairs) > 0 {
		var want []encoding.ID
		asked := map[encoding.ID]bool{}
		for _, p := range pairs {
			for _, id := range p {
				if _, ok := trees[id]; !ok && !asked[id] {
					asked[id] = true
					want = append(want, id)
				}
			}
		}
		// A pair of large trees is the diff's to read by their chunks (largeTreeDiffer), not whole.
		var large map[encoding.ID]bool
		if d, ok := bt.(largeTreeFinder); ok {
			var err error
			if large, err = d.largeOf(want); err != nil {
				return nil, err
			}
		}
		if len(large) > 0 {
			pairs = slices.DeleteFunc(pairs, func(p [2]encoding.ID) bool { return large[p[0]] && large[p[1]] })
			want = slices.DeleteFunc(want, func(id encoding.ID) bool {
				return large[id] && !slices.ContainsFunc(pairs, func(p [2]encoding.ID) bool { return p[0] == id || p[1] == id })
			})
		}
		read, err := bt.readTrees(want)
		if err != nil {
			return nil, err
		}
		for id, t := range read {
			trees[id] = t
		}
		var next [][2]encoding.ID
		for _, p := range pairs {
			ta, tb := trees[p[0]], trees[p[1]]
			ea, eb := entries(ta), entries(tb)
			for _, name := range names(ta, tb) {
				va, vb := ea[name], eb[name]
				if sameValue(va, vb) {
					continue
				}
				_, sa := split(va)
				_, sb := split(vb)
				if sa != sb {
					next = append(next, [2]encoding.ID{sa, sb})
				}
			}
		}
		pairs = next
	}
	return trees, nil
}

// walkDiff is diffTrees' walk, depth first.
func walkDiff(ts treeStore, a, b encoding.ID, dir string, emit func(leafChange) error) error {
	if a == b {
		return nil
	}
	var ta, tb *encoding.Tree
	large := false
	if d, ok := ts.(largeTreeDiffer); ok {
		var err error
		if ta, tb, large, err = d.largeDiff(a, b); err != nil {
			return err
		}
	}
	if !large {
		var err error
		if ta, err = ts.readTree(a); err != nil {
			return err
		}
		if tb, err = ts.readTree(b); err != nil {
			return err
		}
	}
	ea, eb := entries(ta), entries(tb)
	for _, name := range names(ta, tb) {
		va, vb := ea[name], eb[name]
		if sameValue(va, vb) {
			continue
		}
		p := joinPath(dir, name)
		la, sa := split(va)
		lb, sb := split(vb)
		// A leaf sorts before the directory's contents at the same name.
		if !sameValue(la, lb) {
			if err := emit(leafChange{Path: p, Before: la, After: lb}); err != nil {
				return err
			}
		}
		if err := walkDiff(ts, sa, sb, p, emit); err != nil {
			return err
		}
	}
	return nil
}

// prefetchSplices reads, a level of the trees to one read, every tree the splices of triples read
// — each a CL's base P, its commit C, and main's head H (splicer.splice): both sides and main's
// where the CL changed something and main has something else, as splice descends. A landing of a
// batch prefetches its CLs' splices onto main's head at once; one landing on another CL's result
// reads, besides, only the trees that result made, which it holds.
func (p *pendingTrees) prefetchSplices(triples [][3]encoding.ID) error {
	for len(triples) > 0 {
		var want []encoding.ID
		asked := map[encoding.ID]bool{}
		for _, t := range triples {
			for _, id := range t {
				if !asked[id] {
					asked[id] = true
					want = append(want, id)
				}
			}
		}
		trees, err := p.readTrees(want)
		if err != nil {
			return err
		}
		var next [][3]encoding.ID
		for _, t := range triples {
			tp, tc, th := trees[t[0]], trees[t[1]], trees[t[2]]
			ep, ec, eh := entries(tp), entries(tc), entries(th)
			for _, name := range names(tp, tc, th) {
				vp, vc, vh := ep[name], ec[name], eh[name]
				if sameValue(vp, vc) || sameValue(vh, vp) || sameValue(vh, vc) {
					continue
				}
				_, sp := split(vp)
				_, sc := split(vc)
				_, sh := split(vh)
				if sp != sc && sh != sp && sh != sc {
					next = append(next, [3]encoding.ID{sp, sc, sh})
				}
			}
		}
		triples = next
	}
	return nil
}

// splicer applies path-level submit (docs/scm.md, "Submit: path-level landing"):
// the leaf paths a CL changed from its base P to its commit C, applied to main's head H.
type splicer struct {
	ts treeStore
	// conflicts are the paths that refuse the splice, in path order.
	conflicts []string
}

// splice returns the landed tree R: H with every leaf path the CL changed set to C's
// value (a deletion removes it). A changed path that main also changed behind the CL —
// to neither P's nor C's value — is a conflict, and so is a result in which one path is
// both a leaf and a directory. It walks P, C, and H in parallel, descending only where
// they differ, so its cost follows the change and not the repository.
func (sp *splicer) splice(p, c, h encoding.ID, dir string) (encoding.ID, error) {
	switch {
	case p == c:
		return h, nil // the CL changed nothing here
	case h == p, h == c:
		return c, nil // main has nothing here the CL didn't start from or already carries
	}
	tp, err := sp.ts.readTree(p)
	if err != nil {
		return encoding.ID{}, err
	}
	tc, err := sp.ts.readTree(c)
	if err != nil {
		return encoding.ID{}, err
	}
	th, err := sp.ts.readTree(h)
	if err != nil {
		return encoding.ID{}, err
	}
	ep, ec, eh := entries(tp), entries(tc), entries(th)
	out := &encoding.Tree{}
	for _, name := range names(tp, tc, th) {
		vp, vc, vh := ep[name], ec[name], eh[name]
		var r *encoding.TreeValue
		switch {
		case sameValue(vp, vc):
			r = vh
		case sameValue(vh, vp), sameValue(vh, vc):
			r = vc
		default:
			path := joinPath(dir, name)
			lp, sp_ := split(vp)
			lc, sc := split(vc)
			lh, sh := split(vh)
			leaf := lh
			if !sameValue(lp, lc) {
				if !sameValue(lh, lp) && !sameValue(lh, lc) {
					sp.conflicts = append(sp.conflicts, path)
				}
				leaf = lc
			}
			sub, err := sp.splice(sp_, sc, sh, path)
			if err != nil {
				return encoding.ID{}, err
			}
			switch {
			case leaf != nil && sub != encoding.EmptyTreeID:
				sp.conflicts = append(sp.conflicts, path)
				r = leaf
			case leaf != nil:
				r = leaf
			case sub != encoding.EmptyTreeID:
				r = &encoding.TreeValue{Kind: encoding.TreeValueTree, ID: sub}
			}
		}
		if r != nil {
			out.Entries = append(out.Entries, encoding.TreeEntry{Name: name, Value: *r})
		}
	}
	return sp.ts.writeTree(out)
}

// conflictPaths are the splice's conflicts, sorted and without repeats.
func (sp *splicer) conflictPaths() []string {
	out := append([]string(nil), sp.conflicts...)
	sort.Strings(out)
	n := 0
	for i, p := range out {
		if i == 0 || p != out[n-1] {
			out[n] = p
			n++
		}
	}
	return out[:n]
}

// lookupPath resolves a slash-separated path in a tree to its entry, or nil.
func lookupPath(ts treeStore, root encoding.ID, path string) (*encoding.TreeValue, error) {
	dir := root
	parts := strings.Split(path, "/")
	for i, part := range parts {
		var v encoding.TreeValue
		var found, large bool
		if l, ok := ts.(largeTreeLooker); ok {
			var got *encoding.TreeValue
			var err error
			if got, large, err = l.lookupLarge(dir, part); err != nil {
				return nil, err
			}
			if got != nil {
				v, found = *got, true
			}
		}
		if !large {
			t, err := ts.readTree(dir)
			if err != nil {
				return nil, err
			}
			j := sort.Search(len(t.Entries), func(k int) bool { return t.Entries[k].Name >= part })
			if found = j < len(t.Entries) && t.Entries[j].Name == part; found {
				v = t.Entries[j].Value
			}
		}
		if !found {
			return nil, nil
		}
		if i == len(parts)-1 {
			return &v, nil
		}
		if v.Kind != encoding.TreeValueTree {
			return nil, nil
		}
		dir = v.ID
	}
	return nil, nil
}
