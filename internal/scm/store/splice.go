package store

import (
	"context"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/encoding"
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

// pendingTrees is a treeStore over a repository's stored trees, holding the trees it
// writes in memory until insert stores them.
type pendingTrees struct {
	ctx   context.Context
	s     *Store
	repo  int64
	new   map[encoding.ID]*encoding.Tree
	order []encoding.ID
	body  map[encoding.ID][]byte
}

func (s *Store) pendingTrees(ctx context.Context, repo int64) *pendingTrees {
	return &pendingTrees{ctx: ctx, s: s, repo: repo, new: map[encoding.ID]*encoding.Tree{}, body: map[encoding.ID][]byte{}}
}

func (p *pendingTrees) readTree(id encoding.ID) (*encoding.Tree, error) {
	if id == encoding.EmptyTreeID {
		return &encoding.Tree{}, nil
	}
	if t, ok := p.new[id]; ok {
		return t, nil
	}
	return p.s.readTree(p.ctx, p.repo, id)
}

func (p *pendingTrees) readTrees(ids []encoding.ID) (map[encoding.ID]*encoding.Tree, error) {
	out := make(map[encoding.ID]*encoding.Tree, len(ids))
	var stored []encoding.ID
	for _, id := range ids {
		if t, ok := p.new[id]; ok {
			out[id] = t
			continue
		}
		stored = append(stored, id)
	}
	read, err := p.s.readTrees(p.ctx, p.repo, stored)
	if err != nil {
		return nil, err
	}
	for id, t := range read {
		out[id] = t
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
// that tried several splices wrote trees it no longer needs).
func (p *pendingTrees) insert(ctx context.Context, tx pgx.Tx, roots []encoding.ID) error {
	keep := map[encoding.ID]bool{}
	var mark func(id encoding.ID)
	mark = func(id encoding.ID) {
		t, ok := p.new[id]
		if !ok || keep[id] {
			return
		}
		keep[id] = true
		for _, e := range t.Entries {
			if e.Value.Kind == encoding.TreeValueTree {
				mark(e.Value.ID)
			}
		}
	}
	for _, r := range roots {
		mark(r)
	}
	var idCol []encoding.ID
	for _, id := range p.order {
		if keep[id] {
			idCol = append(idCol, id)
		}
	}
	if len(idCol) == 0 {
		return nil
	}
	sortIDs(idCol)
	kinds := make([]int16, len(idCol))
	bodies := make([][]byte, len(idCol))
	for i, id := range idCol {
		kinds[i], bodies[i] = int16(encoding.KindTree), p.body[id]
	}
	_, err := tx.Exec(ctx, `INSERT INTO object (repository_id, id, kind, body)
		SELECT $1, u.id, u.kind, u.body FROM unnest($2::bytea[], $3::smallint[], $4::bytea[]) AS u(id, kind, body)
		ORDER BY u.id ON CONFLICT DO NOTHING`, p.repo, ids(idCol), kinds, bodies)
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
	trees := map[encoding.ID]*encoding.Tree{encoding.EmptyTreeID: {}}
	pairs := [][2]encoding.ID{{a, b}}
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
	ta, err := ts.readTree(a)
	if err != nil {
		return err
	}
	tb, err := ts.readTree(b)
	if err != nil {
		return err
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
		t, err := ts.readTree(dir)
		if err != nil {
			return nil, err
		}
		j := sort.Search(len(t.Entries), func(k int) bool { return t.Entries[k].Name >= part })
		if j == len(t.Entries) || t.Entries[j].Name != part {
			return nil, nil
		}
		v := t.Entries[j].Value
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
