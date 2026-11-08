package store

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/Cidan/ok/internal/membuf"
	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// Large trees a landing, a candidate, or a rollback writes (docs/scm.md, "Large trees"): a splice of
// a directory of thousands of entries is held as its body's chunks, as a publish holds one, not as a
// row — and most of its chunks are the versions' it was spliced from, which the repository holds
// pinned, so it stores only those around what changed. The chunks it lacks go in a pack sent before
// its transaction (stageLargeTrees); the transaction records them, and the trees, in the content
// index (recordLargeTrees) before it stores the rest as rows (pendingTrees.insert), which then finds
// the large ones held.

// stagedTrees are large trees cut into chunks, and the pack of those of their chunks the repository
// did not hold pinned, sent to the bucket.
type stagedTrees struct {
	net    backend.Network
	trees  map[encoding.ID]contentindex.Entry
	bodies map[encoding.ID][]byte
	// pack is the chunks' pack, sent; zero when every chunk was held.
	pack encoding.ID
	size int64
	locs map[encoding.ID]pack.Location
}

// kept are the new trees reachable from roots, in the order they were written: what insert stores.
func (p *pendingTrees) kept(roots []encoding.ID) []encoding.ID {
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
	var out []encoding.ID
	for _, id := range p.order {
		if keep[id] {
			out = append(out, id)
		}
	}
	return out
}

// stageLargeTrees cuts the new trees reachable from roots that are large into their chunks and
// sends a pack of the chunks the repository does not hold pinned — a pinned chunk is never swept —
// to the bucket. None large, it stages nothing. A pack the transaction never records is an orphan,
// which garbage collection deletes.
func (s *Store) stageLargeTrees(ctx context.Context, repo int64, ts *pendingTrees, roots []encoding.ID) (*stagedTrees, error) {
	st := &stagedTrees{trees: map[encoding.ID]contentindex.Entry{}, bodies: map[encoding.ID][]byte{}, locs: map[encoding.ID]pack.Location{}}
	chunks := map[encoding.ID][]byte{}
	for _, id := range ts.kept(roots) {
		body := ts.body[id]
		if len(body) <= encoding.TreeChunkThreshold {
			continue
		}
		cut, err := encoding.ChunkTree(body)
		if err != nil {
			continue
		}
		e := contentindex.Entry{Kind: encoding.KindTree, Body: true, Chunked: true, Size: uint64(len(body))}
		for _, c := range cut {
			cid := pack.ChunkID(c)
			e.Chunks = append(e.Chunks, contentindex.FileChunk{ID: cid, Length: uint32(len(c))})
			chunks[cid] = c
		}
		st.trees[id] = e
		st.bodies[id] = body
	}
	if len(st.trees) == 0 {
		return nil, nil
	}
	net, err := s.network(ctx, repo)
	if err != nil {
		return nil, err
	}
	st.net = net
	ids := make([]encoding.ID, 0, len(chunks))
	for id := range chunks {
		ids = append(ids, id)
	}
	sortIDs(ids)
	_, held, err := s.contentEntries(ctx, repoScope(repo), encoding.KindChunk, ids)
	if err != nil {
		return nil, err
	}
	var buf membuf.Buffer
	pw := pack.NewWriter(&buf)
	for _, id := range ids {
		if e, ok := held[id]; ok && e.Pinned {
			continue
		}
		loc, err := pw.Add(id, chunks[id])
		if err != nil {
			return nil, err
		}
		st.locs[id] = loc
	}
	if len(st.locs) == 0 {
		return st, nil
	}
	if st.pack, st.size, err = pw.Close(); err != nil {
		return nil, err
	}
	if err := s.blobs.Put(ctx, packKey(net, st.pack), buf.Reader(), st.size); err != nil {
		return nil, err
	}
	return st, nil
}

// recordLargeTrees records, in tx, staged large trees: their pack, each of its chunks the repository
// lacks in the repository's content index and each the network lacks a location of in the network's,
// and each tree the repository lacks as its chunks. The walk that pins what a landing reaches goes
// on through them, their bodies read from tx, into their entries and chunks.
func (s *Store) recordLargeTrees(ctx context.Context, tx *txn, repo int64, st *stagedTrees) error {
	if st == nil {
		return nil
	}
	if tx.largeTrees == nil {
		tx.largeTrees = map[encoding.ID][]byte{}
	}
	for id, b := range st.bodies {
		tx.largeTrees[id] = b
	}
	lg, err := s.txIndex(ctx, tx, repoScope(repo))
	if err != nil {
		return err
	}
	netScope := backend.NetworkScope(st.net)
	if len(st.locs) > 0 {
		net, err := s.txIndex(ctx, tx, netScope)
		if err != nil {
			return err
		}
		ids := make([]encoding.ID, 0, len(st.locs))
		for id := range st.locs {
			ids = append(ids, id)
		}
		sortIDs(ids)
		// A location the repository relies on without holding its chunk is decided by under the
		// network's lock, as publishes decide (insertPacks).
		if err := net.lock(ctx); err != nil {
			return err
		}
		if err := lg.load(ctx, ids); err != nil {
			return err
		}
		if err := net.load(ctx, ids); err != nil {
			return err
		}
		num, err := tx.PutPack(ctx, st.net, st.pack, st.size)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if _, has, err := lg.has(ctx, encoding.KindChunk, id); err != nil {
				return err
			} else if !has {
				s.logIndex(tx, repoScope(repo), contentindex.Update{Op: contentindex.Put, ID: id, Entry: contentindex.Entry{
					Kind: encoding.KindChunk, Day: lg.today,
				}})
			}
			if _, located, err := net.has(ctx, encoding.KindChunk, id); err != nil {
				return err
			} else if !located {
				s.logIndex(tx, netScope, contentindex.Update{Op: contentindex.Put, ID: id, Entry: contentindex.Entry{
					Kind: encoding.KindChunk, Day: net.today, Loc: indexLocation(num, st.locs[id]),
				}})
			}
		}
	}
	trees := make([]encoding.ID, 0, len(st.trees))
	for id := range st.trees {
		trees = append(trees, id)
	}
	slices.SortFunc(trees, func(a, b encoding.ID) int { return bytes.Compare(a[:], b[:]) })
	if err := lg.load(ctx, trees); err != nil {
		return err
	}
	for _, id := range trees {
		if _, has, err := lg.has(ctx, encoding.KindTree, id); err != nil {
			return err
		} else if !has {
			e := st.trees[id]
			e.Day = lg.today
			s.logIndex(tx, repoScope(repo), contentindex.Update{Op: contentindex.Put, ID: id, Entry: e})
		}
	}
	return nil
}

// A large tree's index (docs/scm.md, "Large trees"): its leaves — chunks of whole entries, cut where
// their names say, a prolly tree's leaves — each named by its first entry's name. A lookup searches
// the names and reads the one leaf that would hold the entry. A leaf's first name is derived once a
// replica, from the leaf, and kept in memory by its chunk: the leaves a tree's versions share are
// named once, so a version's index costs reading only the leaves that are its own.

// leafNameKey keeps a leaf's first name in memory, by its chunk: a pure function of the leaf.
func leafNameKey(chunk encoding.ID) string { return "leaf-name/" + string(chunk[:]) }

// leafNames are the first names of a large tree's leaves, in order.
func (s *Store) leafNames(ctx context.Context, repo int64, e contentindex.Entry) ([]string, error) {
	leaves := e.Chunks[1:]
	out := make([]string, len(leaves))
	var want []encoding.ID
	for i, c := range leaves {
		if b, ok := s.layers.get(leafNameKey(c.ID)); ok {
			out[i] = string(b)
		} else {
			want = append(want, c.ID)
		}
	}
	if len(want) == 0 {
		return out, nil
	}
	data, err := s.chunkBytes(ctx, repo, want)
	if err != nil {
		return nil, err
	}
	for i, c := range leaves {
		if out[i] != "" {
			continue
		}
		b, ok := data[c.ID]
		if !ok {
			return nil, fmt.Errorf("a large tree's leaf %s: %w", c.ID, ErrNotFound)
		}
		name, err := encoding.TreeLeafFirst(b)
		if err != nil {
			return nil, err
		}
		s.layers.put(leafNameKey(c.ID), []byte(name), len(name)+encoding.IDLen)
		out[i] = name
	}
	return out, nil
}

// lookupLarge finds name in a large tree by its index, reading one leaf; large is false for a tree
// that is not large, which the caller reads whole.
func (s *Store) lookupLarge(ctx context.Context, repo int64, tree encoding.ID, name string) (v *encoding.TreeValue, large bool, err error) {
	if tree == encoding.EmptyTreeID {
		return nil, false, nil
	}
	if _, ok := s.objects.get(objectKey(repo, ObjectRef{Kind: encoding.KindTree, ID: tree})); ok {
		// Held whole in memory: read whole.
		return nil, false, nil
	}
	got, err := s.LargeTrees(ctx, repo, []encoding.ID{tree})
	if err != nil {
		return nil, false, err
	}
	e, ok := got[tree]
	if !ok {
		return nil, false, nil
	}
	names, err := s.leafNames(ctx, repo, e)
	if err != nil {
		return nil, true, err
	}
	i := sort.Search(len(names), func(k int) bool { return names[k] > name }) - 1
	if i < 0 {
		return nil, true, nil
	}
	leaf := e.Chunks[1+i].ID
	data, err := s.chunkBytes(ctx, repo, []encoding.ID{leaf})
	if err != nil {
		return nil, true, err
	}
	b, ok := data[leaf]
	if !ok {
		return nil, true, fmt.Errorf("a large tree's leaf %s: %w", leaf, ErrNotFound)
	}
	entries, err := encoding.TreeLeafEntries(b)
	if err != nil {
		return nil, true, err
	}
	j := sort.Search(len(entries), func(k int) bool { return entries[k].Name >= name })
	if j == len(entries) || entries[j].Name != name {
		return nil, true, nil
	}
	return &entries[j].Value, true, nil
}
