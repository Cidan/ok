package store

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/serve/metrics"
)

// Tree sizes (docs/scm.md, "Tree sizes"): each tree's record of what its entries weigh,
// which a virtual workspace needs to answer a stat without reading any file. A record depends on
// its tree and the files and symlinks the tree names — never on the subtrees — so any tree, in
// any commit, at any depth, derives on its own: there is no backfill and no walk of history. It is
// derived the first time it is read and kept in tree_sizes, which its tree's deletion cascades to.
//
// The records are kept where trees are, so they move with them when trees leave PostgreSQL for
// packs (docs/scm.md, "Sequencing"): a record is self-describing and versioned
// for that.

// treeSizesServed is how each record was served.
var treeSizesServed = metrics.NewCounter("ok_scm_tree_sizes_total",
	"Trees' sizes records served, by where they came from: memory, kept (PostgreSQL), or derived (read for the first time).", "source")

// emptyTreeSizes is the virtual empty tree's record.
var emptyTreeSizes = encoding.EncodeTreeSizes(&encoding.TreeSizes{})

func treeSizesKey(repo int64, tree encoding.ID) string {
	return objectKey(repo, ObjectRef{Kind: encoding.KindTreeSizes, ID: tree})
}

// TreeSizes returns the sizes record (encoding.EncodeTreeSizes) of each tree the repository holds,
// by tree ID: from memory, then PostgreSQL, deriving and keeping those never read before. A tree
// the repository does not hold is not in the result.
func (s *Store) TreeSizes(ctx context.Context, repo int64, trees []encoding.ID) (map[encoding.ID][]byte, error) {
	out := make(map[encoding.ID][]byte, len(trees))
	var want []encoding.ID
	seen := make(map[encoding.ID]bool, len(trees))
	for _, id := range trees {
		if seen[id] {
			continue
		}
		seen[id] = true
		if id == encoding.EmptyTreeID {
			out[id] = emptyTreeSizes
			continue
		}
		if b, ok := s.objects.get(treeSizesKey(repo, id)); ok {
			out[id] = b
			treeSizesServed.With("memory").Inc()
			continue
		}
		want = append(want, id)
	}
	for start := 0; start < len(want); start += treeBatch {
		batch := want[start:min(start+treeBatch, len(want))]
		kept, err := s.keptTreeSizes(ctx, repo, batch)
		if err != nil {
			return nil, err
		}
		var missing []encoding.ID
		for _, id := range batch {
			if b, ok := kept[id]; ok {
				out[id] = b
				s.objects.put(treeSizesKey(repo, id), b, len(b))
				treeSizesServed.With("kept").Inc()
			} else {
				missing = append(missing, id)
			}
		}
		if len(missing) == 0 {
			continue
		}
		derived, err := s.deriveTreeSizes(ctx, repo, missing)
		if err != nil {
			return nil, err
		}
		for id, b := range derived {
			out[id] = b
			s.objects.put(treeSizesKey(repo, id), b, len(b))
			treeSizesServed.With("derived").Inc()
		}
	}
	return out, nil
}

// keptTreeSizes reads the records already kept for trees.
func (s *Store) keptTreeSizes(ctx context.Context, repo int64, trees []encoding.ID) (map[encoding.ID][]byte, error) {
	rows, err := s.pool.Query(ctx, `SELECT tree_id, body FROM tree_sizes WHERE repository_id = $1 AND tree_id = ANY($2)`, repo, ids(trees))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[encoding.ID][]byte, len(trees))
	for rows.Next() {
		var idb, body []byte
		if err := rows.Scan(&idb, &body); err != nil {
			return nil, err
		}
		id, err := toID(idb)
		if err != nil {
			return nil, err
		}
		out[id] = body
	}
	return out, rows.Err()
}

// deriveTreeSizes derives the records of trees never read before, from each tree's entries alone,
// and keeps them. A tree the repository does not hold is skipped.
func (s *Store) deriveTreeSizes(ctx context.Context, repo int64, trees []encoding.ID) (map[encoding.ID][]byte, error) {
	refs := make([]ObjectRef, len(trees))
	for i, id := range trees {
		refs[i] = ObjectRef{Kind: encoding.KindTree, ID: id}
	}
	res, err := s.ReadObjects(ctx, repo, refs)
	if err != nil {
		return nil, err
	}
	decoded := make(map[encoding.ID]*encoding.Tree, len(res))
	var files, symlinks []encoding.ID
	seenFile, seenLink := map[encoding.ID]bool{}, map[encoding.ID]bool{}
	for _, r := range res {
		if !r.Found {
			continue
		}
		o, err := decodeStored(encoding.KindTree, r.Ref.ID, r.Body)
		if err != nil {
			return nil, err
		}
		t := o.(*encoding.Tree)
		decoded[r.Ref.ID] = t
		for _, e := range t.Entries {
			switch e.Value.Kind {
			case encoding.TreeValueFile:
				if !seenFile[e.Value.ID] {
					seenFile[e.Value.ID] = true
					files = append(files, e.Value.ID)
				}
			case encoding.TreeValueSymlink:
				if !seenLink[e.Value.ID] {
					seenLink[e.Value.ID] = true
					symlinks = append(symlinks, e.Value.ID)
				}
			}
		}
	}
	fileSizes, err := s.fileSizes(ctx, repo, files)
	if err != nil {
		return nil, err
	}
	linkSizes, err := s.symlinkSizes(ctx, repo, symlinks)
	if err != nil {
		return nil, err
	}
	out := make(map[encoding.ID][]byte, len(decoded))
	var keepIDs, keepBodies [][]byte
	for id, t := range decoded {
		rec := &encoding.TreeSizes{Sizes: make([]uint64, len(t.Entries))}
		for i, e := range t.Entries {
			switch e.Value.Kind {
			case encoding.TreeValueFile:
				n, ok := fileSizes[e.Value.ID]
				if !ok {
					return nil, fmt.Errorf("tree %s names file %s, which the repository does not hold", id, e.Value.ID)
				}
				rec.Sizes[i] = n
			case encoding.TreeValueSymlink:
				n, ok := linkSizes[e.Value.ID]
				if !ok {
					return nil, fmt.Errorf("tree %s names symlink %s, which the repository does not hold", id, e.Value.ID)
				}
				rec.Sizes[i] = n
			}
		}
		b := encoding.EncodeTreeSizes(rec)
		out[id] = b
		keepIDs = append(keepIDs, id[:])
		keepBodies = append(keepBodies, b)
	}
	if len(keepIDs) > 0 {
		// Keeping is what spares the next reader the derivation; a record not kept — its tree
		// collected meanwhile, or the database busy — is still correct to serve.
		if _, err := s.pool.Exec(ctx, `INSERT INTO tree_sizes (repository_id, tree_id, body)
			SELECT $1, t.id, t.body FROM unnest($2::bytea[], $3::bytea[]) AS t(id, body)
			ON CONFLICT DO NOTHING`, repo, keepIDs, keepBodies); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			slog.Warn("ok scm: keeping trees' sizes", slog.Int64("repository", repo), slog.Int("trees", len(keepIDs)), slog.Any("err", err))
		}
	}
	return out, nil
}

// fileSizes reads files' sizes from the content index; an absent file has none.
func (s *Store) fileSizes(ctx context.Context, repo int64, files []encoding.ID) (map[encoding.ID]uint64, error) {
	_, entries, err := s.contentEntries(ctx, repo, encoding.KindFile, files)
	if err != nil {
		return nil, err
	}
	out := make(map[encoding.ID]uint64, len(entries))
	for id, e := range entries {
		out[id] = e.Size
	}
	return out, nil
}

// symlinkSizes reads symlinks' target lengths: what a stat of each reports.
func (s *Store) symlinkSizes(ctx context.Context, repo int64, links []encoding.ID) (map[encoding.ID]uint64, error) {
	out := make(map[encoding.ID]uint64, len(links))
	for start := 0; start < len(links); start += treeBatch {
		batch := links[start:min(start+treeBatch, len(links))]
		refs := make([]ObjectRef, len(batch))
		for i, id := range batch {
			refs[i] = ObjectRef{Kind: encoding.KindSymlink, ID: id}
		}
		res, err := s.ReadObjects(ctx, repo, refs)
		if err != nil {
			return nil, err
		}
		for _, r := range res {
			if !r.Found {
				continue
			}
			o, err := decodeStored(encoding.KindSymlink, r.Ref.ID, r.Body)
			if err != nil {
				return nil, err
			}
			out[r.Ref.ID] = uint64(len(o.(*encoding.Symlink).Target))
		}
	}
	return out, nil
}
