package store

import (
	"context"
	"slices"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// A repository's commits are in its tail until pinned, then in the commit graph's segments and
// their bodies' (segments.go): every read of one looks in the tail, then the segments.

// commitGraph is the generation and parents of each of ids the repository holds: the tail's, then
// the commit graph's segments'.
func (s *Store) commitGraph(ctx context.Context, repo int64, ids []encoding.ID) (map[encoding.ID]backend.GraphCommit, error) {
	out := map[encoding.ID]backend.GraphCommit{}
	if len(ids) == 0 {
		return out, nil
	}
	tail, err := s.ops(ctx).TailGraph(ctx, repo, ids)
	if err != nil {
		return nil, err
	}
	for _, g := range tail {
		out[g.ID] = g
	}
	if err := s.segmentGraph(ctx, repo, ids, out); err != nil {
		return nil, err
	}
	return out, nil
}

// segmentGraph adds to found the commit graph's records, from segments, of ids it lacks.
func (s *Store) segmentGraph(ctx context.Context, repo int64, ids []encoding.ID, found map[encoding.ID]backend.GraphCommit) error {
	var keys [][]byte
	for i := range ids {
		if _, ok := found[ids[i]]; !ok {
			keys = append(keys, ids[i][:])
		}
	}
	if len(keys) == 0 {
		return nil
	}
	got, err := s.segmentGet(ctx, repoScope(repo), segCommitGraph, keys)
	if err != nil {
		return err
	}
	for k, v := range got {
		g, err := decodeGraph(encoding.ID([]byte(k)), v)
		if err != nil {
			return err
		}
		found[g.ID] = g
	}
	return nil
}

// commitGenerations is the generation of each of ids the repository holds.
func (s *Store) commitGenerations(ctx context.Context, repo int64, ids []encoding.ID) (map[encoding.ID]int64, error) {
	out, err := s.ops(ctx).CommitGenerations(ctx, repo, ids)
	if err != nil || len(out) == len(ids) {
		return out, err
	}
	found := map[encoding.ID]backend.GraphCommit{}
	for id := range out {
		found[id] = backend.GraphCommit{ID: id}
	}
	if err := s.segmentGraph(ctx, repo, ids, found); err != nil {
		return nil, err
	}
	for id, g := range found {
		if _, ok := out[id]; !ok {
			out[id] = g.Generation
		}
	}
	return out, nil
}

// generation is a stored commit's generation; the root's is 0.
func (s *Store) generation(ctx context.Context, repo int64, id encoding.ID) (int64, error) {
	if id == encoding.RootCommitID {
		return 0, nil
	}
	gens, err := s.commitGenerations(ctx, repo, []encoding.ID{id})
	if err != nil {
		return 0, err
	}
	g, ok := gens[id]
	if !ok {
		return 0, notFound("commit %s", id)
	}
	return g, nil
}

// ancestry is the parent edges of from and of every ancestor down to generation minGen, by commit:
// each commit reached, its parents in order and their generations; a parent the repository lacks —
// the virtual root — is no edge. A commit of from the repository lacks is reached, with no edges.
//
// It walks the commit graph's segments in memory, a generation of the walk at a time, and the
// tail with one recursive query from wherever the walk first needs it — as far as the tail goes —
// so a walk costs a query each time it enters the tail, not each generation, whatever its depth;
// pinned history, compacted, costs none once its blocks are cached.
func (s *Store) ancestry(ctx context.Context, repo int64, from []encoding.ID, minGen int64) ([]backend.Ancestor, error) {
	known := map[encoding.ID]backend.GraphCommit{}
	absent := map[encoding.ID]bool{}
	// resolve looks for the commits among ids not yet looked for: in the segments, then the rest in
	// the tail, walking it from them down to minGen at once.
	resolve := func(ids []encoding.ID) error {
		var want []encoding.ID
		for _, id := range ids {
			if _, ok := known[id]; !ok && !absent[id] {
				want = append(want, id)
			}
		}
		if len(want) == 0 {
			return nil
		}
		if err := s.segmentGraph(ctx, repo, want, known); err != nil {
			return err
		}
		var rest []encoding.ID
		for _, id := range want {
			if _, ok := known[id]; !ok {
				rest = append(rest, id)
			}
		}
		if len(rest) == 0 {
			return nil
		}
		tail, err := s.ops(ctx).TailAncestry(ctx, repo, rest, minGen)
		if err != nil {
			return err
		}
		for _, g := range tail {
			known[g.ID] = g
		}
		for _, id := range rest {
			if _, ok := known[id]; !ok {
				absent[id] = true
			}
		}
		return nil
	}
	var out []backend.Ancestor
	visited := map[encoding.ID]bool{}
	frontier := slices.Clone(from)
	for len(frontier) > 0 {
		if err := resolve(frontier); err != nil {
			return nil, err
		}
		var parents []encoding.ID
		for _, id := range frontier {
			if g, ok := known[id]; ok && !visited[id] {
				parents = append(parents, g.Parents...)
			}
		}
		if err := resolve(parents); err != nil {
			return nil, err
		}
		var next []encoding.ID
		for _, id := range frontier {
			if visited[id] {
				continue
			}
			visited[id] = true
			a := backend.Ancestor{ID: id}
			for _, p := range known[id].Parents {
				pg, ok := known[p]
				if !ok {
					continue
				}
				a.Parents = append(a.Parents, p)
				a.Generations = append(a.Generations, pg.Generation)
				if pg.Generation >= minGen && !visited[p] {
					next = append(next, p)
				}
			}
			out = append(out, a)
		}
		frontier = next
	}
	slices.SortFunc(out, func(a, b backend.Ancestor) int { return compareIDs(a.ID, b.ID) })
	return out, nil
}

func compareIDs(a, b encoding.ID) int {
	for i := range a {
		if a[i] != b[i] {
			return int(a[i]) - int(b[i])
		}
	}
	return 0
}
