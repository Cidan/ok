package store

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// walkMode is what a walk over repository objects does to each row it reaches.
type walkMode = backend.Walk

const (
	// touchWalk sets an unpinned row's day to today, if it is older, and recurses only through
	// rows it updated: a row touched today already has its subgraph fresh.
	touchWalk = backend.TouchWalk
	// refreshWalk sets every unpinned row's day to today and recurses through all of them, for
	// a subgraph whose new rows are fresh but reuse older ones.
	refreshWalk = backend.RefreshWalk
	// pinWalk pins an unpinned row.
	pinWalk = backend.PinWalk
)

// touch runs the freshness walk (docs/scm.md, "Garbage collection and
// compaction") from roots, which must be repository objects: files, chunks, and objects
// in the object and commit tables. Touching an unpinned object sets its touched_at to
// today and recurses into what it references, but only through rows the walk actually
// updated, so an unpinned object is never fresher than anything beneath it and a subgraph
// already touched today is walked at most once. A reference to an object that no longer
// exists is returned as missing, attributed to the root it was reached from; the caller
// fails its transaction on any.
func (s *Store) touch(ctx context.Context, tx *txn, repo int64, roots []ref) ([]ref, error) {
	return s.walk(ctx, tx, repo, roots, touchWalk)
}

// errMissing reports a walk that reached an object with no row.
var errMissing = errors.New("missing object")

// pin makes everything reachable from roots permanent (docs/scm.md, "Garbage
// collection and compaction"): it pins each unpinned object and recurses into what it
// references, stopping at objects already pinned, so the cost is proportional to what is
// newly reachable. A reference to an object that does not exist fails the transaction
// (with errMissing) instead of pinning a dangling graph.
func (s *Store) pin(ctx context.Context, tx *txn, repo int64, roots []ref) error {
	missing, err := s.walk(ctx, tx, repo, roots, pinWalk)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: pinning reached %d missing objects, first %s", errMissing, len(missing), missing[0].objectRef())
	}
	// What it pinned is compaction's to move into segments, marked once the transaction commits.
	tx.markAfter(SegmentWork, repo)
	return nil
}

// refresh makes every unpinned object reachable from roots fresh today, recursing through
// all of them rather than stopping at rows already touched today: a transaction that wrote
// new rows (fresh by default) over older ones it reuses refreshes those too. It fails with
// errMissing if any root reaches an object with no row, so its transaction never commits
// a fresh object over a missing one.
func (s *Store) refresh(ctx context.Context, tx *txn, repo int64, roots []ref) error {
	missing, err := s.walk(ctx, tx, repo, roots, refreshWalk)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: refreshing reached %d missing objects, first %s", errMissing, len(missing), missing[0].objectRef())
	}
	return nil
}

// walk updates the rows reachable from roots under mode, recursing only through rows it
// updated, and returns the references that resolve to no row.
func (s *Store) walk(ctx context.Context, tx *txn, repo int64, roots []ref, mode walkMode) ([]ref, error) {
	origin := map[ref]ref{}
	frontier := make([]ref, 0, len(roots))
	for _, r := range roots {
		if _, ok := origin[r]; !ok {
			origin[r] = r
			frontier = append(frontier, r)
		}
	}
	missing := refSet{}
	for len(frontier) > 0 {
		type group struct {
			table table
			kind  encoding.Kind
		}
		groups := map[group][]encoding.ID{}
		for _, r := range frontier {
			g := group{r.table, r.kind}
			groups[g] = append(groups[g], r.id)
		}
		keys := make([]group, 0, len(groups))
		for g := range groups {
			keys = append(keys, g)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].table < keys[j].table || (keys[i].table == keys[j].table && keys[i].kind < keys[j].kind)
		})
		next := refSet{}
		for _, g := range keys {
			members := groups[g]
			sortIDs(members)
			children, held, err := s.walkGroup(ctx, tx, repo, g.table, g.kind, members, mode)
			if err != nil {
				return nil, err
			}
			var stale []encoding.ID
			for _, id := range members {
				if _, updated := children[id]; !updated && !held[id] {
					stale = append(stale, id)
				}
			}
			present, err := s.existing(ctx, tx, repo, g.table, g.kind, stale)
			if err != nil {
				return nil, err
			}
			for _, id := range stale {
				if !present[id] {
					missing.add(origin[ref{g.table, g.kind, id}])
				}
			}
			for parent, kids := range children {
				from := origin[ref{g.table, g.kind, parent}]
				for _, k := range kids {
					if _, seen := origin[k]; !seen {
						origin[k] = from
						next.add(k)
					}
				}
			}
		}
		frontier = next.sorted()
	}
	return missing.sorted(), nil
}

// walkGroup updates the rows among ids that mode applies to — unpinned and stale for a
// touch, unpinned for a refresh or a pin — and returns, for each row it updated, the
// references the walk continues into, and the rows it found held that mode does not apply to
// (backend.Ops.WalkObjects; none for files and chunks, updated in the content index).
func (s *Store) walkGroup(ctx context.Context, tx *txn, repo int64, t table, kind encoding.Kind, members []encoding.ID, mode walkMode) (map[encoding.ID][]ref, map[encoding.ID]bool, error) {
	if t == tFile || t == tChunk {
		out, err := s.walkContent(ctx, tx, repo, t, members, mode)
		return out, nil, err
	}
	var updated map[encoding.ID][]byte
	var held map[encoding.ID]bool
	var err error
	switch t {
	case tObject:
		updated, held, err = tx.WalkObjects(ctx, repo, kind, members, mode)
	case tCommit:
		updated, held, err = tx.WalkCommits(ctx, repo, members, mode)
		kind = encoding.KindCommit
	default:
		return nil, nil, fmt.Errorf("touch: %v is not a repository object table", t)
	}
	if err != nil {
		return nil, nil, err
	}
	out := make(map[encoding.ID][]ref, len(updated))
	for id, body := range updated {
		o, err := decodeStored(kind, id, body)
		if err != nil {
			return nil, nil, err
		}
		rs := refSet{}
		gcRefs(rs, o)
		out[id] = rs.sorted()
	}
	return out, held, nil
}

// walkContent is walkGroup for files and chunks, in the transaction's content index: a file
// it updates continues into its chunks.
func (s *Store) walkContent(ctx context.Context, tx *txn, repo int64, t table, members []encoding.ID, mode walkMode) (map[encoding.ID][]ref, error) {
	lg, err := s.txIndex(ctx, tx, repoScope(repo))
	if err != nil {
		return nil, err
	}
	kind := encoding.KindChunk
	if t == tFile {
		kind = encoding.KindFile
	}
	// A chunk pinned is pinned in its network's chunk index too, so its location folds into the
	// network's segments as its entry folds into the repository's.
	var net *txLog
	var netScope backend.Scope
	if kind == encoding.KindChunk && mode == pinWalk {
		n, err := s.network(ctx, repo)
		if err != nil {
			return nil, err
		}
		netScope = backend.NetworkScope(n)
		if net, err = s.txIndex(ctx, tx, netScope); err != nil {
			return nil, err
		}
	}
	out := map[encoding.ID][]ref{}
	for _, id := range members {
		e, ok, err := lg.has(ctx, kind, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		switch mode {
		case touchWalk:
			if e.Pinned || e.Day >= lg.today || lg.touched[id] {
				continue
			}
			s.logIndex(tx, repoScope(repo), touchUpdate(kind, id, lg.today.Time()))
		case refreshWalk:
			if e.Pinned || lg.touched[id] {
				continue
			}
			s.logIndex(tx, repoScope(repo), touchUpdate(kind, id, lg.today.Time()))
		case pinWalk:
			if e.Pinned || lg.pinned[id] {
				continue
			}
			s.logIndex(tx, repoScope(repo), pinUpdate(kind, id))
			if net != nil {
				if loc, ok, err := net.has(ctx, kind, id); err != nil {
					return nil, err
				} else if ok && !loc.Pinned && !net.pinned[id] {
					s.logIndex(tx, netScope, pinUpdate(kind, id))
				}
			}
		}
		var kids []ref
		for _, c := range e.Chunks {
			kids = append(kids, ref{table: tChunk, id: c.ID})
		}
		out[id] = kids
	}
	return out, nil
}

// existing returns which of ids the repository holds in t (with kind, for the object table):
// files and chunks in its content index — in tx's, when q is a transaction.
func (s *Store) existing(ctx context.Context, q backend.Ops, repo int64, t table, kind encoding.Kind, members []encoding.ID) (map[encoding.ID]bool, error) {
	out := map[encoding.ID]bool{}
	if len(members) == 0 {
		return out, nil
	}
	if t == tFile || t == tChunk {
		kind = encoding.KindChunk
		if t == tFile {
			kind = encoding.KindFile
		}
		if tx, ok := q.(*txn); ok {
			lg, err := s.txIndex(ctx, tx, repoScope(repo))
			if err != nil {
				return nil, err
			}
			for _, id := range members {
				_, ok, err := lg.has(ctx, kind, id)
				if err != nil {
					return nil, err
				}
				if ok {
					out[id] = true
				}
			}
			return out, nil
		}
		// A first look, outside a transaction: what it finds held, the caller's touch checks again
		// under the index's lock (touchHeld).
		_, entries, err := s.knownEntries(ctx, repoScope(repo), kind, members)
		if err != nil {
			return nil, err
		}
		for id := range entries {
			out[id] = true
		}
		return out, nil
	}
	switch t {
	case tObject:
		present, err := q.ExistingObjects(ctx, repo, kind, members)
		if err != nil || len(present) == len(members) {
			return present, err
		}
		// The rest are pinned, in segments, unless the repository lacks them.
		var rest []encoding.ID
		for _, id := range members {
			if !present[id] {
				rest = append(rest, id)
			}
		}
		moved, err := s.segmentObjects(ctx, repo, kind, rest)
		if err != nil {
			return nil, err
		}
		for id := range moved {
			present[id] = true
		}
		return present, nil
	case tCommit:
		present, err := q.ExistingCommits(ctx, repo, members)
		if err != nil || len(present) == len(members) {
			return present, err
		}
		found := map[encoding.ID]backend.GraphCommit{}
		for id := range present {
			found[id] = backend.GraphCommit{ID: id}
		}
		if err := s.segmentGraph(ctx, repo, members, found); err != nil {
			return nil, err
		}
		for id := range found {
			present[id] = true
		}
		return present, nil
	}
	return nil, fmt.Errorf("existing: %v is not a repository object table", t)
}

// decodeStored decodes a stored canonical body, which the store verified on the way in;
// a failure is corruption.
func decodeStored(kind encoding.Kind, id encoding.ID, body []byte) (encoding.Object, error) {
	canonical := append([]byte{byte(kind)}, body...)
	o, err := encoding.Decode(canonical)
	if err != nil {
		return nil, fmt.Errorf("stored %s %s does not decode: %w", kind, id, err)
	}
	return o, nil
}
