package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
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

// pinPlan is what pinning roots pins, found before the transaction from one snapshot (planPin): the
// unpinned commits and objects reachable from them, by table and kind — the walk stops at pinned
// ones, as pinning is permanent, and so is everything a pinned one reaches — and the files,
// chunks, and trees publishes stored that they name, which the transaction's content index pins.
type pinPlan struct {
	rows    map[pinGroup][]encoding.ID
	content []ref
}

type pinGroup struct {
	table table
	kind  encoding.Kind
}

// planPin finds what pinning roots pins, reading as it walks — a statement a level of the graph —
// outside any transaction, so that pinning it (pinPlanned) is a statement or two a kind, not a
// level: a commit's graph is immutable, so what it reaches does not change, and what is unpinned
// is pinned by a transaction meanwhile or not at all. A reference to what the repository does not
// hold fails it, with errMissing, as pin does. It reads one snapshot, the content index's refreshes
// through it too (inSnapshot): a refresh asking the pool for a connection while the snapshot held
// one could, with every connection held so, wait forever.
func (s *Store) planPin(ctx context.Context, repo int64, roots []ref) (pinPlan, error) {
	plan := pinPlan{rows: map[pinGroup][]encoding.ID{}}
	err := s.inSnapshot(ctx, func(ctx context.Context, tx *txn) error {
		// The snapshot itself, not the transaction over it: what it finds in the content index is a
		// first look, which the pin's transaction checks again under the index's lock.
		q := tx.Ops
		clear(plan.rows)
		plan.content = nil
		seen := map[ref]bool{}
		frontier := refSet{}
		for _, r := range roots {
			if !seen[r] {
				seen[r] = true
				frontier.add(r)
			}
		}
		for len(frontier) > 0 {
			groups := map[pinGroup][]encoding.ID{}
			for _, r := range frontier.sorted() {
				if r.table == tFile || r.table == tChunk {
					plan.content = append(plan.content, r)
					continue
				}
				g := pinGroup{r.table, r.kind}
				groups[g] = append(groups[g], r.id)
			}
			next := refSet{}
			for g, members := range groups {
				var unpinned map[encoding.ID][]byte
				var pinned map[encoding.ID]bool
				var err error
				kind := g.kind
				switch g.table {
				case tObject:
					unpinned, pinned, err = q.UnpinnedObjects(ctx, repo, g.kind, members)
				case tCommit:
					unpinned, pinned, err = q.UnpinnedCommits(ctx, repo, members)
					kind = encoding.KindCommit
				default:
					return fmt.Errorf("pin: %v is not a repository object table", g.table)
				}
				if err != nil {
					return err
				}
				var rowless []encoding.ID
				for _, id := range members {
					if _, ok := unpinned[id]; !ok && !pinned[id] {
						rowless = append(rowless, id)
					}
				}
				indexed := map[encoding.ID]bool{}
				if g.table == tObject && g.kind == encoding.KindTree && len(rowless) > 0 {
					// A tree a publish stored is the content index's: an unpinned one is pinned there,
					// and the walk goes on through its body, in its pack.
					_, entries, err := s.contentEntries(ctx, repoScope(repo), encoding.KindTree, rowless)
					if err != nil {
						return err
					}
					var rest, toPin []encoding.ID
					for _, id := range rowless {
						e, ok := entries[id]
						switch {
						case !ok:
							rest = append(rest, id)
						case !e.Pinned:
							toPin = append(toPin, id)
							indexed[id] = true
							plan.content = append(plan.content, ref{tObject, encoding.KindTree, id})
						}
					}
					bodies, err := s.treeBodies(ctx, repo, toPin)
					if err != nil {
						return err
					}
					if unpinned == nil {
						unpinned = map[encoding.ID][]byte{}
					}
					for id, body := range bodies {
						unpinned[id] = body
					}
					rowless = rest
				}
				// What has no row is pinned, in a segment, or not held.
				present, err := s.existing(ctx, q, repo, g.table, g.kind, rowless)
				if err != nil {
					return err
				}
				for _, id := range rowless {
					if !present[id] {
						return fmt.Errorf("%w: pinning reached %s, which the repository lacks", errMissing, ref{g.table, g.kind, id}.objectRef())
					}
				}
				for id, body := range unpinned {
					if !indexed[id] {
						plan.rows[g] = append(plan.rows[g], id)
					}
					o, err := decodeStored(kind, id, body)
					if err != nil {
						return err
					}
					rs := refSet{}
					gcRefs(rs, o)
					for _, k := range rs.sorted() {
						if !seen[k] {
							seen[k] = true
							next.add(k)
						}
					}
				}
			}
			frontier = next
		}
		return nil
	})
	return plan, err
}

// pinPlanned pins, in tx, what plan holds — a kind at a time, in the walk's order, each row only
// if it is unpinned still — and the files and chunks in the content index. A row the plan found
// that has none now fails it with errMissing, as pin does: a sweep took it.
func (s *Store) pinPlanned(ctx context.Context, tx *txn, repo int64, plan pinPlan) error {
	groups := make([]pinGroup, 0, len(plan.rows))
	for g := range plan.rows {
		groups = append(groups, g)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].table < groups[j].table || (groups[i].table == groups[j].table && groups[i].kind < groups[j].kind)
	})
	for _, g := range groups {
		members := plan.rows[g]
		sortIDs(members)
		updated, held, err := s.walkGroup(ctx, tx, repo, g.table, g.kind, members, pinWalk)
		if err != nil {
			return err
		}
		var stale []encoding.ID
		for _, id := range members {
			if _, ok := updated[id]; !ok && !held[id] {
				stale = append(stale, id)
			}
		}
		present, err := s.existing(ctx, tx, repo, g.table, g.kind, stale)
		if err != nil {
			return err
		}
		for _, id := range stale {
			if !present[id] {
				return fmt.Errorf("%w: pinning reached %s, which a sweep took", errMissing, ref{g.table, g.kind, id}.objectRef())
			}
		}
	}
	if len(plan.content) > 0 {
		missing, err := s.walk(ctx, tx, repo, plan.content, pinWalk)
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			return fmt.Errorf("%w: pinning reached %d missing objects, first %s", errMissing, len(missing), missing[0].objectRef())
		}
	}
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
// (backend.Ops.WalkObjects; none for files and chunks, updated in the content index). Trees a
// publish stored are the content index's, the rest the object table's.
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
		var indexed map[encoding.ID][]ref
		var indexHeld map[encoding.ID]bool
		if kind == encoding.KindTree {
			if indexed, indexHeld, members, err = s.walkIndexTrees(ctx, tx, repo, members, mode); err != nil {
				return nil, nil, err
			}
		}
		updated, held, err = tx.WalkObjects(ctx, repo, kind, members, mode)
		if err != nil {
			return nil, nil, err
		}
		out, err := walkedRefs(kind, updated)
		if err != nil {
			return nil, nil, err
		}
		for id, kids := range indexed {
			out[id] = kids
		}
		if len(indexHeld) > 0 {
			if held == nil {
				held = map[encoding.ID]bool{}
			}
			for id := range indexHeld {
				held[id] = true
			}
		}
		return out, held, nil
	case tCommit:
		updated, held, err = tx.WalkCommits(ctx, repo, members, mode)
		kind = encoding.KindCommit
	default:
		return nil, nil, fmt.Errorf("touch: %v is not a repository object table", t)
	}
	if err != nil {
		return nil, nil, err
	}
	out, err := walkedRefs(kind, updated)
	return out, held, err
}

// walkedRefs are, for each object a walk updated, the references it continues into.
func walkedRefs(kind encoding.Kind, updated map[encoding.ID][]byte) (map[encoding.ID][]ref, error) {
	out := make(map[encoding.ID][]ref, len(updated))
	for id, body := range updated {
		o, err := decodeStored(kind, id, body)
		if err != nil {
			return nil, err
		}
		rs := refSet{}
		gcRefs(rs, o)
		out[id] = rs.sorted()
	}
	return out, nil
}

// walkIndexTrees is walkGroup for the trees among members the transaction's content index holds:
// it updates those mode applies to, in the index — a pin in the network's chunk index too, where
// their locations are, as a chunk's — and returns the references each it updated continues into,
// read from its pack, the trees it found held that mode does not apply to, and the members the
// index does not hold, which are the object table's. A tree this transaction stored is held: a
// publish stores trees fresh, and pins come in transactions of their own.
func (s *Store) walkIndexTrees(ctx context.Context, tx *txn, repo int64, members []encoding.ID, mode walkMode) (map[encoding.ID][]ref, map[encoding.ID]bool, []encoding.ID, error) {
	lg, err := s.txIndex(ctx, tx, repoScope(repo))
	if err != nil {
		return nil, nil, nil, err
	}
	var net *txLog
	var netScope backend.Scope
	if mode == pinWalk {
		n, err := s.network(ctx, repo)
		if err != nil {
			return nil, nil, nil, err
		}
		netScope = backend.NetworkScope(n)
		if net, err = s.txIndex(ctx, tx, netScope); err != nil {
			return nil, nil, nil, err
		}
		if err := net.load(ctx, members); err != nil {
			return nil, nil, nil, err
		}
	}
	if err := lg.load(ctx, members); err != nil {
		return nil, nil, nil, err
	}
	held := map[encoding.ID]bool{}
	var rest, read []encoding.ID
	for _, id := range members {
		e, ok, err := lg.has(ctx, encoding.KindTree, id)
		if err != nil {
			return nil, nil, nil, err
		}
		if !ok {
			rest = append(rest, id)
			continue
		}
		_, put := lg.puts[id]
		switch {
		case put:
			held[id] = true
		case mode == touchWalk && (e.Pinned || e.Day >= lg.today || lg.touched[id]),
			mode == refreshWalk && (e.Pinned || lg.touched[id]),
			mode == pinWalk && (e.Pinned || lg.pinned[id]):
			held[id] = true
		case mode == pinWalk:
			s.logIndex(tx, repoScope(repo), pinUpdate(encoding.KindTree, id))
			if loc, ok, err := net.has(ctx, encoding.KindTree, id); err != nil {
				return nil, nil, nil, err
			} else if ok && !loc.Pinned && !net.pinned[id] {
				s.logIndex(tx, netScope, pinUpdate(encoding.KindTree, id))
			}
			read = append(read, id)
		default:
			s.logIndex(tx, repoScope(repo), touchUpdate(encoding.KindTree, id, lg.today.Time()))
			read = append(read, id)
		}
	}
	bodies, err := s.treeBodies(ctx, repo, read)
	if err != nil {
		return nil, nil, nil, err
	}
	out, err := walkedRefs(encoding.KindTree, bodies)
	return out, held, rest, err
}

// treeBodies reads trees the repository's content index holds from their packs: each one's
// canonical body. Every one must be there.
func (s *Store) treeBodies(ctx context.Context, repo int64, ids []encoding.ID) (map[encoding.ID][]byte, error) {
	out := make(map[encoding.ID][]byte, len(ids))
	var misses []encoding.ID
	for _, id := range ids {
		if body, ok := s.objects.get(objectKey(repo, ObjectRef{Kind: encoding.KindTree, ID: id})); ok {
			out[id] = body
		} else {
			misses = append(misses, id)
		}
	}
	if len(misses) == 0 {
		return out, nil
	}
	stored, err := s.storedRecords(ctx, repo, encoding.KindTree, misses)
	if err != nil {
		return nil, err
	}
	net, err := s.network(ctx, repo)
	if err != nil {
		return nil, err
	}
	for id, c := range stored {
		raw, err := pack.Decompress(s.dicts.of(net), id, c.codec, c.raw, c.data)
		if err != nil {
			return nil, err
		}
		out[id] = bytes.Clone(raw)
	}
	return out, nil
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
	if err := lg.load(ctx, members); err != nil {
		return nil, err
	}
	if net != nil {
		if err := net.load(ctx, members); err != nil {
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
// files, chunks, and the trees publishes stored in its content index — in tx's, when q is a
// transaction — and the other objects, and the trees landings made, in the object table and its
// segments.
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
		return s.indexHolds(ctx, q, repo, kind, members)
	}
	switch t {
	case tObject:
		present := map[encoding.ID]bool{}
		rest := members
		if kind == encoding.KindTree {
			var err error
			if present, err = s.indexHolds(ctx, q, repo, kind, members); err != nil || len(present) == len(members) {
				return present, err
			}
			rest = nil
			for _, id := range members {
				if !present[id] {
					rest = append(rest, id)
				}
			}
		}
		inTable, err := q.ExistingObjects(ctx, repo, kind, rest)
		if err != nil {
			return nil, err
		}
		for id := range inTable {
			present[id] = true
		}
		if len(present) == len(members) {
			return present, nil
		}
		// The rest are pinned, in segments, unless the repository lacks them.
		rest = nil
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

// indexHolds returns which of members of kind the repository's content index holds: in tx's view,
// when q is a transaction; outside one, a first look — what it finds held, the caller's touch
// checks again under the index's lock.
func (s *Store) indexHolds(ctx context.Context, q backend.Ops, repo int64, kind encoding.Kind, members []encoding.ID) (map[encoding.ID]bool, error) {
	out := map[encoding.ID]bool{}
	if tx, ok := q.(*txn); ok {
		lg, err := s.txIndex(ctx, tx, repoScope(repo))
		if err != nil {
			return nil, err
		}
		if err := lg.load(ctx, members); err != nil {
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
	_, entries, err := s.knownEntries(ctx, repoScope(repo), kind, members)
	if err != nil {
		return nil, err
	}
	for id := range entries {
		out[id] = true
	}
	return out, nil
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
