package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/work"
)

// Garbage collection's parameters (docs/scm.md, "Garbage collection and
// compaction").
const (
	// OperationRetention is how long an unreachable operation or view is kept after it
	// was last published, the default of jj util gc.
	OperationRetention = 14 * 24 * time.Hour
	// sweepGraceDays is how many days an unpinned, unreachable object is kept after it
	// was last touched: the operation retention, plus a day for touched_at's
	// quantization to days.
	sweepGraceDays = 15
	// RetiredPackGrace is how long a pack compaction retired stays in S3, for reads that
	// located a chunk in it before the chunk moved.
	RetiredPackGrace = time.Hour
	// OrphanPackGrace is how long a pack with no row (a failed publish's or compaction's
	// upload) stays in S3 before it is deleted.
	OrphanPackGrace = 24 * time.Hour

	// gcBatch bounds the IDs one statement of the mark or the sweep names.
	gcBatch = 1000
)

// GCOptions shapes one garbage-collection pass.
type GCOptions struct {
	// DryRun computes and reports what the pass would do, and changes nothing.
	DryRun bool
}

// GCReport is what one pass over a repository did (or, dry, would do).
type GCReport struct {
	Repository int64
	DryRun     bool
	Started    time.Time
	Finished   time.Time

	// Operations and views collected from the repository's operation logs.
	OperationsDeleted, ViewsDeleted int64
	// What no request can reach, from before each workspace had its own operation log, deleted
	// whole: the operation logs of no workspace, and the workspaces with no log of their own
	// (their leases and checkpoints).
	OpLogsDeleted, WorkspacesDeleted int64
	// Checkpoints retention no longer keeps (docs/okfs.md, "Checkpoints").
	CheckpointsDeleted int64
	// Marked counts the unpinned, stale objects still reachable, which the sweep keeps.
	Marked int
	// Rows swept, by table, and the stored bytes of the chunks whose locations went with them.
	CommitsSwept, ObjectsSwept, FilesSwept, ChunksSwept int64
	ChunkBytesSwept                                     int64
	// ChunksReleased counts the swept chunks released to the network, whose locations stay until
	// no repository in it holds them; LocationsDropped, the released chunks of the network's whose
	// locations the pass dropped (their bytes in ChunkBytesSwept).
	ChunksReleased, LocationsDropped int64
	// Compaction: the packs it wrote and retired, the source bytes it read, and the pack
	// bytes it reclaimed (the retired packs' sizes less the written packs').
	PacksWritten, PacksRetired int
	BytesRead, BytesReclaimed  int64
	PacksDeleted, OrphanPacks  int
	// Segments merged away past their grace, and segments no row records, deleted.
	SegmentsDeleted, OrphanSegments int
	// Segments is what the pass moved from the tail into segments, however little was waiting
	// (nil when the repository's compaction was already running).
	Segments *SegmentReport
	// Dictionaries: one the pass trained (its frame ID, and the files it learned from), the one
	// compaction compressed with, and the small chunks it compressed and the stored bytes saved.
	DictionaryTrained                     uint32
	DictionarySamples                     int
	Dictionary                            uint32
	ChunksRecompressed, BytesRecompressed int64
	// IndexGeneration is the content index generation the pass wrote, of IndexEntries, and
	// IndexFolded the pinned entries and moves it folded into an index segment; NetworkIndex*
	// likewise of the repository's network's chunk index.
	IndexGeneration                         int64
	IndexEntries, IndexFolded               int
	NetworkIndexGeneration                  int64
	NetworkIndexEntries, NetworkIndexFolded int
}

// GC runs one garbage-collection pass over a repository, as of now
// (docs/scm.md, "Garbage collection and compaction"):
//
//  1. what no request can reach — the operation logs of no workspace, and the workspaces with
//     no log of their own — is deleted, every other operation log's unreachable operations and
//     views older than OperationRetention are collected, and the workspaces' checkpoints
//     thinned to what retention keeps;
//  2. the mark walks, from every commit a retained operation or view references, every
//     pending candidate, and every kept checkpoint's tree, the unpinned objects not touched
//     within sweepGraceDays;
//  3. the sweep deletes the unpinned objects not touched within sweepGraceDays that the
//     mark did not reach — commits, then trees and the other objects, then files, then
//     chunks — each DELETE checking pinned and touched_at again under the row's lock;
//  4. compaction merges small packs and rewrites sparse ones, retiring what it replaced;
//  5. packs retired longer than RetiredPackGrace, and orphan packs older than
//     OrphanPackGrace, are deleted from S3.
//
// A pass is safe beside publishes, uploads, landings, and other passes: nothing reachable
// or just touched is ever deleted (see the design's argument). Freshness days are the
// database's current day, which is UTC; now is UTC.
func (s *Store) GC(ctx context.Context, repo int64, now time.Time, opts GCOptions) (*GCReport, error) {
	if err := s.repositoryExists(ctx, s.ops(ctx), repo); err != nil {
		return nil, err
	}
	now = now.UTC()
	r := &GCReport{Repository: repo, DryRun: opts.DryRun, Started: now}
	if err := s.collectRepository(ctx, repo, now.Add(-OperationRetention), opts, r); err != nil {
		return r, fmt.Errorf("collecting operations: %w", err)
	}
	thinned, err := s.thinCheckpoints(ctx, repo, now, opts.DryRun)
	if err != nil {
		return r, err
	}
	r.CheckpointsDeleted = thinned
	cutoff := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -sweepGraceDays)
	if err := s.markAndSweep(ctx, repo, cutoff, opts, r); err != nil {
		return r, fmt.Errorf("mark and sweep: %w", err)
	}
	net, err := s.network(ctx, repo)
	if err != nil {
		return r, err
	}
	dropped, bytes, err := s.collectReleases(ctx, net, opts.DryRun)
	if err != nil {
		return r, fmt.Errorf("collecting released chunks: %w", err)
	}
	r.LocationsDropped, r.ChunkBytesSwept = dropped, r.ChunkBytesSwept+bytes
	if !opts.DryRun {
		if r.Segments, err = s.compactSegmentsNow(ctx, repo); err != nil {
			return r, fmt.Errorf("compacting the tail into segments: %w", err)
		}
	}
	if !opts.DryRun {
		if r.DictionaryTrained, r.DictionarySamples, err = s.trainDictionary(ctx, repo, now); err != nil {
			return r, fmt.Errorf("training a dictionary: %w", err)
		}
	}
	if err := s.compact(ctx, repo, opts, r); err != nil {
		return r, fmt.Errorf("compaction: %w", err)
	}
	if err := s.deletePacks(ctx, repo, now, opts, r); err != nil {
		return r, fmt.Errorf("deleting packs: %w", err)
	}
	if r.SegmentsDeleted, r.OrphanSegments, err = s.deleteSegments(ctx, repo, now, opts.DryRun); err != nil {
		return r, fmt.Errorf("deleting segments: %w", err)
	}
	if !opts.DryRun {
		if r.IndexGeneration, r.IndexEntries, r.IndexFolded, err = s.RebuildContentIndex(ctx, repo); err != nil {
			return r, fmt.Errorf("rebuilding the content index: %w", err)
		}
		_, netScope, err := s.networkOf(ctx, repo)
		if err != nil {
			return r, err
		}
		if r.NetworkIndexGeneration, r.NetworkIndexEntries, r.NetworkIndexFolded, err = s.foldIndex(ctx, netScope); err != nil {
			return r, fmt.Errorf("rebuilding the network's chunk index: %w", err)
		}
	}
	r.Finished = time.Now().UTC()
	return r, nil
}

// unreachable are the rows no request can reach, from before each workspace had its own
// operation log (docs/scm.md, "Decisions", 3): the operation logs of no workspace, and
// the workspaces with no log of their own. A workspace and its log are made in one
// transaction, so a workspace being made is never among them.
var unreachable = []struct {
	what   string
	delete func(backend.Ops, context.Context, int64, bool) (int64, error)
}{
	{"operation logs of no workspace", backend.Ops.DeleteOrphanOpLogs},
	{"workspaces with no operation log", backend.Ops.DeleteOrphanWorkspaces},
}

// collectRepository collects every operation log of a repository, first deleting what no request
// can reach (unreachable).
func (s *Store) collectRepository(ctx context.Context, repo int64, keepNewer time.Time, opts GCOptions, r *GCReport) error {
	for i, u := range unreachable {
		n := []*int64{&r.OpLogsDeleted, &r.WorkspacesDeleted}[i]
		got, err := u.delete(s.ops(ctx), ctx, repo, opts.DryRun)
		if err != nil {
			return fmt.Errorf("deleting the %s: %w", u.what, err)
		}
		*n = got
	}
	logs, err := s.ops(ctx).WorkspaceOpLogs(ctx, repo)
	if err != nil {
		return err
	}
	for _, log := range logs {
		c, err := s.collectLog(ctx, repo, log, nil, keepNewer, opts.DryRun)
		if err != nil {
			return fmt.Errorf("operation log %d: %w", log, err)
		}
		r.OperationsDeleted += c.Operations
		r.ViewsDeleted += c.Views
	}
	return nil
}

// sweepTables are the tables the sweep deletes from, in the order it deletes: whatever
// references an object goes before the object, so a row a reader finds rarely points at
// one already gone.
var sweepTables = []table{tCommit, tObject, tFile, tChunk}

// candidates lists a table's unpinned rows not touched since cutoff: what the sweep may
// delete, if the mark does not reach it.
func (s *Store) candidates(ctx context.Context, repo int64, t table, cutoff time.Time) ([]encoding.ID, error) {
	switch t {
	case tFile, tChunk:
		return s.staleContent(ctx, repo, t, cutoff)
	case tCommit:
		return s.ops(ctx).StaleCommitIDs(ctx, repo, cutoff)
	case tObject:
		return s.ops(ctx).StaleObjectIDs(ctx, repo, cutoff)
	}
	return nil, fmt.Errorf("table %d is not a table of repository objects", t)
}

// sweepRows deletes — or, dry, counts — the commits or objects among batch still unpinned and
// stale.
func sweepRows(ctx context.Context, q backend.Ops, repo int64, t table, batch []encoding.ID, cutoff time.Time, dry bool) (int64, error) {
	switch t {
	case tCommit:
		return q.SweepCommits(ctx, repo, batch, cutoff, dry)
	case tObject:
		return q.SweepObjects(ctx, repo, batch, cutoff, dry)
	}
	return 0, fmt.Errorf("table %d is not a table of repository objects", t)
}

// staleContent lists the files or chunks the content index has unpinned and untouched since
// cutoff, in ID order.
func (s *Store) staleContent(ctx context.Context, repo int64, t table, cutoff time.Time) ([]encoding.ID, error) {
	li := s.live(repoScope(repo))
	if err := s.refreshIndex(ctx, repoScope(repo), li); err != nil {
		return nil, err
	}
	kind := encoding.KindChunk
	if t == tFile {
		kind = encoding.KindFile
	}
	day := contentindex.DayOf(cutoff)
	var out []encoding.ID
	li.x.Load().Range(func(id encoding.ID, e contentindex.Entry) bool {
		if e.Kind == kind && !e.Pinned && e.Day < day {
			out = append(out, id)
		}
		return true
	})
	sortIDs(out)
	return out, nil
}

// markAndSweep marks the stale objects still reachable and sweeps the rest.
func (s *Store) markAndSweep(ctx context.Context, repo int64, cutoff time.Time, opts GCOptions, r *GCReport) error {
	cands := map[table][]encoding.ID{}
	found := false
	for _, t := range sweepTables {
		c, err := s.candidates(ctx, repo, t, cutoff)
		if err != nil {
			return err
		}
		cands[t] = c
		found = found || len(c) > 0
	}
	if !found {
		return nil
	}
	marked, err := s.mark(ctx, repo, cutoff)
	if err != nil {
		return err
	}
	r.Marked = len(marked)
	for _, t := range sweepTables {
		var doomed []encoding.ID
		for _, id := range cands[t] {
			if !marked[ref{table: t, id: id}] {
				doomed = append(doomed, id)
			}
		}
		for start := 0; start < len(doomed); start += gcBatch {
			batch := doomed[start:min(start+gcBatch, len(doomed))]
			n, bytes, released, err := s.sweepBatch(ctx, repo, t, batch, cutoff, opts.DryRun)
			if err != nil {
				return err
			}
			switch t {
			case tCommit:
				r.CommitsSwept += n
			case tObject:
				r.ObjectsSwept += n
			case tFile:
				r.FilesSwept += n
			case tChunk:
				r.ChunksSwept += n
				r.ChunkBytesSwept += bytes
				r.ChunksReleased += released
			}
		}
	}
	return nil
}

// sweepBatch deletes a batch of one table's unmarked candidates, each only if it is still
// unpinned and stale under its row lock: a publish that touched it, or an upload or a
// landing that pinned it, since the candidates were listed keeps it. A swept chunk's
// stored bytes come off its pack's live bytes, which compaction reads. It returns the rows
// deleted, and for chunks their stored bytes.
func (s *Store) sweepBatch(ctx context.Context, repo int64, t table, batch []encoding.ID, cutoff time.Time, dryRun bool) (n, bytes, released int64, err error) {
	if dryRun {
		if t == tFile || t == tChunk {
			_, x, err := s.contentIndex(ctx, repoScope(repo))
			if err != nil {
				return 0, 0, 0, err
			}
			_, netScope, err := s.networkOf(ctx, repo)
			if err != nil {
				return 0, 0, 0, err
			}
			_, locs, err := s.contentIndex(ctx, netScope)
			if err != nil {
				return 0, 0, 0, err
			}
			day := contentindex.DayOf(cutoff)
			for _, id := range batch {
				if e, ok := x.Get(id); ok && !e.Pinned && e.Day < day {
					n++
					if l, ok := locs.Get(id); ok && t == tChunk {
						bytes += int64(l.Loc.Stored)
					}
				}
			}
			return n, bytes, 0, nil
		}
		n, err = sweepRows(ctx, s.ops(ctx), repo, t, batch, cutoff, true)
		return n, 0, 0, err
	}
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		n, bytes, released = 0, 0, 0
		if t == tFile || t == tChunk {
			return s.sweepContent(ctx, tx, repo, t, batch, cutoff, &n, &bytes, &released)
		}
		var err error
		n, err = sweepRows(ctx, tx, repo, t, batch, cutoff, false)
		return err
	})
	return n, bytes, released, err
}

// sweepContent drops from the content index the files or chunks among batch still unpinned and
// untouched since cutoff, deciding under the index's row lock, so no transaction touches,
// pins, or relies on one meanwhile. A chunk's location, under its network's chunk index's lock
// too, goes with it in a network of one repository; in a network of more, another may hold the
// chunk, so it is released, and the network's collection drops its location once none does
// (collectReleases). It adds to n what it drops, to bytes the stored bytes of the locations it
// drops, and to released what it releases.
func (s *Store) sweepContent(ctx context.Context, tx *txn, repo int64, t table, batch []encoding.ID, cutoff time.Time, n, bytes, released *int64) error {
	doomed, err := s.staleInIndex(ctx, tx, repo, t, batch, cutoff)
	if err != nil || len(doomed) == 0 {
		return err
	}
	kind := encoding.KindChunk
	if t == tFile {
		kind = encoding.KindFile
	}
	var net backend.Network
	var netScope backend.Scope
	shared := false
	if t == tChunk {
		if net, netScope, err = s.networkOf(ctx, repo); err != nil {
			return err
		}
		// A fork made after this read holds none of these unpinned chunks: forks inherit only what
		// is pinned. One made before is counted.
		if _, err := tx.LockContentIndex(ctx, netScope); err != nil {
			return err
		}
		repos, err := tx.NetworkRepositories(ctx, net)
		if err != nil {
			return err
		}
		shared = len(repos) > 1
	}
	var release []encoding.ID
	// In a network of one, no other repository holds the chunks: their locations go, wherever
	// they are — pinned, in the chunk index's segments, when a repository deleted since pinned them.
	located := map[encoding.ID]contentindex.Entry{}
	if t == tChunk && !shared {
		if _, located, err = s.contentEntries(ctx, netScope, encoding.KindChunk, doomed); err != nil {
			return err
		}
	}
	for _, id := range doomed {
		s.logIndex(tx, repoScope(repo), contentindex.Update{Op: contentindex.Drop, ID: id, Entry: contentindex.Entry{Kind: kind}})
		*n++
		if t != tChunk {
			continue
		}
		if shared {
			release = append(release, id)
			continue
		}
		if l, ok := located[id]; ok {
			*bytes += int64(l.Loc.Stored)
			s.logIndex(tx, netScope, contentindex.Update{Op: contentindex.Drop, ID: id, Entry: contentindex.Entry{Kind: kind}})
		}
	}
	*released += int64(len(release))
	if len(release) == 0 {
		return nil
	}
	return tx.ReleaseChunks(ctx, net, release)
}

// collectReleases drops the locations of a network's released chunks that no repository in it
// holds — in its tail, or in a segment of its manifest, own or inherited — and forgets the
// releases, a batch at a time. Each batch decides under the network's chunk-index lock taken
// exclusively, which a publish relying on a location takes in share mode before it records what it
// holds: a publish that committed first is seen holding the chunk, and one after finds the location
// gone and records its own. A location goes wherever it is: one pinned, in the chunk index's
// segments, was pinned by a repository deleted since, and its drop folds into a tombstone. It
// returns how many locations it dropped and their stored bytes.
func (s *Store) collectReleases(ctx context.Context, net backend.Network, dryRun bool) (dropped, bytes int64, err error) {
	netScope := backend.NetworkScope(net)
	for {
		var batch []encoding.ID
		var n, b int64
		err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			n, b = 0, 0
			if _, err := tx.LockContentIndex(ctx, netScope); err != nil {
				return err
			}
			var err error
			if batch, err = tx.ReleasedChunks(ctx, net, gcBatch); err != nil || len(batch) == 0 {
				return err
			}
			repos, err := tx.NetworkRepositories(ctx, net)
			if err != nil {
				return err
			}
			held := map[encoding.ID]bool{}
			for _, repo := range repos {
				_, entries, err := s.contentEntries(ctx, repoScope(repo), encoding.KindChunk, batch)
				if err != nil {
					return err
				}
				for id := range entries {
					held[id] = true
				}
			}
			var unheld []encoding.ID
			for _, id := range batch {
				if !held[id] {
					unheld = append(unheld, id)
				}
			}
			_, locs, err := s.contentEntries(ctx, netScope, encoding.KindChunk, unheld)
			if err != nil {
				return err
			}
			for _, id := range unheld {
				if l, ok := locs[id]; ok {
					n++
					b += int64(l.Loc.Stored)
					if !dryRun {
						s.logIndex(tx, netScope, contentindex.Update{Op: contentindex.Drop, ID: id, Entry: contentindex.Entry{Kind: encoding.KindChunk}})
					}
				}
			}
			if dryRun {
				return nil
			}
			return tx.DeleteReleases(ctx, net, batch)
		})
		dropped, bytes = dropped+n, bytes+b
		if err != nil || dryRun || len(batch) < gcBatch {
			return dropped, bytes, err
		}
	}
}

// staleInIndex takes the content index's row lock in tx and returns the IDs among batch whose
// entries are of t's kind, unpinned, and untouched since cutoff.
func (s *Store) staleInIndex(ctx context.Context, tx *txn, repo int64, t table, batch []encoding.ID, cutoff time.Time) ([]encoding.ID, error) {
	if _, err := tx.LockContentIndex(ctx, repoScope(repo)); err != nil {
		return nil, err
	}
	li := s.live(repoScope(repo))
	if err := s.refreshIndex(ctx, repoScope(repo), li); err != nil {
		return nil, err
	}
	x := li.x.Load()
	kind := encoding.KindChunk
	if t == tFile {
		kind = encoding.KindFile
	}
	day := contentindex.DayOf(cutoff)
	var out []encoding.ID
	for _, id := range batch {
		if e, ok := x.Get(id); ok && e.Kind == kind && !e.Pinned && e.Day < day {
			out = append(out, id)
		}
	}
	return out, nil
}

// mark returns the unpinned objects not touched since cutoff that are reachable from the
// roots: every commit a view or operation of the repository's operation logs references
// (a view's heads, working-copy commits, and ref targets; an operation's commit
// predecessors), every pending candidate, and every checkpoint's tree. The walk stops at pinned objects, which are
// never swept and whose subgraphs are pinned, and at objects touched since cutoff, whose
// subgraphs were touched no earlier (a touch recurses into what it references).
func (s *Store) mark(ctx context.Context, repo int64, cutoff time.Time) (map[ref]bool, error) {
	roots := map[encoding.ID]bool{}
	for _, q := range []struct {
		kind encoding.Kind
	}{
		{encoding.KindView},
		{encoding.KindOperation},
	} {
		rows, err := s.repositoryOps(ctx, repo, q.kind)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			o, err := decodeStored(q.kind, row.ID, row.Body)
			if err != nil {
				return nil, err
			}
			switch o := o.(type) {
			case *encoding.View:
				for _, c := range viewCommits(o) {
					roots[c] = true
				}
			case *encoding.Operation:
				for c, preds := range o.CommitPredecessors {
					roots[c] = true
					for _, p := range preds {
						roots[p] = true
					}
				}
			}
		}
	}
	cands, err := s.ops(ctx).Candidates(ctx, repo)
	if err != nil {
		return nil, err
	}
	for _, c := range cands {
		roots[c.Commit] = true
	}
	delete(roots, encoding.RootCommitID)

	marked := map[ref]bool{}
	// Commits: their stale, unpinned ancestry from the roots, in one recursive query per
	// batch of roots.
	rootIDs := make([]encoding.ID, 0, len(roots))
	for id := range roots {
		rootIDs = append(rootIDs, id)
	}
	sortIDs(rootIDs)
	var frontier []ref
	for start := 0; start < len(rootIDs); start += gcBatch {
		batch := rootIDs[start:min(start+gcBatch, len(rootIDs))]
		reached, err := s.ops(ctx).StaleAncestry(ctx, repo, batch, cutoff)
		if err != nil {
			return nil, err
		}
		ids := make([]encoding.ID, 0, len(reached))
		for id := range reached {
			ids = append(ids, id)
		}
		sortIDs(ids)
		for _, id := range ids {
			r := ref{table: tCommit, id: id}
			if marked[r] {
				continue
			}
			marked[r] = true
			o, err := decodeStored(encoding.KindCommit, id, reached[id])
			if err != nil {
				return nil, err
			}
			for _, t := range o.(*encoding.Commit).RootTree {
				if t != encoding.EmptyTreeID {
					frontier = append(frontier, ref{table: tObject, id: t})
				}
			}
		}
	}

	checkpointTrees, err := s.checkpointTrees(ctx, repo)
	if err != nil {
		return nil, err
	}
	for _, t := range checkpointTrees {
		frontier = append(frontier, ref{table: tObject, id: t})
	}

	// Trees and the other objects, files, and chunks, breadth first. An object is recorded
	// under kind 0: the object table's IDs are unique across kinds.
	visited := map[ref]bool{}
	for len(frontier) > 0 {
		byTable := map[table][]encoding.ID{}
		for _, r := range frontier {
			r.kind = 0
			if visited[r] {
				continue
			}
			visited[r] = true
			byTable[r.table] = append(byTable[r.table], r.id)
		}
		frontier = nil
		for _, t := range []table{tObject, tFile, tChunk} {
			members := byTable[t]
			sortIDs(members)
			for start := 0; start < len(members); start += gcBatch {
				batch := members[start:min(start+gcBatch, len(members))]
				next, err := s.markBatch(ctx, repo, t, batch, cutoff, marked)
				if err != nil {
					return nil, err
				}
				frontier = append(frontier, next...)
			}
		}
	}
	return marked, nil
}

// markBatch marks the stale, unpinned rows among a batch of one table's IDs and returns
// what they reference.
func (s *Store) markBatch(ctx context.Context, repo int64, t table, batch []encoding.ID, cutoff time.Time, marked map[ref]bool) ([]ref, error) {
	var next []ref
	switch t {
	case tObject:
		objs, err := s.ops(ctx).StaleObjects(ctx, repo, batch, cutoff)
		if err != nil {
			return nil, err
		}
		for _, row := range objs {
			marked[ref{table: tObject, id: row.ID}] = true
			o, err := decodeStored(row.Kind, row.ID, row.Body)
			if err != nil {
				return nil, err
			}
			rs := refSet{}
			gcRefs(rs, o)
			next = append(next, rs.sorted()...)
		}
		return next, nil
	case tFile, tChunk:
		kind := encoding.KindChunk
		if t == tFile {
			kind = encoding.KindFile
		}
		_, x, err := s.contentIndex(ctx, repoScope(repo))
		if err != nil {
			return nil, err
		}
		day := contentindex.DayOf(cutoff)
		for _, id := range batch {
			e, ok := x.Get(id)
			if !ok || e.Kind != kind || e.Pinned || e.Day >= day {
				continue
			}
			marked[ref{table: t, id: id}] = true
			for _, c := range e.Chunks {
				next = append(next, ref{table: tChunk, id: c.ID})
			}
		}
		return next, nil
	}
	return nil, fmt.Errorf("mark: %v is not a table the mark walks", t)
}

// deletePacks deletes a repository's network's retired packs whose grace has passed, and its
// orphan packs (S3 objects no pack row names) older than OrphanPackGrace.
func (s *Store) deletePacks(ctx context.Context, repo int64, now time.Time, opts GCOptions, r *GCReport) error {
	net, err := s.network(ctx, repo)
	if err != nil {
		return err
	}
	n, err := s.deleteRetiredPacks(ctx, &net, now, opts.DryRun)
	if err != nil {
		return err
	}
	r.PacksDeleted = n
	objs, err := s.blobs.List(ctx, packPrefix(net))
	if err != nil {
		return err
	}
	packIDs, err := s.ops(ctx).PackIDs(ctx, net)
	if err != nil {
		return err
	}
	known := make(map[string]bool, len(packIDs))
	for _, id := range packIDs {
		known[packKey(net, id)] = true
	}
	// Every pack is unique (it carries a random nonce), so a key with no row is never
	// written again: an orphan is safe to delete once no upload can still be committing it.
	for _, o := range objs {
		if known[o.Key] || !o.Modified.Before(now.Add(-OrphanPackGrace)) {
			continue
		}
		r.OrphanPacks++
		if opts.DryRun {
			continue
		}
		if err := s.blobs.Delete(ctx, o.Key); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRetiredPacks deletes, in every network, the packs compaction retired more than
// RetiredPackGrace before now: the S3 object, then the row. It returns how many.
func (s *Store) DeleteRetiredPacks(ctx context.Context, now time.Time) (int, error) {
	return s.deleteRetiredPacks(ctx, nil, now, false)
}

// deleteRetiredPacks deletes retired packs past their grace, in one repository or (repo
// nil) all of them. A retired pack no chunk row points at is never pointed at again —
// nothing moves chunks into a retired pack, and every pack written is unique — so its S3
// object goes first and its row after; an interrupted deletion finishes next time.
func (s *Store) deleteRetiredPacks(ctx context.Context, net *backend.Network, now time.Time, dryRun bool) (int, error) {
	rows, err := s.ops(ctx).RetiredPacksBefore(ctx, now.Add(-RetiredPackGrace), net)
	if err != nil {
		return 0, err
	}
	// A retired pack no location in its network's chunk index names — retirement ensured none,
	// and nothing moves a chunk into one.
	used := map[backend.Network]map[uint64]bool{}
	var packs []backend.RetiredPack
	for _, p := range rows {
		u, ok := used[p.Net]
		if !ok {
			v, err := s.contentView(ctx, backend.NetworkScope(p.Net))
			if err != nil {
				return 0, err
			}
			u = map[uint64]bool{}
			err = v.each(ctx, func(_ encoding.ID, e contentindex.Entry) bool {
				if e.Kind == encoding.KindChunk {
					u[e.Loc.Pack] = true
				}
				return true
			})
			if err != nil {
				return 0, err
			}
			used[p.Net] = u
		}
		if !u[p.Num] {
			packs = append(packs, p)
		}
	}
	if dryRun {
		return len(packs), nil
	}
	n := 0
	for _, p := range packs {
		if err := s.blobs.Delete(ctx, packKey(p.Net, p.ID)); err != nil && !errors.Is(err, blob.ErrNotFound) {
			return n, err
		}
		var deleted bool
		if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			var err error
			deleted, err = tx.DeleteRetiredPack(ctx, p.Net, p.ID)
			return err
		}); err != nil {
			return n, err
		}
		if deleted {
			n++
		}
	}
	return n, nil
}

// GCRetry is how long after a failed pass a repository is tried again.
const GCRetry = time.Hour

// GCWork is a repository's garbage collection, as keyed work (internal/work): its daily pass, and
// between passes its retired packs past their grace, done by one replica at a time. Passes are
// safe to overlap; one at a time keeps them from doing the same work twice.
const GCWork = "gc"

// PurgeWork is an expired repository's removal (server.Purge), keyed by its ID.
const PurgeWork = "purge"

// Work is the SCM's keyed work's store (internal/work).
func (s *Store) Work() work.Store { return workStore{s.b} }

type workStore struct{ b backend.Backend }

func (w workStore) Mark(ctx context.Context, kind string, key int64) error {
	return w.b.MarkWork(ctx, kind, key)
}
func (w workStore) MarkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return w.b.MarkWorkAt(ctx, kind, key, at)
}
func (w workStore) Take(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	return w.b.TakeWork(ctx, kind, key, holder, ttl)
}
func (w workStore) Renew(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	return w.b.RenewWork(ctx, kind, key, holder, ttl)
}
func (w workStore) Release(ctx context.Context, kind string, key int64, holder string) (bool, error) {
	return w.b.ReleaseWork(ctx, kind, key, holder)
}
func (w workStore) Due(ctx context.Context, kind string, limit int) ([]int64, error) {
	return w.b.DueWork(ctx, kind, limit)
}

// gcSchedule is when the latest daily pass was scheduled, at now, when passes are scheduled at
// hour (UTC), and the last failure a repository may have had and be due.
func gcSchedule(now time.Time, hour int) (scheduled, retry time.Time) {
	now = now.UTC()
	scheduled = time.Date(now.Year(), now.Month(), now.Day(), hour, 0, 0, 0, time.UTC)
	if now.Before(scheduled) {
		scheduled = scheduled.AddDate(0, 0, -1)
	}
	return scheduled, now.Add(-GCRetry)
}

// DueForGC lists the repositories due a pass at now, when passes are scheduled daily at
// hour (UTC): those whose last successful pass began before the latest scheduled time, and
// whose last failure (if any) is older than GCRetry.
func (s *Store) DueForGC(ctx context.Context, now time.Time, hour int) ([]int64, error) {
	scheduled, retry := gcSchedule(now, hour)
	return s.ops(ctx).DueForGC(ctx, scheduled, retry)
}

// GCDue reports whether a repository is due a pass (DueForGC).
func (s *Store) GCDue(ctx context.Context, repo int64, now time.Time, hour int) (bool, error) {
	scheduled, retry := gcSchedule(now, hour)
	return s.ops(ctx).RepositoryDueForGC(ctx, repo, scheduled, retry)
}

// Retiring lists the repositories with segments retired more than RetiredSegmentGrace before now,
// and the first repository of each network with packs retired more than RetiredPackGrace before
// or segments RetiredSegmentGrace before: what DeleteRetiredOf deletes.
func (s *Store) Retiring(ctx context.Context, now time.Time) ([]int64, error) {
	return s.ops(ctx).RepositoriesRetiringBefore(ctx, now.Add(-RetiredPackGrace), now.Add(-RetiredSegmentGrace))
}

// DeleteRetiredOf deletes a repository's network's packs compaction retired more than
// RetiredPackGrace before now, as DeleteRetiredPacks does every network's, and the repository's and
// its network's segments merged away more than RetiredSegmentGrace before; it returns how many of
// each.
func (s *Store) DeleteRetiredOf(ctx context.Context, repo int64, now time.Time) (packs, segments int, err error) {
	net, err := s.network(ctx, repo)
	if errors.Is(err, ErrNotFound) {
		return 0, 0, nil // no repository, nothing of its retired
	}
	if err != nil {
		return 0, 0, err
	}
	if packs, err = s.deleteRetiredPacks(ctx, &net, now, false); err != nil {
		return packs, 0, err
	}
	segments, _, err = s.deleteSegments(ctx, repo, now, false)
	return packs, segments, err
}

// RecordGC records a pass, with the error that ended it if it failed. A dry run is not
// recorded.
func (s *Store) RecordGC(ctx context.Context, r *GCReport, passErr error) error {
	if r == nil || r.DryRun {
		return nil
	}
	finished := r.Finished
	if finished.IsZero() {
		finished = time.Now().UTC()
	}
	var msg *string
	if passErr != nil {
		m := passErr.Error()
		msg = &m
	}
	pass := gcPassOf(r, finished, msg)
	return s.inTx(ctx, func(ctx context.Context, tx *txn) error { return tx.InsertGCPass(ctx, pass) })
}

// gcPassOf is a pass's record.
func gcPassOf(r *GCReport, finished time.Time, msg *string) backend.GCPass {
	return backend.GCPass{
		Repository: r.Repository, Started: r.Started, Finished: finished,
		OperationsDeleted: r.OperationsDeleted, ViewsDeleted: r.ViewsDeleted, Marked: r.Marked,
		CommitsSwept: r.CommitsSwept, ObjectsSwept: r.ObjectsSwept, FilesSwept: r.FilesSwept, ChunksSwept: r.ChunksSwept,
		ChunkBytesSwept: r.ChunkBytesSwept, PacksWritten: r.PacksWritten, PacksRetired: r.PacksRetired,
		BytesRead: r.BytesRead, BytesReclaimed: r.BytesReclaimed, PacksDeleted: r.PacksDeleted, OrphanPacks: r.OrphanPacks,
		Error: msg, CheckpointsDeleted: r.CheckpointsDeleted, OpLogsDeleted: r.OpLogsDeleted, WorkspacesDeleted: r.WorkspacesDeleted,
	}
}

// String renders a report for the gc command and the log.
func (r *GCReport) String() string {
	dry := ""
	if r.DryRun {
		dry = " (dry run: nothing was changed)"
	}
	dict := ""
	if r.DictionaryTrained != 0 {
		dict += fmt.Sprintf("; trained dictionary %d on %d files", r.DictionaryTrained, r.DictionarySamples)
	}
	if r.ChunksRecompressed > 0 {
		dict += fmt.Sprintf("; compressed %d small chunks with dictionary %d, saving %d bytes", r.ChunksRecompressed, r.Dictionary, r.BytesRecompressed)
	}
	if r.IndexGeneration != 0 {
		dict += fmt.Sprintf("; wrote content index generation %d of %d entries, folding %d into a segment", r.IndexGeneration, r.IndexEntries, r.IndexFolded)
	}
	if r.ChunksReleased > 0 || r.LocationsDropped > 0 {
		dict += fmt.Sprintf("; released %d chunks to the network, and dropped the locations of %d no repository holds",
			r.ChunksReleased, r.LocationsDropped)
	}
	if r.NetworkIndexGeneration != 0 {
		dict += fmt.Sprintf("; wrote its network's chunk index generation %d of %d entries, folding %d into a segment",
			r.NetworkIndexGeneration, r.NetworkIndexEntries, r.NetworkIndexFolded)
	}
	return fmt.Sprintf("repository %d%s: deleted %d operation logs of no workspace and %d workspaces with no log; collected %d operations and %d views; thinned %d checkpoints; kept %d stale reachable objects; "+
		"swept %d commits, %d objects, %d files, %d chunks (%d bytes); "+
		"compaction wrote %d packs and retired %d, reading %d bytes and reclaiming %d; deleted %d retired and %d orphan packs, %d retired and %d orphan segments%s",
		r.Repository, dry, r.OpLogsDeleted, r.WorkspacesDeleted, r.OperationsDeleted, r.ViewsDeleted, r.CheckpointsDeleted, r.Marked,
		r.CommitsSwept, r.ObjectsSwept, r.FilesSwept, r.ChunksSwept, r.ChunkBytesSwept,
		r.PacksWritten, r.PacksRetired, r.BytesRead, r.BytesReclaimed, r.PacksDeleted, r.OrphanPacks, r.SegmentsDeleted, r.OrphanSegments, dict)
}
