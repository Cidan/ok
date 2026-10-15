package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
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
	// Rows swept, by table, and the stored bytes of the swept chunks.
	CommitsSwept, ObjectsSwept, FilesSwept, ChunksSwept int64
	ChunkBytesSwept                                     int64
	// Compaction: the packs it wrote and retired, the source bytes it read, and the pack
	// bytes it reclaimed (the retired packs' sizes less the written packs').
	PacksWritten, PacksRetired int
	BytesRead, BytesReclaimed  int64
	PacksDeleted, OrphanPacks  int
	// Dictionaries: one the pass trained (its frame ID, and the files it learned from), the one
	// compaction compressed with, and the small chunks it compressed and the stored bytes saved.
	DictionaryTrained                     uint32
	DictionarySamples                     int
	Dictionary                            uint32
	ChunksRecompressed, BytesRecompressed int64
	// IndexGeneration is the content index generation the pass wrote, of IndexEntries.
	IndexGeneration int64
	IndexEntries    int
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
// database's current_date, which on PostgreSQL and the test server is UTC; now is UTC.
func (s *Store) GC(ctx context.Context, repo int64, now time.Time, opts GCOptions) (*GCReport, error) {
	if err := s.repositoryExists(ctx, s.pool, repo); err != nil {
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
	if !opts.DryRun {
		if r.IndexGeneration, r.IndexEntries, err = s.RebuildContentIndex(ctx, repo); err != nil {
			return r, fmt.Errorf("rebuilding the content index: %w", err)
		}
	}
	r.Finished = time.Now().UTC()
	return r, nil
}

// unreachable are the rows no request can reach, from before each workspace had its own
// operation log (docs/scm.md, "Decisions", 3): the operation logs of no workspace, and
// the workspaces with no log of their own. A workspace and its log are made in one
// transaction, so a workspace being made is never among them.
var unreachable = []struct{ what, from, where string }{
	{"operation logs of no workspace", "op_log", `repository_id = $1 AND workspace = ''`},
	{"workspaces with no operation log", "workspace w", `w.repository_id = $1 AND NOT EXISTS (SELECT 1 FROM op_log l
		WHERE l.repository_id = w.repository_id AND l.principal = w.principal AND l.workspace = w.name)`},
}

// collectRepository collects every operation log of a repository, first deleting what no request
// can reach (unreachable).
func (s *Store) collectRepository(ctx context.Context, repo int64, keepNewer time.Time, opts GCOptions, r *GCReport) error {
	for i, u := range unreachable {
		n := []*int64{&r.OpLogsDeleted, &r.WorkspacesDeleted}[i]
		if opts.DryRun {
			if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+u.from+` WHERE `+u.where, repo).Scan(n); err != nil {
				return err
			}
			continue
		}
		tag, err := s.pool.Exec(ctx, `DELETE FROM `+u.from+` WHERE `+u.where, repo)
		if err != nil {
			return fmt.Errorf("deleting the %s: %w", u.what, err)
		}
		*n = tag.RowsAffected()
	}
	rows, err := s.pool.Query(ctx, `SELECT id FROM op_log WHERE repository_id = $1 AND workspace <> '' ORDER BY id`, repo)
	if err != nil {
		return err
	}
	logs, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return err
	}
	for _, log := range logs {
		c, err := s.collectLog(ctx, log, nil, keepNewer, opts.DryRun)
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

func tableName(t table) string {
	switch t {
	case tCommit:
		return "commit"
	case tObject:
		return "object"
	}
	panic(fmt.Sprintf("table %d is not a table of repository objects", t))
}

// candidates lists a table's unpinned rows not touched since cutoff: what the sweep may
// delete, if the mark does not reach it.
func (s *Store) candidates(ctx context.Context, repo int64, t table, cutoff time.Time) ([]encoding.ID, error) {
	if t == tFile || t == tChunk {
		return s.staleContent(ctx, repo, t, cutoff)
	}
	rows, err := s.pool.Query(ctx, `SELECT id FROM `+tableName(t)+` WHERE repository_id = $1 AND NOT pinned AND touched_at < $2 ORDER BY id`, repo, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []encoding.ID
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		id, err := toID(b)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// staleContent lists the files or chunks the content index has unpinned and untouched since
// cutoff, in ID order.
func (s *Store) staleContent(ctx context.Context, repo int64, t table, cutoff time.Time) ([]encoding.ID, error) {
	li := s.live(repo)
	if err := s.refreshIndex(ctx, repo, li); err != nil {
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
			n, bytes, err := s.sweepBatch(ctx, repo, t, batch, cutoff, opts.DryRun)
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
func (s *Store) sweepBatch(ctx context.Context, repo int64, t table, batch []encoding.ID, cutoff time.Time, dryRun bool) (int64, int64, error) {
	if dryRun {
		var n, bytes int64
		if t == tFile || t == tChunk {
			_, x, err := s.contentIndex(ctx, repo)
			if err != nil {
				return 0, 0, err
			}
			day := contentindex.DayOf(cutoff)
			for _, id := range batch {
				if e, ok := x.Get(id); ok && !e.Pinned && e.Day < day {
					n++
					bytes += int64(e.Loc.Stored)
				}
			}
			return n, bytes, nil
		}
		err := s.pool.QueryRow(ctx, `SELECT count(*), 0::bigint FROM `+tableName(t)+` WHERE repository_id = $1 AND id = ANY($2) AND NOT pinned AND touched_at < $3`,
			repo, ids(batch), cutoff).Scan(&n, &bytes)
		return n, bytes, err
	}
	var n, bytes int64
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		n, bytes = 0, 0
		if t == tFile || t == tChunk {
			return s.sweepContent(ctx, tx, repo, t, batch, cutoff, &n, &bytes)
		}
		tag, err := tx.Exec(ctx, `DELETE FROM `+tableName(t)+` WHERE repository_id = $1 AND id = ANY($2) AND NOT pinned AND touched_at < $3`,
			repo, ids(batch), cutoff)
		n = tag.RowsAffected()
		return err
	})
	return n, bytes, err
}

// sweepContent drops from the content index the files or chunks among batch still unpinned and
// untouched since cutoff, deciding under the index's row lock, so no transaction touches,
// pins, or relies on one meanwhile. It adds to n what it drops, and to bytes their chunks'
// stored bytes.
func (s *Store) sweepContent(ctx context.Context, tx pgx.Tx, repo int64, t table, batch []encoding.ID, cutoff time.Time, n, bytes *int64) error {
	doomed, err := s.staleInIndex(ctx, tx, repo, t, batch, cutoff)
	if err != nil || len(doomed) == 0 {
		return err
	}
	x := s.live(repo).x.Load()
	kind := encoding.KindChunk
	if t == tFile {
		kind = encoding.KindFile
	}
	for _, id := range doomed {
		if t == tChunk {
			e, _ := x.Get(id)
			*bytes += int64(e.Loc.Stored)
		}
		s.logIndex(tx, repo, contentindex.Update{Op: contentindex.Drop, ID: id, Entry: contentindex.Entry{Kind: kind}})
		*n++
	}
	return nil
}

// staleInIndex takes the content index's row lock in tx and returns the IDs among batch whose
// entries are of t's kind, unpinned, and untouched since cutoff.
func (s *Store) staleInIndex(ctx context.Context, tx pgx.Tx, repo int64, t table, batch []encoding.ID, cutoff time.Time) ([]encoding.ID, error) {
	if _, err := tx.Exec(ctx, `SELECT generation FROM content_index WHERE repository_id = $1 FOR UPDATE`, repo); err != nil {
		return nil, err
	}
	li := s.live(repo)
	if err := s.refreshIndex(ctx, repo, li); err != nil {
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
		kind  encoding.Kind
		query string
	}{
		{encoding.KindView, `SELECT v.id, v.body FROM view v JOIN op_log l ON l.id = v.op_log_id WHERE l.repository_id = $1`},
		{encoding.KindOperation, `SELECT o.id, o.body FROM operation o JOIN op_log l ON l.id = o.op_log_id WHERE l.repository_id = $1`},
	} {
		rows, err := s.pool.Query(ctx, q.query, repo)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var idb, body []byte
			if err := rows.Scan(&idb, &body); err != nil {
				rows.Close()
				return nil, err
			}
			id, err := toID(idb)
			if err != nil {
				rows.Close()
				return nil, err
			}
			o, err := decodeStored(q.kind, id, body)
			if err != nil {
				rows.Close()
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
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	crows, err := s.pool.Query(ctx, `SELECT commit_id FROM candidate WHERE repository_id = $1`, repo)
	if err != nil {
		return nil, err
	}
	cands, err := pgx.CollectRows(crows, pgx.RowTo[[]byte])
	if err != nil {
		return nil, err
	}
	for _, b := range cands {
		id, err := toID(b)
		if err != nil {
			return nil, err
		}
		roots[id] = true
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
		rows, err := s.pool.Query(ctx, `WITH RECURSIVE m(id) AS (
				SELECT c.id FROM commit c
				WHERE c.repository_id = $1 AND c.id = ANY($2) AND NOT c.pinned AND c.touched_at < $3
			UNION
				SELECT c.id FROM m
				JOIN commit_parent p ON p.repository_id = $1 AND p.commit_id = m.id
				JOIN commit c ON c.repository_id = $1 AND c.id = p.parent_id
				WHERE NOT c.pinned AND c.touched_at < $3
			)
			SELECT c.id, c.body FROM m JOIN commit c ON c.repository_id = $1 AND c.id = m.id`, repo, ids(batch), cutoff)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var idb, body []byte
			if err := rows.Scan(&idb, &body); err != nil {
				rows.Close()
				return nil, err
			}
			id, err := toID(idb)
			if err != nil {
				rows.Close()
				return nil, err
			}
			r := ref{table: tCommit, id: id}
			if marked[r] {
				continue
			}
			marked[r] = true
			o, err := decodeStored(encoding.KindCommit, id, body)
			if err != nil {
				rows.Close()
				return nil, err
			}
			for _, t := range o.(*encoding.Commit).RootTree {
				if t != encoding.EmptyTreeID {
					frontier = append(frontier, ref{table: tObject, id: t})
				}
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
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
		rows, err := s.pool.Query(ctx, `SELECT id, kind, body FROM object
			WHERE repository_id = $1 AND id = ANY($2) AND NOT pinned AND touched_at < $3`, repo, ids(batch), cutoff)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var idb, body []byte
			var kind int16
			if err := rows.Scan(&idb, &kind, &body); err != nil {
				return nil, err
			}
			id, err := toID(idb)
			if err != nil {
				return nil, err
			}
			marked[ref{table: tObject, id: id}] = true
			o, err := decodeStored(encoding.Kind(kind), id, body)
			if err != nil {
				return nil, err
			}
			rs := refSet{}
			gcRefs(rs, o)
			next = append(next, rs.sorted()...)
		}
		return next, rows.Err()
	case tFile, tChunk:
		kind := encoding.KindChunk
		if t == tFile {
			kind = encoding.KindFile
		}
		_, x, err := s.contentIndex(ctx, repo)
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

// deletePacks deletes a repository's retired packs whose grace has passed, and its
// orphan packs (S3 objects no pack row names) older than OrphanPackGrace.
func (s *Store) deletePacks(ctx context.Context, repo int64, now time.Time, opts GCOptions, r *GCReport) error {
	n, err := s.deleteRetiredPacks(ctx, &repo, now, opts.DryRun)
	if err != nil {
		return err
	}
	r.PacksDeleted = n
	objs, err := s.blobs.List(ctx, fmt.Sprintf("repos/%d/packs/", repo))
	if err != nil {
		return err
	}
	rows, err := s.pool.Query(ctx, `SELECT id FROM pack WHERE repository_id = $1`, repo)
	if err != nil {
		return err
	}
	packIDs, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		return err
	}
	known := make(map[string]bool, len(packIDs))
	for _, b := range packIDs {
		id, err := toID(b)
		if err != nil {
			return err
		}
		known[packKey(repo, id)] = true
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

// DeleteRetiredPacks deletes, in every repository, the packs compaction retired more than
// RetiredPackGrace before now: the S3 object, then the row. It returns how many.
func (s *Store) DeleteRetiredPacks(ctx context.Context, now time.Time) (int, error) {
	return s.deleteRetiredPacks(ctx, nil, now, false)
}

// deleteRetiredPacks deletes retired packs past their grace, in one repository or (repo
// nil) all of them. A retired pack no chunk row points at is never pointed at again —
// nothing moves chunks into a retired pack, and every pack written is unique — so its S3
// object goes first and its row after; an interrupted deletion finishes next time.
func (s *Store) deleteRetiredPacks(ctx context.Context, repo *int64, now time.Time, dryRun bool) (int, error) {
	rows, err := s.pool.Query(ctx, `SELECT p.repository_id, p.id, p.num FROM pack p
		WHERE p.retired_at < $1 AND ($2::bigint IS NULL OR p.repository_id = $2)
		ORDER BY p.repository_id, p.id`, now.Add(-RetiredPackGrace), repo)
	if err != nil {
		return 0, err
	}
	type retired struct {
		repo int64
		id   encoding.ID
		num  uint64
	}
	var candidates []retired
	for rows.Next() {
		var p retired
		var b []byte
		if err := rows.Scan(&p.repo, &b, &p.num); err != nil {
			rows.Close()
			return 0, err
		}
		if p.id, err = toID(b); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	// A retired pack no chunk in its repository's index is in — retirement ensured none, and
	// nothing moves a chunk into one.
	used := map[int64]map[uint64]bool{}
	var packs []retired
	for _, p := range candidates {
		u, ok := used[p.repo]
		if !ok {
			li := s.live(p.repo)
			if err := s.refreshIndex(ctx, p.repo, li); err != nil {
				return 0, err
			}
			u = map[uint64]bool{}
			li.x.Load().Range(func(_ encoding.ID, e contentindex.Entry) bool {
				if e.Kind == encoding.KindChunk {
					u[e.Loc.Pack] = true
				}
				return true
			})
			used[p.repo] = u
		}
		if !u[p.num] {
			packs = append(packs, p)
		}
	}
	if dryRun {
		return len(packs), nil
	}
	n := 0
	for _, p := range packs {
		if err := s.blobs.Delete(ctx, packKey(p.repo, p.id)); err != nil && !errors.Is(err, blob.ErrNotFound) {
			return n, err
		}
		tag, err := s.pool.Exec(ctx, `DELETE FROM pack WHERE repository_id = $1 AND id = $2 AND retired_at IS NOT NULL`, p.repo, p.id[:])
		if err != nil {
			return n, err
		}
		n += int(tag.RowsAffected())
	}
	return n, nil
}

// LeadGC runs fn holding the lock (internal/lock) whose holder runs garbage collection,
// with a context that ends if the lock is lost, and gives the lock up when fn returns. Passes
// are safe to overlap; one runner at a time keeps them from doing the same work twice. It
// returns lock.ErrNotAcquired when another replica holds the lock, and otherwise fn's error.
func (s *Store) LeadGC(ctx context.Context, fn lock.LockFn) error {
	return s.locks.LockFn(ctx, "leaders", "gc", 0, fn)
}

// GCRetry is how long after a failed pass a repository is tried again.
const GCRetry = time.Hour

// DueForGC lists the repositories due a pass at now, when passes are scheduled daily at
// hour (UTC): those whose last successful pass began before the latest scheduled time, and
// whose last failure (if any) is older than GCRetry.
func (s *Store) DueForGC(ctx context.Context, now time.Time, hour int) ([]int64, error) {
	now = now.UTC()
	scheduled := time.Date(now.Year(), now.Month(), now.Day(), hour, 0, 0, 0, time.UTC)
	if now.Before(scheduled) {
		scheduled = scheduled.AddDate(0, 0, -1)
	}
	rows, err := s.pool.Query(ctx, `SELECT r.id FROM repository r
		WHERE NOT EXISTS (SELECT 1 FROM gc_pass g WHERE g.repository_id = r.id AND g.started_at >= $1 AND g.error IS NULL)
		  AND NOT EXISTS (SELECT 1 FROM gc_pass g WHERE g.repository_id = r.id AND g.error IS NOT NULL AND g.started_at > $2)
		ORDER BY r.id`, scheduled, now.Add(-GCRetry))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
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
	_, err := s.pool.Exec(ctx, `INSERT INTO gc_pass (repository_id, started_at, finished_at, operations_deleted, views_deleted,
			marked, commits_swept, objects_swept, files_swept, chunks_swept, chunk_bytes_swept,
			packs_written, packs_retired, bytes_read, bytes_reclaimed, packs_deleted, orphan_packs, error, checkpoints_deleted, op_logs_deleted, workspaces_deleted)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
		ON CONFLICT DO NOTHING`,
		r.Repository, r.Started, finished, r.OperationsDeleted, r.ViewsDeleted,
		r.Marked, r.CommitsSwept, r.ObjectsSwept, r.FilesSwept, r.ChunksSwept, r.ChunkBytesSwept,
		r.PacksWritten, r.PacksRetired, r.BytesRead, r.BytesReclaimed, r.PacksDeleted, r.OrphanPacks, msg, r.CheckpointsDeleted, r.OpLogsDeleted, r.WorkspacesDeleted)
	return err
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
		dict += fmt.Sprintf("; wrote content index generation %d of %d entries", r.IndexGeneration, r.IndexEntries)
	}
	return fmt.Sprintf("repository %d%s: deleted %d operation logs of no workspace and %d workspaces with no log; collected %d operations and %d views; thinned %d checkpoints; kept %d stale reachable objects; "+
		"swept %d commits, %d objects, %d files, %d chunks (%d bytes); "+
		"compaction wrote %d packs and retired %d, reading %d bytes and reclaiming %d; deleted %d retired and %d orphan packs%s",
		r.Repository, dry, r.OpLogsDeleted, r.WorkspacesDeleted, r.OperationsDeleted, r.ViewsDeleted, r.CheckpointsDeleted, r.Marked,
		r.CommitsSwept, r.ObjectsSwept, r.FilesSwept, r.ChunksSwept, r.ChunkBytesSwept,
		r.PacksWritten, r.PacksRetired, r.BytesRead, r.BytesReclaimed, r.PacksDeleted, r.OrphanPacks, dict)
}
