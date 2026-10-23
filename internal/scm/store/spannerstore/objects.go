package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Objects and commits ───

func (x queries) InsertObjects(ctx context.Context, repo int64, objs []backend.Object) ([]encoding.ID, error) {
	if len(objs) == 0 {
		return nil, nil
	}
	seen := make(map[encoding.ID]bool, len(objs))
	var idCol [][]byte
	var kinds []int64
	var bs [][]byte
	for _, o := range objs {
		if seen[o.ID] {
			continue
		}
		seen[o.ID] = true
		idCol, kinds, bs = append(idCol, o.ID[:]), append(kinds, int64(o.Kind)), append(bs, o.Body)
	}
	return x.returningIDs(ctx, `INSERT OR IGNORE INTO object (repository_id, id, kind, body, pinned, touched_at)
		SELECT @repo, id, @kinds[OFFSET(i)], @bodies[OFFSET(i)], FALSE, CURRENT_DATE('UTC') FROM UNNEST(@ids) AS id WITH OFFSET i
		THEN RETURN id`, map[string]any{"repo": repo, "ids": idCol, "kinds": kinds, "bodies": bs}, bodySize(bs), "ids", "kinds", "bodies")
}

// idPair is two IDs, as a STRUCT parameter.
type idPair struct {
	A []byte `spanner:"a"`
	B []byte `spanner:"b"`
}

func pairsOf(as, bs []encoding.ID) []idPair {
	seen := make(map[[2]encoding.ID]bool, len(as))
	out := make([]idPair, 0, len(as))
	for i := range as {
		k := [2]encoding.ID{as[i], bs[i]}
		if !seen[k] {
			seen[k] = true
			out = append(out, idPair{as[i][:], bs[i][:]})
		}
	}
	return out
}

func (x queries) InsertCopyParents(ctx context.Context, repo int64, copies, parents []encoding.ID) error {
	if len(copies) == 0 {
		return nil
	}
	_, err := x.execIn(ctx, `INSERT OR IGNORE INTO copy_parent (repository_id, copy_id, parent_id)
		SELECT @repo, e.a, e.b FROM UNNEST(@edges) AS e`, map[string]any{"repo": repo, "edges": pairsOf(copies, parents)}, []string{"edges"}, nil)
	return err
}

func (x queries) InsertCommits(ctx context.Context, repo int64, commits []backend.Commit, writtenBy string) ([]encoding.ID, error) {
	if len(commits) == 0 {
		return nil, nil
	}
	seen := make(map[encoding.ID]bool, len(commits))
	var idCol, changes, bs [][]byte
	var gens []int64
	var conflicted []bool
	var committed []time.Time
	for _, c := range commits {
		if seen[c.ID] {
			continue
		}
		seen[c.ID] = true
		idCol, changes, bs = append(idCol, c.ID[:]), append(changes, c.ChangeID), append(bs, c.Body)
		gens, conflicted, committed = append(gens, c.Generation), append(conflicted, c.Conflicted), append(committed, micros(c.CommittedAt))
	}
	return x.returningIDs(ctx, `INSERT OR IGNORE INTO commit (repository_id, id, change_id, body, generation, conflicted, committed_at, written_by,
			pinned, touched_at)
		SELECT @repo, id, @changes[OFFSET(i)], @bodies[OFFSET(i)], @gens[OFFSET(i)], @conflicted[OFFSET(i)], @committed[OFFSET(i)], @by,
			FALSE, CURRENT_DATE('UTC')
		FROM UNNEST(@ids) AS id WITH OFFSET i THEN RETURN id`,
		map[string]any{"repo": repo, "ids": idCol, "changes": changes, "bodies": bs, "gens": gens, "conflicted": conflicted,
			"committed": committed, "by": writtenBy}, bodySize(bs), "ids", "changes", "bodies", "gens", "conflicted", "committed")
}

func (x queries) InsertCommitParents(ctx context.Context, repo int64, edges []backend.ParentEdge) error {
	if len(edges) == 0 {
		return nil
	}
	commits := make([][]byte, len(edges))
	ords := make([]int64, len(edges))
	parents := make([][]byte, len(edges))
	for i, e := range edges {
		commits[i], ords[i], parents[i] = e.Commit[:], int64(e.Ord), e.Parent[:]
	}
	_, err := x.execIn(ctx, `INSERT INTO commit_parent (repository_id, commit_id, ord, parent_id)
		SELECT @repo, c, @ords[OFFSET(i)], @parents[OFFSET(i)] FROM UNNEST(@commits) AS c WITH OFFSET i`,
		map[string]any{"repo": repo, "commits": commits, "ords": ords, "parents": parents}, []string{"commits", "ords", "parents"}, nil)
	return err
}

func (x queries) ExistingObjects(ctx context.Context, repo int64, kind encoding.Kind, members []encoding.ID) (map[encoding.ID]bool, error) {
	return x.idSet(ctx, `SELECT id FROM object WHERE repository_id = @repo AND id IN UNNEST(@ids) AND kind = @kind`,
		map[string]any{"repo": repo, "ids": ids(members), "kind": int64(kind)}, "ids")
}

func (x queries) ExistingCommits(ctx context.Context, repo int64, members []encoding.ID) (map[encoding.ID]bool, error) {
	return x.idSet(ctx, `SELECT id FROM commit WHERE repository_id = @repo AND id IN UNNEST(@ids)`,
		map[string]any{"repo": repo, "ids": ids(members)}, "ids")
}

func (x queries) ObjectBodies(ctx context.Context, repo int64, kind encoding.Kind, members []encoding.ID) (map[encoding.ID][]byte, error) {
	return x.bodies(ctx, `SELECT id, body FROM object WHERE repository_id = @repo AND id IN UNNEST(@ids) AND kind = @kind`,
		map[string]any{"repo": repo, "ids": ids(members), "kind": int64(kind)}, "ids")
}

func (x queries) CommitBodies(ctx context.Context, repo int64, members []encoding.ID) (map[encoding.ID][]byte, error) {
	return x.bodies(ctx, `SELECT id, body FROM commit WHERE repository_id = @repo AND id IN UNNEST(@ids)`,
		map[string]any{"repo": repo, "ids": ids(members)}, "ids")
}

func (x queries) CommitGenerations(ctx context.Context, repo int64, members []encoding.ID) (map[encoding.ID]int64, error) {
	out := map[encoding.ID]int64{}
	err := x.rows(ctx, `SELECT id, generation FROM commit WHERE repository_id = @repo AND id IN UNNEST(@ids)`,
		map[string]any{"repo": repo, "ids": ids(members)}, func() { clear(out) }, func(row *spanner.Row) error {
			var b []byte
			var g int64
			if err := row.Columns(&b, &g); err != nil {
				return err
			}
			id, err := toID(b)
			if err != nil {
				return err
			}
			out[id] = g
			return nil
		}, "ids")
	return out, err
}

// walkSet is what a walk sets of a row, and which rows it applies to. Today is UTC's.
func walkSet(walk backend.Walk) string {
	switch walk {
	case backend.TouchWalk:
		return ` SET touched_at = CURRENT_DATE('UTC') WHERE repository_id = @repo AND id IN UNNEST(@ids) AND NOT pinned
			AND touched_at < CURRENT_DATE('UTC')`
	case backend.RefreshWalk:
		return ` SET touched_at = CURRENT_DATE('UTC') WHERE repository_id = @repo AND id IN UNNEST(@ids) AND NOT pinned`
	}
	return ` SET pinned = TRUE WHERE repository_id = @repo AND id IN UNNEST(@ids) AND NOT pinned`
}

func (x queries) WalkObjects(ctx context.Context, repo int64, kind encoding.Kind, members []encoding.ID, walk backend.Walk) (map[encoding.ID][]byte, error) {
	return x.returningBodies(ctx, `UPDATE object`+walkSet(walk)+` AND kind = @kind THEN RETURN id, body`,
		map[string]any{"repo": repo, "ids": ids(members), "kind": int64(kind)}, "ids")
}

func (x queries) WalkCommits(ctx context.Context, repo int64, members []encoding.ID, walk backend.Walk) (map[encoding.ID][]byte, error) {
	return x.returningBodies(ctx, `UPDATE commit`+walkSet(walk)+` THEN RETURN id, body`, map[string]any{"repo": repo, "ids": ids(members)}, "ids")
}

func (x queries) StaleObjectIDs(ctx context.Context, repo int64, cutoff time.Time) ([]encoding.ID, error) {
	return x.idList(ctx, `SELECT id FROM object WHERE repository_id = @repo AND NOT pinned AND touched_at < @day ORDER BY id`,
		map[string]any{"repo": repo, "day": dayBefore(cutoff)})
}

func (x queries) StaleCommitIDs(ctx context.Context, repo int64, cutoff time.Time) ([]encoding.ID, error) {
	return x.idList(ctx, `SELECT id FROM commit WHERE repository_id = @repo AND NOT pinned AND touched_at < @day ORDER BY id`,
		map[string]any{"repo": repo, "day": dayBefore(cutoff)})
}

func (x queries) StaleObjects(ctx context.Context, repo int64, members []encoding.ID, cutoff time.Time) ([]backend.Object, error) {
	return x.kindedObjects(ctx, `SELECT id, kind, body FROM object
		WHERE repository_id = @repo AND id IN UNNEST(@ids) AND NOT pinned AND touched_at < @day`,
		map[string]any{"repo": repo, "ids": ids(members), "day": dayBefore(cutoff)}, "ids")
}

// StaleAncestry walks the stale commits among roots and their stale ancestors a generation of
// edges at a time, in one read: GoogleSQL has no recursive query.
func (x queries) StaleAncestry(ctx context.Context, repo int64, roots []encoding.ID, cutoff time.Time) (map[encoding.ID][]byte, error) {
	out := map[encoding.ID][]byte{}
	err := x.read(func(r spannerdata.Reader) error {
		clear(out)
		p := map[string]any{"repo": repo, "day": dayBefore(cutoff)}
		sql := `SELECT c.id, c.body FROM commit c
			WHERE c.repository_id = @repo AND c.id IN UNNEST(@ids) AND NOT c.pinned AND c.touched_at < @day`
		for frontier := distinct(roots); len(frontier) > 0; {
			p["ids"] = ids(frontier)
			found := map[encoding.ID][]byte{}
			if err := eachIn(ctx, r, sql, p, []string{"ids"}, func(row *spanner.Row) error { return scanBody(row, found) }); err != nil {
				return err
			}
			frontier = frontier[:0:0]
			for id, body := range found {
				if _, ok := out[id]; !ok {
					out[id] = body
					frontier = append(frontier, id)
				}
			}
			sql = `SELECT c.id, c.body FROM commit_parent p JOIN commit c ON c.repository_id = p.repository_id AND c.id = p.parent_id
				WHERE p.repository_id = @repo AND p.commit_id IN UNNEST(@ids) AND NOT c.pinned AND c.touched_at < @day`
		}
		return nil
	})
	return out, err
}

func (x queries) sweep(ctx context.Context, table string, repo int64, members []encoding.ID, cutoff time.Time, dry bool) (int64, error) {
	p := map[string]any{"repo": repo, "ids": ids(members), "day": dayBefore(cutoff)}
	where := ` WHERE repository_id = @repo AND id IN UNNEST(@ids) AND NOT pinned AND touched_at < @day`
	if dry {
		var n int64
		err := x.read(func(r spannerdata.Reader) error {
			n = 0
			return eachIn(ctx, r, `SELECT COUNT(*) FROM `+table+where, p, []string{"ids"}, func(row *spanner.Row) error {
				var c int64
				err := row.Columns(&c)
				n += c
				return err
			})
		})
		return n, err
	}
	return x.execIn(ctx, `DELETE FROM `+table+where, p, []string{"ids"}, nil)
}

func (x queries) SweepObjects(ctx context.Context, repo int64, members []encoding.ID, cutoff time.Time, dry bool) (int64, error) {
	if dry {
		return x.sweep(ctx, "object", repo, members, cutoff, dry)
	}
	// What a swept tree's or copy's row carried with it once: its sizes record, its edges.
	var n int64
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var gone [][]byte
		if err := inChunks(map[string]any{"repo": repo, "ids": ids(members), "day": dayBefore(cutoff)}, []string{"ids"}, nil,
			func(p map[string]any) error {
				return spannerdata.Each(ctx, tx, `DELETE FROM object
					WHERE repository_id = @repo AND id IN UNNEST(@ids) AND NOT pinned AND touched_at < @day THEN RETURN id`, p,
					func(row *spanner.Row) error {
						var b []byte
						if err := row.Columns(&b); err != nil {
							return err
						}
						gone = append(gone, b)
						return nil
					})
			}); err != nil {
			return err
		}
		n = int64(len(gone))
		if n == 0 {
			return nil
		}
		for _, sql := range []string{
			`DELETE FROM tree_sizes WHERE repository_id = @repo AND tree_id IN UNNEST(@gone)`,
			`DELETE FROM copy_parent WHERE repository_id = @repo AND copy_id IN UNNEST(@gone)`,
		} {
			if _, err := updateIn(ctx, tx, sql, map[string]any{"repo": repo, "gone": gone}, []string{"gone"}, nil); err != nil {
				return err
			}
		}
		return nil
	})
	return n, err
}

// SweepCommits deletes stale commits, their parent edges with them (commit_parent's foreign key).
func (x queries) SweepCommits(ctx context.Context, repo int64, members []encoding.ID, cutoff time.Time, dry bool) (int64, error) {
	return x.sweep(ctx, "commit", repo, members, cutoff, dry)
}

func (x queries) CopyChildren(ctx context.Context, repo int64, parents []encoding.ID) ([][2]encoding.ID, error) {
	return x.pairs(ctx, `SELECT copy_id, parent_id FROM copy_parent WHERE repository_id = @repo AND parent_id IN UNNEST(@ids)`,
		map[string]any{"repo": repo, "ids": ids(parents)}, "ids")
}

// commitParents is a commit's parents in order, as an array.
const commitParents = `ARRAY(SELECT p.parent_id FROM commit_parent p
	WHERE p.repository_id = c.repository_id AND p.commit_id = c.id ORDER BY p.ord)`

// graphCommits reads, in r, rows of a commit's ID, generation, and parents, over the IDs @ids
// names a chunk at a time.
func graphCommits(ctx context.Context, r spannerdata.Reader, sql string, params map[string]any) ([]backend.GraphCommit, error) {
	var out []backend.GraphCommit
	err := eachIn(ctx, r, sql, params, []string{"ids"}, func(row *spanner.Row) error {
		g, err := scanGraphCommit(row)
		if err != nil {
			return err
		}
		out = append(out, g)
		return nil
	})
	return out, err
}

func scanGraphCommit(row *spanner.Row, extra ...any) (backend.GraphCommit, error) {
	var g backend.GraphCommit
	var idb []byte
	var parents [][]byte
	if err := row.Columns(append([]any{&idb, &g.Generation, &parents}, extra...)...); err != nil {
		return g, err
	}
	var err error
	if g.ID, err = toID(idb); err != nil {
		return g, err
	}
	for _, b := range parents {
		id, err := toID(b)
		if err != nil {
			return g, err
		}
		g.Parents = append(g.Parents, id)
	}
	return g, nil
}

// TailAncestry walks the tail's commits from from a generation of edges at a time, in one read —
// GoogleSQL has no recursive query — each step reading the parents the tail holds at or above
// minGen, and stopping at those it lacks, which the store looks for in segments.
func (x queries) TailAncestry(ctx context.Context, repo int64, from []encoding.ID, minGen int64) ([]backend.GraphCommit, error) {
	var out []backend.GraphCommit
	err := x.read(func(r spannerdata.Reader) error {
		out = nil
		seen := map[encoding.ID]bool{}
		p := map[string]any{"repo": repo, "min": minGen}
		sql := `SELECT c.id, c.generation, ` + commitParents + ` FROM commit c WHERE c.repository_id = @repo AND c.id IN UNNEST(@ids)`
		for frontier := distinct(from); len(frontier) > 0; {
			p["ids"] = ids(frontier)
			step, err := graphCommits(ctx, r, sql, p)
			if err != nil {
				return err
			}
			for _, g := range step {
				seen[g.ID] = true
			}
			frontier = frontier[:0:0]
			for _, g := range step {
				out = append(out, g)
				for _, parent := range g.Parents {
					if !seen[parent] {
						seen[parent] = true
						frontier = append(frontier, parent)
					}
				}
			}
			sql = `SELECT c.id, c.generation, ` + commitParents + ` FROM commit c
				WHERE c.repository_id = @repo AND c.id IN UNNEST(@ids) AND c.generation >= @min`
		}
		return nil
	})
	return out, err
}

func (x queries) TailGraph(ctx context.Context, repo int64, members []encoding.ID) ([]backend.GraphCommit, error) {
	var out []backend.GraphCommit
	err := x.read(func(r spannerdata.Reader) error {
		var err error
		out, err = graphCommits(ctx, r, `SELECT c.id, c.generation, `+commitParents+` FROM commit c
			WHERE c.repository_id = @repo AND c.id IN UNNEST(@ids)`, map[string]any{"repo": repo, "ids": ids(members)})
		return err
	})
	return out, err
}

func (x queries) AllObjects(ctx context.Context, repo int64) ([]backend.Object, error) {
	return x.kindedObjects(ctx, `SELECT id, kind, body FROM object WHERE repository_id = @repo ORDER BY id`, map[string]any{"repo": repo})
}

func (x queries) AllCopyParents(ctx context.Context, repo int64) ([][2]encoding.ID, error) {
	return x.pairs(ctx, `SELECT copy_id, parent_id FROM copy_parent WHERE repository_id = @repo`, map[string]any{"repo": repo})
}

func (x queries) AllCommits(ctx context.Context, repo int64) ([]backend.StoredCommit, error) {
	var out []backend.StoredCommit
	err := x.rows(ctx, `SELECT id, body, generation FROM commit WHERE repository_id = @repo`, map[string]any{"repo": repo},
		func() { out = nil }, func(row *spanner.Row) error {
			var b []byte
			var c backend.StoredCommit
			if err := row.Columns(&b, &c.Body, &c.Generation); err != nil {
				return err
			}
			var err error
			if c.ID, err = toID(b); err != nil {
				return err
			}
			out = append(out, c)
			return nil
		})
	return out, err
}

func (x queries) AllCommitParents(ctx context.Context, repo int64) ([][2]encoding.ID, error) {
	return x.pairs(ctx, `SELECT commit_id, parent_id FROM commit_parent WHERE repository_id = @repo ORDER BY commit_id, ord`,
		map[string]any{"repo": repo})
}

func (x queries) TreeSizes(ctx context.Context, repo int64, trees []encoding.ID) (map[encoding.ID][]byte, error) {
	return x.bodies(ctx, `SELECT tree_id, body FROM tree_sizes WHERE repository_id = @repo AND tree_id IN UNNEST(@ids)`,
		map[string]any{"repo": repo, "ids": ids(trees)}, "ids")
}

func (x queries) InsertTreeSizes(ctx context.Context, repo int64, trees []encoding.ID, bs [][]byte) error {
	if len(trees) == 0 {
		return nil
	}
	seen := make(map[encoding.ID]bool, len(trees))
	var idCol, bodies [][]byte
	for i, t := range trees {
		if !seen[t] {
			seen[t] = true
			idCol, bodies = append(idCol, t[:]), append(bodies, bs[i])
		}
	}
	_, err := x.execIn(ctx, `INSERT OR IGNORE INTO tree_sizes (repository_id, tree_id, body)
		SELECT @repo, t, @bodies[OFFSET(i)] FROM UNNEST(@ids) AS t WITH OFFSET i`, map[string]any{"repo": repo, "ids": idCol, "bodies": bodies},
		[]string{"ids", "bodies"}, bodySize(bodies))
	return err
}

// ─── Main's history ───

func (x queries) InsertHeadLog(ctx context.Context, repo int64, h backend.HeadMove) error {
	_, err := x.exec(ctx, `INSERT INTO head_log (repository_id, seq, old_head, new_head, cl_number, patchset, landed_by, landed_at)
		VALUES (@repo, @seq, @old, @new, @cl, @patchset, @by, `+now+`)`,
		map[string]any{"repo": repo, "seq": h.Seq, "old": h.Old[:], "new": h.New[:], "cl": h.CL, "patchset": int64(h.Patchset), "by": h.By})
	return err
}

func (x queries) genCommits(ctx context.Context, sql string, params map[string]any) ([]backend.GenCommit, error) {
	var out []backend.GenCommit
	err := x.rows(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		var b []byte
		var g backend.GenCommit
		if err := row.Columns(&b, &g.Generation); err != nil {
			return err
		}
		var err error
		if g.Commit, err = toID(b); err != nil {
			return err
		}
		out = append(out, g)
		return nil
	})
	return out, err
}

func order(ascending bool) string {
	if ascending {
		return "ASC"
	}
	return "DESC"
}

func (x queries) MainLog(ctx context.Context, repo, from, to int64, ascending bool, limit int) ([]backend.GenCommit, error) {
	return x.genCommits(ctx, `SELECT new_head, seq FROM head_log
		WHERE repository_id = @repo AND seq BETWEEN @from AND @to ORDER BY seq `+order(ascending)+` LIMIT @limit`,
		map[string]any{"repo": repo, "from": from, "to": to, "limit": int64(limit)})
}

func (x queries) HeadSeqOf(ctx context.Context, repo int64, commit encoding.ID) (int64, bool, error) {
	var seq int64
	ok, err := x.one(ctx, `SELECT seq FROM head_log WHERE repository_id = @repo AND new_head = @commit LIMIT 1`,
		map[string]any{"repo": repo, "commit": commit[:]}, &seq)
	return seq, ok, err
}

func (x queries) InsertPathIndex(ctx context.Context, repo int64, p backend.PathIndex) error {
	_, err := x.exec(ctx, `INSERT INTO path_index (repository_id, generation, commit_id, parent_id) VALUES (@repo, @gen, @commit, @parent)`,
		map[string]any{"repo": repo, "gen": p.Generation, "commit": p.Commit[:], "parent": p.Parent[:]})
	return err
}

func (x queries) PathIndexes(ctx context.Context, repo int64) ([]backend.PathIndex, error) {
	var out []backend.PathIndex
	err := x.rows(ctx, `SELECT generation, commit_id, parent_id FROM path_index WHERE repository_id = @repo ORDER BY generation`,
		map[string]any{"repo": repo}, func() { out = nil }, func(row *spanner.Row) error {
			var p backend.PathIndex
			var commit, parent []byte
			if err := row.Columns(&p.Generation, &commit, &parent); err != nil {
				return err
			}
			var err error
			if p.Commit, err = toID(commit); err != nil {
				return err
			}
			if p.Parent, err = toID(parent); err != nil {
				return err
			}
			out = append(out, p)
			return nil
		})
	return out, err
}

func (x queries) DeletePathIndex(ctx context.Context, repo, gen int64) error {
	_, err := x.exec(ctx, `DELETE FROM path_index WHERE repository_id = @repo AND generation = @gen`, map[string]any{"repo": repo, "gen": gen})
	return err
}

func (x queries) InsertCommitPaths(ctx context.Context, repo, gen int64, commit encoding.ID, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	_, err := x.execIn(ctx, `INSERT OR IGNORE INTO commit_path (repository_id, path, generation, commit_id)
		SELECT DISTINCT @repo, p, @gen, @commit FROM UNNEST(@paths) AS p`,
		map[string]any{"repo": repo, "gen": gen, "commit": commit[:], "paths": paths}, []string{"paths"}, nil)
	return err
}

func (x queries) PathCommits(ctx context.Context, repo int64, paths []string, from, to int64, ascending bool, limit int) ([]backend.GenCommit, error) {
	return x.genCommits(ctx, `SELECT commit_id, generation FROM commit_path
		WHERE repository_id = @repo AND path IN UNNEST(@paths) AND generation BETWEEN @from AND @to
		GROUP BY commit_id, generation ORDER BY generation `+order(ascending)+` LIMIT @limit`,
		map[string]any{"repo": repo, "paths": paths, "from": from, "to": to, "limit": int64(limit)})
}

// LastPathCommits reads, for each path, its newest row at or below gen: main has one commit at a
// generation, so the newest generation names one commit.
func (x queries) LastPathCommits(ctx context.Context, repo, gen int64, paths []string) (map[int]backend.GenCommit, error) {
	out := map[int]backend.GenCommit{}
	const newest = `FROM commit_path cp WHERE cp.repository_id = @repo AND cp.path = u AND cp.generation <= @gen ORDER BY cp.generation DESC LIMIT 1`
	err := x.rows(ctx, `SELECT i, (SELECT cp.commit_id `+newest+`), (SELECT cp.generation `+newest+`)
		FROM UNNEST(@paths) AS u WITH OFFSET i`, map[string]any{"repo": repo, "gen": gen, "paths": paths}, func() { clear(out) },
		func(row *spanner.Row) error {
			var i int64
			var b []byte
			var g spanner.NullInt64
			if err := row.Columns(&i, &b, &g); err != nil {
				return err
			}
			if b == nil {
				return nil
			}
			id, err := toID(b)
			if err != nil {
				return err
			}
			out[int(i)] = backend.GenCommit{Commit: id, Generation: g.Int64}
			return nil
		})
	return out, err
}

func (x queries) PathHistory(ctx context.Context, repo int64, path string, gen int64) ([]backend.GenCommit, error) {
	return x.genCommits(ctx, `SELECT commit_id, generation FROM commit_path
		WHERE repository_id = @repo AND path = @path AND generation <= @gen ORDER BY generation DESC`,
		map[string]any{"repo": repo, "path": path, "gen": gen})
}
