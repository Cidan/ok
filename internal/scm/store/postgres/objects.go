package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Objects and commits ───

func (x queries) InsertObjects(ctx context.Context, repo int64, objs []backend.Object) ([]encoding.ID, error) {
	if len(objs) == 0 {
		return nil, nil
	}
	idCol := make([]encoding.ID, len(objs))
	kinds := make([]int16, len(objs))
	bs := make([][]byte, len(objs))
	for i, o := range objs {
		idCol[i], kinds[i], bs[i] = o.ID, int16(o.Kind), o.Body
	}
	return idList(x.q.Query(ctx, `INSERT INTO object (repository_id, id, kind, body)
		SELECT $1, u.id, u.kind, u.body FROM unnest($2::bytea[], $3::smallint[], $4::bytea[]) AS u(id, kind, body)
		ORDER BY u.id ON CONFLICT DO NOTHING RETURNING id`, repo, ids(idCol), kinds, bs))
}

func (x queries) InsertCopyParents(ctx context.Context, repo int64, copies, parents []encoding.ID) error {
	_, err := x.q.Exec(ctx, `INSERT INTO copy_parent (repository_id, copy_id, parent_id)
		SELECT $1, u.c, u.p FROM unnest($2::bytea[], $3::bytea[]) AS u(c, p) ON CONFLICT DO NOTHING`,
		repo, ids(copies), ids(parents))
	return err
}

func (x queries) InsertCommits(ctx context.Context, repo int64, commits []backend.Commit, writtenBy string) ([]encoding.ID, error) {
	if len(commits) == 0 {
		return nil, nil
	}
	n := len(commits)
	idCol := make([]encoding.ID, n)
	changes := make([][]byte, n)
	bs := make([][]byte, n)
	gens := make([]int64, n)
	conflicted := make([]bool, n)
	committed := make([]time.Time, n)
	for i, c := range commits {
		idCol[i], changes[i], bs[i], gens[i], conflicted[i], committed[i] = c.ID, c.ChangeID, c.Body, c.Generation, c.Conflicted, c.CommittedAt
	}
	return idList(x.q.Query(ctx, `INSERT INTO commit (repository_id, id, change_id, body, generation, conflicted, committed_at, written_by)
		SELECT $1, u.id, u.change, u.body, u.gen, u.conflicted, u.committed, $8
		FROM unnest($2::bytea[], $3::bytea[], $4::bytea[], $5::bigint[], $6::bool[], $7::timestamptz[]) AS u(id, change, body, gen, conflicted, committed)
		ORDER BY u.id ON CONFLICT DO NOTHING RETURNING id`,
		repo, ids(idCol), changes, bs, gens, conflicted, committed, writtenBy))
}

func (x queries) InsertCommitParents(ctx context.Context, repo int64, edges []backend.ParentEdge) error {
	if len(edges) == 0 {
		return nil
	}
	commits := make([]encoding.ID, len(edges))
	ords := make([]int16, len(edges))
	parents := make([]encoding.ID, len(edges))
	for i, e := range edges {
		commits[i], ords[i], parents[i] = e.Commit, e.Ord, e.Parent
	}
	_, err := x.q.Exec(ctx, `INSERT INTO commit_parent (repository_id, commit_id, ord, parent_id)
		SELECT $1, u.c, u.o, u.p FROM unnest($2::bytea[], $3::smallint[], $4::bytea[]) AS u(c, o, p)`,
		repo, ids(commits), ords, ids(parents))
	return err
}

func (x queries) ExistingObjects(ctx context.Context, repo int64, kind encoding.Kind, members []encoding.ID) (map[encoding.ID]bool, error) {
	return idSet(x.q.Query(ctx, `SELECT id FROM object WHERE repository_id = $1 AND id = ANY($2) AND kind = $3`, repo, ids(members), int16(kind)))
}

func (x queries) ExistingCommits(ctx context.Context, repo int64, members []encoding.ID) (map[encoding.ID]bool, error) {
	return idSet(x.q.Query(ctx, `SELECT id FROM commit WHERE repository_id = $1 AND id = ANY($2)`, repo, ids(members)))
}

func (x queries) ObjectBodies(ctx context.Context, repo int64, kind encoding.Kind, members []encoding.ID) (map[encoding.ID][]byte, error) {
	return bodies(x.q.Query(ctx, `SELECT id, body FROM object WHERE repository_id = $1 AND id = ANY($2) AND kind = $3`, repo, ids(members), int16(kind)))
}

func (x queries) CommitBodies(ctx context.Context, repo int64, members []encoding.ID) (map[encoding.ID][]byte, error) {
	return bodies(x.q.Query(ctx, `SELECT id, body FROM commit WHERE repository_id = $1 AND id = ANY($2)`, repo, ids(members)))
}

func (x queries) CommitGenerations(ctx context.Context, repo int64, members []encoding.ID) (map[encoding.ID]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT id, generation FROM commit WHERE repository_id = $1 AND id = ANY($2)`, repo, ids(members))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[encoding.ID]int64{}
	for rows.Next() {
		var b []byte
		var g int64
		if err := rows.Scan(&b, &g); err != nil {
			return nil, err
		}
		id, err := toID(b)
		if err != nil {
			return nil, err
		}
		out[id] = g
	}
	return out, rows.Err()
}

// walkSet is what a walk sets of a row, and which rows it applies to.
func walkSet(walk backend.Walk) string {
	switch walk {
	case backend.TouchWalk:
		return ` SET touched_at = current_date WHERE repository_id = $1 AND id = ANY($2) AND NOT pinned AND touched_at < current_date`
	case backend.RefreshWalk:
		return ` SET touched_at = current_date WHERE repository_id = $1 AND id = ANY($2) AND NOT pinned`
	}
	return ` SET pinned = true WHERE repository_id = $1 AND id = ANY($2) AND NOT pinned`
}

func (x queries) WalkObjects(ctx context.Context, repo int64, kind encoding.Kind, members []encoding.ID, walk backend.Walk) (map[encoding.ID][]byte, error) {
	return bodies(x.q.Query(ctx, `UPDATE object`+walkSet(walk)+` AND kind = $3 RETURNING id, body`, repo, ids(members), int16(kind)))
}

func (x queries) WalkCommits(ctx context.Context, repo int64, members []encoding.ID, walk backend.Walk) (map[encoding.ID][]byte, error) {
	return bodies(x.q.Query(ctx, `UPDATE commit`+walkSet(walk)+` RETURNING id, body`, repo, ids(members)))
}

func (x queries) StaleObjectIDs(ctx context.Context, repo int64, cutoff time.Time) ([]encoding.ID, error) {
	return idList(x.q.Query(ctx, `SELECT id FROM object WHERE repository_id = $1 AND NOT pinned AND touched_at < $2 ORDER BY id`, repo, cutoff))
}

func (x queries) StaleCommitIDs(ctx context.Context, repo int64, cutoff time.Time) ([]encoding.ID, error) {
	return idList(x.q.Query(ctx, `SELECT id FROM commit WHERE repository_id = $1 AND NOT pinned AND touched_at < $2 ORDER BY id`, repo, cutoff))
}

func (x queries) StaleObjects(ctx context.Context, repo int64, members []encoding.ID, cutoff time.Time) ([]backend.Object, error) {
	rows, err := x.q.Query(ctx, `SELECT id, kind, body FROM object
		WHERE repository_id = $1 AND id = ANY($2) AND NOT pinned AND touched_at < $3`, repo, ids(members), cutoff)
	return kindedObjects(rows, err)
}

// kindedObjects reads a query's rows of an ID, a kind, and a body each, in order.
func kindedObjects(rows pgx.Rows, err error) ([]backend.Object, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Object
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
		out = append(out, backend.Object{ID: id, Kind: encoding.Kind(kind), Body: body})
	}
	return out, rows.Err()
}

func (x queries) StaleAncestry(ctx context.Context, repo int64, roots []encoding.ID, cutoff time.Time) (map[encoding.ID][]byte, error) {
	return bodies(x.q.Query(ctx, `WITH RECURSIVE m(id) AS (
			SELECT c.id FROM commit c
			WHERE c.repository_id = $1 AND c.id = ANY($2) AND NOT c.pinned AND c.touched_at < $3
		UNION
			SELECT c.id FROM m
			JOIN commit_parent p ON p.repository_id = $1 AND p.commit_id = m.id
			JOIN commit c ON c.repository_id = $1 AND c.id = p.parent_id
			WHERE NOT c.pinned AND c.touched_at < $3
		)
		SELECT c.id, c.body FROM m JOIN commit c ON c.repository_id = $1 AND c.id = m.id`, repo, ids(roots), cutoff))
}

func (x queries) sweep(ctx context.Context, table string, repo int64, members []encoding.ID, cutoff time.Time, dry bool) (int64, error) {
	if dry {
		var n int64
		err := x.q.QueryRow(ctx, `SELECT count(*) FROM `+table+` WHERE repository_id = $1 AND id = ANY($2) AND NOT pinned AND touched_at < $3`,
			repo, ids(members), cutoff).Scan(&n)
		return n, err
	}
	tag, err := x.q.Exec(ctx, `DELETE FROM `+table+` WHERE repository_id = $1 AND id = ANY($2) AND NOT pinned AND touched_at < $3`,
		repo, ids(members), cutoff)
	return tag.RowsAffected(), err
}

func (x queries) SweepObjects(ctx context.Context, repo int64, members []encoding.ID, cutoff time.Time, dry bool) (int64, error) {
	return x.sweep(ctx, "object", repo, members, cutoff, dry)
}

func (x queries) SweepCommits(ctx context.Context, repo int64, members []encoding.ID, cutoff time.Time, dry bool) (int64, error) {
	return x.sweep(ctx, "commit", repo, members, cutoff, dry)
}

// CopyRelatives walks copy_parent with fenced per-row index probes, as Ancestry does.
func (x queries) CopyRelatives(ctx context.Context, repo int64, id encoding.ID) (map[encoding.ID][]byte, error) {
	return bodies(x.q.Query(ctx, `WITH RECURSIVE
		anc(id) AS (
			SELECT $2::bytea
			UNION
			SELECT p.parent_id FROM anc, LATERAL (
				SELECT cp.parent_id FROM copy_parent cp WHERE cp.repository_id = $1 AND cp.copy_id = anc.id OFFSET 0) p
		),
		rel(id) AS (
			SELECT id FROM anc
			UNION
			SELECT c.copy_id FROM rel, LATERAL (
				SELECT cp.copy_id FROM copy_parent cp WHERE cp.repository_id = $1 AND cp.parent_id = rel.id OFFSET 0) c
		)
		SELECT o.id, o.body FROM rel JOIN object o ON o.repository_id = $1 AND o.id = rel.id AND o.kind = $3`,
		repo, id[:], int16(encoding.KindCopyHistory)))
}

// parentsOf is the parent edges of one commit, with each parent's generation. The
// OFFSET 0 fence keeps it a subquery run per row — two primary-key probes — so a walk
// that visits a window of a repository's history never becomes a join over every edge
// in the repository.
const parentsOf = `SELECT cp.parent_id, cp.ord, pc.generation
	FROM commit_parent cp JOIN commit pc ON pc.repository_id = cp.repository_id AND pc.id = cp.parent_id
	WHERE cp.repository_id = $1 AND cp.commit_id = walk.id OFFSET 0`

func (x queries) Ancestry(ctx context.Context, repo int64, from []encoding.ID, minGen int64) ([]backend.Ancestor, error) {
	rows, err := x.q.Query(ctx, `WITH RECURSIVE walk(id) AS (
			SELECT unnest($2::bytea[])
			UNION
			SELECT e.parent_id FROM walk, LATERAL (`+parentsOf+`) e WHERE e.generation >= $3
		)
		SELECT walk.id, e.parent_id, e.generation
		FROM walk LEFT JOIN LATERAL (`+parentsOf+`) e ON true
		ORDER BY walk.id, e.ord`, repo, ids(from), minGen)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Ancestor
	for rows.Next() {
		var idb, parentb []byte
		var pgen *int64
		if err := rows.Scan(&idb, &parentb, &pgen); err != nil {
			return nil, err
		}
		id, err := toID(idb)
		if err != nil {
			return nil, err
		}
		if len(out) == 0 || out[len(out)-1].ID != id {
			out = append(out, backend.Ancestor{ID: id})
		}
		if pgen == nil {
			continue
		}
		parent, err := toID(parentb)
		if err != nil {
			return nil, err
		}
		a := &out[len(out)-1]
		a.Parents = append(a.Parents, parent)
		a.Generations = append(a.Generations, *pgen)
	}
	return out, rows.Err()
}

func (x queries) AllObjects(ctx context.Context, repo int64) ([]backend.Object, error) {
	rows, err := x.q.Query(ctx, `SELECT id, kind, body FROM object WHERE repository_id = $1 ORDER BY id`, repo)
	return kindedObjects(rows, err)
}

// pairs reads a query's rows of two IDs each.
func pairs(rows pgx.Rows, err error) ([][2]encoding.ID, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]encoding.ID
	for rows.Next() {
		var ab, bb []byte
		if err := rows.Scan(&ab, &bb); err != nil {
			return nil, err
		}
		a, err := toID(ab)
		if err != nil {
			return nil, err
		}
		b, err := toID(bb)
		if err != nil {
			return nil, err
		}
		out = append(out, [2]encoding.ID{a, b})
	}
	return out, rows.Err()
}

func (x queries) AllCopyParents(ctx context.Context, repo int64) ([][2]encoding.ID, error) {
	return pairs(x.q.Query(ctx, `SELECT copy_id, parent_id FROM copy_parent WHERE repository_id = $1`, repo))
}

func (x queries) AllCommits(ctx context.Context, repo int64) ([]backend.StoredCommit, error) {
	rows, err := x.q.Query(ctx, `SELECT id, body, generation FROM commit WHERE repository_id = $1`, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.StoredCommit
	for rows.Next() {
		var b []byte
		var c backend.StoredCommit
		if err := rows.Scan(&b, &c.Body, &c.Generation); err != nil {
			return nil, err
		}
		if c.ID, err = toID(b); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (x queries) AllCommitParents(ctx context.Context, repo int64) ([][2]encoding.ID, error) {
	return pairs(x.q.Query(ctx, `SELECT commit_id, parent_id FROM commit_parent WHERE repository_id = $1 ORDER BY commit_id, ord`, repo))
}

func (x queries) TreeSizes(ctx context.Context, repo int64, trees []encoding.ID) (map[encoding.ID][]byte, error) {
	return bodies(x.q.Query(ctx, `SELECT tree_id, body FROM tree_sizes WHERE repository_id = $1 AND tree_id = ANY($2)`, repo, ids(trees)))
}

func (x queries) InsertTreeSizes(ctx context.Context, repo int64, trees []encoding.ID, bs [][]byte) error {
	_, err := x.q.Exec(ctx, `INSERT INTO tree_sizes (repository_id, tree_id, body)
		SELECT $1, t.id, t.body FROM unnest($2::bytea[], $3::bytea[]) AS t(id, body)
		ON CONFLICT DO NOTHING`, repo, ids(trees), bs)
	return err
}

// ─── Main's history ───

func (x queries) InsertHeadLog(ctx context.Context, repo int64, h backend.HeadMove) error {
	_, err := x.q.Exec(ctx, `INSERT INTO head_log (repository_id, seq, old_head, new_head, cl_number, patchset, landed_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, repo, h.Seq, h.Old[:], h.New[:], h.CL, h.Patchset, h.By)
	return err
}

func genCommits(rows pgx.Rows, err error) ([]backend.GenCommit, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.GenCommit
	for rows.Next() {
		var b []byte
		var g backend.GenCommit
		if err := rows.Scan(&b, &g.Generation); err != nil {
			return nil, err
		}
		if g.Commit, err = toID(b); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func order(ascending bool) string {
	if ascending {
		return "ASC"
	}
	return "DESC"
}

func (x queries) MainLog(ctx context.Context, repo, from, to int64, ascending bool, limit int) ([]backend.GenCommit, error) {
	return genCommits(x.q.Query(ctx, `SELECT new_head, seq FROM head_log
		WHERE repository_id = $1 AND seq BETWEEN $2 AND $3 ORDER BY seq `+order(ascending)+` LIMIT $4`, repo, from, to, limit))
}

func (x queries) HeadSeqOf(ctx context.Context, repo int64, commit encoding.ID) (int64, bool, error) {
	var seq int64
	err := x.q.QueryRow(ctx, `SELECT seq FROM head_log WHERE repository_id = $1 AND new_head = $2`, repo, commit[:]).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return seq, err == nil, err
}

func (x queries) InsertCommitPaths(ctx context.Context, repo, gen int64, commit encoding.ID, paths []string) error {
	_, err := x.q.Exec(ctx, `INSERT INTO commit_path (repository_id, path, generation, commit_id)
		SELECT $1, u.path, $2, $3 FROM unnest($4::text[]) AS u(path) ON CONFLICT DO NOTHING`,
		repo, gen, commit[:], paths)
	return err
}

func (x queries) PathCommits(ctx context.Context, repo int64, paths []string, from, to int64, ascending bool, limit int) ([]backend.GenCommit, error) {
	return genCommits(x.q.Query(ctx, `SELECT commit_id, generation FROM commit_path
		WHERE repository_id = $1 AND path = ANY($2::text[]) AND generation BETWEEN $3 AND $4
		GROUP BY commit_id, generation ORDER BY generation `+order(ascending)+` LIMIT $5`, repo, paths, from, to, limit))
}

func (x queries) LastPathCommits(ctx context.Context, repo, gen int64, paths []string) (map[int]encoding.ID, error) {
	rows, err := x.q.Query(ctx, `SELECT u.i, (SELECT cp.commit_id FROM commit_path cp
			WHERE cp.repository_id = $1 AND cp.path = u.path AND cp.generation <= $2
			ORDER BY cp.generation DESC LIMIT 1)
		FROM unnest($3::text[]) WITH ORDINALITY AS u(path, i)`, repo, gen, paths)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int]encoding.ID{}
	for rows.Next() {
		var i int64
		var b []byte
		if err := rows.Scan(&i, &b); err != nil {
			return nil, err
		}
		if b == nil {
			continue
		}
		id, err := toID(b)
		if err != nil {
			return nil, err
		}
		out[int(i-1)] = id
	}
	return out, rows.Err()
}

func (x queries) PathHistory(ctx context.Context, repo int64, path string, gen int64) ([]encoding.ID, error) {
	return idList(x.q.Query(ctx, `SELECT commit_id FROM commit_path WHERE repository_id = $1 AND path = $2 AND generation <= $3 ORDER BY generation DESC`, repo, path, gen))
}
