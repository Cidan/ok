package store

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/encoding"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// LandReason is why a landing or candidate was refused. The values are the protocol's.
type LandReason int

const (
	LandNotOpen            LandReason = 1
	LandStalePatchset      LandReason = 2
	LandParentNotSubmitted LandReason = 3
	LandConflicted         LandReason = 4
	LandPathConflict       LandReason = 5
	LandAlreadyOnMain      LandReason = 6
	LandHeadMoved          LandReason = 7
	LandCandidateMismatch  LandReason = 8
	LandUnknownBase        LandReason = 9
)

// LandRejection refuses a landing or a candidate: which CL, why, and every conflicting
// path.
type LandRejection struct {
	Number  int64
	Reason  LandReason
	Paths   []string
	Message string
}

func (r *LandRejection) Error() string { return "landing refused: " + r.Message }

// LandItem is one CL to land: its patchset, and optionally the candidate post-submit
// automation tested.
type LandItem struct {
	Number    int64
	Patchset  int32
	Candidate *encoding.ID
}

// Landed is the commit one CL landed (or, in a dry run, would land) as.
type Landed struct {
	Number int64
	Commit encoding.ID
}

// Candidate is a commit prepared as exactly what a CL would land as onto Base.
type Candidate struct {
	Commit     encoding.ID
	Base       encoding.ID
	Number     int64
	Patchset   int32
	PreparedBy string
	PreparedAt time.Time
}

// actorSignature is the committer of a commit the SCM writes for actor: a person by handle
// and <handle>@<email domain>, anything else by its principal.
func (s *Store) actorSignature(actor string, at time.Time) encoding.Signature {
	name, email := actor, actor
	if h := s.handleOf(actor); h != actor {
		name, email = h, h+"@"+s.email
	}
	return encoding.Signature{Name: name, Email: email, Time: encoding.Timestamp{Millis: at.UnixMilli()}}
}

// landCommits tracks the commits a landing or candidate reads and the new ones it makes,
// which are not stored until it writes.
type landCommits struct {
	ctx  context.Context
	s    *Store
	repo int64
	new  map[encoding.ID]*encoding.Commit
}

func (lc *landCommits) commit(id encoding.ID) (*encoding.Commit, error) {
	if c, ok := lc.new[id]; ok {
		return c, nil
	}
	return lc.s.readCommit(lc.ctx, lc.repo, id)
}

// tree is a commit's resolved root tree; the root commit's is the empty tree.
func (lc *landCommits) tree(id encoding.ID) (encoding.ID, bool, error) {
	if id == encoding.RootCommitID {
		return encoding.EmptyTreeID, true, nil
	}
	c, err := lc.commit(id)
	if err != nil {
		return encoding.ID{}, false, err
	}
	return c.RootTree[0], len(c.RootTree) == 1, nil
}

// landOnto computes the commit a CL's patchset lands as onto base (docs/scm.md,
// "Landing"): the patchset's commit itself when base is its base, and otherwise a new
// commit on base whose tree is the path-level splice of the patchset's change. It
// returns the commit's ID and, for a new commit, the commit to write.
func (s *Store) landOnto(lc *landCommits, ts treeStore, number int64, ps Patchset, base encoding.ID, actor string, at time.Time) (encoding.ID, *encoding.Commit, error) {
	if ps.Base == base {
		return ps.Commit, nil, nil
	}
	c, err := lc.commit(ps.Commit)
	if err != nil {
		return encoding.ID{}, nil, err
	}
	pTree, pResolved, err := lc.tree(ps.Base)
	if err != nil {
		return encoding.ID{}, nil, err
	}
	hTree, hResolved, err := lc.tree(base)
	if err != nil {
		return encoding.ID{}, nil, err
	}
	if len(c.RootTree) != 1 || !pResolved || !hResolved {
		return encoding.ID{}, nil, &LandRejection{Number: number, Reason: LandConflicted,
			Message: fmt.Sprintf("CL %d's patchset %d, its base, or the commit it lands on has unresolved conflicts", number, ps.Number)}
	}
	sp := &splicer{ts: ts}
	r, err := sp.splice(pTree, c.RootTree[0], hTree, "")
	if err != nil {
		return encoding.ID{}, nil, err
	}
	if paths := sp.conflictPaths(); len(paths) > 0 {
		return encoding.ID{}, nil, &LandRejection{Number: number, Reason: LandPathConflict, Paths: paths,
			Message: fmt.Sprintf("CL %d changes paths main has also changed since its base: %s (run jj sync, resolve any conflicts, and upload again)", number, strings.Join(paths, ", "))}
	}
	if r == hTree {
		return encoding.ID{}, nil, &LandRejection{Number: number, Reason: LandAlreadyOnMain,
			Message: fmt.Sprintf("every change CL %d makes is already on main", number)}
	}
	landed := &encoding.Commit{
		Parents:        []encoding.ID{base},
		Predecessors:   []encoding.ID{ps.Commit},
		RootTree:       encoding.Merge[encoding.ID]{r},
		ConflictLabels: encoding.Merge[string]{""},
		ChangeID:       c.ChangeID,
		Description:    c.Description,
		Author:         c.Author,
		Committer:      s.actorSignature(actor, at),
	}
	id, err := encoding.IDOf(landed)
	if err != nil {
		return encoding.ID{}, nil, err
	}
	lc.new[id] = landed
	return id, landed, nil
}

// generation is a stored commit's generation; the root's is 0.
func generation(ctx context.Context, q querier, repo int64, id encoding.ID) (int64, error) {
	if id == encoding.RootCommitID {
		return 0, nil
	}
	var g int64
	err := q.QueryRow(ctx, `SELECT generation FROM commit WHERE repository_id = $1 AND id = $2`, repo, id[:]).Scan(&g)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, notFound("commit %s", id)
	}
	return g, err
}

// insertCommits stores commits the SCM made (landed commits and candidates), written by
// actor, each one generation above its parent.
func (s *Store) insertCommits(ctx context.Context, tx pgx.Tx, repo int64, commits []*encoding.Commit, gens []int64, actor string) error {
	for i, c := range commits {
		canonical, err := encoding.Encode(c)
		if err != nil {
			return err
		}
		id := encoding.Hash(canonical)
		tag, err := tx.Exec(ctx, `INSERT INTO commit (repository_id, id, change_id, body, generation, conflicted, committed_at, written_by)
			VALUES ($1, $2, $3, $4, $5, false, $6, $7) ON CONFLICT DO NOTHING`,
			repo, id[:], c.ChangeID[:], canonical[1:], gens[i], time.UnixMilli(c.Committer.Time.Millis), actor)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			continue
		}
		if _, err := tx.Exec(ctx, `INSERT INTO commit_parent (repository_id, commit_id, ord, parent_id) VALUES ($1, $2, 0, $3)`,
			repo, id[:], c.Parents[0][:]); err != nil {
			return err
		}
	}
	return nil
}

// Land lands CLs on main, bottom of a stack first, on behalf of actor, the submitter ok
// control authorized (docs/scm.md, "Landing"). main must be at expectedHead, the
// head approval and automation were validated against. One transaction locks the
// repository and the CLs, checks each CL and computes the commit it lands as against the
// running head, then — unless dryRun — writes the new commits, advances main, marks the
// CLs submitted, indexes the landed paths, pins everything, and emits the events. A
// refusal is a *LandRejection, and nothing is written. It returns each landed commit,
// main's new head (the would-be head on a dry run), and the sequence number of the last
// event it emitted (0 on a dry run).
func (s *Store) Land(ctx context.Context, repo int64, items []LandItem, expectedHead encoding.ID, actor string, dryRun bool) ([]Landed, encoding.ID, int64, error) {
	if len(items) == 0 {
		return nil, encoding.ID{}, 0, invalid("a landing names at least one CL")
	}
	numbers := make([]int64, len(items))
	seen := map[int64]bool{}
	for i, it := range items {
		if seen[it.Number] {
			return nil, encoding.ID{}, 0, invalid("CL %d is named twice", it.Number)
		}
		seen[it.Number] = true
		numbers[i] = it.Number
	}
	var landed []Landed
	var newHead encoding.ID
	var last *scmpb.Event
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		landed, newHead, last = nil, encoding.ID{}, nil
		lock := " FOR UPDATE"
		if dryRun {
			lock = ""
		}
		var headB []byte
		var headSeq int64
		err := tx.QueryRow(ctx, `SELECT head, head_seq FROM repository WHERE id = $1`+lock, repo).Scan(&headB, &headSeq)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("repository %d", repo)
		}
		if err != nil {
			return err
		}
		head, err := toID(headB)
		if err != nil {
			return err
		}
		if head != expectedHead {
			return &LandRejection{Number: items[0].Number, Reason: LandHeadMoved,
				Message: fmt.Sprintf("main is at %s, not %s (validate against the new head and land again)", head, expectedHead)}
		}
		var cls map[int64]*ChangeList
		if dryRun {
			list, err := loadChangeLists(ctx, tx, repo, `SELECT `+changelistColumns+` FROM changelist
				WHERE repository_id = $1 AND number = ANY($2)`, repo, numbers)
			if err != nil {
				return err
			}
			cls = map[int64]*ChangeList{}
			for _, c := range list {
				cls[c.Number] = c
			}
		} else if cls, err = lockChangeLists(ctx, tx, repo, numbers); err != nil {
			return err
		}

		lc := &landCommits{ctx: ctx, s: s, repo: repo, new: map[encoding.ID]*encoding.Commit{}}
		ts := s.pendingTrees(ctx, repo)
		at := time.Now()
		running := head
		runningGen, err := generation(ctx, tx, repo, head)
		if err != nil {
			return err
		}
		type step struct {
			cl     *ChangeList
			ps     Patchset
			id     encoding.ID
			commit *encoding.Commit // new: to write
			gen    int64
			parent encoding.ID
		}
		var steps []step
		inCall := map[int64]bool{}
		for _, it := range items {
			c, ok := cls[it.Number]
			if !ok {
				return notFound("CL %d", it.Number)
			}
			reject := func(reason LandReason, format string, args ...any) error {
				return &LandRejection{Number: c.Number, Reason: reason, Message: fmt.Sprintf(format, args...)}
			}
			switch c.Status {
			case StatusDraft:
				return reject(LandNotOpen, "CL %d is a draft (request review first)", c.Number)
			case StatusOpen:
			default:
				return reject(LandNotOpen, "CL %d is %s", c.Number, c.Status)
			}
			if c.CurrentPatchset != it.Patchset {
				return reject(LandStalePatchset, "CL %d is at patchset %d, not %d", c.Number, c.CurrentPatchset, it.Patchset)
			}
			if c.ParentNumber != 0 && !inCall[c.ParentNumber] {
				st, err := clStatus(ctx, tx, repo, c.ParentNumber)
				if err != nil {
					return err
				}
				if st != StatusSubmitted {
					return reject(LandParentNotSubmitted, "CL %d is stacked on CL %d, which is %s (a stack lands bottom first)", c.Number, c.ParentNumber, st)
				}
			}
			ps := c.Current()
			var id encoding.ID
			var commit *encoding.Commit
			if it.Candidate != nil {
				cand, ok, err := s.candidate(ctx, tx, repo, *it.Candidate)
				if err != nil {
					return err
				}
				if !ok || cand.Number != c.Number || cand.Patchset != ps.Number {
					return reject(LandCandidateMismatch, "%s is not a candidate prepared from CL %d at patchset %d", *it.Candidate, c.Number, ps.Number)
				}
				if cand.Base == running {
					id = cand.Commit
				}
			}
			if id == (encoding.ID{}) {
				id, commit, err = s.landOnto(lc, ts, c.Number, ps, running, actor, at)
				if err != nil {
					return err
				}
			}
			steps = append(steps, step{cl: c, ps: ps, id: id, commit: commit, gen: runningGen + 1, parent: running})
			landed = append(landed, Landed{Number: c.Number, Commit: id})
			inCall[c.Number] = true
			running, runningGen = id, runningGen+1
		}
		newHead = running
		if dryRun {
			return nil
		}

		var newCommits []*encoding.Commit
		var newGens []int64
		var treeRoots []encoding.ID
		for _, st := range steps {
			if st.commit != nil {
				newCommits = append(newCommits, st.commit)
				newGens = append(newGens, st.gen)
				treeRoots = append(treeRoots, st.commit.RootTree[0])
			}
		}
		if err := ts.insert(ctx, tx, treeRoots); err != nil {
			return err
		}
		if err := s.insertCommits(ctx, tx, repo, newCommits, newGens, actor); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE repository SET head = $2, head_seq = $3 WHERE id = $1`,
			repo, newHead[:], headSeq+int64(len(steps))); err != nil {
			return err
		}
		var roots []ref
		var events, moves []*scmpb.Event
		for i, st := range steps {
			seq := headSeq + int64(i) + 1
			if _, err := tx.Exec(ctx, `INSERT INTO head_log (repository_id, seq, old_head, new_head, cl_number, patchset, landed_by)
				VALUES ($1, $2, $3, $4, $5, $6, $7)`, repo, seq, st.parent[:], st.id[:], st.cl.Number, st.ps.Number, actor); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE changelist SET status = $3, landed_commit = $4, submitted_by = $5, updated_at = now()
				WHERE repository_id = $1 AND number = $2`, repo, st.cl.Number, int16(StatusSubmitted), st.id[:], actor); err != nil {
				return err
			}
			if err := s.indexPaths(ctx, tx, repo, lc, ts, st.parent, st.id, st.gen); err != nil {
				return err
			}
			roots = append(roots, ref{table: tCommit, id: st.id})
			events = append(events, &scmpb.Event{Kind: &scmpb.Event_Landed{Landed: &scmpb.ChangeListLanded{
				Number: st.cl.Number, Patchset: st.ps.Number, CommitId: st.id[:], Submitter: actor,
			}}})
			moves = append(moves, &scmpb.Event{Kind: &scmpb.Event_HeadMoved{HeadMoved: &scmpb.HeadMoved{
				OldHead: st.parent[:], NewHead: st.id[:], HeadSeq: seq,
			}}})
		}
		if _, err := tx.Exec(ctx, `DELETE FROM candidate WHERE repository_id = $1 AND cl_number = ANY($2)`, repo, numbers); err != nil {
			return err
		}
		if err := s.pin(ctx, tx, repo, roots); err != nil {
			// The landing wrote objects it found already stored; garbage collection can
			// sweep an unreferenced one before this pins it. Running again stores it anew.
			if errors.Is(err, errMissing) {
				return fmt.Errorf("%w: %v", errRace, err)
			}
			return err
		}
		all := append(events, moves...)
		last = all[len(all)-1]
		return appendEvents(ctx, tx, repo, all)
	})
	if err != nil {
		return nil, encoding.ID{}, 0, err
	}
	return landed, newHead, last.GetSeq(), nil
}

// indexPaths records the paths a commit on main changed from its parent, for Log and
// Browse: each changed leaf, and every directory above one, so a directory's history is
// its own rows.
func (s *Store) indexPaths(ctx context.Context, tx pgx.Tx, repo int64, lc *landCommits, ts treeStore, parent, id encoding.ID, gen int64) error {
	from, _, err := lc.tree(parent)
	if err != nil {
		return err
	}
	to, _, err := lc.tree(id)
	if err != nil {
		return err
	}
	var paths []string
	seen := map[string]bool{}
	if err := diffTrees(ts, from, to, "", func(ch leafChange) error {
		for p := ch.Path; p != "." && !seen[p]; p = path.Dir(p) {
			seen[p] = true
			paths = append(paths, p)
		}
		return nil
	}); err != nil {
		return err
	}
	for start := 0; start < len(paths); start += 10_000 {
		batch := paths[start:min(start+10_000, len(paths))]
		if _, err := tx.Exec(ctx, `INSERT INTO commit_path (repository_id, path, generation, commit_id)
			SELECT $1, u.path, $2, $3 FROM unnest($4::text[]) AS u(path) ON CONFLICT DO NOTHING`,
			repo, gen, id[:], batch); err != nil {
			return err
		}
	}
	return nil
}

// clStatus reads a CL's status.
func clStatus(ctx context.Context, q querier, repo, number int64) (Status, error) {
	var st int16
	err := q.QueryRow(ctx, `SELECT status FROM changelist WHERE repository_id = $1 AND number = $2`, repo, number).Scan(&st)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, notFound("CL %d", number)
	}
	return Status(st), err
}

// candidate reads a pending candidate.
func (s *Store) candidate(ctx context.Context, q querier, repo int64, id encoding.ID) (Candidate, bool, error) {
	var base []byte
	c := Candidate{Commit: id}
	err := q.QueryRow(ctx, `SELECT cl_number, patchset, base FROM candidate WHERE repository_id = $1 AND commit_id = $2`,
		repo, id[:]).Scan(&c.Number, &c.Patchset, &base)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, false, nil
	}
	if err != nil {
		return c, false, err
	}
	c.Base, err = toID(base)
	return c, err == nil, err
}

// PrepareCandidate computes, writes, and records the commit a CL's patchset would land as
// onto base — a commit on main, or another pending candidate when automation speculates on
// a conflict group — with Land's checks (docs/scm.md, "Candidates for post-submit
// automation"). A stacked CL's parent must be submitted or among base's candidates. The
// candidate stays a GC root until it lands or is discarded.
func (s *Store) PrepareCandidate(ctx context.Context, repo, number int64, patchset int32, base encoding.ID, actor string) (Candidate, error) {
	var out Candidate
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// The CLs base's candidate chain lands, down to main.
		chain := map[int64]bool{}
		cur := base
		for {
			main, err := onMain(ctx, tx, repo, cur)
			if err != nil {
				return err
			}
			if main {
				break
			}
			cand, ok, err := s.candidate(ctx, tx, repo, cur)
			if err != nil {
				return err
			}
			if !ok {
				return &LandRejection{Number: number, Reason: LandUnknownBase,
					Message: fmt.Sprintf("%s is neither on main nor a pending candidate", cur)}
			}
			chain[cand.Number] = true
			cur = cand.Base
		}
		cls, err := lockChangeLists(ctx, tx, repo, []int64{number})
		if err != nil {
			return err
		}
		c, ok := cls[number]
		if !ok {
			if err := s.repositoryExists(ctx, tx, repo); err != nil {
				return err
			}
			return notFound("CL %d", number)
		}
		reject := func(reason LandReason, format string, args ...any) error {
			return &LandRejection{Number: number, Reason: reason, Message: fmt.Sprintf(format, args...)}
		}
		if c.Status != StatusOpen {
			return reject(LandNotOpen, "CL %d is %s", number, c.Status)
		}
		if c.CurrentPatchset != patchset {
			return reject(LandStalePatchset, "CL %d is at patchset %d, not %d", number, c.CurrentPatchset, patchset)
		}
		if c.ParentNumber != 0 && !chain[c.ParentNumber] {
			st, err := clStatus(ctx, tx, repo, c.ParentNumber)
			if err != nil {
				return err
			}
			if st != StatusSubmitted {
				return reject(LandParentNotSubmitted, "CL %d is stacked on CL %d, which is %s and not in the candidate's base", number, c.ParentNumber, st)
			}
		}
		lc := &landCommits{ctx: ctx, s: s, repo: repo, new: map[encoding.ID]*encoding.Commit{}}
		ts := s.pendingTrees(ctx, repo)
		id, commit, err := s.landOnto(lc, ts, number, c.Current(), base, actor, time.Now())
		if err != nil {
			return err
		}
		if commit != nil {
			gen, err := generation(ctx, tx, repo, base)
			if err != nil {
				return err
			}
			if err := ts.insert(ctx, tx, []encoding.ID{commit.RootTree[0]}); err != nil {
				return err
			}
			if err := s.insertCommits(ctx, tx, repo, []*encoding.Commit{commit}, []int64{gen + 1}, actor); err != nil {
				return err
			}
		}
		out = Candidate{Commit: id, Base: base, Number: number, Patchset: patchset}
		if err := tx.QueryRow(ctx, `WITH added AS (
				INSERT INTO candidate (repository_id, commit_id, cl_number, patchset, base, prepared_by)
				VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING RETURNING prepared_by, created_at)
			SELECT prepared_by, created_at FROM added
			UNION ALL SELECT prepared_by, created_at FROM candidate WHERE repository_id = $1 AND commit_id = $2
			LIMIT 1`, repo, id[:], number, patchset, base[:], actor).Scan(&out.PreparedBy, &out.PreparedAt); err != nil {
			return err
		}
		// A candidate is not pinned, only a root of garbage collection's mark. Refreshing
		// it keeps what it reuses from a sweep that marked before it existed; an object
		// swept after this transaction found it is stored anew by running again.
		if err := s.refresh(ctx, tx, repo, []ref{{table: tCommit, id: id}}); err != nil {
			if errors.Is(err, errMissing) {
				return fmt.Errorf("%w: %v", errRace, err)
			}
			return err
		}
		return nil
	})
	return out, err
}

// DiscardCandidate drops a pending candidate that will not land. Discarding one that is
// not pending is not an error: it may have landed.
// ListCandidates lists a repository's pending candidates, oldest first.
func (s *Store) ListCandidates(ctx context.Context, repo int64) ([]Candidate, error) {
	if err := s.repositoryExists(ctx, s.pool, repo); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT commit_id, base, cl_number, patchset, prepared_by, created_at FROM candidate
		WHERE repository_id = $1 ORDER BY created_at, commit_id`, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Candidate
	for rows.Next() {
		var commit, base []byte
		var c Candidate
		if err := rows.Scan(&commit, &base, &c.Number, &c.Patchset, &c.PreparedBy, &c.PreparedAt); err != nil {
			return nil, err
		}
		if c.Commit, err = toID(commit); err != nil {
			return nil, err
		}
		if c.Base, err = toID(base); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) DiscardCandidate(ctx context.Context, repo int64, id encoding.ID) error {
	if err := s.repositoryExists(ctx, s.pool, repo); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM candidate WHERE repository_id = $1 AND commit_id = $2`, repo, id[:])
	return err
}
