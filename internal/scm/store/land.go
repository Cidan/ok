package store

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
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
	// Actor is its submitter, when not the landing's: each submission's in a landing of several.
	Actor string
}

// Landed is the commit one CL landed (or, in a dry run, would land) as.
type Landed struct {
	Number int64
	Commit encoding.ID
}

// Candidate is a commit prepared as exactly what a CL would land as onto Base.
type Candidate = backend.Candidate

// actorSignature is the committer of a commit the SCM writes for actor: a person by handle
// and <handle>@<email domain>, anything else by its principal.
// person is a principal as the commits the SCM writes for them sign: their stable ID, and the
// name and email their signature carries — resolved before a transaction starts, so no
// transaction waits on the directory.
type person struct{ id, name, email string }

// personOf names the principal with a stable ID.
func (s *Store) personOf(ctx context.Context, id string) (person, error) {
	name, err := s.nameOf(ctx, id)
	if err != nil {
		return person{}, err
	}
	p := person{id: id, name: name, email: name}
	if h, ok := strings.CutSuffix(name, s.names.Human("")); ok {
		p.name, p.email = h, h+"@"+s.email
	}
	return p, nil
}

// nameOf is a principal's current name, by stable ID.
func (s *Store) nameOf(ctx context.Context, id string) (string, error) {
	if s.namer == nil {
		return id, nil
	}
	name, err := s.namer.Name(ctx, id)
	if err != nil {
		return "", fmt.Errorf("naming %s: %w", id, err)
	}
	return name, nil
}

func (p person) signature(at time.Time) encoding.Signature {
	return encoding.Signature{Name: p.name, Email: p.email, Time: encoding.Timestamp{Millis: at.UnixMilli()}}
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
func (s *Store) landOnto(lc *landCommits, ts treeStore, number int64, ps Patchset, base encoding.ID, committer person, at time.Time) (encoding.ID, *encoding.Commit, error) {
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
		Committer:      committer.signature(at),
	}
	id, err := encoding.IDOf(landed)
	if err != nil {
		return encoding.ID{}, nil, err
	}
	lc.new[id] = landed
	return id, landed, nil
}

// insertCommits stores commits the SCM made (landed commits and candidates), written by
// actor, each one generation above its parent.
func (s *Store) insertCommits(ctx context.Context, tx *txn, repo int64, commits []*encoding.Commit, gens []int64, actors []string) error {
	for i, c := range commits {
		canonical, err := encoding.Encode(c)
		if err != nil {
			return err
		}
		id := encoding.Hash(canonical)
		inserted, err := tx.InsertCommits(ctx, repo, []backend.Commit{{
			ID: id, ChangeID: c.ChangeID[:], Body: canonical[1:], Generation: gens[i],
			CommittedAt: time.UnixMilli(c.Committer.Time.Millis),
		}}, actors[i])
		if err != nil {
			return err
		}
		if len(inserted) == 0 {
			continue
		}
		if err := tx.InsertCommitParents(ctx, repo, []backend.ParentEdge{{Commit: id, Ord: 0, Parent: c.Parents[0]}}); err != nil {
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
// main's new head (the would-be head on a dry run), and the last event it emitted (nil on a
// dry run).
func (s *Store) Land(ctx context.Context, repo int64, items []LandItem, expectedHead encoding.ID, actor string, dryRun bool) ([]Landed, encoding.ID, *scmpb.Event, error) {
	if len(items) == 0 {
		return nil, encoding.ID{}, nil, invalid("a landing names at least one CL")
	}
	// Each item's submitter: its own, or the landing's.
	actors := make([]string, len(items))
	committers := map[string]person{}
	for i, it := range items {
		actors[i] = cmp.Or(it.Actor, actor)
		if _, ok := committers[actors[i]]; ok {
			continue
		}
		p, err := s.personOf(ctx, actors[i])
		if err != nil {
			return nil, encoding.ID{}, nil, err
		}
		committers[actors[i]] = p
	}
	numbers := make([]int64, len(items))
	seen := map[int64]bool{}
	for i, it := range items {
		if seen[it.Number] {
			return nil, encoding.ID{}, nil, invalid("CL %d is named twice", it.Number)
		}
		seen[it.Number] = true
		numbers[i] = it.Number
	}
	var landed []Landed
	var newHead encoding.ID
	var last *scmpb.Event
	// A dry run writes nothing: read from a snapshot, it locks nothing a landing waits on.
	run := s.inTx
	if dryRun {
		run = s.inSnapshot
	}
	err := run(ctx, func(ctx context.Context, tx *txn) error {
		landed, newHead, last = nil, encoding.ID{}, nil
		head, headSeq, ok, err := tx.Head(ctx, repo, !dryRun)
		if err != nil {
			return err
		}
		if !ok {
			return notFound("repository %d", repo)
		}
		if head != expectedHead {
			return &LandRejection{Number: items[0].Number, Reason: LandHeadMoved,
				Message: fmt.Sprintf("main is at %s, not %s (validate against the new head and land again)", head, expectedHead)}
		}
		cls, err := s.readChangeLists(ctx, tx, repo, numbers, !dryRun)
		if err != nil {
			return err
		}

		lc := &landCommits{ctx: ctx, s: s, repo: repo, new: map[encoding.ID]*encoding.Commit{}}
		ts := s.pendingTrees(ctx, repo)
		at := time.Now()
		running := head
		runningGen, err := s.generation(ctx, repo, head)
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
			actor  string
		}
		var steps []step
		inCall := map[int64]bool{}
		for i, it := range items {
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
				st, err := s.clStatus(ctx, tx, repo, c.ParentNumber)
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
				id, commit, err = s.landOnto(lc, ts, c.Number, ps, running, committers[actors[i]], at)
				if err != nil {
					return err
				}
			}
			steps = append(steps, step{cl: c, ps: ps, id: id, commit: commit, gen: runningGen + 1, parent: running, actor: actors[i]})
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
		var newActors []string
		var treeRoots []encoding.ID
		for _, st := range steps {
			if st.commit != nil {
				newCommits = append(newCommits, st.commit)
				newGens = append(newGens, st.gen)
				newActors = append(newActors, st.actor)
				treeRoots = append(treeRoots, st.commit.RootTree[0])
			}
		}
		if err := ts.insert(ctx, tx, treeRoots); err != nil {
			return err
		}
		if err := s.insertCommits(ctx, tx, repo, newCommits, newGens, newActors); err != nil {
			return err
		}
		if err := tx.SetHead(ctx, repo, newHead, headSeq+int64(len(steps))); err != nil {
			return err
		}
		var roots []ref
		var events, moves []*scmpb.Event
		left := s.landingPaths
		for i, st := range steps {
			seq := headSeq + int64(i) + 1
			if err := tx.InsertHeadLog(ctx, repo, backend.HeadMove{Seq: seq, Old: st.parent, New: st.id, CL: st.cl.Number,
				Patchset: st.ps.Number, By: st.actor}); err != nil {
				return err
			}
			if err := tx.SetLanded(ctx, repo, st.cl.Number, int16(StatusSubmitted), st.id, st.actor); err != nil {
				return err
			}
			if err := s.indexPaths(ctx, tx, repo, lc, ts, st.parent, st.id, st.gen, &left); err != nil {
				return err
			}
			roots = append(roots, ref{table: tCommit, id: st.id})
			events = append(events, &scmpb.Event{Kind: &scmpb.Event_Landed{Landed: &scmpb.ChangeListLanded{
				Number: st.cl.Number, Patchset: st.ps.Number, CommitId: st.id[:], SubmitterId: st.actor,
			}}})
			moves = append(moves, &scmpb.Event{Kind: &scmpb.Event_HeadMoved{HeadMoved: &scmpb.HeadMoved{
				OldHead: st.parent[:], NewHead: st.id[:], HeadSeq: seq,
			}}})
		}
		if err := tx.DeleteCandidatesOf(ctx, repo, numbers); err != nil {
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
		return nil, encoding.ID{}, nil, err
	}
	return landed, newHead, last, nil
}

// indexPaths records the paths a commit on main changed from its parent, for Log and
// Browse — while they are no more than left of the landing's Config.LandingPaths, which it
// takes them from; otherwise it records the commit in path_index, for the compaction its pins
// mark due to write (finishPathIndexes), and readers to read from its trees until then
// (pendingPaths): the landing's transaction, and main, wait for neither.
func (s *Store) indexPaths(ctx context.Context, tx *txn, repo int64, lc *landCommits, ts treeStore, parent, id encoding.ID, gen int64,
	left *int) error {
	from, _, err := lc.tree(parent)
	if err != nil {
		return err
	}
	to, _, err := lc.tree(id)
	if err != nil {
		return err
	}
	paths, err := changedPaths(ts, from, to)
	if err != nil {
		return err
	}
	if len(paths) > *left {
		return tx.InsertPathIndex(ctx, repo, backend.PathIndex{Generation: gen, Commit: id, Parent: parent})
	}
	*left -= len(paths)
	for start := 0; start < len(paths); start += pathBatch {
		if err := tx.InsertCommitPaths(ctx, repo, gen, id, paths[start:min(start+pathBatch, len(paths))]); err != nil {
			return err
		}
	}
	return nil
}

// pathBatch bounds the paths one InsertCommitPaths, and one transaction of finishPathIndexes, writes.
const pathBatch = 10_000

// changedPaths are the paths of main's history a commit records, its tree to from its parent's
// from: each changed leaf, and every directory above one, so a directory's history is its own rows.
func changedPaths(ts treeStore, from, to encoding.ID) ([]string, error) {
	var paths []string
	seen := map[string]bool{}
	err := diffTrees(ts, from, to, "", func(ch leafChange) error {
		for p := ch.Path; p != "." && !seen[p]; p = path.Dir(p) {
			seen[p] = true
			paths = append(paths, p)
		}
		return nil
	})
	return paths, err
}

// finishPathIndexes writes the path history of each landing that left it (path_index), a
// transaction of up to pathBatch rows at a time, and forgets it once written: compaction's, before
// it moves the tail. Two at once write the same rows, once each.
func (s *Store) finishPathIndexes(ctx context.Context, repo int64) error {
	pending, err := s.pendingPaths(ctx, repo)
	if err != nil || pending == nil {
		return err
	}
	for _, m := range pending.marks {
		paths, err := changedPaths(pending.ts, m.parentTree, m.tree)
		if err != nil {
			return err
		}
		for start := 0; start < len(paths); start += pathBatch {
			batch := paths[start:min(start+pathBatch, len(paths))]
			if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
				return tx.InsertCommitPaths(ctx, repo, m.Generation, m.Commit, batch)
			}); err != nil {
				return err
			}
		}
		if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error { return tx.DeletePathIndex(ctx, repo, m.Generation) }); err != nil {
			return err
		}
	}
	return nil
}

// candidate reads a pending candidate.
func (s *Store) candidate(ctx context.Context, q backend.Ops, repo int64, id encoding.ID) (Candidate, bool, error) {
	return q.Candidate(ctx, repo, id)
}

// PrepareCandidate computes, writes, and records the commit a CL's patchset would land as
// onto base — a commit on main, or another pending candidate when automation speculates on
// a conflict group — with Land's checks (docs/scm.md, "Candidates for post-submit
// automation"). A stacked CL's parent must be submitted or among base's candidates. The
// candidate stays a GC root until it lands or is discarded.
func (s *Store) PrepareCandidate(ctx context.Context, repo, number int64, patchset int32, base encoding.ID, actor string) (Candidate, error) {
	committer, err := s.personOf(ctx, actor)
	if err != nil {
		return Candidate{}, err
	}
	var out Candidate
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		// The CLs base's candidate chain lands, down to main.
		chain := map[int64]bool{}
		cur := base
		for {
			main, err := s.onMain(ctx, tx, repo, cur)
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
		cls, err := s.lockChangeLists(ctx, tx, repo, []int64{number})
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
			st, err := s.clStatus(ctx, tx, repo, c.ParentNumber)
			if err != nil {
				return err
			}
			if st != StatusSubmitted {
				return reject(LandParentNotSubmitted, "CL %d is stacked on CL %d, which is %s and not in the candidate's base", number, c.ParentNumber, st)
			}
		}
		lc := &landCommits{ctx: ctx, s: s, repo: repo, new: map[encoding.ID]*encoding.Commit{}}
		ts := s.pendingTrees(ctx, repo)
		id, commit, err := s.landOnto(lc, ts, number, c.Current(), base, committer, time.Now())
		if err != nil {
			return err
		}
		if commit != nil {
			gen, err := s.generation(ctx, repo, base)
			if err != nil {
				return err
			}
			if err := ts.insert(ctx, tx, []encoding.ID{commit.RootTree[0]}); err != nil {
				return err
			}
			if err := s.insertCommits(ctx, tx, repo, []*encoding.Commit{commit}, []int64{gen + 1}, []string{actor}); err != nil {
				return err
			}
		}
		out, err = tx.PutCandidate(ctx, repo, Candidate{Commit: id, Base: base, Number: number, Patchset: patchset, PreparedBy: actor})
		if err != nil {
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
	if err := s.repositoryExists(ctx, s.ops(ctx), repo); err != nil {
		return nil, err
	}
	return s.ops(ctx).Candidates(ctx, repo)
}

func (s *Store) DiscardCandidate(ctx context.Context, repo int64, id encoding.ID) error {
	if err := s.repositoryExists(ctx, s.ops(ctx), repo); err != nil {
		return err
	}
	return s.inTx(ctx, func(ctx context.Context, tx *txn) error { return tx.DeleteCandidate(ctx, repo, id) })
}
