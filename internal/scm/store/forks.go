package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/wait"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Forks (docs/hosted.md, "Fork networks, as built"). A fork is a repository in its parent's network
// whose manifest names its parent's segments, inherited: forking and syncing cost the parent's
// segment count, not its size. Inherited segments are immutable and never merged, and an object
// leaves the bucket only when no manifest in the network names it, so nothing its parent does
// changes what a fork reads. A fork's main's history follows its parent's up to the place it forked
// or last synced at (ParentSeq); the segments of path history and main's log it took are read only
// that far.

// inheritedKinds are the kinds of segment a fork takes from its parent: what it holds — objects,
// trees' sizes, copies' edges, the commit graph and bodies, its content index — and main's history,
// path history and main's log. CLs, events, and operation logs are each repository's own.
var inheritedKinds = []segment.Kind{segObjects, segTreeSizes, segCopyEdges, segCommitGraph, segCommitBodies, segIndex, segPaths, segMainLog}

// boundedKinds are the inherited kinds read along main, each record at a place in main's history:
// path history (by generation, which on main is the place) and main's log.
var boundedKinds = map[segment.Kind]bool{segPaths: true, segMainLog: true}

// Fork makes repository id — the ID the directory chose — a fork of parent, by createdBy: in
// parent's network, its main parent's main, holding everything parent held pinned. It compacts
// parent's tail first, so everything main reaches — and main's history — is in segments, then takes
// them by reference.
func (s *Store) Fork(ctx context.Context, id, parent int64, createdBy string) (Repository, error) {
	if id == 0 {
		return Repository{}, invalid("a fork is made under the ID the directory chose")
	}
	p, err := s.GetRepository(ctx, parent)
	if err != nil {
		return Repository{}, err
	}
	if err := s.checkMainPlace(ctx, p); err != nil {
		return Repository{}, err
	}
	if err := s.compactFully(ctx, parent); err != nil {
		return Repository{}, fmt.Errorf("compacting repository %d: %w", parent, err)
	}
	var r Repository
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		// The parent's row, held so it is not deleted meanwhile.
		if ok, err := tx.LockRepository(ctx, parent); err != nil || !ok {
			if err == nil {
				err = notFound("repository %d", parent)
			}
			return err
		}
		var err error
		r, err = tx.InsertFork(ctx, id, p, createdBy)
		if errors.Is(err, backend.ErrDuplicate) {
			return errors.Join(ErrAlreadyExists, err)
		}
		if err != nil {
			return err
		}
		if err := tx.InsertContentIndex(ctx, repoScope(r.ID), 1); err != nil {
			return err
		}
		if err := s.inherit(ctx, tx, r.ID, parent, p.HeadSeq); err != nil {
			return err
		}
		if err := made(ctx, tx, r.ID); err != nil {
			return err
		}
		return appendEvent(ctx, tx, r.ID, &scmpb.Event{Kind: &scmpb.Event_RepositoryCreated{
			RepositoryCreated: &scmpb.RepositoryCreated{CreatedById: createdBy},
		}})
	})
	if err != nil {
		return Repository{}, err
	}
	return r, nil
}

// SyncResult is what a sync did.
type SyncResult struct {
	// Moved says the fork's main moved to its parent's: the fork had landed nothing of its own
	// since it forked or last synced.
	Moved bool
	// Head is the fork's main after the sync, and Seq its place.
	Head encoding.ID
	Seq  int64
}

// Sync brings a fork its upstream's newer work, by actor: it compacts the upstream's tail, and takes
// the upstream's segments as they are now in place of those it took before, so the fork holds
// everything the upstream holds pinned. A fork that has landed nothing of its own since it forked
// or last synced moves its main to its upstream's; one that has keeps its main, its history
// following its upstream's no further than before.
//
// The upstream is parent: the one the directory names now, which GitHub's rules may have changed
// since the fork forked (docs/hosted.md, "GitHub's rules") — 0 for a fork that stands alone. A new
// upstream is taken only if its main came to the fork's base — main's commit at the place the
// fork follows up to — at that same place: then everything the fork inherited to there is the new
// upstream's too, and syncing from it is as from the parent it had. One whose history diverged
// before that place cannot be synced from.
func (s *Store) Sync(ctx context.Context, fork, parent int64, actor string) (SyncResult, error) {
	f, err := s.GetRepository(ctx, fork)
	if err != nil {
		return SyncResult{}, err
	}
	if parent == 0 {
		return SyncResult{}, precondition("repository %d is not a fork, or stands alone now", fork)
	}
	adopt := parent != f.Parent
	if adopt {
		if err := s.checkUpstream(ctx, f, parent); err != nil {
			return SyncResult{}, err
		}
		f.Parent = parent
	}
	p, err := s.GetRepository(ctx, f.Parent)
	if err != nil {
		return SyncResult{}, err
	}
	if err := s.checkMainPlace(ctx, p); err != nil {
		return SyncResult{}, err
	}
	if err := s.compactFully(ctx, f.Parent); err != nil {
		return SyncResult{}, fmt.Errorf("compacting repository %d: %w", f.Parent, err)
	}
	var out SyncResult
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		if adopt {
			if err := tx.SetParent(ctx, fork, f.Parent); err != nil {
				return err
			}
		}
		head, seq, ok, err := tx.Head(ctx, fork, true)
		if err != nil {
			return err
		}
		if !ok {
			return notFound("repository %d", fork)
		}
		cur, ok, err := tx.Repository(ctx, fork)
		if err != nil {
			return err
		}
		if !ok {
			return notFound("repository %d", fork)
		}
		out = SyncResult{Head: head, Seq: seq}
		follow := cur.ParentSeq
		if seq == cur.ParentSeq && p.HeadSeq > seq {
			// Nothing landed of its own: main follows the parent's to its head.
			out = SyncResult{Moved: true, Head: p.Head, Seq: p.HeadSeq}
			follow = p.HeadSeq
			if err := tx.SetHead(ctx, fork, p.Head, p.HeadSeq); err != nil {
				return err
			}
			if err := tx.SetParentSeq(ctx, fork, p.HeadSeq); err != nil {
				return err
			}
			if err := appendEvent(ctx, tx, fork, &scmpb.Event{Kind: &scmpb.Event_HeadMoved{
				HeadMoved: &scmpb.HeadMoved{OldHead: head[:], NewHead: p.Head[:], HeadSeq: p.HeadSeq},
			}}); err != nil {
				return err
			}
		}
		return s.inherit(ctx, tx, fork, f.Parent, follow)
	})
	if err != nil {
		return SyncResult{}, err
	}
	return out, nil
}

// checkUpstream checks a fork may take upstream as its new one: in its network, and its main came
// to the fork's base at the place the fork follows up to.
func (s *Store) checkUpstream(ctx context.Context, f Repository, upstream int64) error {
	u, err := s.GetRepository(ctx, upstream)
	if err != nil {
		return err
	}
	if u.Network != f.Network {
		return precondition("repository %d is not in repository %d's network", upstream, f.ID)
	}
	base, err := s.mainLog(ctx, f.ID, f.ParentSeq, f.ParentSeq, true, 1)
	if err != nil {
		return err
	}
	if len(base) != 1 {
		return fmt.Errorf("repository %d's main at place %d: none", f.ID, f.ParentSeq)
	}
	at, ok, err := s.headSeqOf(ctx, s.ops(ctx), upstream, base[0].Commit)
	if err != nil {
		return err
	}
	if !ok || at != f.ParentSeq {
		return precondition("repository %d's upstream, repository %d, did not come to its base %s at place %d: their histories diverged, and it cannot be synced from it",
			f.ID, upstream, base[0].Commit, f.ParentSeq)
	}
	return nil
}

// inherit makes the parent's live segments of the inherited kinds the ones repo inherits — those
// read along main bounded at place follow, or at the parent's own bound if lower — and grants repo
// the dictionaries the parent may read now. Its content index's pinned runs are among them, so the
// index takes a new generation, which every replica's view reads anew (cindex.go): runs that change
// otherwise than by a merge or a retirement, which change no lookup's answer, are a new generation's.
func (s *Store) inherit(ctx context.Context, tx *txn, repo, parent, follow int64) error {
	var segs []backend.Segment
	for _, kind := range inheritedKinds {
		live, err := tx.Segments(ctx, repoScope(parent), kind)
		if err != nil {
			return err
		}
		for _, sg := range live {
			sg.Scope, sg.Inherited = repoScope(repo), true
			if boundedKinds[kind] && (sg.Bound == backend.NoBound || sg.Bound > follow) {
				sg.Bound = follow
			}
			segs = append(segs, sg)
		}
	}
	if _, err := tx.SetInherited(ctx, repo, segs); err != nil {
		return err
	}
	if _, ok, err := tx.ContentIndexGeneration(ctx, repoScope(repo)); err != nil {
		return err
	} else if ok {
		gen, _, err := tx.lockIndex(ctx, repoScope(repo))
		if err != nil {
			return err
		}
		// Read once the index is held: no fold commits meanwhile.
		st, _, err := tx.ContentIndexGeneration(ctx, repoScope(repo))
		if err != nil {
			return err
		}
		if err := tx.SetContentIndexGeneration(ctx, repoScope(repo), gen+1, st.Folded); err != nil {
			return err
		}
	}
	grants, err := s.readableDictionaries(ctx, parent)
	if err != nil {
		return err
	}
	return tx.SetDictionaryGrants(ctx, repo, grants)
}

// checkMainPlace checks a repository's main is at the generation of its place in main's history,
// as every landing keeps it: a fork bounds both path history and main's log at one place.
func (s *Store) checkMainPlace(ctx context.Context, r Repository) error {
	gen, err := s.generation(ctx, r.ID, r.Head)
	if err != nil {
		return err
	}
	if gen != r.HeadSeq {
		return fmt.Errorf("repository %d's main %s is at generation %d, but place %d in main's history", r.ID, r.Head, gen, r.HeadSeq)
	}
	return nil
}

// compactFully moves everything pinned in a repository's tail into segments, and folds its content
// index, holding its compaction's lease — waiting for another holder to finish, told as it does
// (wait.Claims) — so no other compaction merges its segments meanwhile.
func (s *Store) compactFully(ctx context.Context, repo int64) error {
	held, release, _, err := s.Claims().Claim(ctx, SegmentWork, repo, s.holder, time.Minute, wait.Forever)
	if err != nil {
		return err
	}
	defer release()
	// From the tail's start: what a window looked through before it was pinned moves too.
	if err := s.ops(held).SetCompactionCursor(held, repo, tailCursor{}.encode()); err != nil {
		return err
	}
	for {
		r, err := s.CompactSegments(held, repo, 1)
		if err != nil {
			return err
		}
		if !r.More {
			return nil
		}
	}
}
