package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// What garbage collection need not walk again (docs/scm.md, "Garbage collection and compaction").
// An operation log keeps every operation its heads reach, and what those name, for good; a
// collection takes only what an operation abandoned within the collection window reached. So the
// content a settled operation or view reaches — its view's commits, an operation's commit
// predecessors — is pinned as compaction moves the operation into a segment (pinOpContent), and
// the mark reads the tail's operations alone once a repository's earlier settled ones are pinned
// too (OpContentPinned). Likewise a checkpoint kept past its first week has its trees pinned
// (pinKeptCheckpoints), and the mark reads only the checkpoints whose trees are not. A pass then
// costs what was written within the windows, not the repository's history.

// pinBatch bounds the roots one pin transaction walks from.
const pinBatch = 200

// pinRoots pins what roots reach, pinBatch roots a transaction, and marks the repository's
// compaction to move what it pinned. A root under which the repository lacks an object — or that
// it lacks — is passed over and returned: what the mark keeps is never missing, so one missing is
// reported, not refused. Each root is ref as refOf makes it: an object's of its kind.
func (s *Store) pinRoots(ctx context.Context, repo int64, roots []ref) (missing []ref, err error) {
	for start := 0; start < len(roots); start += pinBatch {
		batch := roots[start:min(start+pinBatch, len(roots))]
		var lacked []ref
		if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			var err error
			lacked, err = s.walk(ctx, tx, repo, batch, pinWalk)
			if err == nil {
				tx.markAfter(SegmentWork, repo)
			}
			return err
		}); err != nil {
			return missing, err
		}
		missing = append(missing, lacked...)
	}
	return missing, nil
}

// opRoots are the commits operations and views name: a view's heads, bookmarks, tags, and
// working copies, and an operation's commits with their predecessors.
func opRoots(recs []backend.OpRecord) ([]ref, error) {
	seen := map[encoding.ID]bool{}
	var out []ref
	add := func(id encoding.ID) {
		if id != encoding.RootCommitID && !seen[id] {
			seen[id] = true
			out = append(out, ref{table: tCommit, id: id})
		}
	}
	for _, r := range recs {
		o, err := decodeStored(r.Kind, r.ID, r.Body)
		if err != nil {
			return nil, err
		}
		switch o := o.(type) {
		case *encoding.View:
			for _, c := range viewCommits(o) {
				add(c)
			}
		case *encoding.Operation:
			for c, preds := range o.CommitPredecessors {
				add(c)
				for _, p := range preds {
					add(p)
				}
			}
		}
	}
	return out, nil
}

// pinOpContent pins what settled operations and views reach, before compaction moves them into
// a segment.
func (s *Store) pinOpContent(ctx context.Context, repo int64, recs []backend.OpRecord) error {
	roots, err := opRoots(recs)
	if err != nil {
		return err
	}
	missing, err := s.pinRoots(ctx, repo, roots)
	if len(missing) > 0 {
		slog.Warn("scm: settled operations name what the repository lacks", slog.Int64("repository", repo), slog.Int("missing", len(missing)))
	}
	return err
}

// pinSettledOps pins, once, what a repository's operations and views in segments reach, if they
// moved there before compaction pinned it — each log at a time — and records the repository's so
// (OpContentPinned). Then the mark reads only the tail's.
func (s *Store) pinSettledOps(ctx context.Context, repo int64) error {
	done, err := s.ops(ctx).OpContentPinned(ctx, repo)
	if err != nil || done {
		return err
	}
	logs, err := s.ops(ctx).AllOpLogs(ctx, repo)
	if err != nil {
		return err
	}
	for _, log := range logs {
		var recs []backend.OpRecord
		for _, kind := range []encoding.Kind{encoding.KindOperation, encoding.KindView} {
			ops, err := s.logOps(ctx, repo, log, kind)
			if err != nil {
				return err
			}
			for _, o := range ops {
				if o.inSegment {
					recs = append(recs, o.OpRecord)
				}
			}
		}
		if err := s.pinOpContent(ctx, repo, recs); err != nil {
			return err
		}
	}
	return s.ops(ctx).SetOpContentPinned(ctx, repo)
}

// pinKeptCheckpoints pins the trees of the checkpoints made before before — past every
// checkpoint's window, kept by thinning — whose trees are not pinned yet.
func (s *Store) pinKeptCheckpoints(ctx context.Context, repo int64, before time.Time) (int, error) {
	pinned := 0
	for {
		cps, err := s.ops(ctx).CheckpointsToPin(ctx, repo, before, pinBatch)
		if err != nil || len(cps) == 0 {
			return pinned, err
		}
		var roots []ref
		for _, c := range cps {
			for _, t := range c.Trees {
				if t != encoding.EmptyTreeID {
					roots = append(roots, refOf(encoding.KindTree, t))
				}
			}
		}
		missing, err := s.pinRoots(ctx, repo, roots)
		if err != nil {
			return pinned, err
		}
		// A checkpoint is recorded pinned only if all its trees are: one that names a tree the
		// repository lacks stays a root of the mark, and is reported.
		lacked := map[encoding.ID]bool{}
		for _, m := range missing {
			lacked[m.id] = true
		}
		var ids []int64
		for _, c := range cps {
			whole := true
			for _, t := range c.Trees {
				whole = whole && !lacked[t]
			}
			if whole {
				ids = append(ids, c.ID)
			}
		}
		if len(missing) > 0 {
			slog.Warn("scm: kept checkpoints name trees the repository lacks", slog.Int64("repository", repo),
				slog.Int("missing", len(missing)), slog.Int("checkpoints", len(cps)-len(ids)))
		}
		if len(ids) > 0 {
			if err := s.ops(ctx).SetCheckpointsPinned(ctx, repo, ids); err != nil {
				return pinned, err
			}
		}
		pinned += len(ids)
		if len(cps) < pinBatch || len(ids) == 0 {
			return pinned, nil
		}
	}
}

// opPage bounds the tail's operations or views the mark reads at once.
const opPage = 5000

// eachTailOpRoots calls fn with the commits a page of the tail's operations and views name, a
// page at a time.
func (s *Store) eachTailOpRoots(ctx context.Context, repo int64, fn func([]ref) error) error {
	for _, kind := range []encoding.Kind{encoding.KindView, encoding.KindOperation} {
		var after backend.OpKey
		for {
			page, err := s.ops(ctx).TailOps(ctx, repo, kind, after, opPage)
			if err != nil {
				return err
			}
			roots, err := opRoots(page)
			if err != nil {
				return err
			}
			if err := fn(roots); err != nil {
				return err
			}
			if len(page) < opPage {
				break
			}
			after = page[len(page)-1].OpKey
		}
	}
	return nil
}
