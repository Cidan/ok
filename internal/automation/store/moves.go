package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/move"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Moving repositories between cells (docs/hosted.md, "Cells, as built"): ok automation's side of a
// move — its rows about the repositories, their runs' logs, the runs workers hold, which are given
// back to the queue once the repositories are frozen, and the landing queues, due again where they
// arrive.

// Mover is ok automation's side of moves; its Purge deletes what moved away, a day after.
func (s *Store) Mover() *move.RepositoryRole {
	return &move.RepositoryRole{RoleName: cells.Automation, Rows: s.b.Moves(), Kept: logObjects{s}, Frozen: s.giveBack, Arrived: s.arrived}
}

// movingReason is why a run a worker held was given back: it runs again where its repository goes.
const movingReason = "its repository is moving to another cell"

// giveBack requeues the runs workers hold of frozen repositories, without spending an attempt:
// they run again, from the start, where the repositories go. No worker claims another of them
// meanwhile, nor may one write about it.
func (s *Store) giveBack(ctx context.Context, repos []int64) error {
	ids, err := s.b.RunningRuns(ctx, repos)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.b.Txn(ctx, func(tx Ops) error {
			run, err := tx.LockRun(ctx, id)
			if err != nil || run.Status != StatusRunning {
				return err
			}
			return tx.RequeueRun(ctx, id, movingReason, false)
		}); err != nil {
			return fmt.Errorf("giving back run %d: %w", id, err)
		}
	}
	return nil
}

// arrived has the repositories' landing queues looked at here: what they held to land came with
// them.
func (s *Store) arrived(ctx context.Context, repos []int64) error {
	for _, r := range repos {
		if err := s.b.MarkWork(ctx, QueueWork, r); err != nil {
			return err
		}
	}
	return nil
}

// logObjects are the repositories' runs' logs: each run's whole log (runs/<id>) and the segments of
// its leases (live/<id>/…), in the order of their runs' IDs, a run's segments before its log.
type logObjects struct{ s *Store }

// runOf is the run a log object is of.
func runOf(key string) (int64, bool) {
	rest, ok := strings.CutPrefix(key, "runs/")
	if !ok {
		if rest, ok = strings.CutPrefix(key, "live/"); !ok {
			return 0, false
		}
		rest, _, _ = strings.Cut(rest, "/")
	}
	id, err := strconv.ParseInt(rest, 10, 64)
	return id, err == nil
}

// runIDs are the scope's runs' IDs from first on (inclusive), up to limit, in order.
func (o logObjects) runIDs(ctx context.Context, sc move.Scope, first int64, limit int) ([]int64, error) {
	var ids []int64
	for _, r := range sc.Repositories {
		page, err := o.s.b.RunIDsOf(ctx, r, first-1, limit)
		if err != nil {
			return nil, err
		}
		ids = append(ids, page...)
	}
	slices.Sort(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

// keysOf are a run's log objects, in order.
func (o logObjects) keysOf(ctx context.Context, id int64) ([]*okpb.MovedObject, error) {
	live, err := o.s.blobs.List(ctx, liveDir(id))
	if err != nil {
		return nil, fmt.Errorf("listing run %d's log segments: %w", id, err)
	}
	var out []*okpb.MovedObject
	for _, ob := range live {
		out = append(out, &okpb.MovedObject{Key: ob.Key, Size: ob.Size})
	}
	slices.SortFunc(out, func(a, b *okpb.MovedObject) int { return strings.Compare(a.Key, b.Key) })
	whole, err := o.s.blobs.Stat(ctx, logKey(id))
	switch {
	case err == nil:
		out = append(out, &okpb.MovedObject{Key: whole.Key, Size: whole.Size})
	case !errors.Is(err, blob.ErrNotFound):
		return nil, err
	}
	return out, nil
}

func (o logObjects) List(ctx context.Context, sc move.Scope, after string, limit int) ([]*okpb.MovedObject, error) {
	first := int64(0)
	if after != "" {
		id, ok := runOf(after)
		if !ok {
			return nil, invalid("%s is no run's log", after)
		}
		first = id
	}
	var out []*okpb.MovedObject
	for len(out) < limit {
		ids, err := o.runIDs(ctx, sc, first, limit)
		if err != nil || len(ids) == 0 {
			return out, err
		}
		for _, id := range ids {
			keys, err := o.keysOf(ctx, id)
			if err != nil {
				return nil, err
			}
			for _, k := range keys {
				if after == "" || strings.Compare(k.Key, after) > 0 || id > first {
					out = append(out, k)
				}
			}
			if len(out) >= limit {
				return out, nil
			}
		}
		first = ids[len(ids)-1] + 1
		after = ""
	}
	return out, nil
}

// ofScope refuses a key that is no log of the scope's runs.
func (o logObjects) ofScope(ctx context.Context, sc move.Scope, key string) error {
	id, ok := runOf(key)
	if !ok {
		return invalid("%s is no run's log", key)
	}
	run, err := o.s.b.Run(ctx, id)
	if err != nil {
		return err
	}
	if !sc.Has(run.RepositoryID) {
		return invalid("run %d is none of the moving repositories'", id)
	}
	return nil
}

func (o logObjects) Open(ctx context.Context, sc move.Scope, key string) (io.ReadCloser, error) {
	if err := o.ofScope(ctx, sc, key); err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	go func() {
		_, err := o.s.blobs.Download(ctx, key, pw)
		pw.CloseWithError(err)
	}()
	return pr, nil
}

func (o logObjects) Has(ctx context.Context, key string) (bool, error) {
	_, err := o.s.blobs.Stat(ctx, key)
	if errors.Is(err, blob.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (o logObjects) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if _, ok := runOf(key); !ok {
		return invalid("%s is no run's log", key)
	}
	return o.s.blobs.Put(ctx, key, r, size)
}

// Drop deletes the scope's runs' logs, as a purge does.
func (o logObjects) Drop(ctx context.Context, sc move.Scope) error {
	for _, r := range sc.Repositories {
		var after int64
		for {
			ids, err := o.s.b.RunIDsOf(ctx, r, after, purgePage)
			if err != nil {
				return err
			}
			for _, id := range ids {
				keys, err := o.keysOf(ctx, id)
				if err != nil {
					return err
				}
				if len(keys) == 0 {
					continue
				}
				names := make([]string, len(keys))
				for i, k := range keys {
					names[i] = k.Key
				}
				if err := o.s.blobs.Delete(ctx, names...); err != nil {
					return fmt.Errorf("deleting run %d's log: %w", id, err)
				}
			}
			if len(ids) < purgePage {
				break
			}
			after = ids[len(ids)-1]
		}
	}
	return nil
}
