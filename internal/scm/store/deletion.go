package store

import (
	"context"
	"errors"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// Deleting a repository — the storage half (docs/hosted.md, "Fork networks, as built", "Deleting");
// who may delete, and soft deletion, are phase 4's. Its tail goes first, a transaction of at most
// Config.DeleteBatch rows at a time — a repository's tail is more than one transaction holds on
// Spanner — then its row with what is left, and its forks stand alone, keeping all they inherited.
// Nothing reads it meanwhile: the directory deleted it before the SCM purges it. Then every chunk it held is released to its
// network, whose collection drops the locations no other repository holds; its segment references
// are retired, so an object goes once no other manifest names it; and its content index goes. A
// network whose last repository goes is deleted whole.

// DeleteRepository deletes a repository and everything only it holds — or finishes its deletion,
// should it have been stopped short: the transaction that deletes its row marks its purge's work
// (PurgeWork), whose job finishes it should this one stop. ErrNotFound for none, deleted or not.
func (s *Store) DeleteRepository(ctx context.Context, repo int64) error {
	if d, ok, err := s.ops(ctx).PendingDeletion(ctx, repo); err != nil || ok {
		if err != nil {
			return err
		}
		return s.finishDeletion(ctx, d)
	}
	if err := s.deleteTail(ctx, func(ctx context.Context, tx *txn, limit int) (int64, error) {
		return tx.DeleteRepositoryTail(ctx, repo, limit)
	}); err != nil {
		return err
	}
	var net backend.Network
	err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		var ok bool
		var err error
		if net, ok, err = tx.DeleteRepository(ctx, repo); err != nil {
			return err
		}
		if !ok {
			return notFound("repository %d", repo)
		}
		return tx.MarkWork(ctx, PurgeWork, repo)
	})
	if err != nil {
		return err
	}
	s.networks.Delete(repo)
	return s.finishDeletion(ctx, backend.Deletion{Repo: repo, Net: net})
}

// FinishDeletion finishes a repository's deletion, should one have been stopped short; it reports
// whether there was one.
func (s *Store) FinishDeletion(ctx context.Context, repo int64) (bool, error) {
	d, ok, err := s.ops(ctx).PendingDeletion(ctx, repo)
	if err != nil || !ok {
		return false, err
	}
	return true, s.finishDeletion(ctx, d)
}

// finishDeletion releases every chunk and tree a deleted repository's content index held, a batch at
// a time — again, after a crash: a release is recorded once — retires its segment references and
// deletes its content index, then deletes its network if it was the last of it, or collects the
// network's releases.
func (s *Store) finishDeletion(ctx context.Context, d backend.Deletion) error {
	if err := s.dropCodeIndex(ctx, d.Repo); err != nil {
		return err
	}
	if err := s.dropCheckouts(ctx, d.Repo); err != nil {
		return err
	}
	repos, err := s.ops(ctx).NetworkRepositories(ctx, d.Net)
	if err != nil {
		return err
	}
	if len(repos) > 0 {
		v, err := s.contentView(ctx, repoScope(d.Repo))
		if err != nil {
			// Another replica finished it first: its content index is gone.
			if _, ok, gerr := s.ops(ctx).ContentIndexGeneration(ctx, repoScope(d.Repo)); gerr == nil && !ok {
				return nil
			}
			return err
		}
		var batch []encoding.ID
		var rerr error
		flush := func() bool {
			if len(batch) > 0 {
				rerr = s.inTx(ctx, func(ctx context.Context, tx *txn) error { return tx.ReleaseChunks(ctx, d.Net, batch) })
			}
			batch = batch[:0]
			return rerr == nil
		}
		if err := v.each(ctx, func(id encoding.ID, e contentindex.Entry) bool {
			if contentindex.Located(e.Kind) {
				batch = append(batch, id)
			}
			return len(batch) < gcBatch || flush()
		}); err != nil {
			return err
		}
		if !flush() {
			return rerr
		}
	}
	if err := s.deleteTail(ctx, func(ctx context.Context, tx *txn, limit int) (int64, error) {
		return tx.DeleteContentIndexTail(ctx, repoScope(d.Repo), limit)
	}); err != nil {
		return err
	}
	if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		return tx.FinishDeletion(ctx, d.Repo)
	}); err != nil {
		return err
	}
	s.forgetIndex(repoScope(d.Repo))
	if len(repos) == 0 {
		return s.deleteNetwork(ctx, d.Net)
	}
	_, _, err = s.collectReleases(ctx, d.Net, false)
	return err
}

// deleteNetwork deletes a network no repository is in any longer: every object under its keys, then
// its rows. Nothing reads it.
func (s *Store) deleteNetwork(ctx context.Context, net backend.Network) error {
	for _, prefix := range []string{packPrefix(net), segmentPrefix(net)} {
		objs, err := s.blobs.List(ctx, prefix)
		if err != nil {
			return err
		}
		for _, o := range objs {
			if err := s.blobs.Delete(ctx, o.Key); err != nil && !errors.Is(err, blob.ErrNotFound) {
				return err
			}
		}
	}
	if err := s.deleteTail(ctx, func(ctx context.Context, tx *txn, limit int) (int64, error) {
		return tx.DeleteNetworkTail(ctx, net, limit)
	}); err != nil {
		return err
	}
	if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		return tx.DeleteNetwork(ctx, net)
	}); err != nil {
		return err
	}
	s.forgetIndex(backend.NetworkScope(net))
	return nil
}

// deleteTail runs del, each time in a transaction of its own deleting up to Config.DeleteBatch
// rows, until it deletes fewer.
func (s *Store) deleteTail(ctx context.Context, del func(context.Context, *txn, int) (int64, error)) error {
	for {
		var n int64
		if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			var err error
			n, err = del(ctx, tx, s.deletes)
			return err
		}); err != nil {
			return err
		}
		if n < int64(s.deletes) {
			return nil
		}
	}
}
