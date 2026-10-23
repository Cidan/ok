package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Repository is one repository's registry row: Head is main (the root commit until the first
// landing), and CreatedBy the principal that created it.
type Repository = backend.Repository

// CreateRepository makes repository id, forked from none, the first of network — the ID and network
// the directory chose — whose main is the root commit, created by the principal createdBy.
func (s *Store) CreateRepository(ctx context.Context, id, network int64, createdBy string) (Repository, error) {
	if id == 0 || network == 0 {
		return Repository{}, invalid("a repository is made under the ID and network the directory chose")
	}
	var r Repository
	err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		net := backend.Network(network)
		var err error
		r, err = tx.InsertRepository(ctx, id, net, encoding.RootCommitID, createdBy)
		if errors.Is(err, backend.ErrDuplicate) {
			return errors.Join(ErrAlreadyExists, err)
		}
		if err != nil {
			return err
		}
		// A repository forked from none is the first of its network: its content index and its
		// network's start empty, and every file and chunk it gets, and where each chunk is, is
		// logged.
		if err := tx.InsertContentIndex(ctx, repoScope(r.ID), 1); err != nil {
			return err
		}
		if err := tx.InsertContentIndex(ctx, backend.NetworkScope(net), 1); err != nil {
			return err
		}
		return appendEvent(ctx, tx, r.ID, &scmpb.Event{Kind: &scmpb.Event_RepositoryCreated{
			RepositoryCreated: &scmpb.RepositoryCreated{CreatedById: createdBy},
		}})
	})
	return r, err
}

// GetRepository returns a repository by ID.
func (s *Store) GetRepository(ctx context.Context, id int64) (Repository, error) {
	r, ok, err := s.ops(ctx).Repository(ctx, id)
	if err == nil && !ok {
		err = notFound("repository %d", id)
	}
	return r, err
}

func (s *Store) repositoryExists(ctx context.Context, q backend.Ops, id int64) error {
	ok, err := q.RepositoryExists(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return notFound("repository %d", id)
	}
	return nil
}

// appendEvent records ev with the next sequence number (see appendEvents).
func appendEvent(ctx context.Context, tx *txn, repositoryID int64, ev *scmpb.Event) error {
	return appendEvents(ctx, tx, repositoryID, []*scmpb.Event{ev})
}

// eventChannel is the notification channel an event's commit signals, with its repository.
const eventChannel = "ok_scm_events"

// appendEvents records evs with the next places in their repository's log, in order, for the
// relay to publish, and wakes the relays when the transaction commits. The repository's counter's
// lock is held until commit, so its log's order is commit order. Call it last, just before
// commit: the counter is the transaction's last lock, held briefly and never in a deadlock.
func appendEvents(ctx context.Context, tx *txn, repositoryID int64, evs []*scmpb.Event) error {
	if len(evs) == 0 {
		return nil
	}
	last, err := tx.ReserveRepositoryEvents(ctx, repositoryID, len(evs))
	if err != nil {
		return err
	}
	// Main moved: its code is indexed again (codesearch.go).
	for _, ev := range evs {
		if ev.GetHeadMoved() != nil {
			if err := tx.MarkWork(ctx, CodeSearchWork, repositoryID); err != nil {
				return err
			}
			break
		}
	}
	at := time.Now()
	seqs := make([]int64, len(evs))
	bodies := make([][]byte, len(evs))
	for i, ev := range evs {
		seqs[i] = last - int64(len(evs)) + 1 + int64(i)
		ev.RepositoryId, ev.RepositorySeq, ev.At = repositoryID, seqs[i], timestamppb.New(at)
		body, err := proto.Marshal(ev)
		if err != nil {
			return err
		}
		bodies[i] = body
	}
	if err := tx.InsertEvents(ctx, repositoryID, at, seqs, bodies); err != nil {
		return err
	}
	return tx.Notify(ctx, eventChannel, fmt.Sprint(repositoryID))
}
