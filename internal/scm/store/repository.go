package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
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

// repositoryName is the flat, unique form of a repository name: lowercase letters, digits,
// '.', '_', and '-', starting and ending with a letter or digit, at most 64 characters.
var repositoryName = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,62}[a-z0-9])?$`)

// reservedNames are the paths ok's web site serves beside its
// repositories, each of which is at /<name>.
var reservedNames = map[string]bool{"assets": true, "id": true, "repositories": true, "services": true, "healthz": true, "readyz": true}

func validName(name string) error {
	if !repositoryName.MatchString(name) {
		return invalid("repository name %q: use 1-64 lowercase letters, digits, '.', '_', '-', starting and ending with a letter or digit", name)
	}
	if reservedNames[name] {
		return invalid("repository name %q is reserved for ok's own pages", name)
	}
	return nil
}

// CreateRepository registers a repository whose main is the root commit, created by the
// principal createdBy.
func (s *Store) CreateRepository(ctx context.Context, name, createdBy string) (Repository, error) {
	if err := validName(name); err != nil {
		return Repository{}, err
	}
	var r Repository
	err := s.inTx(ctx, func(tx *txn) error {
		var err error
		r, err = tx.InsertRepository(ctx, name, encoding.RootCommitID, createdBy)
		if errors.Is(err, backend.ErrDuplicate) {
			return errors.Join(ErrAlreadyExists, err)
		}
		if err != nil {
			return err
		}
		// Its content index starts empty: every file and chunk it gets is logged.
		if err := tx.InsertContentIndex(ctx, r.ID, 1); err != nil {
			return err
		}
		return appendEvent(ctx, tx, r.ID, &scmpb.Event{Kind: &scmpb.Event_RepositoryCreated{
			RepositoryCreated: &scmpb.RepositoryCreated{Name: name, CreatedBy: createdBy},
		}})
	})
	return r, err
}

// RenameRepository gives a repository a new name.
func (s *Store) RenameRepository(ctx context.Context, id int64, name string) (Repository, error) {
	if err := validName(name); err != nil {
		return Repository{}, err
	}
	var r Repository
	err := s.inTx(ctx, func(tx *txn) error {
		old, ok, err := tx.LockRepositoryName(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return notFound("repository %d", id)
		}
		r, err = tx.RenameRepository(ctx, id, name)
		if errors.Is(err, backend.ErrDuplicate) {
			return errors.Join(ErrAlreadyExists, err)
		}
		if err != nil || old == name {
			return err
		}
		return appendEvent(ctx, tx, id, &scmpb.Event{Kind: &scmpb.Event_RepositoryRenamed{
			RepositoryRenamed: &scmpb.RepositoryRenamed{OldName: old, Name: name},
		}})
	})
	return r, err
}

// GetRepository returns a repository by ID.
func (s *Store) GetRepository(ctx context.Context, id int64) (Repository, error) {
	r, ok, err := s.b.Repository(ctx, id)
	if err == nil && !ok {
		err = notFound("repository %d", id)
	}
	return r, err
}

// GetRepositoryByName returns a repository by name.
func (s *Store) GetRepositoryByName(ctx context.Context, name string) (Repository, error) {
	r, ok, err := s.b.RepositoryByName(ctx, name)
	if err == nil && !ok {
		err = notFound("repository %q", name)
	}
	return r, err
}

// ListRepositories returns up to pageSize repositories named after `after`, in name
// order, and the name to continue after ("" at the end).
func (s *Store) ListRepositories(ctx context.Context, pageSize int, after string) ([]Repository, string, error) {
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}
	out, err := s.b.Repositories(ctx, after, pageSize+1)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > pageSize {
		out = out[:pageSize]
		next = out[pageSize-1].Name
	}
	return out, next, nil
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
