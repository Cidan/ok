package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/scm/encoding"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Repository is one repository's registry row.
type Repository struct {
	ID        int64
	Name      string
	Head      encoding.ID // main; the root commit until the first landing
	HeadSeq   int64
	CreatedAt time.Time
	// CreatedBy is the principal that created it, who may delete it.
	CreatedBy string
}

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

func isUniqueViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}

const repositoryColumns = `id, name, head, head_seq, created_at, created_by`

func scanRepository(row pgx.Row) (Repository, error) {
	var r Repository
	var head []byte
	if err := row.Scan(&r.ID, &r.Name, &head, &r.HeadSeq, &r.CreatedAt, &r.CreatedBy); err != nil {
		return r, err
	}
	var err error
	r.Head, err = toID(head)
	return r, err
}

// CreateRepository registers a repository whose main is the root commit, created by the
// principal createdBy.
func (s *Store) CreateRepository(ctx context.Context, name, createdBy string) (Repository, error) {
	if err := validName(name); err != nil {
		return Repository{}, err
	}
	var r Repository
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		r, err = scanRepository(tx.QueryRow(ctx,
			`INSERT INTO repository (name, head, created_by) VALUES ($1, $2, $3) RETURNING `+repositoryColumns,
			name, encoding.RootCommitID[:], createdBy))
		if isUniqueViolation(err) {
			return errors.Join(ErrAlreadyExists, err)
		}
		if err != nil {
			return err
		}
		// Its content index starts empty: every file and chunk it gets is logged.
		if _, err := tx.Exec(ctx, `INSERT INTO content_index (repository_id, generation) VALUES ($1, 1)`, r.ID); err != nil {
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
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var old string
		if err := tx.QueryRow(ctx, `SELECT name FROM repository WHERE id = $1 FOR UPDATE`, id).Scan(&old); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return notFound("repository %d", id)
			}
			return err
		}
		var err error
		r, err = scanRepository(tx.QueryRow(ctx,
			`UPDATE repository SET name = $2 WHERE id = $1 RETURNING `+repositoryColumns, id, name))
		if isUniqueViolation(err) {
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
	r, err := scanRepository(s.pool.QueryRow(ctx, `SELECT `+repositoryColumns+` FROM repository WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return r, notFound("repository %d", id)
	}
	return r, err
}

// GetRepositoryByName returns a repository by name.
func (s *Store) GetRepositoryByName(ctx context.Context, name string) (Repository, error) {
	r, err := scanRepository(s.pool.QueryRow(ctx, `SELECT `+repositoryColumns+` FROM repository WHERE name = $1`, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return r, notFound("repository %q", name)
	}
	return r, err
}

// ListRepositories returns up to pageSize repositories named after `after`, in name
// order, and the name to continue after ("" at the end).
func (s *Store) ListRepositories(ctx context.Context, pageSize int, after string) ([]Repository, string, error) {
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}
	rows, err := s.pool.Query(ctx, `SELECT `+repositoryColumns+` FROM repository WHERE name > $1 ORDER BY name LIMIT $2`, after, pageSize+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []Repository
	for rows.Next() {
		r, err := scanRepository(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > pageSize {
		out = out[:pageSize]
		next = out[pageSize-1].Name
	}
	return out, next, nil
}

func (s *Store) repositoryExists(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id int64) error {
	var ok bool
	if err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM repository WHERE id = $1)`, id).Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return notFound("repository %d", id)
	}
	return nil
}

// appendEvent records ev with the next sequence number (see appendEvents).
func appendEvent(ctx context.Context, tx pgx.Tx, repositoryID int64, ev *scmpb.Event) error {
	return appendEvents(ctx, tx, repositoryID, []*scmpb.Event{ev})
}

// eventChannel is the PostgreSQL notification channel an event commit signals.
const eventChannel = "ok_scm_events"

// appendEvents records evs with the next sequence numbers, in order, and notifies
// watchers when the transaction commits. The counter row's lock is held until commit, so
// sequence order is commit order; call it last, just before commit, so the global lock
// is held briefly and never joins a deadlock.
func appendEvents(ctx context.Context, tx pgx.Tx, repositoryID int64, evs []*scmpb.Event) error {
	if len(evs) == 0 {
		return nil
	}
	var last int64
	if err := tx.QueryRow(ctx, `INSERT INTO event_counter (last) VALUES ($1)
		ON CONFLICT (id) DO UPDATE SET last = event_counter.last + EXCLUDED.last RETURNING last`, len(evs)).Scan(&last); err != nil {
		return err
	}
	at := time.Now()
	seqs := make([]int64, len(evs))
	bodies := make([][]byte, len(evs))
	for i, ev := range evs {
		seqs[i] = last - int64(len(evs)) + 1 + int64(i)
		ev.Seq, ev.RepositoryId, ev.At = seqs[i], repositoryID, timestamppb.New(at)
		body, err := proto.Marshal(ev)
		if err != nil {
			return err
		}
		bodies[i] = body
	}
	if _, err := tx.Exec(ctx, `INSERT INTO event (seq, repository_id, at, body)
		SELECT u.seq, $1, $2, u.body FROM unnest($3::bigint[], $4::bytea[]) AS u(seq, body)`,
		repositoryID, at, seqs, bodies); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, eventChannel, fmt.Sprint(last))
	return err
}
