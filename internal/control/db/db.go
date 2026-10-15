// Package db is ok control's database (migrations/): the projection of the SCM's CLs, review —
// reviewers, label grants, comments, each CL's change log — and favorites. Every multi-row write is one transaction, retried on deadlock
// and serialization failure.
package db

import (
	"context"
	"embed"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/migrate"
)

// DB is ok control's database.
type DB struct {
	pool  *pgxpool.Pool
	locks *lock.Lock
}

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are ok control's schema.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Open brings the database behind pool to ok control's schema and returns it.
func Open(ctx context.Context, pool *pgxpool.Pool) (*DB, error) {
	if _, err := migrate.Apply(ctx, pool, "control", Migrations); err != nil {
		return nil, err
	}
	return &DB{pool: pool, locks: lock.New(pool)}, nil
}

// Pool is the underlying pool.
func (d *DB) Pool() *pgxpool.Pool { return d.pool }

// Querier runs queries, inside a transaction or not.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Q is the non-transactional querier.
func (d *DB) Q() Querier { return d.pool }

// InTx runs f in a transaction, again if PostgreSQL aborts it to break a deadlock or a
// serialization conflict.
func (d *DB) InTx(ctx context.Context, f func(tx pgx.Tx) error) error {
	for attempt := 0; ; attempt++ {
		err := pgx.BeginFunc(ctx, d.pool, f)
		var pg *pgconn.PgError
		if err == nil || attempt == 4 || !errors.As(err, &pg) || (pg.Code != "40P01" && pg.Code != "40001") {
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
}

// Status is a CL's lifecycle status; the values are the SCM's.
type Status int16

const (
	StatusDraft     Status = 1
	StatusOpen      Status = 2
	StatusSubmitted Status = 3
	StatusAbandoned Status = 4
)

// Active reports whether a CL in this status takes patchsets and grants.
func (s Status) Active() bool { return s == StatusDraft || s == StatusOpen }

// ChangeList is the projection of an SCM CL, with the approved projection.
type ChangeList struct {
	Repo, Number    int64
	ChangeID        []byte
	Author          string
	Status          Status
	ParentNumber    int64 // 0: not stacked
	CurrentPatchset int32
	Description     string
	LandedCommit    []byte
	SubmittedBy     string
	Approved        bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
	// RollsBack are the CLs a rollback CL rolls back; empty for any other CL.
	RollsBack []int64
}

// Title is the description's first line.
func (c ChangeList) Title() string {
	t, _, _ := strings.Cut(c.Description, "\n")
	return strings.TrimSpace(t)
}

const clColumns = `repository_id, number, change_id, author, status, coalesce(parent_number, 0), current_patchset,
	description, landed_commit, coalesce(submitted_by, ''), approved, created_at, updated_at, rolls_back`

func scanCL(row pgx.Row) (ChangeList, error) {
	var c ChangeList
	err := row.Scan(&c.Repo, &c.Number, &c.ChangeID, &c.Author, &c.Status, &c.ParentNumber, &c.CurrentPatchset,
		&c.Description, &c.LandedCommit, &c.SubmittedBy, &c.Approved, &c.CreatedAt, &c.UpdatedAt, &c.RollsBack)
	return c, err
}

func collectCLs(rows pgx.Rows, err error) ([]ChangeList, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChangeList
	for rows.Next() {
		c, err := scanCL(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// UpsertChangeList records the SCM's state of a CL, unless the projection already holds a
// newer one (the feed and an action can race to record the same CL). The approved
// projection is left as it is. It reports whether it wrote.
func UpsertChangeList(ctx context.Context, q Querier, c ChangeList) (bool, error) {
	var parent *int64
	if c.ParentNumber != 0 {
		parent = &c.ParentNumber
	}
	var submitted *string
	if c.SubmittedBy != "" {
		submitted = &c.SubmittedBy
	}
	rollsBack := c.RollsBack
	if rollsBack == nil {
		rollsBack = []int64{}
	}
	tag, err := q.Exec(ctx, `INSERT INTO changelist (repository_id, number, change_id, author, status, parent_number,
			current_patchset, description, landed_commit, submitted_by, created_at, updated_at, rolls_back)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (repository_id, number) DO UPDATE SET
			status = EXCLUDED.status, parent_number = EXCLUDED.parent_number,
			current_patchset = EXCLUDED.current_patchset, description = EXCLUDED.description,
			landed_commit = EXCLUDED.landed_commit, submitted_by = EXCLUDED.submitted_by,
			updated_at = EXCLUDED.updated_at
		WHERE changelist.updated_at <= EXCLUDED.updated_at`,
		c.Repo, c.Number, c.ChangeID, c.Author, c.Status, parent, c.CurrentPatchset, c.Description,
		c.LandedCommit, submitted, c.CreatedAt, c.UpdatedAt, rollsBack)
	return tag.RowsAffected() > 0, err
}

// RollbacksOf lists the rollback CLs that roll back a CL, oldest first.
func RollbacksOf(ctx context.Context, q Querier, repo, number int64) ([]ChangeList, error) {
	return collectCLs(q.Query(ctx, `SELECT `+clColumns+` FROM changelist
		WHERE repository_id = $1 AND rolls_back @> ARRAY[$2::bigint] ORDER BY number`, repo, number))
}

// GetChangeList reads a CL's projection.
func GetChangeList(ctx context.Context, q Querier, repo, number int64) (ChangeList, bool, error) {
	c, err := scanCL(q.QueryRow(ctx, `SELECT `+clColumns+` FROM changelist WHERE repository_id = $1 AND number = $2`, repo, number))
	if errors.Is(err, pgx.ErrNoRows) {
		return ChangeList{}, false, nil
	}
	return c, err == nil, err
}

// LockChangeList reads a CL's projection under its row lock.
func LockChangeList(ctx context.Context, tx pgx.Tx, repo, number int64) (ChangeList, bool, error) {
	c, err := scanCL(tx.QueryRow(ctx, `SELECT `+clColumns+` FROM changelist WHERE repository_id = $1 AND number = $2 FOR UPDATE`, repo, number))
	if errors.Is(err, pgx.ErrNoRows) {
		return ChangeList{}, false, nil
	}
	return c, err == nil, err
}

// Filter is a CL finder view.
type Filter string

const (
	FilterOpen        Filter = "open"
	FilterDrafts      Filter = "drafts"
	FilterNeedsReview Filter = "review"
	FilterMerged      Filter = "merged"
	FilterAbandoned   Filter = "abandoned"
	FilterAll         Filter = "all"
)

// Filters are the finder's views, in order.
var Filters = []Filter{FilterOpen, FilterDrafts, FilterNeedsReview, FilterMerged, FilterAbandoned, FilterAll}

// ParseFilter reads a view name; anything unknown is Open.
func ParseFilter(s string) Filter {
	for _, f := range Filters {
		if string(f) == s {
			return f
		}
	}
	return FilterOpen
}

// ListChangeLists lists a repository's CLs for a finder view, most recently updated
// first: Open is the open CLs plus the viewer's own drafts; Needs My Review the open CLs
// the viewer reviews and grants nothing on.
func ListChangeLists(ctx context.Context, q Querier, repo int64, f Filter, viewer string, limit int) ([]ChangeList, error) {
	where := `repository_id = $1`
	args := []any{repo}
	switch f {
	case FilterOpen:
		where += ` AND (status = 2 OR (status = 1 AND author = $2))`
		args = append(args, viewer)
	case FilterDrafts:
		where += ` AND status = 1`
	case FilterNeedsReview:
		where += ` AND status = 2 AND EXISTS (SELECT 1 FROM reviewer r WHERE r.repository_id = changelist.repository_id
			AND r.number = changelist.number AND r.principal = $2)
			AND NOT EXISTS (SELECT 1 FROM label_grant g WHERE g.repository_id = changelist.repository_id
			AND g.number = changelist.number AND g.principal = $2 AND g.granted)`
		args = append(args, viewer)
	case FilterMerged:
		where += ` AND status = 3`
	case FilterAbandoned:
		where += ` AND status = 4`
	}
	args = append(args, limit)
	return collectCLs(q.Query(ctx, `SELECT `+clColumns+` FROM changelist WHERE `+where+
		` ORDER BY updated_at DESC, number DESC LIMIT $`+strconv.Itoa(len(args)), args...))
}

// ActiveChangeLists lists a repository's drafts and open CLs.
func ActiveChangeLists(ctx context.Context, q Querier, repo int64) ([]ChangeList, error) {
	return collectCLs(q.Query(ctx, `SELECT `+clColumns+` FROM changelist WHERE repository_id = $1 AND status IN (1, 2) ORDER BY number`, repo))
}

// Stack is the CL's stack: its ancestors, itself, and its descendants, bottom first,
// each descendant after its parent.
func Stack(ctx context.Context, q Querier, repo, number int64) ([]ChangeList, error) {
	anc, err := collectCLs(q.Query(ctx, `WITH RECURSIVE up(number, depth) AS (
			SELECT $2::bigint, 0
			UNION ALL
			SELECT c.parent_number, up.depth + 1 FROM up JOIN changelist c ON c.repository_id = $1 AND c.number = up.number
			WHERE c.parent_number IS NOT NULL
		)
		SELECT `+clColumns+` FROM changelist JOIN up USING (number) WHERE repository_id = $1 ORDER BY up.depth DESC`, repo, number))
	if err != nil {
		return nil, err
	}
	desc, err := collectCLs(q.Query(ctx, `WITH RECURSIVE down(number, depth) AS (
			SELECT number, 1 FROM changelist WHERE repository_id = $1 AND parent_number = $2
			UNION ALL
			SELECT c.number, down.depth + 1 FROM down JOIN changelist c ON c.repository_id = $1 AND c.parent_number = down.number
		)
		SELECT `+clColumns+` FROM changelist JOIN down USING (number) WHERE repository_id = $1 ORDER BY down.depth, number`, repo, number))
	if err != nil {
		return nil, err
	}
	return append(anc, desc...), nil
}

// LandedBy maps landed commits to the CLs that landed as them.
func LandedBy(ctx context.Context, q Querier, repo int64, commits [][]byte) (map[string]int64, error) {
	rows, err := q.Query(ctx, `SELECT landed_commit, number FROM changelist WHERE repository_id = $1 AND landed_commit = ANY($2::bytea[])`, repo, commits)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var c []byte
		var n int64
		if err := rows.Scan(&c, &n); err != nil {
			return nil, err
		}
		out[string(c)] = n
	}
	return out, rows.Err()
}

// SetApproved stores the approved projection, reporting whether it changed.
func SetApproved(ctx context.Context, q Querier, repo, number int64, approved bool) (bool, error) {
	tag, err := q.Exec(ctx, `UPDATE changelist SET approved = $3 WHERE repository_id = $1 AND number = $2 AND approved <> $3`, repo, number, approved)
	return tag.RowsAffected() > 0, err
}

// Cursor is the last SCM event the projection has applied (0: none).
func Cursor(ctx context.Context, q Querier) (int64, error) {
	var seq int64
	err := q.QueryRow(ctx, `SELECT coalesce((SELECT seq FROM feed_cursor), 0)`).Scan(&seq)
	return seq, err
}

// SetCursor records the last SCM event applied.
func SetCursor(ctx context.Context, q Querier, seq int64) error {
	_, err := q.Exec(ctx, `INSERT INTO feed_cursor (seq) VALUES ($1) ON CONFLICT (id) DO UPDATE SET seq = EXCLUDED.seq`, seq)
	return err
}

// LeadFeed runs fn holding the lock (internal/lock) whose holder consumes the SCM's
// change feed, with a context that ends if the lock is lost, and gives the lock up when fn
// returns. It returns lock.ErrNotAcquired when another replica holds the lock, and otherwise
// fn's error.
func (d *DB) LeadFeed(ctx context.Context, fn lock.LockFn) error {
	return d.locks.LockFn(ctx, "leaders", "feed", 0, fn)
}

// Repository is the projection of an SCM repository.
type Repository struct {
	ID        int64
	Name      string
	CreatedBy string
	CreatedAt time.Time
	HeadSeq   int64
	UpdatedAt time.Time
	Favorite  bool // for the principal the listing is for
}

// CreateRepository records a repository the SCM created. The request that created it and the
// change-feed follower both record it, often at once, so a row already there — by its ID, or
// by its name — is left as it is: a conflict clause naming only the ID would let the second of
// two concurrent inserts fail on the name instead. The feed applies events in the SCM's order,
// so a name another repository still holds here (renamed in the SCM, the rename not yet
// applied) is recorded when the feed reaches the creation.
func CreateRepository(ctx context.Context, q Querier, r Repository) error {
	_, err := q.Exec(ctx, `INSERT INTO repository (id, name, created_by, created_at, head_seq, updated_at)
		VALUES ($1, $2, $3, $4, $5, $4) ON CONFLICT DO NOTHING`, r.ID, r.Name, r.CreatedBy, r.CreatedAt, r.HeadSeq)
	return err
}

// RenameRepository records a rename.
func RenameRepository(ctx context.Context, q Querier, id int64, name string) error {
	_, err := q.Exec(ctx, `UPDATE repository SET name = $2 WHERE id = $1`, id, name)
	return err
}

// MoveHead records main moving: its commit count and when.
func MoveHead(ctx context.Context, q Querier, id, headSeq int64, at time.Time) error {
	_, err := q.Exec(ctx, `UPDATE repository SET head_seq = $2, updated_at = $3 WHERE id = $1 AND head_seq < $2`, id, headSeq, at)
	return err
}

// ListRepositories lists the repositories whose name contains keyword (all of them for
// ""), a page at a time: principal's favorites first, then the most recently updated.
func ListRepositories(ctx context.Context, q Querier, principal, keyword string, offset, limit int) ([]Repository, error) {
	pattern := "%" + strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(strings.ToLower(keyword)) + "%"
	rows, err := q.Query(ctx, `SELECT r.id, r.name, r.created_by, r.created_at, r.head_seq, r.updated_at, f.principal IS NOT NULL
		FROM repository r LEFT JOIN favorite f ON f.repository_id = r.id AND f.principal = $1
		WHERE r.name LIKE $2
		ORDER BY f.principal IS NOT NULL DESC, r.updated_at DESC, r.id
		OFFSET $3 LIMIT $4`, principal, pattern, offset, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Repository
	for rows.Next() {
		var r Repository
		if err := rows.Scan(&r.ID, &r.Name, &r.CreatedBy, &r.CreatedAt, &r.HeadSeq, &r.UpdatedAt, &r.Favorite); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
