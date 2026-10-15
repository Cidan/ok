package db

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Reviewer is one of a CL's reviewers.
type Reviewer struct {
	Principal string
	AddedBy   string
	AddedAt   time.Time
}

// Reviewers lists a CL's reviewers in the order they were added.
func Reviewers(ctx context.Context, q Querier, repo, number int64) ([]Reviewer, error) {
	rows, err := q.Query(ctx, `SELECT principal, added_by, added_at FROM reviewer WHERE repository_id = $1 AND number = $2 ORDER BY added_at, principal`, repo, number)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Reviewer
	for rows.Next() {
		var r Reviewer
		if err := rows.Scan(&r.Principal, &r.AddedBy, &r.AddedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AddReviewer assigns a reviewer, reporting whether they were new.
func AddReviewer(ctx context.Context, q Querier, repo, number int64, principal, by string) (bool, error) {
	tag, err := q.Exec(ctx, `INSERT INTO reviewer (repository_id, number, principal, added_by) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, repo, number, principal, by)
	return tag.RowsAffected() > 0, err
}

// RemoveReviewer takes a reviewer off, reporting whether they were one.
func RemoveReviewer(ctx context.Context, q Querier, repo, number int64, principal string) (bool, error) {
	tag, err := q.Exec(ctx, `DELETE FROM reviewer WHERE repository_id = $1 AND number = $2 AND principal = $3`, repo, number, principal)
	return tag.RowsAffected() > 0, err
}

// Label is a review label; the values are stored.
type Label int16

const (
	LabelLGTM     Label = 1
	LabelApproval Label = 2
)

// Labels are the policy's labels, in order.
var Labels = []Label{LabelLGTM, LabelApproval}

// String is the label's name.
func (l Label) String() string {
	switch l {
	case LabelLGTM:
		return "LGTM"
	case LabelApproval:
		return "Approval"
	}
	return "unknown"
}

// ParseLabel reads a label by name.
func ParseLabel(s string) (Label, bool) {
	for _, l := range Labels {
		if l.String() == s {
			return l, true
		}
	}
	return 0, false
}

// Grant is a reviewer's current state for one label.
type Grant struct {
	Number    int64
	Principal string
	Label     Label
	Granted   bool
	Patchset  int32
	UpdatedAt time.Time
}

// Grants lists a CL's label grants and revocations.
func Grants(ctx context.Context, q Querier, repo, number int64) ([]Grant, error) {
	return GrantsOf(ctx, q, repo, []int64{number})
}

// GrantsOf lists the label grants and revocations of several CLs.
func GrantsOf(ctx context.Context, q Querier, repo int64, numbers []int64) ([]Grant, error) {
	rows, err := q.Query(ctx, `SELECT number, principal, label, granted, patchset, updated_at FROM label_grant
		WHERE repository_id = $1 AND number = ANY($2::bigint[]) ORDER BY number, principal, label`, repo, numbers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Grant
	for rows.Next() {
		var g Grant
		if err := rows.Scan(&g.Number, &g.Principal, &g.Label, &g.Granted, &g.Patchset, &g.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// SetGrant grants or revokes a reviewer's label on a patchset, reporting whether its
// state changed. Revoking a label never granted changes nothing.
func SetGrant(ctx context.Context, q Querier, repo, number int64, principal string, label Label, granted bool, patchset int32) (bool, error) {
	if !granted {
		tag, err := q.Exec(ctx, `UPDATE label_grant SET granted = false, patchset = $5, updated_at = now()
			WHERE repository_id = $1 AND number = $2 AND principal = $3 AND label = $4 AND granted`,
			repo, number, principal, label, patchset)
		return tag.RowsAffected() > 0, err
	}
	tag, err := q.Exec(ctx, `INSERT INTO label_grant (repository_id, number, principal, label, granted, patchset)
		VALUES ($1, $2, $3, $4, true, $5)
		ON CONFLICT (repository_id, number, principal, label) DO UPDATE
			SET granted = true, patchset = EXCLUDED.patchset, updated_at = now()
		WHERE NOT label_grant.granted`, repo, number, principal, label, patchset)
	return tag.RowsAffected() > 0, err
}

// Side is the diff side an inline comment anchors to; the values are stored.
type Side int16

const (
	SideNone     Side = 0
	SideBase     Side = 1
	SidePatchset Side = 2
)

// Comment is one comment. A thread is a root (ParentID 0) and its replies; the root
// carries the thread's anchor and resolution.
type Comment struct {
	ID        int64
	Number    int64
	Patchset  int32
	Author    string
	ParentID  int64
	FilePath  string // "": a CL-level comment
	Line      int32  // 0: file-level
	Side      Side
	Body      string
	Resolved  bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

const commentColumns = `id, number, patchset, author, coalesce(parent_id, 0), coalesce(file_path, ''), coalesce(line, 0),
	coalesce(side, 0), body, resolved, created_at, updated_at`

func scanComment(row pgx.Row) (Comment, error) {
	var c Comment
	err := row.Scan(&c.ID, &c.Number, &c.Patchset, &c.Author, &c.ParentID, &c.FilePath, &c.Line, &c.Side, &c.Body, &c.Resolved, &c.CreatedAt, &c.UpdatedAt)
	return c, err
}

// Comments lists a CL's comments, oldest first.
func Comments(ctx context.Context, q Querier, repo, number int64) ([]Comment, error) {
	rows, err := q.Query(ctx, `SELECT `+commentColumns+` FROM comment WHERE repository_id = $1 AND number = $2 ORDER BY created_at, id`, repo, number)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Comment
	for rows.Next() {
		c, err := scanComment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// GetComment reads one of a CL's comments.
func GetComment(ctx context.Context, q Querier, repo, number, id int64) (Comment, bool, error) {
	c, err := scanComment(q.QueryRow(ctx, `SELECT `+commentColumns+` FROM comment WHERE repository_id = $1 AND number = $2 AND id = $3`, repo, number, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Comment{}, false, nil
	}
	return c, err == nil, err
}

// CreateComment stores a comment and returns its ID.
func CreateComment(ctx context.Context, q Querier, repo int64, c Comment) (int64, error) {
	var parent *int64
	if c.ParentID != 0 {
		parent = &c.ParentID
	}
	var file *string
	if c.FilePath != "" {
		file = &c.FilePath
	}
	var line *int32
	var side *int16
	if c.Line != 0 {
		line = &c.Line
		s := int16(c.Side)
		side = &s
	} else if c.Side != SideNone && c.FilePath != "" {
		s := int16(c.Side)
		side = &s
	}
	var id int64
	err := q.QueryRow(ctx, `INSERT INTO comment (repository_id, number, patchset, author, parent_id, file_path, line, side, body)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id`,
		repo, c.Number, c.Patchset, c.Author, parent, file, line, side, c.Body).Scan(&id)
	return id, err
}

// SetResolved sets a thread root's resolution, reporting whether it changed.
func SetResolved(ctx context.Context, q Querier, repo, number, rootID int64, resolved bool) (bool, error) {
	tag, err := q.Exec(ctx, `UPDATE comment SET resolved = $4, updated_at = now()
		WHERE repository_id = $1 AND number = $2 AND id = $3 AND parent_id IS NULL AND resolved <> $4`, repo, number, rootID, resolved)
	return tag.RowsAffected() > 0, err
}

// DeleteComment deletes a comment and, for a thread root, its replies.
func DeleteComment(ctx context.Context, q Querier, repo, number, id int64) error {
	_, err := q.Exec(ctx, `DELETE FROM comment WHERE repository_id = $1 AND number = $2 AND id = $3`, repo, number, id)
	return err
}

// UnresolvedThreads counts a CL's unresolved file and inline threads, from every
// patchset. CL-level comments are messages, not threads.
func UnresolvedThreads(ctx context.Context, q Querier, repo, number int64) (int64, error) {
	var n int64
	err := q.QueryRow(ctx, `SELECT count(*) FROM comment WHERE repository_id = $1 AND number = $2
		AND parent_id IS NULL AND file_path IS NOT NULL AND NOT resolved`, repo, number).Scan(&n)
	return n, err
}

// EventKind is what happened to a CL; the values are stored.
type EventKind int16

const (
	EventUploaded        EventKind = 1  // a patchset (Patchset); the first created the CL
	EventReviewRequested EventKind = 2  // draft → open
	EventReturnedToDraft EventKind = 3  // open → draft
	EventAbandoned       EventKind = 4  // From → abandoned; Message is the reason
	EventRestored        EventKind = 5  // abandoned → To
	EventSubmitted       EventKind = 6  // landed on main as Commit
	EventReviewerAdded   EventKind = 7  // Subject was assigned
	EventReviewerRemoved EventKind = 8  // Subject stopped reviewing
	EventLabelGranted    EventKind = 9  // Label granted on Patchset
	EventLabelRevoked    EventKind = 10 // Label revoked on Patchset
	EventApproved        EventKind = 11 // automatic: every review requirement is met
	EventApprovalLost    EventKind = 12 // automatic: a review requirement is no longer met
	EventRolledBack      EventKind = 13 // Related, a rollback CL, was created to roll this one back
)

// Event is one entry of a CL's history. An empty Actor is ok itself.
type Event struct {
	ID       int64
	At       time.Time
	Actor    string
	Kind     EventKind
	From, To Status
	Subject  string
	Label    Label
	Patchset int32
	Commit   []byte
	Message  string
	Related  int64 // the other CL it names (EventRolledBack)
}

// RecordEvent appends to a CL's history. A zero At is now.
func RecordEvent(ctx context.Context, q Querier, repo, number int64, e Event) error {
	at := e.At
	if at.IsZero() {
		at = time.Now()
	}
	_, err := q.Exec(ctx, `INSERT INTO event (repository_id, number, at, actor, kind, from_status, to_status, subject, label, patchset, commit_id, message, related)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		repo, number, at, e.Actor, e.Kind, e.From, e.To, e.Subject, e.Label, e.Patchset, e.Commit, e.Message, e.Related)
	return err
}

// Events lists a CL's history, oldest first.
func Events(ctx context.Context, q Querier, repo, number int64) ([]Event, error) {
	rows, err := q.Query(ctx, `SELECT id, at, actor, kind, from_status, to_status, subject, label, patchset, commit_id, message, related
		FROM event WHERE repository_id = $1 AND number = $2 ORDER BY at, id`, repo, number)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.At, &e.Actor, &e.Kind, &e.From, &e.To, &e.Subject, &e.Label, &e.Patchset, &e.Commit, &e.Message, &e.Related); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// LastAbandon is a CL's most recent abandonment, if any.
func LastAbandon(ctx context.Context, q Querier, repo, number int64) (Event, bool, error) {
	var e Event
	err := q.QueryRow(ctx, `SELECT id, from_status FROM event WHERE repository_id = $1 AND number = $2 AND kind = $3 ORDER BY id DESC LIMIT 1`,
		repo, number, EventAbandoned).Scan(&e.ID, &e.From)
	if errors.Is(err, pgx.ErrNoRows) {
		return Event{}, false, nil
	}
	return e, err == nil, err
}

// Favorites are the repositories a person has favorited.
func Favorites(ctx context.Context, q Querier, principal string) (map[int64]bool, error) {
	rows, err := q.Query(ctx, `SELECT repository_id FROM favorite WHERE principal = $1`, principal)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// SetFavorite favorites a repository for a person, or stops.
func SetFavorite(ctx context.Context, q Querier, principal string, repo int64, on bool) error {
	var err error
	if on {
		_, err = q.Exec(ctx, `INSERT INTO favorite (principal, repository_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, principal, repo)
	} else {
		_, err = q.Exec(ctx, `DELETE FROM favorite WHERE principal = $1 AND repository_id = $2`, principal, repo)
	}
	return err
}
