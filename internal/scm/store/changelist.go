package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/encoding"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// ErrPrecondition refuses a request the current state does not allow, such as a
// transition out of a status it does not apply to.
var ErrPrecondition = errors.New("failed precondition")

func precondition(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrPrecondition, fmt.Sprintf(format, args...))
}

// Status is a ChangeList's status. The values are the protocol's ChangeListStatus and the
// changelist table's.
type Status int16

const (
	StatusDraft     Status = 1
	StatusOpen      Status = 2
	StatusSubmitted Status = 3
	StatusAbandoned Status = 4
)

func (st Status) String() string {
	switch st {
	case StatusDraft:
		return "draft"
	case StatusOpen:
		return "open"
	case StatusSubmitted:
		return "submitted"
	case StatusAbandoned:
		return "abandoned"
	}
	return fmt.Sprintf("status(%d)", int16(st))
}

func (st Status) proto() scmpb.ChangeListStatus { return scmpb.ChangeListStatus(st) }

// Patchset is one uploaded revision of a CL.
type Patchset struct {
	Number     int32
	Commit     encoding.ID
	Base       encoding.ID // the commit's parent
	UploadedBy string
	UploadedAt time.Time
}

// ChangeList is a CL as the SCM owns it (docs/scm.md, "Who owns what").
type ChangeList struct {
	Repository int64
	Number     int64
	ChangeID   encoding.ChangeID
	Author     string
	Status     Status
	// AbandonedFrom is the status an abandoned CL returns to when restored; 0 otherwise.
	AbandonedFrom Status
	// ParentNumber is the CL this one is stacked on; 0 when it is not stacked.
	ParentNumber    int64
	CurrentPatchset int32
	// Patchsets are every patchset, numbered from 1 in order.
	Patchsets []Patchset
	// LandedCommit is the commit a submitted CL landed as; nil until it lands.
	LandedCommit *encoding.ID
	SubmittedBy  string
	// Description is the current patchset's; its first line is the title.
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	// Rollback is what a rollback CL rolls back; nil for any other CL.
	Rollback *Rollback
}

// Rollback is what a rollback CL rolls back (docs/scm.md, "Rollbacks").
type Rollback struct {
	// CLs are the CLs it rolls back, in the order they landed: the one asked for, and those
	// landed above it in its stack.
	CLs []int64
	// Paths are the paths its first patchset rolled back, and Left those it left as main has
	// them, because main changed them after the CLs landed.
	Paths []string
	Left  []string
}

// Current is the CL's current patchset.
func (c *ChangeList) Current() Patchset { return c.Patchsets[c.CurrentPatchset-1] }

// active reports whether the CL takes patchsets and transitions other than restore.
func (c *ChangeList) active() bool { return c.Status == StatusDraft || c.Status == StatusOpen }

const changelistColumns = `number, change_id, author, status, abandoned_from, parent_number, current_patchset,
	landed_commit, submitted_by, description, created_at, updated_at, rolls_back, rollback_paths, rollback_left`

type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// loadChangeLists reads the CLs a query over changelistColumns selects (it must select
// from one repository), with their patchsets, in the query's order.
func loadChangeLists(ctx context.Context, q querier, repo int64, sql string, args ...any) ([]*ChangeList, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	var out []*ChangeList
	byNumber := map[int64]*ChangeList{}
	for rows.Next() {
		c := &ChangeList{Repository: repo}
		var change, landed []byte
		var abandonedFrom *int16
		var parent *int64
		var submittedBy *string
		var status int16
		var rb Rollback
		if err := rows.Scan(&c.Number, &change, &c.Author, &status, &abandonedFrom, &parent, &c.CurrentPatchset,
			&landed, &submittedBy, &c.Description, &c.CreatedAt, &c.UpdatedAt, &rb.CLs, &rb.Paths, &rb.Left); err != nil {
			rows.Close()
			return nil, err
		}
		if len(change) != encoding.ChangeIDLen {
			rows.Close()
			return nil, fmt.Errorf("CL %d: stored change ID of %d bytes", c.Number, len(change))
		}
		c.ChangeID = encoding.ChangeID(change)
		c.Status = Status(status)
		if abandonedFrom != nil {
			c.AbandonedFrom = Status(*abandonedFrom)
		}
		if parent != nil {
			c.ParentNumber = *parent
		}
		if landed != nil {
			id, err := toID(landed)
			if err != nil {
				rows.Close()
				return nil, err
			}
			c.LandedCommit = &id
		}
		if submittedBy != nil {
			c.SubmittedBy = *submittedBy
		}
		if len(rb.CLs) > 0 {
			c.Rollback = &rb
		}
		out = append(out, c)
		byNumber[c.Number] = c
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	numbers := make([]int64, 0, len(out))
	for _, c := range out {
		numbers = append(numbers, c.Number)
	}
	prows, err := q.Query(ctx, `SELECT cl_number, number, commit_id, base_commit_id, uploaded_by, uploaded_at
		FROM patchset WHERE repository_id = $1 AND cl_number = ANY($2) ORDER BY cl_number, number`, repo, numbers)
	if err != nil {
		return nil, err
	}
	defer prows.Close()
	for prows.Next() {
		var cl int64
		var ps Patchset
		var commit, base []byte
		if err := prows.Scan(&cl, &ps.Number, &commit, &base, &ps.UploadedBy, &ps.UploadedAt); err != nil {
			return nil, err
		}
		if ps.Commit, err = toID(commit); err != nil {
			return nil, err
		}
		if ps.Base, err = toID(base); err != nil {
			return nil, err
		}
		c := byNumber[cl]
		if int(ps.Number) != len(c.Patchsets)+1 {
			return nil, fmt.Errorf("CL %d: patchset %d follows %d", cl, ps.Number, len(c.Patchsets))
		}
		c.Patchsets = append(c.Patchsets, ps)
	}
	if err := prows.Err(); err != nil {
		return nil, err
	}
	for _, c := range out {
		if c.CurrentPatchset < 1 || int(c.CurrentPatchset) > len(c.Patchsets) {
			return nil, fmt.Errorf("CL %d: current patchset %d of %d", c.Number, c.CurrentPatchset, len(c.Patchsets))
		}
	}
	return out, nil
}

// lockChangeLists locks the named CLs, in number order, and reads them.
func lockChangeLists(ctx context.Context, tx pgx.Tx, repo int64, numbers []int64) (map[int64]*ChangeList, error) {
	out := map[int64]*ChangeList{}
	if len(numbers) == 0 {
		return out, nil
	}
	cls, err := loadChangeLists(ctx, tx, repo, `SELECT `+changelistColumns+` FROM changelist
		WHERE repository_id = $1 AND number = ANY($2) ORDER BY number FOR UPDATE`, repo, numbers)
	if err != nil {
		return nil, err
	}
	for _, c := range cls {
		out[c.Number] = c
	}
	return out, nil
}

// GetChangeList returns one CL.
func (s *Store) GetChangeList(ctx context.Context, repo, number int64) (ChangeList, error) {
	cls, err := loadChangeLists(ctx, s.pool, repo, `SELECT `+changelistColumns+` FROM changelist
		WHERE repository_id = $1 AND number = $2`, repo, number)
	if err != nil {
		return ChangeList{}, err
	}
	if len(cls) == 0 {
		if err := s.repositoryExists(ctx, s.pool, repo); err != nil {
			return ChangeList{}, err
		}
		return ChangeList{}, notFound("CL %d", number)
	}
	return *cls[0], nil
}

// ListChangeLists returns up to pageSize of a repository's CLs numbered below `before`
// (0: from the newest), newest first, optionally only those in statuses or by author, and
// the number to continue below (0 at the end).
func (s *Store) ListChangeLists(ctx context.Context, repo int64, statuses []Status, author string, pageSize int, before int64) ([]ChangeList, int64, error) {
	if err := s.repositoryExists(ctx, s.pool, repo); err != nil {
		return nil, 0, err
	}
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}
	sts := make([]int16, len(statuses))
	for i, st := range statuses {
		sts[i] = int16(st)
	}
	cls, err := loadChangeLists(ctx, s.pool, repo, `SELECT `+changelistColumns+` FROM changelist
		WHERE repository_id = $1 AND ($2 = 0 OR number < $2)
		  AND (cardinality($3::smallint[]) = 0 OR status = ANY($3))
		  AND ($4 = '' OR author = $4)
		ORDER BY number DESC LIMIT $5`, repo, before, sts, author, pageSize+1)
	if err != nil {
		return nil, 0, err
	}
	var next int64
	if len(cls) > pageSize {
		cls = cls[:pageSize]
		next = cls[pageSize-1].Number
	}
	out := make([]ChangeList, len(cls))
	for i, c := range cls {
		out[i] = *c
	}
	return out, next, nil
}

// LookupChanges returns the CLs of the given change IDs that have one, in number order.
func (s *Store) LookupChanges(ctx context.Context, repo int64, changeIDs []encoding.ChangeID) ([]ChangeList, error) {
	if err := s.repositoryExists(ctx, s.pool, repo); err != nil {
		return nil, err
	}
	keys := make([][]byte, len(changeIDs))
	for i := range changeIDs {
		keys[i] = changeIDs[i][:]
	}
	cls, err := loadChangeLists(ctx, s.pool, repo, `SELECT `+changelistColumns+` FROM changelist
		WHERE repository_id = $1 AND change_id = ANY($2) ORDER BY number`, repo, keys)
	if err != nil {
		return nil, err
	}
	out := make([]ChangeList, len(cls))
	for i, c := range cls {
		out[i] = *c
	}
	return out, nil
}

// TransitionKind is a lifecycle transition (docs/scm.md, "Lifecycle"). The values
// are the protocol's.
type TransitionKind int

const (
	RequestReview TransitionKind = 1
	ReturnToDraft TransitionKind = 2
	Abandon       TransitionKind = 3
	Restore       TransitionKind = 4
)

// Transition applies a lifecycle transition on behalf of actor, the human ok control
// authorized. It takes the CL's row lock, refuses a CL outside the transition's from-set,
// and emits a transitioned event, whose sequence number it returns. reason is for abandon
// only.
func (s *Store) Transition(ctx context.Context, repo, number int64, kind TransitionKind, actor, reason string) (ChangeList, int64, error) {
	if kind != Abandon && reason != "" {
		return ChangeList{}, 0, invalid("only abandon takes a reason")
	}
	var out ChangeList
	var ev *scmpb.Event
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		cls, err := lockChangeLists(ctx, tx, repo, []int64{number})
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
		from := c.Status
		var to Status
		switch kind {
		case RequestReview:
			if from != StatusDraft {
				return precondition("CL %d is %s; only a draft can be sent for review", number, from)
			}
			to = StatusOpen
		case ReturnToDraft:
			if from != StatusOpen {
				return precondition("CL %d is %s; only an open CL returns to draft", number, from)
			}
			to = StatusDraft
		case Abandon:
			if !c.active() {
				return precondition("CL %d is %s; only a draft or open CL can be abandoned", number, from)
			}
			to = StatusAbandoned
		case Restore:
			if from != StatusAbandoned {
				return precondition("CL %d is %s; only an abandoned CL can be restored", number, from)
			}
			to = c.AbandonedFrom
		default:
			return invalid("transition %d", kind)
		}
		var abandonedFrom *int16
		if to == StatusAbandoned {
			f := int16(from)
			abandonedFrom = &f
		}
		if _, err := tx.Exec(ctx, `UPDATE changelist SET status = $3, abandoned_from = $4, updated_at = now()
			WHERE repository_id = $1 AND number = $2`, repo, number, int16(to), abandonedFrom); err != nil {
			return err
		}
		cls, err = lockChangeLists(ctx, tx, repo, []int64{number})
		if err != nil {
			return err
		}
		out = *cls[number]
		ev = &scmpb.Event{Kind: &scmpb.Event_Transitioned{Transitioned: &scmpb.ChangeListTransitioned{
			Number: number, From: from.proto(), To: to.proto(), Actor: actor, Reason: reason,
		}}}
		return appendEvent(ctx, tx, repo, ev)
	})
	if err != nil {
		return ChangeList{}, 0, err
	}
	return out, ev.GetSeq(), nil
}
