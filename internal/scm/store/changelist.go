package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
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

// loadChangeLists makes CLs of rows, all of one repository's, with their patchsets, in the rows'
// order.
func loadChangeLists(ctx context.Context, q backend.Ops, repo int64, rows []backend.ChangeList) ([]*ChangeList, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]*ChangeList, 0, len(rows))
	byNumber := map[int64]*ChangeList{}
	for _, r := range rows {
		if len(r.ChangeID) != encoding.ChangeIDLen {
			return nil, fmt.Errorf("CL %d: stored change ID of %d bytes", r.Number, len(r.ChangeID))
		}
		c := &ChangeList{
			Repository: repo, Number: r.Number, ChangeID: encoding.ChangeID(r.ChangeID), Author: r.Author,
			Status: Status(r.Status), AbandonedFrom: Status(r.AbandonedFrom), ParentNumber: r.Parent,
			CurrentPatchset: r.CurrentPatchset, LandedCommit: r.Landed, SubmittedBy: r.SubmittedBy,
			Description: r.Description, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
		}
		if len(r.RollsBack) > 0 {
			c.Rollback = &Rollback{CLs: r.RollsBack, Paths: r.RollbackPaths, Left: r.RollbackLeft}
		}
		out = append(out, c)
		byNumber[c.Number] = c
	}
	numbers := make([]int64, 0, len(out))
	for _, c := range out {
		numbers = append(numbers, c.Number)
	}
	pss, err := q.Patchsets(ctx, repo, numbers)
	if err != nil {
		return nil, err
	}
	for _, p := range pss {
		c := byNumber[p.CL]
		if int(p.Number) != len(c.Patchsets)+1 {
			return nil, fmt.Errorf("CL %d: patchset %d follows %d", p.CL, p.Number, len(c.Patchsets))
		}
		c.Patchsets = append(c.Patchsets, Patchset{Number: p.Number, Commit: p.Commit, Base: p.Base, UploadedBy: p.UploadedBy, UploadedAt: p.UploadedAt})
	}
	for _, c := range out {
		if c.CurrentPatchset < 1 || int(c.CurrentPatchset) > len(c.Patchsets) {
			return nil, fmt.Errorf("CL %d: current patchset %d of %d", c.Number, c.CurrentPatchset, len(c.Patchsets))
		}
	}
	return out, nil
}

// readChangeLists reads the named CLs — held, with lock — by number.
func readChangeLists(ctx context.Context, q backend.Ops, repo int64, numbers []int64, lock bool) (map[int64]*ChangeList, error) {
	out := map[int64]*ChangeList{}
	if len(numbers) == 0 {
		return out, nil
	}
	rows, err := q.ChangeLists(ctx, repo, numbers, lock)
	if err != nil {
		return nil, err
	}
	cls, err := loadChangeLists(ctx, q, repo, rows)
	if err != nil {
		return nil, err
	}
	for _, c := range cls {
		out[c.Number] = c
	}
	return out, nil
}

// lockChangeLists locks the named CLs, in number order, and reads them.
func lockChangeLists(ctx context.Context, tx *txn, repo int64, numbers []int64) (map[int64]*ChangeList, error) {
	return readChangeLists(ctx, tx, repo, numbers, true)
}

// GetChangeList returns one CL.
func (s *Store) GetChangeList(ctx context.Context, repo, number int64) (ChangeList, error) {
	cls, err := readChangeLists(ctx, s.b, repo, []int64{number}, false)
	if err != nil {
		return ChangeList{}, err
	}
	c, ok := cls[number]
	if !ok {
		if err := s.repositoryExists(ctx, s.b, repo); err != nil {
			return ChangeList{}, err
		}
		return ChangeList{}, notFound("CL %d", number)
	}
	return *c, nil
}

// ListChangeLists returns up to pageSize of a repository's CLs numbered below `before`
// (0: from the newest), newest first, optionally only those in statuses or by author, and
// the number to continue below (0 at the end).
func (s *Store) ListChangeLists(ctx context.Context, repo int64, statuses []Status, author string, pageSize int, before int64) ([]ChangeList, int64, error) {
	if err := s.repositoryExists(ctx, s.b, repo); err != nil {
		return nil, 0, err
	}
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}
	sts := make([]int16, len(statuses))
	for i, st := range statuses {
		sts[i] = int16(st)
	}
	rows, err := s.b.ListChangeLists(ctx, repo, before, sts, author, pageSize+1)
	if err != nil {
		return nil, 0, err
	}
	cls, err := loadChangeLists(ctx, s.b, repo, rows)
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
	if err := s.repositoryExists(ctx, s.b, repo); err != nil {
		return nil, err
	}
	keys := make([][]byte, len(changeIDs))
	for i := range changeIDs {
		keys[i] = changeIDs[i][:]
	}
	rows, err := s.b.ChangeListsOfChanges(ctx, repo, keys)
	if err != nil {
		return nil, err
	}
	cls, err := loadChangeLists(ctx, s.b, repo, rows)
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
// and emits a transitioned event, which it returns. reason is for abandon only.
func (s *Store) Transition(ctx context.Context, repo, number int64, kind TransitionKind, actor, reason string) (ChangeList, *scmpb.Event, error) {
	if kind != Abandon && reason != "" {
		return ChangeList{}, nil, invalid("only abandon takes a reason")
	}
	var out ChangeList
	var ev *scmpb.Event
	err := s.inTx(ctx, func(tx *txn) error {
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
		if err := tx.SetChangeListStatus(ctx, repo, number, int16(to), abandonedFrom); err != nil {
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
		return ChangeList{}, nil, err
	}
	return out, ev, nil
}
