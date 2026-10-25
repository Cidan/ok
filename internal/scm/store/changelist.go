package store

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
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
	// ID keys the CL inside the SCM; Number names it everywhere else (sequence.go).
	ID       int64
	Number   int64
	ChangeID encoding.ChangeID
	Author   string
	Status   Status
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

// changeListOf makes a CL of its row and its patchsets, in number order.
func changeListOf(repo int64, r backend.ChangeList, pss []backend.Patchset) (*ChangeList, error) {
	if len(r.ChangeID) != encoding.ChangeIDLen {
		return nil, fmt.Errorf("CL %d: stored change ID of %d bytes", r.Number, len(r.ChangeID))
	}
	c := &ChangeList{
		Repository: repo, ID: r.ID, Number: r.Number, ChangeID: encoding.ChangeID(r.ChangeID), Author: r.Author,
		Status: Status(r.Status), AbandonedFrom: Status(r.AbandonedFrom), ParentNumber: r.Parent,
		CurrentPatchset: r.CurrentPatchset, LandedCommit: r.Landed, SubmittedBy: r.SubmittedBy,
		Description: r.Description, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
	if len(r.RollsBack) > 0 {
		c.Rollback = &Rollback{CLs: r.RollsBack, Paths: r.RollbackPaths, Left: r.RollbackLeft}
	}
	for _, p := range pss {
		if int(p.Number) != len(c.Patchsets)+1 {
			return nil, fmt.Errorf("CL %d: patchset %d follows %d", r.Number, p.Number, len(c.Patchsets))
		}
		c.Patchsets = append(c.Patchsets, Patchset{Number: p.Number, Commit: p.Commit, Base: p.Base, UploadedBy: p.UploadedBy, UploadedAt: p.UploadedAt})
	}
	if c.CurrentPatchset < 1 || int(c.CurrentPatchset) > len(c.Patchsets) {
		return nil, fmt.Errorf("CL %d: current patchset %d of %d", c.Number, c.CurrentPatchset, len(c.Patchsets))
	}
	return c, nil
}

// loadChangeLists makes CLs of the tail's rows, all of one repository's, with their patchsets, in
// the rows' order.
func (s *Store) loadChangeLists(ctx context.Context, q backend.Ops, repo int64, rows []backend.ChangeList) ([]*ChangeList, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	if s.afterChangeListRows != nil {
		s.afterChangeListRows()
	}
	ids := make([]int64, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	pss, err := q.Patchsets(ctx, repo, ids)
	if err != nil {
		return nil, err
	}
	byCL := map[int64][]backend.Patchset{}
	for _, p := range pss {
		byCL[p.CL] = append(byCL[p.CL], p)
	}
	out := make([]*ChangeList, 0, len(rows))
	for _, r := range rows {
		c, err := changeListOf(repo, r, byCL[r.ID])
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// settledOf makes CLs of landed CLs' records in the segments.
func settledOf(repo int64, settled map[int64]backend.SettledChangeList) ([]*ChangeList, error) {
	out := make([]*ChangeList, 0, len(settled))
	for _, sc := range settled {
		c, err := changeListOf(repo, sc.ChangeList, sc.Patchsets)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// readChangeLists reads the named CLs by number: the tail's — held, with lock — and the
// segments' for the rest, which are landed and so never change, and need no lock.
func (s *Store) readChangeLists(ctx context.Context, q backend.Ops, repo int64, numbers []int64, lock bool) (map[int64]*ChangeList, error) {
	out := map[int64]*ChangeList{}
	if len(numbers) == 0 {
		return out, nil
	}
	rows, err := q.ChangeLists(ctx, repo, numbers, lock)
	if err != nil {
		return nil, err
	}
	cls, err := s.loadChangeLists(ctx, q, repo, rows)
	if err != nil {
		return nil, err
	}
	for _, c := range cls {
		out[c.Number] = c
	}
	var rest []int64
	for _, n := range numbers {
		if out[n] == nil {
			rest = append(rest, n)
		}
	}
	settled, err := s.settledChangeLists(ctx, repo, rest)
	if err != nil {
		return nil, err
	}
	more, err := settledOf(repo, settled)
	if err != nil {
		return nil, err
	}
	for _, c := range more {
		out[c.Number] = c
	}
	return out, nil
}

// lockChangeLists locks the named CLs in the tail, in number order, and reads them.
func (s *Store) lockChangeLists(ctx context.Context, tx *txn, repo int64, numbers []int64) (map[int64]*ChangeList, error) {
	return s.readChangeLists(ctx, tx, repo, numbers, true)
}

// clStatus is a CL's status, in the tail or the segments.
func (s *Store) clStatus(ctx context.Context, q backend.Ops, repo, number int64) (Status, error) {
	st, ok, err := q.ChangeListStatus(ctx, repo, number)
	if err != nil || ok {
		return Status(st), err
	}
	settled, err := s.settledChangeLists(ctx, repo, []int64{number})
	if err != nil {
		return 0, err
	}
	if sc, ok := settled[number]; ok {
		return Status(sc.Status), nil
	}
	return 0, notFound("CL %d", number)
}

// checkNumber refuses a CL number no CL is named by outside the SCM.
func checkNumber(number int64) error {
	if number <= 0 {
		return invalid("CL %d: CL numbers start at 1", number)
	}
	return nil
}

// sequencedRead runs read, which reads CLs of repo, over one snapshot of the database
// (inSnapshot) — a CL's row and its patchsets read together, so a landed CL compaction moves into
// segments meanwhile is read whole from one or the other, never its row without its patchsets — and
// again once their pass has run, if any it read was provisional (ensureSequenced): every read
// outside the SCM's transactions shows numbers alone. Within a transaction, it reads through it.
func (s *Store) sequencedRead(ctx context.Context, repo int64, read func(ctx context.Context, q backend.Ops) ([]backend.ChangeList, error)) error {
	for {
		var rows []backend.ChangeList
		var err error
		if inTxContext(ctx) {
			rows, err = read(ctx, s.ops(ctx))
		} else {
			err = s.inSnapshot(ctx, func(ctx context.Context, tx *txn) error {
				var err error
				rows, err = read(ctx, tx)
				return err
			})
		}
		if err != nil {
			return err
		}
		again, err := s.ensureSequenced(ctx, repo, rows)
		if err != nil || !again {
			return err
		}
	}
}

// changeListRows are the rows of cls, for ensureSequenced.
func changeListRows(cls map[int64]*ChangeList) []backend.ChangeList {
	out := make([]backend.ChangeList, 0, len(cls))
	for _, c := range cls {
		out = append(out, backend.ChangeList{Number: c.Number, Parent: c.ParentNumber})
	}
	return out
}

// GetChangeList returns one CL.
func (s *Store) GetChangeList(ctx context.Context, repo, number int64) (ChangeList, error) {
	if err := checkNumber(number); err != nil {
		return ChangeList{}, err
	}
	var cls map[int64]*ChangeList
	err := s.sequencedRead(ctx, repo, func(ctx context.Context, q backend.Ops) ([]backend.ChangeList, error) {
		var err error
		cls, err = s.readChangeLists(ctx, q, repo, []int64{number}, false)
		return changeListRows(cls), err
	})
	if err != nil {
		return ChangeList{}, err
	}
	c, ok := cls[number]
	if !ok {
		if err := s.repositoryExists(ctx, s.ops(ctx), repo); err != nil {
			return ChangeList{}, err
		}
		return ChangeList{}, notFound("CL %d", number)
	}
	return *c, nil
}

// ListChangeLists returns up to pageSize of a repository's CLs numbered below `before`
// (0: from the newest), newest first, optionally only those in statuses, by author, or stacked on
// parent, and the number to continue below (0 at the end).
func (s *Store) ListChangeLists(ctx context.Context, repo int64, statuses []Status, author string, parent int64, pageSize int, before int64) ([]ChangeList, int64, error) {
	if err := s.repositoryExists(ctx, s.ops(ctx), repo); err != nil {
		return nil, 0, err
	}
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}
	sts := make([]int16, len(statuses))
	for i, st := range statuses {
		sts[i] = int16(st)
	}
	if parent < 0 || before < 0 {
		return nil, 0, invalid("CL numbers start at 1")
	}
	var rows []backend.ChangeList
	var cls []*ChangeList
	err := s.sequencedRead(ctx, repo, func(ctx context.Context, q backend.Ops) ([]backend.ChangeList, error) {
		var err error
		if rows, err = q.ListChangeLists(ctx, repo, before, sts, author, parent, pageSize+1); err != nil {
			return nil, err
		}
		cls, err = s.loadChangeLists(ctx, q, repo, rows)
		return rows, err
	})
	if err != nil {
		return nil, 0, err
	}
	// Landed CLs in segments, each source's first page merged: those are all a page can use.
	if len(statuses) == 0 || slices.Contains(statuses, StatusSubmitted) {
		numbers, err := s.settledNumbers(ctx, repo, before, author, pageSize+1)
		if err != nil {
			return nil, 0, err
		}
		have := map[int64]bool{}
		for _, c := range cls {
			have[c.Number] = true
		}
		numbers = slices.DeleteFunc(numbers, func(n int64) bool { return have[n] })
		settled, err := s.settledChangeLists(ctx, repo, numbers)
		if err != nil {
			return nil, 0, err
		}
		more, err := settledOf(repo, settled)
		if err != nil {
			return nil, 0, err
		}
		if parent != 0 {
			more = slices.DeleteFunc(more, func(c *ChangeList) bool { return c.ParentNumber != parent })
		}
		cls = append(cls, more...)
		slices.SortFunc(cls, func(a, b *ChangeList) int { return cmp.Compare(b.Number, a.Number) })
		cls = cls[:min(len(cls), pageSize+1)]
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
	if err := s.repositoryExists(ctx, s.ops(ctx), repo); err != nil {
		return nil, err
	}
	keys := make([][]byte, len(changeIDs))
	for i := range changeIDs {
		keys[i] = changeIDs[i][:]
	}
	var cls []*ChangeList
	err := s.sequencedRead(ctx, repo, func(ctx context.Context, q backend.Ops) ([]backend.ChangeList, error) {
		rows, err := q.ChangeListsOfChanges(ctx, repo, keys)
		if err != nil {
			return nil, err
		}
		cls, err = s.loadChangeLists(ctx, q, repo, rows)
		return rows, err
	})
	if err != nil {
		return nil, err
	}
	found := map[encoding.ChangeID]bool{}
	for _, c := range cls {
		found[c.ChangeID] = true
	}
	var rest [][]byte
	for i, id := range changeIDs {
		if !found[id] {
			rest = append(rest, keys[i])
		}
	}
	numbers, err := s.settledByChange(ctx, repo, rest)
	if err != nil {
		return nil, err
	}
	settled, err := s.settledChangeLists(ctx, repo, numbers)
	if err != nil {
		return nil, err
	}
	more, err := settledOf(repo, settled)
	if err != nil {
		return nil, err
	}
	cls = append(cls, more...)
	slices.SortFunc(cls, func(a, b *ChangeList) int { return cmp.Compare(a.Number, b.Number) })
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
	if err := checkNumber(number); err != nil {
		return ChangeList{}, nil, err
	}
	var out ChangeList
	var ev *scmpb.Event
	err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		cls, err := s.lockChangeLists(ctx, tx, repo, []int64{number})
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
		if err := tx.SetChangeListStatus(ctx, repo, c.ID, int16(to), abandonedFrom); err != nil {
			return err
		}
		cls, err = s.lockChangeLists(ctx, tx, repo, []int64{number})
		if err != nil {
			return err
		}
		out = *cls[number]
		ev = &scmpb.Event{Kind: &scmpb.Event_Transitioned{Transitioned: &scmpb.ChangeListTransitioned{
			Number: number, From: from.proto(), To: to.proto(), ActorId: actor, Reason: reason,
		}}}
		return appendEvent(ctx, tx, repo, ev)
	})
	if err != nil {
		return ChangeList{}, nil, err
	}
	return out, ev, nil
}
