package db

import (
	"context"
	"time"

	"github.com/Cidan/ok/internal/wait"
)

// Changing is s saying, in the transaction of each write that changes what a CL's status shows —
// its record, approval, reviewers, labels, comments, history — that the CL changed
// (wait.CLSubject), for its watchers: a write made on the store runs in a transaction of its own to
// say so, and one made in a transaction says so before it commits.
func Changing(s Store) Store { return changing{s} }

type changing struct{ Store }

// Txn says, before the transaction commits, which CLs its writes changed.
func (c changing) Txn(ctx context.Context, fn func(tx Ops) error) error {
	return c.Store.Txn(ctx, func(tx Ops) error {
		t := changingTx{Ops: tx, cls: map[[2]int64]bool{}}
		if err := fn(t); err != nil {
			return err
		}
		return t.changed(ctx)
	})
}

// write runs fn in a transaction of its own that says what it changed.
func (c changing) write(ctx context.Context, fn func(tx Ops) error) error { return c.Txn(ctx, fn) }

func (c changing) UpsertChangeList(ctx context.Context, cl ChangeList) (changed bool, err error) {
	err = c.write(ctx, func(tx Ops) error { changed, err = tx.UpsertChangeList(ctx, cl); return err })
	return changed, err
}

func (c changing) SetApproved(ctx context.Context, repo, number int64, approved bool) (changed bool, err error) {
	err = c.write(ctx, func(tx Ops) error { changed, err = tx.SetApproved(ctx, repo, number, approved); return err })
	return changed, err
}

func (c changing) AddReviewer(ctx context.Context, repo, number int64, principal, by string) (added bool, err error) {
	err = c.write(ctx, func(tx Ops) error { added, err = tx.AddReviewer(ctx, repo, number, principal, by); return err })
	return added, err
}

func (c changing) RemoveReviewer(ctx context.Context, repo, number int64, principal string) (removed bool, err error) {
	err = c.write(ctx, func(tx Ops) error { removed, err = tx.RemoveReviewer(ctx, repo, number, principal); return err })
	return removed, err
}

func (c changing) SetGrant(ctx context.Context, repo, number int64, principal string, label Label, granted bool, patchset int32) (changed bool, err error) {
	err = c.write(ctx, func(tx Ops) error {
		changed, err = tx.SetGrant(ctx, repo, number, principal, label, granted, patchset)
		return err
	})
	return changed, err
}

func (c changing) CreateComment(ctx context.Context, repo int64, cm Comment) (id int64, err error) {
	err = c.write(ctx, func(tx Ops) error { id, err = tx.CreateComment(ctx, repo, cm); return err })
	return id, err
}

func (c changing) SetResolved(ctx context.Context, repo, number, rootID int64, resolved bool) (changed bool, err error) {
	err = c.write(ctx, func(tx Ops) error { changed, err = tx.SetResolved(ctx, repo, number, rootID, resolved); return err })
	return changed, err
}

func (c changing) DeleteComment(ctx context.Context, repo, number, id int64) error {
	return c.write(ctx, func(tx Ops) error { return tx.DeleteComment(ctx, repo, number, id) })
}

func (c changing) RecordEvent(ctx context.Context, repo, number int64, e Event) error {
	return c.write(ctx, func(tx Ops) error { return tx.RecordEvent(ctx, repo, number, e) })
}

// changingTx is a transaction's operations, recording the CLs its writes change: a write that says
// it changed nothing — an upsert of the state held, which every status a watch reads makes — says
// nothing, or each read would wake the watch that made it.
type changingTx struct {
	Ops
	cls map[[2]int64]bool
}

func (t changingTx) touch(repo, number int64) { t.cls[[2]int64{repo, number}] = true }

func (t changingTx) changed(ctx context.Context) error {
	for c := range t.cls {
		kind, key, body := wait.Changed(wait.CLSubject(c[0], c[1]))
		if err := t.Ops.PendStep(ctx, kind, key, time.Time{}, body); err != nil {
			return err
		}
	}
	return nil
}

func (t changingTx) UpsertChangeList(ctx context.Context, cl ChangeList) (bool, error) {
	changed, err := t.Ops.UpsertChangeList(ctx, cl)
	if changed {
		t.touch(cl.Repo, cl.Number)
	}
	return changed, err
}

func (t changingTx) SetApproved(ctx context.Context, repo, number int64, approved bool) (bool, error) {
	changed, err := t.Ops.SetApproved(ctx, repo, number, approved)
	if changed {
		t.touch(repo, number)
	}
	return changed, err
}

func (t changingTx) AddReviewer(ctx context.Context, repo, number int64, principal, by string) (bool, error) {
	changed, err := t.Ops.AddReviewer(ctx, repo, number, principal, by)
	if changed {
		t.touch(repo, number)
	}
	return changed, err
}

func (t changingTx) RemoveReviewer(ctx context.Context, repo, number int64, principal string) (bool, error) {
	changed, err := t.Ops.RemoveReviewer(ctx, repo, number, principal)
	if changed {
		t.touch(repo, number)
	}
	return changed, err
}

func (t changingTx) SetGrant(ctx context.Context, repo, number int64, principal string, label Label, granted bool, patchset int32) (bool, error) {
	changed, err := t.Ops.SetGrant(ctx, repo, number, principal, label, granted, patchset)
	if changed {
		t.touch(repo, number)
	}
	return changed, err
}

func (t changingTx) CreateComment(ctx context.Context, repo int64, cm Comment) (int64, error) {
	t.touch(repo, cm.Number)
	return t.Ops.CreateComment(ctx, repo, cm)
}

func (t changingTx) SetResolved(ctx context.Context, repo, number, rootID int64, resolved bool) (bool, error) {
	changed, err := t.Ops.SetResolved(ctx, repo, number, rootID, resolved)
	if changed {
		t.touch(repo, number)
	}
	return changed, err
}

func (t changingTx) DeleteComment(ctx context.Context, repo, number, id int64) error {
	t.touch(repo, number)
	return t.Ops.DeleteComment(ctx, repo, number, id)
}

func (t changingTx) RecordEvent(ctx context.Context, repo, number int64, e Event) error {
	t.touch(repo, number)
	return t.Ops.RecordEvent(ctx, repo, number, e)
}
