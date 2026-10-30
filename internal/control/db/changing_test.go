package db

import (
	"context"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/wait"
)

// store is a Store whose writes change what a test says, its steps recorded.
type store struct {
	Store
	ops *ops
}

func (s store) Txn(_ context.Context, fn func(tx Ops) error) error { return fn(s.ops) }

type ops struct {
	Ops
	changes bool
	pended  []string
}

func (o *ops) UpsertChangeList(context.Context, ChangeList) (bool, error) { return o.changes, nil }
func (o *ops) SetApproved(context.Context, int64, int64, bool) (bool, error) {
	return o.changes, nil
}
func (o *ops) RecordEvent(context.Context, int64, int64, Event) error { return nil }

func (o *ops) PendStep(_ context.Context, kind string, _ int64, _ time.Time, body []byte) error {
	o.pended = append(o.pended, kind+" "+string(body))
	return nil
}

// A write that changed a CL says so, for its watchers; one that changed nothing — the upsert of the
// state held that every status a watch reads makes — says nothing, or the read would wake the watch.
func TestChangingSaysWhatChanged(t *testing.T) {
	o := &ops{}
	s := Changing(store{ops: o})
	ctx := context.Background()
	changed := wait.ChangedKind + " " + wait.CLSubject(1, 4)
	if _, err := s.UpsertChangeList(ctx, ChangeList{Repo: 1, Number: 4}); err != nil || len(o.pended) != 0 {
		t.Fatalf("an upsert of the state held: %v, %v", o.pended, err)
	}
	if _, err := s.SetApproved(ctx, 1, 4, true); err != nil || len(o.pended) != 0 {
		t.Fatalf("an approval as it stood: %v, %v", o.pended, err)
	}
	o.changes = true
	if _, err := s.UpsertChangeList(ctx, ChangeList{Repo: 1, Number: 4}); err != nil || len(o.pended) != 1 || o.pended[0] != changed {
		t.Fatalf("an upsert that wrote: %v, %v", o.pended, err)
	}
	o.pended, o.changes = nil, false
	if err := s.RecordEvent(ctx, 1, 4, Event{}); err != nil || len(o.pended) != 1 || o.pended[0] != changed {
		t.Fatalf("an event recorded: %v, %v", o.pended, err)
	}
}
