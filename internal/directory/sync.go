package directory

import (
	"context"
	"errors"
	"sync"

	"github.com/Cidan/ok/internal/identity"
)

// Store keeps the directory's state and its mirror of the system of record — what the source
// last said, and its cursor — and applies each plan with the mirror it was made from, in one
// transaction.
type Store interface {
	Load(ctx context.Context) (*State, *Snapshot, error)
	Apply(ctx context.Context, plan *Plan, mirror *Snapshot) error
}

// Sync reads what changed in the system of record since the store's mirror — a delta where the
// source can say, else everything — plans the directory to follow it, and applies the plan. It
// returns the plan, whose Waiting and Notes say what it could not do yet.
func Sync(ctx context.Context, names identity.Names, src Source, store Store, opts Options) (*Plan, error) {
	st, mirror, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	var snap *Snapshot
	if mirror != nil && mirror.Cursor != "" {
		d, err := src.Changes(ctx, mirror.Cursor)
		switch {
		case err == nil:
			snap = Merge(mirror, d)
		case !errors.Is(err, ErrNoDelta):
			return nil, err
		}
	}
	if snap == nil {
		if snap, err = src.Snapshot(ctx); err != nil {
			return nil, err
		}
	}
	plan, err := Reconcile(names, st, snap, opts)
	if err != nil {
		return nil, err
	}
	if err := store.Apply(ctx, plan, snap); err != nil {
		return nil, err
	}
	return plan, nil
}

// Memory is a Store held in memory, for tests and for trying ok out.
type Memory struct {
	mu     sync.Mutex
	state  *State
	mirror *Snapshot
}

// NewMemory is an empty directory in memory.
func NewMemory() *Memory { return &Memory{state: NewState()} }

func (m *Memory) Load(context.Context) (*State, *Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state.clone(), m.mirror, nil
}

func (m *Memory) Apply(_ context.Context, plan *Plan, mirror *Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	Apply(m.state, plan)
	m.mirror = mirror
	return nil
}

func (s *State) clone() *State {
	c := NewState()
	for k, v := range s.Principals {
		c.Principals[k] = v
	}
	for k, v := range s.Groups {
		v.Members = append([]string(nil), v.Members...)
		c.Groups[k] = v
	}
	for k := range s.Retired {
		c.Retired[k] = true
	}
	return c
}
