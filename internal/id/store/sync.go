package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/identity"
)

// Load reads the directory's state as the sync decides it — every principal, robots and
// organizations too (whose names the sync must not give), the synced groups, and the retired names
// — and the snapshot of the system of record it last followed (nil before the first sync). People
// and organizations share one namespace of handles, so an organization's handle, held or retired,
// is retired for people too.
func (s *Store) Load(ctx context.Context) (*directory.State, *directory.Snapshot, error) {
	st := directory.NewState()
	all, err := s.Principals(ctx, Filter{IncludeInactive: true})
	if err != nil {
		return nil, nil, err
	}
	for _, p := range all {
		st.Principals[p.Name] = directory.Principal{Name: p.Name, ExternalID: p.ExternalID, Email: p.Email, DisplayName: p.DisplayName, Status: p.Status}
	}
	groups, err := s.b.SyncedGroups(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, g := range groups {
		if g.Members == nil {
			g.Members = []string{}
		}
		st.Groups[g.Name] = g
	}
	retired, err := s.RetiredNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, r := range retired {
		st.Retired[r.Name] = true
	}
	for name := range st.Principals {
		s.retireAsPerson(st, name)
	}
	for _, r := range retired {
		s.retireAsPerson(st, r.Name)
	}
	raw, _, _, _, _, err := s.b.SyncState(ctx)
	if err != nil {
		return nil, nil, err
	}
	if raw == nil {
		return st, nil, nil
	}
	var mirror directory.Snapshot
	if err := json.Unmarshal(raw, &mirror); err != nil {
		return nil, nil, err
	}
	return st, &mirror, nil
}

// applyChunk bounds the changes a sync applies in one transaction: a directory of any size, in
// transactions a database takes.
const applyChunk = 100

// inChunks runs fn over items, at most applyChunk at a time, each in a transaction of its own.
func inChunks[T any](ctx context.Context, s *Store, items []T, fn func(tx Ops, chunk []T) error) error {
	for len(items) > 0 {
		chunk := items[:min(len(items), applyChunk)]
		if err := s.txn(ctx, func(tx Ops) error { return fn(tx, chunk) }); err != nil {
			return err
		}
		items = items[len(chunk):]
	}
	return nil
}

// Apply carries out a sync's plan — people made, changed, renamed (the old name retired), suspended
// (their sessions ended), and deleted (their name retired, their credentials revoked, their
// sessions ended); groups likewise — a chunk of it at a time, each in a transaction of its own,
// then records the snapshot it followed. A sync cut short leaves what it applied, and the next,
// planned against the directory as it is, does the rest: the plan is the directory's distance from
// the source, and the snapshot only where the source's changes are read from next.
func (s *Store) Apply(ctx context.Context, plan *directory.Plan, mirror *directory.Snapshot) error {
	raw, err := json.Marshal(mirror)
	if err != nil {
		return err
	}
	waiting, _ := json.Marshal(nonNil(plan.Waiting))
	notes, _ := json.Marshal(nonNil(plan.Notes))
	if err := inChunks(ctx, s, plan.Humans, func(tx Ops, chunk []directory.HumanChange) error {
		if err := tx.LockNames(ctx); err != nil {
			return err
		}
		for _, c := range chunk {
			a := c.After
			if c.Before == nil || c.Before.Name != a.Name {
				// An organization may have taken the handle since the plan was made.
				held, err := tx.NameHeld(ctx, a.Name)
				if err != nil {
					return err
				}
				if h, ok := s.names.HandleOf(a.Name); ok && !held {
					held, err = tx.NameHeld(ctx, s.names.Org(h))
					if err != nil {
						return err
					}
				}
				if held {
					return ErrTaken
				}
			}
			if c.Before == nil {
				if _, err := tx.InsertPrincipal(ctx, Principal{ID: newID("prn_"), Name: a.Name, Kind: Human, Status: a.Status,
					ExternalID: a.ExternalID, Email: a.Email, DisplayName: a.DisplayName}); err != nil {
					return err
				}
				continue
			}
			id, err := tx.UpdatePerson(ctx, a.ExternalID, a.Name, a.Status, a.Email, a.DisplayName)
			if err != nil {
				return err
			}
			if c.Before.Name != a.Name {
				if err := tx.RetireName(ctx, c.Before.Name, "renamed to "+a.Name); err != nil {
					return err
				}
				if err := s.logOwnerRenamed(ctx, tx, id, c.Before.Name); err != nil {
					return err
				}
			}
			if a.Status != directory.Active {
				if err := tx.DeleteSessionsOf(ctx, id); err != nil {
					return err
				}
			}
			if a.Status == directory.Deleted {
				if err := tx.RevokeCredentialsOf(ctx, id); err != nil {
					return err
				}
				if err := tx.RetireName(ctx, a.Name, "deleted from the system of record"); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := inChunks(ctx, s, plan.Groups, func(tx Ops, chunk []directory.GroupChange) error {
		if err := tx.LockNames(ctx); err != nil {
			return err
		}
		for _, c := range chunk {
			a := c.After
			if a.Members == nil {
				a.Members = []string{}
			}
			if c.Before == nil {
				if err := tx.InsertSyncedGroup(ctx, a); err != nil {
					return err
				}
				continue
			}
			if err := tx.UpdateSyncedGroup(ctx, a); err != nil {
				return err
			}
			if c.Before.Name != a.Name {
				if err := tx.RetireName(ctx, c.Before.Name, "renamed to "+a.Name); err != nil {
					return err
				}
			}
			if a.Deleted {
				if err := tx.RetireName(ctx, a.Name, "deleted from the system of record"); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return s.txn(ctx, func(tx Ops) error { return tx.PutSyncState(ctx, raw, waiting, notes) })
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// SyncFailed records that a sync failed, and how.
func (s *Store) SyncFailed(ctx context.Context, cause error) error {
	return s.b.PutSyncFailure(ctx, cause.Error())
}

// SyncStatus is how the sync last went.
type SyncStatus struct {
	LastRun time.Time
	Error   string
	Waiting []directory.Waiting
	Notes   []string
}

// SyncStatus says how the sync last went; LastRun is zero before it first ran.
func (s *Store) SyncStatus(ctx context.Context) (SyncStatus, error) {
	var st SyncStatus
	_, last, lastErr, waiting, notes, err := s.b.SyncState(ctx)
	if err != nil {
		return st, err
	}
	st.LastRun, st.Error = last, lastErr
	if len(waiting) > 0 {
		if err := json.Unmarshal(waiting, &st.Waiting); err != nil {
			return st, err
		}
	}
	if len(notes) > 0 {
		if err := json.Unmarshal(notes, &st.Notes); err != nil {
			return st, err
		}
	}
	return st, nil
}

// SetName sets the handle (or, for a group, the name) a person or group of the system of record
// gets, over the source's; "" removes it.
func (s *Store) SetName(ctx context.Context, externalID string, group bool, name string) error {
	if name == "" {
		return s.b.DeleteSyncName(ctx, externalID, group)
	}
	if !identity.ValidLabel(name) {
		return errors.New("a handle, or a group's name, is a lowercase DNS label")
	}
	return s.b.PutSyncName(ctx, externalID, group, name)
}

// SyncOptions are what administrators decided for the sync, and the names the tree declares.
func (s *Store) SyncOptions(ctx context.Context) (directory.Options, error) {
	o := directory.Options{Handles: map[string]string{}, GroupNames: map[string]string{}, Declared: map[string]bool{}}
	handles, groups, err := s.b.SyncNames(ctx)
	if err != nil {
		return o, err
	}
	for id, n := range handles {
		o.Handles[id] = n
	}
	for id, n := range groups {
		o.GroupNames[id] = n
	}
	sets, err := s.b.TreeSets(ctx)
	if err != nil {
		return o, err
	}
	for _, ts := range sets {
		o.Declared[ts.Name] = true
	}
	return o, nil
}

// SyncedGroups are the system of record's groups that stand, by full name, with their members.
func (s *Store) SyncedGroups(ctx context.Context) (map[string][]string, error) {
	groups, err := s.b.SyncedGroups(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, g := range groups {
		if !g.Deleted {
			out[g.Name] = g.Members
		}
	}
	return out, nil
}

// retireAsPerson marks the person's name with an organization's handle taken.
func (s *Store) retireAsPerson(st *directory.State, name string) {
	if k, ok := s.names.KindOf(name); ok && k == identity.Org {
		h, _ := s.names.HandleOf(name)
		st.Retired[s.names.Human(h)] = true
	}
}
