package store

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/identity"
)

// namesLock serializes what gives out names — the sync, applying the tree, making robots — so two
// never give one name at once: their names live in different tables.
var namesLock = func() int64 {
	h := fnv.New64a()
	h.Write([]byte("ok-identity-names"))
	return int64(h.Sum64())
}()

func lockNames(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, namesLock)
	return err
}

// Load reads the directory's state as the sync decides it — every principal, robots too (whose
// names the sync must not give), the synced groups, and the retired names — and the snapshot of
// the system of record it last followed (nil before the first sync).
func (s *Store) Load(ctx context.Context) (*directory.State, *directory.Snapshot, error) {
	st := directory.NewState()
	all, err := s.Principals(ctx, Filter{IncludeInactive: true})
	if err != nil {
		return nil, nil, err
	}
	for _, p := range all {
		st.Principals[p.Name] = directory.Principal{Name: p.Name, ExternalID: p.ExternalID, Email: p.Email, DisplayName: p.DisplayName, Status: p.Status}
	}
	rows, err := s.pool.Query(ctx, `SELECT name, external_id, members, deleted FROM synced_groups`)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var g directory.SyncedGroup
		if err := rows.Scan(&g.Name, &g.ExternalID, &g.Members, &g.Deleted); err != nil {
			rows.Close()
			return nil, nil, err
		}
		if g.Members == nil {
			g.Members = []string{}
		}
		st.Groups[g.Name] = g
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	retired, err := s.RetiredNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, r := range retired {
		st.Retired[r.Name] = true
	}
	var raw []byte
	err = s.pool.QueryRow(ctx, `SELECT mirror FROM sync_state`).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && raw == nil {
		return st, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var mirror directory.Snapshot
	if err := json.Unmarshal(raw, &mirror); err != nil {
		return nil, nil, err
	}
	return st, &mirror, nil
}

// Apply carries out a sync's plan in one transaction, with the snapshot it followed: people made,
// changed, renamed (the old name retired), suspended (their sessions ended), and deleted (their
// name retired, their credentials revoked, their sessions ended); groups likewise.
func (s *Store) Apply(ctx context.Context, plan *directory.Plan, mirror *directory.Snapshot) error {
	raw, err := json.Marshal(mirror)
	if err != nil {
		return err
	}
	waiting, _ := json.Marshal(nonNil(plan.Waiting))
	notes, _ := json.Marshal(nonNil(plan.Notes))
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		for _, c := range plan.Humans {
			a := c.After
			if c.Before == nil {
				free, err := nameFree(ctx, tx, a.Name)
				if err != nil {
					return err
				}
				if !free {
					return ErrTaken
				}
				if _, err := tx.Exec(ctx, `INSERT INTO principals (id, name, kind, status, external_id, email, display_name)
					VALUES ($1, $2, 'human', $3, $4, $5, $6)`, newID("prn_"), a.Name, statusText(a.Status), a.ExternalID, a.Email, a.DisplayName); err != nil {
					return err
				}
				continue
			}
			var id string
			if err := tx.QueryRow(ctx, `UPDATE principals SET name = $2, status = $3, email = $4, display_name = $5, updated_at = now()
				WHERE external_id = $1 RETURNING id`, a.ExternalID, a.Name, statusText(a.Status), a.Email, a.DisplayName).Scan(&id); err != nil {
				return err
			}
			if c.Before.Name != a.Name {
				if err := retire(ctx, tx, c.Before.Name, "renamed to "+a.Name); err != nil {
					return err
				}
			}
			if a.Status != directory.Active {
				if _, err := tx.Exec(ctx, `DELETE FROM sessions WHERE principal_id = $1`, id); err != nil {
					return err
				}
			}
			if a.Status == directory.Deleted {
				if _, err := tx.Exec(ctx, `UPDATE credentials SET revoked_at = now() WHERE principal_id = $1 AND revoked_at IS NULL`, id); err != nil {
					return err
				}
				if err := retire(ctx, tx, a.Name, "deleted from the system of record"); err != nil {
					return err
				}
			}
		}
		for _, c := range plan.Groups {
			a := c.After
			members := a.Members
			if members == nil {
				members = []string{}
			}
			if c.Before == nil {
				if _, err := tx.Exec(ctx, `INSERT INTO synced_groups (name, external_id, members) VALUES ($1, $2, $3)`, a.Name, a.ExternalID, members); err != nil {
					return err
				}
				continue
			}
			if _, err := tx.Exec(ctx, `UPDATE synced_groups SET name = $2, members = $3, deleted = $4, updated_at = now() WHERE external_id = $1`,
				a.ExternalID, a.Name, members, a.Deleted); err != nil {
				return err
			}
			if c.Before.Name != a.Name {
				if err := retire(ctx, tx, c.Before.Name, "renamed to "+a.Name); err != nil {
					return err
				}
			}
			if a.Deleted {
				if err := retire(ctx, tx, a.Name, "deleted from the system of record"); err != nil {
					return err
				}
			}
		}
		_, err := tx.Exec(ctx, `INSERT INTO sync_state (singleton, mirror, last_run, last_error, waiting, notes) VALUES (true, $1, now(), '', $2, $3)
			ON CONFLICT (singleton) DO UPDATE SET mirror = $1, last_run = now(), last_error = '', waiting = $2, notes = $3`, raw, waiting, notes)
		return err
	})
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// SyncFailed records that a sync failed, and how.
func (s *Store) SyncFailed(ctx context.Context, cause error) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO sync_state (singleton, last_run, last_error) VALUES (true, now(), $1)
		ON CONFLICT (singleton) DO UPDATE SET last_run = now(), last_error = $1`, cause.Error())
	return err
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
	var last *time.Time
	var waiting, notes []byte
	err := s.pool.QueryRow(ctx, `SELECT last_run, last_error, waiting, notes FROM sync_state`).Scan(&last, &st.Error, &waiting, &notes)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	if last != nil {
		st.LastRun = *last
	}
	if err := json.Unmarshal(waiting, &st.Waiting); err != nil {
		return st, err
	}
	return st, json.Unmarshal(notes, &st.Notes)
}

// SetName sets the handle (or, for a group, the name) a person or group of the system of record
// gets, over the source's; "" removes it.
func (s *Store) SetName(ctx context.Context, externalID string, group bool, name string) error {
	if name == "" {
		_, err := s.pool.Exec(ctx, `DELETE FROM sync_names WHERE external_id = $1 AND is_group = $2`, externalID, group)
		return err
	}
	if !identity.ValidLabel(name) {
		return errors.New("a handle, or a group's name, is a lowercase DNS label")
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO sync_names (external_id, is_group, name) VALUES ($1, $2, $3)
		ON CONFLICT (external_id, is_group) DO UPDATE SET name = $3`, externalID, group, name)
	return err
}

// SyncOptions are what administrators decided for the sync, and the names the tree declares.
func (s *Store) SyncOptions(ctx context.Context) (directory.Options, error) {
	o := directory.Options{Handles: map[string]string{}, GroupNames: map[string]string{}, Declared: map[string]bool{}}
	rows, err := s.pool.Query(ctx, `SELECT external_id, is_group, name FROM sync_names`)
	if err != nil {
		return o, err
	}
	for rows.Next() {
		var id, name string
		var group bool
		if err := rows.Scan(&id, &group, &name); err != nil {
			rows.Close()
			return o, err
		}
		if group {
			o.GroupNames[id] = name
		} else {
			o.Handles[id] = name
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return o, err
	}
	rows, err = s.pool.Query(ctx, `SELECT name FROM tree_sets`)
	if err != nil {
		return o, err
	}
	defer rows.Close()
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return o, err
		}
		o.Declared[n] = true
	}
	return o, rows.Err()
}

// SyncedGroups are the system of record's groups that stand, by full name, with their members.
func (s *Store) SyncedGroups(ctx context.Context) (map[string][]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT name, members FROM synced_groups WHERE NOT deleted`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var n string
		var ms []string
		if err := rows.Scan(&n, &ms); err != nil {
			return nil, err
		}
		out[n] = ms
	}
	return out, rows.Err()
}
