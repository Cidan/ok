package spannerstore

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
)

// ─── Enterprise organizations ───

// PutIdentityProvider sets an organization's provider, its plain handles kept once set, as on
// PostgreSQL.
func (x queries) PutIdentityProvider(ctx context.Context, p store.IdentityProvider) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		params := map[string]any{"org": p.Organization, "source": p.Source, "settings": []byte(p.Settings), "sealed": p.Sealed,
			"interval": int64(p.Interval / time.Second), "updated": p.Updated, "plain": p.PlainHandles, "empty": []byte("[]"),
			"key": spanner.NullInt64{Int64: p.Key, Valid: p.Key != 0}}
		n, err := spannerdata.Update(ctx, tx, `UPDATE organization_identity SET source = @source, settings = @settings, sealed = @sealed,
				interval_secs = @interval, updated_at = @updated, plain_handles = plain_handles OR @plain, sync_key = IFNULL(sync_key, @key)
			WHERE organization_id = @org`, params)
		if err != nil || n > 0 {
			return err
		}
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO organization_identity (organization_id, source, settings, sealed, interval_secs,
				last_error, waiting, notes, updated_at, plain_handles, sync_key)
			VALUES (@org, @source, @settings, @sealed, @interval, '', @empty, @empty, @updated, @plain, @key)`, params)
		return err
	})
}

func (x queries) IdentityProviderOf(ctx context.Context, org string) (store.IdentityProvider, error) {
	var p store.IdentityProvider
	var secs int64
	var settings, mirror, waiting, notes []byte
	var lastRun spanner.NullTime
	var key spanner.NullInt64
	err := x.readOne(ctx, `SELECT organization_id, source, settings, sealed, interval_secs, mirror, last_run, last_error, waiting, notes,
			updated_at, plain_handles, sync_key
		FROM organization_identity WHERE organization_id = @org`, map[string]any{"org": org}, func(row *spanner.Row) error {
		return row.Columns(&p.Organization, &p.Source, &settings, &p.Sealed, &secs, &mirror, &lastRun, &p.LastError, &waiting, &notes, &p.Updated,
			&p.PlainHandles, &key)
	})
	p.Key = key.Int64
	if err != nil {
		return store.IdentityProvider{}, err
	}
	p.Settings, p.Mirror, p.Interval, p.LastRun = settings, mirror, time.Duration(secs)*time.Second, lastRun.Time
	if err := json.Unmarshal(waiting, &p.Waiting); err != nil {
		return store.IdentityProvider{}, err
	}
	if err := json.Unmarshal(notes, &p.Notes); err != nil {
		return store.IdentityProvider{}, err
	}
	return p, nil
}

func (x queries) DeleteIdentityProvider(ctx context.Context, org string) (bool, error) {
	n, err := x.Exec(ctx, `DELETE FROM organization_identity WHERE organization_id = @org`, map[string]any{"org": org})
	return n == 1, err
}

func (x queries) IdentityProviderOrg(ctx context.Context, key int64) (string, error) {
	var org string
	err := x.readOne(ctx, `SELECT organization_id FROM organization_identity@{FORCE_INDEX=organization_identity_key} WHERE sync_key = @key`,
		map[string]any{"key": key}, func(row *spanner.Row) error { return row.Columns(&org) })
	return org, err
}

func (x queries) SetIdentityProviderKey(ctx context.Context, org string, key int64) error {
	_, err := x.Exec(ctx, `UPDATE organization_identity SET sync_key = @key WHERE organization_id = @org AND sync_key IS NULL`,
		map[string]any{"org": org, "key": key})
	return err
}

// DueIdentityProviders are the organizations whose sync is due, never run first.
func (x queries) DueIdentityProviders(ctx context.Context, now time.Time) ([]string, error) {
	var out []string
	err := x.readAll(ctx, `SELECT organization_id FROM organization_identity
		WHERE last_run IS NULL OR TIMESTAMP_ADD(last_run, INTERVAL interval_secs SECOND) <= @now ORDER BY last_run`,
		map[string]any{"now": now}, func() { out = nil }, func(row *spanner.Row) error {
			var id string
			if err := row.Columns(&id); err != nil {
				return err
			}
			out = append(out, id)
			return nil
		})
	return out, err
}

func (x queries) PutOrganizationSync(ctx context.Context, org string, mirror, waiting, notes []byte, at time.Time) error {
	_, err := x.Exec(ctx, `UPDATE organization_identity SET mirror = @mirror, waiting = @waiting, notes = @notes, last_run = @at, last_error = ''
		WHERE organization_id = @org`, map[string]any{"org": org, "mirror": mirror, "waiting": orJSON(waiting), "notes": orJSON(notes), "at": at})
	return err
}

func (x queries) PutOrganizationSyncFailure(ctx context.Context, org, cause string, at time.Time) error {
	_, err := x.Exec(ctx, `UPDATE organization_identity SET last_run = @at, last_error = @cause WHERE organization_id = @org`,
		map[string]any{"org": org, "cause": cause, "at": at})
	return err
}

func (x queries) OrganizationPeople(ctx context.Context, org string) (map[string]store.Principal, error) {
	out := map[string]store.Principal{}
	err := x.readAll(ctx, `SELECT `+prefixed("p")+`, op.external_id FROM organization_people op JOIN principals p ON p.id = op.principal_id
		WHERE op.organization_id = @org`, map[string]any{"org": org}, func() { clear(out) }, func(row *spanner.Row) error {
		var id string
		p, err := scanPrincipal(row, &id)
		if err != nil {
			return err
		}
		out[id] = p
		return nil
	})
	return out, err
}

func (x queries) InsertOrganizationPerson(ctx context.Context, org, externalID, principal string) error {
	_, err := x.Exec(ctx, `INSERT INTO organization_people (organization_id, external_id, principal_id) VALUES (@org, @ext, @principal)`,
		map[string]any{"org": org, "ext": externalID, "principal": principal})
	return err
}

func (x queries) PersonOrganization(ctx context.Context, principal string) (string, string, error) {
	var org, ext string
	err := x.readOne(ctx, `SELECT organization_id, external_id FROM organization_people WHERE principal_id = @principal`,
		map[string]any{"principal": principal}, func(row *spanner.Row) error { return row.Columns(&org, &ext) })
	return org, ext, err
}

func (x queries) UpdatePrincipal(ctx context.Context, id, name string, status directory.Status, email, displayName string) error {
	_, err := x.Exec(ctx, `UPDATE principals SET name = @name, status = @status, email = @email, display_name = @display,
		updated_at = CURRENT_TIMESTAMP() WHERE id = @id`,
		map[string]any{"id": id, "name": name, "status": statusText(status), "email": email, "display": displayName})
	return err
}

// UpdateSyncedTeam renames a synced team and sets its members. A team's name is its key: renamed,
// it is written anew under the name and its grants moved to it.
func (x queries) UpdateSyncedTeam(ctx context.Context, org, externalID, name string, members []string) (string, error) {
	var before string
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var t store.Team
		found := false
		if err := spannerdata.Each(ctx, tx, `SELECT `+teamColumns+` FROM teams WHERE organization_id = @org AND external_id = @ext FOR UPDATE`,
			map[string]any{"org": org, "ext": externalID}, func(row *spanner.Row) error {
				var err error
				t, err = scanTeam(row)
				found = true
				return err
			}); err != nil {
			return err
		}
		if !found {
			return store.ErrNotFound
		}
		before = t.Name
		if before == name {
			_, err := spannerdata.Update(ctx, tx, `UPDATE teams SET members = @members WHERE name = @name`,
				map[string]any{"name": name, "members": nonNil(members)})
			return err
		}
		return renameTeam(ctx, tx, t, name, members)
	})
	return before, err
}

// renameTeam writes t anew as name with members, in tx: its grants moved to it, and its old row
// deleted. Its external ID moves with it, so the new row is written once the old one has let it
// go. A team naming it as its parent holds it to its name, as PostgreSQL's foreign key does: the
// old row's delete fails.
func renameTeam(ctx context.Context, tx *spanner.ReadWriteTransaction, t store.Team, name string, members []string) error {
	p := map[string]any{"old": t.Name}
	if _, err := spannerdata.Update(ctx, tx, `UPDATE teams SET external_id = NULL WHERE name = @old`, p); err != nil {
		return err
	}
	moved := t
	moved.Name, moved.Members = name, members
	if _, err := spannerdata.Update(ctx, tx, insertTeam, teamParams(moved)); err != nil {
		return err
	}
	if _, err := moveGrants(ctx, tx, t.Name, name); err != nil {
		return err
	}
	_, err := spannerdata.Update(ctx, tx, `DELETE FROM teams WHERE name = @old`, p)
	return err
}

func (x queries) DeleteSyncedTeam(ctx context.Context, org, externalID string) (string, error) {
	var name string
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		name = ""
		if err := tx.Query(ctx, spanner.Statement{SQL: `DELETE FROM teams WHERE organization_id = @org AND external_id = @ext THEN RETURN name`,
			Params: map[string]any{"org": org, "ext": externalID}}).Do(func(row *spanner.Row) error { return row.Columns(&name) }); err != nil {
			return err
		}
		if name == "" {
			return store.ErrNotFound
		}
		_, err := spannerdata.Update(ctx, tx, `DELETE FROM grants WHERE subject = @name`, map[string]any{"name": name})
		return err
	})
	return name, err
}

func (x queries) PutSCIMToken(ctx context.Context, org string, hash []byte, at time.Time) error {
	_, err := x.Exec(ctx, `INSERT OR UPDATE INTO organization_scim_tokens (organization_id, token_hash, created_at) VALUES (@org, @hash, @at)`,
		map[string]any{"org": org, "hash": hash, "at": at})
	return err
}

func (x queries) SCIMTokenOrganization(ctx context.Context, hash []byte) (string, error) {
	var org string
	err := x.readOne(ctx, `SELECT organization_id FROM organization_scim_tokens WHERE token_hash = @hash`, map[string]any{"hash": hash},
		func(row *spanner.Row) error { return row.Columns(&org) })
	return org, err
}

// ─── Deletions ───

func (x queries) deletions(ctx context.Context, rd spannerdata.Reader, sql string, params map[string]any) ([]store.Deletion, error) {
	var out []store.Deletion
	err := spannerdata.Each(ctx, rd, sql, params, func(row *spanner.Row) error {
		var d store.Deletion
		if err := row.Columns(&d.Principal, &d.Requested, &d.By); err != nil {
			return err
		}
		out = append(out, d)
		return nil
	})
	return out, err
}

func (x queries) InsertDeletion(ctx context.Context, d store.Deletion) error {
	_, err := x.Exec(ctx, `INSERT INTO deletions (principal_id, requested_at, requested_by) VALUES (@principal, @at, @by)`,
		map[string]any{"principal": d.Principal, "at": d.Requested, "by": d.By})
	if unique(err) {
		return store.ErrTaken
	}
	return err
}

func (x queries) DeletionOf(ctx context.Context, principal string) (store.Deletion, error) {
	var out []store.Deletion
	err := x.Read(func(rd spannerdata.Reader) error {
		var err error
		out, err = x.deletions(ctx, rd, `SELECT principal_id, requested_at, requested_by FROM deletions WHERE principal_id = @principal`,
			map[string]any{"principal": principal})
		return err
	})
	if err == nil && len(out) == 0 {
		return store.Deletion{}, store.ErrNotFound
	}
	if err != nil {
		return store.Deletion{}, err
	}
	return out[0], nil
}

func (x queries) RemoveDeletion(ctx context.Context, principal string) (bool, error) {
	n, err := x.Exec(ctx, `DELETE FROM deletions WHERE principal_id = @principal`, map[string]any{"principal": principal})
	return n == 1, err
}

// DueDeletions reads the due deletions FOR UPDATE: with no SKIP LOCKED, two sweepers that read one
// deletion meet at its row, and Spanner runs the second after the first, which finds it gone.
func (x queries) DueDeletions(ctx context.Context, before time.Time, limit int) ([]store.Deletion, error) {
	var out []store.Deletion
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		out, err = x.deletions(ctx, tx, `SELECT principal_id, requested_at, requested_by FROM deletions WHERE requested_at < @before
			ORDER BY requested_at LIMIT @limit FOR UPDATE`, map[string]any{"before": before, "limit": int64(limit)})
		return err
	})
	return out, err
}

// removeFrom is a statement taking a principal out of every row's members, their order kept.
func removeFrom(table, where string) string {
	return `UPDATE ` + table + ` SET members = ARRAY(SELECT m FROM UNNEST(members) AS m WITH OFFSET o WHERE m != @member ORDER BY o)` +
		` WHERE @member IN UNNEST(members)` + where
}

func (x queries) RemoveFromTeams(ctx context.Context, principal string) error {
	_, err := x.Exec(ctx, removeFrom("teams", ""), map[string]any{"member": principal})
	return err
}

func (x queries) DeleteMembersOf(ctx context.Context, org string) error {
	_, err := x.Exec(ctx, `DELETE FROM memberships WHERE organization_id = @org`, map[string]any{"org": org})
	return err
}

func (x queries) DeleteMembershipsOf(ctx context.Context, principal string) error {
	_, err := x.Exec(ctx, `DELETE FROM memberships WHERE principal_id = @principal`, map[string]any{"principal": principal})
	return err
}

func (x queries) RevokeTokensOf(ctx context.Context, principal string, at time.Time) error {
	_, err := x.Exec(ctx, `UPDATE tokens SET revoked_at = @at WHERE principal_id = @principal AND revoked_at IS NULL`,
		map[string]any{"principal": principal, "at": at})
	return err
}

func (x queries) DeleteSignInsOf(ctx context.Context, principal string) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		for _, sql := range []string{`DELETE FROM passwords WHERE principal_id = @principal`, `DELETE FROM login_links WHERE principal_id = @principal`,
			`DELETE FROM passkeys WHERE principal_id = @principal`, `DELETE FROM email_tokens WHERE principal_id = @principal`} {
			if _, err := spannerdata.Update(ctx, tx, sql, map[string]any{"principal": principal}); err != nil {
				return err
			}
		}
		return nil
	})
}

// ─── Moving in ───

func (x queries) TenancyState(ctx context.Context) (string, string, error) {
	var mode string
	var movedIn spanner.NullString
	err := x.readOne(ctx, `SELECT mode, moved_in FROM tenancy_state`, nil, func(row *spanner.Row) error { return row.Columns(&mode, &movedIn) })
	if err == store.ErrNotFound {
		return "", "", nil
	}
	return mode, movedIn.StringVal, err
}

// PutTenancyState records the mode, and the organization moved in when one is given, keeping the
// one recorded otherwise.
func (x queries) PutTenancyState(ctx context.Context, mode, movedIn string, at time.Time) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		p := map[string]any{"mode": mode, "movedIn": nullString(movedIn), "at": spanner.NullTime{Time: at, Valid: movedIn != ""}}
		n, err := spannerdata.Update(ctx, tx, `UPDATE tenancy_state SET mode = @mode, moved_in = IFNULL(@movedIn, moved_in),
			moved_at = IFNULL(@at, moved_at) WHERE singleton`, p)
		if err != nil || n > 0 {
			return err
		}
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO tenancy_state (singleton, mode, moved_in, moved_at) VALUES (TRUE, @mode, @movedIn, @at)`, p)
		return err
	})
}

func (x queries) InsertAlias(ctx context.Context, old, new string) error {
	_, err := x.Exec(ctx, `INSERT OR UPDATE INTO name_aliases (old_name, new_name) VALUES (@old, @new)`, map[string]any{"old": old, "new": new})
	return err
}

func (x queries) Alias(ctx context.Context, old string) (string, error) {
	var n string
	err := x.readOne(ctx, `SELECT new_name FROM name_aliases WHERE old_name = @old`, map[string]any{"old": old},
		func(row *spanner.Row) error { return row.Columns(&n) })
	return n, err
}

func (x queries) Aliases(ctx context.Context) (map[string]string, error) {
	out := map[string]string{}
	err := x.readAll(ctx, `SELECT old_name, new_name FROM name_aliases`, nil, func() { clear(out) }, func(row *spanner.Row) error {
		var o, n string
		if err := row.Columns(&o, &n); err != nil {
			return err
		}
		out[o] = n
		return nil
	})
	return out, err
}

func (x queries) RenameGrantSubject(ctx context.Context, old, new string) ([]int64, error) {
	var repos []int64
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		repos, err = moveGrants(ctx, tx, old, new)
		return err
	})
	return repos, err
}

func (x queries) DeleteSyncedGroup(ctx context.Context, name string) error {
	_, err := x.Exec(ctx, `DELETE FROM synced_groups WHERE name = @name`, map[string]any{"name": name})
	return err
}

func (x queries) ClearSyncState(ctx context.Context) error {
	_, err := x.Exec(ctx, `DELETE FROM sync_state WHERE TRUE`, nil)
	return err
}

func (x queries) MoveSCIM(ctx context.Context, org string) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		for _, sql := range []string{`UPDATE scim_users SET scope = @org WHERE scope = ''`, `UPDATE scim_groups SET scope = @org WHERE scope = ''`} {
			if _, err := spannerdata.Update(ctx, tx, sql, map[string]any{"org": org}); err != nil {
				return err
			}
		}
		return nil
	})
}
