package spannerstore

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/data/spannertxn"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// ─── Organizations and members ───

const organizationColumns = `o.base_role, o.members_create, o.private_forks, o.enterprise`

func scanOrganization(row *spanner.Row) (store.Organization, error) {
	var o store.Organization
	var role string
	p, err := scanPrincipal(row, &role, &o.MembersCreate, &o.PrivateForks, &o.Enterprise)
	if err != nil {
		return store.Organization{}, err
	}
	o.Principal = p
	if o.BaseRole, err = tenancy.ParseRole(role); err != nil {
		return store.Organization{}, err
	}
	return o, nil
}

func (x queries) InsertOrganization(ctx context.Context, o store.Organization) error {
	_, err := x.Exec(ctx, `INSERT INTO organizations (principal_id, base_role, members_create, private_forks, enterprise)
		VALUES (@id, @role, @create, @forks, @enterprise)`,
		map[string]any{"id": o.ID, "role": o.BaseRole.String(), "create": o.MembersCreate, "forks": o.PrivateForks, "enterprise": o.Enterprise})
	return err
}

const organizationQuery = `SELECT ` + `p.id, p.name, p.kind, p.status, IFNULL(p.external_id, ''), p.email, p.display_name, p.created_at, ` +
	organizationColumns + ` FROM organizations o JOIN principals p ON p.id = o.principal_id WHERE o.principal_id = @id`

func (x queries) Organization(ctx context.Context, id string) (store.Organization, error) {
	var o store.Organization
	err := x.readOne(ctx, organizationQuery, map[string]any{"id": id}, func(row *spanner.Row) error {
		var err error
		o, err = scanOrganization(row)
		return err
	})
	return o, err
}

// LockOrganization holds the organization's settings, as PostgreSQL's FOR UPDATE OF o does: the
// join reads its principal, and the lock is the organizations row's.
func (x queries) LockOrganization(ctx context.Context, id string) (store.Organization, error) {
	var o store.Organization
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var one int64
		if ok, err := spannerdata.First(ctx, tx, `SELECT 1 FROM organizations WHERE principal_id = @id FOR UPDATE`, map[string]any{"id": id}, &one); err != nil || !ok {
			if err == nil {
				err = store.ErrNotFound
			}
			return err
		}
		found := false
		if err := spannerdata.Each(ctx, tx, organizationQuery, map[string]any{"id": id}, func(row *spanner.Row) error {
			var err error
			o, err = scanOrganization(row)
			found = true
			return err
		}); err != nil {
			return err
		}
		if !found {
			return store.ErrNotFound
		}
		return nil
	})
	return o, err
}

func (x queries) UpdateOrganization(ctx context.Context, o store.Organization) error {
	_, err := x.Exec(ctx, `UPDATE organizations SET base_role = @role, members_create = @create, private_forks = @forks, enterprise = @enterprise
		WHERE principal_id = @id`,
		map[string]any{"id": o.ID, "role": o.BaseRole.String(), "create": o.MembersCreate, "forks": o.PrivateForks, "enterprise": o.Enterprise})
	return err
}

func (x queries) PutMember(ctx context.Context, org, principal string, role tenancy.Membership) error {
	_, err := x.Exec(ctx, `INSERT OR UPDATE INTO memberships (organization_id, principal_id, role) VALUES (@org, @principal, @role)`,
		map[string]any{"org": org, "principal": principal, "role": role.String()})
	return err
}

func (x queries) DeleteMember(ctx context.Context, org, principal string) (bool, error) {
	n, err := x.Exec(ctx, `DELETE FROM memberships WHERE organization_id = @org AND principal_id = @principal`,
		map[string]any{"org": org, "principal": principal})
	return n > 0, err
}

func (x queries) Members(ctx context.Context, org string) ([]store.Member, error) {
	var out []store.Member
	err := x.readAll(ctx, `SELECT principal_id, role FROM memberships WHERE organization_id = @org ORDER BY principal_id`, map[string]any{"org": org},
		func() { out = nil }, func(row *spanner.Row) error {
			var m store.Member
			var role string
			if err := row.Columns(&m.Principal, &role); err != nil {
				return err
			}
			var err error
			if m.Role, err = tenancy.ParseMembership(role); err != nil {
				return err
			}
			out = append(out, m)
			return nil
		})
	return out, err
}

func (x queries) MembershipsOf(ctx context.Context, principal string) (map[string]tenancy.Membership, error) {
	out := map[string]tenancy.Membership{}
	err := x.readAll(ctx, `SELECT organization_id, role FROM memberships WHERE principal_id = @principal`, map[string]any{"principal": principal},
		func() { clear(out) }, func(row *spanner.Row) error {
			var org, role string
			if err := row.Columns(&org, &role); err != nil {
				return err
			}
			m, err := tenancy.ParseMembership(role)
			if err != nil {
				return err
			}
			out[org] = m
			return nil
		})
	return out, err
}

// ─── Teams ───

const teamColumns = `name, organization_id, IFNULL(parent, ''), members, IFNULL(external_id, '')`

func scanTeam(row *spanner.Row) (store.Team, error) {
	var t store.Team
	if err := row.Columns(&t.Name, &t.Organization, &t.Parent, &t.Members, &t.ExternalID); err != nil {
		return store.Team{}, err
	}
	t.Members = nonNil(t.Members)
	return t, nil
}

func teamParams(t store.Team) map[string]any {
	return map[string]any{"name": t.Name, "org": t.Organization, "parent": nullString(t.Parent), "members": nonNil(t.Members),
		"ext": nullString(t.ExternalID)}
}

const insertTeam = `INSERT INTO teams (name, organization_id, parent, members, external_id) VALUES (@name, @org, @parent, @members, @ext)`

func (x queries) InsertTeam(ctx context.Context, t store.Team) error {
	_, err := x.Exec(ctx, insertTeam, teamParams(t))
	return err
}

func (x queries) teams(ctx context.Context, sql string, params map[string]any) ([]store.Team, error) {
	var out []store.Team
	err := x.readAll(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		t, err := scanTeam(row)
		if err != nil {
			return err
		}
		out = append(out, t)
		return nil
	})
	return out, err
}

func (x queries) Team(ctx context.Context, name string) (store.Team, error) {
	var t store.Team
	err := x.readOne(ctx, `SELECT `+teamColumns+` FROM teams WHERE name = @name`, map[string]any{"name": name}, func(row *spanner.Row) error {
		var err error
		t, err = scanTeam(row)
		return err
	})
	return t, err
}

func (x queries) LockTeam(ctx context.Context, name string) (store.Team, error) {
	var t store.Team
	err := x.lockOne(ctx, `SELECT `+teamColumns+` FROM teams WHERE name = @name FOR UPDATE`, map[string]any{"name": name}, func(row *spanner.Row) error {
		var err error
		t, err = scanTeam(row)
		return err
	})
	return t, err
}

func (x queries) Teams(ctx context.Context, org string) ([]store.Team, error) {
	return x.teams(ctx, `SELECT `+teamColumns+` FROM teams WHERE organization_id = @org ORDER BY name`, map[string]any{"org": org})
}

func (x queries) AllTeams(ctx context.Context) ([]store.Team, error) {
	return x.teams(ctx, `SELECT `+teamColumns+` FROM teams ORDER BY name`, nil)
}

func (x queries) SetTeamMembers(ctx context.Context, name string, members []string) error {
	_, err := x.Exec(ctx, `UPDATE teams SET members = @members WHERE name = @name`, map[string]any{"name": name, "members": nonNil(members)})
	return err
}

func (x queries) DeleteTeam(ctx context.Context, name string) error {
	_, err := x.Exec(ctx, `DELETE FROM teams WHERE name = @name`, map[string]any{"name": name})
	return err
}

// ─── Repositories ───

const repositoryColumns = `repository_id, owner_id, name, visibility, propose, IFNULL(parent_id, 0), network_id, cell, state, state_at,
	created_by, created_at`

func scanRepository(row *spanner.Row) (store.Repository, error) {
	var r store.Repository
	var visibility, propose, state string
	if err := row.Columns(&r.ID, &r.Owner, &r.Name, &visibility, &propose, &r.Parent, &r.Network, &r.Cell, &state, &r.StateAt,
		&r.CreatedBy, &r.CreatedAt); err != nil {
		return store.Repository{}, err
	}
	var err error
	if r.Visibility, err = tenancy.ParseVisibility(visibility); err != nil {
		return store.Repository{}, err
	}
	if r.Propose, err = tenancy.ParseRole(propose); err != nil {
		return store.Repository{}, err
	}
	r.State = store.RepositoryState(state)
	return r, nil
}

func repositories(ctx context.Context, rd spannerdata.Reader, sql string, params map[string]any) ([]store.Repository, error) {
	var out []store.Repository
	err := spannerdata.Each(ctx, rd, sql, params, func(row *spanner.Row) error {
		r, err := scanRepository(row)
		if err != nil {
			return err
		}
		out = append(out, r)
		return nil
	})
	return out, err
}

func (x queries) repositories(ctx context.Context, sql string, params map[string]any) ([]store.Repository, error) {
	var out []store.Repository
	err := x.Read(func(rd spannerdata.Reader) error {
		var err error
		out, err = repositories(ctx, rd, sql, params)
		return err
	})
	return out, err
}

func oneRepository(rs []store.Repository, err error) (store.Repository, error) {
	if err != nil {
		return store.Repository{}, err
	}
	if len(rs) == 0 {
		return store.Repository{}, store.ErrNotFound
	}
	return rs[0], nil
}

func (x queries) InsertRepository(ctx context.Context, r store.Repository) (store.Repository, error) {
	var out store.Repository
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO repositories (repository_id, owner_id, name, visibility, propose, parent_id,
				network_id, cell, state, state_at, created_by, created_at)
			VALUES (@id, @owner, @name, @visibility, @propose, @parent, @network, @cell, @state, @stateAt, @by, @created)`,
			map[string]any{"id": r.ID, "owner": r.Owner, "name": r.Name, "visibility": r.Visibility.String(), "propose": r.Propose.String(),
				"parent": spanner.NullInt64{Int64: r.Parent, Valid: r.Parent != 0}, "network": r.Network, "cell": r.Cell, "state": string(r.State),
				"stateAt": r.StateAt, "by": r.CreatedBy, "created": r.CreatedAt}); err != nil {
			return err
		}
		var err error
		out, err = oneRepository(repositories(ctx, tx, `SELECT `+repositoryColumns+` FROM repositories WHERE repository_id = @id`,
			map[string]any{"id": r.ID}))
		return err
	})
	if unique(err) {
		return store.Repository{}, store.ErrTaken
	}
	return out, err
}

func (x queries) Repository(ctx context.Context, id int64) (store.Repository, error) {
	return oneRepository(x.repositories(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE repository_id = @id`, map[string]any{"id": id}))
}

func (x queries) LockRepository(ctx context.Context, id int64) (store.Repository, error) {
	var out store.Repository
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		out, err = oneRepository(repositories(ctx, tx, `SELECT `+repositoryColumns+` FROM repositories WHERE repository_id = @id FOR UPDATE`,
			map[string]any{"id": id}))
		return err
	})
	return out, err
}

func (x queries) RepositoryByName(ctx context.Context, owner, name string) (store.Repository, error) {
	return oneRepository(x.repositories(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE owner_id = @owner AND name = @name`,
		map[string]any{"owner": owner, "name": name}))
}

func (x queries) UpdateRepository(ctx context.Context, r store.Repository) error {
	_, err := x.Exec(ctx, `UPDATE repositories SET owner_id = @owner, name = @name, visibility = @visibility, propose = @propose, state = @state,
		state_at = @stateAt, parent_id = @parent, cell = @cell WHERE repository_id = @id`,
		map[string]any{"id": r.ID, "owner": r.Owner, "name": r.Name, "visibility": r.Visibility.String(), "propose": r.Propose.String(),
			"state": string(r.State), "stateAt": r.StateAt, "parent": spanner.NullInt64{Int64: r.Parent, Valid: r.Parent != 0}, "cell": r.Cell})
	if unique(err) {
		return store.ErrTaken
	}
	return err
}

func (x queries) ForksOf(ctx context.Context, parent int64) ([]store.Repository, error) {
	var out []store.Repository
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		out, err = repositories(ctx, tx, `SELECT `+repositoryColumns+` FROM repositories WHERE parent_id = @parent
			ORDER BY created_at, repository_id FOR UPDATE`, map[string]any{"parent": parent})
		return err
	})
	return out, err
}

func (x queries) PersonalForksIn(ctx context.Context, networks []int64) ([]store.Repository, error) {
	return x.repositories(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE network_id IN UNNEST(@networks) AND parent_id IS NOT NULL
		AND state = 'active' AND owner_id IN (SELECT id FROM principals WHERE kind = 'human') ORDER BY repository_id`,
		map[string]any{"networks": networks})
}

// DeleteRepository removes an entry as PostgreSQL's foreign keys have it: its forks' parent set
// NULL, its redirects deleted, its grants with it (interleaved).
func (x queries) DeleteRepository(ctx context.Context, id int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		for _, sql := range []string{
			`UPDATE repositories SET parent_id = NULL WHERE parent_id = @id`,
			`DELETE FROM redirects WHERE repository_id = @id`,
			`DELETE FROM inbox WHERE repository_id = @id`,
			`DELETE FROM notifications WHERE repository_id = @id`,
			`DELETE FROM inbox_version WHERE repository_id = @id`,
			`DELETE FROM repositories WHERE repository_id = @id`,
		} {
			if _, err := spannerdata.Update(ctx, tx, sql, map[string]any{"id": id}); err != nil {
				return err
			}
		}
		return nil
	})
}

func (x queries) RepositoryCellCounts(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	err := x.Read(func(rd spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, rd, `SELECT cell, COUNT(*) FROM repositories GROUP BY cell`, nil, func(row *spanner.Row) error {
			var cell string
			var n int64
			if err := row.Columns(&cell, &n); err != nil {
				return err
			}
			out[cell] = n
			return nil
		})
	})
	return out, err
}

func (x queries) RepositoriesOf(ctx context.Context, owner string) ([]store.Repository, error) {
	return x.repositories(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE owner_id = @owner ORDER BY name`, map[string]any{"owner": owner})
}

func (x queries) RepositoriesIn(ctx context.Context, state store.RepositoryState, before time.Time, limit int) ([]store.Repository, error) {
	return x.repositories(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE state = @state AND state_at < @before
		ORDER BY state_at, repository_id LIMIT @limit`, map[string]any{"state": string(state), "before": before, "limit": int64(limit)})
}

func (x queries) Redirect(ctx context.Context, owner, name string) (int64, error) {
	var id int64
	err := x.readOne(ctx, `SELECT repository_id FROM redirects WHERE owner_id = @owner AND name = @name`,
		map[string]any{"owner": owner, "name": name}, func(row *spanner.Row) error { return row.Columns(&id) })
	return id, err
}

func (x queries) PutRedirect(ctx context.Context, owner, name string, repo int64) error {
	_, err := x.Exec(ctx, `INSERT OR UPDATE INTO redirects (owner_id, name, repository_id) VALUES (@owner, @name, @repo)`,
		map[string]any{"owner": owner, "name": name, "repo": repo})
	return err
}

func (x queries) DeleteRedirect(ctx context.Context, owner, name string) error {
	_, err := x.Exec(ctx, `DELETE FROM redirects WHERE owner_id = @owner AND name = @name`, map[string]any{"owner": owner, "name": name})
	return err
}

// ─── Grants ───

func (x queries) PutGrant(ctx context.Context, g store.Grant) error {
	_, err := x.Exec(ctx, `INSERT OR UPDATE INTO grants (repository_id, subject, role) VALUES (@repo, @subject, @role)`,
		map[string]any{"repo": g.Repository, "subject": g.Subject, "role": g.Role.String()})
	return err
}

func (x queries) DeleteGrant(ctx context.Context, repo int64, subject string) (bool, error) {
	n, err := x.Exec(ctx, `DELETE FROM grants WHERE repository_id = @repo AND subject = @subject`, map[string]any{"repo": repo, "subject": subject})
	return n > 0, err
}

func (x queries) DeleteGrantsTo(ctx context.Context, subject string) ([]int64, error) {
	var out []int64
	err := x.Returning(ctx, `DELETE FROM grants WHERE subject = @subject THEN RETURN repository_id`, map[string]any{"subject": subject},
		func() { out = nil }, func(row *spanner.Row) error {
			var id int64
			if err := row.Columns(&id); err != nil {
				return err
			}
			out = append(out, id)
			return nil
		})
	return out, err
}

func (x queries) Grants(ctx context.Context, repo int64) ([]store.Grant, error) {
	var out []store.Grant
	err := x.readAll(ctx, `SELECT repository_id, subject, role FROM grants WHERE repository_id = @repo ORDER BY subject`, map[string]any{"repo": repo},
		func() { out = nil }, func(row *spanner.Row) error {
			var g store.Grant
			var role string
			if err := row.Columns(&g.Repository, &g.Subject, &role); err != nil {
				return err
			}
			var err error
			if g.Role, err = tenancy.ParseRole(role); err != nil {
				return err
			}
			out = append(out, g)
			return nil
		})
	return out, err
}

// moveGrants moves every grant to old onto new, in tx: a grant's subject is in its key, so each is
// written anew and the old deleted. It answers the repositories whose grants moved.
func moveGrants(ctx context.Context, tx *spanner.ReadWriteTransaction, old, new string) ([]int64, error) {
	type grant struct {
		repo int64
		role string
	}
	var gs []grant
	if err := spannerdata.Each(ctx, tx, `SELECT repository_id, role FROM grants WHERE subject = @old`, map[string]any{"old": old},
		func(row *spanner.Row) error {
			var g grant
			if err := row.Columns(&g.repo, &g.role); err != nil {
				return err
			}
			gs = append(gs, g)
			return nil
		}); err != nil {
		return nil, err
	}
	repos := make([]int64, 0, len(gs))
	for _, g := range gs {
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO grants (repository_id, subject, role) VALUES (@repo, @new, @role)`,
			map[string]any{"repo": g.repo, "new": new, "role": g.role}); err != nil {
			return nil, fmt.Errorf("moving repository %d's grant to %s: %w", g.repo, new, err)
		}
		repos = append(repos, g.repo)
	}
	if _, err := spannerdata.Update(ctx, tx, `DELETE FROM grants WHERE subject = @old`, map[string]any{"old": old}); err != nil {
		return nil, err
	}
	return repos, nil
}

// ─── The directory's log ───

func (x queries) NextDirectorySeq(ctx context.Context, repo int64) (int64, error) {
	var seq int64
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		seq = 0
		ok, err := spannerdata.First(ctx, tx, `SELECT seq FROM directory_event_head WHERE repository_id = @repo`, map[string]any{"repo": repo}, &seq)
		if err != nil {
			return err
		}
		seq++
		sql := `INSERT INTO directory_event_head (repository_id, seq) VALUES (@repo, @seq)`
		if ok {
			sql = `UPDATE directory_event_head SET seq = @seq WHERE repository_id = @repo`
		}
		_, err = spannerdata.Update(ctx, tx, sql, map[string]any{"repo": repo, "seq": seq})
		return err
	})
	return seq, err
}

func (x queries) InsertDirectoryEvent(ctx context.Context, e store.DirectoryEventRow, at time.Time) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		p := map[string]any{"repo": e.Repository, "seq": e.Seq, "at": at, "body": e.Body}
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO directory_event (repository_id, seq, event_at, body) VALUES (@repo, @seq, @at, @body)`, p); err != nil {
			return err
		}
		_, err := spannerdata.Update(ctx, tx, `INSERT INTO directory_event_pending (repository_id, seq) VALUES (@repo, @seq)`, p)
		return err
	})
}

func (x queries) DirectoryEvents(ctx context.Context, repo, after int64, limit int) ([]store.DirectoryEventRow, error) {
	var out []store.DirectoryEventRow
	err := x.readAll(ctx, `SELECT repository_id, seq, body FROM directory_event WHERE repository_id = @repo AND seq > @after ORDER BY seq LIMIT @limit`,
		map[string]any{"repo": repo, "after": after, "limit": int64(limit)}, func() { out = nil }, func(row *spanner.Row) error {
			var e store.DirectoryEventRow
			if err := row.Columns(&e.Repository, &e.Seq, &e.Body); err != nil {
				return err
			}
			out = append(out, e)
			return nil
		})
	return out, err
}

func (x queries) DirectoryHeads(ctx context.Context, after int64, limit int) ([]store.DirectoryHead, error) {
	var out []store.DirectoryHead
	err := x.readAll(ctx, `SELECT repository_id, seq FROM directory_event_head WHERE repository_id > @after ORDER BY repository_id LIMIT @limit`,
		map[string]any{"after": after, "limit": int64(limit)}, func() { out = nil }, func(row *spanner.Row) error {
			var h store.DirectoryHead
			if err := row.Columns(&h.Repository, &h.Seq); err != nil {
				return err
			}
			out = append(out, h)
			return nil
		})
	return out, err
}

// relayLease is how long a relay holds the pending events it took before another may take them:
// publishing a page takes far less, and a relay that died has its events taken again once it
// lapses.
const relayLease = time.Minute

// RelayDirectoryEvents takes a page of pending events no other relay holds, holding them for a
// lease in a transaction of its own; publishes them outside any transaction, as Spanner aborts a
// transaction left waiting and may run its function again; then deletes them from pending, or,
// if publishing failed, lets them go for another to take. A relay that dies after publishing has
// its events published again once its lease lapses, as on PostgreSQL a relay that publishes and
// dies before committing does.
func (b *Backend) RelayDirectoryEvents(ctx context.Context, limit int, publish func([]store.DirectoryEventRow) error) (int, error) {
	holder := fmt.Sprintf("relay-%d", data.NewID())
	var evs []store.DirectoryEventRow
	_, err := spannertxn.ReadWrite(ctx, b.Client, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		evs = nil
		if err := spannerdata.Each(ctx, tx, `SELECT e.repository_id, e.seq, e.body
			FROM directory_event_pending p JOIN directory_event e ON e.repository_id = p.repository_id AND e.seq = p.seq
			WHERE p.lease_until IS NULL OR p.lease_until < CURRENT_TIMESTAMP()
			ORDER BY p.repository_id, p.seq LIMIT @limit`, map[string]any{"limit": int64(limit)}, func(row *spanner.Row) error {
			var e store.DirectoryEventRow
			if err := row.Columns(&e.Repository, &e.Seq, &e.Body); err != nil {
				return err
			}
			evs = append(evs, e)
			return nil
		}); err != nil || len(evs) == 0 {
			return err
		}
		for _, e := range evs {
			if _, err := spannerdata.Update(ctx, tx, `UPDATE directory_event_pending SET holder = @holder,
					lease_until = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @lease SECOND)
				WHERE repository_id = @repo AND seq = @seq`,
				map[string]any{"holder": holder, "lease": int64(relayLease / time.Second), "repo": e.Repository, "seq": e.Seq}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil || len(evs) == 0 {
		return 0, err
	}
	end := `DELETE FROM directory_event_pending WHERE holder = @holder`
	perr := publish(evs)
	if perr != nil {
		end = `UPDATE directory_event_pending SET holder = NULL, lease_until = NULL WHERE holder = @holder`
	}
	if _, err := spannertxn.ReadWrite(context.WithoutCancel(ctx), b.Client, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := spannerdata.Update(ctx, tx, end, map[string]any{"holder": holder})
		return err
	}); err != nil && perr == nil {
		return 0, fmt.Errorf("the published events, ending their lease: %w", err)
	}
	if perr != nil {
		return 0, perr
	}
	return len(evs), nil
}
