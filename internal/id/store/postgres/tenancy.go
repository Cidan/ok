package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// ─── Organizations and members ───

const organizationColumns = `o.base_role, o.members_create, o.private_forks, o.enterprise`

func scanOrganization(row scanner) (store.Organization, error) {
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
	_, err := x.q.Exec(ctx, `INSERT INTO organizations (principal_id, base_role, members_create, private_forks, enterprise)
		VALUES ($1, $2, $3, $4, $5)`, o.ID, o.BaseRole.String(), o.MembersCreate, o.PrivateForks, o.Enterprise)
	return err
}

func (x queries) organization(ctx context.Context, id, lock string) (store.Organization, error) {
	return scanOrganization(x.q.QueryRow(ctx, `SELECT `+prefixed("p")+`, `+organizationColumns+`
		FROM organizations o JOIN principals p ON p.id = o.principal_id WHERE o.principal_id = $1`+lock, id))
}

func (x queries) Organization(ctx context.Context, id string) (store.Organization, error) {
	return x.organization(ctx, id, "")
}

func (x queries) LockOrganization(ctx context.Context, id string) (store.Organization, error) {
	return x.organization(ctx, id, " FOR UPDATE OF o")
}

func (x queries) UpdateOrganization(ctx context.Context, o store.Organization) error {
	_, err := x.q.Exec(ctx, `UPDATE organizations SET base_role = $2, members_create = $3, private_forks = $4, enterprise = $5
		WHERE principal_id = $1`, o.ID, o.BaseRole.String(), o.MembersCreate, o.PrivateForks, o.Enterprise)
	return err
}

func (x queries) PutMember(ctx context.Context, org, principal string, role tenancy.Membership) error {
	_, err := x.q.Exec(ctx, `INSERT INTO memberships (organization_id, principal_id, role) VALUES ($1, $2, $3)
		ON CONFLICT (organization_id, principal_id) DO UPDATE SET role = EXCLUDED.role`, org, principal, role.String())
	return err
}

func (x queries) DeleteMember(ctx context.Context, org, principal string) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM memberships WHERE organization_id = $1 AND principal_id = $2`, org, principal)
	return tag.RowsAffected() > 0, err
}

func (x queries) Members(ctx context.Context, org string) ([]store.Member, error) {
	rows, err := x.q.Query(ctx, `SELECT principal_id, role FROM memberships WHERE organization_id = $1 ORDER BY principal_id`, org)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.Member, error) {
		var m store.Member
		var role string
		if err := row.Scan(&m.Principal, &role); err != nil {
			return m, err
		}
		var err error
		m.Role, err = tenancy.ParseMembership(role)
		return m, err
	})
}

func (x queries) MembershipsOf(ctx context.Context, principal string) (map[string]tenancy.Membership, error) {
	rows, err := x.q.Query(ctx, `SELECT organization_id, role FROM memberships WHERE principal_id = $1`, principal)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]tenancy.Membership{}
	for rows.Next() {
		var org, role string
		if err := rows.Scan(&org, &role); err != nil {
			return nil, err
		}
		m, err := tenancy.ParseMembership(role)
		if err != nil {
			return nil, err
		}
		out[org] = m
	}
	return out, rows.Err()
}

// ─── Teams ───

const teamColumns = `name, organization_id, COALESCE(parent, ''), members, COALESCE(external_id, '')`

func scanTeam(row scanner) (store.Team, error) {
	var t store.Team
	if err := row.Scan(&t.Name, &t.Organization, &t.Parent, &t.Members, &t.ExternalID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return store.Team{}, store.ErrNotFound
		}
		return store.Team{}, err
	}
	if t.Members == nil {
		t.Members = []string{}
	}
	return t, nil
}

func (x queries) InsertTeam(ctx context.Context, t store.Team) error {
	var parent *string
	if t.Parent != "" {
		parent = &t.Parent
	}
	var external *string
	if t.ExternalID != "" {
		external = &t.ExternalID
	}
	_, err := x.q.Exec(ctx, `INSERT INTO teams (name, organization_id, parent, members, external_id) VALUES ($1, $2, $3, $4, $5)`,
		t.Name, t.Organization, parent, nonNilStrings(t.Members), external)
	return err
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func (x queries) Team(ctx context.Context, name string) (store.Team, error) {
	return scanTeam(x.q.QueryRow(ctx, `SELECT `+teamColumns+` FROM teams WHERE name = $1`, name))
}

func (x queries) LockTeam(ctx context.Context, name string) (store.Team, error) {
	return scanTeam(x.q.QueryRow(ctx, `SELECT `+teamColumns+` FROM teams WHERE name = $1 FOR UPDATE`, name))
}

func (x queries) teams(ctx context.Context, sql string, args ...any) ([]store.Team, error) {
	rows, err := x.q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.Team, error) { return scanTeam(row) })
}

func (x queries) Teams(ctx context.Context, org string) ([]store.Team, error) {
	return x.teams(ctx, `SELECT `+teamColumns+` FROM teams WHERE organization_id = $1 ORDER BY name`, org)
}

func (x queries) AllTeams(ctx context.Context) ([]store.Team, error) {
	return x.teams(ctx, `SELECT `+teamColumns+` FROM teams ORDER BY name`)
}

func (x queries) SetTeamMembers(ctx context.Context, name string, members []string) error {
	_, err := x.q.Exec(ctx, `UPDATE teams SET members = $2 WHERE name = $1`, name, nonNilStrings(members))
	return err
}

func (x queries) DeleteTeam(ctx context.Context, name string) error {
	_, err := x.q.Exec(ctx, `DELETE FROM teams WHERE name = $1`, name)
	return err
}

// ─── Repositories ───

const repositoryColumns = `id, owner_id, name, visibility, propose, COALESCE(parent_id, 0), network_id, cell, state, state_at, created_by, created_at`

func scanRepository(row scanner) (store.Repository, error) {
	var r store.Repository
	var visibility, propose, state string
	if err := row.Scan(&r.ID, &r.Owner, &r.Name, &visibility, &propose, &r.Parent, &r.Network, &r.Cell, &state, &r.StateAt,
		&r.CreatedBy, &r.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return store.Repository{}, store.ErrNotFound
		}
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

func (x queries) InsertRepository(ctx context.Context, r store.Repository) (store.Repository, error) {
	var parent *int64
	if r.Parent != 0 {
		parent = &r.Parent
	}
	out, err := scanRepository(x.q.QueryRow(ctx, `INSERT INTO repositories (id, owner_id, name, visibility, propose, parent_id,
		network_id, cell, state, state_at, created_by, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING `+repositoryColumns,
		r.ID, r.Owner, r.Name, r.Visibility.String(), r.Propose.String(), parent, r.Network, r.Cell, string(r.State), r.StateAt, r.CreatedBy, r.CreatedAt))
	if unique(err) {
		return store.Repository{}, store.ErrTaken
	}
	return out, err
}

func (x queries) Repository(ctx context.Context, id int64) (store.Repository, error) {
	return scanRepository(x.q.QueryRow(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE id = $1`, id))
}

func (x queries) LockRepository(ctx context.Context, id int64) (store.Repository, error) {
	return scanRepository(x.q.QueryRow(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE id = $1 FOR UPDATE`, id))
}

func (x queries) RepositoryByName(ctx context.Context, owner, name string) (store.Repository, error) {
	return scanRepository(x.q.QueryRow(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE owner_id = $1 AND name = $2`, owner, name))
}

func (x queries) UpdateRepository(ctx context.Context, r store.Repository) error {
	var parent *int64
	if r.Parent != 0 {
		parent = &r.Parent
	}
	_, err := x.q.Exec(ctx, `UPDATE repositories SET owner_id = $2, name = $3, visibility = $4, propose = $5, state = $6, state_at = $7,
		parent_id = $8, cell = $9 WHERE id = $1`, r.ID, r.Owner, r.Name, r.Visibility.String(), r.Propose.String(), string(r.State), r.StateAt, parent,
		r.Cell)
	if unique(err) {
		return store.ErrTaken
	}
	return err
}

func (x queries) ForksOf(ctx context.Context, parent int64) ([]store.Repository, error) {
	return x.repositories(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE parent_id = $1 ORDER BY created_at, id FOR UPDATE`, parent)
}

func (x queries) PersonalForksIn(ctx context.Context, networks []int64) ([]store.Repository, error) {
	return x.repositories(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE network_id = ANY($1) AND parent_id IS NOT NULL
		AND state = 'active' AND owner_id IN (SELECT id FROM principals WHERE kind = 'human') ORDER BY id`, networks)
}

func (x queries) DeleteRepository(ctx context.Context, id int64) error {
	_, err := x.q.Exec(ctx, `WITH i AS (DELETE FROM inbox WHERE repository_id = $1), v AS (DELETE FROM inbox_version WHERE repository_id = $1),
			n AS (DELETE FROM notifications WHERE repository_id = $1)
		DELETE FROM repositories WHERE id = $1`, id)
	return err
}

func (x queries) repositories(ctx context.Context, sql string, args ...any) ([]store.Repository, error) {
	rows, err := x.q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.Repository, error) { return scanRepository(row) })
}

func (x queries) RepositoryCellCounts(ctx context.Context) (map[string]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT cell, count(*) FROM repositories GROUP BY cell`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var cell string
		var n int64
		if err := rows.Scan(&cell, &n); err != nil {
			return nil, err
		}
		out[cell] = n
	}
	return out, rows.Err()
}

func (x queries) RepositoriesOf(ctx context.Context, owner string) ([]store.Repository, error) {
	return x.repositories(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE owner_id = $1 ORDER BY name`, owner)
}

func (x queries) RepositoriesIn(ctx context.Context, state store.RepositoryState, before time.Time, limit int) ([]store.Repository, error) {
	return x.repositories(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE state = $1 AND state_at < $2
		ORDER BY state_at, id LIMIT $3`, string(state), before, limit)
}

func (x queries) Redirect(ctx context.Context, owner, name string) (int64, error) {
	var id int64
	err := x.q.QueryRow(ctx, `SELECT repository_id FROM redirects WHERE owner_id = $1 AND name = $2`, owner, name).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, store.ErrNotFound
	}
	return id, err
}

func (x queries) PutRedirect(ctx context.Context, owner, name string, repo int64) error {
	_, err := x.q.Exec(ctx, `INSERT INTO redirects (owner_id, name, repository_id) VALUES ($1, $2, $3)
		ON CONFLICT (owner_id, name) DO UPDATE SET repository_id = EXCLUDED.repository_id`, owner, name, repo)
	return err
}

func (x queries) DeleteRedirect(ctx context.Context, owner, name string) error {
	_, err := x.q.Exec(ctx, `DELETE FROM redirects WHERE owner_id = $1 AND name = $2`, owner, name)
	return err
}

// ─── Grants ───

func (x queries) PutGrant(ctx context.Context, g store.Grant) error {
	_, err := x.q.Exec(ctx, `INSERT INTO grants (repository_id, subject, role) VALUES ($1, $2, $3)
		ON CONFLICT (repository_id, subject) DO UPDATE SET role = EXCLUDED.role`, g.Repository, g.Subject, g.Role.String())
	return err
}

func (x queries) DeleteGrant(ctx context.Context, repo int64, subject string) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM grants WHERE repository_id = $1 AND subject = $2`, repo, subject)
	return tag.RowsAffected() > 0, err
}

func (x queries) DeleteGrantsTo(ctx context.Context, subject string) ([]int64, error) {
	rows, err := x.q.Query(ctx, `DELETE FROM grants WHERE subject = $1 RETURNING repository_id`, subject)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// ─── The directory's log ───

func (x queries) NextDirectorySeq(ctx context.Context, repo int64) (int64, error) {
	var seq int64
	err := x.q.QueryRow(ctx, `INSERT INTO directory_event_head (repository_id, seq) VALUES ($1, 1)
		ON CONFLICT (repository_id) DO UPDATE SET seq = directory_event_head.seq + 1 RETURNING seq`, repo).Scan(&seq)
	return seq, err
}

func (x queries) InsertDirectoryEvent(ctx context.Context, e store.DirectoryEventRow, at time.Time) error {
	if _, err := x.q.Exec(ctx, `INSERT INTO directory_event (repository_id, seq, at, body) VALUES ($1, $2, $3, $4)`,
		e.Repository, e.Seq, at, e.Body); err != nil {
		return err
	}
	_, err := x.q.Exec(ctx, `INSERT INTO directory_event_pending (repository_id, seq) VALUES ($1, $2)`, e.Repository, e.Seq)
	return err
}

func directoryEvents(rows pgx.Rows, err error) ([]store.DirectoryEventRow, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.DirectoryEventRow, error) {
		var e store.DirectoryEventRow
		err := row.Scan(&e.Repository, &e.Seq, &e.Body)
		return e, err
	})
}

func (x queries) DirectoryEvents(ctx context.Context, repo, after int64, limit int) ([]store.DirectoryEventRow, error) {
	return directoryEvents(x.q.Query(ctx, `SELECT repository_id, seq, body FROM directory_event
		WHERE repository_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`, repo, after, limit))
}

func (x queries) DirectoryHeads(ctx context.Context, after int64, limit int) ([]store.DirectoryHead, error) {
	rows, err := x.q.Query(ctx, `SELECT repository_id, seq FROM directory_event_head WHERE repository_id > $1
		ORDER BY repository_id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.DirectoryHead, error) {
		var h store.DirectoryHead
		err := row.Scan(&h.Repository, &h.Seq)
		return h, err
	})
}

// RelayDirectoryEvents takes a page of pending events in its own transaction, skipping those
// another relay holds, and deletes them from pending once publish succeeds.
func (b *Backend) RelayDirectoryEvents(ctx context.Context, limit int, publish func([]store.DirectoryEventRow) error) (int, error) {
	var n int
	err := pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		evs, err := directoryEvents(tx.Query(ctx, `WITH taken AS (
				SELECT repository_id, seq FROM directory_event_pending
				ORDER BY repository_id, seq LIMIT $1 FOR UPDATE SKIP LOCKED)
			SELECT e.repository_id, e.seq, e.body
			FROM taken t JOIN directory_event e ON e.repository_id = t.repository_id AND e.seq = t.seq
			ORDER BY e.repository_id, e.seq`, limit))
		if err != nil || len(evs) == 0 {
			return err
		}
		if err := publish(evs); err != nil {
			return err
		}
		repos, seqs := make([]int64, len(evs)), make([]int64, len(evs))
		for i, e := range evs {
			repos[i], seqs[i] = e.Repository, e.Seq
		}
		if _, err := tx.Exec(ctx, `DELETE FROM directory_event_pending p USING unnest($1::bigint[], $2::bigint[]) AS u(r, s)
			WHERE p.repository_id = u.r AND p.seq = u.s`, repos, seqs); err != nil {
			return err
		}
		n = len(evs)
		return nil
	})
	return n, err
}

func (x queries) Grants(ctx context.Context, repo int64) ([]store.Grant, error) {
	rows, err := x.q.Query(ctx, `SELECT repository_id, subject, role FROM grants WHERE repository_id = $1 ORDER BY subject`, repo)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.Grant, error) {
		var g store.Grant
		var role string
		if err := row.Scan(&g.Repository, &g.Subject, &role); err != nil {
			return g, err
		}
		var err error
		g.Role, err = tenancy.ParseRole(role)
		return g, err
	})
}
