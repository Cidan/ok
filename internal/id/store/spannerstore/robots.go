package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// ─── Organizations' robots ───

func (x queries) InsertOrganizationRobot(ctx context.Context, robot, org string) error {
	_, err := x.Exec(ctx, `INSERT INTO organization_robots (principal_id, organization_id) VALUES (@robot, @org)`,
		map[string]any{"robot": robot, "org": org})
	return err
}

func (x queries) RobotOrganization(ctx context.Context, robot string) (string, error) {
	var org string
	err := x.readOne(ctx, `SELECT organization_id FROM organization_robots WHERE principal_id = @robot`, map[string]any{"robot": robot},
		func(row *spanner.Row) error { return row.Columns(&org) })
	return org, err
}

func (x queries) OrganizationRobots(ctx context.Context, org string) ([]store.Principal, error) {
	return x.principals(ctx, `SELECT `+principalColumns+` FROM principals
		WHERE id IN (SELECT principal_id FROM organization_robots WHERE organization_id = @org) ORDER BY name`, map[string]any{"org": org})
}

func (x queries) InsertIssuer(ctx context.Context, org, issuer string, at time.Time) error {
	_, err := x.Exec(ctx, `INSERT OR IGNORE INTO organization_issuers (organization_id, issuer, created_at) VALUES (@org, @issuer, @at)`,
		map[string]any{"org": org, "issuer": issuer, "at": at})
	return err
}

func (x queries) DeleteIssuer(ctx context.Context, org, issuer string) (bool, error) {
	var had bool
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		p := map[string]any{"org": org, "issuer": issuer}
		n, err := spannerdata.Update(ctx, tx, `DELETE FROM organization_issuers WHERE organization_id = @org AND issuer = @issuer`, p)
		if err != nil {
			return err
		}
		had = n == 1
		_, err = spannerdata.Update(ctx, tx, `DELETE FROM robot_bindings WHERE organization_id = @org AND issuer = @issuer`, p)
		return err
	})
	return had, err
}

func (x queries) Issuers(ctx context.Context, org string) ([]string, error) {
	var out []string
	err := x.readAll(ctx, `SELECT issuer FROM organization_issuers WHERE organization_id = @org ORDER BY issuer`, map[string]any{"org": org},
		func() { out = nil }, func(row *spanner.Row) error {
			var s string
			if err := row.Columns(&s); err != nil {
				return err
			}
			out = append(out, s)
			return nil
		})
	return out, err
}

func (x queries) TrustsIssuer(ctx context.Context, org, issuer string) (bool, error) {
	var ok bool
	err := x.readOne(ctx, `SELECT EXISTS (SELECT 1 FROM organization_issuers WHERE organization_id = @org AND issuer = @issuer)`,
		map[string]any{"org": org, "issuer": issuer}, func(row *spanner.Row) error { return row.Columns(&ok) })
	return ok, err
}

func (x queries) OrganizationBindings(ctx context.Context, org string) (map[string][]store.Binding, error) {
	out := map[string][]store.Binding{}
	err := x.readAll(ctx, `SELECT principal_id, issuer, subject FROM robot_bindings WHERE organization_id = @org ORDER BY issuer, subject`,
		map[string]any{"org": org}, func() { clear(out) }, func(row *spanner.Row) error {
			var id string
			b := store.Binding{Organization: org}
			if err := row.Columns(&id, &b.Issuer, &b.Subject); err != nil {
				return err
			}
			out[id] = append(out[id], b)
			return nil
		})
	return out, err
}

func (x queries) DeleteOrganizationBinding(ctx context.Context, robot string, b store.Binding) (bool, error) {
	n, err := x.Exec(ctx, `DELETE FROM robot_bindings WHERE principal_id = @robot AND organization_id = @org AND issuer = @issuer AND subject = @subject`,
		map[string]any{"robot": robot, "org": b.Organization, "issuer": b.Issuer, "subject": b.Subject})
	return n == 1, err
}

// ─── Tokens ───

// tokenColumns are a token's, its installation's organization with them (t: tokens, i: its
// installation).
const tokenColumns = `t.id, t.principal_id, t.kind, t.label, t.role, t.repositories, t.created_at, t.expires_at, t.last_used_at,
	t.revoked_at, IFNULL(t.installation_id, ''), IFNULL(i.organization_id, '')`

const tokenFrom = `tokens t LEFT JOIN app_installations i ON i.id = t.installation_id`

func scanToken(row *spanner.Row) (store.Token, error) {
	var t store.Token
	var role string
	var lastUsed, revoked spanner.NullTime
	if err := row.Columns(&t.ID, &t.Principal, &t.Kind, &t.Label, &role, &t.Repositories, &t.Created, &t.Expires, &lastUsed, &revoked,
		&t.Installation, &t.Owner); err != nil {
		return store.Token{}, err
	}
	var err error
	if t.Role, err = tenancy.ParseRole(role); err != nil {
		return store.Token{}, err
	}
	t.Repositories, t.LastUsed, t.Revoked = nonNil(t.Repositories), lastUsed.Time, revoked.Time
	return t, nil
}

func (x queries) tokens(ctx context.Context, where string, params map[string]any) ([]store.Token, error) {
	var out []store.Token
	err := x.readAll(ctx, `SELECT `+tokenColumns+` FROM `+tokenFrom+` `+where, params, func() { out = nil }, func(row *spanner.Row) error {
		t, err := scanToken(row)
		if err != nil {
			return err
		}
		out = append(out, t)
		return nil
	})
	return out, err
}

func (x queries) InsertToken(ctx context.Context, t store.Token, hash []byte) error {
	_, err := x.Exec(ctx, `INSERT INTO tokens (id, token_hash, principal_id, kind, label, role, repositories, created_at, expires_at, installation_id)
		VALUES (@id, @hash, @principal, @kind, @label, @role, @repos, @created, @expires, @installation)`,
		map[string]any{"id": t.ID, "hash": hash, "principal": t.Principal, "kind": t.Kind, "label": t.Label, "role": t.Role.String(),
			"repos": nonNil(t.Repositories), "created": t.Created, "expires": t.Expires, "installation": nullString(t.Installation)})
	return err
}

func (x queries) TokenByHash(ctx context.Context, hash []byte) (store.Token, error) {
	ts, err := x.tokens(ctx, `WHERE t.token_hash = @hash`, map[string]any{"hash": hash})
	if err != nil {
		return store.Token{}, err
	}
	if len(ts) == 0 {
		return store.Token{}, store.ErrNotFound
	}
	return ts[0], nil
}

func (x queries) TouchToken(ctx context.Context, id string, at time.Time) error {
	_, err := x.Exec(ctx, `UPDATE tokens SET last_used_at = @at WHERE id = @id`, map[string]any{"id": id, "at": at})
	return err
}

func (x queries) Tokens(ctx context.Context, principal string) ([]store.Token, error) {
	return x.tokens(ctx, `WHERE t.principal_id = @principal ORDER BY t.created_at DESC, t.id`, map[string]any{"principal": principal})
}

func (x queries) RevokeToken(ctx context.Context, principal, id string, at time.Time) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE tokens SET revoked_at = @at WHERE id = @id AND principal_id = @principal AND revoked_at IS NULL`,
		map[string]any{"principal": principal, "id": id, "at": at})
	return n == 1, err
}
