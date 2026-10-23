package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) PutIdentityProvider(ctx context.Context, p store.IdentityProvider) error {
	_, err := x.q.Exec(ctx, `INSERT INTO organization_identity (organization_id, source, settings, sealed, interval_secs, updated_at, plain_handles)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (organization_id) DO UPDATE SET source = EXCLUDED.source, settings = EXCLUDED.settings, sealed = EXCLUDED.sealed,
			interval_secs = EXCLUDED.interval_secs, updated_at = EXCLUDED.updated_at,
			plain_handles = organization_identity.plain_handles OR EXCLUDED.plain_handles`,
		p.Organization, p.Source, []byte(p.Settings), p.Sealed, int(p.Interval/time.Second), p.Updated, p.PlainHandles)
	return err
}

func (x queries) IdentityProviderOf(ctx context.Context, org string) (store.IdentityProvider, error) {
	var p store.IdentityProvider
	var secs int
	var mirror, waiting, notes []byte
	var lastRun *time.Time
	err := x.q.QueryRow(ctx, `SELECT organization_id, source, settings, sealed, interval_secs, mirror, last_run, last_error, waiting, notes, updated_at,
		plain_handles FROM organization_identity WHERE organization_id = $1`, org).
		Scan(&p.Organization, &p.Source, &p.Settings, &p.Sealed, &secs, &mirror, &lastRun, &p.LastError, &waiting, &notes, &p.Updated, &p.PlainHandles)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.IdentityProvider{}, store.ErrNotFound
	}
	if err != nil {
		return store.IdentityProvider{}, err
	}
	p.Interval = time.Duration(secs) * time.Second
	p.Mirror = mirror
	if lastRun != nil {
		p.LastRun = *lastRun
	}
	if err := json.Unmarshal(waiting, &p.Waiting); err != nil {
		return store.IdentityProvider{}, err
	}
	if err := json.Unmarshal(notes, &p.Notes); err != nil {
		return store.IdentityProvider{}, err
	}
	return p, nil
}

func (x queries) DeleteIdentityProvider(ctx context.Context, org string) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM organization_identity WHERE organization_id = $1`, org)
	return tag.RowsAffected() == 1, err
}

func (x queries) DueIdentityProviders(ctx context.Context, now time.Time) ([]string, error) {
	rows, err := x.q.Query(ctx, `SELECT organization_id FROM organization_identity
		WHERE last_run IS NULL OR last_run + make_interval(secs => interval_secs) <= $1 ORDER BY last_run NULLS FIRST`, now)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (x queries) PutOrganizationSync(ctx context.Context, org string, mirror, waiting, notes []byte, at time.Time) error {
	_, err := x.q.Exec(ctx, `UPDATE organization_identity SET mirror = $2, waiting = $3, notes = $4, last_run = $5, last_error = ''
		WHERE organization_id = $1`, org, mirror, waiting, notes, at)
	return err
}

func (x queries) PutOrganizationSyncFailure(ctx context.Context, org, cause string, at time.Time) error {
	_, err := x.q.Exec(ctx, `UPDATE organization_identity SET last_run = $2, last_error = $3 WHERE organization_id = $1`, org, at, cause)
	return err
}

func (x queries) OrganizationPeople(ctx context.Context, org string) (map[string]store.Principal, error) {
	rows, err := x.q.Query(ctx, `SELECT `+prefixed("p")+`, op.external_id FROM organization_people op JOIN principals p ON p.id = op.principal_id
		WHERE op.organization_id = $1`, org)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]store.Principal{}
	for rows.Next() {
		var id string
		p, err := scanPrincipal(rows, &id)
		if err != nil {
			return nil, err
		}
		out[id] = p
	}
	return out, rows.Err()
}

func (x queries) InsertOrganizationPerson(ctx context.Context, org, externalID, principal string) error {
	_, err := x.q.Exec(ctx, `INSERT INTO organization_people (organization_id, external_id, principal_id) VALUES ($1, $2, $3)`, org, externalID, principal)
	return err
}

func (x queries) PersonOrganization(ctx context.Context, principal string) (string, string, error) {
	var org, ext string
	err := x.q.QueryRow(ctx, `SELECT organization_id, external_id FROM organization_people WHERE principal_id = $1`, principal).Scan(&org, &ext)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", store.ErrNotFound
	}
	return org, ext, err
}

func (x queries) UpdatePrincipal(ctx context.Context, id, name string, status directory.Status, email, displayName string) error {
	_, err := x.q.Exec(ctx, `UPDATE principals SET name = $2, status = $3, email = $4, display_name = $5, updated_at = now() WHERE id = $1`,
		id, name, statusText(status), email, displayName)
	return err
}

func (x queries) UpdateSyncedTeam(ctx context.Context, org, externalID, name string, members []string) (string, error) {
	var before string
	err := x.q.QueryRow(ctx, `SELECT name FROM teams WHERE organization_id = $1 AND external_id = $2 FOR UPDATE`, org, externalID).Scan(&before)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", store.ErrNotFound
	}
	if err != nil {
		return "", err
	}
	if _, err := x.q.Exec(ctx, `UPDATE teams SET name = $2, members = $3 WHERE name = $1`, before, name, nonNilStrings(members)); err != nil {
		return "", err
	}
	if before != name {
		if _, err := x.q.Exec(ctx, `UPDATE grants SET subject = $2 WHERE subject = $1`, before, name); err != nil {
			return "", err
		}
	}
	return before, nil
}

func (x queries) DeleteSyncedTeam(ctx context.Context, org, externalID string) (string, error) {
	var name string
	err := x.q.QueryRow(ctx, `DELETE FROM teams WHERE organization_id = $1 AND external_id = $2 RETURNING name`, org, externalID).Scan(&name)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", store.ErrNotFound
	}
	if err != nil {
		return "", err
	}
	_, err = x.q.Exec(ctx, `DELETE FROM grants WHERE subject = $1`, name)
	return name, err
}

func (x queries) PutSCIMToken(ctx context.Context, org string, hash []byte, at time.Time) error {
	_, err := x.q.Exec(ctx, `INSERT INTO organization_scim_tokens (organization_id, token_hash, created_at) VALUES ($1, $2, $3)
		ON CONFLICT (organization_id) DO UPDATE SET token_hash = EXCLUDED.token_hash, created_at = EXCLUDED.created_at`, org, hash, at)
	return err
}

func (x queries) SCIMTokenOrganization(ctx context.Context, hash []byte) (string, error) {
	var org string
	err := x.q.QueryRow(ctx, `SELECT organization_id FROM organization_scim_tokens WHERE token_hash = $1`, hash).Scan(&org)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", store.ErrNotFound
	}
	return org, err
}
