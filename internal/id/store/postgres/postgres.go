// Package postgres is identity's store backend (internal/id/store) on PostgreSQL, in the schema
// identity.
package postgres

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/migrate"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are identity's schema.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Backend is identity's store backend on PostgreSQL.
type Backend struct {
	queries
	pool  *pgxpool.Pool
	locks *lock.Lock
}

var _ store.Backend = (*Backend)(nil)

// Open migrates the database pool reaches and returns a backend over it.
func Open(ctx context.Context, pool *pgxpool.Pool) (*Backend, error) {
	if _, err := migrate.Apply(ctx, pool, "identity", Migrations); err != nil {
		return nil, err
	}
	return &Backend{queries: queries{pool}, pool: pool, locks: lock.New(pool)}, nil
}

// Txn runs fn in a transaction, again if PostgreSQL aborts it to break a deadlock or a
// serialization conflict.
func (b *Backend) Txn(ctx context.Context, fn func(store.Ops) error) error {
	return pgdata.Txn(ctx, b.pool, func(tx pgx.Tx) error {
		// What ctx's lease guards is written only while its holder holds it (data.Fence).
		if err := pgdata.CheckFence(ctx, tx); err != nil {
			return err
		}
		return fn(queries{tx})
	})
}

func (b *Backend) Lock(ctx context.Context, bucket, id string, wait time.Duration, fn lock.LockFn) error {
	return b.locks.LockFn(ctx, bucket, id, wait, fn)
}

func (b *Backend) Ping(ctx context.Context) error { return b.pool.Ping(ctx) }

func (b *Backend) SCIM() scim.Store { return scimStore{pool: b.pool} }

// SCIMOf is an organization's SCIM, by its ID.
func (b *Backend) SCIMOf(org string) scim.Store { return scimStore{pool: b.pool, scope: org} }

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// queries are the backend's operations over a pool or a transaction.
type queries struct{ q querier }

func unique(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}

func statusText(s directory.Status) string {
	switch s {
	case directory.Suspended:
		return "suspended"
	case directory.Deleted:
		return "deleted"
	}
	return "active"
}

func parseStatus(s string) directory.Status {
	switch s {
	case "suspended":
		return directory.Suspended
	case "deleted":
		return directory.Deleted
	}
	return directory.Active
}

const principalColumns = `id, name, kind, status, COALESCE(external_id, ''), email, display_name, created_at`

// prefixed is principalColumns qualified by a table alias.
func prefixed(alias string) string {
	return alias + `.id, ` + alias + `.name, ` + alias + `.kind, ` + alias + `.status, COALESCE(` + alias + `.external_id, ''), ` +
		alias + `.email, ` + alias + `.display_name, ` + alias + `.created_at`
}

type scanner interface{ Scan(...any) error }

func scanPrincipal(row scanner, extra ...any) (store.Principal, error) {
	var p store.Principal
	var kind, status string
	dest := append([]any{&p.ID, &p.Name, &kind, &status, &p.ExternalID, &p.Email, &p.DisplayName, &p.Created}, extra...)
	if err := row.Scan(dest...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return store.Principal{}, store.ErrNotFound
		}
		return store.Principal{}, err
	}
	p.Kind, p.Status = store.Kind(kind), parseStatus(status)
	return p, nil
}

func collectPrincipals(rows pgx.Rows, err error) ([]store.Principal, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Principal
	for rows.Next() {
		p, err := scanPrincipal(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ─── Principals and names ───

func (x queries) PrincipalByName(ctx context.Context, name string) (store.Principal, error) {
	return scanPrincipal(x.q.QueryRow(ctx, `SELECT `+principalColumns+` FROM principals WHERE name = $1`, name))
}

func (x queries) PrincipalByID(ctx context.Context, id string) (store.Principal, error) {
	return scanPrincipal(x.q.QueryRow(ctx, `SELECT `+principalColumns+` FROM principals WHERE id = $1`, id))
}

func (x queries) PrincipalByExternalID(ctx context.Context, externalID string) (store.Principal, error) {
	return scanPrincipal(x.q.QueryRow(ctx, `SELECT `+principalColumns+` FROM principals WHERE external_id = $1`, externalID))
}

func (x queries) LockPrincipalByName(ctx context.Context, name string) (store.Principal, error) {
	return scanPrincipal(x.q.QueryRow(ctx, `SELECT `+principalColumns+` FROM principals WHERE name = $1 FOR UPDATE`, name))
}

func likeEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

func (x queries) Principals(ctx context.Context, f store.Filter) ([]store.Principal, error) {
	q := `SELECT ` + principalColumns + ` FROM principals WHERE name > $1`
	args := []any{f.After}
	if f.Kind != "" {
		args = append(args, string(f.Kind))
		q += fmt.Sprintf(" AND kind = $%d", len(args))
	}
	if !f.IncludeInactive {
		q += " AND status = 'active'"
	}
	if f.Query != "" {
		args = append(args, "%"+likeEscape(strings.ToLower(f.Query))+"%")
		q += fmt.Sprintf(` AND (lower(name) LIKE $%[1]d OR lower(email) LIKE $%[1]d OR lower(display_name) LIKE $%[1]d)`, len(args))
	}
	q += " ORDER BY name"
	if f.Limit > 0 {
		args = append(args, f.Limit)
		q += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	return collectPrincipals(x.q.Query(ctx, q, args...))
}

func (x queries) PeopleByEmail(ctx context.Context, email string) ([]store.Principal, error) {
	return collectPrincipals(x.q.Query(ctx, `SELECT `+principalColumns+` FROM principals
		WHERE kind = 'human' AND email <> '' AND lower(email) = lower($1) LIMIT 2`, email))
}

func (x queries) InsertPrincipal(ctx context.Context, p store.Principal) (store.Principal, error) {
	var ext *string
	if p.ExternalID != "" {
		ext = &p.ExternalID
	}
	return scanPrincipal(x.q.QueryRow(ctx, `INSERT INTO principals (id, name, kind, status, external_id, email, display_name)
		VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING `+principalColumns,
		p.ID, p.Name, string(p.Kind), statusText(p.Status), ext, p.Email, p.DisplayName))
}

func (x queries) UpdatePerson(ctx context.Context, externalID, name string, status directory.Status, email, displayName string) (string, error) {
	var id string
	err := x.q.QueryRow(ctx, `UPDATE principals SET name = $2, status = $3, email = $4, display_name = $5, updated_at = now()
		WHERE external_id = $1 RETURNING id`, externalID, name, statusText(status), email, displayName).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", store.ErrNotFound
	}
	return id, err
}

func (x queries) SetStatus(ctx context.Context, id string, status directory.Status) error {
	_, err := x.q.Exec(ctx, `UPDATE principals SET status = $2, updated_at = now() WHERE id = $1`, id, statusText(status))
	return err
}

func (x queries) LockNames(ctx context.Context) error { return lock.InTx(ctx, x.q, "names", "all") }

func (x queries) NameHeld(ctx context.Context, name string) (bool, error) {
	var held bool
	err := x.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM principals WHERE name = $1)
		OR EXISTS (SELECT 1 FROM retired_names WHERE name = $1)
		OR EXISTS (SELECT 1 FROM synced_groups WHERE name = $1)
		OR EXISTS (SELECT 1 FROM tree_sets WHERE name = $1)
		OR EXISTS (SELECT 1 FROM teams WHERE name = $1)`, name).Scan(&held)
	return held, err
}

func (x queries) RetireName(ctx context.Context, name, reason string) error {
	_, err := x.q.Exec(ctx, `INSERT INTO retired_names (name, reason) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING`, name, reason)
	return err
}

func (x queries) RetiredNames(ctx context.Context) ([]store.RetiredName, error) {
	rows, err := x.q.Query(ctx, `SELECT name, retired_at, reason FROM retired_names ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.RetiredName, error) {
		var n store.RetiredName
		err := r.Scan(&n.Name, &n.Retired, &n.Reason)
		return n, err
	})
}

func (x queries) AppendAudit(ctx context.Context, actor, action string, detail map[string]any) error {
	_, err := x.q.Exec(ctx, `INSERT INTO audit (id, actor, action, detail) VALUES ($4, $1, $2, $3)`, actor, action, detail, data.NewID())
	return err
}

// ─── Robots' bindings ───

func (x queries) InsertBinding(ctx context.Context, principalID string, b store.Binding) error {
	_, err := x.q.Exec(ctx, `INSERT INTO robot_bindings (organization_id, issuer, subject, principal_id) VALUES ($1, $2, $3, $4)`,
		b.Organization, b.Issuer, b.Subject, principalID)
	if unique(err) {
		return store.ErrBindingTaken
	}
	return err
}

func (x queries) DeleteBinding(ctx context.Context, robot string, b store.Binding) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM robot_bindings rb USING principals p
		WHERE rb.principal_id = p.id AND p.name = $1 AND rb.organization_id = $2 AND rb.issuer = $3 AND rb.subject = $4`,
		robot, b.Organization, b.Issuer, b.Subject)
	return tag.RowsAffected() > 0, err
}

func (x queries) DeleteBindingsOf(ctx context.Context, principalID string) error {
	_, err := x.q.Exec(ctx, `DELETE FROM robot_bindings WHERE principal_id = $1`, principalID)
	return err
}

func (x queries) BoundPrincipal(ctx context.Context, b store.Binding) (store.Principal, error) {
	return scanPrincipal(x.q.QueryRow(ctx, `SELECT `+prefixed("p")+` FROM robot_bindings rb JOIN principals p ON p.id = rb.principal_id
		WHERE rb.organization_id = $1 AND rb.issuer = $2 AND rb.subject = $3`, b.Organization, b.Issuer, b.Subject))
}

func (x queries) Bindings(ctx context.Context) (map[string][]store.Binding, error) {
	rows, err := x.q.Query(ctx, `SELECT principal_id, organization_id, issuer, subject FROM robot_bindings ORDER BY organization_id, issuer, subject`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]store.Binding{}
	for rows.Next() {
		var id string
		var b store.Binding
		if err := rows.Scan(&id, &b.Organization, &b.Issuer, &b.Subject); err != nil {
			return nil, err
		}
		out[id] = append(out[id], b)
	}
	return out, rows.Err()
}

// ─── Credentials ───

const credentialColumns = `c.id, c.label, c.public_key, c.created_at, c.last_issued_at, c.not_after, c.revoked_at, ` // then the principal's

func scanCredential(row scanner) (store.Credential, error) {
	var c store.Credential
	var last, notAfter, revoked *time.Time
	var p store.Principal
	var kind, status string
	err := row.Scan(&c.ID, &c.Label, &c.PublicKey, &c.Created, &last, &notAfter, &revoked,
		&p.ID, &p.Name, &kind, &status, &p.ExternalID, &p.Email, &p.DisplayName, &p.Created)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Credential{}, store.ErrNotFound
	}
	if err != nil {
		return store.Credential{}, err
	}
	p.Kind, p.Status = store.Kind(kind), parseStatus(status)
	c.Principal = p
	if last != nil {
		c.LastIssued = *last
	}
	if notAfter != nil {
		c.NotAfter = *notAfter
	}
	if revoked != nil {
		c.Revoked = *revoked
	}
	return c, nil
}

func (x queries) CredentialByKey(ctx context.Context, publicKey []byte) (id, owner string, revoked, ok bool, err error) {
	var at *time.Time
	err = x.q.QueryRow(ctx, `SELECT id, principal_id, revoked_at FROM credentials WHERE public_key = $1`, publicKey).Scan(&id, &owner, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, false, nil
	}
	return id, owner, at != nil, err == nil, err
}

func (x queries) InsertCredential(ctx context.Context, id, principalID, label string, publicKey []byte) error {
	_, err := x.q.Exec(ctx, `INSERT INTO credentials (id, principal_id, label, public_key) VALUES ($1, $2, $3, $4)`, id, principalID, label, publicKey)
	if unique(err) {
		return store.ErrKeyEnrolled
	}
	return err
}

func (x queries) Credential(ctx context.Context, id string) (store.Credential, error) {
	return scanCredential(x.q.QueryRow(ctx, `SELECT `+credentialColumns+prefixed("p")+`
		FROM credentials c JOIN principals p ON p.id = c.principal_id WHERE c.id = $1`, id))
}

func (x queries) CredentialsOf(ctx context.Context, principalID string) ([]store.Credential, error) {
	rows, err := x.q.Query(ctx, `SELECT `+credentialColumns+prefixed("p")+`
		FROM credentials c JOIN principals p ON p.id = c.principal_id WHERE c.principal_id = $1 ORDER BY c.created_at DESC, c.id`, principalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Credential
	for rows.Next() {
		c, err := scanCredential(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (x queries) SetIssued(ctx context.Context, id string, notAfter time.Time) error {
	_, err := x.q.Exec(ctx, `UPDATE credentials SET last_issued_at = now(), not_after = $2 WHERE id = $1`, id, notAfter)
	return err
}

func (x queries) RevokeCredential(ctx context.Context, id string) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE credentials SET revoked_at = COALESCE(revoked_at, now()) WHERE id = $1`, id)
	return tag.RowsAffected() > 0, err
}

func (x queries) RevokeCredentialsOf(ctx context.Context, principalID string) error {
	_, err := x.q.Exec(ctx, `UPDATE credentials SET revoked_at = now() WHERE principal_id = $1 AND revoked_at IS NULL`, principalID)
	return err
}

// ─── Device logins ───

const deviceLoginColumns = `user_code, public_key, label, client_address, created_at, expires_at, state, COALESCE(principal_id, ''), COALESCE(credential_id, '')`

func scanDeviceLogin(row scanner, extra ...any) (store.DeviceLogin, error) {
	var d store.DeviceLogin
	dest := append([]any{&d.UserCode, &d.PublicKey, &d.Label, &d.ClientAddress, &d.Created, &d.Expires, &d.State, &d.PrincipalID, &d.CredentialID}, extra...)
	err := row.Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.DeviceLogin{}, store.ErrNotFound
	}
	return d, err
}

func (x queries) InsertDeviceLogin(ctx context.Context, deviceCodeHash []byte, userCode string, publicKey []byte, label, clientAddress string, expires time.Time) error {
	_, err := x.q.Exec(ctx, `INSERT INTO device_logins (device_code_hash, user_code, public_key, label, client_address, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)`, deviceCodeHash, userCode, publicKey, label, clientAddress, expires)
	if unique(err) {
		return store.ErrUserCodeTaken
	}
	return err
}

func (x queries) PendingDeviceLogin(ctx context.Context, userCode string) (store.DeviceLogin, error) {
	return scanDeviceLogin(x.q.QueryRow(ctx, `SELECT `+deviceLoginColumns+` FROM device_logins
		WHERE user_code = $1 AND state = 'pending' AND expires_at > now()`, userCode))
}

func (x queries) DecideDeviceLogin(ctx context.Context, userCode, principalID, state string) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE device_logins SET state = $3, principal_id = $2
		WHERE user_code = $1 AND state = 'pending' AND expires_at > now()`, userCode, principalID, state)
	return tag.RowsAffected() > 0, err
}

func (x queries) LockDeviceLogin(ctx context.Context, deviceCodeHash []byte) (store.DeviceLogin, time.Time, bool, error) {
	var last *time.Time
	d, err := scanDeviceLogin(x.q.QueryRow(ctx, `SELECT `+deviceLoginColumns+`, last_poll_at FROM device_logins WHERE device_code_hash = $1 FOR UPDATE`,
		deviceCodeHash), &last)
	if errors.Is(err, store.ErrNotFound) {
		return store.DeviceLogin{}, time.Time{}, false, nil
	}
	if err != nil {
		return store.DeviceLogin{}, time.Time{}, false, err
	}
	var lp time.Time
	if last != nil {
		lp = *last
	}
	return d, lp, true, nil
}

func (x queries) SetPolled(ctx context.Context, deviceCodeHash []byte) error {
	_, err := x.q.Exec(ctx, `UPDATE device_logins SET last_poll_at = now() WHERE device_code_hash = $1`, deviceCodeHash)
	return err
}

func (x queries) CompleteDeviceLogin(ctx context.Context, deviceCodeHash []byte, credentialID string) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE device_logins SET state = 'done', credential_id = $2
		WHERE device_code_hash = $1 AND state = 'approved'`, deviceCodeHash, credentialID)
	return tag.RowsAffected() > 0, err
}

func (x queries) DeleteStaleDeviceLogins(ctx context.Context) error {
	_, err := x.q.Exec(ctx, `DELETE FROM device_logins WHERE expires_at < now() - interval '1 day'`)
	return err
}

// ─── Sessions and ceremonies ───

func (x queries) InsertSession(ctx context.Context, tokenHash []byte, principalID, via string, expires, idleUntil time.Time) error {
	_, err := x.q.Exec(ctx, `INSERT INTO sessions (token_hash, principal_id, via, expires_at, idle_until) VALUES ($1, $2, $3, $4, $5)`,
		tokenHash, principalID, via, expires, idleUntil)
	return err
}

func (x queries) LiveSession(ctx context.Context, tokenHash []byte) (store.Principal, time.Time, error) {
	var idleUntil time.Time
	p, err := scanPrincipal(x.q.QueryRow(ctx, `SELECT `+prefixed("p")+`, s.idle_until FROM sessions s JOIN principals p ON p.id = s.principal_id
		WHERE s.token_hash = $1 AND s.expires_at > now() AND s.idle_until > now()`, tokenHash), &idleUntil)
	return p, idleUntil, err
}

func (x queries) ExtendSession(ctx context.Context, tokenHash []byte, idleUntil time.Time) error {
	_, err := x.q.Exec(ctx, `UPDATE sessions SET idle_until = LEAST($2, expires_at) WHERE token_hash = $1`, tokenHash, idleUntil)
	return err
}

func (x queries) DeleteSession(ctx context.Context, tokenHash []byte) error {
	_, err := x.q.Exec(ctx, `DELETE FROM sessions WHERE token_hash = $1`, tokenHash)
	return err
}

func (x queries) DeleteSessionsOf(ctx context.Context, principalID string) error {
	_, err := x.q.Exec(ctx, `DELETE FROM sessions WHERE principal_id = $1`, principalID)
	return err
}

func (x queries) DeleteExpired(ctx context.Context) error {
	for _, q := range []string{
		`DELETE FROM sessions WHERE expires_at < now() OR idle_until < now()`,
		`DELETE FROM ceremonies WHERE expires_at < now()`,
		`DELETE FROM approvals WHERE expires_at < now() - interval '1 day'`,
	} {
		if _, err := x.q.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (x queries) SecretsRoot(ctx context.Context, org string) (string, error) {
	var root string
	err := x.q.QueryRow(ctx, `SELECT root FROM organization_secrets_roots WHERE organization_id = $1`, org).Scan(&root)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return root, err
}

func (x queries) SetSecretsRoot(ctx context.Context, org, root, by string) error {
	if root == "" {
		_, err := x.q.Exec(ctx, `DELETE FROM organization_secrets_roots WHERE organization_id = $1`, org)
		return err
	}
	_, err := x.q.Exec(ctx, `INSERT INTO organization_secrets_roots (organization_id, root, set_by) VALUES ($1, $2, $3)
		ON CONFLICT (organization_id) DO UPDATE SET root = $2, set_by = $3, set_at = now()`, org, root, by)
	return err
}

func (x queries) InsertApproval(ctx context.Context, a store.Approval) error {
	_, err := x.q.Exec(ctx, `INSERT INTO approvals (id, principal_id, scope, description, created_at, expires_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		a.ID, a.PrincipalID, a.Scope, a.Description, a.Created, a.Expires)
	return err
}

func (x queries) Approval(ctx context.Context, id string) (store.Approval, error) {
	var a store.Approval
	err := x.q.QueryRow(ctx, `SELECT id, principal_id, scope, description, created_at, expires_at, decision, decided_at FROM approvals WHERE id = $1`,
		id).Scan(&a.ID, &a.PrincipalID, &a.Scope, &a.Description, &a.Created, &a.Expires, &a.Decision, &a.Decided)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, store.ErrNotFound
	}
	return a, err
}

func (x queries) DecideApproval(ctx context.Context, id, principalID string, approved bool) (bool, error) {
	decision := store.ApprovalDeclined
	if approved {
		decision = store.ApprovalApproved
	}
	tag, err := x.q.Exec(ctx, `UPDATE approvals SET decision = $3, decided_at = now()
		WHERE id = $1 AND principal_id = $2 AND decision = '' AND expires_at > now()`, id, principalID, decision)
	return tag.RowsAffected() == 1, err
}

func (x queries) InsertCeremony(ctx context.Context, id, kind string, data []byte, expires time.Time) error {
	_, err := x.q.Exec(ctx, `INSERT INTO ceremonies (id, kind, data, expires_at) VALUES ($1, $2, $3, $4)`, id, kind, data, expires)
	return err
}

func (x queries) TakeCeremony(ctx context.Context, id, kind string) ([]byte, error) {
	var raw []byte
	err := x.q.QueryRow(ctx, `DELETE FROM ceremonies WHERE id = $1 AND kind = $2 AND expires_at > now() RETURNING data`, id, kind).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return raw, err
}

func (x queries) PeekCeremony(ctx context.Context, id, kind string) ([]byte, error) {
	var raw []byte
	err := x.q.QueryRow(ctx, `SELECT data FROM ceremonies WHERE id = $1 AND kind = $2 AND expires_at > now()`, id, kind).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return raw, err
}

// ─── Passkeys ───

func (x queries) InsertPasskey(ctx context.Context, p store.Passkey) error {
	_, err := x.q.Exec(ctx, `INSERT INTO passkeys (id, principal_id, data, label) VALUES ($1, $2, $3, $4)`, p.ID, p.PrincipalID, []byte(p.Data), p.Label)
	return err
}

func scanPasskeys(rows pgx.Rows, err error) ([]store.Passkey, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Passkey
	for rows.Next() {
		var p store.Passkey
		var last *time.Time
		var raw []byte
		if err := rows.Scan(&p.ID, &p.PrincipalID, &raw, &p.Label, &p.Created, &last); err != nil {
			return nil, err
		}
		p.Data = raw
		if last != nil {
			p.LastUsed = *last
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (x queries) PasskeysOf(ctx context.Context, principalID string) ([]store.Passkey, error) {
	return scanPasskeys(x.q.Query(ctx, `SELECT id, principal_id, data, label, created_at, last_used_at FROM passkeys WHERE principal_id = $1 ORDER BY created_at`, principalID))
}

func (x queries) Passkey(ctx context.Context, id []byte) (store.Passkey, error) {
	ps, err := scanPasskeys(x.q.Query(ctx, `SELECT id, principal_id, data, label, created_at, last_used_at FROM passkeys WHERE id = $1`, id))
	if err != nil {
		return store.Passkey{}, err
	}
	if len(ps) == 0 {
		return store.Passkey{}, store.ErrNotFound
	}
	return ps[0], nil
}

func (x queries) UsePasskey(ctx context.Context, id []byte, data json.RawMessage) error {
	_, err := x.q.Exec(ctx, `UPDATE passkeys SET data = $2, last_used_at = now() WHERE id = $1`, id, []byte(data))
	return err
}

func (x queries) DeletePasskey(ctx context.Context, principalID string, id []byte) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM passkeys WHERE id = $1 AND principal_id = $2`, id, principalID)
	return tag.RowsAffected() > 0, err
}

// ─── The sync ───

func (x queries) SyncedGroups(ctx context.Context) ([]directory.SyncedGroup, error) {
	rows, err := x.q.Query(ctx, `SELECT name, external_id, members, deleted FROM synced_groups ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (directory.SyncedGroup, error) {
		var g directory.SyncedGroup
		err := r.Scan(&g.Name, &g.ExternalID, &g.Members, &g.Deleted)
		return g, err
	})
}

func (x queries) InsertSyncedGroup(ctx context.Context, g directory.SyncedGroup) error {
	_, err := x.q.Exec(ctx, `INSERT INTO synced_groups (name, external_id, members) VALUES ($1, $2, $3)`, g.Name, g.ExternalID, g.Members)
	return err
}

func (x queries) UpdateSyncedGroup(ctx context.Context, g directory.SyncedGroup) error {
	_, err := x.q.Exec(ctx, `UPDATE synced_groups SET name = $2, members = $3, deleted = $4, updated_at = now() WHERE external_id = $1`,
		g.ExternalID, g.Name, g.Members, g.Deleted)
	return err
}

func (x queries) SyncState(ctx context.Context) (mirror []byte, lastRun time.Time, lastError string, waiting, notes []byte, err error) {
	var last *time.Time
	err = x.q.QueryRow(ctx, `SELECT mirror, last_run, last_error, waiting, notes FROM sync_state`).Scan(&mirror, &last, &lastError, &waiting, &notes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, time.Time{}, "", nil, nil, nil
	}
	if last != nil {
		lastRun = *last
	}
	return mirror, lastRun, lastError, waiting, notes, err
}

func (x queries) PutSyncState(ctx context.Context, mirror, waiting, notes []byte) error {
	_, err := x.q.Exec(ctx, `INSERT INTO sync_state (singleton, mirror, last_run, last_error, waiting, notes) VALUES (true, $1, now(), '', $2, $3)
		ON CONFLICT (singleton) DO UPDATE SET mirror = $1, last_run = now(), last_error = '', waiting = $2, notes = $3`, mirror, waiting, notes)
	return err
}

func (x queries) PutSyncFailure(ctx context.Context, cause string) error {
	_, err := x.q.Exec(ctx, `INSERT INTO sync_state (singleton, last_run, last_error) VALUES (true, now(), $1)
		ON CONFLICT (singleton) DO UPDATE SET last_run = now(), last_error = $1`, cause)
	return err
}

func (x queries) SyncNames(ctx context.Context) (map[string]string, map[string]string, error) {
	rows, err := x.q.Query(ctx, `SELECT external_id, is_group, name FROM sync_names`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	handles, groups := map[string]string{}, map[string]string{}
	for rows.Next() {
		var id, name string
		var group bool
		if err := rows.Scan(&id, &group, &name); err != nil {
			return nil, nil, err
		}
		if group {
			groups[id] = name
		} else {
			handles[id] = name
		}
	}
	return handles, groups, rows.Err()
}

func (x queries) PutSyncName(ctx context.Context, externalID string, group bool, name string) error {
	_, err := x.q.Exec(ctx, `INSERT INTO sync_names (external_id, is_group, name) VALUES ($1, $2, $3)
		ON CONFLICT (external_id, is_group) DO UPDATE SET name = $3`, externalID, group, name)
	return err
}

func (x queries) DeleteSyncName(ctx context.Context, externalID string, group bool) error {
	_, err := x.q.Exec(ctx, `DELETE FROM sync_names WHERE external_id = $1 AND is_group = $2`, externalID, group)
	return err
}

// ─── The groups-and-ACLs tree ───

func (x queries) TreeState(ctx context.Context) (store.Applied, bool, error) {
	var a store.Applied
	err := x.q.QueryRow(ctx, `SELECT commit, sequence, applied_at FROM tree_state`).Scan(&a.Commit, &a.Sequence, &a.At)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Applied{}, false, nil
	}
	return a, err == nil, err
}

func (x queries) TreeSets(ctx context.Context) ([]store.StoredSet, error) {
	rows, err := x.q.Query(ctx, `SELECT name, kind, path, owner, members FROM tree_sets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.StoredSet, error) {
		var s store.StoredSet
		err := r.Scan(&s.Name, &s.Kind, &s.Path, &s.Owner, &s.Members)
		return s, err
	})
}

func (x queries) ReplaceTreeSets(ctx context.Context, sets []store.StoredSet) error {
	if _, err := x.q.Exec(ctx, `DELETE FROM tree_sets`); err != nil {
		return err
	}
	for _, s := range sets {
		if _, err := x.q.Exec(ctx, `INSERT INTO tree_sets (name, kind, path, owner, members) VALUES ($1, $2, $3, $4, $5)`,
			s.Name, s.Kind, s.Path, s.Owner, s.Members); err != nil {
			return err
		}
	}
	return nil
}

func (x queries) PutTreeState(ctx context.Context, commit string, sequence int64) error {
	_, err := x.q.Exec(ctx, `INSERT INTO tree_state (singleton, commit, sequence, applied_at) VALUES (true, $1, $2, now())
		ON CONFLICT (singleton) DO UPDATE SET commit = $1, sequence = $2, applied_at = now()`, commit, sequence)
	return err
}
