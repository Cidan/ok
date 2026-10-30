// Package spannerstore is identity's store backend (internal/id/store) on Spanner, in a database
// of its own (docs/hosted.md, "Spanner"). Every write is DML, so a transaction reads what it wrote,
// as on PostgreSQL; an operation outside a transaction is a transaction of its own
// (spannerdata.Ops). A duplicate in a unique index fails at its statement, as on PostgreSQL, and
// the transaction with it.
package spannerstore

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/lock/spannerlock"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/wait"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are identity's schema on Spanner.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Backend is identity's store backend on Spanner.
type Backend struct {
	queries
}

var _ store.Backend = (*Backend)(nil)

// Open brings the database client reaches to identity's schema, through admin, and returns a
// backend over it.
func Open(ctx context.Context, admin *database.DatabaseAdminClient, client *spanner.Client) (*Backend, error) {
	if _, err := spannerdata.Migrate(ctx, admin, client, Migrations); err != nil {
		return nil, err
	}
	return &Backend{queries: queries{spannerdata.Ops{Client: client}}}, nil
}

func (b *Backend) Txn(ctx context.Context, fn func(store.Ops) error) error {
	return spannerdata.Txn(ctx, b.Client, func(_ context.Context, tx *spanner.ReadWriteTransaction) error {
		return fn(queries{spannerdata.Ops{Client: b.Client, Tx: tx}})
	})
}

func (b *Backend) Waits() wait.Store { return spannerdata.Waits(b.Client) }

func (b *Backend) Ping(ctx context.Context) error {
	return b.Client.Single().Query(ctx, spanner.Statement{SQL: `SELECT 1`}).Do(func(*spanner.Row) error { return nil })
}

func (b *Backend) SCIM() scim.Store { return scimStore{c: b.Client} }

// SCIMOf is an organization's SCIM, by its ID.
func (b *Backend) SCIMOf(org string) scim.Store { return scimStore{c: b.Client, scope: org} }

// queries are the backend's operations, in a transaction or each in one of its own.
type queries struct{ spannerdata.Ops }

// unique reports a duplicate in a unique index or of a key.
func unique(err error) bool { return spanner.ErrCode(err) == codes.AlreadyExists }

func nullString(s string) spanner.NullString { return spanner.NullString{StringVal: s, Valid: s != ""} }

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// orJSON is b, or JSON's empty list for none.
func orJSON(b []byte) []byte {
	if b == nil {
		return []byte("[]")
	}
	return b
}

// readOne runs a query and scans its first row with scan; ErrNotFound when it has none.
func (x queries) readOne(ctx context.Context, sql string, params map[string]any, scan func(*spanner.Row) error) error {
	found := false
	err := x.Read(func(r spannerdata.Reader) error {
		found = false
		it := r.Query(ctx, spanner.Statement{SQL: sql, Params: params})
		defer it.Stop()
		row, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return scan(row)
	})
	if err == nil && !found {
		return store.ErrNotFound
	}
	return err
}

// readAll runs a query, calling scan with each row; reset runs before each attempt.
func (x queries) readAll(ctx context.Context, sql string, params map[string]any, reset func(), scan func(*spanner.Row) error) error {
	return x.Read(func(r spannerdata.Reader) error {
		reset()
		return spannerdata.Each(ctx, r, sql, params, scan)
	})
}

// lockOne is readOne in the operation's transaction, the query a FOR UPDATE one.
func (x queries) lockOne(ctx context.Context, sql string, params map[string]any, scan func(*spanner.Row) error) error {
	found := false
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		found = false
		return spannerdata.Each(ctx, tx, sql, params, func(row *spanner.Row) error {
			if found {
				return nil
			}
			found = true
			return scan(row)
		})
	})
	if err == nil && !found {
		return store.ErrNotFound
	}
	return err
}

// sweep deletes what sql selects: by partitioned DML outside a transaction — a backlog of expired
// rows may be more than one transaction's mutations hold, and a sweep run again deletes nothing
// more — and in the transaction otherwise.
func (x queries) sweep(ctx context.Context, sql string, params map[string]any) error {
	if x.Tx != nil {
		_, err := x.Tx.Update(ctx, spanner.Statement{SQL: sql, Params: params})
		return err
	}
	_, err := x.Client.PartitionedUpdate(ctx, spanner.Statement{SQL: sql, Params: params})
	return err
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

const principalColumns = `id, name, kind, status, IFNULL(external_id, ''), email, display_name, created_at`

// prefixed is principalColumns qualified by a table alias.
func prefixed(alias string) string {
	return alias + `.id, ` + alias + `.name, ` + alias + `.kind, ` + alias + `.status, IFNULL(` + alias + `.external_id, ''), ` +
		alias + `.email, ` + alias + `.display_name, ` + alias + `.created_at`
}

// scanPrincipal reads a principal's columns, then extra's.
func scanPrincipal(row *spanner.Row, extra ...any) (store.Principal, error) {
	var p store.Principal
	var kind, status string
	if err := row.Columns(append([]any{&p.ID, &p.Name, &kind, &status, &p.ExternalID, &p.Email, &p.DisplayName, &p.Created}, extra...)...); err != nil {
		return store.Principal{}, err
	}
	p.Kind, p.Status = store.Kind(kind), parseStatus(status)
	return p, nil
}

func (x queries) principal(ctx context.Context, sql string, params map[string]any) (store.Principal, error) {
	var p store.Principal
	err := x.readOne(ctx, sql, params, func(row *spanner.Row) error {
		var err error
		p, err = scanPrincipal(row)
		return err
	})
	return p, err
}

func (x queries) principals(ctx context.Context, sql string, params map[string]any) ([]store.Principal, error) {
	var out []store.Principal
	err := x.readAll(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		p, err := scanPrincipal(row)
		if err != nil {
			return err
		}
		out = append(out, p)
		return nil
	})
	return out, err
}

// ─── Principals and names ───

func (x queries) PrincipalByName(ctx context.Context, name string) (store.Principal, error) {
	return x.principal(ctx, `SELECT `+principalColumns+` FROM principals WHERE name = @name`, map[string]any{"name": name})
}

func (x queries) PrincipalByID(ctx context.Context, id string) (store.Principal, error) {
	return x.principal(ctx, `SELECT `+principalColumns+` FROM principals WHERE id = @id`, map[string]any{"id": id})
}

func (x queries) PrincipalByExternalID(ctx context.Context, externalID string) (store.Principal, error) {
	return x.principal(ctx, `SELECT `+principalColumns+` FROM principals WHERE external_id = @ext`, map[string]any{"ext": externalID})
}

func (x queries) LockPrincipalByName(ctx context.Context, name string) (store.Principal, error) {
	var p store.Principal
	err := x.lockOne(ctx, `SELECT `+principalColumns+` FROM principals WHERE name = @name FOR UPDATE`, map[string]any{"name": name},
		func(row *spanner.Row) error {
			var err error
			p, err = scanPrincipal(row)
			return err
		})
	return p, err
}

// likeEscape escapes LIKE's wildcards and its escape, a backslash in GoogleSQL as in PostgreSQL.
func likeEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

func (x queries) Principals(ctx context.Context, f store.Filter) ([]store.Principal, error) {
	q := `SELECT ` + principalColumns + ` FROM principals WHERE name > @after`
	p := map[string]any{"after": f.After}
	if f.Kind != "" {
		q += ` AND kind = @kind`
		p["kind"] = string(f.Kind)
	}
	if !f.IncludeInactive {
		q += ` AND status = 'active'`
	}
	if f.Query != "" {
		q += ` AND (LOWER(name) LIKE @q OR email_lower LIKE @q OR LOWER(display_name) LIKE @q)`
		p["q"] = "%" + likeEscape(strings.ToLower(f.Query)) + "%"
	}
	q += ` ORDER BY name`
	if f.Limit > 0 {
		q += ` LIMIT @limit`
		p["limit"] = int64(f.Limit)
	}
	return x.principals(ctx, q, p)
}

func (x queries) PeopleByEmail(ctx context.Context, email string) ([]store.Principal, error) {
	return x.principals(ctx, `SELECT `+principalColumns+` FROM principals
		WHERE kind = 'human' AND email != '' AND email_lower = LOWER(@email) LIMIT 2`, map[string]any{"email": email})
}

func (x queries) InsertPrincipal(ctx context.Context, p store.Principal) (store.Principal, error) {
	var out store.Principal
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO principals (id, name, kind, status, external_id, email, display_name, created_at, updated_at)
			VALUES (@id, @name, @kind, @status, @ext, @email, @display, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())`,
			map[string]any{"id": p.ID, "name": p.Name, "kind": string(p.Kind), "status": statusText(p.Status), "ext": nullString(p.ExternalID),
				"email": p.Email, "display": p.DisplayName}); err != nil {
			return err
		}
		return spannerdata.Each(ctx, tx, `SELECT `+principalColumns+` FROM principals WHERE id = @id`, map[string]any{"id": p.ID},
			func(row *spanner.Row) error {
				var err error
				out, err = scanPrincipal(row)
				return err
			})
	})
	return out, err
}

func (x queries) UpdatePerson(ctx context.Context, externalID, name string, status directory.Status, email, displayName string) (string, error) {
	var id string
	err := x.Returning(ctx, `UPDATE principals SET name = @name, status = @status, email = @email, display_name = @display,
			updated_at = CURRENT_TIMESTAMP()
		WHERE external_id = @ext THEN RETURN id`,
		map[string]any{"ext": externalID, "name": name, "status": statusText(status), "email": email, "display": displayName},
		func() { id = "" }, func(row *spanner.Row) error { return row.Columns(&id) })
	if err == nil && id == "" {
		return "", store.ErrNotFound
	}
	return id, err
}

func (x queries) SetStatus(ctx context.Context, id string, status directory.Status) error {
	_, err := x.Exec(ctx, `UPDATE principals SET status = @status, updated_at = CURRENT_TIMESTAMP() WHERE id = @id`,
		map[string]any{"id": id, "status": statusText(status)})
	return err
}

func (x queries) LockNames(ctx context.Context) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerlock.InTx(ctx, tx, "names", "all") })
}

func (x queries) NameHeld(ctx context.Context, name string) (bool, error) {
	var held bool
	err := x.readOne(ctx, `SELECT EXISTS (SELECT 1 FROM principals WHERE name = @name)
		OR EXISTS (SELECT 1 FROM retired_names WHERE name = @name)
		OR EXISTS (SELECT 1 FROM synced_groups WHERE name = @name)
		OR EXISTS (SELECT 1 FROM tree_sets WHERE name = @name)
		OR EXISTS (SELECT 1 FROM teams WHERE name = @name)`, map[string]any{"name": name},
		func(row *spanner.Row) error { return row.Columns(&held) })
	return held, err
}

func (x queries) RetireName(ctx context.Context, name, reason string) error {
	_, err := x.Exec(ctx, `INSERT OR IGNORE INTO retired_names (name, retired_at, reason) VALUES (@name, CURRENT_TIMESTAMP(), @reason)`,
		map[string]any{"name": name, "reason": reason})
	return err
}

func (x queries) RetiredNames(ctx context.Context) ([]store.RetiredName, error) {
	var out []store.RetiredName
	err := x.readAll(ctx, `SELECT name, retired_at, reason FROM retired_names ORDER BY name`, nil, func() { out = nil },
		func(row *spanner.Row) error {
			var n store.RetiredName
			if err := row.Columns(&n.Name, &n.Retired, &n.Reason); err != nil {
				return err
			}
			out = append(out, n)
			return nil
		})
	return out, err
}

func (x queries) AppendAudit(ctx context.Context, actor, action string, detail map[string]any) error {
	if detail == nil {
		detail = map[string]any{}
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	_, err = x.Exec(ctx, `INSERT INTO audit (id, audit_at, actor, action, detail) VALUES (@id, CURRENT_TIMESTAMP(), @actor, @action, PARSE_JSON(@detail))`,
		map[string]any{"id": data.NewID(), "actor": actor, "action": action, "detail": string(raw)})
	return err
}

// ─── Robots' bindings ───

func (x queries) InsertBinding(ctx context.Context, principalID string, b store.Binding) error {
	_, err := x.Exec(ctx, `INSERT INTO robot_bindings (organization_id, issuer, subject, principal_id) VALUES (@org, @issuer, @subject, @principal)`,
		map[string]any{"org": b.Organization, "issuer": b.Issuer, "subject": b.Subject, "principal": principalID})
	if unique(err) {
		return store.ErrBindingTaken
	}
	return err
}

func (x queries) DeleteBinding(ctx context.Context, robot string, b store.Binding) (bool, error) {
	n, err := x.Exec(ctx, `DELETE FROM robot_bindings WHERE organization_id = @org AND issuer = @issuer AND subject = @subject
		AND principal_id IN (SELECT id FROM principals WHERE name = @robot)`,
		map[string]any{"robot": robot, "org": b.Organization, "issuer": b.Issuer, "subject": b.Subject})
	return n > 0, err
}

func (x queries) DeleteBindingsOf(ctx context.Context, principalID string) error {
	_, err := x.Exec(ctx, `DELETE FROM robot_bindings WHERE principal_id = @principal`, map[string]any{"principal": principalID})
	return err
}

func (x queries) BoundPrincipal(ctx context.Context, b store.Binding) (store.Principal, error) {
	return x.principal(ctx, `SELECT `+prefixed("p")+` FROM robot_bindings rb JOIN principals p ON p.id = rb.principal_id
		WHERE rb.organization_id = @org AND rb.issuer = @issuer AND rb.subject = @subject`,
		map[string]any{"org": b.Organization, "issuer": b.Issuer, "subject": b.Subject})
}

func (x queries) Bindings(ctx context.Context) (map[string][]store.Binding, error) {
	out := map[string][]store.Binding{}
	err := x.readAll(ctx, `SELECT principal_id, organization_id, issuer, subject FROM robot_bindings ORDER BY organization_id, issuer, subject`, nil,
		func() { clear(out) }, func(row *spanner.Row) error {
			var id string
			var b store.Binding
			if err := row.Columns(&id, &b.Organization, &b.Issuer, &b.Subject); err != nil {
				return err
			}
			out[id] = append(out[id], b)
			return nil
		})
	return out, err
}

// ─── Credentials ───

const credentialColumns = `c.id, c.label, c.public_key, c.created_at, c.last_issued_at, c.not_after, c.revoked_at, ` // then the principal's

func scanCredential(row *spanner.Row) (store.Credential, error) {
	var c store.Credential
	var last, notAfter, revoked spanner.NullTime
	p, err := scanPrincipalAfter(row, []any{&c.ID, &c.Label, &c.PublicKey, &c.Created, &last, &notAfter, &revoked})
	if err != nil {
		return store.Credential{}, err
	}
	c.Principal, c.LastIssued, c.NotAfter, c.Revoked = p, last.Time, notAfter.Time, revoked.Time
	return c, nil
}

// scanPrincipalAfter reads before's columns, then a principal's.
func scanPrincipalAfter(row *spanner.Row, before []any) (store.Principal, error) {
	var p store.Principal
	var kind, status string
	if err := row.Columns(append(before, &p.ID, &p.Name, &kind, &status, &p.ExternalID, &p.Email, &p.DisplayName, &p.Created)...); err != nil {
		return store.Principal{}, err
	}
	p.Kind, p.Status = store.Kind(kind), parseStatus(status)
	return p, nil
}

func (x queries) CredentialByKey(ctx context.Context, publicKey []byte) (id, owner string, revoked, ok bool, err error) {
	var at spanner.NullTime
	err = x.readOne(ctx, `SELECT id, principal_id, revoked_at FROM credentials WHERE public_key = @key`, map[string]any{"key": publicKey},
		func(row *spanner.Row) error { return row.Columns(&id, &owner, &at) })
	if err == store.ErrNotFound {
		return "", "", false, false, nil
	}
	return id, owner, at.Valid, err == nil, err
}

func (x queries) InsertCredential(ctx context.Context, id, principalID, label string, publicKey []byte) error {
	_, err := x.Exec(ctx, `INSERT INTO credentials (id, principal_id, label, public_key, created_at) VALUES (@id, @principal, @label, @key, CURRENT_TIMESTAMP())`,
		map[string]any{"id": id, "principal": principalID, "label": label, "key": publicKey})
	if unique(err) {
		return store.ErrKeyEnrolled
	}
	return err
}

func (x queries) credentials(ctx context.Context, where string, params map[string]any) ([]store.Credential, error) {
	var out []store.Credential
	err := x.readAll(ctx, `SELECT `+credentialColumns+prefixed("p")+` FROM credentials c JOIN principals p ON p.id = c.principal_id `+where,
		params, func() { out = nil }, func(row *spanner.Row) error {
			c, err := scanCredential(row)
			if err != nil {
				return err
			}
			out = append(out, c)
			return nil
		})
	return out, err
}

func (x queries) Credential(ctx context.Context, id string) (store.Credential, error) {
	cs, err := x.credentials(ctx, `WHERE c.id = @id`, map[string]any{"id": id})
	if err != nil {
		return store.Credential{}, err
	}
	if len(cs) == 0 {
		return store.Credential{}, store.ErrNotFound
	}
	return cs[0], nil
}

func (x queries) CredentialsOf(ctx context.Context, principalID string) ([]store.Credential, error) {
	return x.credentials(ctx, `WHERE c.principal_id = @principal ORDER BY c.created_at DESC, c.id`, map[string]any{"principal": principalID})
}

func (x queries) SetIssued(ctx context.Context, id string, notAfter time.Time) error {
	_, err := x.Exec(ctx, `UPDATE credentials SET last_issued_at = CURRENT_TIMESTAMP(), not_after = @notAfter WHERE id = @id`,
		map[string]any{"id": id, "notAfter": notAfter})
	return err
}

func (x queries) RevokeCredential(ctx context.Context, id string) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE credentials SET revoked_at = IFNULL(revoked_at, CURRENT_TIMESTAMP()) WHERE id = @id`, map[string]any{"id": id})
	return n > 0, err
}

func (x queries) RevokeCredentialsOf(ctx context.Context, principalID string) error {
	_, err := x.Exec(ctx, `UPDATE credentials SET revoked_at = CURRENT_TIMESTAMP() WHERE principal_id = @principal AND revoked_at IS NULL`,
		map[string]any{"principal": principalID})
	return err
}

// ─── Device logins ───

const deviceLoginColumns = `user_code, public_key, label, client_address, created_at, expires_at, state, IFNULL(principal_id, ''),
	IFNULL(credential_id, '')`

func scanDeviceLogin(row *spanner.Row, extra ...any) (store.DeviceLogin, error) {
	var d store.DeviceLogin
	err := row.Columns(append([]any{&d.UserCode, &d.PublicKey, &d.Label, &d.ClientAddress, &d.Created, &d.Expires, &d.State, &d.PrincipalID,
		&d.CredentialID}, extra...)...)
	return d, err
}

func (x queries) InsertDeviceLogin(ctx context.Context, deviceCodeHash []byte, userCode string, publicKey []byte, label, clientAddress string, expires time.Time) error {
	_, err := x.Exec(ctx, `INSERT INTO device_logins (device_code_hash, user_code, public_key, label, client_address, created_at, expires_at, state)
		VALUES (@hash, @code, @key, @label, @address, CURRENT_TIMESTAMP(), @expires, 'pending')`,
		map[string]any{"hash": deviceCodeHash, "code": userCode, "key": publicKey, "label": label, "address": clientAddress, "expires": expires})
	if unique(err) {
		return store.ErrUserCodeTaken
	}
	return err
}

func (x queries) PendingDeviceLogin(ctx context.Context, userCode string) (store.DeviceLogin, error) {
	var d store.DeviceLogin
	err := x.readOne(ctx, `SELECT `+deviceLoginColumns+` FROM device_logins
		WHERE user_code = @code AND state = 'pending' AND expires_at > CURRENT_TIMESTAMP()`, map[string]any{"code": userCode},
		func(row *spanner.Row) error {
			var err error
			d, err = scanDeviceLogin(row)
			return err
		})
	return d, err
}

func (x queries) DecideDeviceLogin(ctx context.Context, userCode, principalID, state string) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE device_logins SET state = @state, principal_id = @principal
		WHERE user_code = @code AND state = 'pending' AND expires_at > CURRENT_TIMESTAMP()`,
		map[string]any{"code": userCode, "principal": principalID, "state": state})
	return n > 0, err
}

func (x queries) LockDeviceLogin(ctx context.Context, deviceCodeHash []byte) (store.DeviceLogin, time.Time, bool, error) {
	var d store.DeviceLogin
	var last spanner.NullTime
	err := x.lockOne(ctx, `SELECT `+deviceLoginColumns+`, last_poll_at FROM device_logins WHERE device_code_hash = @hash FOR UPDATE`,
		map[string]any{"hash": deviceCodeHash}, func(row *spanner.Row) error {
			var err error
			d, err = scanDeviceLogin(row, &last)
			return err
		})
	if err == store.ErrNotFound {
		return store.DeviceLogin{}, time.Time{}, false, nil
	}
	if err != nil {
		return store.DeviceLogin{}, time.Time{}, false, err
	}
	return d, last.Time, true, nil
}

func (x queries) SetPolled(ctx context.Context, deviceCodeHash []byte) error {
	_, err := x.Exec(ctx, `UPDATE device_logins SET last_poll_at = CURRENT_TIMESTAMP() WHERE device_code_hash = @hash`,
		map[string]any{"hash": deviceCodeHash})
	return err
}

func (x queries) CompleteDeviceLogin(ctx context.Context, deviceCodeHash []byte, credentialID string) (bool, error) {
	n, err := x.Exec(ctx, `UPDATE device_logins SET state = 'done', credential_id = @credential WHERE device_code_hash = @hash AND state = 'approved'`,
		map[string]any{"hash": deviceCodeHash, "credential": credentialID})
	return n > 0, err
}

func (x queries) DeleteStaleDeviceLogins(ctx context.Context) error {
	return x.sweep(ctx, `DELETE FROM device_logins WHERE expires_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)`, nil)
}

// ─── Sessions and ceremonies ───

func (x queries) InsertSession(ctx context.Context, tokenHash []byte, principalID, via string, expires, idleUntil time.Time) error {
	_, err := x.Exec(ctx, `INSERT INTO sessions (token_hash, principal_id, via, created_at, expires_at, idle_until)
		VALUES (@hash, @principal, @via, CURRENT_TIMESTAMP(), @expires, @idle)`,
		map[string]any{"hash": tokenHash, "principal": principalID, "via": via, "expires": expires, "idle": idleUntil})
	return err
}

func (x queries) LiveSession(ctx context.Context, tokenHash []byte) (store.Principal, time.Time, error) {
	var p store.Principal
	var idleUntil time.Time
	err := x.readOne(ctx, `SELECT `+prefixed("p")+`, s.idle_until FROM sessions s JOIN principals p ON p.id = s.principal_id
		WHERE s.token_hash = @hash AND s.expires_at > CURRENT_TIMESTAMP() AND s.idle_until > CURRENT_TIMESTAMP()`,
		map[string]any{"hash": tokenHash}, func(row *spanner.Row) error {
			var err error
			p, err = scanPrincipal(row, &idleUntil)
			return err
		})
	return p, idleUntil, err
}

func (x queries) ExtendSession(ctx context.Context, tokenHash []byte, idleUntil time.Time) error {
	_, err := x.Exec(ctx, `UPDATE sessions SET idle_until = LEAST(@idle, expires_at) WHERE token_hash = @hash`,
		map[string]any{"hash": tokenHash, "idle": idleUntil})
	return err
}

func (x queries) DeleteSession(ctx context.Context, tokenHash []byte) error {
	_, err := x.Exec(ctx, `DELETE FROM sessions WHERE token_hash = @hash`, map[string]any{"hash": tokenHash})
	return err
}

func (x queries) DeleteSessionsOf(ctx context.Context, principalID string) error {
	_, err := x.Exec(ctx, `DELETE FROM sessions WHERE principal_id = @principal`, map[string]any{"principal": principalID})
	return err
}

func (x queries) DeleteExpired(ctx context.Context) error {
	for _, sql := range []string{
		`DELETE FROM sessions WHERE expires_at < CURRENT_TIMESTAMP() OR idle_until < CURRENT_TIMESTAMP()`,
		`DELETE FROM ceremonies WHERE expires_at < CURRENT_TIMESTAMP()`,
		`DELETE FROM approvals WHERE expires_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)`,
	} {
		if err := x.sweep(ctx, sql, nil); err != nil {
			return err
		}
	}
	return nil
}

func (x queries) SecretsRoot(ctx context.Context, org string) (string, error) {
	var root string
	err := x.Read(func(r spannerdata.Reader) error {
		root = ""
		_, err := spannerdata.First(ctx, r, `SELECT root FROM organization_secrets_roots WHERE organization_id = @org`, map[string]any{"org": org}, &root)
		return err
	})
	return root, err
}

func (x queries) SetSecretsRoot(ctx context.Context, org, root, by string) error {
	if root == "" {
		_, err := x.Exec(ctx, `DELETE FROM organization_secrets_roots WHERE organization_id = @org`, map[string]any{"org": org})
		return err
	}
	_, err := x.Exec(ctx, `INSERT OR UPDATE INTO organization_secrets_roots (organization_id, root, set_by, set_at)
		VALUES (@org, @root, @by, CURRENT_TIMESTAMP())`, map[string]any{"org": org, "root": root, "by": by})
	return err
}

func (x queries) InsertApproval(ctx context.Context, a store.Approval) error {
	_, err := x.Exec(ctx, `INSERT INTO approvals (id, principal_id, scope, description, created_at, expires_at, decision)
		VALUES (@id, @principal, @scope, @description, @created, @expires, '')`,
		map[string]any{"id": a.ID, "principal": a.PrincipalID, "scope": a.Scope, "description": a.Description, "created": a.Created, "expires": a.Expires})
	return err
}

func (x queries) Approval(ctx context.Context, id string) (store.Approval, error) {
	var a store.Approval
	err := x.readOne(ctx, `SELECT id, principal_id, scope, description, created_at, expires_at, decision, decided_at FROM approvals WHERE id = @id`,
		map[string]any{"id": id}, func(row *spanner.Row) error {
			var decided spanner.NullTime
			if err := row.Columns(&a.ID, &a.PrincipalID, &a.Scope, &a.Description, &a.Created, &a.Expires, &a.Decision, &decided); err != nil {
				return err
			}
			if decided.Valid {
				a.Decided = &decided.Time
			}
			return nil
		})
	return a, err
}

func (x queries) DecideApproval(ctx context.Context, id, principalID string, approved bool) (bool, error) {
	decision := store.ApprovalDeclined
	if approved {
		decision = store.ApprovalApproved
	}
	n, err := x.Exec(ctx, `UPDATE approvals SET decision = @decision, decided_at = CURRENT_TIMESTAMP()
		WHERE id = @id AND principal_id = @principal AND decision = '' AND expires_at > CURRENT_TIMESTAMP()`,
		map[string]any{"id": id, "principal": principalID, "decision": decision})
	return n == 1, err
}

func (x queries) InsertCeremony(ctx context.Context, id, kind string, data []byte, expires time.Time) error {
	_, err := x.Exec(ctx, `INSERT INTO ceremonies (id, kind, data, expires_at) VALUES (@id, @kind, @data, @expires)`,
		map[string]any{"id": id, "kind": kind, "data": data, "expires": expires})
	return err
}

func (x queries) TakeCeremony(ctx context.Context, id, kind string) ([]byte, error) {
	var raw []byte
	found := false
	err := x.Returning(ctx, `DELETE FROM ceremonies WHERE id = @id AND kind = @kind AND expires_at > CURRENT_TIMESTAMP() THEN RETURN data`,
		map[string]any{"id": id, "kind": kind}, func() { raw, found = nil, false },
		func(row *spanner.Row) error {
			found = true
			return row.Columns(&raw)
		})
	if err == nil && !found {
		return nil, store.ErrNotFound
	}
	return raw, err
}

func (x queries) PeekCeremony(ctx context.Context, id, kind string) ([]byte, error) {
	var raw []byte
	err := x.readOne(ctx, `SELECT data FROM ceremonies WHERE id = @id AND kind = @kind AND expires_at > CURRENT_TIMESTAMP()`,
		map[string]any{"id": id, "kind": kind}, func(row *spanner.Row) error { return row.Columns(&raw) })
	return raw, err
}

// ─── Passkeys ───

func (x queries) InsertPasskey(ctx context.Context, p store.Passkey) error {
	_, err := x.Exec(ctx, `INSERT INTO passkeys (id, principal_id, data, label, created_at) VALUES (@id, @principal, @data, @label, CURRENT_TIMESTAMP())`,
		map[string]any{"id": p.ID, "principal": p.PrincipalID, "data": []byte(p.Data), "label": p.Label})
	return err
}

func (x queries) passkeys(ctx context.Context, where string, params map[string]any) ([]store.Passkey, error) {
	var out []store.Passkey
	err := x.readAll(ctx, `SELECT id, principal_id, data, label, created_at, last_used_at FROM passkeys `+where, params, func() { out = nil },
		func(row *spanner.Row) error {
			var p store.Passkey
			var raw []byte
			var last spanner.NullTime
			if err := row.Columns(&p.ID, &p.PrincipalID, &raw, &p.Label, &p.Created, &last); err != nil {
				return err
			}
			p.Data, p.LastUsed = raw, last.Time
			out = append(out, p)
			return nil
		})
	return out, err
}

func (x queries) PasskeysOf(ctx context.Context, principalID string) ([]store.Passkey, error) {
	return x.passkeys(ctx, `WHERE principal_id = @principal ORDER BY created_at`, map[string]any{"principal": principalID})
}

func (x queries) Passkey(ctx context.Context, id []byte) (store.Passkey, error) {
	ps, err := x.passkeys(ctx, `WHERE id = @id`, map[string]any{"id": id})
	if err != nil {
		return store.Passkey{}, err
	}
	if len(ps) == 0 {
		return store.Passkey{}, store.ErrNotFound
	}
	return ps[0], nil
}

func (x queries) UsePasskey(ctx context.Context, id []byte, data json.RawMessage) error {
	_, err := x.Exec(ctx, `UPDATE passkeys SET data = @data, last_used_at = CURRENT_TIMESTAMP() WHERE id = @id`,
		map[string]any{"id": id, "data": []byte(data)})
	return err
}

func (x queries) DeletePasskey(ctx context.Context, principalID string, id []byte) (bool, error) {
	n, err := x.Exec(ctx, `DELETE FROM passkeys WHERE id = @id AND principal_id = @principal`, map[string]any{"id": id, "principal": principalID})
	return n > 0, err
}

// ─── The sync ───

func (x queries) SyncedGroups(ctx context.Context) ([]directory.SyncedGroup, error) {
	var out []directory.SyncedGroup
	err := x.readAll(ctx, `SELECT name, external_id, members, deleted FROM synced_groups ORDER BY name`, nil, func() { out = nil },
		func(row *spanner.Row) error {
			var g directory.SyncedGroup
			if err := row.Columns(&g.Name, &g.ExternalID, &g.Members, &g.Deleted); err != nil {
				return err
			}
			g.Members = nonNil(g.Members)
			out = append(out, g)
			return nil
		})
	return out, err
}

func (x queries) InsertSyncedGroup(ctx context.Context, g directory.SyncedGroup) error {
	_, err := x.Exec(ctx, `INSERT INTO synced_groups (external_id, name, members, deleted, updated_at)
		VALUES (@ext, @name, @members, FALSE, CURRENT_TIMESTAMP())`,
		map[string]any{"ext": g.ExternalID, "name": g.Name, "members": nonNil(g.Members)})
	return err
}

func (x queries) UpdateSyncedGroup(ctx context.Context, g directory.SyncedGroup) error {
	_, err := x.Exec(ctx, `UPDATE synced_groups SET name = @name, members = @members, deleted = @deleted, updated_at = CURRENT_TIMESTAMP()
		WHERE external_id = @ext`, map[string]any{"ext": g.ExternalID, "name": g.Name, "members": nonNil(g.Members), "deleted": g.Deleted})
	return err
}

func (x queries) SyncState(ctx context.Context) (mirror []byte, lastRun time.Time, lastError string, waiting, notes []byte, err error) {
	var last spanner.NullTime
	err = x.readOne(ctx, `SELECT mirror, last_run, last_error, waiting, notes FROM sync_state`, nil,
		func(row *spanner.Row) error { return row.Columns(&mirror, &last, &lastError, &waiting, &notes) })
	if err == store.ErrNotFound {
		return nil, time.Time{}, "", nil, nil, nil
	}
	return mirror, last.Time, lastError, waiting, notes, err
}

func (x queries) PutSyncState(ctx context.Context, mirror, waiting, notes []byte) error {
	_, err := x.Exec(ctx, `INSERT OR UPDATE INTO sync_state (singleton, mirror, last_run, last_error, waiting, notes)
		VALUES (TRUE, @mirror, CURRENT_TIMESTAMP(), '', @waiting, @notes)`,
		map[string]any{"mirror": mirror, "waiting": orJSON(waiting), "notes": orJSON(notes)})
	return err
}

// PutSyncFailure records the failure over the sync's state, keeping what the last sync that went
// recorded, or making the state with nothing else.
func (x queries) PutSyncFailure(ctx context.Context, cause string) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		n, err := spannerdata.Update(ctx, tx, `UPDATE sync_state SET last_run = CURRENT_TIMESTAMP(), last_error = @cause WHERE singleton`,
			map[string]any{"cause": cause})
		if err != nil || n > 0 {
			return err
		}
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO sync_state (singleton, last_run, last_error, waiting, notes)
			VALUES (TRUE, CURRENT_TIMESTAMP(), @cause, @empty, @empty)`, map[string]any{"cause": cause, "empty": []byte("[]")})
		return err
	})
}

func (x queries) SyncNames(ctx context.Context) (map[string]string, map[string]string, error) {
	handles, groups := map[string]string{}, map[string]string{}
	err := x.readAll(ctx, `SELECT external_id, is_group, name FROM sync_names`, nil, func() { clear(handles); clear(groups) },
		func(row *spanner.Row) error {
			var id, name string
			var group bool
			if err := row.Columns(&id, &group, &name); err != nil {
				return err
			}
			if group {
				groups[id] = name
			} else {
				handles[id] = name
			}
			return nil
		})
	return handles, groups, err
}

func (x queries) PutSyncName(ctx context.Context, externalID string, group bool, name string) error {
	_, err := x.Exec(ctx, `INSERT OR UPDATE INTO sync_names (external_id, is_group, name) VALUES (@ext, @group, @name)`,
		map[string]any{"ext": externalID, "group": group, "name": name})
	return err
}

func (x queries) DeleteSyncName(ctx context.Context, externalID string, group bool) error {
	_, err := x.Exec(ctx, `DELETE FROM sync_names WHERE external_id = @ext AND is_group = @group`, map[string]any{"ext": externalID, "group": group})
	return err
}

// ─── The groups-and-ACLs tree ───

func (x queries) TreeState(ctx context.Context) (store.Applied, bool, error) {
	var a store.Applied
	err := x.readOne(ctx, `SELECT commit_id, sequence, applied_at FROM tree_state`, nil,
		func(row *spanner.Row) error { return row.Columns(&a.Commit, &a.Sequence, &a.At) })
	if err == store.ErrNotFound {
		return store.Applied{}, false, nil
	}
	return a, err == nil, err
}

func (x queries) TreeSets(ctx context.Context) ([]store.StoredSet, error) {
	var out []store.StoredSet
	err := x.readAll(ctx, `SELECT name, kind, path, owner, members FROM tree_sets ORDER BY name`, nil, func() { out = nil },
		func(row *spanner.Row) error {
			var s store.StoredSet
			if err := row.Columns(&s.Name, &s.Kind, &s.Path, &s.Owner, &s.Members); err != nil {
				return err
			}
			s.Members = nonNil(s.Members)
			out = append(out, s)
			return nil
		})
	return out, err
}

func (x queries) ReplaceTreeSets(ctx context.Context, sets []store.StoredSet) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `DELETE FROM tree_sets WHERE TRUE`, nil); err != nil {
			return err
		}
		for _, s := range sets {
			if _, err := spannerdata.Update(ctx, tx, `INSERT INTO tree_sets (name, kind, path, owner, members) VALUES (@name, @kind, @path, @owner, @members)`,
				map[string]any{"name": s.Name, "kind": s.Kind, "path": s.Path, "owner": s.Owner, "members": nonNil(s.Members)}); err != nil {
				return fmt.Errorf("the tree's set %s: %w", s.Name, err)
			}
		}
		return nil
	})
}

func (x queries) PutTreeState(ctx context.Context, commit string, sequence int64) error {
	_, err := x.Exec(ctx, `INSERT OR UPDATE INTO tree_state (singleton, commit_id, sequence, applied_at) VALUES (TRUE, @commit, @sequence, CURRENT_TIMESTAMP())`,
		map[string]any{"commit": commit, "sequence": sequence})
	return err
}
