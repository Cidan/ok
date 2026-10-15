package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Session lifetimes, unless the installation says otherwise.
const (
	DefaultSessionLifetime = 7 * 24 * time.Hour
	DefaultSessionIdle     = 12 * time.Hour
)

// CreateSession begins a browser session for a principal, signed in via an authenticator, and
// returns its token: the cookie's value. Only its hash is kept.
func (s *Store) CreateSession(ctx context.Context, principalID, via string, lifetime, idle time.Duration) (string, error) {
	t := token()
	now := s.now()
	_, err := s.pool.Exec(ctx, `INSERT INTO sessions (token_hash, principal_id, via, expires_at, idle_until) VALUES ($1, $2, $3, $4, $5)`,
		hashToken(t), principalID, via, now.Add(lifetime), now.Add(idle))
	return t, err
}

// Session returns the principal of a live session, pushing its idle deadline on by idle (at most
// once a minute, so a busy page does not write on every request). A session whose principal no
// longer stands is no session.
func (s *Store) Session(ctx context.Context, t string, idle time.Duration) (Principal, error) {
	var idleUntil time.Time
	row := s.pool.QueryRow(ctx, `SELECT `+prefixed("p")+`, s.idle_until FROM sessions s JOIN principals p ON p.id = s.principal_id
		WHERE s.token_hash = $1 AND s.expires_at > now() AND s.idle_until > now()`, hashToken(t))
	var p Principal
	var kind, status string
	err := row.Scan(&p.ID, &p.Name, &kind, &status, &p.ExternalID, &p.Email, &p.DisplayName, &p.Created, &idleUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, ErrNotFound
	}
	if err != nil {
		return Principal{}, err
	}
	p.Kind, p.Status = Kind(kind), parseStatus(status)
	if !p.Active() {
		return Principal{}, ErrNotFound
	}
	if next := s.now().Add(idle); next.Sub(idleUntil) > time.Minute {
		if _, err := s.pool.Exec(ctx, `UPDATE sessions SET idle_until = LEAST($2, expires_at) WHERE token_hash = $1`, hashToken(t), next); err != nil {
			return Principal{}, err
		}
	}
	return p, nil
}

// EndSession ends a session: signing out.
func (s *Store) EndSession(ctx context.Context, t string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM sessions WHERE token_hash = $1`, hashToken(t))
	return err
}

// Expire deletes sessions, ceremonies, and device logins whose time is up.
func (s *Store) Expire(ctx context.Context) error {
	for _, q := range []string{
		`DELETE FROM sessions WHERE expires_at < now() OR idle_until < now()`,
		`DELETE FROM ceremonies WHERE expires_at < now()`,
		`DELETE FROM device_logins WHERE expires_at < now() - interval '1 day'`,
	} {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// BeginCeremony keeps a sign-in ceremony's state — an OIDC redirect's, a passkey challenge's —
// until it returns, at most lifetime; its ID goes to the browser.
func (s *Store) BeginCeremony(ctx context.Context, kind string, data any, lifetime time.Duration) (string, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	id := token()
	_, err = s.pool.Exec(ctx, `INSERT INTO ceremonies (id, kind, data, expires_at) VALUES ($1, $2, $3, $4)`, id, kind, raw, s.now().Add(lifetime))
	return id, err
}

// EndCeremony takes a ceremony's state, once: a second return of the same ceremony finds nothing.
func (s *Store) EndCeremony(ctx context.Context, id, kind string, into any) error {
	var raw []byte
	err := s.pool.QueryRow(ctx, `DELETE FROM ceremonies WHERE id = $1 AND kind = $2 AND expires_at > now() RETURNING data`, id, kind).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, into)
}

// Passkey is a WebAuthn credential of a principal's: Data is the library's record of it.
type Passkey struct {
	ID          []byte
	PrincipalID string
	Data        json.RawMessage
	Label       string
	Created     time.Time
	LastUsed    time.Time
}

// AddPasskey records a principal's new passkey.
func (s *Store) AddPasskey(ctx context.Context, p Passkey) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO passkeys (id, principal_id, data, label) VALUES ($1, $2, $3, $4)`, p.ID, p.PrincipalID, []byte(p.Data), p.Label)
	return err
}

func scanPasskeys(rows pgx.Rows) ([]Passkey, error) {
	defer rows.Close()
	var out []Passkey
	for rows.Next() {
		var p Passkey
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

// Passkeys lists a principal's passkeys.
func (s *Store) Passkeys(ctx context.Context, principalID string) ([]Passkey, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, principal_id, data, label, created_at, last_used_at FROM passkeys WHERE principal_id = $1 ORDER BY created_at`, principalID)
	if err != nil {
		return nil, err
	}
	return scanPasskeys(rows)
}

// PasskeyByID returns a passkey by its credential ID.
func (s *Store) PasskeyByID(ctx context.Context, id []byte) (Passkey, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, principal_id, data, label, created_at, last_used_at FROM passkeys WHERE id = $1`, id)
	if err != nil {
		return Passkey{}, err
	}
	ps, err := scanPasskeys(rows)
	if err != nil {
		return Passkey{}, err
	}
	if len(ps) == 0 {
		return Passkey{}, ErrNotFound
	}
	return ps[0], nil
}

// PasskeyUsed records a passkey's use, with its record as the ceremony updated it (its signature
// counter).
func (s *Store) PasskeyUsed(ctx context.Context, id []byte, data json.RawMessage) error {
	_, err := s.pool.Exec(ctx, `UPDATE passkeys SET data = $2, last_used_at = now() WHERE id = $1`, id, []byte(data))
	return err
}

// RemovePasskey removes one of a principal's passkeys.
func (s *Store) RemovePasskey(ctx context.Context, principalID string, id []byte) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM passkeys WHERE id = $1 AND principal_id = $2`, id, principalID)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}
