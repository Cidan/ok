package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Credential is a key's enrollment as a principal's.
type Credential struct {
	ID         string
	Principal  Principal
	Label      string
	PublicKey  []byte // PKIX DER
	Created    time.Time
	LastIssued time.Time // zero before the first certificate
	NotAfter   time.Time
	Revoked    time.Time // zero while it stands
}

// Standing reports whether the credential may be used: not revoked, its principal active.
func (c Credential) Standing() bool { return c.Revoked.IsZero() && c.Principal.Active() }

// ErrKeyEnrolled is a key already enrolled as someone else's credential, or revoked.
var ErrKeyEnrolled = errors.New("the key is enrolled already, or was revoked: make a new key")

const credentialColumns = `c.id, c.label, c.public_key, c.created_at, c.last_issued_at, c.not_after, c.revoked_at, ` // then the principal's

func scanCredential(row scanner) (Credential, error) {
	var c Credential
	var last, notAfter, revoked *time.Time
	var p Principal
	var kind, status string
	err := row.Scan(&c.ID, &c.Label, &c.PublicKey, &c.Created, &last, &notAfter, &revoked,
		&p.ID, &p.Name, &kind, &status, &p.ExternalID, &p.Email, &p.DisplayName, &p.Created)
	if errors.Is(err, pgx.ErrNoRows) {
		return Credential{}, ErrNotFound
	}
	if err != nil {
		return Credential{}, err
	}
	p.Kind, p.Status = Kind(kind), parseStatus(status)
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

// EnrollKey enrolls a key as a principal's credential. Enrolling a key that is already that
// principal's standing credential returns it.
func (s *Store) EnrollKey(ctx context.Context, principalID, label string, publicKey []byte) (Credential, error) {
	var id string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var owner string
		var revoked *time.Time
		err := tx.QueryRow(ctx, `SELECT id, principal_id, revoked_at FROM credentials WHERE public_key = $1`, publicKey).Scan(&id, &owner, &revoked)
		switch {
		case err == nil && owner == principalID && revoked == nil:
			return nil
		case err == nil:
			return ErrKeyEnrolled
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		id = newID("crd_")
		_, err = tx.Exec(ctx, `INSERT INTO credentials (id, principal_id, label, public_key) VALUES ($1, $2, $3, $4)`, id, principalID, label, publicKey)
		var pg *pgconn.PgError
		if errors.As(err, &pg) && pg.Code == "23505" {
			return ErrKeyEnrolled
		}
		return err
	})
	if err != nil {
		return Credential{}, err
	}
	return s.Credential(ctx, id)
}

// Credential returns a credential by ID.
func (s *Store) Credential(ctx context.Context, id string) (Credential, error) {
	return scanCredential(s.pool.QueryRow(ctx, `SELECT `+credentialColumns+prefixed("p")+`
		FROM credentials c JOIN principals p ON p.id = c.principal_id WHERE c.id = $1`, id))
}

// CredentialsOf lists a principal's credentials, newest first.
func (s *Store) CredentialsOf(ctx context.Context, principalID string) ([]Credential, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+credentialColumns+prefixed("p")+`
		FROM credentials c JOIN principals p ON p.id = c.principal_id WHERE c.principal_id = $1 ORDER BY c.created_at DESC, c.id`, principalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Credential
	for rows.Next() {
		c, err := scanCredential(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Issued records that a certificate was issued for a credential.
func (s *Store) Issued(ctx context.Context, id string, notAfter time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE credentials SET last_issued_at = now(), not_after = $2 WHERE id = $1`, id, notAfter)
	return err
}

// Revoke revokes a credential. Revoking a revoked one changes nothing.
func (s *Store) Revoke(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE credentials SET revoked_at = COALESCE(revoked_at, now()) WHERE id = $1`, id)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// DeviceLogin is an OAuth device authorization grant in flight.
type DeviceLogin struct {
	UserCode      string
	PublicKey     []byte
	Label         string
	ClientAddress string
	Created       time.Time
	Expires       time.Time
	State         string // pending, approved, denied, done
	PrincipalID   string // who approved or denied it
	CredentialID  string // once its certificate is issued
}

// Device login timing.
const (
	DeviceLoginLifetime = 10 * time.Minute
	PollInterval        = 5 * time.Second
)

// userCodeAlphabet is what user codes are made of: no vowels (no words), nothing that looks alike.
const userCodeAlphabet = "BCDFGHJKLMNPQRSTVWXZ"

func hashToken(t string) []byte {
	h := sha256.Sum256([]byte(t))
	return h[:]
}

func token() string {
	var b [32]byte
	rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}

func userCode() string {
	out := make([]byte, 0, 9)
	for i := range 8 {
		if i == 4 {
			out = append(out, '-')
		}
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(userCodeAlphabet))))
		out = append(out, userCodeAlphabet[n.Int64()])
	}
	return string(out)
}

// NormalizeUserCode is a user code as typed made canonical: upper case, the hyphen in place.
func NormalizeUserCode(typed string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(typed) {
		if strings.ContainsRune(userCodeAlphabet, r) {
			b.WriteRune(r)
		}
	}
	s := b.String()
	if len(s) != 8 {
		return s
	}
	return s[:4] + "-" + s[4:]
}

// StartDeviceLogin begins a device login for a key: the device code, the command line's secret,
// and the user code, the person's.
func (s *Store) StartDeviceLogin(ctx context.Context, publicKey []byte, label, clientAddress string) (deviceCode, code string, expires time.Time, err error) {
	if _, err := s.pool.Exec(ctx, `DELETE FROM device_logins WHERE expires_at < now() - interval '1 day'`); err != nil {
		return "", "", time.Time{}, err
	}
	expires = s.now().Add(DeviceLoginLifetime)
	for range 5 {
		deviceCode, code = token(), userCode()
		_, err = s.pool.Exec(ctx, `INSERT INTO device_logins (device_code_hash, user_code, public_key, label, client_address, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6)`, hashToken(deviceCode), code, publicKey, label, clientAddress, expires)
		var pg *pgconn.PgError
		if errors.As(err, &pg) && pg.Code == "23505" {
			continue // a user code in use: draw another
		}
		return deviceCode, code, expires, err
	}
	return "", "", time.Time{}, fmt.Errorf("no free user code: %w", err)
}

const deviceLoginColumns = `user_code, public_key, label, client_address, created_at, expires_at, state, COALESCE(principal_id, ''), COALESCE(credential_id, '')`

func scanDeviceLogin(row scanner) (DeviceLogin, error) {
	var d DeviceLogin
	err := row.Scan(&d.UserCode, &d.PublicKey, &d.Label, &d.ClientAddress, &d.Created, &d.Expires, &d.State, &d.PrincipalID, &d.CredentialID)
	if errors.Is(err, pgx.ErrNoRows) {
		return DeviceLogin{}, ErrNotFound
	}
	return d, err
}

// DeviceLoginByUserCode returns the pending device login a user code names.
func (s *Store) DeviceLoginByUserCode(ctx context.Context, code string) (DeviceLogin, error) {
	return scanDeviceLogin(s.pool.QueryRow(ctx, `SELECT `+deviceLoginColumns+` FROM device_logins
		WHERE user_code = $1 AND state = 'pending' AND expires_at > now()`, NormalizeUserCode(code)))
}

// DecideDeviceLogin approves or denies a pending device login, as a principal.
func (s *Store) DecideDeviceLogin(ctx context.Context, code, principalID string, approve bool) error {
	state := "denied"
	if approve {
		state = "approved"
	}
	tag, err := s.pool.Exec(ctx, `UPDATE device_logins SET state = $3, principal_id = $2
		WHERE user_code = $1 AND state = 'pending' AND expires_at > now()`, NormalizeUserCode(code), principalID, state)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// Poll states.
const (
	PollPending  = "pending"
	PollSlowDown = "slow_down"
	PollDenied   = "denied"
	PollExpired  = "expired"
	PollApproved = "approved"
)

// PollDeviceLogin is the command line's poll of its device login: pending, slow_down for one
// polling faster than PollInterval, denied, expired (or already completed), or approved — once.
func (s *Store) PollDeviceLogin(ctx context.Context, deviceCode string) (string, DeviceLogin, error) {
	var state string
	var d DeviceLogin
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var last *time.Time
		row := tx.QueryRow(ctx, `SELECT `+deviceLoginColumns+`, last_poll_at FROM device_logins WHERE device_code_hash = $1 FOR UPDATE`, hashToken(deviceCode))
		err := row.Scan(&d.UserCode, &d.PublicKey, &d.Label, &d.ClientAddress, &d.Created, &d.Expires, &d.State, &d.PrincipalID, &d.CredentialID, &last)
		if errors.Is(err, pgx.ErrNoRows) {
			state = PollExpired
			return nil
		}
		if err != nil {
			return err
		}
		now := s.now()
		switch {
		case d.State == "done", d.State == "pending" && now.After(d.Expires):
			state = PollExpired
		case d.State == "denied":
			state = PollDenied
		case d.State == "approved":
			state = PollApproved
		case last != nil && now.Sub(*last) < PollInterval:
			state = PollSlowDown
		default:
			state = PollPending
		}
		_, err = tx.Exec(ctx, `UPDATE device_logins SET last_poll_at = now() WHERE device_code_hash = $1`, hashToken(deviceCode))
		return err
	})
	return state, d, err
}

// CompleteDeviceLogin records the credential an approved device login's certificate was issued
// for: the login is done, and polls after say expired.
func (s *Store) CompleteDeviceLogin(ctx context.Context, deviceCode, credentialID string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE device_logins SET state = 'done', credential_id = $2
		WHERE device_code_hash = $1 AND state = 'approved'`, hashToken(deviceCode), credentialID)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}
