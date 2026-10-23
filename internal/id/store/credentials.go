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

// EnrollKey enrolls a key as a principal's credential. Enrolling a key that is already that
// principal's standing credential returns it.
func (s *Store) EnrollKey(ctx context.Context, principalID, label string, publicKey []byte) (Credential, error) {
	var id string
	err := s.b.Txn(ctx, func(tx Ops) error {
		cid, owner, revoked, ok, err := tx.CredentialByKey(ctx, publicKey)
		switch {
		case err != nil:
			return err
		case ok && owner == principalID && !revoked:
			id = cid
			return nil
		case ok:
			return ErrKeyEnrolled
		}
		id = newID("crd_")
		return tx.InsertCredential(ctx, id, principalID, label, publicKey)
	})
	if err != nil {
		return Credential{}, err
	}
	return s.Credential(ctx, id)
}

// Credential returns a credential by ID.
func (s *Store) Credential(ctx context.Context, id string) (Credential, error) {
	return s.b.Credential(ctx, id)
}

// CredentialsOf lists a principal's credentials, newest first.
func (s *Store) CredentialsOf(ctx context.Context, principalID string) ([]Credential, error) {
	return s.b.CredentialsOf(ctx, principalID)
}

// Issued records that a certificate was issued for a credential.
func (s *Store) Issued(ctx context.Context, id string, notAfter time.Time) error {
	return s.b.SetIssued(ctx, id, notAfter)
}

// Revoke revokes a credential. Revoking a revoked one changes nothing.
func (s *Store) Revoke(ctx context.Context, id string) error {
	ok, err := s.b.RevokeCredential(ctx, id)
	if err == nil && !ok {
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

// hashToken is what is kept of a secret token: its SHA-256.
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
	expires = s.now().Add(DeviceLoginLifetime)
	for range 5 {
		deviceCode, code = token(), userCode()
		err = s.b.InsertDeviceLogin(ctx, hashToken(deviceCode), code, publicKey, label, clientAddress, expires)
		if errors.Is(err, ErrUserCodeTaken) {
			continue // a user code in use: draw another
		}
		return deviceCode, code, expires, err
	}
	return "", "", time.Time{}, fmt.Errorf("no free user code: %w", err)
}

// DeviceLoginByUserCode returns the pending device login a user code names.
func (s *Store) DeviceLoginByUserCode(ctx context.Context, code string) (DeviceLogin, error) {
	return s.b.PendingDeviceLogin(ctx, NormalizeUserCode(code))
}

// DecideDeviceLogin approves or denies a pending device login, as a principal.
func (s *Store) DecideDeviceLogin(ctx context.Context, code, principalID string, approve bool) error {
	state := "denied"
	if approve {
		state = "approved"
	}
	ok, err := s.b.DecideDeviceLogin(ctx, NormalizeUserCode(code), principalID, state)
	if err == nil && !ok {
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
	err := s.b.Txn(ctx, func(tx Ops) error {
		var last time.Time
		var ok bool
		var err error
		d, last, ok, err = tx.LockDeviceLogin(ctx, hashToken(deviceCode))
		if err != nil {
			return err
		}
		if !ok {
			state = PollExpired
			return nil
		}
		now := s.now()
		switch {
		case d.State == "done", d.State == "pending" && now.After(d.Expires):
			state = PollExpired
		case d.State == "denied":
			state = PollDenied
		case d.State == "approved":
			state = PollApproved
		case !last.IsZero() && now.Sub(last) < PollInterval:
			state = PollSlowDown
		default:
			state = PollPending
		}
		return tx.SetPolled(ctx, hashToken(deviceCode))
	})
	return state, d, err
}

// CompleteDeviceLogin records the credential an approved device login's certificate was issued
// for: the login is done, and polls after say expired.
func (s *Store) CompleteDeviceLogin(ctx context.Context, deviceCode, credentialID string) error {
	ok, err := s.b.CompleteDeviceLogin(ctx, hashToken(deviceCode), credentialID)
	if err == nil && !ok {
		return ErrNotFound
	}
	return err
}
