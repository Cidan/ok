package store

import (
	"context"
	"encoding/json"
	"time"
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
	return t, s.b.InsertSession(ctx, hashToken(t), principalID, via, now.Add(lifetime), now.Add(idle))
}

// Session returns the principal of a live session, pushing its idle deadline on by idle (at most
// once a minute, so a busy page does not write on every request). A session whose principal no
// longer stands is no session.
func (s *Store) Session(ctx context.Context, t string, idle time.Duration) (Principal, error) {
	p, idleUntil, err := s.b.LiveSession(ctx, hashToken(t))
	if err != nil {
		return Principal{}, err
	}
	if !p.Active() {
		return Principal{}, ErrNotFound
	}
	if next := s.now().Add(idle); next.Sub(idleUntil) > time.Minute {
		if err := s.b.ExtendSession(ctx, hashToken(t), next); err != nil {
			return Principal{}, err
		}
	}
	return p, nil
}

// EndSession ends a session: signing out.
func (s *Store) EndSession(ctx context.Context, t string) error {
	return s.b.DeleteSession(ctx, hashToken(t))
}

// Expire deletes sessions, ceremonies, and device logins whose time is up.
func (s *Store) Expire(ctx context.Context) error {
	if err := s.b.DeleteExpired(ctx); err != nil {
		return err
	}
	return s.b.DeleteStaleDeviceLogins(ctx)
}

// BeginCeremony keeps a sign-in ceremony's state — an OIDC redirect's, a passkey challenge's —
// until it returns, at most lifetime; its ID goes to the browser.
func (s *Store) BeginCeremony(ctx context.Context, kind string, data any, lifetime time.Duration) (string, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	id := token()
	return id, s.b.InsertCeremony(ctx, id, kind, raw, s.now().Add(lifetime))
}

// EndCeremony takes a ceremony's state, once: a second return of the same ceremony finds nothing.
func (s *Store) EndCeremony(ctx context.Context, id, kind string, into any) error {
	raw, err := s.b.TakeCeremony(ctx, id, kind)
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
func (s *Store) AddPasskey(ctx context.Context, p Passkey) error { return s.b.InsertPasskey(ctx, p) }

// Passkeys lists a principal's passkeys.
func (s *Store) Passkeys(ctx context.Context, principalID string) ([]Passkey, error) {
	return s.b.PasskeysOf(ctx, principalID)
}

// PasskeyByID returns a passkey by its credential ID.
func (s *Store) PasskeyByID(ctx context.Context, id []byte) (Passkey, error) {
	return s.b.Passkey(ctx, id)
}

// PasskeyUsed records a passkey's use, with its record as the ceremony updated it (its signature
// counter).
func (s *Store) PasskeyUsed(ctx context.Context, id []byte, data json.RawMessage) error {
	return s.b.UsePasskey(ctx, id, data)
}

// RemovePasskey removes one of a principal's passkeys.
func (s *Store) RemovePasskey(ctx context.Context, principalID string, id []byte) error {
	ok, err := s.b.DeletePasskey(ctx, principalID, id)
	if err == nil && !ok {
		return ErrNotFound
	}
	return err
}
