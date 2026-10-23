package signin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/protocol/webauthncose"
	"github.com/go-webauthn/webauthn/webauthn"

	"github.com/Cidan/ok/internal/id/store"
)

// PasskeyStore keeps people's passkeys.
type PasskeyStore interface {
	PrincipalByID(ctx context.Context, id string) (store.Principal, error)
	Passkeys(ctx context.Context, principalID string) ([]store.Passkey, error)
	AddPasskey(ctx context.Context, p store.Passkey) error
	PasskeyUsed(ctx context.Context, id []byte, data json.RawMessage) error
}

// Passkeys signs people in with passkeys — usernameless: the authenticator says whose it is — and
// lets a signed-in person add one. ok is the WebAuthn relying party for its own host name, so a
// passkey made for ok works only on ok's pages.
type Passkeys struct {
	wa    *webauthn.WebAuthn
	store PasskeyStore
}

// algorithms are the passkey algorithms ok accepts, and so offers: ES256 and EdDSA.
var algorithms = []protocol.CredentialParameter{
	{Type: protocol.PublicKeyCredentialType, Algorithm: webauthncose.AlgES256},
	{Type: protocol.PublicKeyCredentialType, Algorithm: webauthncose.AlgEdDSA},
}

// NewPasskeys is the relying party rpID (ok's host name) for origin (ok's public URL).
func NewPasskeys(rpID, displayName, origin string, st PasskeyStore) (*Passkeys, error) {
	wa, err := webauthn.New(&webauthn.Config{RPID: rpID, RPDisplayName: displayName, RPOrigins: []string{origin}})
	if err != nil {
		return nil, err
	}
	return &Passkeys{wa: wa, store: st}, nil
}

// user is a person as WebAuthn sees them: their principal ID is the user handle, so a passkey
// stays theirs across renames.
type user struct {
	p     store.Principal
	creds []webauthn.Credential
}

func (u *user) WebAuthnID() []byte                         { return []byte(u.p.ID) }
func (u *user) WebAuthnName() string                       { return u.p.Name }
func (u *user) WebAuthnDisplayName() string                { return displayName(u.p) }
func (u *user) WebAuthnCredentials() []webauthn.Credential { return u.creds }

func displayName(p store.Principal) string {
	if p.DisplayName != "" {
		return p.DisplayName
	}
	return p.Name
}

func (k *Passkeys) user(ctx context.Context, p store.Principal) (*user, error) {
	pks, err := k.store.Passkeys(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	u := &user{p: p}
	for _, pk := range pks {
		var c webauthn.Credential
		if err := json.Unmarshal(pk.Data, &c); err != nil {
			return nil, fmt.Errorf("passkey %x: %w", pk.ID, err)
		}
		u.creds = append(u.creds, c)
	}
	return u, nil
}

// BeginRegistration starts adding a passkey for a signed-in person: the options for the browser,
// and the session to keep until it finishes.
func (k *Passkeys) BeginRegistration(ctx context.Context, p store.Principal) (*protocol.CredentialCreation, *webauthn.SessionData, error) {
	u, err := k.user(ctx, p)
	if err != nil {
		return nil, nil, err
	}
	var exclude []protocol.CredentialDescriptor
	for _, c := range u.creds {
		exclude = append(exclude, c.Descriptor())
	}
	return k.wa.BeginRegistration(u,
		webauthn.WithCredentialParameters(algorithms),
		webauthn.WithExclusions(exclude),
		webauthn.WithResidentKeyRequirement(protocol.ResidentKeyRequirementRequired),
		webauthn.WithAuthenticatorSelection(protocol.AuthenticatorSelection{
			ResidentKey:      protocol.ResidentKeyRequirementRequired,
			UserVerification: protocol.VerificationRequired,
		}),
	)
}

// FinishRegistration checks the browser's response to a registration and keeps the passkey.
func (k *Passkeys) FinishRegistration(ctx context.Context, p store.Principal, session webauthn.SessionData, r *http.Request, label string) error {
	u, err := k.user(ctx, p)
	if err != nil {
		return err
	}
	c, err := k.wa.FinishRegistration(u, session, r)
	if err != nil {
		return fmt.Errorf("the passkey: %w", err)
	}
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return k.store.AddPasskey(ctx, store.Passkey{ID: c.ID, PrincipalID: p.ID, Data: data, Label: label})
}

// BeginLogin starts a usernameless sign-in.
func (k *Passkeys) BeginLogin() (*protocol.CredentialAssertion, *webauthn.SessionData, error) {
	return k.wa.BeginDiscoverableLogin(webauthn.WithUserVerification(protocol.VerificationRequired))
}

// FinishLogin checks the browser's response to a sign-in and returns whose passkey it was: an
// active person's.
func (k *Passkeys) FinishLogin(ctx context.Context, session webauthn.SessionData, r *http.Request) (store.Principal, error) {
	var who store.Principal
	handler := func(_, userHandle []byte) (webauthn.User, error) {
		p, err := k.store.PrincipalByID(ctx, string(userHandle))
		if err != nil {
			return nil, err
		}
		who = p
		return k.user(ctx, p)
	}
	c, err := k.wa.FinishDiscoverableLogin(handler, session, r)
	if err != nil {
		return store.Principal{}, fmt.Errorf("the passkey: %w", err)
	}
	if c.Authenticator.CloneWarning {
		return store.Principal{}, errors.New("the passkey's signature counter went backwards: the authenticator may be cloned; it is refused")
	}
	if !who.Active() || who.Kind != store.Human {
		return store.Principal{}, ErrNoPerson
	}
	data, err := json.Marshal(c)
	if err != nil {
		return store.Principal{}, err
	}
	if err := k.store.PasskeyUsed(ctx, c.ID, data); err != nil {
		return store.Principal{}, err
	}
	return who, nil
}

// BeginApproval starts an approval of something a person was asked, with one of their own
// passkeys and its user verification: the assertion's options, and the session to keep until it
// finishes.
func (k *Passkeys) BeginApproval(ctx context.Context, p store.Principal) (*protocol.CredentialAssertion, *webauthn.SessionData, error) {
	u, err := k.user(ctx, p)
	if err != nil {
		return nil, nil, err
	}
	if len(u.creds) == 0 {
		return nil, nil, errors.New("you have no passkey to approve with: add one on your account page first")
	}
	return k.wa.BeginLogin(u, webauthn.WithUserVerification(protocol.VerificationRequired))
}

// FinishApproval checks the browser's answer to an approval: an assertion by one of the person's
// own passkeys, their presence and identity verified in this ceremony (the session requires user
// verification, which the assertion's own flags must show).
func (k *Passkeys) FinishApproval(ctx context.Context, p store.Principal, session webauthn.SessionData, r *http.Request) error {
	u, err := k.user(ctx, p)
	if err != nil {
		return err
	}
	c, err := k.wa.FinishLogin(u, session, r)
	if err != nil {
		return fmt.Errorf("the passkey: %w", err)
	}
	if c.Authenticator.CloneWarning {
		return errors.New("the passkey's signature counter went backwards: the authenticator may be cloned; it is refused")
	}
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return k.store.PasskeyUsed(ctx, c.ID, data)
}
