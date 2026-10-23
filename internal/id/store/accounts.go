package store

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/tenancy"
)

// Accounts a hosted service's people make themselves (docs/hosted.md, "Signing up and in,
// hosted"): with an email they verify and a password, or a social provider's sign-in, linked by
// its subject — never by an email the provider says alone, which would hand an account to whoever
// controls that provider's claim.

// Emailed links' lifetimes, and how failed sign-ins slow a guesser.
const (
	SignUpLinkLifetime = 24 * time.Hour
	ResetLinkLifetime  = time.Hour
	// MaxSignInFailures failures within SignInFailureWindow stop an account's password sign-ins
	// until the window passes.
	MaxSignInFailures   = 10
	SignInFailureWindow = 15 * time.Minute
)

// Purposes of an emailed link.
const (
	PurposeSignUp = "sign-up"
	PurposeReset  = "reset"
)

// EmailToken is what an emailed link stands for.
type EmailToken struct {
	Purpose   string
	Email     string
	Principal string // a reset's account
	Handle    string // a sign-up's
	Hash      string // a sign-up's password, hashed
	Expires   time.Time
}

// LoginLink is an account's link to a provider's subject.
type LoginLink struct {
	Provider, Subject string
	Created           time.Time
}

// NormalEmail is an address as accounts are found by it: valid, a bare address, lowercased.
func NormalEmail(email string) (string, error) {
	a, err := mail.ParseAddress(strings.TrimSpace(email))
	if err != nil || a.Name != "" || len(a.Address) > 254 {
		return "", invalid("%q is not an email address", email)
	}
	return strings.ToLower(a.Address), nil
}

// HandleAvailable reports whether a person may take a handle: valid, and held by no account,
// organization, or retired name.
func (s *Store) HandleAvailable(ctx context.Context, handle string) error {
	if err := tenancy.ValidHandle(handle); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	held, err := s.handleHeld(ctx, s.b, handle)
	if err != nil {
		return err
	}
	if held {
		return ErrTaken
	}
	return nil
}

// BeginSignUp records a sign-up waiting for its email to be verified — the handle it asks for, and
// the password already hashed — returning the link's token.
func (s *Store) BeginSignUp(ctx context.Context, email, handle, hash string) (string, error) {
	email, err := NormalEmail(email)
	if err != nil {
		return "", err
	}
	if err := tenancy.ValidHandle(handle); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	t := token()
	return t, s.b.InsertEmailToken(ctx, hashToken(t), EmailToken{Purpose: PurposeSignUp, Email: email, Handle: handle, Hash: hash,
		Expires: s.now().Add(SignUpLinkLifetime)})
}

// CompleteSignUp makes the account a sign-up's link verified: its handle, if still free, and its
// email, if no account holds it now. The link works once.
func (s *Store) CompleteSignUp(ctx context.Context, link string) (Principal, error) {
	t, err := s.b.TakeEmailToken(ctx, hashToken(link), PurposeSignUp, s.now())
	if err != nil {
		return Principal{}, err
	}
	return s.createAccount(ctx, t.Handle, t.Email, t.Hash)
}

// CreateAccount makes an account with no password: a social provider's sign-up, linked to its
// subject. The email is the provider's verified one, or "".
func (s *Store) CreateAccount(ctx context.Context, handle, email, provider, subject string) (Principal, error) {
	if email != "" {
		var err error
		if email, err = NormalEmail(email); err != nil {
			return Principal{}, err
		}
	}
	p, err := s.createAccount(ctx, handle, email, "")
	if err != nil {
		return Principal{}, err
	}
	if err := s.b.InsertLoginLink(ctx, provider, subject, p.ID, s.now()); err != nil {
		return Principal{}, err
	}
	return p, nil
}

func (s *Store) createAccount(ctx context.Context, handle, email, hash string) (Principal, error) {
	if err := tenancy.ValidHandle(handle); err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	var p Principal
	err := s.b.Txn(ctx, func(tx Ops) error {
		if err := tx.LockNames(ctx); err != nil {
			return err
		}
		held, err := s.handleHeld(ctx, tx, handle)
		if err != nil {
			return err
		}
		if held {
			return ErrTaken
		}
		if email != "" {
			others, err := tx.PeopleByEmail(ctx, email)
			if err != nil {
				return err
			}
			for _, o := range others {
				if o.Status != directory.Deleted {
					return ErrEmailTaken
				}
			}
		}
		if p, err = tx.InsertPrincipal(ctx, Principal{ID: newID("prn_"), Name: s.names.Human(handle), Kind: Human, Status: directory.Active,
			Email: email, DisplayName: handle}); err != nil {
			return err
		}
		if hash != "" {
			return tx.PutPassword(ctx, p.ID, hash, s.now())
		}
		return nil
	})
	return p, err
}

// ErrEmailTaken is a sign-up for an email an account holds.
var ErrEmailTaken = errors.New("an account holds that email")

// Account is the active account an email signs in to with a password, and its hash; ErrNotFound
// for none.
func (s *Store) Account(ctx context.Context, email string) (Principal, string, error) {
	email, err := NormalEmail(email)
	if err != nil {
		return Principal{}, "", ErrNotFound
	}
	people, err := s.b.PeopleByEmail(ctx, email)
	if err != nil {
		return Principal{}, "", err
	}
	for _, p := range people {
		if !p.Active() {
			continue
		}
		hash, err := s.b.Password(ctx, p.ID)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return Principal{}, "", err
		}
		return p, hash, nil
	}
	return Principal{}, "", ErrNotFound
}

// SignInAllowed reports whether an email's password sign-ins are allowed now: fewer than
// MaxSignInFailures failed in the window.
func (s *Store) SignInAllowed(ctx context.Context, email string) (bool, error) {
	n, err := s.b.SignInFailures(ctx, strings.ToLower(email), s.now().Add(-SignInFailureWindow))
	return n < MaxSignInFailures, err
}

// NoteSignInFailure counts a failed password sign-in for an email; ClearSignInFailures forgets
// them, as a sign-in succeeds.
func (s *Store) NoteSignInFailure(ctx context.Context, email string) error {
	now := s.now()
	return s.b.NoteSignInFailure(ctx, strings.ToLower(email), now, now.Add(-SignInFailureWindow))
}

func (s *Store) ClearSignInFailures(ctx context.Context, email string) error {
	return s.b.ClearSignInFailures(ctx, strings.ToLower(email))
}

// BeginReset records a reset of an account's password, returning the link's token.
func (s *Store) BeginReset(ctx context.Context, p Principal) (string, error) {
	t := token()
	return t, s.b.InsertEmailToken(ctx, hashToken(t), EmailToken{Purpose: PurposeReset, Email: p.Email, Principal: p.ID,
		Expires: s.now().Add(ResetLinkLifetime)})
}

// CompleteReset sets the password a reset's link was for, ending every session of the account: the
// link works once. It returns the account.
func (s *Store) CompleteReset(ctx context.Context, link, hash string) (Principal, error) {
	t, err := s.b.TakeEmailToken(ctx, hashToken(link), PurposeReset, s.now())
	if err != nil {
		return Principal{}, err
	}
	p, err := s.b.PrincipalByID(ctx, t.Principal)
	if err != nil {
		return Principal{}, err
	}
	if !p.Active() {
		return Principal{}, ErrNotFound
	}
	err = s.b.Txn(ctx, func(tx Ops) error {
		if err := tx.PutPassword(ctx, p.ID, hash, s.now()); err != nil {
			return err
		}
		return tx.DeleteSessionsOf(ctx, p.ID)
	})
	return p, err
}

// SetPassword sets an account's password.
func (s *Store) SetPassword(ctx context.Context, principal, hash string) error {
	return s.b.PutPassword(ctx, principal, hash, s.now())
}

// Password is an account's password hash; ErrNotFound for none.
func (s *Store) Password(ctx context.Context, principal string) (string, error) {
	return s.b.Password(ctx, principal)
}

// HasPassword reports whether an account has a password.
func (s *Store) HasPassword(ctx context.Context, principal string) (bool, error) {
	_, err := s.b.Password(ctx, principal)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// LinkLogin links an account to a provider's subject; one already linked to another is refused.
func (s *Store) LinkLogin(ctx context.Context, principal, provider, subject string) error {
	err := s.b.InsertLoginLink(ctx, provider, subject, principal, s.now())
	if errors.Is(err, ErrTaken) {
		if p, lerr := s.b.LinkedPrincipal(ctx, provider, subject); lerr == nil && p.ID == principal {
			return nil
		}
		return invalid("that %s account signs in to another of ok's accounts", provider)
	}
	return err
}

// LinkedAccount is the active account a provider's subject is linked to; ErrNotFound for none.
func (s *Store) LinkedAccount(ctx context.Context, provider, subject string) (Principal, error) {
	p, err := s.b.LinkedPrincipal(ctx, provider, subject)
	if err != nil {
		return Principal{}, err
	}
	if !p.Active() {
		return Principal{}, ErrNotFound
	}
	return p, nil
}

// LoginLinks are an account's links; Unlink removes one, unless it is the account's only way in.
func (s *Store) LoginLinks(ctx context.Context, principal string) ([]LoginLink, error) {
	return s.b.LoginLinks(ctx, principal)
}

func (s *Store) Unlink(ctx context.Context, principal, provider string) (bool, error) {
	links, err := s.b.LoginLinks(ctx, principal)
	if err != nil {
		return false, err
	}
	hasPassword, err := s.HasPassword(ctx, principal)
	if err != nil {
		return false, err
	}
	if len(links) == 1 && links[0].Provider == provider && !hasPassword {
		return false, invalid("that is your only way to sign in: set a password first")
	}
	return s.b.DeleteLoginLink(ctx, principal, provider)
}
