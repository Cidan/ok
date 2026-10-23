package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
)

// Accounts people make themselves: a sign-up's link makes the account once, unless its handle or
// email was taken meanwhile; an account is found by its email, whatever its case; failed sign-ins
// stop an address's password sign-ins for a window; a reset's link sets the password once and ends
// every session; an account a provider's sign-in made is found by its subject, linked once, and
// keeps a way in.
func testAccounts(t *testing.T, s *store.Store) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0)
	s.SetClock(func() time.Time { return now })
	src := &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "bea", true)}}}
	sync(t, s, src)

	if err := s.HandleAvailable(ctx, "ada"); err != nil {
		t.Errorf("a free handle: %v", err)
	}
	for h, want := range map[string]error{"bea": store.ErrTaken, "Ada": store.ErrInvalid, "api": store.ErrInvalid} {
		if err := s.HandleAvailable(ctx, h); !errors.Is(err, want) {
			t.Errorf("handle %q: %v, want %v", h, err, want)
		}
	}
	if _, err := s.BeginSignUp(ctx, "not an address", "ada", "h"); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a sign-up with no address: %v", err)
	}
	link, err := s.BeginSignUp(ctx, "Ada@Example.com", "ada", "$argon2id$hash")
	if err != nil {
		t.Fatal(err)
	}
	ada, err := s.CompleteSignUp(ctx, link)
	if err != nil || ada.Name != "ada.human.example.com" || ada.Email != "ada@example.com" || !ada.Active() {
		t.Fatalf("CompleteSignUp = %+v, %v", ada, err)
	}
	if _, err := s.CompleteSignUp(ctx, link); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a sign-up's link twice: %v", err)
	}
	if p, hash, err := s.Account(ctx, "ADA@example.COM"); err != nil || p.ID != ada.ID || hash != "$argon2id$hash" {
		t.Errorf("Account = %+v, %q, %v", p, hash, err)
	}
	if _, _, err := s.Account(ctx, "bea@example.com"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("an account with no password: %v", err)
	}
	// Taken meanwhile: the handle, the email.
	late, _ := s.BeginSignUp(ctx, "other@example.com", "ada", "h")
	if _, err := s.CompleteSignUp(ctx, late); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a sign-up whose handle was taken meanwhile: %v", err)
	}
	twice, _ := s.BeginSignUp(ctx, "ada@example.com", "ada2", "h")
	if _, err := s.CompleteSignUp(ctx, twice); !errors.Is(err, store.ErrEmailTaken) {
		t.Errorf("a sign-up whose email has an account: %v", err)
	}
	expired, _ := s.BeginSignUp(ctx, "cal@example.com", "cal", "h")
	now = now.Add(store.SignUpLinkLifetime + time.Minute)
	if _, err := s.CompleteSignUp(ctx, expired); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("an expired sign-up's link: %v", err)
	}

	// Failed sign-ins: a window's worth stops the address, until the window passes; success clears.
	for i := 0; i < store.MaxSignInFailures; i++ {
		if ok, err := s.SignInAllowed(ctx, "ada@example.com"); err != nil || !ok {
			t.Fatalf("allowed after %d failures = %v, %v", i, ok, err)
		}
		if err := s.NoteSignInFailure(ctx, "ADA@example.com"); err != nil {
			t.Fatal(err)
		}
	}
	if ok, _ := s.SignInAllowed(ctx, "ada@example.com"); ok {
		t.Error("allowed after a window's worth of failures")
	}
	now = now.Add(store.SignInFailureWindow + time.Second)
	if ok, _ := s.SignInAllowed(ctx, "ada@example.com"); !ok {
		t.Error("still stopped once the window passed")
	}
	s.NoteSignInFailure(ctx, "ada@example.com")
	if err := s.ClearSignInFailures(ctx, "ada@example.com"); err != nil {
		t.Fatal(err)
	}

	// A reset sets the password once, and ends every session.
	session, err := s.CreateSession(ctx, ada.ID, "password", time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	reset, err := s.BeginReset(ctx, ada)
	if err != nil {
		t.Fatal(err)
	}
	if p, err := s.CompleteReset(ctx, reset, "$argon2id$new"); err != nil || p.ID != ada.ID {
		t.Fatalf("CompleteReset = %+v, %v", p, err)
	}
	if _, hash, _ := s.Account(ctx, "ada@example.com"); hash != "$argon2id$new" {
		t.Errorf("the password after a reset: %q", hash)
	}
	if _, err := s.Session(ctx, session, time.Hour); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a session that outlived a reset: %v", err)
	}
	if _, err := s.CompleteReset(ctx, reset, "h"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a reset's link twice: %v", err)
	}

	// A provider's sign-up: found by its subject, linked once, keeping a way in.
	gh, err := s.CreateAccount(ctx, "dee", "dee@example.com", "github", "1234")
	if err != nil {
		t.Fatal(err)
	}
	if p, err := s.LinkedAccount(ctx, "github", "1234"); err != nil || p.ID != gh.ID {
		t.Errorf("LinkedAccount = %+v, %v", p, err)
	}
	if err := s.LinkLogin(ctx, ada.ID, "github", "1234"); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("linking another's subject: %v", err)
	}
	if err := s.LinkLogin(ctx, gh.ID, "github", "1234"); err != nil {
		t.Errorf("linking one's own subject again: %v", err)
	}
	if _, err := s.Unlink(ctx, gh.ID, "github"); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("unlinking the only way in: %v", err)
	}
	if err := s.LinkLogin(ctx, ada.ID, "google", "ada-g"); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Unlink(ctx, ada.ID, "google"); err != nil || !ok {
		t.Errorf("unlinking with a password to sign in with = %v, %v", ok, err)
	}
}
