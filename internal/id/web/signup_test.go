package web_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/id/mail"
	"github.com/Cidan/ok/internal/id/mail/mailtest"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/id/web"
	"github.com/Cidan/ok/internal/serve/authn"
)

// signUpEnv is a hosted service's pages: email-and-password accounts, mailed through a test
// server, and the fake provider's people making accounts linked to their subjects.
func signUpEnv(t *testing.T) (*env, *mailtest.Server) {
	t.Helper()
	smtp := mailtest.New(t)
	e := newEnvWith(t, func(c *web.Config) {
		c.SignUp = &web.SignUp{Mail: mail.SMTP{Addr: smtp.Addr, From: "ok <no-reply@example.com>"}, Social: map[string]bool{"fake": true}}
	})
	return e, smtp
}

var linkRE = regexp.MustCompile(`https?://\S+token=\S+`)

// link is the link a message carries, as a path on the server.
func link(t *testing.T, e *env, m mailtest.Message) string {
	t.Helper()
	l := linkRE.FindString(m.Body)
	if l == "" {
		t.Fatalf("no link in %q", m.Body)
	}
	return strings.TrimPrefix(l, e.base)
}

// token is a link's token.
func token(t *testing.T, path string) string {
	t.Helper()
	u, err := url.Parse(path)
	if err != nil {
		t.Fatal(err)
	}
	return u.Query().Get("token")
}

// fresh forgets the browser's session: another browser.
func (e *env) fresh() {
	jar, _ := cookiejar.New(nil)
	e.client = &http.Client{Jar: jar}
}

// Making an account with an email and a password: the link it mails makes it once, after a button
// — not merely followed — and signs the person in; then they sign in with the password, and a
// wrong one, or an address with no account, is refused alike, until too many stop the address.
// An address that has an account is told so, by mail; the page says the same either way.
func TestSignUpWithEmail(t *testing.T) {
	e, smtp := signUpEnv(t)
	for name, form := range map[string]url.Values{
		"a taken handle":   {"email": {"x@example.com"}, "handle": {"ada"}, "password": {"a long enough password"}},
		"a short password": {"email": {"x@example.com"}, "handle": {"newbie"}, "password": {"short"}},
		"no address":       {"email": {"nope"}, "handle": {"newbie"}, "password": {"a long enough password"}},
	} {
		if code, _, _ := e.post(t, "/id/signup", form, e.base); code != http.StatusBadRequest {
			t.Errorf("signing up with %s: %d", name, code)
		}
	}
	form := url.Values{"email": {"dee@example.com"}, "handle": {"dee"}, "password": {"correct horse battery staple"}}
	code, body, _ := e.post(t, "/id/signup", form, e.base)
	if code != http.StatusOK || !strings.Contains(body, "We sent a link to dee@example.com") {
		t.Fatalf("signing up: %d\n%s", code, body)
	}
	m := smtp.Next(t)
	if m.To[0] != "dee@example.com" || m.Subject != "Verify your address" {
		t.Errorf("the message = %+v", m)
	}
	verify := link(t, e, m)
	// Following the link shows a button; only posting it makes the account.
	if _, body, _ := e.get(t, verify); !strings.Contains(body, "Make it") {
		t.Fatalf("the link's page:\n%s", body)
	}
	if _, err := e.store.Principal(context.Background(), "dee.human.example.com"); err == nil {
		t.Fatal("following the link made the account")
	}
	if _, body, where := e.post(t, "/id/signup/verify", url.Values{"token": {token(t, verify)}}, e.base); where.Path != "/id/account" || !strings.Contains(body, "dee.human.example.com") {
		t.Fatalf("making the account: %s\n%s", where, body)
	}
	if code, _, _ := e.post(t, "/id/signup/verify", url.Values{"token": {token(t, verify)}}, e.base); code != http.StatusBadRequest {
		t.Errorf("the link twice: %d", code)
	}

	// Signing in with the password; refused alike for a wrong one and an address with no account.
	e.fresh()
	if _, _, where := e.post(t, "/id/login/password", url.Values{"email": {"DEE@example.com"}, "password": {"correct horse battery staple"}}, e.base); where.Path != "/id/account" {
		t.Errorf("signing in: %s", where)
	}
	e.fresh()
	_, wrong, _ := e.post(t, "/id/login/password", url.Values{"email": {"dee@example.com"}, "password": {"wrong horse battery staple"}}, e.base)
	_, nobody, _ := e.post(t, "/id/login/password", url.Values{"email": {"nobody@example.com"}, "password": {"wrong horse battery staple"}}, e.base)
	const refused = "That email and password do not match an account."
	if !strings.Contains(wrong, refused) || !strings.Contains(nobody, refused) {
		t.Errorf("refusals differ, or say nothing:\n%s\n---\n%s", wrong, nobody)
	}
	for i := 1; i < store.MaxSignInFailures; i++ {
		e.post(t, "/id/login/password", url.Values{"email": {"dee@example.com"}, "password": {"wrong horse battery staple"}}, e.base)
	}
	if code, _, _ := e.post(t, "/id/login/password", url.Values{"email": {"dee@example.com"}, "password": {"correct horse battery staple"}}, e.base); code != http.StatusTooManyRequests {
		t.Errorf("signing in after too many failures: %d", code)
	}

	// Signing up again with the address: the same page, and a note to the address.
	if code, body, _ := e.post(t, "/id/signup", url.Values{"email": {"dee@example.com"}, "handle": {"dee2"}, "password": {"another long password"}}, e.base); code != http.StatusOK ||
		!strings.Contains(body, "We sent a link to dee@example.com") {
		t.Errorf("signing up with an address that has an account: %d\n%s", code, body)
	}
	if m := smtp.Next(t); m.Subject != "You already have an account" || linkRE.MatchString(m.Body) {
		t.Errorf("the note = %+v", m)
	}
}

// Resetting a password: the same answer whether an account has the address; the link sets a new
// password once, signs the person in, and ends their other sessions.
func TestPasswordReset(t *testing.T) {
	e, smtp := signUpEnv(t)
	e.post(t, "/id/signup", url.Values{"email": {"dee@example.com"}, "handle": {"dee"}, "password": {"correct horse battery staple"}}, e.base)
	verify := link(t, e, smtp.Next(t))
	e.post(t, "/id/signup/verify", url.Values{"token": {token(t, verify)}}, e.base)
	other := e.client // a session elsewhere

	e.fresh()
	_, nobody, _ := e.post(t, "/id/reset", url.Values{"email": {"nobody@example.com"}}, e.base)
	_, dee, _ := e.post(t, "/id/reset", url.Values{"email": {"dee@example.com"}}, e.base)
	if nobody != dee || !strings.Contains(dee, "If an account has that address") {
		t.Errorf("reset answers differ:\n%s\n---\n%s", nobody, dee)
	}
	reset := link(t, e, smtp.Next(t))
	if len(smtp.Messages()) != 2 {
		t.Errorf("messages: %d (a reset for an address with no account was mailed)", len(smtp.Messages()))
	}
	if code, _, _ := e.post(t, "/id/reset/confirm", url.Values{"token": {token(t, reset)}, "password": {"short"}}, e.base); code != http.StatusBadRequest {
		t.Errorf("a short new password: %d", code)
	}
	if _, _, where := e.post(t, "/id/reset/confirm", url.Values{"token": {token(t, reset)}, "password": {"a brand new passphrase"}}, e.base); where.Path != "/id/account" {
		t.Fatalf("setting the new password: %s", where)
	}
	// The other session is gone (a role that cached it lets it go within the resolver's TTL).
	base, _ := url.Parse(e.base)
	var session string
	for _, c := range other.Jar.Cookies(base) {
		if c.Name == authn.SessionCookie {
			session = c.Value
		}
	}
	if session == "" {
		t.Fatal("the other browser had no session")
	}
	if _, err := e.store.Session(context.Background(), session, time.Hour); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the other session after a reset: %v", err)
	}
	e.fresh()
	if _, _, where := e.post(t, "/id/login/password", url.Values{"email": {"dee@example.com"}, "password": {"a brand new passphrase"}}, e.base); where.Path != "/id/account" {
		t.Errorf("signing in with the new password: %s", where)
	}
}

// A provider's people make accounts linked to their subjects: the first sign-in chooses a handle,
// the next finds the account; a person with an account links a provider from its page.
func TestSocialSignUp(t *testing.T) {
	e, smtp := signUpEnv(t)
	e.idp.SignInAs(map[string]any{"sub": "gh-1", "email": "eve@example.com", "email_verified": true, "preferred_username": "Eve.Smith"})
	_, body, where := e.get(t, "/id/login/oidc/fake?next="+url.QueryEscape("/id/account"))
	if where.Path != "/id/signup/handle" || where.Query().Get("handle") != "eve-smith" {
		t.Fatalf("a first sign-in: %s\n%s", where, body)
	}
	ceremony := where.Query().Get("ceremony")
	if code, _, _ := e.post(t, "/id/signup/handle", url.Values{"ceremony": {ceremony}, "handle": {"ada"}}, e.base); code != http.StatusBadRequest {
		t.Errorf("a taken handle: %d", code)
	}
	if _, body, where := e.post(t, "/id/signup/handle", url.Values{"ceremony": {ceremony}, "handle": {"eve"}}, e.base); where.Path != "/id/account" ||
		!strings.Contains(body, "eve.human.example.com") || !strings.Contains(body, "Unlink") {
		t.Fatalf("making the account: %s\n%s", where, body)
	}
	if code, _, _ := e.post(t, "/id/signup/handle", url.Values{"ceremony": {ceremony}, "handle": {"eve2"}}, e.base); code != http.StatusBadRequest {
		t.Errorf("the sign-up twice: %d", code)
	}
	e.fresh()
	e.idp.SignInAs(map[string]any{"sub": "gh-1", "email": "eve@example.com", "email_verified": true})
	if _, body, where := e.get(t, "/id/login/oidc/fake?next="+url.QueryEscape("/id/account")); where.Path != "/id/account" || !strings.Contains(body, "eve.human.example.com") {
		t.Errorf("signing in again: %s", where)
	}
	// Its only way in is not unlinked; with a password set, it is.
	if code, body, _ := e.post(t, "/id/account/links/remove", url.Values{"provider": {"fake"}}, e.base); code != http.StatusOK || !strings.Contains(body, "only way to sign in") {
		t.Errorf("unlinking the only way in: %d\n%s", code, body)
	}
	if _, _, where := e.post(t, "/id/account/password", url.Values{"password": {"a long unrelated passphrase"}}, e.base); where.Path != "/id/account" {
		t.Errorf("setting a password: %s", where)
	}
	if _, body, _ := e.post(t, "/id/account/links/remove", url.Values{"provider": {"fake"}}, e.base); !strings.Contains(body, "Link</a>") {
		t.Errorf("unlinking with a password:\n%s", body)
	}

	// A person with an email account links the provider.
	e.fresh()
	e.post(t, "/id/signup", url.Values{"email": {"dee@example.com"}, "handle": {"dee"}, "password": {"correct horse battery staple"}}, e.base)
	verify := link(t, e, smtp.Next(t))
	e.post(t, "/id/signup/verify", url.Values{"token": {token(t, verify)}}, e.base)
	e.idp.SignInAs(map[string]any{"sub": "gh-dee"})
	if _, body, where := e.get(t, "/id/login/oidc/fake?link=1"); where.Path != "/id/account" || !strings.Contains(body, "Unlink") {
		t.Fatalf("linking: %s\n%s", where, body)
	}
	e.fresh()
	e.idp.SignInAs(map[string]any{"sub": "gh-dee"})
	if _, body, _ := e.get(t, "/id/login/oidc/fake?next="+url.QueryEscape("/id/account")); !strings.Contains(body, "dee.human.example.com") {
		t.Errorf("signing in with the linked provider:\n%s", body)
	}
}

// A hosted service's person deletes their account from its page, typing their handle: they are
// signed out, and told until when it may be restored; a wrong handle deletes nothing.
func TestDeleteAccountPage(t *testing.T) {
	smtp := mailtest.New(t)
	e := newEnvWith(t, func(c *web.Config) {
		c.SignUp = &web.SignUp{Mail: mail.SMTP{Addr: smtp.Addr, From: "ok <no-reply@example.com>"}, Social: map[string]bool{"fake": true}}
		c.Hosted = true
	})
	e.post(t, "/id/signup", url.Values{"email": {"dee@example.com"}, "handle": {"dee"}, "password": {"correct horse battery staple"}}, e.base)
	verify := link(t, e, smtp.Next(t))
	if _, body, _ := e.post(t, "/id/signup/verify", url.Values{"token": {token(t, verify)}}, e.base); !strings.Contains(body, "Delete your account") {
		t.Fatalf("the account page offers no deletion:\n%s", body)
	}
	if _, body, _ := e.post(t, "/id/account/delete", url.Values{"confirm": {"someone"}}, e.base); !strings.Contains(body, "type your handle") {
		t.Errorf("deleting with another's handle:\n%s", body)
	}
	if code, body, _ := e.post(t, "/id/account/delete", url.Values{"confirm": {"dee"}}, e.base); code != http.StatusOK || !strings.Contains(body, "Your account is deleted") {
		t.Fatalf("deleting the account: %d\n%s", code, body)
	}
	if _, _, where := e.get(t, "/id/account"); where.Path != "/id/login" {
		t.Errorf("the account page after the deletion: %s", where)
	}
	if p, _ := e.store.Principal(context.Background(), "dee.human.example.com"); p.Active() {
		t.Errorf("the deleted account = %+v", p)
	}
}
