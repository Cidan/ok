package web

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"golang.org/x/oauth2"

	"github.com/Cidan/ok/internal/id/mail"
	"github.com/Cidan/ok/internal/id/password"
	"github.com/Cidan/ok/internal/id/signin"
	"github.com/Cidan/ok/internal/id/store"
)

// Accounts a hosted service's people make themselves (docs/identity.md, "Accounts"). Every answer
// about an email — signing up, resetting — is the same whether an account holds it or not, so the
// pages tell no one who has an account; what differs goes to the address itself.

// SignUp is how a hosted service's people make accounts.
type SignUp struct {
	// Mail sends the links that verify an address and reset a password; nil: no email-and-password
	// accounts.
	Mail mail.Sender
	// Social are the sign-in providers, by ID, whose people make accounts linked to their subjects.
	Social map[string]bool
}

// mailTimeout bounds sending one message.
const mailTimeout = 30 * time.Second

func (w *Web) mountSignUp(r chi.Router) {
	r.Get("/signup", w.signUpForm)
	r.Post("/signup", w.signUp)
	r.Get("/signup/verify", w.signUpConfirm)
	r.Post("/signup/verify", w.signUpVerify)
	r.Get("/signup/handle", w.handleForm)
	r.Post("/signup/handle", w.handleChosen)
	r.Post("/login/password", w.passwordLogin)
	r.Get("/reset", w.resetForm)
	r.Post("/reset", w.reset)
	r.Get("/reset/confirm", w.resetConfirm)
	r.Post("/reset/confirm", w.resetSet)
	r.Post("/account/password", w.setPassword)
	r.Post("/account/links/remove", w.unlink)
}

func (w *Web) emailOn() bool { return w.cfg.SignUp != nil && w.cfg.SignUp.Mail != nil }

func (w *Web) social(provider string) bool {
	return w.cfg.SignUp != nil && w.cfg.SignUp.Social[provider]
}

func (w *Web) signUpView(errMsg string) SignUpView {
	v := SignUpView{Installation: w.cfg.Installation, Error: errMsg, EmailOn: w.emailOn()}
	for _, p := range w.cfg.OIDC {
		if w.social(p.ID()) {
			v.Providers = append(v.Providers, Provider{ID: p.ID(), Name: p.Name()})
		}
	}
	return v
}

func (w *Web) signUpForm(rw http.ResponseWriter, r *http.Request) {
	if w.cfg.SignUp == nil {
		http.NotFound(rw, r)
		return
	}
	w.render(rw, r, http.StatusOK, SignUpPage(w.signUpView("")))
}

// signUp checks what can be checked now — the handle, the password — then emails the address a
// link that makes the account: to an address an account holds already, a note saying so.
func (w *Web) signUp(rw http.ResponseWriter, r *http.Request) {
	if !w.emailOn() {
		http.NotFound(rw, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	email, handle, pw := strings.TrimSpace(r.PostForm.Get("email")), strings.TrimSpace(r.PostForm.Get("handle")), r.PostForm.Get("password")
	again := func(msg string) {
		v := w.signUpView(msg)
		v.Email, v.Handle = email, handle
		w.render(rw, r, http.StatusBadRequest, SignUpPage(v))
	}
	normal, err := store.NormalEmail(email)
	if err != nil {
		again("That is not an email address.")
		return
	}
	if err := w.cfg.Store.HandleAvailable(r.Context(), handle); errors.Is(err, store.ErrTaken) {
		again("That handle is taken: choose another.")
		return
	} else if err != nil {
		again(strings.TrimPrefix(err.Error(), store.ErrInvalid.Error()+": "))
		return
	}
	if err := password.Check(pw, handle, normal); err != nil {
		again(upperFirst(err.Error()) + ".")
		return
	}
	if _, _, err := w.cfg.Store.Account(r.Context(), normal); err == nil {
		w.send(normal, "You already have an account", fmt.Sprintf("Someone — you, perhaps — tried to make an account at %s with this address, which "+
			"already has one.\n\nSign in at %s/id/login, or reset your password at %s/id/reset.\n", w.cfg.Installation, w.base(), w.base()))
	} else {
		hash, err := password.Hash(r.Context(), pw)
		if err != nil {
			w.message(rw, r, http.StatusServiceUnavailable, "Make an account", "ok is busy: try again in a moment.")
			return
		}
		link, err := w.cfg.Store.BeginSignUp(r.Context(), normal, handle, hash)
		if err != nil {
			w.message(rw, r, http.StatusInternalServerError, "Make an account", err.Error())
			return
		}
		w.send(normal, "Verify your address", fmt.Sprintf("To make your account @%s at %s, follow this link within a day:\n\n%s/id/signup/verify?token=%s\n\n"+
			"If you did not ask for an account, do nothing: none is made.\n", handle, w.cfg.Installation, w.base(), url.QueryEscape(link)))
	}
	w.message(rw, r, http.StatusOK, "Check your email", "We sent a link to "+normal+". Follow it to make your account.")
}

// send mails a message in the background: the page answers at once, the same for every address.
func (w *Web) send(to, subject, body string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), mailTimeout)
		defer cancel()
		if err := w.cfg.SignUp.Mail.Send(ctx, to, subject, body); err != nil {
			slog.Error("identity web: sending mail", "subject", subject, "err", err)
		}
	}()
}

func (w *Web) base() string { return strings.TrimRight(w.cfg.PublicURL, "/") }

func (w *Web) signUpConfirm(rw http.ResponseWriter, r *http.Request) {
	if !w.emailOn() {
		http.NotFound(rw, r)
		return
	}
	w.render(rw, r, http.StatusOK, ConfirmPage(ConfirmView{Installation: w.cfg.Installation, Title: "Make your account",
		Text: "Your address is verified. Make the account?", Action: "/id/signup/verify", Token: r.URL.Query().Get("token"), Button: "Make it"}))
}

func (w *Web) signUpVerify(rw http.ResponseWriter, r *http.Request) {
	if !w.emailOn() {
		http.NotFound(rw, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	p, err := w.cfg.Store.CompleteSignUp(r.Context(), r.PostForm.Get("token"))
	switch {
	case errors.Is(err, store.ErrNotFound):
		w.message(rw, r, http.StatusBadRequest, "Make your account", "That link expired, or was followed already: sign up again.")
	case errors.Is(err, store.ErrTaken):
		w.message(rw, r, http.StatusConflict, "Make your account", "Someone took that handle meanwhile: sign up again with another.")
	case errors.Is(err, store.ErrEmailTaken):
		w.message(rw, r, http.StatusConflict, "Make your account", "That address has an account now: sign in, or reset its password.")
	case err != nil:
		w.message(rw, r, http.StatusInternalServerError, "Make your account", err.Error())
	default:
		w.signIn(rw, r, p, "sign-up", "/id/account")
	}
}

// passwordLogin signs a person in with their email and password: refused alike for an address no
// account holds and a wrong password, in the same time, and refused at all once an address has
// failed too often lately.
func (w *Web) passwordLogin(rw http.ResponseWriter, r *http.Request) {
	if !w.emailOn() {
		http.NotFound(rw, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	email, pw, to := strings.ToLower(strings.TrimSpace(r.PostForm.Get("email"))), r.PostForm.Get("password"), r.PostForm.Get("next")
	refuse := func(code int, msg string) {
		v := w.loginView(r, msg)
		v.Next = next(to)
		v.NextQuery = url.QueryEscape(v.Next)
		w.render(rw, r, code, LoginPage(v))
	}
	allowed, err := w.cfg.Store.SignInAllowed(r.Context(), email)
	if err != nil {
		refuse(http.StatusServiceUnavailable, "ok could not sign you in: try again in a moment.")
		return
	}
	if !allowed {
		refuse(http.StatusTooManyRequests, "Too many failed sign-ins for that address lately: wait a while, or reset its password.")
		return
	}
	p, hash, err := w.cfg.Store.Account(r.Context(), email)
	ok := false
	switch {
	case errors.Is(err, store.ErrNotFound):
		password.Waste(r.Context(), pw)
	case err != nil:
		refuse(http.StatusServiceUnavailable, "ok could not sign you in: try again in a moment.")
		return
	default:
		if ok, err = password.Verify(r.Context(), hash, pw); err != nil {
			refuse(http.StatusServiceUnavailable, "ok could not sign you in: try again in a moment.")
			return
		}
	}
	if !ok {
		if err := w.cfg.Store.NoteSignInFailure(r.Context(), email); err != nil {
			slog.Error("identity web: noting a failed sign-in", "err", err)
		}
		refuse(http.StatusUnauthorized, "That email and password do not match an account.")
		return
	}
	if err := w.cfg.Store.ClearSignInFailures(r.Context(), email); err != nil {
		slog.Error("identity web: clearing failed sign-ins", "err", err)
	}
	w.signIn(rw, r, p, "password", to)
}

func (w *Web) resetForm(rw http.ResponseWriter, r *http.Request) {
	if !w.emailOn() {
		http.NotFound(rw, r)
		return
	}
	w.render(rw, r, http.StatusOK, ResetPage(ResetView{Installation: w.cfg.Installation}))
}

func (w *Web) reset(rw http.ResponseWriter, r *http.Request) {
	if !w.emailOn() {
		http.NotFound(rw, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	email, err := store.NormalEmail(r.PostForm.Get("email"))
	if err != nil {
		w.render(rw, r, http.StatusBadRequest, ResetPage(ResetView{Installation: w.cfg.Installation, Error: "That is not an email address."}))
		return
	}
	if p, _, err := w.cfg.Store.Account(r.Context(), email); err == nil {
		link, err := w.cfg.Store.BeginReset(r.Context(), p)
		if err != nil {
			w.message(rw, r, http.StatusInternalServerError, "Reset your password", err.Error())
			return
		}
		w.send(email, "Reset your password", fmt.Sprintf("To choose a new password for your account at %s, follow this link within an hour:\n\n"+
			"%s/id/reset/confirm?token=%s\n\nIf you did not ask, do nothing: your password stays as it is.\n", w.cfg.Installation, w.base(), url.QueryEscape(link)))
	}
	w.message(rw, r, http.StatusOK, "Check your email", "If an account has that address, we sent it a link to reset its password.")
}

func (w *Web) resetConfirm(rw http.ResponseWriter, r *http.Request) {
	if !w.emailOn() {
		http.NotFound(rw, r)
		return
	}
	w.render(rw, r, http.StatusOK, NewPasswordPage(NewPasswordView{Installation: w.cfg.Installation, Token: r.URL.Query().Get("token")}))
}

// resetSet sets the new password a reset's link allows, signing the person out everywhere else and
// in here.
func (w *Web) resetSet(rw http.ResponseWriter, r *http.Request) {
	if !w.emailOn() {
		http.NotFound(rw, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	tok, pw := r.PostForm.Get("token"), r.PostForm.Get("password")
	if err := password.Check(pw, "", ""); err != nil {
		w.render(rw, r, http.StatusBadRequest, NewPasswordPage(NewPasswordView{Installation: w.cfg.Installation, Token: tok, Error: upperFirst(err.Error()) + "."}))
		return
	}
	hash, err := password.Hash(r.Context(), pw)
	if err != nil {
		w.message(rw, r, http.StatusServiceUnavailable, "Reset your password", "ok is busy: try again in a moment.")
		return
	}
	p, err := w.cfg.Store.CompleteReset(r.Context(), tok, hash)
	if errors.Is(err, store.ErrNotFound) {
		w.message(rw, r, http.StatusBadRequest, "Reset your password", "That link expired, or was followed already: ask for another.")
		return
	}
	if err != nil {
		w.message(rw, r, http.StatusInternalServerError, "Reset your password", err.Error())
		return
	}
	if err := w.cfg.Store.ClearSignInFailures(r.Context(), p.Email); err != nil {
		slog.Error("identity web: clearing failed sign-ins", "err", err)
	}
	w.signIn(rw, r, p, "reset", "/id/account")
}

// socialSignIn finishes a sign-in with a provider whose people make accounts: linking it to the
// signed-in person who asked, signing in the account it is linked to, or making one — the person
// choosing its handle first.
func (w *Web) socialSignIn(rw http.ResponseWriter, r *http.Request, p *signin.OIDC, st signin.OIDCState, code string) {
	id, err := p.Identify(r.Context(), st, code)
	if err != nil {
		w.render(rw, r, http.StatusUnauthorized, LoginPage(w.loginView(r, err.Error())))
		return
	}
	if st.LinkFor != "" {
		c, ok := caller(r)
		if !ok || c.ID != st.LinkFor {
			w.message(rw, r, http.StatusForbidden, "Link an account", "Sign in as the account you are linking first.")
			return
		}
		if err := w.cfg.Store.LinkLogin(r.Context(), c.ID, p.ID(), id.Subject); err != nil {
			w.renderAccount(rw, r, c, strings.TrimPrefix(err.Error(), store.ErrInvalid.Error()+": "))
			return
		}
		http.Redirect(rw, r, "/id/account", http.StatusSeeOther)
		return
	}
	acct, err := w.cfg.Store.LinkedAccount(r.Context(), p.ID(), id.Subject)
	if err == nil {
		w.signIn(rw, r, acct, "oidc:"+p.ID(), st.Next)
		return
	}
	if !errors.Is(err, store.ErrNotFound) {
		w.message(rw, r, http.StatusInternalServerError, "Sign-in failed", err.Error())
		return
	}
	pending := pendingSignUp{Provider: p.ID(), ProviderName: p.Name(), Subject: id.Subject, Email: id.Email}
	ceremony, err := w.cfg.Store.BeginCeremony(r.Context(), "social-sign-up", pending, ceremonyLifetime)
	if err != nil {
		w.message(rw, r, http.StatusInternalServerError, "Sign-in failed", err.Error())
		return
	}
	http.Redirect(rw, r, "/id/signup/handle?ceremony="+url.QueryEscape(ceremony)+"&handle="+url.QueryEscape(suggest(id.Handle)), http.StatusSeeOther)
}

// pendingSignUp is a provider's sign-in waiting for its account's handle.
type pendingSignUp struct {
	Provider, ProviderName, Subject, Email string
}

// suggest is a handle made from what a provider calls the person, as far as it can be one.
func suggest(h string) string {
	var b strings.Builder
	for _, c := range strings.ToLower(h) {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			b.WriteRune(c)
		case b.Len() > 0 && !strings.HasSuffix(b.String(), "-"):
			b.WriteByte('-')
		}
	}
	s := strings.Trim(b.String(), "-")
	if len(s) > 39 {
		s = strings.TrimRight(s[:39], "-")
	}
	return s
}

func (w *Web) handleForm(rw http.ResponseWriter, r *http.Request) {
	var p pendingSignUp
	if err := w.cfg.Store.PeekCeremony(r.Context(), r.URL.Query().Get("ceremony"), "social-sign-up", &p); err != nil {
		w.message(rw, r, http.StatusBadRequest, "Make an account", "That sign-up expired: sign in again.")
		return
	}
	w.render(rw, r, http.StatusOK, HandlePage(HandleView{Installation: w.cfg.Installation, Ceremony: r.URL.Query().Get("ceremony"),
		Handle: r.URL.Query().Get("handle"), Provider: p.ProviderName}))
}

// handleChosen makes the account a provider's sign-in waits for, under the handle chosen.
func (w *Web) handleChosen(rw http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	ceremony, handle := r.PostForm.Get("ceremony"), strings.TrimSpace(r.PostForm.Get("handle"))
	var p pendingSignUp
	if err := w.cfg.Store.PeekCeremony(r.Context(), ceremony, "social-sign-up", &p); err != nil {
		w.message(rw, r, http.StatusBadRequest, "Make an account", "That sign-up expired: sign in again.")
		return
	}
	again := func(msg string) {
		w.render(rw, r, http.StatusBadRequest, HandlePage(HandleView{Installation: w.cfg.Installation, Ceremony: ceremony, Handle: handle,
			Provider: p.ProviderName, Error: msg}))
	}
	acct, err := w.cfg.Store.CreateAccount(r.Context(), handle, p.Email, p.Provider, p.Subject)
	switch {
	case errors.Is(err, store.ErrTaken):
		again("That handle is taken: choose another.")
		return
	case errors.Is(err, store.ErrEmailTaken):
		// The address has an account: it signs in, and links the provider from its account page.
		acct, err = w.cfg.Store.CreateAccount(r.Context(), handle, "", p.Provider, p.Subject)
		if err != nil {
			again(strings.TrimPrefix(err.Error(), store.ErrInvalid.Error()+": "))
			return
		}
	case errors.Is(err, store.ErrInvalid):
		again(strings.TrimPrefix(err.Error(), store.ErrInvalid.Error()+": "))
		return
	case err != nil:
		w.message(rw, r, http.StatusInternalServerError, "Make an account", err.Error())
		return
	}
	if err := w.cfg.Store.EndCeremony(r.Context(), ceremony, "social-sign-up", &p); err != nil {
		slog.Error("identity web: ending a sign-up", "err", err)
	}
	w.signIn(rw, r, acct, "oidc:"+p.Provider, "/id/account")
}

// setPassword sets a signed-in person's password: a new account's first, or a change, which asks
// for the current one.
func (w *Web) setPassword(rw http.ResponseWriter, r *http.Request) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	if !w.emailOn() {
		http.NotFound(rw, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	p, err := w.cfg.Store.PrincipalByID(r.Context(), c.ID)
	if err != nil {
		w.message(rw, r, http.StatusInternalServerError, "Your account", err.Error())
		return
	}
	if old, err := w.cfg.Store.Password(r.Context(), p.ID); err == nil {
		if ok, err := password.Verify(r.Context(), old, r.PostForm.Get("current")); err != nil || !ok {
			w.renderAccount(rw, r, c, "Your current password is not that.")
			return
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		w.message(rw, r, http.StatusInternalServerError, "Your account", err.Error())
		return
	}
	handle, _ := w.cfg.Store.Names().HandleOf(p.Name)
	pw := r.PostForm.Get("password")
	if err := password.Check(pw, handle, p.Email); err != nil {
		w.renderAccount(rw, r, c, upperFirst(err.Error())+".")
		return
	}
	hash, err := password.Hash(r.Context(), pw)
	if err != nil {
		w.message(rw, r, http.StatusServiceUnavailable, "Your account", "ok is busy: try again in a moment.")
		return
	}
	if err := w.cfg.Store.SetPassword(r.Context(), p.ID, hash); err != nil {
		w.message(rw, r, http.StatusInternalServerError, "Your account", err.Error())
		return
	}
	http.Redirect(rw, r, "/id/account", http.StatusSeeOther)
}

func (w *Web) unlink(rw http.ResponseWriter, r *http.Request) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := w.cfg.Store.Unlink(r.Context(), c.ID, r.PostForm.Get("provider")); err != nil {
		w.renderAccount(rw, r, c, strings.TrimPrefix(err.Error(), store.ErrInvalid.Error()+": "))
		return
	}
	http.Redirect(rw, r, "/id/account", http.StatusSeeOther)
}

// beginLink starts linking a provider to the signed-in person's account.
func (w *Web) beginLink(rw http.ResponseWriter, r *http.Request, p *signin.OIDC) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	st := signin.OIDCState{Provider: p.ID(), Nonce: randomString(), Verifier: oauth2.GenerateVerifier(), Next: "/id/account", LinkFor: c.ID}
	id, err := w.cfg.Store.BeginCeremony(r.Context(), "oidc", st, ceremonyLifetime)
	if err != nil {
		w.message(rw, r, http.StatusInternalServerError, "Link an account", err.Error())
		return
	}
	to, err := p.Begin(r.Context(), id, st)
	if err != nil {
		w.renderAccount(rw, r, c, err.Error())
		return
	}
	http.Redirect(rw, r, to, http.StatusFound)
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
