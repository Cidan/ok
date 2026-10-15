// Package web is ok identity's pages: signing in (OpenID Connect, LDAP, passkeys), approving a
// command line's device login, a person's account (their devices and passkeys), and the
// administrators' view of the sync. They live under /id/.
package web

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/go-chi/chi/v5"
	"github.com/go-webauthn/webauthn/webauthn"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/id/service"
	"github.com/Cidan/ok/internal/id/signin"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/web/static"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

//go:embed app.css passkeys.js
var assets embed.FS

// Config is what the pages need.
type Config struct {
	Store    *store.Store
	Services *service.Server
	// Installation names the installation on every page: "Example ok".
	Installation string
	// PublicURL is where people reach ok: https://ok.example.com.
	PublicURL string
	// OIDC are the OpenID Connect providers people sign in with, in their buttons' order.
	OIDC []*signin.OIDC
	// LDAP, when set, signs people in with their LDAP password; LDAPName names it on the page.
	LDAP     *signin.LDAP
	LDAPName string
	// Passkeys, when set, signs people in with passkeys; nil when ok has no domain to bind them to.
	Passkeys *signin.Passkeys
	// SessionLifetime and SessionIdle bound a browser session.
	SessionLifetime, SessionIdle time.Duration
}

// Web is the pages' node.
type Web struct {
	cfg    Config
	secure bool // cookies only over HTTPS: the public URL is https
}

var _ node.WebServer = (*Web)(nil)

// New returns the pages over cfg.
func New(cfg Config) *Web {
	if cfg.SessionLifetime == 0 {
		cfg.SessionLifetime = store.DefaultSessionLifetime
	}
	if cfg.SessionIdle == 0 {
		cfg.SessionIdle = store.DefaultSessionIdle
	}
	return &Web{cfg: cfg, secure: strings.HasPrefix(cfg.PublicURL, "https://")}
}

func (w *Web) Name() string                     { return "ok/identity/web" }
func (w *Web) Dependencies() []node.Node        { return nil }
func (w *Web) Healthy(ctx context.Context) bool { return w.cfg.Store.Healthy(ctx) }
func (w *Web) Setup(context.Context) error      { return nil }

// Mount serves the pages under /id.
func (w *Web) Mount(r *chi.Mux) (string, error) {
	sub, err := fs.Sub(assets, ".")
	if err != nil {
		return "", err
	}
	r.Use(sameSite)
	r.Handle("/assets/favicon.svg", http.StripPrefix("/id/assets/", static.Handler()))
	r.Handle("/assets/*", http.StripPrefix("/id/assets/", http.FileServer(http.FS(sub))))
	r.Get("/login", w.login)
	r.Get("/login/oidc/callback", w.oidcCallback)
	r.Get("/login/oidc/{provider}", w.oidcBegin)
	r.Post("/login/ldap", w.ldapLogin)
	if w.cfg.Passkeys != nil {
		r.Post("/login/passkey/begin", w.passkeyLoginBegin)
		r.Post("/login/passkey/finish", w.passkeyLoginFinish)
		r.Post("/account/passkeys/begin", w.passkeyAddBegin)
		r.Post("/account/passkeys/finish", w.passkeyAddFinish)
	}
	r.Post("/logout", w.logout)
	r.Get("/device", w.device)
	r.Post("/device", w.deviceDecide)
	r.Get("/account", w.account)
	r.Post("/account/credentials/revoke", w.revokeCredential)
	r.Post("/account/passkeys/remove", w.passkeyRemove)
	r.Get("/admin", w.admin)
	r.Post("/admin/sync", w.adminSync)
	r.Post("/admin/name", w.adminName)
	return "/id", nil
}

// sameSite refuses a request that changes something when a browser sent it from another site:
// the session cookie is SameSite=Lax, and this holds besides.
func sameSite(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			switch r.Header.Get("Sec-Fetch-Site") {
			case "", "same-origin", "none":
			default:
				http.Error(rw, "a request from another site is refused", http.StatusForbidden)
				return
			}
			if o := r.Header.Get("Origin"); o != "" {
				if u, err := url.Parse(o); err != nil || u.Host != r.Host {
					http.Error(rw, "a request from another site is refused", http.StatusForbidden)
					return
				}
			}
		}
		rw.Header().Set("X-Frame-Options", "DENY")
		rw.Header().Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'; form-action 'self' https:")
		next.ServeHTTP(rw, r)
	})
}

func (w *Web) render(rw http.ResponseWriter, r *http.Request, code int, c templ.Component) {
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	rw.Header().Set("Cache-Control", "no-store")
	rw.WriteHeader(code)
	if err := c.Render(r.Context(), rw); err != nil {
		slog.Error("identity web: rendering", "path", r.URL.Path, "err", err)
	}
}

func (w *Web) message(rw http.ResponseWriter, r *http.Request, code int, title, msg string) {
	w.render(rw, r, code, MessagePage(w.cfg.Installation, title, msg, code < 400))
}

// next is where to go after signing in: a path on this site, never another's.
func next(raw string) string {
	if !strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, "//") || strings.HasPrefix(raw, `/\`) || strings.ContainsAny(raw, "\r\n") {
		return "/id/account"
	}
	return raw
}

// caller is the signed-in person of a request, if any.
func caller(r *http.Request) (authn.Caller, bool) {
	c, ok := authn.FromContext(r.Context())
	if !ok || !c.Human {
		return authn.Caller{}, false
	}
	return c, true
}

// requireSignIn sends someone not signed in to sign in, and back here after.
func (w *Web) requireSignIn(rw http.ResponseWriter, r *http.Request) (authn.Caller, bool) {
	c, ok := caller(r)
	if !ok {
		http.Redirect(rw, r, "/id/login?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
	}
	return c, ok
}

func (w *Web) loginView(r *http.Request, errMsg string) LoginView {
	n := next(r.URL.Query().Get("next"))
	v := LoginView{Installation: w.cfg.Installation, Next: n, NextQuery: url.QueryEscape(n), Error: errMsg, LDAP: w.cfg.LDAP != nil, LDAPName: w.cfg.LDAPName, Passkeys: w.cfg.Passkeys != nil}
	for _, p := range w.cfg.OIDC {
		v.Providers = append(v.Providers, Provider{ID: p.ID(), Name: p.Name()})
	}
	return v
}

func (w *Web) login(rw http.ResponseWriter, r *http.Request) {
	if _, ok := caller(r); ok {
		http.Redirect(rw, r, next(r.URL.Query().Get("next")), http.StatusSeeOther)
		return
	}
	w.render(rw, r, http.StatusOK, LoginPage(w.loginView(r, "")))
}

// signIn begins a session for a person and sends them on.
func (w *Web) signIn(rw http.ResponseWriter, r *http.Request, p store.Principal, via, to string) {
	tok, err := w.cfg.Store.CreateSession(r.Context(), p.ID, via, w.cfg.SessionLifetime, w.cfg.SessionIdle)
	if err != nil {
		w.message(rw, r, http.StatusInternalServerError, "Sign-in failed", err.Error())
		return
	}
	http.SetCookie(rw, &http.Cookie{
		Name: authn.SessionCookie, Value: tok, Path: "/", HttpOnly: true, Secure: w.secure,
		SameSite: http.SameSiteLaxMode, MaxAge: int(w.cfg.SessionLifetime.Seconds()),
	})
	if err := w.cfg.Store.Audit(r.Context(), p.Name, "sign in", map[string]any{"via": via}); err != nil {
		slog.Error("identity web: audit", "err", err)
	}
	http.Redirect(rw, r, next(to), http.StatusSeeOther)
}

func (w *Web) logout(rw http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(authn.SessionCookie); err == nil {
		if err := w.cfg.Store.EndSession(r.Context(), c.Value); err != nil {
			slog.Error("identity web: ending a session", "err", err)
		}
	}
	http.SetCookie(rw, &http.Cookie{Name: authn.SessionCookie, Value: "", Path: "/", HttpOnly: true, Secure: w.secure, SameSite: http.SameSiteLaxMode, MaxAge: -1})
	http.Redirect(rw, r, "/id/login", http.StatusSeeOther)
}

func randomString() string {
	var b [24]byte
	rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}

const ceremonyLifetime = 10 * time.Minute

func (w *Web) provider(id string) *signin.OIDC {
	for _, p := range w.cfg.OIDC {
		if p.ID() == id {
			return p
		}
	}
	return nil
}

func (w *Web) oidcBegin(rw http.ResponseWriter, r *http.Request) {
	p := w.provider(chi.URLParam(r, "provider"))
	if p == nil {
		http.NotFound(rw, r)
		return
	}
	st := signin.OIDCState{Provider: p.ID(), Nonce: randomString(), Verifier: oauth2.GenerateVerifier(), Next: next(r.URL.Query().Get("next"))}
	id, err := w.cfg.Store.BeginCeremony(r.Context(), "oidc", st, ceremonyLifetime)
	if err != nil {
		w.message(rw, r, http.StatusInternalServerError, "Sign-in failed", err.Error())
		return
	}
	to, err := p.Begin(r.Context(), id, st)
	if err != nil {
		w.render(rw, r, http.StatusServiceUnavailable, LoginPage(w.loginView(r, err.Error())))
		return
	}
	http.Redirect(rw, r, to, http.StatusFound)
}

func (w *Web) oidcCallback(rw http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var st signin.OIDCState
	if err := w.cfg.Store.EndCeremony(r.Context(), q.Get("state"), "oidc", &st); err != nil {
		w.render(rw, r, http.StatusBadRequest, LoginPage(w.loginView(r, "That sign-in expired, or was finished already: sign in again.")))
		return
	}
	if e := q.Get("error"); e != "" {
		w.render(rw, r, http.StatusUnauthorized, LoginPage(w.loginView(r, "The provider did not sign you in: "+e)))
		return
	}
	p := w.provider(st.Provider)
	if p == nil {
		http.NotFound(rw, r)
		return
	}
	who, err := p.Finish(r.Context(), w.cfg.Store, st, q.Get("code"))
	if err != nil {
		w.render(rw, r, http.StatusUnauthorized, LoginPage(w.loginView(r, err.Error())))
		return
	}
	w.signIn(rw, r, who, "oidc:"+p.ID(), st.Next)
}

func (w *Web) ldapLogin(rw http.ResponseWriter, r *http.Request) {
	if w.cfg.LDAP == nil {
		http.NotFound(rw, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	who, err := w.cfg.LDAP.SignIn(r.Context(), w.cfg.Store, r.PostForm.Get("username"), r.PostForm.Get("password"))
	if err != nil {
		v := w.loginView(r, err.Error())
		v.Next = next(r.PostForm.Get("next"))
		v.NextQuery = url.QueryEscape(v.Next)
		w.render(rw, r, http.StatusUnauthorized, LoginPage(v))
		return
	}
	w.signIn(rw, r, who, "ldap", r.PostForm.Get("next"))
}

// begun is what a passkey ceremony's beginning answers: its ID and the authenticator's options.
type begun struct {
	Ceremony string `json:"ceremony"`
	Options  any    `json:"options"`
}

func writeJSON(rw http.ResponseWriter, code int, v any) {
	rw.Header().Set("Content-Type", "application/json")
	rw.Header().Set("Cache-Control", "no-store")
	rw.WriteHeader(code)
	json.NewEncoder(rw).Encode(v)
}

func (w *Web) passkeyLoginBegin(rw http.ResponseWriter, r *http.Request) {
	opts, session, err := w.cfg.Passkeys.BeginLogin()
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	id, err := w.cfg.Store.BeginCeremony(r.Context(), "passkey-login", session, ceremonyLifetime)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(rw, http.StatusOK, begun{Ceremony: id, Options: opts})
}

func (w *Web) passkeyLoginFinish(rw http.ResponseWriter, r *http.Request) {
	var session webauthn.SessionData
	if err := w.cfg.Store.EndCeremony(r.Context(), r.URL.Query().Get("ceremony"), "passkey-login", &session); err != nil {
		http.Error(rw, "that sign-in expired: try again", http.StatusBadRequest)
		return
	}
	who, err := w.cfg.Passkeys.FinishLogin(r.Context(), session, r)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusUnauthorized)
		return
	}
	tok, err := w.cfg.Store.CreateSession(r.Context(), who.ID, "passkey", w.cfg.SessionLifetime, w.cfg.SessionIdle)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	http.SetCookie(rw, &http.Cookie{
		Name: authn.SessionCookie, Value: tok, Path: "/", HttpOnly: true, Secure: w.secure,
		SameSite: http.SameSiteLaxMode, MaxAge: int(w.cfg.SessionLifetime.Seconds()),
	})
	if err := w.cfg.Store.Audit(r.Context(), who.Name, "sign in", map[string]any{"via": "passkey"}); err != nil {
		slog.Error("identity web: audit", "err", err)
	}
	writeJSON(rw, http.StatusOK, map[string]string{"next": next(r.URL.Query().Get("next"))})
}

func (w *Web) device(rw http.ResponseWriter, r *http.Request) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	v := DeviceView{Installation: w.cfg.Installation, Principal: c.Principal, Code: r.URL.Query().Get("code")}
	if v.Code == "" {
		w.render(rw, r, http.StatusOK, DevicePage(v))
		return
	}
	d, err := w.cfg.Store.DeviceLoginByUserCode(r.Context(), v.Code)
	if err != nil {
		v.Error = "No device is waiting with that code: it may have expired. Start again on the device."
		w.render(rw, r, http.StatusNotFound, DevicePage(v))
		return
	}
	w.render(rw, r, http.StatusOK, DeviceConfirmPage(DeviceConfirmView{
		Installation: w.cfg.Installation, Principal: c.Principal, Code: d.UserCode, Label: d.Label,
		From: hostOnly(d.ClientAddress), Asked: d.Created.Format("15:04:05 MST, Jan 2"),
	}))
}

// hostOnly is an address without its port.
func hostOnly(addr string) string {
	if i := strings.LastIndexByte(addr, ':'); i > 0 && !strings.HasSuffix(addr, "]") {
		return strings.Trim(addr[:i], "[]")
	}
	return addr
}

func (w *Web) deviceDecide(rw http.ResponseWriter, r *http.Request) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	approve := r.PostForm.Get("decision") == "approve"
	if err := w.cfg.Store.DecideDeviceLogin(r.Context(), r.PostForm.Get("code"), c.ID, approve); err != nil {
		w.message(rw, r, http.StatusNotFound, "Nothing to decide", "No device is waiting with that code: it may have expired, or been decided already.")
		return
	}
	if !approve {
		w.message(rw, r, http.StatusOK, "Denied", "The device is not signed in. If you did not start this, someone may be trying to sign in as you: tell an administrator.")
		return
	}
	w.message(rw, r, http.StatusOK, "Approved", "The device is signing in. You can close this page.")
}

// asCaller is ctx carrying the request's signed-in person, so the services authorize the pages'
// calls as they would the person's own.
func asCaller(r *http.Request, c authn.Caller) context.Context {
	return authn.NewContext(r.Context(), c)
}

func (w *Web) account(rw http.ResponseWriter, r *http.Request) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	w.renderAccount(rw, r, c, "")
}

func (w *Web) renderAccount(rw http.ResponseWriter, r *http.Request, c authn.Caller, errMsg string) {
	p, err := w.cfg.Store.PrincipalByID(r.Context(), c.ID)
	if err != nil {
		w.message(rw, r, http.StatusInternalServerError, "Your account", err.Error())
		return
	}
	v := AccountView{Installation: w.cfg.Installation, Principal: p.Name, Email: p.Email, Error: errMsg, PasskeysOn: w.cfg.Passkeys != nil}
	creds, err := w.cfg.Store.CredentialsOf(r.Context(), p.ID)
	if err != nil {
		w.message(rw, r, http.StatusInternalServerError, "Your account", err.Error())
		return
	}
	for _, cr := range creds {
		row := CredentialRow{ID: cr.ID, Label: cr.Label, Revocable: cr.Revoked.IsZero()}
		switch {
		case !cr.Revoked.IsZero():
			row.Detail = "signed out " + cr.Revoked.Format("Jan 2, 2006")
		case cr.LastIssued.IsZero():
			row.Detail = "signed in " + cr.Created.Format("Jan 2, 2006")
		default:
			row.Detail = "signed in " + cr.Created.Format("Jan 2, 2006") + ", last renewed " + cr.LastIssued.Format("Jan 2 15:04")
		}
		v.Credentials = append(v.Credentials, row)
	}
	pks, err := w.cfg.Store.Passkeys(r.Context(), p.ID)
	if err != nil {
		w.message(rw, r, http.StatusInternalServerError, "Your account", err.Error())
		return
	}
	for _, pk := range pks {
		detail := "added " + pk.Created.Format("Jan 2, 2006")
		if !pk.LastUsed.IsZero() {
			detail += ", last used " + pk.LastUsed.Format("Jan 2 15:04")
		}
		v.Passkeys = append(v.Passkeys, PasskeyRow{ID: base64.RawURLEncoding.EncodeToString(pk.ID), Label: pk.Label, Detail: detail})
	}
	if _, err := w.cfg.Services.GetSyncStatus(asCaller(r, c), &identitypb.GetSyncStatusRequest{}); err == nil {
		v.Admin = true
	}
	w.render(rw, r, http.StatusOK, AccountPage(v))
}

func (w *Web) revokeCredential(rw http.ResponseWriter, r *http.Request) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := w.cfg.Services.Revoke(asCaller(r, c), &identitypb.RevokeRequest{Id: r.PostForm.Get("id")}); err != nil {
		w.renderAccount(rw, r, c, status.Convert(err).Message())
		return
	}
	http.Redirect(rw, r, "/id/account", http.StatusSeeOther)
}

func (w *Web) passkeyAddBegin(rw http.ResponseWriter, r *http.Request) {
	c, ok := caller(r)
	if !ok {
		http.Error(rw, "sign in first", http.StatusUnauthorized)
		return
	}
	p, err := w.cfg.Store.PrincipalByID(r.Context(), c.ID)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	opts, session, err := w.cfg.Passkeys.BeginRegistration(r.Context(), p)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	id, err := w.cfg.Store.BeginCeremony(r.Context(), "passkey-add:"+p.ID, session, ceremonyLifetime)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(rw, http.StatusOK, begun{Ceremony: id, Options: opts})
}

func (w *Web) passkeyAddFinish(rw http.ResponseWriter, r *http.Request) {
	c, ok := caller(r)
	if !ok {
		http.Error(rw, "sign in first", http.StatusUnauthorized)
		return
	}
	var session webauthn.SessionData
	if err := w.cfg.Store.EndCeremony(r.Context(), r.URL.Query().Get("ceremony"), "passkey-add:"+c.ID, &session); err != nil {
		http.Error(rw, "that took too long: try again", http.StatusBadRequest)
		return
	}
	p, err := w.cfg.Store.PrincipalByID(r.Context(), c.ID)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	label := strings.TrimSpace(r.URL.Query().Get("label"))
	if label == "" || len(label) > 100 {
		label = "Passkey"
	}
	if err := w.cfg.Passkeys.FinishRegistration(r.Context(), p, session, r, label); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	if err := w.cfg.Store.Audit(r.Context(), p.Name, "add passkey", map[string]any{"label": label}); err != nil {
		slog.Error("identity web: audit", "err", err)
	}
	writeJSON(rw, http.StatusOK, map[string]string{})
}

func (w *Web) passkeyRemove(rw http.ResponseWriter, r *http.Request) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := base64.RawURLEncoding.DecodeString(r.PostForm.Get("id"))
	if err == nil {
		err = w.cfg.Store.RemovePasskey(r.Context(), c.ID, id)
	}
	if err != nil {
		w.renderAccount(rw, r, c, "That passkey is not yours, or is gone already.")
		return
	}
	http.Redirect(rw, r, "/id/account", http.StatusSeeOther)
}

func (w *Web) adminView(r *http.Request, c authn.Caller, errMsg string, changes []string) (AdminView, error) {
	st, err := w.cfg.Services.GetSyncStatus(asCaller(r, c), &identitypb.GetSyncStatusRequest{})
	if err != nil {
		return AdminView{}, err
	}
	v := AdminView{Installation: w.cfg.Installation, Source: st.GetSource(), SyncError: st.GetError(), Notes: st.GetNotes(), Error: errMsg, Changes: changes, LastRun: "never"}
	if v.Source == "" {
		v.Source = "none configured"
	}
	if st.GetLastRun() != nil {
		v.LastRun = st.GetLastRun().AsTime().Local().Format("Jan 2 15:04:05 MST")
	}
	for _, wt := range st.GetWaiting() {
		ph := "a handle: ada-l"
		if wt.GetGroup() {
			ph = "a group name: eng-infra"
		}
		v.Waiting = append(v.Waiting, WaitingRow{ExternalID: wt.GetExternalId(), Group: wt.GetGroup(), Reason: wt.GetReason(), Placeholder: ph})
	}
	return v, nil
}

func (w *Web) admin(rw http.ResponseWriter, r *http.Request) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	v, err := w.adminView(r, c, "", nil)
	if err != nil {
		w.message(rw, r, http.StatusForbidden, "Administration", status.Convert(err).Message())
		return
	}
	w.render(rw, r, http.StatusOK, AdminPage(v))
}

func (w *Web) adminSync(rw http.ResponseWriter, r *http.Request) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	resp, err := w.cfg.Services.Sync(asCaller(r, c), &identitypb.SyncRequest{})
	errMsg := ""
	var changes []string
	if err != nil {
		errMsg = status.Convert(err).Message()
	} else if changes = resp.GetChanges(); len(changes) == 0 {
		changes = []string{"nothing: the directory follows the system of record"}
	}
	v, verr := w.adminView(r, c, errMsg, changes)
	if verr != nil {
		w.message(rw, r, http.StatusForbidden, "Administration", status.Convert(verr).Message())
		return
	}
	w.render(rw, r, http.StatusOK, AdminPage(v))
}

func (w *Web) adminName(rw http.ResponseWriter, r *http.Request) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	_, err := w.cfg.Services.SetName(asCaller(r, c), &identitypb.SetNameRequest{
		ExternalId: r.PostForm.Get("external_id"), Group: r.PostForm.Get("group") == "true", Name: r.PostForm.Get("name"),
	})
	if err != nil {
		v, verr := w.adminView(r, c, status.Convert(err).Message(), nil)
		if verr != nil {
			w.message(rw, r, http.StatusForbidden, "Administration", status.Convert(verr).Message())
			return
		}
		w.render(rw, r, http.StatusBadRequest, AdminPage(v))
		return
	}
	http.Redirect(rw, r, "/id/admin", http.StatusSeeOther)
}
