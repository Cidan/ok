package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"golang.org/x/oauth2"

	"github.com/Cidan/ok/internal/id/enterprise"
	"github.com/Cidan/ok/internal/id/signin"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/netguard"
)

// An enterprise organization's single sign-on (docs/hosted.md, "Enterprise organizations"): its
// people sign in at /id/login/org/<handle> through its own OpenID Connect provider, reached at
// public addresses alone, and are matched among the organization's people alone — never another
// account.

// orgProviders keeps each organization's sign-in provider, built anew when its settings change.
type orgProviders struct {
	mu   sync.Mutex
	byID map[string]orgProvider
}

type orgProvider struct {
	p       *signin.OIDC
	signIn  enterprise.SignIn
	updated time.Time
}

// orgProvider is an organization's sign-in provider, and what it matches people by.
func (w *Web) orgProvider(ctx context.Context, o store.Organization) (*signin.OIDC, enterprise.SignIn, error) {
	if w.cfg.Enterprise == nil {
		return nil, enterprise.SignIn{}, errors.New("this ok keeps no organization's identity provider")
	}
	p, err := w.cfg.Store.IdentityProviderOf(ctx, o.ID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, enterprise.SignIn{}, fmt.Errorf("%s has no single sign-on", o.Handle())
	}
	if err != nil {
		return nil, enterprise.SignIn{}, err
	}
	w.orgs.mu.Lock()
	cached, ok := w.orgs.byID[o.ID]
	w.orgs.mu.Unlock()
	if ok && cached.updated.Equal(p.Updated) {
		return cached.p, cached.signIn, nil
	}
	set, sec, err := enterprise.Open(w.cfg.Enterprise.Sealer, p)
	if err != nil {
		return nil, enterprise.SignIn{}, err
	}
	if set.SignIn == nil {
		return nil, enterprise.SignIn{}, fmt.Errorf("%s has no single sign-on", o.Handle())
	}
	match, claim := signin.ByEmail, "email"
	if set.SignIn.Match == "external_id" {
		match, claim = signin.ByExternalID, set.SignIn.Claim
		if claim == "" {
			claim = "sub"
		}
	}
	provider, err := signin.NewOIDC(ctx, signin.OIDCConfig{ID: "org-" + o.Handle(), Name: o.Handle(), Issuer: set.SignIn.Issuer, ClientID: set.SignIn.ClientID,
		ClientSecret: sec.OIDCClientSecret, Match: match, Claim: claim, Client: netguard.HTTPClient(w.cfg.Enterprise.AllowPrivate, 1<<20)},
		w.base()+"/id/login/oidc/callback")
	if err != nil {
		return nil, enterprise.SignIn{}, err
	}
	si := *set.SignIn
	si.Claim = claim
	w.orgs.mu.Lock()
	w.orgs.byID[o.ID] = orgProvider{p: provider, signIn: si, updated: p.Updated}
	w.orgs.mu.Unlock()
	return provider, si, nil
}

func (w *Web) orgLogin(rw http.ResponseWriter, r *http.Request) {
	o, err := w.cfg.Store.OrganizationByHandle(r.Context(), chi.URLParam(r, "handle"))
	if err != nil {
		http.NotFound(rw, r)
		return
	}
	p, _, err := w.orgProvider(r.Context(), o)
	if err != nil {
		w.render(rw, r, http.StatusNotFound, LoginPage(w.loginView(r, err.Error())))
		return
	}
	st := signin.OIDCState{Provider: "org:" + o.ID, Nonce: randomString(), Verifier: oauth2.GenerateVerifier(), Next: next(r.URL.Query().Get("next"))}
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

// orgCallback finishes an organization's sign-in: the person the provider names, among the
// organization's people, active.
func (w *Web) orgCallback(rw http.ResponseWriter, r *http.Request, orgID string, st signin.OIDCState, code string) {
	refuse := func(msg string) { w.render(rw, r, http.StatusUnauthorized, LoginPage(w.loginView(r, msg))) }
	o, err := w.cfg.Store.Organization(r.Context(), orgID)
	if err != nil {
		refuse("That sign-in's organization is gone.")
		return
	}
	p, si, err := w.orgProvider(r.Context(), o)
	if err != nil {
		refuse(err.Error())
		return
	}
	id, err := p.Identify(r.Context(), st, code)
	if err != nil {
		refuse(err.Error())
		return
	}
	var who store.Principal
	if si.Match == "external_id" {
		value, _ := id.Claims[si.Claim].(string)
		who, err = w.cfg.Store.OrganizationPerson(r.Context(), o.ID, value)
	} else {
		err = store.ErrNotFound
		if id.Email != "" {
			people, perr := w.cfg.Store.OrganizationPeople(r.Context(), o.ID)
			if perr != nil {
				refuse(perr.Error())
				return
			}
			for _, person := range people {
				if person.Email == id.Email && person.Active() {
					who, err = person, nil
					break
				}
			}
		}
	}
	if err != nil || !who.Active() {
		refuse(fmt.Sprintf("You are not one of %s's people, or your account there is suspended.", o.Handle()))
		return
	}
	w.signIn(rw, r, who, "oidc:org:"+o.Handle(), st.Next)
}
