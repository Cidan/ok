package web

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-webauthn/webauthn/webauthn"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/serve/authn"
)

// Approvals (docs/secrets.md, "Unsealing"): a person answers what ok secrets asked them — signed
// in, they see what it is, and approve it with their own passkey, verified, or decline it. A
// stolen session alone approves nothing: the passkey is the approval.

func (w *Web) mountApprovals(r chi.Router) {
	r.Get("/approve/{id}", w.approval)
	r.Post("/approve/{id}/decline", w.approvalDecline)
	if w.cfg.Passkeys != nil {
		r.Post("/approve/{id}/begin", w.approvalBegin)
		r.Post("/approve/{id}/finish", w.approvalFinish)
	}
}

// waitingApproval is the approval a request names, if it is the caller's and waits for them.
func (w *Web) waitingApproval(r *http.Request, c authn.Caller) (store.Approval, string) {
	a, err := w.cfg.Store.Approval(r.Context(), chi.URLParam(r, "id"))
	switch {
	case errors.Is(err, store.ErrNotFound), err == nil && a.PrincipalID != c.ID:
		return a, "Nothing of yours waits under that link: it may have expired, or be someone else's."
	case err != nil:
		return a, err.Error()
	case a.Decision == store.ApprovalApproved:
		return a, "You approved this already."
	case a.Decision == store.ApprovalDeclined:
		return a, "You declined this already."
	case !time.Now().Before(a.Expires):
		return a, "This expired: ask again."
	}
	return a, ""
}

// approvalCeremony is the passkey ceremony's kind for one approval and its person: a ceremony
// begun for one approval finishes no other.
func approvalCeremony(a store.Approval) string { return "approve:" + a.ID + ":" + a.PrincipalID }

func (w *Web) approval(rw http.ResponseWriter, r *http.Request) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	a, why := w.waitingApproval(r, c)
	if why != "" {
		w.message(rw, r, http.StatusNotFound, "Nothing to approve", why)
		return
	}
	w.render(rw, r, http.StatusOK, ApprovePage(ApproveView{Installation: w.cfg.Installation, Principal: c.Principal, ID: a.ID,
		Description: a.Description, Asked: a.Created.Format("15:04:05 MST, Jan 2"), Expires: a.Expires.Format("15:04:05 MST"),
		PasskeysOn: w.cfg.Passkeys != nil}))
}

func (w *Web) approvalBegin(rw http.ResponseWriter, r *http.Request) {
	c, ok := caller(r)
	if !ok {
		http.Error(rw, "sign in first", http.StatusUnauthorized)
		return
	}
	a, why := w.waitingApproval(r, c)
	if why != "" {
		http.Error(rw, why, http.StatusNotFound)
		return
	}
	p, err := w.cfg.Store.PrincipalByID(r.Context(), c.ID)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	opts, session, err := w.cfg.Passkeys.BeginApproval(r.Context(), p)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := w.cfg.Store.BeginCeremony(r.Context(), approvalCeremony(a), session, time.Until(a.Expires))
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(rw, http.StatusOK, begun{Ceremony: id, Options: opts})
}

func (w *Web) approvalFinish(rw http.ResponseWriter, r *http.Request) {
	c, ok := caller(r)
	if !ok {
		http.Error(rw, "sign in first", http.StatusUnauthorized)
		return
	}
	a, why := w.waitingApproval(r, c)
	if why != "" {
		http.Error(rw, why, http.StatusNotFound)
		return
	}
	var session webauthn.SessionData
	if err := w.cfg.Store.EndCeremony(r.Context(), r.URL.Query().Get("ceremony"), approvalCeremony(a), &session); err != nil {
		http.Error(rw, "that took too long: try again", http.StatusBadRequest)
		return
	}
	p, err := w.cfg.Store.PrincipalByID(r.Context(), c.ID)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := w.cfg.Passkeys.FinishApproval(r.Context(), p, session, r); err != nil {
		http.Error(rw, err.Error(), http.StatusUnauthorized)
		return
	}
	if err := w.cfg.Store.DecideApproval(r.Context(), a.ID, c.ID, true); err != nil {
		http.Error(rw, err.Error(), http.StatusConflict)
		return
	}
	if err := w.cfg.Store.Audit(r.Context(), p.Name, "approval.approve", map[string]any{"approval": a.ID, "description": a.Description}); err != nil {
		slog.Error("identity web: audit", "err", err)
	}
	writeJSON(rw, http.StatusOK, map[string]string{})
}

func (w *Web) approvalDecline(rw http.ResponseWriter, r *http.Request) {
	c, ok := w.requireSignIn(rw, r)
	if !ok {
		return
	}
	a, why := w.waitingApproval(r, c)
	if why != "" {
		w.message(rw, r, http.StatusNotFound, "Nothing to decline", why)
		return
	}
	if err := w.cfg.Store.DecideApproval(r.Context(), a.ID, c.ID, false); err != nil {
		w.message(rw, r, http.StatusConflict, "Nothing to decline", err.Error())
		return
	}
	if err := w.cfg.Store.Audit(r.Context(), c.Principal, "approval.decline", map[string]any{"approval": a.ID, "description": a.Description}); err != nil {
		slog.Error("identity web: audit", "err", err)
	}
	w.message(rw, r, http.StatusOK, "Declined", "Nothing is unsealed. If you did not ask for this, someone may be using your credential: tell an administrator.")
}
