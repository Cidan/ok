package web

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/webhooks"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// deliveriesShown is how many of each webhook's deliveries its page lists.
const deliveriesShown = 10

// administered is the request's repository, if the person administers it — whose webhooks are
// theirs to see and change; otherwise it answers the request, and is nil.
func (s *Server) administered(w http.ResponseWriter, r *http.Request) *scmpb.Repository {
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return nil
	}
	if !s.review.May(r.Context(), repo, principal(r), authz.Administer) {
		pages.ServeError(w, r, http.StatusForbidden, "Forbidden", "Only the repository's administrators manage its webhooks.")
		return nil
	}
	return repo
}

// webhooksPage is a repository's webhooks (docs/hosted.md, "ok control at scale, as built").
func (s *Server) webhooksPage(w http.ResponseWriter, r *http.Request) {
	repo := s.administered(w, r)
	if repo == nil {
		return
	}
	s.serveWebhooks(w, r, repo, nil, "")
}

func (s *Server) serveWebhooks(w http.ResponseWriter, r *http.Request, repo *scmpb.Repository, created *pages.NewWebhook, errMsg string) {
	base := repoLink(repo.GetName()) + "/webhooks"
	v := pages.WebhooksView{Chrome: chrome(repo, "main", repo.GetHead(), ""), Enabled: s.hooks != nil && s.hooks.Sealer != nil, Created: created,
		Error: errMsg, CreateHref: base}
	if v.Enabled {
		hooks, err := s.db.Webhooks(r.Context(), repo.GetId())
		if err != nil {
			fail(w, r, err)
			return
		}
		for _, h := range hooks {
			ago, _ := age(h.CreatedAt)
			item := pages.WebhookItem{URL: h.URL, Events: eventsOf(h.Events), CreatedBy: people.Handle(r.Context(), h.CreatedBy), Ago: ago,
				DeleteHref: base + "/" + strconv.FormatInt(h.ID, 10) + "/delete"}
			ds, err := s.db.Deliveries(r.Context(), repo.GetId(), h.ID, deliveriesShown)
			if err != nil {
				fail(w, r, err)
				return
			}
			for _, d := range ds {
				dago, _ := age(d.CreatedAt)
				item.Deliveries = append(item.Deliveries, pages.DeliveryItem{State: stateOf(d.State), Event: d.Event, Detail: detailOf(d), Ago: dago,
					RedeliverHref: base + "/deliveries/" + strconv.FormatInt(d.ID, 10) + "/redeliver"})
			}
			v.Hooks = append(v.Hooks, item)
		}
	}
	pages.ServeWebhooks(w, r, v)
}

func eventsOf(events int64) string {
	var out []string
	if events&db.WebhookCL != 0 {
		out = append(out, "CL events")
	}
	if events&db.WebhookPush != 0 {
		out = append(out, "main moved")
	}
	return strings.Join(out, ", ")
}

func stateOf(state int) string {
	switch state {
	case db.DeliveryDelivered:
		return "delivered"
	case db.DeliveryDead:
		return "dead"
	}
	return "waiting"
}

// detailOf says what a delivery's endpoint last answered, and how often it was tried.
func detailOf(d db.Delivery) string {
	var parts []string
	if d.LastStatus != 0 {
		parts = append(parts, "HTTP "+strconv.Itoa(d.LastStatus))
	}
	if d.LastError != "" && d.State != db.DeliveryDelivered {
		parts = append(parts, d.LastError)
	}
	if d.Attempts > 1 {
		parts = append(parts, fmt.Sprintf("%d tries", d.Attempts))
	}
	return strings.Join(parts, " · ")
}

// createWebhook adds a webhook, showing its secret this once.
func (s *Server) createWebhook(w http.ResponseWriter, r *http.Request) {
	repo := s.administered(w, r)
	if repo == nil {
		return
	}
	if s.hooks == nil {
		s.serveWebhooks(w, r, repo, nil, "")
		return
	}
	if err := r.ParseForm(); err != nil {
		pages.ServeError(w, r, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	var events int64
	for _, e := range r.PostForm["event"] {
		switch e {
		case "cl":
			events |= db.WebhookCL
		case "push":
			events |= db.WebhookPush
		}
	}
	url := strings.TrimSpace(r.PostFormValue("url"))
	_, secret, err := s.hooks.Create(r.Context(), repo.GetId(), url, events, principal(r))
	if errors.Is(err, webhooks.ErrRefused) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		s.serveWebhooks(w, r, repo, nil, err.Error())
		return
	}
	if err != nil {
		fail(w, r, err)
		return
	}
	s.serveWebhooks(w, r, repo, &pages.NewWebhook{URL: url, Secret: secret}, "")
}

// deleteWebhook removes a webhook, and its deliveries.
func (s *Server) deleteWebhook(w http.ResponseWriter, r *http.Request) {
	repo := s.administered(w, r)
	if repo == nil {
		return
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		pages.ServeError(w, r, http.StatusNotFound, "Not Found", "No such webhook.")
		return
	}
	if err := s.db.DeleteWebhook(r.Context(), repo.GetId(), id); err != nil {
		fail(w, r, err)
		return
	}
	http.Redirect(w, r, repoLink(repo.GetName())+"/webhooks", http.StatusSeeOther)
}

// redeliver queues a delivery's payload again.
func (s *Server) redeliver(w http.ResponseWriter, r *http.Request) {
	repo := s.administered(w, r)
	if repo == nil {
		return
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err == nil && s.hooks != nil {
		err = s.hooks.Redeliver(r.Context(), repo.GetId(), id)
	}
	if err != nil {
		if errors.Is(err, webhooks.ErrRefused) || s.hooks == nil {
			pages.ServeError(w, r, http.StatusNotFound, "Not Found", "No such delivery.")
			return
		}
		fail(w, r, err)
		return
	}
	http.Redirect(w, r, repoLink(repo.GetName())+"/webhooks", http.StatusSeeOther)
}
