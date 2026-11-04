package web

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"

	"github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/pages/servicegroup"
	"github.com/Cidan/ok/internal/control/people"
)

// How much of each list the dashboard and the palette hold. The inbox is read whole, as far as the
// directory lists it, so its lists' counts are the reader's.
const (
	dashboardInbox         = 200
	dashboardLanded        = 6
	dashboardNotifications = 5
	dashboardRepos         = 8
	paletteRepos           = 6
	paletteCLsShown        = 8
	paletteServices        = 6
	servicesDeadline       = 2 * time.Second
)

// dashboard is ok's home: the reader's inbox from every cell — the CLs that wait on them, theirs in
// flight, those they review, theirs lately landed — their latest notifications, the repositories
// (their favorites first), and the services' health. The directory's and the deployer's parts say
// so when they could not be read, and the page shows the rest.
func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	ctx, who := r.Context(), principal(r)
	v := pages.DashboardView{User: people.Handle(ctx, who)}
	if entries, err := s.inboxEntries(ctx, who); err != nil {
		slog.Warn("control: the dashboard's inbox", "err", err)
		v.InboxError = "The directory could not be reached, so your changes are not shown."
	} else {
		v.Waiting, v.Yours, v.Reviewing, v.Landed = inboxLists(entries)
	}
	if who != "" {
		list, err := s.notify.ListNotifications(ctx, &identitypb.ListNotificationsRequest{ForPersonId: who, Limit: dashboardNotifications})
		if err != nil {
			slog.Warn("control: the dashboard's notifications", "err", err)
		} else {
			v.Unread = list.GetUnread()
			for _, n := range list.GetNotifications() {
				ago, title := age(n.GetAt().AsTime())
				v.Notifications = append(v.Notifications, pages.NotificationItem{Who: people.Handle(ctx, n.GetActorId()), Text: n.GetText(),
					Repository: n.GetRepository(), Title: n.GetTitle(), Number: n.GetNumber(), Link: n.GetLink(), Read: n.GetRead(), Ago: ago, AgoTitle: title})
			}
		}
	}
	repos, _, err := s.repoPage(r, "", 1)
	if err != nil {
		fail(w, r, err)
		return
	}
	v.Repos = repos[:min(len(repos), dashboardRepos)]
	if v.Services, err = s.briefServiceGroups(ctx); err != nil {
		slog.Warn("control: the dashboard's services", "err", err)
		v.ServicesError = "The deployer could not be reached."
	}
	pages.ServeDashboard(w, r, v)
}

// repositories is the repository finder.
func (s *Server) repositories(w http.ResponseWriter, r *http.Request) {
	keyword := strings.TrimSpace(r.URL.Query().Get("q"))
	items, more, err := s.findRepositories(r, keyword, 1)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeRepositories(w, r, people.Handle(r.Context(), principal(r)), keyword, items, rowsURL(keyword), repoPageSize, more)
}

// repoName is what a repository's name may be, as the palette's repo parameter names one: an
// owner's, or an installation's bare one.
var repoName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)?$`)

// palette is the command palette's results for a query (q), in a repository (repo): the
// repositories it finds (the directory's search, across every cell), the reader's CLs — those in
// flight without a query, else those its number or every word of it names — and the groups of
// services it names, the deployer asked only briefly; beside the pages and commands, which the
// results' template filters.
func (s *Server) palette(w http.ResponseWriter, r *http.Request) {
	ctx, who := r.Context(), principal(r)
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	v := pages.PaletteView{Query: q}
	if repo := r.URL.Query().Get("repo"); repoName.MatchString(repo) {
		v.Repo = repo
	}
	repos, _, err := s.findRepositories(r, q, 1)
	if err != nil {
		fail(w, r, err)
		return
	}
	v.Repos = repos[:min(len(repos), paletteRepos)]
	if entries, err := s.inboxEntries(ctx, who); err != nil {
		slog.Warn("control: the palette's inbox", "err", err)
	} else {
		v.CLs = paletteCLs(entries, q)
	}
	if q != "" {
		groups, err := s.briefServiceGroups(ctx)
		if err != nil {
			slog.Warn("control: the palette's services", "err", err)
		}
		for _, g := range groups {
			if len(v.Services) < paletteServices && strings.Contains(strings.ToLower(g.Name), strings.ToLower(q)) {
				v.Services = append(v.Services, g)
			}
		}
	}
	pages.ServePalette(w, r, v)
}

// inboxEntries is the person's inbox from the directory, from every cell, closed CLs too: those that
// wait on them first, then the latest changed. None for a request no person signed.
func (s *Server) inboxEntries(ctx context.Context, who string) ([]*identitypb.InboxEntry, error) {
	if who == "" {
		return nil, nil
	}
	resp, err := s.inboxes.ListInbox(ctx, &identitypb.ListInboxRequest{ForPersonId: who, Closed: true, Limit: dashboardInbox})
	if err != nil {
		return nil, err
	}
	return resp.GetEntries(), nil
}

// inboxLists splits an inbox into the dashboard's lists, each CL in the first it belongs to: those
// in flight that wait on the reader (as reviewer, their review; as author, their reply), theirs in
// flight that do not, those they review that do not, and theirs lately landed.
func inboxLists(entries []*identitypb.InboxEntry) (waiting, yours, reviewing, landed []pages.CLItem) {
	seen := map[string]bool{}
	for _, e := range entries {
		key := e.GetRepository() + "#" + strconv.FormatInt(e.GetNumber(), 10)
		if seen[key] {
			continue
		}
		seen[key] = true
		item := inboxItem(e)
		authored := e.GetRelation() == identitypb.InboxEntry_RELATION_AUTHORED
		switch e.GetStatus() {
		case "draft", "open":
			switch {
			case e.GetAttention():
				item.Waiting = pages.WaitingFor(authored)
				waiting = append(waiting, item)
			case authored:
				yours = append(yours, item)
			default:
				reviewing = append(reviewing, item)
			}
		case "submitted":
			if authored && len(landed) < dashboardLanded {
				landed = append(landed, item)
			}
		}
	}
	return waiting, yours, reviewing, landed
}

// paletteCLs are the palette's CLs from an inbox: without a query, those in flight; with one, those
// it numbers (with or without its #) and those whose titles hold every word of it.
func paletteCLs(entries []*identitypb.InboxEntry, q string) []pages.CLItem {
	words := strings.Fields(strings.ToLower(q))
	var number int64
	if len(words) == 1 {
		number, _ = strconv.ParseInt(strings.TrimPrefix(words[0], "#"), 10, 64)
	}
	seen := map[string]bool{}
	var out []pages.CLItem
	for _, e := range entries {
		key := e.GetRepository() + "#" + strconv.FormatInt(e.GetNumber(), 10)
		if seen[key] || len(out) == paletteCLsShown {
			continue
		}
		open := e.GetStatus() == "draft" || e.GetStatus() == "open"
		match := open
		if len(words) > 0 {
			title := strings.ToLower(e.GetTitle())
			match = number > 0 && e.GetNumber() == number
			if !match {
				match = true
				for _, w := range words {
					match = match && strings.Contains(title, w)
				}
			}
		}
		if !match {
			continue
		}
		seen[key] = true
		item := inboxItem(e)
		if open && e.GetAttention() {
			item.Waiting = pages.WaitingFor(e.GetRelation() == identitypb.InboxEntry_RELATION_AUTHORED)
		}
		out = append(out, item)
	}
	return out
}

// inboxItem is an inbox entry as a list's row.
func inboxItem(e *identitypb.InboxEntry) pages.CLItem {
	label, kind := inboxStatus(e.GetStatus())
	item := pages.CLItem{Repository: e.GetRepository(), Number: e.GetNumber(), Title: e.GetTitle(), StatusLabel: label, StatusKind: kind,
		Href: clHref(e.GetRepository(), e.GetNumber())}
	item.Age, item.AgeTitle = age(e.GetUpdatedAt().AsTime())
	return item
}

// inboxStatus is an inbox entry's status as a CL's chip says it.
func inboxStatus(s string) (label, kind string) {
	switch s {
	case "draft":
		return "Draft", "draft"
	case "submitted":
		return "Merged", "merged"
	case "abandoned":
		return "Abandoned", "abandoned"
	}
	return "Open", "open"
}

// briefServiceGroups is every group of services, if the deployer answers within servicesDeadline.
func (s *Server) briefServiceGroups(ctx context.Context) ([]servicegroup.Group, error) {
	ctx, cancel := context.WithTimeout(ctx, servicesDeadline)
	defer cancel()
	return s.serviceGroups(ctx)
}
