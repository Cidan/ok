package web

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"

	"github.com/Cidan/ok/internal/control/db"
	pages "github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/serve/authn"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// repoPageSize is how many repositories the finder loads at a time.
const repoPageSize = 25

// rowsURL is the finder's infinite-scroll endpoint, carrying the search.
func rowsURL(keyword string) string {
	u := "/repositories/rows"
	if keyword != "" {
		u += "?q=" + url.QueryEscape(keyword)
	}
	return u
}

// repoPage is one page of the finder, of the repositories the principal may read: their favorites
// first, then the most recently updated, and whether another page follows. It walks the
// projection a batch at a time, past the earlier pages' readable repositories.
// unread is how many of the signed-in person's notifications are unread; 0 when the directory
// cannot say, which the home does without.
func (s *Server) unread(r *http.Request) int64 {
	who := principal(r)
	if who == "" {
		return 0
	}
	resp, err := s.notify.ListNotifications(r.Context(), &identitypb.ListNotificationsRequest{ForPersonId: who, Unread: true, Limit: 1})
	if err != nil {
		slog.Warn("control: the unread notifications", "err", err)
		return 0
	}
	return resp.GetUnread()
}

// inboxShown is how many of a person's CLs under way the home shows.
const inboxShown = 10

// inbox is the signed-in person's CLs under way, from the directory, whichever cell holds each; none
// when the directory cannot say, which the home does without.
func (s *Server) inbox(r *http.Request) []pages.InboxItem {
	who := principal(r)
	if who == "" {
		return nil
	}
	resp, err := s.inboxes.ListInbox(r.Context(), &identitypb.ListInboxRequest{ForPersonId: who, Limit: inboxShown})
	if err != nil {
		slog.Warn("control: the inbox", "err", err)
		return nil
	}
	out := make([]pages.InboxItem, 0, len(resp.GetEntries()))
	for _, e := range resp.GetEntries() {
		role := "author"
		if e.GetRelation() == identitypb.InboxEntry_RELATION_REVIEWING {
			role = "reviewer"
		}
		ago, title := age(e.GetUpdatedAt().AsTime())
		out = append(out, pages.InboxItem{Repository: e.GetRepository(), Number: e.GetNumber(), Title: e.GetTitle(), Role: role,
			Attention: e.GetAttention(), Status: e.GetStatus(), Link: repoLink(e.GetRepository()) + "/cl/" + strconv.FormatInt(e.GetNumber(), 10),
			UpdatedAgo: ago, UpdatedTitle: title})
	}
	return out
}

// searchLimit is how many repositories a search through the directory lists: one page of them.
const searchLimit = 50

// findRepositories is a page of the home's repositories: those this cell holds, newest first, or —
// asked for some — the directory's search's, across every cell (docs/hosted.md, "ok control at
// scale, as built"), one page of them.
func (s *Server) findRepositories(r *http.Request, keyword string, page int) ([]pages.RepoItem, bool, error) {
	if keyword == "" {
		return s.repoPage(r, keyword, page)
	}
	who := principal(r)
	if page > 1 || who == "" {
		return nil, false, nil
	}
	found, err := s.tenancy.Search(r.Context(), &identitypb.SearchRequest{Query: keyword, Limit: searchLimit, ForPersonId: who})
	if err != nil {
		return nil, false, err
	}
	ids := make([]int64, 0, len(found.GetRepositories()))
	for _, e := range found.GetRepositories() {
		ids = append(ids, e.GetId())
	}
	held, err := s.db.RepositoriesByID(r.Context(), who, ids)
	if err != nil {
		return nil, false, err
	}
	here := make(map[int64]db.Repository, len(held))
	for _, h := range held {
		here[h.ID] = h
	}
	items := make([]pages.RepoItem, 0, len(ids))
	for _, e := range found.GetRepositories() {
		if h, ok := here[e.GetId()]; ok {
			items = append(items, s.repoItem(r.Context(), h))
			continue
		}
		// Another cell's: what the directory says of it.
		at := e.GetCreatedAt().AsTime()
		ago, title := age(at)
		items = append(items, pages.RepoItem{
			Name:         e.GetFullName(),
			Detail:       fmt.Sprintf("%s · created by %s", e.GetVisibility(), people.Handle(r.Context(), e.GetCreatedById())),
			Link:         repoLink(e.GetFullName()),
			UpdatedUnix:  at.Unix(),
			UpdatedAgo:   ago,
			UpdatedTitle: title,
		})
	}
	return items, false, nil
}

func (s *Server) repoPage(r *http.Request, keyword string, page int) ([]pages.RepoItem, bool, error) {
	const batch = 4 * repoPageSize
	skip := (page - 1) * repoPageSize
	var rs []db.Repository
	for off := 0; len(rs) <= repoPageSize; off += batch {
		rows, err := s.db.ListRepositories(r.Context(), principal(r), keyword, off, batch)
		if err != nil {
			return nil, false, err
		}
		for _, repo := range rows {
			if !s.readable(r.Context(), repo.ID) {
				continue
			}
			if skip > 0 {
				skip--
				continue
			}
			if rs = append(rs, repo); len(rs) > repoPageSize {
				break
			}
		}
		if len(rows) < batch {
			break
		}
	}
	more := len(rs) > repoPageSize
	if more {
		rs = rs[:repoPageSize]
	}
	items := make([]pages.RepoItem, 0, len(rs))
	for _, repo := range rs {
		items = append(items, s.repoItem(r.Context(), repo))
	}
	return items, more, nil
}

// repoItem is a row of the home's list for a repository this cell holds.
func (s *Server) repoItem(ctx context.Context, repo db.Repository) pages.RepoItem {
	ago, title := age(repo.UpdatedAt)
	return pages.RepoItem{
		Name:         repo.Name,
		Detail:       fmt.Sprintf("%d %s · created by %s", repo.HeadSeq, plural(repo.HeadSeq, "commit", "commits"), people.Handle(ctx, repo.CreatedBy)),
		Link:         repoLink(repo.Name),
		Starred:      repo.Favorite,
		UpdatedUnix:  repo.UpdatedAt.Unix(),
		UpdatedAgo:   ago,
		UpdatedTitle: title,
	}
}

func plural(n int64, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// home is the repository finder.
func (s *Server) home(w http.ResponseWriter, r *http.Request) {
	keyword := strings.TrimSpace(r.URL.Query().Get("q"))
	items, more, err := s.findRepositories(r, keyword, 1)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeHome(w, r, people.Handle(r.Context(), principal(r)), s.unread(r), s.inbox(r), keyword, items, rowsURL(keyword), repoPageSize, more)
}

// homeList is the finder's list region for a search, which the search box swaps in.
func (s *Server) homeList(w http.ResponseWriter, r *http.Request) {
	keyword := strings.TrimSpace(r.URL.Query().Get("q"))
	items, more, err := s.findRepositories(r, keyword, 1)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeHomeList(w, r, keyword, items, rowsURL(keyword), repoPageSize, more)
}

// homeRows is one more page of the finder's rows.
func (s *Server) homeRows(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	page = max(page, 1)
	items, _, err := s.findRepositories(r, strings.TrimSpace(r.URL.Query().Get("q")), page)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeRepoRows(w, r, items)
}

// newRepoForm is the create-repository form for the person a request is for: the owners they may
// choose, the installation's organization first.
func newRepoForm(r *http.Request) pages.NewRepoForm {
	f := pages.NewRepoForm{Organization: installationHandle}
	if installationHandle != "" {
		f.Owners = append(f.Owners, installationHandle)
	}
	if c, ok := authn.FromContext(r.Context()); ok && c.Human {
		f.Owners = append(f.Owners, people.HandleOf(c.Principal))
	}
	if len(f.Owners) > 0 {
		f.Owner = f.Owners[0]
	}
	return f
}

func (s *Server) newRepository(w http.ResponseWriter, r *http.Request) {
	pages.ServeNewRepo(w, r, newRepoForm(r), "")
}

// createRepository creates a repository on the SCM for the principal, of the owner they chose,
// then shows it.
func (s *Server) createRepository(w http.ResponseWriter, r *http.Request) {
	f := newRepoForm(r)
	f.Name = strings.TrimSpace(r.FormValue("name"))
	if o := r.FormValue("owner"); o != "" {
		f.Owner = o
	}
	if f.Name == "" {
		pages.ServeNewRepo(w, r, f, "A repository needs a name.")
		return
	}
	repo, err := s.review.CreateRepository(r.Context(), fullNameOf(f.Owner, f.Name), principal(r))
	if err != nil {
		if review.IsUserError(err) {
			pages.ServeNewRepo(w, r, f, err.Error())
			return
		}
		fail(w, r, err)
		return
	}
	http.Redirect(w, r, repoLink(repo.GetName()), http.StatusSeeOther)
}

// forkForm asks where to fork a repository: the owner, and the fork's name — the upstream's.
func (s *Server) forkForm(w http.ResponseWriter, r *http.Request) {
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeNewRepo(w, r, forkForm(r, repo), "")
}

func forkForm(r *http.Request, repo *scmpb.Repository) pages.NewRepoForm {
	f := newRepoForm(r)
	f.Upstream, f.Action = repo.GetName(), repoLink(repo.GetName())+"/fork"
	f.Name = repo.GetName()[strings.LastIndexByte(repo.GetName(), '/')+1:]
	if len(f.Owners) > 1 {
		f.Owner = f.Owners[len(f.Owners)-1] // the person's own, by default
	}
	return f
}

// fork forks a repository for the principal into the owner they chose, then shows the fork.
func (s *Server) fork(w http.ResponseWriter, r *http.Request) {
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	f := forkForm(r, repo)
	f.Name = strings.TrimSpace(r.FormValue("name"))
	if o := r.FormValue("owner"); o != "" {
		f.Owner = o
	}
	if f.Name == "" {
		pages.ServeNewRepo(w, r, f, "A fork needs a name.")
		return
	}
	fork, err := s.review.ForkRepository(r.Context(), repo.GetName(), fullNameOf(f.Owner, f.Name), principal(r))
	if err != nil {
		if review.IsUserError(err) {
			pages.ServeNewRepo(w, r, f, err.Error())
			return
		}
		fail(w, r, err)
		return
	}
	http.Redirect(w, r, repoLink(fork.GetName()), http.StatusSeeOther)
}

// syncFork brings a fork its upstream's newer work, then shows it again.
func (s *Server) syncFork(w http.ResponseWriter, r *http.Request) {
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	if _, err := s.review.SyncFork(r.Context(), repo, principal(r)); err != nil {
		if review.IsUserError(err) {
			pages.ServeError(w, r, http.StatusConflict, "The fork was not synced", err.Error())
			return
		}
		fail(w, r, err)
		return
	}
	http.Redirect(w, r, repoLink(repo.GetName()), http.StatusSeeOther)
}

// star favorites a repository for the principal, or stops.
func (s *Server) star(on bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		repo, err := s.repository(r)
		if err != nil {
			fail(w, r, err)
			return
		}
		if err := s.db.SetFavorite(r.Context(), principal(r), repo.GetId(), on); err != nil {
			fail(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
