package web

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/Cidan/ok/internal/control/db"
	pages "github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
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

// repoPage is one page of the finder: the principal's favorites first, then the most
// recently updated, and whether another page follows.
func (s *Server) repoPage(r *http.Request, keyword string, page int) ([]pages.RepoItem, bool, error) {
	rs, err := db.ListRepositories(r.Context(), s.db.Q(), principal(r), keyword, (page-1)*repoPageSize, repoPageSize+1)
	if err != nil {
		return nil, false, err
	}
	more := len(rs) > repoPageSize
	if more {
		rs = rs[:repoPageSize]
	}
	items := make([]pages.RepoItem, 0, len(rs))
	for _, repo := range rs {
		ago, title := age(repo.UpdatedAt)
		detail := fmt.Sprintf("%d %s · created by %s", repo.HeadSeq, plural(repo.HeadSeq, "commit", "commits"), people.Handle(repo.CreatedBy))
		items = append(items, pages.RepoItem{
			Name:         repo.Name,
			Detail:       detail,
			Link:         repoLink(repo.Name),
			Starred:      repo.Favorite,
			UpdatedUnix:  repo.UpdatedAt.Unix(),
			UpdatedAgo:   ago,
			UpdatedTitle: title,
		})
	}
	return items, more, nil
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
	items, more, err := s.repoPage(r, keyword, 1)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeHome(w, r, people.Handle(principal(r)), keyword, items, rowsURL(keyword), repoPageSize, more)
}

// homeList is the finder's list region for a search, which the search box swaps in.
func (s *Server) homeList(w http.ResponseWriter, r *http.Request) {
	keyword := strings.TrimSpace(r.URL.Query().Get("q"))
	items, more, err := s.repoPage(r, keyword, 1)
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
	items, _, err := s.repoPage(r, strings.TrimSpace(r.URL.Query().Get("q")), page)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeRepoRows(w, r, items)
}

func (s *Server) newRepository(w http.ResponseWriter, r *http.Request) {
	pages.ServeNewRepo(w, r, pages.NewRepoForm{}, "")
}

// createRepository creates a repository on the SCM for the principal, then shows it.
func (s *Server) createRepository(w http.ResponseWriter, r *http.Request) {
	f := pages.NewRepoForm{Name: strings.TrimSpace(r.FormValue("name"))}
	if f.Name == "" {
		pages.ServeNewRepo(w, r, f, "A repository needs a name.")
		return
	}
	repo, err := s.review.CreateRepository(r.Context(), f.Name, principal(r))
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

// star favorites a repository for the principal, or stops.
func (s *Server) star(on bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		repo, err := s.repository(r)
		if err != nil {
			fail(w, r, err)
			return
		}
		if err := db.SetFavorite(r.Context(), s.db.Q(), principal(r), repo.GetId(), on); err != nil {
			fail(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
