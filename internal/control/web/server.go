// Package web is ok control's web site: the repository finder, the repository, file, blame,
// history, and commit pages, and ChangeLists — rendered by internal/control/pages from the SCM's
// Browse and ChangeLists services and ok control's review. Every request is a signed-in person's:
// ok's runtime resolves their session (ok identity's sign-in), and a page sends anyone else to sign
// in.
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/control/db"
	pages "github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	"github.com/Cidan/ok/internal/serve/authn"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Server serves ok control's pages.
type Server struct {
	db          db.Store
	scm         *scm.Client
	runs        automationpb.RunsClient
	deploys     deploypb.DeploymentsClient
	review      *review.Service
	people      *people.Directory
	changeCache *changeCache
}

// New builds the site over ok control's database, the SCM, ok automation's runs, the
// deployer's deployments, and review.
func New(d db.Store, c *scm.Client, runs automationpb.RunsClient, deploys deploypb.DeploymentsClient, r *review.Service, dir *people.Directory) *Server {
	return &Server{db: d, scm: c, runs: runs, deploys: deploys, review: r, people: dir, changeCache: newChangeCache()}
}

// principal is the person a request is for: the caller ok's runtime resolved, a person.
func principal(r *http.Request) string {
	c, ok := authn.FromContext(r.Context())
	if !ok || !c.Human {
		return ""
	}
	return c.Principal
}

// identify requires a signed-in person: a browser asking for a page is sent to sign in, and back
// here after; anything else is refused.
func identify(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if principal(r) != "" {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodGet && r.Header.Get("HX-Request") == "" {
			http.Redirect(w, r, "/id/login?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
			return
		}
		pages.ServeError(w, r, http.StatusUnauthorized, "Not Signed In", "Sign in to ok, then try again.")
	})
}

// Mount routes the site on r.
func (s *Server) Mount(r chi.Router) {
	assets := pages.Assets()
	r.Handle("/assets/*", assets)
	r.Handle("/favicon.svg", assets)
	r.Group(func(r chi.Router) {
		r.Use(identify)
		r.Get("/", s.home)
		r.Get("/repositories/list", s.homeList)
		r.Get("/repositories/rows", s.homeRows)
		r.Get("/repositories/new", s.newRepository)
		r.Get("/services", s.services)
		r.Get("/services/{id}", s.service)
		r.Get("/services/group/{name}", s.serviceGroup)
		r.Post("/repositories/new", s.createRepository)

		r.Route("/{repo}", func(r chi.Router) {
			r.Get("/", s.tree)
			r.Get("/tree/{rev}", s.tree)
			r.Get("/tree/{rev}/*", s.tree)
			r.Get("/blame/{rev}/*", s.blame)
			r.Get("/tree-children/{rev}/*", s.treeChildren)
			r.Get("/raw/{rev}/*", s.raw)
			r.Get("/commits", s.commits)
			r.Get("/commit/{id}", s.commit)
			r.Get("/commit/{id}/files", s.commitFiles)
			r.Get("/commit/{id}/file", s.commitFile)
			r.Post("/action/star", s.star(true))
			r.Post("/action/unstar", s.star(false))

			r.Get("/cls", s.clList)
			r.Route("/deploy/{id}", func(r chi.Router) {
				r.Get("/", s.deploymentPage)
				r.Get("/status", s.deploymentStatusFragment)
				r.Post("/retry", s.deploymentAction("retry"))
				r.Post("/halt", s.deploymentAction("halt"))
				r.Post("/allow", s.deploymentAction("allow"))
			})
			r.Route("/cl/{number}", func(r chi.Router) {
				r.Get("/", s.clView)
				r.Post("/reply", s.clReply)
				r.Post("/submit", s.clSubmit(false))
				r.Post("/submit-stack", s.clSubmit(true))
				r.Post("/request-review", s.clRequestReview)
				r.Post("/reviewers", s.clAddReviewers)
				r.Get("/reviewers/search", s.clReviewerSearch)
				r.Post("/reviewers/remove", s.clRemoveReviewer)
				r.Post("/draft", s.clReturnToDraft)
				r.Post("/abandon", s.clAbandon)
				r.Post("/restore", s.clRestore)
				r.Post("/rollback", s.clRollback)
				r.Post("/cancel-submit", s.clCancelSubmit)
				r.Post("/comment", s.clComment)
				r.Post("/comment/{id}/resolve", s.clResolve(true))
				r.Post("/comment/{id}/unresolve", s.clResolve(false))
				r.Post("/comment/{id}/delete", s.clDeleteComment)
				r.Get("/checks", s.clChecks)
				r.Get("/deployments", s.clDeploysTab)
				r.Get("/files", s.clFiles)
				r.Get("/file", s.clFile)
				r.Route("/run/{run}", func(r chi.Router) {
					r.Get("/", s.runPage)
					r.Get("/status", s.runStatus)
					r.Get("/log", s.runLog)
					r.Get("/raw", s.runRaw)
					r.Post("/rerun", s.runAction(true))
					r.Post("/cancel", s.runAction(false))
				})
			})
		})
	})
}

// Handler is the site with cross-origin protection — a state-changing request must come
// from ok's own pages — and every response under securityHeaders.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	s.Mount(r)
	return http.NewCrossOriginProtection().Handler(securityHeaders(r))
}

// pageCSP is every page's Content-Security-Policy: scripts, styles, and fonts from ok alone —
// the pages hold no inline script or style — images from ok or any HTTPS host (a README's
// badges), no plugins, no other site framing it, and forms posting to ok alone. A response that
// sets its own (raw bytes, under a sandbox) replaces it.
const pageCSP = "default-src 'self'; img-src 'self' https: data:; object-src 'none'; base-uri 'none'; " +
	"frame-ancestors 'none'; form-action 'self'"

// securityHeaders sets what every response carries: the policy above, framing refused for
// browsers before frame-ancestors, no sniffing a response's type, and no full URL — a CL's, a
// file path's — sent to the sites its links lead to.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", pageCSP)
		h.Set("X-Frame-Options", "DENY")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "strict-origin-when-cross-origin")
		next.ServeHTTP(w, r)
	})
}

// fail renders an error: a missing thing as not found, a refusal as it reads, and
// anything else as a failure, logged.
func fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, review.ErrNotFound) || status.Code(err) == codes.NotFound:
		pages.ServeError(w, r, http.StatusNotFound, "Not Found", "Nothing is here.")
	case review.IsUserError(err):
		pages.ServeError(w, r, http.StatusBadRequest, "Can't Do That", err.Error())
	case status.Code(err) == codes.InvalidArgument:
		pages.ServeError(w, r, http.StatusBadRequest, "Can't Show That", upperFirst(status.Convert(err).Message()))
	case errors.Is(err, context.Canceled):
	default:
		slog.Error("ok control: request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		pages.ServeError(w, r, http.StatusInternalServerError, "Something Went Wrong", "ok could not show this page. Try again in a moment.")
	}
}

// repository loads the repository the path names, fresh from the SCM.
func (s *Server) repository(r *http.Request) (*scmpb.Repository, error) {
	return s.scm.Repositories.GetRepository(r.Context(), &scmpb.GetRepositoryRequest{
		Key: &scmpb.GetRepositoryRequest_Name{Name: chi.URLParam(r, "repo")},
	})
}

// repoLink is a repository's URL path.
func repoLink(name string) string { return "/" + name }

// treeHref is a path's page at a revision: main, or a commit's hex ID.
func treeHref(repo, rev, p string) string {
	if p == "" {
		if rev == "main" {
			return repoLink(repo)
		}
		return repoLink(repo) + "/tree/" + rev
	}
	return repoLink(repo) + "/tree/" + rev + "/" + scm.PathEscape(p)
}

// treeChildrenHref is where the browser's tree fetches a folder's rows.
func treeChildrenHref(repo, rev, dir string) string {
	return repoLink(repo) + "/tree-children/" + rev + "/" + scm.PathEscape(dir)
}

func rawHref(repo, rev, p string) string {
	return repoLink(repo) + "/raw/" + rev + "/" + scm.PathEscape(p)
}

func blameHref(repo, rev, p string) string {
	return repoLink(repo) + "/blame/" + rev + "/" + scm.PathEscape(p)
}

func commitHref(repo string, id []byte) string {
	return repoLink(repo) + "/commit/" + scm.Hex(id)
}

func clHref(repo string, number int64) string {
	return fmt.Sprintf("%s/cl/%d", repoLink(repo), number)
}

// chrome is a repository page's shared header and toolbar. Its breadcrumb, shown below the root
// only in the browser, leads to the root — the landing, in place of the browser (#content) —
// and to each folder above the path, in the browser's pane (#browse-main).
func chrome(repo *scmpb.Repository, rev string, at []byte, p string) pages.RepoChrome {
	name := repo.GetName()
	c := pages.RepoChrome{
		Name:         name,
		Subtitle:     "Created by " + people.Handle(repo.GetCreatedBy()),
		Link:         repoLink(name),
		CommitsCount: repo.GetHeadSeq(),
		CommitsHref:  repoLink(name) + "/commits",
		ChangesHref:  repoLink(name) + "/cls",
		Clone:        pages.CloneURLs{Command: "jj clone " + name},
	}
	if rev != "main" {
		c.At = pages.CommitAt{Short: scm.Short(at), MainHref: treeHref(name, "main", p)}
	}
	c.Crumbs = []pages.Crumb{{Label: name, Href: treeHref(name, rev, ""), Target: "#content"}}
	if p != "" {
		parts := strings.Split(p, "/")
		for i, part := range parts {
			crumb := pages.Crumb{Label: part}
			if i < len(parts)-1 {
				crumb.Href, crumb.Target, crumb.Swap = treeHref(name, rev, strings.Join(parts[:i+1], "/")), "#browse-main", pages.PaneSwap
			}
			c.Crumbs = append(c.Crumbs, crumb)
		}
	} else {
		c.Crumbs[0].Href = ""
	}
	return c
}

// resolveRev reads a revision in a URL: main (its head), or a commit's full hex ID.
func resolveRev(repo *scmpb.Repository, rev string) ([]byte, bool) {
	if rev == "" || rev == "main" {
		return repo.GetHead(), true
	}
	return scm.ParseHex(rev)
}

// pathParam is the path after a route's revision, unescaped by the router.
func pathParam(r *http.Request) string {
	return strings.Trim(chi.URLParam(r, "*"), "/")
}

// Humanized times.

func age(t time.Time) (string, string) {
	title := t.UTC().Format("Jan 2, 2006 15:04 MST")
	d := time.Since(t)
	if d < 0 {
		d = 0
	}
	unit := func(n int, u string) string {
		if n == 1 {
			return fmt.Sprintf("1 %s ago", u)
		}
		return fmt.Sprintf("%d %ss ago", n, u)
	}
	switch {
	case d < time.Minute:
		return "just now", title
	case d < time.Hour:
		return unit(int(d.Minutes()), "minute"), title
	case d < 24*time.Hour:
		return unit(int(d.Hours()), "hour"), title
	case d < 30*24*time.Hour:
		return unit(int(d.Hours()/24), "day"), title
	case d < 365*24*time.Hour:
		return unit(int(d.Hours()/24/30), "month"), title
	default:
		return unit(int(d.Hours()/24/365), "year"), title
	}
}

func commitTime(c *scmpb.Commit) time.Time {
	return time.UnixMilli(c.GetCommitter().GetTimeMillis())
}

// commitTitle is a commit description's first line.
func commitTitle(c *scmpb.Commit) string {
	t, _, _ := strings.Cut(c.GetDescription(), "\n")
	t = strings.TrimSpace(t)
	if t == "" {
		return "(no description)"
	}
	return t
}

// commitAuthor is who wrote a commit: a person's handle, else their name.
func commitAuthor(c *scmpb.Commit) string {
	if p := people.FromEmail(c.GetAuthor().GetEmail()); p != "" {
		return people.Handle(p)
	}
	if n := c.GetAuthor().GetName(); n != "" {
		return n
	}
	return c.GetAuthor().GetEmail()
}

func humanSize(n uint64) string {
	switch {
	case n < 1<<10:
		return fmt.Sprintf("%d B", n)
	case n < 1<<20:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	case n < 1<<30:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	}
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
