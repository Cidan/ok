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

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/control/db"
	pages "github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	"github.com/Cidan/ok/internal/control/webhooks"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/tenancy"
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
	// tenancy and inboxes are the directory's, for what spans cells: its search, and people's
	// inboxes.
	tenancy identitypb.TenancyClient
	inboxes identitypb.InboxClient
	notify  identitypb.NotificationsClient
	// hooks are repositories' webhooks; nil, or of no sealer, where the installation keeps none.
	hooks *webhooks.Hooks
}

// New builds the site over ok control's database, the SCM, ok automation's runs, the
// deployer's deployments, and review. installation is the installation's organization's handle,
// "" for a hosted service.
func New(d db.Store, c *scm.Client, runs automationpb.RunsClient, deploys deploypb.DeploymentsClient, r *review.Service, dir *people.Directory,
	identity grpc.ClientConnInterface, hooks *webhooks.Hooks, installation string) *Server {
	installationHandle = installation
	return &Server{db: d, scm: c, runs: runs, deploys: deploys, review: r, people: dir, changeCache: newChangeCache(),
		tenancy: identitypb.NewTenancyClient(identity), inboxes: identitypb.NewInboxClient(identity), notify: identitypb.NewNotificationsClient(identity),
		hooks: hooks}
}

// installationHandle is the installation's organization's handle, whose repositories' full names
// are their names alone; "" for a hosted service. New sets it, once.
var installationHandle string

// principal is the stable ID of the person a request is for: the caller ok's runtime resolved, a
// person.
func principal(r *http.Request) string {
	c, ok := authn.FromContext(r.Context())
	if !ok || !c.Human {
		return ""
	}
	return c.ID
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
		r.Get("/notifications", s.notifications)
		r.Post("/notifications/read", s.markNotificationsRead)
		r.Post("/notifications/preferences", s.setMailed)
	})
	// A repository is at /{owner}/{name} (docs/hosted.md, "Tenancy, as built"); an installation's
	// organization's at its bare name too, which redirects. Anyone reads a public one; what else
	// a person may do, Authorize decides.
	r.Group(func(r chi.Router) {
		r.Get("/{repo}", s.bareRepository)
		r.Route("/{owner}/{repo}", func(r chi.Router) {
			r.Use(s.loadRepository)
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
			r.Get("/fork", s.forkForm)
			r.Post("/fork", s.fork)
			r.Post("/action/sync-fork", s.syncFork)
			r.Post("/action/star", s.star(true))
			r.Post("/action/unstar", s.star(false))

			r.Get("/cls", s.clList)
			r.Get("/search", s.codeSearch)
			r.Group(func(r chi.Router) {
				r.Use(identify)
				r.Get("/webhooks", s.webhooksPage)
				r.Post("/webhooks", s.createWebhook)
				r.Post("/webhooks/{id}/delete", s.deleteWebhook)
				r.Post("/webhooks/deliveries/{id}/redeliver", s.redeliver)
			})
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
				r.Post("/run-pre", s.clRunPre)
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
	if m, ok := cells.MovedOf(err); ok {
		// Another cell holds the repository: the edge in front sends the request there.
		w.Header().Set(cells.Header, m.GetCell())
		pages.ServeError(w, r, http.StatusMisdirectedRequest, "Moved", "This repository is served from elsewhere now. Reload the page to reach it.")
		return
	}
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

// repoKey is the request context's key for the repository its path names.
type repoKey struct{}

// repository is the repository the path names, as loadRepository found it, fresh from the SCM.
func (s *Server) repository(r *http.Request) (*scmpb.Repository, error) {
	if repo, ok := r.Context().Value(repoKey{}).(*scmpb.Repository); ok {
		return repo, nil
	}
	return nil, status.Error(codes.NotFound, "no repository")
}

// fullNameOf is the full name a path's owner and name make: a name alone for the installation's
// organization's.
func fullNameOf(owner, name string) string {
	if owner == installationHandle && owner != "" {
		return name
	}
	return owner + "/" + name
}

// loadRepository finds the repository /{owner}/{repo} names for the handlers below it, as the
// person may see it (Authorize): one they cannot see answers as though there were none — to someone
// not signed in, by asking them to — and no redirect says otherwise. A name it had redirects a page
// to the one it has now; a path that names none, but whose first part is an installation's
// repository's bare name, redirects to the repository's path with the rest of it (/app/cls is
// /example/app/cls). Anyone reads a public repository; a form posted to one needs a person.
func (s *Server) loadRepository(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		owner, name := chi.URLParam(r, "owner"), chi.URLParam(r, "repo")
		full := fullNameOf(owner, name)
		target := ""
		repo, err := s.scm.Repositories.GetRepository(r.Context(), &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: full}})
		if status.Code(err) == codes.NotFound && installationHandle != "" {
			if bare, berr := s.scm.Repositories.GetRepository(r.Context(), &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: owner}}); berr == nil {
				repo, err = bare, nil
				target = repoLink(bare.GetName()) + strings.TrimPrefix(r.URL.Path, "/"+owner)
			}
		}
		if status.Code(err) == codes.NotFound {
			s.unseen(w, r)
			return
		}
		if err != nil {
			fail(w, r, err)
			return
		}
		d := s.review.Decide(r.Context(), repo, principal(r), authz.ReadCode)
		if !d.Visible {
			s.unseen(w, r)
			return
		}
		if target == "" && repo.GetName() != full {
			target = repoLink(repo.GetName()) + strings.TrimPrefix(r.URL.Path, "/"+owner+"/"+name)
		}
		if target != "" {
			redirectTo(w, r, target)
			return
		}
		if !d.Allowed {
			pages.ServeError(w, r, http.StatusNotFound, "Deleted", "This repository is deleted. Its admins may restore it for 30 days.")
			return
		}
		if r.Method != http.MethodGet && principal(r) == "" {
			pages.ServeError(w, r, http.StatusUnauthorized, "Not Signed In", "Sign in to ok, then try again.")
			return
		}
		// Its writes about the repository pass its move gate (move.WithRepository).
		ctx := move.WithRepository(context.WithValue(r.Context(), repoKey{}, repo), repo.GetId())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// readable reports whether the person a request is for may read a repository, by ID.
func (s *Server) readable(ctx context.Context, id int64) bool {
	who := ""
	if c, ok := authn.FromContext(ctx); ok && c.Human {
		who = c.ID
	}
	return s.review.May(ctx, &scmpb.Repository{Id: id}, who, authz.ReadCode)
}

// unseen answers a request for a repository the person cannot see, or there is none of: someone
// not signed in is asked to, as signing in may show it; anyone else is told nothing is here.
func (s *Server) unseen(w http.ResponseWriter, r *http.Request) {
	if principal(r) == "" {
		identify(http.NotFoundHandler()).ServeHTTP(w, r)
		return
	}
	pages.ServeError(w, r, http.StatusNotFound, "Not Found", "Nothing is here.")
}

// bareRepository is an installation's organization's repository at its bare name: it redirects to
// its path, for a person who may see it.
func (s *Server) bareRepository(w http.ResponseWriter, r *http.Request) {
	repo, err := s.scm.Repositories.GetRepository(r.Context(), &scmpb.GetRepositoryRequest{
		Key: &scmpb.GetRepositoryRequest_Name{Name: chi.URLParam(r, "repo")}})
	if status.Code(err) == codes.NotFound {
		s.unseen(w, r)
		return
	}
	if err != nil {
		fail(w, r, err)
		return
	}
	if !s.review.Decide(r.Context(), repo, principal(r), authz.ReadCode).Visible {
		s.unseen(w, r)
		return
	}
	redirectTo(w, r, repoLink(repo.GetName()))
}

// redirectTo sends a page's request to where it is now, its query kept — for now: a name a
// repository had answers until it is taken again — and a form's post, or a fragment's request, is
// refused as not found instead: it was for a page that moved.
func redirectTo(w http.ResponseWriter, r *http.Request, path string) {
	if r.Method != http.MethodGet || r.Header.Get("HX-Request") != "" {
		pages.ServeError(w, r, http.StatusNotFound, "Moved", "This repository has moved: reload the page.")
		return
	}
	if r.URL.RawQuery != "" {
		path += "?" + r.URL.RawQuery
	}
	http.Redirect(w, r, path, http.StatusFound)
}

// repoLink is a repository's URL path, /{owner}/{name}, from its full name.
func repoLink(full string) string { return tenancy.URLPath(full, installationHandle) }

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
		Subtitle:     "Created by " + people.HandleOf(repo.GetCreatedBy()),
		Link:         repoLink(name),
		CommitsCount: repo.GetHeadSeq(),
		CommitsHref:  repoLink(name) + "/commits",
		ChangesHref:  repoLink(name) + "/cls",
		Clone:        pages.CloneURLs{Command: "jj clone " + name},
	}
	if up := repo.GetParent(); up != "" {
		c.Upstream, c.UpstreamHref = up, repoLink(up)
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
	if h := people.EmailHandle(c.GetAuthor().GetEmail()); h != "" {
		return h
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
