// Package pages is ok control's web UI: templ pages over the shared shell (internal/web: layout,
// the navigation rail, and the hydration runtime), with the pages' own assets beside them and
// /assets serving. The handlers that fill them are internal/control/web.
package pages

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/a-h/templ"

	"github.com/Cidan/ok/internal/web/breadcrumb"
	"github.com/Cidan/ok/internal/web/layout"
	"github.com/Cidan/ok/internal/web/nav"
	"github.com/Cidan/ok/internal/web/static"
)

// ok's head assets, served by Assets() at /assets. layout.Shell is app-neutral, so ok
// lists exactly what its pages need: htmx (SPA content swaps), the hydration runtime, nav.js
// (the rail drawer), infinite.js (the repository list), livefilter.js (the search box),
// clipboard.js (the clone-URL copy buttons), and tree.js (the browser's file tree), plus its
// fonts and Tailwind output. No
// chat/figure/markdown/api.js — ok renders markup server-side.
var styles = []string{"/assets/fonts.css", "/assets/app.css"}

var scripts = []string{"/assets/htmx.min.js", "/assets/hydrate.js", "/assets/nav.js", "/assets/infinite.js", "/assets/livefilter.js", "/assets/stars.js", "/assets/clcomments.js", "/assets/clipboard.js", "/assets/tree.js"}

func shell(title string, variant layout.Variant, n nav.Model) layout.Props {
	return layout.Props{
		Title:   title,
		Variant: variant,
		Nav:     n,
		Favicon: "/assets/favicon.svg",
		Styles:  styles,
		Scripts: scripts,
		// Every page renders the whole document to a history restore (hxNav).
		HistoryRestore: true,
	}
}

// Brand is what the pages call the installation: in the navigation rail and every title. The
// installation's configuration names it (installation); set it before serving.
var Brand = "ok"

// NavModel builds the left rail for the given request path (active-state prefix match).
func NavModel(current string) nav.Model {
	return nav.Model{
		Brand: nav.Item{Label: Brand, Href: "/", Icon: "bolt"},
		Actions: []nav.Item{
			{Label: "Repositories", Href: "/", Icon: "repo"},
			{Label: "Services", Href: "/services", Icon: "server"},
		},
		Current:          current,
		HighlightActions: true,
	}
}

// RepoItem is one repository row on the home/repo-finder list. Link is the repo's URL path;
// Detail its size and creator; Starred whether the signed-in person has favorited it;
// UpdatedUnix (when main last moved) drives the client-side favorites-first reorder.
type RepoItem struct {
	Name         string
	Detail       string
	Link         string
	Starred      bool
	UpdatedUnix  int64
	UpdatedAgo   string
	UpdatedTitle string
}

// InboxItem is a CL in the signed-in person's inbox (docs/hosted.md, "ok control at scale, as
// built"), whichever cell holds it.
type InboxItem struct {
	Repository string
	Number     int64
	Title      string
	// Role is "author" or "reviewer"; Attention, that the CL waits on them.
	Role         string
	Attention    bool
	Status       string
	Link         string
	UpdatedAgo   string
	UpdatedTitle string
}

// NotificationItem is one of a person's notifications.
type NotificationItem struct {
	Who, Text         string
	Repository, Title string
	Number            int64
	Link              string
	Read              bool
	Ago, AgoTitle     string
}

// MailKind is a kind of notification, and whether it is mailed.
type MailKind struct {
	Value, Label string
	Mailed       bool
}

// NotificationsView is a person's notifications page.
type NotificationsView struct {
	Items  []NotificationItem
	Unread int64
	Kinds  []MailKind
}

// ServeNotifications renders a person's notifications.
func ServeNotifications(w http.ResponseWriter, r *http.Request, v NotificationsView) {
	if hxNav(r) {
		render(w, r, NotificationsContent(v))
		return
	}
	render(w, r, NotificationsPage(NavModel(r.URL.Path), v))
}

// SearchView is a repository's code search page.
type SearchView struct {
	Chrome                RepoChrome
	Action                string
	Query, Path           string
	Regexp, CaseSensitive bool
	// Notice says the index is being built, or older than main; Error, the query is not one.
	Notice, Error string
	Files         []SearchFile
	Truncated     bool
	Shown         int
}

// SearchFile is a file's lines found.
type SearchFile struct {
	Path, Href string
	Lines      []SearchLine
}

// SearchLine is a line found: its number, its link, and its text around the match.
type SearchLine struct {
	Number               int
	Href                 string
	Before, Match, After string
}

// ServeSearch renders a repository's code search.
func ServeSearch(w http.ResponseWriter, r *http.Request, v SearchView) {
	if hxNav(r) {
		render(w, r, SearchContent(v))
		return
	}
	render(w, r, SearchPage(NavModel(r.URL.Path), v))
}

// WebhooksView is a repository's webhooks page.
type WebhooksView struct {
	Chrome  RepoChrome
	Enabled bool
	// Created is a webhook just made, whose secret is shown this once.
	Created    *NewWebhook
	Error      string
	Hooks      []WebhookItem
	CreateHref string
}

// NewWebhook is a webhook just made.
type NewWebhook struct{ URL, Secret string }

// WebhookItem is one of a repository's webhooks.
type WebhookItem struct {
	URL, Events, CreatedBy, Ago string
	DeleteHref                  string
	Deliveries                  []DeliveryItem
}

// DeliveryItem is one of a webhook's latest deliveries: its state (waiting, delivered, dead), event,
// and what the endpoint last answered.
type DeliveryItem struct {
	State, Event, Detail, Ago string
	RedeliverHref             string
}

// ServeWebhooks renders a repository's webhooks.
func ServeWebhooks(w http.ResponseWriter, r *http.Request, v WebhooksView) {
	if hxNav(r) {
		render(w, r, WebhooksContent(v))
		return
	}
	render(w, r, WebhooksPage(NavModel(r.URL.Path), v))
}

// ServeHome renders the home repo-finder: the bare content fragment on an htmx navigation, the
// full document otherwise. user is the signed-in person's handle; unread their unread
// notifications; inbox their CLs under way;
// keyword is the active search filter; page1 is the first batch of rows; url is the ?page=N rows
// endpoint (carrying the keyword); hasMore says whether page 2 exists.
func ServeHome(w http.ResponseWriter, r *http.Request, user string, unread int64, inbox []InboxItem, keyword string, page1 []RepoItem, url string,
	pageSize int, hasMore bool) {
	if hxNav(r) {
		render(w, r, HomeContent(user, unread, inbox, keyword, page1, url, pageSize, hasMore))
		return
	}
	render(w, r, HomePage(NavModel(r.URL.Path), user, unread, inbox, keyword, page1, url, pageSize, hasMore))
}

// ServeHomeList renders just the repository list region — the fragment the search box (livefilter)
// swaps into #repo-list for a keyword.
func ServeHomeList(w http.ResponseWriter, r *http.Request, keyword string, page1 []RepoItem, url string, pageSize int, hasMore bool) {
	render(w, r, HomeList(keyword, page1, url, pageSize, hasMore))
}

// NewRepoForm holds the create-repository form's field, echoed back on a validation error so
// the input is preserved.
type NewRepoForm struct {
	// Owners are whom the person may make a repository of — the installation's organization,
	// first, then themselves — and Owner the one chosen.
	Owners []string
	Owner  string
	// Organization is the installation's organization's handle; "" for a hosted service.
	Organization string
	Name         string
	// Upstream, for a fork, is the repository forked, and Action where the form posts.
	Upstream, Action string
}

// formAction is where the form posts: a fork's own action, or a new repository's.
func formAction(f NewRepoForm) string {
	if f.Action != "" {
		return f.Action
	}
	return "/repositories/new"
}

// ServeNewRepo renders the create-repository form: the bare fragment on an htmx navigation,
// the full document otherwise. errMsg (when non-empty) shows a validation/creation error.
func ServeNewRepo(w http.ResponseWriter, r *http.Request, f NewRepoForm, errMsg string) {
	if hxNav(r) {
		render(w, r, NewRepoContent(f, errMsg))
		return
	}
	render(w, r, NewRepoPage(NavModel(r.URL.Path), f, errMsg))
}

// ServeError renders an error page with its status: the content region on an htmx navigation
// (htmx swaps every status but 204 and 304), the full document otherwise.
func ServeError(w http.ResponseWriter, r *http.Request, status int, title, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if hxNav(r) {
		render(w, r, ErrorContent(title, message))
		return
	}
	render(w, r, ErrorPage(NavModel(r.URL.Path), title, message))
}

// ServeRepoRows renders just a batch of repository rows — the fragment the infinite-scroll
// controller fetches for each page.
func ServeRepoRows(w http.ResponseWriter, r *http.Request, repos []RepoItem) {
	render(w, r, RepoRows(repos))
}

// Crumb is a path-breadcrumb segment, re-exported so handlers build the repo chrome without
// importing //web directly (the alias keeps it identical to breadcrumb.Crumb).
type Crumb = breadcrumb.Crumb

// CloneURLs is how a repository is cloned: the jj command (ok's jj is the only client).
type CloneURLs struct {
	Command string
}

// CommitAt names the commit a repository page shows: empty for main's head, else the commit's
// short ID and a link to the same path on main.
type CommitAt struct {
	Short    string
	MainHref string
}

// RepoChrome is the shared repo-view header/toolbar model: identity (the name, the subtitle),
// the clone command, the commit being browsed, the path breadcrumb, and main's commit count.
type RepoChrome struct {
	Name     string
	Subtitle string // the header subtitle (who created it)
	// Upstream is a fork's upstream, as the directory names it now, and UpstreamHref its page;
	// ForkHref is where the person forks it, and SyncAction where they sync a fork they may write
	// to: each "" when there is none, or they may not.
	Upstream, UpstreamHref string
	ForkHref, SyncAction   string
	// WebhooksHref is its webhooks' page, for its administrators; "" for anyone else.
	WebhooksHref string
	Link         string // the repo's root URL path
	CommitsCount int64
	CommitsHref  string // main's history (/commits)
	ChangesHref  string // the ChangeList (CL) finder for this repo (/cls)
	Clone        CloneURLs
	At           CommitAt
	Crumbs       []Crumb
}

// EntryItem is one row in a directory listing: a file or directory with its last-touching
// commit summary and age.
type EntryItem struct {
	Name       string
	IsDir      bool
	IsLink     bool
	Href       string
	CommitMsg  string
	CommitHref string
	Age        string
	AgeTitle   string
}

// LatestCommit is the "latest commit" bar above a directory listing. Present is false for an
// empty repository.
type LatestCommit struct {
	Present  bool
	Author   string
	Message  string
	Href     string
	Age      string
	AgeTitle string
}

// ReadmeView is a discovered, server-rendered README shown below the directory listing. HTML is
// already sanitized markup (rendered by ok control's markup rendering), injected verbatim.
type ReadmeView struct {
	Name string
	HTML string
}

// RepoView is the directory view: the shared chrome, the entries + latest-commit bar, and an
// optional README. IsEmpty renders the quick-start state instead of a listing.
type RepoView struct {
	Chrome  RepoChrome
	Entries []EntryItem
	Latest  LatestCommit
	Readme  *ReadmeView
	IsEmpty bool
}

// FileBlob is a single file's rendered content. Mode selects the presentation:
//   - "markup": HTML holds sanitized rendered markup (README/markdown/etc.).
//   - "source": Lines holds server-highlighted (chroma) HTML, one per source line.
//   - "image":  ImageSrc is the raw image URL.
//   - "" (default): unrenderable/too-large — offer the raw download.
type FileBlob struct {
	Name        string
	SizeHuman   string
	RawHref     string
	HistoryHref string // the commits on main that changed the file
	BlameHref   string // the file's blame ("" when it has none: binary, too large)
	LexerName   string
	Mode        string
	HTML        string
	Lines       []string
	ImageSrc    string
}

// ServeRepoView renders a repository's landing — its root, centered. Three render modes: the
// #repo-main fragment for an in-repo swap (HX-Target=repo-main — header + rail stay put), the
// whole content region for a generic htmx navigation (HX-Request), and the full document for a
// deep link or a history restore.
func ServeRepoView(w http.ResponseWriter, r *http.Request, v RepoView) {
	if hxTargetIs(r, "repo-main") {
		render(w, r, RepoViewMain(v))
		return
	}
	if hxNav(r) {
		render(w, r, RepoViewContent(v))
		return
	}
	render(w, r, RepoViewPage(NavModel(r.URL.Path), v))
}

// TreeNode is one entry of the browser's file tree. A folder is Open when its children are
// rendered with it (the path being browsed runs through it); one that is not carries
// ChildrenHref, where the tree fetches its rows the first time it is opened, into the list
// named ChildrenID.
type TreeNode struct {
	Name         string
	Path         string // repository-relative
	IsDir        bool
	IsLink       bool
	Depth        int // 0 for the root's entries
	Href         string
	Current      bool // the page the browser shows
	Open         bool
	Children     []TreeNode
	ChildrenHref string
	ChildrenID   string
}

// BrowseDir is a directory in the browser's pane: its entries, the latest commit that changed
// it, its README, and its parent (ParentHref; ParentRoot when the parent is the repository's
// root, the landing).
type BrowseDir struct {
	Entries    []EntryItem
	Latest     LatestCommit
	Readme     *ReadmeView
	ParentHref string
	ParentRoot bool
}

// BrowseBlame is a file's blame in the browser's pane. Message, when set, says why there is no
// blame to show (a binary or too large file).
type BrowseBlame struct {
	FileHref string
	Message  string
	Lines    []BlameLine
}

// BrowseView is the repository browser: the shared chrome, the file tree (the root's entries,
// with every folder on Path open), and what the pane shows for Path — Kind "dir", "file", or
// "blame", with Dir, File, or Blame set to match.
type BrowseView struct {
	Chrome RepoChrome
	Tree   []TreeNode
	Path   string
	Kind   string
	Dir    *BrowseDir
	File   *FileBlob
	Blame  *BrowseBlame
}

// Browser pane kinds.
const (
	BrowseDirKind   = "dir"
	BrowseFileKind  = "file"
	BrowseBlameKind = "blame"
)

// ServeBrowse renders the repository browser, at the smallest region the request swaps: the
// pane alone (#browse-main) when navigating within the browser — the header and the tree stay
// put — the whole content region on any other htmx navigation (entering the browser from the
// landing or another page), and the full document on a deep link or a history restore.
func ServeBrowse(w http.ResponseWriter, r *http.Request, v BrowseView) {
	if hxTargetIs(r, "browse-main") {
		render(w, r, RepoBrowseMain(v))
		return
	}
	if hxNav(r) {
		render(w, r, RepoBrowseContent(v))
		return
	}
	render(w, r, RepoBrowsePage(NavModel(r.URL.Path), v))
}

// ServeTreeChildren renders a folder's rows in the browser's tree: what the tree fetches the
// first time the folder is opened.
func ServeTreeChildren(w http.ResponseWriter, r *http.Request, nodes []TreeNode) {
	render(w, r, treeNodes(nodes))
}

// CommitRow is one entry in main's history: the commit's identity (short and full ID), its
// author and age, the subject line, the CL it landed from, and the two link targets — Href to
// the commit's diff, and BrowseHref to browse the repository tree at that commit.
type CommitRow struct {
	SHA        string
	ShortSHA   string
	Href       string
	BrowseHref string
	Author     string
	Title      string
	Age        string
	AgeTitle   string
	CL         int64
	CLHref     string
}

// CommitsView is main's history: the shared chrome, an optional path filter (file history),
// the commit rows, main's commit count, and the newer/older page links.
type CommitsView struct {
	Chrome   RepoChrome
	FilePath string // non-empty on a file-history view
	Commits  []CommitRow
	Count    int64
	PrevHref string
	NextHref string
}

// ServeCommits renders the commit-history page: the #repo-main fragment on an in-repo swap, the
// content region on a generic htmx navigation, the full document on a deep link.
func ServeCommits(w http.ResponseWriter, r *http.Request, v CommitsView) {
	if hxTargetIs(r, "repo-main") {
		render(w, r, CommitsMain(v))
		return
	}
	if hxNav(r) {
		render(w, r, CommitsContent(v))
		return
	}
	render(w, r, CommitsPage(NavModel(r.URL.Path), v))
}

// ParentRef is one parent of a commit: its short SHA and the link to its diff.
type ParentRef struct {
	ShortSHA string
	Href     string
}

// DiffLineRow is one rendered line of a file diff. Type is "add" | "del" | "same" | "tag" (a hunk
// header). LeftIdx/RightIdx are the 1-based old/new line numbers (0 = no line on that side).
// Content is already-sanitized, syntax-highlighted HTML (chroma tokens + intra-line word-diff
// spans), injected verbatim.
type DiffLineRow struct {
	Type     string
	LeftIdx  int
	RightIdx int
	Content  string
	// Threads are the inline review threads anchored to THIS row (either side — each thread
	// carries its own Side). The UI pass renders them below the row.
	Threads []CommentThread
}

// CommentView is one comment inside an inline review thread: its id, author display name, body,
// humanized age, and whether the signed-in user may edit it (author-only).
type CommentView struct {
	ID       int64
	Author   string
	Body     string
	Age      string
	AgeTitle string
	CanEdit  bool
}

// CommentThread is one inline review conversation on a CL's diff: the thread-root id, its anchor
// on the viewed patchset (FilePath + Side "left"/"right" + Line, 0 = the file as a whole),
// resolution state, and its comments (root first, then replies oldest-first). PatchSet is the
// patchset it was written on (PatchSetHref views it). A thread from an earlier patchset that is
// still unresolved is ported onto the viewed one: Port says how its position survived — "exact"
// (same content), "changed" (its line was modified or removed; it sits at the nearest line),
// "deleted" (the file is gone), or "unknown" (its line couldn't be followed) — and
// OrigFilePath/OrigLine are where it was written. Any other thread from another patchset is
// Outdated and appears only on the Comments tab. The *Href fields are the POST targets for
// replying to / resolving / unresolving the thread.
type CommentThread struct {
	RootID        int64
	FilePath      string
	Side          string // "left" | "right"
	Line          int
	Resolved      bool
	Outdated      bool
	PatchSet      int64
	PatchSetHref  string
	Port          string // "" (not ported) | "exact" | "changed" | "deleted" | "unknown"
	OrigFilePath  string
	OrigLine      int
	Comments      []CommentView
	ReplyHref     string
	ResolveHref   string
	UnresolveHref string
}

// DiffFileView is one file's diff: its path (and old path on a rename), a status label, the
// per-file addition/deletion counts, and the diff body split into hunks. Binary, generated,
// or too-large files carry no body — the corresponding flag drives a placeholder row instead.
// Threads are the review threads on the file as a whole (a CL's file-level threads, and ported
// threads whose line can't be shown). OffDiff marks a file the viewed diff doesn't show, listed
// only for its review threads: Status is then "unmodified" (its body is the lines around each
// thread), "removed" (the file no longer exists), or the change the diff left unshown.
type DiffFileView struct {
	Name      string
	OldName   string
	Anchor    string // stable id for in-page links
	Status    string // "added" | "modified" | "deleted" | "renamed" | "copied" | "unmodified" | "removed"
	Addition  int
	Deletion  int
	IsBinary  bool
	Collapsed bool // generated/vendored — hidden body by default
	TooLarge  bool // exceeded the per-file line cap
	Sections  [][]DiffLineRow
	Threads   []CommentThread
	OffDiff   bool
	// A file list's row holds no diff: BodyHref is where its diff loads from when the row is
	// opened, Open whether it starts open, and ThreadCount the review threads on the file.
	BodyHref    string
	Open        bool
	ThreadCount int
}

// CommitDiffView is the single-commit view: the shared chrome, the commit header (SHA, subject,
// body, author, parents), the diff short-stat, and the per-file diffs. Incomplete is true when the
// diff hit the max-files cap and not every file is shown.
type CommitDiffView struct {
	Chrome   RepoChrome
	SHA      string
	ShortSHA string
	ChangeID string // jj's short change ID
	CL       int64  // the CL it landed from (0: none)
	CLHref   string
	Title    string
	Body     string
	Author   string
	Age      string
	AgeTitle string
	Parents  []ParentRef
	NumFiles int
	// Addition and Deletion total the diff's lines when TotalsKnown: a diff of many files
	// counts its lines per file only.
	Addition    int
	Deletion    int
	TotalsKnown bool
	// Files is the file list's first page; MoreFilesHref loads the next ("" when none).
	Files         []DiffFileView
	MoreFilesHref string
}

// ServeCommitDiff renders a single commit's diff: the #repo-main fragment on an in-repo swap, the
// content region on a generic htmx navigation, the full document on a deep link.
func ServeCommitDiff(w http.ResponseWriter, r *http.Request, v CommitDiffView) {
	if hxTargetIs(r, "repo-main") {
		render(w, r, CommitDiffMain(v))
		return
	}
	if hxNav(r) {
		render(w, r, CommitDiffContent(v))
		return
	}
	render(w, r, CommitDiffPage(NavModel(r.URL.Path), v))
}

// ServeFileRows renders a page of a file list's rows after the first (its next page's loader
// last), for a CL (comments on) or a commit.
func ServeFileRows(w http.ResponseWriter, r *http.Request, rows []DiffFileView, next string) {
	render(w, r, FileRows(rows, next))
}

// ServeCLFileBody renders a CL's file's diff, with its review threads: what its row loads when
// opened.
func ServeCLFileBody(w http.ResponseWriter, r *http.Request, f DiffFileView, commentHref string, canComment bool) {
	render(w, r, CLFileBody(f, commentHref, canComment))
}

// ServeFileBody renders a commit's file's diff: what its row loads when opened.
func ServeFileBody(w http.ResponseWriter, r *http.Request, f DiffFileView) {
	render(w, r, FileBody(f))
}

// BlameLine is one line of a blame: its number, highlighted HTML, and the commit that last
// changed it — shown (First) at the start of each run of lines that commit changed.
type BlameLine struct {
	Number      int
	HTML        string
	First       bool
	CommitShort string
	CommitTitle string
	CommitHref  string
	Author      string
	Age         string
	AgeTitle    string
}

// CLItem is one ChangeList row on the CL finder: its number, subject, author, status, and age.
type CLItem struct {
	Number      int64
	Title       string
	Author      string
	StatusLabel string // "Open" | "Merged" | "Abandoned" | "Draft" | "Approved"
	StatusKind  string // style key: "open" | "merged" | "abandoned" | "draft" | "approved"
	Rollback    bool   // the CL is a rollback
	Age         string
	AgeTitle    string
	Href        string
}

// CLFilter is one finder view tab (Open, Drafts, Needs My Review, Merged, Abandoned, All).
type CLFilter struct {
	Label  string
	Href   string
	Active bool
}

// CLListView is the repo's ChangeList finder: the shared chrome, the view tabs, and the CL rows of
// the active view.
type CLListView struct {
	Chrome  RepoChrome
	Filters []CLFilter
	Empty   string // the active view's empty-state text
	Items   []CLItem
}

// ServeCLList renders the CL finder: the #repo-main fragment on an in-repo swap, the content
// region on a generic htmx nav, the full document on a deep link.
func ServeCLList(w http.ResponseWriter, r *http.Request, v CLListView) {
	if hxTargetIs(r, "repo-main") {
		render(w, r, CLListMain(v))
		return
	}
	if hxNav(r) {
		render(w, r, CLListContent(v))
		return
	}
	render(w, r, CLListPage(NavModel(r.URL.Path), v))
}

// CLReviewer is one reviewer chip in Change Info: a name, the labels they currently grant, and the
// POST target that removes them ("" when the viewer can't).
type CLReviewer struct {
	Name       string
	Labels     []string
	RemoveHref string
}

// CLCandidate is one person offered by the Request Review / Add Reviewers dialog: a checkbox,
// pre-checked when Selected; an Assigned reviewer is shown checked and disabled. Principal is
// the value the form posts; Name how they are shown.
type CLCandidate struct {
	Principal string
	Name      string
	Selected  bool
	Assigned  bool
}

// CLSuggestSet is one owner set still needing Approval in the review dialog, with the owners whose
// Approval would count.
type CLSuggestSet struct {
	Label      string
	Candidates []CLCandidate
}

// CLReviewDialog is the Request Review (a draft) / Add Reviewers (an open CL) dialog: per owner set
// the owners who can approve it, recent reviewers and editors of the changed paths, a live search for
// anyone else, and an optional message.
type CLReviewDialog struct {
	Title      string // "Request Review" | "Add Reviewers"
	Action     string // the send button's label
	Href       string // POST target
	SearchHref string // GET target of the live reviewer search
	OwnerSets  []CLSuggestSet
	Recent     []CLCandidate
	NeedsLGTM  bool // nobody chosen can give a counting LGTM yet
}

// CLSubmitReq is one submit-requirement row (a label the CL must have granted to submit): the
// label, whether it's satisfied, who grants it (LGTM), or a summary of its owner sets (Approval).
type CLSubmitReq struct {
	Label     string
	Satisfied bool
	GrantedBy []string
	Detail    string
}

// CLOwnerSet is one owner set governing part of a CL (the approval popover): the governing OWNERS
// (Label/Href; Href "" when no OWNERS applies), who may approve, the changed files it governs, and
// how it is satisfied (an Approval grant, or implicitly because the author is a listed owner).
type CLOwnerSet struct {
	Label  string
	Href   string
	Owners []string
	// NumFiles is how many of the CL's files the set governs, and FilesTitle names them — the
	// first few, when there are many.
	NumFiles   int
	FilesTitle string
	// What is, for an ACL a part of a change to the groups-and-ACLs tree needs a member of, that part
	// ("create deploy.acl.example.com"), shown in place of the files.
	What       string
	Satisfied  bool
	Implicit   bool
	AllowSelf  bool
	ApprovedBy []string
}

// CLPatchSet is one entry in the patchset range selector.
type CLPatchSet struct {
	Number      int64
	CommitShort string
	Href        string
	Current     bool
}

// CLRelated is one CL in this CL's stack (the parent→child chain), shown in "Related changes".
type CLRelated struct {
	Number  int64
	Title   string
	Href    string
	Current bool
}

// CLDeployment is a deployment on a CL's page: its deploy, the workload it rolls, where it
// stands, and its page.
type CLDeployment struct {
	ID          int64
	Name        string
	Workload    string // "deployment web/web"; "" for one that only migrates
	StatusLabel string
	StatusKind  string // deployStatusTone's key
	Unfinished  bool   // waiting or running (a halting one too)
	Stage       string // where a running one is: "Pushing", "Canary", "Waiting for an Allowance", …
	Reason      string
	Href        string
	Age         string
	AgeTitle    string
}

// DeploymentView is a deployment's page: what it deploys, where it stands, what it pushed and
// migrated, its roll, its log, and the actions the viewer may take.
type DeploymentView struct {
	Chrome      RepoChrome
	CLNumber    int64  // the CL the viewer came from (0: none)
	CLHref      string // its page
	ID          int64
	Name        string
	Deploy      string // its identity, //<file>:deploy:<name>
	Commit      string // short
	CommitHref  string
	Why         string
	Wave        string // "1 of plan 12", "a retry of deployment 9"
	Workload    string
	RequestedBy string
	StatusLabel string
	StatusKind  string
	Stage       string
	Reason      string
	Roll        string // "Canary · green beside 3 blue", "" before it rolls
	Started     string
	Finished    string
	Live        bool   // it has not finished: its status refreshes itself
	StatusHref  string // the status panel's fragment
	Images      []DeploymentImage
	Migrations  []DeploymentMigration
	Events      []DeploymentEvent
	// Actions ("" when the viewer may not take them in its state).
	RetryHref string
	HaltHref  string
	AllowHref string
	Notice    string
}

// DeploymentImage is a container a deployment rolls.
type DeploymentImage struct {
	Container string
	Target    string
	Reference string // "" until pushed
	Run       int64  // ok automation's push run (0: not started)
}

// DeploymentMigration is a migration a deployment applies: its plan, as the migrate protocol reported it.
type DeploymentMigration struct {
	Target      string
	StatusLabel string
	StatusKind  string
	Steps       []MigrationStep
	Refused     []string
	Allowed     []string
	AllowedBy   string
	Output      string
}

// MigrationStep is one step of a migration's plan.
type MigrationStep struct {
	Description string
	SQL         string
	Hazards     []MigrationHazard
}

// MigrationHazard is a step's hazard; a gated one waits for a repository admin to allow it.
type MigrationHazard struct {
	Class  string
	Detail string
	Gated  bool
}

// DeploymentEvent is one line of a deployment's log.
type DeploymentEvent struct {
	At      string
	AtTitle string
	Message string
}

// ServeDeploymentView renders a deployment's page.
func ServeDeploymentView(w http.ResponseWriter, r *http.Request, v DeploymentView) {
	if hxTargetIs(r, "repo-main") {
		render(w, r, DeploymentViewMain(v))
		return
	}
	if hxNav(r) {
		render(w, r, DeploymentViewContent(v))
		return
	}
	render(w, r, DeploymentViewPage(NavModel(r.URL.Path), v))
}

// ServeDeploymentStatus renders a deployment page's status panel, which refreshes itself until
// the deployment is over.
func ServeDeploymentStatus(w http.ResponseWriter, r *http.Request, v DeploymentView) {
	render(w, r, deploymentStatus(v))
}

// ServicesView is the Services page: every group (groups.go), with its status. Error says the
// deployer could not be asked.
type ServicesView struct {
	Groups []ServiceGroup
	Error  string
}

// ServiceView is a service, as its group's page shows it: its status, what ok deployed, what
// runs — every pod, and the image each container actually runs — and its recent deployments.
type ServiceView struct {
	ID             int64
	Name           string
	Deploy         string
	Repository     string
	RepositoryHref string
	Workload       string
	Declared       bool
	StatusLabel    string
	StatusKind     string
	Reason         string
	Found          bool   // the workload exists
	Live           string // a Deployment's live copy; "" for a StatefulSet
	Ready          int32
	Replicas       int32
	Deployed       *ServiceDeployment // nil: never deployed
	Latest         *ServiceDeployment // the latest deployment, when it is not the deployed one
	Images         []ServiceImage
	Pods           []ServicePod
	Recent         []ServiceDeployment
}

// ServiceDeployment is a deployment of a service. Service names it, in a group's deployments.
type ServiceDeployment struct {
	ID          int64
	Service     string
	Href        string
	StatusLabel string
	StatusKind  string // deployStatusTone's key
	Stage       string
	Reason      string
	Commit      string
	CommitHref  string
	CL          int64
	CLHref      string
	Age         string
	AgeTitle    string
}

// ServiceImage is a container: the image ok deployed, and the image its live copy runs.
type ServiceImage struct {
	Container string
	Deployed  string // "" when ok has not deployed it
	Running   string
	Matches   bool
}

// ServicePod is one of a service's pods, and what each of its containers runs.
type ServicePod struct {
	Name        string
	Copy        string // blue or green; "" for a StatefulSet's
	Ready       bool
	Restarts    int32
	Reason      string
	Images      []ServicePodImage
	Terminating bool
	Age         string
	AgeTitle    string
}

// ServicePodImage is what a pod's container runs, and whether it is what ok deployed.
type ServicePodImage struct {
	Container string
	Image     string
	Matches   bool // "" deployed images count as matching
}

// ServiceGroupView is a group's page: the group, and what its pane shows — the overview (Selected
// 0: its services, and Recent, its latest deployments) or one of its services (Service). Error,
// on a live region's refresh, says the deployer could not be asked; nothing else is set.
type ServiceGroupView struct {
	Group    ServiceGroup
	Selected int64
	Service  *ServiceView
	Recent   []ServiceDeployment
	Error    string
}

// Href is what the page shows: the group's overview, or the service selected.
func (v ServiceGroupView) Href() string {
	if v.Selected != 0 {
		return ServiceHref(v.Selected)
	}
	return v.Group.Href()
}

// ServeServices renders the Services page, at the region the request swaps: its live list alone
// when it refreshes itself (#service-groups), the content region on an htmx navigation, the full
// document on a deep link or a history restore.
func ServeServices(w http.ResponseWriter, r *http.Request, v ServicesView) {
	if hxTargetIs(r, "service-groups") {
		render(w, r, serviceGroupsPanel(v))
		return
	}
	if hxNav(r) {
		render(w, r, ServicesContent(v))
		return
	}
	render(w, r, ServicesPage(NavModel(r.URL.Path), v))
}

// ServeServiceGroup renders a group's page, at the region the request swaps: on its pane's
// refresh (#service-live), what the pane shows; on a navigation within the group (#service-main),
// the pane; on any other htmx navigation, the content region; on a deep link or a history
// restore, the full document. A pane's refresh or navigation carries the group's sub-nav too,
// out of band, so its statuses and its selection follow the pane.
func ServeServiceGroup(w http.ResponseWriter, r *http.Request, v ServiceGroupView) {
	switch {
	case hxTargetIs(r, "service-live"):
		render(w, r, serviceGroupLive(v))
	case hxTargetIs(r, "service-main"):
		render(w, r, serviceGroupPane(v))
	case hxNav(r):
		render(w, r, ServiceGroupContent(v))
	default:
		render(w, r, ServiceGroupPage(NavModel(r.URL.Path), v))
	}
}

// dotClass is a status dot's: the compact form of a chip's tone (statusDot).
func dotClass(tone string) string {
	const base = "inline-block h-2 w-2 shrink-0 rounded-full "
	switch tone {
	case toneSuccess:
		return base + "bg-emerald-400"
	case toneDanger:
		return base + "bg-red-400"
	case toneInfo:
		return base + "bg-sky-400"
	case toneWarn:
		return base + "bg-amber-400"
	default:
		return base + "bg-zinc-500"
	}
}

// ServeServiceGroupUnavailable answers a group's page when the deployer could not be asked: the
// pane's refresh keeps its place and says why — refreshing again on its next tick — and anything
// else is an error page.
func ServeServiceGroupUnavailable(w http.ResponseWriter, r *http.Request, why string) {
	if hxTargetIs(r, "service-live") {
		render(w, r, serviceGroupLive(ServiceGroupView{Error: why}))
		return
	}
	ServeError(w, r, http.StatusServiceUnavailable, "Services Unavailable", why)
}

// serviceStatusTone maps a service's status to its chip tone.
func serviceStatusTone(kind string) string {
	switch kind {
	case "healthy":
		return toneSuccess
	case "deploying":
		return toneInfo
	case "degraded", "failed":
		return toneDanger
	case "drifted":
		return toneWarn
	default: // undeployed, unknown
		return toneNeutral
	}
}

// deployStatusTone maps a deployment's (or migration's) status to its chip tone.
func deployStatusTone(kind string) string {
	switch kind {
	case "succeeded", "applied":
		return toneSuccess
	case "failed":
		return toneDanger
	case "running":
		return toneInfo
	case "halted", "hazard":
		return toneWarn
	default: // waiting, canceled, pending
		return toneNeutral
	}
}

// CLLink is another CL a CL's page names, with its status: one a rollback rolls back, or a
// rollback of it.
type CLLink struct {
	Number      int64
	Title       string
	Href        string
	StatusLabel string
	StatusKind  string
}

// CLMessage is one entry in the change-log timeline: an author, a human action ("uploaded
// patchset 2", "granted", "revoked", "commented", "submitted to main as"), the label a
// grant/revocation applies to, the commit a submit landed as, an optional body, the patchset it
// belongs to, and age.
type CLMessage struct {
	Author     string
	Action     string
	Body       string
	Label      string // the review label granted/revoked ("" for non-review entries)
	Revoked    bool   // Label was revoked rather than granted
	Commit     string // short SHA of the commit a submit landed as ("" for other entries)
	CommitHref string // link to that commit
	CL         int64  // another CL the entry names — the rollback that rolls this one back (0 for other entries)
	CLHref     string // link to that CL
	PatchSet   int64
	Age        string
	AgeTitle   string
}

// CLReplyLabel is one review label in the Reply form: a checkbox, pre-checked when the signed-in
// reviewer currently grants it.
type CLReplyLabel struct {
	Name    string
	Granted bool
}

// CLChecks is the CL's Checks tab: the viewed patchset's pre (docs/automation.md) — its
// verdict, its plan and each job's latest run, and the workflow jobs it leaves to main — and
// the CL's post, from its latest submission. While either runs, the tab refreshes itself from
// Href.
type CLChecks struct {
	Patchset    int64
	Verdict     string // "Passed" | "Failed" | "Running" | "Not Planned" | "Unavailable"
	VerdictKind string // style key: "passed" | "failed" | "running" | "none" | "unavailable"
	Live        bool   // pre is still running
	Planned     bool   // its plan has run: the unaffected workflow jobs are known
	Href        string // the tab's own fragment
	Unavailable string // why automation could not be read ("" when it could)
	// NotRun says why the current patchset runs no pre of its own — a draft, or a CL under its
	// stack's top — and RunPreHref runs it by hand ("" when the viewer may not).
	NotRun     string
	RunPreHref string
	Runs       []CLCheck
	Skipped    []CLSkippedCheck
	Post       CLPost
}

// CLPost is the CL's post: its latest submission to the landing queue — where it stands, what
// it waits for or why it ended — its post runs on the attempt that counts (the one testing, or
// the one it landed or failed on), and every attempt, newest first.
type CLPost struct {
	Submitted   bool   // the CL has been submitted
	Status      string // "Submitting" | "Landed" | "Failed" | "Canceled" | "Unavailable"
	StatusKind  string // style key: "running" | "passed" | "failed" | "canceled" | "unavailable"
	Detail      string // what it waits for while submitting; why it failed or was canceled
	SubmittedBy string
	Age         string
	AgeTitle    string
	Runs        []CLCheck
	Skipped     []CLSkippedCheck
	Attempts    []CLAttempt
	Unavailable string // why automation could not be read ("" when it could)
}

// CLAttempt is one attempt at landing a submission: main's commit it was prepared on, the
// submissions it speculated land first, and how it went.
type CLAttempt struct {
	Number     int    // 1 for the first
	Base       string // main's commit, short
	OnTopOf    string // "CL 12 and CL 15", "" when it was tested on main alone
	Status     string // "Planning" | "Waiting" | "Testing" | "Passed" | "Failed" | "Landed" | "Discarded"
	StatusKind string
	Reason     string // why it was discarded or failed
}

// CLCheck is one run on the Checks tab: the plan, or a job's latest run.
type CLCheck struct {
	Kind        string // which group it is in: CheckPlan, CheckBuild, CheckTest, CheckLint, or CheckWorkflow
	Name        string // the job ("plan", "build", "test //pkg:test", a workflow job's name)
	Source      string // where the job comes from: its workflow file, or "Built In"
	Why         string // what selected it
	StatusLabel string // "Queued" | "Running" | "Passed" | "Failed" | "Canceled" | "Error"
	StatusKind  string // style key: "queued" | "running" | "passed" | "failed" | "canceled" | "error"
	Reason      string // why it failed or was canceled ("" otherwise)
	Duration    string // "1m20s" once it has run, "" before, "In Pre" for a post run that carries pre's pass
	LogHref     string // its run's page: for a carried run, the pre run's
	// Its one action: Cancel while it is queued or running, Rerun once it has finished.
	ActionLabel string
	ActionHref  string // "" when the viewer may not take it
	ActionTitle string // why the action is disabled ("" when it is not)
}

// CLSkippedCheck is a workflow job pre selected but does not run: its target is not affected,
// so its result on main stands.
type CLSkippedCheck struct {
	Name   string
	Source string
	Target string
	Why    string
}

// RunView is one run's page: what it runs and why, where it stands, and its log.
type RunView struct {
	Chrome     RepoChrome
	CLNumber   int64
	CLHref     string
	ID         int64
	Check      CLCheck
	Patchset   int64
	Commit     string // short
	Worker     string // "w1 (attempt 2)", "" before it starts
	Queued     string
	Started    string
	Finished   string
	Live       bool   // it has not finished: the page refreshes itself
	StatusHref string // the status panel's fragment
	Log        RunLog
	RawHref    string
}

// RunLog is a run's log from an offset: its text, and — while the run goes on — where to read
// the rest from.
type RunLog struct {
	Text      string
	Truncated bool   // the page shows the log's tail; Raw has all of it
	NextHref  string // "" once the run has finished and the log is whole
}

// CLView is a single ChangeList in the Gerrit/Critique style: the status+number+subject header,
// the Change-Info column (owner/reviewers/submit-requirements), the commit message, the file
// table + diff, related changes (the stack), and the change-log timeline.
type CLView struct {
	Chrome      RepoChrome
	Number      int64
	Title       string
	StatusLabel string
	StatusKind  string

	// Change Info
	Author              string
	ChangeID            string // jj's change ID, as `jj log` shows it
	Reviewers           []CLReviewer
	SubmitReqs          []CLSubmitReq
	CanReply            bool
	ShowSubmit          bool   // the CL is active + the viewer is signed in — render Submit (active or disabled)
	CanSubmit           bool   // the viewer can submit the CL now — Submit is active
	SubmitBlockedReason string // why Submit is disabled ("" when submittable), shown as its tooltip
	SubmitHref          string
	// Submit Stack: shown on a CL stacked on unsubmitted CLs; StackSize counts the CLs it lands.
	ShowSubmitStack          bool
	CanSubmitStack           bool
	SubmitStackBlockedReason string
	SubmitStackHref          string
	StackSize                int
	Notice                   string // an error/notice banner (e.g. a failed submit)
	DropCommand              string // an abandoned CL's jj command that drops it from a checkout ("" otherwise)
	ReplyHref                string
	ReplyLabels              []CLReplyLabel // empty on a closed CL: its Reply only comments
	OwnerSets                []CLOwnerSet
	OwnersError              string // OWNERS could not be resolved (submit fails closed)
	IdentityError            string // the change to the groups-and-ACLs tree cannot be approved (submit fails closed)
	Age                      string
	AgeTitle                 string

	// Submitting: the CL is in the landing queue; CancelSubmitHref stops it ("" when the viewer
	// may not).
	Submitting       bool
	CancelSubmitHref string

	// Lifecycle actions ("" when the viewer can't take them in the CL's state)
	RequestReview     *CLReviewDialog // the header's Request Review dialog (a draft)
	AddReviewers      *CLReviewDialog // Change Info's Add Reviewers dialog (an open CL)
	ReturnToDraftHref string
	AbandonHref       string
	RestoreHref       string
	RollbackHref      string // Roll Back: a submitted CL no open rollback already rolls back

	// Rollbacks (docs/automation.md, "Rollbacks"). A rollback CL names the CLs it rolls
	// back, and the paths its first patchset left as main has them, with the jj commands that
	// start a CL stacked on it to roll those back by hand (while it is active). A rolled-back
	// CL names its rollbacks.
	IsRollback   bool
	RollsBack    []CLLink
	RollbackLeft []string
	GetCommand   string
	StackCommand string
	RolledBackBy []CLLink

	// Deploys is the Deployments tab: the deployments that cover a merged CL's commit
	// (docs/deploy.md, "On the CL").
	Deploys CLDeploys

	// Commit message
	Message string

	// Patchsets + diff
	PatchSets       []CLPatchSet
	CurrentPatchSet int64
	CommitShort     string
	NumFiles        int
	// Addition and Deletion total the diff's lines when TotalsKnown: a diff of many files
	// counts its lines per file only.
	Addition    int
	Deletion    int
	TotalsKnown bool
	// Files is the file list's first page; MoreFilesHref loads the next ("" when none).
	Files         []DiffFileView
	MoreFilesHref string

	// Stack + timeline
	Related  []CLRelated
	Messages []CLMessage
	Checks   CLChecks

	// Inline review comments. Threads written on the viewed patchset, and unresolved ones ported
	// onto it from earlier patchsets, attach to their diff row (DiffLineRow.Threads) or, when no
	// line of theirs can be shown, to their file (DiffFileView.Threads); outdated threads appear
	// only in AllThreads. CommentHref is the base POST target for creating a new inline comment
	// (the UI form posts ps/file/line/side/content here).
	CommentHref  string
	CommentCount int             // total inline threads (all patchsets), for the Comments tab badge
	AllThreads   []CommentThread // every inline thread (all patchsets), flat, for the Comments tab
}

// ServeCLView renders a single CL: the #repo-main fragment on an in-repo swap, the content region
// on a generic htmx nav, the full document on a deep link.
func ServeCLView(w http.ResponseWriter, r *http.Request, v CLView) {
	if hxTargetIs(r, "repo-main") {
		render(w, r, CLViewMain(v))
		return
	}
	if hxNav(r) {
		render(w, r, CLViewContent(v))
		return
	}
	render(w, r, CLViewPage(NavModel(r.URL.Path), v))
}

// ServeCLChecks renders the Checks tab's live region, which refreshes itself while pre runs and
// which a run's Rerun or Cancel rewrites.
func ServeCLChecks(w http.ResponseWriter, r *http.Request, c CLChecks) {
	render(w, r, clChecksRegion(c))
}

// CLDeploys is the Deployments tab: a merged CL's deployments, newest first. Before the
// deployer's plan reaches its commit (Planned false), they are yet to be made. Error says the
// deployer could not be asked. Href is the tab's own fragment, which refreshes itself while its
// deployments are yet to be planned or are under way.
type CLDeploys struct {
	Landed  bool
	Planned bool
	Items   []CLDeployment
	Error   string
	Href    string
}

// Live reports whether the tab refreshes itself: the CL has landed, and its deployments are yet
// to be planned (or could not be read) or one of them is under way.
func (d CLDeploys) Live() bool {
	if !d.Landed {
		return false
	}
	if d.Error != "" || !d.Planned {
		return true
	}
	for _, it := range d.Items {
		if it.Unfinished {
			return true
		}
	}
	return false
}

// ServeCLDeploys renders the Deployments tab's live region.
func ServeCLDeploys(w http.ResponseWriter, r *http.Request, d CLDeploys) {
	render(w, r, clDeploysRegion(d))
}

// ServeRunView renders a run's page: the #repo-main fragment on an in-repo swap, the content
// region on a generic htmx nav, the full document on a deep link.
func ServeRunView(w http.ResponseWriter, r *http.Request, v RunView) {
	if hxTargetIs(r, "repo-main") {
		render(w, r, RunViewMain(v))
		return
	}
	if hxNav(r) {
		render(w, r, RunViewContent(v))
		return
	}
	render(w, r, RunViewPage(NavModel(r.URL.Path), v))
}

// ServeRunStatus renders a run page's status panel, which refreshes itself until the run ends.
func ServeRunStatus(w http.ResponseWriter, r *http.Request, v RunView) {
	render(w, r, runStatus(v))
}

// ServeRunLog renders what a run's log gained since the page last read it, and — while the run
// goes on — the reader of the rest.
func ServeRunLog(w http.ResponseWriter, r *http.Request, l RunLog) {
	render(w, r, runLogChunk(l))
}

// ServeCLReviewerResults renders the review dialog's live reviewer search results (a fragment
// swapped into the dialog); searched is whether the query was non-empty.
func ServeCLReviewerResults(w http.ResponseWriter, r *http.Request, searched bool, users []CLCandidate) {
	render(w, r, CLReviewerResults(searched, users))
}

// ReviewDialog is the CL's one review dialog — Request Review on a draft, Add Reviewers on an open
// CL — or nil when the viewer has neither.
func (v CLView) ReviewDialog() *CLReviewDialog {
	if v.RequestReview != nil {
		return v.RequestReview
	}
	return v.AddReviewers
}

// replyVariant is the Reply button's variant: primary, unless the header already leads with a
// primary lifecycle action (Request Review, Restore).
func replyVariant(v CLView) string {
	if v.RequestReview != nil || v.RestoreHref != "" {
		return btnSecondary
	}
	return btnPrimary
}

// boolAttr renders a bool as the string "true"/"false" for a data-*/aria-* attribute value.
func boolAttr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// hxTargetIs reports whether the htmx swap target (the HX-Target request header) is the element
// with the given id. htmx v4 serialises the target as "<tag>#<id>" (e.g. "div#repo-main"), NOT
// the bare id, so we compare the id part after the '#'.
// The swap specs of a navigation. swapNav is every page's: a short fade of the region it
// replaces. swapPane is the browser pane's, which scrolls back to the top, since the pane is
// its own scroll container and keeps its scroll position across a swap.
const (
	swapNav  = "innerHTML swap:70ms settle:1ms"
	swapPane = "innerHTML swap:70ms settle:1ms scroll:top"
)

// PaneSwap is the swap spec of a navigation into the browser's pane (#browse-main), for the
// links ok control builds (the breadcrumb's).
const PaneSwap = swapPane

// browseTitle is the browser's document title: the path (a file's blame named so), the
// repository, ok.
func browseTitle(v BrowseView) string {
	t := v.Path + " · " + v.Chrome.Name + " · " + Brand
	if v.Kind == BrowseBlameKind {
		return "Blame · " + t
	}
	return t
}

// treeRowClass is a row of the browser's tree — a folder's summary or a file's link — with the
// row the pane shows (aria-current, set by the server and kept by tree.js) highlighted.
const treeRowClass = "flex items-center gap-1.5 rounded-md py-1 pr-2 text-sm text-zinc-300 transition-colors hover:bg-zinc-900/70 aria-[current=page]:bg-zinc-800/80 aria-[current=page]:font-medium aria-[current=page]:text-zinc-50"

// treeIndents are the tree's row indents by depth, written out so Tailwind sees each class: a
// row spans the panel's width, so its highlight does too, and its content is inset by depth.
var treeIndents = []string{"pl-1", "pl-5", "pl-9", "pl-13", "pl-17", "pl-21", "pl-25", "pl-29", "pl-33", "pl-37", "pl-41", "pl-45", "pl-49", "pl-53", "pl-57", "pl-61"}

// treeIndent is a tree row's indent: one step a level, flat past the deepest step.
func treeIndent(depth int) string {
	return treeIndents[min(depth, len(treeIndents)-1)]
}

// treeLevel is a tree row's aria-level (the root's entries are level 1).
func treeLevel(depth int) string { return strconv.Itoa(depth + 1) }

// iconBtnClass is a borderless icon button (copy, the Files panel's show/hide): the caller sets
// its display, which may vary (hidden until a state applies).
const iconBtnClass = "shrink-0 items-center justify-center rounded-md p-1.5 text-zinc-500 transition-colors hover:bg-zinc-800 hover:text-zinc-200"

// hxNav reports whether a request is an htmx navigation that swaps a region of the page —
// not a deep link, and not a history restore: on Back and Forward, htmx 4 fetches the page
// again (HX-History-Restore-Request) and takes the shell's #content from the whole document.
func hxNav(r *http.Request) bool {
	return r.Header.Get("HX-Request") == "true" && r.Header.Get("HX-History-Restore-Request") != "true"
}

func hxTargetIs(r *http.Request, id string) bool {
	t := r.Header.Get("HX-Target")
	if at := strings.LastIndexByte(t, '#'); at >= 0 {
		return t[at+1:] == id
	}
	return false
}

// Navigate sends the browser to href after an action that made something new there: an htmx
// request follows HX-Location — a GET swapped into the #repo-main boundary, pushing href — and
// any other a 303 redirect.
func Navigate(w http.ResponseWriter, r *http.Request, href string) {
	if r.Header.Get("HX-Request") != "true" {
		http.Redirect(w, r, href, http.StatusSeeOther)
		return
	}
	loc, _ := json.Marshal(map[string]string{"path": href, "target": "#repo-main", "swap": "innerHTML swap:70ms settle:1ms"})
	w.Header().Set("HX-Location", string(loc))
	w.WriteHeader(http.StatusOK)
}

// Chip tones — the only colours a chip may carry, chosen by meaning, never by call site.
const (
	toneSuccess = "success"
	toneDanger  = "danger"
	toneInfo    = "info"
	toneWarn    = "warn"
	toneNeutral = "neutral"
)

// chipClass is the one chip style: a fixed size + shape, coloured by tone.
func chipClass(tone string) string {
	const base = "inline-flex shrink-0 items-center gap-1 whitespace-nowrap rounded-md border px-2 text-sm leading-6 font-medium "
	switch tone {
	case toneSuccess:
		return base + "border-emerald-700/50 bg-emerald-500/10 text-emerald-300"
	case toneDanger:
		return base + "border-red-700/50 bg-red-500/10 text-red-300"
	case toneInfo:
		return base + "border-sky-700/50 bg-sky-500/10 text-sky-300"
	case toneWarn:
		return base + "border-amber-700/50 bg-amber-500/10 text-amber-300"
	default:
		return base + "border-zinc-700 bg-zinc-800/40 text-zinc-300"
	}
}

// Button variants. Every button is the same size and shape; only the variant colour differs.
const (
	btnPrimary   = "primary"
	btnInverse   = "inverse"
	btnSuccess   = "success"
	btnSecondary = "secondary"
	btnDisabled  = "disabled"
)

// btnClass is the one button style.
func btnClass(variant string) string {
	const base = "inline-flex shrink-0 items-center justify-center gap-1.5 rounded-lg border px-3 py-1.5 text-sm font-medium transition-colors "
	switch variant {
	case btnPrimary:
		return base + "cursor-pointer border-transparent bg-sky-500/90 text-white hover:bg-sky-500"
	case btnInverse:
		return base + "cursor-pointer border-transparent bg-zinc-100 text-zinc-900 hover:bg-white"
	case btnSuccess:
		return base + "cursor-pointer border-transparent bg-emerald-500/90 text-white hover:bg-emerald-500"
	case btnDisabled:
		return base + "cursor-not-allowed border-transparent bg-zinc-800 text-zinc-500"
	default:
		return base + "cursor-pointer border-zinc-700 bg-zinc-800 text-zinc-200 hover:bg-zinc-700"
	}
}

// clStatusTone maps a CL status kind to its chip tone.
func clStatusTone(kind string) string {
	switch kind {
	case "merged", "approved":
		return toneSuccess
	case "abandoned":
		return toneNeutral
	case "draft":
		return toneWarn
	default: // open
		return toneInfo
	}
}

// clCheckTone maps a run's (or pre's) status kind to its chip tone.
func clCheckTone(kind string) string {
	switch kind {
	case "passed":
		return toneSuccess
	case "failed", "error", "unavailable":
		return toneDanger
	case "running":
		return toneInfo
	case "canceled":
		return toneWarn
	default: // queued, none
		return toneNeutral
	}
}

// pluralCount renders "1 Comment" / "3 Comments".
func pluralCount(n int, singular, plural string) string {
	return strconv.Itoa(n) + " " + pluralize(int64(n), singular, plural)
}

// diffStatusTone / diffStatusLabel give a changed file's status chip.
func diffStatusTone(status string) string {
	switch status {
	case "added":
		return toneSuccess
	case "deleted", "removed":
		return toneDanger
	case "renamed":
		return toneInfo
	default:
		return toneNeutral
	}
}

func diffStatusLabel(status string) string {
	switch status {
	case "added":
		return "Added"
	case "deleted":
		return "Deleted"
	case "renamed":
		return "Renamed"
	case "copied":
		return "Copied"
	case "unmodified":
		return "Unmodified"
	case "removed":
		return "Removed"
	default:
		return "Modified"
	}
}

// sideLabel names the diff side a review thread is anchored to, in the Base → Patchset terms the
// CL view uses.
func sideLabel(side string) string {
	if side == "left" {
		return "Base"
	}
	return "Patchset"
}

// clFullMessage reconstructs a commit message from its subject + body for the message box.
func clFullMessage(title, body string) string {
	if body == "" {
		return title
	}
	return title + "\n\n" + body
}

// clInitial returns the uppercase first letter of a name for the timeline avatar.
func clInitial(name string) string {
	if name == "" {
		return "?"
	}
	return strings.ToUpper(name[:1])
}

// pluralize returns singular when n == 1, else plural (used for the commit-count label).
func pluralize(n int64, singular, plural string) string {
	if n == 1 {
		return singular
	}
	return plural
}

// lineNo renders a 1-based diff line number, or "" for the 0 sentinel (no line on that side).
func lineNo(n int) string {
	if n <= 0 {
		return ""
	}
	return strconv.Itoa(n)
}

// diffMarker is the leading gutter character for a diff line: "+" added, "-" deleted, else a
// space (context) so every code line aligns.
func diffMarker(t string) string {
	switch t {
	case "add":
		return "+"
	case "del":
		return "-"
	default:
		return " "
	}
}

// diffRowClass is the whole-line background for a diff row, keyed on the change type.
func diffRowClass(t string) string {
	switch t {
	case "add":
		return "bg-emerald-500/10"
	case "del":
		return "bg-red-500/10"
	default:
		return ""
	}
}

// threadOriginLabel is the chip naming where a thread was written relative to the viewed patchset:
// a ported thread says it came from an earlier patchset (and whether its line or file changed since),
// an outdated one says it is from another patchset, and a native one names its own patchset.
func threadOriginLabel(t CommentThread) string {
	ps := strconv.FormatInt(t.PatchSet, 10)
	switch {
	case t.Port == "changed":
		return "Line Changed Since Patchset " + ps
	case t.Port == "deleted":
		return "File Deleted Since Patchset " + ps
	case t.Port != "":
		return "From Patchset " + ps
	case t.Outdated:
		return "Outdated · Patchset " + ps
	default:
		return "Patchset " + ps
	}
}

// threadOriginTone flags a ported thread whose content moved out from under it.
func threadOriginTone(t CommentThread) string {
	if t.Port == "changed" || t.Port == "deleted" {
		return toneWarn
	}
	return toneNeutral
}

// threadOriginTitle is the origin chip's tooltip: where the thread was written, and for a ported
// thread what became of that place.
func threadOriginTitle(t CommentThread) string {
	written := "Written on Patchset " + strconv.FormatInt(t.PatchSet, 10)
	if t.Port == "" {
		return written
	}
	written += " at " + t.OrigFilePath
	if t.OrigLine > 0 {
		written += ":" + strconv.Itoa(t.OrigLine)
	}
	switch t.Port {
	case "changed":
		return written + "; that line has changed since"
	case "deleted":
		return written + "; that file has been deleted since"
	case "unknown":
		return written + "; its line can't be followed onto this patchset"
	default:
		return written
	}
}

// commentAnchor picks the side + line an inline comment on this diff row anchors to: the new side
// for an added/context line, the old side for a pure deletion. ok is false for a hunk header (a row
// that carries no code line to comment on).
func commentAnchor(ln DiffLineRow) (side string, line int, ok bool) {
	if ln.Type == "tag" {
		return "", 0, false
	}
	if ln.RightIdx > 0 {
		return "right", ln.RightIdx, true
	}
	if ln.LeftIdx > 0 {
		return "left", ln.LeftIdx, true
	}
	return "", 0, false
}

// commentable / anchorSide / anchorLine are templ-friendly wrappers over commentAnchor (templ if
// conditions take a bare boolean, so the anchor is split into three no-arg-style accessors).
func commentable(ln DiffLineRow) bool  { _, _, ok := commentAnchor(ln); return ok }
func anchorSide(ln DiffLineRow) string { s, _, _ := commentAnchor(ln); return s }
func anchorLine(ln DiffLineRow) string { _, l, _ := commentAnchor(ln); return strconv.Itoa(l) }

func render(w http.ResponseWriter, r *http.Request, c templ.Component) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := c.Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Assets serves the pages' assets and the shared ones (internal/web/static). Mount under
// /assets/ and /favicon.svg.
func Assets() http.Handler {
	mux := http.NewServeMux()
	js := "text/javascript; charset=utf-8" // RFC 9239, as the shared assets are served
	mux.HandleFunc("/assets/app.css", asset(appCSS, "text/css; charset=utf-8"))
	mux.HandleFunc("/assets/stars.js", asset(starsJS, js))
	mux.HandleFunc("/assets/clcomments.js", asset(clcommentsJS, js))
	mux.HandleFunc("/assets/tree.js", asset(treeJS, js))
	shared := http.StripPrefix("/assets/", static.Handler())
	mux.Handle("/assets/", shared)
	mux.HandleFunc("/favicon.svg", func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = "/assets/favicon.svg"
		shared.ServeHTTP(w, r)
	})
	return mux
}

func asset(body []byte, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_, _ = w.Write(body)
	}
}
