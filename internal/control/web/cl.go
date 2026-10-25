package web

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Cidan/ok/internal/control/db"
	pages "github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/owners"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// clListLimit bounds how many CLs a finder view lists.
const clListLimit = 500

var filterLabels = map[db.Filter]string{
	db.FilterOpen: "Open", db.FilterDrafts: "Drafts", db.FilterNeedsReview: "Needs My Review",
	db.FilterMerged: "Merged", db.FilterAbandoned: "Abandoned", db.FilterAll: "All",
}

var filterEmpty = map[db.Filter]string{
	db.FilterOpen: "No open changes.", db.FilterDrafts: "No drafts.", db.FilterNeedsReview: "Nothing is waiting for your review.",
	db.FilterMerged: "No merged changes.", db.FilterAbandoned: "No abandoned changes.", db.FilterAll: "No changes yet.",
}

// statusLabel is a CL's status chip: an open CL whose review is met shows as Approved.
func statusLabel(s db.Status, approved bool) (label, kind string) {
	switch s {
	case db.StatusDraft:
		return "Draft", "draft"
	case db.StatusSubmitted:
		return "Merged", "merged"
	case db.StatusAbandoned:
		return "Abandoned", "abandoned"
	}
	if approved {
		return "Approved", "approved"
	}
	return "Open", "open"
}

// clList is a repository's CL finder, in one of its views (?f=).
func (s *Server) clList(w http.ResponseWriter, r *http.Request) {
	repo, err := s.repository(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	f := db.ParseFilter(r.URL.Query().Get("f"))
	cls, err := s.db.ListChangeLists(r.Context(), repo.GetId(), f, principal(r), clListLimit)
	if err != nil {
		fail(w, r, err)
		return
	}
	v := pages.CLListView{Chrome: chrome(repo, "main", repo.GetHead(), ""), Empty: filterEmpty[f]}
	for _, c := range cls {
		label, kind := statusLabel(c.Status, c.Approved)
		item := pages.CLItem{Number: c.Number, Title: c.Title(), Author: people.Handle(r.Context(), c.Author), StatusLabel: label, StatusKind: kind,
			Rollback: len(c.RollsBack) > 0, Href: clHref(repo.GetName(), c.Number)}
		item.Age, item.AgeTitle = age(c.UpdatedAt)
		v.Items = append(v.Items, item)
	}
	for _, each := range db.Filters {
		href := repoLink(repo.GetName()) + "/cls"
		if each != db.FilterOpen {
			href += "?" + url.Values{"f": {string(each)}}.Encode()
		}
		v.Filters = append(v.Filters, pages.CLFilter{Label: filterLabels[each], Href: href, Active: each == f})
	}
	pages.ServeCLList(w, r, v)
}

// clTarget is the repository and CL a CL route names.
type clTarget struct {
	repo *scmpb.Repository
	cl   db.ChangeList
	scm  *scmpb.ChangeList
}

func (s *Server) loadCL(r *http.Request) (*clTarget, error) {
	repo, err := s.repository(r)
	if err != nil {
		return nil, err
	}
	number, err := strconv.ParseInt(chi.URLParam(r, "number"), 10, 64)
	if err != nil || number < 1 {
		return nil, review.ErrNotFound
	}
	cl, c, err := s.review.ChangeList(r.Context(), repo.GetId(), number)
	if err != nil {
		return nil, err
	}
	return &clTarget{repo: repo, cl: cl, scm: c}, nil
}

// clView is a CL's page.
func (s *Server) clView(w http.ResponseWriter, r *http.Request) {
	t, err := s.loadCL(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	s.serveCL(w, r, t, "")
}

// serveCL renders a CL's page with an optional notice, re-reading it first.
func (s *Server) serveCL(w http.ResponseWriter, r *http.Request, t *clTarget, notice string) {
	v, err := s.buildCL(r, t)
	if err != nil {
		fail(w, r, err)
		return
	}
	v.Notice = notice
	pages.ServeCLView(w, r, *v)
}

// act runs a CL action, then shows the CL: a refusal meant for the person is the page's
// notice, anything else a failure.
func (s *Server) act(w http.ResponseWriter, r *http.Request, action func(*clTarget) error) {
	t, err := s.loadCL(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	notice := ""
	if err := action(t); err != nil {
		if !review.IsUserError(err) {
			fail(w, r, err)
			return
		}
		notice = err.Error()
	}
	fresh, err := s.loadCL(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	s.serveCL(w, r, fresh, notice)
}

// buildCL assembles a CL's page: its header and Change Info, the review requirements
// evaluated live, the viewed patchset's diff with the review threads placed on it, the
// stack, the change log, and the checks.
func (s *Server) buildCL(r *http.Request, t *clTarget) (*pages.CLView, error) {
	ctx := r.Context()
	me := principal(r)
	repo, cl, c := t.repo, t.cl, t.scm
	link := clHref(repo.GetName(), cl.Number)

	viewed := viewedPatchset(r, cl)
	ps := scm.Patchset(c, viewed)
	cs, err := s.changes(ctx, repo.GetId(), ps.GetBaseCommitId(), ps.GetCommitId())
	if err != nil {
		return nil, err
	}
	reviewers, err := s.db.Reviewers(ctx, cl.Repo, cl.Number)
	if err != nil {
		return nil, err
	}
	grants, err := s.db.Grants(ctx, cl.Repo, cl.Number)
	if err != nil {
		return nil, err
	}
	comments, err := s.db.Comments(ctx, cl.Repo, cl.Number)
	if err != nil {
		return nil, err
	}
	events, err := s.db.Events(ctx, cl.Repo, cl.Number)
	if err != nil {
		return nil, err
	}
	stack, err := s.db.Stack(ctx, cl.Repo, cl.Number)
	if err != nil {
		return nil, err
	}
	ev, err := s.review.Evaluate(ctx, cl, c, repo.GetHead())
	if err != nil {
		return nil, err
	}
	st := ev.Status
	canManage := s.review.CanManage(ctx, repo, cl, me)
	active := cl.Status.Active()
	isReviewer := slices.ContainsFunc(reviewers, func(rv db.Reviewer) bool { return rv.Principal == me })

	label, kind := statusLabel(cl.Status, cl.Approved)
	v := &pages.CLView{
		Chrome:      chrome(repo, "main", repo.GetHead(), ""),
		Number:      cl.Number,
		Title:       cl.Title(),
		StatusLabel: label,
		StatusKind:  kind,
		Author:      people.Handle(ctx, cl.Author),
		ChangeID:    scm.ShortChangeID(cl.ChangeID),
		CanReply:    true,
		ReplyHref:   link + "/reply",
		SubmitHref:  link + "/submit",

		CurrentPatchSet: int64(viewed),
		CommitShort:     scm.Short(ps.GetCommitId()),
		NumFiles:        len(cs.changes),
		CommentHref:     link + "/comment",
	}
	v.Age, v.AgeTitle = age(cl.UpdatedAt)
	// The message box shows the title, then the rest of the description.
	if _, body, ok := strings.Cut(strings.TrimSpace(cl.Description), "\n"); ok {
		v.Message = strings.TrimSpace(body)
	}

	for _, p := range c.GetPatchsets() {
		v.PatchSets = append(v.PatchSets, pages.CLPatchSet{
			Number: int64(p.GetNumber()), CommitShort: scm.Short(p.GetCommitId()),
			Href: link + "?ps=" + strconv.Itoa(int(p.GetNumber())), Current: p.GetNumber() == viewed,
		})
	}
	for _, sc := range stack {
		v.Related = append(v.Related, pages.CLRelated{Number: sc.Number, Title: sc.Title(), Href: clHref(repo.GetName(), sc.Number), Current: sc.Number == cl.Number})
	}
	if len(v.Related) == 1 {
		v.Related = nil
	}

	// Reviewers, each with the labels they grant; the author, an admin, and the reviewer
	// themself may remove a reviewer from an active CL.
	grantedBy := map[string]map[db.Label]bool{}
	for _, g := range grants {
		if grantedBy[g.Principal] == nil {
			grantedBy[g.Principal] = map[db.Label]bool{}
		}
		grantedBy[g.Principal][g.Label] = g.Granted
	}
	for _, rv := range reviewers {
		chip := pages.CLReviewer{Name: people.Handle(ctx, rv.Principal)}
		for _, l := range db.Labels {
			if grantedBy[rv.Principal][l] {
				chip.Labels = append(chip.Labels, l.String())
			}
		}
		if active && (canManage || rv.Principal == me) {
			chip.RemoveHref = link + "/reviewers/remove?" + url.Values{"principal": {rv.Principal}}.Encode()
		}
		v.Reviewers = append(v.Reviewers, chip)
	}

	// The review requirements: OWNERS' by stable ID, the groups-and-ACLs tree's by name.
	handles := func(ps []string) []string { return people.Handles(ctx, ps) }
	treeHandles := func(ps []string) []string {
		out := make([]string, len(ps))
		for i, p := range ps {
			out[i] = people.HandleOf(p)
		}
		return out
	}
	approvalDetail := ""
	if st.OwnersErr == nil {
		identity := len(st.Identity) > 0 || st.IdentityErr != nil
		var parts []string
		if len(st.Owners) > 0 || !identity {
			met := 0
			for _, o := range st.Owners {
				if o.Satisfied {
					met++
				}
			}
			parts = append(parts, strconv.Itoa(met)+" of "+strconv.Itoa(len(st.Owners))+" owner sets")
		}
		if len(st.Identity) > 0 {
			accepted := 0
			for _, r := range st.Identity {
				if r.Accepted {
					accepted++
				}
			}
			parts = append(parts, strconv.Itoa(accepted)+" of "+strconv.Itoa(len(st.Identity))+" parts of the groups-and-ACLs tree's change")
		}
		if st.IdentityErr != nil {
			parts = append(parts, "a change to the groups-and-ACLs tree that can't apply")
		}
		approvalDetail = strings.Join(parts, ", ")
		switch {
		case st.Rollback && len(st.Owners) == 0 && !identity:
			approvalDetail = "Not needed: a rollback"
		case st.Rollback:
			approvalDetail += ", for the files edited by hand"
		}
	} else {
		v.OwnersError = st.OwnersErr.Error()
	}
	if st.IdentityErr != nil {
		v.IdentityError = "This change to the groups-and-ACLs tree can't be approved: " + st.IdentityErr.Error()
	}
	unresolved := "None"
	if st.Unresolved > 0 {
		unresolved = strconv.FormatInt(st.Unresolved, 10)
	}
	v.SubmitReqs = []pages.CLSubmitReq{
		{Label: db.LabelLGTM.String(), Satisfied: st.LGTMSatisfied, GrantedBy: handles(st.LGTMBy)},
		{Label: db.LabelApproval.String(), Satisfied: st.ApprovalSatisfied(), Detail: approvalDetail},
		{Label: "Unresolved Comments", Satisfied: st.Unresolved == 0, Detail: unresolved},
	}
	for _, o := range st.Owners {
		set := pages.CLOwnerSet{
			Label: review.OwnersLabel(o.Set), NumFiles: len(o.Set.Files), FilesTitle: filesTitle(o.Set.Files), Satisfied: o.Satisfied, Implicit: o.Implicit,
			AllowSelf: o.Set.AllowSelf, ApprovedBy: handles(o.ApprovedBy),
		}
		if o.Set.File != "" {
			set.Href = treeHref(repo.GetName(), "main", o.Set.File)
		}
		for _, owner := range o.Set.Owners {
			set.Owners = append(set.Owners, ownerName(owner))
		}
		v.OwnerSets = append(v.OwnerSets, set)
	}
	for _, r := range st.Identity {
		set := pages.CLOwnerSet{
			Label: r.ACL, What: r.What, Satisfied: r.Accepted, Implicit: r.Implicit, ApprovedBy: treeHandles(r.ApprovedBy),
			Href: treeHref(repo.GetName(), "main", path.Join(r.Dir, r.ACLPath, identity.ACL.File())),
		}
		set.Owners = treeHandles(r.Eligible)
		v.OwnerSets = append(v.OwnerSets, set)
	}

	// The change log: the CL's history, its submissions to the landing queue, and every comment,
	// oldest first.
	var entries []timed
	subs := s.submissions(ctx, repo, cl.Number)
	for _, sub := range subs.list {
		entries = append(entries, submissionMessages(ctx, sub, cl.Number)...)
	}
	for _, e := range events {
		m := eventMessage(ctx, e, repo.GetName())
		m.Age, m.AgeTitle = age(e.At)
		entries = append(entries, timed{e.At, m})
	}
	for _, cm := range comments {
		m := pages.CLMessage{Author: people.Handle(ctx, cm.Author), Action: "commented", Body: cm.Body, PatchSet: int64(cm.Patchset)}
		m.Age, m.AgeTitle = age(cm.CreatedAt)
		entries = append(entries, timed{cm.CreatedAt, m})
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].at.Before(entries[j].at) })
	for _, e := range entries {
		v.Messages = append(v.Messages, e.msg)
	}

	// Checks: the viewed patchset's pre, and the CL's post. Submit Requirements' Pre row is the
	// current patchset's.
	v.Checks = s.checks(ctx, repo, cl, viewed, me, subs)
	current := v.Checks
	if viewed != cl.CurrentPatchset {
		current = s.preChecks(ctx, repo, cl, cl.CurrentPatchset, me)
	}
	if sub := subs.latest(); sub.GetStatus() == automationpb.Submission_STATUS_ACTIVE && !landedFirst(cl, sub) {
		v.Submitting = true
		if s.review.CanSubmit(ctx, repo, cl, me) {
			v.CancelSubmitHref = link + "/cancel-submit"
		}
	}
	v.SubmitReqs = append(v.SubmitReqs, preRequirement(current))

	// The Reply form: on an active CL, a checkbox per label the viewer's grant would count
	// toward, and any label they grant, so it can always be revoked. A closed CL takes
	// comments only.
	if active {
		counts := map[db.Label]bool{
			db.LabelLGTM:     me != cl.Author || st.SelfLGTMAllowed,
			db.LabelApproval: st.CanApprove(ctx, me, cl.Author),
		}
		for _, l := range db.Labels {
			if counts[l] || grantedBy[me][l] {
				v.ReplyLabels = append(v.ReplyLabels, pages.CLReplyLabel{Name: l.String(), Granted: grantedBy[me][l]})
			}
		}
	}

	// The files: the first page of the list, each row loading its diff — with its review
	// threads placed on it — when opened.
	views, placed := s.clThreads(ctx, c, viewed, link, me, comments)
	files := clFileList(repo.GetName(), cl.Number, viewed, cs, placed)
	if v.Files, v.MoreFilesHref, err = s.rows(ctx, repo.GetId(), files, 1); err != nil {
		return nil, err
	}
	if v.Addition, v.Deletion, v.TotalsKnown, err = s.totals(ctx, repo.GetId(), cs); err != nil {
		return nil, err
	}
	for _, tv := range views {
		v.AllThreads = append(v.AllThreads, *tv)
	}
	sort.SliceStable(v.AllThreads, func(i, j int) bool {
		if v.AllThreads[i].FilePath != v.AllThreads[j].FilePath {
			return v.AllThreads[i].FilePath < v.AllThreads[j].FilePath
		}
		return v.AllThreads[i].Line < v.AllThreads[j].Line
	})
	v.CommentCount = len(views)

	// Submit, and Submit Stack for a CL stacked on unsubmitted CLs: shown on an active
	// CL, disabled with the reason until the viewer can land it.
	if active {
		v.ShowSubmit = true
		if v.SubmitBlockedReason, err = s.review.Blocked(ctx, repo, cl, c, me, false); err != nil {
			return nil, err
		}
		v.CanSubmit = v.SubmitBlockedReason == ""
		if v.StackSize, err = s.review.StackSize(ctx, cl); err != nil {
			return nil, err
		}
		if v.StackSize > 1 {
			v.ShowSubmitStack = true
			v.SubmitStackHref = link + "/submit-stack"
			if v.SubmitStackBlockedReason, err = s.review.Blocked(ctx, repo, cl, c, me, true); err != nil {
				return nil, err
			}
			v.CanSubmitStack = v.SubmitStackBlockedReason == ""
		}
	}

	// Lifecycle actions.
	switch cl.Status {
	case db.StatusDraft:
		if canManage {
			if v.RequestReview, err = s.reviewDialog(ctx, cl, ev, link, true); err != nil {
				return nil, err
			}
			v.AbandonHref = link + "/abandon"
		}
	case db.StatusOpen:
		if canManage || isReviewer {
			if v.AddReviewers, err = s.reviewDialog(ctx, cl, ev, link, false); err != nil {
				return nil, err
			}
		}
		if canManage {
			v.ReturnToDraftHref = link + "/draft"
			v.AbandonHref = link + "/abandon"
		}
	case db.StatusAbandoned:
		v.DropCommand = "jj abandon " + scm.ChangeID(cl.ChangeID)
		if canManage {
			v.RestoreHref = link + "/restore"
		}
	}
	if err := s.rollbacks(ctx, v, repo, cl, c, me); err != nil {
		return nil, err
	}
	v.Deploys = s.clDeploys(ctx, repo, cl)
	return v, nil
}

func ownerName(o owners.Owner) string {
	switch o.Kind {
	case owners.Anyone:
		return "Anyone"
	case owners.ACL:
		return "acl: " + o.Name
	}
	return o.Name
}

// viewedPatchset is the patchset a CL's page shows: ?ps, or the current one.
func viewedPatchset(r *http.Request, cl db.ChangeList) int32 {
	if n, err := strconv.ParseInt(r.URL.Query().Get("ps"), 10, 32); err == nil && n >= 1 && n <= int64(cl.CurrentPatchset) {
		return int32(n)
	}
	return cl.CurrentPatchset
}

// clThreads are a CL's review threads as its viewed patchset shows them: every thread's view,
// and those that render on the patchset's diff — written on it, or unresolved and ported onto it
// from an earlier patchset — with where each lands. The rest are outdated.
func (s *Server) clThreads(ctx context.Context, c *scmpb.ChangeList, viewed int32, link, me string, comments []db.Comment) ([]*pages.CommentThread, []placedThread) {
	threads := review.BuildThreads(comments)
	ported := s.review.PortThreads(ctx, c, viewed, threads)
	views := make([]*pages.CommentThread, 0, len(threads))
	var placed []placedThread
	for _, th := range threads {
		tv := threadView(ctx, th, link, viewed, me)
		views = append(views, &tv)
		anchor, isPorted := ported[th.RootID]
		switch {
		case th.Patchset == viewed:
			placed = append(placed, placedThread{view: &tv, side: tv.Side, path: th.FilePath, byName: true, line: int64(th.Line)})
		case isPorted:
			tv.Port = portKind(anchor.State)
			tv.OrigFilePath, tv.OrigLine = th.FilePath, int(th.Line)
			tv.Line = int(anchor.Line)
			placed = append(placed, placedThread{view: &tv, side: tv.Side, path: anchor.Path, byName: th.Side != db.SideBase,
				line: anchor.Line, gone: anchor.State == review.PortFileDeleted})
		default:
			tv.Outdated = true
		}
	}
	return views, placed
}

// threadRows finds the row each placed thread lands on — the change by its path in the patchset
// (byName) or in the base, else a row of its own for a file the diff does not list — and names
// each thread's file as that row does.
type threadRows struct {
	byPath, byOld map[string]string
}

func newThreadRows(cs *changeSet) *threadRows {
	t := &threadRows{byPath: map[string]string{}, byOld: map[string]string{}}
	for _, c := range cs.changes {
		t.byPath[c.GetPath()] = c.GetPath()
		old := c.GetPath()
		if op := oldPath(c); op != "" {
			old = op
		}
		t.byOld[old] = c.GetPath()
	}
	return t
}

// row is the path of the row a thread lands on, and whether the diff lists it.
func (t *threadRows) row(p placedThread) (string, bool) {
	rows := t.byOld
	if p.byName {
		rows = t.byPath
	}
	if r, ok := rows[p.path]; ok {
		return r, true
	}
	return p.path, false
}

// clFileList is a CL's patchset's file list: its changes, the files its threads land on that the
// diff does not list, and how many threads land on each row.
func clFileList(repoName string, number int64, viewed int32, cs *changeSet, placed []placedThread) *fileList {
	l := &fileList{cs: cs, threads: map[string]int{}}
	rows := newThreadRows(cs)
	seen := map[string]bool{}
	for _, t := range placed {
		r, listed := rows.row(t)
		if !listed && !seen[r] {
			seen[r] = true
			l.offDiff = append(l.offDiff, r)
		}
		l.threads[r]++
		t.view.FilePath = r
	}
	sort.Strings(l.offDiff)
	link := clHref(repoName, number)
	ps := strconv.Itoa(int(viewed))
	l.body = func(row pages.DiffFileView) string {
		return link + "/file?" + bodyQuery(url.Values{"ps": {ps}}, row)
	}
	l.page = func(n int) string {
		return link + "/files?" + url.Values{"ps": {ps}, "page": {strconv.Itoa(n)}}.Encode()
	}
	return l
}

// clFiles is a page of a CL's patchset's file list after the first.
func (s *Server) clFiles(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	t, err := s.loadCL(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	viewed := viewedPatchset(r, t.cl)
	ps := scm.Patchset(t.scm, viewed)
	cs, err := s.changes(ctx, t.repo.GetId(), ps.GetBaseCommitId(), ps.GetCommitId())
	if err != nil {
		fail(w, r, err)
		return
	}
	comments, err := s.db.Comments(ctx, t.cl.Repo, t.cl.Number)
	if err != nil {
		fail(w, r, err)
		return
	}
	_, placed := s.clThreads(ctx, t.scm, viewed, clHref(t.repo.GetName(), t.cl.Number), principal(r), comments)
	rows, next, err := s.rows(ctx, t.repo.GetId(), clFileList(t.repo.GetName(), t.cl.Number, viewed, cs, placed), pageNumber(r.URL.Query()))
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeFileRows(w, r, rows, next)
}

// clFile is one file of a CL's patchset's diff, with the review threads that land on it: what
// its row loads when opened.
func (s *Server) clFile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	t, err := s.loadCL(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	viewed := viewedPatchset(r, t.cl)
	ps := scm.Patchset(t.scm, viewed)
	cs, err := s.changes(ctx, t.repo.GetId(), ps.GetBaseCommitId(), ps.GetCommitId())
	if err != nil {
		fail(w, r, err)
		return
	}
	q := r.URL.Query()
	path := q.Get("path")
	var which []*scmpb.PathChange
	if q.Get("offdiff") == "" {
		c := findChange(cs, path, q.Get("old"))
		if c == nil {
			fail(w, r, review.ErrNotFound)
			return
		}
		which = []*scmpb.PathChange{c}
	}
	d, err := s.renderDiff(ctx, t.repo.GetId(), ps.GetBaseCommitId(), ps.GetCommitId(), cs, which)
	if err != nil {
		fail(w, r, err)
		return
	}
	comments, err := s.db.Comments(ctx, t.cl.Repo, t.cl.Number)
	if err != nil {
		fail(w, r, err)
		return
	}
	link := clHref(t.repo.GetName(), t.cl.Number)
	_, placed := s.clThreads(ctx, t.scm, viewed, link, principal(r), comments)
	rows := newThreadRows(cs)
	var mine []placedThread
	for _, th := range placed {
		if row, _ := rows.row(th); row == path {
			mine = append(mine, th)
		}
	}
	pl := placer{ctx: ctx, s: s, repo: t.repo.GetId(), base: ps.GetBaseCommitId(), ps: ps.GetCommitId(), d: d}
	if err := pl.place(mine); err != nil {
		fail(w, r, err)
		return
	}
	f := pages.DiffFileView{Name: path, OldName: path, OffDiff: true}
	if len(d.files) > 0 {
		f = d.files[0]
	}
	pages.ServeCLFileBody(w, r, f, link+"/comment", true)
}

// titleFiles is the most files an owner set's title names.
const titleFiles = 20

// filesTitle names an owner set's files, one to a line: the first few of many, and how many
// more.
func filesTitle(files []string) string {
	if len(files) <= titleFiles {
		return strings.Join(files, "\n")
	}
	return strings.Join(files[:titleFiles], "\n") + fmt.Sprintf("\n… and %d more", len(files)-titleFiles)
}

func portKind(s review.PortState) string {
	switch s {
	case review.PortLineChanged:
		return "changed"
	case review.PortFileDeleted:
		return "deleted"
	case review.PortUnknown:
		return "unknown"
	}
	return "exact"
}

func sideName(s db.Side) string {
	switch s {
	case db.SideBase:
		return "left"
	case db.SidePatchset:
		return "right"
	}
	return ""
}

// threadView is a thread as the page shows it, with its reply and resolve targets, which
// re-render the viewed patchset.
func threadView(ctx context.Context, t review.Thread, link string, viewed int32, me string) pages.CommentThread {
	var comments []pages.CommentView
	for _, c := range t.Comments {
		cv := pages.CommentView{ID: c.ID, Author: people.Handle(ctx, c.Author), Body: c.Body, CanEdit: c.Author == me}
		cv.Age, cv.AgeTitle = age(c.CreatedAt)
		comments = append(comments, cv)
	}
	root := strconv.FormatInt(t.RootID, 10)
	ps := strconv.Itoa(int(viewed))
	base := link + "/comment"
	return pages.CommentThread{
		RootID:        t.RootID,
		FilePath:      t.FilePath,
		Side:          sideName(t.Side),
		Line:          int(t.Line),
		Resolved:      t.Resolved,
		PatchSet:      int64(t.Patchset),
		PatchSetHref:  link + "?ps=" + strconv.Itoa(int(t.Patchset)),
		Comments:      comments,
		ReplyHref:     base + "?" + url.Values{"parent": {root}, "ps": {ps}}.Encode(),
		ResolveHref:   base + "/" + root + "/resolve?ps=" + ps,
		UnresolveHref: base + "/" + root + "/unresolve?ps=" + ps,
	}
}

// clLink is a CL another CL's page names, with its status.
func clLink(repo string, c db.ChangeList) pages.CLLink {
	label, kind := statusLabel(c.Status, c.Approved)
	return pages.CLLink{Number: c.Number, Title: c.Title(), Href: clHref(repo, c.Number), StatusLabel: label, StatusKind: kind}
}

// rollbacks fills in a CL's rollbacks: for a rollback, the CLs it rolls back and the paths it
// left (with the commands that stack a CL on it to roll them back by hand); for a submitted
// CL, its rollbacks, and Roll Back while none of them is open.
func (s *Server) rollbacks(ctx context.Context, v *pages.CLView, repo *scmpb.Repository, cl db.ChangeList, c *scmpb.ChangeList, me string) error {
	if rb := c.GetRollback(); rb != nil {
		v.IsRollback = true
		for _, n := range rb.GetChangeLists() {
			rolled, ok, err := s.db.GetChangeList(ctx, cl.Repo, n)
			if err != nil {
				return err
			}
			if ok {
				v.RollsBack = append(v.RollsBack, clLink(repo.GetName(), rolled))
			}
		}
		v.RollbackLeft = rb.GetLeft()
		if len(v.RollbackLeft) > 0 && cl.Status.Active() {
			n := strconv.FormatInt(cl.Number, 10)
			v.GetCommand, v.StackCommand = "jj cl get "+n, "jj new cl/"+n+"@ok"
		}
	}
	if cl.Status != db.StatusSubmitted {
		return nil
	}
	by, err := s.db.RollbacksOf(ctx, cl.Repo, cl.Number)
	if err != nil {
		return err
	}
	underway := false
	for _, b := range by {
		v.RolledBackBy = append(v.RolledBackBy, clLink(repo.GetName(), b))
		underway = underway || b.Status.Active()
	}
	if !underway && people.IsHumanID(ctx, me) {
		v.RollbackHref = clHref(repo.GetName(), cl.Number) + "/rollback"
	}
	return nil
}

// eventMessage renders one entry of a CL's history for the change log.
func eventMessage(ctx context.Context, e db.Event, repo string) pages.CLMessage {
	actor := "ok"
	if e.Actor != "" {
		actor = people.Handle(ctx, e.Actor)
	}
	m := pages.CLMessage{Author: actor, PatchSet: int64(e.Patchset)}
	switch e.Kind {
	case db.EventUploaded:
		m.Action = "uploaded patchset " + strconv.Itoa(int(e.Patchset))
	case db.EventReviewRequested:
		m.Action = "requested review"
	case db.EventReturnedToDraft:
		m.Action = "returned the change to draft"
	case db.EventAbandoned:
		m.Action, m.Body = "abandoned the change", e.Message
	case db.EventRestored:
		m.Action = "restored the change"
		if e.To == db.StatusDraft {
			m.Action += " as a draft"
		}
	case db.EventSubmitted:
		m.Action = "submitted to main as"
		m.Commit = scm.Short(e.Commit)
		m.CommitHref = commitHref(repo, e.Commit)
	case db.EventReviewerAdded:
		m.Action = "added reviewer " + people.Handle(ctx, e.Subject)
	case db.EventReviewerRemoved:
		if e.Subject == e.Actor {
			m.Action = "stopped reviewing"
		} else {
			m.Action = "removed reviewer " + people.Handle(ctx, e.Subject)
		}
	case db.EventLabelGranted:
		m.Action, m.Label = "granted", e.Label.String()
	case db.EventLabelRevoked:
		m.Action, m.Label, m.Revoked = "revoked", e.Label.String(), true
	case db.EventApproved:
		m.Action = "every review requirement is met"
	case db.EventApprovalLost:
		m.Action = "a review requirement is no longer met"
	case db.EventRolledBack:
		m.Action = "rolled the change back in"
		m.CL, m.CLHref = e.Related, clHref(repo, e.Related)
	}
	return m
}

// reviewDialog builds the Request Review (a draft) or Add Reviewers (an open CL) dialog
// from the suggestions for the CL's live evaluation.
func (s *Server) reviewDialog(ctx context.Context, cl db.ChangeList, ev *review.Evaluation, link string, request bool) (*pages.CLReviewDialog, error) {
	sug, err := s.review.Suggest(ctx, cl, ev)
	if err != nil {
		return nil, err
	}
	cand := func(c review.Candidate) pages.CLCandidate {
		return pages.CLCandidate{Principal: c.Principal, Name: people.Handle(ctx, c.Principal), Selected: c.Selected, Assigned: c.Assigned}
	}
	d := &pages.CLReviewDialog{Title: "Add Reviewers", Action: "Add", Href: link + "/reviewers", SearchHref: link + "/reviewers/search", NeedsLGTM: sug.NeedsLGTM}
	if request {
		d.Title, d.Action, d.Href = "Request Review", "Send", link+"/request-review"
	}
	for _, o := range sug.OwnerSets {
		set := pages.CLSuggestSet{Label: o.Label}
		for _, c := range o.Candidates {
			set.Candidates = append(set.Candidates, cand(c))
		}
		d.OwnerSets = append(d.OwnerSets, set)
	}
	for _, c := range sug.Recent {
		d.Recent = append(d.Recent, cand(c))
	}
	return d, nil
}

// formReviewers reads the dialog's chosen reviewers.
func formReviewers(r *http.Request) []string {
	var out []string
	for _, v := range r.Form["reviewer"] {
		if p := strings.TrimSpace(v); p != "" && !slices.Contains(out, p) {
			out = append(out, p)
		}
	}
	return out
}

// The CL actions.

func (s *Server) clReply(w http.ResponseWriter, r *http.Request) {
	s.act(w, r, func(t *clTarget) error {
		if err := r.ParseForm(); err != nil {
			return &review.UserError{Msg: "That reply could not be read"}
		}
		grants := map[db.Label]bool{}
		for _, name := range r.Form["label"] {
			l, ok := db.ParseLabel(name)
			if !ok {
				return &review.UserError{Msg: "There is no review label " + strconv.Quote(name)}
			}
			grants[l] = r.FormValue("grant:"+name) != ""
		}
		return s.review.Reply(r.Context(), t.cl, principal(r), grants, r.FormValue("comment"))
	})
}

func (s *Server) clSubmit(stack bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.act(w, r, func(t *clTarget) error {
			return s.review.Submit(r.Context(), t.repo, t.cl, t.scm, principal(r), stack)
		})
	}
}

func (s *Server) clRequestReview(w http.ResponseWriter, r *http.Request) {
	s.act(w, r, func(t *clTarget) error {
		if err := r.ParseForm(); err != nil {
			return &review.UserError{Msg: "That request could not be read"}
		}
		return s.review.RequestReview(r.Context(), t.repo, t.cl, principal(r), formReviewers(r), r.FormValue("message"))
	})
}

func (s *Server) clAddReviewers(w http.ResponseWriter, r *http.Request) {
	s.act(w, r, func(t *clTarget) error {
		if err := r.ParseForm(); err != nil {
			return &review.UserError{Msg: "That request could not be read"}
		}
		return s.review.AddReviewers(r.Context(), t.repo, t.cl, principal(r), formReviewers(r), r.FormValue("message"))
	})
}

func (s *Server) clRemoveReviewer(w http.ResponseWriter, r *http.Request) {
	s.act(w, r, func(t *clTarget) error {
		return s.review.RemoveReviewer(r.Context(), t.repo, t.cl, principal(r), r.URL.Query().Get("principal"))
	})
}

func (s *Server) clReturnToDraft(w http.ResponseWriter, r *http.Request) {
	s.act(w, r, func(t *clTarget) error { return s.review.ReturnToDraft(r.Context(), t.repo, t.cl, principal(r)) })
}

func (s *Server) clCancelSubmit(w http.ResponseWriter, r *http.Request) {
	s.act(w, r, func(t *clTarget) error {
		return s.review.CancelSubmit(r.Context(), t.repo, t.cl, principal(r))
	})
}

// timed is a change-log entry at its time.
type timed struct {
	at  time.Time
	msg pages.CLMessage
}

// submissionMessages are a submission's entries in a CL's change log: its submitting, and — when
// it did not land (the landing is the CL's own entry) — how it ended.
func submissionMessages(ctx context.Context, sub *automationpb.Submission, number int64) []timed {
	var ps int64
	for _, c := range sub.GetCls() {
		if c.GetNumber() == number {
			ps = int64(c.GetPatchset())
		}
	}
	action := "sent it to the landing queue"
	if len(sub.GetCls()) > 1 {
		action = "sent its stack to the landing queue"
	}
	m := pages.CLMessage{Author: people.Handle(ctx, sub.GetSubmittedById()), Action: action, PatchSet: ps}
	m.Age, m.AgeTitle = age(sub.GetCreatedAt().AsTime())
	out := []timed{{sub.GetCreatedAt().AsTime(), m}}
	var end pages.CLMessage
	switch sub.GetStatus() {
	case automationpb.Submission_STATUS_FAILED:
		end = pages.CLMessage{Author: "ok", Action: "could not land it", Body: upperFirst(sub.GetReason()), PatchSet: ps}
	case automationpb.Submission_STATUS_CANCELED:
		end = pages.CLMessage{Author: "ok", Action: "stopped submitting it", Body: upperFirst(people.Reason(ctx, sub.GetReason(), sub.GetCanceledById())), PatchSet: ps}
	default:
		return out
	}
	end.Age, end.AgeTitle = age(sub.GetFinishedAt().AsTime())
	return append(out, timed{sub.GetFinishedAt().AsTime(), end})
}

func (s *Server) clAbandon(w http.ResponseWriter, r *http.Request) {
	s.act(w, r, func(t *clTarget) error {
		return s.review.Abandon(r.Context(), t.repo, t.cl, principal(r), r.FormValue("reason"))
	})
}

func (s *Server) clRestore(w http.ResponseWriter, r *http.Request) {
	s.act(w, r, func(t *clTarget) error { return s.review.Restore(r.Context(), t.repo, t.cl, principal(r)) })
}

// clRollback rolls back a submitted CL, then shows the rollback CL it made; a refusal is the
// CL's notice.
func (s *Server) clRollback(w http.ResponseWriter, r *http.Request) {
	t, err := s.loadCL(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	number, err := s.review.Rollback(r.Context(), t.cl, principal(r), r.FormValue("reason"))
	if err != nil {
		if !review.IsUserError(err) {
			fail(w, r, err)
			return
		}
		s.serveCL(w, r, t, err.Error())
		return
	}
	pages.Navigate(w, r, clHref(t.repo.GetName(), number))
}

// clComment posts a new file or inline comment on the patchset it was written on, or a
// reply to a thread.
func (s *Server) clComment(w http.ResponseWriter, r *http.Request) {
	s.act(w, r, func(t *clTarget) error {
		ps := t.cl.CurrentPatchset
		if n, err := strconv.ParseInt(r.FormValue("ps"), 10, 32); err == nil {
			ps = int32(n)
		}
		line, _ := strconv.ParseInt(r.FormValue("line"), 10, 32)
		parent, _ := strconv.ParseInt(r.FormValue("parent"), 10, 64)
		side := db.SideNone
		switch r.FormValue("side") {
		case "left":
			side = db.SideBase
		case "right":
			side = db.SidePatchset
		}
		return s.review.Comment(r.Context(), t.cl, principal(r), ps, r.FormValue("file"), int32(line), side, parent, r.FormValue("content"))
	})
}

func (s *Server) clResolve(resolved bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.act(w, r, func(t *clTarget) error {
			id, _ := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
			return s.review.Resolve(r.Context(), t.cl, id, resolved)
		})
	}
}

func (s *Server) clDeleteComment(w http.ResponseWriter, r *http.Request) {
	s.act(w, r, func(t *clTarget) error {
		id, _ := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
		return s.review.DeleteComment(r.Context(), t.cl, principal(r), id)
	})
}

// clReviewerSearch answers the review dialog's search with people who could review the
// CL, excluding those the dialog already has and the CL's reviewers.
func (s *Server) clReviewerSearch(w http.ResponseWriter, r *http.Request) {
	t, err := s.loadCL(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	if err := r.ParseForm(); err != nil {
		pages.ServeError(w, r, http.StatusBadRequest, "Can't Do That", "That search could not be read.")
		return
	}
	exclude := formReviewers(r)
	rs, err := s.db.Reviewers(r.Context(), t.cl.Repo, t.cl.Number)
	if err != nil {
		fail(w, r, err)
		return
	}
	for _, rv := range rs {
		exclude = append(exclude, rv.Principal)
	}
	keyword := strings.TrimSpace(r.FormValue("q"))
	found, err := s.review.SearchReviewers(r.Context(), t.cl, keyword, exclude)
	if err != nil {
		fail(w, r, err)
		return
	}
	var out []pages.CLCandidate
	for _, p := range found {
		out = append(out, pages.CLCandidate{Principal: p, Name: people.Handle(r.Context(), p)})
	}
	pages.ServeCLReviewerResults(w, r, keyword != "", out)
}
