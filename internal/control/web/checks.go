package web

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/control/checks"
	"github.com/Cidan/ok/internal/control/db"
	pages "github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// logTail is how much of a run's log its page shows: the end of a longer one (Raw has it all).
const logTail = 1 << 20

// submissions are a CL's submissions to the landing queue, newest first, or why ok automation
// could not say.
type submissions struct {
	list []*automationpb.Submission
	err  error
}

// latest is the newest submission, or nil.
func (ss submissions) latest() *automationpb.Submission {
	if len(ss.list) == 0 {
		return nil
	}
	return ss.list[0]
}

// maxSubmissions bounds the submissions a CL's page reads: its change log shows each.
const maxSubmissions = 50

// submissions reads a CL's submissions from ok automation.
func (s *Server) submissions(ctx context.Context, repo *scmpb.Repository, number int64) submissions {
	resp, err := s.runs.ListSubmissions(ctx, &automationpb.ListSubmissionsRequest{RepositoryId: repo.GetId(), ClNumber: number, Limit: maxSubmissions})
	if err != nil && ctx.Err() == nil {
		slog.Warn("ok control: reading submissions", "repository", repo.GetName(), "cl", number, "err", err)
	}
	return submissions{list: resp.GetSubmissions(), err: err}
}

// checks is the Checks tab: a patchset's pre and the CL's post, read from ok automation, with
// the viewer's actions on each run. Automation being unreachable is the tab's own state, never the
// page's failure.
func (s *Server) checks(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, patchset int32, me string, subs submissions) pages.CLChecks {
	c := s.preChecks(ctx, repo, cl, patchset, me)
	c.Post = s.post(ctx, repo, cl, subs)
	c.Live = c.Live || c.Post.Status == "Submitting"
	return c
}

// preChecks is a patchset's pre, as the Checks tab shows it.
func (s *Server) preChecks(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, patchset int32, me string) pages.CLChecks {
	link := clHref(repo.GetName(), cl.Number)
	c := pages.CLChecks{Patchset: int64(patchset), Href: link + "/checks?ps=" + strconv.Itoa(int(patchset))}
	pre, err := s.runs.GetPre(ctx, &automationpb.GetPreRequest{RepositoryId: repo.GetId(), ClNumber: cl.Number, Patchset: patchset})
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("ok control: reading pre", "repository", repo.GetName(), "cl", cl.Number, "patchset", patchset, "err", err)
		}
		c.Verdict, c.VerdictKind = "Unavailable", "unavailable"
		c.Unavailable = "ok could not read this patchset's checks. Reload to try again."
		return c
	}
	current := patchset == cl.CurrentPatchset && cl.Status.Active()
	switch pre.GetVerdict() {
	case automationpb.Pre_VERDICT_PASSED:
		c.Verdict, c.VerdictKind = "Passed", "passed"
	case automationpb.Pre_VERDICT_FAILED:
		c.Verdict, c.VerdictKind = "Failed", "failed"
	case automationpb.Pre_VERDICT_PENDING:
		c.Verdict, c.VerdictKind, c.Live = "Running", "running", true
	default:
		// Not planned yet: the change feed plans the current patchset of a CL in review at its
		// stack's top shortly. A draft runs none until it is sent for review, and a CL under the
		// top none of its own: either may have it run by hand (docs/automation.md, "pre").
		c.Verdict, c.VerdictKind, c.Live = "Not Planned", "none", current
		if !current {
			break
		}
		at, err := db.PreAt(ctx, s.db, repo.GetId(), cl.Number)
		if err != nil {
			slog.Warn("ok control: reading a CL's stack", "repository", repo.GetName(), "cl", cl.Number, "err", err)
			break
		}
		switch {
		case at != 0:
			c.NotRun = fmt.Sprintf("Pre runs at CL %d, the top of this changelist's stack, over every changelist of it.", at)
		case cl.Status == db.StatusDraft:
			c.NotRun = "A draft runs no pre until it is sent for review."
		}
		if c.NotRun != "" {
			c.Live = false
			if s.review.May(ctx, repo, me, authz.Land) {
				c.RunPreHref = link + "/run-pre"
			}
		}
	}
	if pre.GetPlan() == nil {
		return c
	}
	c.Planned = pre.GetPlan().GetStatus() == automationpb.Run_STATUS_PASSED
	manage := s.review.May(ctx, repo, me, authz.Land)
	c.Runs = append(c.Runs, s.checkRow(ctx, repo, cl, pre.GetPlan(), manage, current, c.Planned))
	for _, r := range pre.GetJobs() {
		c.Runs = append(c.Runs, s.checkRow(ctx, repo, cl, r, manage, current, false))
	}
	for _, sk := range pre.GetSkipped() {
		name, source := jobName(sk.GetJob())
		c.Skipped = append(c.Skipped, pages.CLSkippedCheck{Name: name, Source: source, Target: sk.GetTarget(), Why: sk.GetWhy()})
	}
	return c
}

// post is the CL's post, from its latest submission: where it stands, and its runs on the attempt
// that counts — the one testing, else the one it landed or failed on, else the newest.
func (s *Server) post(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, subs submissions) pages.CLPost {
	if subs.err != nil {
		return pages.CLPost{Status: "Unavailable", StatusKind: "unavailable",
			Unavailable: "ok could not read this change's submissions. Reload to try again."}
	}
	sub := subs.latest()
	if sub == nil {
		return pages.CLPost{}
	}
	p := pages.CLPost{Submitted: true, SubmittedBy: people.Handle(ctx, sub.GetSubmittedById())}
	p.Age, p.AgeTitle = age(sub.GetCreatedAt().AsTime())
	switch sub.GetStatus() {
	case automationpb.Submission_STATUS_ACTIVE:
		p.Status, p.StatusKind, p.Detail = "Submitting", "running", upperFirst(sub.GetNote())
		if p.Detail == "" {
			p.Detail = "Waiting for the landing queue"
		}
	case automationpb.Submission_STATUS_LANDED:
		p.Status, p.StatusKind = "Landed", "passed"
	case automationpb.Submission_STATUS_FAILED:
		p.Status, p.StatusKind, p.Detail = "Failed", "failed", upperFirst(sub.GetReason())
	default:
		p.Status, p.StatusKind, p.Detail = "Canceled", "canceled", upperFirst(people.Reason(ctx, sub.GetReason(), sub.GetCanceledById()))
	}
	attempts := sub.GetAttempts()
	for i, a := range attempts {
		ca := pages.CLAttempt{Number: len(attempts) - i, Base: scm.Short(a.GetBase()), Reason: upperFirst(a.GetReason())}
		var on []string
		for _, sp := range a.GetChain() {
			on = append(on, clNames(sp.GetClNumbers()))
		}
		ca.OnTopOf = strings.Join(on, " and ")
		ca.Status, ca.StatusKind = attemptStatus(a)
		p.Attempts = append(p.Attempts, ca)
	}
	if c := checks.Candidate(checks.Counting(sub), cl.Number); c != nil {
		if c.GetPlan() != nil {
			p.Runs = append(p.Runs, postRow(ctx, repo, cl, c.GetPlan()))
		}
		for _, r := range c.GetJobs() {
			p.Runs = append(p.Runs, postRow(ctx, repo, cl, r))
		}
		for _, sk := range c.GetSkipped() {
			name, source := jobName(sk.GetJob())
			p.Skipped = append(p.Skipped, pages.CLSkippedCheck{Name: name, Source: source, Target: sk.GetTarget(), Why: sk.GetWhy()})
		}
	}
	return p
}

// postRow is one post run: its action is the submission's, so it is always disabled, saying so.
// A run that carries pre's pass never ran: its log is the pre run's.
func postRow(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, r *automationpb.Run) pages.CLCheck {
	c := runCheck(ctx, r)
	c.LogHref = runHref(repo.GetName(), cl.Number, r.GetId())
	if from := r.GetCarriedFrom(); from > 0 {
		c.LogHref = runHref(repo.GetName(), cl.Number, from)
	}
	if finished(r) {
		c.ActionLabel, c.ActionTitle = "Rerun", "A post run is its submission's: submit the change again"
	} else {
		c.ActionLabel, c.ActionTitle = "Cancel", "A post run is its submission's: cancel the submission"
	}
	return c
}

// attemptStatus words where an attempt stands, for its chip.
func attemptStatus(a *automationpb.Attempt) (string, string) {
	switch a.GetStatus() {
	case automationpb.Attempt_STATUS_LANDED:
		return "Landed", "passed"
	case automationpb.Attempt_STATUS_DISCARDED:
		return "Discarded", "none"
	case automationpb.Attempt_STATUS_FAILED:
		return "Failed", "failed"
	}
	switch a.GetVerdict() {
	case automationpb.Attempt_VERDICT_PLANNING:
		return "Planning", "running"
	case automationpb.Attempt_VERDICT_WAITING:
		return "Waiting", "queued"
	case automationpb.Attempt_VERDICT_TESTING:
		return "Testing", "running"
	case automationpb.Attempt_VERDICT_PASSED:
		return "Passed", "passed"
	}
	return "Failed", "failed"
}

// clNames words CLs: "CL 12", "CLs 12 and 13", "CLs 12, 13, and 14".
func clNames(cls []int64) string {
	n := make([]string, len(cls))
	for i, c := range cls {
		n[i] = strconv.FormatInt(c, 10)
	}
	switch len(n) {
	case 0:
		return "no CLs"
	case 1:
		return "CL " + n[0]
	case 2:
		return "CLs " + n[0] + " and " + n[1]
	}
	return "CLs " + strings.Join(n[:len(n)-1], ", ") + ", and " + n[len(n)-1]
}

// preRequirement is Submit Requirements' Pre row: the current patchset's pre passed.
func preRequirement(c pages.CLChecks) pages.CLSubmitReq {
	return pages.CLSubmitReq{Label: "Pre", Satisfied: c.VerdictKind == "passed", Detail: c.Verdict}
}

// checkRow is one run on the Checks tab, with its one action — Cancel while it is queued or
// running, Rerun once it has finished — which only the CL's author or a repository admin may
// take, and which runs again only the checks of an active CL's current patchset, and not a plan
// whose jobs are queued.
func (s *Server) checkRow(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, r *automationpb.Run, manage, current, planned bool) pages.CLCheck {
	c := runCheck(ctx, r)
	c.LogHref = runHref(repo.GetName(), cl.Number, r.GetId())
	verb := "rerun"
	if !finished(r) {
		c.ActionLabel, verb = "Cancel", "cancel"
	} else {
		c.ActionLabel = "Rerun"
	}
	switch {
	case !manage:
		c.ActionTitle = fmt.Sprintf("Only someone who may land on this repository can %s this check", verb)
	case verb == "rerun" && !current:
		c.ActionTitle = "Only an active change's current patchset runs its checks again"
	case verb == "rerun" && planned:
		c.ActionTitle = "The patchset is planned: rerun its jobs instead"
	default:
		c.ActionHref = c.LogHref + "/" + verb
	}
	return c
}

// runCheck is a run as the Checks tab and its page show it.
func runCheck(ctx context.Context, r *automationpb.Run) pages.CLCheck {
	name, source := jobName(r.GetOrigin().GetJob())
	c := pages.CLCheck{Kind: checkKind(r.GetOrigin().GetJob()), Name: name, Source: source, Why: r.GetWhy(),
		Reason: people.Reason(ctx, r.GetReason(), r.GetCanceledById())}
	switch r.GetStatus() {
	case automationpb.Run_STATUS_QUEUED:
		c.StatusLabel, c.StatusKind = "Queued", "queued"
	case automationpb.Run_STATUS_RUNNING:
		c.StatusLabel, c.StatusKind = "Running", "running"
	case automationpb.Run_STATUS_PASSED:
		c.StatusLabel, c.StatusKind = "Passed", "passed"
	case automationpb.Run_STATUS_FAILED:
		c.StatusLabel, c.StatusKind = "Failed", "failed"
	case automationpb.Run_STATUS_CANCELED:
		c.StatusLabel, c.StatusKind = "Canceled", "canceled"
	default:
		c.StatusLabel, c.StatusKind = "Error", "error"
	}
	if r.GetCarriedFrom() > 0 {
		c.Duration = "In Pre"
		return c
	}
	if start := r.GetStartedAt(); start != nil {
		end := time.Now()
		if f := r.GetFinishedAt(); f != nil {
			end = f.AsTime()
		}
		if d := end.Sub(start.AsTime()); d >= 0 {
			c.Duration = d.Round(time.Second).String()
		}
	}
	return c
}

// checkKind is the group a job's runs show in on the Checks tab: a workflow job's, or the built-in
// job it is — the plan, the build, a test, or the lint.
func checkKind(job string) string {
	switch {
	case strings.HasPrefix(job, "//"):
		return pages.CheckWorkflow
	case strings.HasPrefix(job, "test "):
		return pages.CheckTest
	case job == "build" || job == "build since pre":
		return pages.CheckBuild
	case job == "lint":
		return pages.CheckLint
	}
	return pages.CheckPlan
}

// jobName splits a job's identity into its name and where it comes from: a workflow job's
// //<file>:<phase>:<name> is its name, from its file; anything else is built in.
func jobName(id string) (name, source string) { return checks.JobName(id) }

func finished(r *automationpb.Run) bool { return checks.Finished(r) }

func runHref(repo string, cl, id int64) string {
	return fmt.Sprintf("%s/run/%d", clHref(repo, cl), id)
}

// clChecks is the Checks tab's live region, for a patchset (default: the current one).
func (s *Server) clChecks(w http.ResponseWriter, r *http.Request) {
	t, err := s.loadCL(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	patchset := t.cl.CurrentPatchset
	if n, err := strconv.ParseInt(r.URL.Query().Get("ps"), 10, 32); err == nil && n >= 1 && n <= int64(t.cl.CurrentPatchset) {
		patchset = int32(n)
	}
	pages.ServeCLChecks(w, r, s.checks(r.Context(), t.repo, t.cl, patchset, principal(r), s.submissions(r.Context(), t.repo, t.cl.Number)))
}

// loadRun loads the CL a path names and the run of its checks it names: a run of another
// repository's or CL's is not found here.
func (s *Server) loadRun(r *http.Request) (*clTarget, *automationpb.Run, error) {
	t, err := s.loadCL(r)
	if err != nil {
		return nil, nil, err
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "run"), 10, 64)
	if err != nil || id < 1 {
		return nil, nil, review.ErrNotFound
	}
	run, err := s.runs.GetRun(r.Context(), &automationpb.GetRunRequest{Id: id})
	if err != nil {
		return nil, nil, err
	}
	if run.GetRepositoryId() != t.repo.GetId() || run.GetOrigin().GetClNumber() != t.cl.Number {
		return nil, nil, review.ErrNotFound
	}
	return t, run, nil
}

// runView is a run's page, its log read from offset.
func (s *Server) runView(ctx context.Context, t *clTarget, run *automationpb.Run) (pages.RunView, error) {
	repo := t.repo
	link := runHref(repo.GetName(), t.cl.Number, run.GetId())
	v := pages.RunView{
		Chrome:     chrome(repo, "main", repo.GetHead(), ""),
		CLNumber:   t.cl.Number,
		CLHref:     clHref(repo.GetName(), t.cl.Number),
		ID:         run.GetId(),
		Check:      runCheck(ctx, run),
		Patchset:   int64(run.GetOrigin().GetPatchset()),
		Commit:     scm.Short(run.GetCommitId()),
		Live:       !finished(run),
		StatusHref: link + "/status",
		RawHref:    link + "/raw",
		Queued:     when(run.GetQueuedAt()),
		Started:    when(run.GetStartedAt()),
		Finished:   when(run.GetFinishedAt()),
	}
	if run.GetWorker() != "" {
		v.Worker = run.GetWorker()
		if run.GetAttempt() > 1 {
			v.Worker += fmt.Sprintf(" (attempt %d)", run.GetAttempt())
		}
	}
	from := max(run.GetLogSize()-logTail, 0)
	l, err := s.readLog(ctx, repo.GetName(), t.cl.Number, run.GetId(), from)
	if err != nil {
		return v, err
	}
	l.Truncated = from > 0
	v.Log = l
	return v, nil
}

func when(t *timestamppb.Timestamp) string {
	if t == nil {
		return ""
	}
	return t.AsTime().UTC().Format("Jan 2, 2006 15:04:05 MST")
}

// ansi matches the terminal escape sequences a tool may print; a log shows its text.
var ansi = regexp.MustCompile("\x1b\\[[0-9;?]*[ -/]*[@-~]")

// readLog reads a run's log from offset to what is written so far, and — unless the run has
// finished and the log is whole — where to read the rest from.
func (s *Server) readLog(ctx context.Context, repo string, cl, id, offset int64) (pages.RunLog, error) {
	stream, err := s.runs.ReadLog(ctx, &automationpb.ReadLogRequest{Id: id, Offset: offset})
	if err != nil {
		return pages.RunLog{}, err
	}
	var b strings.Builder
	next, end := offset, false
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return pages.RunLog{}, err
		}
		b.Write(chunk.GetData())
		next = chunk.GetOffset() + int64(len(chunk.GetData()))
		end = end || chunk.GetEnd()
	}
	l := pages.RunLog{Text: ansi.ReplaceAllString(strings.ToValidUTF8(b.String(), "�"), "")}
	if !end {
		l.NextHref = fmt.Sprintf("%s/log?offset=%d", runHref(repo, cl, id), next)
	}
	return l, nil
}

// runPage is a run's page.
func (s *Server) runPage(w http.ResponseWriter, r *http.Request) {
	t, run, err := s.loadRun(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	v, err := s.runView(r.Context(), t, run)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeRunView(w, r, v)
}

// runStatus is a run page's status panel, which refreshes itself until the run ends.
func (s *Server) runStatus(w http.ResponseWriter, r *http.Request) {
	t, run, err := s.loadRun(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	v, err := s.runView(r.Context(), t, run)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeRunStatus(w, r, v)
}

// runLog is what a run's log gained from an offset.
func (s *Server) runLog(w http.ResponseWriter, r *http.Request) {
	t, run, err := s.loadRun(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil || offset < 0 {
		pages.ServeError(w, r, http.StatusBadRequest, "Can't Show That", "The log offset is not a number of bytes.")
		return
	}
	l, err := s.readLog(r.Context(), t.repo.GetName(), t.cl.Number, run.GetId(), offset)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeRunLog(w, r, l)
}

// runRaw is a run's whole log as plain text, under the same sandboxing as a file's raw bytes.
func (s *Server) runRaw(w http.ResponseWriter, r *http.Request) {
	_, run, err := s.loadRun(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	stream, err := s.runs.ReadLog(r.Context(), &automationpb.ReadLogRequest{Id: run.GetId()})
	if err != nil {
		fail(w, r, err)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("Content-Security-Policy", "default-src 'none'; sandbox")
	h.Set("X-Content-Type-Options", "nosniff")
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			slog.Warn("ok control: reading a run's log", "run", run.GetId(), "err", err)
			return
		}
		if _, err := w.Write(chunk.GetData()); err != nil {
			return
		}
	}
}

// runAction reruns or cancels a run of a CL's checks for the author or a repository admin, then
// shows the Checks tab of the run's patchset: a refusal meant for the person is not the page's
// failure, but the tab as it stands.
// clRunPre runs pre by hand on a CL's current patchset, for whoever may land on the repository,
// then shows its Checks tab: a refusal meant for the person is not the page's failure, but the
// tab as it stands.
func (s *Server) clRunPre(w http.ResponseWriter, r *http.Request) {
	t, err := s.loadCL(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	me := principal(r)
	if s.review.MayRunPre(r.Context(), t.repo, t.cl, me) == nil {
		_, err := s.runs.StartPre(r.Context(), &automationpb.StartPreRequest{RepositoryId: t.repo.GetId(), ClNumber: t.cl.Number, ForPersonId: me})
		if err != nil && status.Code(err) != codes.FailedPrecondition {
			fail(w, r, err)
			return
		}
	}
	pages.ServeCLChecks(w, r, s.checks(r.Context(), t.repo, t.cl, t.cl.CurrentPatchset, me, s.submissions(r.Context(), t.repo, t.cl.Number)))
}

func (s *Server) runAction(rerun bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t, run, err := s.loadRun(r)
		if err != nil {
			fail(w, r, err)
			return
		}
		me := principal(r)
		patchset := run.GetOrigin().GetPatchset()
		refused := s.review.MayCancelRuns(r.Context(), t.repo, t.cl, me)
		if rerun {
			refused = s.review.MayRerun(r.Context(), t.repo, t.cl, me, patchset)
		}
		if refused != nil {
			pages.ServeCLChecks(w, r, s.checks(r.Context(), t.repo, t.cl, patchset, me, s.submissions(r.Context(), t.repo, t.cl.Number)))
			return
		}
		if rerun {
			_, err = s.runs.RerunRun(r.Context(), &automationpb.RerunRunRequest{Id: run.GetId(), ForPersonId: me})
		} else {
			_, err = s.runs.CancelRun(r.Context(), &automationpb.CancelRunRequest{Id: run.GetId(), ForPersonId: me})
		}
		switch status.Code(err) {
		case codes.OK, codes.FailedPrecondition:
			// A run finished (or rerun already) by the time it was asked: the tab shows it.
		default:
			fail(w, r, err)
			return
		}
		pages.ServeCLChecks(w, r, s.checks(r.Context(), t.repo, t.cl, patchset, me, s.submissions(r.Context(), t.repo, t.cl.Number)))
	}
}
