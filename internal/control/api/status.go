package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/Cidan/ok/internal/control/checks"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	"github.com/Cidan/ok/internal/owners"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	controlpb "github.com/Cidan/ok/proto/ok/control/v1"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// What the status names when ok could not read a part of it.
const (
	unavailableAutomation = "ok automation"
	unavailableDeployer   = "the deployer"
)

// logTailLines is how much of a failed check's log its failure carries.
const (
	logTailLines = 40
	logTailBytes = 16 << 10
)

// target is a CL as one request reads it.
type target struct {
	repo *scmpb.Repository
	cl   db.ChangeList
	scm  *scmpb.ChangeList
}

// load reads the CL a request names, fresh from the SCM.
func (a *API) load(ctx context.Context, ref *controlpb.CLRef) (*target, error) {
	if ref.GetRepository() == "" || ref.GetNumber() < 1 {
		return nil, invalid("name a CL: its repository and number")
	}
	repo, err := a.scm.Repositories.GetRepository(ctx, &scmpb.GetRepositoryRequest{
		Key: &scmpb.GetRepositoryRequest_Name{Name: ref.GetRepository()},
	})
	if err != nil {
		return nil, err
	}
	cl, c, err := a.review.ChangeList(ctx, repo.GetId(), ref.GetNumber())
	if err != nil {
		return nil, err
	}
	return &target{repo: repo, cl: cl, scm: c}, nil
}

// readAttempts bounds how many times a status is read again because ok moved while it was read.
const readAttempts = 3

// statusOf reads where a CL stands from one state of ok. Its parts come from three services, read
// one after another, so a landing between two reads would pair a CL read before it with a
// submission read after: an open CL whose submission has landed, judged against a head of main the
// landing replaced. Such a status is read again (settled), up to readAttempts times.
func (a *API) statusOf(ctx context.Context, ref *controlpb.CLRef, bc *blockedCache) (*controlpb.CLStatus, error) {
	var st *controlpb.CLStatus
	for attempt := 0; attempt < readAttempts; attempt++ {
		t, err := a.load(ctx, ref)
		if err != nil {
			return nil, err
		}
		if st, err = a.status(ctx, t, bc); err != nil {
			return nil, err
		}
		now, err := a.scm.Repositories.GetRepository(ctx, &scmpb.GetRepositoryRequest{
			Key: &scmpb.GetRepositoryRequest_Id{Id: t.repo.GetId()},
		})
		if err != nil {
			return nil, err
		}
		if settled(st, t.repo.GetHead(), now.GetHead()) {
			return st, nil
		}
	}
	return st, nil
}

// settled reports whether a status was read from one state of ok: main did not move while it was
// read, and no landing its submission reports is missing from the CL it read. ok scm lands a CL and
// moves main in one transaction, and ok automation records a landing only after ok scm has
// committed it, so a CL read after a submission that says it landed always shows it.
func settled(st *controlpb.CLStatus, headBefore, headAfter []byte) bool {
	if !bytes.Equal(headBefore, headAfter) {
		return false
	}
	return st.GetPost().GetStatus() != controlpb.Post_STATUS_LANDED || st.GetCl().GetStatus() == controlpb.CL_STATUS_SUBMITTED
}

// blockedCache remembers what keeps a CL from submitting, per patchset and main's head: the dry
// run behind it is the SCM's work, and a watch asks every few seconds.
type blockedCache struct {
	key, reason string
}

// status reads where t stands. bc, when not nil, carries the submit gates' answer across a
// watch's reads.
func (a *API) status(ctx context.Context, t *target, bc *blockedCache) (*controlpb.CLStatus, error) {
	st := &controlpb.CLStatus{Cl: a.clOut(t)}
	active := t.cl.Status.Active()

	rv, ev, err := a.reviewOut(ctx, t)
	if err != nil {
		return nil, err
	}
	st.Review = rv

	pre, err := a.runs.GetPre(ctx, &automationpb.GetPreRequest{RepositoryId: t.repo.GetId(), ClNumber: t.cl.Number, Patchset: t.cl.CurrentPatchset})
	if err != nil {
		a.unavailable(ctx, st, unavailableAutomation, "reading pre", t, err)
		st.Pre = &controlpb.Checks{Patchset: t.cl.CurrentPatchset}
	} else {
		st.Pre = a.preOut(t, pre)
	}

	subs, err := a.runs.ListSubmissions(ctx, &automationpb.ListSubmissionsRequest{RepositoryId: t.repo.GetId(), ClNumber: t.cl.Number, Limit: 1})
	if err != nil {
		a.unavailable(ctx, st, unavailableAutomation, "reading submissions", t, err)
	} else if len(subs.GetSubmissions()) > 0 {
		st.Post = a.postOut(t, subs.GetSubmissions()[0])
	}

	if t.cl.Status == db.StatusSubmitted && len(t.cl.LandedCommit) > 0 {
		resp, err := a.deploys.ListDeployments(ctx, &deploypb.ListDeploymentsRequest{RepositoryId: t.repo.GetId(), CommitId: t.cl.LandedCommit, Limit: 500})
		if err != nil {
			a.unavailable(ctx, st, unavailableDeployer, "listing deployments", t, err)
		} else {
			st.Deployments = a.deploymentsOut(t, resp)
		}
	}

	// What still keeps an approved CL with a passing pre from submitting — a conflict with main,
	// say — as its author would meet it.
	if active && ev != nil && ev.Status.Satisfied() && st.GetPre().GetVerdict() == controlpb.Checks_VERDICT_PASSED &&
		t.cl.Status == db.StatusOpen && st.GetPost().GetStatus() != controlpb.Post_STATUS_ACTIVE {
		key := strconv.Itoa(int(t.cl.CurrentPatchset)) + "@" + scm.Hex(t.repo.GetHead())
		if bc != nil && bc.key == key {
			rv.Blocked = bc.reason
		} else {
			blocked, err := a.review.Blocked(ctx, t.repo, t.cl, t.scm, t.cl.Author, false)
			if err != nil {
				return nil, err
			}
			rv.Blocked = blocked
			if bc != nil {
				bc.key, bc.reason = key, blocked
			}
		}
	}

	derive(st, a.admins(ctx, t.repo))
	a.failure(ctx, t, st)
	return st, nil
}

func (a *API) unavailable(ctx context.Context, st *controlpb.CLStatus, what, doing string, t *target, err error) {
	if ctx.Err() == nil {
		slog.Warn("ok control: status: "+doing, "repository", t.repo.GetName(), "cl", t.cl.Number, "err", err)
	}
	if !slices.Contains(st.Unavailable, what) {
		st.Unavailable = append(st.Unavailable, what)
	}
}

func (a *API) clOut(t *target) *controlpb.CL {
	out := &controlpb.CL{
		Repository:  t.repo.GetName(),
		Number:      t.cl.Number,
		Title:       t.cl.Title(),
		Author:      people.Handle(t.cl.Author),
		Status:      controlpb.CL_Status(t.cl.Status),
		Patchset:    t.cl.CurrentPatchset,
		ChangeId:    scm.ChangeID(t.cl.ChangeID),
		Parent:      t.cl.ParentNumber,
		Rollback:    len(t.cl.RollsBack) > 0,
		Url:         a.clURL(t.repo.GetName(), t.cl.Number),
		SubmittedBy: people.Handle(t.cl.SubmittedBy),
	}
	if p := scm.Patchset(t.scm, t.cl.CurrentPatchset); p != nil {
		out.CommitId = scm.Hex(p.GetCommitId())
	}
	if len(t.cl.LandedCommit) > 0 {
		out.LandedCommit = scm.Hex(t.cl.LandedCommit)
	}
	return out
}

// reviewOut is the CL's review against its current patchset, evaluated live while it is under
// review; a closed CL's is its reviewers and threads alone.
func (a *API) reviewOut(ctx context.Context, t *target) (*controlpb.Review, *review.Evaluation, error) {
	rv := &controlpb.Review{Approved: t.cl.Approved}
	reviewers, err := db.Reviewers(ctx, a.db.Q(), t.repo.GetId(), t.cl.Number)
	if err != nil {
		return nil, nil, err
	}
	for _, r := range reviewers {
		rv.Reviewers = append(rv.Reviewers, people.Handle(r.Principal))
	}
	comments, err := db.Comments(ctx, a.db.Q(), t.repo.GetId(), t.cl.Number)
	if err != nil {
		return nil, nil, err
	}
	replies := map[int64]int32{}
	for _, c := range comments {
		if c.ParentID != 0 {
			replies[c.ParentID]++
		}
	}
	// A thread is a comment on a file or a line; a message on the CL itself resolves nothing
	// (db.UnresolvedThreads, which review counts).
	for _, c := range comments {
		if c.ParentID == 0 && c.FilePath != "" && !c.Resolved {
			rv.Unresolved = append(rv.Unresolved, &controlpb.Thread{Id: c.ID, Author: people.Handle(c.Author), File: c.FilePath,
				Line: c.Line, Body: c.Body, Patchset: c.Patchset, Replies: replies[c.ID]})
		}
	}
	if !t.cl.Status.Active() {
		return rv, nil, nil
	}
	ev, err := a.review.Evaluate(ctx, t.cl, t.scm, t.repo.GetHead())
	if err != nil {
		return nil, nil, err
	}
	s := ev.Status
	rv.Approved = s.Satisfied()
	rv.Lgtm = s.LGTMSatisfied
	rv.SelfLgtmAllowed = s.SelfLGTMAllowed
	for _, p := range s.LGTMBy {
		rv.LgtmBy = append(rv.LgtmBy, people.Handle(p))
	}
	for _, o := range s.Owners {
		set := &controlpb.OwnerSet{Satisfied: o.Satisfied, Implicit: o.Implicit}
		if o.Set != nil {
			set.Config, set.Rules, set.Files = o.Set.File, o.Set.Rules, int32(len(o.Set.Files))
			for _, w := range o.Set.Owners {
				set.Owners = append(set.Owners, ownerOut(w))
			}
		}
		for _, p := range o.ApprovedBy {
			set.ApprovedBy = append(set.ApprovedBy, people.Handle(p))
		}
		rv.OwnerSets = append(rv.OwnerSets, set)
	}
	for _, r := range s.Identity {
		if !r.Accepted {
			rv.Identity = append(rv.Identity, "Approval from "+r.ACL+" to "+r.What)
		}
	}
	switch {
	case s.OwnersErr != nil:
		rv.Error = s.OwnersErr.Error()
	case s.IdentityErr != nil:
		rv.Error = s.IdentityErr.Error()
	}
	return rv, ev, nil
}

// ownerOut writes an owner as OWNERS does.
func ownerOut(o owners.Owner) string { return o.String() }

func (a *API) preOut(t *target, pre *automationpb.Pre) *controlpb.Checks {
	out := &controlpb.Checks{Patchset: t.cl.CurrentPatchset, CommitId: scm.Hex(pre.GetCommitId())}
	switch pre.GetVerdict() {
	case automationpb.Pre_VERDICT_PASSED:
		out.Verdict = controlpb.Checks_VERDICT_PASSED
	case automationpb.Pre_VERDICT_FAILED:
		out.Verdict = controlpb.Checks_VERDICT_FAILED
	case automationpb.Pre_VERDICT_PENDING:
		out.Verdict = controlpb.Checks_VERDICT_PENDING
	default:
		out.Verdict = controlpb.Checks_VERDICT_NONE
	}
	if pre.GetPlan() != nil {
		out.Runs = append(out.Runs, a.checkOut(t, pre.GetPlan()))
	}
	for _, r := range pre.GetJobs() {
		out.Runs = append(out.Runs, a.checkOut(t, r))
	}
	out.Passed, out.Total = tally(out.Runs)
	return out
}

func (a *API) postOut(t *target, sub *automationpb.Submission) *controlpb.Post {
	p := &controlpb.Post{Submission: sub.GetId(), Status: controlpb.Post_Status(sub.GetStatus()),
		SubmittedBy: people.Handle(sub.GetSubmittedBy()), Note: sub.GetNote(), Reason: sub.GetReason()}
	for _, c := range sub.GetCls() {
		if c.GetNumber() == t.cl.Number {
			p.Patchset = c.GetPatchset()
		}
	}
	att := checks.Counting(sub)
	p.Attempt = attemptWord(att)
	if c := checks.Candidate(att, t.cl.Number); c != nil {
		if c.GetPlan() != nil {
			p.Runs = append(p.Runs, a.checkOut(t, c.GetPlan()))
		}
		for _, r := range c.GetJobs() {
			p.Runs = append(p.Runs, a.checkOut(t, r))
		}
	}
	p.Passed, p.Total = tally(p.Runs)
	return p
}

// attemptWord is where an attempt stands, in a word.
func attemptWord(at *automationpb.Attempt) string {
	if at == nil {
		return ""
	}
	switch at.GetStatus() {
	case automationpb.Attempt_STATUS_LANDED:
		return "landed"
	case automationpb.Attempt_STATUS_DISCARDED:
		return "discarded"
	case automationpb.Attempt_STATUS_FAILED:
		return "failed"
	}
	switch at.GetVerdict() {
	case automationpb.Attempt_VERDICT_PLANNING:
		return "planning"
	case automationpb.Attempt_VERDICT_WAITING:
		return "waiting"
	case automationpb.Attempt_VERDICT_TESTING:
		return "testing"
	case automationpb.Attempt_VERDICT_PASSED:
		return "passed"
	}
	return "failed"
}

func (a *API) checkOut(t *target, r *automationpb.Run) *controlpb.Check {
	name, source := checks.JobName(r.GetOrigin().GetJob())
	c := &controlpb.Check{Run: r.GetId(), Name: name, Source: source, Kind: kindWord(r.GetJob().GetKind()),
		Targets: r.GetJob().GetTargets(), Status: controlpb.Check_Status(r.GetStatus()), Reason: r.GetReason(),
		Url: a.runURL(t.repo.GetName(), t.cl.Number, r.GetId()), CarriedFrom: r.GetCarriedFrom(), Why: r.GetWhy()}
	if from := r.GetCarriedFrom(); from > 0 {
		c.Url = a.runURL(t.repo.GetName(), t.cl.Number, from)
	}
	return c
}

func kindWord(k automationpb.Job_Kind) string {
	return strings.ToLower(strings.TrimPrefix(k.String(), "KIND_"))
}

func tally(runs []*controlpb.Check) (passed, total int32) {
	for _, r := range runs {
		total++
		if r.GetStatus() == controlpb.Check_STATUS_PASSED {
			passed++
		}
	}
	return passed, total
}

// deploymentsOut are each deploy's latest deployment covering the commit, newest first: a
// deployment a newer commit replaced is covered by its replacement, which decides the deploy.
func (a *API) deploymentsOut(t *target, resp *deploypb.ListDeploymentsResponse) *controlpb.Deployments {
	out := &controlpb.Deployments{Planned: resp.GetPlanned()}
	latest := map[string]*deploypb.Deployment{}
	for _, d := range resp.GetDeployments() {
		if cur, ok := latest[d.GetDeploy()]; !ok || d.GetId() > cur.GetId() {
			latest[d.GetDeploy()] = d
		}
	}
	ds := make([]*deploypb.Deployment, 0, len(latest))
	for _, d := range latest {
		ds = append(ds, d)
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i].GetId() > ds[j].GetId() })
	for _, d := range ds {
		item := &controlpb.Deployment{Id: d.GetId(), Name: d.GetName(), Status: controlpb.Deployment_Status(d.GetStatus()),
			Stage: strings.ToLower(strings.TrimPrefix(d.GetStage().String(), "STAGE_")), Reason: d.GetReason(),
			Url: a.deploymentURL(t.repo.GetName(), d.GetId(), t.cl.Number)}
		if w := d.GetWorkload(); w != nil {
			item.Workload = w.GetNamespace() + "/" + w.GetName()
		}
		out.Items = append(out.Items, item)
	}
	return out
}

// failure fills in what failed, at a stage that failed: the first check of pre or post that did
// not pass, with its log's last lines, or the deployment that failed.
func (a *API) failure(ctx context.Context, t *target, st *controlpb.CLStatus) {
	var f *controlpb.Failure
	switch st.GetStage() {
	case controlpb.Stage_STAGE_PRE_FAILED:
		f = failedCheck("pre", st.GetPre().GetRuns())
	case controlpb.Stage_STAGE_POST_FAILED:
		f = failedCheck("post", st.GetPost().GetRuns())
		if f == nil {
			f = &controlpb.Failure{What: "submission", Reason: st.GetPost().GetReason(), Url: st.GetCl().GetUrl()}
		}
	case controlpb.Stage_STAGE_DEPLOY_FAILED:
		for _, d := range st.GetDeployments().GetItems() {
			switch d.GetStatus() {
			case controlpb.Deployment_STATUS_FAILED, controlpb.Deployment_STATUS_HALTED, controlpb.Deployment_STATUS_CANCELED:
				f = &controlpb.Failure{What: "deployment", Name: d.GetName(), Deployment: d.GetId(), Reason: d.GetReason(), Url: d.GetUrl()}
			}
			if f != nil {
				break
			}
		}
	}
	if f == nil {
		return
	}
	if f.GetRun() > 0 {
		tail, err := a.logTail(ctx, f.GetRun())
		if err != nil {
			a.unavailable(ctx, st, unavailableAutomation, "reading a failed run's log", t, err)
		}
		f.LogTail = tail
	}
	st.Failure = f
}

// failedCheck is the first check that did not pass and is over, or nil.
func failedCheck(what string, runs []*controlpb.Check) *controlpb.Failure {
	for _, r := range runs {
		switch r.GetStatus() {
		case controlpb.Check_STATUS_FAILED, controlpb.Check_STATUS_ERROR, controlpb.Check_STATUS_CANCELED:
			run := r.GetRun()
			if r.GetCarriedFrom() > 0 {
				run = r.GetCarriedFrom()
			}
			return &controlpb.Failure{What: what, Name: r.GetName(), Run: run, Reason: r.GetReason(), Url: r.GetUrl()}
		}
	}
	return nil
}

// logTail is the last lines of a run's log.
func (a *API) logTail(ctx context.Context, id int64) (string, error) {
	run, err := a.runs.GetRun(ctx, &automationpb.GetRunRequest{Id: id})
	if err != nil {
		return "", err
	}
	offset := max(run.GetLogSize()-logTailBytes, 0)
	stream, err := a.runs.ReadLog(ctx, &automationpb.ReadLogRequest{Id: id, Offset: offset})
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
		b.Write(chunk.GetData())
		if chunk.GetEnd() {
			break
		}
	}
	lines := strings.Split(strings.TrimRight(strings.ToValidUTF8(b.String(), "�"), "\n"), "\n")
	if offset > 0 && len(lines) > 1 {
		lines = lines[1:] // the first is cut at the offset
	}
	if len(lines) > logTailLines {
		lines = lines[len(lines)-logTailLines:]
	}
	return strings.Join(lines, "\n"), nil
}

// admins are the repository's admins, by handle: its creator, and the operators.
func (a *API) admins(ctx context.Context, repo *scmpb.Repository) []string {
	var out []string
	for _, p := range a.review.Admins(ctx, repo) {
		out = append(out, people.Handle(p))
	}
	return out
}

func (a *API) clURL(repo string, number int64) string {
	return a.base + "/" + repo + "/cl/" + strconv.FormatInt(number, 10)
}

func (a *API) runURL(repo string, number, run int64) string {
	return a.clURL(repo, number) + "/run/" + strconv.FormatInt(run, 10)
}

func (a *API) deploymentURL(repo string, id, number int64) string {
	return a.base + "/" + repo + "/deploy/" + strconv.FormatInt(id, 10) + "?cl=" + strconv.FormatInt(number, 10)
}
