package queue

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/automation/changes"
	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/work"
	"github.com/Cidan/ok/internal/workflow"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// SCM is what the runner uses of the SCM.
type SCM struct {
	ChangeLists  scmpb.ChangeListsClient
	Landing      scmpb.LandingClient
	Repositories scmpb.RepositoriesClient
	Browse       scmpb.BrowseClient
}

// Config tunes the runner.
type Config struct {
	// Depth bounds how many earlier submissions an attempt speculates on.
	Depth int
	// Idle bounds how long the runner waits for a notification between passes.
	Idle time.Duration
	// Grace is how old a candidate no attempt holds must be before it is discarded: one
	// prepared just now is recorded a moment later.
	Grace time.Duration
}

// DefaultConfig is the service's.
var DefaultConfig = Config{Depth: DefaultDepth, Idle: 10 * time.Second, Grace: 5 * time.Minute}

// Runner runs the landing queue: each repository's queue is keyed work (internal/work), done by
// one replica at a time and by every replica between them, which reads the queue, asks Decide
// what to do, and does it through the SCM and the store.
type Runner struct {
	store *store.Store
	scm   SCM
	cfg   Config
	log   *slog.Logger
}

// New builds a runner.
func New(s *store.Store, scm SCM, cfg Config, log *slog.Logger) *Runner {
	return &Runner{store: s, scm: scm, cfg: cfg, log: log}
}

// Run runs the queue until ctx ends: each repository's whenever something it acts on changes,
// and every repository with work in the queue at least every Idle.
func (r *Runner) Run(ctx context.Context) error {
	return (&work.Runner{
		Kind:  store.QueueWork,
		Store: r.store.Work(),
		Do:    r.repository,
		Watch: r.store.WatchQueue,
		Sweep: r.sweep,
		Idle:  r.cfg.Idle,
		Log:   r.log,
	}).Run(ctx)
}

// sweep marks the queue of every repository it has work in, to act on what no change announces:
// what time decides.
func (r *Runner) sweep(ctx context.Context) error {
	repos, err := r.store.QueueRepositories(ctx, time.Hour)
	if err != nil {
		return err
	}
	w := r.store.Work()
	for _, repo := range repos {
		if err := w.Mark(ctx, store.QueueWork, repo); err != nil {
			return err
		}
	}
	return nil
}

// maxSteps bounds one pass over a repository: each step lands at most one submission.
const maxSteps = 1000

// repository acts on a repository's queue until nothing lands, then discards the candidates no
// attempt holds.
func (r *Runner) repository(ctx context.Context, repo int64) error {
	for step := 0; step < maxSteps; step++ {
		rep, err := r.scm.Repositories.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}})
		if err != nil {
			return err
		}
		st, err := r.store.QueueState(ctx, repo, rep.GetHeadSeq())
		if err != nil {
			return err
		}
		acts := Decide(r.snapshot(st, rep))
		if len(acts) > 0 && (acts[0].Kind == Land || acts[0].Kind == Adopt) {
			// Landing moves main: everything else is decided again from the new head.
			if err := r.act(ctx, rep, st, acts[0]); err != nil {
				return err
			}
			continue
		}
		var errs []error
		for _, a := range acts {
			if err := r.act(ctx, rep, st, a); err != nil {
				errs = append(errs, err)
			}
		}
		if err := errors.Join(errs...); err != nil {
			return err
		}
		return r.collect(ctx, repo)
	}
	return fmt.Errorf("still landing after %d steps", maxSteps)
}

// snapshot is Decide's view of a repository's queue.
func (r *Runner) snapshot(st *store.QueueState, rep *scmpb.Repository) Snapshot {
	s := Snapshot{HeadSeq: rep.GetHeadSeq(), From: st.From, Depth: r.cfg.Depth, Finished: map[int64]Outcome{},
		FinishedCLs: map[int64][]int64{}, Landings: map[int64]*Footprint{}}
	for _, q := range st.Submissions {
		sub := Submission{ID: q.ID}
		for _, c := range q.CLs {
			sub.CLs = append(sub.CLs, c.Number)
			if _, ok := st.LandedCLs[c.Number]; ok {
				sub.Landed = append(sub.Landed, c.Number)
			}
		}
		for _, a := range q.Attempts {
			state, reason, fp := attemptState(a)
			sub.Attempts = append(sub.Attempts, Attempt{ID: a.ID, BaseSeq: a.BaseSeq, Chain: a.Chain,
				Active: a.Status == store.AttemptActive, State: state, Reason: reason, Footprint: fp})
		}
		s.Submissions = append(s.Submissions, sub)
	}
	for id, o := range st.Others {
		s.Finished[id] = Ended
		if o.Status == store.SubmissionLanded {
			s.Finished[id] = Landed
		}
		for _, c := range o.CLs {
			s.FinishedCLs[id] = append(s.FinishedCLs[id], c.Number)
		}
	}
	for _, m := range st.Main {
		s.Main = append(s.Main, MainCommit{Seq: m.Seq, CL: m.CLNumber, Submission: m.SubmissionID})
	}
	for id, a := range st.Landings {
		if _, _, fp := attemptState(a); fp != nil {
			s.Landings[id] = fp
		}
	}
	return s
}

// StateOf is where an attempt stands, and why it failed.
func StateOf(a *store.Attempt) (State, string) {
	st, reason, _ := attemptState(a)
	return st, reason
}

// attemptState is where an attempt stands, why it failed, and — once its plans are planned —
// its footprint.
func attemptState(a *store.Attempt) (State, string, *Footprint) {
	if a.PrepareError != "" {
		return Failed, a.PrepareError, nil
	}
	own := a.Own()
	planning := len(own) == 0
	for _, c := range own {
		switch {
		case c.PlanRun == nil || !c.PlanRun.Status.Finished():
			planning = true
		case c.PlanRun.Status != store.StatusPassed:
			return Failed, fmt.Sprintf("post's plan for CL %d %s", c.CLNumber, outcome(c.PlanRun)), nil
		}
	}
	if planning {
		return Planning, "", nil
	}
	fps := make([]*Footprint, 0, len(own))
	for _, c := range own {
		fps = append(fps, footprint(c.Plan))
	}
	fp := Merge(fps...)
	if !a.Released {
		return Planned, "", fp
	}
	testing := false
	for _, c := range own {
		for _, j := range c.Jobs {
			switch {
			case !j.Status.Finished():
				testing = true
			case j.Status != store.StatusPassed:
				return Failed, fmt.Sprintf("CL %d's %s %s", c.CLNumber, jobName(j.Origin.Job), outcome(j)), fp
			}
		}
	}
	if testing {
		return Testing, "", fp
	}
	return Passed, "", fp
}

// footprint is a post plan's change's footprint: its affected targets, what they build on, and
// the paths it touches.
func footprint(p *store.Plan) *Footprint {
	return NewFootprint(p.Affected, p.Upstream, p.Touched)
}

// outcome words how a run that did not pass ended.
func outcome(run *store.Run) string {
	var what string
	switch run.Status {
	case store.StatusFailed:
		what = "failed"
	case store.StatusCanceled:
		what = "was canceled"
	default:
		what = "could not run"
	}
	if run.Reason != "" {
		what += ": " + run.Reason
	}
	return what
}

// jobName is a job's name: a workflow job's //<file>:<phase>:<name> is its last part.
func jobName(id string) string {
	if strings.HasPrefix(id, "//") {
		if i := strings.LastIndexByte(id, ':'); i >= 0 {
			return id[i+1:]
		}
	}
	return id
}

// act does one action.
func (r *Runner) act(ctx context.Context, rep *scmpb.Repository, st *store.QueueState, a Action) error {
	var err error
	switch a.Kind {
	case Create:
		err = r.create(ctx, rep, st, a.Submission, a.Chain)
	case Release:
		err = r.store.ReleaseAttempt(ctx, a.Attempt)
	case Discard:
		r.log.Info("queue: discarded an attempt", "repository", rep.GetName(), "submission", a.Submission, "attempt", a.Attempt, "reason", a.Reason)
		err = r.store.DiscardAttempt(ctx, a.Attempt, a.Reason)
	case Land:
		err = r.land(ctx, rep, st, a.Submission, a.Attempt)
	case Adopt:
		var landed []store.LandedCL
		for _, c := range submission(st, a.Submission).CLs {
			landed = append(landed, store.LandedCL{CLNumber: c.Number, Commit: st.LandedCLs[c.Number]})
		}
		r.log.Info("queue: adopted a landing", "repository", rep.GetName(), "submission", a.Submission)
		err = r.store.RecordLanding(ctx, a.Submission, a.Attempt, landed, 0)
	case Fail:
		r.log.Info("queue: failed a submission", "repository", rep.GetName(), "submission", a.Submission, "reason", a.Reason)
		err = r.store.FailSubmission(ctx, a.Submission, a.Reason)
	case Cancel:
		r.log.Info("queue: canceled a submission", "repository", rep.GetName(), "submission", a.Submission, "reason", a.Reason)
		_, err = r.store.CancelSubmission(ctx, a.Submission, a.Reason)
	case Note:
		err = r.store.SetNote(ctx, a.Submission, a.Reason)
	}
	var conflict *store.ConflictError
	if errors.As(err, &conflict) {
		// The submission or attempt moved on meanwhile — a person canceled it, a new patchset
		// came: the next pass sees it.
		return nil
	}
	if err != nil {
		return fmt.Errorf("submission %d: %w", a.Submission, err)
	}
	return nil
}

func submission(st *store.QueueState, id int64) *store.QueuedSubmission {
	for _, q := range st.Submissions {
		if q.ID == id {
			return q
		}
	}
	return nil
}

func attemptOf(q *store.QueuedSubmission, id int64) *store.Attempt {
	for _, a := range q.Attempts {
		if a.ID == id {
			return a
		}
	}
	return nil
}

// create prepares an attempt of a submission on main's head and chain: each chain CL's
// candidate on the one before it — or one an active attempt already prepared there — then the
// submission's own, each with its post plan. A candidate the SCM refuses ends the preparing:
// the attempt is recorded failed with the SCM's words.
func (r *Runner) create(ctx context.Context, rep *scmpb.Repository, st *store.QueueState, id int64, chain []int64) error {
	repo := rep.GetId()
	e := submission(st, id)
	na := store.NewAttempt{SubmissionID: id, Base: rep.GetHead(), BaseSeq: rep.GetHeadSeq(), Chain: chain}
	prev := rep.GetHead()
	prepare := func(sub *store.Submission, cl store.SubmittedCL) ([]byte, error) {
		if c, ok, err := r.store.ReusableCandidate(ctx, repo, prev, cl.Number, cl.Patchset); err != nil || ok {
			return c, err
		}
		c, err := r.scm.Landing.PrepareCandidate(ctx, &scmpb.PrepareCandidateRequest{RepositoryId: repo,
			Number: cl.Number, Patchset: cl.Patchset, Base: prev, Actor: sub.SubmittedBy})
		return c.GetCommitId(), err
	}
	var on []string
	onCLs := 0
	for _, sid := range chain {
		s := submission(st, sid)
		if s == nil {
			return fmt.Errorf("submission %d, in the chain, is not in the queue", sid)
		}
		on = append(on, clList(numbers(s.CLs)))
		onCLs += len(s.CLs)
		for _, cl := range s.CLs {
			commit, err := prepare(s.Submission, cl)
			if rej := landRejection(err); rej != nil {
				na.PrepareError = fmt.Sprintf("preparing %s, which it is tested on: %s", clList(numbers(s.CLs)), rejectionMessage(rej))
				return r.record(ctx, na)
			}
			if err != nil {
				return err
			}
			na.Candidates = append(na.Candidates, store.NewCandidate{SubmissionID: sid, CLNumber: cl.Number, Patchset: cl.Patchset,
				Commit: commit, Base: prev})
			prev = commit
		}
	}
	na.Why = fmt.Sprintf("%s was submitted: post runs on its candidate over main at %s", clList(numbers(e.CLs)), short(rep.GetHead()))
	if len(on) > 0 {
		verb := "land"
		if onCLs == 1 {
			verb = "lands"
		}
		na.Why += fmt.Sprintf(", speculating that %s %s first", strings.Join(on, " and "), verb)
	}
	for _, cl := range e.CLs {
		commit, err := prepare(e.Submission, cl)
		if rej := landRejection(err); rej != nil {
			na.PrepareError = rejectionMessage(rej)
			return r.record(ctx, na)
		}
		if err != nil {
			return err
		}
		files, err := changes.Files(ctx, r.scm.Browse, repo, prev, commit)
		if err != nil {
			return err
		}
		c, err := r.scm.ChangeLists.GetChangeList(ctx, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: cl.Number})
		if err != nil {
			return err
		}
		jc := workflow.Context{
			Change: workflow.Change{
				Phase:      workflow.Post,
				Repository: workflow.Repository{ID: repo, Name: rep.GetName()},
				CL:         cl.Number,
				Patchset:   cl.Patchset,
				Author:     c.GetAuthor(),
				Commit:     hex.EncodeToString(commit),
				Base:       hex.EncodeToString(prev),
			},
			Files: files,
		}
		change, err := json.Marshal(jc.Change)
		if err != nil {
			return err
		}
		na.Candidates = append(na.Candidates, store.NewCandidate{SubmissionID: id, CLNumber: cl.Number, Patchset: cl.Patchset,
			Commit: commit, Base: prev, Own: true, WorkflowsCommit: rep.GetHead(), Context: change, Touched: jc.Touched()})
		prev = commit
	}
	return r.record(ctx, na)
}

// record records a prepared attempt.
func (r *Runner) record(ctx context.Context, na store.NewAttempt) error {
	id, err := r.store.CreateAttempt(ctx, na)
	if err == nil {
		r.log.Info("queue: prepared an attempt", "submission", na.SubmissionID, "attempt", id, "chain", na.Chain,
			"refused", na.PrepareError)
	}
	return err
}

// land lands a submission on its passed attempt's candidates, against main's head: the SCM
// lands each candidate as it is when main is still its base, and splices it onto main
// otherwise. A refusal fails the submission; a moved head is decided again.
func (r *Runner) land(ctx context.Context, rep *scmpb.Repository, st *store.QueueState, id, attemptID int64) error {
	e := submission(st, id)
	a := attemptOf(e, attemptID)
	req := &scmpb.LandRequest{RepositoryId: rep.GetId(), ExpectedHead: rep.GetHead(), Actor: e.SubmittedBy}
	for _, c := range a.Own() {
		req.Items = append(req.Items, &scmpb.LandRequest_Item{Number: c.CLNumber, Patchset: c.Patchset, Candidate: c.Commit})
	}
	resp, err := r.scm.Landing.Land(ctx, req)
	if rej := landRejection(err); rej != nil {
		if rej.GetReason() == scmpb.LandRejection_REASON_HEAD_MOVED {
			return nil
		}
		return r.store.FailSubmission(ctx, id, rejectionMessage(rej))
	}
	if err != nil {
		return err
	}
	var landed []store.LandedCL
	for _, l := range resp.GetLanded() {
		landed = append(landed, store.LandedCL{CLNumber: l.GetNumber(), Commit: l.GetCommitId()})
	}
	r.log.Info("queue: landed", "repository", rep.GetName(), "submission", id, "attempt", attemptID, "head", short(resp.GetHead()))
	return r.store.RecordLanding(ctx, id, attemptID, landed, rep.GetHeadSeq())
}

// collect discards the repository's candidates no active attempt holds, once they are older
// than the grace period.
func (r *Runner) collect(ctx context.Context, repo int64) error {
	resp, err := r.scm.Landing.ListCandidates(ctx, &scmpb.ListCandidatesRequest{RepositoryId: repo})
	if err != nil {
		return err
	}
	inUse, err := r.store.CandidatesInUse(ctx, repo)
	if err != nil {
		return err
	}
	for _, c := range resp.GetCandidates() {
		if inUse[string(c.GetCommitId())] || time.Since(c.GetPreparedAt().AsTime()) < r.cfg.Grace {
			continue
		}
		if _, err := r.scm.Landing.DiscardCandidate(ctx, &scmpb.DiscardCandidateRequest{RepositoryId: repo,
			CommitId: c.GetCommitId()}); err != nil {
			return err
		}
	}
	return nil
}

// landRejection is the SCM's refusal of a candidate or a landing, when it is one.
func landRejection(err error) *scmpb.LandRejection {
	if status.Code(err) != codes.FailedPrecondition {
		return nil
	}
	for _, d := range status.Convert(err).Details() {
		if r, ok := d.(*scmpb.LandRejection); ok {
			return r
		}
	}
	return nil
}

// rejectionMessage words the SCM's refusal for the person who submitted.
func rejectionMessage(r *scmpb.LandRejection) string {
	switch r.GetReason() {
	case scmpb.LandRejection_REASON_PATH_CONFLICT:
		return fmt.Sprintf("CL %d conflicts with main in %s: run jj sync, resolve the conflicts, and upload a new patchset with jj cl",
			r.GetNumber(), strings.Join(r.GetPaths(), ", "))
	case scmpb.LandRejection_REASON_ALREADY_ON_MAIN:
		return fmt.Sprintf("CL %d changes nothing main does not already have", r.GetNumber())
	case scmpb.LandRejection_REASON_STALE_PATCHSET:
		return fmt.Sprintf("CL %d has a newer patchset: submit it once it is reviewed", r.GetNumber())
	case scmpb.LandRejection_REASON_CONFLICTED:
		return fmt.Sprintf("CL %d has unresolved conflicts: resolve them and upload a new patchset with jj cl", r.GetNumber())
	}
	m := r.GetMessage()
	if m == "" {
		return "the SCM refused it"
	}
	return strings.ToUpper(m[:1]) + m[1:]
}

func numbers(cls []store.SubmittedCL) []int64 {
	out := make([]int64, len(cls))
	for i, c := range cls {
		out[i] = c.Number
	}
	return out
}

// short is a commit ID's first ten hex digits, as ok shows it.
func short(id []byte) string {
	h := hex.EncodeToString(id)
	if len(h) > 10 {
		return h[:10]
	}
	return h
}
