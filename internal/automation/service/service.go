// Package service is ok automation's gRPC services over its store: Runs, for ok control
// and administrators, and Work, for workers. Each admits only its callers, by the
// principal ok's runtime resolves: ok control's, which acts on behalf of the person it names; the
// deployer's; and the workers'.
package service

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/automation/queue"
	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/serve/authn"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// Callers names who may call what.
// maxLeaseWait bounds how long one Lease call waits for a run.
const maxLeaseWait = time.Minute

// Services are the gRPC services over one store.
type Services struct {
	Runs   automationpb.RunsServer
	Pushes automationpb.PushesServer
	Work   automationpb.WorkServer
	// SecretRuns is ok secrets'.
	SecretRuns automationpb.SecretRunsServer
}

// New builds the services. Who may call each is ok's runtime's to decide, from its protocol's
// ok.v1.callers: Runs ok control, Pushes ok's deployer, Work ok's workers, SecretRuns ok secrets.
// Nothing here checks a caller against a list.
func New(s *store.Store, heartbeat time.Duration, cache bool, pre PreStarter) Services {
	c := &core{store: s, heartbeat: heartbeat, cache: cache, pre: pre}
	return Services{Runs: &runs{c: c}, Pushes: &pushes{c: c}, Work: &work{c: c}, SecretRuns: &secretRuns{c: c}}
}

// PreStarter plans pre by hand (the feed's follower, over the SCM): Runs.StartPre.
type PreStarter interface {
	StartPre(ctx context.Context, repo, number int64, requestedBy string) (int32, error)
}

type core struct {
	store     *store.Store
	heartbeat time.Duration
	// cache: ok automation serves a remote cache, which leases say.
	cache bool
	pre   PreStarter
}

// caller returns the stable ID of the principal a request is for: ok automation records principals
// by ID (docs/hosted.md, "Tenancy, as built").
func caller(ctx context.Context) (string, error) {
	c, ok := authn.FromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "ok automation serves identified principals only")
	}
	return c.ID, nil
}

// requester returns who a Runs request is for: the person ok control names.
func (c *core) requester(ctx context.Context, forPerson string) (string, error) {
	if _, err := caller(ctx); err != nil {
		return "", err
	}
	if forPerson == "" {
		return "", status.Error(codes.InvalidArgument, "ok control names the person it acts for")
	}
	return forPerson, nil
}

// identified refuses a caller it cannot attribute.
func (c *core) identified(ctx context.Context) error {
	_, err := caller(ctx)
	return err
}

// toStatus maps a store error onto its gRPC status.
func toStatus(err error) error {
	var inv *store.InvalidError
	var conflict *store.ConflictError
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.As(err, &inv):
		return status.Error(codes.InvalidArgument, inv.Message)
	case errors.As(err, &conflict):
		return status.Error(codes.FailedPrecondition, conflict.Message)
	case errors.Is(err, store.ErrLeaseLost):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrLogFull):
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// ─── Pushes ───

// pushes serves the deployer its deployments' pushes. Only the deployer reaches it: ok's runtime
// holds it to its protocol's ok.v1.callers.
type pushes struct {
	automationpb.UnimplementedPushesServer
	c *core
}

func (s *pushes) StartRun(ctx context.Context, req *automationpb.StartRunRequest) (*automationpb.Run, error) {
	p, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetJob().GetKind() != automationpb.Job_KIND_PUSH || req.GetOrigin().GetPhase() != automationpb.Origin_PHASE_DEPLOY {
		return nil, status.Error(codes.InvalidArgument, "Pushes starts a deployment's push: a push job of the deploy phase")
	}
	if req.GetForPersonId() != "" {
		return nil, status.Error(codes.InvalidArgument, "a deployment's push is the deployer's own: it names no person")
	}
	r, err := s.c.store.Start(ctx, store.StartRequest{
		RepositoryID: req.GetRepositoryId(),
		CommitID:     req.GetCommitId(),
		Job:          jobIn(req.GetJob()),
		Origin:       originIn(req.GetOrigin()),
		RequestedBy:  p,
		Timeout:      time.Duration(req.GetTimeoutSeconds()) * time.Second,
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return runOut(r), nil
}

func (s *pushes) GetRun(ctx context.Context, req *automationpb.GetRunRequest) (*automationpb.Run, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	r, err := s.c.store.Get(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return runOut(r), nil
}

func (s *pushes) CancelRun(ctx context.Context, req *automationpb.CancelRunRequest) (*automationpb.Run, error) {
	p, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	run, err := s.c.store.Get(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	if run.Origin.Phase != store.PhaseDeploy {
		return nil, status.Error(codes.FailedPrecondition, "Pushes cancels deployments' pushes only")
	}
	r, err := s.c.store.Cancel(ctx, req.GetId(), req.GetReason(), p)
	if err != nil {
		return nil, toStatus(err)
	}
	return runOut(r), nil
}

// ─── Runs ───

type runs struct {
	automationpb.UnimplementedRunsServer
	c *core
}

func (s *runs) StartRun(ctx context.Context, req *automationpb.StartRunRequest) (*automationpb.Run, error) {
	if req.GetJob().GetKind() == automationpb.Job_KIND_PUSH || req.GetOrigin().GetPhase() == automationpb.Origin_PHASE_DEPLOY {
		return nil, status.Error(codes.InvalidArgument, "a push is a deployment's: the deployer starts it through Pushes")
	}
	who, err := s.c.requester(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	r, err := s.c.store.Start(ctx, store.StartRequest{
		RepositoryID: req.GetRepositoryId(),
		CommitID:     req.GetCommitId(),
		Job:          jobIn(req.GetJob()),
		Origin:       originIn(req.GetOrigin()),
		RequestedBy:  who,
		Timeout:      time.Duration(req.GetTimeoutSeconds()) * time.Second,
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return runOut(r), nil
}

func (s *runs) GetRun(ctx context.Context, req *automationpb.GetRunRequest) (*automationpb.Run, error) {
	if err := s.c.identified(ctx); err != nil {
		return nil, err
	}
	r, err := s.c.store.Get(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return runOut(r), nil
}

func (s *runs) ListRuns(ctx context.Context, req *automationpb.ListRunsRequest) (*automationpb.ListRunsResponse, error) {
	if err := s.c.identified(ctx); err != nil {
		return nil, err
	}
	rs, err := s.c.store.List(ctx, store.ListQuery{
		RepositoryID: req.GetRepositoryId(),
		CLNumber:     req.GetClNumber(),
		Patchset:     req.GetPatchset(),
		CommitID:     req.GetCommitId(),
		Limit:        int(req.GetLimit()),
		BeforeID:     req.GetBeforeId(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	out := &automationpb.ListRunsResponse{}
	for _, r := range rs {
		out.Runs = append(out.Runs, runOut(r))
	}
	return out, nil
}

func (s *runs) CancelRun(ctx context.Context, req *automationpb.CancelRunRequest) (*automationpb.Run, error) {
	who, err := s.c.requester(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	run, err := s.c.store.Get(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	if run.Origin.Phase == store.PhaseDeploy {
		return nil, status.Error(codes.FailedPrecondition, "a push is its deployment's: halt the deployment")
	}
	r, err := s.c.store.Cancel(ctx, req.GetId(), req.GetReason(), who)
	if err != nil {
		return nil, toStatus(err)
	}
	return runOut(r), nil
}

func (s *runs) RerunRun(ctx context.Context, req *automationpb.RerunRunRequest) (*automationpb.Run, error) {
	who, err := s.c.requester(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	r, err := s.c.store.Rerun(ctx, req.GetId(), who)
	if err != nil {
		return nil, toStatus(err)
	}
	return runOut(r), nil
}

func (s *runs) ReadLog(req *automationpb.ReadLogRequest, stream grpc.ServerStreamingServer[automationpb.LogChunk]) error {
	ctx := stream.Context()
	if err := s.c.identified(ctx); err != nil {
		return err
	}
	return toStatus(s.c.store.ReadLog(ctx, req.GetId(), req.GetOffset(), req.GetFollow(), func(offset int64, data []byte, end bool) error {
		return stream.Send(&automationpb.LogChunk{Offset: offset, Data: data, End: end})
	}))
}

func (s *runs) Submit(ctx context.Context, req *automationpb.SubmitRequest) (*automationpb.Submission, error) {
	if _, err := s.c.requester(ctx, req.GetForPersonId()); err != nil {
		return nil, err
	}
	var cls []store.SubmittedCL
	for _, c := range req.GetCls() {
		cls = append(cls, store.SubmittedCL{Number: c.GetNumber(), Patchset: c.GetPatchset()})
	}
	sub, err := s.c.store.Submit(ctx, store.SubmitRequest{RepositoryID: req.GetRepositoryId(), CLs: cls, SubmittedBy: req.GetForPersonId()})
	if err != nil {
		return nil, toStatus(err)
	}
	return s.submissionOut(ctx, sub)
}

func (s *runs) CancelSubmission(ctx context.Context, req *automationpb.CancelSubmissionRequest) (*automationpb.Submission, error) {
	who, err := s.c.requester(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	sub, err := s.c.store.CancelSubmission(ctx, req.GetId(), req.GetReason(), who)
	if err != nil {
		return nil, toStatus(err)
	}
	return s.submissionOut(ctx, sub)
}

func (s *runs) GetSubmission(ctx context.Context, req *automationpb.GetSubmissionRequest) (*automationpb.Submission, error) {
	if err := s.c.identified(ctx); err != nil {
		return nil, err
	}
	sub, err := s.c.store.GetSubmission(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return s.submissionOut(ctx, sub)
}

func (s *runs) ListSubmissions(ctx context.Context, req *automationpb.ListSubmissionsRequest) (*automationpb.ListSubmissionsResponse, error) {
	if err := s.c.identified(ctx); err != nil {
		return nil, err
	}
	subs, err := s.c.store.ListSubmissions(ctx, req.GetRepositoryId(), req.GetClNumber(), int(req.GetLimit()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &automationpb.ListSubmissionsResponse{}
	for _, sub := range subs {
		o, err := s.submissionOut(ctx, sub)
		if err != nil {
			return nil, err
		}
		out.Submissions = append(out.Submissions, o)
	}
	return out, nil
}

// submissionOut is a submission with its attempts, newest first.
func (s *runs) submissionOut(ctx context.Context, sub *store.Submission) (*automationpb.Submission, error) {
	out := &automationpb.Submission{
		Id:            sub.ID,
		RepositoryId:  sub.RepositoryID,
		SubmittedById: sub.SubmittedBy,
		CanceledById:  sub.CanceledBy,
		Status:        automationpb.Submission_Status(sub.Status),
		Reason:        sub.Reason,
		Note:          sub.Note,
		CreatedAt:     timeOut(sub.CreatedAt),
		FinishedAt:    timeOut(sub.FinishedAt),
	}
	for _, c := range sub.CLs {
		out.Cls = append(out.Cls, &automationpb.Submission_CL{Number: c.Number, Patchset: c.Patchset, LandedCommit: c.LandedCommit})
	}
	attempts, err := s.c.store.Attempts(ctx, sub.ID)
	if err != nil {
		return nil, toStatus(err)
	}
	var chained []int64
	for _, a := range attempts {
		chained = append(chained, a.Chain...)
	}
	named, err := s.c.store.GetSubmissions(ctx, chained)
	if err != nil {
		return nil, toStatus(err)
	}
	for i := len(attempts) - 1; i >= 0; i-- {
		out.Attempts = append(out.Attempts, attemptOut(attempts[i], named))
	}
	return out, nil
}

func attemptOut(a *store.Attempt, named map[int64]*store.Submission) *automationpb.Attempt {
	state, reason := queue.StateOf(a)
	out := &automationpb.Attempt{
		Id:         a.ID,
		Base:       a.Base,
		BaseSeq:    a.BaseSeq,
		Status:     automationpb.Attempt_Status(a.Status),
		Verdict:    verdictOut(state),
		Reason:     a.Reason,
		CreatedAt:  timeOut(a.CreatedAt),
		FinishedAt: timeOut(a.FinishedAt),
	}
	if out.Reason == "" && state == queue.Failed {
		out.Reason = reason
	}
	for _, id := range a.Chain {
		sp := &automationpb.Attempt_Speculation{SubmissionId: id}
		if sub := named[id]; sub != nil {
			for _, c := range sub.CLs {
				sp.ClNumbers = append(sp.ClNumbers, c.Number)
			}
		}
		out.Chain = append(out.Chain, sp)
	}
	for _, c := range a.Own() {
		oc := &automationpb.Attempt_Candidate{ClNumber: c.CLNumber, Patchset: c.Patchset, CommitId: c.Commit, Base: c.Base}
		if c.PlanRun != nil {
			oc.Plan = runOut(c.PlanRun)
			oc.Plan.Job.WorkflowsCommit = c.Plan.WorkflowsCommit
		}
		for _, j := range c.Jobs {
			oc.Jobs = append(oc.Jobs, runOut(j))
		}
		if c.Plan != nil {
			for _, sk := range c.Plan.Skipped {
				oc.Skipped = append(oc.Skipped, &automationpb.SkippedJob{Job: sk.Job, Target: sk.Target, Why: sk.Why})
			}
		}
		out.Candidates = append(out.Candidates, oc)
	}
	return out
}

func verdictOut(s queue.State) automationpb.Attempt_Verdict {
	switch s {
	case queue.Planning:
		return automationpb.Attempt_VERDICT_PLANNING
	case queue.Planned:
		return automationpb.Attempt_VERDICT_WAITING
	case queue.Testing:
		return automationpb.Attempt_VERDICT_TESTING
	case queue.Passed:
		return automationpb.Attempt_VERDICT_PASSED
	}
	return automationpb.Attempt_VERDICT_FAILED
}

func (s *runs) StartPre(ctx context.Context, req *automationpb.StartPreRequest) (*automationpb.Pre, error) {
	who, err := s.c.requester(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if req.GetRepositoryId() <= 0 || req.GetClNumber() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "a CL is named by its repository and number")
	}
	if s.c.pre == nil {
		return nil, status.Error(codes.Unavailable, "this replica cannot plan pre: it reaches no SCM")
	}
	patchset, err := s.c.pre.StartPre(ctx, req.GetRepositoryId(), req.GetClNumber(), who)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, toStatus(err)
	}
	return s.GetPre(ctx, &automationpb.GetPreRequest{RepositoryId: req.GetRepositoryId(), ClNumber: req.GetClNumber(), Patchset: patchset})
}

func (s *runs) GetPre(ctx context.Context, req *automationpb.GetPreRequest) (*automationpb.Pre, error) {
	if err := s.c.identified(ctx); err != nil {
		return nil, err
	}
	pre, err := s.c.store.GetPre(ctx, req.GetRepositoryId(), req.GetClNumber(), req.GetPatchset())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &automationpb.Pre{Verdict: automationpb.Pre_Verdict(pre.Verdict)}
	if pre.Plan == nil {
		return out, nil
	}
	out.CommitId = pre.Plan.CommitID
	out.WorkflowsCommit = pre.Plan.WorkflowsCommit
	out.Plan = runOut(pre.PlanRun)
	out.Plan.Job.WorkflowsCommit = pre.Plan.WorkflowsCommit
	for _, r := range pre.Jobs {
		out.Jobs = append(out.Jobs, runOut(r))
	}
	for _, sk := range pre.Plan.Skipped {
		out.Skipped = append(out.Skipped, &automationpb.SkippedJob{Job: sk.Job, Target: sk.Target, Why: sk.Why})
	}
	return out, nil
}

// ─── Work ───

type work struct {
	automationpb.UnimplementedWorkServer
	c *core
}

func (s *work) Lease(ctx context.Context, req *automationpb.LeaseRequest) (*automationpb.LeaseResponse, error) {
	if err := s.c.identified(ctx); err != nil {
		return nil, err
	}
	wait := min(time.Duration(req.GetWaitSeconds())*time.Second, maxLeaseWait)
	r, err := s.c.store.Lease(ctx, req.GetWorker(), req.GetPool(), max(wait, 0))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &automationpb.LeaseResponse{HeartbeatSeconds: int32(s.c.heartbeat / time.Second), Cache: s.c.cache}
	if r == nil {
		return out, nil
	}
	out.Run, out.RunKey = runOut(r), r.LeaseKey
	if r.PlanID > 0 {
		plan, err := s.c.store.GetPlan(ctx, r.PlanID)
		if err != nil {
			return nil, toStatus(err)
		}
		out.Context = plan.Context
		if r.Job.Kind == store.KindPlan {
			out.Run.Job.WorkflowsCommit = plan.WorkflowsCommit
			out.Run.Job.PreCommit = plan.PreCommit
		}
	}
	return out, nil
}

func (s *work) Heartbeat(ctx context.Context, req *automationpb.HeartbeatRequest) (*automationpb.HeartbeatResponse, error) {
	if err := s.c.identified(ctx); err != nil {
		return nil, err
	}
	stop, reason, err := s.c.store.Heartbeat(ctx, req.GetRunId(), req.GetWorker())
	if err != nil {
		return nil, toStatus(err)
	}
	return &automationpb.HeartbeatResponse{Stop: automationpb.HeartbeatResponse_Stop(stop), Reason: reason}, nil
}

func (s *work) Release(ctx context.Context, req *automationpb.ReleaseRequest) (*automationpb.ReleaseResponse, error) {
	if err := s.c.identified(ctx); err != nil {
		return nil, err
	}
	if err := s.c.store.Release(ctx, req.GetRunId(), req.GetWorker(), req.GetReason()); err != nil {
		return nil, toStatus(err)
	}
	return &automationpb.ReleaseResponse{}, nil
}

func (s *work) AppendLog(stream grpc.ClientStreamingServer[automationpb.AppendLogRequest, automationpb.AppendLogResponse]) error {
	ctx := stream.Context()
	if err := s.c.identified(ctx); err != nil {
		return err
	}
	var size int64
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&automationpb.AppendLogResponse{Size: size})
		}
		if err != nil {
			return err
		}
		if size, err = s.c.store.AppendLog(ctx, req.GetRunId(), req.GetWorker(), req.GetOffset(), req.GetData()); err != nil {
			return toStatus(err)
		}
	}
}

func (s *work) Finish(ctx context.Context, req *automationpb.FinishRequest) (*automationpb.FinishResponse, error) {
	if err := s.c.identified(ctx); err != nil {
		return nil, err
	}
	err := s.c.store.Finish(ctx, req.GetRunId(), req.GetWorker(), store.Status(req.GetStatus()), req.GetExitCode(), req.GetReason(), planIn(req.GetPlan()),
		pushedIn(req.GetPushed())...)
	if err != nil {
		return nil, toStatus(err)
	}
	return &automationpb.FinishResponse{}, nil
}

// ─── Conversions ───

func jobIn(j *automationpb.Job) store.Job {
	return store.Job{Kind: store.Kind(j.GetKind()), Targets: j.GetTargets(), Args: j.GetArgs(), Pool: j.GetPool()}
}

// planIn is a planning run's plan, or nil when the request carries none.
func planIn(p *automationpb.Plan) *store.PlanResult {
	if p == nil {
		return nil
	}
	out := &store.PlanResult{Affected: p.GetAffected(), Upstream: p.GetUpstream()}
	for _, j := range p.GetJobs() {
		out.Jobs = append(out.Jobs, store.PlannedJob{Job: j.GetJob(), Spec: jobIn(j.GetSpec()), Why: j.GetWhy(),
			UnchangedSincePre: j.GetUnchangedSincePre()})
	}
	for _, sk := range p.GetSkipped() {
		out.Skipped = append(out.Skipped, store.SkippedJob{Job: sk.GetJob(), Target: sk.GetTarget(), Why: sk.GetWhy()})
	}
	return out
}

func pushedIn(ps []*automationpb.PushedImage) []store.Pushed {
	var out []store.Pushed
	for _, p := range ps {
		out = append(out, store.Pushed{Target: p.GetTarget(), Registry: p.GetRegistry(), Repository: p.GetRepository(), Digest: p.GetDigest(), Tags: p.GetTags()})
	}
	return out
}

func originIn(o *automationpb.Origin) store.Origin {
	return store.Origin{Phase: store.Phase(o.GetPhase()), CLNumber: o.GetClNumber(), Patchset: o.GetPatchset(), Job: o.GetJob()}
}

func timeOut(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func runOut(r *store.Run) *automationpb.Run {
	return &automationpb.Run{
		Id:           r.ID,
		RepositoryId: r.RepositoryID,
		CommitId:     r.CommitID,
		Job: &automationpb.Job{
			Kind:    automationpb.Job_Kind(r.Job.Kind),
			Targets: r.Job.Targets,
			Args:    r.Job.Args,
			Pool:    r.Job.Pool,
		},
		Origin: &automationpb.Origin{
			Phase:    automationpb.Origin_Phase(r.Origin.Phase),
			ClNumber: r.Origin.CLNumber,
			Patchset: r.Origin.Patchset,
			Job:      r.Origin.Job,
		},
		Status:         automationpb.Run_Status(r.Status),
		RequestedById:  r.RequestedBy,
		CanceledById:   r.CanceledBy,
		RerunOf:        r.RerunOf,
		Attempt:        r.Attempt,
		Worker:         r.Worker,
		ExitCode:       r.ExitCode,
		Reason:         r.Reason,
		LogSize:        r.LogSize,
		TimeoutSeconds: r.TimeoutSeconds,
		Why:            r.Why,
		QueuedAt:       timeOut(r.QueuedAt),
		StartedAt:      timeOut(r.StartedAt),
		FinishedAt:     timeOut(r.FinishedAt),
		Pushed:         pushedOut(r.Pushed),
		CarriedFrom:    r.CarriedFrom,
	}
}

func pushedOut(ps []store.Pushed) []*automationpb.PushedImage {
	var out []*automationpb.PushedImage
	for _, p := range ps {
		out = append(out, &automationpb.PushedImage{Target: p.Target, Registry: p.Registry, Repository: p.Repository, Digest: p.Digest, Tags: p.Tags})
	}
	return out
}

// ─── SecretRuns ───

type secretRuns struct {
	automationpb.UnimplementedSecretRunsServer
	c *core
}

// CheckRun is a run, while it runs under the lease the key was minted for.
func (s *secretRuns) CheckRun(ctx context.Context, req *automationpb.CheckRunRequest) (*automationpb.Run, error) {
	if len(req.GetRunKey()) != 32 {
		return nil, status.Error(codes.InvalidArgument, "a run's key is 32 bytes")
	}
	r, err := s.c.store.RunOfLease(ctx, req.GetRunId(), req.GetRunKey())
	if err != nil {
		return nil, toStatus(err)
	}
	return runOut(r), nil
}
