package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/testdb"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

const (
	control = "control.ok.robot.example.com"
	worker  = "worker.ok.robot.example.com"
	person  = "alice.human.example.com"
)

// named attaches the principal a request's "principal" metadata names, as ok's runtime attaches
// the caller it resolved.
func named(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if p := md.Get("principal"); len(p) == 1 {
			return authn.NewContext(ctx, authn.Caller{Principal: p[0]})
		}
	}
	return ctx
}

type clients struct {
	runs   automationpb.RunsClient
	pushes automationpb.PushesClient
	work   automationpb.WorkClient
	store  *store.Store
}

func newClients(t *testing.T) clients {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s3 := testdb.S3(t)
	blobs, err := blob.NewS3(blob.Config{Endpoint: s3.Endpoint, Bucket: testdb.Bucket(t), AccessKey: s3.AccessKey, SecretKey: s3.SecretKey, Region: s3.Region})
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, pool, blobs, store.DefaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	go st.Listen(ctx)
	svc := New(st, 10*time.Second)
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			return h(named(ctx), req)
		}),
		grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			return h(srv, &namedStream{ServerStream: ss})
		}),
	)
	automationpb.RegisterRunsServer(srv, svc.Runs)
	automationpb.RegisterPushesServer(srv, svc.Pushes)
	automationpb.RegisterWorkServer(srv, svc.Work)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return clients{runs: automationpb.NewRunsClient(conn), pushes: automationpb.NewPushesClient(conn), work: automationpb.NewWorkClient(conn), store: st}
}

type namedStream struct{ grpc.ServerStream }

func (s *namedStream) Context() context.Context { return named(s.ServerStream.Context()) }

func as(p string) context.Context {
	if p == "" {
		return context.Background()
	}
	return metadata.AppendToOutgoingContext(context.Background(), "principal", p)
}

func commit() []byte {
	b := make([]byte, 32)
	rand.Read(b)
	return b
}

func startReq(forPerson string) *automationpb.StartRunRequest {
	return &automationpb.StartRunRequest{
		RepositoryId: 1,
		CommitId:     commit(),
		Job:          &automationpb.Job{Kind: automationpb.Job_KIND_TEST, Targets: []string{"//a:test"}},
		Origin:       &automationpb.Origin{Phase: automationpb.Origin_PHASE_PRE, ClNumber: 5, Patchset: 2, Job: "tests"},
		ForPerson:    forPerson,
	}
}

// Runs acts for the person ok control names, and refuses a caller it cannot attribute. Which
// callers reach each service is the mesh's: TestCallers.
func TestAuthorization(t *testing.T) {
	c := newClients(t)
	for _, tc := range []struct {
		name      string
		principal string
		forPerson string
		code      codes.Code
		requester string
	}{
		{"ok control, for a person", control, person, codes.OK, person},
		{"ok control, for no one", control, "", codes.InvalidArgument, ""},
		{"no one", "", "", codes.Unauthenticated, ""},
	} {
		r, err := c.runs.StartRun(as(tc.principal), startReq(tc.forPerson))
		if status.Code(err) != tc.code {
			t.Errorf("%s: %v, want %s", tc.name, err, tc.code)
			continue
		}
		if err == nil && r.GetRequestedBy() != tc.requester {
			t.Errorf("%s: requested by %q, want %q", tc.name, r.GetRequestedBy(), tc.requester)
		}
	}
	// A push is a deployment's: ok control cannot start one through Runs.
	push := startReq(person)
	push.Job.Kind = automationpb.Job_KIND_PUSH
	push.Origin = &automationpb.Origin{Phase: automationpb.Origin_PHASE_DEPLOY, Job: "a"}
	if _, err := c.runs.StartRun(as(control), push); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a push through Runs: %v", err)
	}
	if _, err := c.work.Lease(as(""), &automationpb.LeaseRequest{Worker: "w"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("an unidentified lease: %v", err)
	}
}

// A run, end to end through the protocol: started for a person, leased, its log streamed in
// and followed out, finished.
func TestRun(t *testing.T) {
	c := newClients(t)
	started, err := c.runs.StartRun(as(control), startReq(person))
	if err != nil {
		t.Fatal(err)
	}
	if started.GetStatus() != automationpb.Run_STATUS_QUEUED || started.GetQueuedAt() == nil || started.GetStartedAt() != nil {
		t.Fatalf("started: %v", started)
	}
	leased, err := c.work.Lease(as(worker), &automationpb.LeaseRequest{Worker: "w-1", WaitSeconds: 5})
	if err != nil || leased.GetRun().GetId() != started.GetId() || leased.GetHeartbeatSeconds() != 10 {
		t.Fatalf("lease: %v, %v", leased, err)
	}
	if hb, err := c.work.Heartbeat(as(worker), &automationpb.HeartbeatRequest{RunId: started.GetId(), Worker: "w-1"}); err != nil || hb.GetStop() != automationpb.HeartbeatResponse_STOP_UNSPECIFIED {
		t.Fatalf("heartbeat: %v, %v", hb, err)
	}

	follow, err := c.runs.ReadLog(as(control), &automationpb.ReadLogRequest{Id: started.GetId(), Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 1)
	go func() {
		var b strings.Builder
		for {
			chunk, err := follow.Recv()
			if err != nil {
				t.Errorf("follow: %v", err)
				break
			}
			b.Write(chunk.GetData())
			if chunk.GetEnd() {
				break
			}
		}
		got <- b.String()
	}()

	log, err := c.work.AppendLog(as(worker))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []*automationpb.AppendLogRequest{
		{RunId: started.GetId(), Worker: "w-1", Offset: 0, Data: []byte("building\n")},
		{RunId: started.GetId(), Worker: "w-1", Offset: 9, Data: []byte("ok\n")},
	} {
		if err := log.Send(r); err != nil {
			t.Fatal(err)
		}
	}
	if res, err := log.CloseAndRecv(); err != nil || res.GetSize() != 12 {
		t.Fatalf("append: %v, %v", res, err)
	}
	if _, err := c.work.Finish(as(worker), &automationpb.FinishRequest{RunId: started.GetId(), Worker: "w-2", Status: automationpb.Run_STATUS_PASSED}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("another worker's finish: %v", err)
	}
	if _, err := c.work.Finish(as(worker), &automationpb.FinishRequest{RunId: started.GetId(), Worker: "w-1", Status: automationpb.Run_STATUS_PASSED}); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-got:
		if s != "building\nok\n" {
			t.Errorf("followed %q", s)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the follower did not finish")
	}
	done, err := c.runs.GetRun(as(control), &automationpb.GetRunRequest{Id: started.GetId()})
	if err != nil || done.GetStatus() != automationpb.Run_STATUS_PASSED || done.GetLogSize() != 12 || done.GetFinishedAt() == nil ||
		done.GetWorker() != "w-1" || done.GetOrigin().GetJob() != "tests" {
		t.Errorf("finished: %v, %v", done, err)
	}
	list, err := c.runs.ListRuns(as(control), &automationpb.ListRunsRequest{RepositoryId: 1, ClNumber: 5, Patchset: 2})
	if err != nil || len(list.GetRuns()) != 1 || list.GetRuns()[0].GetId() != started.GetId() {
		t.Errorf("list: %v, %v", list, err)
	}
	rerun, err := c.runs.RerunRun(as(control), &automationpb.RerunRunRequest{Id: started.GetId(), ForPerson: "bob.human.example.com"})
	if err != nil || rerun.GetRerunOf() != started.GetId() || rerun.GetRequestedBy() != "bob.human.example.com" {
		t.Errorf("rerun: %v, %v", rerun, err)
	}
	canceled, err := c.runs.CancelRun(as(control), &automationpb.CancelRunRequest{Id: rerun.GetId(), ForPerson: person})
	if err != nil || canceled.GetStatus() != automationpb.Run_STATUS_CANCELED || canceled.GetReason() != "canceled by "+person {
		t.Errorf("cancel: %v, %v", canceled, err)
	}
}

func TestErrors(t *testing.T) {
	c := newClients(t)
	if _, err := c.runs.GetRun(as(control), &automationpb.GetRunRequest{Id: 999}); status.Code(err) != codes.NotFound {
		t.Errorf("a missing run: %v", err)
	}
	r, err := c.runs.StartRun(as(control), startReq(person))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.runs.RerunRun(as(control), &automationpb.RerunRunRequest{Id: r.GetId(), ForPerson: person}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("a queued run's rerun: %v", err)
	}
	bad := startReq(person)
	bad.Job.Kind = automationpb.Job_KIND_RUN
	bad.Job.Targets = nil
	if _, err := c.runs.StartRun(as(control), bad); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a malformed job: %v", err)
	}
	if _, err := c.work.Lease(as(worker), &automationpb.LeaseRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a nameless worker: %v", err)
	}
	stream, err := c.runs.ReadLog(as(control), &automationpb.ReadLogRequest{Id: r.GetId(), Offset: 50})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("reading past a log's end: %v", err)
	}
	if _, err := stream.Recv(); err != io.EOF && status.Code(err) != codes.InvalidArgument {
		t.Errorf("after the error: %v", err)
	}
}

// sameJSON reports whether two JSON documents say the same.
func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatalf("%s: %v", a, err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatalf("%s: %v", b, err)
	}
	return reflect.DeepEqual(x, y)
}

// A patchset's pre, as automation plans it: the worker takes its planning run with the job
// context and main's workflows commit, and its plan's jobs are queued as it finishes; ok
// control reads the pre, and no one else starts a plan.
func TestPre(t *testing.T) {
	c := newClients(t)
	commitID, workflows := commit(), commit()
	jobContext := []byte(`{"phase": "pre", "cl": 3, "patchset": 1}`)
	plan, _, err := c.store.CreatePlan(context.Background(), store.PlanRequest{RepositoryID: 1, CLNumber: 3, Patchset: 1,
		CommitID: commitID, WorkflowsCommit: workflows, Context: jobContext, RequestedBy: person})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := c.work.Lease(as(worker), &automationpb.LeaseRequest{Worker: "w"})
	if err != nil {
		t.Fatal(err)
	}
	r := lease.GetRun()
	if r.GetJob().GetKind() != automationpb.Job_KIND_PLAN || !sameJSON(t, lease.GetContext(), jobContext) ||
		string(r.GetJob().GetWorkflowsCommit()) != string(workflows) || r.GetWhy() != "patchset 1 was uploaded" {
		t.Fatalf("the planning run's lease: %v", lease)
	}
	_, err = c.work.Finish(as(worker), &automationpb.FinishRequest{RunId: r.GetId(), Worker: "w", Status: automationpb.Run_STATUS_PASSED,
		Plan: &automationpb.Plan{
			Jobs: []*automationpb.PlannedJob{{Job: "test //a:test", Why: "the test is affected",
				Spec: &automationpb.Job{Kind: automationpb.Job_KIND_TEST, Targets: []string{"//a:test"}}}},
			Skipped: []*automationpb.SkippedJob{{Job: "//.ok/workflow/ci.yaml:pre:x", Target: "//b:x", Why: "not affected"}},
		}})
	if err != nil {
		t.Fatal(err)
	}
	pre, err := c.runs.GetPre(as(control), &automationpb.GetPreRequest{RepositoryId: 1, ClNumber: 3, Patchset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if pre.GetVerdict() != automationpb.Pre_VERDICT_PENDING || pre.GetPlan().GetId() != r.GetId() || len(pre.GetJobs()) != 1 ||
		pre.GetJobs()[0].GetWhy() != "the test is affected" || len(pre.GetSkipped()) != 1 || string(pre.GetCommitId()) != string(commitID) {
		t.Fatalf("the pre: %v", pre)
	}

	// A job's lease carries its plan's context too.
	lease, err = c.work.Lease(as(worker), &automationpb.LeaseRequest{Worker: "w"})
	if err != nil || lease.GetRun().GetOrigin().GetJob() != "test //a:test" || !sameJSON(t, lease.GetContext(), jobContext) {
		t.Fatalf("a planned job's lease: %v, %v", lease, err)
	}
	if plan.ID == 0 {
		t.Fatal("no plan")
	}

	// A patchset no one planned.
	if pre, err := c.runs.GetPre(as(control), &automationpb.GetPreRequest{RepositoryId: 1, ClNumber: 3, Patchset: 2}); err != nil || pre.GetVerdict() != automationpb.Pre_VERDICT_NONE {
		t.Errorf("an unplanned patchset: %v, %v", pre, err)
	}
	_, err = c.runs.StartRun(as(control), &automationpb.StartRunRequest{RepositoryId: 1, CommitId: commit(), ForPerson: person,
		Job:    &automationpb.Job{Kind: automationpb.Job_KIND_PLAN},
		Origin: &automationpb.Origin{Phase: automationpb.Origin_PHASE_PRE, ClNumber: 3, Patchset: 1}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("starting a planning run: %v", err)
	}
}

// ok control submits, for the person it names, and reads and cancels submissions.
func TestSubmissions(t *testing.T) {
	c := newClients(t)
	req := &automationpb.SubmitRequest{RepositoryId: 1, Cls: []*automationpb.SubmitRequest_CL{{Number: 5, Patchset: 2}, {Number: 6, Patchset: 1}}}
	for who, code := range map[string]codes.Code{"": codes.Unauthenticated} {
		if _, err := c.runs.Submit(as(who), req); status.Code(err) != code {
			t.Errorf("%q submitting: %v", who, err)
		}
	}
	if _, err := c.runs.Submit(as(control), req); status.Code(err) != codes.InvalidArgument {
		t.Errorf("control submitting for no one: %v", err)
	}
	req.ForPerson = person
	sub, err := c.runs.Submit(as(control), req)
	if err != nil {
		t.Fatal(err)
	}
	if sub.GetStatus() != automationpb.Submission_STATUS_ACTIVE || sub.GetSubmittedBy() != person || len(sub.GetCls()) != 2 ||
		sub.GetCls()[0].GetNumber() != 5 || sub.GetCls()[0].GetPatchset() != 2 || len(sub.GetAttempts()) != 0 || sub.GetCreatedAt() == nil {
		t.Fatalf("the submission: %v", sub)
	}
	if _, err := c.runs.Submit(as(control), req); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("submitting it again: %v", err)
	}

	// An attempt, as the queue prepares it, shows with its candidate's post.
	cand := commit()
	if _, err := c.store.CreateAttempt(context.Background(), store.NewAttempt{SubmissionID: sub.GetId(), Base: commit(), BaseSeq: 3,
		Candidates: []store.NewCandidate{{SubmissionID: sub.GetId(), CLNumber: 5, Patchset: 2, Commit: cand, Base: commit(), Own: true,
			WorkflowsCommit: commit(), Context: []byte(`{"phase": "post"}`)}}}); err != nil {
		t.Fatal(err)
	}
	got, err := c.runs.GetSubmission(as(control), &automationpb.GetSubmissionRequest{Id: sub.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if a := got.GetAttempts(); len(a) != 1 || a[0].GetVerdict() != automationpb.Attempt_VERDICT_PLANNING || a[0].GetBaseSeq() != 3 ||
		a[0].GetStatus() != automationpb.Attempt_STATUS_ACTIVE || len(a[0].GetCandidates()) != 1 ||
		a[0].GetCandidates()[0].GetPlan().GetOrigin().GetPhase() != automationpb.Origin_PHASE_POST ||
		string(a[0].GetCandidates()[0].GetCommitId()) != string(cand) {
		t.Fatalf("the submission's attempt: %v", a)
	}
	list, err := c.runs.ListSubmissions(as(control), &automationpb.ListSubmissionsRequest{RepositoryId: 1, ClNumber: 6})
	if err != nil || len(list.GetSubmissions()) != 1 || list.GetSubmissions()[0].GetId() != sub.GetId() {
		t.Fatalf("CL 6's submissions: %v, %v", list, err)
	}

	canceled, err := c.runs.CancelSubmission(as(control), &automationpb.CancelSubmissionRequest{Id: sub.GetId(), ForPerson: person})
	if err != nil || canceled.GetStatus() != automationpb.Submission_STATUS_CANCELED || canceled.GetReason() != "canceled by "+person ||
		canceled.GetAttempts()[0].GetStatus() != automationpb.Attempt_STATUS_DISCARDED {
		t.Fatalf("canceling: %v, %v", canceled, err)
	}
	if _, err := c.runs.CancelSubmission(as(control), &automationpb.CancelSubmissionRequest{Id: sub.GetId()}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("canceling for no one: %v", err)
	}
}

// The deployer starts, reads, and cancels its deployments' pushes through Pushes, as itself;
// Pushes runs nothing else.
func TestPushes(t *testing.T) {
	c := newClients(t)
	const deployer = "deploy.ok.robot.example.com"
	push := &automationpb.StartRunRequest{
		RepositoryId: 1,
		CommitId:     commit(),
		Job:          &automationpb.Job{Kind: automationpb.Job_KIND_PUSH, Targets: []string{"//a:push"}},
		Origin:       &automationpb.Origin{Phase: automationpb.Origin_PHASE_DEPLOY, Job: "a"},
	}
	r, err := c.pushes.StartRun(as(deployer), push)
	if err != nil || r.GetRequestedBy() != deployer || r.GetStatus() != automationpb.Run_STATUS_QUEUED {
		t.Fatalf("starting a push: %v (%v)", r, err)
	}
	if got, err := c.pushes.GetRun(as(deployer), &automationpb.GetRunRequest{Id: r.GetId()}); err != nil || got.GetId() != r.GetId() {
		t.Errorf("reading the push: %v (%v)", got, err)
	}
	for name, req := range map[string]*automationpb.StartRunRequest{
		"a test":          startReq(""),
		"a push of pre":   {RepositoryId: 1, CommitId: commit(), Job: push.GetJob(), Origin: &automationpb.Origin{Phase: automationpb.Origin_PHASE_PRE, ClNumber: 5, Patchset: 1}},
		"naming a person": {RepositoryId: 1, CommitId: commit(), Job: push.GetJob(), Origin: push.GetOrigin(), ForPerson: person},
	} {
		if _, err := c.pushes.StartRun(as(deployer), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v, want InvalidArgument", name, err)
		}
	}
	if _, err := c.pushes.StartRun(as(""), push); status.Code(err) != codes.Unauthenticated {
		t.Errorf("an unidentified caller: %v", err)
	}
	other, err := c.runs.StartRun(as(control), startReq(person))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.pushes.CancelRun(as(deployer), &automationpb.CancelRunRequest{Id: other.GetId()}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("canceling a run that is no push: %v", err)
	}
	canceled, err := c.pushes.CancelRun(as(deployer), &automationpb.CancelRunRequest{Id: r.GetId()})
	if err != nil || canceled.GetStatus() != automationpb.Run_STATUS_CANCELED || canceled.GetReason() != "canceled by "+deployer {
		t.Errorf("canceling the push: %v (%v)", canceled, err)
	}
}

// TestCallers pins who may call each service: ok's runtime holds each to its protocol's
// ok.v1.callers.
func TestCallers(t *testing.T) {
	for svc, want := range map[string][]okpb.Caller{
		"ok.automation.v1.Runs":   {okpb.Caller_CALLER_CONTROL},
		"ok.automation.v1.Pushes": {okpb.Caller_CALLER_DEPLOY},
		"ok.automation.v1.Work":   {okpb.Caller_CALLER_WORKER},
	} {
		if got := authn.Callers(svc); !slices.Equal(got, want) {
			t.Errorf("%s: callers %v, want %v", svc, got, want)
		}
	}
}
