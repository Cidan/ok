package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/Cidan/ok/internal/buildsys"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/deploy/kubesim"
	"github.com/Cidan/ok/internal/deploy/plan"
	"github.com/Cidan/ok/internal/deploy/roll"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/deploy/store/postgres"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/workflow"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

const principal = "deploy.ok.robot.example.com"

// ─── Stand-ins ───

// runs is ok automation for the deployer: each push passes on its second read — its digest
// the target's and commit's — unless the target is set to fail.
type runs struct {
	mu    sync.Mutex
	runs  map[int64]*automationpb.Run
	reads map[int64]int
	fail  map[string]bool // target → its push fails
	next  int64
}

func newRuns() *runs {
	return &runs{runs: map[int64]*automationpb.Run{}, reads: map[int64]int{}, fail: map[string]bool{}}
}

func (r *runs) StartRun(_ context.Context, in *automationpb.StartRunRequest, _ ...grpc.CallOption) (*automationpb.Run, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if in.GetJob().GetKind() != automationpb.Job_KIND_PUSH || in.GetOrigin().GetPhase() != automationpb.Origin_PHASE_DEPLOY {
		return nil, status.Error(codes.InvalidArgument, "the deployer only pushes")
	}
	r.next++
	run := &automationpb.Run{Id: r.next, CommitId: in.GetCommitId(), Job: in.GetJob(), Origin: in.GetOrigin(), Status: automationpb.Run_STATUS_QUEUED}
	r.runs[r.next] = run
	return run, nil
}

// digest is the digest a push of target at commit makes.
func digest(target string, commit []byte) string {
	sum := sha256.Sum256(append([]byte(target), commit...))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// image is the reference the deployer rolls to for a push of target at commit.
func image(target string, commit []byte) string {
	return "registry.test/" + repository(target) + "@" + digest(target, commit)
}

func repository(target string) string {
	return "apps/" + strings.TrimSuffix(strings.TrimPrefix(target, "//"), ":push")
}

func (r *runs) GetRun(_ context.Context, in *automationpb.GetRunRequest, _ ...grpc.CallOption) (*automationpb.Run, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	run, ok := r.runs[in.GetId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such run")
	}
	r.reads[in.GetId()]++
	if run.GetStatus() == automationpb.Run_STATUS_QUEUED && r.reads[in.GetId()] >= 2 {
		target := run.GetJob().GetTargets()[0]
		if r.fail[target] {
			run.Status, run.Reason = automationpb.Run_STATUS_FAILED, "bazel run exited with status 1"
		} else {
			run.Status = automationpb.Run_STATUS_PASSED
			run.Pushed = []*automationpb.PushedImage{{Target: target, Registry: "https://registry.test", Repository: repository(target),
				Digest: digest(target, run.GetCommitId())}}
		}
	}
	return run, nil
}

func (r *runs) CancelRun(_ context.Context, in *automationpb.CancelRunRequest, _ ...grpc.CallOption) (*automationpb.Run, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	run := r.runs[in.GetId()]
	if run.GetStatus() == automationpb.Run_STATUS_QUEUED {
		run.Status, run.Reason = automationpb.Run_STATUS_CANCELED, in.GetReason()
	}
	return run, nil
}

// build is the repository's build system for the deployer: what each changed file affects, what
// each target is, and how each migration goes.
type build struct {
	mu       sync.Mutex
	affects  map[string][]string // changed file → affected targets
	kinds    map[string]string   // a target's kind; one ending _binary is a binary
	migrate  map[string]string   // target → "ok" (the default), "refuse" (DELETES_DATA, until allowed), "fail"
	applied  []string            // each migration run, "target allow…"
	affected [][]string          // each Affected call's files
}

func (b *build) Affected(_ context.Context, _ int64, _, _ []byte, files []string) (*plan.Affected, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.affected = append(b.affected, files)
	a := &plan.Affected{}
	seen := map[string]bool{}
	for _, f := range files {
		for _, t := range b.affects[f] {
			if !seen[t] {
				seen[t] = true
				a.Affected = append(a.Affected, plan.Target{Label: t, Because: []plan.Step{{Label: t, File: f}}})
			}
		}
	}
	return a, nil
}

func (b *build) Describe(_ context.Context, _ int64, _ []byte, targets []string) (map[string]buildsys.Description, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]buildsys.Description{}
	for _, t := range targets {
		if k, ok := b.kinds[t]; ok {
			out[t] = buildsys.Description{Label: t, Kind: k, Binary: strings.HasSuffix(k, "_binary")}
		}
	}
	return out, nil
}

func (b *build) Migrate(_ context.Context, target string, allow []string) (Applied, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.applied = append(b.applied, strings.TrimSpace(target+" "+strings.Join(allow, " ")))
	switch b.migrate[target] {
	case "refuse":
		if !slices.Contains(allow, "DELETES_DATA") {
			return Applied{Output: "refusing to apply", Report: []byte(`{"steps":[],"refused":["DELETES_DATA"]}`), Refused: []string{"DELETES_DATA"},
				Failed: "the plan carries gated hazards that were not allowed: DELETES_DATA"}, nil
		}
	case "fail":
		return Applied{Output: "connecting: refused", Failed: "connecting to \"db\": refused"}, nil
	}
	return Applied{Output: "applied 1 step(s)", Report: []byte(`{"steps":[],"executed":true}`)}, nil
}

// repo is the repository's commits: each one's files.
type repo struct {
	mu      sync.Mutex
	commits map[string]map[string]string
	dir     string
	at      []byte
}

// commit makes a commit of files: a Bazel workspace's, unless they hold settings of their own.
func (r *repo) commit(n byte, files map[string]string) []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := make([]byte, 32)
	c[0] = n
	if _, ok := files[workflow.SettingsFile]; !ok {
		files = maps.Clone(files)
		if files == nil {
			files = map[string]string{}
		}
		files[workflow.SettingsFile] = "build: bazel\n"
	}
	r.commits[string(c)] = files
	return c
}

func (r *repo) Dir() string { return r.dir }

func (r *repo) Sync(_ context.Context, _ int64, commit []byte, _ map[string][]byte) (checkout.Stats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	os.RemoveAll(r.dir)
	for p, data := range r.commits[string(commit)] {
		path := filepath.Join(r.dir, p)
		os.MkdirAll(filepath.Dir(path), 0o755)
		if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
			return checkout.Stats{}, err
		}
	}
	r.at = commit
	return checkout.Stats{}, nil
}

func (r *repo) Entries(_ context.Context, _ int64, commit []byte) ([]checkout.Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []checkout.Entry
	for p := range r.commits[string(commit)] {
		out = append(out, checkout.Entry{Path: p, Kind: checkout.File})
	}
	return out, nil
}

func (r *repo) ReadFiles(context.Context, int64, []checkout.ID, func(checkout.ID, []byte, bool) error) error {
	return nil
}

// Changed are the files whose contents differ between two commits.
func (r *repo) Changed(_ context.Context, _ int64, from, to []byte) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, b := r.commits[string(from)], r.commits[string(to)]
	var out []string
	for p, d := range b {
		if a[p] != d {
			out = append(out, p)
		}
	}
	for p := range a {
		if _, ok := b[p]; !ok {
			out = append(out, p)
		}
	}
	slices.Sort(out)
	return out, nil
}

// ─── The harness ───

type env struct {
	t       *testing.T
	store   *store.Store
	cluster *kubesim.Cluster
	runs    *runs
	build   *build
	tree    *tree
	// treeRepository is the repository whose tree is ok's: "home", the one repository, or none.
	treeRepository string
	repo           *repo
	seq            int64
	headSeq        int64
}

func newEnv(t *testing.T, objs ...runtime.Object) *env {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	b, err := postgres.Open(context.Background(), pool)
	st := store.New(b)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, store: st, cluster: kubesim.New(t, objs...), runs: newRuns(),
		build: &build{affects: map[string][]string{}, kinds: map[string]string{}, migrate: map[string]string{}},
		tree:  &tree{},
		repo:  &repo{commits: map[string]map[string]string{}, dir: t.TempDir()}}
	e.feed(&storeEvent{created: true})
	return e
}

type storeEvent struct {
	created bool
	head    []byte
}

// feed applies an event of the change feed.
func (e *env) feed(ev *storeEvent) {
	e.t.Helper()
	e.seq++
	fe := store.FeedEvent{Seq: e.seq, Repository: 1}
	if ev.created {
		fe.Name, fe.Created = "home", true
	} else {
		e.headSeq++
		fe.Head, fe.HeadSeq = ev.head, e.headSeq
	}
	if err := e.store.Apply(context.Background(), fe); err != nil {
		e.t.Fatal(err)
	}
}

// land puts a commit on main.
func (e *env) land(n byte, files map[string]string) []byte {
	c := e.repo.commit(n, files)
	e.feed(&storeEvent{head: c})
	return c
}

// tree is ok identity's Tree: each apply it was asked for, refusing what refuse says.
type tree struct {
	mu      sync.Mutex
	applies []*identitypb.ApplyRequest
	refuse  string
}

func (t *tree) Apply(_ context.Context, in *identitypb.ApplyRequest, _ ...grpc.CallOption) (*identitypb.ApplyResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.refuse != "" {
		return nil, status.Error(codes.FailedPrecondition, t.refuse)
	}
	t.applies = append(t.applies, in)
	return &identitypb.ApplyResponse{Changes: []string{fmt.Sprintf("%d files", len(in.GetFiles()))}}, nil
}

// engine is a deployer running in its blue or green copy.
func (e *env) engine(color string) *Engine {
	return New(Config{Store: e.store, Kube: e.cluster.Kube, Runs: e.runs, Build: e.build, Main: e.repo, Source: e.repo,
		Tree: e.tree, TreeRepository: e.treeRepository, Labels: roll.LabelsFor("example.com"),
		Checkout: e.repo, Timing: roll.DefaultTiming, Self: Self{Namespace: "ok-deploy", Name: "ok-deploy", Color: color},
		Principal: principal, Now: func() time.Time { return e.cluster.Now }})
}

// tick is one pass of the engine, its migrations waited for, and five seconds on.
func (e *env) tick(en *Engine, leader bool) {
	e.t.Helper()
	if err := en.Tick(context.Background(), leader); err != nil {
		e.t.Fatalf("a pass: %v", err)
	}
	en.Wait()
	e.cluster.Advance(5 * time.Second)
}

// until ticks the engine as the leader until cond holds.
func (e *env) until(en *Engine, what string, cond func() bool) {
	e.t.Helper()
	for i := 0; i < 1000; i++ {
		if cond() {
			return
		}
		e.tick(en, true)
	}
	e.t.Fatalf("never: %s\n%s", what, e.dump())
}

// deployments are every deployment, newest last.
func (e *env) deployments() []*store.Deployment {
	e.t.Helper()
	var out []*store.Deployment
	for id := int64(1); ; id++ {
		d, err := e.store.Get(context.Background(), id)
		if err != nil {
			return out
		}
		out = append(out, d)
	}
}

func (e *env) byName(name string) *store.Deployment {
	e.t.Helper()
	d := e.find(name)
	if d == nil {
		e.t.Fatalf("no deployment of %s", name)
	}
	return d
}

// find is the latest deployment of a deploy, or nil.
func (e *env) find(name string) *store.Deployment {
	var last *store.Deployment
	for _, d := range e.deployments() {
		if d.Name == name {
			last = d
		}
	}
	return last
}

func (e *env) settled() bool {
	for _, d := range e.deployments() {
		if d.Status.Open() {
			return false
		}
	}
	return len(e.deployments()) > 0
}

func (e *env) log(d *store.Deployment) string {
	e.t.Helper()
	evs, err := e.store.Events(context.Background(), d.ID)
	if err != nil {
		e.t.Fatal(err)
	}
	var lines []string
	for _, ev := range evs {
		lines = append(lines, ev.Message)
	}
	return strings.Join(lines, "\n")
}

func (e *env) dump() string {
	var b strings.Builder
	for _, d := range e.deployments() {
		fmt.Fprintf(&b, "deployment %d %s: %s at stage %d (%s)\n%s\n", d.ID, d.Name, d.Status, d.Stage, d.Reason, e.log(d))
	}
	return b.String()
}

// workflow is a workflow file declaring deploys.
const workflowFile = `deploy:
  web:
    migrate: [//web/schema:apply]
    images: {web: //web:push}
    workload: {namespace: web, deployment: web}
  api:
    images: {api: //api:push}
    workload: {namespace: api, deployment: api}
  data:
    migrate: [//data:apply]
`

func (e *env) kinds() {
	for t, k := range map[string]string{"//web/schema:apply": "go_binary", "//data:apply": "sh_binary",
		"//web:push": "oci_push", "//api:push": "oci_push", "//ok/deploy:push": "oci_push"} {
		e.build.kinds[t] = k
	}
}

// ─── Tests ───

// The first plan deploys everything, in waves: web (three replicas) through its migration,
// canary, and full stage; then data's migration; then api (one replica). Each image
// rolls by the digest its push reported, and every step is in its deployment's log.
func TestDeploy(t *testing.T) {
	e := newEnv(t, kubesim.Deployment("web", "web", 3, "old/web:1"), kubesim.Deployment("api", "api", 1, "old/api:1"))
	e.kinds()
	c1 := e.land(1, map[string]string{".ok/workflow/deploy.yaml": workflowFile, "web/web.go": "v1"})
	en := e.engine(roll.Blue)
	e.tick(en, true)
	ds := e.deployments()
	if len(ds) != 3 {
		t.Fatalf("%d deployments:\n%s", len(ds), e.dump())
	}
	waves := map[string]int{}
	for _, d := range ds {
		waves[d.Name] = d.Wave
	}
	if waves["web"] != 1 || waves["data"] != 2 || waves["api"] != 3 {
		t.Errorf("waves %v", waves)
	}
	// The plan records the services main declares: the deploys that roll a workload.
	svs, err := e.store.Services(context.Background(), 0)
	if err != nil || len(svs) != 2 || svs[0].Name != "api" || svs[1].Name != "web" || !svs[1].Declared || svs[1].Workload.String() != "deployment web/web" {
		t.Errorf("services %+v (%v)", svs, err)
	}
	// While web runs, the second wave waits.
	e.until(en, "web to reach its canary", func() bool {
		return e.cluster.Replicas("web", "web-green") == 1
	})
	if d := e.byName("api"); d.Status != store.Waiting {
		t.Errorf("api is %s while web's canary runs", d.Status)
	}
	e.until(en, "everything to settle", e.settled)
	for _, d := range e.deployments() {
		if d.Status != store.Succeeded {
			t.Errorf("%s: %s (%s)\n%s", d.Name, d.Status, d.Reason, e.log(d))
		}
	}
	web := image("//web:push", c1)
	if got := e.cluster.Image("web", "deployment", "web-green", "web"); got != web {
		t.Errorf("web-green runs %s, want %s", got, web)
	}
	if e.cluster.Replicas("web", "web") != 0 || e.cluster.Replicas("web", "web-green") != 3 {
		t.Errorf("web: blue %d, green %d", e.cluster.Replicas("web", "web"), e.cluster.Replicas("web", "web-green"))
	}
	if got := e.cluster.Running("api"); len(got) != 1 || got[0] != "deploy/api-green "+image("//api:push", c1) {
		t.Errorf("api runs %v", got)
	}
	log := e.log(e.byName("web"))
	for _, want := range []string{"planned at 01", "pushing container web's image, //web:push (run", "pushed container web's image: " + web,
		"applying //web/schema:apply", "applied //web/schema:apply", "canary: one green pod beside 3 blue", "full passed", "succeeded"} {
		if !strings.Contains(log, want) {
			t.Errorf("web's log lacks %q:\n%s", want, log)
		}
	}
	if !slices.Equal(e.build.applied, []string{"//web/schema:apply", "//data:apply"}) {
		t.Errorf("applied %v", e.build.applied)
	}

	// A change to web alone deploys web alone; one to nothing deployed deploys nothing.
	e.build.affects["web/web.go"] = []string{"//web:web", "//web:push"}
	c2 := e.land(2, map[string]string{".ok/workflow/deploy.yaml": workflowFile, "web/web.go": "v2"})
	e.tick(en, true)
	if d := e.byName("web"); d.Status == store.Succeeded || !strings.Contains(d.Why, "container web's image //web:push is affected") || d.FromSeq != 1 || d.ToSeq != 2 {
		t.Fatalf("web at c2: %+v", d)
	}
	if n := len(e.deployments()); n != 4 {
		t.Errorf("%d deployments after a change to web", n)
	}
	e.until(en, "web at c2 to settle", e.settled)
	if got := e.cluster.Image("web", "deployment", "web", "web"); got != image("//web:push", c2) || e.cluster.Replicas("web", "web") != 3 {
		t.Errorf("web at c2: blue runs %s ×%d", got, e.cluster.Replicas("web", "web"))
	}
	e.land(3, map[string]string{".ok/workflow/deploy.yaml": workflowFile, "web/web.go": "v2", "README": "hi"})
	e.tick(en, true)
	if n := len(e.deployments()); n != 4 {
		t.Errorf("%d deployments after a change to nothing deployed", n)
	}
}

// A canary that crash-loops rolls back, fails its deployment, and stops its plan: the waves
// after it never run. A retry at the same commit fails the same way; a fix deploys.
func TestFailureStopsThePlan(t *testing.T) {
	e := newEnv(t, kubesim.Deployment("web", "web", 3, "old/web:1"), kubesim.Deployment("api", "api", 1, "old/api:1"))
	e.kinds()
	c1 := e.land(1, map[string]string{".ok/workflow/deploy.yaml": workflowFile})
	e.cluster.Behavior[image("//web:push", c1)] = kubesim.Crashes
	en := e.engine(roll.Blue)
	e.until(en, "everything to settle", e.settled)
	web, api, data := e.byName("web"), e.byName("api"), e.byName("data")
	if web.Status != store.Failed || !strings.Contains(web.Reason, "rolled back: canary: pod web-green") {
		t.Errorf("web %s: %s\n%s", web.Status, web.Reason, e.log(web))
	}
	for _, d := range []*store.Deployment{api, data} {
		if d.Status != store.Canceled || d.Reason != fmt.Sprintf("deployment %d of its plan failed", web.ID) {
			t.Errorf("%s %s: %s", d.Name, d.Status, d.Reason)
		}
	}
	if e.cluster.Replicas("web", "web") != 3 || e.cluster.Replicas("web", "web-green") != 0 || e.cluster.Image("web", "deployment", "web", "web") != "old/web:1" {
		t.Errorf("web after the rollback: blue %d on %s", e.cluster.Replicas("web", "web"), e.cluster.Image("web", "deployment", "web", "web"))
	}
	if got := e.cluster.Running("api"); len(got) != 1 || got[0] != "deploy/api old/api:1" {
		t.Errorf("api was deployed: %v", got)
	}

	// A person retries api, canceled with the plan: it runs on its own.
	r, err := e.store.Retry(context.Background(), api.ID, "alice.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	e.until(en, "the retry to settle", e.settled)
	if got, _ := e.store.Get(context.Background(), r.ID); got.Status != store.Succeeded {
		t.Errorf("the retry of api: %s (%s)", got.Status, got.Reason)
	}
}

// A push that fails fails its deployment before anything changes in the cluster; a retry once
// the push works deploys.
func TestPushFails(t *testing.T) {
	e := newEnv(t, kubesim.Deployment("api", "api", 1, "old/api:1"))
	e.kinds()
	e.runs.fail["//api:push"] = true
	e.land(1, map[string]string{".ok/workflow/deploy.yaml": "deploy:\n  api:\n    images: {api: //api:push}\n    workload: {namespace: api, deployment: api}\n"})
	en := e.engine(roll.Blue)
	e.until(en, "api to settle", e.settled)
	api := e.byName("api")
	if api.Status != store.Failed || !strings.Contains(api.Reason, "the push of container api's image, //api:push, failed (run 1)") {
		t.Fatalf("api %s: %s", api.Status, api.Reason)
	}
	if got := e.cluster.Running("api"); got[0] != "deploy/api old/api:1" {
		t.Errorf("api changed: %v", got)
	}
	delete(e.runs.fail, "//api:push")
	if _, err := e.store.Retry(context.Background(), api.ID, "alice.human.example.com"); err != nil {
		t.Fatal(err)
	}
	e.until(en, "the retry to settle", e.settled)
	if got := e.byName("api"); got.Status != store.Succeeded || got.RetryOf != api.ID {
		t.Errorf("the retry: %s (%s)", got.Status, got.Reason)
	}
}

// A migration whose plan deletes data waits for a person; once they allow it, it applies with
// the allowance and the deployment rolls. One that fails fails its deployment before the roll.
func TestHazard(t *testing.T) {
	e := newEnv(t, kubesim.Deployment("web", "web", 3, "old/web:1"))
	e.kinds()
	e.build.migrate["//web/schema:apply"] = "refuse"
	e.land(1, map[string]string{".ok/workflow/deploy.yaml": "deploy:\n  web:\n    migrate: [//web/schema:apply]\n    images: {web: //web:push}\n    workload: {namespace: web, deployment: web}\n"})
	en := e.engine(roll.Blue)
	e.until(en, "web to wait for an allowance", func() bool { d := e.find("web"); return d != nil && d.Stage == store.Hazard })
	for range 5 {
		e.tick(en, true)
	}
	web := e.byName("web")
	if web.Stage != store.Hazard || web.Migrations[0].Status != store.MigrationWaiting || !slices.Equal(web.Migrations[0].Refused, []string{"DELETES_DATA"}) ||
		e.cluster.Replicas("web", "web") != 3 {
		t.Fatalf("waiting: %+v", web)
	}
	if !strings.Contains(e.log(web), "carries gated hazards (DELETES_DATA): it waits for a repository admin to allow them") {
		t.Errorf("the log:\n%s", e.log(web))
	}
	if _, err := e.store.Allow(context.Background(), web.ID, "ada.human.example.com"); err != nil {
		t.Fatal(err)
	}
	e.until(en, "web to settle", e.settled)
	if got := e.byName("web"); got.Status != store.Succeeded || got.Migrations[0].AllowedBy != "ada.human.example.com" {
		t.Errorf("after the allowance: %s (%s)", got.Status, got.Reason)
	}
	if !slices.Equal(e.build.applied, []string{"//web/schema:apply", "//web/schema:apply DELETES_DATA"}) {
		t.Errorf("applied %v", e.build.applied)
	}

	e.build.migrate["//web/schema:apply"] = "fail"
	e.build.affects["web/schema/web.sdl"] = []string{"//web/schema:apply"}
	e.land(2, map[string]string{".ok/workflow/deploy.yaml": "deploy:\n  web:\n    migrate: [//web/schema:apply]\n    images: {web: //web:push}\n    workload: {namespace: web, deployment: web}\n",
		"web/schema/web.sdl": "x"})
	e.tick(en, true)
	e.until(en, "web at 2 to settle", e.settled)
	if got := e.byName("web"); got.Status != store.Failed || got.Reason != "migration //web/schema:apply failed" || got.Migrations[0].Output != "connecting: refused" {
		t.Errorf("a failed migration: %s (%s)", got.Status, got.Reason)
	}
}

// A person halts a deployment mid-canary: it rolls back and stops its plan.
func TestHalt(t *testing.T) {
	e := newEnv(t, kubesim.Deployment("web", "web", 3, "old/web:1"), kubesim.Deployment("api", "api", 1, "old/api:1"))
	e.kinds()
	e.land(1, map[string]string{".ok/workflow/deploy.yaml": workflowFile})
	en := e.engine(roll.Blue)
	e.until(en, "web's canary", func() bool { return e.cluster.Replicas("web", "web-green") == 1 })
	if _, err := e.store.Halt(context.Background(), e.byName("web").ID, "bob.human.example.com"); err != nil {
		t.Fatal(err)
	}
	e.until(en, "everything to settle", e.settled)
	web := e.byName("web")
	if web.Status != store.Halted || web.Reason != "halted by bob.human.example.com" || e.cluster.Replicas("web", "web-green") != 0 || e.cluster.Replicas("web", "web") != 3 {
		t.Errorf("web %s: %s; green %d", web.Status, web.Reason, e.cluster.Replicas("web", "web-green"))
	}
	if api := e.byName("api"); api.Status != store.Canceled {
		t.Errorf("api %s", api.Status)
	}
}

// A deployer that stops mid-deployment, and another that takes over, pick it up where it
// stands.
func TestResume(t *testing.T) {
	e := newEnv(t, kubesim.Deployment("web", "web", 3, "old/web:1"))
	e.kinds()
	c1 := e.land(1, map[string]string{".ok/workflow/deploy.yaml": "deploy:\n  web:\n    images: {web: //web:push}\n    workload: {namespace: web, deployment: web}\n"})
	first := e.engine(roll.Blue)
	e.until(first, "web's canary to be watched a while", func() bool {
		d := e.find("web")
		if d == nil || d.Stage != store.Roll {
			return false
		}
		st, err := loadRoll(d)
		return err == nil && st.Stage == roll.StageCanary && e.cluster.Now.Sub(st.Started) > roll.DefaultTiming.Watch/2
	})
	second := e.engine(roll.Blue)
	e.until(second, "web to settle", e.settled)
	if d := e.byName("web"); d.Status != store.Succeeded || e.cluster.Image("web", "deployment", "web-green", "web") != image("//web:push", c1) {
		t.Errorf("web %s: %s\n%s", d.Status, d.Reason, e.log(d))
	}
	if n := strings.Count(e.log(e.byName("web")), "canary: one green pod"); n != 1 {
		t.Errorf("the canary began %d times", n)
	}
}

// Main moving again before a deployment starts replaces it — only the newest commit deploys —
// and the newer one takes its place: api, in the first plan's third wave, still waits for web
// and data to succeed, however many commits replace it.
func TestNewestWins(t *testing.T) {
	e := newEnv(t, kubesim.Deployment("web", "web", 3, "old/web:1"), kubesim.Deployment("api", "api", 1, "old/api:1"))
	e.kinds()
	e.build.affects["api/api.go"] = []string{"//api:push"}
	e.land(1, map[string]string{".ok/workflow/deploy.yaml": workflowFile, "api/api.go": "v1"})
	en := e.engine(roll.Blue)
	e.tick(en, true)
	// api waits behind web's wave; two more commits change api.
	e.land(2, map[string]string{".ok/workflow/deploy.yaml": workflowFile, "api/api.go": "v2"})
	e.tick(en, true)
	c3 := e.land(3, map[string]string{".ok/workflow/deploy.yaml": workflowFile, "api/api.go": "v3"})
	e.tick(en, true)
	for e.find("web").Status != store.Succeeded || e.find("data").Status != store.Succeeded {
		if d := e.find("api"); d.Status != store.Waiting && d.Status != store.Canceled {
			t.Fatalf("api at %x began before the first plan's earlier waves succeeded:\n%s", d.Commit[:1], e.dump())
		}
		e.tick(en, true)
	}
	e.until(en, "everything to settle", e.settled)
	if !strings.Contains(e.log(e.find("api")), "it takes the place of the deployment it replaced: wave 3 of plan 1, wave 1 of plan 2") {
		t.Errorf("api's log:\n%s", e.log(e.find("api")))
	}
	var apis []string
	for _, d := range e.deployments() {
		if d.Name == "api" {
			apis = append(apis, fmt.Sprintf("%x %s", d.Commit[:1], d.Status))
		}
	}
	if !slices.Equal(apis, []string{"01 canceled", "02 canceled", "03 succeeded"}) {
		t.Errorf("api's deployments %v", apis)
	}
	if got := e.cluster.Running("api"); got[0] != "deploy/api-green "+image("//api:push", c3) {
		t.Errorf("api runs %v", got)
	}
}

// The deployer's own deployment is driven by the copy that started it: the new copy never
// judges itself, the old one keeps running through every stage, and scales itself to 0 only
// once the deployment has succeeded.
func TestSelf(t *testing.T) {
	e := newEnv(t, kubesim.Deployment("ok-deploy", "ok-deploy", 1, "old/deploy:1"))
	e.kinds()
	c1 := e.land(1, map[string]string{".ok/workflow/deploy.yaml": "deploy:\n  self:\n    images: {ok-deploy: //ok/deploy:push}\n    workload: {namespace: ok-deploy, deployment: ok-deploy}\n"})
	blue, green := e.engine(roll.Blue), e.engine(roll.Green)
	e.tick(blue, true)
	d := e.byName("self")
	if !d.Self || d.Driver != roll.Blue {
		t.Fatalf("self %+v", d)
	}
	// The green copy, even as the leader, leaves it alone.
	before := e.log(d)
	for range 3 {
		e.tick(green, true)
	}
	if after := e.log(e.byName("self")); after != before {
		t.Errorf("the green copy stepped its own deployment:\n%s", after)
	}
	// The blue copy drives it without the leader lock.
	for i := 0; i < 1000 && !e.settled(); i++ {
		if e.cluster.Replicas("ok-deploy", "ok-deploy") != 1 {
			t.Fatalf("the driving copy went down mid-deployment:\n%s", e.dump())
		}
		e.tick(blue, false)
	}
	d = e.byName("self")
	if d.Status != store.Succeeded || e.cluster.Replicas("ok-deploy", "ok-deploy") != 0 || e.cluster.Replicas("ok-deploy", "ok-deploy-green") != 1 ||
		e.cluster.Image("ok-deploy", "deployment", "ok-deploy-green", "ok-deploy") != image("//ok/deploy:push", c1) {
		t.Fatalf("self %s: %s\n%s", d.Status, d.Reason, e.log(d))
	}
	if !strings.Contains(e.log(d), "the blue copy, the deployer that drove the roll, goes to 0") {
		t.Errorf("the log:\n%s", e.log(d))
	}
	if rs, _ := e.store.Retiring(context.Background()); len(rs) != 0 {
		t.Errorf("still retiring: %+v", rs)
	}
}

// The deployer's own deployment, planned with others, takes a wave of its own after theirs: it
// waits until every other one has succeeded — it never rolls itself beside another roll — and
// its failure (here its own database's migration) stops no one else's, all done already.
func TestSelfGoesLast(t *testing.T) {
	e := newEnv(t, kubesim.Deployment("web", "web", 3, "old/web:1"), kubesim.Deployment("api", "api", 1, "old/api:1"),
		kubesim.Deployment("ok-deploy", "ok-deploy", 1, "old/deploy:1"))
	e.kinds()
	e.build.kinds["//ok/deploy/schema:apply"] = "go_binary"
	e.build.migrate["//ok/deploy/schema:apply"] = "fail"
	e.land(1, map[string]string{".ok/workflow/deploy.yaml": workflowFile,
		"ok/.ok/workflow/deploy.yaml": "deploy:\n  self:\n    migrate: [//ok/deploy/schema:apply]\n    images: {ok-deploy: //ok/deploy:push}\n    workload: {namespace: ok-deploy, deployment: ok-deploy}\n"})
	en := e.engine(roll.Blue)
	e.tick(en, true)
	waves := map[string]int{}
	for _, d := range e.deployments() {
		waves[d.Name] = d.Wave
	}
	if waves["web"] != 1 || waves["data"] != 2 || waves["api"] != 3 || waves["self"] != 4 {
		t.Errorf("waves %v", waves)
	}
	e.until(en, "api to start", func() bool { return e.byName("api").Status != store.Waiting })
	for e.byName("api").Status.Open() {
		if s := e.byName("self"); s.Status != store.Waiting {
			t.Fatalf("self is %s while api deploys:\n%s", s.Status, e.dump())
		}
		e.tick(en, true)
	}
	e.until(en, "everything to settle", e.settled)
	for _, name := range []string{"web", "data", "api"} {
		if d := e.byName(name); d.Status != store.Succeeded {
			t.Errorf("%s: %s (%s)", name, d.Status, d.Reason)
		}
	}
	if d := e.byName("self"); d.Status != store.Failed || d.Reason != "migration //ok/deploy/schema:apply failed" {
		t.Errorf("self %s: %s\n%s", d.Status, d.Reason, e.log(d))
	}
	if e.cluster.Replicas("ok-deploy", "ok-deploy") != 1 || e.cluster.Image("ok-deploy", "deployment", "ok-deploy", "ok-deploy") != "old/deploy:1" {
		t.Errorf("the deployer changed though its deployment failed before its roll")
	}
}

// A deploy that runs alone — a database's, which restarts what every other service reaches — takes a wave of its own after
// the rest of its plan, and runs while no other deployment does: a newer plan's deployment that
// becomes due while it runs waits for it.
func TestAlone(t *testing.T) {
	e := newEnv(t, kubesim.Deployment("web", "web", 3, "old/web:1"), kubesim.Deployment("api", "api", 1, "old/api:1"),
		kubesim.StatefulSet("postgres", "postgres", 1, "old/db:1"))
	e.kinds()
	e.build.kinds["//db:push"] = "oci_push"
	const database = "deploy:\n  database:\n    images: {postgres: //db:push}\n    workload: {namespace: postgres, statefulset: postgres}\n    alone: true\n"
	e.land(1, map[string]string{".ok/workflow/deploy.yaml": workflowFile, "db/.ok/workflow/db.yaml": database})
	en := e.engine(roll.Blue)
	e.tick(en, true)
	waves := map[string]int{}
	for _, d := range e.deployments() {
		waves[d.Name] = d.Wave
	}
	if waves["web"] != 1 || waves["data"] != 2 || waves["api"] != 3 || waves["database"] != 4 {
		t.Errorf("waves %v", waves)
	}
	if !e.byName("database").Alone {
		t.Error("the database's deployment does not run alone")
	}
	e.until(en, "database to start", func() bool { return e.byName("database").Status != store.Waiting })
	for _, name := range []string{"web", "data", "api"} {
		if d := e.byName(name); d.Status != store.Succeeded {
			t.Errorf("database started while %s was %s", name, d.Status)
		}
	}
	// A change to api lands while database rolls: api's new deployment waits for it.
	e.build.affects["api/api.go"] = []string{"//api:push"}
	e.land(2, map[string]string{".ok/workflow/deploy.yaml": workflowFile, "db/.ok/workflow/db.yaml": database, "api/api.go": "v2"})
	e.tick(en, true)
	for e.byName("database").Status.Open() {
		if api := e.byName("api"); api.Status != store.Waiting && api.Status != store.Succeeded {
			t.Fatalf("api is %s while database runs alone:\n%s", api.Status, e.dump())
		}
		e.tick(en, true)
	}
	if d := e.byName("database"); d.Status != store.Succeeded {
		t.Fatalf("the database: %s (%s)\n%s", d.Status, d.Reason, e.log(d))
	}
	e.until(en, "everything to settle", e.settled)
	if d := e.byName("api"); d.Status != store.Succeeded || d.ToSeq != 2 {
		t.Errorf("api at c2: %s (%s), to %d", d.Status, d.Reason, d.ToSeq)
	}
}

// A deployment that runs alone waits for every running deployment, and none starts while it
// runs.
func TestOccupancy(t *testing.T) {
	w := func(ns, name string) *workflow.Workload {
		return &workflow.Workload{Namespace: ns, Kind: workflow.Deployment, Name: name}
	}
	web := &store.Deployment{Deploy: "//a:deploy:web", Workload: w("web", "web")}
	api := &store.Deployment{Deploy: "//a:deploy:api", Workload: w("api", "api")}
	db := &store.Deployment{Deploy: "//a:deploy:db", Workload: w("postgres", "postgres"), Alone: true}
	idle := occupancy{claims: map[string]bool{}}
	if !idle.admits(db) || !idle.admits(web) {
		t.Error("nothing running admits nothing")
	}
	busy := occupancy{claims: map[string]bool{}}
	busy.take(web)
	if busy.admits(db) || busy.admits(web) || !busy.admits(api) {
		t.Errorf("with web running: db %v, web %v, api %v", busy.admits(db), busy.admits(web), busy.admits(api))
	}
	alone := occupancy{claims: map[string]bool{}}
	alone.take(db)
	if alone.admits(api) || alone.admits(web) {
		t.Error("a deployment started while one that runs alone runs")
	}
}

// A deploy whose image is already running succeeds without rolling; a plan's notes carry a
// workflow file that does not parse.
func TestUnchanged(t *testing.T) {
	e := newEnv(t)
	e.kinds()
	c1 := e.repo.commit(1, nil)
	if _, err := e.cluster.Kube.AppsV1().Deployments("api").Create(context.Background(), kubesim.Deployment("api", "api", 2, image("//api:push", c1)), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	e.cluster.Reconcile()
	e.repo.commits[string(c1)] = map[string]string{".ok/workflow/deploy.yaml": "deploy:\n  api:\n    images: {api: //api:push}\n    workload: {namespace: api, deployment: api}\n",
		"x/.ok/workflow/bad.yaml": "pre: [\n", workflow.SettingsFile: "build: bazel\n"}
	e.feed(&storeEvent{head: c1})
	en := e.engine(roll.Blue)
	e.until(en, "api to settle", e.settled)
	if d := e.byName("api"); d.Status != store.Succeeded || d.Reason != "the workload already runs these images" {
		t.Errorf("api %s: %s", d.Status, d.Reason)
	}
	plans, err := e.store.Plans(context.Background(), 0)
	if err != nil || len(plans) != 1 || len(plans[0].Notes) != 1 || !strings.Contains(plans[0].Notes[0], "x/.ok/workflow/bad.yaml") {
		t.Errorf("the plans %+v (%v)", plans, err)
	}
}

// An imported repository's commits deploy nothing — not even a deploy its workflow files
// declare — and the deployer runs no Bazel on them.
func TestImported(t *testing.T) {
	e := newEnv(t)
	e.kinds()
	e.build.affects["web/web.go"] = []string{"//web:push"}
	files := map[string]string{workflow.SettingsFile: "build: none\n", ".ok/workflow/deploy.yaml": workflowFile, "web/web.go": "package web\n"}
	e.land(1, files)
	en := e.engine(roll.Blue)
	e.tick(en, true)
	changed := map[string]string{}
	for p, d := range files {
		changed[p] = d
	}
	changed["web/web.go"] = "package web // changed\n"
	e.land(2, changed)
	e.tick(en, true)
	e.tick(en, true)
	if ds := e.deployments(); len(ds) != 0 {
		t.Errorf("deployments of an imported repository:\n%s", e.dump())
	}
	if len(e.build.affected) != 0 || len(e.build.applied) != 0 || len(e.runs.runs) != 0 {
		t.Errorf("the deployer ran something: affected %v, applied %v, %d runs", e.build.affected, e.build.applied, len(e.runs.runs))
	}
	plans, err := e.store.Plans(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	var seqs []int64
	for _, p := range plans {
		seqs = append(seqs, p.HeadSeq)
		if len(p.Notes) != 1 || p.Notes[0] != "the repository is imported: ok deploys nothing from it" {
			t.Errorf("the plan at %d: notes %v", p.HeadSeq, p.Notes)
		}
	}
	if !slices.Equal(seqs, []int64{1, 2}) {
		t.Errorf("plans at %v, want each head planned", seqs)
	}
}

// The tree's repository applies ok's groups-and-ACLs tree: on its first plan, and whenever the
// tree or the settings change — its files, from the tree's folder, at the commit, ordered by its
// place on main. A refusal fails the deployment; another repository's tree folder is files.
func TestTree(t *testing.T) {
	e := newEnv(t)
	e.kinds()
	e.treeRepository = "home"
	settings := "build: bazel\nidentity: identity\n"
	files := map[string]string{workflow.SettingsFile: settings, ".ok/workflow/deploy.yaml": workflowFile, "web/web.go": "v1",
		"identity/acl.yaml": "members: [ada.human.example.com]\n", "identity/eng/group.yaml": "members: []\n"}
	c1 := e.land(1, files)
	en := e.engine(roll.Blue)
	e.tick(en, true)
	e.until(en, "the first plan to settle", e.settled)
	d := e.byName("identity")
	if d == nil || d.Status != store.Succeeded || d.Deploy != "//.ok/repository.yaml:deploy:identity" || len(e.tree.applies) != 1 {
		t.Fatalf("the tree's deployment: %+v\n%s", d, e.dump())
	}
	got := e.tree.applies[0]
	paths := []string{}
	for _, f := range got.GetFiles() {
		paths = append(paths, f.GetPath())
	}
	slices.Sort(paths)
	if got.GetCommit() != hex.EncodeToString(c1) || got.GetSequence() != 1 || !slices.Equal(paths, []string{"acl.yaml", "eng/group.yaml"}) {
		t.Errorf("the apply: commit %s, sequence %d, files %v", got.GetCommit(), got.GetSequence(), paths)
	}
	if m := d.Migrations[0]; m.Target != "ok:identity-tree" || m.Status != store.MigrationApplied || m.Output != "2 files" {
		t.Errorf("the tree's migration: %+v", m)
	}

	// A change beside the tree applies nothing.
	next := maps.Clone(files)
	next["web/web.go"] = "v2"
	e.land(2, next)
	e.tick(en, true)
	e.until(en, "the second plan to settle", e.settled)
	if len(e.tree.applies) != 1 {
		t.Errorf("a change beside the tree applied it: %d applies", len(e.tree.applies))
	}
	// A change to the tree applies it, at the change's place on main.
	next = maps.Clone(next)
	next["identity/eng/group.yaml"] = "members: [ada.human.example.com]\n"
	e.land(3, next)
	e.tick(en, true)
	e.until(en, "the third plan to settle", e.settled)
	if len(e.tree.applies) != 2 || e.tree.applies[1].GetSequence() != 3 {
		t.Fatalf("a change to the tree: %d applies\n%s", len(e.tree.applies), e.dump())
	}
	// ok identity refusing the tree fails its deployment.
	e.tree.refuse = "admins.acl.example.com would hold no one"
	next = maps.Clone(next)
	next["identity/acl.yaml"] = "members: []\n"
	e.land(4, next)
	e.tick(en, true)
	e.until(en, "the fourth plan to settle", e.settled)
	var last *store.Deployment
	for _, d := range e.deployments() {
		if d.Name == "identity" && (last == nil || d.ID > last.ID) {
			last = d
		}
	}
	if last.Status != store.Failed || !strings.Contains(last.Migrations[0].Output+last.Reason+e.log(last), "ok identity refused the tree: admins.acl.example.com would hold no one") {
		t.Errorf("a refused tree: %s (%s)\n%s", last.Status, last.Reason, e.log(last))
	}

	// Another repository's identity: folder is its own files: its plans apply nothing.
	other := newEnv(t)
	other.kinds()
	other.treeRepository = "infra"
	other.land(1, files)
	en = other.engine(roll.Blue)
	other.tick(en, true)
	other.until(en, "the plan to settle", other.settled)
	if d := other.find("identity"); d != nil || len(other.tree.applies) != 0 {
		t.Errorf("a repository other than the tree's applied it: %+v", d)
	}
}

// A migration is a binary speaking the migrate protocol: anything else fails, and runs nothing.
func TestMigrationNotABinary(t *testing.T) {
	e := newEnv(t)
	e.kinds()
	e.build.kinds["//data:apply"] = "genrule"
	e.land(1, map[string]string{".ok/workflow/deploy.yaml": "deploy:\n  data:\n    migrate: [//data:apply]\n"})
	en := e.engine(roll.Blue)
	e.until(en, "data to settle", e.settled)
	d := e.byName("data")
	if d.Status != store.Failed || len(e.build.applied) != 0 || !strings.Contains(e.log(d), "//data:apply is a genrule: a migration is a binary that speaks the migrate protocol") {
		t.Errorf("a genrule migration: %s (%s), applied %v\n%s", d.Status, d.Reason, e.build.applied, e.log(d))
	}
}
