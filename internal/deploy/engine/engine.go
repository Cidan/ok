// Package engine is the deployer (docs/deploy.md): it follows main through the SCM's change feed,
// plans the deployments each move of main makes due, and drives each deployment through its
// stages — the pushes on ok automation's workers, the migrations it runs with Bazel (and the
// groups-and-ACLs tree it applies to ok identity), and the watched roll — rolling back what
// fails.
//
// Everything it decides is taken again from its database and the cluster on each pass, so a
// deployer that restarts, or another replica that takes over, resumes every deployment where
// it stands. One replica plans and drives at a time (the leader lock); each deployment is
// stepped under a lock of its own, and a deployment of the deployer itself is driven only by
// the copy that started it (docs/deploy.md, "Resuming, and deploying the deployer").
package engine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/Cidan/ok/internal/buildsys"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/deploy/plan"
	"github.com/Cidan/ok/internal/deploy/roll"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/workflow"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Runs is what the deployer asks of ok automation: its pushes.
type Runs interface {
	StartRun(ctx context.Context, in *automationpb.StartRunRequest, opts ...grpc.CallOption) (*automationpb.Run, error)
	GetRun(ctx context.Context, in *automationpb.GetRunRequest, opts ...grpc.CallOption) (*automationpb.Run, error)
	CancelRun(ctx context.Context, in *automationpb.CancelRunRequest, opts ...grpc.CallOption) (*automationpb.Run, error)
}

// Build is what the deployer asks of a repository's build system, in its checkout.
type Build interface {
	// Affected is what the files changed between two commits of a repository affect, the
	// checkout holding the later one, to.
	Affected(ctx context.Context, repo int64, from, to []byte, files []string) (*plan.Affected, error)
	// Describe says what targets are at the commit the checkout holds; one that does not exist
	// is missing.
	Describe(ctx context.Context, repo int64, commit []byte, targets []string) (map[string]buildsys.Description, error)
	// Migrate runs a migration — a target speaking ok's migrate protocol — at the checkout,
	// allowing the gated hazard classes allow. An error is one worth trying again; the
	// migration's own failure is in Applied.
	Migrate(ctx context.Context, target string, allow []string) (Applied, error)
}

// Tree is ok identity's, which applies the groups-and-ACLs tree.
type Tree interface {
	Apply(ctx context.Context, in *identitypb.ApplyRequest, opts ...grpc.CallOption) (*identitypb.ApplyResponse, error)
}

// Applied is what a migration did.
type Applied struct {
	Output  string   // what the migration printed
	Report  []byte   // its report (the migrate protocol's), JSON
	Refused []string // the gated hazard classes it refused: it waits for a person to allow them
	Failed  string   // why it failed; "" when it applied (or only refused)
}

// Main reads main's history.
type Main interface {
	// Changed are the files that differ between two commits of a repository, a rename's both
	// paths.
	Changed(ctx context.Context, repo int64, from, to []byte) ([]string, error)
}

// Self is the deployer's own workload, and the copy of it this process runs in.
type Self struct {
	Namespace, Name string
	Color           string // roll.Blue or roll.Green
}

// Workload is the deployer's workload as deploys name it.
func (s Self) Workload() string {
	return (workflow.Workload{Namespace: s.Namespace, Kind: workflow.Deployment, Name: s.Name}).String()
}

// Config is what the engine works with.
type Config struct {
	Store     *store.Store
	Kube      kubernetes.Interface
	Runs      Runs
	Build     Build
	Main      Main
	Source    checkout.Source
	Checkout  checkout.Checkout
	Timing    roll.Timing
	Labels    roll.Labels
	Self      Self
	Principal string // the deployer's: plans are its, and pushes are started as it
	// Tree is ok identity's, and TreeRepository the repository whose groups-and-ACLs tree is ok's:
	// its plans have the built-in deploy that applies it.
	Tree           Tree
	TreeRepository string
	Now            func() time.Time // the clock; time.Now when nil
	Log            *slog.Logger
}

// Engine is the deployer.
type Engine struct {
	cfg    Config
	roller *roll.Roller
	// checkoutMu makes planning and migrations take the one checkout in turn.
	checkoutMu sync.Mutex
	mu         sync.Mutex
	migrating  map[int64]bool
	wg         sync.WaitGroup
}

// New makes an engine.
func New(cfg Config) *Engine {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Engine{cfg: cfg, roller: &roll.Roller{Kube: cfg.Kube, Timing: cfg.Timing, Labels: cfg.Labels}, migrating: map[int64]bool{}}
}

// Run plans and drives until ctx ends, every interval: as the leader when it holds the leader
// lock — a pass that loses it ends — and otherwise only its own deployment when this copy
// drives it.
func (e *Engine) Run(ctx context.Context, interval time.Duration) error {
	for {
		err := e.cfg.Store.Lead(ctx, func(ctx context.Context) error { return e.Tick(ctx, true) })
		if errors.Is(err, lock.ErrNotAcquired) {
			err = e.Tick(ctx, false)
		}
		if err != nil && ctx.Err() == nil {
			e.cfg.Log.Warn("deploy: a pass", "err", err)
		}
		select {
		case <-ctx.Done():
			e.wg.Wait()
			return nil
		case <-time.After(interval):
		}
	}
}

// Tick is one pass: as the leader, it plans each repository whose main moved and drives every
// deployment; otherwise it drives only a deployment of the deployer this copy started.
func (e *Engine) Tick(ctx context.Context, leader bool) error {
	var errs []error
	if leader {
		repos, err := e.cfg.Store.Unplanned(ctx)
		if err != nil {
			return err
		}
		for _, r := range repos {
			if err := e.planRepository(ctx, r); err != nil {
				errs = append(errs, fmt.Errorf("planning repository %d at %d: %w", r.ID, r.HeadSeq, err))
			}
		}
	}
	if err := e.drive(ctx, leader); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Wait waits for the migrations running in the background.
func (e *Engine) Wait() { e.wg.Wait() }

// ─── Planning ───

// checkOut moves the checkout to a commit (the caller holds checkoutMu), logging what it took.
func (e *Engine) checkOut(ctx context.Context, repo int64, commit []byte) error {
	start := time.Now()
	st, err := e.cfg.Checkout.Sync(ctx, repo, commit, nil)
	if err != nil {
		return fmt.Errorf("checking out %s: %w", short(commit), err)
	}
	e.cfg.Log.Info("deploy: checked out", "commit", short(commit), "took", time.Since(start).Round(time.Millisecond), "checkout", st.String())
	return nil
}

// planRepository plans a repository at its head (docs/deploy.md, "When a deploy runs").
func (e *Engine) planRepository(ctx context.Context, r store.Repository) error {
	e.checkoutMu.Lock()
	defer e.checkoutMu.Unlock()
	if err := e.checkOut(ctx, r.ID, r.Head); err != nil {
		return err
	}
	settings, err := ReadSettings(e.cfg.Checkout.Dir())
	if err != nil {
		// main's settings parse: the SCM refuses an upload whose settings do not.
		return err
	}
	if settings.Build == workflow.BuildNone {
		if _, err := e.cfg.Store.RecordPlan(ctx, store.PlanResult{Repository: r.ID, Head: r.Head, HeadSeq: r.HeadSeq, BaseSeq: r.PlannedSeq,
			Notes: []string{"the repository is imported: ok deploys nothing from it"}, By: e.cfg.Principal}); err != nil {
			return err
		}
		e.cfg.Log.Info("deploy: planned an imported repository", "repository", r.Name, "head", short(r.Head))
		return nil
	}
	files, err := e.workflowFiles(ctx, r.ID, r.Head)
	if err != nil {
		return err
	}
	deploys, problems := plan.Deploys(files)
	var notes []string
	for _, p := range problems {
		notes = append(notes, p.Error())
	}
	in := plan.Input{Deploys: deploys, First: r.PlannedHead == nil, Self: e.cfg.Self.Workload()}
	if e.cfg.TreeRepository != "" && r.Name == e.cfg.TreeRepository {
		in.TreeDir = settings.Identity
	}
	if !in.First {
		if in.Changed, err = e.cfg.Main.Changed(ctx, r.ID, r.PlannedHead, r.Head); err != nil {
			return fmt.Errorf("the files changed since %s: %w", short(r.PlannedHead), err)
		}
		if len(in.Changed) > 0 {
			if in.Affected, err = e.cfg.Build.Affected(ctx, r.ID, r.PlannedHead, r.Head, in.Changed); err != nil {
				return err
			}
			if len(in.Affected.Problems) > 0 {
				return fmt.Errorf("main's build graph: %s", strings.Join(in.Affected.Problems, "; "))
			}
		}
	}
	if in.Replicas, err = e.replicas(ctx, deploys); err != nil {
		return err
	}
	var news []store.NewDeployment
	for _, d := range plan.Make(in) {
		news = append(news, store.NewDeployment{Deploy: d.Deploy, Why: d.Why, Wave: d.Wave, FailedWhy: d.Failed,
			Self: d.Deploy.Workload != nil && d.Deploy.Workload.String() == e.cfg.Self.Workload()})
	}
	if _, err := e.cfg.Store.RecordPlan(ctx, store.PlanResult{Repository: r.ID, Head: r.Head, HeadSeq: r.HeadSeq, BaseSeq: r.PlannedSeq,
		Notes: notes, Deployments: news, Services: deploys, By: e.cfg.Principal}); err != nil {
		return err
	}
	e.cfg.Log.Info("deploy: planned", "repository", r.Name, "head", short(r.Head), "deployments", len(news), "notes", len(notes))
	return nil
}

// workflowFiles reads a commit's workflow files from the checkout, which holds it.
func (e *Engine) workflowFiles(ctx context.Context, repo int64, commit []byte) (map[string][]byte, error) {
	entries, err := e.cfg.Source.Entries(ctx, repo, commit)
	if err != nil {
		return nil, err
	}
	out := map[string][]byte{}
	for _, en := range entries {
		if en.Kind != checkout.File || !workflow.IsWorkflowFile(en.Path) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(e.cfg.Checkout.Dir(), filepath.FromSlash(en.Path)))
		if err != nil {
			return nil, err
		}
		out[en.Path] = data
	}
	return out, nil
}

// replicas are each deploy's workload's replicas now: a Deployment's live copy's.
func (e *Engine) replicas(ctx context.Context, deploys []workflow.Deploy) (map[string]int32, error) {
	out := map[string]int32{}
	for _, d := range deploys {
		w := d.Workload
		if w == nil {
			continue
		}
		n, err := e.liveReplicas(ctx, *w)
		if err != nil {
			return nil, err
		}
		if n >= 0 {
			out[w.String()] = n
		}
	}
	return out, nil
}

// liveReplicas are a workload's replicas now, or -1 when it does not exist.
func (e *Engine) liveReplicas(ctx context.Context, w workflow.Workload) (int32, error) {
	apps := e.cfg.Kube.AppsV1()
	count := func(n *int32) int32 {
		if n == nil {
			return 1
		}
		return *n
	}
	if w.Kind == workflow.StatefulSet {
		s, err := apps.StatefulSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if notFound(err) {
			return -1, nil
		}
		if err != nil {
			return 0, err
		}
		return count(s.Spec.Replicas), nil
	}
	best := int32(-1)
	for _, name := range []string{w.Name, w.Name + "-green"} {
		d, err := apps.Deployments(w.Namespace).Get(ctx, name, metav1.GetOptions{})
		if notFound(err) {
			continue
		}
		if err != nil {
			return 0, err
		}
		best = max(best, count(d.Spec.Replicas))
	}
	return best, nil
}

// ─── Driving ───

// drive retires what a finished deployment of the deployer leaves behind, starts the waiting
// deployments whose turn it is, and steps every running one this copy drives.
func (e *Engine) drive(ctx context.Context, leader bool) error {
	var errs []error
	retiring, err := e.cfg.Store.Retiring(ctx)
	if err != nil {
		return err
	}
	for _, d := range retiring {
		if !leader && d.Driver != e.cfg.Self.Color {
			continue
		}
		st, err := loadRoll(d)
		if err == nil {
			err = e.retire(ctx, d, &st)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("deployment %d: %w", d.ID, err))
		}
	}
	ds, err := e.cfg.Store.Unfinished(ctx)
	if err != nil {
		return err
	}
	busy := occupancy{claims: map[string]bool{}}
	for _, d := range ds {
		if d.Status == store.Running {
			busy.take(d)
		}
	}
	for _, d := range ds {
		if d.Status == store.Waiting {
			if !leader {
				continue
			}
			ok, err := e.mayStart(ctx, d, busy)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if !ok {
				continue
			}
			driver := ""
			if d.Self {
				driver = e.cfg.Self.Color
			}
			if err := e.cfg.Store.Start(ctx, d.ID, driver); err != nil {
				errs = append(errs, err)
				continue
			}
			busy.take(d)
			d.Driver = driver
		}
		if d.Self && d.Driver != e.cfg.Self.Color {
			continue // the copy that started it drives it
		}
		if !leader && !d.Self {
			continue
		}
		if err := e.stepLocked(ctx, d.ID); err != nil {
			errs = append(errs, fmt.Errorf("deployment %d: %w", d.ID, err))
		}
	}
	return errors.Join(errs...)
}

// occupancy is what the running deployments hold.
type occupancy struct {
	claims  map[string]bool // their deploys and workloads
	running int
	alone   bool // one of them runs alone
}

// take records a deployment as running.
func (o *occupancy) take(d *store.Deployment) {
	for _, k := range claims(d) {
		o.claims[k] = true
	}
	o.running++
	o.alone = o.alone || d.Alone
}

// admits reports whether d may run beside what is running: nothing else deploys its deploy or
// workload, no deployment that runs alone is running, and — for one that runs alone — nothing
// is running at all.
func (o *occupancy) admits(d *store.Deployment) bool {
	if o.alone || (d.Alone && o.running > 0) {
		return false
	}
	for _, k := range claims(d) {
		if o.claims[k] {
			return false
		}
	}
	return true
}

// claims are what a running deployment holds: its deploy, and its workload.
func claims(d *store.Deployment) []string {
	out := []string{fmt.Sprintf("deploy %d %s", d.Repository, d.Deploy)}
	if d.Workload != nil {
		out = append(out, d.Workload.String())
	}
	return out
}

// mayStart reports whether a waiting deployment's turn has come: what is running admits it
// (occupancy.admits), and every deployment of its plan's earlier waves — and of the earlier
// waves of each place it took in an earlier plan — succeeded (or was replaced).
func (e *Engine) mayStart(ctx context.Context, d *store.Deployment, busy occupancy) (bool, error) {
	if !busy.admits(d) {
		return false, nil
	}
	gates := d.Gates
	if d.PlanID != 0 {
		gates = append(gates, store.Gate{Plan: d.PlanID, Wave: d.Wave})
	}
	for _, g := range gates {
		waves, err := e.cfg.Store.PlanStatuses(ctx, g.Plan)
		if err != nil {
			return false, err
		}
		for w := 1; w < g.Wave; w++ {
			for _, st := range waves[w] {
				if st != store.Succeeded && st != store.Canceled {
					return false, nil
				}
			}
		}
	}
	return true, nil
}

// stepLocked steps a deployment under its lock; a deployment someone else steps is skipped.
func (e *Engine) stepLocked(ctx context.Context, id int64) error {
	release, ok, err := e.cfg.Store.LockDeployment(ctx, id)
	if err != nil || !ok {
		return err
	}
	kept := false
	defer func() {
		if !kept {
			release()
		}
	}()
	d, err := e.cfg.Store.Get(ctx, id)
	if err != nil {
		return err
	}
	if !d.Status.Open() || d.Status != store.Running {
		return nil
	}
	if d.HaltBy != "" {
		return e.halt(ctx, d)
	}
	switch d.Stage {
	case store.Push:
		return e.push(ctx, d)
	case store.Migrate:
		kept, err = e.migrate(ctx, d, release)
		return err
	case store.Roll:
		return e.roll(ctx, d)
	}
	return nil // a hazard waits for a person
}

// fail ends a deployment failed, and stops its plan.
func (e *Engine) fail(ctx context.Context, d *store.Deployment, reason string) error {
	return e.end(ctx, d, store.Failed, reason)
}

func (e *Engine) end(ctx context.Context, d *store.Deployment, st store.Status, reason string) error {
	if err := e.cfg.Store.Finish(ctx, d.ID, st, reason); err != nil {
		return err
	}
	e.cfg.Log.Info("deploy: "+st.String(), "deployment", d.ID, "deploy", d.Deploy, "reason", reason)
	if d.PlanID != 0 && (st == store.Failed || st == store.Halted) {
		return e.cfg.Store.StopPlan(ctx, d.PlanID, d.ID)
	}
	return nil
}

// ─── Push ───

// push starts each image's push on a worker, and waits for them all.
func (e *Engine) push(ctx context.Context, d *store.Deployment) error {
	if len(d.Images) == 0 {
		return e.cfg.Store.SetStage(ctx, d.ID, store.Migrate)
	}
	for _, im := range d.Images {
		if im.RunID != 0 {
			continue
		}
		r, err := e.cfg.Runs.StartRun(ctx, &automationpb.StartRunRequest{
			RepositoryId: d.Repository, CommitId: d.Commit,
			Job:    &automationpb.Job{Kind: automationpb.Job_KIND_PUSH, Targets: []string{im.Target}},
			Origin: &automationpb.Origin{Phase: automationpb.Origin_PHASE_DEPLOY, Job: d.Deploy},
		})
		switch status.Code(err) {
		case codes.OK:
		case codes.InvalidArgument, codes.PermissionDenied, codes.NotFound:
			return e.fail(ctx, d, fmt.Sprintf("ok automation refused the push of %s: %s", im.Target, status.Convert(err).Message()))
		default:
			return err
		}
		if err := e.cfg.Store.SetPushRun(ctx, d.ID, im.Container, r.GetId()); err != nil {
			return err
		}
		if err := e.cfg.Store.Log(ctx, d.ID, fmt.Sprintf("pushing container %s's image, %s (run %d)", im.Container, im.Target, r.GetId())); err != nil {
			return err
		}
		im.RunID = r.GetId()
		d.Images = replaceImage(d.Images, im)
	}
	pending := 0
	for _, im := range d.Images {
		if im.Reference != "" {
			continue
		}
		r, err := e.cfg.Runs.GetRun(ctx, &automationpb.GetRunRequest{Id: im.RunID})
		if err != nil {
			return err
		}
		switch r.GetStatus() {
		case automationpb.Run_STATUS_QUEUED, automationpb.Run_STATUS_RUNNING:
			pending++
		case automationpb.Run_STATUS_PASSED:
			if len(r.GetPushed()) != 1 {
				return e.fail(ctx, d, fmt.Sprintf("run %d passed without saying what it pushed", im.RunID))
			}
			ref := Reference(r.GetPushed()[0])
			if err := e.cfg.Store.SetPushed(ctx, d.ID, im.Container, ref); err != nil {
				return err
			}
			if err := e.cfg.Store.Log(ctx, d.ID, fmt.Sprintf("pushed container %s's image: %s", im.Container, ref)); err != nil {
				return err
			}
		default:
			return e.fail(ctx, d, fmt.Sprintf("the push of container %s's image, %s, %s (run %d): %s", im.Container, im.Target,
				runOutcome(r.GetStatus()), im.RunID, r.GetReason()))
		}
	}
	if pending > 0 {
		return nil
	}
	return e.cfg.Store.SetStage(ctx, d.ID, store.Migrate)
}

func replaceImage(ims []store.Image, im store.Image) []store.Image {
	for i := range ims {
		if ims[i].Container == im.Container {
			ims[i] = im
		}
	}
	return ims
}

func runOutcome(s automationpb.Run_Status) string {
	switch s {
	case automationpb.Run_STATUS_FAILED:
		return "failed"
	case automationpb.Run_STATUS_CANCELED:
		return "was canceled"
	}
	return "could not run"
}

// Reference is the image reference a pushed image is pulled by: registry/repository@digest.
func Reference(p *automationpb.PushedImage) string {
	registry := p.GetRegistry()
	if _, host, ok := strings.Cut(registry, "://"); ok {
		registry = host
	}
	return strings.TrimSuffix(registry, "/") + "/" + p.GetRepository() + "@" + p.GetDigest()
}

// ─── Migrate ───

// migrate runs the next migration in the background, handing it the deployment's lock, and
// moves on once each has applied. It reports whether it handed the lock off.
func (e *Engine) migrate(ctx context.Context, d *store.Deployment, release func()) (bool, error) {
	for _, m := range d.Migrations {
		switch m.Status {
		case store.MigrationApplied:
			continue
		case store.MigrationFailed:
			return false, e.fail(ctx, d, fmt.Sprintf("migration %s failed", m.Target))
		case store.MigrationWaiting:
			return false, e.cfg.Store.SetStage(ctx, d.ID, store.Hazard)
		}
		e.mu.Lock()
		if e.migrating[d.ID] {
			e.mu.Unlock()
			return false, nil
		}
		e.migrating[d.ID] = true
		e.mu.Unlock()
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			defer release()
			defer func() {
				e.mu.Lock()
				delete(e.migrating, d.ID)
				e.mu.Unlock()
			}()
			if err := e.apply(context.WithoutCancel(ctx), d, m); err != nil {
				e.cfg.Log.Warn("deploy: a migration", "deployment", d.ID, "migration", m.Target, "err", err)
			}
		}()
		return true, nil
	}
	return false, e.cfg.Store.SetStage(ctx, d.ID, store.Roll)
}

// apply runs one migration on the checkout of the deployment's commit, and records what it did.
func (e *Engine) apply(ctx context.Context, d *store.Deployment, m store.Migration) error {
	e.checkoutMu.Lock()
	defer e.checkoutMu.Unlock()
	if err := e.checkOut(ctx, d.Repository, d.Commit); err != nil {
		return err
	}
	if m.Target == plan.TreeTarget {
		return e.applyTree(ctx, d, m)
	}
	described, err := e.cfg.Build.Describe(ctx, d.Repository, d.Commit, []string{m.Target})
	if err != nil {
		return err
	}
	desc, ok := described[m.Target]
	switch {
	case !ok:
		return e.recordMigration(ctx, d, m, store.MigrationFailed, Applied{Failed: fmt.Sprintf("%s does not exist at %s", m.Target, short(d.Commit))})
	case !desc.Binary:
		return e.recordMigration(ctx, d, m, store.MigrationFailed, Applied{Failed: fmt.Sprintf("%s is a %s: a migration is a binary that speaks the migrate protocol", m.Target, desc.Kind)})
	}
	if err := e.cfg.Store.Log(ctx, d.ID, fmt.Sprintf("applying %s", m.Target)); err != nil {
		return err
	}
	a, err := e.cfg.Build.Migrate(ctx, m.Target, m.Allowed)
	if err != nil {
		return err
	}
	switch {
	case a.Failed == "":
		return e.recordMigration(ctx, d, m, store.MigrationApplied, a)
	case len(a.Refused) > 0:
		return e.recordMigration(ctx, d, m, store.MigrationWaiting, a)
	}
	return e.recordMigration(ctx, d, m, store.MigrationFailed, a)
}

// applyTree applies the groups-and-ACLs tree at the deployment's commit — the checkout holds it —
// to ok identity, ordered by the commit's place on main.
func (e *Engine) applyTree(ctx context.Context, d *store.Deployment, m store.Migration) error {
	if e.cfg.Tree == nil {
		return e.recordMigration(ctx, d, m, store.MigrationFailed, Applied{Failed: "the deployer has no ok identity to apply the tree to"})
	}
	settings, err := ReadSettings(e.cfg.Checkout.Dir())
	if err != nil {
		return err
	}
	if settings.Identity == "" {
		return e.recordMigration(ctx, d, m, store.MigrationFailed, Applied{Failed: fmt.Sprintf("the settings at %s name no tree (identity:)", short(d.Commit))})
	}
	entries, err := e.cfg.Source.Entries(ctx, d.Repository, d.Commit)
	if err != nil {
		return err
	}
	req := &identitypb.ApplyRequest{Commit: hex.EncodeToString(d.Commit), Sequence: d.ToSeq}
	for _, en := range entries {
		rel, ok := strings.CutPrefix(en.Path, settings.Identity+"/")
		if !ok || en.Kind != checkout.File {
			continue
		}
		data, err := os.ReadFile(filepath.Join(e.cfg.Checkout.Dir(), filepath.FromSlash(en.Path)))
		if err != nil {
			return err
		}
		req.Files = append(req.Files, &identitypb.File{Path: rel, Content: data})
	}
	if err := e.cfg.Store.Log(ctx, d.ID, fmt.Sprintf("applying the groups-and-ACLs tree (%s, %d files) to ok identity", settings.Identity, len(req.Files))); err != nil {
		return err
	}
	resp, err := e.cfg.Tree.Apply(ctx, req)
	switch status.Code(err) {
	case codes.OK:
	case codes.InvalidArgument, codes.FailedPrecondition, codes.Aborted:
		return e.recordMigration(ctx, d, m, store.MigrationFailed, Applied{Failed: "ok identity refused the tree: " + status.Convert(err).Message()})
	default:
		return err
	}
	out := "the tree is as ok identity holds it: nothing changed"
	if len(resp.GetChanges()) > 0 {
		out = strings.Join(resp.GetChanges(), "\n")
	}
	return e.recordMigration(ctx, d, m, store.MigrationApplied, Applied{Output: out})
}

// ReadSettings reads a repository's settings at a checkout.
func ReadSettings(dir string) (*workflow.Settings, error) {
	data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(workflow.SettingsFile)))
	if errors.Is(err, os.ErrNotExist) {
		return workflow.ParseSettings(nil, false)
	}
	if err != nil {
		return nil, err
	}
	return workflow.ParseSettings(data, true)
}

func (e *Engine) recordMigration(ctx context.Context, d *store.Deployment, m store.Migration, st store.MigrationStatus, a Applied) error {
	if err := e.cfg.Store.SetMigration(ctx, d.ID, m.Position, st, a.Output, a.Report, a.Refused); err != nil {
		return err
	}
	switch st {
	case store.MigrationApplied:
		return e.cfg.Store.Log(ctx, d.ID, fmt.Sprintf("applied %s", m.Target))
	case store.MigrationWaiting:
		if err := e.cfg.Store.SetStage(ctx, d.ID, store.Hazard); err != nil {
			return err
		}
		return e.cfg.Store.Log(ctx, d.ID, fmt.Sprintf("%s's plan carries gated hazards (%s): it waits for a repository admin to allow them",
			m.Target, strings.Join(a.Refused, ", ")))
	}
	return e.cfg.Store.Log(ctx, d.ID, fmt.Sprintf("%s failed: %s", m.Target, a.Failed))
}

// ─── Roll ───

// target is what a deployment rolls.
func (e *Engine) target(d *store.Deployment) roll.Target {
	images := map[string]string{}
	for _, im := range d.Images {
		images[im.Container] = im.Reference
	}
	return roll.Target{Namespace: d.Workload.Namespace, Kind: d.Workload.Kind, Name: d.Workload.Name, Images: images, Self: d.Self}
}

func loadRoll(d *store.Deployment) (roll.State, error) {
	var st roll.State
	if len(d.Roll) == 0 {
		return st, nil
	}
	err := json.Unmarshal(d.Roll, &st)
	return st, err
}

func (e *Engine) saveRoll(ctx context.Context, d *store.Deployment, st *roll.State) error {
	if said := st.TakeEvents(); len(said) > 0 {
		if err := e.cfg.Store.Log(ctx, d.ID, said...); err != nil {
			return err
		}
	}
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return e.cfg.Store.SetRoll(ctx, d.ID, data)
}

// roll steps the deployment's roll, and ends the deployment when the roll is over.
func (e *Engine) roll(ctx context.Context, d *store.Deployment) error {
	if d.Workload == nil {
		return e.end(ctx, d, store.Succeeded, "migrated")
	}
	st, err := loadRoll(d)
	if err != nil {
		return err
	}
	tgt := e.target(d)
	err = e.roller.Step(ctx, tgt, &st, e.cfg.Now())
	if serr := e.saveRoll(ctx, d, &st); serr != nil {
		return serr
	}
	var cfgErr *roll.ConfigError
	if errors.As(err, &cfgErr) {
		return e.fail(ctx, d, cfgErr.Message)
	}
	if err != nil || !st.Done() {
		return err
	}
	switch st.Outcome {
	case roll.Unchanged:
		return e.end(ctx, d, store.Succeeded, "the workload already runs these images")
	case roll.Rolled:
		if err := e.end(ctx, d, store.Succeeded, ""); err != nil {
			return err
		}
		if st.Retire {
			return e.retire(ctx, d, &st)
		}
		return nil
	case roll.Stuck:
		return e.fail(ctx, d, fmt.Sprintf("%s; the rollback did not come back Ready", st.Reason))
	}
	return e.fail(ctx, d, "rolled back: "+st.Reason)
}

// retire ends the old copy of a successful deployment of the deployer — likely this process —
// and records that it did.
func (e *Engine) retire(ctx context.Context, d *store.Deployment, st *roll.State) error {
	if err := e.roller.Retire(ctx, e.target(d), st); err != nil {
		return err
	}
	return e.saveRoll(ctx, d, st)
}

// ─── Halt ───

// halt stops a deployment someone asked to halt: its pushes are canceled, and a roll under way
// rolls back first.
func (e *Engine) halt(ctx context.Context, d *store.Deployment) error {
	reason := "halted by " + d.HaltBy
	switch d.Stage {
	case store.Queued, store.Push:
		for _, im := range d.Images {
			if im.RunID != 0 && im.Reference == "" {
				if _, err := e.cfg.Runs.CancelRun(ctx, &automationpb.CancelRunRequest{Id: im.RunID, Reason: "its deployment was " + reason}); err != nil &&
					status.Code(err) != codes.NotFound {
					return err
				}
			}
		}
	case store.Roll:
		if d.Workload == nil {
			break
		}
		st, err := loadRoll(d)
		if err != nil {
			return err
		}
		tgt := e.target(d)
		switch {
		case st.Done():
		case st.Stage == roll.StageRollback:
			err = e.roller.Step(ctx, tgt, &st, e.cfg.Now())
		default:
			err = e.roller.Abort(ctx, tgt, &st, e.cfg.Now(), reason)
		}
		if serr := e.saveRoll(ctx, d, &st); serr != nil {
			return serr
		}
		if err != nil {
			return err
		}
		if !st.Done() {
			return nil
		}
		if st.Outcome == roll.Stuck {
			reason += "; the rollback did not come back Ready"
		} else if st.Outcome == roll.Rolled {
			return e.end(ctx, d, store.Succeeded, "it had rolled before the halt")
		}
	}
	return e.end(ctx, d, store.Halted, reason)
}

// ─── Helpers ───

func short(commit []byte) string {
	return fmt.Sprintf("%x", commit)[:min(12, 2*len(commit))]
}

func notFound(err error) bool { return apierrors.IsNotFound(err) }
