// Package worker is an ok automation worker (docs/automation.md, "Execution"): it takes runs from
// ok automation — as many at once as it has slots, each with a checkout of its own, while it has
// the memory for another — checks out each run's commit, runs Bazel on it, streams Bazel's output
// into the run's log, and records the outcome.
//
// Bazel is the only thing it runs. What a job's code runs at build time, Bazel runs in its sandbox
// without network; what a run job runs, and the formatter a lint runs, run under bubblewrap: no
// network, every file read-only but the run's own scratch directory.
package worker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/automation/changes"
	"github.com/Cidan/ok/internal/buildsys/bazel"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/proc"
	"github.com/Cidan/ok/internal/workflow"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Config is a worker's configuration.
type Config struct {
	// Name is the worker's name among the workers (its pod's). With several slots, each holds
	// its runs' leases as Name/<slot>.
	Name string
	Work automationpb.WorkClient
	// Checkouts present each run's commit, one per slot: the worker runs a run in each at once.
	// Bases, one per slot too, present a planning run's other commit — the change's base, or the
	// commit pre ran on — when its graph is not cached. Source reads commits from the SCM: a
	// planning run's workflow files on main, and the files a change touches at its base.
	Checkouts []checkout.Checkout
	Bases     []checkout.Checkout
	Source    checkout.Source
	// Browse reads a run's change's files from the SCM: they are its job context's, and
	// never travel with the run.
	Browse scmpb.BrowseClient
	// Bazel is Bazel's path (or bazelisk's), BazelFlags the installation's flags for every
	// command (a remote cache), Bwrap bubblewrap's path, and Env their whole environment; the
	// worker adds TMPDIR, a directory of the run's own under Scratch. Proc is how the sandbox
	// gets its /proc, as CheckSandbox found it can: ProcFresh when nil.
	Bazel      string
	BazelFlags []string
	Bwrap      string
	Proc       []string
	// Secrets are what every Bazel command runs out of sight of (HideSecrets): the files and
	// directories the worker's configuration names, and its okfs daemon's state and credential.
	// With none, Bazel runs in the worker's own view (tests).
	Secrets []string
	Env     []string
	Scratch string
	// OutputBases is where each slot keeps its Bazel output bases — slot n's in n/ and n-base/ —
	// outside every checkout, kept from run to run so a slot builds incrementally. Graphs is where
	// commits' build graphs are kept, by commit, so each is queried once.
	OutputBases string
	Graphs      string
	// Names are the installation's: what OWNERS files and the groups-and-ACLs tree name people
	// and sets under, for the planning run's approvability check. Directory is ok identity's, whose
	// applied tree a repository other than TreeRepository — the one whose tree is ok's — resolves
	// its acl: owners with.
	Names          identity.Names
	Directory      identitypb.DirectoryClient
	TreeRepository string
	// Grace is how long a command has to stop when a run is stopped, before it is killed.
	Grace time.Duration
	Log   *slog.Logger
	// Contact, when set, records each answer from ok automation: the worker's readiness.
	Contact *Contact
	// Room, when set, says whether the worker has the memory for another run beside starting
	// ones that started too recently to show in its use (MemoryRoom). Without it, the slots alone
	// bound the runs.
	Room func(starting int) bool

	// A slot's: its checkouts and their output bases, and the other slots', which its runs do not
	// see.
	checkout, base             checkout.Checkout
	outputBase, baseOutputBase string
	others                     []string
}

// leaseWait is how long one Lease call waits for a run.
const leaseWait = 60 * time.Second

// settling is how long a run that started counts as starting: its memory use is not yet its own.
const settling = 15 * time.Second

// roomPoll is how often a worker without room for another run looks again.
const roomPoll = 2 * time.Second

// slot is where one run at a time runs: its checkout, and its lease name.
type slot struct {
	cfg   Config
	files *changedFiles
}

// Run takes and runs runs until ctx ends, in every slot at once. It leases one run at a time,
// for a free slot, and only while there is room for it. A run in progress when ctx ends goes
// back to the queue.
func Run(ctx context.Context, cfg Config) error {
	if len(cfg.Checkouts) == 0 || len(cfg.Bases) != len(cfg.Checkouts) {
		return errors.New("a worker runs its runs in at least one checkout, each with a checkout for a plan's other commit")
	}
	for _, dir := range []string{cfg.Scratch, cfg.OutputBases, cfg.Graphs} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	free := make(chan *slot, len(cfg.Checkouts))
	for i, co := range cfg.Checkouts {
		sc := cfg
		sc.checkout, sc.base = co, cfg.Bases[i]
		sc.outputBase = filepath.Join(cfg.OutputBases, strconv.Itoa(i+1))
		sc.baseOutputBase = filepath.Join(cfg.OutputBases, strconv.Itoa(i+1)+"-base")
		for j := range cfg.Checkouts {
			if j != i {
				sc.others = append(sc.others, cfg.Checkouts[j].Dir(), cfg.Bases[j].Dir(),
					filepath.Join(cfg.OutputBases, strconv.Itoa(j+1)), filepath.Join(cfg.OutputBases, strconv.Itoa(j+1)+"-base"))
			}
		}
		if len(cfg.Checkouts) > 1 {
			sc.Name = fmt.Sprintf("%s/%d", cfg.Name, i+1)
		}
		free <- &slot{cfg: sc, files: &changedFiles{browse: cfg.Browse}}
	}
	var running sync.WaitGroup
	defer running.Wait()
	var started []time.Time
	backoff := time.Second
	for {
		var s *slot
		select {
		case s = <-free:
		case <-ctx.Done():
			return nil
		}
		starting := func() int {
			now := time.Now()
			started = slices.DeleteFunc(started, func(t time.Time) bool { return now.Sub(t) > settling })
			return len(started)
		}
		if !waitForRoom(ctx, cfg, starting) {
			return nil
		}
		resp, err := cfg.Work.Lease(ctx, &automationpb.LeaseRequest{Worker: s.cfg.Name, WaitSeconds: int32(leaseWait / time.Second)})
		if err != nil {
			free <- s
			if ctx.Err() != nil {
				return nil
			}
			cfg.Log.Warn("leasing a run", "err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, 30*time.Second)
			continue
		}
		cfg.Contact.heard(time.Now())
		backoff = time.Second
		if resp.GetRun() == nil {
			free <- s
			continue
		}
		started = append(started, time.Now())
		every := time.Duration(resp.GetHeartbeatSeconds()) * time.Second
		if every <= 0 {
			every = 10 * time.Second
		}
		running.Add(1)
		go func() {
			defer running.Done()
			execute(ctx, s.cfg, s.files, resp.GetRun(), resp.GetContext(), every)
			free <- s
		}()
	}
}

// waitForRoom waits until the worker has room for another run, beside the starting ones, and
// reports whether it may take one: false once ctx ends.
func waitForRoom(ctx context.Context, cfg Config, starting func() int) bool {
	waited := false
	for cfg.Room != nil && !cfg.Room(starting()) {
		if !waited {
			cfg.Log.Info("waiting for the memory to take another run")
			waited = true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(roomPoll):
		}
	}
	return ctx.Err() == nil
}

// changedFiles reads a change's files from the SCM, remembering the last change it read: a
// worker runs a plan's jobs in turn, each over the same change.
type changedFiles struct {
	browse scmpb.BrowseClient
	key    string
	files  []workflow.ChangedFile
}

func (c *changedFiles) read(ctx context.Context, repo int64, change workflow.Change) ([]workflow.ChangedFile, error) {
	key := fmt.Sprintf("%d %s %s", repo, change.Base, change.Commit)
	if key == c.key {
		return c.files, nil
	}
	base, err := hex.DecodeString(change.Base)
	if err != nil {
		return nil, fmt.Errorf("the change's base %q: %v", change.Base, err)
	}
	commit, err := hex.DecodeString(change.Commit)
	if err != nil {
		return nil, fmt.Errorf("the change's commit %q: %v", change.Commit, err)
	}
	files, err := changes.Files(ctx, c.browse, repo, base, commit)
	if err != nil {
		return nil, err
	}
	c.key, c.files = key, files
	return files, nil
}

// stop is why a run's execution stopped early, the first reason to arrive.
type stop struct {
	mu     sync.Mutex
	kind   automationpb.HeartbeatResponse_Stop
	reason string
	cancel context.CancelFunc
}

// stopRelease is the worker shutting down: its run goes back to the queue.
const stopRelease automationpb.HeartbeatResponse_Stop = -1

func (s *stop) set(kind automationpb.HeartbeatResponse_Stop, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kind == automationpb.HeartbeatResponse_STOP_UNSPECIFIED {
		s.kind, s.reason = kind, reason
		s.cancel()
	}
}

func (s *stop) get() (automationpb.HeartbeatResponse_Stop, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kind, s.reason
}

// execute runs one leased run, with its change (none for a manual run), and records its
// outcome. It renews the run's lease until the outcome is recorded.
func execute(ctx context.Context, cfg Config, files *changedFiles, run *automationpb.Run, change []byte, every time.Duration) {
	log := cfg.Log.With("run", run.GetId())
	log.Info("running", "job", run.GetJob().GetKind().String(), "targets", run.GetJob().GetTargets(), "attempt", run.GetAttempt())

	jobCtx, cancelJob := context.WithCancel(context.Background())
	defer cancelJob()
	st := &stop{cancel: cancelJob}
	stream := newLogStream(cfg.Work, run.GetId(), cfg.Name)
	sendCtx, stopSending := context.WithCancel(context.Background())
	sent := make(chan struct{})
	go func() { stream.sendLoop(sendCtx); close(sent) }()

	// Heartbeats, until the outcome is recorded, and the worker's own shutdown.
	done := make(chan struct{})
	var beats sync.WaitGroup
	beats.Add(1)
	defer func() {
		close(done)
		beats.Wait()
	}()
	go func() {
		defer beats.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				st.set(stopRelease, "worker "+cfg.Name+" is shutting down")
				return
			case <-t.C:
				// A heartbeat that does not return in time is as good as missed: the next
				// one is due.
				beatCtx, cancel := context.WithTimeout(context.Background(), every)
				resp, err := cfg.Work.Heartbeat(beatCtx, &automationpb.HeartbeatRequest{RunId: run.GetId(), Worker: cfg.Name})
				cancel()
				if err != nil {
					log.Warn("heartbeat", "err", err)
					continue
				}
				cfg.Contact.heard(time.Now())
				if resp.GetStop() != automationpb.HeartbeatResponse_STOP_UNSPECIFIED {
					st.set(resp.GetStop(), resp.GetReason())
				}
			}
		}
	}()

	result := perform(jobCtx, cfg, files, run, change, stream)
	// The slot's Bazel server, which the run's code ran in, goes with the run: the next starts one
	// afresh, keeping nothing of this run but what is on disk.
	stopBazel(cfg)

	kind, reason := st.get()
	if !result.stopped && kind != automationpb.HeartbeatResponse_STOP_LEASE_LOST {
		// It finished on its own before the stop reached it: its outcome stands.
		kind = automationpb.HeartbeatResponse_STOP_UNSPECIFIED
	}
	switch kind {
	case automationpb.HeartbeatResponse_STOP_CANCELED:
		result = outcome{status: automationpb.Run_STATUS_CANCELED, reason: reason}
	case automationpb.HeartbeatResponse_STOP_TIMED_OUT:
		result = outcome{status: automationpb.Run_STATUS_FAILED, reason: reason, exit: result.exit}
	}
	stopSending()
	<-sent

	finishCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	switch kind {
	case automationpb.HeartbeatResponse_STOP_LEASE_LOST:
		log.Warn("abandoned: the run's lease was lost", "reason", reason)
		return
	case stopRelease:
		err := retry(finishCtx, func() error {
			_, err := cfg.Work.Release(finishCtx, &automationpb.ReleaseRequest{RunId: run.GetId(), Worker: cfg.Name, Reason: reason})
			return err
		})
		log.Info("released", "err", err)
		return
	}
	stream.Printf("%s", result.summary())
	if err := stream.flush(finishCtx); err != nil {
		if errors.Is(err, errLeaseLost) {
			log.Warn("abandoned: the run's lease was lost while sending its log")
			return
		}
		log.Warn("the run's log is incomplete", "err", err)
	}
	err := retry(finishCtx, func() error {
		_, err := cfg.Work.Finish(finishCtx, &automationpb.FinishRequest{RunId: run.GetId(), Worker: cfg.Name,
			Status: result.status, ExitCode: result.exit, Reason: result.reason, Plan: result.plan, Pushed: pushedList(result.pushed)})
		return err
	})
	log.Info("finished", "status", result.status.String(), "reason", result.reason, "err", err)
}

// retry calls f until it succeeds, the error is final (the run is not the worker's, or the
// request is refused — too large, among others), or ctx ends.
func retry(ctx context.Context, f func() error) error {
	for {
		err := f()
		switch status.Code(err) {
		case codes.OK, codes.FailedPrecondition, codes.InvalidArgument, codes.PermissionDenied, codes.NotFound,
			codes.ResourceExhausted:
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), err)
		case <-time.After(2 * time.Second):
		}
	}
}

// outcome is how a run ended.
type outcome struct {
	status automationpb.Run_Status
	exit   int32
	reason string
	// stopped reports that a stop interrupted it, rather than its ending on its own.
	stopped bool
	plan    *automationpb.Plan        // a planning run's, when it passed
	pushed  *automationpb.PushedImage // a push run's, when it passed
}

func (o outcome) summary() string {
	switch o.status {
	case automationpb.Run_STATUS_PASSED:
		return "the run passed"
	case automationpb.Run_STATUS_CANCELED:
		return "the run was canceled: " + o.reason
	case automationpb.Run_STATUS_FAILED:
		return "the run failed: " + o.reason
	}
	return "the run could not be run: " + o.reason
}

// formatBatch bounds how many files one formatter check names: a whole repository's are checked
// in turn, each command well under the system's limit on arguments.
const formatBatch = 1000

// jobInput is what a job's commands are made from besides the job: where it runs, the
// repository's settings at its commit, and, for a lint, the rules that read its files.
type jobInput struct {
	job      *automationpb.Job
	jc       *workflow.Context
	settings *workflow.Settings
	driver   *bazel.Driver
	dir      string
	// tree are the commit's files, for a lint of the whole repository.
	tree []string
	// owners are the rules that read the files a lint checks.
	owners []string
	// sandbox is the command prefix the repository's own programs run within.
	sandbox []string
	commit  []byte
}

// commands are the Bazel commands a job is, in order (docs/automation.md, "The jobs, as Bazel
// commands"): every one runs, and the job fails if any does. With a job context, a test's and a
// run's arguments expand from it, and a lint checks the files the change touches that exist.
func commands(in jobInput) ([][]string, error) {
	job, d, dir := in.job, in.driver, in.dir
	targets := job.GetTargets()
	args := job.GetArgs()
	if in.jc != nil {
		expanded, err := workflow.Expand(args, in.jc)
		if err != nil {
			return nil, err
		}
		args = expanded
	}
	var out [][]string
	switch job.GetKind() {
	case automationpb.Job_KIND_BUILD:
		if len(targets) > 0 {
			out = append(out, d.Build(dir, targets))
		}
	case automationpb.Job_KIND_TEST:
		for _, t := range targets {
			out = append(out, d.Test(dir, t, args))
		}
	case automationpb.Job_KIND_RUN:
		if len(targets) != 1 {
			return nil, fmt.Errorf("a run job names one target, not %d", len(targets))
		}
		out = append(out, d.Run(dir, targets[0], args, in.sandbox))
	case automationpb.Job_KIND_PUSH:
		if len(targets) != 1 {
			return nil, fmt.Errorf("a push names one oci_push, not %d targets", len(targets))
		}
		out = append(out, d.Push(dir, targets[0], hex.EncodeToString(in.commit)))
	case automationpb.Job_KIND_LINT:
		lint := in.settings.Bazel.Lint
		if len(lint.Aspects) == 0 && lint.Format == "" {
			return nil, fmt.Errorf("the repository names no linters (%s: bazel.lint)", workflow.SettingsFile)
		}
		files := in.tree
		var lintTargets []string
		if wholeLint(job) {
			lintTargets = []string{"//..."}
		} else {
			if in.jc != nil {
				files = in.jc.Existing()
			}
			lintTargets = slices.Compact(slices.Sorted(slices.Values(append(append([]string(nil), targets...), in.owners...))))
		}
		if len(lint.Aspects) > 0 && len(lintTargets) > 0 {
			out = append(out, d.Lint(dir, lint.Aspects, lintTargets))
		}
		if lint.Format != "" {
			for batch := range slices.Chunk(files, formatBatch) {
				out = append(out, d.FormatCheck(dir, lint.Format, batch, in.sandbox))
			}
		}
		if len(out) == 0 {
			return nil, nil // nothing the linters check: it passes
		}
		return out, nil
	case automationpb.Job_KIND_PLAN:
		return nil, nil // planned, not run: see planChange
	default:
		return nil, fmt.Errorf("unknown job kind %s", job.GetKind())
	}
	if len(out) == 0 && job.GetKind() != automationpb.Job_KIND_PLAN {
		return nil, errors.New("the job has nothing to run")
	}
	return out, nil
}

// How the sandbox gets its /proc. ProcFresh is a procfs of its own PID namespace, showing only its
// own processes. A container runtime masks parts of a pod's /proc with mounts over them
// (/proc/kcore, /proc/keys, …), and the kernel then refuses a fresh procfs to a user namespace;
// ProcBound binds the pod's /proc instead, masks and all — no more than the pod shows, and no
// process outside the sandbox's PID namespace is one it can signal, nor one of another user
// namespace one it can trace.
var (
	ProcFresh = []string{"--proc", "/proc"}
	ProcBound = []string{"--ro-bind", "/proc", "/proc"}
)

// sandbox is the prefix a run's own programs run within (bubblewrap): every file visible and
// none writable but tmp, its own, no network, no other process, a session of its own, and gone
// when the worker is.
func sandbox(bwrap string, proc []string, tmp, contextFile string) []string {
	if proc == nil {
		proc = ProcFresh
	}
	out := []string{bwrap, "--ro-bind", "/", "/", "--dev", "/dev"}
	out = append(out, proc...)
	out = append(out, "--bind", tmp, tmp,
		"--setenv", "TMPDIR", tmp, "--unshare-net", "--unshare-ipc", "--unshare-pid", "--new-session", "--die-with-parent")
	if contextFile != "" {
		out = append(out, "--setenv", workflow.ContextEnv, contextFile)
	}
	return out
}

// HideSecrets is the prefix every Bazel command a worker runs runs under (bubblewrap): the worker's
// own view of the file system and its network, but each of hidden covered — a directory with an
// empty one, a file with an empty file, and a link with an empty directory over the directory
// that holds it (a Kubernetes projected volume's files are links to the volume's own, renewed
// in place) — so nothing a change's build, tests, or repository rules run can read it; then each
// of kept, beneath a hidden directory, bound back. A path that does not exist is passed over.
func HideSecrets(bwrap string, hidden, kept []string) []string {
	out := []string{bwrap, "--dev-bind", "/", "/"}
	for _, p := range hidden {
		info, err := os.Lstat(p)
		switch {
		case err != nil:
		case info.Mode()&os.ModeSymlink != 0:
			out = append(out, "--tmpfs", filepath.Dir(p))
		case info.IsDir():
			out = append(out, "--tmpfs", p)
		default:
			out = append(out, "--ro-bind", "/dev/null", p)
		}
	}
	for _, p := range kept {
		if _, err := os.Stat(p); err == nil {
			out = append(out, "--dev-bind", p, p)
		}
	}
	return append(out, "--die-with-parent", "--")
}

// hide is the prefix a slot's Bazel commands run under (HideSecrets): the worker's secrets, the
// other slots, and every run's scratch directory but tmp, this run's, out of sight. None without
// secrets to hide.
func (cfg Config) hide(tmp string) []string {
	if len(cfg.Secrets) == 0 {
		return nil
	}
	hidden := append(append(append([]string(nil), cfg.Secrets...), cfg.others...), cfg.Scratch)
	var kept []string
	if tmp != "" {
		kept = []string{tmp}
	}
	return HideSecrets(cfg.Bwrap, hidden, kept)
}

// stopBazel stops the slot's Bazel server after a run, if one runs.
func stopBazel(cfg Config) {
	if _, err := os.Stat(filepath.Join(cfg.outputBase, "server", "server.pid.txt")); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	d := &bazel.Driver{Bazel: cfg.Bazel, Wrapper: cfg.hide(""), OutputBase: cfg.outputBase, Env: cfg.Env}
	if err := d.Shutdown(ctx, cfg.checkout.Dir()); err != nil {
		slog.Warn("stopping the slot's Bazel server", "output_base", cfg.outputBase, "err", err)
	}
}

// CheckSandbox runs nothing in the sandbox runs' programs run in, and under the prefix that hides
// secrets from Bazel (HideSecrets), once each, and returns how it gives the sandbox its /proc
// (Config.Proc): a fresh one where it can, else the pod's. A worker whose node makes neither fails
// as it starts, saying why, instead of failing every run job and lint.
func CheckSandbox(bwrap string, secrets []string) ([]string, error) {
	tmp, err := os.MkdirTemp("", "ok-sandbox-check-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	if len(secrets) > 0 {
		args := append(HideSecrets(bwrap, secrets, nil), "true")
		if out, err := exec.Command(args[0], args[1:]...).CombinedOutput(); err != nil {
			return nil, fmt.Errorf("bubblewrap cannot hide the worker's secrets from Bazel (%v: %s); %s", err, strings.TrimSpace(string(out)), checkout.UserNamespaces())
		}
	}
	var failed []string
	for _, proc := range [][]string{ProcFresh, ProcBound} {
		args := append(sandbox(bwrap, proc, tmp, ""), "true")
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		if err == nil {
			return proc, nil
		}
		failed = append(failed, fmt.Sprintf("%v: %s", err, strings.TrimSpace(string(out))))
	}
	return nil, fmt.Errorf("bubblewrap cannot make the sandbox run jobs run in (%s); %s", strings.Join(failed, "; with the pod's /proc: "), checkout.UserNamespaces())
}

// wholeLint reports whether a job lints the whole repository: the lint a change that makes an
// imported repository a Bazel workspace plans (docs/automation.md, "Imported repositories").
func wholeLint(job *automationpb.Job) bool {
	return job.GetKind() == automationpb.Job_KIND_LINT && slices.Contains(job.GetTargets(), "//...")
}

// perform reads the run's change's files, checks out the run's commit, writes its job context
// there, and runs its Bazel commands, every one of them — or, for a planning run, plans the
// patchset.
func perform(ctx context.Context, cfg Config, files *changedFiles, run *automationpb.Run, change []byte, log *logStream) outcome {
	log.Printf("run %d, attempt %d, on %s", run.GetId(), run.GetAttempt(), cfg.Name)
	if run.GetWhy() != "" {
		log.Printf("why: %s", run.GetWhy())
	}
	var jc *workflow.Context
	if len(change) > 0 {
		jc = &workflow.Context{}
		if err := json.Unmarshal(change, &jc.Change); err != nil {
			return outcome{status: automationpb.Run_STATUS_ERROR, reason: "the job context: " + err.Error()}
		}
		read, err := files.read(ctx, run.GetRepositoryId(), jc.Change)
		switch {
		case ctx.Err() != nil:
			return outcome{status: automationpb.Run_STATUS_CANCELED, reason: "stopped while reading the change's files", stopped: true}
		case err != nil:
			return outcome{status: automationpb.Run_STATUS_ERROR, reason: "reading the change's files: " + err.Error()}
		}
		jc.Files = read
		log.Printf("the change touches %d files", len(read))
	}
	contextFile := ""
	if jc != nil {
		contextFile = filepath.Join(cfg.checkout.Dir(), workflow.ContextFile)
	}
	if run.GetJob().GetKind() == automationpb.Job_KIND_PLAN && jc == nil {
		return outcome{status: automationpb.Run_STATUS_ERROR, reason: "a planning run without its job context"}
	}
	var tree []string
	if wholeLint(run.GetJob()) {
		entries, err := cfg.Source.Entries(ctx, run.GetRepositoryId(), run.GetCommitId())
		switch {
		case ctx.Err() != nil:
			return outcome{status: automationpb.Run_STATUS_CANCELED, reason: "stopped while listing the commit's files", stopped: true}
		case err != nil:
			return outcome{status: automationpb.Run_STATUS_ERROR, reason: "listing the commit's files: " + err.Error()}
		}
		for _, e := range entries {
			if e.Kind == checkout.File {
				tree = append(tree, e.Path)
			}
		}
		log.Printf("the lint is the whole repository's: its %d files", len(tree))
	}
	tmp := filepath.Join(cfg.Scratch, "run-"+strconv.FormatInt(run.GetId(), 10))
	// The job context is shown beside the commit's files until the next Sync.
	var shown map[string][]byte
	if contextFile != "" {
		data, err := json.MarshalIndent(jc, "", "  ")
		if err != nil {
			return outcome{status: automationpb.Run_STATUS_ERROR, reason: "the job context: " + err.Error()}
		}
		shown = map[string][]byte{workflow.ContextFile: append(data, '\n')}
	}
	start := time.Now()
	log.Printf("checking out %x", run.GetCommitId())
	stats, err := cfg.checkout.Sync(ctx, run.GetRepositoryId(), run.GetCommitId(), shown)
	var ce *checkout.CommitError
	switch {
	case ctx.Err() != nil:
		return outcome{status: automationpb.Run_STATUS_CANCELED, reason: "stopped while checking out", stopped: true}
	case errors.As(err, &ce):
		return outcome{status: automationpb.Run_STATUS_FAILED, reason: "the commit has no checkout: " + ce.Message}
	case err != nil:
		return outcome{status: automationpb.Run_STATUS_ERROR, reason: "checking out: " + err.Error()}
	}
	log.Printf("checked out in %s: %s", time.Since(start).Round(time.Millisecond), stats)

	if err := os.RemoveAll(tmp); err != nil {
		return outcome{status: automationpb.Run_STATUS_ERROR, reason: err.Error()}
	}
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return outcome{status: automationpb.Run_STATUS_ERROR, reason: err.Error()}
	}
	defer os.RemoveAll(tmp)
	env := append(append([]string(nil), cfg.Env...), "TMPDIR="+tmp)
	if contextFile != "" {
		env = append(env, workflow.ContextEnv+"="+contextFile)
	}
	settings, err := readSettings(cfg.checkout.Dir())
	if err != nil {
		return outcome{status: automationpb.Run_STATUS_FAILED, reason: err.Error()}
	}
	if run.GetJob().GetKind() == automationpb.Job_KIND_PLAN {
		return planChange(ctx, cfg, run, jc, settings, env, tmp, log)
	}
	if settings.Build == workflow.BuildNone {
		return outcome{status: automationpb.Run_STATUS_FAILED, reason: "the repository is imported (" + workflow.SettingsFile + "): ok runs no Bazel in it"}
	}
	d := cfg.driver(settings, env, tmp, log)
	in := jobInput{job: run.GetJob(), jc: jc, settings: settings, driver: d, dir: cfg.checkout.Dir(), tree: tree,
		sandbox: sandbox(cfg.Bwrap, cfg.Proc, tmp, contextFile), commit: run.GetCommitId()}
	if run.GetJob().GetKind() == automationpb.Job_KIND_LINT && !wholeLint(run.GetJob()) && jc != nil {
		g, err := graphAt(ctx, cfg, d, run.GetRepositoryId(), run.GetCommitId(), cfg.checkout, log)
		if err != nil || ctx.Err() != nil {
			return stopped(ctx, "the build graph, for the rules that read the change's files: "+fmt.Sprint(err))
		}
		in.owners = g.Owners(jc.Existing())
		log.Printf("%d rules read the change's files", len(in.owners))
	}
	cmds, err := commands(in)
	if err != nil {
		return outcome{status: automationpb.Run_STATUS_ERROR, reason: err.Error()}
	}

	var failed []string
	var firstExit int32
	for _, argv := range cmds {
		if ctx.Err() != nil {
			break
		}
		cmdline := strings.Join(append([]string{"bazel"}, argv[1:]...), " ")
		log.Printf("$ %s", cmdline)
		code, err := runCommand(ctx, cfg, append(cfg.hide(tmp), argv...), cfg.checkout.Dir(), env, log)
		switch {
		case err != nil && ctx.Err() == nil:
			return outcome{status: automationpb.Run_STATUS_ERROR, reason: fmt.Sprintf("running %s: %v", cmdline, err)}
		case code != 0 && ctx.Err() == nil:
			failed = append(failed, fmt.Sprintf("%s exited with status %d", cmdline, code))
			if firstExit == 0 {
				firstExit = int32(code)
			}
		}
	}
	if ctx.Err() != nil {
		return outcome{status: automationpb.Run_STATUS_CANCELED, reason: "stopped", stopped: true}
	}
	if len(failed) > 0 {
		return outcome{status: automationpb.Run_STATUS_FAILED, exit: firstExit, reason: strings.Join(failed, "; ")}
	}
	if run.GetJob().GetKind() == automationpb.Job_KIND_PUSH {
		pushed, err := pushedImage(ctx, cfg, d, run)
		if err != nil || ctx.Err() != nil {
			return stopped(ctx, "reading what was pushed: "+fmt.Sprint(err))
		}
		log.Printf("pushed %s/%s@%s", pushed.GetRegistry(), pushed.GetRepository(), pushed.GetDigest())
		return outcome{status: automationpb.Run_STATUS_PASSED, pushed: pushed}
	}
	return outcome{status: automationpb.Run_STATUS_PASSED}
}

// readSettings reads the repository's settings at the checkout's commit.
func readSettings(dir string) (*workflow.Settings, error) {
	data, err := os.ReadFile(filepath.Join(dir, workflow.SettingsFile))
	if errors.Is(err, fs.ErrNotExist) {
		return workflow.ParseSettings(nil, false)
	}
	if err != nil {
		return nil, err
	}
	return workflow.ParseSettings(data, true)
}

// driver is Bazel as a slot runs it, for a repository's settings: the slot's own output base,
// outside the checkout, the installation's flags and then the repository's.
func (cfg Config) driver(settings *workflow.Settings, env []string, tmp string, log *logStream) *bazel.Driver {
	return &bazel.Driver{
		Bazel:      cfg.Bazel,
		Wrapper:    cfg.hide(tmp),
		OutputBase: cfg.outputBase,
		Flags:      append(append([]string(nil), cfg.BazelFlags...), settings.Bazel.Flags...),
		Env:        env,
		Stderr:     log,
	}
}

// pushedImage is what a push run pushed: the oci_push target's repository, and the digest of the
// image it names, read from that image's OCI layout as the build made it.
func pushedImage(ctx context.Context, cfg Config, d *bazel.Driver, run *automationpb.Run) (*automationpb.PushedImage, error) {
	target := run.GetJob().GetTargets()[0]
	g, err := graphAt(ctx, cfg, d, run.GetRepositoryId(), run.GetCommitId(), cfg.checkout, nil)
	if err != nil {
		return nil, err
	}
	image, ok := g.Attr(target, "image")
	if !ok {
		return nil, fmt.Errorf("%s names no image", target)
	}
	repository, ok := g.Attr(target, "repository")
	if !ok {
		return nil, fmt.Errorf("%s names no repository (rules_oci's repository attribute)", target)
	}
	outs, err := d.OutputFiles(ctx, cfg.checkout.Dir(), image)
	if err != nil {
		return nil, err
	}
	if len(outs) != 1 {
		return nil, fmt.Errorf("%s builds %d outputs, not its OCI layout", image, len(outs))
	}
	index, err := os.ReadFile(filepath.Join(outs[0], "index.json"))
	if err != nil {
		return nil, fmt.Errorf("%s's OCI layout: %w", image, err)
	}
	var layout struct {
		Manifests []struct {
			Digest string `json:"digest"`
		} `json:"manifests"`
	}
	if err := json.Unmarshal(index, &layout); err != nil || len(layout.Manifests) == 0 || !strings.HasPrefix(layout.Manifests[0].Digest, "sha256:") {
		return nil, fmt.Errorf("%s's OCI layout names no image (%v)", image, err)
	}
	registry, repo, _ := strings.Cut(repository, "/")
	return &automationpb.PushedImage{Target: target, Registry: registry, Repository: repo, Digest: layout.Manifests[0].Digest,
		Tags: []string{hex.EncodeToString(run.GetCommitId())}}, nil
}

// pushedList is a push run's image as Finish carries it: none, or the one it pushed.
func pushedList(p *automationpb.PushedImage) []*automationpb.PushedImage {
	if p == nil {
		return nil
	}
	return []*automationpb.PushedImage{p}
}

// runCommand runs one command in dir, its output into the log, in a process group of its own:
// stopping the run stops it and everything it started, and one that does not stop within the
// grace period is killed. It returns the command's exit status.
func runCommand(ctx context.Context, cfg Config, argv []string, dir string, env []string, log *logStream) (int, error) {
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = proc.DieWithParent(&syscall.SysProcAttr{Setpgid: true}, syscall.SIGKILL)
	cmd.WaitDelay = 10 * time.Second
	return wait(ctx, cfg, cmd)
}
