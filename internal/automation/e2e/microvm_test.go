package e2e

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/automation/microvm"
	"github.com/Cidan/ok/internal/automation/worker"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/identity"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
)

var (
	okStatic    string
	okStaticErr error
	okBuilt     sync.Once
)

// staticOK is ok built static: the microVMs' init, and the wrapper every command runs under.
func staticOK(t *testing.T) string {
	t.Helper()
	okBuilt.Do(func() {
		dir, err := os.MkdirTemp("", "ok-e2e")
		if err != nil {
			okStaticErr = err
			return
		}
		okStatic = filepath.Join(dir, "ok")
		cmd := exec.Command("go", "build", "-o", okStatic, "github.com/Cidan/ok/cmd/ok")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			okStaticErr = err
			okStatic = string(out)
		}
	})
	if okStaticErr != nil {
		t.Fatalf("building ok: %v: %s", okStaticErr, okStatic)
	}
	return okStatic
}

// microVMTools are the microVMs' kernel, Cloud Hypervisor, and virtiofsd: from
// OK_TEST_MICROVM_KERNEL, _HYPERVISOR, and _VIRTIOFSD; the test is skipped without them unless
// OK_TEST_MICROVM=require.
func microVMTools(t *testing.T) microvm.Config {
	t.Helper()
	cfg := microvm.Config{Kernel: os.Getenv("OK_TEST_MICROVM_KERNEL"), Hypervisor: os.Getenv("OK_TEST_MICROVM_HYPERVISOR"),
		Virtiofsd: os.Getenv("OK_TEST_MICROVM_VIRTIOFSD"), CPUs: 2, MemoryMiB: 4096}
	if cfg.Kernel == "" || cfg.Hypervisor == "" || cfg.Virtiofsd == "" {
		if os.Getenv("OK_TEST_MICROVM") == "require" {
			t.Fatal("OK_TEST_MICROVM=require, and OK_TEST_MICROVM_KERNEL, _HYPERVISOR, or _VIRTIOFSD is unset")
		}
		t.Skip("no microVM tools (OK_TEST_MICROVM_KERNEL, _HYPERVISOR, _VIRTIOFSD)")
	}
	return cfg
}

// startMicroVMWorker runs a worker whose runs each run in a microVM of its own, in its trust
// domain's copy: real checkouts under the test's worker state. Bazel is what bazelisk keeps on the
// host, shared read-only — the version a test VM's cache holds — so no run downloads it.
func (e *env) startMicroVMWorker(name string, slots int) *worker.MicroVM {
	t := e.t
	t.Helper()
	vm := microVMTools(t)
	ok := staticOK(t)
	scm := as(t, e.scm, workerBot)
	browse := scmpb.NewBrowseClient(scm)
	src := checkout.SCM(browse, scmpb.NewStoreClient(scm))
	state := filepath.Join(e.state, name)
	vm.Root, vm.Init = filepath.Join(state, "microvm", "root"), ok
	vm.System = append([]string(nil), microvm.DefaultSystem...)
	env := []string{"PATH=" + os.Getenv("PATH"), "LANG=C.UTF-8", "TZ=UTC"}
	for _, tool := range []string{e.bazel, e.bwrap} {
		if dir := filepath.Dir(tool); !strings.HasPrefix(dir, "/usr/") {
			vm.System = append(vm.System, dir)
		}
	}
	if home, err := os.UserHomeDir(); err == nil {
		if cache := filepath.Join(home, ".cache", "bazelisk"); dirExists(cache) {
			vm.System = append(vm.System, cache)
			env = append(env, "BAZELISK_HOME="+cache)
			if v := cachedBazel(cache); v != "" {
				env = append(env, "USE_BAZEL_VERSION="+v)
			}
		}
	}
	if err := microvm.Prepare(vm, os.Getuid(), os.Getgid()); err != nil {
		t.Fatal(err)
	}
	m := &worker.MicroVM{VM: vm, OK: ok, Slots: slots, Domains: filepath.Join(state, "domains"), Most: 8, Egress: config.DefaultEgress,
		NewCheckout: func(dir string) (checkout.Checkout, error) { return checkout.NewReal(dir, src) }}
	cfg := worker.Config{
		Name:       name,
		Work:       automationpb.NewWorkClient(as(t, e.auto, workerBot)),
		Automation: as(t, e.auto, workerBot),
		Source:     src,
		Browse:     browse,
		Bazel:      e.bazel,
		Bwrap:      e.bwrap,
		Names:      identity.MustNames("example.com"),
		Directory:  identitypb.NewDirectoryClient(as(t, e.id, workerBot)),
		Env:        env,
		Grace:      10 * time.Second,
		Log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Contact:    &worker.Contact{},
		MicroVM:    m,
	}
	if e.secrets != "" {
		cfg.RunSecrets = secretspb.NewRunSecretsClient(as(t, e.secrets, workerBot))
	}
	ctx, cancel := context.WithCancel(e.ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("worker %s: %v", name, err)
		}
	})
	return m
}

func dirExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.IsDir()
}

// cachedBazel is a version of Bazel bazelisk's cache holds: bazel-<version>-<os>-<arch>.
func cachedBazel(cache string) string {
	es, err := os.ReadDir(filepath.Join(cache, "downloads", "metadata", "bazelbuild"))
	if err != nil {
		return ""
	}
	for _, e := range es {
		if rest, ok := strings.CutPrefix(e.Name(), "bazel-"); ok {
			if v, _, ok := strings.Cut(rest, "-linux"); ok {
				return v
			}
		}
	}
	return ""
}

// copiesOf are a domain's copies a microVM worker keeps, by directory.
func copiesOf(t *testing.T, m *worker.MicroVM, domain string) []string {
	t.Helper()
	es, err := os.ReadDir(filepath.Join(m.Domains, domain))
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range es {
		if e.IsDir() && e.Name() != "graphs" {
			out = append(out, e.Name())
		}
	}
	return out
}

// TestMicroVMs drives pre and post through a worker that runs each run in a microVM of its own
// (docs/hosted.md, "Hosted automation, as built"): every run's log names the trust domain it ran
// in — its repository's pre or post — the fixture's sandboxed tool passes in the guest, each
// repository's domains are its own, and what a domain keeps serves its next runs: the base's build
// graph, queried once; and the remote cache reaches its guests.
func TestMicroVMs(t *testing.T) {
	e := start(t, func(c *config.Config) { c.Automation.Cache = &config.AutomationCache{TTL: time.Hour, MemoryMiB: 64} })
	m := e.startMicroVMWorker("vm1", 2)
	a, one := e.newRepository("one")
	b, two := e.newRepository("two")
	domain := func(repo int64, side string) string { return strconv.FormatInt(repo, 10) + "-" + side }

	a.jj("one", "new", "main@ok", "-m", "hello, world")
	write(t, a.dir("one"), map[string]*string{"lib/a.txt": ptr("hello, world\n")})
	cl := uploaded(a.jj("one", "cl", "--ready"))[0]
	p := e.settled(one, cl, 1)
	if p.GetVerdict() != automationpb.Pre_VERDICT_PASSED {
		t.Fatalf("pre in microVMs: %s\nthe plan's log:\n%s", e.describe(p), e.logOf(p.GetPlan().GetId()))
	}
	runs := append([]*automationpb.Run{p.GetPlan()}, p.GetJobs()...)
	for _, r := range runs {
		if log := e.logOf(r.GetId()); !strings.Contains(log, "in a microVM of trust domain "+domain(one, "pre")) {
			t.Errorf("run %d's log does not name its domain:\n%s", r.GetId(), log)
		}
	}
	if log := e.logOf(jobs(p)["//.ok/workflow/ci.yaml:pre:sandbox"].GetId()); !strings.Contains(log, "the tool ran:") {
		t.Errorf("the sandboxed tool's log:\n%s", log)
	}
	// Its next patchset plans on the same base: the graph its domain kept.
	write(t, a.dir("one"), map[string]*string{"lib/a.txt": ptr("hello again\n")})
	a.jj("one", "cl")
	p = e.settled(one, cl, 2)
	if p.GetVerdict() != automationpb.Pre_VERDICT_PASSED {
		t.Fatalf("the second patchset's pre: %s", e.describe(p))
	}
	if log := e.logOf(p.GetPlan().GetId()); !strings.Contains(log, "kept from before") {
		t.Errorf("the second plan did not take its domain's kept graph:\n%s", log)
	}

	// Landed through the queue, its post runs in repository one's post domain.
	sub, err := e.runs.Submit(e.ctx, &automationpb.SubmitRequest{RepositoryId: one, ForPersonId: e.adaID,
		Cls: []*automationpb.SubmitRequest_CL{{Number: cl, Patchset: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	var landed *automationpb.Submission
	for deadline := time.Now().Add(10 * time.Minute); ; {
		s, err := e.runs.GetSubmission(e.ctx, &automationpb.GetSubmissionRequest{Id: sub.GetId()})
		if err != nil {
			t.Fatal(err)
		}
		if s.GetStatus() != automationpb.Submission_STATUS_ACTIVE {
			landed = s
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("submission %d did not finish: %v", sub.GetId(), s)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if landed.GetStatus() != automationpb.Submission_STATUS_LANDED {
		t.Fatalf("the submission: %v", landed)
	}
	var post []*automationpb.Run
	for _, at := range landed.GetAttempts() {
		for _, c := range at.GetCandidates() {
			post = append(post, c.GetPlan())
			post = append(post, c.GetJobs()...)
		}
	}
	if len(post) == 0 {
		t.Fatalf("the submission ran no post: %v", landed)
	}
	for _, r := range post {
		if r.GetCarriedFrom() != 0 {
			continue // carried from pre, never run
		}
		if log := e.logOf(r.GetId()); !strings.Contains(log, "in a microVM of trust domain "+domain(one, "post")) {
			t.Errorf("post run %d's log does not name repository one's post domain:\n%s", r.GetId(), log)
		}
	}

	// The same change to repository two, also ada's: its pre runs in two's domain, whose output
	// base never built it, and its guest takes from the remote cache what one's built under the
	// account — through vsock and the run's pass-through.
	b.jj("two", "new", "main@ok", "-m", "hello, world")
	write(t, b.dir("two"), map[string]*string{"lib/a.txt": ptr("hello, world\n")})
	cl2 := uploaded(b.jj("two", "cl", "--ready"))[0]
	p2 := e.settled(two, cl2, 1)
	if p2.GetVerdict() != automationpb.Pre_VERDICT_PASSED {
		t.Fatalf("repository two's pre: %s", e.describe(p2))
	}
	if log := e.logOf(jobs(p2)["build"].GetId()); !strings.Contains(log, "in a microVM of trust domain "+domain(two, "pre")) ||
		!strings.Contains(log, "remote cache hit") {
		t.Errorf("repository two's build, in its microVM, took nothing from the remote cache:\n%s", log)
	}

	// The worker keeps the domains its runs ran in, and none else: each repository's pre (both
	// uploaded the fixture, whose pre ran), and repository one's post.
	es, err := os.ReadDir(m.Domains)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range es {
		got = append(got, e.Name())
	}
	want := []string{domain(one, "pre"), domain(one, "post"), domain(two, "pre")}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("the domains kept: %v, want %v", got, want)
	}
	for _, d := range want {
		if len(copiesOf(t, m, d)) == 0 {
			t.Errorf("domain %s keeps no copy", d)
		}
	}
}
