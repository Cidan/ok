package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Cidan/ok/internal/automation/microvm"
	"github.com/Cidan/ok/internal/automation/role"
	"github.com/Cidan/ok/internal/automation/worker"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/client/auth"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/serve/node"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
)

// automationServe runs ok automation until it is told to stop.
func automationServe(args []string) error {
	flags := flag.NewFlagSet("automation serve", flag.ContinueOnError)
	path := flags.String("config", "ok.yaml", "the installation's configuration")
	dev := flags.Bool("dev", false, "accept development principals (ok-dev-principal), on a loopback address only")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if *dev && !loopback(cfg.Listen) {
		return fmt.Errorf("-dev serves only on loopback, and listen is %s", cfg.Listen)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r, err := role.Build(ctx, cfg, role.Options{Dev: *dev})
	if err != nil {
		return err
	}
	defer r.Close()
	opts := []serve.Option{serve.WithAuthn(r.Resolver), serve.WithListenAddr(cfg.Listen), serve.WithMetricsAddr(cfg.MetricsListen), serve.WithProfiling(cfg.Profiling)}
	if cfg.TLS != nil {
		tc, err := serve.LoadTLS(cfg.TLS.Cert, cfg.TLS.Key)
		if err != nil {
			return err
		}
		opts = append(opts, serve.WithTLS(tc))
	}
	return serve.Run(ctx, r.Root, opts...)
}

// workerStopTimeout is how long a worker has to stop once told to: a run's grace to stop its
// commands, then giving the run back. Its pod's termination grace period is a minute.
const workerStopTimeout = 50 * time.Second

// cgroup is the container's own cgroup (v2), whose memory limit bounds the runs.
const cgroup = "/sys/fs/cgroup"

// workerEnv are the variables of the worker's environment Bazel gets: where it finds its tools,
// reaches the network for what a module fetches, and finds the registry credentials a push uses
// (DOCKER_CONFIG, a directory holding Docker's config.json).
var workerEnv = []string{"PATH", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy", "SSL_CERT_FILE", "DOCKER_CONFIG",
	"BAZELISK_HOME", "USE_BAZEL_VERSION"}

// workerRun runs an ok automation worker until it is told to stop: it takes runs from ok
// automation, checks each run's commit out — through okfs, or by writing it — runs Bazel on it,
// and records the outcome and the log. A virtual checkout's worker runs only where it was started
// in a user and mount namespace of its own (checkout.Launch), where its okfs daemon mounts.
func workerRun(args []string) error {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	path := flags.String("config", "ok.yaml", "the installation's configuration")
	dev := flags.String("dev-principal", "", "the principal to name to loopback development services (tests)")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	w := cfg.Worker
	if w.Checkout == "virtual" {
		if err := checkout.RequireIsolated(); err != nil {
			return err
		}
	}
	if w.State == "" {
		return errors.New("worker.state is the directory the worker keeps its checkouts and caches in")
	}
	if cfg.Services.Automation == "" || cfg.Services.SCM == "" || cfg.Services.Identity == "" {
		return errors.New("services.automation, services.scm, and services.identity are where the worker takes runs, reads commits, and resolves ACLs")
	}
	name := w.Name
	if name == "" {
		if name, err = os.Hostname(); err != nil {
			return err
		}
	}
	bazel, err := lookPath(w.Bazel, "bazelisk", "bazel")
	if err != nil {
		return fmt.Errorf("Bazel: %w", err)
	}
	bwrap, err := lookPath(w.Bwrap, "bwrap")
	if err != nil {
		return fmt.Errorf("bubblewrap: %w", err)
	}
	// What a change's build and tests run never sees: the secrets the configuration names, the
	// configuration itself, and the okfs daemon's state, its credential among it.
	configPath, err := filepath.Abs(*path)
	if err != nil {
		return err
	}
	secrets := append(cfg.SecretPaths(), configPath, filepath.Join(w.State, "okfs"))
	if err := os.MkdirAll(filepath.Join(w.State, "okfs"), 0o700); err != nil {
		return err
	}
	// In microVMs, what a run runs sees none of the worker's: no sandbox to check here.
	proc := worker.ProcFresh
	if w.MicroVM == nil {
		if proc, err = worker.CheckSandbox(bwrap, secrets); err != nil {
			return err
		}
		if proc[0] != worker.ProcFresh[0] {
			slog.Info("ok worker: the sandbox binds the pod's /proc: its runtime's masks refuse a fresh one")
		}
	}

	o, err := idclient.Options(cfg.Services)
	if err != nil {
		return err
	}
	var cred dial.Credential
	if *dev != "" {
		o.DevPrincipal = *dev
	} else if cred, err = idclient.Credential(cfg, o); err != nil {
		return err
	} else {
		o.Credential = cred
	}
	automation, err := dial.Dial(cfg.Services.Automation, o)
	if err != nil {
		return err
	}
	defer automation.Close()
	scm, err := dial.Dial(cfg.Services.SCM, o)
	if err != nil {
		return err
	}
	defer scm.Close()
	id, err := dial.Dial(cfg.Services.Identity, o)
	if err != nil {
		return err
	}
	defer id.Close()
	// ok secrets, which gives post runs the secrets their jobs name, where the installation runs it.
	var runSecrets secretspb.RunSecretsClient
	if cfg.Services.Secrets != "" {
		sec, err := dial.Dial(cfg.Services.Secrets, o)
		if err != nil {
			return err
		}
		defer sec.Close()
		runSecrets = secretspb.NewRunSecretsClient(sec)
	}
	browse := scmpb.NewBrowseClient(scm)
	src := checkout.SCM(scmpb.NewStoreClient(scm))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slotDir := func(i int, base bool) string {
		n := strconv.Itoa(i)
		if base {
			n += "-base"
		}
		return filepath.Join(w.State, "slots", n)
	}
	var checkouts, bases []checkout.Checkout
	// newCheckout makes a checkout at a directory: a microVM's trust domain's.
	var newCheckout func(dir string) (checkout.Checkout, error)
	switch w.Checkout {
	case "virtual":
		jj, err := lookPath(w.JJ, "jj")
		if err != nil {
			return fmt.Errorf("ok's jj, the okfs daemon: %w", err)
		}
		vc := checkout.VirtualConfig{Dir: slotDir(1, false), State: filepath.Join(w.State, "okfs"), JJ: jj, SCM: cfg.Services.SCM,
			TLS: cfg.Services.TLS, CAFile: cfg.Services.CAFile, DevPrincipal: *dev, Log: os.Stderr}
		if cred != nil {
			vc.ConfigDir = filepath.Join(w.State, "okfs", "config")
			srv := auth.Server{Addr: cfg.Services.SCM, TLS: cfg.Services.TLS}
			if err := keepCredential(ctx, cred, filepath.Join(vc.ConfigDir, "credential"), srv); err != nil {
				return fmt.Errorf("the okfs daemon's credential: %w", err)
			}
		}
		v, err := checkout.NewVirtual(vc)
		if err != nil {
			return err
		}
		defer v.Close()
		newCheckout = v.At
		for i := 1; i <= w.Slots && w.MicroVM == nil; i++ {
			for _, base := range []bool{false, true} {
				var co checkout.Checkout = v
				if i > 1 || base {
					if co, err = v.At(slotDir(i, base)); err != nil {
						return err
					}
				}
				if base {
					bases = append(bases, co)
				} else {
					checkouts = append(checkouts, co)
				}
			}
		}
	case "real":
		slog.Warn("worker.checkout is real: every file of each run's commit is written to disk; present it through okfs (virtual) unless a legacy use needs its whole source on disk")
		newCheckout = func(dir string) (checkout.Checkout, error) { return checkout.NewReal(dir, src) }
		for i := 1; i <= w.Slots && w.MicroVM == nil; i++ {
			co, err := checkout.NewReal(slotDir(i, false), src)
			if err != nil {
				return err
			}
			base, err := checkout.NewReal(slotDir(i, true), src)
			if err != nil {
				return err
			}
			checkouts, bases = append(checkouts, co), append(bases, base)
		}
	}

	home := filepath.Join(w.State, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		return err
	}
	// Every slot's Bazel shares its downloads and caches, under the worker's own home.
	env := []string{"LANG=C.UTF-8", "TZ=UTC", "HOME=" + home}
	for _, k := range workerEnv {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	runs := &runsNode{}
	runs.cfg = worker.Config{
		Name:           name,
		Pool:           cfg.Worker.Pool,
		Work:           automationpb.NewWorkClient(automation),
		Automation:     automation,
		Checkouts:      checkouts,
		Bases:          bases,
		Source:         src,
		Browse:         browse,
		Bazel:          bazel,
		BazelFlags:     w.BazelFlags,
		Bwrap:          bwrap,
		Proc:           proc,
		Secrets:        secrets,
		RunSecrets:     runSecrets,
		Names:          cfg.Names(),
		Directory:      identitypb.NewDirectoryClient(id),
		TreeRepository: cfg.Identity.TreeRepository,
		Env:            env,
		Scratch:        filepath.Join(w.State, "scratch"),
		OutputBases:    filepath.Join(w.State, "bazel"),
		Graphs:         filepath.Join(w.State, "graphs"),
		Grace:          30 * time.Second,
		Log:            slog.Default(),
		Contact:        &runs.contact,
		Room:           worker.MemoryRoom(cgroup, int64(w.RunMemoryGiB*(1<<30))),
	}
	if w.MicroVM != nil {
		if runs.cfg.MicroVM, err = microVMs(w, bazel, bwrap, newCheckout); err != nil {
			return err
		}
	}
	slog.Info("ok worker: taking runs", "name", name, "slots", w.Slots, "checkout", w.Checkout, "bazel", bazel)
	return serve.Run(ctx, runs, serve.WithListenAddr(cfg.Listen), serve.WithMetricsAddr(cfg.MetricsListen), serve.WithProfiling(cfg.Profiling),
		serve.WithStopTimeout(workerStopTimeout))
}

// runsNode takes runs and runs them. It is healthy, and so ready, while ok automation answers
// the worker: one that cannot reach automation takes no runs, though it keeps trying.
type runsNode struct {
	cfg     worker.Config
	contact worker.Contact
}

var _ node.Application = (*runsNode)(nil)

func (*runsNode) Name() string              { return "ok/worker" }
func (*runsNode) Dependencies() []node.Node { return nil }
func (*runsNode) Setup(context.Context) error {
	return nil
}
func (n *runsNode) Healthy(context.Context) bool {
	return n.contact.Within(time.Now(), worker.ReadyWithin)
}
func (n *runsNode) Start(ctx context.Context) error {
	return worker.Run(ctx, n.cfg)
}

// keepCredential writes the worker's credential where its okfs daemon reads it, as `ok auth login`
// writes one, and writes it again whenever the worker's is renewed, until ctx ends: the daemon
// takes up a newer certificate it finds there.
func keepCredential(ctx context.Context, cred dial.Credential, dir string, srv auth.Server) error {
	var written []byte
	write := func() error {
		cert, signer, err := cred.Current(ctx)
		if err != nil {
			return err
		}
		if bytes.Equal(cert, written) {
			return nil
		}
		key, ok := signer.(*ecdsa.PrivateKey)
		if !ok {
			return fmt.Errorf("the credential's key is a %T, not an ECDSA key", signer)
		}
		if err := auth.Save(dir, srv, cert, key); err != nil {
			return err
		}
		written = cert
		return nil
	}
	if err := write(); err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Minute):
			}
			if err := write(); err != nil {
				slog.Warn("worker: keeping the okfs daemon's credential", "err", err)
			}
		}
	}()
	return nil
}

// lookPath is the configured program's path, or the first of names on the PATH.
func lookPath(configured string, names ...string) (string, error) {
	if configured != "" {
		names = []string{configured}
	}
	var errs []error
	for _, n := range names {
		p, err := exec.LookPath(n)
		if err == nil {
			return p, nil
		}
		errs = append(errs, err)
	}
	return "", errors.Join(errs...)
}

// microVMs is how a worker runs each run in a microVM of its own (worker.microvm): the guests' root
// made under its state, with ok itself as their init.
func microVMs(w config.Worker, bazel, bwrap string, newCheckout func(string) (checkout.Checkout, error)) (*worker.MicroVM, error) {
	m := w.MicroVM
	hypervisor, err := lookPath(m.Hypervisor, "cloud-hypervisor")
	if err != nil {
		return nil, fmt.Errorf("Cloud Hypervisor: %w", err)
	}
	virtiofsd, err := lookPath(m.Virtiofsd, "virtiofsd")
	if err != nil {
		if _, serr := os.Stat("/usr/libexec/virtiofsd"); serr != nil {
			return nil, fmt.Errorf("virtiofsd: %w", err)
		}
		virtiofsd = "/usr/libexec/virtiofsd"
	}
	if out, err := exec.Command(virtiofsd, "--help").CombinedOutput(); err != nil || !bytes.Contains(out, []byte("--readonly")) {
		return nil, fmt.Errorf("virtiofsd %s has no --readonly, which a run's checkout is shared with: it is 1.13 or later", virtiofsd)
	}
	ok, err := os.Executable()
	if err != nil {
		return nil, err
	}
	// The guest sees Bazel and bubblewrap where the worker does.
	system := append(slices.Clone(microvm.DefaultSystem), m.Share...)
	for _, tool := range []string{bazel, bwrap} {
		dir := filepath.Dir(tool)
		if !slices.ContainsFunc(system, func(s string) bool { return dir == s || strings.HasPrefix(dir, s+"/") }) {
			system = append(system, dir)
		}
	}
	cpus := m.CPUs
	if cpus == 0 {
		cpus = max(1, runtime.NumCPU()/w.Slots)
	}
	memory := m.MemoryGiB
	if memory == 0 {
		memory = w.RunMemoryGiB
	}
	vm := microvm.Config{Hypervisor: hypervisor, Virtiofsd: virtiofsd, Kernel: m.Kernel, CPUs: cpus, MemoryMiB: int(memory * 1024),
		Root: filepath.Join(w.State, "microvm", "root"), System: system, Init: ok}
	if err := microvm.Prepare(vm, os.Getuid(), os.Getgid()); err != nil {
		return nil, fmt.Errorf("the microVMs' root: %w", err)
	}
	slog.Info("ok worker: runs in microVMs", "kernel", m.Kernel, "cpus", cpus, "memory_gib", memory, "egress", len(*m.Egress))
	return &worker.MicroVM{VM: vm, OK: ok, Slots: w.Slots, Domains: filepath.Join(w.State, "domains"), NewCheckout: newCheckout,
		Reserve: uint64(m.ReserveGiB * (1 << 30)), Most: m.Domains, Egress: *m.Egress, PushEgress: m.PushEgress}, nil
}
