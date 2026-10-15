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
	"strconv"
	"syscall"
	"time"

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
	opts := []serve.Option{serve.WithAuthn(r.Resolver), serve.WithListenAddr(cfg.Listen), serve.WithMetricsAddr(cfg.MetricsListen)}
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
var workerEnv = []string{"PATH", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy", "SSL_CERT_FILE", "DOCKER_CONFIG"}

// workerRun runs an ok automation worker until it is told to stop: it takes runs from ok
// automation, checks each run's commit out — through okfs, or by writing it — runs Bazel on it,
// and records the outcome and the log. A virtual checkout's worker first runs itself again in a
// user and mount namespace of its own, where its okfs daemon mounts.
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
		if err := checkout.Isolate(); err != nil {
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
	proc, err := worker.CheckSandbox(bwrap, secrets)
	if err != nil {
		return err
	}
	if proc[0] != worker.ProcFresh[0] {
		slog.Info("ok worker: the sandbox binds the pod's /proc: its runtime's masks refuse a fresh one")
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
	browse := scmpb.NewBrowseClient(scm)
	src := checkout.SCM(browse, scmpb.NewStoreClient(scm))

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
		for i := 1; i <= w.Slots; i++ {
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
		for i := 1; i <= w.Slots; i++ {
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
		Work:           automationpb.NewWorkClient(automation),
		Checkouts:      checkouts,
		Bases:          bases,
		Source:         src,
		Browse:         browse,
		Bazel:          bazel,
		BazelFlags:     w.BazelFlags,
		Bwrap:          bwrap,
		Proc:           proc,
		Secrets:        secrets,
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
	slog.Info("ok worker: taking runs", "name", name, "slots", w.Slots, "checkout", w.Checkout, "bazel", bazel)
	return serve.Run(ctx, runs, serve.WithListenAddr(cfg.Listen), serve.WithMetricsAddr(cfg.MetricsListen),
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
