package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/client/auth"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/deploy/role"
	"github.com/Cidan/ok/internal/deploy/roll"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/serve"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// colorEnv names the copy of the deployer a process runs in: its pod's color label, through the
// downward API. addressEnv is where its cell's other replicas reach it: its pod's address and port.
const (
	colorEnv   = "OK_DEPLOY_COLOR"
	addressEnv = "OK_DEPLOY_ADDRESS"
)

// planningReads are what planning reads of a commit, which a virtual checkout's Sync has local
// before it returns: the build graph Bazel loads, the repository's settings, and the workflow
// files.
var planningReads = []string{"MODULE.bazel", "MODULE.bazel.lock", ".bazelrc", ".bazelversion", "**/BUILD", "**/BUILD.bazel", "**/*.bzl",
	".ok/repository.yaml", "**/.ok/workflow/*.yaml"}

// deployServe runs the deployer until it is told to stop.
func deployServe(args []string) error {
	flags := flag.NewFlagSet("deploy serve", flag.ContinueOnError)
	path := flags.String("config", "ok.yaml", "the installation's configuration")
	color := flags.String("color", "", "the copy of the deployer this is, blue or green (default: $"+colorEnv+", else blue)")
	devPrincipal := flags.String("dev-principal", "", "the principal to name to loopback development services (tests)")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if cfg.Deploy.Checkout == "virtual" {
		if err := checkout.RequireIsolated(); err != nil {
			return err
		}
	}
	o, err := idclient.Options(cfg.Services)
	if err != nil {
		return err
	}
	var cred dial.Credential
	if *devPrincipal != "" {
		o.DevPrincipal = *devPrincipal
	} else if cred, err = idclient.Credential(cfg, o); err != nil {
		return err
	} else {
		o.Credential = cred
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	scm, err := dial.Dial(cfg.Services.SCM, o)
	if err != nil {
		return err
	}
	defer scm.Close()
	parts, err := deployParts(ctx, cfg, scm, cfg.Services.SCM, cfg.Services.TLS, cred, *devPrincipal)
	if err != nil {
		return err
	}
	defer parts.close()
	r, err := role.Build(ctx, cfg, role.Options{Dev: *devPrincipal != "", Color: deployColor(*color), Checkout: parts.checkout, Source: parts.source,
		Bazel: parts.bazel, Env: parts.env, SCM: scm, Address: os.Getenv(addressEnv)})
	if err != nil {
		return err
	}
	defer r.Close()
	opts := []serve.Option{serve.WithAuthn(r.Resolver), serve.WithListenAddr(cfg.Listen), serve.WithMetricsAddr(cfg.MetricsListen), serve.WithProfiling(cfg.Profiling),
		serve.WithStopTimeout(workerStopTimeout)}
	if cfg.TLS != nil {
		tc, err := serve.LoadTLS(cfg.TLS.Cert, cfg.TLS.Key)
		if err != nil {
			return err
		}
		opts = append(opts, serve.WithTLS(tc))
	}
	slog.Info("ok deploy: driving deployments", "self", cfg.Deploy.Self.Namespace+"/"+cfg.Deploy.Self.Deployment, "checkout", cfg.Deploy.Checkout, "bazel", parts.bazel)
	return serve.Run(ctx, r.Root, opts...)
}

// deployColor is the copy of the deployer this process is: the flag's, else $OK_DEPLOY_COLOR's,
// else blue.
func deployColor(flagged string) string {
	if flagged != "" {
		return flagged
	}
	if c := os.Getenv(colorEnv); c != "" {
		return c
	}
	return roll.Blue
}

// deployer are what the deployer's engine works with beside its configuration: its checkout, the
// SCM it reads commits from, Bazel, and the environment of the Bazel and migrations it runs.
type deployer struct {
	checkout checkout.Checkout
	source   checkout.Source
	bazel    string
	env      []string
	close    func()
}

// deployParts makes the deployer's checkout — through okfs, its daemon reaching the SCM at scmAddr
// with cred, or written to disk — and environment. The caller isolated the process for a virtual
// checkout.
func deployParts(ctx context.Context, cfg *config.Config, scm grpc.ClientConnInterface, scmAddr string, scmTLS bool, cred dial.Credential, devPrincipal string) (*deployer, error) {
	d := cfg.Deploy
	if d.State == "" {
		return nil, errors.New("deploy.state is the directory the deployer keeps its checkout and caches in")
	}
	bazel, err := lookPath(d.Bazel, "bazelisk", "bazel")
	if err != nil {
		return nil, fmt.Errorf("Bazel: %w", err)
	}
	out := &deployer{bazel: bazel, source: checkout.SCM(scmpb.NewBrowseClient(scm), scmpb.NewStoreClient(scm)), close: func() {}}
	dir := filepath.Join(d.State, "repo")
	switch d.Checkout {
	case "virtual":
		jj, err := lookPath(d.JJ, "jj")
		if err != nil {
			return nil, fmt.Errorf("ok's jj, the okfs daemon: %w", err)
		}
		vc := checkout.VirtualConfig{Dir: dir, State: filepath.Join(d.State, "okfs"), JJ: jj, SCM: scmAddr, TLS: scmTLS,
			CAFile: cfg.Services.CAFile, DevPrincipal: devPrincipal, ReadAhead: planningReads, Log: os.Stderr}
		if cred != nil {
			vc.ConfigDir = filepath.Join(d.State, "okfs", "config")
			if err := keepCredential(ctx, cred, filepath.Join(vc.ConfigDir, "credential"), auth.Server{Addr: scmAddr, TLS: scmTLS}); err != nil {
				return nil, fmt.Errorf("the okfs daemon's credential: %w", err)
			}
		}
		v, err := checkout.NewVirtual(vc)
		if err != nil {
			return nil, err
		}
		out.checkout, out.close = v, func() { v.Close() }
	case "real":
		if out.checkout, err = checkout.NewReal(dir, out.source); err != nil {
			return nil, err
		}
	}
	home := filepath.Join(d.State, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		return nil, err
	}
	// Bazel's environment, and the migrations': where each finds its database.
	out.env = []string{"LANG=C.UTF-8", "TZ=UTC", "HOME=" + home}
	for _, k := range workerEnv {
		if v, ok := os.LookupEnv(k); ok {
			out.env = append(out.env, k+"="+v)
		}
	}
	for k, v := range d.MigrationEnv {
		value, err := v.Resolve()
		if err != nil {
			return nil, fmt.Errorf("deploy.migration_env.%s: %w", k, err)
		}
		out.env = append(out.env, k+"="+value)
	}
	return out, nil
}
