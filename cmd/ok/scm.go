package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Cidan/ok/internal/config"
	idrole "github.com/Cidan/ok/internal/id/role"
	idstore "github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/scm/store"
	"github.com/Cidan/ok/internal/serve"
)

// scmServe runs the SCM role until it is told to stop.
func scmServe(args []string) error {
	flags := flag.NewFlagSet("scm serve", flag.ContinueOnError)
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
	if *dev {
		opts = append(opts, serve.WithReflection())
	}
	return serve.Run(ctx, r.Root, opts...)
}

// scmStore opens the SCM's store for an administrator's command, and the repository it names, by
// ID or full name, with its full name.
func scmStore(ctx context.Context, name string, args []string, extra func(*flag.FlagSet)) (*store.Store, store.Repository, string, func(), error) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	path := flags.String("config", "ok.yaml", "the installation's configuration")
	if extra != nil {
		extra(flags)
	}
	if err := flags.Parse(args); err != nil {
		return nil, store.Repository{}, "", nil, flagError(err)
	}
	if flags.NArg() != 1 {
		return nil, store.Repository{}, "", nil, usageError("ok " + name + " names one repository, by full name or ID")
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return nil, store.Repository{}, "", nil, err
	}
	id, full, err := repositoryNamed(ctx, cfg, flags.Arg(0))
	if err != nil {
		return nil, store.Repository{}, "", nil, err
	}
	s, done, err := role.OpenStore(ctx, cfg, role.Options{})
	if err != nil {
		return nil, store.Repository{}, "", nil, err
	}
	repo, err := s.GetRepository(ctx, id)
	if err != nil {
		done()
		return nil, store.Repository{}, "", nil, err
	}
	return s, repo, full, done, nil
}

// repositoryNamed is the repository an administrator names, by ID, or by full name — or one it
// had — which the directory's store resolves; and its full name.
func repositoryNamed(ctx context.Context, cfg *config.Config, arg string) (int64, string, error) {
	b, _, done, err := idrole.OpenBackend(ctx, cfg, 2)
	if err != nil {
		return 0, "", err
	}
	defer done()
	st := idstore.New(b, cfg.Names())
	if !cfg.Tenancy.Hosted() {
		if o, err := st.OrganizationByHandle(ctx, cfg.Tenancy.Organization); err == nil {
			st.SetInstallation(o.ID)
		}
	}
	var r idstore.Repository
	if id, perr := strconv.ParseInt(arg, 10, 64); perr == nil {
		r, err = st.Repository(ctx, id)
	} else {
		r, _, err = st.ResolveFullName(ctx, arg)
	}
	if errors.Is(err, idstore.ErrNotFound) {
		return 0, "", fmt.Errorf("no repository is called %s", arg)
	}
	if err != nil {
		return 0, "", err
	}
	e, err := st.Entry(ctx, r)
	if err != nil {
		return 0, "", err
	}
	return r.ID, e.GetFullName(), nil
}

// scmFsck verifies one repository against the SCM's invariants.
func scmFsck(args []string, stdout io.Writer) error {
	var repair *bool
	ctx := context.Background()
	s, repo, full, done, err := scmStore(ctx, "scm fsck", args, func(f *flag.FlagSet) {
		repair = f.Bool("repair", false, "rebuild the content index's entries for chunks files use that it lacks, from the packs that hold them")
	})
	if err != nil {
		return err
	}
	defer done()
	r, err := s.Fsck(ctx, repo.ID, *repair)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "repository %s (%d)\n%s", full, repo.ID, r)
	if len(r.Problems) > 0 {
		return fmt.Errorf("%d problems", len(r.Problems))
	}
	return nil
}

// scmGC runs a garbage-collection pass over one repository now.
func scmGC(args []string, stdout io.Writer) error {
	var dryRun *bool
	ctx := context.Background()
	s, repo, full, done, err := scmStore(ctx, "scm gc", args, func(f *flag.FlagSet) {
		dryRun = f.Bool("dry-run", false, "report what the pass would collect, sweep, compact, and delete, and change nothing")
	})
	if err != nil {
		return err
	}
	defer done()
	r, err := s.GC(ctx, repo.ID, time.Now(), store.GCOptions{DryRun: *dryRun})
	if r != nil {
		if rerr := s.RecordGC(ctx, r, err); rerr != nil {
			fmt.Fprintln(os.Stderr, "ok scm gc: recording the pass:", rerr)
		}
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "repository %s: %s\n", full, r)
	return nil
}
