package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/edge/role"
	"github.com/Cidan/ok/internal/serve"
)

// edgeServe is ok edge serve: the edge in front of a deployment's cells (docs/hosted.md, "Cells, as
// built"), alone in its process.
func edgeServe(args []string) error {
	flags := flag.NewFlagSet("edge serve", flag.ContinueOnError)
	path := flags.String("config", "ok.yaml", "the installation's configuration")
	dev := flags.Bool("dev", false, "pass development principals (ok-dev-principal) on, on a loopback address only")
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
	opts := []serve.Option{serve.WithListenAddr(cfg.Listen), serve.WithMetricsAddr(cfg.MetricsListen), serve.WithProfiling(cfg.Profiling)}
	if cfg.TLS != nil {
		tc, err := serve.LoadTLS(cfg.TLS.Cert, cfg.TLS.Key)
		if err != nil {
			return err
		}
		opts = append(opts, serve.WithTLS(tc))
	}
	return serve.Run(ctx, r.Root, opts...)
}
