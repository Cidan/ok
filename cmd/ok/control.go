package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/control/role"
	"github.com/Cidan/ok/internal/serve"
)

// controlServe runs ok control until it is told to stop.
func controlServe(args []string) error {
	flags := flag.NewFlagSet("control serve", flag.ContinueOnError)
	path := flags.String("config", "ok.yaml", "the installation's configuration")
	dev := flags.Bool("dev", false, "accept development principals (ok-dev-principal), on a loopback address only")
	if err := flags.Parse(args); err != nil {
		return usageError(err.Error())
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
	if *dev {
		opts = append(opts, serve.WithReflection())
	}
	return serve.Run(ctx, r.Root, opts...)
}
