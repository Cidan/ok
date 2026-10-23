package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Cidan/ok/internal/config"
	idrole "github.com/Cidan/ok/internal/id/role"
	idstore "github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/secrets/keys"
	"github.com/Cidan/ok/internal/secrets/role"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/serve"
)

// secretsServe runs ok secrets until it is told to stop.
func secretsServe(args []string) error {
	flags := flag.NewFlagSet("secrets serve", flag.ContinueOnError)
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

// secretsStore opens ok secrets' store for an administrator's command, and the repository it
// names, by ID or full name, with its full name.
func secretsStore(ctx context.Context, name string, args []string, extra func(*flag.FlagSet)) (*store.Store, int64, string, func(), error) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	path := flags.String("config", "ok.yaml", "the installation's configuration")
	if extra != nil {
		extra(flags)
	}
	if err := flags.Parse(args); err != nil {
		return nil, 0, "", nil, flagError(err)
	}
	if flags.NArg() != 1 {
		return nil, 0, "", nil, usageError("ok " + name + " names one repository, by full name or ID")
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return nil, 0, "", nil, err
	}
	id, full, err := repositoryNamed(ctx, cfg, flags.Arg(0))
	if err != nil {
		return nil, 0, "", nil, err
	}
	// The repository's keys are wrapped by its organization's own root, where it names one, as
	// the role's are: read from identity's database.
	b, _, idDone, err := idrole.OpenBackend(ctx, cfg, 2)
	if err != nil {
		return nil, 0, "", nil, err
	}
	ids := idstore.New(b, cfg.Names())
	rootOf := func(ctx context.Context, repo int64) (string, error) {
		r, err := ids.Repository(ctx, repo)
		if err != nil {
			return "", err
		}
		return ids.SecretsRoot(ctx, r.Owner)
	}
	s, done, err := role.OpenStore(ctx, cfg, role.Options{RootOf: rootOf})
	if err != nil {
		idDone()
		return nil, 0, "", nil, err
	}
	return s, id, full, func() { done(); idDone() }, nil
}

// secretsRotate makes a repository's next key version, which every value written after is sealed
// under.
func secretsRotate(args []string, stdout io.Writer) error {
	ctx := context.Background()
	s, repo, full, done, err := secretsStore(ctx, "secrets rotate", args, nil)
	if err != nil {
		return err
	}
	defer done()
	v, err := s.Rotate(ctx, repo)
	if err != nil {
		return err
	}
	if err := s.Audit(ctx, store.AuditEntry{Repository: repo, Actor: "ok secrets rotate", Op: "rotate", Outcome: fmt.Sprintf("key version %d", v)}); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%s: key version %d seals what is written from now; ok secrets rewrap %s wraps what came before under it\n", full, v, full)
	return nil
}

// secretsTransfer moves a repository's secrets' sealed values into another store (docs/secrets.md,
// "Stores"): an external store the configuration names (secrets.stores), or default.
func secretsTransfer(args []string, stdout io.Writer) error {
	ctx := context.Background()
	var to *string
	s, repo, full, done, err := secretsStore(ctx, "secrets transfer", args, func(f *flag.FlagSet) {
		to = f.String("to", "", "the store to move them into: a name secrets.stores gives one, or default")
	})
	if err != nil {
		return err
	}
	defer done()
	if *to == "" {
		return usageError("ok secrets transfer -to STORE REPOSITORY: name the store, or default")
	}
	n, err := s.Transfer(ctx, repo, *to)
	if aerr := s.Audit(ctx, store.AuditEntry{Repository: repo, Actor: "ok secrets transfer", Op: "transfer",
		Outcome: fmt.Sprintf("%d versions into %s; %v", n, *to, err)}); aerr != nil && err == nil {
		err = aerr
	}
	if err != nil {
		return fmt.Errorf("%s: %d versions moved into %s, then: %w", full, n, *to, err)
	}
	fmt.Fprintf(stdout, "%s: %d versions moved into %s; once its settings name %s (secrets: {store: %s}), transfer again for what was written meanwhile\n",
		full, n, *to, *to, *to)
	return nil
}

// secretsRewrap wraps a repository's data keys again under its newest key version, deletes the
// older versions, and wraps the newest again under the repository's root as it is now.
func secretsRewrap(args []string, stdout io.Writer) error {
	ctx := context.Background()
	s, repo, full, done, err := secretsStore(ctx, "secrets rewrap", args, nil)
	if err != nil {
		return err
	}
	defer done()
	res, err := s.Rewrap(ctx, repo)
	if err != nil {
		return err
	}
	if err := s.Audit(ctx, store.AuditEntry{Repository: repo, Actor: "ok secrets rewrap", Op: "rewrap",
		Outcome: fmt.Sprintf("%d versions; retired %v; kept %v", res.Versions, res.Retired, res.Kept)}); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%s: %d versions wrapped again; key versions retired %v", full, res.Versions, res.Retired)
	if len(res.Kept) > 0 {
		fmt.Fprintf(stdout, "; kept %v, written under since — rewrap again", res.Kept)
	}
	fmt.Fprintln(stdout)
	return nil
}

// secretsAudit prints a repository's decisions, oldest first.
func secretsAudit(args []string, stdout io.Writer) error {
	ctx := context.Background()
	var limit *int
	s, repo, full, done, err := secretsStore(ctx, "secrets audit", args, func(f *flag.FlagSet) {
		limit = f.Int("limit", 1000, "how many decisions to print at most")
	})
	if err != nil {
		return err
	}
	defer done()
	fmt.Fprintf(stdout, "# %s\n", full)
	var after int64
	for printed := 0; printed < *limit; {
		es, err := s.AuditOf(ctx, repo, after, min(500, *limit-printed))
		if err != nil {
			return err
		}
		for _, e := range es {
			fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", e.At.UTC().Format(time.RFC3339), e.Op, e.Actor, e.For, e.Path, e.Digest, e.Outcome)
		}
		printed += len(es)
		if len(es) < 500 {
			break
		}
		after = es[len(es)-1].ID
	}
	return nil
}

// secretsKeyring prints a keyring line for a new key: a root of keys outside any database, for an
// installation on one machine (secrets.root: keyring:<file>). A new key goes first, as the primary.
func secretsKeyring(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("secrets keyring", flag.ContinueOnError)
	id := flags.String("id", "", "the key's ID: letters, digits, - and _")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	if *id == "" || flags.NArg() != 0 {
		return usageError("ok secrets keyring -id ID")
	}
	line, err := keys.NewKeyringLine(*id)
	if err != nil {
		return err
	}
	_, err = io.WriteString(stdout, line)
	return err
}
