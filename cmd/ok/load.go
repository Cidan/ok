package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/client/auth"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/idclient"
	"github.com/Cidan/ok/internal/load"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	controlpb "github.com/Cidan/ok/proto/ok/control/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// loadRun drives an installation at a fraction of a cell's share of the target (docs/hosted.md,
// "Proof at scale, as built"), and reports.
func loadRun(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("load run", flag.ContinueOnError)
	addr := flags.String("address", "", "where the installation is reached, host:port: its public address, or ok serve's")
	plaintext := flags.Bool("insecure", false, "plaintext, to an installation on a network this one trusts")
	caFile := flags.String("ca-file", "", "the CA the installation's certificate is signed by, when not one the system trusts")
	personDir := flags.String("credential-dir", "", "the load's person's credential (ok auth login's)")
	devPerson := flags.String("dev-person", "", "the person to name to a loopback development installation instead")
	workerDir := flags.String("worker-credential-dir", "", "a worker's credential, for the synthetic workers")
	workerToken := flags.String("worker-token-file", "", "a worker's token instead, which it certifies a key with")
	devWorker := flags.String("dev-worker", "", "the worker to name to a loopback development installation instead")
	owner := flags.String("owner", "", "the repositories' owner's handle (none: the installation's organization)")
	prefix := flags.String("prefix", "load", "the repositories' names' prefix")
	repositories := flags.Int("repositories", 100, "repositories to drive, each at the target's rates")
	speed := flags.Float64("speed", 1, "each repository's rates times this: N repositories at speed S offer what N×S do")
	peak := flags.Bool("peak", false, "publishes at the target's peak, not its average")
	clsPerDay := flags.Float64("cls-per-day", 10, "CLs a repository a day: the target names no rate")
	duration := flags.Duration("duration", 5*time.Minute, "how long load is offered")
	drain := flags.Duration("drain", 5*time.Minute, "how long after that it waits for CLs under way")
	publishBytes := flags.Int("publish-bytes", load.PublishBytes, "bytes a publish sends: by default, an account's ingest a day over its publishes")
	checkpointBytes := flags.Int("checkpoint-bytes", load.CheckpointFile, "bytes a checkpoint's changed file holds")
	clBytes := flags.Int("cl-bytes", 64<<10, "bytes a CL's change holds")
	seedFiles := flags.Int("seed-files", load.SeedFiles, "files in each repository's seeded tree, shaped as a monorepo's: checkpoints, publishes, and CLs change them, and publishes carry jj's operations (0: none, each write a directory of its own)")
	seedFileBytes := flags.Int("seed-file-bytes", load.SeedFileBytes, "bytes each seeded file holds")
	workers := flags.Int("workers", 16, "synthetic workers")
	runTime := flags.Duration("run-time", 10*time.Second, "how long a synthetic job takes")
	read := flags.Bool("read", true, "synthetic jobs read their commit's files through the SCM")
	checkoutDir := flags.String("checkout-dir", "", "with -read, each synthetic worker keeps a real checkout in a directory of its own here and moves it to each job's commit, as a worker does (default: each job reads its whole commit)")
	inFlight := flags.Int("max-in-flight", 2000, "an operation's requests under way at most: one offered past it is shed")
	reportPath := flags.String("report", "", "where to write the report, JSON (default: none; -: standard output, one line marked \"ok load report:\")")
	people := flags.Int("people", 0, "people to act as, each signed in through the installation's sign-in and on a connection of their own (0: the one person of -credential-dir or -dev-person)")
	pods := flags.Int("pods", 1, "the run's pods: each acts as its share of the people")
	pod := flags.Int("pod", envInt("JOB_COMPLETION_INDEX"), "this pod's index among them (default $JOB_COMPLETION_INDEX)")
	server := flags.String("server", "", "ok's public URL, where people sign in (with -people)")
	passwordFile := flags.String("password-file", "", "people's password at the installation's OpenID Connect provider (with -people)")
	signinParallel := flags.Int("signin-parallel", 32, "people signed in at once")
	workerConns := flags.Int("worker-connections", 4, "connections the synthetic workers spread over")
	browsesPerDay := flags.Float64("browses-per-day", 100, "reads of main's listing a unit a day: the target names no rate")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	if *addr == "" {
		return errors.New("ok load run: -address is where the installation is reached")
	}
	base := dial.Options{Insecure: *plaintext}
	if !*plaintext {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if *caFile != "" {
			pem, err := os.ReadFile(*caFile)
			if err != nil {
				return err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return fmt.Errorf("%s holds no certificate", *caFile)
			}
			tc.RootCAs = pool
		}
		base.TLS = tc
	}
	as := func(what, dev, dir, token string) (dial.Options, error) {
		o := base
		switch {
		case dev != "":
			o.DevPrincipal = dev
		case dir != "":
			c, err := auth.Load(dir)
			if err != nil {
				return o, err
			}
			o.Credential = c
		case token != "":
			w, err := idclient.NewWorkload(*addr, base, token, hostLabel())
			if err != nil {
				return o, err
			}
			o.Credential = w
		default:
			return o, fmt.Errorf("ok load run: how the %s proves itself: a credential, a token, or a development principal", what)
		}
		return o, nil
	}
	wo, err := as("worker", *devWorker, *workerDir, *workerToken)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The people this pod acts as: its share of -people, each signed in and on a connection of
	// their own; or the one person.
	var who []load.Person
	if *people > 0 {
		if *server == "" || *passwordFile == "" || *pods < 1 || *pod < 0 || *pod >= *pods {
			return errors.New("ok load run: -people signs people in at -server with -password-file, as pod -pod of -pods")
		}
		pw, err := os.ReadFile(*passwordFile)
		if err != nil {
			return err
		}
		lo, hi := *pod**people / *pods, (*pod+1)**people / *pods
		root, err := os.MkdirTemp("", "ok-load-people-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(root)
		slog.Info("ok load: signing people in", "from", load.PersonHandle(lo), "to", load.PersonHandle(hi-1))
		t0 := time.Now()
		creds, err := load.SignInAll(ctx, *server, root, lo, hi, strings.TrimSpace(string(pw)), *signinParallel)
		if err != nil {
			return err
		}
		slog.Info("ok load: signed in", "people", len(creds), "took", time.Since(t0).Round(time.Millisecond))
		for i, c := range creds {
			o := base
			o.Credential = c
			conn, err := dial.Dial(*addr, o)
			if err != nil {
				return err
			}
			defer conn.Close()
			who = append(who, load.Person{Handle: load.PersonHandle(lo + i), Clients: personClients(conn)})
		}
	} else {
		po, err := as("person", *devPerson, *personDir, "")
		if err != nil {
			return err
		}
		conn, err := dial.Dial(*addr, po)
		if err != nil {
			return err
		}
		defer conn.Close()
		who = []load.Person{{Clients: personClients(conn)}}
	}
	var workerClients []load.Clients
	for range max(*workerConns, 1) {
		conn, err := dial.Dial(*addr, wo)
		if err != nil {
			return err
		}
		defer conn.Close()
		workerClients = append(workerClients, load.Clients{Work: automationpb.NewWorkClient(conn), WorkerBrowse: scmpb.NewBrowseClient(conn),
			WorkerStore: scmpb.NewStoreClient(conn)})
	}

	slog.Info("ok load: setting up repositories", "repositories", *repositories)
	rs, err := load.Setup(ctx, who[0].Repositories, *owner, *prefix, *repositories, 32)
	if err != nil {
		return err
	}
	shape := load.Shape{Repositories: *repositories, Speed: *speed, Peak: *peak, CLsPerDay: *clsPerDay, BrowsesPerDay: *browsesPerDay}
	if *people > 0 {
		shape.Repositories, shape.People = len(who), true
	}
	rates := shape.Rates()
	slog.Info("ok load: offering", "shape", shape.String(), "publishes/s", rates.Publishes, "checkpoints/s", rates.Checkpoints, "cls/s", rates.CLs,
		"browses/s", rates.Browses)
	rep, err := load.Run(ctx, load.Config{People: who, WorkerClients: workerClients, WorkerPrefix: strconv.Itoa(*pod),
		Repos: rs, Shape: shape, Duration: *duration, Drain: *drain, MaxInFlight: *inFlight, PublishBytes: *publishBytes, CheckpointBytes: *checkpointBytes, CLBytes: *clBytes,
		Workers: *workers, RunTime: *runTime, Read: *read, CheckoutDir: *checkoutDir, SeedFiles: *seedFiles, SeedFileBytes: *seedFileBytes, SeedLands: *pod == 0,
		Pod: *pod, Pods: *pods, Log: slog.Default()})
	if err != nil {
		return err
	}
	for _, line := range rep.Summary() {
		fmt.Fprintln(stdout, line)
	}
	switch *reportPath {
	case "":
	case "-":
		data, err := json.Marshal(rep)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s%s\n", reportMark, data)
	default:
		data, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(*reportPath, append(data, '\n'), 0o644); err != nil {
			return err
		}
	}
	if !rep.Pass {
		return errors.New("ok load run: a check failed")
	}
	return nil
}

// reportMark begins the line a report written to standard output is on.
const reportMark = "ok load report: "

// personClients are a person's services over their connection.
func personClients(conn *grpc.ClientConn) load.Clients {
	return load.Clients{Repositories: scmpb.NewRepositoriesClient(conn), Store: scmpb.NewStoreClient(conn), Upload: scmpb.NewChangeListsClient(conn),
		Workspaces: scmpb.NewWorkspacesClient(conn), Control: controlpb.NewChangeListsClient(conn), Browse: scmpb.NewBrowseClient(conn)}
}

// envInt is an environment variable's integer, 0 when unset or none.
func envInt(name string) int {
	n, _ := strconv.Atoi(os.Getenv(name))
	return n
}

// loadMerge merges a run's pods' reports — files of them, or logs holding their marked lines — as
// one, and says what it holds.
func loadMerge(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("load merge", flag.ContinueOnError)
	out := flags.String("report", "", "where to write the merged report, JSON (default: none)")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	var reps []*load.Report
	for _, path := range flags.Args() {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		found := false
		for _, line := range strings.Split(string(data), "\n") {
			if i := strings.Index(line, reportMark); i >= 0 {
				var r load.Report
				if err := json.Unmarshal([]byte(line[i+len(reportMark):]), &r); err != nil {
					return fmt.Errorf("%s: %w", path, err)
				}
				reps, found = append(reps, &r), true
			}
		}
		if !found {
			var r load.Report
			if err := json.Unmarshal(data, &r); err != nil {
				return fmt.Errorf("%s holds no report: %w", path, err)
			}
			reps = append(reps, &r)
		}
	}
	rep, err := load.Merge(reps)
	if err != nil {
		return err
	}
	for _, line := range rep.Summary() {
		fmt.Fprintln(stdout, line)
	}
	if *out != "" {
		data, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(*out, append(data, '\n'), 0o644); err != nil {
			return err
		}
	}
	if !rep.Pass {
		return errors.New("ok load merge: a check failed")
	}
	return nil
}

// loadPeople writes the people a run with -people acts as: the system of record's file, beside the
// load's person, and Dex's static users, each with the password whose bcrypt hash -hash-file holds.
func loadPeople(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("load people", flag.ContinueOnError)
	n := flags.Int("n", 2000, "people")
	hashFile := flags.String("hash-file", "", "the bcrypt hash of their password, as Dex keeps it")
	dir := flags.String("dir", ".", "where to write people.yaml and dex-people.yaml")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	hash, err := os.ReadFile(*hashFile)
	if err != nil {
		return err
	}
	people, dex := load.PeopleFiles(*n, strings.TrimSpace(string(hash)))
	if err := os.WriteFile(filepath.Join(*dir, "people.yaml"), people, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*dir, "dex-people.yaml"), dex, 0o600); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "wrote %d people to %s\n", *n, *dir)
	return nil
}

// loadCost prices a run (docs/hosted.md, "Proof at scale, as built"): a cell's cost a month at its
// full share of the target, the target's, and an account's.
func loadCost(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("load cost", flag.ContinueOnError)
	reportPath := flags.String("report", "", "the run's report (ok load run -report)")
	measuredPath := flags.String("measured", "", "what the installation used over the run, JSON")
	pricesPath := flags.String("prices", "", "the cloud's list prices (deploy/load/gcp-us-central1.yaml)")
	headroom := flags.Float64("headroom", 0.5, "the share of a core ok's servers run at")
	compression := flags.Float64("compression", 2, "how much smaller a repository is stored than written: the target assumes 1.5–3")
	dbBytes := flags.Float64("database-bytes-per-repository", 50e6, "a repository's bytes in its cell's database at steady state")
	jsonOut := flags.Bool("json", false, "the cost as JSON")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	var rep load.Report
	var m load.Measured
	for path, v := range map[string]any{*reportPath: &rep, *measuredPath: &m} {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, v); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	p, err := load.LoadPrices(*pricesPath)
	if err != nil {
		return err
	}
	c, err := m.Price(&rep, p, load.Assumptions{Headroom: *headroom, Compression: *compression, DatabaseBytesPerRepository: *dbBytes})
	if err != nil {
		return err
	}
	if *jsonOut {
		data, err := json.MarshalIndent(c, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "%s\n", data)
		return err
	}
	fmt.Fprintf(stdout, "A cell at its full share of the target (%d accounts), a month:\n", load.CellAccounts)
	for _, l := range c.Lines {
		fmt.Fprintf(stdout, "  %-28s %14.1f %-20s $%12.2f  (%s)\n", l.What, l.Quantity, l.Unit, l.Monthly, l.Basis)
	}
	fmt.Fprintf(stdout, "  %-28s %50s $%12.2f\n", "the cell", "", c.Cell)
	fmt.Fprintf(stdout, "The target (%d accounts): $%.0f a month; an account: $%.2f.\n", load.TargetAccounts, c.Target, c.PerAccount)
	fmt.Fprintln(stdout, "Excluded:")
	for _, e := range c.Excluded {
		fmt.Fprintln(stdout, "  "+e)
	}
	fmt.Fprintln(stdout, "Assumed:")
	for _, a := range c.Assumptions {
		fmt.Fprintln(stdout, "  "+a)
	}
	return nil
}
