package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"google.golang.org/grpc/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/client/auth"
	"github.com/Cidan/ok/internal/deploy/agent"
	"github.com/Cidan/ok/internal/deploy/engine"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/idclient"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// deployAgent runs an organization's deploy agent in its cluster (docs/hosted.md, "Deploy agents, as
// built"), as the robot its organization designated: connected out to each cell's deployer that
// holds its repositories, it calls its cluster's API with its own ServiceAccount, and runs
// migrations with its own environment.
func deployAgent(args []string) error {
	flags := flag.NewFlagSet("deploy agent", flag.ContinueOnError)
	addr := flags.String("address", os.Getenv("OK_AGENT_ADDRESS"), "where ok is reached, host:port: its public address (default $OK_AGENT_ADDRESS)")
	tokenFile := flags.String("token-file", "", "a token the cluster's issuer signs for the agent's robot, its audience the organization's ID")
	credDir := flags.String("credential-dir", "", "a key and certificate an owner certified for the agent's robot, instead")
	caFile := flags.String("ca-file", "", "the CA ok's certificate is signed by, when not one the system trusts")
	plaintext := flags.Bool("insecure", false, "plaintext, to an ok on a network the cluster trusts")
	kubeconfig := flags.String("kubeconfig", "", "the cluster, from outside it (default: the cluster it runs in)")
	state := flags.String("state", "", "where it checks migrations out and builds them; unset, it runs none")
	bazel := flags.String("bazel", "", "the Bazel, or bazelisk, migrations run with (default: bazelisk, else bazel, on PATH)")
	devPrincipal := flags.String("dev-principal", "", "the principal to name to a loopback development ok (tests)")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	if *addr == "" {
		return errors.New("ok deploy agent: -address is where ok is reached")
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
	o := base
	switch {
	case *devPrincipal != "":
		o.DevPrincipal = *devPrincipal
	case *tokenFile != "":
		w, err := idclient.NewWorkload(*addr, base, *tokenFile, hostLabel())
		if err != nil {
			return err
		}
		o.Credential = w
	case *credDir != "":
		c, err := auth.Load(*credDir)
		if err != nil {
			return err
		}
		o.Credential = c
	default:
		return errors.New("ok deploy agent: -token-file or -credential-dir is how its robot proves itself")
	}
	conn, err := dial.Dial(*addr, o)
	if err != nil {
		return err
	}
	defer conn.Close()

	var kube *rest.Config
	if *kubeconfig != "" {
		kube, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	} else {
		kube, err = rest.InClusterConfig()
	}
	if err != nil {
		return fmt.Errorf("the cluster: %w", err)
	}
	a := &agent.Agent{Kube: kube, Version: version, Log: slog.Default()}
	agents := identitypb.NewDeployAgentsClient(conn)
	if *state != "" {
		path, err := lookPath(*bazel, "bazelisk", "bazel")
		if err != nil {
			return fmt.Errorf("Bazel, which migrations run with: %w", err)
		}
		src := checkout.SCM(scmpb.NewStoreClient(conn))
		co, err := checkout.NewReal(filepath.Join(*state, "repo"), src)
		if err != nil {
			return err
		}
		m := &agent.Migrations{Identity: agents, SCM: struct {
			scmpb.RepositoriesClient
			scmpb.BrowseClient
		}{scmpb.NewRepositoriesClient(conn), scmpb.NewBrowseClient(conn)}, Checkout: co,
			// The agent's own environment: where its organization's migrations find their databases.
			Bazel: &engine.Bazel{Path: path, Env: os.Environ(), OutputBase: filepath.Join(*state, "bazel"), Checkout: co, Source: src, Log: slog.Default()}}
		a.Migrate = m.Run
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r := &agent.Runner{Agent: a, Log: slog.Default(),
		Cells: func(ctx context.Context) ([]string, error) {
			resp, err := agents.DeployCells(ctx, &identitypb.DeployCellsRequest{})
			return resp.GetCells(), err
		},
		Connect: func(ctx context.Context, cell string) (deploypb.Agents_ConnectClient, error) {
			if cell != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, cells.Header, cell)
			}
			return deploypb.NewAgentsClient(conn).Connect(ctx)
		}}
	slog.Info("ok deploy agent: connecting", "address", *addr, "migrations", *state != "")
	return r.Run(ctx)
}

// hostLabel names the agent's process: its pod's name, or its host's.
func hostLabel() string {
	if h := os.Getenv("HOSTNAME"); h != "" {
		return h
	}
	h, _ := os.Hostname()
	return h
}
