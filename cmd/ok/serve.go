package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	autorole "github.com/Cidan/ok/internal/automation/role"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/config"
	ctlrole "github.com/Cidan/ok/internal/control/role"
	deployrole "github.com/Cidan/ok/internal/deploy/role"
	"github.com/Cidan/ok/internal/dial"
	idrole "github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/idclient"
	scmrole "github.com/Cidan/ok/internal/scm/role"
	secretsrole "github.com/Cidan/ok/internal/secrets/role"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/serve/inproc"
	"github.com/Cidan/ok/internal/serve/node"
)

// serverRoles are the roles ok serve runs, in the order they are built: each after those it
// calls.
var serverRoles = []string{"identity", "scm", "automation", "deploy", "control", "secrets"}

// serveRoles runs several server roles in one process, on one port: a small installation's whole
// ok. ok identity is among them, and certifies the others' keys directly, each its own robot's; the
// roles call each other over the process's own port, and any role not among them at its services
// address.
func serveRoles(args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serveRolesUntil(ctx, args)
}

// serveRolesUntil is ok serve, until ctx ends.
func serveRolesUntil(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	path := flags.String("config", "ok.yaml", "the installation's configuration")
	list := flags.String("roles", "all", "the roles to run, comma-separated: identity, scm, automation, deploy, control, secrets; or all (secrets when secrets.root names a root)")
	dev := flags.Bool("dev", false, "accept development principals (ok-dev-principal), on a loopback address only")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	run := map[string]bool{}
	for _, r := range strings.Split(*list, ",") {
		switch r = strings.TrimSpace(r); {
		case r == "all":
			for _, s := range serverRoles {
				run[s] = true
			}
		case slices.Contains(serverRoles, r):
			run[r] = true
		default:
			return usageError(fmt.Sprintf("%q is no server role: identity, scm, automation, deploy, control, secrets, or all", r))
		}
	}
	all := strings.Contains(","+strings.ReplaceAll(*list, " ", "")+",", ",all,")
	if !run["identity"] {
		return usageError("ok serve runs ok identity among its roles, which certifies the others' keys: run one role alone with ok <role> serve")
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if *dev && !loopback(cfg.Listen) {
		return fmt.Errorf("-dev serves only on loopback, and listen is %s", cfg.Listen)
	}
	// All runs ok secrets where the installation keeps secrets: where it names their root.
	if all && cfg.Secrets.Root == "" {
		run["secrets"] = false
	}
	if run["deploy"] && cfg.Deploy.Checkout == "virtual" {
		if err := checkout.RequireIsolated(); err != nil {
			return err
		}
	}
	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	local, localTLS, err := localAddress(cfg, lis.Addr().(*net.TCPAddr))
	if err != nil {
		return err
	}
	id, err := idrole.Build(ctx, cfg, idrole.Options{Dev: *dev})
	if err != nil {
		return err
	}
	defer id.Close()
	roots := []node.Node{id.Root}
	// A connection, as a local role's robot, to a role: this process's own port when it runs the
	// role, else its services address.
	var conns []*grpc.ClientConn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	remote, err := idclient.Options(cfg.Services)
	if err != nil {
		return err
	}
	// A role this process runs is reached within it (inproc), as its robot: no credential is proven
	// or checked, as nothing leaves the process.
	self := inproc.Listen()
	as := func(robot, role, addr string) (grpc.ClientConnInterface, dial.Credential, error) {
		var o dial.Options
		cred := dial.Credential(idclient.NewLocal(id.Services, robot))
		if run[role] {
			c, err := dial.Dial("", dial.Options{InProcess: self.Dial, InProcessRobot: cfg.RobotName(robot)})
			if err != nil {
				return nil, nil, fmt.Errorf("reaching %s within the process for %s: %w", role, robot, err)
			}
			conns = append(conns, c)
			// What reaches the role from outside the process — the deployer's okfs daemon, the SCM's —
			// proves the robot as before: a development server's names it instead.
			if *dev {
				cred = nil
			}
			return c, cred, nil
		}
		switch {
		case addr == "":
			return nil, cred, nil
		default:
			o = remote
		}
		if *dev {
			o.DevPrincipal, cred = cfg.RobotName(robot), nil
		} else {
			o.Credential = cred
		}
		c, err := dial.Dial(addr, o)
		if err != nil {
			return nil, nil, fmt.Errorf("dialing %s for %s: %w", addr, robot, err)
		}
		conns = append(conns, c)
		return c, cred, nil
	}
	// dialAs is as, for each of the roles a role calls: its connections, by role.
	dialAs := func(robot string, roles ...string) (map[string]grpc.ClientConnInterface, error) {
		addrs := map[string]string{"identity": cfg.Services.Identity, "scm": cfg.Services.SCM, "automation": cfg.Services.Automation, "deploy": cfg.Services.Deploy,
			"secrets": cfg.Services.Secrets}
		out := map[string]grpc.ClientConnInterface{}
		for _, r := range roles {
			c, _, err := as(robot, r, addrs[r])
			if err != nil {
				return nil, err
			}
			out[r] = c
		}
		return out, nil
	}
	var closers []func()
	defer func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}()
	if run["scm"] {
		c, err := dialAs(cfg.Roles.SCM, "identity", "secrets")
		if err != nil {
			return err
		}
		scm, err := scmrole.Build(ctx, cfg, scmrole.Options{Dev: *dev, Identity: c["identity"], Secrets: c["secrets"], Roots: idclient.LocalRoots(id.CA)})
		if err != nil {
			return fmt.Errorf("the SCM: %w", err)
		}
		closers = append(closers, scm.Close)
		roots = append(roots, scm.Root)
	}
	if run["automation"] {
		c, err := dialAs(cfg.Roles.Automation, "identity", "scm")
		if err != nil {
			return err
		}
		auto, err := autorole.Build(ctx, cfg, autorole.Options{Dev: *dev, Identity: c["identity"], SCM: c["scm"], Roots: idclient.LocalRoots(id.CA)})
		if err != nil {
			return fmt.Errorf("ok automation: %w", err)
		}
		closers = append(closers, auto.Close)
		roots = append(roots, auto.Root)
	}
	if run["deploy"] {
		r := cfg.Roles.Deploy
		c, err := dialAs(r, "identity", "automation", "secrets")
		if err != nil {
			return err
		}
		scmc, cred, err := as(r, "scm", cfg.Services.SCM)
		if err != nil {
			return err
		}
		scmAddr, scmTLS := cfg.Services.SCM, cfg.Services.TLS
		if run["scm"] {
			scmAddr, scmTLS = local, localTLS != nil
		}
		devPrincipal := ""
		if *dev {
			devPrincipal = cfg.RobotName(r)
		}
		parts, err := deployParts(ctx, cfg, scmc, scmAddr, scmTLS, cred, devPrincipal)
		if err != nil {
			return fmt.Errorf("the deployer: %w", err)
		}
		closers = append(closers, parts.close)
		dep, err := deployrole.Build(ctx, cfg, deployrole.Options{Dev: *dev, Color: deployColor(""), Checkout: parts.checkout, Source: parts.source,
			Bazel: parts.bazel, Env: parts.env, Identity: c["identity"], SCM: scmc, Automation: c["automation"], Secrets: c["secrets"],
			Roots: idclient.LocalRoots(id.CA)})
		if err != nil {
			return fmt.Errorf("the deployer: %w", err)
		}
		closers = append(closers, dep.Close)
		roots = append(roots, dep.Root)
	}
	if run["secrets"] {
		c, err := dialAs(cfg.Roles.Secrets, "identity", "scm", "automation")
		if err != nil {
			return err
		}
		sec, err := secretsrole.Build(ctx, cfg, secretsrole.Options{Dev: *dev, Identity: c["identity"], SCM: c["scm"], Automation: c["automation"],
			Roots: idclient.LocalRoots(id.CA)})
		if err != nil {
			return fmt.Errorf("ok secrets: %w", err)
		}
		closers = append(closers, sec.Close)
		roots = append(roots, sec.Root)
	}
	if run["control"] {
		c, err := dialAs(cfg.Roles.Control, "identity", "scm", "automation", "deploy")
		if err != nil {
			return err
		}
		ctl, err := ctlrole.Build(ctx, cfg, ctlrole.Options{Dev: *dev, Identity: c["identity"], SCM: c["scm"], Automation: c["automation"], Deploy: c["deploy"],
			Roots: idclient.LocalRoots(id.CA)})
		if err != nil {
			return fmt.Errorf("ok control: %w", err)
		}
		closers = append(closers, ctl.Close)
		roots = append(roots, ctl.Root)
	}

	opts := []serve.Option{serve.WithAuthn(id.Resolver), serve.WithListener(lis), serve.WithLocal(self), serve.WithMetricsAddr(cfg.MetricsListen), serve.WithProfiling(cfg.Profiling),
		serve.WithStopTimeout(workerStopTimeout)}
	if cfg.TLS != nil {
		tc, err := serve.LoadTLS(cfg.TLS.Cert, cfg.TLS.Key)
		if err != nil {
			return err
		}
		opts = append(opts, serve.WithTLS(tc))
	}
	var names []string
	for _, r := range serverRoles {
		if run[r] {
			names = append(names, r)
		}
	}
	slog.Info("ok serve: serving", "roles", strings.Join(names, ","), "listen", lis.Addr().String())
	return serve.Run(ctx, &together{deps: roots}, opts...)
}

// localAddress is where this process's roles reach its own port — loopback, at the port it
// listens on — and how: over TLS when it serves TLS, checked as its public address's certificate.
func localAddress(cfg *config.Config, at *net.TCPAddr) (string, *tls.Config, error) {
	addr := net.JoinHostPort("127.0.0.1", fmt.Sprint(at.Port))
	if at.IP != nil && !at.IP.IsUnspecified() {
		addr = at.String()
	}
	if cfg.TLS == nil {
		return addr, nil, nil
	}
	u, err := url.Parse(cfg.PublicURL)
	if err != nil {
		return "", nil, fmt.Errorf("public_url: %w", err)
	}
	tc := &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}
	if cfg.Services.CAFile != "" {
		pem, err := os.ReadFile(cfg.Services.CAFile)
		if err != nil {
			return "", nil, fmt.Errorf("services.ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return "", nil, errors.New("services.ca_file holds no certificate")
		}
		tc.RootCAs = pool
	}
	return addr, tc, nil
}

// together is the roles ok serve runs, as one graph.
type together struct{ deps []node.Node }

func (*together) Name() string                 { return "ok/serve" }
func (t *together) Dependencies() []node.Node  { return t.deps }
func (*together) Healthy(context.Context) bool { return true }
func (*together) Setup(context.Context) error  { return nil }
