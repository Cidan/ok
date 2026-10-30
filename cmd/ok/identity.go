package main

import (
	"context"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/Cidan/ok/internal/client/auth"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/role"
	idstore "github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/serve"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// identityServe runs the identity role until it is told to stop.
func identityServe(args []string) error {
	flags := flag.NewFlagSet("identity serve", flag.ContinueOnError)
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

func loopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return host == "localhost" || ip != nil && ip.IsLoopback()
}

// caInit makes an identity CA.
func caInit(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("identity ca init", flag.ContinueOnError)
	dir := flags.String("dir", "", "the directory to make the CA in: identity.ca_dir")
	name := flags.String("name", "ok", "the installation's name, in the CA's")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	if *dir == "" {
		return usageError("-dir is required")
	}
	if err := ca.Init(*dir, *name); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "made the identity CA in %s: keep tls.key secret, and back it up\n", *dir)
	return nil
}

// identityCertify certifies a key for one of the robots the configuration names, from identity's
// database and CA directly — an operator's command, for a role that runs where no issuer vouches
// for it, before anyone has signed in (docker compose): what ok admin robot certify does, without
// an administrator's credential. A credential in DIR that is the robot's and good for another hour
// is kept; otherwise a new key is certified there and the one it replaces revoked. Identity need
// not be running: the command makes its tables and the robot as identity does when it starts.
func identityCertify(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("identity certify", flag.ContinueOnError)
	path := flags.String("config", "ok.yaml", "the installation's configuration")
	dir := flags.String("dir", "", "where the robot keeps its key and certificate: its workload.credential_dir")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	if *dir == "" || flags.NArg() != 1 {
		return usageError("ok identity certify [-config ok.yaml] -dir DIR ROBOT")
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	names := cfg.Names()
	short := flags.Arg(0)
	name := names.Robot(short)
	named := false
	for _, r := range cfg.Roles.All() {
		named = named || names.Robot(r) == name
	}
	for _, r := range cfg.Identity.Robots {
		named = named || names.Robot(r.Name) == name
	}
	if !named {
		return fmt.Errorf("%s is not a robot the configuration names (roles, identity.robots)", short)
	}
	old, err := auth.Load(*dir)
	if err == nil && old.Principal() == name && time.Until(old.NotAfter()) > time.Hour {
		fmt.Fprintf(stdout, "kept %s's credential in %s, good until %s\n", name, *dir, old.NotAfter().UTC().Format(time.RFC3339))
		return nil
	}

	ctx := context.Background()
	b, _, done, err := role.OpenBackend(ctx, cfg, 2)
	if err != nil {
		return err
	}
	defer done()
	st := idstore.New(b, names)
	authority, err := ca.Load(cfg.Identity.CADir)
	if err != nil {
		return fmt.Errorf("identity.ca_dir: %w (make one with: ok identity ca init)", err)
	}
	p, err := st.EnsureRobot(ctx, name)
	if err != nil {
		return err
	}
	if !p.Active() {
		return fmt.Errorf("%s is not an active robot", name)
	}
	key, err := credential.NewKey()
	if err != nil {
		return err
	}
	spki, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return err
	}
	c, err := st.EnrollKey(ctx, p.ID, short, spki)
	if err != nil {
		return err
	}
	chain, notAfter, err := authority.Issue(&key.PublicKey, name, c.ID)
	if err != nil {
		return err
	}
	if err := st.Issued(ctx, c.ID, notAfter); err != nil {
		return err
	}
	if err := auth.Save(*dir, auth.Server{URL: cfg.PublicURL, Addr: cfg.Services.Identity, TLS: cfg.Services.TLS}, chain[0], key); err != nil {
		return err
	}
	if old != nil && old.Principal() == name {
		if id := old.ID(); id != "" && id != c.ID {
			if err := st.Revoke(ctx, id); err != nil && !errors.Is(err, idstore.ErrNotFound) {
				return fmt.Errorf("revoking the credential it replaces, %s: %w", id, err)
			}
		}
	}
	fmt.Fprintf(stdout, "certified %s's key in %s, good until %s; it renews itself\n", name, *dir, notAfter.UTC().Format(time.RFC3339))
	return nil
}

// authLogin signs this machine in.
func authLogin(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("auth login", flag.ContinueOnError)
	server := flags.String("server", os.Getenv("OK_SERVER"), "ok's address: https://ok.example.com")
	label := flags.String("label", defaultLabel(), "what this machine is called on your account page")
	noBrowser := flags.Bool("no-browser", false, "do not open a browser")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	if *server == "" {
		return usageError("-server is required (or OK_SERVER)")
	}
	srv, err := auth.ParseServer(*server)
	if err != nil {
		return err
	}
	dir, err := auth.Dir()
	if err != nil {
		return err
	}
	var open func(string) error
	if !*noBrowser {
		open = openBrowser
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	who, err := auth.Login(ctx, srv, dir, *label, stdout, open)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "signed in as %s\n", who)
	return nil
}

func defaultLabel() string {
	host, _ := os.Hostname()
	if u := os.Getenv("USER"); u != "" && host != "" {
		return u + "@" + host
	}
	if host != "" {
		return host
	}
	return "a command line"
}

func openBrowser(url string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("open", url).Start()
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	}
	if os.Getenv("DISPLAY") == "" && os.Getenv("WAYLAND_DISPLAY") == "" {
		return errors.New("no display")
	}
	return exec.Command("xdg-open", url).Start()
}

func authStatus(stdout io.Writer) error {
	dir, err := auth.Dir()
	if err != nil {
		return err
	}
	c, err := auth.Load(dir)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "signed in to %s as %s; the certificate renews before %s\n", c.Server().URL, c.Principal(), c.NotAfter().Local().Format("Jan 2 15:04 MST"))
	return nil
}

func authLogout(stdout io.Writer) error {
	dir, err := auth.Dir()
	if err != nil {
		return err
	}
	if err := auth.Logout(context.Background(), dir); err != nil {
		return err
	}
	fmt.Fprintln(stdout, "signed out")
	return nil
}

// adminClient dials ok as the signed-in machine's principal, and says where it is.
func adminClient() (identitypb.AdminClient, auth.Server, func(), error) {
	dir, err := auth.Dir()
	if err != nil {
		return nil, auth.Server{}, nil, err
	}
	c, err := auth.Load(dir)
	if err != nil {
		return nil, auth.Server{}, nil, err
	}
	conn, err := c.Server().Dial(dial.Options{Credential: c})
	if err != nil {
		return nil, auth.Server{}, nil, err
	}
	return identitypb.NewAdminClient(conn), c.Server(), func() { conn.Close() }, nil
}

const adminUsage = `ok admin — administering ok identity, as the signed-in administrator

  ok admin sync                              run the sync from the system of record now
  ok admin status                            how the sync went, and who waits
  ok admin name [-group] EXTERNAL_ID NAME    set a waiting person's handle, or a group's name
  ok admin robots                            list robots and their bindings
  ok admin robot create NAME                 make a robot: NAME.robot.<domain>
  ok admin robot delete NAME                 delete a robot; its name is retired
  ok admin robot bind NAME ISSUER SUBJECT    let a robot sign in with an issuer's tokens
  ok admin robot unbind NAME ISSUER SUBJECT
  ok admin robot certify NAME -dir DIR       certify a key for a robot no issuer vouches for, and
                                             write its credential to DIR (workload.credential_dir)
  ok admin credentials PRINCIPAL             list a principal's credentials
  ok admin revoke CREDENTIAL                 revoke a credential
  ok admin move REPOSITORY CELL              move a repository's fork network to another cell
  ok admin moves                             list the moves between cells, and where each is
  ok admin cancel-move NETWORK               abandon a move the directory has not finished naming
  ok admin quota ACCOUNT [-concurrent N] [-minutes M] [-reset]
                                             show an account's quota of CI, or set it: concurrent
                                             runs, run minutes a month (0: unlimited)
`

func admin(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return usageError("ok admin needs a command\n" + adminUsage)
	}
	client, srv, done, err := adminClient()
	if err != nil {
		return err
	}
	defer done()
	ctx := context.Background()
	switch args[0] {
	case "sync":
		resp, err := client.Sync(ctx, &identitypb.SyncRequest{})
		if err != nil {
			return err
		}
		for _, c := range resp.GetChanges() {
			fmt.Fprintln(stdout, c)
		}
		for _, w := range resp.GetWaiting() {
			fmt.Fprintf(stdout, "waiting: %s: %s\n", w.GetExternalId(), w.GetReason())
		}
		if len(resp.GetChanges()) == 0 {
			fmt.Fprintln(stdout, "nothing changed")
		}
		return nil
	case "status":
		st, err := client.GetSyncStatus(ctx, &identitypb.GetSyncStatusRequest{})
		if err != nil {
			return err
		}
		last := "never"
		if st.GetLastRun() != nil {
			last = st.GetLastRun().AsTime().Local().Format("Jan 2 15:04:05 MST")
		}
		fmt.Fprintf(stdout, "system of record: %s\nlast sync: %s\n", or(st.GetSource(), "none"), last)
		if st.GetError() != "" {
			fmt.Fprintf(stdout, "it failed: %s\n", st.GetError())
		}
		for _, w := range st.GetWaiting() {
			fmt.Fprintf(stdout, "waiting: %s: %s\n", w.GetExternalId(), w.GetReason())
		}
		return nil
	case "name":
		flags := flag.NewFlagSet("admin name", flag.ContinueOnError)
		group := flags.Bool("group", false, "the external ID is a group's")
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 2 {
			return usageError("ok admin name [-group] EXTERNAL_ID NAME")
		}
		_, err := client.SetName(ctx, &identitypb.SetNameRequest{ExternalId: flags.Arg(0), Group: *group, Name: flags.Arg(1)})
		return err
	case "robots":
		resp, err := client.ListRobots(ctx, &identitypb.ListRobotsRequest{})
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		for _, r := range resp.GetRobots() {
			var bs []string
			for _, b := range r.GetBindings() {
				bs = append(bs, b.GetIssuer()+" "+b.GetSubject())
			}
			fmt.Fprintf(tw, "%s\t%s\n", r.GetPrincipal().GetName(), strings.Join(bs, "; "))
		}
		return tw.Flush()
	case "robot":
		return adminRobot(ctx, client, srv, args[1:], stdout)
	case "credentials":
		if len(args) != 2 {
			return usageError("ok admin credentials PRINCIPAL")
		}
		resp, err := client.ListCredentials(ctx, &identitypb.AdminListCredentialsRequest{Principal: args[1]})
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		for _, c := range resp.GetCredentials() {
			state := "active"
			if c.GetRevoked() != nil {
				state = "revoked"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\n", c.GetId(), c.GetLabel(), state)
		}
		return tw.Flush()
	case "revoke":
		if len(args) != 2 {
			return usageError("ok admin revoke CREDENTIAL")
		}
		_, err := client.RevokeCredential(ctx, &identitypb.RevokeCredentialRequest{Id: args[1]})
		return err
	case "move":
		if len(args) != 3 {
			return usageError("ok admin move REPOSITORY CELL")
		}
		m, err := client.MoveNetwork(ctx, &identitypb.MoveNetworkRequest{Repository: args[1], Cell: args[2]})
		if err != nil {
			return err
		}
		printMove(stdout, m)
		return nil
	case "moves":
		resp, err := client.ListMoves(ctx, &identitypb.ListMovesRequest{})
		if err != nil {
			return err
		}
		for _, m := range resp.GetMoves() {
			printMove(stdout, m)
		}
		return nil
	case "quota":
		return adminQuota(ctx, client, args[1:], stdout)
	case "cancel-move":
		if len(args) != 2 {
			return usageError("ok admin cancel-move NETWORK")
		}
		network, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return usageError("ok admin cancel-move NETWORK: a network's ID, as ok admin moves shows it")
		}
		m, err := client.CancelMove(ctx, &identitypb.CancelMoveRequest{NetworkId: network})
		if err != nil {
			return err
		}
		printMove(stdout, m)
		return nil
	}
	return usageError(fmt.Sprintf("unknown command ok admin %s\n%s", args[0], adminUsage))
}

// printMove shows a move: its network and repositories, where from and to, where it is, and why
// its last step failed.
func printMove(w io.Writer, m *identitypb.Move) {
	fmt.Fprintf(w, "network %d  %s -> %s  %s  asked by %s %s\n", m.GetNetworkId(), m.GetSource(), m.GetDestination(), m.GetPhase(),
		m.GetAskedBy(), m.GetAskedAt().AsTime().Format(time.RFC3339))
	fmt.Fprintf(w, "  %s\n", strings.Join(m.GetRepositories(), ", "))
	if m.GetError() != "" {
		fmt.Fprintf(w, "  last step failed: %s\n", m.GetError())
	}
}

func adminRobot(ctx context.Context, client identitypb.AdminClient, srv auth.Server, args []string, stdout io.Writer) error {
	if len(args) < 2 {
		return usageError(adminUsage)
	}
	switch args[0] {
	case "create":
		resp, err := client.CreateRobot(ctx, &identitypb.CreateRobotRequest{Name: args[1]})
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, resp.GetPrincipal().GetName())
		return nil
	case "delete":
		_, err := client.DeleteRobot(ctx, &identitypb.DeleteRobotRequest{Name: args[1]})
		return err
	case "certify":
		flags := flag.NewFlagSet("admin robot certify", flag.ContinueOnError)
		dir := flags.String("dir", "", "where to write the robot's key and certificate")
		label := flags.String("label", "", "what the credential is called (default: the robot's name)")
		if err := flags.Parse(args[2:]); err != nil || *dir == "" {
			return usageError("ok admin robot certify NAME -dir DIR [-label L]")
		}
		if *label == "" {
			*label = args[1]
		}
		key, err := credential.NewKey()
		if err != nil {
			return err
		}
		spki, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
		if err != nil {
			return err
		}
		resp, err := client.CertifyRobot(ctx, &identitypb.CertifyRobotRequest{Robot: args[1], PublicKey: spki, Label: *label})
		if err != nil {
			return err
		}
		if err := auth.Save(*dir, srv, resp.GetCertificate().GetChain()[0], key); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "certified %s's key as credential %s in %s; keep key.pem secret\n", args[1], resp.GetCertificate().GetCredential(), *dir)
		return nil
	case "bind", "unbind":
		if len(args) != 4 {
			return usageError("ok admin robot " + args[0] + " NAME ISSUER SUBJECT")
		}
		b := &identitypb.Binding{Issuer: args[2], Subject: args[3]}
		var err error
		if args[0] == "bind" {
			_, err = client.Bind(ctx, &identitypb.BindRequest{Robot: args[1], Binding: b})
		} else {
			_, err = client.Unbind(ctx, &identitypb.UnbindRequest{Robot: args[1], Binding: b})
		}
		return err
	}
	return usageError(adminUsage)
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// adminQuota shows an account's quota of CI, or sets it.
func adminQuota(ctx context.Context, client identitypb.AdminClient, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("admin quota", flag.ContinueOnError)
	concurrent := flags.Int("concurrent", -1, "concurrent runs (0: unlimited)")
	minutes := flags.Int64("minutes", -1, "run minutes in a calendar month (0: unlimited)")
	reset := flags.Bool("reset", false, "give the account the default quota again")
	if len(args) == 0 {
		return usageError("ok admin quota ACCOUNT [-concurrent N] [-minutes M] [-reset]")
	}
	account := args[0]
	if err := flags.Parse(args[1:]); err != nil {
		return flagError(err)
	}
	var q *identitypb.Quota
	var err error
	switch {
	case *reset:
		q, err = client.SetQuota(ctx, &identitypb.SetQuotaRequest{Account: account, Reset_: true})
	case *concurrent >= 0 || *minutes >= 0:
		had, err := client.GetQuota(ctx, &identitypb.GetQuotaRequest{Account: account})
		if err != nil {
			return err
		}
		req := &identitypb.SetQuotaRequest{Account: account, Concurrent: had.GetConcurrent(), Minutes: had.GetMinutes()}
		if *concurrent >= 0 {
			req.Concurrent = int32(*concurrent)
		}
		if *minutes >= 0 {
			req.Minutes = *minutes
		}
		q, err = client.SetQuota(ctx, req)
		if err != nil {
			return err
		}
	default:
		q, err = client.GetQuota(ctx, &identitypb.GetQuotaRequest{Account: account})
	}
	if err != nil {
		return err
	}
	limit := func(n int64, unit string) string {
		if n == 0 {
			return "unlimited " + unit
		}
		return fmt.Sprintf("%d %s", n, unit)
	}
	kind := ""
	if q.GetDefault() {
		kind = " (the default)"
	}
	fmt.Fprintf(stdout, "%s: %s, %s a month%s\n", account, limit(int64(q.GetConcurrent()), "concurrent runs"),
		limit(q.GetMinutes(), "minutes"), kind)
	return nil
}
