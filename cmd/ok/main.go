// Command ok is ok's one binary: its roles' servers (ok identity serve, ok scm serve, ok control serve, ok automation serve, ok worker run, ok deploy serve), signing a machine in (ok
// auth), administering (ok admin), and checks of what a repository declares — its groups-and-ACLs
// tree, OWNERS files, workflow files — and what a change affects in a Bazel workspace.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Cidan/ok/internal/buildsys/bazel"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/owners"
	"github.com/Cidan/ok/internal/workflow"
)

const usage = `ok — source control and code review, with automation on Bazel

Usage:
  ok version                               the release this ok is
  ok serve [-config ok.yaml] [-roles all|identity,scm,automation,deploy,control] [-dev]
                                           run several server roles in one process, on one port
  ok identity serve [-config ok.yaml] [-dev]
                                           run the identity role
  ok identity ca init -dir DIR [-name N]   make the identity CA
  ok identity certify [-config ok.yaml] -dir DIR ROBOT
                                           certify a key for a robot the configuration names, from
                                           identity's database and CA: a role where nothing vouches for it
  ok scm serve [-config ok.yaml] [-dev]    run the SCM role
  ok control serve [-config ok.yaml] [-dev]  run ok control: review and the web UI
  ok automation serve [-config ok.yaml] [-dev]
                                           run ok automation: pre, post, and the landing queue
  ok worker run [-config ok.yaml]          run an automation worker: Bazel on each run's commit
  ok deploy serve [-config ok.yaml] [-color blue|green]
                                           run the deployer: migrations, rolls, and the tree
  ok scm fsck [-repair] REPOSITORY         verify a repository against the SCM's invariants
  ok scm gc [-dry-run] REPOSITORY          run a garbage-collection pass over a repository now
  ok auth login -server URL                sign this machine in, in a browser
  ok auth status                           who this machine is signed in as
  ok auth logout                           sign this machine out, revoking its credential
  ok admin COMMAND                         administer the directory (ok admin help)
  ok config check [-config ok.yaml]        check a configuration as the roles read it
  ok identity check -domain D DIR          check a groups-and-ACLs tree, and show each set's owner
  ok owners check -domain D [-identity DIR] ROOT
                                           check every OWNERS file in a repository: syntax, imports,
                                           and that each set's own people could approve its changes
  ok workflow check ROOT                   check every workflow file and the repository's settings
  ok directory plan -domain D -static FILE what a sync from a static system of record would do to an
                                           empty directory
  ok affected -base DIR -change DIR [-bazel PATH] [-json] < touched
                                           what a change affects in a Bazel workspace: the paths it
                                           touches on standard input, one a line
`

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		// A command's -h printed its flags already.
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "ok:", err)
		var u usageError
		if errors.As(err, &u) {
			fmt.Fprint(os.Stderr, usage)
			os.Exit(2)
		}
		os.Exit(1)
	}
}

// version is ok's release, stamped at build (-ldflags "-X main.version=…").
var version = "dev"

type usageError string

func (e usageError) Error() string { return string(e) }

// flagError is a command's flags' parse error: help asked for (-h), which the flag set has
// printed, as itself; anything else, a usage error.
func flagError(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return err
	}
	return usageError(err.Error())
}

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	if len(args) == 1 && args[0] == "version" {
		fmt.Fprintln(stdout, "ok", version)
		return nil
	}
	if len(args) >= 1 && args[0] == "serve" {
		return serveRoles(args[1:])
	}
	if len(args) >= 1 && args[0] == "admin" {
		if len(args) == 1 || args[1] == "help" {
			fmt.Fprint(stdout, adminUsage)
			return nil
		}
		return admin(args[1:], stdout)
	}
	help := func(a string) bool { return a == "help" || a == "-h" || a == "--help" }
	if len(args) < 2 {
		if len(args) == 1 && help(args[0]) {
			fmt.Fprint(stdout, usage)
			return nil
		}
		return usageError("a command is two words, like ok identity check")
	}
	// A group's help (ok scm --help) is every command's.
	if len(args) == 2 && help(args[1]) {
		fmt.Fprint(stdout, usage)
		return nil
	}
	if len(args) >= 3 && args[0] == "identity" && args[1] == "ca" && args[2] == "init" {
		return caInit(args[3:], stdout)
	}
	switch args[0] + " " + args[1] {
	case "identity serve":
		return identityServe(args[2:])
	case "scm serve":
		return scmServe(args[2:])
	case "control serve":
		return controlServe(args[2:])
	case "automation serve":
		return automationServe(args[2:])
	case "worker run":
		return workerRun(args[2:])
	case "deploy serve":
		return deployServe(args[2:])
	case "config check":
		return configCheck(args[2:], stdout)
	case "scm fsck":
		return scmFsck(args[2:], stdout)
	case "scm gc":
		return scmGC(args[2:], stdout)
	case "auth login":
		return authLogin(args[2:], stdout)
	case "auth status":
		return authStatus(stdout)
	case "auth logout":
		return authLogout(stdout)
	case "identity certify":
		return identityCertify(args[2:], stdout)
	case "identity check":
		return identityCheck(args[2:], stdout)
	case "owners check":
		return ownersCheck(args[2:], stdout)
	case "workflow check":
		return workflowCheck(args[2:], stdout)
	case "directory plan":
		return directoryPlan(args[2:], stdout)
	}
	if args[0] == "affected" {
		return affected(args[1:], stdin, stdout)
	}
	return usageError(fmt.Sprintf("unknown command %q", strings.Join(args, " ")))
}

// domainFlag adds -domain to flags and returns the naming it gives.
func domainFlag(flags *flag.FlagSet) func() (identity.Names, error) {
	domain := flags.String("domain", "", "the installation's domain, which every name is under")
	return func() (identity.Names, error) {
		if *domain == "" {
			return identity.Names{}, usageError("-domain is required")
		}
		return identity.NewNames(*domain)
	}
}

func identityCheck(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("identity check", flag.ContinueOnError)
	names := domainFlag(flags)
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	if flags.NArg() != 1 {
		return usageError("identity check takes the tree's folder")
	}
	n, err := names()
	if err != nil {
		return err
	}
	tree, err := identity.Load(n, os.DirFS(flags.Arg(0)))
	if err != nil {
		return err
	}
	for _, s := range tree.Sets() {
		name := n.OfSet(s)
		owner, _ := tree.Owner(name)
		fmt.Fprintf(stdout, "%s, owned by %s: %d members, %d people\n", name, owner, len(s.Members), len(tree.Expand(name)))
	}
	return nil
}

func ownersCheck(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("owners check", flag.ContinueOnError)
	names := domainFlag(flags)
	tree := flags.String("identity", "", "the groups-and-ACLs tree's folder in the repository, for acl: owners (default: the repository's settings')")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	if flags.NArg() != 1 {
		return usageError("owners check takes the repository's root")
	}
	n, err := names()
	if err != nil {
		return err
	}
	root := flags.Arg(0)
	rel := *tree
	if rel == "" {
		s, err := settings(root)
		if err != nil {
			return err
		}
		rel = s.Identity
	}
	roster, err := owners.LoadTreeRoster(os.DirFS(root), n, rel)
	if err != nil {
		return fmt.Errorf("the groups-and-ACLs tree: %w", err)
	}
	problems, configs, err := owners.CheckRepository(os.DirFS(root), roster)
	if err != nil {
		return err
	}
	for _, p := range problems {
		fmt.Fprintln(stdout, p.Error())
	}
	if len(problems) > 0 {
		return fmt.Errorf("%d problems in %d OWNERS files", len(problems), configs)
	}
	fmt.Fprintf(stdout, "%d OWNERS files: every set can be approved by its own people\n", configs)
	return nil
}

// ownersOf is every owner an OWNERS file names, its per-file rules' too.
func workflowCheck(args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return usageError("workflow check takes the repository's root")
	}
	root := args[0]
	s, err := settings(root)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "the repository's build: %s\n", s.Build)
	src := dirSource(root)
	var problems []workflow.Error
	files := 0
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if !workflow.IsWorkflowFile(rel) {
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		files++
		problems = append(problems, workflow.Validate(rel, data, src)...)
		return nil
	})
	if err != nil {
		return err
	}
	for _, p := range problems {
		fmt.Fprintln(stdout, p.Error())
	}
	if len(problems) > 0 {
		return fmt.Errorf("%d problems in %d workflow files", len(problems), files)
	}
	fmt.Fprintf(stdout, "%d workflow files, each valid\n", files)
	return nil
}

func directoryPlan(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("directory plan", flag.ContinueOnError)
	names := domainFlag(flags)
	static := flags.String("static", "", "the static system of record's file")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	if *static == "" {
		return usageError("-static is required")
	}
	n, err := names()
	if err != nil {
		return err
	}
	snap, err := directory.Static{Path: *static}.Snapshot(context.Background())
	if err != nil {
		return err
	}
	plan, err := directory.Reconcile(n, directory.NewState(), snap, directory.Options{})
	if err != nil {
		return err
	}
	for _, c := range plan.Humans {
		fmt.Fprintln(stdout, c)
	}
	for _, c := range plan.Groups {
		fmt.Fprintln(stdout, c)
	}
	for _, w := range plan.Waiting {
		fmt.Fprintln(stdout, w)
	}
	for _, note := range plan.Notes {
		fmt.Fprintln(stdout, "note:", note)
	}
	return nil
}

func affected(args []string, stdin io.Reader, stdout io.Writer) error {
	flags := flag.NewFlagSet("affected", flag.ContinueOnError)
	base := flags.String("base", "", "the checkout of the commit the change changes")
	change := flags.String("change", "", "the checkout of the change")
	bin := flags.String("bazel", "bazel", "bazel's path, or bazelisk's")
	asJSON := flags.Bool("json", false, "answer in JSON")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	if *base == "" || *change == "" {
		return usageError("-base and -change are required")
	}
	var touched []string
	sc := bufio.NewScanner(stdin)
	for sc.Scan() {
		if p := strings.TrimSpace(sc.Text()); p != "" {
			touched = append(touched, p)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	ctx := context.Background()
	d := &bazel.Driver{Bazel: *bin, Env: os.Environ(), Stderr: os.Stderr}
	var commits [2]bazel.Commit
	for i, dir := range []string{*base, *change} {
		g, err := d.Graph(ctx, dir)
		if err != nil {
			return err
		}
		commits[i] = bazel.Commit{Files: os.DirFS(dir), Graph: g}
	}
	a, err := bazel.Affected(commits[0], commits[1], touched)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(a)
	}
	if a.Global != "" {
		fmt.Fprintf(stdout, "every target: the change touches %s\n", a.Global)
	}
	for _, t := range a.Affected {
		var chain []string
		for _, s := range t.Because {
			switch {
			case s.File != "":
				chain = append(chain, s.Label+", which holds "+s.File)
			case s.Via != "":
				chain = append(chain, s.Label+" —"+s.Via+"→")
			default:
				chain = append(chain, s.Label)
			}
		}
		fmt.Fprintf(stdout, "%s (%s): %s\n", t.Label, t.Kind, strings.Join(chain, " "))
	}
	fmt.Fprintf(stdout, "%d affected, %d tests, %d upstream\n", len(a.Affected), len(a.Tests), len(a.Upstream))
	return nil
}

// settings reads a repository's settings from its checkout.
func settings(root string) (*workflow.Settings, error) {
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(workflow.SettingsFile)))
	if errors.Is(err, fs.ErrNotExist) {
		return workflow.ParseSettings(nil, false)
	}
	if err != nil {
		return nil, err
	}
	return workflow.ParseSettings(data, true)
}

// dirSource reads a repository's files from its checkout, for OWNERS and workflow files' imports.
type dirSource string

func (d dirSource) Read(p string) ([]byte, bool, error) {
	data, err := os.ReadFile(filepath.Join(string(d), filepath.FromSlash(p)))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	return data, err == nil, err
}

// configCheck reads a configuration as every role does, refusing what they would, and says what it
// names.
func configCheck(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("config check", flag.ContinueOnError)
	path := flags.String("config", "ok.yaml", "the installation's configuration")
	if err := flags.Parse(args); err != nil {
		return flagError(err)
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%s: %s, at %s\n", *path, cfg.Domain, cfg.PublicURL)
	var roles []string
	for role, robot := range cfg.Roles.All() {
		roles = append(roles, role+" as "+cfg.RobotName(robot))
	}
	slices.Sort(roles)
	fmt.Fprintf(stdout, "roles: %s\n", strings.Join(roles, ", "))
	return nil
}
