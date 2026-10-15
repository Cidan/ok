package bazel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Driver runs Bazel in checkouts: a worker's slot's, or the deployer's.
type Driver struct {
	// Bazel is bazel's path, or bazelisk's, which runs the version a checkout's .bazelversion
	// names.
	Bazel string
	// Wrapper is a command prefix every Bazel process the driver starts itself runs under (a
	// worker's, hiding its secrets); the commands it returns carry none — whoever runs them adds it.
	Wrapper []string
	// OutputBase is where Bazel keeps its outputs and its server for these checkouts: outside them,
	// since a checkout is a read-only presentation of a commit. Empty leaves Bazel's own.
	OutputBase string
	// StartupFlags go before every command; Flags after it — the installation's (a remote cache)
	// and the repository's (Settings.Bazel.Flags).
	StartupFlags []string
	Flags        []string
	// Env is Bazel's whole environment.
	Env []string
	// Stderr receives what Bazel says as it works.
	Stderr io.Writer
}

// Graph queries a checkout's build graph (QueryArgs).
func (d *Driver) Graph(ctx context.Context, dir string) (*Graph, error) {
	return d.GraphTo(ctx, dir, io.Discard)
}

// GraphTo queries a checkout's build graph, and writes the query's answer to w as well, for
// ParseGraph to read again.
func (d *Driver) GraphTo(ctx context.Context, dir string, w io.Writer) (*Graph, error) {
	var stdout, stderr bytes.Buffer
	argv := d.wrapped(append([]string{d.Bazel}, append(d.startup(), append(append([]string(nil), QueryArgs...), lockfileMode(dir))...)...))
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Env = d.Env
	cmd.Stdout = &stdout
	cmd.Stderr = io.MultiWriter(&stderr, d.stderr())
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("bazel query of %s: %w%s", dir, err, tail(stderr.String()))
	}
	if _, err := w.Write(stdout.Bytes()); err != nil {
		return nil, err
	}
	return ParseGraph(&stdout)
}

// Build is the command that builds targets, every one of them however many fail.
func (d *Driver) Build(dir string, targets []string) []string {
	return d.command(dir, "build", []string{"--keep_going"}, targets)
}

// Test is the command that runs a test target, its arguments given to the test, and a failing
// test's log printed with Bazel's: a test.log on the worker is one no one can read.
func (d *Driver) Test(dir, target string, args []string) []string {
	flags := []string{"--test_output=errors"}
	for _, a := range args {
		flags = append(flags, "--test_arg="+a)
	}
	return d.command(dir, "test", flags, []string{target})
}

// Run is the command that builds a binary target and runs it with args, under sandbox — the
// command prefix every process the repository's code starts runs within (Bazel's --run_under,
// split as a shell splits words).
func (d *Driver) Run(dir, target string, args, sandbox []string) []string {
	argv := d.command(dir, "run", runUnder(sandbox), []string{target})
	return append(argv, args...)
}

// Lint is the command that runs lint aspects (rules_lint's, //pkg:file.bzl%name) over targets,
// every one however many fail, a finding failing the build.
func (d *Driver) Lint(dir string, aspects, targets []string) []string {
	return d.command(dir, "build", []string{"--keep_going", "--aspects=" + strings.Join(aspects, ","),
		"--@aspect_rules_lint//lint:fail_on_violation", "--output_groups=rules_lint_human"}, targets)
}

// FormatCheck is the command that checks files are formatted, with rules_lint's format_multirun
// target format (its check mode, format.check), under sandbox.
func (d *Driver) FormatCheck(dir, format string, files, sandbox []string) []string {
	return d.Run(dir, format+".check", files, sandbox)
}

// Push is the command that pushes a rules_oci oci_push target's image, tagged with tag.
func (d *Driver) Push(dir, target, tag string) []string {
	return d.Run(dir, target, []string{"--tag", tag}, nil)
}

// OutputFiles is what a target builds (cquery --output=files), as absolute paths.
func (d *Driver) OutputFiles(ctx context.Context, dir, target string) ([]string, error) {
	root, err := d.output(ctx, dir, append(d.startup(), "info", "execution_root"))
	if err != nil {
		return nil, err
	}
	out, err := d.output(ctx, dir, d.command(dir, "cquery", []string{"--output=files"}, []string{target})[1:])
	if err != nil {
		return nil, err
	}
	execRoot := strings.TrimSpace(string(root))
	var files []string
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			if !filepath.IsAbs(l) {
				l = filepath.Join(execRoot, l)
			}
			files = append(files, l)
		}
	}
	return files, nil
}

// output runs Bazel with args in dir and returns what it printed.
func (d *Driver) output(ctx context.Context, dir string, args []string) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	argv := d.wrapped(append([]string{d.Bazel}, args...))
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Env = d.Env
	cmd.Stdout = &stdout
	cmd.Stderr = io.MultiWriter(&stderr, d.stderr())
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("bazel %s: %w%s", strings.Join(args, " "), err, tail(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// wrapped is argv under the driver's Wrapper.
func (d *Driver) wrapped(argv []string) []string {
	return append(append([]string(nil), d.Wrapper...), argv...)
}

// runUnder is the --run_under flag of a sandbox prefix; none without one.
func runUnder(sandbox []string) []string {
	if len(sandbox) == 0 {
		return nil
	}
	return []string{"--run_under=" + strings.Join(sandbox, " ")}
}

// command is a Bazel command over targets in a checkout: no convenience symlinks in it (it is
// read-only), plain output for a log, no network for actions and tests, the lockfile honored, the
// installation's and repository's flags, then the command's own.
func (d *Driver) command(dir, verb string, flags, targets []string) []string {
	argv := append([]string{d.Bazel}, d.startup()...)
	argv = append(argv, verb, "--symlink_prefix=/", "--color=no", "--curses=no",
		"--sandbox_default_allow_network=false", lockfileMode(dir))
	argv = append(argv, d.Flags...)
	argv = append(argv, flags...)
	argv = append(argv, "--")
	return append(argv, targets...)
}

func (d *Driver) startup() []string {
	var out []string
	if d.OutputBase != "" {
		out = append(out, "--output_base="+d.OutputBase)
	}
	return append(out, d.StartupFlags...)
}

func (d *Driver) stderr() io.Writer {
	if d.Stderr == nil {
		return io.Discard
	}
	return d.Stderr
}

// lockfileMode holds a checkout to its MODULE.bazel.lock when it has one — a lockfile out of date
// is an error, never rewritten in a read-only checkout — and resolves without one otherwise.
func lockfileMode(dir string) string {
	if _, err := os.Stat(filepath.Join(dir, "MODULE.bazel.lock")); err == nil {
		return "--lockfile_mode=error"
	}
	return "--lockfile_mode=off"
}

// tail is the end of what a command said, for an error.
func tail(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	lines := strings.Split(s, "\n")
	if len(lines) > 20 {
		lines = lines[len(lines)-20:]
	}
	return ":\n" + strings.Join(lines, "\n")
}
