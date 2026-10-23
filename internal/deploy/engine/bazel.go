package engine

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Cidan/ok/internal/buildsys"
	"github.com/Cidan/ok/internal/buildsys/bazel"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/deploy/plan"
	"github.com/Cidan/ok/internal/proc"
	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/migrate"
)

// Bazel is Build over Bazel, in the deployer's one checkout: the graphs it queries kept by commit,
// so the head a plan read is the next plan's base without a second query.
type Bazel struct {
	// Path is bazel's, or bazelisk's; Flags the installation's, before a repository's own; Env
	// Bazel's whole environment, which the migrations it runs get too — where each finds its
	// database.
	Path       string
	Flags      []string
	Env        []string
	OutputBase string
	Graphs     bazel.Graphs
	Checkout   checkout.Checkout
	Source     checkout.Source
	Log        *slog.Logger
}

// driver is Bazel for the repository whose settings the checkout holds now.
func (b *Bazel) driver() (*bazel.Driver, error) {
	settings, err := ReadSettings(b.Checkout.Dir())
	if err != nil {
		return nil, err
	}
	return &bazel.Driver{Bazel: b.Path, OutputBase: b.OutputBase, Flags: append(append([]string(nil), b.Flags...), settings.Bazel.Flags...), Env: b.Env}, nil
}

// graph is the build graph at a commit of a repository, which the checkout holds when it must be
// queried.
func (b *Bazel) graph(ctx context.Context, repo int64, commit []byte) (*bazel.Graph, error) {
	key := strconv.FormatInt(repo, 10) + "-" + hex.EncodeToString(commit)
	if g, ok := b.Graphs.Get(key); ok {
		return g, nil
	}
	d, err := b.driver()
	if err != nil {
		return nil, err
	}
	g, keepErr, err := b.Graphs.Query(ctx, d, b.Checkout.Dir(), key)
	if keepErr != nil && b.Log != nil {
		b.Log.Warn("deploy: keeping a build graph", "err", keepErr)
	}
	return g, err
}

func (b *Bazel) Affected(ctx context.Context, repo int64, from, to []byte, files []string) (*plan.Affected, error) {
	change, err := b.graph(ctx, repo, to)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		return &plan.Affected{Problems: []string{err.Error()}}, nil
	}
	base, err := b.baseGraph(ctx, repo, from, to)
	if err != nil {
		return nil, err
	}
	baseFiles, err := checkout.FilesAt(ctx, b.Source, repo, from, files)
	if err != nil {
		return nil, err
	}
	return bazel.Affected(bazel.Commit{Files: baseFiles, Graph: base}, bazel.Commit{Files: os.DirFS(b.Checkout.Dir()), Graph: change}, files)
}

// baseGraph is the graph at from: kept, or queried with the checkout moved there and back to to.
// A base whose graph does not load is the empty graph: everything at to is affected.
func (b *Bazel) baseGraph(ctx context.Context, repo int64, from, to []byte) (*bazel.Graph, error) {
	if g, ok := b.Graphs.Get(strconv.FormatInt(repo, 10) + "-" + hex.EncodeToString(from)); ok {
		return g, nil
	}
	if _, err := b.Checkout.Sync(ctx, repo, from, nil); err != nil {
		return nil, err
	}
	g, qerr := b.graph(ctx, repo, from)
	if _, err := b.Checkout.Sync(ctx, repo, to, nil); err != nil {
		return nil, err
	}
	if qerr != nil {
		if ctx.Err() != nil {
			return nil, qerr
		}
		if b.Log != nil {
			b.Log.Info("deploy: the previous plan's build graph does not load: everything is affected", "err", qerr)
		}
		return bazel.Empty(), nil
	}
	return g, nil
}

func (b *Bazel) Describe(ctx context.Context, repo int64, commit []byte, targets []string) (map[string]buildsys.Description, error) {
	g, err := b.graph(ctx, repo, commit)
	if err != nil {
		return nil, err
	}
	out := map[string]buildsys.Description{}
	for _, t := range bazel.Describe(g, targets).Targets {
		out[t.Label] = t
	}
	return out, nil
}

// Migrate is `bazel run <target> -- apply [--allow-hazard C]...` at the checkout: the report the
// migration prints on its standard output, and everything printed. Its secrets are files in a
// directory of their own on a memory file system ($OK_SECRETS_DIR), removed when it ends, and
// redacted from what it printed.
func (b *Bazel) Migrate(ctx context.Context, target string, allow []string, values map[string][]byte) (Applied, error) {
	d, err := b.driver()
	if err != nil {
		return Applied{}, err
	}
	args := []string{"apply"}
	for _, h := range allow {
		args = append(args, "--allow-hazard", h)
	}
	dir, remove, err := secrets.TempDir(values)
	if err != nil {
		return Applied{}, fmt.Errorf("writing %s's secrets: %w", target, err)
	}
	defer remove()
	env := b.Env
	if dir != "" {
		env = append(slices.Clone(b.Env), secrets.DirEnv+"="+dir)
	}
	stdout, all, code, err := run(ctx, b.Checkout.Dir(), env, d.Run(b.Checkout.Dir(), target, args, nil))
	if err != nil {
		return Applied{}, fmt.Errorf("running %s: %w", target, err)
	}
	stdout, all = secrets.Redacted(stdout, values), secrets.Redacted(all, values)
	a := Applied{Output: string(all)}
	var r migrate.Report
	if report := bytes.TrimSpace(stdout); json.Unmarshal(report, &r) == nil {
		a.Report, a.Refused = report, r.Refused
		if code != 0 && r.Error != "" {
			a.Failed = firstLine(r.Error)
		}
	}
	if code != 0 && a.Failed == "" {
		a.Failed = fmt.Sprintf("bazel run %s exited with status %d: %s", target, code, tail(all))
	}
	return a, nil
}

// run runs argv in dir, and returns its standard output, everything it printed, and its exit
// status. It runs in a process group of its own, which ctx's end kills.
func run(ctx context.Context, dir string, env, argv []string) (stdout, all []byte, code int, err error) {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Env = env
	// exec copies each stream from its own pipe in its own goroutine, so what both write to is
	// written to under a lock.
	var out bytes.Buffer
	var combined lockedBuffer
	cmd.Stdout = multi{&out, &combined}
	cmd.Stderr = &combined
	cmd.SysProcAttr = proc.DieWithParent(&syscall.SysProcAttr{Setpgid: true}, syscall.SIGKILL)
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 10 * time.Second
	err = cmd.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return out.Bytes(), combined.buf.Bytes(), exit.ExitCode(), nil
	}
	return out.Bytes(), combined.buf.Bytes(), 0, err
}

// multi writes standard output to its own buffer and to everything printed.
type multi struct {
	a *bytes.Buffer
	b *lockedBuffer
}

func (m multi) Write(p []byte) (int, error) {
	m.a.Write(p)
	return m.b.Write(p)
}

// lockedBuffer is a buffer two streams write to: each write is whole, never interleaved with or
// written over by the other's.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// tail is the last line a command printed, for a message.
func tail(out []byte) string {
	s := strings.TrimSpace(string(out))
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		s = s[i+1:]
	}
	return s
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return line
}
