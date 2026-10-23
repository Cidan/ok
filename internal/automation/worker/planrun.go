package worker

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/Cidan/ok/internal/automation/changes"
	"github.com/Cidan/ok/internal/automation/plan"
	"github.com/Cidan/ok/internal/buildsys/bazel"
	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/owners"
	"github.com/Cidan/ok/internal/workflow"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// planChange runs a planning run (docs/automation.md, "Execution"): on the checkout of the
// patchset — or, for post, of its candidate — the Bazel graphs at the change and its base say what
// the change affects; the workflow files on main select the workflow jobs of the phase, those the
// change edits are checked at its revision (pre), and the change's graph describes every target
// they name. The plan is the built-in jobs and the workflow jobs whose targets are affected, and
// the change's footprint, which the landing queue decides by; anything wrong fails it, naming each
// problem.
func planChange(ctx context.Context, cfg Config, run *automationpb.Run, jc *workflow.Context, settings *workflow.Settings, env []string, log *logStream) outcome {
	if jc.Phase == workflow.Post {
		log.Printf("planning post for CL %d, patchset %d, on its candidate %.10s over %.10s", jc.CL, jc.Patchset, jc.Commit, jc.Base)
	} else {
		log.Printf("planning pre for CL %d, patchset %d", jc.CL, jc.Patchset)
	}
	touched := jc.Touched()
	log.Printf("the change touches %d paths", len(touched))
	repo := run.GetRepositoryId()
	base, err := hex.DecodeString(jc.Base)
	if err != nil {
		return outcome{status: automationpb.Run_STATUS_ERROR, reason: fmt.Sprintf("the change's base %q: %v", jc.Base, err)}
	}

	standing, err := standingOf(ctx, cfg, repo, base, settings, touched)
	if err != nil || ctx.Err() != nil {
		return stopped(ctx, "reading the base's "+workflow.SettingsFile+": "+fmt.Sprint(err))
	}
	switch standing {
	case workflow.Imported:
		log.Printf("the repository is imported (%s: build: none, or none at all): ok runs nothing on it", workflow.SettingsFile)
		return outcome{status: automationpb.Run_STATUS_PASSED, plan: &automationpb.Plan{}}
	case workflow.BecomesImported:
		return outcome{status: automationpb.Run_STATUS_FAILED, reason: workflow.BecomesImportedReason}
	case workflow.Graduates:
		log.Printf("the change makes an imported repository a Bazel workspace: everything in it is checked")
	}

	treeDir := ""
	if jc.Repository.Name == cfg.TreeRepository {
		treeDir = settings.Identity
	}
	if owners.Touches(touched, treeDir, workflow.SettingsFile) {
		// The tree's repository is checked against the tree at the change; any other, against the
		// tree ok identity has applied.
		var roster owners.Roster = identityRoster{ctx: ctx, dir: cfg.Directory, names: cfg.Names}
		if treeDir != "" {
			if roster, err = owners.LoadTreeRoster(os.DirFS(cfg.checkout.Dir()), cfg.Names, treeDir); err != nil {
				return outcome{status: automationpb.Run_STATUS_FAILED, reason: "the groups-and-ACLs tree: " + err.Error()}
			}
		}
		problems, configs, err := owners.CheckRepository(os.DirFS(cfg.checkout.Dir()), roster)
		if ctx.Err() != nil {
			return stopped(ctx, "")
		}
		if err != nil {
			return outcome{status: automationpb.Run_STATUS_ERROR, reason: "checking that every OWNERS set can be approved: " + err.Error()}
		}
		for _, p := range problems {
			log.Printf("problem: %s", p.Error())
		}
		if len(problems) > 0 {
			reason := problems[0].Error()
			if len(problems) > 1 {
				reason = fmt.Sprintf("%s (and %d more problems)", reason, len(problems)-1)
			}
			return outcome{status: automationpb.Run_STATUS_FAILED, reason: reason}
		}
		log.Printf("%d OWNERS files: every set can be approved by its own people", configs)
	}
	d := cfg.driver(settings, env, log)
	changeGraph, err := graphAt(ctx, cfg, d, repo, run.GetCommitId(), cfg.checkout, log)
	if ctx.Err() != nil {
		return stopped(ctx, "")
	}
	if err != nil {
		return outcome{status: automationpb.Run_STATUS_FAILED, reason: "the change leaves no build graph Bazel can load: " + err.Error()}
	}
	baseGraph := bazel.Empty()
	if standing == workflow.Checked {
		if baseGraph, err = graphAt(ctx, cfg, cfg.baseDriver(d), repo, base, cfg.base, log); err != nil {
			if ctx.Err() != nil {
				return stopped(ctx, "")
			}
			// A base whose graph cannot load affects nothing the change can rest on: everything it
			// builds is checked.
			log.Printf("note: the base's build graph does not load, so every target is affected: %v", err)
			baseGraph = bazel.Empty()
		}
	}
	baseFiles, err := checkout.FilesAt(ctx, cfg.Source, repo, base, touched)
	if err != nil || ctx.Err() != nil {
		return stopped(ctx, "reading the touched files at the base: "+fmt.Sprint(err))
	}
	aff, err := bazel.Affected(bazel.Commit{Files: baseFiles, Graph: baseGraph}, bazel.Commit{Files: os.DirFS(cfg.checkout.Dir()), Graph: changeGraph}, touched)
	if err != nil {
		return stopped(ctx, "what the change affects: "+err.Error())
	}
	aff.LintTools = lintTools(settings)
	log.Printf("the change affects %d targets, %d tests; %d upstream", len(aff.Affected), len(aff.Tests), len(aff.Upstream))
	if aff.Global != "" {
		log.Printf("every target is affected: the change touches %s", aff.Global)
	}

	main, err := readWorkflows(ctx, cfg.Source, repo, run.GetJob().GetWorkflowsCommit(), nil)
	if err != nil {
		return stopped(ctx, "reading main's workflow files: "+err.Error())
	}
	log.Printf("main's workflow files, at %x: %d", run.GetJob().GetWorkflowsCommit(), len(main))
	var change map[string][]byte
	for _, p := range touched {
		if standing == workflow.Graduates || jc.Phase == workflow.Pre && workflow.IsWorkflowFile(p) {
			change, err = readWorkflows(ctx, cfg.Source, repo, run.GetCommitId(), cfg.checkout)
			if err != nil {
				return stopped(ctx, "reading the change's workflow files: "+err.Error())
			}
			break
		}
	}

	var sincePre *plan.Affected
	if pre := run.GetJob().GetPreCommit(); jc.Phase == workflow.Post && len(pre) > 0 {
		sincePre, err = changedSincePre(ctx, cfg, d, run, jc, pre, changeGraph, log)
		if err != nil || ctx.Err() != nil {
			return stopped(ctx, "comparing the candidate with the commit pre ran on: "+fmt.Sprint(err))
		}
	}

	pool, err := mainPool(ctx, cfg.Source, repo, run.GetJob().GetWorkflowsCommit())
	if err != nil {
		return stopped(ctx, "reading main's "+workflow.SettingsFile+": "+err.Error())
	}
	pl := plan.New(plan.Input{Context: jc, Affected: aff, Main: main, Change: change, Standing: standing, SincePre: sincePre, Pool: pool})
	for _, n := range pl.Notes() {
		log.Printf("note: %s", n)
	}
	desc := &plan.Described{}
	if targets := pl.Targets(); len(targets) > 0 && len(pl.Problems()) == 0 {
		desc = bazel.Describe(changeGraph, targets)
	}
	result, problems := pl.Plan(desc)
	if len(problems) > 0 {
		for _, p := range problems {
			log.Printf("problem: %s", p)
		}
		reason := problems[0]
		if len(problems) > 1 {
			reason = fmt.Sprintf("%s (and %d more problems)", reason, len(problems)-1)
		}
		return outcome{status: automationpb.Run_STATUS_FAILED, reason: reason}
	}
	for _, line := range plan.Summary(result) {
		log.Printf("%s", line)
	}
	log.Printf("planned %d jobs; %d workflow jobs left to main", len(result.GetJobs()), len(result.GetSkipped()))
	for _, t := range aff.Affected {
		result.Affected = append(result.Affected, t.Label)
	}
	result.Upstream = aff.Upstream
	return outcome{status: automationpb.Run_STATUS_PASSED, plan: result}
}

// lintTools are what a lint's findings depend on besides the files and targets it checks: the
// repository's lint aspects' .bzl files and its formatter.
func lintTools(settings *workflow.Settings) []string {
	var out []string
	for _, a := range settings.Bazel.Lint.Aspects {
		file, _, _ := strings.Cut(a, "%")
		out = append(out, file)
	}
	if f := settings.Bazel.Lint.Format; f != "" {
		out = append(out, f)
	}
	return slices.Compact(slices.Sorted(slices.Values(out)))
}

// changedSincePre is what the differences between the commit the CL's pre ran on and the
// candidate affect, at the candidate: what main brought in under the CL, and nothing of the CL's
// own, which the candidate holds as the patchset does.
func changedSincePre(ctx context.Context, cfg Config, d *bazel.Driver, run *automationpb.Run, jc *workflow.Context, pre []byte, candidate *bazel.Graph, log *logStream) (*plan.Affected, error) {
	repo := run.GetRepositoryId()
	files, err := changes.Files(ctx, cfg.Browse, repo, pre, run.GetCommitId())
	if err != nil {
		return nil, err
	}
	diff := (&workflow.Context{Files: files}).Touched()
	log.Printf("since pre, on patchset %d's commit %.10x: %d paths differ", jc.Patchset, pre, len(diff))
	if len(diff) == 0 {
		return &plan.Affected{}, nil
	}
	preGraph, err := graphAt(ctx, cfg, cfg.baseDriver(d), repo, pre, cfg.base, log)
	if err != nil {
		// What pre's graph cannot say, a graph with problems says: every job runs.
		return &plan.Affected{Changed: diff, Problems: []string{"the build graph at the commit pre ran on: " + err.Error()}}, nil
	}
	preFiles, err := checkout.FilesAt(ctx, cfg.Source, repo, pre, diff)
	if err != nil {
		return nil, err
	}
	aff, err := bazel.Affected(bazel.Commit{Files: preFiles, Graph: preGraph}, bazel.Commit{Files: os.DirFS(cfg.checkout.Dir()), Graph: candidate}, diff)
	if err != nil {
		return nil, err
	}
	log.Printf("since pre: %d affected targets", len(aff.Affected))
	return aff, nil
}

// stopped is the outcome of a planning step that failed: canceled when the run was stopped,
// else an error — automation could not plan it.
func stopped(ctx context.Context, reason string) outcome {
	if ctx.Err() != nil {
		return outcome{status: automationpb.Run_STATUS_CANCELED, reason: "stopped", stopped: true}
	}
	return outcome{status: automationpb.Run_STATUS_ERROR, reason: reason}
}

// standingOf is the change's standing (docs/automation.md, "Imported repositories"): from its
// settings, in the checkout, and — when the change touches them — its base's, read from the SCM.
func standingOf(ctx context.Context, cfg Config, repo int64, base []byte, change *workflow.Settings, touched []string) (workflow.Standing, error) {
	if !slices.Contains(touched, workflow.SettingsFile) {
		return workflow.StandingOf(change, change), nil
	}
	files, err := checkout.FilesAt(ctx, cfg.Source, repo, base, []string{workflow.SettingsFile})
	if err != nil {
		return 0, err
	}
	data, err := fs.ReadFile(files, workflow.SettingsFile)
	present := err == nil
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return 0, err
	}
	was, err := workflow.ParseSettings(data, present)
	if err != nil {
		// Settings that do not parse never landed on a main whose uploads checked them; one from
		// before that is taken as a Bazel workspace's, so the change is checked.
		was = &workflow.Settings{Build: workflow.BuildBazel}
	}
	return workflow.StandingOf(change, was), nil
}

// mainPool is the pool main's settings name for the repository's jobs ("" for none).
func mainPool(ctx context.Context, src checkout.Source, repo int64, commit []byte) (string, error) {
	files, err := checkout.FilesAt(ctx, src, repo, commit, []string{workflow.SettingsFile})
	if err != nil {
		return "", err
	}
	data, err := fs.ReadFile(files, workflow.SettingsFile)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	s, err := workflow.ParseSettings(data, true)
	if err != nil {
		return "", nil // settings on main that do not parse landed before uploads checked them
	}
	return s.Pool, nil
}

// readWorkflows reads a commit's workflow files: from the SCM, or — when co holds the commit —
// from the checkout.
func readWorkflows(ctx context.Context, src checkout.Source, repo int64, commit []byte, co checkout.Checkout) (map[string][]byte, error) {
	entries, err := src.Entries(ctx, repo, commit)
	if err != nil {
		return nil, err
	}
	out := map[string][]byte{}
	byID := map[checkout.ID][]string{}
	var ids []checkout.ID
	for _, e := range entries {
		if e.Kind != checkout.File || !workflow.IsWorkflowFile(e.Path) {
			continue
		}
		if co != nil {
			data, err := os.ReadFile(filepath.Join(co.Dir(), filepath.FromSlash(e.Path)))
			if err != nil {
				return nil, err
			}
			out[e.Path] = data
			continue
		}
		if _, ok := byID[e.ID]; !ok {
			ids = append(ids, e.ID)
		}
		byID[e.ID] = append(byID[e.ID], e.Path)
	}
	if len(ids) == 0 {
		return out, nil
	}
	parts := map[checkout.ID][]byte{}
	err = src.ReadFiles(ctx, repo, ids, func(id checkout.ID, data []byte, last bool) error {
		parts[id] = append(parts[id], data...)
		if last {
			for _, p := range byID[id] {
				out[p] = parts[id]
			}
		}
		return nil
	})
	return out, err
}

// wait starts a command in a process group of its own and waits for it: stopping the run stops it
// and everything it started, and one that does not stop within the grace period is killed. It
// returns the command's exit status.
func wait(ctx context.Context, cfg Config, cmd *exec.Cmd) (int, error) {
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	var err error
	select {
	case err = <-waited:
	case <-ctx.Done():
		pgid := cmd.Process.Pid
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
		select {
		case err = <-waited:
		case <-time.After(cfg.Grace):
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
			err = <-waited
		}
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode(), nil
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return 0, err
}
