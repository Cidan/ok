// Package migrate is ok's migrate protocol: how ok's deployer runs a repository's migrations
// (docs/deploy.md, "Migrations"). A migration is a runnable Bazel target; the deployer runs it as
//
//	bazel run //db:migrate -- plan
//	bazel run //db:migrate -- apply [--allow-hazard CLASS]...
//
// and it prints one JSON Report on its standard output, whatever the outcome: plan says what apply
// would do, doing nothing; apply does it, refusing a plan that carries a gated hazard not allowed
// (Gated). The exit status is 0 when it planned or applied, ExitRefused when it refused, and 1 when
// it failed. The flags a target's BUILD rule gives it come before the command.
//
// A program implements Migrator and calls Main; package postgres is a Migrator of ordered SQL
// files.
package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
)

// The hazard classes a step can carry. Every one is shown with the plan; the Gated ones are
// refused unless allowed.
const (
	// DeletesData drops or empties a table, column, sequence, schema, or extension, or deletes
	// rows.
	DeletesData = "DELETES_DATA"
	// AccessExclusiveLock blocks reads and writes on an existing table for the step.
	AccessExclusiveLock = "ACCESS_EXCLUSIVE_LOCK"
	// TableRewrite rewrites every row of a table.
	TableRewrite = "TABLE_REWRITE"
	// IndexBuild builds an index, holding writes to its table unless built concurrently.
	IndexBuild = "INDEX_BUILD"
	// ValidationScan scans a table to validate a constraint.
	ValidationScan = "VALIDATION_SCAN"
	// MayFailOnData can fail on rows already there.
	MayFailOnData = "MAY_FAIL_ON_DATA"
)

// Gated are the hazard classes a plan is refused for unless they are allowed.
var Gated = []string{DeletesData}

// ExitRefused is apply's exit status when it refused a plan's gated hazards.
const ExitRefused = 2

// Hazard is a risk a step carries.
type Hazard struct {
	Class  string `json:"class"`
	Detail string `json:"detail,omitempty"`
	Gated  bool   `json:"gated"`
}

// Step is one thing a plan does.
type Step struct {
	Description string   `json:"description"`
	SQL         string   `json:"sql,omitempty"`
	Hazards     []Hazard `json:"hazards,omitempty"`
}

// Report is what a migration prints: where it applies, its plan, the gated classes it refused,
// whether it executed, and the error that ended it.
type Report struct {
	Database string   `json:"database,omitempty"`
	Server   string   `json:"server,omitempty"`
	Steps    []Step   `json:"steps"`
	Refused  []string `json:"refused,omitempty"`
	Executed bool     `json:"executed"`
	Error    string   `json:"error,omitempty"`
}

// Migrator plans and applies a migration.
type Migrator interface {
	// Plan says what Apply would do, doing nothing: the report's Database, Server, and Steps.
	Plan(ctx context.Context) (Report, error)
	// Apply does what plan, which Plan made, says.
	Apply(ctx context.Context, plan Report) error
}

// Main runs a migration's command line — os.Args after the program's own flags, which flags (when
// not nil) has parsed — prints its report, and exits with its status. connect makes the Migrator,
// once the command is known to be well formed.
func Main(flags *flag.FlagSet, connect func(ctx context.Context) (Migrator, error)) {
	args := os.Args[1:]
	if flags != nil {
		if err := flags.Parse(args); err != nil {
			os.Exit(1)
		}
		args = flags.Args()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := Run(ctx, args, os.Stdout, connect)
	stop()
	os.Exit(code)
}

// Run runs a migration's command — plan, or apply with its --allow-hazard flags — printing the
// report to stdout, and returns the exit status.
func Run(ctx context.Context, args []string, stdout io.Writer, connect func(ctx context.Context) (Migrator, error)) int {
	var r Report
	code := run(ctx, args, connect, &r)
	if r.Steps == nil {
		r.Steps = []Step{}
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil && code == 0 {
		code = 1
	}
	return code
}

func run(ctx context.Context, args []string, connect func(ctx context.Context) (Migrator, error), r *Report) int {
	fail := func(err error) int {
		r.Error = err.Error()
		return 1
	}
	if len(args) == 0 || (args[0] != "plan" && args[0] != "apply") {
		return fail(errors.New("usage: plan | apply [--allow-hazard CLASS]..."))
	}
	cmd := flag.NewFlagSet(args[0], flag.ContinueOnError)
	cmd.SetOutput(io.Discard)
	var allowed []string
	if args[0] == "apply" {
		cmd.Func("allow-hazard", "a gated hazard class to allow (repeated)", func(v string) error {
			allowed = append(allowed, v)
			return nil
		})
	}
	if err := cmd.Parse(args[1:]); err != nil {
		return fail(err)
	}
	if cmd.NArg() > 0 {
		return fail(fmt.Errorf("%s takes no arguments: %q", args[0], cmd.Args()))
	}
	m, err := connect(ctx)
	if err != nil {
		return fail(err)
	}
	plan, err := m.Plan(ctx)
	if err != nil {
		return fail(fmt.Errorf("planning: %w", err))
	}
	*r = plan
	r.Refused, r.Executed, r.Error = nil, false, ""
	for i := range r.Steps {
		for j := range r.Steps[i].Hazards {
			h := &r.Steps[i].Hazards[j]
			h.Gated = slices.Contains(Gated, h.Class)
		}
	}
	if args[0] == "plan" {
		return 0
	}
	for _, s := range r.Steps {
		for _, h := range s.Hazards {
			if h.Gated && !slices.Contains(allowed, h.Class) && !slices.Contains(r.Refused, h.Class) {
				r.Refused = append(r.Refused, h.Class)
			}
		}
	}
	if len(r.Refused) > 0 {
		r.Error = fmt.Sprintf("the plan carries gated hazards that were not allowed: %s", strings.Join(r.Refused, ", "))
		return ExitRefused
	}
	if len(r.Steps) == 0 {
		return 0
	}
	if err := m.Apply(ctx, *r); err != nil {
		return fail(err)
	}
	r.Executed = true
	return 0
}
