package workflow

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"gopkg.in/yaml.v3"
)

// SettingsFile is the file that says how ok checks a repository. A repository without one is
// imported: brought into ok as it was, it runs no job and deploys nothing, and its changes pass
// review alone.
const SettingsFile = ".ok/repository.yaml"

// Build is how a repository's changes are checked.
type Build string

const (
	// BuildNone is an imported repository's: nothing runs.
	BuildNone Build = "none"
	// BuildBazel is a Bazel workspace's: every job is a Bazel command.
	BuildBazel Build = "bazel"
)

// Settings are a repository's settings (SettingsFile):
//
//	build: bazel                  # or none: an imported repository
//	bazel:
//	  flags: [--config=ci]        # added to every Bazel command a job runs
//	  lint:
//	    aspects:                  # target linters, rules_lint style, run over the change's targets
//	      - //tools/lint:linters.bzl%golangci_lint
//	    format: //tools/format:format   # a formatter run in check mode over the change's files
//	identity: identity            # the folder holding the groups-and-ACLs tree, in the repository that holds it
type Settings struct {
	Build Build
	Bazel Bazel
	// Identity is the folder, repository-relative, of the groups-and-ACLs tree, when this is the
	// repository that holds the installation's.
	Identity string
}

// Bazel is how jobs run Bazel in a repository.
type Bazel struct {
	Flags []string
	Lint  Lint
}

// Lint is what a repository's lint job runs. With neither, a change has no lint job.
type Lint struct {
	// Aspects are Bazel aspects (//package:file.bzl%name) run over the targets that own the
	// change's files, each finding failing the job.
	Aspects []string
	// Format is a runnable target run in check mode over the change's files.
	Format string
}

type repositoryFile struct {
	Build *string `yaml:"build"`
	Bazel *struct {
		Flags []string `yaml:"flags"`
		Lint  *struct {
			Aspects []string `yaml:"aspects"`
			Format  string   `yaml:"format"`
		} `yaml:"lint"`
	} `yaml:"bazel"`
	Identity string `yaml:"identity"`
}

// ParseSettings reads a repository's settings: present is whether SettingsFile exists, and
// data its content. A repository without the file is imported.
func ParseSettings(data []byte, present bool) (*Settings, error) {
	if !present {
		return &Settings{Build: BuildNone}, nil
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var f repositoryFile
	if err := dec.Decode(&f); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%s says nothing: it needs build: bazel, or build: none", SettingsFile)
		}
		return nil, fmt.Errorf("%s: %w", SettingsFile, err)
	}
	var extra repositoryFile
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s holds more than one YAML document", SettingsFile)
	}
	if f.Build == nil {
		return nil, fmt.Errorf("%s: build is required: bazel, or none", SettingsFile)
	}
	r := &Settings{Build: Build(*f.Build), Identity: f.Identity}
	switch r.Build {
	case BuildBazel:
	case BuildNone:
		if f.Bazel != nil {
			return nil, fmt.Errorf("%s: an imported repository (build: none) runs no Bazel, so has no bazel settings", SettingsFile)
		}
	default:
		return nil, fmt.Errorf("%s: build is bazel or none, not %q", SettingsFile, *f.Build)
	}
	if f.Bazel != nil {
		r.Bazel.Flags = f.Bazel.Flags
		for _, flag := range f.Bazel.Flags {
			if !strings.HasPrefix(flag, "--") {
				return nil, fmt.Errorf("%s: bazel flag %q is not a flag (--name or --name=value)", SettingsFile, flag)
			}
		}
		if l := f.Bazel.Lint; l != nil {
			for _, a := range l.Aspects {
				file, name, ok := strings.Cut(a, "%")
				if !ok || name == "" || !strings.HasSuffix(file, ".bzl") || !isLabel(file) {
					return nil, fmt.Errorf("%s: lint aspect %q is not an aspect, //package:file.bzl%%name", SettingsFile, a)
				}
			}
			r.Bazel.Lint.Aspects = l.Aspects
			if l.Format != "" {
				format, err := CanonicalLabel(l.Format)
				if err != nil {
					return nil, fmt.Errorf("%s: lint format: %w", SettingsFile, err)
				}
				r.Bazel.Lint.Format = format
			}
		}
	}
	if r.Identity != "" {
		for _, seg := range strings.Split(r.Identity, "/") {
			if seg == "" || seg == "." || seg == ".." || strings.HasPrefix(seg, ".") {
				return nil, fmt.Errorf("%s: identity %q is not a folder of the repository", SettingsFile, r.Identity)
			}
		}
	}
	return r, nil
}

// isLabel reports whether s is a Bazel label in the main repository or another
// (@repo//package:name): an aspect may come from a dependency.
func isLabel(s string) bool {
	if rest, ok := strings.CutPrefix(s, "@"); ok {
		repo, label, ok := strings.Cut(strings.TrimPrefix(rest, "@"), "//")
		if !ok || repo == "" || !onlyOf(repo, packageChar) {
			return false
		}
		s = "//" + label
	}
	_, err := CanonicalLabel(s)
	return err == nil
}

// Standing is how a change stands toward ok's checks, from its repository settings and its
// base's.
type Standing int

const (
	// Checked is a Bazel workspace's change: planned and checked as any.
	Checked Standing = iota + 1
	// Imported is an imported repository's change: nothing runs.
	Imported
	// BecomesImported is a change that makes a Bazel workspace imported: it never lands, since
	// nothing would check it or any change after it.
	BecomesImported
	// Graduates is a change that makes an imported repository a Bazel workspace: checked whole —
	// everything built, every test run, and its own workflow files read.
	Graduates
)

// BecomesImportedReason is why a change that makes a Bazel workspace imported fails.
const BecomesImportedReason = "the change makes a Bazel workspace imported (build: none, or no " + SettingsFile +
	"): nothing would check it, or any change after it, so it cannot land"

// StandingOf is a change's standing, from its repository settings and its base's.
func StandingOf(change, base *Settings) Standing {
	switch {
	case change.Build == BuildNone && base.Build == BuildNone:
		return Imported
	case change.Build == BuildNone:
		return BecomesImported
	case base.Build == BuildNone:
		return Graduates
	}
	return Checked
}
