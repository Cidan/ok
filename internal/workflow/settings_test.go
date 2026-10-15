package workflow

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseSettings(t *testing.T) {
	r, err := ParseSettings([]byte(`
build: bazel
bazel:
  flags: [--config=ci, --keep_going]
  lint:
    aspects:
      - //tools/lint:linters.bzl%golangci_lint
      - "@aspect_rules_lint//lint:shellcheck.bzl%shellcheck"
    format: //tools/format
identity: identity
`), true)
	if err != nil {
		t.Fatal(err)
	}
	want := &Settings{
		Build: BuildBazel,
		Bazel: Bazel{
			Flags: []string{"--config=ci", "--keep_going"},
			Lint: Lint{
				Aspects: []string{"//tools/lint:linters.bzl%golangci_lint", "@aspect_rules_lint//lint:shellcheck.bzl%shellcheck"},
				Format:  "//tools/format:format",
			},
		},
		Identity: "identity",
	}
	if !reflect.DeepEqual(r, want) {
		t.Errorf("ParseSettings = %+v, want %+v", r, want)
	}
	if r, err := ParseSettings(nil, false); err != nil || r.Build != BuildNone {
		t.Errorf("no file: %+v, %v; want an imported repository", r, err)
	}
	if r, err := ParseSettings([]byte("build: none\n"), true); err != nil || r.Build != BuildNone {
		t.Errorf("build: none: %+v, %v", r, err)
	}
}

func TestParseSettingsRefuses(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"empty":           {"", "says nothing"},
		"no build":        {"identity: x\n", "build is required"},
		"other build":     {"build: make\n", `not "make"`},
		"unknown key":     {"build: bazel\nci: true\n", "field ci not found"},
		"two documents":   {"build: bazel\n---\nbuild: none\n", "more than one YAML document"},
		"imported bazel":  {"build: none\nbazel:\n  flags: [--x]\n", "runs no Bazel"},
		"not a flag":      {"build: bazel\nbazel:\n  flags: [config=ci]\n", "is not a flag"},
		"not an aspect":   {"build: bazel\nbazel:\n  lint:\n    aspects: [//tools/lint:linters.bzl]\n", "is not an aspect"},
		"aspect not bzl":  {"build: bazel\nbazel:\n  lint:\n    aspects: ['//tools/lint:x%y']\n", "is not an aspect"},
		"bad format":      {"build: bazel\nbazel:\n  lint:\n    format: tools/format\n", "lint format"},
		"identity escape": {"build: bazel\nidentity: ../x\n", "is not a folder"},
		"identity hidden": {"build: bazel\nidentity: .ok\n", "is not a folder"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseSettings([]byte(tc.in), true)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ParseSettings(%q) = %v, want an error saying %q", tc.in, err, tc.want)
			}
		})
	}
}

func TestStandingOf(t *testing.T) {
	bazel, none := &Settings{Build: BuildBazel}, &Settings{Build: BuildNone}
	for _, tc := range []struct {
		change, base *Settings
		want         Standing
	}{
		{bazel, bazel, Checked},
		{none, none, Imported},
		{none, bazel, BecomesImported},
		{bazel, none, Graduates},
	} {
		if got := StandingOf(tc.change, tc.base); got != tc.want {
			t.Errorf("StandingOf(%s, %s) = %v, want %v", tc.change.Build, tc.base.Build, got, tc.want)
		}
	}
}
