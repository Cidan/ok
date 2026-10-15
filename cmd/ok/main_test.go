package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/buildsys"
)

func ok(t *testing.T, stdin string, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	if err := run(args, strings.NewReader(stdin), &out); err != nil {
		t.Fatalf("ok %s: %v\n%s", strings.Join(args, " "), err, out.String())
	}
	return out.String()
}

func fails(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := run(args, strings.NewReader(""), &out)
	if err == nil {
		t.Fatalf("ok %s succeeded:\n%s", strings.Join(args, " "), out.String())
	}
	return out.String(), err
}

// ownersFiles are the sample repository's OWNERS files. They are written into a copy of it
// rather than kept in testdata, where they would be OWNERS files of this repository itself.
var ownersFiles = map[string]string{
	"OWNERS":        "user: ada\nset allowself\n",
	"server/OWNERS": "acl: eng\nper-file *.proto=user: bea\n",
	"web/OWNERS":    "acl: eng\nper-file *.css=user: cal\n",
}

// copyRepo is a writable copy of the sample repository, with its OWNERS files.
func copyRepo(t *testing.T) string {
	t.Helper()
	dst := t.TempDir()
	defer func() {
		for p, body := range ownersFiles {
			if err := os.MkdirAll(filepath.Dir(filepath.Join(dst, p)), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dst, p), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}()
	err := filepath.WalkDir("testdata/repo", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel("testdata/repo", p)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func TestIdentityCheck(t *testing.T) {
	got := ok(t, "", "identity", "check", "-domain", "example.com", "testdata/repo/identity")
	want := "admins.acl.example.com, owned by admins.acl.example.com: 1 members, 1 people\n" +
		"eng.acl.example.com, owned by admins.acl.example.com: 2 members, 3 people\n" +
		"eng.group.example.com, owned by eng.acl.example.com: 2 members, 2 people\n"
	if got != want {
		t.Errorf("identity check:\n%s\nwant:\n%s", got, want)
	}
	// The same tree under another domain names no one it holds.
	if _, err := fails(t, "identity", "check", "-domain", "example.org", "testdata/repo/identity"); !strings.Contains(err.Error(), "is no name under example.org") {
		t.Errorf("under another domain: %v", err)
	}
	if _, err := fails(t, "identity", "check", "testdata/repo/identity"); !errors.As(err, new(usageError)) {
		t.Errorf("no domain: %v", err)
	}
}

func TestOwnersCheck(t *testing.T) {
	repo := copyRepo(t)
	if got := ok(t, "", "owners", "check", "-domain", "example.com", repo); got != "3 OWNERS files: every set can be approved by its own people\n" {
		t.Errorf("owners check: %q", got)
	}
	os.WriteFile(filepath.Join(repo, "web", "OWNERS"), []byte("acl: eng\nper-file *.css=set noparent\nper-file *.css=user: cal\n"), 0o644)
	os.WriteFile(filepath.Join(repo, "server", "OWNERS"), []byte("acl: platform\n"), 0o644)
	out, err := fails(t, "owners", "check", "-domain", "example.com", repo)
	for _, want := range []string{
		"server/OWNERS: directory-level owners: acl: platform names no ACL the groups-and-ACLs tree declares",
		"web/OWNERS:2: per-file *.css: cal alone owns these files, without set allowself",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("owners check says no %q:\n%s", want, out)
		}
	}
	if !strings.Contains(err.Error(), "problems in 3 OWNERS files") {
		t.Errorf("owners check: %v", err)
	}

	// -identity names the tree within the repository, wherever the command runs; settings that do
	// not parse are an error, not a repository without a tree.
	os.WriteFile(filepath.Join(repo, "server", "OWNERS"), []byte("acl: eng\n"), 0o644)
	os.WriteFile(filepath.Join(repo, "web", "OWNERS"), []byte("acl: eng\n"), 0o644)
	os.Remove(filepath.Join(repo, ".ok", "repository.yaml"))
	if got := ok(t, "", "owners", "check", "-domain", "example.com", "-identity", "identity", repo); got != "3 OWNERS files: every set can be approved by its own people\n" {
		t.Errorf("owners check -identity: %q", got)
	}
	os.WriteFile(filepath.Join(repo, ".ok", "repository.yaml"), []byte("identity: identity\n"), 0o644)
	if _, err := fails(t, "owners", "check", "-domain", "example.com", repo); !strings.Contains(err.Error(), "build is required") {
		t.Errorf("settings that do not parse: %v", err)
	}
}

func TestWorkflowCheck(t *testing.T) {
	if got := ok(t, "", "workflow", "check", "testdata/repo"); got != "the repository's build: bazel\n2 workflow files, each valid\n" {
		t.Errorf("workflow check: %q", got)
	}
	repo := copyRepo(t)
	os.WriteFile(filepath.Join(repo, ".ok", "workflow", "common.yaml"), []byte("pre:\n  gazelle:\n    target: '@other//:gazelle'\n"), 0o644)
	out, _ := fails(t, "workflow", "check", repo)
	if !strings.Contains(out, `target "@other//:gazelle" is not a Bazel label in the main repository`) {
		t.Errorf("workflow check:\n%s", out)
	}
	os.Remove(filepath.Join(repo, ".ok", "repository.yaml"))
	os.WriteFile(filepath.Join(repo, ".ok", "workflow", "common.yaml"), nil, 0o644)
	if got := ok(t, "", "workflow", "check", repo); !strings.HasPrefix(got, "the repository's build: none\n") {
		t.Errorf("without settings: %q", got)
	}
}

func TestDirectoryPlan(t *testing.T) {
	got := ok(t, "", "directory", "plan", "-domain", "example.com", "-static", "testdata/people.yaml")
	want := "grace.human.example.com: create, suspended\n" +
		"engineering.group.example.com: create, with members grace.human.example.com\n" +
		"the person 1001 waits: ada-lovelace.human.example.com is wanted by 1001 and 1003 at once: set their handles\n" +
		"the person 1003 waits: ada-lovelace.human.example.com is wanted by 1001 and 1003 at once: set their handles\n" +
		"note: engineering.group.example.com: 1001 is not in the directory (it waits), so it is not a member yet\n" +
		"note: engineering.group.example.com: 1003 is not in the directory (it waits), so it is not a member yet\n"
	if got != want {
		t.Errorf("directory plan:\n%s\nwant:\n%s", got, want)
	}
}

// fakeBazel is a bazel that answers a query with the real answer recorded for the sample
// workspace's variant named by the checkout's directory.
func fakeBazel(t *testing.T) string {
	t.Helper()
	answers, err := filepath.Abs("../../internal/buildsys/bazel/testdata/sample")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "bazel")
	os.WriteFile(bin, []byte("#!/bin/sh\ncat \""+answers+"/$(basename \"$PWD\").jsonl\"\n"), 0o755)
	return bin
}

func TestAffected(t *testing.T) {
	sample := "../../internal/buildsys/bazel/testdata/sample"
	bin := fakeBazel(t)
	got := ok(t, "lib/a.txt\n", "affected", "-bazel", bin, "-base", sample+"/base", "-change", sample+"/src-edit")
	want := "//app:joined (cat): //app:joined —deps→ //lib:files, which holds lib/a.txt\n" +
		"//app:joined_test (check_test): //app:joined_test —deps→ //app:joined —deps→ //lib:files, which holds lib/a.txt\n" +
		"//lib:files (filegroup): //lib:files, which holds lib/a.txt\n" +
		"3 affected, 1 tests, 1 upstream\n"
	if got != want {
		t.Errorf("affected:\n%s\nwant:\n%s", got, want)
	}
	var a buildsys.Affected
	if err := json.Unmarshal([]byte(ok(t, "MODULE.bazel\n", "affected", "-json", "-bazel", bin, "-base", sample+"/base", "-change", sample+"/module-edit")), &a); err != nil {
		t.Fatal(err)
	}
	if a.Global != "MODULE.bazel" || len(a.Affected) != 5 {
		t.Errorf("affected -json: global %q, %d affected", a.Global, len(a.Affected))
	}
}

// Help, asked for a group or a command, is help: printed, with no error to exit on.
func TestHelp(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"scm", "--help"}, {"identity", "-h"}, {"worker", "help"}} {
		var out bytes.Buffer
		if err := run(args, strings.NewReader(""), &out); err != nil || !strings.Contains(out.String(), "ok scm fsck") {
			t.Errorf("ok %s: %v\n%s", strings.Join(args, " "), err, out.String())
		}
	}
	if err := run([]string{"scm", "fsck", "-h"}, strings.NewReader(""), io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("ok scm fsck -h: %v, want flag.ErrHelp (which exits 0)", err)
	}
}
