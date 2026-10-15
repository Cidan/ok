package bazel

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	gopath "path"
	"slices"
	"sort"
	"strings"

	"github.com/Cidan/ok/internal/buildsys"
)

// GlobalFiles are the files that decide how every target builds — the module and its lockfile,
// the workspace files, Bazel's version and its flags, and ok's settings for the repository — so a
// change to one affects every target.
var GlobalFiles = []string{
	"MODULE.bazel", "MODULE.bazel.lock", "WORKSPACE", "WORKSPACE.bazel", "WORKSPACE.bzlmod",
	".bazelversion", ".bazelrc", ".ok/repository.yaml",
}

// buildFiles are the names of a package's BUILD file.
var buildFiles = []string{"BUILD.bazel", "BUILD"}

// Commit is one commit's files and its graph. Files holds at least the paths a change touches:
// a checkout (os.DirFS), or those files read from the SCM.
type Commit struct {
	Files fs.FS
	Graph *Graph
}

// Affected says what a change affects: base is the commit it changes, change the commit itself,
// and touched every path that differs between them (both paths of a rename). A target is affected
// when its hash (Graph.hashes) differs between the two, or it is new: when what it is — its
// attributes, its rule's definition — differs, or a file it reads, or anything it depends on.
// Every target is affected when the change touches one of GlobalFiles, or a toolchain is
// affected: a toolchain is resolved when Bazel analyzes, out of a query's sight, so who uses one
// cannot be told.
//
// Only touched files are read, at each commit: every other file is the same at both.
func Affected(base, change Commit, touched []string) (*buildsys.Affected, error) {
	res := &buildsys.Affected{Changed: slices.Sorted(slices.Values(touched)), Problems: []string{}}
	isTouched := map[string]bool{}
	for _, p := range touched {
		isTouched[p] = true
	}
	baseContent, baseErr := contents(base.Files, touched)
	changeContent, changeErr := contents(change.Files, touched)
	hb := base.Graph.hashes(baseContent.of)
	hc := change.Graph.hashes(changeContent.of)
	if err := errors.Join(*baseErr, *changeErr); err != nil {
		return nil, err
	}

	g := change.Graph
	changed := map[string]bool{} // every target, of any kind, whose hash differs or that is new
	for l := range g.nodes {
		if h, ok := hb[l]; !ok || h != hc[l] {
			changed[l] = true
		}
	}

	// Why a target is directly affected: the changed file it reads, or "" for its own declaration.
	direct := map[string]string{}
	for l := range changed {
		nd := g.nodes[l]
		if nd.kind == "source file" {
			continue
		}
		var srcs []string
		for _, in := range nd.inputs {
			if s := g.nodes[in]; s != nil && s.kind == "source file" && changed[in] && baseContent.of(in) != changeContent.of(in) {
				srcs = append(srcs, sourceFile(in, touched))
			}
		}
		sort.Strings(srcs)
		if b, ok := base.Graph.nodes[l]; !ok || b.own != nd.own {
			direct[l] = buildFileOf(l, isTouched)
			if direct[l] == "" && len(srcs) > 0 {
				direct[l] = srcs[0]
			}
			continue
		}
		if len(srcs) > 0 {
			direct[l] = srcs[0]
		}
	}

	global := ""
	for _, f := range GlobalFiles {
		if isTouched[f] {
			global = f
			break
		}
	}
	if global == "" {
		for _, l := range slices.Sorted(maps.Keys(changed)) {
			if g.nodes[l].class == "toolchain" {
				global = l
				break
			}
		}
	}

	// Dependents, through targets of every kind, for the chains and the roots.
	rdeps := map[string][]string{}
	for l, nd := range g.nodes {
		for _, in := range nd.inputs {
			rdeps[in] = append(rdeps[in], l)
		}
	}
	affected := map[string]bool{}
	pred := map[string]string{} // a target reached from a directly affected one: the input it was reached from
	if global != "" {
		res.Global = global
		for l, nd := range g.nodes {
			if nd.kind == "rule" && mainRepo(l) {
				affected[l] = true
			}
		}
	} else {
		var frontier []string
		for l := range direct {
			frontier = append(frontier, l)
		}
		sort.Strings(frontier)
		seen := map[string]bool{}
		for _, l := range frontier {
			seen[l] = true
		}
		for len(frontier) > 0 {
			var level []string
			for _, l := range frontier {
				for _, d := range rdeps[l] {
					if seen[d] || !changed[d] {
						continue
					}
					seen[d] = true
					pred[d] = l
					level = append(level, d)
				}
			}
			sort.Strings(level)
			frontier = level
		}
		for l := range changed {
			if nd := g.nodes[l]; nd.kind == "rule" && mainRepo(l) {
				affected[l] = true
			}
		}
	}

	notRoot := map[string]bool{}
	for l := range affected {
		for _, r := range g.ruleInputs(l) {
			notRoot[r] = true
		}
	}
	for _, l := range slices.Sorted(maps.Keys(affected)) {
		nd := g.nodes[l]
		t := buildsys.Target{Label: l, Kind: nd.class, Test: isTest(nd.class), Binary: strings.HasSuffix(nd.class, "_binary"), Root: !notRoot[l]}
		switch {
		case global != "" && g.nodes[global] != nil:
			t.Because = []buildsys.Step{{Label: l, Via: "the toolchain " + global + " changed"}}
		case global != "":
			t.Because = []buildsys.Step{{Label: l, File: global}}
		default:
			cur := l
			for {
				if file, ok := direct[cur]; ok {
					t.Because = append(t.Because, buildsys.Step{Label: cur, File: file})
					break
				}
				p, ok := pred[cur]
				if !ok {
					// Reached by no chain from a directly affected target: its hash differs through
					// something outside the repository's targets.
					t.Because = append(t.Because, buildsys.Step{Label: cur, Via: "something it depends on outside the repository changed"})
					break
				}
				t.Because = append(t.Because, buildsys.Step{Label: cur, Via: "deps"})
				cur = p
			}
		}
		t.Direct = len(t.Because) == 1
		res.Affected = append(res.Affected, t)
		if t.Test {
			res.Tests = append(res.Tests, l)
		}
	}

	up := map[string]bool{}
	var stack []string
	for l := range affected {
		stack = append(stack, g.nodes[l].inputs...)
	}
	visited := map[string]bool{}
	for len(stack) > 0 {
		l := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[l] {
			continue
		}
		visited[l] = true
		nd := g.nodes[l]
		if nd == nil {
			continue
		}
		if nd.kind == "rule" && mainRepo(l) && !affected[l] {
			up[l] = true
		}
		stack = append(stack, nd.inputs...)
	}
	res.Upstream = slices.Sorted(maps.Keys(up))
	if res.Tests == nil {
		res.Tests = []string{}
	}
	if res.Upstream == nil {
		res.Upstream = []string{}
	}
	return res, nil
}

// ruleInputs are the rules a target depends on directly, or through targets that are not rules
// (a generated file, its generating rule).
func (g *Graph) ruleInputs(label string) []string {
	var out []string
	seen := map[string]bool{}
	stack := append([]string(nil), g.nodes[label].inputs...)
	for len(stack) > 0 {
		l := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[l] {
			continue
		}
		seen[l] = true
		nd := g.nodes[l]
		switch {
		case nd == nil:
		case nd.kind == "rule":
			out = append(out, l)
		default:
			stack = append(stack, nd.inputs...)
		}
	}
	return out
}

func isTest(class string) bool { return strings.HasSuffix(class, "_test") || class == "test_suite" }

// buildFileOf is the BUILD file of a target's package, when the change touches it: what changed
// its declaration. It is "" otherwise — its rule's definition, in a .bzl file, changed.
func buildFileOf(label string, touched map[string]bool) string {
	pkg, _, _ := strings.Cut(strings.TrimPrefix(label, "//"), ":")
	for _, name := range buildFiles {
		if p := gopath.Join(pkg, name); touched[p] {
			return p
		}
	}
	return ""
}

// sourceFile is the touched path a source file's label stands for: its own, or — for a source
// directory — the first touched file beneath it.
func sourceFile(label string, touched []string) string {
	p := path(label)
	under := ""
	for _, t := range touched {
		if t == p {
			return p
		}
		if strings.HasPrefix(t, p+"/") && (under == "" || t < under) {
			under = t
		}
	}
	return under
}

// content is a commit's touched files' contents, as a source file's part of its hash: "same" for
// what the change does not touch (it is the same at both commits), else each touched path at or
// beneath the file's (a source directory's files) with its SHA-256, or "missing".
type content struct {
	files   fs.FS
	touched []string
	memo    map[string]string
	err     *error
}

func contents(files fs.FS, touched []string) (*content, *error) {
	var err error
	return &content{files: files, touched: touched, memo: map[string]string{}, err: &err}, &err
}

func (c *content) of(label string) string {
	if !mainRepo(label) {
		return "external"
	}
	if s, ok := c.memo[label]; ok {
		return s
	}
	p := path(label)
	var parts []string
	for _, t := range c.touched {
		if t != p && !strings.HasPrefix(t, p+"/") {
			continue
		}
		data, err := fs.ReadFile(c.files, t)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			parts = append(parts, t+" missing")
		case err != nil:
			// A directory at a touched path holds files the change touches by their own paths.
			if info, serr := fs.Stat(c.files, t); serr == nil && info.IsDir() {
				continue
			}
			if *c.err == nil {
				*c.err = fmt.Errorf("reading %s: %w", t, err)
			}
			parts = append(parts, t+" unreadable")
		default:
			sum := sha256.Sum256(data)
			parts = append(parts, t+" "+hex.EncodeToString(sum[:]))
		}
	}
	s := "same"
	if len(parts) > 0 {
		sort.Strings(parts)
		s = strings.Join(parts, "\n")
	}
	c.memo[label] = s
	return s
}
