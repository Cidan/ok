// Package workflow parses, validates, and resolves ok's workflow files: the
// `.ok/workflow/*.yaml` files in which a directory declares the jobs its changes run beyond the
// built-in ones, and the deploys that put what lands into the cluster. A job's target is a Bazel
// label in the repository's main repository. It also carries the job context — what ok tells a
// job about the change it runs for — and expands its placeholders in a job's arguments, and reads
// a repository's own settings (.ok/repository.yaml).
//
// Pure Go, shared by the SCM (validating the workflow files an upload edits), automation's planner
// (selecting a change's jobs from the workflow files on main), and the deployer (reading every
// deploy on main).
package workflow

import (
	"fmt"
	gopath "path"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Phase is when a job runs.
type Phase string

const (
	// Pre runs on every patchset of a CL and blocks submit.
	Pre Phase = "pre"
	// Post runs at submit on the CL's candidate and gates main.
	Post Phase = "post"
	// DeployPhase names the deploys of a workflow file (Deploy): what runs after a commit
	// lands, not a phase jobs are selected for.
	DeployPhase Phase = "deploy"
)

// Job is one workflow job: a Bazel target that must succeed, with its arguments.
type Job struct {
	// File is the workflow file that defines it, repository-relative.
	File   string
	Phase  Phase
	Name   string
	Target string // a Bazel label, canonical: //package:name
	// Args are the job's arguments, placeholders unexpanded.
	Args []string
	Line int // the job's line in File
}

// ID is the job's identity: its defining file, phase, and name. Two directories that include
// the same file bring in the same jobs, which count once.
func (j Job) ID() string {
	return "//" + j.File + ":" + string(j.Phase) + ":" + j.Name
}

// Include is one `include:` entry.
type Include struct {
	Path string // the included file, repository-relative
	Line int
}

// File is one parsed workflow file.
type File struct {
	Path     string
	Includes []Include
	Jobs     []Job    // in file order
	Deploys  []Deploy // in file order
}

// Deploy is one deploy: the migrations it applies, then the images it rolls into one workload.
type Deploy struct {
	File string // the workflow file that defines it, repository-relative
	Name string
	Line int
	// Migrate are runnable targets speaking ok's migrate protocol, applied in order.
	Migrate []Ref
	// Images are the containers it rolls, in file order, each with the rules_oci oci_push target
	// that pushes its image.
	Images []Image
	// Workload is what the images roll; nil for a deploy that only migrates.
	Workload *Workload
	// Group is the service group the workload belongs to, on the Services pages: services that
	// make up one system, across repositories (one namespace, by name). Empty: the service is a
	// group of its own. Only a deploy with a workload — a service — has one.
	Group string
	// Alone is a deploy whose deployment runs while no other does: one whose roll interrupts what
	// other services need, such as a database's. It takes a wave of its own after the rest of its
	// plan's, and no other deployment, of any plan, runs beside it.
	Alone bool
}

// ID is the deploy's identity: its defining file and name.
func (d Deploy) ID() string {
	return "//" + d.File + ":" + string(DeployPhase) + ":" + d.Name
}

// Ref is one target a stanza names, with its line.
type Ref struct {
	Target string
	Line   int
}

// Image is one container a deploy rolls, and the oci_push target that pushes its image.
type Image struct {
	Container string
	Target    string
	Line      int
}

// WorkloadKind is the kind of workload a deploy rolls.
type WorkloadKind string

const (
	Deployment  WorkloadKind = "deployment"
	StatefulSet WorkloadKind = "statefulset"
)

// Workload is the Kubernetes workload a deploy rolls.
type Workload struct {
	Namespace string
	Kind      WorkloadKind
	Name      string
	Line      int
}

// String is the workload as people name it: "deployment web/web".
func (w Workload) String() string {
	return string(w.Kind) + " " + w.Namespace + "/" + w.Name
}

// Error is one problem in a workflow file.
type Error struct {
	Path string
	Line int // 1-based; 0 when the problem is not line-specific
	Msg  string
}

func (e Error) Error() string {
	return fmt.Sprintf("%s:%d: %s", e.Path, e.Line, e.Msg)
}

// workflowDir is the directory, inside the one declaring automation, that holds its
// workflow files.
const workflowDir = ".ok/workflow"

// IsWorkflowFile reports whether a repository-relative path names a workflow file: a
// `*.yaml` directly in a `.ok/workflow/` directory.
func IsWorkflowFile(p string) bool {
	dir, base := gopath.Split(p)
	if !strings.HasSuffix(base, ".yaml") || base == ".yaml" {
		return false
	}
	dir = strings.TrimSuffix(dir, "/")
	return dir == workflowDir || strings.HasSuffix(dir, "/"+workflowDir)
}

// DeclaringDir is the directory a workflow file declares automation for, repository-relative:
// "server" for "server/.ok/workflow/server.yaml", "" for the root's.
func DeclaringDir(p string) string {
	dir := gopath.Dir(p)
	if dir == workflowDir {
		return ""
	}
	return strings.TrimSuffix(dir, "/"+workflowDir)
}

// jobName is a job's name: short, lowercase, and safe in an identity.
var jobName = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)

// Parse parses one workflow file at a repository-relative path. It returns what parsed and
// every problem found, in line order.
func Parse(path string, data []byte) (*File, []Error) {
	f := &File{Path: path}
	p := &parser{path: path}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		p.errorf(0, "not valid YAML: %v", err)
		return f, p.errs
	}
	if len(doc.Content) == 0 {
		return f, nil // an empty file declares nothing
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		p.errorf(root.Line, "a workflow file is a mapping of include, pre, post, and deploy")
		return f, p.errs
	}
	for _, kv := range p.pairs(root) {
		switch kv.key.Value {
		case "include":
			f.Includes = p.includes(kv.value)
		case string(Pre), string(Post):
			f.Jobs = append(f.Jobs, p.jobs(Phase(kv.key.Value), kv.value)...)
		case string(DeployPhase):
			f.Deploys = p.deploys(kv.value)
		default:
			p.errorf(kv.key.Line, "unknown key %q (a workflow file has include, pre, post, and deploy)", kv.key.Value)
		}
	}
	return f, p.errs
}

type parser struct {
	path string
	errs []Error
}

func (p *parser) errorf(line int, format string, args ...any) {
	p.errs = append(p.errs, Error{Path: p.path, Line: line, Msg: fmt.Sprintf(format, args...)})
}

type pair struct{ key, value *yaml.Node }

// pairs returns a mapping's entries, reporting a key that is not a plain scalar or appears
// twice.
func (p *parser) pairs(m *yaml.Node) []pair {
	seen := map[string]bool{}
	var out []pair
	for i := 0; i+1 < len(m.Content); i += 2 {
		k, v := m.Content[i], m.Content[i+1]
		if k.Kind != yaml.ScalarNode {
			p.errorf(k.Line, "a key must be a plain name")
			continue
		}
		if seen[k.Value] {
			p.errorf(k.Line, "%q appears twice", k.Value)
			continue
		}
		seen[k.Value] = true
		out = append(out, pair{k, v})
	}
	return out
}

func (p *parser) includes(n *yaml.Node) []Include {
	if n.Kind != yaml.SequenceNode {
		p.errorf(n.Line, "include is a list of workflow files, each //path/.ok/workflow/name.yaml")
		return nil
	}
	var out []Include
	for _, e := range n.Content {
		if e.Kind != yaml.ScalarNode {
			p.errorf(e.Line, "an include is a //path/.ok/workflow/name.yaml")
			continue
		}
		target, ok := strings.CutPrefix(e.Value, "//")
		if !ok || !IsWorkflowFile(target) || gopath.Clean(target) != target {
			p.errorf(e.Line, "include %q is not a workflow file, //path/.ok/workflow/name.yaml", e.Value)
			continue
		}
		out = append(out, Include{Path: target, Line: e.Line})
	}
	return out
}

func (p *parser) jobs(phase Phase, n *yaml.Node) []Job {
	if n.Kind != yaml.MappingNode {
		p.errorf(n.Line, "%s is a mapping of job names to jobs", phase)
		return nil
	}
	var out []Job
	for _, kv := range p.pairs(n) {
		name := kv.key.Value
		if !jobName.MatchString(name) {
			p.errorf(kv.key.Line, "job name %q must be lowercase letters, digits, and _.- (at most 64, starting with a letter or digit)", name)
			continue
		}
		if j, ok := p.job(phase, name, kv); ok {
			out = append(out, j)
		}
	}
	return out
}

func (p *parser) job(phase Phase, name string, kv pair) (Job, bool) {
	j := Job{File: p.path, Phase: phase, Name: name, Line: kv.key.Line}
	if kv.value.Kind != yaml.MappingNode {
		p.errorf(kv.value.Line, "job %s is a mapping with a target (and optional args)", name)
		return j, false
	}
	ok := true
	for _, f := range p.pairs(kv.value) {
		switch f.key.Value {
		case "target":
			if f.value.Kind != yaml.ScalarNode || f.value.Tag == "!!null" {
				p.errorf(f.value.Line, "job %s: target is a Bazel label, //package:name", name)
				ok = false
				continue
			}
			target, err := CanonicalLabel(f.value.Value)
			if err != nil {
				p.errorf(f.value.Line, "job %s: %v", name, err)
				ok = false
				continue
			}
			j.Target = target
		case "args":
			if f.value.Kind != yaml.SequenceNode {
				p.errorf(f.value.Line, "job %s: args is a list of strings", name)
				ok = false
				continue
			}
			for _, a := range f.value.Content {
				if a.Kind != yaml.ScalarNode {
					p.errorf(a.Line, "job %s: an argument is a string", name)
					ok = false
					continue
				}
				if _, err := parseArg(a.Value); err != nil {
					p.errorf(a.Line, "job %s: argument %q: %v", name, a.Value, err)
					ok = false
					continue
				}
				j.Args = append(j.Args, a.Value)
			}
		default:
			p.errorf(f.key.Line, "job %s: unknown field %q (a job has target and args)", name, f.key.Value)
			ok = false
		}
	}
	if j.Target == "" && ok {
		p.errorf(kv.key.Line, "job %s has no target", name)
		ok = false
	}
	return j, ok
}

func (p *parser) deploys(n *yaml.Node) []Deploy {
	if n.Kind != yaml.MappingNode {
		p.errorf(n.Line, "deploy is a mapping of deploy names to deploys")
		return nil
	}
	var out []Deploy
	for _, kv := range p.pairs(n) {
		name := kv.key.Value
		if !jobName.MatchString(name) {
			p.errorf(kv.key.Line, "deploy name %q must be lowercase letters, digits, and _.- (at most 64, starting with a letter or digit)", name)
			continue
		}
		if d, ok := p.deploy(name, kv); ok {
			out = append(out, d)
		}
	}
	return out
}

// kubeLabel is a Kubernetes DNS-1123 label: a namespace or a container name.
var kubeLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// workloadName is a workload's name: a DNS-1123 label short enough that its green copy,
// <name>-green, is one too.
var workloadName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,55}[a-z0-9])?$`)

func (p *parser) deploy(name string, kv pair) (Deploy, bool) {
	d := Deploy{File: p.path, Name: name, Line: kv.key.Line}
	if kv.value.Kind != yaml.MappingNode {
		p.errorf(kv.value.Line, "deploy %s is a mapping of migrate, images, workload, group, and alone", name)
		return d, false
	}
	ok := true
	fail := func(line int, format string, args ...any) {
		p.errorf(line, "deploy %s: "+format, append([]any{name}, args...)...)
		ok = false
	}
	for _, f := range p.pairs(kv.value) {
		switch f.key.Value {
		case "migrate":
			if f.value.Kind != yaml.SequenceNode || len(f.value.Content) == 0 {
				fail(f.value.Line, "migrate is a list of runnable targets speaking ok's migrate protocol")
				continue
			}
			seen := map[string]bool{}
			for _, e := range f.value.Content {
				if e.Kind != yaml.ScalarNode || e.Tag == "!!null" {
					fail(e.Line, "a migration is a Bazel label, //package:name")
					continue
				}
				target, err := CanonicalLabel(e.Value)
				if err != nil {
					fail(e.Line, "%v", err)
					continue
				}
				if seen[target] {
					fail(e.Line, "%s is migrated twice", target)
					continue
				}
				seen[target] = true
				d.Migrate = append(d.Migrate, Ref{Target: target, Line: e.Line})
			}
		case "images":
			if f.value.Kind != yaml.MappingNode || len(f.value.Content) == 0 {
				fail(f.value.Line, "images is a mapping of container names to oci_push targets")
				continue
			}
			for _, c := range p.pairs(f.value) {
				if !kubeLabel.MatchString(c.key.Value) {
					fail(c.key.Line, "container %q is not a container name (lowercase letters, digits, and -, at most 63)", c.key.Value)
					continue
				}
				if c.value.Kind != yaml.ScalarNode || c.value.Tag == "!!null" {
					fail(c.value.Line, "container %s's image is an oci_push target, //package:name", c.key.Value)
					continue
				}
				target, err := CanonicalLabel(c.value.Value)
				if err != nil {
					fail(c.value.Line, "container %s: %v", c.key.Value, err)
					continue
				}
				d.Images = append(d.Images, Image{Container: c.key.Value, Target: target, Line: c.key.Line})
			}
		case "workload":
			w, good := p.workload(name, f.value)
			if !good {
				ok = false
				continue
			}
			d.Workload = w
		case "group":
			if f.value.Kind != yaml.ScalarNode || f.value.Tag == "!!null" || !jobName.MatchString(f.value.Value) {
				fail(f.value.Line, "group %q must be lowercase letters, digits, and _.- (at most 64, starting with a letter or digit)", f.value.Value)
				continue
			}
			d.Group = f.value.Value
		case "alone":
			if f.value.Kind != yaml.ScalarNode || f.value.Tag != "!!bool" {
				fail(f.value.Line, "alone is true or false")
				continue
			}
			d.Alone = f.value.Value == "true"
		default:
			fail(f.key.Line, "unknown field %q (a deploy has migrate, images, workload, group, and alone)", f.key.Value)
		}
	}
	if !ok {
		return d, false
	}
	switch {
	case len(d.Images) > 0 && d.Workload == nil:
		p.errorf(kv.key.Line, "deploy %s has images but no workload to roll them into", name)
		return d, false
	case d.Workload != nil && len(d.Images) == 0:
		p.errorf(kv.key.Line, "deploy %s has a workload but no images to roll into it", name)
		return d, false
	case len(d.Migrate) == 0 && len(d.Images) == 0:
		p.errorf(kv.key.Line, "deploy %s deploys nothing: it needs migrate, or images and a workload", name)
		return d, false
	case d.Group != "" && d.Workload == nil:
		p.errorf(kv.key.Line, "deploy %s has a group but no workload: only a service — a deploy with a workload — is grouped", name)
		return d, false
	}
	return d, true
}

func (p *parser) workload(name string, n *yaml.Node) (*Workload, bool) {
	if n.Kind != yaml.MappingNode {
		p.errorf(n.Line, "deploy %s: workload is a mapping of namespace and deployment or statefulset", name)
		return nil, false
	}
	w := &Workload{Line: n.Line}
	ok := true
	for _, f := range p.pairs(n) {
		if f.value.Kind != yaml.ScalarNode || f.value.Tag == "!!null" {
			p.errorf(f.value.Line, "deploy %s: workload's %s is a name", name, f.key.Value)
			ok = false
			continue
		}
		switch f.key.Value {
		case "namespace":
			if !kubeLabel.MatchString(f.value.Value) {
				p.errorf(f.value.Line, "deploy %s: namespace %q is not a namespace name (lowercase letters, digits, and -, at most 63)", name, f.value.Value)
				ok = false
				continue
			}
			w.Namespace = f.value.Value
		case string(Deployment), string(StatefulSet):
			if w.Kind != "" {
				p.errorf(f.key.Line, "deploy %s: a workload is one deployment or one statefulset", name)
				ok = false
				continue
			}
			if !workloadName.MatchString(f.value.Value) {
				p.errorf(f.value.Line, "deploy %s: %s %q is not a workload name (lowercase letters, digits, and -, at most 57)", name, f.key.Value, f.value.Value)
				ok = false
				continue
			}
			w.Kind, w.Name = WorkloadKind(f.key.Value), f.value.Value
		default:
			p.errorf(f.key.Line, "deploy %s: unknown workload field %q (a workload has namespace and deployment or statefulset)", name, f.key.Value)
			ok = false
		}
	}
	if !ok {
		return nil, false
	}
	switch {
	case w.Namespace == "":
		p.errorf(n.Line, "deploy %s: the workload has no namespace", name)
		return nil, false
	case w.Kind == "":
		p.errorf(n.Line, "deploy %s: the workload names no deployment or statefulset", name)
		return nil, false
	}
	return w, true
}

// CanonicalLabel checks that a target is a Bazel label in the main repository and returns it
// canonical, //package:name: //package alone is //package:last, as Bazel reads it, and the root
// package is //:name. A label in another repository (@repo//…) is refused: a job's target is
// the repository's own.
func CanonicalLabel(s string) (string, error) {
	bad := fmt.Errorf("target %q is not a Bazel label in the main repository, //package:name", s)
	body, ok := strings.CutPrefix(s, "//")
	if !ok {
		return "", bad
	}
	pkg, name, hasName := strings.Cut(body, ":")
	if !hasName {
		if pkg == "" {
			return "", bad
		}
		name = gopath.Base(pkg)
	}
	if pkg != "" {
		for _, seg := range strings.Split(pkg, "/") {
			if seg == "" || seg == "." || seg == ".." || !onlyOf(seg, packageChar) {
				return "", bad
			}
		}
	}
	if name == "" || !onlyOf(name, targetChar) {
		return "", bad
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", bad
		}
	}
	return "//" + pkg + ":" + name, nil
}

// packageChar reports whether r may be in a Bazel package's name.
func packageChar(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
		strings.ContainsRune("-._@~+", r)
}

// targetChar reports whether r may be in a Bazel target's name.
func targetChar(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
		strings.ContainsRune("!%-@^_\"#$&'()*+,;<=>?[]{|}~/.", r)
}

func onlyOf(s string, ok func(rune) bool) bool {
	for _, r := range s {
		if !ok(r) {
			return false
		}
	}
	return true
}
