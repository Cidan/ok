// Package bazel is ok's build-system driver for Bazel workspaces: what a change affects, found by
// comparing every target's hash at the change's base and at the change, as bazel-diff and
// target-determinator do; what each target is; and the Bazel commands each job is.
package bazel

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
)

// QueryArgs are the arguments of the query whose answer a Graph reads: every target the
// repository's targets depend on, each rule with Bazel's hash of its attributes and of its rule's
// Starlark definition, with every .bzl it loads ($internal_attr_hash), and nothing that names
// where the checkout is.
var QueryArgs = []string{
	"query", "deps(//...)",
	"--output=streamed_jsonproto",
	"--proto:include_synthetic_attribute_hash",
	"--noproto:locations",
	"--noproto:default_values",
}

// Graph is a commit's build graph, as `bazel query` (QueryArgs) answers.
type Graph struct {
	nodes map[string]*node
}

// node is one target.
type node struct {
	label string
	kind  string // "rule", "source file", "generated file", "package group", "environment group"
	class string // a rule's class
	// own is what the target is, apart from its inputs: a rule's class and attribute hash, a
	// group's declaration. A source file's is its content, which the commit's checkout holds.
	own    string
	inputs []string
	// attrs are a rule's attributes that hold one string or label, as the BUILD file sets them.
	attrs map[string]string
}

// queryTarget is one line of `bazel query --output=streamed_jsonproto`: a Target message.
type queryTarget struct {
	Type string `json:"type"`
	Rule *struct {
		Name      string `json:"name"`
		RuleClass string `json:"ruleClass"`
		Attribute []struct {
			Name        string `json:"name"`
			StringValue string `json:"stringValue"`
		} `json:"attribute"`
		RuleInput []string `json:"ruleInput"`
	} `json:"rule"`
	SourceFile *struct {
		Name string `json:"name"`
	} `json:"sourceFile"`
	GeneratedFile *struct {
		Name           string `json:"name"`
		GeneratingRule string `json:"generatingRule"`
	} `json:"generatedFile"`
	PackageGroup     json.RawMessage `json:"packageGroup"`
	EnvironmentGroup json.RawMessage `json:"environmentGroup"`
}

// ParseGraph reads a query's answer (QueryArgs).
func ParseGraph(r io.Reader) (*Graph, error) {
	g := &Graph{nodes: map[string]*node{}}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for n := 1; sc.Scan(); n++ {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var t queryTarget
		if err := json.Unmarshal(line, &t); err != nil {
			return nil, fmt.Errorf("the query's line %d: %w", n, err)
		}
		nd, err := t.node()
		if err != nil {
			return nil, fmt.Errorf("the query's line %d: %w", n, err)
		}
		if _, dup := g.nodes[nd.label]; dup {
			return nil, fmt.Errorf("the query names %s twice", nd.label)
		}
		g.nodes[nd.label] = nd
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return g, nil
}

func (t *queryTarget) node() (*node, error) {
	switch {
	case t.Type == "RULE" && t.Rule != nil:
		hash := ""
		for _, a := range t.Rule.Attribute {
			if a.Name == "$internal_attr_hash" {
				hash = a.StringValue
			}
		}
		if hash == "" {
			return nil, fmt.Errorf("%s has no $internal_attr_hash: the query needs --proto:include_synthetic_attribute_hash", t.Rule.Name)
		}
		attrs := map[string]string{}
		for _, a := range t.Rule.Attribute {
			if a.StringValue != "" && !strings.HasPrefix(a.Name, "$") {
				attrs[a.Name] = a.StringValue
			}
		}
		return &node{label: t.Rule.Name, kind: "rule", class: t.Rule.RuleClass, own: t.Rule.RuleClass + "\n" + hash, inputs: t.Rule.RuleInput, attrs: attrs}, nil
	case t.Type == "SOURCE_FILE" && t.SourceFile != nil:
		return &node{label: t.SourceFile.Name, kind: "source file"}, nil
	case t.Type == "GENERATED_FILE" && t.GeneratedFile != nil:
		return &node{label: t.GeneratedFile.Name, kind: "generated file", own: "generated", inputs: []string{t.GeneratedFile.GeneratingRule}}, nil
	case t.Type == "PACKAGE_GROUP" && t.PackageGroup != nil:
		return groupNode("package group", t.PackageGroup)
	case t.Type == "ENVIRONMENT_GROUP" && t.EnvironmentGroup != nil:
		return groupNode("environment group", t.EnvironmentGroup)
	}
	return nil, fmt.Errorf("a target of type %q the driver does not know", t.Type)
}

// groupNode is a package or environment group: its declaration is what it is.
func groupNode(kind string, body json.RawMessage) (*node, error) {
	var named struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(body, &named); err != nil || named.Name == "" {
		return nil, fmt.Errorf("a %s without a name", kind)
	}
	return &node{label: named.Name, kind: kind, own: string(body)}, nil
}

// Len is how many targets the graph holds.
func (g *Graph) Len() int { return len(g.nodes) }

// Attr is a rule's attribute that holds one string or label, as its BUILD file sets it.
func (g *Graph) Attr(label, name string) (string, bool) {
	nd := g.nodes[label]
	if nd == nil {
		return "", false
	}
	v, ok := nd.attrs[name]
	return v, ok
}

// Owners are the repository's rules that read any of the files at paths (repository-relative) as
// a source, sorted: what a linter of those files analyzes.
func (g *Graph) Owners(paths []string) []string {
	want := map[string]bool{}
	for _, p := range paths {
		want[p] = true
	}
	var out []string
	for label, nd := range g.nodes {
		if nd.kind != "rule" || !mainRepo(label) {
			continue
		}
		for _, in := range nd.inputs {
			if src := g.nodes[in]; src != nil && src.kind == "source file" && mainRepo(in) && want[path(in)] {
				out = append(out, label)
				break
			}
		}
	}
	slices.Sort(out)
	return out
}

// mainRepo reports whether a label is the repository's own: //package:name, not @repo//….
func mainRepo(label string) bool { return strings.HasPrefix(label, "//") }

// path is the repository-relative path of a main repository's source file: //pkg:sub/f is
// pkg/sub/f, //:f is f.
func path(label string) string {
	pkg, name, _ := strings.Cut(strings.TrimPrefix(label, "//"), ":")
	if pkg == "" {
		return name
	}
	return pkg + "/" + name
}

// hashes is every target's hash: what it is (own, or a source file's content, from content) and
// the hashes of its inputs, sorted. A target's hash differs between two commits exactly when it,
// or anything it depends on, differs.
func (g *Graph) hashes(content func(label string) string) map[string]string {
	out := make(map[string]string, len(g.nodes))
	var visit func(label string, stack map[string]bool) string
	visit = func(label string, stack map[string]bool) string {
		if h, ok := out[label]; ok {
			return h
		}
		nd, ok := g.nodes[label]
		if !ok {
			return "absent " + label
		}
		if stack[label] {
			return "cycle " + label // Bazel's graph is acyclic; this only keeps a bad answer finite
		}
		stack[label] = true
		h := sha256.New()
		io.WriteString(h, nd.kind+"\n"+label+"\n")
		if nd.kind == "source file" {
			io.WriteString(h, content(label))
		} else {
			io.WriteString(h, nd.own)
		}
		in := make([]string, 0, len(nd.inputs))
		for _, i := range nd.inputs {
			in = append(in, i+"="+visit(i, stack))
		}
		sort.Strings(in)
		for _, i := range in {
			io.WriteString(h, "\n"+i)
		}
		delete(stack, label)
		sum := hex.EncodeToString(h.Sum(nil))
		out[label] = sum
		return sum
	}
	for label := range g.nodes {
		visit(label, map[string]bool{})
	}
	return out
}
