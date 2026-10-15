package workflow

import (
	"reflect"
	"strings"
	"testing"
)

func TestIsWorkflowFile(t *testing.T) {
	for p, want := range map[string]bool{
		".ok/workflow/ci.yaml":                 true,
		"apps/server/.ok/workflow/server.yaml": true,
		"apps/server/.ok/workflow/server.yml":  false,
		"apps/server/.ok/workflow/.yaml":       false,
		"apps/server/.ok/workflow/x/a.yaml":    false,
		"apps/server/.ok/a.yaml":               false,
		"apps/server/workflow/a.yaml":          false,
		"apps/x.ok/workflow/a.yaml":            false,
	} {
		if got := IsWorkflowFile(p); got != want {
			t.Errorf("IsWorkflowFile(%q) = %v, want %v", p, got, want)
		}
	}
	if d := DeclaringDir("apps/server/.ok/workflow/server.yaml"); d != "apps/server" {
		t.Errorf("DeclaringDir = %q", d)
	}
	if d := DeclaringDir(".ok/workflow/ci.yaml"); d != "" {
		t.Errorf("DeclaringDir of the root's = %q", d)
	}
}

func TestParse(t *testing.T) {
	src := `include:
  - //.ok/workflow/common.yaml
pre:
  build:
    target: //apps/server:ok
  schema-comments:
    target: //tools/sdlcheck:sdlcheck
    args: ["--require-comments", "{changed_files:**/*.sdl}"]
post:
  e2e:
    target: //apps/server/e2e:e2e
`
	f, errs := Parse("apps/server/.ok/workflow/server.yaml", []byte(src))
	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	if want := []Include{{Path: ".ok/workflow/common.yaml", Line: 2}}; !reflect.DeepEqual(f.Includes, want) {
		t.Errorf("includes = %+v", f.Includes)
	}
	var ids []string
	for _, j := range f.Jobs {
		ids = append(ids, j.ID())
	}
	want := []string{
		"//apps/server/.ok/workflow/server.yaml:pre:build",
		"//apps/server/.ok/workflow/server.yaml:pre:schema-comments",
		"//apps/server/.ok/workflow/server.yaml:post:e2e",
	}
	if !reflect.DeepEqual(ids, want) {
		t.Errorf("jobs = %v, want %v", ids, want)
	}
	if j := f.Jobs[1]; j.Target != "//tools/sdlcheck:sdlcheck" || !reflect.DeepEqual(j.Args, []string{"--require-comments", "{changed_files:**/*.sdl}"}) || j.Line != 6 {
		t.Errorf("schema-comments = %+v", j)
	}

	if f, errs := Parse(".ok/workflow/empty.yaml", nil); len(errs) != 0 || len(f.Jobs) != 0 {
		t.Errorf("an empty file: %+v, %v", f, errs)
	}
}

func TestParseErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		src  string
		line int
		msg  string
	}{
		"not yaml":            {"pre: [", 0, "not valid YAML"},
		"not a mapping":       {"- a\n", 1, "a workflow file is a mapping"},
		"unknown key":         {"on:\n  push: {}\n", 1, `unknown key "on"`},
		"duplicate key":       {"pre:\n  a:\n    target: //a:b\npre:\n  b:\n    target: //a:b\n", 4, `"pre" appears twice`},
		"duplicate job":       {"pre:\n  a:\n    target: //a:b\n  a:\n    target: //a:c\n", 4, `"a" appears twice`},
		"include not list":    {"include: //.ok/workflow/a.yaml\n", 1, "include is a list"},
		"include not file":    {"include:\n  - //apps/server/ci.yaml\n", 2, "is not a workflow file"},
		"include relative":    {"include:\n  - .ok/workflow/a.yaml\n", 2, "is not a workflow file"},
		"include unclean":     {"include:\n  - //a/../.ok/workflow/a.yaml\n", 2, "is not a workflow file"},
		"bad job name":        {"pre:\n  Build:\n    target: //a:b\n", 2, `job name "Build"`},
		"job not mapping":     {"pre:\n  a: //a:b\n", 2, "job a is a mapping"},
		"no target":           {"pre:\n  a:\n    args: [x]\n", 2, "job a has no target"},
		"target not label":    {"pre:\n  a:\n    target: apps/a\n", 3, `target "apps/a" is not a Bazel label in the main repository`},
		"target no name":      {"pre:\n  a:\n    target: '//apps/a:'\n", 3, "is not a Bazel label"},
		"target elsewhere":    {"pre:\n  a:\n    target: '@other//apps/a:a'\n", 3, "is not a Bazel label in the main repository"},
		"target bad chars":    {"pre:\n  a:\n    target: //apps/a b:c\n", 3, "is not a Bazel label"},
		"unknown field":       {"pre:\n  a:\n    target: //a:b\n    timeout: 5\n", 4, `unknown field "timeout"`},
		"args not list":       {"pre:\n  a:\n    target: //a:b\n    args: -v\n", 4, "args is a list"},
		"unknown placeholder": {"pre:\n  a:\n    target: //a:b\n    args: [\"{files}\"]\n", 4, "unknown placeholder {files}"},
		"list inside arg":     {"pre:\n  a:\n    target: //a:b\n    args: [\"--f={changed_files}\"]\n", 4, "must be the whole argument"},
		"glob on scalar":      {"pre:\n  a:\n    target: //a:b\n    args: [\"{cl:*}\"]\n", 4, "takes no glob"},
		"unclosed":            {"pre:\n  a:\n    target: //a:b\n    args: [\"{cl\"]\n", 4, "never closed"},
		"stray close":         {"pre:\n  a:\n    target: //a:b\n    args: [\"a}\"]\n", 4, "closes no placeholder"},
		"bad glob":            {"pre:\n  a:\n    target: //a:b\n    args: [\"{changed_files:[a}\"]\n", 4, "never closed"},
		"phase not mapping":   {"pre: [a]\n", 1, "pre is a mapping"},

		"deploy not mapping":     {"deploy: [a]\n", 1, "deploy is a mapping of deploy names"},
		"deploy bad name":        {"deploy:\n  Svc:\n    migrate: [//a:b]\n", 2, `deploy name "Svc"`},
		"deploy not a mapping":   {"deploy:\n  a: //a:b\n", 2, "deploy a is a mapping"},
		"deploy nothing":         {"deploy:\n  a: {}\n", 2, "deploy a deploys nothing"},
		"deploy unknown field":   {"deploy:\n  a:\n    migrate: [//a:b]\n    replicas: 3\n", 4, `unknown field "replicas"`},
		"migrate not list":       {"deploy:\n  a:\n    migrate: //a:b\n", 3, "migrate is a list"},
		"migrate empty":          {"deploy:\n  a:\n    migrate: []\n", 3, "migrate is a list"},
		"migrate not label":      {"deploy:\n  a:\n    migrate: [apps/a]\n", 3, "is not a Bazel label"},
		"migrate twice":          {"deploy:\n  a:\n    migrate: [//a:b, //a:b]\n", 3, "//a:b is migrated twice"},
		"images not mapping":     {"deploy:\n  a:\n    images: [//a:push]\n", 3, "images is a mapping"},
		"images bad container":   {"deploy:\n  a:\n    images:\n      Web: //a:push\n", 4, `container "Web" is not a container name`},
		"images not label":       {"deploy:\n  a:\n    images:\n      web: a:push\n", 4, "container web: target"},
		"images no workload":     {"deploy:\n  a:\n    images:\n      web: //a:push\n", 2, "has images but no workload"},
		"workload no images":     {"deploy:\n  a:\n    migrate: [//a:b]\n    workload: {namespace: a, deployment: a}\n", 2, "has a workload but no images"},
		"workload not mapping":   {"deploy:\n  a:\n    images: {web: //a:push}\n    workload: a/a\n", 4, "workload is a mapping"},
		"workload no namespace":  {"deploy:\n  a:\n    images: {web: //a:push}\n    workload: {deployment: a}\n", 4, "the workload has no namespace"},
		"workload no kind":       {"deploy:\n  a:\n    images: {web: //a:push}\n    workload: {namespace: a}\n", 4, "names no deployment or statefulset"},
		"workload both kinds":    {"deploy:\n  a:\n    images: {web: //a:push}\n    workload: {namespace: a, deployment: a, statefulset: a}\n", 4, "one deployment or one statefulset"},
		"workload bad namespace": {"deploy:\n  a:\n    images: {web: //a:push}\n    workload: {namespace: A, deployment: a}\n", 4, `namespace "A" is not a namespace name`},
		"workload name too long": {"deploy:\n  a:\n    images: {web: //a:push}\n    workload: {namespace: a, deployment: " + strings.Repeat("a", 58) + "}\n", 4, "is not a workload name"},
		"workload unknown field": {"deploy:\n  a:\n    images: {web: //a:push}\n    workload: {namespace: a, daemonset: a}\n", 4, `unknown workload field "daemonset"`},
		"group no workload":      {"deploy:\n  a:\n    migrate: [//a:b]\n    group: g\n", 2, "has a group but no workload"},
		"group bad name":         {"deploy:\n  a:\n    images: {web: //a:push}\n    workload: {namespace: a, deployment: a}\n    group: Platform\n", 5, `group "Platform" must be lowercase`},
		"group not a name":       {"deploy:\n  a:\n    images: {web: //a:push}\n    workload: {namespace: a, deployment: a}\n    group: [a, b]\n", 5, "must be lowercase"},
		"group twice":            {"deploy:\n  a:\n    images: {web: //a:push}\n    workload: {namespace: a, deployment: a}\n    group: a\n    group: b\n", 6, `"group" appears twice`},
		"alone not a bool":       {"deploy:\n  a:\n    migrate: [//a:b]\n    alone: yes please\n", 4, "alone is true or false"},
		"alone a string":         {"deploy:\n  a:\n    migrate: [//a:b]\n    alone: \"true\"\n", 4, "alone is true or false"},
	} {
		t.Run(name, func(t *testing.T) {
			_, errs := Parse("x/.ok/workflow/a.yaml", []byte(tc.src))
			if len(errs) == 0 {
				t.Fatalf("no errors for %q", tc.src)
			}
			e := errs[0]
			if e.Path != "x/.ok/workflow/a.yaml" || e.Line != tc.line || !strings.Contains(e.Msg, tc.msg) {
				t.Errorf("first error = %v, want line %d containing %q", e, tc.line, tc.msg)
			}
		})
	}
}

func TestParseDeploys(t *testing.T) {
	src := `deploy:
  web-api:
    migrate:
      - //apps/server/automation/schema:apply
      - //apps/server/automation:logs-apply
    images:
      web-api: //apps/server/automation:push
      sidecar: //apps/server/automation/sidecar:push
    workload:
      namespace: web-api
      deployment: web-api
    group: web
  database:
    images:
      postgres: //services/database:push
    workload:
      namespace: database
      statefulset: postgres
    alone: true
  canary:
    migrate: [//services/database/canary:apply]
`
	f, errs := Parse("apps/server/automation/.ok/workflow/automation.yaml", []byte(src))
	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	if len(f.Jobs) != 0 || len(f.Deploys) != 3 {
		t.Fatalf("parsed %+v", f)
	}
	a := f.Deploys[0]
	want := Deploy{
		File: "apps/server/automation/.ok/workflow/automation.yaml", Name: "web-api", Line: 2,
		Migrate:  []Ref{{"//apps/server/automation/schema:apply", 4}, {"//apps/server/automation:logs-apply", 5}},
		Images:   []Image{{"web-api", "//apps/server/automation:push", 7}, {"sidecar", "//apps/server/automation/sidecar:push", 8}},
		Workload: &Workload{Namespace: "web-api", Kind: Deployment, Name: "web-api", Line: 10},
		Group:    "web",
	}
	if !reflect.DeepEqual(a, want) {
		t.Errorf("web-api = %+v, want %+v", a, want)
	}
	if a.ID() != "//apps/server/automation/.ok/workflow/automation.yaml:deploy:web-api" || a.Workload.String() != "deployment web-api/web-api" {
		t.Errorf("identity %s, workload %s", a.ID(), a.Workload)
	}
	if g := f.Deploys[1]; g.Workload.Kind != StatefulSet || g.Workload.String() != "statefulset database/postgres" || len(g.Migrate) != 0 || g.Group != "" || !g.Alone {
		t.Errorf("database = %+v", g)
	}
	if c := f.Deploys[2]; c.Workload != nil || len(c.Images) != 0 || len(c.Migrate) != 1 || c.Alone {
		t.Errorf("a migration-only deploy = %+v", c)
	}
}

// Deploys count once, wherever they are declared, and two of one workload are refused.
func TestSetDeploys(t *testing.T) {
	s := NewSet(map[string][]byte{
		".ok/workflow/root.yaml":      []byte("deploy:\n  web:\n    images: {web: //web:push}\n    workload: {namespace: web, deployment: web}\n"),
		"a/.ok/workflow/a.yaml":       []byte("include: [//.ok/workflow/root.yaml]\ndeploy:\n  data:\n    migrate: [//a:apply]\n"),
		"b/.ok/workflow/b.yaml":       []byte("deploy:\n  web-again:\n    images: {web: //b:push}\n    workload:\n      namespace: web\n      deployment: web\n"),
		"c/.ok/workflow/c.yaml":       []byte("deploy:\n  web-set:\n    images: {web: //c:push}\n    workload: {namespace: web, statefulset: web}\n"),
		"d/.ok/workflow/not-yaml.txt": []byte("ignored"),
	})
	var ids []string
	for _, d := range s.Deploys() {
		ids = append(ids, d.ID())
	}
	want := []string{"//.ok/workflow/root.yaml:deploy:web", "//a/.ok/workflow/a.yaml:deploy:data",
		"//b/.ok/workflow/b.yaml:deploy:web-again", "//c/.ok/workflow/c.yaml:deploy:web-set"}
	if !reflect.DeepEqual(ids, want) {
		t.Errorf("deploys %v, want %v", ids, want)
	}
	problems := s.DeployProblems()
	if len(problems) != 1 || problems[0].Path != "b/.ok/workflow/b.yaml" || problems[0].Line != 5 ||
		!strings.Contains(problems[0].Msg, "deploy web-again rolls deployment web/web, which deploy web (//.ok/workflow/root.yaml:deploy:web) already rolls") {
		t.Errorf("problems %v", problems)
	}
}

// mapSource serves files from a map.
type mapSource map[string]string

func (m mapSource) Read(p string) ([]byte, bool, error) {
	d, ok := m[p]
	return []byte(d), ok, nil
}

func TestValidate(t *testing.T) {
	src := mapSource{
		".ok/workflow/common.yaml":          "pre:\n  lint:\n    target: //tools/x:x\n",
		".ok/workflow/broken.yaml":          "pre:\n  a:\n    target: nope\n",
		"apps/.ok/workflow/apps.yaml":       "include:\n  - //.ok/workflow/common.yaml\n",
		"apps/server/.ok/workflow/sib.yaml": "pre: {}\n",
	}
	good := "include:\n  - //.ok/workflow/common.yaml\n  - //apps/.ok/workflow/apps.yaml\npre:\n  build:\n    target: //apps/server:ok\n"
	if errs := Validate("apps/server/.ok/workflow/server.yaml", []byte(good), src); len(errs) != 0 {
		t.Errorf("valid file: %v", errs)
	}

	bad := strings.Join([]string{
		"include:",
		"  - //apps/server/.ok/workflow/sib.yaml", // the same directory: not upward
		"  - //apps/server/x/.ok/workflow/a.yaml", // below
		"  - //.ok/workflow/missing.yaml",
		"  - //.ok/workflow/broken.yaml",
		"pre:",
		"  a:",
		"    target: nope",
		"",
	}, "\n")
	errs := Validate("apps/server/.ok/workflow/server.yaml", []byte(bad), src)
	var got []string
	for _, e := range errs {
		got = append(got, e.Error())
	}
	want := []string{
		"apps/server/.ok/workflow/server.yaml:2: include //apps/server/.ok/workflow/sib.yaml is not in an ancestor of //apps/server: includes point strictly upward",
		"apps/server/.ok/workflow/server.yaml:3: include //apps/server/x/.ok/workflow/a.yaml is not in an ancestor of //apps/server: includes point strictly upward",
		"apps/server/.ok/workflow/server.yaml:4: include //.ok/workflow/missing.yaml does not exist",
		`apps/server/.ok/workflow/server.yaml:8: job a: target "nope" is not a Bazel label in the main repository, //package:name`,
		`.ok/workflow/broken.yaml:3: job a: target "nope" is not a Bazel label in the main repository, //package:name`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("errors:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestSet(t *testing.T) {
	s := NewSet(map[string][]byte{
		".ok/workflow/common.yaml":             []byte("pre:\n  lint:\n    target: //tools/x:x\n"),
		"apps/.ok/workflow/apps.yaml":          []byte("include:\n  - //.ok/workflow/common.yaml\npre:\n  apps:\n    target: //apps/a:a\n"),
		"apps/server/.ok/workflow/server.yaml": []byte("include:\n  - //.ok/workflow/common.yaml\n  - //apps/.ok/workflow/apps.yaml\npost:\n  e2e:\n    target: //apps/server/e2e:e2e\n"),
		"apps/server/.ok/workflow/more.yaml":   []byte("pre:\n  more:\n    target: //apps/server:more\n"),
		"tools/.ok/workflow/bad.yaml":          []byte("include:\n  - //tools/x/.ok/workflow/a.yaml\npre:\n  t:\n    target: //tools/t:t\n"),
		"apps/server/README.md":                []byte("not a workflow"),
	})
	for dir, want := range map[string]string{
		"apps/server/control/internal": "apps/server",
		"apps/server":                  "apps/server",
		"apps/other":                   "apps",
		"docs":                         "",
		"":                             "",
	} {
		got, ok := s.Governing(dir)
		if !ok || got != want {
			t.Errorf("Governing(%q) = %q, %v; want %q", dir, got, ok, want)
		}
	}

	ids := func(jobs []Job) []string {
		var out []string
		for _, j := range jobs {
			out = append(out, j.ID())
		}
		return out
	}
	// A directory's own files, then what they include, each once (common.yaml is included
	// twice).
	if got, want := ids(s.Jobs("apps/server")), []string{
		"//apps/server/.ok/workflow/more.yaml:pre:more",
		"//apps/server/.ok/workflow/server.yaml:post:e2e",
		"//.ok/workflow/common.yaml:pre:lint",
		"//apps/.ok/workflow/apps.yaml:pre:apps",
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("Jobs(apps/server) = %v, want %v", got, want)
	}
	// An include that does not point upward brings nothing in.
	if got, want := ids(s.Jobs("tools")), []string{"//tools/.ok/workflow/bad.yaml:pre:t"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Jobs(tools) = %v, want %v", got, want)
	}
	if n := len(s.All()); n != 5 {
		t.Errorf("All has %d jobs, want 5", n)
	}
	if got := s.Problems(); len(got) != 1 || got[0].Path != "tools/.ok/workflow/bad.yaml" || !strings.Contains(got[0].Msg, "strictly upward") {
		t.Errorf("Problems = %v", got)
	}
	if got, want := s.Includers([]string{".ok/workflow/common.yaml"}), []string{"apps/.ok/workflow/apps.yaml", "apps/server/.ok/workflow/server.yaml"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Includers = %v, want %v", got, want)
	}
}

func TestCanonicalLabel(t *testing.T) {
	for in, want := range map[string]string{
		"//server/e2e:e2e":       "//server/e2e:e2e",
		"//server/e2e":           "//server/e2e:e2e",
		"//:gazelle":             "//:gazelle",
		"//web:static/app.css":   "//web:static/app.css",
		"//tools/lint:lint+fix":  "//tools/lint:lint+fix",
		"//third_party/@npm:pkg": "//third_party/@npm:pkg",
	} {
		got, err := CanonicalLabel(in)
		if err != nil || got != want {
			t.Errorf("CanonicalLabel(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{
		"", "//", "//:", "server:ok", "@repo//server:ok", "@@repo+//server:ok", "//server:", "//a/../b:c",
		"//a//b:c", "//a:b:c", "//a:../c", "//a:b/./c", "//a b:c", "//a:b c",
	} {
		if got, err := CanonicalLabel(bad); err == nil {
			t.Errorf("CanonicalLabel(%q) = %q, want it refused", bad, got)
		}
	}
}
