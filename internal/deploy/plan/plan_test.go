package plan

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/workflow"
)

func deploy(file, name string, migrate []string, image, ns, workload string) workflow.Deploy {
	d := workflow.Deploy{File: file, Name: name}
	for _, m := range migrate {
		d.Migrate = append(d.Migrate, workflow.Ref{Target: m})
	}
	if image != "" {
		d.Images = []workflow.Image{{Container: name, Target: image}}
		d.Workload = &workflow.Workload{Namespace: ns, Kind: workflow.Deployment, Name: workload}
	}
	return d
}

func names(ds []Deployment) []string {
	var out []string
	for _, d := range ds {
		out = append(out, d.Deploy.Name)
	}
	return out
}

// A deploy is due when a change affects one of its targets, or edits its workflow file; the
// first plan makes every deploy due.
func TestDue(t *testing.T) {
	web := deploy("web/.ok/workflow/w.yaml", "web", nil, "//web:push", "web", "web")
	api := deploy("api/.ok/workflow/a.yaml", "api", []string{"//api/schema:apply"}, "//api:push", "api", "api")
	data := deploy("data/.ok/workflow/d.yaml", "data", []string{"//data:apply"}, "", "", "")
	all := []workflow.Deploy{web, api, data}
	aff := &Affected{Affected: []Target{
		{Label: "//web:push", Because: []Step{{Label: "//web:push", Via: "image"}, {Label: "//web:image", Via: "binary"}, {Label: "//web:web", Via: "deps"}, {Label: "//lib:lib", File: "lib/lib.go"}}},
		{Label: "//api/schema:apply", Because: []Step{{Label: "//api/schema:apply", Via: "schema"}, {Label: "//api/schema:schema", File: "api/schema/api.sdl"}}},
	}}
	got := Make(Input{Deploys: all, Changed: []string{"lib/lib.go", "api/schema/api.sdl"}, Affected: aff})
	if !reflect.DeepEqual(names(got), []string{"api", "web"}) {
		t.Fatalf("due %v", names(got))
	}
	for _, d := range got {
		switch d.Deploy.Name {
		case "web":
			if d.Why != "container web's image //web:push is affected: //web:push —image→ //web:image —binary→ //web:web —deps→ //lib:lib, which holds lib/lib.go" {
				t.Errorf("web: %s", d.Why)
			}
		case "api":
			if !strings.HasPrefix(d.Why, "its migration //api/schema:apply is affected") {
				t.Errorf("api: %s", d.Why)
			}
		}
	}
	if got := Make(Input{Deploys: all, Changed: []string{"data/.ok/workflow/d.yaml"}, Affected: &Affected{}}); !reflect.DeepEqual(names(got), []string{"data"}) {
		t.Errorf("an edited workflow file: %v", names(got))
	}
	if got := Make(Input{Deploys: all, Changed: []string{"README"}, Affected: &Affected{}}); len(got) != 0 {
		t.Errorf("an unrelated change: %v", names(got))
	}
	if got := Make(Input{Deploys: all, First: true}); len(got) != 3 || !strings.Contains(got[0].Why, "has not planned") {
		t.Errorf("the first plan: %+v", got)
	}
}

// Several deployments run in three waves — one, half the rest, the rest — multi-replica
// workloads first, then migrations, single replicas; and the deployer's own in a wave of its
// own after them.
func TestWaves(t *testing.T) {
	var ds []workflow.Deploy
	for _, n := range []string{"a", "b", "c", "d", "e", "self"} {
		ds = append(ds, deploy(n+"/.ok/workflow/w.yaml", n, nil, "//"+n+":push", n, n))
	}
	ds = append(ds, deploy("m/.ok/workflow/w.yaml", "m", []string{"//m:apply"}, "", "", ""))
	got := Make(Input{Deploys: ds, First: true, Self: "deployment self/self",
		Replicas: map[string]int32{"deployment a/a": 1, "deployment b/b": 3, "deployment c/c": 2, "deployment d/d": 1, "deployment self/self": 2}})
	var order []string
	for _, d := range got {
		order = append(order, d.Deploy.Name+":"+string(rune('0'+d.Wave)))
	}
	// b and c have several replicas; m only migrates; a, d, and e (unknown) have one; self after.
	if want := []string{"b:1", "c:2", "m:2", "a:2", "d:3", "e:3", "self:4"}; !reflect.DeepEqual(order, want) {
		t.Errorf("waves %v, want %v", order, want)
	}
	two := Make(Input{Deploys: ds[:2], First: true})
	if two[0].Wave != 1 || two[1].Wave != 2 {
		t.Errorf("two deployments: %+v", two)
	}
	self := ds[5]
	if alone := Make(Input{Deploys: []workflow.Deploy{self}, First: true, Self: "deployment self/self"}); len(alone) != 1 || alone[0].Wave != 1 {
		t.Errorf("the deployer alone: %+v", alone)
	}
	if pair := Make(Input{Deploys: []workflow.Deploy{self, ds[0]}, First: true, Self: "deployment self/self"}); len(pair) != 2 ||
		pair[0].Deploy.Name != "a" || pair[0].Wave != 1 || pair[1].Deploy.Name != "self" || pair[1].Wave != 2 {
		t.Errorf("the deployer and one other: %+v", pair)
	}
}

// A deploy that runs alone takes a wave of its own after the rest's — two take one each — and
// the deployer's own runs after them.
func TestAloneWaves(t *testing.T) {
	var ds []workflow.Deploy
	for _, n := range []string{"a", "b", "database", "other", "self"} {
		ds = append(ds, deploy(n+"/.ok/workflow/w.yaml", n, nil, "//"+n+":push", n, n))
	}
	ds[2].Alone, ds[3].Alone = true, true
	got := Make(Input{Deploys: ds, First: true, Self: "deployment self/self"})
	var order []string
	for _, d := range got {
		order = append(order, d.Deploy.Name+":"+string(rune('0'+d.Wave)))
	}
	if want := []string{"a:1", "b:2", "database:3", "other:4", "self:5"}; !reflect.DeepEqual(order, want) {
		t.Errorf("waves %v, want %v", order, want)
	}
	if only := Make(Input{Deploys: []workflow.Deploy{ds[2]}, First: true}); len(only) != 1 || only[0].Wave != 1 {
		t.Errorf("a deploy alone in its plan: %+v", only)
	}
	// The deployer's own deploy runs last even if it is marked alone.
	self := ds[4]
	self.Alone = true
	if got := Make(Input{Deploys: []workflow.Deploy{ds[0], self, ds[2]}, First: true, Self: "deployment self/self"}); len(got) != 3 ||
		got[0].Deploy.Name != "a" || got[0].Wave != 1 || got[1].Deploy.Name != "database" || got[1].Wave != 2 || got[2].Deploy.Name != "self" || got[2].Wave != 3 {
		t.Errorf("the deployer marked alone: %+v", got)
	}
}

// Two deploys of one workload both fail, and take no wave.
func TestSharedWorkload(t *testing.T) {
	a := deploy("a/.ok/workflow/w.yaml", "a", nil, "//a:push", "web", "web")
	b := deploy("b/.ok/workflow/w.yaml", "b", nil, "//b:push", "web", "web")
	c := deploy("c/.ok/workflow/w.yaml", "c", nil, "//c:push", "c", "c")
	got := Make(Input{Deploys: []workflow.Deploy{a, b, c}, First: true})
	if len(got) != 3 || got[0].Deploy.Name != "c" || got[0].Wave != 1 || got[1].Failed == "" || got[2].Failed == "" ||
		!strings.Contains(got[1].Failed, "deployment web/web is rolled by more than one deploy") {
		t.Errorf("plan %+v", got)
	}
}

// The repository holding ok's groups-and-ACLs tree has the built-in deploy that applies it, due
// when the tree or the settings change — and on the first plan.
func TestTreeDeploy(t *testing.T) {
	web := deploy("web/.ok/workflow/w.yaml", "web", nil, "//web:push", "web", "web")
	for name, tc := range map[string]struct {
		in   Input
		want []string
	}{
		"the first plan":            {Input{Deploys: []workflow.Deploy{web}, First: true, TreeDir: "identity"}, []string{"identity", "web"}},
		"a change to the tree":      {Input{Deploys: []workflow.Deploy{web}, Changed: []string{"identity/eng/acl.yaml"}, Affected: &Affected{}, TreeDir: "identity"}, []string{"identity"}},
		"a change to the settings":  {Input{Changed: []string{".ok/repository.yaml"}, Affected: &Affected{}, TreeDir: "identity"}, []string{"identity"}},
		"a file beside the tree":    {Input{Changed: []string{"identityx/a", "identity"}, Affected: &Affected{}, TreeDir: "identity"}, nil},
		"not the tree's repository": {Input{Changed: []string{"identity/eng/acl.yaml"}, Affected: &Affected{}}, nil},
	} {
		got := Make(tc.in)
		if !reflect.DeepEqual(names(got), tc.want) {
			t.Errorf("%s: %v, want %v", name, names(got), tc.want)
		}
		for _, d := range got {
			if d.Deploy.Name == "identity" && (d.Deploy.ID() != "//.ok/repository.yaml:deploy:identity" || d.Deploy.Workload != nil ||
				len(d.Deploy.Migrate) != 1 || d.Deploy.Migrate[0].Target != TreeTarget || d.Wave == 0 || d.Why == "") {
				t.Errorf("%s: the tree's deployment %+v", name, d)
			}
		}
	}
}
