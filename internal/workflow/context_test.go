package workflow

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func sampleContext() *Context {
	return &Context{
		Change: Change{
			Phase:      Pre,
			Repository: Repository{ID: 3, Name: "home"},
			CL:         42,
			Patchset:   2,
			Author:     "alice.human.example.com",
			Commit:     "c0ffee",
			Base:       "ba5e",
		},
		Files: []ChangedFile{
			{Path: "server/schema/run.sdl", Status: Modified},
			{Path: "server/new.sdl", Status: Added},
			{Path: "server/gone.go", Status: Deleted},
			{Path: "docs/new.md", Status: Renamed, OldPath: "docs/old.md"},
			{Path: "README.md", Status: Copied, OldPath: "docs/new.md"},
			{Path: "a.go", Status: Modified},
		},
	}
}

func TestContextPaths(t *testing.T) {
	c := sampleContext()
	if got, want := c.Existing(), []string{"server/schema/run.sdl", "server/new.sdl", "docs/new.md", "README.md", "a.go"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Existing = %v", got)
	}
	if got, want := c.Removed(), []string{"server/gone.go", "docs/old.md"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Removed = %v", got)
	}
	if got, want := c.Touched(), []string{"server/schema/run.sdl", "server/new.sdl", "server/gone.go", "docs/new.md", "docs/old.md", "README.md", "a.go"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Touched = %v", got)
	}
}

func TestExpand(t *testing.T) {
	c := sampleContext()
	for name, tc := range map[string]struct {
		args, want []string
	}{
		"no placeholders":    {[]string{"-v", "x"}, []string{"-v", "x"}},
		"changed files":      {[]string{"{changed_files}"}, []string{"server/schema/run.sdl", "server/new.sdl", "docs/new.md", "README.md", "a.go"}},
		"globbed, any depth": {[]string{"--check", "{changed_files:**/*.sdl}"}, []string{"--check", "server/schema/run.sdl", "server/new.sdl"}},
		"globbed, root":      {[]string{"{changed_files:**/*.go}"}, []string{"a.go"}},
		"globbed, subtree":   {[]string{"{changed_files:docs/**}"}, []string{"docs/new.md"}},
		"alternation":        {[]string{"{changed_files:{docs,server}/*.{md,sdl}}"}, []string{"server/new.sdl", "docs/new.md"}},
		"nothing matches":    {[]string{"-x", "{changed_files:**/*.rs}"}, []string{"-x"}},
		"deleted":            {[]string{"{deleted_files}"}, []string{"server/gone.go", "docs/old.md"}},
		"deleted, globbed":   {[]string{"{deleted_files:**/*.md}"}, []string{"docs/old.md"}},
		"scalars":            {[]string{"--cl={cl}/{patchset}", "{commit}..{base}", "{phase}"}, []string{"--cl=42/2", "c0ffee..ba5e", "pre"}},
		"literal braces":     {[]string{"{{x}}", "a{{b"}, []string{"{x}", "a{b"}},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := Expand(tc.args, c)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) && !(len(got) == 0 && len(tc.want) == 0) {
				t.Errorf("Expand(%q) = %q, want %q", tc.args, got, tc.want)
			}
		})
	}
	if _, err := Expand([]string{"{nope}"}, c); err == nil || !strings.Contains(err.Error(), "unknown placeholder") {
		t.Errorf("an unknown placeholder: %v", err)
	}
}

func TestGlob(t *testing.T) {
	for glob, cases := range map[string]map[string]bool{
		"*.go":         {"a.go": true, "x/a.go": false},
		"**/*.go":      {"a.go": true, "x/a.go": true, "x/y/a.go": true, "a.gox": false},
		"apps/**":      {"apps/a": true, "apps/x/y": true, "apps": false, "appsx/a": false},
		"apps/**/b.md": {"apps/b.md": true, "apps/x/y/b.md": true, "b.md": false},
		"a?.go":        {"ab.go": true, "a/.go": false, "abc.go": false},
		"[ab].go":      {"a.go": true, "c.go": false},
		"[!ab].go":     {"c.go": true, "a.go": false, "/.go": false},
		"{a,b}/*.md":   {"a/x.md": true, "b/x.md": true, "c/x.md": false},
		`a\*.go`:       {"a*.go": true, "ab.go": false},
		"a.b":          {"a.b": true, "axb": false},
	} {
		re, err := compileGlob(glob)
		if err != nil {
			t.Fatalf("compileGlob(%q): %v", glob, err)
		}
		for p, want := range cases {
			if got := re.MatchString(p); got != want {
				t.Errorf("%q matching %q = %v, want %v", glob, p, got, want)
			}
		}
	}
	for _, bad := range []string{"[ab", "{a,b", "{a,{b}}", "a}", `a\`} {
		if _, err := compileGlob(bad); err == nil {
			t.Errorf("compileGlob(%q) compiled", bad)
		}
	}
}

// The job context's JSON is one flat object: the change's fields beside changed_files, as
// jobs read ok-context.json.
func TestContextJSON(t *testing.T) {
	data, err := json.Marshal(sampleContext())
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"phase", "repository", "cl", "patchset", "author", "commit", "base", "changed_files"} {
		if _, ok := got[k]; !ok {
			t.Errorf("no %q in %s", k, data)
		}
	}
	if len(got) != 8 {
		t.Errorf("%d fields in %s", len(got), data)
	}
	var back Context
	if err := json.Unmarshal(data, &back); err != nil || !reflect.DeepEqual(&back, sampleContext()) {
		t.Errorf("round trip: %+v (%v)", back, err)
	}
}

// A job's arguments depend on its change when any carries a placeholder; literal braces do not
// count, and an argument that does not parse does.
func TestHasPlaceholders(t *testing.T) {
	for _, c := range []struct {
		args []string
		want bool
	}{
		{nil, false},
		{[]string{"-v", "--run={{x}}"}, false},
		{[]string{"-v", "{changed_files:**/*.go}"}, true},
		{[]string{"--cl={cl}"}, true},
		{[]string{"{commit}"}, true},
		{[]string{"{unknown}"}, true},
		{[]string{"a } b"}, true},
	} {
		if got := HasPlaceholders(c.args); got != c.want {
			t.Errorf("HasPlaceholders(%q) = %v, want %v", c.args, got, c.want)
		}
	}
}
