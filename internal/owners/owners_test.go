package owners

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/identity"
)

// repo is a repository's files, as a Source.
type repo map[string]string

func (r repo) Read(p string) ([]byte, bool, error) {
	data, ok := r[p]
	return []byte(data), ok, nil
}

func user(h string) Owner { return Owner{Kind: User, Name: h} }
func acl(n string) Owner  { return Owner{Kind: ACL, Name: n} }

var anyone = Owner{Kind: Anyone}

func TestIsOwnersFile(t *testing.T) {
	for p, want := range map[string]bool{
		"OWNERS": true, "a/b/OWNERS": true, "a/TEAM_OWNERS": true, "OWNERS_infra": true, "OWNERS_a-b_9": true, "x-y_z_OWNERS": true,
		"_OWNERS": false, "OWNERS_": false, "OWNERS.md": false, "a/owners": false, "NOT_OWNERS_FILE": false,
		"OWNERS_a.b": false, "a.b_OWNERS": false, "OWNERS_é": false, "a!_OWNERS": false,
	} {
		if got := IsOwnersFile(p); got != want {
			t.Errorf("IsOwnersFile(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestParse(t *testing.T) {
	f, problems := Parse("srv/api/OWNERS", []byte(`# the API's owners
user: ada            # a person
user:Bea
USER: cal
acl: Eng
*
user: ada
set noparent
set	allowself

per-file *.proto=user: dee, acl: protos
per-file BUILD,*.{js,ts}=*
per-file secrets/**=set noparent
per-file secrets/**=file: //infra/SECURITY_OWNERS
file: ../TEAM_OWNERS
include /shared/OWNERS_common
`))
	wantProblems := []string{`4: "USER: cal" is not an owner (user: <handle>, acl: <name>, acl: @<organization>/<team>, or *), a per-file rule, a set option, or an import`}
	if got := problemLines(problems); !reflect.DeepEqual(got, wantProblems) {
		t.Errorf("problems = %v", got)
	}
	owners := []Located[Owner]{{user("ada"), 2}, {user("bea"), 3}, {acl("eng"), 5}, {anyone, 6}}
	if !reflect.DeepEqual(f.Owners, owners) {
		t.Errorf("owners = %v", f.Owners)
	}
	if !f.AllowSelf {
		t.Error("set allowself (after a tab) was not read")
	}
	imports := []Import{{Path: "srv/TEAM_OWNERS", Line: 15}, {Path: "shared/OWNERS_common", Include: true, Line: 16}}
	if !reflect.DeepEqual(f.Imports, imports) {
		t.Errorf("imports = %+v", f.Imports)
	}
	var rules []string
	for _, r := range f.Rules {
		s := r.Label() + "="
		switch {
		case r.NoParent:
			s += "set noparent"
		case r.Import != nil:
			s += "file: " + r.Import.Path
		default:
			var os []string
			for _, o := range r.Owners {
				os = append(os, o.String())
			}
			s += strings.Join(os, ", ")
		}
		rules = append(rules, s)
	}
	wantRules := []string{
		"per-file *.proto=user: dee, acl: protos",
		"per-file BUILD,*.{js,ts}=*",
		"per-file secrets/**=set noparent",
		"per-file secrets/**=file: infra/SECURITY_OWNERS",
	}
	if !reflect.DeepEqual(rules, wantRules) {
		t.Errorf("rules = %v", rules)
	}
}

func problemLines(ps []Problem) []string {
	var out []string
	for _, p := range ps {
		out = append(out, strings.TrimPrefix(p.Error(), p.Path+":"))
	}
	return out
}

// A hosted service's forms parse as an installation's do: user: @ada is ada, acl: @acme/eng
// acme's team eng, shown as written.
func TestParseHostedForms(t *testing.T) {
	f, problems := Parse("OWNERS", []byte("user: @Ada\nacl: @acme/eng\nacl: acme/web\nacl: platform\n"))
	if len(problems) != 0 {
		t.Fatalf("problems: %v", problems)
	}
	want := []Owner{{Kind: User, Name: "ada"}, {Kind: ACL, Name: "acme/eng"}, {Kind: ACL, Name: "acme/web"}, {Kind: ACL, Name: "platform"}}
	if len(f.Owners) != len(want) {
		t.Fatalf("owners: %+v", f.Owners)
	}
	for i, o := range f.Owners {
		if o.Value != want[i] {
			t.Errorf("owner %d = %+v, want %+v", i, o.Value, want[i])
		}
	}
	if got := f.Owners[1].Value.String(); got != "acl: @acme/eng" {
		t.Errorf("a team shown as %q", got)
	}
}

func TestParseRefuses(t *testing.T) {
	for _, tc := range []struct{ line, want string }{
		{"bea", `"bea" is not an owner`},
		{"user:", `"" is not a handle`},
		{"user: -ada", `"-ada" is not a handle`},
		{"user: bea@example.com", `"bea@example.com" is not a handle`},
		{"acl: eng team", `"eng team" is not an ACL's name`},
		{"acl: @acme/", `"@acme/" is not an organization's team`},
		{"acl: @/eng", `"@/eng" is not an organization's team`},
		{"file: //a/OWNERS x", `"//a/OWNERS x" is not one path`},
		{"set inherit", `unknown option "inherit"`},
		{"per-file *.go", "a per-file rule is <globs>=<owners>"},
		{"per-file ,*.go=*", "an empty glob"},
		{"per-file [a=*", `glob "[a": an unmatched [`},
		{"per-file {a,b=*", `an unmatched {`},
		{"per-file {a,{b}}=*", `a { inside braces`},
		{"per-file *.go=user: ada, bea", `"bea" is not an owner`},
		{"per-file *.go=include //OWNERS", "a rule imports with file:, not include"},
		{"per-file *.go=set allowself", "is not an owner"},
		{"file: project:main:OWNERS", "names another repository or branch"},
		{"file: ../../OWNERS", "is outside the repository"},
		{"file: //docs/README.md", "is not an OWNERS file"},
		{"include", `"include" is not an owner`},
		{"file:", "an import names no file"},
	} {
		_, problems := Parse("a/OWNERS", []byte("user: ada\n"+tc.line+"\n"))
		if len(problems) != 1 || problems[0].Line != 2 || !strings.Contains(problems[0].Msg, tc.want) {
			t.Errorf("Parse(%q) = %v, want line 2 saying %q", tc.line, problems, tc.want)
		}
	}
}

func TestGlobs(t *testing.T) {
	for _, tc := range []struct {
		glob string
		yes  []string
		no   []string
	}{
		{"*.go", []string{"a.go", "sub/dir/a.go", ".go"}, []string{"a.go.txt", "go"}},
		{"BUILD", []string{"BUILD", "x/BUILD"}, []string{"BUILD.bazel"}},
		{"docs/*.md", []string{"docs/a.md"}, []string{"docs/x/a.md", "a/docs/a.md"}},
		{"docs/**/*.md", []string{"docs/x/a.md", "docs/x/y/a.md"}, []string{"docs/a.md"}},
		{"docs/{**/,}*.md", []string{"docs/a.md", "docs/x/y/a.md"}, []string{"a.md"}},
		{"secrets/**", []string{"secrets/a", "secrets/x/y"}, []string{"secrets", "x/secrets/a"}},
		{"?.txt", []string{"a.txt", "d/b.txt"}, []string{"ab.txt"}},
		{"[a-c].txt", []string{"b.txt"}, []string{"d.txt"}},
		{"[!a-c].txt", []string{"d.txt"}, []string{"b.txt"}},
		{"*.{js,ts}", []string{"a.js", "b/c.ts"}, []string{"a.jsx"}},
		{"[!]a].go", []string{"b.go"}, []string{"].go", "a.go"}},
		{"gen**", []string{"gen.go", "x/gen/y.go", "generated/a/b.txt"}, []string{"x/agen.go"}},
		{`\*.txt`, []string{"*.txt"}, []string{"a.txt"}},
		{"a+b(c).txt", []string{"a+b(c).txt"}, []string{"aab(c).txt"}},
	} {
		m, err := compileGlob(tc.glob)
		if err != nil {
			t.Errorf("compileGlob(%q): %v", tc.glob, err)
			continue
		}
		for _, p := range tc.yes {
			if !m.match(p) {
				t.Errorf("%q does not match %q", tc.glob, p)
			}
		}
		for _, p := range tc.no {
			if m.match(p) {
				t.Errorf("%q matches %q", tc.glob, p)
			}
		}
	}
	if m, err := compileGlob("a}"); err != nil || !m.match("a}") {
		t.Errorf("a } outside braces is not itself: %v", err)
	}
	for _, bad := range []string{"[a", "{a", `a\`, "[!]", "{a,{b,c}}"} {
		if _, err := compileGlob(bad); err == nil {
			t.Errorf("compileGlob(%q) accepted it", bad)
		}
	}
}

// describe is a resolution as lines: each set's file, rules, owners, allowself, and files.
func describe(sets []*Set) []string {
	var out []string
	for _, s := range sets {
		var os []string
		for _, o := range s.Owners {
			os = append(os, o.String())
		}
		line := s.File
		if s.Unrestricted() {
			line = "(unrestricted)"
		}
		if len(s.Rules) > 0 {
			line += " [" + strings.Join(s.Rules, "; ") + "]"
		}
		line += ": " + strings.Join(os, ", ")
		if s.AllowSelf {
			line += " (allowself)"
		}
		out = append(out, line+" <- "+strings.Join(s.Files, " "))
	}
	return out
}

func resolve(t *testing.T, r repo, files ...string) []string {
	t.Helper()
	sets, err := NewResolver(r).Resolve(files)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	return describe(sets)
}

func wantSets(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sets:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

func TestNearestOnly(t *testing.T) {
	r := repo{
		"OWNERS":         "user: root\n",
		"srv/OWNERS":     "user: ada\nuser: bea\n",
		"srv/api/x.go":   "",
		"docs/README.md": "",
	}
	// srv's OWNERS alone governs beneath it, however deep: the root's owners are not srv's.
	wantSets(t, resolve(t, r, "srv/api/v1/x.go", "srv/main.go", "docs/README.md", "top.txt"),
		"OWNERS: user: root <- docs/README.md top.txt",
		"srv/OWNERS: user: ada, user: bea <- srv/api/v1/x.go srv/main.go")
	// With no OWNERS above them, files are unrestricted.
	wantSets(t, resolve(t, repo{"a/OWNERS": "user: ada\n"}, "b/c.go", "c.go"),
		"(unrestricted): * (allowself) <- b/c.go c.go")
}

func TestPerFile(t *testing.T) {
	r := repo{
		"srv/OWNERS": `user: ada
user: bea
per-file *.proto=user: cal
per-file secrets/**=set noparent
per-file secrets/**=user: dee, user: eve
`,
	}
	wantSets(t, resolve(t, r, "srv/a.go", "srv/api/a.proto", "srv/secrets/key.txt", "srv/b.go"),
		"srv/OWNERS: user: ada, user: bea <- srv/a.go srv/b.go",
		"srv/OWNERS [per-file *.proto]: user: ada, user: bea, user: cal <- srv/api/a.proto",
		"srv/OWNERS [per-file secrets/**; per-file secrets/**]: user: dee, user: eve <- srv/secrets/key.txt")
}

func TestImports(t *testing.T) {
	r := repo{
		"srv/OWNERS": `user: ada
file: //teams/TEAM_OWNERS
include ../shared/OWNERS_common
per-file *.sql=file: DB_OWNERS
`,
		"srv/DB_OWNERS": "user: dba\nfile: //teams/TEAM_OWNERS\n",
		// file: brings only these directory-level owners: not its rules, not allowself.
		"teams/TEAM_OWNERS": "user: tom\nset allowself\nper-file *.go=user: nobody\n",
		// include brings everything; its rules match relative to srv/, which governs.
		"shared/OWNERS_common": "acl: eng\nset allowself\nper-file docs/*=user: writer\n",
	}
	wantSets(t, resolve(t, r, "srv/a.go", "srv/docs/a.md", "srv/schema.sql"),
		"srv/OWNERS: user: ada, user: tom, acl: eng (allowself) <- srv/a.go",
		"srv/OWNERS [per-file *.sql]: user: ada, user: dba, user: tom, acl: eng (allowself) <- srv/schema.sql",
		"srv/OWNERS [per-file docs/*]: user: ada, user: tom, user: writer, acl: eng (allowself) <- srv/docs/a.md")
}

// An OWNERS file that cannot be read governs nothing it could be wrong about: resolution fails, and
// approval with it.
func TestResolveFailsClosed(t *testing.T) {
	for name, r := range map[string]repo{
		"syntax":         {"a/OWNERS": "user ada\n"},
		"missing import": {"a/OWNERS": "file: //b/OWNERS\n"},
		"cycle":          {"a/OWNERS": "include //b/OWNERS\n", "b/OWNERS": "include //a/OWNERS\n"},
		"bad import":     {"a/OWNERS": "file: //b/OWNERS\n", "b/OWNERS": "user ada\n"},
	} {
		if sets, err := NewResolver(r).Resolve([]string{"a/x.go"}); err == nil {
			t.Errorf("%s: Resolve = %v, want an error", name, describe(sets))
		}
	}
	_, err := NewResolver(repo{"a/OWNERS": "include //b/OWNERS\n", "b/OWNERS": "include //a/OWNERS\n"}).Resolve([]string{"a/x"})
	if err == nil || !strings.Contains(err.Error(), "imports go round: a/OWNERS → b/OWNERS → a/OWNERS") {
		t.Errorf("cycle: %v", err)
	}
}

func known(names ...string) func(string) bool {
	return func(n string) bool {
		for _, x := range names {
			if x == n {
				return true
			}
		}
		return false
	}
}

func validate(r repo, p, data string) []string {
	var out []string
	for _, pr := range Validate(p, []byte(data), r, known("ada", "bea", "cal"), known("eng")) {
		out = append(out, pr.Error())
	}
	return out
}

func TestValidate(t *testing.T) {
	r := repo{"teams/TEAM_OWNERS": "user: bea\n", "teams/EMPTY_OWNERS": "set allowself\n", "bad/BAD_OWNERS": "user ada\n", "bad/TWO_OWNERS": "x\ny\n"}
	for _, tc := range []struct {
		name, path, data string
		want             []string
	}{
		{"good", "a/OWNERS", "user: ada\nacl: eng\nper-file *.go=user: cal\n", nil},
		{"an import gives owners", "a/OWNERS", "file: //teams/TEAM_OWNERS\n", nil},
		{"an ACL is an owner", "a/OWNERS", "acl: eng\n", nil},
		{"unknown user and ACL", "a/OWNERS", "user: ada\nuser: zed\nper-file *.go=acl: ops\n",
			[]string{"a/OWNERS:2: user: zed names no person", "a/OWNERS:3: acl: ops names no ACL the groups-and-ACLs tree declares"}},
		{"no directory-level owners", "a/OWNERS", "per-file *.go=user: ada\n",
			[]string{"a/OWNERS: no directory-level owners: files no per-file rule matches could never be approved (add a user:, an acl:, *, or an import that gives owners)"}},
		{"an import that gives none", "a/OWNERS", "file: //teams/EMPTY_OWNERS\n",
			[]string{"a/OWNERS: no directory-level owners: files no per-file rule matches could never be approved (add a user:, an acl:, *, or an import that gives owners)"}},
		{"another name may have none", "a/TEAM_OWNERS", "set allowself\n", nil},
		{"noparent with no owners", "a/OWNERS", "user: ada\nper-file x/**=set noparent\nper-file y/**=user: bea\n",
			[]string{"a/OWNERS:2: per-file x/**: set noparent, and no per-file rule gives these files owners, so no change to them could be approved"}},
		{"noparent owned through an import", "a/OWNERS", "user: ada\nper-file x/**=set noparent\nper-file x/**=file: //teams/TEAM_OWNERS\n", nil},
		{"noparent beside a failing import", "a/OWNERS", "user: ada\nper-file x/**=set noparent\nper-file x/**=file: //gone/OWNERS\n",
			[]string{"a/OWNERS:3: gone/OWNERS does not exist"}},
		{"every line of a broken import", "a/OWNERS", "user: ada\nfile: //bad/TWO_OWNERS\n",
			[]string{`bad/TWO_OWNERS:1: "x" is not an owner (user: <handle>, acl: <name>, acl: @<organization>/<team>, or *), a per-file rule, a set option, or an import`, `bad/TWO_OWNERS:2: "y" is not an owner (user: <handle>, acl: <name>, acl: @<organization>/<team>, or *), a per-file rule, a set option, or an import`}},
		{"a missing import", "a/OWNERS", "user: ada\nfile: //b/OWNERS\n", []string{"a/OWNERS:2: b/OWNERS does not exist"}},
		{"an import with problems", "a/OWNERS", "user: ada\nfile: //bad/BAD_OWNERS\n",
			[]string{`bad/BAD_OWNERS:1: "user ada" is not an owner (user: <handle>, acl: <name>, acl: @<organization>/<team>, or *), a per-file rule, a set option, or an import`}},
		{"an import back to itself", "a/OWNERS", "user: ada\ninclude //teams/LOOP_OWNERS\n",
			[]string{"teams/LOOP_OWNERS:1: imports go round: a/OWNERS → teams/LOOP_OWNERS → a/OWNERS"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := repo{"teams/LOOP_OWNERS": "include //a/OWNERS\n"}
			for k, v := range r {
				rr[k] = v
			}
			if got := validate(rr, tc.path, tc.data); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Validate:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(tc.want, "\n  "))
			}
		})
	}
}

func estate(n int, acls map[string][]string) People {
	return People{Estate: n, ACL: func(name string) ([]string, bool) {
		h, ok := acls[name]
		return h, ok
	}}
}

func TestApprovable(t *testing.T) {
	one := estate(1, map[string][]string{"admins": {"ada"}, "robots": nil})
	two := estate(2, map[string][]string{"admins": {"ada", "bea"}, "solo": {"ada"}})
	for _, tc := range []struct {
		name   string
		files  repo
		people People
		want   []string
	}{
		{"two people", repo{"a/OWNERS": "user: ada\nuser: bea\n"}, two, nil},
		{"one person who allows self", repo{"a/OWNERS": "user: ada\nset allowself\n"}, one, nil},
		{"one person", repo{"a/OWNERS": "user: ada\n"}, two,
			[]string{"a/OWNERS: directory-level owners: ada alone owns these files, without set allowself, so ada's own changes to them could never be approved (add a second person, or set allowself)"}},
		{"one person through an ACL, and as a user", repo{"a/OWNERS": "acl: solo\nuser: ada\n"}, two,
			[]string{"a/OWNERS: directory-level owners: ada alone owns these files, without set allowself, so ada's own changes to them could never be approved (add a second person, or set allowself)"}},
		{"an ACL of two", repo{"a/OWNERS": "acl: admins\n"}, two, nil},
		{"robots only", repo{"a/OWNERS": "acl: robots\nset allowself\n"}, one,
			[]string{"a/OWNERS: directory-level owners: no person owns these files, so no change to them could be approved (name a person, an ACL with people in it, or *)"}},
		{"an undeclared ACL", repo{"a/OWNERS": "user: ada\nuser: bea\nacl: ops\n"}, two,
			[]string{"a/OWNERS: directory-level owners: acl: ops names no ACL the groups-and-ACLs tree declares"}},
		{"anyone, among two", repo{"a/OWNERS": "*\n"}, two, nil},
		{"anyone, alone", repo{"a/OWNERS": "*\n"}, one,
			[]string{"a/OWNERS: directory-level owners: * without set allowself, and fewer than two people are known: a change by the only one could never be approved (set allowself)"}},
		{"anyone, alone, allowing self", repo{"a/OWNERS": "*\nset allowself\n"}, one, nil},
		{"a per-file set of one", repo{"a/OWNERS": "user: ada\nuser: bea\nper-file *.go=set noparent\nper-file *.go=user: bea\n"}, two,
			[]string{"a/OWNERS:3: per-file *.go: bea alone owns these files, without set allowself, so bea's own changes to them could never be approved (add a second person, or set allowself)"}},
		{"a per-file rule beside its parents", repo{"a/OWNERS": "user: ada\nuser: bea\nper-file *.go=user: bea\n"}, two, nil},
		{"an included allowself", repo{"a/OWNERS": "include //b/TEAM_OWNERS\n", "b/TEAM_OWNERS": "user: ada\nset allowself\n"}, one, nil},
		{"a missing file", repo{}, one, []string{"a/OWNERS: a/OWNERS does not exist"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, p := range Approvable(tc.files, []string{"a/OWNERS"}, tc.people) {
				got = append(got, p.Error())
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Approvable:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(tc.want, "\n  "))
			}
		})
	}
}

func TestHumanHandle(t *testing.T) {
	names := identity.MustNames("example.com")
	for in, want := range map[string]string{
		"ada.human.example.com": "ada", "ci.robot.example.com": "", "eng.acl.example.com": "",
		"ada.human.other.com": "", ".human.example.com": "", "a.b.human.example.com": "",
	} {
		got, ok := HumanHandle(names, in)
		if got != want || ok != (want != "") {
			t.Errorf("HumanHandle(%q) = %q, %v", in, got, ok)
		}
	}
}
