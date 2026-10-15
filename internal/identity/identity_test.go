package identity

import (
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
)

var testNames = MustNames("example.com")

const (
	ada     = "ada.human.example.com"
	bea     = "bea.human.example.com"
	cal     = "cal.human.example.com"
	deployR = "deployer.ok.robot.example.com"
)

// fsOf builds a filesystem from file paths to their contents.
func fsOf(files map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for p, c := range files {
		fsys[p] = &fstest.MapFile{Data: []byte(c)}
	}
	return fsys
}

func members(names ...string) string {
	if len(names) == 0 {
		return "members: []\n"
	}
	return "members:\n  - " + strings.Join(names, "\n  - ") + "\n"
}

// estate is a valid tree of every shape: the root's ACL and group, a folder with both, an ACL
// beneath it, a folder with only a group, and an ACL beneath a group-only folder.
func estate() map[string]string {
	return map[string]string{
		"acl.yaml":                 members(ada),
		"group.yaml":               members(ada, bea),
		"README.md":                "# identity\n",
		"NOTES.txt":                "notes\n",
		".ok/workflow/x.yaml":      "deploy: {}\n",
		"platform/acl.yaml":        members(bea, "deploy.acl.example.com"),
		"platform/group.yaml":      members(bea, cal, "eng.group.example.com"),
		"platform/deploy/acl.yaml": members(deployR),
		"eng/group.yaml":           members(cal),
		"eng/infra/acl.yaml":       members(cal),
		"eng/infra/group.yaml":     members(cal),
	}
}

func mustLoad(t *testing.T, files map[string]string) *Tree {
	t.Helper()
	tr, err := Load(testNames, fsOf(files))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return tr
}

func TestLoadOwnership(t *testing.T) {
	tr := mustLoad(t, estate())
	want := map[string]string{
		"admins.acl.example.com":     "admins.acl.example.com",
		"admins.group.example.com":   "admins.acl.example.com",
		"platform.acl.example.com":   "admins.acl.example.com",
		"platform.group.example.com": "platform.acl.example.com",
		"deploy.acl.example.com":     "platform.acl.example.com",
		"eng.group.example.com":      "admins.acl.example.com",
		"infra.acl.example.com":      "admins.acl.example.com", // eng/ declares no ACL
		"infra.group.example.com":    "infra.acl.example.com",
	}
	var got []string
	for _, s := range tr.Sets() {
		got = append(got, testNames.OfSet(s))
		owner, ok := tr.Owner(testNames.OfSet(s))
		if !ok || owner != want[testNames.OfSet(s)] {
			t.Errorf("Owner(%s) = %q, %v; want %q", testNames.OfSet(s), owner, ok, want[testNames.OfSet(s)])
		}
	}
	if len(got) != len(want) {
		t.Errorf("sets = %v; want %d", got, len(want))
	}
	if s, ok := tr.Set("deploy.acl.example.com"); !ok || s.Path != "platform/deploy" || s.Name != "deploy" || s.Kind != ACL {
		t.Errorf("Set(deploy.acl) = %+v, %v", s, ok)
	}
	if got, want := tr.Owned("admins.acl.example.com"), []string{"admins.group.example.com", "eng.group.example.com", "infra.acl.example.com", "platform.acl.example.com"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Owned(admins.acl) = %v; want %v", got, want)
	}
	if _, ok := tr.Owner("nope.acl.example.com"); ok {
		t.Error("Owner of an undeclared set reported ok")
	}
}

func TestMembership(t *testing.T) {
	tr := mustLoad(t, estate())
	if got, want := tr.Expand("platform.acl.example.com"), []string{bea, deployR}; !reflect.DeepEqual(got, want) {
		t.Errorf("Expand(platform.acl) = %v; want %v", got, want)
	}
	if got, want := tr.Expand("platform.group.example.com"), []string{bea, cal}; !reflect.DeepEqual(got, want) {
		t.Errorf("Expand(platform.group) = %v; want %v", got, want)
	}
	if got := tr.Expand("nope.acl.example.com"); len(got) != 0 {
		t.Errorf("Expand(undeclared) = %v", got)
	}
	if got, want := tr.Memberships(cal), []string{"eng.group.example.com", "infra.acl.example.com", "infra.group.example.com", "platform.group.example.com"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Memberships(cal) = %v; want %v", got, want)
	}
	if got, want := tr.Memberships(deployR), []string{"deploy.acl.example.com", "platform.acl.example.com"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Memberships(robot) = %v; want %v", got, want)
	}
	chain, ok := tr.Contains("platform.group.example.com", cal)
	if !ok || !reflect.DeepEqual(chain, []string{"platform.group.example.com", cal}) {
		t.Errorf("Contains(platform.group, cal) = %v, %v; want the direct chain", chain, ok)
	}
	chain, ok = tr.Contains("platform.acl.example.com", deployR)
	if !ok || !reflect.DeepEqual(chain, []string{"platform.acl.example.com", "deploy.acl.example.com", deployR}) {
		t.Errorf("Contains(platform.acl, robot) = %v, %v", chain, ok)
	}
	if _, ok := tr.Contains("platform.acl.example.com", cal); ok {
		t.Error("cal is reported in platform.acl")
	}
	if _, ok := tr.Contains("nope.acl.example.com", cal); ok {
		t.Error("an undeclared set reported containing someone")
	}
}

func TestSetsAreCopies(t *testing.T) {
	tr := mustLoad(t, estate())
	s, _ := tr.Set("platform.acl.example.com")
	s.Members[0] = "changed.human.example.com"
	for _, c := range tr.Sets() {
		c.Members = nil
	}
	if got, _ := tr.Set("platform.acl.example.com"); got.Members[0] != bea {
		t.Errorf("mutating a returned set changed the tree: %v", got.Members)
	}
}

func TestLoadRefuses(t *testing.T) {
	with := func(change map[string]string, drop ...string) map[string]string {
		files := estate()
		for _, d := range drop {
			delete(files, d)
		}
		for k, v := range change {
			files[k] = v
		}
		return files
	}
	cases := []struct {
		name  string
		files map[string]string
		want  string
	}{
		{"no root ACL", with(nil, "acl.yaml"), "the root declares no ACL"},
		{"an empty root ACL", with(map[string]string{"acl.yaml": members()}), "admins.acl.example.com has no members"},
		{"a folder declaring nothing", with(map[string]string{"empty/sub/group.yaml": members(cal)}), "the folder empty/ declares no group or ACL"},
		{"admins below the root", with(map[string]string{"admins/group.yaml": members(cal)}), `only the tree's root is named "admins"`},
		{"a name twice", with(map[string]string{"eng/platform/group.yaml": members(cal)}), `the name "platform" is also the folder`},
		{"a folder that is no label", with(map[string]string{"Platform/group.yaml": members(cal)}), "Platform/: a folder is a set's name"},
		{"a stray file", with(map[string]string{"platform/notes.md": "hi"}), "platform/notes.md: a folder in the tree holds only acl.yaml, group.yaml, and folders"},
		{"a build.yaml below the root", with(map[string]string{"platform/build.yaml": "name: x\n"}), "platform/build.yaml: a folder in the tree holds only"},
		{"an unknown key", with(map[string]string{"platform/acl.yaml": "owner: admins\nmembers: []\n"}), "field owner not found"},
		{"no members key", with(map[string]string{"platform/acl.yaml": "members:\n"}), "declares no members"},
		{"an empty file", with(map[string]string{"platform/acl.yaml": ""}), "declares nothing"},
		{"two documents", with(map[string]string{"platform/acl.yaml": members(bea) + "---\n" + members(cal)}), "more than one YAML document"},
		{"a malformed member", with(map[string]string{"platform/acl.yaml": members("bea@example.com")}), `"bea@example.com" is no name under example.com`},
		{"a member twice", with(map[string]string{"platform/acl.yaml": members(bea, bea)}), "bea.human.example.com is a member twice"},
		{"a group holding an ACL", with(map[string]string{"eng/group.yaml": members("platform.acl.example.com")}), "a group cannot contain an ACL"},
		{"an undeclared set", with(map[string]string{"eng/group.yaml": members("ops.group.example.com")}), "ops.group.example.com is not declared in the tree"},
		{"a set holding itself", with(map[string]string{"eng/group.yaml": members("eng.group.example.com")}), "a set cannot contain itself"},
		{"a membership cycle", with(map[string]string{"eng/group.yaml": members("platform.group.example.com")}), "membership is circular: eng.group.example.com contains platform.group.example.com contains eng.group.example.com"},
		{"a cycle through an ACL", with(map[string]string{"platform/deploy/acl.yaml": members("platform.acl.example.com")}), "membership is circular"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(testNames, fsOf(tc.files))
			if err == nil {
				t.Fatalf("Load accepted the tree; want %q", tc.want)
			}
			if _, ok := AsError(err); !ok {
				t.Errorf("Load's error is a %T, not an *Error", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Load: %v\nwant it to say %q", err, tc.want)
			}
		})
	}
}

func TestLoadReportsEveryProblem(t *testing.T) {
	files := estate()
	files["platform/acl.yaml"] = members(bea, bea)
	files["eng/group.yaml"] = members("platform.acl.example.com")
	_, err := Load(testNames, fsOf(files))
	e, ok := AsError(err)
	if !ok || len(e.Problems) != 2 {
		t.Fatalf("Load: %v; want 2 problems", err)
	}
}

func TestNewRefusesInconsistentDeclarations(t *testing.T) {
	root := Set{Kind: ACL, Name: Root, Members: []string{ada}}
	cases := []struct {
		name string
		sets []Set
		want string
	}{
		{"a name that is not its folder's", []Set{root, {Kind: Group, Name: "eng", Path: "platform"}}, `named "platform"`},
		{"a root not named admins", []Set{{Kind: ACL, Name: "top", Members: []string{ada}}}, `named "admins"`},
		{"an unknown kind", []Set{root, {Kind: Kind(7), Name: "x", Path: "x"}}, "neither a group nor an ACL"},
		{"a bad path", []Set{root, {Kind: Group, Name: "x", Path: "a//x"}}, "is not a folder of lowercase DNS labels"},
		{"a set twice", []Set{root, {Kind: Group, Name: "x", Path: "x"}, {Kind: Group, Name: "x", Path: "x"}}, "declared twice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(testNames, tc.sets); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("New: %v; want %q", err, tc.want)
			}
		})
	}
	tr, err := New(testNames, []Set{root, {Kind: Group, Name: "x", Path: "x", Members: []string{cal}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if o, _ := tr.Owner("x.group.example.com"); o != "admins.acl.example.com" {
		t.Errorf("owner = %q", o)
	}
}

func TestKindOf(t *testing.T) {
	cases := map[string]MemberKind{
		"ada.human.example.com":         Human,
		"deployer.ok.robot.example.com": Robot,
		"ci.robot.example.com":          Robot,
		"eng.group.example.com":         GroupMember,
		"admins.acl.example.com":        ACLMember,
	}
	for name, want := range cases {
		if got, ok := testNames.KindOf(name); !ok || got != want {
			t.Errorf("testNames.KindOf(%q) = %v, %v; want %v", name, got, ok, want)
		}
	}
	for _, bad := range []string{
		"", "other.net", "human.example.com", "a.b.human.example.com", "a.b.c.robot.example.com", "a.b.group.example.com",
		"a.svc.example.com", "A.human.example.com", "a.human.other.com", "-a.human.example.com", "a..robot.example.com",
	} {
		if k, ok := testNames.KindOf(bad); ok {
			t.Errorf("testNames.KindOf(%q) = %v; want it refused", bad, k)
		}
	}
}

func TestLoadFiles(t *testing.T) {
	files := map[string][]byte{}
	for p, c := range estate() {
		files[p] = []byte(c)
	}
	tr, err := LoadFiles(testNames, files)
	if err != nil {
		t.Fatal(err)
	}
	if fromFS := mustLoad(t, estate()); !reflect.DeepEqual(tr.Sets(), fromFS.Sets()) {
		t.Errorf("LoadFiles and Load disagree:\n %v\n %v", tr.Sets(), fromFS.Sets())
	}
	// Only the set files' contents are read: the rest may be absent.
	files["CLAUDE.md"], files["build.yaml"] = nil, nil
	if _, err := LoadFiles(testNames, files); err != nil {
		t.Errorf("without the root's other contents: %v", err)
	}
	files["Bad/x/group.yaml"] = []byte(members(cal))
	files["Bad/y/acl.yaml"] = []byte(members(cal))
	_, err = LoadFiles(testNames, files)
	if e, ok := AsError(err); !ok || len(e.Problems) != 1 || !strings.Contains(e.Problems[0], "Bad/: a folder is a set's name") {
		t.Errorf("a bad folder with two sets beneath: %v", err)
	}
}

func TestNames(t *testing.T) {
	for _, bad := range []string{"", "Example.com", "example..com", "-x.com", "x_y.com", strings.Repeat("a.", 127) + "com"} {
		if _, err := NewNames(bad); err == nil {
			t.Errorf("NewNames(%q) accepted it", bad)
		}
	}
	corp := MustNames("corp.example.co.uk")
	if got := corp.Human("ada"); got != "ada.human.corp.example.co.uk" {
		t.Errorf("Human = %q", got)
	}
	if got := corp.Robot("deployer.ok"); got != "deployer.ok.robot.corp.example.co.uk" {
		t.Errorf("Robot = %q", got)
	}
	if got := corp.Of(ACL, "eng"); got != "eng.acl.corp.example.co.uk" {
		t.Errorf("Of = %q", got)
	}
	for name, want := range map[string]MemberKind{
		"ada.human.corp.example.co.uk":         Human,
		"ci.robot.corp.example.co.uk":          Robot,
		"deployer.ok.robot.corp.example.co.uk": Robot,
		"eng.group.corp.example.co.uk":         GroupMember,
		"eng.acl.corp.example.co.uk":           ACLMember,
	} {
		if got, ok := corp.KindOf(name); !ok || got != want {
			t.Errorf("KindOf(%q) = %v, %v; want %v", name, got, ok, want)
		}
	}
	// A name under one domain is no name under another, even one it ends in.
	for _, foreign := range []string{"ada.human.example.co.uk", "ada.human.corp.example.co.uk.evil.com"} {
		if k, ok := corp.KindOf(foreign); ok {
			t.Errorf("KindOf(%q) = %v under corp.example.co.uk", foreign, k)
		}
	}
	if k, ok := testNames.KindOf("ada.human.corp.example.com"); ok {
		t.Errorf("KindOf(ada.human.corp.example.com) = %v under example.com: corp is no kind", k)
	}
}

// A tree set may hold a group synced from the system of record: membership goes through it, and
// approving a change counts its members as they are.
func TestSyncedGroups(t *testing.T) {
	synced := WithSynced(map[string][]string{
		"it.group.example.com":    {"dee.human.example.com", "ops.group.example.com"},
		"ops.group.example.com":   {"eve.human.example.com"},
		"sales.group.example.com": nil,
	})
	files := estate()
	files["eng/infra/acl.yaml"] = members(cal, "it.group.example.com")
	tr, err := Load(testNames, fsOf(files), synced)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{cal, "dee.human.example.com", "eve.human.example.com"}
	if got := tr.Expand("infra.acl.example.com"); !reflect.DeepEqual(got, want) {
		t.Errorf("Expand = %v, want %v", got, want)
	}
	if chain, ok := tr.Contains("infra.acl.example.com", "eve.human.example.com"); !ok ||
		!reflect.DeepEqual(chain, []string{"infra.acl.example.com", "it.group.example.com", "ops.group.example.com", "eve.human.example.com"}) {
		t.Errorf("Contains = %v, %v", chain, ok)
	}
	if got := tr.Memberships("eve.human.example.com"); !reflect.DeepEqual(got, []string{"infra.acl.example.com", "it.group.example.com", "ops.group.example.com"}) {
		t.Errorf("Memberships = %v", got)
	}
	if !tr.Synced("it.group.example.com") || tr.Synced("platform.group.example.com") {
		t.Error("Synced is wrong")
	}

	// Without knowing the synced groups, the tree is refused; so is one declaring a synced name.
	if _, err := Load(testNames, fsOf(files)); err == nil || !strings.Contains(err.Error(), "nor synced from the system of record") {
		t.Errorf("an unknown group: %v", err)
	}
	files["sales/group.yaml"] = members(cal)
	if _, err := Load(testNames, fsOf(files), synced); err == nil || !strings.Contains(err.Error(), "synced from the system of record; the tree cannot declare it too") {
		t.Errorf("a synced name declared: %v", err)
	}
	delete(files, "sales/group.yaml")

	// dee, through the synced group, may change what infra.acl owns.
	before := tr
	files["eng/infra/group.yaml"] = members(cal, bea)
	after, err := Load(testNames, fsOf(files), synced)
	if err != nil {
		t.Fatal(err)
	}
	if d := Authorize(before, after, []string{"dee.human.example.com"}); !d.Approved {
		t.Errorf("a synced group's member could not approve: %+v", d.Refused())
	}
}
