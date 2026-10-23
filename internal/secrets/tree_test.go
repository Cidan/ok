package secrets

import (
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/identity"
)

var names = identity.MustNames("example.com")

func ptr(b byte) []byte {
	var d Digest
	for i := range d {
		d[i] = b
	}
	return Pointer(d)
}

func file(p string, contents string) Entry {
	return Entry{Path: p, Kind: File, Contents: []byte(contents)}
}

const (
	prodACL   = "readers:\n  - prod-db\n  - oncall.acl.example.com\nwriters:\n  - prod-db\nunseal: passkey\n"
	adminsACL = "readers: [admins]\nwriters: [admins]\n"
)

func TestLoad(t *testing.T) {
	tree, err := Load(names, []Entry{
		file("secret/acl.yaml", adminsACL),
		file("secret/OWNERS", "set noparent\nacl: admins\n"),
		file("secret/token", string(ptr(1))),
		file("secret/prod/acl.yaml", prodACL),
		file("secret/prod/OWNERS", "ada\n"),
		file("secret/prod/db-password", string(ptr(2))),
		file("secret/prod/tls/acl.yaml", "readers: ['@acme/edge']\nwriters: []\n"),
		file("secret/prod/tls/server.key", string(ptr(3))),
	})
	if err != nil {
		t.Fatal(err)
	}
	prod := tree.Folders["secret/prod"]
	if len(prod.Readers) != 2 || prod.Readers[0] != "prod-db.acl.example.com" || prod.Readers[1] != "oncall.acl.example.com" ||
		len(prod.Writers) != 1 || prod.Writers[0] != "prod-db.acl.example.com" || prod.Unseal != UnsealPasskey {
		t.Errorf("secret/prod's access: %+v", prod)
	}
	if tls := tree.Folders["secret/prod/tls"]; len(tls.Readers) != 1 || tls.Readers[0] != "edge.acme.team.example.com" ||
		len(tls.Writers) != 0 || tls.Unseal != UnsealDevice {
		t.Errorf("secret/prod/tls's access: %+v", tls)
	}
	if d := tree.Secrets["secret/prod/tls/server.key"]; d[0] != 3 {
		t.Errorf("server.key's digest: %v", d)
	}
	// OWNERS files are the tree's approval list, and no secret.
	if _, ok := tree.Secrets["secret/prod/OWNERS"]; ok || len(tree.Secrets) != 3 {
		t.Errorf("secrets: %v", tree.Secrets)
	}
}

func TestLoadRefuses(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []Entry
		want    []string
	}{
		{"a folder without an acl.yaml", []Entry{
			file("secret/acl.yaml", adminsACL),
			file("secret/prod/db-password", string(ptr(1))),
		}, []string{"secret/prod/: has no acl.yaml"}},
		{"the root without one, through a subfolder", []Entry{
			file("secret/prod/acl.yaml", prodACL),
		}, []string{"secret/: has no acl.yaml"}},
		{"an OWNERS file is no acl.yaml", []Entry{
			file("secret/OWNERS", "ada\n"),
		}, []string{"secret/: has no acl.yaml"}},
		{"a value in place of a pointer", []Entry{
			file("secret/acl.yaml", adminsACL),
			file("secret/password", "hunter2\n"),
		}, []string{"secret/password: holds a value, not a pointer"}},
		{"another format's pointer", []Entry{
			file("secret/acl.yaml", adminsACL),
			file("secret/token", "other-secret v1 blake3:"+strings.Repeat("0", 64)+"\n"),
		}, []string{"secret/token: holds a value, not a pointer"}},
		{"a person named in an acl.yaml", []Entry{
			file("secret/acl.yaml", "readers: [ada.human.example.com]\nwriters: []\n"),
		}, []string{`readers: "ada.human.example.com" names no set`}},
		{"a group named in an acl.yaml", []Entry{
			file("secret/acl.yaml", "readers: []\nwriters: [eng.group.example.com]\n"),
		}, []string{"writers: eng.group.example.com is a group"}},
		{"no writers key", []Entry{
			file("secret/acl.yaml", "readers: [a]\n"),
		}, []string{"secret/acl.yaml: has no writers"}},
		{"an unknown key", []Entry{
			file("secret/acl.yaml", "members: [a]\n"),
		}, []string{"secret/acl.yaml: yaml: unmarshal errors"}},
		{"a name twice, by two forms", []Entry{
			file("secret/acl.yaml", "readers: [a, a.acl.example.com]\nwriters: [a]\n"),
		}, []string{"readers: a.acl.example.com is named twice"}},
		{"an unseal of neither kind", []Entry{
			file("secret/acl.yaml", "readers: [a]\nwriters: [a]\nunseal: password\n"),
		}, []string{`unseal: "password" is neither device nor passkey`}},
		{"a symlink, an executable, a dotfile, and a linked OWNERS", []Entry{
			file("secret/acl.yaml", "readers: [a]\nwriters: [a]\n"),
			{Path: "secret/link", Kind: Symlink},
			{Path: "secret/run", Kind: Executable, Contents: ptr(1)},
			file("secret/.swp", "x"),
			{Path: "secret/OWNERS", Kind: Symlink},
		}, []string{"secret/link: is a symlink", "secret/run: is executable", `secret/.swp: ".swp" is not a valid name`, "secret/OWNERS: an OWNERS file is a regular file"}},
		{"a file at //secret itself", []Entry{
			file("secret", "x"),
		}, []string{"secret: is a file"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(names, tc.entries)
			e, ok := AsError(err)
			if !ok {
				t.Fatalf("got %v, want an *Error", err)
			}
			all := strings.Join(e.Problems, "\n")
			for _, w := range tc.want {
				if !strings.Contains(all, w) {
					t.Errorf("problems do not mention %q:\n%s", w, all)
				}
			}
			if len(e.Problems) != len(tc.want) {
				t.Errorf("%d problems, want %d:\n%s", len(e.Problems), len(tc.want), all)
			}
		})
	}
}

// Deploys and post jobs are given a folder's secrets by their identities, which name their
// workflow file, phase, and name.
func TestGrants(t *testing.T) {
	const deploy, run = "//services/api/.ok/workflow/deploy.yaml:deploy:api", "//.ok/workflow/ci.yaml:post:push"
	a, err := ParseACL(names, []byte("readers: []\nwriters: []\ndeploys:\n  - "+deploy+"\nruns: ["+run+"]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !a.GrantsDeploy(deploy) || a.GrantsDeploy("//.ok/workflow/x.yaml:deploy:other") || !a.GrantsRun(run) || a.GrantsRun(deploy) {
		t.Errorf("grants: %+v", a)
	}
	if a, err := ParseACL(names, []byte(adminsACL)); err != nil || len(a.Deploys)+len(a.Runs) != 0 {
		t.Errorf("no grants: %+v %v", a, err)
	}
	for _, bad := range []string{
		"//services/api/.ok/workflow/deploy.yaml:deploy:Api",
		"//services/api/.ok/workflow/deploy.yaml:pre:api",
		"services/api/.ok/workflow/deploy.yaml:deploy:api",
		"//services/../.ok/workflow/deploy.yaml:deploy:api",
		"//services/api/workflow/deploy.yaml:deploy:api",
		run,
		"ada.human.example.com",
	} {
		if _, err := ParseACL(names, []byte("readers: []\nwriters: []\ndeploys: [\""+bad+"\"]\n")); err == nil || !strings.Contains(err.Error(), "is not a deploy's identity") {
			t.Errorf("deploys %s: %v", bad, err)
		}
	}
	for _, bad := range []string{deploy, "//.ok/workflow/ci.yaml:pre:push", "//.ok/workflow/ci.yaml:post:Push"} {
		if _, err := ParseACL(names, []byte("readers: []\nwriters: []\nruns: [\""+bad+"\"]\n")); err == nil || !strings.Contains(err.Error(), "is not a post job's identity") {
			t.Errorf("runs %s: %v", bad, err)
		}
	}
	for _, good := range []string{deploy, "//.ok/workflow/root.yaml:deploy:a", "//a/b-c/.ok/workflow/x.y.yaml:deploy:d_e.f"} {
		if !ValidDeploy(good) {
			t.Errorf("%s is a deploy", good)
		}
	}
}

func TestRefs(t *testing.T) {
	for in, want := range map[string]string{"//secret/prod/db": "secret/prod/db", "secret/a": "secret/a"} {
		if got, ok := ParseRef(in); !ok || got != want {
			t.Errorf("ParseRef(%q) = %q, %v", in, got, ok)
		}
	}
	for _, bad := range []string{"//secret", "//secret/acl.yaml", "//secret/x/OWNERS", "//other/x", "//secret/.x"} {
		if _, ok := ParseRef(bad); ok {
			t.Errorf("ParseRef(%q) took it", bad)
		}
	}
}
