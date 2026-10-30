package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/enterprise"
	"github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/id/seal"
	idstore "github.com/Cidan/ok/internal/id/store"
	idpostgres "github.com/Cidan/ok/internal/id/store/postgres"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/tenancy"
	"github.com/Cidan/ok/internal/testdb"
)

type moveInSource struct{ snap *directory.Snapshot }

func (s moveInSource) Snapshot(context.Context) (*directory.Snapshot, error) { return s.snap, nil }
func (moveInSource) Changes(context.Context, string) (*directory.Delta, error) {
	return nil, directory.ErrNoDelta
}

// ok identity move-in: an installation whose system of record is Entra ID moves in as acme, its
// administrator its owner, its people linked to the organization's identity provider — the
// installation's, its secret sealed for the organization — and says what of the configuration to
// change; it moves in once, and refuses a hosted service's configuration.
func TestIdentityMoveIn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	file := func(name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	dsn := testdb.Postgres(t)
	cfgPath := file("ok.yaml", `
domain: example.com
public_url: https://ok.example.com
database: {url_file: `+file("database-url", dsn)+`}
identity:
  bootstrap_admins: [ada]
  tree_repository: org
  sealing_key_file: `+file("sealing-key", seal.NewKey())+`
  system_of_record:
    entra: {tenant_id: contoso, client_id: ok, client_secret_file: `+file("client-secret", "the secret\n")+`}
`+busYAML(t))

	// The installation, as identity left it: its organization, and its people synced.
	pool, err := pgdb.Open(ctx, dsn, role.Schema, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	b, err := idpostgres.Open(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	names := identity.MustNames("example.com")
	st := idstore.New(b, names)
	if _, err := st.CheckMode(ctx, false); err != nil {
		t.Fatal(err)
	}
	inst, err := st.EnsureOrganization(ctx, "example", idstore.OrganizationSettings{BaseRole: tenancy.Write, MembersCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	st.SetInstallation(inst.ID)
	src := moveInSource{&directory.Snapshot{People: []directory.Person{
		{ExternalID: "e-ada", Handle: "ada", Email: "ada@example.com", Name: "Ada", Active: true},
		{ExternalID: "e-bea", Handle: "bea", Email: "bea@example.com", Name: "Bea", Active: true},
	}}}
	opts, err := st.SyncOptions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := directory.Sync(ctx, names, src, st, opts); err != nil {
		t.Fatal(err)
	}

	cmd := func(args ...string) (string, error) {
		var out bytes.Buffer
		err := run(args, strings.NewReader(""), &out)
		return out.String(), err
	}
	out, err := cmd("identity", "move-in", "-config", cfgPath, "-organization", "acme")
	if err != nil {
		t.Fatalf("move-in: %v\n%s", err, out)
	}
	for _, want := range []string{
		"moved the installation in as the organization acme (renamed from example",
		"2 members, 1 of them owners",
		"entra, the organization's identity provider, 2 people linked",
		"set tenancy.mode: hosted",
		"set identity.tree_repository: acme/org",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("move-in says no %q:\n%s", want, out)
		}
	}
	p, err := st.IdentityProviderOf(ctx, inst.ID)
	if err != nil || p.Source != "entra" || !p.PlainHandles {
		t.Fatalf("the organization's identity provider = %+v, %v", p, err)
	}
	sealer, err := seal.Load(filepath.Join(dir, "sealing-key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, sec, err := enterprise.Open(sealer, p); err != nil || sec.EntraClientSecret != "the secret" {
		t.Errorf("the provider's secret = %+v, %v", sec, err)
	}
	if bytes.Contains(p.Sealed, []byte("the secret")) {
		t.Error("the provider's secret is stored in the clear")
	}
	if ada, err := st.OrganizationPerson(ctx, inst.ID, "e-ada"); err != nil || ada.Name != "ada.human.example.com" {
		t.Errorf("ada, linked = %+v, %v", ada, err)
	}

	if _, err := cmd("identity", "move-in", "-config", cfgPath); !errors.Is(err, idstore.ErrMode) {
		t.Errorf("moving in twice: %v", err)
	}
	hosted := file("hosted.yaml", "domain: example.com\npublic_url: https://ok.example.com\ntenancy: {mode: hosted}\n")
	if _, err := cmd("identity", "move-in", "-config", hosted); err == nil || !strings.Contains(err.Error(), "installation's configuration") {
		t.Errorf("moving in with a hosted service's configuration: %v", err)
	}
}
