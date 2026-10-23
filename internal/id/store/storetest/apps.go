package storetest

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

func pkix(t *testing.T, k any) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

// Apps: a robot of their organization with a key (ECDSA P-256 or Ed25519); installed by their own
// organization, or any when public, on its repositories alone; installed again, their role and
// repositories change and their tokens go; an installation's token is the app's robot's, for an
// hour, reaching the installing organization alone, and goes with its installation.
func testApps(t *testing.T, s *store.Store) {
	ctx := context.Background()
	src := &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true)}}}
	sync(t, s, src)
	ada, _ := s.Principal(ctx, "ada.human.example.com")
	acme, err := s.CreateOrganization(ctx, "acme", ada.ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}
	globex, err := s.CreateOrganization(ctx, "globex", ada.ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}
	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	edPub, _, _ := ed25519.GenerateKey(rand.Reader)
	p384, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	rsaKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	for name, key := range map[string][]byte{"no key": nil, "P-384": pkix(t, &p384.PublicKey), "RSA": pkix(t, &rsaKey.PublicKey), "garbage": []byte("nope")} {
		if _, _, err := s.CreateApp(ctx, acme.ID, "x", false, key); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("an app with %s: %v", name, err)
		}
	}
	deployer, robot, err := s.CreateApp(ctx, acme.ID, "deployer", false, pkix(t, &ec.PublicKey))
	if err != nil || !strings.HasPrefix(deployer.ID, "app_") || robot.Name != "deployer.acme.robot.example.com" {
		t.Fatalf("CreateApp = %+v, %+v, %v", deployer, robot, err)
	}
	if _, _, err := s.CreateApp(ctx, acme.ID, "deployer", false, pkix(t, &ec.PublicKey)); !errors.Is(err, store.ErrTaken) {
		t.Errorf("an app's name twice: %v", err)
	}
	helper, _, err := s.CreateApp(ctx, acme.ID, "helper", true, pkix(t, edPub))
	if err != nil {
		t.Fatal(err)
	}
	if a, err := s.AppOfRobot(ctx, robot.ID); err != nil || a.ID != deployer.ID {
		t.Errorf("AppOfRobot = %+v, %v", a, err)
	}
	if of, err := s.RobotOrganization(ctx, robot.ID); err != nil || of != acme.ID {
		t.Errorf("an app's robot's organization = %q, %v", of, err)
	}

	app, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: "app", Propose: tenancy.Read, CreatedBy: ada.ID})
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.ReserveRepository(ctx, store.NewRepository{Owner: globex.ID, Name: "app", Propose: tenancy.Read, CreatedBy: ada.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Install(ctx, deployer.ID, globex.ID, tenancy.Read, nil, ada.ID); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("another organization installing a private app: %v", err)
	}
	if _, err := s.Install(ctx, deployer.ID, acme.ID, tenancy.Write, []int64{other.ID}, ada.ID); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("an installation on another organization's repository: %v", err)
	}
	ins, err := s.Install(ctx, deployer.ID, acme.ID, tenancy.Write, []int64{app.ID, app.ID}, ada.ID)
	if err != nil || !strings.HasPrefix(ins.ID, "ins_") || len(ins.Repositories) != 1 || !ins.Covers(app.ID) || ins.Covers(other.ID) {
		t.Fatalf("Install = %+v, %v", ins, err)
	}
	everywhere, err := s.Install(ctx, helper.ID, globex.ID, tenancy.Read, nil, ada.ID)
	if err != nil || !everywhere.Covers(other.ID) {
		t.Errorf("a public app installed on every repository = %+v, %v", everywhere, err)
	}

	secret, tok, err := s.CreateInstallationToken(ctx, ins.ID)
	if err != nil || !strings.HasPrefix(secret, store.InstallationTokenPrefix) || tok.Principal != robot.ID || tok.Owner != acme.ID {
		t.Fatalf("CreateInstallationToken = %q, %+v, %v", secret, tok, err)
	}
	got, holder, err := s.TokenFor(ctx, secret)
	if err != nil || holder.ID != robot.ID || got.Owner != acme.ID || got.Installation != ins.ID || got.Role != tenancy.Write ||
		got.Expires.Sub(got.Created) != store.InstallationTokenLifetime {
		t.Errorf("TokenFor an installation token = %+v, %+v, %v", got, holder, err)
	}

	// Installed again: what it was installed with changes, and its tokens go.
	again, err := s.Install(ctx, deployer.ID, acme.ID, tenancy.Read, nil, ada.ID)
	if err != nil || again.ID != ins.ID || again.Role != tenancy.Read || !again.Covers(app.ID) {
		t.Errorf("installing again = %+v, %v", again, err)
	}
	if _, _, err := s.TokenFor(ctx, secret); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a token of what an installation was before: %v", err)
	}
	secret, _, err = s.CreateInstallationToken(ctx, ins.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Uninstall(ctx, ins.ID); err != nil || !ok {
		t.Fatalf("Uninstall = %v, %v", ok, err)
	}
	if _, _, err := s.TokenFor(ctx, secret); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("an uninstalled installation's token: %v", err)
	}
	if _, _, err := s.CreateInstallationToken(ctx, ins.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a token of an installation there is none of: %v", err)
	}

	// Deleted, an app is uninstalled everywhere and its robot retired.
	if err := s.DeleteApp(ctx, helper.ID); err != nil {
		t.Fatal(err)
	}
	if is, err := s.Installations(ctx, globex.ID); err != nil || len(is) != 0 {
		t.Errorf("globex's installations after the app's deletion = %+v, %v", is, err)
	}
	if _, err := s.App(ctx, helper.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a deleted app: %v", err)
	}
}
