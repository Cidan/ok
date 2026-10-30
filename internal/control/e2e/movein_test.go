package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/Cidan/ok/internal/bus/busconfig"
	idrole "github.com/Cidan/ok/internal/id/role"
	idstore "github.com/Cidan/ok/internal/id/store"
	idpostgres "github.com/Cidan/ok/internal/id/store/postgres"
)

// Moving an installation in: an installation's repository, with a CL landed on it, is moved in as
// acme's while ok is stopped; started again as a hosted service, ok serves it as acme/svc — its old
// URL redirecting there, its landed CL's page naming its author as a hosted service does — a clone
// made before the move syncs and lands another CL, and the repository's bare name clones it still.
func TestMoveIn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	controlLis := listen(t, "")
	base := "http://" + controlLis.Addr().String()
	cfg := testConfig(t, base)
	res := &resources{cfg: cfg, open: opener(t, cfg), blobs: bucket(t), logs: bucket(t), seed: true}
	res.deployer, res.deployAddr = startDeployer(t)
	e, stop := startStack(t, ctx, res, controlLis)
	wctx, stopWorker := context.WithCancel(ctx)
	go e.passEverything(wctx)

	e.post(dana, "/repositories/new", url.Values{"name": {"svc"}})
	b := e.person(bob)
	b.jj("", "clone", "svc")
	b.write("svc/serve/main.go", "package main\n")
	b.jj("svc", "describe", "-m", "add serve")
	first := uploaded(b.jj("svc", "cl", "--ready"))[0]
	e.post(alice, clPath("svc", first)+"/reply", grant("LGTM", "Approval"))
	e.prePassed("svc", first)
	e.submitted(bob, clPath("svc", first), "submit")
	stopWorker()
	stop()

	// Moved in, ok stopped: the installation's organization, acme.
	st := idstore.New(mustIdentity(t, ctx, res), res.cfg.Names())
	inst, err := st.OrganizationByHandle(ctx, "example")
	if err != nil {
		t.Fatal(err)
	}
	st.SetInstallation(inst.ID)
	admin, err := st.Principal(ctx, dana)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := st.MoveIn(ctx, idstore.MoveInPlan{Organization: inst.ID, Handle: "acme", Owners: []string{admin.ID}})
	if err != nil || rep.Repositories != 1 {
		t.Fatalf("MoveIn = %+v, %v", rep, err)
	}
	res.cfg.Tenancy.Mode = "hosted"
	res.cfg.Identity.SystemOfRecord = nil
	res.cfg.Identity.TreeRepository = "acme/" + treeRepository

	// Started again, a hosted service.
	e, _ = startStack(t, ctx, res, listen(t, controlLis.Addr().String()))
	go e.passEverything(ctx)
	if p := e.do(alice, http.MethodGet, "/example/svc", nil, nil); p.status != http.StatusFound || p.header.Get("Location") != "/acme/svc" {
		t.Errorf("the installation's URL: %d to %q", p.status, p.header.Get("Location"))
	}
	e.waitFor("ok control to name the repository acme/svc", func() bool {
		_, ok, _ := e.db.RepositoryByName(ctx, "acme/svc")
		return ok
	})
	if p := e.get(alice, "/acme/svc"); !p.has(">serve<") {
		t.Errorf("the repository's page: %s", excerpt(p.body))
	}
	if p := e.get(alice, fmt.Sprintf("/acme/svc/cl/%d", first)); !p.has("@bob") {
		t.Errorf("the landed CL's page names its author otherwise: %s", excerpt(p.body))
	}

	// bob's clone, from before the move, syncs and lands another CL.
	b.jj("svc", "sync")
	b.jj("svc", "new", "main@ok")
	b.write("svc/later.go", "package main\n")
	b.jj("svc", "describe", "-m", "add later")
	second := uploaded(b.jj("svc", "cl", "--ready"))[0]
	path := fmt.Sprintf("/acme/svc/cl/%d", second)
	e.post(alice, path+"/reply", grant("LGTM", "Approval"))
	e.waitFor("the second CL's pre to pass", func() bool {
		_, verdict, _, _ := checks(t, e.get(alice, path+"/checks"))
		return verdict == "Passed"
	})
	e.submitted(bob, path, "submit")

	// The bare name an installation's people knew it by clones it still.
	c := e.person(carol)
	c.jj("", "clone", "svc")
	if _, err := os.Stat(filepath.Join(c.home, "svc", "later.go")); err != nil {
		t.Errorf("carol's clone of svc lacks what landed after the move: %v", err)
	}
}

// mustIdentity opens identity's database as ok identity's commands do: on Spanner where the
// configuration says (idrole.OpenBackend).
func mustIdentity(t *testing.T, ctx context.Context, res *resources) idstore.Backend {
	t.Helper()
	if p := res.open(idrole.Schema); p != nil {
		b, err := idpostgres.Open(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		// As ok identity move-in opens it (idrole.OpenBackend): its keyed work's jobs, the directory's
		// relays among them, on the configuration's bus.
		bb, err := busconfig.Open(ctx, res.cfg.Bus)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(bb.Close)
		if err := bb.RegisterWork(b.Work(), "identity"); err != nil {
			t.Fatal(err)
		}
		return b
	}
	b, _, done, err := idrole.OpenBackend(ctx, res.cfg, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(done)
	return b
}
