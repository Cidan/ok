package move_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/control/db"
	controlrole "github.com/Cidan/ok/internal/control/role"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/idtest"
	idstore "github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/scm/encoding"
	scmrole "github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/scm/scmtest"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/testdb/roledb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

const ada = "ada.human.example.com"

func listen(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func dialAs(t *testing.T, addr, principal string) *grpc.ClientConn {
	t.Helper()
	conn, err := dial.Dial(addr, dial.Options{DevPrincipal: principal})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// cellRoles are a cell's development SCM and ok control, serving on their listeners.
type cellRoles struct {
	scm     *scmrole.Role
	control *controlrole.Role
}

func startCell(t *testing.T, ctx context.Context, id *idtest.Identity, name string, scmLis, controlLis net.Listener) cellRoles {
	t.Helper()
	return startCellOn(t, ctx, id, name, roledb.NewPostgres(t), scmLis, controlLis)
}

// startCellOn starts a cell's development SCM and ok control on its databases, PostgreSQL's or
// Spanner's, and a bus of its own.
func startCellOn(t *testing.T, ctx context.Context, id *idtest.Identity, name string, dbs *roledb.DBs, scmLis, controlLis net.Listener) cellRoles {
	t.Helper()
	cfg, err := config.Parse([]byte("domain: example.com\npublic_url: http://127.0.0.1:1\nroles: " + idtest.Roles + "\ncell: {name: " + name + "}\n"))
	if err != nil {
		t.Fatal(err)
	}
	dbs.Configure(cfg)
	cfg.Bus = bustest.Config(t)
	open := func(schema string) *pgxpool.Pool { return dbs.Pool(t, schema, 0) }
	scm, err := scmrole.Build(ctx, cfg, scmrole.Options{Dev: true, Pool: open(scmrole.Schema), Blobs: testobjects.Store(t),
		Identity: id.As("scm.ok.robot.example.com")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scm.Close)
	serveRole(t, ctx, scm.Root, scm.Resolver, scmLis)
	ctl, err := controlrole.Build(ctx, cfg, controlrole.Options{Dev: true, Pool: open(controlrole.Schema),
		Identity: id.As("control.ok.robot.example.com"), SCM: dialAs(t, scmLis.Addr().String(), "control.ok.robot.example.com")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ctl.Close)
	serveRole(t, ctx, ctl.Root, ctl.Resolver, controlLis)
	return cellRoles{scm: scm, control: ctl}
}

func serveRole(t *testing.T, ctx context.Context, root node.Node, resolver *authn.Resolver, l net.Listener) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, root, serve.WithAuthn(resolver), serve.WithListener(l)) }()
	t.Cleanup(func() { <-done })
}

// The directory moves a repository's network between two cells, each its own SCM and ok control,
// through each role's Moves service, as itself: the destination copies, the source freezes, the
// destination copies the rest, the directory names it, it opens there, and the source refuses it,
// naming the cell. Its history reads at the destination as at the source, it is written there, and
// ok control there holds its head.
func TestMoveBetweenCells(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scmLis := map[string]net.Listener{"c1": listen(t), "c2": listen(t)}
	ctlLis := map[string]net.Listener{"c1": listen(t), "c2": listen(t)}
	var list []config.IdentityCell
	for _, name := range []string{"c1", "c2"} {
		list = append(list, config.IdentityCell{Name: name, SCM: scmLis[name].Addr().String(), Control: ctlLis[name].Addr().String()})
	}
	id := idtest.StartWithCells(t, list, "ada")
	roles := map[string]cellRoles{}
	for _, name := range []string{"c1", "c2"} {
		roles[name] = startCell(t, ctx, id, name, scmLis[name], ctlLis[name])
	}
	at := func(cell string) *grpc.ClientConn { return dialAs(t, scmLis[cell].Addr().String(), ada) }

	r, err := scmpb.NewRepositoriesClient(at("c1")).CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	e, err := id.Store.Repository(ctx, r.GetId())
	if err != nil || e.Cell != "c1" {
		t.Fatalf("the repository's entry: %+v, %v; want it in c1, the first of two holding none", e, err)
	}
	publish := func(cell string, n int) {
		t.Helper()
		b := scmtest.NewObjects(t)
		tree := b.Add(&encoding.Tree{Entries: []encoding.TreeEntry{{Name: "f", Value: encoding.TreeValue{Kind: encoding.TreeValueFile,
			ID: b.File([]byte(fmt.Sprint("version ", n, "\n")))}}}})
		b.Commit(tree, fmt.Sprint("change ", n), encoding.RootCommitID)
		if _, _, err := b.Publish(ctx, scmpb.NewStoreClient(at(cell)), r.GetId(), nil); err != nil {
			t.Fatalf("publishing to cell %s: %v", cell, err)
		}
	}
	publish("c1", 1)

	m, err := id.Store.AskMove(ctx, r.GetId(), "c2", id.ID(ada))
	if err != nil || m.Phase != idstore.MoveCopying {
		t.Fatalf("asking the move: %+v, %v", m, err)
	}
	if _, err := id.Store.AskMove(ctx, r.GetId(), "c2", id.ID(ada)); err == nil {
		t.Error("asking the move again while it runs")
	}
	// Asked, its first step is booked, and identity's mover drives it.
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(50 * time.Millisecond) {
		if m, err = id.Store.GetMove(ctx, m.Network); err != nil {
			t.Fatal(err)
		}
		if m.Phase.Finished() || time.Now().After(deadline) {
			break
		}
	}
	if m.Phase != idstore.MoveDone || m.Error != "" {
		t.Fatalf("the move: %+v", m)
	}
	if e, err := id.Store.Repository(ctx, r.GetId()); err != nil || e.Cell != "c2" {
		t.Fatalf("the entry after the move: %+v, %v", e, err)
	}

	// The destination serves it — once the facts each SCM keeps a few seconds name the cell —
	// and the source refuses it, naming the destination.
	byID := &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: r.GetId()}}
	deadline := time.Now().Add(20 * time.Second)
	for {
		got, err := scmpb.NewRepositoriesClient(at("c2")).GetRepository(ctx, byID)
		_, srcErr := scmpb.NewRepositoriesClient(at("c1")).GetRepository(ctx, byID)
		m, refused := cells.MovedOf(srcErr)
		if err == nil && got.GetId() == r.GetId() && refused && m.GetCell() == "c2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the move: the destination %v, %v; the source %v", got, err, srcErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
	publish("c2", 2)

	// ok control's gates: open at the destination, and the source refusing it, naming the cell.
	about := move.WithRepository(ctx, r.GetId())
	if err := roles["c2"].control.DB.Txn(about, func(db.Ops) error { return nil }); err != nil {
		t.Errorf("a write about it at the destination's ok control: %v", err)
	}
	err = roles["c1"].control.DB.Txn(about, func(db.Ops) error { return nil })
	if m, moved := cells.MovedOf(err); !moved || m.GetCell() != "c2" {
		t.Errorf("a write about it at the source's ok control: %v; want it refused, naming c2", err)
	}
}
