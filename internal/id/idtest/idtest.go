// Package idtest is a development ok identity for the tests of roles that need one: on loopback,
// over a new database, its people from a static system of record, its robots ok's roles', and its
// installation's organization example; it trusts the development principal a request names, as
// roles in development do.
package idtest

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/mover"
	idrole "github.com/Cidan/ok/internal/id/role"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/testdb"
)

// Roles are the robots of ok's roles the identity knows, as a configuration's roles name them.
const Roles = "{identity: identity.ok, scm: scm.ok, control: control.ok, automation: automation.ok, worker: worker.ok, deploy: deploy.ok, edge: edge.ok, secrets: secrets.ok}"

// Identity is a running development identity.
type Identity struct {
	// Addr is where it serves: host:port.
	Addr string
	// Store is its store.
	Store *store.Store
	// Mover runs its moves between cells: a test drives a pass at once (Pass).
	Mover *mover.Runner
	t     testing.TB
}

// Start starts one for the domain example.com, whose people have the handles given (each
// <handle>@example.com), and waits for its sync to make them. It runs until the test ends.
func Start(t testing.TB, people ...string) *Identity {
	t.Helper()
	return StartWithCells(t, nil, people...)
}

// StartWithCells is Start, the directory placing repositories in cells (identity.cells; none, the
// one cell default).
func StartWithCells(t testing.TB, cells []config.IdentityCell, people ...string) *Identity {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	dir := t.TempDir()
	if err := ca.Init(filepath.Join(dir, "ca"), "Example"); err != nil {
		t.Fatal(err)
	}
	var file strings.Builder
	file.WriteString("people:\n")
	for i, h := range people {
		fmt.Fprintf(&file, "  - {id: \"%d\", email: %s@example.com, name: %s}\n", i+1, h, h)
	}
	if err := os.WriteFile(filepath.Join(dir, "people.yaml"), []byte(file.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte("domain: example.com\npublic_url: http://127.0.0.1:1\nroles: " + Roles + "\nidentity:\n  ca_dir: " +
		filepath.Join(dir, "ca") + "\n  system_of_record: {static: {file: " + filepath.Join(dir, "people.yaml") + "}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Bus = bustest.Config(t)
	cfg.Identity.Cells = cells
	pool, err := pgdb.Open(ctx, testdb.Postgres(t), idrole.Schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	id, err := idrole.Build(ctx, cfg, idrole.Options{Dev: true, Pool: pool})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, id.Root, serve.WithAuthn(id.Resolver), serve.WithListener(lis)) }()
	t.Cleanup(func() {
		cancel()
		<-done
		id.Close()
	})
	for _, h := range people {
		for i := 0; ; i++ {
			if _, err := id.Store.Principal(ctx, h+".human.example.com"); err == nil {
				break
			}
			if i == 200 {
				t.Fatalf("the sync never made %s", h)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	return &Identity{Addr: lis.Addr().String(), Store: id.Store, Mover: id.Mover, t: t}
}

// As is a connection to the identity naming a principal, as a development role's.
func (i *Identity) As(principal string) *grpc.ClientConn {
	i.t.Helper()
	conn, err := dial.Dial(i.Addr, dial.Options{DevPrincipal: principal})
	if err != nil {
		i.t.Fatal(err)
	}
	i.t.Cleanup(func() { conn.Close() })
	return conn
}

// ID is a principal's stable ID.
func (i *Identity) ID(name string) string {
	i.t.Helper()
	p, err := i.Store.Principal(context.Background(), name)
	if err != nil {
		i.t.Fatalf("%s: %v", name, err)
	}
	return p.ID
}
