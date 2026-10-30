package edge_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/edge"
	"github.com/Cidan/ok/internal/edge/role"
	"github.com/Cidan/ok/internal/id/idtest"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/scm/encoding"
	scmrole "github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/scm/scmtest"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

const ada = "ada.human.example.com"

// listen is a loopback listener for a test.
func listen(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// startEdge serves an edge in front of id's cells, as a development edge, with cfgYAML's edge
// section; it returns where it serves.
func startEdge(t *testing.T, ctx context.Context, id *idtest.Identity, edgeYAML string) string {
	t.Helper()
	cfg, err := config.Parse([]byte("domain: example.com\npublic_url: http://127.0.0.1:1\nroles: " + idtest.Roles +
		"\nservices: {identity: " + id.Addr + "}\n" + edgeYAML))
	if err != nil {
		t.Fatal(err)
	}
	r, err := role.Build(ctx, cfg, role.Options{Dev: true, Identity: id.As("edge.ok.robot.example.com")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	l := listen(t)
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, r.Root, serve.WithListener(l)) }()
	t.Cleanup(func() { <-done })
	return l.Addr().String()
}

// startSCM serves a development SCM of a cell on l.
func startSCM(t *testing.T, ctx context.Context, id *idtest.Identity, cell string, l net.Listener) {
	t.Helper()
	cfg, err := config.Parse([]byte("domain: example.com\npublic_url: http://127.0.0.1:1\nroles: " + idtest.Roles + "\ncell: {name: " + cell + "}\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Bus = bustest.Config(t)
	pool, err := pgdb.Open(ctx, testdb.Postgres(t), scmrole.Schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	scm, err := scmrole.Build(ctx, cfg, scmrole.Options{Dev: true, Pool: pool, Blobs: testobjects.Store(t), Identity: id.As("scm.ok.robot.example.com")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scm.Close)
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, scm.Root, serve.WithAuthn(scm.Resolver), serve.WithListener(l)) }()
	t.Cleanup(func() { <-done })
}

// dialAs dials addr as a development principal.
func dialAs(t *testing.T, addr, principal string) *grpc.ClientConn {
	t.Helper()
	conn, err := dial.Dial(addr, dial.Options{DevPrincipal: principal})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// naming is ctx with the ok-repository header naming repo.
func naming(ctx context.Context, repo string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, strings.ToLower(edge.RepositoryHeader), repo)
}

// Through one edge in front of two cells, a person makes repositories in the cells the directory
// places them in, reads each from its cell whether the request names it in the ok-repository header
// or not — a request the home cell refuses as the other's is sent again there, whole — and
// publishes to each, a stream sent again just as a unary call is (docs/hosted.md, "Cells, as
// built").
func TestEdgeRoutesToCells(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lis := map[string]net.Listener{"c1": listen(t), "c2": listen(t)}
	id := idtest.StartWithCells(t, []config.IdentityCell{{Name: "c1", SCM: lis["c1"].Addr().String()}, {Name: "c2", SCM: lis["c2"].Addr().String()}}, "ada")
	for name, l := range lis {
		startSCM(t, ctx, id, name, l)
	}
	edgeAddr := startEdge(t, ctx, id, "")
	conn := dialAs(t, edgeAddr, ada)
	repos, store := scmpb.NewRepositoriesClient(conn), scmpb.NewStoreClient(conn)

	cellOf := map[int64]string{}
	var ids []int64
	for i := range 4 {
		name := fmt.Sprint("app-", i)
		r, err := repos.CreateRepository(naming(ctx, name), &scmpb.CreateRepositoryRequest{Name: name})
		if err != nil {
			t.Fatalf("making %s through the edge: %v", name, err)
		}
		e, err := id.Store.Repository(ctx, r.GetId())
		if err != nil {
			t.Fatal(err)
		}
		cellOf[r.GetId()] = e.Cell
		ids = append(ids, r.GetId())
	}
	if n := len(map[string]bool{cellOf[ids[0]]: true, cellOf[ids[1]]: true}); n != 2 {
		t.Fatalf("the repositories' cells: %v; want both cells used", cellOf)
	}
	for _, repo := range ids {
		byID := &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}}
		for how, c := range map[string]context.Context{"named": naming(ctx, fmt.Sprint(repo)), "unnamed": ctx} {
			if got, err := repos.GetRepository(c, byID); err != nil || got.GetId() != repo {
				t.Errorf("repository %d in %s, %s: %v, %v", repo, cellOf[repo], how, got, err)
			}
		}
		// A publish, its stream naming no repository: the home cell refuses it for the other's, and
		// the edge sends it again there.
		b := scmtest.NewObjects(t)
		tree := b.Add(&encoding.Tree{Entries: []encoding.TreeEntry{{Name: "f", Value: encoding.TreeValue{Kind: encoding.TreeValueFile,
			ID: b.File([]byte(fmt.Sprint("repository ", repo, "\n")))}}}})
		b.Commit(tree, "a change", encoding.RootCommitID)
		if _, _, err := b.Publish(ctx, store, repo, nil); err != nil {
			t.Errorf("publishing to repository %d in %s: %v", repo, cellOf[repo], err)
		}
	}

	// identity's services go to the directory.
	if _, err := identitypb.NewCAClient(conn).GetRoots(ctx, &identitypb.GetRootsRequest{}); err != nil {
		t.Errorf("identity's CA through the edge: %v", err)
	}
	// A principal there is none of is refused at the edge.
	nobody := dialAs(t, edgeAddr, "nobody.human.example.com")
	if _, err := scmpb.NewRepositoriesClient(nobody).GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: ids[0]}}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("an unknown principal: %v", err)
	}
}

// A page a cell's ok control answers 421 for, naming another cell, is sent there — a form's body
// too — and the edge goes there first next time; and each principal's and address's requests are
// limited.
func TestEdgePagesAndLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var asked [2]int
	control := func(i int, answer func(w http.ResponseWriter, r *http.Request)) net.Listener {
		l := listen(t)
		var p http.Protocols
		p.SetHTTP1(true)
		p.SetUnencryptedHTTP2(true)
		srv := &http.Server{Protocols: &p, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			asked[i]++
			answer(w, r)
		})}
		go srv.Serve(l)
		t.Cleanup(func() { srv.Close() })
		return l
	}
	c1 := control(0, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(cells.Header, "c2")
		w.WriteHeader(http.StatusMisdirectedRequest)
	})
	c2 := control(1, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "c2 %s %s %s", r.Method, r.URL.Path, b)
	})
	id := idtest.StartWithCells(t, []config.IdentityCell{
		{Name: "c1", SCM: "127.0.0.1:1", Control: c1.Addr().String()},
		{Name: "c2", SCM: "127.0.0.1:1", Control: c2.Addr().String()},
	}, "ada")
	edgeAddr := startEdge(t, ctx, id, "edge: {limits: {principal: {per_second: 0.001, burst: 3}, address: {per_second: 0.001, burst: 6}}}\n")

	post := func(principal string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, "http://"+edgeAddr+"/acme/app/cls", strings.NewReader(url.Values{"title": {"x"}}.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if principal != "" {
			req.Header.Set("Ok-Dev-Principal", principal)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := post(ada); code != http.StatusOK || body != "c2 POST /acme/app/cls title=x" {
		t.Fatalf("a page c1 sent to c2: %d %q", code, body)
	}
	if code, _ := post(ada); code != http.StatusOK || asked != [2]int{1, 2} {
		t.Errorf("the page again: %d, asked c1 %d and c2 %d times; want c2 first", code, asked[0], asked[1])
	}
	// ada's third request is her last for a while; the address's sixth its last — a request refused
	// for its principal counts for no address.
	if code, _ := post(ada); code != http.StatusOK {
		t.Errorf("ada's third request: %d", code)
	}
	if code, _ := post(ada); code != http.StatusTooManyRequests {
		t.Errorf("ada's fourth request: %d; want her limited", code)
	}
	for i := range 3 {
		if code, _ := post(""); code != http.StatusOK {
			t.Errorf("no one's request %d: %d", i, code)
		}
	}
	if code, _ := post(""); code != http.StatusTooManyRequests {
		t.Errorf("the address's seventh request: %d; want it limited", code)
	}
}

// A request about no repository that names its cell goes to that cell's role — a deploy agent's
// connection to a cell's deployer — and one naming none to the home cell.
func TestEdgeNamedCell(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deployer := func(name string) net.Listener {
		l := listen(t)
		var p http.Protocols
		p.SetHTTP1(true)
		p.SetUnencryptedHTTP2(true)
		srv := &http.Server{Protocols: &p, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "%s %s", name, r.URL.Path)
		})}
		go srv.Serve(l)
		t.Cleanup(func() { srv.Close() })
		return l
	}
	d1, d2 := deployer("c1"), deployer("c2")
	id := idtest.StartWithCells(t, []config.IdentityCell{
		{Name: "c1", SCM: "127.0.0.1:1", Deploy: d1.Addr().String()},
		{Name: "c2", SCM: "127.0.0.1:1", Deploy: d2.Addr().String()},
	}, "ada")
	edgeAddr := startEdge(t, ctx, id, "")
	ask := func(cell string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, "http://"+edgeAddr+"/ok.deploy.v1.Agents/Connect", nil)
		req.Header.Set("Ok-Dev-Principal", ada)
		if cell != "" {
			req.Header.Set(cells.Header, cell)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}
	for cell, want := range map[string]string{"c2": "c2 /ok.deploy.v1.Agents/Connect", "c1": "c1 /ok.deploy.v1.Agents/Connect"} {
		if got := ask(cell); got != want {
			t.Errorf("naming %s: %q, want %q", cell, got, want)
		}
	}
	if got := ask(""); !strings.HasPrefix(got, "c") || !strings.HasSuffix(got, " /ok.deploy.v1.Agents/Connect") {
		t.Errorf("naming no cell: %q, want the home cell's", got)
	}
}
