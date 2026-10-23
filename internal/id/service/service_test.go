package service_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/ca"
	"github.com/Cidan/ok/internal/id/service"
	"github.com/Cidan/ok/internal/id/store"
	idpostgres "github.com/Cidan/ok/internal/id/store/postgres"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/health"
	"github.com/Cidan/ok/internal/serve/interceptor"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/serve/serving"
	"github.com/Cidan/ok/internal/testdb"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

var names = identity.MustNames("example.com")

const (
	ada     = "ada.human.example.com"
	bea     = "bea.human.example.com"
	deploy  = "deploy.ok.robot.example.com"
	control = "control.ok.robot.example.com"
)

type svcNode struct{ s *service.Server }

func (svcNode) Name() string                 { return "ok/identity/test" }
func (svcNode) Dependencies() []node.Node    { return nil }
func (svcNode) Healthy(context.Context) bool { return true }
func (svcNode) Setup(context.Context) error  { return nil }
func (n svcNode) RegisterRPC(_ context.Context, r grpc.ServiceRegistrar) error {
	n.s.Register(r)
	return nil
}

type source struct{ snap *directory.Snapshot }

func (s source) Snapshot(context.Context) (*directory.Snapshot, error) { return s.snap, nil }
func (source) Changes(context.Context, string) (*directory.Delta, error) {
	return nil, directory.ErrNoDelta
}

// env is an identity server on loopback, over a real database and CA.
type env struct {
	t     *testing.T
	addr  string
	store *store.Store
	ca    *ca.CA
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	b, err := idpostgres.Open(ctx, pool)
	st := store.New(b, names)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := ca.Init(dir, "Example"); err != nil {
		t.Fatal(err)
	}
	authority, err := ca.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	snap := &directory.Snapshot{
		People: []directory.Person{
			{ExternalID: "1", Handle: "ada", Email: "ada@example.com", Active: true},
			{ExternalID: "2", Handle: "bea", Email: "bea@example.com", Active: true},
			{ExternalID: "3", Handle: "cal", Email: "cal@example.com", Active: true},
		},
		Groups: []directory.Group{{ExternalID: "g1", Name: "it", Members: []directory.Member{{ExternalID: "3"}}}},
	}
	opts, _ := st.SyncOptions(ctx)
	if _, err := directory.Sync(ctx, names, source{snap}, st, opts); err != nil {
		t.Fatal(err)
	}
	for _, r := range []string{deploy, control} {
		if _, err := st.CreateRobot(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	srv := service.New(service.Config{Store: st, CA: authority, PublicURL: "https://ok.example.com/", BootstrapAdmins: []string{"ada"}})
	resolver := authn.NewResolver(authn.Config{
		Directory:   service.Directory{Store: st},
		Credentials: credential.NewVerifier(authority.Pool),
		Dev:         true,
		Roles:       authn.Roles{okpb.Caller_CALLER_DEPLOY: deploy, okpb.Caller_CALLER_CONTROL: control},
	})
	chain := interceptor.New()
	chain.UseUnary(resolver.UnaryInterceptor())
	chain.UseStream(resolver.StreamInterceptor())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- serving.Serve(sctx, serving.Config{
			Nodes: []node.Node{svcNode{srv}}, Chain: chain, Listener: lis,
			Health: health.New(func(context.Context) bool { return true }),
		})
	}()
	t.Cleanup(func() { cancel(); <-done })
	return &env{t: t, addr: lis.Addr().String(), store: st, ca: authority}
}

// as dials the server as a principal ("" for no one).
func (e *env) as(principal string) *grpc.ClientConn {
	e.t.Helper()
	conn, err := dial.Dial(e.addr, dial.Options{DevPrincipal: principal})
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { conn.Close() })
	return conn
}

// with dials the server proving a credential.
func (e *env) with(cert []byte, key *ecdsa.PrivateKey) *grpc.ClientConn {
	e.t.Helper()
	conn, err := dial.Dial(e.addr, dial.Options{Credential: dial.Static{Cert: cert, Key: key}})
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { conn.Close() })
	return conn
}

func code(err error) codes.Code { return status.Code(err) }

func TestDirectoryAndCallers(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	d := identitypb.NewDirectoryClient(e.as(ada))
	resp, err := d.GetPrincipal(ctx, &identitypb.GetPrincipalRequest{Key: &identitypb.GetPrincipalRequest_Name{Name: bea}})
	if err != nil || resp.Principal.Kind != identitypb.Kind_KIND_HUMAN || resp.Principal.Email != "bea@example.com" {
		t.Fatalf("GetPrincipal = %v, %v", resp, err)
	}
	byID, err := d.GetPrincipal(ctx, &identitypb.GetPrincipalRequest{Key: &identitypb.GetPrincipalRequest_Id{Id: resp.Principal.Id}})
	if err != nil || byID.Principal.Name != bea {
		t.Errorf("by ID: %v, %v", byID, err)
	}
	if _, err := d.GetPrincipal(ctx, &identitypb.GetPrincipalRequest{Key: &identitypb.GetPrincipalRequest_Name{Name: "nobody.human.example.com"}}); code(err) != codes.NotFound {
		t.Errorf("no such principal: %v", err)
	}
	var all []string
	token := ""
	for {
		page, err := d.ListPrincipals(ctx, &identitypb.ListPrincipalsRequest{PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range page.Principals {
			all = append(all, p.Name)
		}
		if token = page.NextPageToken; token == "" {
			break
		}
	}
	if want := []string{ada, bea, "cal.human.example.com", control, deploy}; !reflect.DeepEqual(all, want) {
		t.Errorf("paged = %v, want %v", all, want)
	}
	humans, _ := d.ListPrincipals(ctx, &identitypb.ListPrincipalsRequest{Kind: identitypb.Kind_KIND_ROBOT, Query: "deploy"})
	if len(humans.Principals) != 1 || humans.Principals[0].Name != deploy {
		t.Errorf("a robot query = %v", humans.Principals)
	}

	// No caller may not read the directory; anyone may read the CA's roots.
	anon := e.as("")
	if _, err := identitypb.NewDirectoryClient(anon).ListSets(ctx, &identitypb.ListSetsRequest{}); code(err) != codes.Unauthenticated {
		t.Errorf("an anonymous directory read: %v", err)
	}
	roots, err := identitypb.NewCAClient(anon).GetRoots(ctx, &identitypb.GetRootsRequest{})
	if err != nil || len(roots.Certificates) != 1 {
		t.Errorf("GetRoots = %v, %v", roots, err)
	}
	// Verify is the roles', and Tree the deployer's.
	if _, err := identitypb.NewVerifyClient(e.as(ada)).GetSession(ctx, &identitypb.GetSessionRequest{Id: "x"}); code(err) != codes.PermissionDenied {
		t.Errorf("a person verifying sessions: %v", err)
	}
	if _, err := identitypb.NewTreeClient(e.as(control)).GetApplied(ctx, &identitypb.GetAppliedRequest{}); code(err) != codes.PermissionDenied {
		t.Errorf("control applying trees: %v", err)
	}
}

func treeFiles(files map[string]string) []*identitypb.File {
	var out []*identitypb.File
	for p, c := range files {
		out = append(out, &identitypb.File{Path: p, Content: []byte(c)})
	}
	return out
}

func TestTreeAndAdmins(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	adminAda := identitypb.NewAdminClient(e.as(ada))
	adminBea := identitypb.NewAdminClient(e.as(bea))

	// Before a tree, the bootstrap admins administer.
	if _, err := adminAda.CreateRobot(ctx, &identitypb.CreateRobotRequest{Name: "ci"}); err != nil {
		t.Fatal(err)
	}
	if _, err := adminBea.CreateRobot(ctx, &identitypb.CreateRobotRequest{Name: "other"}); code(err) != codes.PermissionDenied {
		t.Errorf("a non-admin making robots: %v", err)
	}
	if _, err := adminAda.CreateRobot(ctx, &identitypb.CreateRobotRequest{Name: "Not A Name"}); code(err) != codes.InvalidArgument {
		t.Errorf("a bad robot name: %v", err)
	}
	if _, err := identitypb.NewAdminClient(e.as(deploy)).ListRobots(ctx, &identitypb.ListRobotsRequest{}); code(err) != codes.PermissionDenied {
		t.Errorf("a robot administering: %v", err)
	}
	isAdmin := func(who string) bool {
		t.Helper()
		resp, err := identitypb.NewDirectoryClient(e.as(control)).CheckAdmin(ctx, &identitypb.CheckAdminRequest{Principal: who})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Admin
	}
	if !isAdmin(ada) || isAdmin(bea) || isAdmin("nobody.human.example.com") {
		t.Errorf("CheckAdmin before a tree: ada %v, bea %v", isAdmin(ada), isAdmin(bea))
	}
	listAdmins := func() []string {
		t.Helper()
		resp, err := identitypb.NewDirectoryClient(e.as(control)).ListAdmins(ctx, &identitypb.ListAdminsRequest{})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Principals
	}
	if got := listAdmins(); !reflect.DeepEqual(got, []string{ada}) {
		t.Errorf("ListAdmins before a tree = %v", got)
	}

	tree := identitypb.NewTreeClient(e.as(deploy))
	files := treeFiles(map[string]string{
		"acl.yaml":       "members: [" + bea + "]\n",
		"eng/acl.yaml":   "members: [it.group.example.com]\n",
		"eng/group.yaml": "members: [" + ada + "]\n",
	})
	plan, err := tree.Plan(ctx, &identitypb.PlanRequest{Files: files})
	if err != nil || len(plan.Changes) != 3 {
		t.Fatalf("Plan = %v, %v", plan, err)
	}
	if _, err := tree.Apply(ctx, &identitypb.ApplyRequest{Commit: "c1", Sequence: 1, Files: files}); err != nil {
		t.Fatal(err)
	}
	if again, err := tree.Apply(ctx, &identitypb.ApplyRequest{Commit: "c1", Sequence: 1, Files: files}); err != nil || len(again.Changes) != 0 {
		t.Errorf("the same apply again: %v, %v", again, err)
	}
	if _, err := tree.Apply(ctx, &identitypb.ApplyRequest{Commit: "c0", Sequence: 0, Files: files}); code(err) != codes.Aborted {
		t.Errorf("an older commit: %v", err)
	}
	bad := treeFiles(map[string]string{"acl.yaml": "members: [nobody@example.com]\n"})
	if _, err := tree.Apply(ctx, &identitypb.ApplyRequest{Commit: "c2", Sequence: 2, Files: bad}); code(err) != codes.InvalidArgument {
		t.Errorf("an invalid tree: %v", err)
	}

	// The tree's admins are the admins now.
	if _, err := adminAda.ListRobots(ctx, &identitypb.ListRobotsRequest{}); code(err) != codes.PermissionDenied {
		t.Errorf("a bootstrap admin after the tree: %v", err)
	}
	robots, err := adminBea.ListRobots(ctx, &identitypb.ListRobotsRequest{})
	if err != nil || len(robots.Robots) != 3 {
		t.Errorf("ListRobots = %v, %v", robots, err)
	}
	if isAdmin(ada) || !isAdmin(bea) {
		t.Errorf("CheckAdmin after the tree: ada %v, bea %v", isAdmin(ada), isAdmin(bea))
	}
	if got := listAdmins(); !reflect.DeepEqual(got, []string{bea}) {
		t.Errorf("ListAdmins after the tree = %v", got)
	}

	d := identitypb.NewDirectoryClient(e.as(ada))
	ex, err := d.ExpandSet(ctx, &identitypb.ExpandSetRequest{Name: "eng.acl.example.com"})
	if err != nil || !reflect.DeepEqual(ex.Principals, []string{"cal.human.example.com"}) {
		t.Errorf("ExpandSet through a synced group = %v, %v", ex, err)
	}
	m, err := d.CheckMembership(ctx, &identitypb.CheckMembershipRequest{Set: "eng.acl.example.com", Principal: "cal.human.example.com"})
	if err != nil || !m.Member || len(m.Chain) != 3 {
		t.Errorf("CheckMembership = %v, %v", m, err)
	}
	set, err := d.GetSet(ctx, &identitypb.GetSetRequest{Name: "it.group.example.com"})
	if err != nil || set.Set.Source != identitypb.SetSource_SET_SOURCE_SYNCED {
		t.Errorf("a synced group = %v, %v", set, err)
	}
	set, err = d.GetSet(ctx, &identitypb.GetSetRequest{Name: "eng.group.example.com"})
	if err != nil || set.Set.Owner != "eng.acl.example.com" || set.Set.Path != "eng" {
		t.Errorf("a tree group = %v, %v", set, err)
	}
}

func TestDeviceLoginAndCredentials(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	key, err := credential.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	spki, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	login := identitypb.NewDeviceLoginClient(e.as(""))
	start, err := login.Start(ctx, &identitypb.StartRequest{PublicKey: spki, Label: "ada's laptop"})
	if err != nil {
		t.Fatal(err)
	}
	if start.VerificationUri != "https://ok.example.com/id/device" || start.VerificationUriComplete != start.VerificationUri+"?code="+start.UserCode || start.Interval != 5 {
		t.Errorf("Start = %v", start)
	}
	if poll, err := login.Poll(ctx, &identitypb.PollRequest{DeviceCode: start.DeviceCode}); err != nil || poll.State != identitypb.PollState_POLL_STATE_PENDING {
		t.Errorf("a pending poll = %v, %v", poll, err)
	}
	if _, err := login.Start(ctx, &identitypb.StartRequest{PublicKey: []byte("junk"), Label: "x"}); code(err) != codes.InvalidArgument {
		t.Errorf("a junk key: %v", err)
	}

	// ada approves in the browser (the web page calls the store).
	adaP, _ := e.store.Principal(ctx, ada)
	if err := e.store.DecideDeviceLogin(ctx, start.UserCode, adaP.ID, true); err != nil {
		t.Fatal(err)
	}
	time.Sleep(store.PollInterval)
	poll, err := login.Poll(ctx, &identitypb.PollRequest{DeviceCode: start.DeviceCode})
	if err != nil || poll.State != identitypb.PollState_POLL_STATE_APPROVED || poll.Principal != ada || len(poll.Certificate.Chain) != 2 {
		t.Fatalf("an approved poll = %v, %v", poll, err)
	}
	if again, _ := login.Poll(ctx, &identitypb.PollRequest{DeviceCode: start.DeviceCode}); again.State != identitypb.PollState_POLL_STATE_EXPIRED {
		t.Errorf("a poll after the certificate: %v", again.State)
	}

	// The certificate signs in.
	conn := e.with(poll.Certificate.Chain[0], key)
	creds := identitypb.NewCredentialsClient(conn)
	renewed, err := creds.Renew(ctx, &identitypb.RenewRequest{})
	if err != nil || renewed.Certificate.Credential != poll.Certificate.Credential {
		t.Fatalf("Renew = %v, %v", renewed, err)
	}
	list, err := creds.List(ctx, &identitypb.ListCredentialsRequest{})
	if err != nil || len(list.Credentials) != 1 || list.Credentials[0].Label != "ada's laptop" {
		t.Errorf("List = %v, %v", list, err)
	}
	// A person renews nothing without a credential.
	if _, err := identitypb.NewCredentialsClient(e.as(ada)).Renew(ctx, &identitypb.RenewRequest{}); code(err) != codes.FailedPrecondition {
		t.Errorf("Renew without a credential: %v", err)
	}
	// bea cannot revoke ada's credential; ada can.
	if _, err := identitypb.NewCredentialsClient(e.as(bea)).Revoke(ctx, &identitypb.RevokeRequest{Id: poll.Certificate.Credential}); code(err) != codes.NotFound {
		t.Errorf("revoking another's: %v", err)
	}
	if _, err := creds.Revoke(ctx, &identitypb.RevokeRequest{Id: poll.Certificate.Credential}); err != nil {
		t.Fatal(err)
	}
	got, err := identitypb.NewVerifyClient(e.as(control)).GetCredential(ctx, &identitypb.GetCredentialRequest{Id: poll.Certificate.Credential})
	if err != nil || got.Active || got.Principal.Name != ada {
		t.Errorf("GetCredential after revoking = %v, %v", got, err)
	}
}

func TestRobotsAndSessions(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	admin := identitypb.NewAdminClient(e.as(ada))
	if _, err := admin.CreateRobot(ctx, &identitypb.CreateRobotRequest{Name: "ci"}); err != nil {
		t.Fatal(err)
	}
	b := &identitypb.Binding{Issuer: "https://token.actions.githubusercontent.com", Subject: "repo:acme/app:ref:refs/heads/main"}
	if _, err := admin.Bind(ctx, &identitypb.BindRequest{Robot: "ci", Binding: b}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Bind(ctx, &identitypb.BindRequest{Robot: "ci.deploy", Binding: b}); code(err) != codes.NotFound {
		t.Errorf("binding a robot that does not exist: %v", err)
	}
	key, _ := credential.NewKey()
	spki, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	cert, err := admin.CertifyRobot(ctx, &identitypb.CertifyRobotRequest{Robot: "ci", PublicKey: spki, Label: "runner"})
	if err != nil {
		t.Fatal(err)
	}
	conn := e.with(cert.Certificate.Chain[0], key)
	if _, err := identitypb.NewDirectoryClient(conn).ListSets(ctx, &identitypb.ListSetsRequest{}); err != nil {
		t.Errorf("the robot reading the directory: %v", err)
	}
	if _, err := identitypb.NewAdminClient(conn).ListRobots(ctx, &identitypb.ListRobotsRequest{}); code(err) != codes.PermissionDenied {
		t.Errorf("a robot administering: %v", err)
	}
	if _, err := admin.DeleteRobot(ctx, &identitypb.DeleteRobotRequest{Name: "ci"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := identitypb.NewVerifyClient(e.as(control)).GetCredential(ctx, &identitypb.GetCredentialRequest{Id: cert.Certificate.Credential}); got.Active {
		t.Error("a deleted robot's credential is active")
	}

	adaP, _ := e.store.Principal(ctx, ada)
	tok, _ := e.store.CreateSession(ctx, adaP.ID, "oidc", time.Hour, time.Hour)
	verify := identitypb.NewVerifyClient(e.as(control))
	s, err := verify.GetSession(ctx, &identitypb.GetSessionRequest{Id: tok})
	if err != nil || s.Principal.Name != ada {
		t.Errorf("GetSession = %v, %v", s, err)
	}
	if _, err := verify.GetSession(ctx, &identitypb.GetSessionRequest{Id: "forged"}); code(err) != codes.NotFound {
		t.Errorf("a forged session: %v", err)
	}
}
