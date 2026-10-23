package service

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// A request names its repository at its top, in its lease, or in its beginning.
func TestRepositoryOf(t *testing.T) {
	cases := []struct {
		name string
		got  func() (int64, bool)
		want int64
		ok   bool
	}{
		{"at its top", func() (int64, bool) { return repositoryOf(&scmpb.ListChangeListsRequest{RepositoryId: 7}) }, 7, true},
		{"in its lease", func() (int64, bool) { return repositoryOf(&scmpb.RenewRequest{Lease: &scmpb.Lease{RepositoryId: 8}}) }, 8, true},
		{"in its beginning", func() (int64, bool) {
			return repositoryOf(&scmpb.PublishRequest{Step: &scmpb.PublishRequest_Begin_{Begin: &scmpb.PublishRequest_Begin{RepositoryId: 9}}})
		}, 9, true},
		{"nowhere", func() (int64, bool) { return repositoryOf(&scmpb.CreateRepositoryRequest{Name: "x"}) }, 0, false},
	}
	for _, tc := range cases {
		if got, ok := tc.got(); got != tc.want || ok != tc.ok {
			t.Errorf("%s: %d, %v; want %d, %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

// Every request a person makes is decided by Authorize: one's own private repository is unseen
// by others, as though there were none, until they are granted a role; a reader reads and
// publishes to their own workspace, and proposes only where proposing needs read; the
// installation's repositories are its people's. ok's own roles are not checked here.
func TestAuthorize(t *testing.T) {
	c := testServer(t)
	alice, bob := human("alice.human.example.com"), human("bob.human.example.com")
	own, err := c.repos.CreateRepository(alice, &scmpb.CreateRepositoryRequest{Name: "alice/private"})
	if err != nil {
		t.Fatal(err)
	}
	repo := own.GetId()
	read := func(who context.Context) error {
		_, err := c.cls.ListChangeLists(who, &scmpb.ListChangeListsRequest{RepositoryId: repo})
		return err
	}
	stream := func(who context.Context) error {
		s, err := c.store.ReadObjects(who, &scmpb.ReadObjectsRequest{RepositoryId: repo})
		if err != nil {
			return err
		}
		_, err = s.Recv()
		return err
	}
	publish := func(who context.Context) error {
		p, err := c.store.Publish(who)
		if err != nil {
			return err
		}
		if err := p.Send(&scmpb.PublishRequest{Step: &scmpb.PublishRequest_Begin_{Begin: &scmpb.PublishRequest_Begin{RepositoryId: repo}}}); err != nil {
			return err
		}
		p.CloseSend()
		_, err = p.Recv()
		return err
	}
	lease := func(who context.Context) error {
		_, err := c.ws.Renew(who, &scmpb.RenewRequest{Lease: &scmpb.Lease{RepositoryId: repo, Workspace: "default", Machine: "m"}})
		return err
	}
	byID := func(who context.Context) error {
		_, err := c.repos.GetRepository(who, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}})
		return err
	}
	upload := func(who context.Context) error {
		_, err := c.cls.Upload(who, &scmpb.UploadRequest{RepositoryId: repo, CommitIds: [][]byte{make([]byte, 32)}})
		return err
	}
	all := map[string]func(context.Context) error{"listing CLs": read, "reading objects": stream, "publishing": publish,
		"renewing a lease": lease, "getting it by ID": byID, "uploading": upload}
	unseen := fmt.Sprintf("no repository has the ID %d", repo)
	// refused is Authorize's answer, whatever the method itself would answer past it.
	refused := func(err error) bool {
		s := status.Convert(err)
		return s.Code() == codes.PermissionDenied || s.Code() == codes.NotFound && s.Message() == unseen
	}

	// bob sees none of it.
	for what, do := range all {
		if err := do(bob); status.Code(err) != codes.NotFound || status.Convert(err).Message() != unseen {
			t.Errorf("bob %s on alice's private repository: %v, want it unseen", what, err)
		}
	}
	// Its owner, and ok's roles, are let through to the method itself.
	for what, do := range all {
		for who, ctx := range map[string]context.Context{"alice": alice, "ok control": control(), "ok automation": automation()} {
			if err := do(ctx); refused(err) {
				t.Errorf("%s %s: %v", who, what, err)
			}
		}
	}

	// Granted read, bob reads, publishes to his workspace, and proposes; where proposing needs
	// write, he is told so.
	c.ten.grant(repo, "id-bob.human.example.com", "read")
	c.access.checker.Forget(repo)
	for what, do := range all {
		if err := do(bob); refused(err) {
			t.Errorf("bob, a reader, %s: %v", what, err)
		}
	}
	c.ten.set(repo, "", "write")
	c.access.checker.Forget(repo)
	err = upload(bob)
	if status.Code(err) != codes.PermissionDenied || status.Convert(err).Message() != "To propose needs write, and the caller is read" {
		t.Errorf("a reader uploading where proposing needs write: %v", err)
	}
	if err := read(bob); err != nil {
		t.Errorf("a reader still reads: %v", err)
	}

	// The installation's repositories are its people's: carol reads and proposes on alice's.
	shared, err := c.repos.CreateRepository(alice, &scmpb.CreateRepositoryRequest{Name: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	carol := human("carol.human.example.com")
	if _, err := c.cls.ListChangeLists(carol, &scmpb.ListChangeListsRequest{RepositoryId: shared.GetId()}); err != nil {
		t.Errorf("a member reading the organization's repository: %v", err)
	}
	if _, err := c.cls.Upload(carol, &scmpb.UploadRequest{RepositoryId: shared.GetId(), CommitIds: [][]byte{make([]byte, 32)}}); status.Code(err) == codes.PermissionDenied {
		t.Errorf("a member proposing on the organization's repository: %v", err)
	}
	// A repository there is none of is none, however asked.
	if _, err := c.cls.ListChangeLists(bob, &scmpb.ListChangeListsRequest{RepositoryId: 42}); status.Code(err) != codes.NotFound {
		t.Errorf("a repository there is none of: %v", err)
	}
	if _, err := c.cls.ListChangeLists(bob, &scmpb.ListChangeListsRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a request naming no repository: %v", err)
	}
}

// A personal token narrows its holder: a writer with a read token proposes only where reading
// suffices, and reaches no repository it does not name.
func TestAuthorizeTokens(t *testing.T) {
	c := testServer(t)
	alice := human("alice.human.example.com")
	own, err := c.repos.CreateRepository(alice, &scmpb.CreateRepositoryRequest{Name: "alice/tokened"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := c.repos.CreateRepository(alice, &scmpb.CreateRepositoryRequest{Name: "alice/other"})
	if err != nil {
		t.Fatal(err)
	}
	repo := own.GetId()
	for _, id := range []int64{repo, other.GetId()} {
		c.ten.grant(id, "id-bob.human.example.com", "write")
	}
	c.ten.set(repo, "", "write")
	bearer := func(secret string) context.Context {
		return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+secret)
	}
	upload := func(ctx context.Context, id int64) error {
		_, err := c.cls.Upload(ctx, &scmpb.UploadRequest{RepositoryId: id, CommitIds: [][]byte{make([]byte, 32)}})
		return err
	}
	if err := upload(human("bob.human.example.com"), repo); status.Code(err) == codes.PermissionDenied {
		t.Errorf("bob, a writer, proposing: %v", err)
	}
	read := bearer(fmt.Sprintf("okp_read_%d_bob.human.example.com", repo))
	if err := upload(read, repo); status.Code(err) != codes.PermissionDenied || status.Convert(err).Message() != "To propose needs write, and the caller is read" {
		t.Errorf("bob proposing with a read token: %v", err)
	}
	if _, err := c.cls.ListChangeLists(read, &scmpb.ListChangeListsRequest{RepositoryId: repo}); err != nil {
		t.Errorf("bob reading with a read token: %v", err)
	}
	if _, err := c.cls.ListChangeLists(read, &scmpb.ListChangeListsRequest{RepositoryId: other.GetId()}); status.Code(err) != codes.NotFound {
		t.Errorf("bob's read token on a repository it does not name: %v", err)
	}
	// By name too: the token, not the directory's answer for bob, says what he sees.
	if _, err := c.repos.GetRepository(read, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: "alice/other"}}); status.Code(err) != codes.NotFound {
		t.Errorf("bob's read token naming another repository: %v", err)
	}
	if got, err := c.repos.GetRepository(read, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: "alice/tokened"}}); err != nil || got.GetId() != repo {
		t.Errorf("bob's read token naming its repository: %v, %v", got, err)
	}
	if _, err := c.cls.ListChangeLists(bearer("okp_nonsense"), &scmpb.ListChangeListsRequest{RepositoryId: repo}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("a token there is none of: %v", err)
	}
}

// A robot that is none of ok's roles — an organization's CI — is decided by Authorize like a person:
// a member of nothing, it reads what it is granted, and is refused what its grant does not reach.
func TestAuthorizeRobots(t *testing.T) {
	c := testServer(t)
	alice := human("alice.human.example.com")
	shared, err := c.repos.CreateRepository(alice, &scmpb.CreateRepositoryRequest{Name: "robotic"})
	if err != nil {
		t.Fatal(err)
	}
	ci := as("ci.acme.robot.example.com")
	read := func() error {
		_, err := c.cls.ListChangeLists(ci, &scmpb.ListChangeListsRequest{RepositoryId: shared.GetId()})
		return err
	}
	if err := read(); status.Code(err) != codes.NotFound {
		t.Errorf("a robot granted nothing reading the installation's repository: %v", err)
	}
	if _, err := c.repos.GetRepository(ci, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: "robotic"}}); status.Code(err) != codes.NotFound {
		t.Errorf("a robot granted nothing naming it: %v", err)
	}
	c.ten.grant(shared.GetId(), "id-ci.acme.robot.example.com", "read")
	c.access.checker.Forget(shared.GetId())
	if err := read(); err != nil {
		t.Errorf("a robot granted read reading: %v", err)
	}
	if _, err := c.cls.Upload(ci, &scmpb.UploadRequest{RepositoryId: shared.GetId(), CommitIds: [][]byte{make([]byte, 32)}}); status.Code(err) == codes.NotFound {
		t.Errorf("a robot granted read proposing where reading suffices: %v", err)
	}
	// ok's own roles are not decided here.
	if _, err := c.cls.ListChangeLists(automation(), &scmpb.ListChangeListsRequest{RepositoryId: shared.GetId()}); err != nil {
		t.Errorf("ok automation reading: %v", err)
	}
}

// An app's installation token reaches the installing organization's repositories alone, however
// much the app's robot is granted elsewhere.
func TestAuthorizeInstallationTokens(t *testing.T) {
	c := testServer(t)
	alice := human("alice.human.example.com")
	shared, err := c.repos.CreateRepository(alice, &scmpb.CreateRepositoryRequest{Name: "installed"})
	if err != nil {
		t.Fatal(err)
	}
	private, err := c.repos.CreateRepository(alice, &scmpb.CreateRepositoryRequest{Name: "alice/elsewhere"})
	if err != nil {
		t.Fatal(err)
	}
	const robot = "id-deployer.acme.robot.example.com"
	for _, id := range []int64{shared.GetId(), private.GetId()} {
		c.ten.grant(id, robot, "write")
	}
	tok := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer oki_write_org-"+installation+"_deployer.acme.robot.example.com")
	if _, err := c.cls.ListChangeLists(tok, &scmpb.ListChangeListsRequest{RepositoryId: shared.GetId()}); err != nil {
		t.Errorf("the installation token on its organization's repository: %v", err)
	}
	if _, err := c.cls.ListChangeLists(tok, &scmpb.ListChangeListsRequest{RepositoryId: private.GetId()}); status.Code(err) != codes.NotFound {
		t.Errorf("the installation token on another owner's repository: %v", err)
	}
}
