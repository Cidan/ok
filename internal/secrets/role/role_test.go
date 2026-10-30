package role_test

import (
	"context"
	"errors"
	"net"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/idtest"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/scm/encoding"
	scmrole "github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/scm/scmtest"
	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/secrets/keys"
	"github.com/Cidan/ok/internal/secrets/keys/keystest"
	"github.com/Cidan/ok/internal/secrets/role"
	"github.com/Cidan/ok/internal/secrets/service"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
)

const (
	ada        = "ada.human.example.com"
	bea        = "bea.human.example.com"
	scmBot     = "scm.ok.robot.example.com"
	secretsBot = "secrets.ok.robot.example.com"
	deployBot  = "deploy.ok.robot.example.com"
	autoBot    = "automation.ok.robot.example.com"
)

func serveRole(t *testing.T, ctx context.Context, root node.Node, opts ...serve.Option) string {
	t.Helper()
	return serveOn(t, ctx, listen(t), root, opts...)
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return lis
}

// serveOn serves a role's root on lis until the test ends.
func serveOn(t *testing.T, ctx context.Context, lis net.Listener, root node.Node, opts ...serve.Option) string {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, root, append(opts, serve.WithListener(lis))...) }()
	t.Cleanup(func() { cancel(); <-done })
	return lis.Addr().String()
}

func as(t *testing.T, addr, principal string) *grpc.ClientConn {
	t.Helper()
	c, err := dial.Dial(addr, dial.Options{DevPrincipal: principal})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// tree stages a tree holding files at their paths, and its subtrees.
func tree(b *scmtest.Objects, files map[string]string) encoding.ID {
	dirs := map[string]map[string]encoding.TreeValue{"": {}}
	var paths []string
	for p := range files {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	for _, p := range paths {
		for d := path.Dir(p); ; d = path.Dir(d) {
			if d == "." {
				break
			}
			if dirs[d] == nil {
				dirs[d] = map[string]encoding.TreeValue{}
			}
		}
		dir := path.Dir(p)
		if dir == "." {
			dir = ""
		}
		dirs[dir][path.Base(p)] = encoding.TreeValue{Kind: encoding.TreeValueFile, ID: b.File([]byte(files[p]))}
	}
	var build func(dir string) encoding.ID
	build = func(dir string) encoding.ID {
		entries := map[string]encoding.TreeValue{}
		for name, v := range dirs[dir] {
			entries[name] = v
		}
		for d := range dirs {
			if d != "" && (path.Dir(d) == dir || dir == "" && path.Dir(d) == ".") {
				entries[path.Base(d)] = encoding.TreeValue{Kind: encoding.TreeValueTree, ID: build(d)}
			}
		}
		var names []string
		for n := range entries {
			names = append(names, n)
		}
		slices.Sort(names)
		t := &encoding.Tree{}
		for _, n := range names {
			t.Entries = append(t.Entries, encoding.TreeEntry{Name: n, Value: entries[n]})
		}
		return b.Add(t)
	}
	return build("")
}

// ok secrets, assembled as it runs: a real identity whose tree declares ops (ada), a real SCM
// whose main holds the repository's settings and //secret's acl.yaml, and ok secrets reading both
// as its robot — people writing, unsealing, and reading through its services, the SCM checking
// pointers, and each service admitting only its callers.
func TestRole(t *testing.T) {
	ctx := context.Background()
	id := idtest.Start(t, "ada", "bea")
	trees := identitypb.NewTreeClient(id.As(deployBot))
	if _, err := trees.Apply(ctx, &identitypb.ApplyRequest{Commit: "seed", Sequence: 1, Files: []*identitypb.File{
		{Path: "acl.yaml", Content: []byte("members: [" + ada + "]\n")},
		{Path: "ops/acl.yaml", Content: []byte("members: [" + ada + "]\n")},
	}}); err != nil {
		t.Fatalf("applying the tree: %v", err)
	}
	cfg, err := config.Parse([]byte("domain: example.com\npublic_url: http://127.0.0.1:1\nroles: " + idtest.Roles + "\nsecrets: {root: " +
		keystest.Keyring(t, 1) + "}\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Bus = bustest.Config(t)
	scmPool, err := pgdb.Open(ctx, testdb.Postgres(t), scmrole.Schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scmPool.Close)
	// ok secrets listens first: the SCM asks it about uploads' secrets.
	secLis := listen(t)
	scm, err := scmrole.Build(ctx, cfg, scmrole.Options{Dev: true, Pool: scmPool, Blobs: testobjects.Store(t), Identity: id.As(scmBot),
		Secrets: as(t, secLis.Addr().String(), scmBot)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scm.Close)
	scmAddr := serveRole(t, ctx, scm.Root, serve.WithAuthn(scm.Resolver))

	// The repository, its main holding settings that keep secrets and //secret's acl.yaml.
	r, err := scmpb.NewRepositoriesClient(as(t, scmAddr, ada)).CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	repo := r.GetId()
	b := scmtest.NewObjects(t)
	commit := b.Commit(tree(b, map[string]string{
		".ok/repository.yaml":  "build: none\nsecrets: {}\n",
		"secret/acl.yaml":      "readers: [ops]\nwriters: [ops]\n",
		"secret/prod/acl.yaml": "readers: [ops]\nwriters: [ops]\nunseal: passkey\n",
	}), "keep secrets", encoding.RootCommitID)
	if _, _, err := b.Publish(ctx, scmpb.NewStoreClient(as(t, scmAddr, ada)), repo, nil); err != nil {
		t.Fatal(err)
	}
	up, err := scmpb.NewChangeListsClient(as(t, scmAddr, ada)).Upload(ctx, &scmpb.UploadRequest{RepositoryId: repo, CommitIds: [][]byte{commit[:]},
		Readiness: scmpb.UploadRequest_READINESS_READY})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scmpb.NewLandingClient(as(t, scmAddr, autoBot)).Land(ctx, &scmpb.LandRequest{RepositoryId: repo, ActorId: id.ID(ada),
		ExpectedHead: encoding.RootCommitID[:], Items: []*scmpb.LandRequest_Item{{Number: up.GetResults()[0].GetNumber(), Patchset: 1}}}); err != nil {
		t.Fatal(err)
	}

	secPool, err := pgdb.Open(ctx, testdb.Postgres(t), role.Schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(secPool.Close)
	// The repository's organization names its own root, a key in its Cloud KMS (a fake): the
	// repository's keys are wrapped by it, not the installation's keyring.
	org, err := id.Store.OrganizationByHandle(ctx, "example")
	if err != nil {
		t.Fatal(err)
	}
	orgRoot := "gcp-kms:" + keystest.GCPKeys[1]
	if err := id.Store.SetSecretsRoot(ctx, org.ID, orgRoot, id.ID(ada)); err != nil {
		t.Fatal(err)
	}
	sec, err := role.Build(ctx, cfg, role.Options{Dev: true, Pool: secPool, Identity: id.As(secretsBot), SCM: as(t, scmAddr, secretsBot),
		Opener: &keys.Opener{GCP: keystest.GCPKMS(t)}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sec.Close)
	addr := serveOn(t, ctx, secLis, sec.Root, serve.WithAuthn(sec.Resolver))
	adas, beas := secretspb.NewSecretsClient(as(t, addr, ada)), secretspb.NewSecretsClient(as(t, addr, bea))

	if a, err := adas.CheckAccess(ctx, &secretspb.CheckAccessRequest{RepositoryId: repo, Path: "secret/token"}); err != nil || !a.GetKeepsSecrets() || !a.GetRead() || !a.GetWrite() {
		t.Fatalf("ada's access: %+v, %v", a, err)
	}
	if a, err := beas.CheckAccess(ctx, &secretspb.CheckAccessRequest{RepositoryId: repo, Path: "secret/token"}); err != nil || a.GetRead() || a.GetWrite() ||
		!strings.Contains(a.GetWhyNotRead(), "ops.acl.example.com") {
		t.Fatalf("bea's access: %+v, %v", a, err)
	}
	w, err := adas.WriteSecret(ctx, &secretspb.WriteSecretRequest{RepositoryId: repo, Path: "secret/token", Value: []byte("t0ken")})
	if err != nil {
		t.Fatal(err)
	}
	var wrappedBy string
	if err := secPool.QueryRow(ctx, `SELECT root_id FROM repository_key WHERE repository_id = $1`, repo).Scan(&wrappedBy); err != nil || wrappedBy != orgRoot {
		t.Errorf("the repository's key is wrapped by %q (%v), not its organization's %q", wrappedBy, err, orgRoot)
	}
	if _, err := adas.ReadSecret(ctx, &secretspb.ReadSecretRequest{RepositoryId: repo, Path: "secret/token", Digest: w.GetDigest()}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("a read before unsealing: %v", err)
	}
	// A folder that asks for a passkey asks ok identity, which — at an address, as here — has none.
	if a, err := adas.CheckAccess(ctx, &secretspb.CheckAccessRequest{RepositoryId: repo, Path: "secret/prod/db"}); err != nil || !a.GetPasskey() {
		t.Errorf("secret/prod asks for a passkey: %+v, %v", a, err)
	}
	if _, err := adas.Unseal(ctx, &secretspb.UnsealRequest{RepositoryId: repo, Paths: []string{"secret/prod/db"}}); status.Code(err) != codes.FailedPrecondition ||
		!strings.Contains(err.Error(), "signs no one in with passkeys") {
		t.Errorf("unsealing a passkey folder where identity has no passkeys: %v", err)
	}
	if u, err := adas.Unseal(ctx, &secretspb.UnsealRequest{RepositoryId: repo, Paths: []string{"secret/token"}}); err != nil || u.GetState() != secretspb.UnsealResponse_STATE_GRANTED {
		t.Fatalf("unsealing: %+v, %v", u, err)
	}
	got, err := adas.ReadSecret(ctx, &secretspb.ReadSecretRequest{RepositoryId: repo, Path: "secret/token", Digest: w.GetDigest()})
	if err != nil || string(got.GetValue()) != "t0ken" {
		t.Fatalf("read %q, %v", got.GetValue(), err)
	}
	var d secrets.Digest
	var o secrets.Opening
	copy(d[:], w.GetDigest())
	copy(o[:], got.GetOpening())
	if !secrets.Verify(d, repo, "secret/token", o, got.GetValue()) || string(w.GetPointer()) != string(secrets.Pointer(d)) {
		t.Error("the value read does not match the pointer written")
	}
	if _, err := beas.WriteSecret(ctx, &secretspb.WriteSecretRequest{RepositoryId: repo, Path: "secret/token", Value: []byte("x")}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("bea writing: %v", err)
	}

	// The SCM's checks, and each service's callers.
	versions := secretspb.NewVersionsClient(as(t, addr, scmBot))
	if k, err := versions.CheckPointers(ctx, &secretspb.CheckPointersRequest{RepositoryId: repo, Pointers: []*secretspb.Pointer{
		{Path: "secret/token", Digest: w.GetDigest()}, {Path: "secret/other", Digest: w.GetDigest()}}}); err != nil || !slices.Equal(k.GetKept(), []bool{true, false}) {
		t.Errorf("the SCM's check: %v, %v", k.GetKept(), err)
	}
	if _, err := secretspb.NewVersionsClient(as(t, addr, ada)).CheckPointers(ctx, &secretspb.CheckPointersRequest{RepositoryId: repo}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("a person calling the SCM's service: %v", err)
	}
	if _, err := secretspb.NewSecretsClient(as(t, addr, scmBot)).CheckAccess(ctx, &secretspb.CheckAccessRequest{RepositoryId: repo, Path: "secret/token"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("a robot calling people's service: %v", err)
	}
	if a, err := sec.Store.AuditOf(ctx, repo, 0, 10); err != nil || len(a) < 4 {
		t.Errorf("the audit: %+v, %v", a, err)
	}

	// The pointer, committed and uploaded: the SCM asks ok secrets whether it keeps the version,
	// and whether its uploader writes the folder.
	files := map[string]string{
		".ok/repository.yaml":  "build: none\nsecrets: {}\n",
		"secret/acl.yaml":      "readers: [ops]\nwriters: [ops]\ndeploys: [\"//.ok/workflow/cd.yaml:deploy:db\"]\n",
		".ok/workflow/cd.yaml": "deploy:\n  db:\n    migrate: [//db:migrate]\n    secrets: [//secret/token]\n",
		"secret/prod/acl.yaml": "readers: [ops]\nwriters: [ops]\nunseal: passkey\n",
		"secret/token":         string(w.GetPointer()),
	}
	head := scmpb.NewRepositoriesClient(as(t, scmAddr, ada))
	upload := func(who string, files map[string]string, guards bool) (*scmpb.UploadResponse, error) {
		t.Helper()
		h, err := head.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}})
		if err != nil {
			t.Fatal(err)
		}
		var parent encoding.ID
		copy(parent[:], h.GetHead())
		// The first commit's builder, so each commit is a change of its own.
		b.GuardsSecrets = guards
		c := b.Commit(tree(b, files), "a change by "+who, parent)
		if _, _, err := b.Publish(ctx, scmpb.NewStoreClient(as(t, scmAddr, who)), repo, nil); err != nil {
			return nil, err
		}
		return scmpb.NewChangeListsClient(as(t, scmAddr, who)).Upload(ctx, &scmpb.UploadRequest{RepositoryId: repo, CommitIds: [][]byte{c[:]},
			Readiness: scmpb.UploadRequest_READINESS_READY})
	}
	if _, err := upload(ada, files, false); status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "does not guard them") {
		t.Fatalf("a publish that does not guard secrets: %v", err)
	}
	up2, err := upload(ada, files, true)
	if err != nil {
		t.Fatalf("ada's pointer: %v", err)
	}
	h, _ := head.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}})
	if _, err := scmpb.NewLandingClient(as(t, scmAddr, autoBot)).Land(ctx, &scmpb.LandRequest{RepositoryId: repo, ActorId: id.ID(ada),
		ExpectedHead: h.GetHead(), Items: []*scmpb.LandRequest_Item{{Number: up2.GetResults()[0].GetNumber(), Patchset: 1}}}); err != nil {
		t.Fatal(err)
	}
	// The deployer reads the token for a deployment of main's head, which names it, as main's
	// acl.yaml gives the deploy; and nothing for a commit off main, nor as anyone else.
	h, _ = head.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}})
	deploys := secretspb.NewDeploySecretsClient(as(t, addr, deployBot))
	readDeploy := func(commit []byte) (*secretspb.ReadSecretResponse, error) {
		return deploys.ReadDeploySecret(ctx, &secretspb.ReadDeploySecretRequest{RepositoryId: repo, CommitId: commit,
			Deploy: "//.ok/workflow/cd.yaml:deploy:db", Path: "secret/token"})
	}
	if v, err := readDeploy(h.GetHead()); err != nil || string(v.GetValue()) != "t0ken" {
		t.Errorf("the deploy's secret at main's head: %q, %v", v.GetValue(), err)
	}
	if _, err := readDeploy(nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("no commit: %v", err)
	}
	if _, err := secretspb.NewDeploySecretsClient(as(t, addr, ada)).ReadDeploySecret(ctx, &secretspb.ReadDeploySecretRequest{RepositoryId: repo,
		CommitId: h.GetHead(), Deploy: "//.ok/workflow/cd.yaml:deploy:db", Path: "secret/token"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("a person reading a deploy's secret: %v", err)
	}
	rotated, err := adas.WriteSecret(ctx, &secretspb.WriteSecretRequest{RepositoryId: repo, Path: "secret/token", Value: []byte("r0tated")})
	if err != nil {
		t.Fatal(err)
	}
	beas2 := map[string]string{}
	for k, v := range files {
		beas2[k] = v
	}
	beas2["secret/token"] = string(rotated.GetPointer())
	if _, err := upload(bea, beas2, true); !refusedFor(err, "only a writer of secret") {
		t.Errorf("bea changing a secret she does not write: %v", err)
	}
	forged := map[string]string{}
	for k, v := range files {
		forged[k] = v
	}
	forged["secret/token"] = string(secrets.Pointer(secrets.Digest{9}))
	if _, err := upload(ada, forged, true); !refusedFor(err, "does not keep") {
		t.Errorf("a pointer to no version: %v", err)
	}

	// A collection, asking the real SCM: main's version stays; the rotation nothing references is
	// destroyed past the grace.
	c := &service.Collector{Store: sec.Store, Refs: service.SCMReferences{Client: scmpb.NewSecretReferencesClient(as(t, scmAddr, secretsBot))},
		Grace: time.Nanosecond}
	for range 2 {
		if err := c.Pass(ctx, repo); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	var rd secrets.Digest
	copy(rd[:], rotated.GetDigest())
	if _, _, err := sec.Store.Open(ctx, repo, "secret/token", rd); !errors.Is(err, store.ErrDestroyed) {
		t.Errorf("the unreferenced rotation: %v", err)
	}
	if _, v, err := sec.Store.Open(ctx, repo, "secret/token", d); err != nil || string(v) != "t0ken" {
		t.Errorf("main's version: %q, %v", v, err)
	}
	if _, err := scmpb.NewSecretReferencesClient(as(t, scmAddr, ada)).ListSecretReferences(ctx, &scmpb.ListSecretReferencesRequest{RepositoryId: repo}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("a person listing references: %v", err)
	}
}

// refusedFor reports whether an upload was refused for its //secret, with a problem mentioning want.
func refusedFor(err error, want string) bool {
	for _, d := range status.Convert(err).Details() {
		if r, ok := d.(*scmpb.UploadRejection); ok && r.GetReason() == scmpb.UploadRejection_REASON_INVALID_SECRETS {
			for _, p := range r.GetProblems() {
				if strings.Contains(p, want) {
					return true
				}
			}
		}
	}
	return false
}
