package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/scm/store"
	scmpostgres "github.com/Cidan/ok/internal/scm/store/postgres"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

const (
	controlPrincipal    = "control.ok.robot.example.com"
	automationPrincipal = "automation.ok.robot.example.com"
	workerPrincipal     = "worker.ok.robot.example.com"
	followerPrincipal   = "deploy.ok.robot.example.com"
)

var testNames = identity.MustNames("example.com")

// directory is ok identity's directory for the tests, both as the resolver asks it and as the SCM
// does: every principal but those whose names start with "nobody" or "stranger", and one ACL,
// admins.acl.example.com.
type directory struct{}

func unknown(name string) bool {
	return strings.HasPrefix(name, "nobody") || strings.HasPrefix(name, "stranger")
}

func (directory) Principal(_ context.Context, name string) (authn.Principal, error) {
	if unknown(name) {
		return authn.Principal{}, authn.ErrNotFound
	}
	return authn.Principal{Name: name, ID: "id-" + name, Human: strings.Contains(name, ".human."), Active: true}, nil
}

func (directory) Credential(context.Context, string) (authn.Principal, error) {
	return authn.Principal{}, authn.ErrNotFound
}

func (directory) Session(context.Context, string) (authn.Principal, error) {
	return authn.Principal{}, authn.ErrNotFound
}

func (directory) GetPrincipal(_ context.Context, in *identitypb.GetPrincipalRequest, _ ...grpc.CallOption) (*identitypb.GetPrincipalResponse, error) {
	name := in.GetName()
	if unknown(name) {
		return nil, status.Error(codes.NotFound, "no such principal")
	}
	kind := identitypb.Kind_KIND_ROBOT
	if strings.Contains(name, ".human.") {
		kind = identitypb.Kind_KIND_HUMAN
	}
	return &identitypb.GetPrincipalResponse{Principal: &identitypb.Principal{Name: name, Kind: kind, Status: identitypb.Status_STATUS_ACTIVE}}, nil
}

func (directory) GetSet(_ context.Context, in *identitypb.GetSetRequest, _ ...grpc.CallOption) (*identitypb.GetSetResponse, error) {
	if in.GetName() != "admins.acl.example.com" {
		return nil, status.Error(codes.NotFound, "no such group or ACL")
	}
	return &identitypb.GetSetResponse{Set: &identitypb.Set{Name: in.GetName(), Kind: identitypb.SetKind_SET_KIND_ACL}}, nil
}

type clients struct {
	repos   scmpb.RepositoriesClient
	store   scmpb.StoreClient
	cls     scmpb.ChangeListsClient
	review  scmpb.ReviewClient
	landing scmpb.LandingClient
	checks  scmpb.LandingChecksClient
	browse  scmpb.BrowseClient
	events  scmpb.EventsClient
	ws      scmpb.WorkspacesClient
}

// testServer serves the services over a store on a new database and a new S3 bucket on the
// servers the test is given (internal/testdb), behind ok's authentication: callers are
// development principals, and each service admits only its declared callers.
func testServer(t *testing.T) clients {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	blobs := testobjects.Store(t)
	b, err := scmpostgres.Open(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.New(b, blobs, store.Config{Names: testNames, ObjectCacheBytes: 8 << 20, ChunkCacheBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	callers := authn.NewResolver(authn.Config{Directory: directory{}, Dev: true, Roles: authn.Roles{
		okpb.Caller_CALLER_CONTROL: controlPrincipal, okpb.Caller_CALLER_AUTOMATION: automationPrincipal,
		okpb.Caller_CALLER_WORKER: workerPrincipal, okpb.Caller_CALLER_DEPLOY: followerPrincipal,
	}})
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(callers.UnaryInterceptor()), grpc.ChainStreamInterceptor(callers.StreamInterceptor()))
	svc := New(s, directory{})
	scmpb.RegisterRepositoriesServer(srv, svc.Repositories)
	scmpb.RegisterStoreServer(srv, svc.Store)
	scmpb.RegisterChangeListsServer(srv, svc.ChangeLists)
	scmpb.RegisterReviewServer(srv, svc.Review)
	scmpb.RegisterLandingServer(srv, svc.Landing)
	scmpb.RegisterLandingChecksServer(srv, svc.LandingChecks)
	scmpb.RegisterBrowseServer(srv, svc.Browse)
	scmpb.RegisterEventsServer(srv, svc.Events)
	scmpb.RegisterWorkspacesServer(srv, svc.Workspaces)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return clients{repos: scmpb.NewRepositoriesClient(conn), store: scmpb.NewStoreClient(conn),
		cls: scmpb.NewChangeListsClient(conn), review: scmpb.NewReviewClient(conn), landing: scmpb.NewLandingClient(conn),
		checks: scmpb.NewLandingChecksClient(conn), browse: scmpb.NewBrowseClient(conn), events: scmpb.NewEventsClient(conn),
		ws: scmpb.NewWorkspacesClient(conn)}
}

// as is a request from a principal, named as a development server accepts.
func as(name string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), authn.DevPrincipalKey, name)
}

// human is a request from a person.
func human(name string) context.Context { return as(name) }

// control is a request from ok control.
func control() context.Context { return as(controlPrincipal) }

// follower is ok's deployer, which follows the change feed and does nothing else a person
// cannot.
func follower() context.Context { return as(followerPrincipal) }

// automation is a request from ok automation.
func automation() context.Context { return as(automationPrincipal) }

var repoSeq atomic.Int64

func newRepo(t *testing.T, c clients) *scmpb.Repository {
	t.Helper()
	r, err := c.repos.CreateRepository(human("alice.human.example.com"), &scmpb.CreateRepositoryRequest{
		Name: fmt.Sprintf("svc-%d-%d", time.Now().UnixNano(), repoSeq.Add(1)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func wantCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("got %v, want %s", err, code)
	}
}

// drain reads a server stream to its end.
func drain[T any](s grpc.ServerStreamingClient[T], err error) ([]*T, error) {
	if err != nil {
		return nil, err
	}
	var out []*T
	for {
		m, err := s.Recv()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, m)
	}
}

// objects builds canonical objects the way jj's client stages them.
type objects struct {
	t      *testing.T
	seq    int
	order  []*scmpb.EncodedObject
	files  map[encoding.ID][]byte
	chunks map[encoding.ID][]byte
	// workspace is whose operation log a publish's operations and views go to.
	workspace string
}

func newObjects(t *testing.T) *objects {
	return &objects{t: t, files: map[encoding.ID][]byte{}, chunks: map[encoding.ID][]byte{}}
}

func (b *objects) add(o encoding.Object) encoding.ID {
	b.t.Helper()
	canonical, err := encoding.Encode(o)
	if err != nil {
		b.t.Fatal(err)
	}
	id := encoding.Hash(canonical)
	b.order = append(b.order, &scmpb.EncodedObject{Kind: scmpb.ObjectKind(o.Kind()), Id: id[:], Body: canonical[1:]})
	return id
}

func (b *objects) file(contents []byte) encoding.ID {
	h := encoding.NewHasher(encoding.KindFile)
	h.Write(contents)
	id := h.Sum()
	b.files[id] = contents
	return id
}

func (b *objects) commit(tree encoding.ID, desc string, parents ...encoding.ID) encoding.ID {
	b.seq++
	sig := encoding.Signature{Name: "t", Email: "t@example.com", Time: encoding.Timestamp{Millis: 1_790_000_000_000 + int64(b.seq)}}
	var change encoding.ChangeID
	copy(change[:], fmt.Sprintf("svc-change-%05d", b.seq))
	return b.add(&encoding.Commit{
		Parents: parents, RootTree: encoding.Merge[encoding.ID]{tree}, ConflictLabels: encoding.Merge[string]{""},
		ChangeID: change, Description: desc, Author: sig, Committer: sig,
	})
}

func (b *objects) operation(view encoding.ID, parents ...encoding.ID) encoding.ID {
	b.seq++
	return b.add(&encoding.Operation{
		ViewID: view, Parents: parents,
		Metadata: encoding.OperationMetadata{
			Start: encoding.Timestamp{Millis: int64(b.seq)}, End: encoding.Timestamp{Millis: int64(b.seq)},
			Description: "op", Hostname: "test", Username: "t",
		},
		CommitPredecessors: map[encoding.ID][]encoding.ID{},
	})
}

const chunkSize = 64 << 10

func (b *objects) manifests() []*scmpb.FileManifest {
	var out []*scmpb.FileManifest
	for id, contents := range b.files {
		m := &scmpb.FileManifest{FileId: id[:], Size: uint64(len(contents)), PathHint: "f"}
		for off := 0; off < len(contents); off += chunkSize {
			c := contents[off:min(off+chunkSize, len(contents))]
			cid := pack.ChunkID(c)
			b.chunks[cid] = c
			m.Chunks = append(m.Chunks, &scmpb.ChunkRef{Id: cid[:], Length: uint32(len(c))})
		}
		out = append(out, m)
	}
	return out
}

func (b *objects) begin(repo int64) *scmpb.PublishRequest {
	refs := make([]*scmpb.ObjectRef, len(b.order))
	for i, o := range b.order {
		refs[i] = &scmpb.ObjectRef{Kind: o.Kind, Id: o.Id}
	}
	return &scmpb.PublishRequest{Step: &scmpb.PublishRequest_Begin_{Begin: &scmpb.PublishRequest_Begin{RepositoryId: repo, Objects: refs, Files: b.manifests(), Workspace: b.workspace}}}
}

// publish runs a whole Publish exchange, sending exactly what the server reports missing.
func (b *objects) publish(ctx context.Context, c clients, repo int64, heads *scmpb.OpHeadsUpdate) ([][]byte, *scmpb.PublishResponse_Missing, error) {
	stream, err := c.store.Publish(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := stream.Send(b.begin(repo)); err != nil {
		return nil, nil, err
	}
	resp, err := stream.Recv()
	if err != nil {
		return nil, nil, err
	}
	missing := resp.GetMissing()
	byID := map[string]*scmpb.EncodedObject{}
	for _, o := range b.order {
		byID[string(o.Id)] = o
	}
	for _, r := range missing.GetObjects() {
		if err := stream.Send(&scmpb.PublishRequest{Step: &scmpb.PublishRequest_Object{Object: byID[string(r.GetId())]}}); err != nil {
			return nil, missing, err
		}
	}
	for _, cid := range missing.GetChunks() {
		if err := stream.Send(&scmpb.PublishRequest{Step: &scmpb.PublishRequest_Chunk{Chunk: &scmpb.ChunkData{Id: cid, Data: b.chunks[encoding.ID(cid)]}}}); err != nil {
			return nil, missing, err
		}
	}
	if err := stream.Send(&scmpb.PublishRequest{Step: &scmpb.PublishRequest_Commit_{Commit: &scmpb.PublishRequest_Commit{OpHeads: heads}}}); err != nil {
		return nil, missing, err
	}
	done, err := stream.Recv()
	if err != nil {
		return nil, missing, err
	}
	return done.GetDone().GetOpHeads(), missing, nil
}

func TestAuthorization(t *testing.T) {
	c := testServer(t)
	anon := context.Background()
	alice := human("alice.human.example.com")

	_, err := c.repos.GetRepository(anon, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: 1}})
	wantCode(t, err, codes.Unauthenticated)
	_, err = drain(c.store.ReadObjects(anon, &scmpb.ReadObjectsRequest{RepositoryId: 1}))
	wantCode(t, err, codes.Unauthenticated)
	pub, err := c.store.Publish(anon)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pub.Recv()
	wantCode(t, err, codes.Unauthenticated)
	// A principal the directory does not know is no one.
	_, err = c.repos.CreateRepository(as("stranger.robot.example.com"), &scmpb.CreateRepositoryRequest{Name: "x"})
	wantCode(t, err, codes.Unauthenticated)
	// A robot that is none of ok's roles reads no code.
	_, err = c.repos.CreateRepository(as("ci.robot.example.com"), &scmpb.CreateRepositoryRequest{Name: "x"})
	wantCode(t, err, codes.PermissionDenied)

	// Anyone may create a repository, and becomes its creator.
	r := newRepo(t, c)
	if !bytes.Equal(r.GetHead(), encoding.RootCommitID[:]) || r.GetHeadSeq() != 0 || r.GetCreatedAt() == nil || r.GetCreatedBy() != "alice.human.example.com" {
		t.Fatalf("new repository: %v", r)
	}
	_, err = c.repos.CreateRepository(human("bob.human.example.com"), &scmpb.CreateRepositoryRequest{Name: r.GetName()})
	wantCode(t, err, codes.AlreadyExists)

	byName, err := c.repos.GetRepository(alice, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: r.GetName()}})
	if err != nil || byName.GetId() != r.GetId() {
		t.Fatalf("get by name: %v %v", byName, err)
	}
	byID, err := c.repos.GetRepository(alice, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: r.GetId()}})
	if err != nil || byID.GetName() != r.GetName() {
		t.Fatalf("get by id: %v %v", byID, err)
	}
	_, err = c.repos.GetRepository(alice, &scmpb.GetRepositoryRequest{})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.repos.GetRepository(alice, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: r.GetName() + "-absent"}})
	wantCode(t, err, codes.NotFound)

	found, token := false, ""
	for {
		page, err := c.repos.ListRepositories(alice, &scmpb.ListRepositoriesRequest{PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range page.GetRepositories() {
			found = found || x.GetId() == r.GetId()
		}
		if token = page.GetNextPageToken(); token == "" {
			break
		}
	}
	if !found {
		t.Fatal("list never returned the repository")
	}

	// Anyone may rename any repository.
	bob := human("bob.human.example.com")
	renamed, err := c.repos.RenameRepository(bob, &scmpb.RenameRepositoryRequest{RepositoryId: r.GetId(), Name: r.GetName() + "-renamed"})
	if err != nil || renamed.GetName() != r.GetName()+"-renamed" || renamed.GetCreatedBy() != "alice.human.example.com" {
		t.Fatalf("rename: %v %v", renamed, err)
	}
	_, err = c.repos.RenameRepository(anon, &scmpb.RenameRepositoryRequest{RepositoryId: r.GetId(), Name: "anon"})
	wantCode(t, err, codes.Unauthenticated)
}

func TestPublishAndRead(t *testing.T) {
	c := testServer(t)
	alice, bob := human("alice.human.example.com"), human("bob.human.example.com")
	repo := newRepo(t, c).GetId()
	b := newObjects(t)
	contents := bytes.Repeat([]byte("ok's scm\n"), 12_000) // two chunks
	file := b.file(contents)
	empty := b.file(nil)
	cp := b.add(&encoding.CopyHistory{CurrentPath: "a.txt"})
	tree := b.add(&encoding.Tree{Entries: []encoding.TreeEntry{
		{Name: "a.txt", Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: file, CopyID: &cp}},
		{Name: "empty", Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: empty}},
	}})
	commit := b.commit(tree, "first\n", encoding.RootCommitID)
	view := b.add(&encoding.View{HeadIDs: []encoding.ID{commit}, WCCommitIDs: map[string]encoding.ID{"default": commit}})
	op := b.operation(view, encoding.RootOperationID)
	for _, who := range []context.Context{alice, bob} {
		if _, err := c.ws.CreateWorkspace(who, &scmpb.CreateWorkspaceRequest{RepositoryId: repo, Workspace: "default"}); err != nil {
			t.Fatal(err)
		}
	}
	b.workspace = "default"

	heads, missing, err := b.publish(alice, c, repo, &scmpb.OpHeadsUpdate{OldIds: [][]byte{encoding.RootOperationID[:]}, NewId: op[:]})
	if err != nil {
		t.Fatal(err)
	}
	if len(missing.GetObjects()) != 5 || len(missing.GetChunks()) != 2 {
		t.Fatalf("first publish missing %d objects and %d chunks", len(missing.GetObjects()), len(missing.GetChunks()))
	}
	if len(heads) != 1 || !bytes.Equal(heads[0], op[:]) {
		t.Fatalf("op heads after publish: %x", heads)
	}

	// Each workspace has its own operation log, and a principal's are its own.
	got, err := c.store.GetOpHeads(alice, &scmpb.GetOpHeadsRequest{RepositoryId: repo, Workspace: "default"})
	if err != nil || len(got.GetOpIds()) != 1 || !bytes.Equal(got.GetOpIds()[0], op[:]) {
		t.Fatalf("alice's heads: %v %v", got, err)
	}
	got, err = c.store.GetOpHeads(bob, &scmpb.GetOpHeadsRequest{RepositoryId: repo, Workspace: "default"})
	if err != nil || len(got.GetOpIds()) != 1 || !bytes.Equal(got.GetOpIds()[0], encoding.RootOperationID[:]) {
		t.Fatalf("bob's heads: %v %v", got, err)
	}
	ops, err := drain(c.store.ReadOperations(alice, &scmpb.ReadOperationsRequest{RepositoryId: repo, Workspace: "default", Refs: []*scmpb.ObjectRef{
		{Kind: scmpb.ObjectKind_OBJECT_KIND_OPERATION, Id: op[:]}, {Kind: scmpb.ObjectKind_OBJECT_KIND_VIEW, Id: view[:]},
	}}))
	if err != nil || len(ops) != 2 || ops[0].GetObject() == nil || ops[1].GetObject() == nil {
		t.Fatalf("alice reads her operation and view: %v %v", ops, err)
	}
	ops, err = drain(c.store.ReadOperations(bob, &scmpb.ReadOperationsRequest{RepositoryId: repo, Workspace: "default", Refs: []*scmpb.ObjectRef{{Kind: scmpb.ObjectKind_OBJECT_KIND_OPERATION, Id: op[:]}}}))
	if err != nil || len(ops) != 1 || ops[0].GetNotFound() == nil {
		t.Fatalf("bob reads alice's operation: %v %v", ops, err)
	}
	res, err := c.store.ResolveOperationPrefix(alice, &scmpb.ResolveOperationPrefixRequest{RepositoryId: repo, Workspace: "default", Prefix: op[:3]})
	if err != nil || res.GetResolution() != scmpb.ResolveOperationPrefixResponse_RESOLUTION_SINGLE || !bytes.Equal(res.GetOpId(), op[:]) {
		t.Fatalf("alice resolves her operation: %v %v", res, err)
	}
	if op[0] != 0 {
		res, err = c.store.ResolveOperationPrefix(bob, &scmpb.ResolveOperationPrefixRequest{RepositoryId: repo, Workspace: "default", Prefix: op[:3]})
		if err != nil || res.GetResolution() != scmpb.ResolveOperationPrefixResponse_RESOLUTION_NO_MATCH {
			t.Fatalf("bob resolves alice's operation: %v %v", res, err)
		}
	}

	// The repository's objects are public.
	unknown := encoding.ID{0x42}
	objs, err := drain(c.store.ReadObjects(bob, &scmpb.ReadObjectsRequest{RepositoryId: repo, Refs: []*scmpb.ObjectRef{
		{Kind: scmpb.ObjectKind_OBJECT_KIND_TREE, Id: tree[:]},
		{Kind: scmpb.ObjectKind_OBJECT_KIND_COMMIT, Id: commit[:]},
		{Kind: scmpb.ObjectKind_OBJECT_KIND_TREE, Id: unknown[:]},
	}}))
	if err != nil || len(objs) != 3 {
		t.Fatalf("read objects: %v %v", objs, err)
	}
	for i, want := range []*scmpb.EncodedObject{b.order[1], b.order[2]} {
		if o := objs[i].GetObject(); o == nil || !bytes.Equal(o.GetBody(), want.GetBody()) || !bytes.Equal(o.GetId(), want.GetId()) {
			t.Fatalf("object %d: %v", i, objs[i])
		}
	}
	if objs[2].GetNotFound() == nil {
		t.Fatalf("unknown tree: %v", objs[2])
	}
	_, err = drain(c.store.ReadObjects(bob, &scmpb.ReadObjectsRequest{RepositoryId: repo, Refs: []*scmpb.ObjectRef{{Kind: scmpb.ObjectKind_OBJECT_KIND_OPERATION, Id: op[:]}}}))
	wantCode(t, err, codes.InvalidArgument)

	frames, err := drain(c.store.ReadFiles(bob, &scmpb.ReadFilesRequest{RepositoryId: repo, Files: []*scmpb.FileRange{{FileId: file[:]}, {FileId: unknown[:]}}}))
	if err != nil {
		t.Fatal(err)
	}
	var whole []byte
	for _, f := range frames {
		if bytes.Equal(f.GetFileId(), file[:]) {
			whole = append(whole, f.GetData()...)
		}
	}
	if !bytes.Equal(whole, contents) || !frames[len(frames)-2].GetLast() || frames[0].GetSize() != uint64(len(contents)) {
		t.Fatalf("file read back as %d bytes over %d frames", len(whole), len(frames))
	}
	if last := frames[len(frames)-1]; !last.GetNotFound() || !bytes.Equal(last.GetFileId(), unknown[:]) {
		t.Fatalf("unknown file: %v", last)
	}
	// A file with no chunks is one frame carrying no chunk.
	frames, err = drain(c.store.ReadFiles(bob, &scmpb.ReadFilesRequest{RepositoryId: repo, Files: []*scmpb.FileRange{{FileId: empty[:]}}}))
	if err != nil || len(frames) != 1 || !frames[0].GetLast() || frames[0].GetSize() != 0 || frames[0].GetChunkId() != nil || frames[0].GetData() != nil {
		t.Fatalf("empty file: %v %v", frames, err)
	}
	// Stored: the chunks as their packs store them — this file's compress — each frame naming its
	// codec and raw length; decompressed and verified, they are the file. A frame without a chunk
	// names no codec.
	frames, err = drain(c.store.ReadFiles(bob, &scmpb.ReadFilesRequest{RepositoryId: repo, Stored: true,
		Files: []*scmpb.FileRange{{FileId: file[:]}, {FileId: empty[:]}}}))
	if err != nil {
		t.Fatal(err)
	}
	whole = nil
	wire := 0
	for _, f := range frames {
		if len(f.GetData()) == 0 {
			if f.GetCodec() != scmpb.ChunkCodec_CHUNK_CODEC_UNSPECIFIED || f.GetRawLength() != 0 {
				t.Fatalf("a frame without a chunk names codec %v, raw length %d", f.GetCodec(), f.GetRawLength())
			}
			continue
		}
		if f.GetCodec() != scmpb.ChunkCodec_CHUNK_CODEC_ZSTD {
			t.Fatalf("a compressible chunk sent as %v", f.GetCodec())
		}
		var id encoding.ID
		copy(id[:], f.GetChunkId())
		raw, err := pack.Decode(nil, id, pack.Zstd, f.GetRawLength(), f.GetData())
		if err != nil {
			t.Fatal(err)
		}
		wire += len(f.GetData())
		whole = append(whole, raw...)
	}
	if !bytes.Equal(whole, contents) || wire*10 > len(contents) {
		t.Fatalf("stored: %d bytes on the wire for %d, reading back as %d", wire, len(contents), len(whole))
	}
	// Manifests only: every chunk's ID and offset, and no data.
	frames, err = drain(c.store.ReadFiles(bob, &scmpb.ReadFilesRequest{RepositoryId: repo, ManifestsOnly: true,
		Files: []*scmpb.FileRange{{FileId: file[:]}, {FileId: empty[:]}, {FileId: unknown[:]}}}))
	if err != nil || len(frames) < 3 {
		t.Fatalf("manifests: %v %v", frames, err)
	}
	var chunks int
	for _, f := range frames[:len(frames)-2] {
		if !bytes.Equal(f.GetFileId(), file[:]) || f.GetData() != nil || len(f.GetChunkId()) != encoding.IDLen {
			t.Fatalf("a manifest frame: %v", f)
		}
		chunks++
	}
	if frames[0].GetSize() != uint64(len(contents)) || !frames[len(frames)-3].GetLast() || chunks != (len(contents)+chunkSize-1)/chunkSize {
		t.Fatalf("the file's manifest: %d frames, size %d", chunks, frames[0].GetSize())
	}
	if e := frames[len(frames)-2]; !bytes.Equal(e.GetFileId(), empty[:]) || !e.GetLast() || e.GetChunkId() != nil {
		t.Fatalf("the empty file's manifest: %v", e)
	}
	if u := frames[len(frames)-1]; !u.GetNotFound() {
		t.Fatalf("the unknown file's manifest: %v", u)
	}

	commits, err := drain(c.store.PrefetchCommits(bob, &scmpb.PrefetchCommitsRequest{RepositoryId: repo, Want: [][]byte{commit[:]}}))
	if err != nil || len(commits) != 1 || !bytes.Equal(commits[0].GetId(), commit[:]) {
		t.Fatalf("prefetch: %v %v", commits, err)
	}
	commits, err = drain(c.store.PrefetchCommits(alice, &scmpb.PrefetchCommitsRequest{RepositoryId: repo, OpLog: true, Workspace: "default"}))
	if err != nil || len(commits) != 1 {
		t.Fatalf("prefetch of alice's operation log: %v %v", commits, err)
	}
	closure, err := drain(c.store.ReadTreeClosure(bob, &scmpb.ReadTreeClosureRequest{RepositoryId: repo, TreeId: tree[:]}))
	if err != nil || len(closure) != 2 {
		t.Fatalf("tree closure: %v %v", closure, err)
	}
	// With sizes: the tree, its sizes record, then the copy history.
	closure, err = drain(c.store.ReadTreeClosure(bob, &scmpb.ReadTreeClosureRequest{RepositoryId: repo, TreeId: tree[:], WithSizes: true}))
	if err != nil || len(closure) != 3 || closure[1].GetKind() != scmpb.ObjectKind_OBJECT_KIND_TREE_SIZES || !bytes.Equal(closure[1].GetId(), tree[:]) {
		t.Fatalf("tree closure with sizes: %v %v", closure, err)
	}
	var unknownTree encoding.ID
	unknownTree[0] = 1
	sizes, err := drain(c.store.ReadTreeSizes(bob, &scmpb.ReadTreeSizesRequest{RepositoryId: repo, TreeIds: [][]byte{tree[:], unknownTree[:], tree[:]}}))
	if err != nil || len(sizes) != 1 || !bytes.Equal(sizes[0].GetTreeId(), tree[:]) || !bytes.Equal(sizes[0].GetRecord(), closure[1].GetBody()) {
		t.Fatalf("tree sizes: %v %v (one answer, the tree's, as the closure sent it)", sizes, err)
	}
	if rec, err := encoding.DecodeTreeSizes(sizes[0].GetRecord()); err != nil || len(rec.Sizes) != 2 || rec.Sizes[0] != uint64(len(contents)) || rec.Sizes[1] != 0 {
		t.Fatalf("the tree's sizes: %v %v", rec, err)
	}
	if _, err := drain(c.store.ReadTreeSizes(bob, &scmpb.ReadTreeSizesRequest{RepositoryId: repo, TreeIds: [][]byte{{1, 2}}})); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a malformed tree ID: %v", err)
	}
	related, err := c.store.GetRelatedCopies(bob, &scmpb.GetRelatedCopiesRequest{RepositoryId: repo, CopyId: cp[:]})
	if err != nil || len(related.GetCopies()) != 1 || !bytes.Equal(related.GetCopies()[0].GetId(), cp[:]) {
		t.Fatalf("related copies: %v %v", related, err)
	}
	records, err := drain(c.store.GetCopyRecords(bob, &scmpb.GetCopyRecordsRequest{RepositoryId: repo, AllPaths: true, Root: encoding.RootCommitID[:], Head: commit[:]}))
	if err != nil || len(records) != 0 {
		t.Fatalf("copy records of an addition: %v %v", records, err)
	}

	// Republishing sends only what the caller's workspace lacks: nothing for alice's; for bob's,
	// whose operation log is its own, the operation and the view.
	again := newObjects(t)
	again.order, again.files, again.workspace = b.order, b.files, "default"
	_, missing, err = again.publish(alice, c, repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing.GetObjects()) != 0 || len(missing.GetChunks()) != 0 {
		t.Fatalf("alice's republish missing %v", missing)
	}
	_, missing, err = again.publish(bob, c, repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	gotMissing := map[string]scmpb.ObjectKind{}
	for _, r := range missing.GetObjects() {
		gotMissing[string(r.GetId())] = r.GetKind()
	}
	if len(missing.GetChunks()) != 0 || len(gotMissing) != 2 ||
		gotMissing[string(op[:])] != scmpb.ObjectKind_OBJECT_KIND_OPERATION || gotMissing[string(view[:])] != scmpb.ObjectKind_OBJECT_KIND_VIEW {
		t.Fatalf("bob's republish missing %v", missing)
	}

	if _, err := c.store.CollectOperations(alice, &scmpb.CollectOperationsRequest{RepositoryId: repo, Workspace: "default", HeadIds: [][]byte{op[:]}}); err != nil {
		t.Fatal(err)
	}
}

func TestPublishRefusals(t *testing.T) {
	c := testServer(t)
	alice := human("alice.human.example.com")
	repo := newRepo(t, c).GetId()

	// A commit whose parent is neither stored nor sent is refused, naming the parent.
	b := newObjects(t)
	absent := encoding.ID{0x77}
	orphan := b.commit(encoding.EmptyTreeID, "orphan\n", absent)
	_, _, err := b.publish(alice, c, repo, nil)
	wantCode(t, err, codes.FailedPrecondition)
	var rej *scmpb.PublishRejection
	for _, d := range status.Convert(err).Details() {
		if r, ok := d.(*scmpb.PublishRejection); ok {
			rej = r
		}
	}
	if rej == nil || len(rej.GetMissing()) != 1 || rej.GetMissing()[0].GetKind() != scmpb.ObjectKind_OBJECT_KIND_COMMIT || !bytes.Equal(rej.GetMissing()[0].GetId(), absent[:]) {
		t.Fatalf("rejection details: %v", status.Convert(err).Details())
	}
	objs, err := drain(c.store.ReadObjects(alice, &scmpb.ReadObjectsRequest{RepositoryId: repo, Refs: []*scmpb.ObjectRef{{Kind: scmpb.ObjectKind_OBJECT_KIND_COMMIT, Id: orphan[:]}}}))
	if err != nil || objs[0].GetNotFound() == nil {
		t.Fatalf("a refused publish left %v", objs)
	}

	// A publish that ends before commit makes nothing visible.
	b = newObjects(t)
	tree := b.add(&encoding.Tree{Entries: []encoding.TreeEntry{{Name: "x", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: b.add(&encoding.Symlink{Target: "y"})}}}})
	stream, err := c.store.Publish(alice)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(b.begin(repo)); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	wantCode(t, err, codes.InvalidArgument)
	objs, err = drain(c.store.ReadObjects(alice, &scmpb.ReadObjectsRequest{RepositoryId: repo, Refs: []*scmpb.ObjectRef{{Kind: scmpb.ObjectKind_OBJECT_KIND_TREE, Id: tree[:]}}}))
	if err != nil || objs[0].GetNotFound() == nil {
		t.Fatalf("an abandoned publish left %v", objs)
	}

	// Malformed exchanges.
	for name, first := range map[string]*scmpb.PublishRequest{
		"not begin": {Step: &scmpb.PublishRequest_Commit_{Commit: &scmpb.PublishRequest_Commit{}}},
		"short id":  {Step: &scmpb.PublishRequest_Begin_{Begin: &scmpb.PublishRequest_Begin{RepositoryId: repo, Objects: []*scmpb.ObjectRef{{Kind: scmpb.ObjectKind_OBJECT_KIND_TREE, Id: []byte{1, 2}}}}}},
		"bad kind":  {Step: &scmpb.PublishRequest_Begin_{Begin: &scmpb.PublishRequest_Begin{RepositoryId: repo, Objects: []*scmpb.ObjectRef{{Kind: 1 << 20, Id: tree[:]}}}}},
		"file kind": {Step: &scmpb.PublishRequest_Begin_{Begin: &scmpb.PublishRequest_Begin{RepositoryId: repo, Objects: []*scmpb.ObjectRef{{Kind: scmpb.ObjectKind_OBJECT_KIND_FILE, Id: tree[:]}}}}},
		"no repo":   {Step: &scmpb.PublishRequest_Begin_{Begin: &scmpb.PublishRequest_Begin{RepositoryId: 1 << 62}}},
	} {
		stream, err := c.store.Publish(alice)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(first); err != nil {
			t.Fatal(err)
		}
		_, err = stream.Recv()
		want := codes.InvalidArgument
		if name == "no repo" {
			want = codes.NotFound
		}
		if status.Code(err) != want {
			t.Fatalf("%s: got %v, want %s", name, err, want)
		}
	}
	_, err = c.store.GetOpHeads(alice, &scmpb.GetOpHeadsRequest{RepositoryId: 1 << 62})
	wantCode(t, err, codes.NotFound)
}

// TestCallers pins who may call each service: each declares its callers (ok.v1.callers), which
// the serving runtime enforces before the service sees a request.
func TestCallers(t *testing.T) {
	people, control, automation := okpb.Caller_CALLER_PEOPLE, okpb.Caller_CALLER_CONTROL, okpb.Caller_CALLER_AUTOMATION
	workers, deployer := okpb.Caller_CALLER_WORKER, okpb.Caller_CALLER_DEPLOY
	code := []okpb.Caller{people, control, automation, workers, deployer}
	for svc, want := range map[string][]okpb.Caller{
		"ok.scm.v1.Repositories":  code,
		"ok.scm.v1.Store":         code,
		"ok.scm.v1.ChangeLists":   code,
		"ok.scm.v1.Browse":        code,
		"ok.scm.v1.Workspaces":    code,
		"ok.scm.v1.Review":        {control},
		"ok.scm.v1.Landing":       {automation},
		"ok.scm.v1.LandingChecks": {control, automation},
		"ok.scm.v1.Events":        {control, automation, deployer},
	} {
		got := authn.Callers(svc)
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s: callers %v, want %v", svc, got, want)
		}
	}
}

// A backend that does not answer is Unavailable, which clients retry; a request's own failure
// keeps its code, and anything else stays Internal.
func TestUnreachableIsUnavailable(t *testing.T) {
	connect, err := pgconn.Connect(context.Background(), "postgres://nobody@127.0.0.1:1/x?connect_timeout=1")
	if err == nil {
		connect.Close(context.Background())
		t.Fatal("a connection to a closed port succeeded")
	}
	for _, c := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{"a connection that could not be made", fmt.Errorf("reading: %w", err), codes.Unavailable},
		{"a shutdown", &pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"}, codes.Unavailable},
		{"a connection exception", &pgconn.PgError{Code: "08006", Message: "connection failure"}, codes.Unavailable},
		{"a reset", fmt.Errorf("read: %w", syscall.ECONNRESET), codes.Unavailable},
		{"a dial", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, codes.Unavailable},
		{"a cut-off read", fmt.Errorf("pack: %w", io.ErrUnexpectedEOF), codes.Unavailable},
		{"a constraint", &pgconn.PgError{Code: "23505", Message: "duplicate key"}, codes.Internal},
		{"a missing object", fmt.Errorf("tree: %w", store.ErrNotFound), codes.NotFound},
		{"anything else", errors.New("a bug"), codes.Internal},
	} {
		if got := status.Code(toStatus(c.err)); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}
