// Package service serves the SCM's gRPC services (proto/ok/scm/v1) — Repositories, Store,
// ChangeLists, Review, Landing, LandingChecks, Browse, Events, and Workspaces — over the storage
// core, under the authorization table in docs/scm.md: every request needs a caller, the
// operation-log RPCs act on the caller's own log, and each service admits only the callers it
// declares (only ok control transitions CLs, only ok automation lands them). The caller is the one
// ok's serving runtime resolved for the request.
package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/scm/store"
	"github.com/Cidan/ok/internal/serve/authn"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// maxRefs bounds the refs, IDs, and paths one request may name.
const maxRefs = 100_000

// core is the services' shared state. Who may call each service is its proto's ok.v1.callers,
// which the serving runtime enforces before a request reaches it: nothing here checks a caller
// against a list.
type core struct {
	store  *store.Store
	people store.People
	now    func() time.Time // the clock leases are measured by
}

func newCore(s *store.Store, dir Directory) *core {
	return &core{store: s, people: people{dir: dir, names: s.Names()}, now: time.Now}
}

// caller returns the principal a request is for.
func caller(ctx context.Context) (string, error) {
	c, ok := authn.FromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "the SCM serves identified principals only")
	}
	return c.Principal, nil
}

// toStatus maps a store error onto its gRPC status.
func toStatus(err error) error {
	var rej *store.Rejection
	var upload *store.UploadRejection
	var land *store.LandRejection
	var rollback *store.RollbackRejection
	switch {
	case err == nil:
		return nil
	case errors.As(err, &rej):
		return withDetails(codes.FailedPrecondition, rej.Error(), &scmpb.PublishRejection{Missing: refsOut(rej.Missing), Message: rej.Message})
	case errors.As(err, &upload):
		d := &scmpb.UploadRejection{Reason: scmpb.UploadRejection_Reason(upload.Reason), Message: upload.Message, Number: upload.Number, Problems: upload.Problems}
		if upload.Commit != (encoding.ID{}) {
			d.CommitId = upload.Commit[:]
		}
		return withDetails(codes.FailedPrecondition, upload.Error(), d)
	case errors.As(err, &rollback):
		return withDetails(codes.FailedPrecondition, rollback.Error(), &scmpb.RollbackRejection{
			Number: rollback.Number, Reason: scmpb.RollbackRejection_Reason(rollback.Reason), Message: rollback.Message, Paths: rollback.Paths,
		})
	case errors.As(err, &land):
		return withDetails(codes.FailedPrecondition, land.Error(), &scmpb.LandRejection{
			Number: land.Number, Reason: scmpb.LandRejection_Reason(land.Reason), Paths: land.Paths, Message: land.Message,
		})
	case errors.Is(err, store.ErrPrecondition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, store.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case unreachable(err):
		slog.Warn("ok scm: a backend did not answer", slog.Any("err", err))
		return status.Error(codes.Unavailable, "ok scm could not reach its database or storage; try again")
	}
	if st, ok := status.FromError(err); ok {
		return st.Err()
	}
	slog.Error("ok scm: internal error", slog.Any("err", err))
	return status.Error(codes.Internal, "internal error")
}

// withDetails is a status carrying a rejection in its details.
func withDetails(code codes.Code, msg string, detail protoadapt.MessageV1) error {
	st := status.New(code, msg)
	if d, err := st.WithDetails(detail); err == nil {
		st = d
	}
	return st.Err()
}

func idIn(b []byte, field string) (encoding.ID, error) {
	if len(b) != encoding.IDLen {
		return encoding.ID{}, status.Errorf(codes.InvalidArgument, "%s: an ID is %d bytes, not %d", field, encoding.IDLen, len(b))
	}
	return encoding.ID(b), nil
}

func idsIn(bs [][]byte, field string) ([]encoding.ID, error) {
	if len(bs) > maxRefs {
		return nil, status.Errorf(codes.InvalidArgument, "%s: %d IDs (at most %d)", field, len(bs), maxRefs)
	}
	out := make([]encoding.ID, len(bs))
	for i, b := range bs {
		id, err := idIn(b, field)
		if err != nil {
			return nil, err
		}
		out[i] = id
	}
	return out, nil
}

func idsOut(ids []encoding.ID) [][]byte {
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = id[:]
	}
	return out
}

func kindIn(k scmpb.ObjectKind) (encoding.Kind, error) {
	if k <= 0 || k > math.MaxUint8 {
		return 0, status.Errorf(codes.InvalidArgument, "object kind %d", k)
	}
	return encoding.Kind(k), nil
}

func refsIn(rs []*scmpb.ObjectRef, field string) ([]store.ObjectRef, error) {
	if len(rs) > maxRefs {
		return nil, status.Errorf(codes.InvalidArgument, "%s: %d refs (at most %d)", field, len(rs), maxRefs)
	}
	out := make([]store.ObjectRef, len(rs))
	for i, r := range rs {
		k, err := kindIn(r.GetKind())
		if err != nil {
			return nil, err
		}
		id, err := idIn(r.GetId(), field)
		if err != nil {
			return nil, err
		}
		out[i] = store.ObjectRef{Kind: k, ID: id}
	}
	return out, nil
}

func refOut(r store.ObjectRef) *scmpb.ObjectRef {
	return &scmpb.ObjectRef{Kind: scmpb.ObjectKind(r.Kind), Id: r.ID[:]}
}

func refsOut(rs []store.ObjectRef) []*scmpb.ObjectRef {
	out := make([]*scmpb.ObjectRef, len(rs))
	for i, r := range rs {
		out[i] = refOut(r)
	}
	return out
}

func objectOut(o store.EncodedObject) *scmpb.EncodedObject {
	return &scmpb.EncodedObject{Kind: scmpb.ObjectKind(o.Kind), Id: o.ID[:], Body: o.Body}
}

func resultOut(r store.ObjectResult) *scmpb.ObjectResult {
	if !r.Found {
		return &scmpb.ObjectResult{Result: &scmpb.ObjectResult_NotFound{NotFound: refOut(r.Ref)}}
	}
	return &scmpb.ObjectResult{Result: &scmpb.ObjectResult_Object{Object: objectOut(store.EncodedObject{Kind: r.Ref.Kind, ID: r.Ref.ID, Body: r.Body})}}
}

// Repositories serves the repository registry.
type Repositories struct {
	scmpb.UnimplementedRepositoriesServer
	c *core
}

func repositoryOut(r store.Repository) *scmpb.Repository {
	return &scmpb.Repository{Id: r.ID, Name: r.Name, Head: r.Head[:], HeadSeq: r.HeadSeq, CreatedAt: timestamppb.New(r.CreatedAt), CreatedBy: r.CreatedBy}
}

func (s *Repositories) CreateRepository(ctx context.Context, req *scmpb.CreateRepositoryRequest) (*scmpb.Repository, error) {
	principal, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetActor() != "" {
		return nil, status.Error(codes.InvalidArgument, "a repository is created for another person through Review.CreateRepository")
	}
	r, err := s.c.store.CreateRepository(ctx, req.GetName(), principal)
	if err != nil {
		return nil, toStatus(err)
	}
	return repositoryOut(r), nil
}

func (s *Repositories) RenameRepository(ctx context.Context, req *scmpb.RenameRepositoryRequest) (*scmpb.Repository, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	r, err := s.c.store.RenameRepository(ctx, req.GetRepositoryId(), req.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	return repositoryOut(r), nil
}

func (s *Repositories) GetRepository(ctx context.Context, req *scmpb.GetRepositoryRequest) (*scmpb.Repository, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	var r store.Repository
	var err error
	switch k := req.GetKey().(type) {
	case *scmpb.GetRepositoryRequest_Id:
		r, err = s.c.store.GetRepository(ctx, k.Id)
	case *scmpb.GetRepositoryRequest_Name:
		r, err = s.c.store.GetRepositoryByName(ctx, k.Name)
	default:
		return nil, status.Error(codes.InvalidArgument, "name a repository by id or name")
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return repositoryOut(r), nil
}

func (s *Repositories) ListRepositories(ctx context.Context, req *scmpb.ListRepositoriesRequest) (*scmpb.ListRepositoriesResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if req.GetPageSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "page_size is negative")
	}
	rs, next, err := s.c.store.ListRepositories(ctx, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.ListRepositoriesResponse{NextPageToken: next}
	for _, r := range rs {
		out.Repositories = append(out.Repositories, repositoryOut(r))
	}
	return out, nil
}

// Store serves jj's storage surface: Backend reads, the caller's operation log, and
// Publish.
type Store struct {
	scmpb.UnimplementedStoreServer
	c *core
}

func (s *Store) ReadObjects(req *scmpb.ReadObjectsRequest, stream grpc.ServerStreamingServer[scmpb.ObjectResult]) error {
	ctx := stream.Context()
	if _, err := caller(ctx); err != nil {
		return err
	}
	refs, err := refsIn(req.GetRefs(), "refs")
	if err != nil {
		return err
	}
	res, err := s.c.store.ReadObjects(ctx, req.GetRepositoryId(), refs)
	if err != nil {
		return toStatus(err)
	}
	for _, r := range res {
		if err := stream.Send(resultOut(r)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ReadFiles(req *scmpb.ReadFilesRequest, stream grpc.ServerStreamingServer[scmpb.FileData]) error {
	ctx := stream.Context()
	if _, err := caller(ctx); err != nil {
		return err
	}
	if len(req.GetFiles()) > maxRefs {
		return status.Errorf(codes.InvalidArgument, "files: %d ranges (at most %d)", len(req.GetFiles()), maxRefs)
	}
	ranges := make([]store.FileRange, len(req.GetFiles()))
	for i, f := range req.GetFiles() {
		id, err := idIn(f.GetFileId(), "files.file_id")
		if err != nil {
			return err
		}
		ranges[i] = store.FileRange{FileID: id, Offset: f.GetOffset(), Length: f.GetLength()}
	}
	if req.GetManifestsOnly() {
		files := make([]encoding.ID, len(ranges))
		for i, r := range ranges {
			files[i] = r.FileID
		}
		return toStatus(s.c.store.ReadManifests(ctx, req.GetRepositoryId(), files, func(f store.FileFrame) error {
			out := &scmpb.FileData{FileId: f.FileID[:], Size: f.Size, Offset: f.Offset, Last: f.Last, NotFound: f.NotFound}
			if !f.NotFound && f.ChunkID != (encoding.ID{}) {
				out.ChunkId = f.ChunkID[:]
			}
			return stream.Send(out)
		}))
	}
	stored := req.GetStored()
	form := store.RawChunks
	switch {
	case stored && req.GetDictionaries():
		form = store.StoredChunksWithDictionaries
	case stored:
		form = store.StoredChunks
	}
	return toStatus(s.c.store.ReadFiles(ctx, req.GetRepositoryId(), ranges, form, func(f store.FileFrame) error {
		out := &scmpb.FileData{
			FileId: f.FileID[:], Size: f.Size, ChunkId: chunkIDOut(f), Offset: f.Offset,
			Data: f.Data, Last: f.Last, NotFound: f.NotFound,
		}
		if stored && len(f.Data) > 0 {
			out.Codec, out.RawLength = codecOut(f.Codec), f.RawLength
		}
		return stream.Send(out)
	}))
}

// codecOut is a stored chunk's codec on the wire.
func codecOut(c pack.Codec) scmpb.ChunkCodec {
	switch c {
	case pack.Zstd:
		return scmpb.ChunkCodec_CHUNK_CODEC_ZSTD
	case pack.ZstdDict:
		return scmpb.ChunkCodec_CHUNK_CODEC_ZSTD_DICT
	}
	return scmpb.ChunkCodec_CHUNK_CODEC_RAW
}

// ReadDictionary returns a dictionary of the repository's, as its chunks' frames name it.
func (s *Store) ReadDictionary(ctx context.Context, req *scmpb.ReadDictionaryRequest) (*scmpb.Dictionary, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	body, err := s.c.store.Dictionary(ctx, req.GetRepositoryId(), req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.Dictionary{Id: req.GetId(), Body: body}, nil
}

// chunkIDOut is the frame's chunk ID, or none when the frame carries no chunk: an absent
// file's frame, or the one frame of a file with no chunks (the empty file).
func chunkIDOut(f store.FileFrame) []byte {
	if f.NotFound || len(f.Data) == 0 {
		return nil
	}
	return f.ChunkID[:]
}

func (s *Store) GetRelatedCopies(ctx context.Context, req *scmpb.GetRelatedCopiesRequest) (*scmpb.GetRelatedCopiesResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	id, err := idIn(req.GetCopyId(), "copy_id")
	if err != nil {
		return nil, err
	}
	copies, err := s.c.store.GetRelatedCopies(ctx, req.GetRepositoryId(), id)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.GetRelatedCopiesResponse{Copies: make([]*scmpb.EncodedObject, len(copies))}
	for i, c := range copies {
		out.Copies[i] = objectOut(c)
	}
	return out, nil
}

func (s *Store) GetCopyRecords(req *scmpb.GetCopyRecordsRequest, stream grpc.ServerStreamingServer[scmpb.CopyRecord]) error {
	ctx := stream.Context()
	if _, err := caller(ctx); err != nil {
		return err
	}
	root, err := idIn(req.GetRoot(), "root")
	if err != nil {
		return err
	}
	head, err := idIn(req.GetHead(), "head")
	if err != nil {
		return err
	}
	var paths []string
	if !req.GetAllPaths() {
		if len(req.GetPaths()) > maxRefs {
			return status.Errorf(codes.InvalidArgument, "paths: %d (at most %d)", len(req.GetPaths()), maxRefs)
		}
		paths = append(make([]string, 0, len(req.GetPaths())), req.GetPaths()...)
	}
	records, err := s.c.store.GetCopyRecords(ctx, req.GetRepositoryId(), paths, root, head)
	if err != nil {
		return toStatus(err)
	}
	for _, r := range records {
		if err := stream.Send(&scmpb.CopyRecord{
			Target: r.Target, TargetCommit: r.TargetCommit[:], Source: r.Source,
			SourceFile: r.SourceFile[:], SourceCommit: r.SourceCommit[:],
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) PrefetchCommits(req *scmpb.PrefetchCommitsRequest, stream grpc.ServerStreamingServer[scmpb.EncodedObject]) error {
	ctx := stream.Context()
	principal, err := caller(ctx)
	if err != nil {
		return err
	}
	want, err := idsIn(req.GetWant(), "want")
	if err != nil {
		return err
	}
	have, err := idsIn(req.GetHave(), "have")
	if err != nil {
		return err
	}
	if req.GetLimit() < 0 {
		return status.Error(codes.InvalidArgument, "limit is negative")
	}
	return toStatus(s.c.store.PrefetchCommits(ctx, req.GetRepositoryId(), principal, req.GetWorkspace(), want, have, req.GetOpLog(), int(req.GetLimit()),
		func(o store.EncodedObject) error { return stream.Send(objectOut(o)) }))
}

func (s *Store) ReadTreeClosure(req *scmpb.ReadTreeClosureRequest, stream grpc.ServerStreamingServer[scmpb.EncodedObject]) error {
	ctx := stream.Context()
	if _, err := caller(ctx); err != nil {
		return err
	}
	id, err := idIn(req.GetTreeId(), "tree_id")
	if err != nil {
		return err
	}
	if len(req.GetPathPrefixes()) > maxRefs {
		return status.Errorf(codes.InvalidArgument, "path_prefixes: %d (at most %d)", len(req.GetPathPrefixes()), maxRefs)
	}
	return toStatus(s.c.store.ReadTreeClosure(ctx, req.GetRepositoryId(), id, req.GetPathPrefixes(), req.GetWithSizes(),
		func(o store.EncodedObject) error { return stream.Send(objectOut(o)) }))
}

func (s *Store) ReadTreeSizes(req *scmpb.ReadTreeSizesRequest, stream grpc.ServerStreamingServer[scmpb.TreeSizes]) error {
	ctx := stream.Context()
	if _, err := caller(ctx); err != nil {
		return err
	}
	if len(req.GetTreeIds()) > maxRefs {
		return status.Errorf(codes.InvalidArgument, "tree_ids: %d (at most %d)", len(req.GetTreeIds()), maxRefs)
	}
	trees := make([]encoding.ID, len(req.GetTreeIds()))
	for i, b := range req.GetTreeIds() {
		id, err := idIn(b, "tree_ids")
		if err != nil {
			return err
		}
		trees[i] = id
	}
	sizes, err := s.c.store.TreeSizes(ctx, req.GetRepositoryId(), trees)
	if err != nil {
		return toStatus(err)
	}
	sent := make(map[encoding.ID]bool, len(sizes))
	for _, id := range trees {
		b, ok := sizes[id]
		if !ok || sent[id] {
			continue
		}
		sent[id] = true
		if err := stream.Send(&scmpb.TreeSizes{TreeId: id[:], Record: b}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetOpHeads(ctx context.Context, req *scmpb.GetOpHeadsRequest) (*scmpb.GetOpHeadsResponse, error) {
	principal, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	heads, err := s.c.store.GetOpHeads(ctx, req.GetRepositoryId(), principal, req.GetWorkspace())
	if err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.GetOpHeadsResponse{OpIds: idsOut(heads)}, nil
}

func (s *Store) ReadOperations(req *scmpb.ReadOperationsRequest, stream grpc.ServerStreamingServer[scmpb.ObjectResult]) error {
	ctx := stream.Context()
	principal, err := caller(ctx)
	if err != nil {
		return err
	}
	refs, err := refsIn(req.GetRefs(), "refs")
	if err != nil {
		return err
	}
	res, err := s.c.store.ReadOperations(ctx, req.GetRepositoryId(), principal, req.GetWorkspace(), refs)
	if err != nil {
		return toStatus(err)
	}
	for _, r := range res {
		if err := stream.Send(resultOut(r)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) PrefetchOperations(req *scmpb.PrefetchOperationsRequest, stream grpc.ServerStreamingServer[scmpb.EncodedObject]) error {
	ctx := stream.Context()
	principal, err := caller(ctx)
	if err != nil {
		return err
	}
	want, err := idsIn(req.GetWant(), "want")
	if err != nil {
		return err
	}
	have, err := idsIn(req.GetHave(), "have")
	if err != nil {
		return err
	}
	if req.GetLimit() < 0 {
		return status.Error(codes.InvalidArgument, "limit is negative")
	}
	return toStatus(s.c.store.PrefetchOperations(ctx, req.GetRepositoryId(), principal, req.GetWorkspace(), want, have, int(req.GetLimit()),
		func(o store.EncodedObject) error { return stream.Send(objectOut(o)) }))
}

func (s *Store) ResolveOperationPrefix(ctx context.Context, req *scmpb.ResolveOperationPrefixRequest) (*scmpb.ResolveOperationPrefixResponse, error) {
	principal, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	res, id, err := s.c.store.ResolveOperationPrefix(ctx, req.GetRepositoryId(), principal, req.GetWorkspace(), req.GetPrefix(), req.GetOddNibble())
	if err != nil {
		return nil, toStatus(err)
	}
	switch res {
	case store.SingleMatch:
		return &scmpb.ResolveOperationPrefixResponse{Resolution: scmpb.ResolveOperationPrefixResponse_RESOLUTION_SINGLE, OpId: id[:]}, nil
	case store.AmbiguousMatch:
		return &scmpb.ResolveOperationPrefixResponse{Resolution: scmpb.ResolveOperationPrefixResponse_RESOLUTION_AMBIGUOUS}, nil
	default:
		return &scmpb.ResolveOperationPrefixResponse{Resolution: scmpb.ResolveOperationPrefixResponse_RESOLUTION_NO_MATCH}, nil
	}
}

func (s *Store) CollectOperations(ctx context.Context, req *scmpb.CollectOperationsRequest) (*scmpb.CollectOperationsResponse, error) {
	principal, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	heads, err := idsIn(req.GetHeadIds(), "head_ids")
	if err != nil {
		return nil, err
	}
	var keepNewer time.Time
	if req.GetKeepNewer() != nil {
		if err := req.GetKeepNewer().CheckValid(); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "keep_newer: %v", err)
		}
		keepNewer = req.GetKeepNewer().AsTime()
	}
	if err := s.c.store.CollectOperations(ctx, req.GetRepositoryId(), principal, req.GetWorkspace(), heads, keepNewer); err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.CollectOperationsResponse{}, nil
}

// Publish runs one publish exchange: begin, the server's missing list, the missing
// objects and chunks, and commit, answered by the resulting operation heads.
func (s *Store) Publish(stream grpc.BidiStreamingServer[scmpb.PublishRequest, scmpb.PublishResponse]) error {
	ctx := stream.Context()
	principal, err := caller(ctx)
	if err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	begin := first.GetBegin()
	if begin == nil {
		return status.Error(codes.InvalidArgument, "a publish starts with begin")
	}
	objects, err := refsIn(begin.GetObjects(), "begin.objects")
	if err != nil {
		return err
	}
	if len(begin.GetFiles()) > maxRefs {
		return status.Errorf(codes.InvalidArgument, "begin.files: %d manifests (at most %d)", len(begin.GetFiles()), maxRefs)
	}
	files := make([]store.FileManifest, len(begin.GetFiles()))
	for i, f := range begin.GetFiles() {
		id, err := idIn(f.GetFileId(), "begin.files.file_id")
		if err != nil {
			return err
		}
		m := store.FileManifest{FileID: id, Size: f.GetSize(), PathHint: f.GetPathHint(), Chunks: make([]store.ChunkRef, len(f.GetChunks()))}
		for j, c := range f.GetChunks() {
			cid, err := idIn(c.GetId(), "begin.files.chunks.id")
			if err != nil {
				return err
			}
			m.Chunks[j] = store.ChunkRef{ID: cid, Length: c.GetLength()}
		}
		files[i] = m
	}
	p, err := s.c.store.BeginPublish(ctx, begin.GetRepositoryId(), principal, begin.GetWorkspace(), objects, files)
	if err != nil {
		return toStatus(err)
	}
	defer p.Close()
	missingObjects, missingChunks := p.Missing()
	if err := stream.Send(&scmpb.PublishResponse{Step: &scmpb.PublishResponse_Missing_{Missing: &scmpb.PublishResponse_Missing{
		Objects: refsOut(missingObjects), Chunks: idsOut(missingChunks),
	}}}); err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return status.Error(codes.InvalidArgument, "the publish ended before commit")
		}
		if err != nil {
			return err
		}
		switch step := req.GetStep().(type) {
		case *scmpb.PublishRequest_Object:
			k, err := kindIn(step.Object.GetKind())
			if err != nil {
				return err
			}
			id, err := idIn(step.Object.GetId(), "object.id")
			if err != nil {
				return err
			}
			if err := p.AddObject(store.EncodedObject{Kind: k, ID: id, Body: step.Object.GetBody()}); err != nil {
				return toStatus(err)
			}
		case *scmpb.PublishRequest_Chunk:
			id, err := idIn(step.Chunk.GetId(), "chunk.id")
			if err != nil {
				return err
			}
			if err := p.AddChunk(id, step.Chunk.GetData()); err != nil {
				return toStatus(err)
			}
		case *scmpb.PublishRequest_Commit_:
			var heads *store.OpHeadsUpdate
			if u := step.Commit.GetOpHeads(); u != nil {
				old, err := idsIn(u.GetOldIds(), "commit.op_heads.old_ids")
				if err != nil {
					return err
				}
				nu, err := idIn(u.GetNewId(), "commit.op_heads.new_id")
				if err != nil {
					return err
				}
				heads = &store.OpHeadsUpdate{Old: old, New: nu}
			}
			result, err := p.Commit(ctx, heads)
			if err != nil {
				return toStatus(err)
			}
			return stream.Send(&scmpb.PublishResponse{Step: &scmpb.PublishResponse_Done_{Done: &scmpb.PublishResponse_Done{OpHeads: idsOut(result)}}})
		default:
			return status.Errorf(codes.InvalidArgument, "unexpected %T after begin", step)
		}
	}
}

// Services are ok's SCM's gRPC services over one store.
type Services struct {
	Repositories  *Repositories
	Store         *Store
	ChangeLists   *ChangeLists
	Review        *Review
	Landing       *Landing
	LandingChecks *LandingChecks
	Browse        *Browse
	Events        *Events
	Workspaces    *Workspaces
}

// New returns the services over s. dir is ok identity's directory, which upload asks about OWNERS
// users and ACLs, and reviewers.
func New(s *store.Store, dir Directory) Services {
	c := newCore(s, dir)
	return Services{Repositories: &Repositories{c: c}, Store: &Store{c: c}, ChangeLists: &ChangeLists{c: c}, Review: &Review{c: c},
		Landing: &Landing{c: c}, LandingChecks: &LandingChecks{c: c}, Browse: &Browse{c: c}, Events: &Events{c: c}, Workspaces: &Workspaces{c: c}}
}

// unreachable reports an error that is a backend not answering — the database, or object storage
// — rather than anything about the request: a connection that could not be made, or
// broke; a failure pgx knows came before anything was sent; and PostgreSQL's connection-exception
// class and the codes a server shutting down or starting up sends. It is temporary, so the caller
// is told Unavailable, which clients retry.
func unreachable(err error) bool {
	var connect *pgconn.ConnectError
	if errors.As(err, &connect) || pgconn.SafeToRetry(err) {
		return true
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "57P01", "57P02", "57P03": // admin_shutdown, crash_shutdown, cannot_connect_now
			return true
		}
		return strings.HasPrefix(pg.Code, "08") // connection_exception
	}
	var op *net.OpError
	return errors.As(err, &op) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}
