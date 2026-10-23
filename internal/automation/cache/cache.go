// Package cache is ok automation's Bazel remote cache (docs/hosted.md, "Hosted automation, as
// built"): the Remote Execution API's Capabilities, ContentAddressableStorage, and ActionCache, and
// ByteStream, over a bucket and a memory tier, each call within the namespaces its run is given.
package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	smpb "github.com/bazelbuild/remote-apis/build/bazel/semver"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/blob"
)

// Namespaces are where a call writes, and where it reads, in order.
type Namespaces struct {
	Write string
	Read  []string
}

// Resolve gives a call its namespaces, or refuses it.
type Resolve func(ctx context.Context) (Namespaces, error)

// Prefix is where the cache keeps its objects in its bucket.
const Prefix = "cache/"

// maxBatch bounds a batch's bytes, which Capabilities tells clients.
const maxBatch = 4 << 20

// chunk is how much a ByteStream read sends at once.
const chunk = 64 << 10

// Server is the remote cache.
type Server struct {
	repb.UnimplementedContentAddressableStorageServer
	repb.UnimplementedActionCacheServer
	repb.UnimplementedCapabilitiesServer

	blobs   blob.Store
	resolve Resolve
	mem     *memory
}

// New is a cache over blobs, its calls' namespaces resolve gives, with a memory tier of memBytes.
func New(blobs blob.Store, resolve Resolve, memBytes int64) *Server {
	return &Server{blobs: blobs, resolve: resolve, mem: newMemory(memBytes)}
}

// Register registers the cache's services.
func (s *Server) Register(reg grpc.ServiceRegistrar) {
	repb.RegisterContentAddressableStorageServer(reg, s)
	repb.RegisterActionCacheServer(reg, s)
	repb.RegisterCapabilitiesServer(reg, s)
	reg.RegisterService(&byteStreamDesc, s)
}

// Services are the cache's services' full names.
var Services = []string{
	"build.bazel.remote.execution.v2.ContentAddressableStorage",
	"build.bazel.remote.execution.v2.ActionCache",
	"build.bazel.remote.execution.v2.Capabilities",
	"google.bytestream.ByteStream",
}

func casKey(ns, hash string) string { return Prefix + ns + "/cas/" + hash }
func acKey(ns, hash string) string  { return Prefix + ns + "/ac/" + hash }

// validDigest refuses a digest that is not a SHA-256's.
func validDigest(d *repb.Digest) error {
	if d == nil || len(d.GetHash()) != 64 || d.GetSizeBytes() < 0 {
		return status.Error(codes.InvalidArgument, "a digest is a SHA-256, in hex, and a size")
	}
	if _, err := hex.DecodeString(d.GetHash()); err != nil || strings.ToLower(d.GetHash()) != d.GetHash() {
		return status.Error(codes.InvalidArgument, "a digest's hash is lowercase hex")
	}
	return nil
}

// emptyHash is SHA-256 of nothing, which every store holds.
var emptyHash = hex.EncodeToString(sha256.New().Sum(nil))

// find is the namespace of ns that holds a blob, and whether one does.
func (s *Server) find(ctx context.Context, read []string, d *repb.Digest) (string, bool, error) {
	if d.GetHash() == emptyHash && d.GetSizeBytes() == 0 {
		return "", true, nil
	}
	for _, ns := range read {
		if s.mem.has(ns, d.GetHash()) {
			return ns, true, nil
		}
		o, err := s.blobs.Stat(ctx, casKey(ns, d.GetHash()))
		switch {
		case errors.Is(err, blob.ErrNotFound):
			continue
		case err != nil:
			return "", false, err
		case o.Size == d.GetSizeBytes():
			return ns, true, nil
		}
	}
	return "", false, nil
}

// read reads a whole blob from where read finds it.
func (s *Server) read(ctx context.Context, read []string, d *repb.Digest) ([]byte, error) {
	if d.GetSizeBytes() == 0 {
		return nil, nil
	}
	ns, ok, err := s.find(ctx, read, d)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no blob %s/%d", d.GetHash(), d.GetSizeBytes())
	}
	if b, ok := s.mem.get(ns, d.GetHash()); ok {
		return b, nil
	}
	var buf bytes.Buffer
	if _, err := s.blobs.Download(ctx, casKey(ns, d.GetHash()), &buf); err != nil {
		return nil, err
	}
	s.mem.put(ns, d.GetHash(), buf.Bytes())
	return buf.Bytes(), nil
}

// write stores a blob whose bytes match its digest.
func (s *Server) write(ctx context.Context, ns string, d *repb.Digest, data []byte) error {
	if int64(len(data)) != d.GetSizeBytes() {
		return status.Errorf(codes.InvalidArgument, "a blob of %d bytes named %d", len(data), d.GetSizeBytes())
	}
	if sum := sha256.Sum256(data); hex.EncodeToString(sum[:]) != d.GetHash() {
		return status.Errorf(codes.InvalidArgument, "a blob whose bytes are not %s", d.GetHash())
	}
	if err := s.blobs.Put(ctx, casKey(ns, d.GetHash()), bytes.NewReader(data), int64(len(data))); err != nil {
		return err
	}
	s.mem.put(ns, d.GetHash(), data)
	return nil
}

// ─── Capabilities ───

func (s *Server) GetCapabilities(ctx context.Context, _ *repb.GetCapabilitiesRequest) (*repb.ServerCapabilities, error) {
	if _, err := s.resolve(ctx); err != nil {
		return nil, err
	}
	return &repb.ServerCapabilities{
		CacheCapabilities: &repb.CacheCapabilities{
			DigestFunctions:                 []repb.DigestFunction_Value{repb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities:   &repb.ActionCacheUpdateCapabilities{UpdateEnabled: true},
			MaxBatchTotalSizeBytes:          maxBatch,
			SymlinkAbsolutePathStrategy:     repb.SymlinkAbsolutePathStrategy_DISALLOWED,
			SupportedCompressors:            nil,
			SupportedBatchUpdateCompressors: nil,
		},
		LowApiVersion:  &smpb.SemVer{Major: 2},
		HighApiVersion: &smpb.SemVer{Major: 2, Minor: 3},
	}, nil
}

// ─── ContentAddressableStorage ───

func (s *Server) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	ns, err := s.resolve(ctx)
	if err != nil {
		return nil, err
	}
	out := &repb.FindMissingBlobsResponse{}
	for _, d := range req.GetBlobDigests() {
		if err := validDigest(d); err != nil {
			return nil, err
		}
		// What another namespace holds is missing here: a write would put it in this one's.
		_, ok, err := s.find(ctx, []string{ns.Write}, d)
		if err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		if !ok {
			out.MissingBlobDigests = append(out.MissingBlobDigests, d)
		}
	}
	return out, nil
}

func (s *Server) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	ns, err := s.resolve(ctx)
	if err != nil {
		return nil, err
	}
	var total int64
	for _, r := range req.GetRequests() {
		total += int64(len(r.GetData()))
	}
	if total > maxBatch {
		return nil, status.Errorf(codes.InvalidArgument, "a batch of %d bytes, past %d", total, maxBatch)
	}
	out := &repb.BatchUpdateBlobsResponse{}
	for _, r := range req.GetRequests() {
		st := status.New(codes.OK, "")
		switch {
		case r.GetCompressor() != repb.Compressor_IDENTITY:
			st = status.New(codes.InvalidArgument, "blobs are uncompressed")
		case validDigest(r.GetDigest()) != nil:
			st = status.Convert(validDigest(r.GetDigest()))
		default:
			if err := s.write(ctx, ns.Write, r.GetDigest(), r.GetData()); err != nil {
				st = status.Convert(err)
			}
		}
		out.Responses = append(out.Responses, &repb.BatchUpdateBlobsResponse_Response{Digest: r.GetDigest(), Status: st.Proto()})
	}
	return out, nil
}

func (s *Server) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	ns, err := s.resolve(ctx)
	if err != nil {
		return nil, err
	}
	var total int64
	for _, d := range req.GetDigests() {
		total += d.GetSizeBytes()
	}
	if total > maxBatch {
		return nil, status.Errorf(codes.InvalidArgument, "a batch of %d bytes, past %d", total, maxBatch)
	}
	out := &repb.BatchReadBlobsResponse{}
	for _, d := range req.GetDigests() {
		r := &repb.BatchReadBlobsResponse_Response{Digest: d, Compressor: repb.Compressor_IDENTITY}
		if err := validDigest(d); err != nil {
			r.Status = status.Convert(err).Proto()
		} else if data, err := s.read(ctx, ns.Read, d); err != nil {
			r.Status = status.Convert(err).Proto()
		} else {
			r.Data, r.Status = data, status.New(codes.OK, "").Proto()
		}
		out.Responses = append(out.Responses, r)
	}
	return out, nil
}

func (s *Server) GetTree(*repb.GetTreeRequest, grpc.ServerStreamingServer[repb.GetTreeResponse]) error {
	return status.Error(codes.Unimplemented, "the cache serves no trees: Bazel caches, it does not execute remotely")
}

// ─── ActionCache ───

func (s *Server) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	ns, err := s.resolve(ctx)
	if err != nil {
		return nil, err
	}
	if err := validDigest(req.GetActionDigest()); err != nil {
		return nil, err
	}
	for _, n := range ns.Read {
		var buf bytes.Buffer
		_, err := s.blobs.Download(ctx, acKey(n, req.GetActionDigest().GetHash()), &buf)
		if errors.Is(err, blob.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		var r repb.ActionResult
		if err := proto.Unmarshal(buf.Bytes(), &r); err != nil {
			continue
		}
		// A result is answered only when everything it names is there to read: one whose outputs
		// went is a miss, which Bazel builds again.
		complete, err := s.complete(ctx, ns.Read, &r)
		if err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		if complete {
			return &r, nil
		}
	}
	return nil, status.Error(codes.NotFound, "no result for the action")
}

// complete reports whether every blob a result names is where read finds it: its files, its
// directories' trees and every file in them, its stdout and stderr.
func (s *Server) complete(ctx context.Context, read []string, r *repb.ActionResult) (bool, error) {
	var ds []*repb.Digest
	for _, f := range r.GetOutputFiles() {
		ds = append(ds, f.GetDigest())
	}
	for _, d := range r.GetOutputDirectories() {
		ds = append(ds, d.GetTreeDigest())
		data, err := s.read(ctx, read, d.GetTreeDigest())
		if status.Code(err) == codes.NotFound {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		var tree repb.Tree
		if err := proto.Unmarshal(data, &tree); err != nil {
			return false, nil
		}
		for _, dir := range append([]*repb.Directory{tree.GetRoot()}, tree.GetChildren()...) {
			for _, f := range dir.GetFiles() {
				ds = append(ds, f.GetDigest())
			}
		}
	}
	for _, d := range []*repb.Digest{r.GetStdoutDigest(), r.GetStderrDigest()} {
		if d != nil {
			ds = append(ds, d)
		}
	}
	for _, d := range ds {
		if validDigest(d) != nil {
			return false, nil
		}
		_, ok, err := s.find(ctx, read, d)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

func (s *Server) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	ns, err := s.resolve(ctx)
	if err != nil {
		return nil, err
	}
	if err := validDigest(req.GetActionDigest()); err != nil {
		return nil, err
	}
	data, err := proto.Marshal(req.GetActionResult())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.blobs.Put(ctx, acKey(ns.Write, req.GetActionDigest().GetHash()), bytes.NewReader(data), int64(len(data))); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return req.GetActionResult(), nil
}

// ─── ByteStream ───

// resource is a ByteStream resource's blob: [instance/]blobs/<hash>/<size>, or, written,
// [instance/]uploads/<uuid>/blobs/<hash>/<size>.
func resource(name string, upload bool) (*repb.Digest, error) {
	parts := strings.Split(name, "/")
	for i := range parts {
		if parts[i] != "blobs" || i+2 >= len(parts) {
			continue
		}
		if upload && (i < 2 || parts[i-2] != "uploads") {
			continue
		}
		size, err := strconv.ParseInt(parts[i+2], 10, 64)
		if err != nil {
			break
		}
		d := &repb.Digest{Hash: parts[i+1], SizeBytes: size}
		if err := validDigest(d); err != nil {
			return nil, err
		}
		return d, nil
	}
	return nil, status.Errorf(codes.InvalidArgument, "no blob in the resource %q (compressed blobs are not served)", name)
}

func (s *Server) Read(req *bspb.ReadRequest, stream grpc.ServerStreamingServer[bspb.ReadResponse]) error {
	ctx := stream.Context()
	ns, err := s.resolve(ctx)
	if err != nil {
		return err
	}
	d, err := resource(req.GetResourceName(), false)
	if err != nil {
		return err
	}
	off, limit := req.GetReadOffset(), req.GetReadLimit()
	if off < 0 || off > d.GetSizeBytes() || limit < 0 {
		return status.Error(codes.OutOfRange, "a read past the blob")
	}
	end := d.GetSizeBytes()
	if limit > 0 && off+limit < end {
		end = off + limit
	}
	if d.GetSizeBytes() == 0 {
		if d.GetHash() != emptyHash {
			return status.Error(codes.NotFound, "no such blob")
		}
		return nil
	}
	n, ok, err := s.find(ctx, ns.Read, d)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	if !ok {
		return status.Errorf(codes.NotFound, "no blob %s/%d", d.GetHash(), d.GetSizeBytes())
	}
	if b, ok := s.mem.get(n, d.GetHash()); ok {
		for p := off; p < end; p += chunk {
			if err := stream.Send(&bspb.ReadResponse{Data: b[p:min(p+chunk, end)]}); err != nil {
				return err
			}
		}
		return nil
	}
	for p := off; p < end; p += chunk {
		data, err := s.blobs.GetRange(ctx, casKey(n, d.GetHash()), p, min(chunk, end-p))
		if err != nil {
			return status.Error(codes.Unavailable, err.Error())
		}
		if err := stream.Send(&bspb.ReadResponse{Data: data}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) Write(stream grpc.ClientStreamingServer[bspb.WriteRequest, bspb.WriteResponse]) error {
	ctx := stream.Context()
	ns, err := s.resolve(ctx)
	if err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	d, err := resource(first.GetResourceName(), true)
	if err != nil {
		return err
	}
	// One the namespace holds already is done at once: the client stops sending.
	if _, ok, err := s.find(ctx, []string{ns.Write}, d); err == nil && ok {
		return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: d.GetSizeBytes()})
	}
	f, err := os.CreateTemp("", "okcache")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer os.Remove(f.Name())
	defer f.Close()
	h := sha256.New()
	w := io.MultiWriter(f, h)
	var n int64
	req := first
	for {
		if req.GetWriteOffset() != n {
			return status.Errorf(codes.InvalidArgument, "a write at %d, where %d were written", req.GetWriteOffset(), n)
		}
		if n+int64(len(req.GetData())) > d.GetSizeBytes() {
			return status.Error(codes.InvalidArgument, "a write past the blob's size")
		}
		if _, err := w.Write(req.GetData()); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		n += int64(len(req.GetData()))
		if req.GetFinishWrite() {
			break
		}
		if req, err = stream.Recv(); err != nil {
			return err
		}
	}
	if err := matches(h, n, d); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if err := s.blobs.Put(ctx, casKey(ns.Write, d.GetHash()), f, n); err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: n})
}

func matches(h hash.Hash, n int64, d *repb.Digest) error {
	if n != d.GetSizeBytes() {
		return status.Errorf(codes.InvalidArgument, "a blob of %d bytes named %d", n, d.GetSizeBytes())
	}
	if hex.EncodeToString(h.Sum(nil)) != d.GetHash() {
		return status.Errorf(codes.InvalidArgument, "a blob whose bytes are not %s", d.GetHash())
	}
	return nil
}

func (s *Server) QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.NotFound, "writes are not resumed: write the blob again")
}

// ─── Expiry ───

// Sweep deletes what the cache wrote before a time, a page at a time, and returns how many.
func (s *Server) Sweep(ctx context.Context, before time.Time) (int, error) {
	n := 0
	after := ""
	for {
		page, next, err := s.blobs.ListPage(ctx, Prefix, after, 1000)
		if err != nil {
			return n, err
		}
		var old []string
		for _, o := range page {
			if o.Modified.Before(before) {
				old = append(old, o.Key)
			}
		}
		if len(old) > 0 {
			if err := s.blobs.Delete(ctx, old...); err != nil {
				return n, fmt.Errorf("sweeping the cache: %w", err)
			}
			n += len(old)
		}
		if next == "" {
			return n, nil
		}
		after = next
	}
}
