package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/blob"
)

func digest(b []byte) *repb.Digest {
	s := sha256.Sum256(b)
	return &repb.Digest{Hash: hex.EncodeToString(s[:]), SizeBytes: int64(len(b))}
}

// served serves a cache over a directory's bucket, each call's namespaces those its x-ns header
// names (write, then reads): a client of it, and the bucket's directory.
func served(t *testing.T) (*grpc.ClientConn, *Server, string) {
	t.Helper()
	return servedOver(t, func(s blob.Store) blob.Store { return s })
}

// exactly is a store whose Put reads exactly the size it is given, as S3's multipart upload reads
// each part — never past the last byte, to learn of an error a reader would give after it — and
// stores it once it has.
type exactly struct{ blob.Store }

func (e exactly) Put(ctx context.Context, key string, r io.Reader, size int64, opts ...blob.PutOption) error {
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	return e.Store.Put(ctx, key, bytes.NewReader(buf), size, opts...)
}

// servedOver is served, over the directory's bucket as wrap makes it.
func servedOver(t *testing.T, wrap func(blob.Store) blob.Store) (*grpc.ClientConn, *Server, string) {
	t.Helper()
	dir := t.TempDir()
	fs, err := blob.NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	blobs := wrap(fs)
	resolve := func(ctx context.Context) (Namespaces, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		ns := md.Get("x-ns")
		if len(ns) == 0 {
			return Namespaces{}, status.Error(codes.PermissionDenied, "no namespace")
		}
		return Namespaces{Write: ns[0], Read: ns}, nil
	}
	s := New(blobs, resolve, 1<<20)
	g := grpc.NewServer()
	s.Register(g)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(l)
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, s, dir
}

func in(ns ...string) context.Context {
	md := metadata.MD{}
	for _, n := range ns {
		md.Append("x-ns", n)
	}
	return metadata.NewOutgoingContext(context.Background(), md)
}

// Blobs are stored only as their digests say; found where the namespace holds them; read whole
// or a stream at a time; and an action's result is answered only when everything it names is
// there.
func TestCache(t *testing.T) {
	conn, _, _ := served(t)
	cas := repb.NewContentAddressableStorageClient(conn)
	ac := repb.NewActionCacheClient(conn)
	bs := bspb.NewByteStreamClient(conn)
	acme := in("acme")

	if caps, err := repb.NewCapabilitiesClient(conn).GetCapabilities(acme, &repb.GetCapabilitiesRequest{}); err != nil ||
		caps.GetCacheCapabilities().GetDigestFunctions()[0] != repb.DigestFunction_SHA256 || !caps.GetCacheCapabilities().GetActionCacheUpdateCapabilities().GetUpdateEnabled() {
		t.Fatalf("capabilities: %v, %v", caps, err)
	}
	if _, err := cas.FindMissingBlobs(context.Background(), &repb.FindMissingBlobsRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("a call of no namespace: %v", err)
	}
	out, stderr := []byte("an output\n"), []byte("warnings\n")
	resp, err := cas.BatchUpdateBlobs(acme, &repb.BatchUpdateBlobsRequest{Requests: []*repb.BatchUpdateBlobsRequest_Request{
		{Digest: digest(out), Data: out},
		{Digest: digest([]byte("something else")), Data: stderr}, // bytes that are not its digest's
	}})
	if err != nil {
		t.Fatal(err)
	}
	if c := resp.GetResponses()[0].GetStatus().GetCode(); c != 0 {
		t.Errorf("a blob as its digest says: %v", resp.GetResponses()[0].GetStatus())
	}
	if c := resp.GetResponses()[1].GetStatus().GetCode(); codes.Code(c) != codes.InvalidArgument {
		t.Errorf("a blob whose bytes are not its digest's: %v", resp.GetResponses()[1].GetStatus())
	}
	missing, err := cas.FindMissingBlobs(acme, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{digest(out), digest(stderr)}})
	if err != nil || len(missing.GetMissingBlobDigests()) != 1 || missing.GetMissingBlobDigests()[0].GetHash() != digest(stderr).GetHash() {
		t.Errorf("missing: %v, %v", missing, err)
	}
	read, err := cas.BatchReadBlobs(acme, &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{digest(out)}})
	if err != nil || !bytes.Equal(read.GetResponses()[0].GetData(), out) {
		t.Errorf("reading it: %v, %v", read, err)
	}

	// A blob of many chunks, through ByteStream.
	big := bytes.Repeat([]byte("0123456789abcdef"), 20000)
	d := digest(big)
	w, err := bs.Write(acme)
	if err != nil {
		t.Fatal(err)
	}
	name := "uploads/u1/blobs/" + d.GetHash() + "/" + strconv.FormatInt(d.GetSizeBytes(), 10)
	for off := 0; off < len(big); off += 100000 {
		end := min(off+100000, len(big))
		if err := w.Send(&bspb.WriteRequest{ResourceName: name, WriteOffset: int64(off), Data: big[off:end], FinishWrite: end == len(big)}); err != nil {
			t.Fatal(err)
		}
	}
	if wr, err := w.CloseAndRecv(); err != nil || wr.GetCommittedSize() != d.GetSizeBytes() {
		t.Fatalf("writing it: %v, %v", wr, err)
	}
	r, err := bs.Read(acme, &bspb.ReadRequest{ResourceName: "blobs/" + d.GetHash() + "/" + strconv.FormatInt(d.GetSizeBytes(), 10)})
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	for {
		m, err := r.Recv()
		if err != nil {
			break
		}
		got = append(got, m.GetData()...)
	}
	if !bytes.Equal(got, big) {
		t.Errorf("read back %d bytes, want %d", len(got), len(big))
	}
	wrong, err := bs.Write(acme)
	if err != nil {
		t.Fatal(err)
	}
	if err := wrong.Send(&bspb.WriteRequest{ResourceName: "uploads/u2/blobs/" + digest([]byte("x")).GetHash() + "/1", Data: []byte("y"), FinishWrite: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := wrong.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a stream whose bytes are not its digest's: %v", err)
	}

	// An action's result, answered while its outputs are there.
	action := digest([]byte("an action"))
	result := &repb.ActionResult{OutputFiles: []*repb.OutputFile{{Path: "out", Digest: digest(out)}}, StderrDigest: digest(stderr)}
	if _, err := ac.UpdateActionResult(acme, &repb.UpdateActionResultRequest{ActionDigest: action, ActionResult: result}); err != nil {
		t.Fatal(err)
	}
	if _, err := ac.GetActionResult(acme, &repb.GetActionResultRequest{ActionDigest: action}); status.Code(err) != codes.NotFound {
		t.Errorf("a result whose stderr is not there: %v", err)
	}
	if _, err := cas.BatchUpdateBlobs(acme, &repb.BatchUpdateBlobsRequest{Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: digest(stderr), Data: stderr}}}); err != nil {
		t.Fatal(err)
	}
	if got, err := ac.GetActionResult(acme, &repb.GetActionResultRequest{ActionDigest: action}); err != nil || got.GetOutputFiles()[0].GetPath() != "out" {
		t.Errorf("a whole result: %v, %v", got, err)
	}
}

// A written blob is stored only whole and as its digest says: bytes whose last differs from the
// digest's, a stream that ends short of its size, and one cut off before it finishes leave nothing,
// and the blob, written again rightly, is stored.
func TestWritesStoreOnlyTheirDigest(t *testing.T) {
	conn, _, _ := servedOver(t, func(s blob.Store) blob.Store { return exactly{s} })
	bs := bspb.NewByteStreamClient(conn)
	cas := repb.NewContentAddressableStorageClient(conn)
	acme := in("acme")
	big := bytes.Repeat([]byte("0123456789abcdef"), 200000)
	d := digest(big)
	name := "uploads/u/blobs/" + d.GetHash() + "/" + strconv.FormatInt(d.GetSizeBytes(), 10)
	missing := func() bool {
		t.Helper()
		resp, err := cas.FindMissingBlobs(acme, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{d}})
		if err != nil {
			t.Fatal(err)
		}
		return len(resp.GetMissingBlobDigests()) == 1
	}
	write := func(data []byte, finish bool) error {
		t.Helper()
		w, err := bs.Write(acme)
		if err != nil {
			t.Fatal(err)
		}
		for off := 0; off < len(data); off += 100000 {
			end := min(off+100000, len(data))
			if err := w.Send(&bspb.WriteRequest{ResourceName: name, WriteOffset: int64(off), Data: data[off:end],
				FinishWrite: finish && end == len(data)}); err != nil {
				return err
			}
		}
		if !finish {
			w.CloseSend()
		}
		_, err = w.CloseAndRecv()
		return err
	}
	lastWrong := bytes.Clone(big)
	lastWrong[len(lastWrong)-1] ^= 1
	if err := write(lastWrong, true); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a blob whose last byte is not its digest's: %v", err)
	}
	if !missing() {
		t.Fatal("a blob whose last byte is not its digest's was stored")
	}
	if err := write(big[:len(big)-5], true); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a blob short of its size: %v", err)
	}
	if !missing() {
		t.Fatal("a blob short of its size was stored")
	}
	if err := write(big[:len(big)/2], false); err == nil {
		t.Error("a write cut off before it finished succeeded")
	}
	if !missing() {
		t.Fatal("a write cut off before it finished was stored")
	}
	if err := write(big, true); err != nil {
		t.Fatalf("the blob, written rightly: %v", err)
	}
	if missing() {
		t.Error("the blob, written rightly, is missing")
	}
}

// Each namespace is its own: one's results and blobs are not another's, a write goes to the
// call's namespace alone, and a call that reads several finds what the first holds first.
func TestNamespaces(t *testing.T) {
	conn, _, _ := served(t)
	cas := repb.NewContentAddressableStorageClient(conn)
	ac := repb.NewActionCacheClient(conn)
	b := []byte("acme's output")
	action := digest([]byte("build //lib"))
	put := func(ctx context.Context, data []byte, r *repb.ActionResult) {
		t.Helper()
		if _, err := cas.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: digest(data), Data: data}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := ac.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{ActionDigest: action, ActionResult: r}); err != nil {
			t.Fatal(err)
		}
	}
	put(in("acme"), b, &repb.ActionResult{ExitCode: 0, OutputFiles: []*repb.OutputFile{{Path: "out", Digest: digest(b)}}})

	// Another account has none of it.
	if _, err := ac.GetActionResult(in("globex"), &repb.GetActionResultRequest{ActionDigest: action}); status.Code(err) != codes.NotFound {
		t.Errorf("another account read acme's result: %v", err)
	}
	if r, err := cas.BatchReadBlobs(in("globex"), &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{digest(b)}}); err != nil ||
		codes.Code(r.GetResponses()[0].GetStatus().GetCode()) != codes.NotFound {
		t.Errorf("another account read acme's blob: %v, %v", r, err)
	}
	// A pre of someone who may not land reads acme's, writes its own: what it writes is never
	// acme's.
	pre := in("acme/u/eve", "acme")
	if got, err := ac.GetActionResult(pre, &repb.GetActionResultRequest{ActionDigest: action}); err != nil || got.GetOutputFiles()[0].GetPath() != "out" {
		t.Errorf("the pre reading acme's result: %v, %v", got, err)
	}
	theirs := []byte("eve's output")
	put(pre, theirs, &repb.ActionResult{ExitCode: 1, OutputFiles: []*repb.OutputFile{{Path: "out", Digest: digest(theirs)}}})
	if got, err := ac.GetActionResult(in("acme"), &repb.GetActionResultRequest{ActionDigest: action}); err != nil || got.GetExitCode() != 0 {
		t.Errorf("acme's result after the pre wrote its own: %v, %v", got, err)
	}
	if got, err := ac.GetActionResult(pre, &repb.GetActionResultRequest{ActionDigest: action}); err != nil || got.GetExitCode() != 1 {
		t.Errorf("the pre's own result, first: %v, %v", got, err)
	}
	if missing, err := cas.FindMissingBlobs(in("acme"), &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{digest(theirs)}}); err != nil ||
		len(missing.GetMissingBlobDigests()) != 1 {
		t.Errorf("the pre's blob in acme's: %v, %v", missing, err)
	}
}

// What was written ttl ago goes; what was written since stays.
func TestSweep(t *testing.T) {
	conn, s, dir := served(t)
	cas := repb.NewContentAddressableStorageClient(conn)
	old, fresh := []byte("old"), []byte("fresh")
	var cutoff time.Time
	for i, b := range [][]byte{old, fresh} {
		if i == 1 {
			time.Sleep(20 * time.Millisecond)
			cutoff = time.Now()
			time.Sleep(20 * time.Millisecond)
		}
		if _, err := cas.BatchUpdateBlobs(in("acme"), &repb.BatchUpdateBlobsRequest{Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: digest(b), Data: b}}}); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.Sweep(context.Background(), cutoff)
	if err != nil || n != 1 {
		t.Fatalf("Sweep = %d, %v; want the old blob alone", n, err)
	}
	// A fresh server, whose memory holds nothing.
	blobs, _ := blob.NewFS(dir)
	again := New(blobs, func(context.Context) (Namespaces, error) {
		return Namespaces{Write: "acme", Read: []string{"acme"}}, nil
	}, 0)
	for b, want := range map[string]bool{"old": false, "fresh": true} {
		if _, ok, err := again.find(context.Background(), []string{"acme"}, digest([]byte(b))); err != nil || ok != want {
			t.Errorf("%s after the sweep: %v, %v", b, ok, err)
		}
	}
}
