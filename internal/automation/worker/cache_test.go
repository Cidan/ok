package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/automation/cache"
	"github.com/Cidan/ok/internal/blob"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// A run's pass-through reaches the remote cache naming its run alone — whatever its client says
// it is — streams both ways, and passes on nothing but the cache's methods.
func TestCachePassThrough(t *testing.T) {
	blobs, err := blob.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var named []string
	srv := cache.New(blobs, func(ctx context.Context) (cache.Namespaces, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		mu.Lock()
		named = append(named, md.Get(runHeader)...)
		mu.Unlock()
		return cache.Namespaces{Write: "acme", Read: []string{"acme"}}, nil
	}, 1<<20)
	g := grpc.NewServer()
	srv.Register(g)
	automationpb.RegisterWorkServer(g, automationpb.UnimplementedWorkServer{})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(l)
	defer g.Stop()
	auto, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer auto.Close()

	proxy, err := startCacheProxy(filepath.Join(t.TempDir(), "cache.sock"), auto, 42)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Stop()
	conn, err := grpc.NewClient("unix:"+proxy.path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The client says it is another run: the pass-through says 42.
	ctx := metadata.AppendToOutgoingContext(context.Background(), runHeader, "7")

	data := bytes.Repeat([]byte("abc"), 100000)
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	w, err := bspb.NewByteStreamClient(conn).Write(ctx)
	if err != nil {
		t.Fatal(err)
	}
	name := "uploads/u/blobs/" + hash + "/" + strconv.Itoa(len(data))
	for off := 0; off < len(data); off += 64 << 10 {
		end := min(off+64<<10, len(data))
		if err := w.Send(&bspb.WriteRequest{ResourceName: name, WriteOffset: int64(off), Data: data[off:end], FinishWrite: end == len(data)}); err != nil {
			t.Fatal(err)
		}
	}
	if resp, err := w.CloseAndRecv(); err != nil || resp.GetCommittedSize() != int64(len(data)) {
		t.Fatalf("writing through the pass-through: %v, %v", resp, err)
	}
	r, err := bspb.NewByteStreamClient(conn).Read(ctx, &bspb.ReadRequest{ResourceName: "blobs/" + hash + "/" + strconv.Itoa(len(data))})
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
	if !bytes.Equal(got, data) {
		t.Errorf("read back %d bytes, want %d", len(got), len(data))
	}
	if _, err := repb.NewActionCacheClient(conn).GetActionResult(ctx, &repb.GetActionResultRequest{ActionDigest: &repb.Digest{Hash: hash, SizeBytes: 1}}); status.Code(err) != codes.NotFound {
		t.Errorf("a result not there, through the pass-through: %v", err)
	}
	mu.Lock()
	for _, n := range named {
		if n != "42" {
			t.Errorf("the cache was told run %q, want 42", n)
		}
	}
	if len(named) == 0 {
		t.Error("the cache was told no run")
	}
	mu.Unlock()
	// Not the cache: refused, never forwarded.
	if _, err := automationpb.NewWorkClient(conn).Lease(ctx, &automationpb.LeaseRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("another service through the pass-through: %v", err)
	}
}
