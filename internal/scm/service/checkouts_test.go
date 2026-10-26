package service

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/scmtest"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// watchedStore is a Store client recording what a checkout asks for and is sent.
type watchedStore struct {
	scmpb.StoreClient
	mu sync.Mutex
	// reads are the files each ReadFiles asked for, with their bases; deltas the delta frames
	// received; layers the layers each ReadCheckout sent.
	reads  [][]*scmpb.FileRange
	deltas int
	layers [][]byte
}

func (w *watchedStore) ReadFiles(ctx context.Context, in *scmpb.ReadFilesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[scmpb.FileData], error) {
	w.mu.Lock()
	w.reads = append(w.reads, in.GetFiles())
	w.mu.Unlock()
	s, err := w.StoreClient.ReadFiles(ctx, in, opts...)
	return &watchedFiles{ServerStreamingClient: s, w: w}, err
}

type watchedFiles struct {
	grpc.ServerStreamingClient[scmpb.FileData]
	w *watchedStore
}

func (f *watchedFiles) Recv() (*scmpb.FileData, error) {
	m, err := f.ServerStreamingClient.Recv()
	if err == nil && m.GetCodec() == scmpb.ChunkCodec_CHUNK_CODEC_DELTA {
		f.w.mu.Lock()
		f.w.deltas++
		f.w.mu.Unlock()
	}
	return m, err
}

func (w *watchedStore) ReadCheckout(ctx context.Context, in *scmpb.ReadCheckoutRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[scmpb.CheckoutFrame], error) {
	s, err := w.StoreClient.ReadCheckout(ctx, in, opts...)
	return &watchedCheckout{ServerStreamingClient: s, w: w}, err
}

type watchedCheckout struct {
	grpc.ServerStreamingClient[scmpb.CheckoutFrame]
	w *watchedStore
}

func (c *watchedCheckout) Recv() (*scmpb.CheckoutFrame, error) {
	m, err := c.ServerStreamingClient.Recv()
	if l := m.GetLayer(); err == nil && l != nil && l.GetLast() {
		c.w.mu.Lock()
		c.w.layers = append(c.w.layers, l.GetLayerId())
		c.w.mu.Unlock()
	}
	return m, err
}

func (w *watchedStore) take() (reads [][]*scmpb.FileRange, deltas int, layers [][]byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	reads, deltas, layers = w.reads, w.deltas, w.layers
	w.reads, w.deltas, w.layers = nil, 0, nil
	return
}

// A worker's real checkout moves between commits reading what changed (docs/scm.md, "Checkouts",
// "Deltas"): the commit's checkout in one request — its first whole, then a layer of what the next
// commit changed — the files planning reads from it, and each changed file as a delta against the
// one it replaces.
func TestCheckoutMoves(t *testing.T) {
	c := testServer(t)
	alice, worker := human("alice.human.example.com"), as(workerPrincipal)
	repo := newRepo(t, c).GetId()
	if _, err := c.ws.CreateWorkspace(alice, &scmpb.CreateWorkspaceRequest{RepositoryId: repo, Workspace: "default"}); err != nil {
		t.Fatal(err)
	}
	b := scmtest.NewObjects(t)
	b.Workspace = "default"
	var text []byte
	for i := range 3000 {
		text = append(text, []byte("func f"+string(rune('a'+i%26))+"() { return }\n")...)
	}
	edited := bytes.Replace(text, []byte("func fq()"), []byte("func fQ()"), 1)
	build, v1, v2, fresh := b.File([]byte("cc_library(name = \"x\")\n")), b.File(text), b.File(edited), b.File([]byte("new\n"))
	file := func(name string, id encoding.ID) encoding.TreeEntry {
		return encoding.TreeEntry{Name: name, Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: id}}
	}
	t1 := b.Add(&encoding.Tree{Entries: []encoding.TreeEntry{file("BUILD", build), file("a.go", v1)}})
	t2 := b.Add(&encoding.Tree{Entries: []encoding.TreeEntry{file("BUILD", build), file("a.go", v2), file("new.txt", fresh)}})
	c1 := b.Commit(t1, "one\n", encoding.RootCommitID)
	c2 := b.Commit(t2, "two\n", c1)
	view := b.Add(&encoding.View{HeadIDs: []encoding.ID{c2}, WCCommitIDs: map[string]encoding.ID{"default": c2}})
	op := b.Operation(view, encoding.RootOperationID)
	if _, _, err := b.Publish(alice, c.store, repo, &scmpb.OpHeadsUpdate{OldIds: [][]byte{encoding.RootOperationID[:]}, NewId: op[:]}); err != nil {
		t.Fatal(err)
	}

	w := &watchedStore{StoreClient: c.store}
	co, err := checkout.NewReal(filepath.Join(t.TempDir(), "co"), checkout.SCM(w))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := co.Sync(worker, repo, c1[:], nil); err != nil {
		t.Fatal(err)
	}
	reads, deltas, layers := w.take()
	if len(layers) != 1 || deltas != 0 {
		t.Errorf("the first checkout: %d layers, %d deltas; want its one layer", len(layers), deltas)
	}
	// BUILD came with the layer; a.go alone was read.
	if len(reads) != 1 || len(reads[0]) != 1 || !bytes.Equal(reads[0][0].GetFileId(), v1[:]) {
		t.Errorf("the first checkout read %v; want a.go alone", reads)
	}

	if _, err := co.Sync(worker, repo, c2[:], nil); err != nil {
		t.Fatal(err)
	}
	reads, deltas, layers = w.take()
	if len(layers) != 1 || deltas != 1 {
		t.Errorf("the move: %d layers, %d deltas; want one each", len(layers), deltas)
	}
	var bases [][]byte
	for _, r := range slices.Concat(reads...) {
		bases = append(bases, r.GetBaseFileId())
	}
	if len(bases) != 2 || !slices.ContainsFunc(bases, func(b []byte) bool { return bytes.Equal(b, v1[:]) }) {
		t.Errorf("the move read with bases %x; want a.go against its last version, new.txt against none", bases)
	}
	// The checkout records the tree it holds, which its next move compares from.
	if st, err := os.ReadFile(filepath.Join(filepath.Dir(co.Dir()), ".co.checkout")); err != nil || len(st) < 40 || !bytes.Equal(st[8:40], t2[:]) {
		t.Errorf("the checkout's record names %x, want tree %x (%v)", st[8:min(40, len(st))], t2[:], err)
	}
	for name, want := range map[string][]byte{"BUILD": []byte("cc_library(name = \"x\")\n"), "a.go": edited, "new.txt": []byte("new\n")} {
		if got, err := os.ReadFile(filepath.Join(co.Dir(), name)); err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s: %d bytes, %v", name, len(got), err)
		}
	}
	// Moving back reads nothing new but the layer it holds: none is sent.
	if _, err := co.Sync(worker, repo, c1[:], nil); err != nil {
		t.Fatal(err)
	}
	if _, deltas, layers := w.take(); len(layers) != 0 || deltas != 1 {
		t.Errorf("moving back: %d layers, %d deltas; want none, and a.go against the edit", len(layers), deltas)
	}
	if got, _ := os.ReadFile(filepath.Join(co.Dir(), "a.go")); !bytes.Equal(got, text) {
		t.Error("moving back left a.go edited")
	}
}
