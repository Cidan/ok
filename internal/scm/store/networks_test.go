package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
)

// A repository is in a network of its own, which holds where its chunks are: a publish records
// the files and chunks it holds in the repository's content index, without locations, and each
// chunk's location in the network's chunk index; packs and segments — the chunk index's and the
// repository's — are under the network's keys, which every repository in it may name.
func TestNetworkHoldsLocations(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	other := newRepo(t, e.s)
	net := netOf(t, e.s, e.repo)
	if r, err := e.s.GetRepository(ctx, e.repo); err != nil || r.Network != net || int64(net) == e.repo {
		t.Fatalf("the repository's network: %+v, %v; a network has an ID of its own", r, err)
	}
	if netOf(t, e.s, other.ID) == net {
		t.Fatal("two repositories forked from none share a network")
	}

	contents := []byte("a file of its own\n")
	f := uploadFile(t, e, "f", contents)
	chunk := pack.ChunkID(contents)
	held := indexOf(t, e.s, e.repo)
	if fe, ok := held.Get(f); !ok || len(fe.Chunks) != 1 || fe.Chunks[0].ID != chunk {
		t.Fatalf("the repository's index of the file: %+v, %v", fe, ok)
	}
	if ce, ok := held.Get(chunk); !ok || ce.Loc != (contentindex.Location{}) {
		t.Fatalf("the repository's index of the chunk: %+v, %v; it holds no location", ce, ok)
	}
	loc, ok := locationsOf(t, e.s, e.repo).Get(chunk)
	if !ok || loc.Loc.Stored == 0 {
		t.Fatalf("the network's location of the chunk: %+v, %v", loc, ok)
	}
	if _, ok := locationsOf(t, e.s, e.repo).Get(f); ok {
		t.Fatal("the network's chunk index holds a file")
	}
	if got := readAll(t, reopen(t, e.s), e.repo, f); string(got) != string(contents) {
		t.Fatalf("read back %q", got)
	}

	if _, err := e.s.CompactSegments(ctx, e.repo, 1); err != nil {
		t.Fatal(err)
	}
	keys := func(prefix string) []string {
		t.Helper()
		objs, err := e.s.blobs.List(ctx, prefix)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, o := range objs {
			out = append(out, o.Key)
		}
		return out
	}
	netKeys := keys(fmt.Sprintf("nets/%d/", net))
	var packs, segs int
	for _, k := range netKeys {
		switch {
		case strings.HasPrefix(k, packPrefix(net)):
			packs++
		case strings.HasPrefix(k, segmentPrefix(net)):
			segs++
		}
	}
	all, err := e.s.ops(ctx).NetworkSegments(ctx, net)
	if err != nil {
		t.Fatal(err)
	}
	var netRows, repoRows int
	for _, sg := range all {
		if sg.Scope.Network {
			netRows++
		} else {
			repoRows++
		}
	}
	if packs == 0 || segs != len(all) || netRows == 0 || repoRows == 0 {
		t.Errorf("under the network's keys: %d packs and %d segments; want packs, and the %d segments of the chunk index and the "+
			"repository's %d", packs, segs, netRows, repoRows)
	}
	if left := keys(fmt.Sprintf("repos/%d/", e.repo)); len(left) != 0 {
		t.Errorf("under the repository's keys: %v; want nothing", left)
	}
	clean(t, e.s, e.repo)
}

// A sweep takes a chunk nothing reaches from the repository's content index and its location from
// its network's chunk index together.
func TestSweepDropsLocations(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	contents := bytesOf(50_000, 9)
	b.commit(b.tree(fileEntry("gone", b.file(contents))), "garbage", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	chunks := chunkIDs(contents)
	if _, ok := locationsOf(t, e.s, e.repo).Get(chunks[0]); !ok {
		t.Fatal("no location before the sweep")
	}
	age(t, e.s, e.repo, 20)
	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil || r.ChunksSwept != int64(len(chunks)) || r.ChunkBytesSwept == 0 {
		t.Fatalf("the sweep: %s, %v", r, err)
	}
	locs := locationsOf(t, e.s, e.repo)
	for _, c := range chunks {
		if has(t, e.s, e.repo, "chunk", c) {
			t.Errorf("chunk %s is still held after the sweep", c)
		}
		if _, ok := locs.Get(c); ok {
			t.Errorf("chunk %s still has a location after the sweep", c)
		}
	}
	clean(t, e.s, e.repo)
}

// fsck finds a chunk of a file the repository holds that the repository does not hold, and with
// repair records it held.
func TestFsckFindsUnheldChunks(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	contents := []byte("held, and its chunk with it\n")
	f := uploadFile(t, e, "f", contents)
	chunk := pack.ChunkID(contents)
	if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		e.s.logIndex(tx, repoScope(e.repo), contentindex.Update{Op: contentindex.Drop, ID: chunk, Entry: contentindex.Entry{Kind: encoding.KindChunk}})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r, err := e.s.Fsck(ctx, e.repo, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Problems) != 1 || !strings.Contains(r.Problems[0], chunk.String()) || !strings.Contains(r.Problems[0], "not held") {
		t.Fatalf("fsck: %v; want the chunk not held", r.Problems)
	}
	if r, err = e.s.Fsck(ctx, e.repo, true); err != nil || len(r.Repaired) != 1 {
		t.Fatalf("repair: %v, %v", r, err)
	}
	clean(t, e.s, e.repo)
	if got := readAll(t, reopen(t, e.s), e.repo, f); string(got) != string(contents) {
		t.Fatalf("read back %q", got)
	}
}
