package store

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/testdb/testobjects"
)

// A publish larger than a pack is held in packs of at most PublishPackBytes, each sent to the
// bucket as it fills: its files read back whole, the content index points into every pack, and
// fsck finds nothing wrong.
func TestPublishInPacks(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, newTestDB(t, 32), testobjects.Store(t), Config{PublishPackBytes: 150_000})
	repo := newRepo(t, s)
	b := newBuilder(t)
	var entries []encoding.TreeEntry
	files := map[encoding.ID][]byte{}
	for i := range 10 {
		contents := bytesOf(200_000, byte(i+1)) // four chunks each
		f := b.file(contents)
		files[f] = contents
		entries = append(entries, fileEntry(string(rune('a'+i))+".bin", f))
	}
	root := b.tree(entries...)
	c := b.commit(root, "many packs\n", encoding.RootCommitID)
	op := b.operation(b.view(c), "commit", encoding.RootOperationID)
	mustPublish(t, s, repo.ID, "alice", b, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op})

	net, err := s.network(ctx, repo.ID)
	if err != nil {
		t.Fatal(err)
	}
	packs, err := s.ops(ctx).PackIDs(ctx, net)
	if err != nil {
		t.Fatal(err)
	}
	// 40 chunks of 64 KiB, two to a pack of at most 150,000 bytes.
	if len(packs) < 15 {
		t.Fatalf("the publish wrote %d packs, want one each two chunks", len(packs))
	}
	for f, want := range files {
		if got := readAll(t, s, repo.ID, f); !slices.Equal(got, want) {
			t.Fatalf("file %s read back %d bytes, want %d", f, len(got), len(want))
		}
	}
	report, err := s.Fsck(ctx, repo.ID, false)
	if err != nil || len(report.Problems) != 0 || len(report.OrphanPacks) != 0 {
		t.Fatalf("fsck: %+v, %v", report, err)
	}
}

// A publish whose file is not its chunks is refused once it commits, though the chunks went to the
// bucket in packs already: those packs are orphans, which collection deletes after their grace.
func TestPublishRefusedAfterPacksSent(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, newTestDB(t, 32), testobjects.Store(t), Config{PublishPackBytes: 150_000})
	repo := newRepo(t, s)
	b := newBuilder(t)
	f := b.file(bytesOf(600_000, 3))
	root := b.tree(fileEntry("big.bin", f))
	c := b.commit(root, "lies\n", encoding.RootCommitID)
	op := b.operation(b.view(c), "commit", encoding.RootOperationID)
	manifests := b.manifests()
	for i := range manifests {
		if manifests[i].FileID == f {
			ch := slices.Clone(manifests[i].Chunks)
			ch[0], ch[1] = ch[1], ch[0]
			manifests[i].Chunks = ch
		}
	}
	p, err := s.BeginPublish(ctx, repo.ID, "alice", testWorkspace, b.order, manifests)
	if err != nil {
		t.Fatal(err)
	}
	objs, chunks := p.Missing()
	for _, r := range objs {
		if err := p.AddObject(EncodedObject{Kind: r.Kind, ID: r.ID, Body: b.body(r)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range chunks {
		if err := p.AddChunk(ctx, id, b.chunk(id)); err != nil {
			t.Fatal(err)
		}
	}
	if len(p.packs) < 2 || !p.packs[0].sent {
		t.Fatalf("the publish held %d packs, the first sent %v: want packs sent before it commits", len(p.packs), len(p.packs) > 0 && p.packs[0].sent)
	}
	if _, err := p.Commit(ctx, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a file whose chunks are out of order: %v", err)
	}
	if got := s.publishMemory.TryAcquire(s.publishMemoryBytes); !got {
		t.Fatal("the refused publish holds memory still")
	}
	s.publishMemory.Release(s.publishMemoryBytes)
	r, err := s.GC(ctx, repo.ID, time.Now().Add(OrphanPackGrace+time.Hour), GCOptions{})
	if err != nil || r.OrphanPacks < 2 {
		t.Fatalf("collection after the grace: %+v, %v; want the refused publish's packs deleted", r, err)
	}
}

// Publishes at once hold no more than PublishMemoryBytes: one that finds too little of it left sends
// what it holds, and waits only if that is still too little; the other, adding on, sends its pack
// and lets it go — neither waits on the other for ever, and both commit.
func TestPublishMemoryBudget(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, newTestDB(t, 32), testobjects.Store(t), Config{PublishMemoryBytes: 300_000})
	repo := newRepo(t, s)
	begin := func(seed byte) (*Publication, *builder, encoding.ID, encoding.ID) {
		b := newBuilder(t)
		f := b.file(bytesOf(400_000, seed)) // seven chunks
		c := b.commit(b.tree(fileEntry("f.bin", f)), "budget\n", encoding.RootCommitID)
		op := b.operation(b.view(c), "commit", encoding.RootOperationID)
		p, err := s.BeginPublish(ctx, repo.ID, "alice", testWorkspace, b.order, b.manifests())
		if err != nil {
			t.Fatal(err)
		}
		objs, _ := p.Missing()
		for _, r := range objs {
			if err := p.AddObject(EncodedObject{Kind: r.Kind, ID: r.ID, Body: b.body(r)}); err != nil {
				t.Fatal(err)
			}
		}
		return p, b, f, op
	}
	p1, b1, f1, op1 := begin(5)
	_, chunks1 := p1.Missing()
	// The four the first adds are whole chunks, which leave the second too little for one of its
	// own, though it sends all it holds.
	slices.SortStableFunc(chunks1, func(a, b encoding.ID) int { return len(b1.chunk(b)) - len(b1.chunk(a)) })
	for _, id := range chunks1[:4] {
		if err := p1.AddChunk(ctx, id, b1.chunk(id)); err != nil {
			t.Fatal(err)
		}
	}
	p2, b2, f2, _ := begin(6)
	_, chunks2 := p2.Missing()
	added := make(chan error, 1)
	go func() {
		for _, id := range chunks2 {
			if err := p2.AddChunk(ctx, id, b2.chunk(id)); err != nil {
				added <- err
				return
			}
		}
		added <- nil
	}()
	select {
	case err := <-added:
		t.Fatalf("the second publish added its chunks while the first held the memory: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	for _, id := range chunks1[4:] {
		if err := p1.AddChunk(ctx, id, b1.chunk(id)); err != nil {
			t.Fatal(err)
		}
	}
	if !p1.packs[0].sent {
		t.Fatal("the first publish, finding the memory held, did not send its pack")
	}
	select {
	case err := <-added:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the second publish waited for ever")
	}
	if _, err := p1.Commit(ctx, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op1}); err != nil {
		t.Fatal(err)
	}
	if _, err := p2.Commit(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for _, f := range []encoding.ID{f1, f2} {
		if got := readAll(t, s, repo.ID, f); len(got) != 400_000 {
			t.Errorf("file %s read back %d bytes", f, len(got))
		}
	}
	// A wait for memory ends with its context.
	p3, b3, _, _ := begin(7)
	_, chunks3 := p3.Missing()
	hold := s.publishMemoryBytes
	if !s.publishMemory.TryAcquire(hold) {
		t.Fatal("the memory is held after both publishes committed")
	}
	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := p3.AddChunk(cctx, chunks3[0], b3.chunk(chunks3[0])); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a wait for memory past its context: %v", err)
	}
	s.publishMemory.Release(hold)
	p3.Close()
}
