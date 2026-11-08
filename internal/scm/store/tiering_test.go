package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
)

// classes records the class each key was put, committed, or set in: the test object stores keep
// none (blobtest's KeepsClasses).
type classes struct {
	blob.Store
	mu   sync.Mutex
	of   map[string]blob.Class
	sets int
}

func (c *classes) record(key string, class blob.Class, set bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.of[key] = class
	if set {
		c.sets++
	}
}

func (c *classes) class(key string) blob.Class {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.of[key]
}

func (c *classes) Put(ctx context.Context, key string, r io.Reader, size int64, opts ...blob.PutOption) error {
	if err := c.Store.Put(ctx, key, r, size, opts...); err != nil {
		return err
	}
	c.record(key, blob.ClassOf(opts...), false)
	return nil
}

func (c *classes) Stage(ctx context.Context, tmp string) (blob.Staged, error) {
	st, err := c.Store.Stage(ctx, tmp)
	if err != nil {
		return nil, err
	}
	return &classedStage{Staged: st, c: c}, nil
}

func (c *classes) SetClass(ctx context.Context, key string, class blob.Class) error {
	if err := c.Store.SetClass(ctx, key, class); err != nil {
		return err
	}
	c.record(key, class, true)
	return nil
}

type classedStage struct {
	blob.Staged
	c *classes
}

func (st *classedStage) Commit(ctx context.Context, key string, opts ...blob.PutOption) error {
	if err := st.Staged.Commit(ctx, key, opts...); err != nil {
		return err
	}
	st.c.record(key, blob.ClassOf(opts...), false)
	return nil
}

// tieringEnv is a CL environment whose store keeps cold packs in the infrequent class, and the
// classes it asks its object store for.
func tieringEnv(t *testing.T) (*clEnv, *classes) {
	t.Helper()
	e := newCLEnv(t)
	c := &classes{Store: e.s.blobs, of: map[string]blob.Class{}}
	e.s = mustOpen(t, dbOf(e.s), c, Config{ColdClass: blob.Infrequent})
	return e, c
}

// agePinned makes a repository's pinned files and chunks days old in its content index.
func agePinned(t *testing.T, s *Store, repo int64, days int) {
	t.Helper()
	old := contentindex.DayOf(dbOf(s).today(t)) - contentindex.Day(days)
	rewriteIndex(t, s, repo, func(_ encoding.ID, e *contentindex.Entry) {
		if e.Pinned {
			e.Day = old
		}
	})
}

// incompressible is n bytes no codec shrinks, from seed.
func incompressible(n int, seed uint64) string {
	r := rand.New(rand.NewPCG(seed, seed))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return string(b)
}

// packClass is the class every pack holding a file's chunks is kept in, by its row and as its object
// store was asked; the file's chunks are all in packs of one class.
func packClass(t *testing.T, s *Store, c *classes, repo int64, contents string) blob.Class {
	t.Helper()
	ctx := context.Background()
	h := encoding.NewHasher(encoding.KindFile)
	h.Write([]byte(contents))
	file := h.Sum()
	_, files, err := s.contentEntries(ctx, repoScope(repo), encoding.KindFile, []encoding.ID{file})
	if err != nil || files[file].Kind != encoding.KindFile {
		t.Fatalf("file %s: %+v, %v", file, files[file], err)
	}
	var ids []encoding.ID
	for _, ch := range files[file].Chunks {
		ids = append(ids, ch.ID)
	}
	net, netScope, err := s.networkOf(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	_, locs, err := s.contentEntries(ctx, netScope, encoding.KindChunk, ids)
	if err != nil {
		t.Fatal(err)
	}
	live, err := s.ops(ctx).LivePacks(ctx, net)
	if err != nil {
		t.Fatal(err)
	}
	byNum := map[uint64]int16{}
	idOf := map[uint64]encoding.ID{}
	for _, p := range live {
		byNum[p.Num], idOf[p.Num] = p.Class, p.ID
	}
	var class blob.Class
	for i, id := range ids {
		num := locs[id].Loc.Pack
		got := blob.Class(byNum[num])
		if asked := c.class(packKey(net, idOf[num])); asked != got {
			t.Fatalf("pack %s's row says class %d, its object store was asked for %d", idOf[num], got, asked)
		}
		if i > 0 && got != class {
			t.Fatalf("file %s's chunks are in packs of classes %d and %d", file, class, got)
		}
		class = got
	}
	return class
}

// Main's older versions go cold — written by compaction in the cold class, apart from what main's
// head holds and from trees — once they have been pinned past ColdAfter; a version that leaves the
// head takes its pack cold, and one that comes back warms it. Reads are the same, from any class.
func TestTiering(t *testing.T) {
	ctx := context.Background()
	e, c := tieringEnv(t)
	v := func(i int) string { return incompressible(1_500_000, uint64(i)) }
	for i := 1; i <= 3; i++ {
		e.landChange(fmt.Sprintf("v%d", i), map[string]string{"big.bin": v(i), "note.txt": fmt.Sprintf("v%d\n", i)})
	}
	gc := func() *GCReport {
		t.Helper()
		agePinned(t, e.s, e.repo, 40)
		r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := gc()
	if got := [3]blob.Class{packClass(t, e.s, c, e.repo, v(1)), packClass(t, e.s, c, e.repo, v(2)), packClass(t, e.s, c, e.repo, v(3))}; got != [3]blob.Class{blob.Infrequent, blob.Infrequent, blob.Standard} {
		t.Fatalf("versions 1 to 3 in classes %v, want the old two cold; %s", got, r)
	}
	if r.ColdPacksWritten == 0 {
		t.Fatalf("compaction wrote no cold pack: %s", r)
	}

	// Version 3 leaves main's head: its pack goes cold, version 4's stays standard.
	e.landChange("v4", map[string]string{"big.bin": v(4), "note.txt": "v4\n"})
	r = gc()
	if got := packClass(t, e.s, c, e.repo, v(3)); got != blob.Infrequent {
		t.Fatalf("version 3, off main's head, is in class %d; %s", got, r)
	}
	if got := packClass(t, e.s, c, e.repo, v(4)); got != blob.Standard {
		t.Fatalf("version 4, on main's head, is in class %d; %s", got, r)
	}

	// Version 3 comes back: its pack warms.
	e.landChange("v3 again", map[string]string{"big.bin": v(3), "note.txt": "v3\n"})
	r = gc()
	if got := packClass(t, e.s, c, e.repo, v(3)); got != blob.Standard {
		t.Fatalf("version 3, back on main's head, is in class %d; %s", got, r)
	}
	for i := 1; i <= 4; i++ {
		h := encoding.NewHasher(encoding.KindFile)
		h.Write([]byte(v(i)))
		got, err := reopen(t, e.s).readFile(ctx, e.repo, h.Sum())
		if err != nil || !bytes.Equal(got, []byte(v(i))) {
			t.Fatalf("version %d reads back: %v", i, err)
		}
	}
	clean(t, e.s, e.repo)
}

// Nothing is cold that a fork's head holds: the network is decided as one, by its first
// repository's collection, a fork's moving no pack.
func TestTieringAcrossForks(t *testing.T) {
	ctx := context.Background()
	e, c := tieringEnv(t)
	v := func(i int) string { return incompressible(1_500_000, uint64(10+i)) }
	e.landChange("v1", map[string]string{"big.bin": v(1)})
	fork := newFork(t, e.s, e.repo)
	e.landChange("v2", map[string]string{"big.bin": v(2)})
	agePinned(t, e.s, e.repo, 40)
	agePinned(t, e.s, fork.ID, 40)
	r, err := e.s.GC(ctx, fork.ID, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.PacksCooled != 0 || r.PacksWarmed != 0 {
		t.Fatalf("a fork's collection moved packs: %s", r)
	}
	if _, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{}); err != nil {
		t.Fatal(err)
	}
	// Version 1 left main's head here, but not the fork's.
	if got := packClass(t, e.s, c, e.repo, v(1)); got != blob.Standard {
		t.Fatalf("version 1, on the fork's head, is in class %d", got)
	}
	if got := packClass(t, e.s, c, e.repo, v(2)); got != blob.Standard {
		t.Fatalf("version 2, on main's head, is in class %d", got)
	}
}

// Off, tiering asks for no class: every pack stays standard however old.
func TestTieringOff(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	c := &classes{Store: e.s.blobs, of: map[string]blob.Class{}}
	e.s = mustOpen(t, dbOf(e.s), c, Config{})
	for i := 1; i <= 3; i++ {
		e.landChange(fmt.Sprintf("v%d", i), map[string]string{"big.bin": incompressible(1_500_000, uint64(20+i))})
	}
	agePinned(t, e.s, e.repo, 400)
	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.ColdPacksWritten != 0 || r.PacksCooled != 0 || c.sets != 0 {
		t.Fatalf("tiering, off, moved packs: %s", r)
	}
	for key, class := range c.of {
		if class != blob.Standard {
			t.Fatalf("%s asked for class %d", key, class)
		}
	}
}
