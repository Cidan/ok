package store

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/testdb/testobjects"
)

// testNames are the tests' installation's.
var testNames = identity.MustNames("example.com")

// mustOpen opens a Store over db and blobs, with small caches unless cfg says otherwise.
func mustOpen(t *testing.T, db testDB, blobs blob.Store, cfg Config) *Store {
	t.Helper()
	cfg.Names = testNames
	if cfg.ObjectCacheBytes == 0 {
		cfg.ObjectCacheBytes = 8 << 20
	}
	if cfg.ChunkCacheBytes == 0 {
		cfg.ChunkCacheBytes = 8 << 20
	}
	s, err := New(db.open(t), blobs, cfg)
	if err != nil {
		t.Fatal(err)
	}
	testDBs.Store(s, db)
	t.Cleanup(func() {
		s.Close()
		testDBs.Delete(s)
	})
	return s
}

// ids converts IDs to the [][]byte pgx sends as bytea[].
func ids(in []encoding.ID) [][]byte {
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = in[i][:]
	}
	return out
}

func toID(b []byte) (encoding.ID, error) {
	if len(b) != encoding.IDLen {
		return encoding.ID{}, fmt.Errorf("stored ID of %d bytes", len(b))
	}
	return encoding.ID(b), nil
}

// testStore opens a Store over a new database of OK_TEST_DATABASE's (newTestDB) and a new bucket
// of the store OK_TEST_OBJECTS names (internal/testdb/testobjects), both dropped when the test
// ends; the backend's Open brings the database to the schema.
func testStore(t *testing.T) *Store { return testStoreOf(t, 32) }

// testStoreOf is testStore with, on PostgreSQL, a pool of conns connections.
func testStoreOf(t *testing.T, conns int32) *Store {
	t.Helper()
	return mustOpen(t, newTestDB(t, conns), testobjects.Store(t), Config{})
}

// reopen opens a second Store over s's database and bucket, with cold caches: another replica,
// hearing s's signals as s hears its.
func reopen(t *testing.T, s *Store) *Store {
	t.Helper()
	return mustOpen(t, dbOf(s), s.blobs, Config{})
}

var repoSeq atomic.Int64

// testWorkspace is the workspace every test principal acts in: the builder's views name its
// working copy.
const testWorkspace = "default"

// testPrincipals are the principals tests act as; newRepo makes each a testWorkspace.
var testPrincipals = []string{"alice", "bob", "carol", "alice.human.example.com", "bob.human.example.com", "carol.human.example.com"}

func newRepo(t *testing.T, s *Store) Repository {
	t.Helper()
	r, err := s.CreateRepository(context.Background(), data.NewID(), data.NewID(), "alice.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range testPrincipals {
		if _, err := s.CreateWorkspace(context.Background(), p, r.ID, testWorkspace); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

// builder accumulates objects and files to publish.
type builder struct {
	t       *testing.T
	objects map[ObjectRef]encoding.Object
	bodies  map[ObjectRef][]byte
	files   map[encoding.ID][]byte
	order   []ObjectRef
	seq     int
	// chunks are the chunks of the files indexed, by ID: files may be set directly, so chunk
	// indexes those it has not seen when it misses.
	chunks  map[encoding.ID][]byte
	indexed map[encoding.ID]bool
}

func newBuilder(t *testing.T) *builder {
	return &builder{t: t, objects: map[ObjectRef]encoding.Object{}, bodies: map[ObjectRef][]byte{}, files: map[encoding.ID][]byte{}}
}

func (b *builder) add(o encoding.Object) encoding.ID {
	b.t.Helper()
	canonical, err := encoding.Encode(o)
	if err != nil {
		b.t.Fatal(err)
	}
	id := encoding.Hash(canonical)
	r := ObjectRef{Kind: o.Kind(), ID: id}
	if _, ok := b.objects[r]; !ok {
		b.objects[r] = o
		b.bodies[r] = canonical[1:]
		b.order = append(b.order, r)
	}
	return id
}

// testChunk is the fixed chunk size tests split files at (the client uses FastCDC).
const testChunk = 64 << 10

func (b *builder) file(contents []byte) encoding.ID {
	h := encoding.NewHasher(encoding.KindFile)
	h.Write(contents)
	id := h.Sum()
	b.files[id] = contents
	return id
}

func (b *builder) symlink(target string) encoding.ID { return b.add(&encoding.Symlink{Target: target}) }

func fileEntry(name string, id encoding.ID) encoding.TreeEntry {
	return encoding.TreeEntry{Name: name, Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: id}}
}

func treeEntry(name string, id encoding.ID) encoding.TreeEntry {
	return encoding.TreeEntry{Name: name, Value: encoding.TreeValue{Kind: encoding.TreeValueTree, ID: id}}
}

func (b *builder) tree(entries ...encoding.TreeEntry) encoding.ID {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return b.add(&encoding.Tree{Entries: entries})
}

func (b *builder) commit(tree encoding.ID, desc string, parents ...encoding.ID) encoding.ID {
	b.seq++
	sig := encoding.Signature{Name: "t", Email: "t@example.com", Time: encoding.Timestamp{Millis: 1_790_000_000_000 + int64(b.seq)}}
	var change encoding.ChangeID
	copy(change[:], fmt.Sprintf("change-%09d", b.seq))
	return b.add(&encoding.Commit{
		Parents:        parents,
		RootTree:       encoding.Merge[encoding.ID]{tree},
		ConflictLabels: encoding.Merge[string]{""},
		ChangeID:       change,
		Description:    desc,
		Author:         sig,
		Committer:      sig,
	})
}

func (b *builder) view(heads ...encoding.ID) encoding.ID {
	v := &encoding.View{HeadIDs: heads, WCCommitIDs: map[string]encoding.ID{}}
	if len(heads) > 0 {
		v.WCCommitIDs["default"] = heads[0]
	}
	return b.add(v)
}

func (b *builder) operation(view encoding.ID, desc string, parents ...encoding.ID) encoding.ID {
	b.seq++
	return b.add(&encoding.Operation{
		ViewID:  view,
		Parents: parents,
		Metadata: encoding.OperationMetadata{
			Start:       encoding.Timestamp{Millis: int64(b.seq)},
			End:         encoding.Timestamp{Millis: int64(b.seq)},
			Description: desc,
			Hostname:    "test",
			Username:    "t",
		},
		CommitPredecessors: map[encoding.ID][]encoding.ID{},
	})
}

func (b *builder) body(r ObjectRef) []byte { return b.bodies[r] }

func (b *builder) manifests() []FileManifest {
	var out []FileManifest
	for id, contents := range b.files {
		m := FileManifest{FileID: id, Size: uint64(len(contents)), PathHint: "f"}
		for off := 0; off < len(contents); off += testChunk {
			c := contents[off:min(off+testChunk, len(contents))]
			m.Chunks = append(m.Chunks, ChunkRef{ID: pack.ChunkID(c), Length: uint32(len(c))})
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].FileID[:]) < string(out[j].FileID[:]) })
	return out
}

func (b *builder) chunk(id encoding.ID) []byte {
	if c, ok := b.chunks[id]; ok {
		return c
	}
	if b.chunks == nil {
		b.chunks, b.indexed = map[encoding.ID][]byte{}, map[encoding.ID]bool{}
	}
	for f, contents := range b.files {
		if b.indexed[f] {
			continue
		}
		b.indexed[f] = true
		for off := 0; off < len(contents); off += testChunk {
			c := contents[off:min(off+testChunk, len(contents))]
			b.chunks[pack.ChunkID(c)] = c
		}
	}
	if c, ok := b.chunks[id]; ok {
		return c
	}
	b.t.Fatalf("builder has no chunk %s", id)
	return nil
}

// publish runs a whole Publish exchange for everything in b.
func publish(ctx context.Context, s *Store, repo int64, principal string, b *builder, heads *OpHeadsUpdate) ([]encoding.ID, error) {
	p, err := s.BeginPublish(ctx, repo, principal, testWorkspace, b.order, b.manifests())
	if err != nil {
		return nil, err
	}
	objs, chunks := p.Missing()
	for _, r := range objs {
		if err := p.AddObject(EncodedObject{Kind: r.Kind, ID: r.ID, Body: b.body(r)}); err != nil {
			p.Close()
			return nil, err
		}
	}
	for _, c := range chunks {
		if err := p.AddChunk(ctx, c, b.chunk(c)); err != nil {
			p.Close()
			return nil, err
		}
	}
	return p.Commit(ctx, heads)
}

func mustPublish(t *testing.T, s *Store, repo int64, principal string, b *builder, heads *OpHeadsUpdate) []encoding.ID {
	t.Helper()
	got, err := publish(context.Background(), s, repo, principal, b, heads)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	return got
}

func bytesOf(n int, seed byte) []byte {
	out := make([]byte, n)
	x := uint32(seed) + 1
	for i := range out {
		x = x*1664525 + 1013904223
		out[i] = byte(x >> 24)
	}
	return out
}
