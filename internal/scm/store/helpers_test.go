package store

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/testdb"
)

// testNames are the tests' installation's.
var testNames = identity.MustNames("example.com")

// mustOpen opens a Store over pool and blobs, with small caches unless cfg says otherwise.
func mustOpen(t *testing.T, pool *pgxpool.Pool, blobs blob.Store, cfg Config) *Store {
	t.Helper()
	cfg.Names = testNames
	if cfg.ObjectCacheBytes == 0 {
		cfg.ObjectCacheBytes = 8 << 20
	}
	if cfg.ChunkCacheBytes == 0 {
		cfg.ChunkCacheBytes = 8 << 20
	}
	s, err := Open(context.Background(), pool, blobs, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

// testStore opens a Store over a new database and a new S3 bucket on the servers the test is
// given (internal/testdb), both dropped when the test ends; Open brings the database to the
// schema.
func testStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s3 := testdb.S3(t)
	blobs, err := blob.NewS3(blob.Config{
		Endpoint:  s3.Endpoint,
		Bucket:    testdb.Bucket(t),
		AccessKey: s3.AccessKey,
		SecretKey: s3.SecretKey,
		Region:    s3.Region,
	})
	if err != nil {
		t.Fatal(err)
	}
	return mustOpen(t, pool, blobs, Config{})
}

// reopen opens a second Store over s's database and bucket, with cold caches.
func reopen(t *testing.T, s *Store) *Store {
	t.Helper()
	return mustOpen(t, s.pool, s.blobs, Config{})
}

var repoSeq atomic.Int64

// testWorkspace is the workspace every test principal acts in: the builder's views name its
// working copy.
const testWorkspace = "default"

// testPrincipals are the principals tests act as; newRepo makes each a testWorkspace.
var testPrincipals = []string{"alice", "bob", "carol", "alice.human.example.com", "bob.human.example.com", "carol.human.example.com"}

func newRepo(t *testing.T, s *Store) Repository {
	t.Helper()
	name := fmt.Sprintf("t%d-%d", time.Now().UnixNano(), repoSeq.Add(1))
	r, err := s.CreateRepository(context.Background(), name, "alice.human.example.com")
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
	for _, contents := range b.files {
		for off := 0; off < len(contents); off += testChunk {
			c := contents[off:min(off+testChunk, len(contents))]
			if pack.ChunkID(c) == id {
				return c
			}
		}
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
		if err := p.AddChunk(c, b.chunk(c)); err != nil {
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
