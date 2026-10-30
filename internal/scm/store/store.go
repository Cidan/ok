// Package store is ok's SCM's storage core (docs/scm.md, "The server"): the Publish protocol
// with full verification, closure, transitive touching, and atomicity, every Store read, the
// per-principal operation logs, operation collection, and fsck — over a database (a
// backend.Backend: internal/scm/store/postgres) and the packs in object storage.
//
// Every object a repository holds lives in the database as its canonical body, except file
// contents, which live as chunks in packs in object storage. Replicas are stateless: all
// coordination is the database's row locks, and every mutation is one transaction.
package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/work"
	"golang.org/x/sync/semaphore"
)

// Errors a caller maps to a gRPC status. Wrap them with %w.
var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrInvalid       = errors.New("invalid argument")
)

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

func notFound(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrNotFound, fmt.Sprintf(format, args...))
}

// Rejection refuses a publish whose references do not resolve: Missing lists what was
// referenced but neither stored nor sent.
type Rejection struct {
	Missing []ObjectRef
	Message string
}

func (r *Rejection) Error() string { return "publish rejected: " + r.Message }

// ObjectRef names an object by kind and ID.
type ObjectRef struct {
	Kind encoding.Kind
	ID   encoding.ID
}

func (r ObjectRef) String() string { return r.Kind.String() + " " + r.ID.String() }

// EncodedObject is an object as its kind and canonical body.
type EncodedObject struct {
	Kind encoding.Kind
	ID   encoding.ID
	Body []byte
}

// Config sizes the store's in-memory caches.
type Config struct {
	// Names are the installation's: people are <handle>.human.<domain>, ACLs <name>.acl.<domain>.
	Names identity.Names
	// EmailDomain is what a person's handle is an address at, in the commits the SCM writes for
	// them and the reviewers an upload names; Names' domain unless set.
	EmailDomain string
	// Principals names the principals the store records by stable ID (docs/hosted.md, "Tenancy,
	// as built"), for the commits it writes for them and the messages that name them; with none
	// (tests), a principal's ID is its name. SetPrincipals sets it later.
	Principals Namer
	// CodeIndexCacheBytes is the memory a replica keeps code search indexes in:
	// DefaultCodeIndexCacheBytes unless set.
	CodeIndexCacheBytes int
	// ObjectCacheBytes bounds the cache of object bodies (trees, commits, …).
	ObjectCacheBytes int
	// ChunkCacheBytes bounds the cache of file-content chunks.
	ChunkCacheBytes int
	// DiffCacheBytes bounds the cache of file pairs' diffs (DiffFiles).
	DiffCacheBytes int
	// DeltaCacheBytes bounds the cache of files' deltas against bases (ReadFiles with deltas).
	DeltaCacheBytes int
	// CheckoutCacheBytes bounds the cache of checkout layers' bytes (ReadCheckout).
	CheckoutCacheBytes int
	// PathCommitCacheBytes bounds the cache of the newest commits of main that changed paths, which
	// a tree's listing with its entries' last commits reads (lastPathCommits).
	PathCommitCacheBytes int
	// SegmentCacheBytes bounds the cache of segments' decoded blocks; their readers' fences and
	// filters take up to a quarter of it again.
	SegmentCacheBytes int
	// SegmentBatch bounds the tail's records of each kind one compaction moves into a segment —
	// its one transaction records the segments and deletes all it moved, and Spanner holds a
	// transaction to about 100,000 rows deleted — and SegmentMin is the fewest pinned rows a
	// compaction waits for before it runs (the daily collection moves however few there are).
	SegmentBatch, SegmentMin int
	// DeleteBatch bounds the rows one transaction of a deletion deletes: a repository's, a deleted
	// repository's content index's, and a network's go a transaction of at most this many at a
	// time before the one that deletes their rows.
	DeleteBatch int
	// LandingPaths bounds the rows of main's path history a landing's transaction writes: a landing
	// that changed more paths writes them after it, a transaction of up to 10,000 at a time, its
	// commit's answered from its trees meanwhile (paths.go).
	LandingPaths int
	// PublishMemoryBytes bounds the memory every publish at once holds its packs in before they go
	// to the bucket, and PublishPackBytes the most one pack of a publish holds: past it, the pack
	// goes and another starts (publish.go).
	PublishMemoryBytes, PublishPackBytes int64
}

// Default cache sizes, for a Config that leaves them zero.
const (
	DefaultObjectCacheBytes     = 256 << 20
	DefaultChunkCacheBytes      = 512 << 20
	DefaultDiffCacheBytes       = 64 << 20
	DefaultDeltaCacheBytes      = 64 << 20
	DefaultCheckoutCacheBytes   = 512 << 20
	DefaultPathCommitCacheBytes = 32 << 20
	DefaultSegmentCacheBytes    = 256 << 20
	DefaultSegmentBatch         = 1 << 12
	DefaultSegmentMin           = 1 << 12
	DefaultDeleteBatch          = 10_000
	DefaultLandingPaths         = 20_000
	DefaultPublishMemoryBytes   = 512 << 20
	DefaultPublishPackBytes     = 64 << 20
)

// readLimits bound how files are read (read.go, browse_diff.go).
type readLimits struct {
	// windowBytes and windowChunks bound the chunks ReadFiles reads together: their bytes, and
	// how many one location query names.
	windowBytes  uint64
	windowChunks int
	// mergeGap is how far apart two chunks' data in a pack may lie and still be read with one
	// range GET; maxSpan bounds one GET; fetchesAtOnce bounds the GETs in flight, per call.
	mergeGap      int64
	maxSpan       int64
	fetchesAtOnce int
	// diffWindowBytes bounds the file bytes DiffFiles reads together.
	diffWindowBytes uint64
}

var defaultReadLimits = readLimits{
	windowBytes:     16 << 20,
	windowChunks:    1000,
	mergeGap:        128 << 10,
	maxSpan:         4 << 20,
	fetchesAtOnce:   8,
	diffWindowBytes: 32 << 20,
}

// MaxChunkLen is the largest chunk a publish may carry: FastCDC's maximum (the client
// chunks files over 128 KiB at a 16/64/256 KiB min/avg/max).
const MaxChunkLen = 256 << 10

// MaxFileSize is the largest file a repository holds (docs/scm.md, "What the server
// verifies on every write"). ok's jj snapshots nothing larger by default.
const MaxFileSize = 50 << 20

// Namer names a principal by stable ID: their current name, or their ID when the directory has
// none.
type Namer interface {
	Name(ctx context.Context, id string) (string, error)
}

// SetPrincipals sets the store's namer, before it serves (Config.Principals).
func (s *Store) SetPrincipals(n Namer) { s.namer = n }

// Store is the storage core over one database and one bucket.
type Store struct {
	b backend.Backend
	// publishMemory is what publishes may hold their packs in, of publishMemoryBytes; each pack of
	// one holds at most publishPackBytes.
	publishMemory                        *semaphore.Weighted
	publishMemoryBytes, publishPackBytes int64
	names                                identity.Names
	email                                string // the email domain
	namer                                Namer
	blobs                                blob.Store
	objects                              *cache[[]byte]
	chunks                               *cache[storedChunk]
	diffs                                *cache[FileDiff]
	// deltas are files' deltas against bases, compressed — empty where none pays (read.go); layers
	// checkout layers' bytes, and checkouts the checkouts of commits, by repository and commit
	// (checkouts.go). All are derived from IDs, so never stale.
	deltas    *cache[[]byte]
	layers    *cache[[]byte]
	checkouts *cache[Checkout]
	// deriving are checkouts this replica is deriving, by key: a reader of one waits for it.
	deriving singleflight
	// newestOfPaths are answers lastPathCommits read: main's history fixes each for good.
	newestOfPaths *cache[pathCommit]
	// codeIndexes are the code search indexes this replica holds (codesearch.go).
	codeIndexes *cache[*codeIndex]
	dicts       *dictionaries
	reads       readLimits
	// indexes are the content indexes this replica reads files and chunks through.
	indexes liveIndexes
	// segs are the segments' manifests and readers this replica holds (segments.go).
	segs segmentState
	// networks are repositories' networks, which never change (networks.go).
	networks sync.Map
	// dictTrainers are dictionaries' trainers, by frame ID, which never change (dictionary.go).
	dictTrainers sync.Map
	// holder names this replica's claims on keyed work it does inline (compactSegmentsNow).
	holder string
	// deletes is Config.DeleteBatch, and landingPaths Config.LandingPaths.
	deletes, landingPaths int
	// txAttempts counts transaction attempts (a test observes deadlock retries).
	txAttempts func()
	// marker makes the marks transactions ask for after they commit (markSoon); marking runs before
	// each (tests).
	marker  marker
	marking func(workMark)
	// scanningRun observes each unpinned run garbage collection scans for stale entries (tests).
	scanningRun func(backend.Segment)
	// afterChangeListRows runs between a read of CLs' rows and the read of their patchsets (tests).
	afterChangeListRows func()
	// sequenceAfter is how long after a writer leaves rows pending its sequence step looks
	// (SequenceAfter); sequencePasses observes each pass that ran, skipSequencing leaves what a
	// transaction made pending, as a writer stopped between its commit and its pass does, and
	// beforeSequenced runs as a writer begins its pass (tests).
	sequenceAfter   time.Duration
	sequencePasses  func(repo int64)
	passing         func(repo int64, enter bool) // as a pass begins and ends (tests)
	skipSequencing  bool
	beforeSequenced func()
	// indexReads counts the content index refreshes that read the database (a test observes
	// refreshes coalescing), and indexLocked each content index a transaction took the row lock of
	// in share mode (txLog.lock).
	indexReads  func()
	indexLocked func(backend.Scope)
	// settleUnpinned moves settled operations into segments without pinning what they reach, as
	// compaction did before (a test makes a repository of then).
	settleUnpinned bool
	// afterLocate runs between reading chunk locations and fetching them (a test
	// repacks there to exercise the retired-pack re-read).
	afterLocate func()
	// beforeCommit runs just before a publish's commit transaction (a test sweeps there).
	beforeCommit func()
	// dictionaryMinSamples, when set, is the fewest small files a dictionary trains on (a test
	// trains on few).
	dictionaryMinSamples int
	// secrets is ok secrets, which upload asks (secrets.go); mainSecrets whether each repository's
	// main keeps secrets, at its head; checkedSecrets the //secret trees found to hold no value;
	// secretPointers the pointers each //secret subtree holds.
	secrets        Secrets
	mainSecrets    sync.Map
	checkedSecrets checkedSecretTrees
	secretPointers pointerCache
}

// New is a Store over a database and the bucket behind blobs.
func New(b backend.Backend, blobs blob.Store, cfg Config) (*Store, error) {
	if cfg.Names.Domain() == "" {
		return nil, errors.New("the SCM's store needs the installation's names")
	}
	if cfg.EmailDomain == "" {
		cfg.EmailDomain = cfg.Names.Domain()
	}
	if cfg.ObjectCacheBytes == 0 {
		cfg.ObjectCacheBytes = DefaultObjectCacheBytes
	}
	if cfg.ChunkCacheBytes == 0 {
		cfg.ChunkCacheBytes = DefaultChunkCacheBytes
	}
	if cfg.DiffCacheBytes == 0 {
		cfg.DiffCacheBytes = DefaultDiffCacheBytes
	}
	if cfg.DeltaCacheBytes == 0 {
		cfg.DeltaCacheBytes = DefaultDeltaCacheBytes
	}
	if cfg.CheckoutCacheBytes == 0 {
		cfg.CheckoutCacheBytes = DefaultCheckoutCacheBytes
	}
	if cfg.PathCommitCacheBytes == 0 {
		cfg.PathCommitCacheBytes = DefaultPathCommitCacheBytes
	}
	if cfg.CodeIndexCacheBytes == 0 {
		cfg.CodeIndexCacheBytes = DefaultCodeIndexCacheBytes
	}
	if cfg.SegmentCacheBytes == 0 {
		cfg.SegmentCacheBytes = DefaultSegmentCacheBytes
	}
	if cfg.SegmentBatch == 0 {
		cfg.SegmentBatch = DefaultSegmentBatch
	}
	if cfg.SegmentMin == 0 {
		cfg.SegmentMin = DefaultSegmentMin
	}
	if cfg.DeleteBatch == 0 {
		cfg.DeleteBatch = DefaultDeleteBatch
	}
	if cfg.LandingPaths == 0 {
		cfg.LandingPaths = DefaultLandingPaths
	}
	if cfg.PublishMemoryBytes == 0 {
		cfg.PublishMemoryBytes = DefaultPublishMemoryBytes
	}
	if cfg.PublishPackBytes == 0 {
		cfg.PublishPackBytes = DefaultPublishPackBytes
	}
	s := &Store{
		publishMemory:      semaphore.NewWeighted(cfg.PublishMemoryBytes),
		publishMemoryBytes: cfg.PublishMemoryBytes,
		publishPackBytes:   cfg.PublishPackBytes,
		b:                  b,
		names:              cfg.Names,
		email:              cfg.EmailDomain,
		namer:              cfg.Principals,
		blobs:              blobs,
		objects:            newCache[[]byte]("objects", cfg.ObjectCacheBytes),
		chunks:             newCache[storedChunk]("chunks", cfg.ChunkCacheBytes),
		diffs:              newCache[FileDiff]("diffs", cfg.DiffCacheBytes),
		deltas:             newCache[[]byte]("deltas", cfg.DeltaCacheBytes),
		layers:             newCache[[]byte]("checkout_layers", cfg.CheckoutCacheBytes),
		checkouts:          newCache[Checkout]("checkouts", cfg.CheckoutCacheBytes/64),
		newestOfPaths:      newCache[pathCommit]("path_commits", cfg.PathCommitCacheBytes),
		codeIndexes:        newCache[*codeIndex]("code_indexes", cfg.CodeIndexCacheBytes),
		reads:              defaultReadLimits,
		holder:             work.NewHolder(),
		deletes:            cfg.DeleteBatch,
		landingPaths:       cfg.LandingPaths,
		segs: segmentState{
			readers: newCache[*segment.Reader]("segment_readers", cfg.SegmentCacheBytes/4),
			blocks:  newCache[*segment.Block]("segment_blocks", cfg.SegmentCacheBytes),
			batch:   cfg.SegmentBatch,
			min:     cfg.SegmentMin,
		},
	}
	s.sequenceAfter = SequenceAfter
	s.dicts = newDictionaries(s)
	return s, nil
}

// Names are the installation's names.
func (s *Store) Names() identity.Names { return s.names }

// EmailDomain is what a person's handle is an address at.
func (s *Store) EmailDomain() string { return s.email }

// txn is one attempt at a read-write transaction: the database's operations, bound to it, and
// what it changed in a repository's content index (cindex.go), written as it commits.
type txn struct {
	backend.Ops
	logs  []*txLog    // the content indexes it changes: a repository's and its network's at most
	done  atomic.Bool // the transaction has ended
	marks []workMark  // the keyed work to mark once it commits (markAfter)
	// after: its backend sequences after commit (sequence.go). tie orders the rows it leaves pending
	// among those of a commit timestamp's, places counts them, sequence are their repositories, and
	// appended the events among them, as its callers hold them.
	after    bool
	tie      int64
	places   int64
	sequence map[int64]bool
	appended []appended
	// manifests are the segment manifests it read, each once (Store.manifest).
	manifests map[manifestKey][]backend.Segment
	// snapshotAt is, for a read-only snapshot (inSnapshot), when it began: it sees everything
	// committed before then, and nothing committed after its first read. Zero for a transaction.
	snapshotAt time.Time
	// exclusive are the content indexes it holds alone (lockIndex), each as the lock read it.
	exclusive map[backend.Scope]heldIndex
}

// heldIndex is a content index a transaction holds alone: its generation, and the database's day,
// as the lock read them.
type heldIndex struct {
	gen   int64
	today time.Time
}

// lockIndex holds a content index alone for the rest of the transaction (LockContentIndex): no
// writer relying on it, sweep, compaction, or fold commits beside it. The transaction's view of it
// is read under the lock (txIndex).
func (t *txn) lockIndex(ctx context.Context, sc backend.Scope) (int64, time.Time, error) {
	gen, today, err := t.LockContentIndex(ctx, sc)
	if err != nil {
		return gen, today, err
	}
	if t.exclusive == nil {
		t.exclusive = map[backend.Scope]heldIndex{}
	}
	t.exclusive[sc] = heldIndex{gen, today}
	return gen, today, nil
}

// workMark is keyed work of a kind and key.
type workMark struct {
	kind string
	key  int64
}

// marker marks keyed work after commits (markAfter), one mark of a piece of work in flight at a time
// in this replica: a mark asked while one is in flight is made once that one ends. So every commit
// is followed by a mark begun after it, the request that committed waits for none, and a busy
// repository's writes, each asking, mark its work a few times a second rather than once each — every
// mark of one is a write of one row, which its claims and its holder's renewals write too.
type marker struct {
	mu       sync.Mutex
	inFlight map[workMark]bool
	again    map[workMark]bool
	running  sync.WaitGroup
}

// markSoon marks m, or has it marked again once the mark in flight ends.
func (s *Store) markSoon(m workMark) {
	q := &s.marker
	q.mu.Lock()
	if q.inFlight == nil {
		q.inFlight, q.again = map[workMark]bool{}, map[workMark]bool{}
	}
	if q.inFlight[m] {
		q.again[m] = true
		q.mu.Unlock()
		return
	}
	q.inFlight[m] = true
	q.running.Add(1)
	q.mu.Unlock()
	go func() {
		defer q.running.Done()
		for {
			if s.marking != nil {
				s.marking(m)
			}
			if err := s.Work().Mark(context.Background(), m.kind, m.key); err != nil {
				slog.Warn("scm: marking work", slog.String("kind", m.kind), slog.Int64("key", m.key), slog.Any("err", err))
			}
			q.mu.Lock()
			if !q.again[m] {
				delete(q.inFlight, m)
				q.mu.Unlock()
				return
			}
			delete(q.again, m)
			q.mu.Unlock()
		}
	}()
}

// marksMade waits for the marks in flight to be made (tests, and Close).
func (s *Store) marksMade() { s.marker.running.Wait() }

// markAfter marks keyed work once the transaction commits, not in it: marked in it, the work's row
// is held by every transaction marking it until each commits — a repository's every upload and
// landing marks its compaction (pin) — and the work's own claims of it, writing the row, wait on
// them all (on Spanner, abort them). A mark lost to a replica stopping between the commit and the
// mark is work done later: compaction's next mark, or the repository's daily pass, which compacts.
func (t *txn) markAfter(kind string, key int64) {
	for _, m := range t.marks {
		if m.kind == kind && m.key == key {
			return
		}
	}
	t.marks = append(t.marks, workMark{kind, key})
}

// txKey is the context key a transaction's context holds it under.
type txKey struct{}

// ops is what the store reads through in ctx: the transaction ctx is within, while it lasts —
// whose connection it holds already — or the pool. A read that asked the pool for a connection
// while its transaction held one could, with every connection held so, wait forever.
func (s *Store) ops(ctx context.Context) backend.Ops {
	if tx, ok := ctx.Value(txKey{}).(*txn); ok && !tx.done.Load() {
		return tx
	}
	return s.b
}

// snapshotOf is when the read-only snapshot ctx reads through began (inSnapshot), or zero.
func snapshotOf(ctx context.Context) time.Time {
	if tx, ok := ctx.Value(txKey{}).(*txn); ok && !tx.done.Load() {
		return tx.snapshotAt
	}
	return time.Time{}
}

// inTxContext reports whether ctx is within a transaction that has not ended.
func inTxContext(ctx context.Context) bool {
	tx, ok := ctx.Value(txKey{}).(*txn)
	return ok && !tx.done.Load()
}

// snapshot runs fn over one view of the database: a read-only snapshot, or — within a
// transaction — the transaction, which holds its rows as they were when it read them under its
// locks.
func (s *Store) snapshot(ctx context.Context, fn func(backend.Ops) error) error {
	if tx, ok := ctx.Value(txKey{}).(*txn); ok && !tx.done.Load() {
		return fn(tx)
	}
	return s.b.Snapshot(ctx, fn)
}

// inSnapshot runs fn as inTx runs it, over a read-only snapshot instead: for what reads as a
// transaction does — through the store's helpers that take one — and writes nothing, a landing's
// dry run. It takes no locks, so it holds no writer back: in a Spanner read-write transaction a read
// locks what it reads, and a check of every submittable CL would hold main's head from its landing.
func (s *Store) inSnapshot(ctx context.Context, fn func(context.Context, *txn) error) error {
	began := time.Now()
	return s.b.Snapshot(ctx, func(ops backend.Ops) error {
		tx := &txn{Ops: ops, after: s.b.SequencesAfterCommit(), snapshotAt: began}
		defer tx.done.Store(true)
		return fn(context.WithValue(ctx, txKey{}, tx), tx)
	})
}

// retryable reports a transaction the database aborted to break a deadlock or a conflict, or
// one that lost a race the store detects itself (errRace); running it again is safe.
func (s *Store) retryable(err error) bool {
	return errors.Is(err, errRace) || s.b.Retryable(err)
}

// inTx runs fn in a transaction, re-running it (data.Retry) when the database aborts it as a
// deadlock victim or it loses a race the store detects. fn must have no effects outside the
// transaction. The context fn gets holds the transaction: what fn reads through the store with it
// reads through the transaction (ops).
//
// Once it commits, what it left pending is sequenced (sequence.go) before inTx returns: its CLs
// numbered and its events placed, their places filled into the events fn appended. An error then is
// the pass's, the transaction committed: the request's caller may ask again.
func (s *Store) inTx(ctx context.Context, fn func(context.Context, *txn) error) error {
	var committed *txn
	err := data.Retry(ctx, s.retryable, func() error {
		committed = nil
		err := s.b.Txn(ctx, func(ops backend.Ops) error {
			if s.txAttempts != nil {
				s.txAttempts()
			}
			tx := &txn{Ops: ops, after: s.b.SequencesAfterCommit(), tie: data.NewID()}
			committed = tx
			defer tx.done.Store(true)
			tctx := context.WithValue(ctx, txKey{}, tx)
			if err := s.gate(tctx, tx); err != nil {
				return err
			}
			if err := fn(tctx, tx); err != nil {
				return err
			}
			if err := tx.viewsCurrent(); err != nil {
				return err
			}
			if err := s.flushIndexLog(tctx, tx); err != nil {
				return err
			}
			// Should this replica stop between the commit and its pass, a job runs the pass.
			return s.pendSequence(tctx, tx)
		})
		if err == nil {
			for _, m := range committed.marks {
				s.markSoon(m)
			}
		}
		return err
	})
	if err != nil || len(committed.sequence) == 0 || s.skipSequencing {
		return err
	}
	return s.sequenced(committed)
}

// sortIDs sorts IDs bytewise, the order every multi-row write takes its row locks in, so
// concurrent transactions over overlapping rows queue rather than deadlock where they can.
func sortIDs(in []encoding.ID) {
	sort.Slice(in, func(i, j int) bool { return string(in[i][:]) < string(in[j][:]) })
}

// Healthy reports whether the database answers.
func (s *Store) Healthy(ctx context.Context) bool { return s.b.Ping(ctx) == nil }
