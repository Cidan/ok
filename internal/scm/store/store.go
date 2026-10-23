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
	"sort"
	"sync"
	"sync/atomic"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/work"
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
	// FoldBatch bounds the blocks one transaction of a fold writes: a fold writes its index's next
	// generation a transaction at a time before the one that makes it the generation.
	FoldBatch int
	// Signals carry the store's wake-ups between replicas (docs/hosted.md, "Signals"): its events,
	// its content indexes', and its segments' changes. Nil is within the process (bus.NewLocal).
	Signals bus.Signals
}

// Default cache sizes, for a Config that leaves them zero.
const (
	DefaultObjectCacheBytes  = 256 << 20
	DefaultChunkCacheBytes   = 512 << 20
	DefaultDiffCacheBytes    = 64 << 20
	DefaultSegmentCacheBytes = 256 << 20
	DefaultSegmentBatch      = 1 << 12
	DefaultSegmentMin        = 1 << 12
	DefaultDeleteBatch       = 10_000
	DefaultLandingPaths      = 20_000
	DefaultFoldBatch         = 500
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
	b       backend.Backend
	names   identity.Names
	email   string // the email domain
	signals bus.Signals
	namer   Namer
	blobs   blob.Store
	objects *cache[[]byte]
	chunks  *cache[storedChunk]
	diffs   *cache[FileDiff]
	// codeIndexes are the code search indexes this replica holds (codesearch.go).
	codeIndexes *cache[*codeIndex]
	dicts       *dictionaries
	reads       readLimits
	events      eventHub
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
	// deletes is Config.DeleteBatch, landingPaths Config.LandingPaths, and foldBlocks
	// Config.FoldBatch.
	deletes, landingPaths, foldBlocks int
	// txAttempts counts transaction attempts (a test observes deadlock retries).
	txAttempts func()
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
	if cfg.Signals == nil {
		cfg.Signals = bus.NewLocal()
	}
	if cfg.ChunkCacheBytes == 0 {
		cfg.ChunkCacheBytes = DefaultChunkCacheBytes
	}
	if cfg.DiffCacheBytes == 0 {
		cfg.DiffCacheBytes = DefaultDiffCacheBytes
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
	if cfg.FoldBatch == 0 {
		cfg.FoldBatch = DefaultFoldBatch
	}
	s := &Store{
		b:            b,
		names:        cfg.Names,
		email:        cfg.EmailDomain,
		signals:      cfg.Signals,
		namer:        cfg.Principals,
		blobs:        blobs,
		objects:      newCache[[]byte]("objects", cfg.ObjectCacheBytes),
		chunks:       newCache[storedChunk]("chunks", cfg.ChunkCacheBytes),
		diffs:        newCache[FileDiff]("diffs", cfg.DiffCacheBytes),
		codeIndexes:  newCache[*codeIndex]("code_indexes", cfg.CodeIndexCacheBytes),
		reads:        defaultReadLimits,
		holder:       work.NewHolder(),
		deletes:      cfg.DeleteBatch,
		landingPaths: cfg.LandingPaths,
		foldBlocks:   cfg.FoldBatch,
		segs: segmentState{
			readers: newCache[*segment.Reader]("segment_readers", cfg.SegmentCacheBytes/4),
			blocks:  newCache[*segment.Block]("segment_blocks", cfg.SegmentCacheBytes),
			batch:   cfg.SegmentBatch,
			min:     cfg.SegmentMin,
		},
	}
	s.events.ctx, s.events.stop = context.WithCancel(context.Background())
	s.events.done = make(chan struct{})
	s.dicts = newDictionaries(s)
	return s, nil
}

// SetSignals sets the signals the store sends and listens on (Config.Signals), before it serves.
func (s *Store) SetSignals(sig bus.Signals) { s.signals = sig }

// Names are the installation's names.
func (s *Store) Names() identity.Names { return s.names }

// EmailDomain is what a person's handle is an address at.
func (s *Store) EmailDomain() string { return s.email }

// txn is one attempt at a read-write transaction: the database's operations, bound to it, and
// what it changed in a repository's content index (cindex.go), written as it commits.
type txn struct {
	backend.Ops
	logs    []*txLog    // the content indexes it changes: a repository's and its network's at most
	done    atomic.Bool // the transaction has ended
	pending bus.Pending // the signals to send once it commits
}

// Notify records a signal to send once the transaction commits.
func (t *txn) Notify(_ context.Context, channel, payload string) error {
	t.pending.Add(channel, payload)
	return nil
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

// retryable reports a transaction the database aborted to break a deadlock or a conflict, or
// one that lost a race the store detects itself (errRace); running it again is safe.
func (s *Store) retryable(err error) bool {
	return errors.Is(err, errRace) || s.b.Retryable(err)
}

// inTx runs fn in a transaction, re-running it (data.Retry) when the database aborts it as a
// deadlock victim or it loses a race the store detects. fn must have no effects outside the
// transaction. The context fn gets holds the transaction: what fn reads through the store with it
// reads through the transaction (ops).
func (s *Store) inTx(ctx context.Context, fn func(context.Context, *txn) error) error {
	return data.Retry(ctx, s.retryable, func() error {
		if s.txAttempts != nil {
			s.txAttempts()
		}
		var logged []backend.Scope
		var committed *txn
		err := s.b.Txn(ctx, func(ops backend.Ops) error {
			tx := &txn{Ops: ops}
			committed = tx
			defer tx.done.Store(true)
			tctx := context.WithValue(ctx, txKey{}, tx)
			if err := s.gate(tctx, tx); err != nil {
				return err
			}
			if err := fn(tctx, tx); err != nil {
				return err
			}
			var err error
			logged, err = s.flushIndexLog(tctx, tx)
			return err
		})
		if err == nil {
			for _, sc := range logged {
				s.indexChanged(sc)
			}
			committed.pending.Send(ctx, s.signals)
		}
		return err
	})
}

// sortIDs sorts IDs bytewise, the order every multi-row write takes its row locks in, so
// concurrent transactions over overlapping rows queue rather than deadlock where they can.
func sortIDs(in []encoding.ID) {
	sort.Slice(in, func(i, j int) bool { return string(in[i][:]) < string(in[j][:]) })
}

// Healthy reports whether the database answers.
func (s *Store) Healthy(ctx context.Context) bool { return s.b.Ping(ctx) == nil }
