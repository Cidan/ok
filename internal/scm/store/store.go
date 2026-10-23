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

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
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
	// ObjectCacheBytes bounds the cache of object bodies (trees, commits, …).
	ObjectCacheBytes int
	// ChunkCacheBytes bounds the cache of file-content chunks.
	ChunkCacheBytes int
	// DiffCacheBytes bounds the cache of file pairs' diffs (DiffFiles).
	DiffCacheBytes int
}

// Default cache sizes, for a Config that leaves them zero.
const (
	DefaultObjectCacheBytes = 256 << 20
	DefaultChunkCacheBytes  = 512 << 20
	DefaultDiffCacheBytes   = 64 << 20
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

// Store is the storage core over one database and one bucket.
type Store struct {
	b       backend.Backend
	names   identity.Names
	email   string // the email domain
	blobs   blob.Store
	objects *cache[[]byte]
	chunks  *cache[storedChunk]
	diffs   *cache[FileDiff]
	dicts   *dictionaries
	reads   readLimits
	events  eventHub
	// indexes are the content indexes this replica reads files and chunks through.
	indexes liveIndexes
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
	s := &Store{
		b:       b,
		names:   cfg.Names,
		email:   cfg.EmailDomain,
		blobs:   blobs,
		objects: newCache[[]byte]("objects", cfg.ObjectCacheBytes),
		chunks:  newCache[storedChunk]("chunks", cfg.ChunkCacheBytes),
		diffs:   newCache[FileDiff]("diffs", cfg.DiffCacheBytes),
		reads:   defaultReadLimits,
	}
	s.events.ctx, s.events.stop = context.WithCancel(context.Background())
	s.events.done = make(chan struct{})
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
	log *txLog
}

// retryable reports a transaction the database aborted to break a deadlock or a conflict, or
// one that lost a race the store detects itself (errRace); running it again is safe.
func (s *Store) retryable(err error) bool {
	return errors.Is(err, errRace) || s.b.Retryable(err)
}

// inTx runs fn in a transaction, re-running it (data.Retry) when the database aborts it as a
// deadlock victim or it loses a race the store detects. fn must have no effects outside the
// transaction.
func (s *Store) inTx(ctx context.Context, fn func(*txn) error) error {
	return data.Retry(ctx, s.retryable, func() error {
		if s.txAttempts != nil {
			s.txAttempts()
		}
		var logged *loggedRow
		err := s.b.Txn(ctx, func(ops backend.Ops) error {
			tx := &txn{Ops: ops}
			if err := fn(tx); err != nil {
				return err
			}
			var err error
			logged, err = s.flushIndexLog(ctx, tx)
			return err
		})
		if err == nil && logged != nil {
			s.indexChanged(logged.repo)
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
