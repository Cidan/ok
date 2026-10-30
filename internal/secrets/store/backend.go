package store

import (
	"context"
	"errors"
	"time"

	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/work"
)

// Backend is ok secrets' database: what each database it runs on implements (postgres,
// spannerstore), and the conformance suite in storetest checks. Its operations are the database's
// steps; the rules they make up — keys, sealing, unseals, collection — are Store's, written once.
type Backend interface {
	Ops
	// Txn runs fn in one read-write transaction, again if the database aborts it; fn has no
	// effects outside the transaction. A transaction about a repository (move.WithRepository)
	// reads its move gate first, and one under a lease checks its fence (data.Fence).
	Txn(ctx context.Context, fn func(tx Ops) error) error
	// Work is its keyed work's store (internal/work).
	Work() work.Store
	// Moves are its rows about repositories, for a move between cells.
	Moves() move.Rows
}

// ErrNoKey is a version inserted under a repository key version the repository does not have:
// deleted by a rewrap since the caller read it.
var ErrNoKey = errors.New("store: no such repository key version")

// Ops are the database's operations. Outside a transaction each is its own; inside Txn they are
// the transaction's.
type Ops interface {
	// ─── Repository keys ───

	// RepositoryKeys are a repository's keys, oldest version first.
	RepositoryKeys(ctx context.Context, repo int64) ([]RepositoryKey, error)
	// InsertRepositoryKey records a key version; false when the repository has that version.
	InsertRepositoryKey(ctx context.Context, k RepositoryKey) (bool, error)
	// MarkWorkAt marks ok secrets' keyed work (internal/work), due at at, in the operations'
	// transaction, which enqueues its job before it commits, or in one of its own.
	MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error
	// UpdateRepositoryKey records a key version wrapped again, by a root.
	UpdateRepositoryKey(ctx context.Context, repo, version int64, root string, wrapped []byte) error
	// ShredRepositoryKeys forgets every wrapped key of a repository, keeping the rows: what a
	// purge does first, so nothing it leaves opens.
	ShredRepositoryKeys(ctx context.Context, repo int64) error
	// DeleteRepositoryKey deletes a key version no version uses; false while one does.
	DeleteRepositoryKey(ctx context.Context, repo, version int64) (bool, error)

	// ─── Versions ───

	// InsertVersion records a version; false when the repository has one at its path and digest,
	// ErrNoKey when its key version is not the repository's.
	InsertVersion(ctx context.Context, v Version) (bool, error)
	// Version is one version, destroyed or not.
	Version(ctx context.Context, repo int64, path string, digest secrets.Digest) (Version, bool, error)
	// Versions are up to limit of a repository's versions after a path and digest (the zero
	// VersionKey: from the first), in their order.
	Versions(ctx context.Context, repo int64, after VersionKey, limit int) ([]Version, error)
	// SetUnreferenced records when a version was first found unreferenced; nil, that it is
	// referenced again.
	SetUnreferenced(ctx context.Context, repo int64, path string, digest secrets.Digest, at *time.Time) error
	// DestroyVersion forgets a version's wrapped data key and sealed value, recording who
	// destroyed it and when; false when it was destroyed already.
	DestroyVersion(ctx context.Context, repo int64, path string, digest secrets.Digest, by string) (bool, error)
	// RewrapVersion records a version's data key wrapped again, under another key version;
	// ErrNoKey when that version is not the repository's.
	RewrapVersion(ctx context.Context, repo int64, path string, digest secrets.Digest, keyVersion int64, wrapped []byte) error
	// MoveVersion records a version's sealed value in another store — to, holding sealed when to
	// is ok secrets' own database, nil otherwise — while it is in from and not destroyed; false
	// when it is not.
	MoveVersion(ctx context.Context, repo int64, path string, digest secrets.Digest, from, to string, sealed []byte) (bool, error)

	// ─── Unseals ───

	// InsertUnseals records unseals, each good until ttl from now by the database's clock.
	InsertUnseals(ctx context.Context, us []Unseal, ttl time.Duration) error
	// UseUnseal marks used one of a person's unsealings of a path that is unused and unexpired;
	// false when there is none.
	UseUnseal(ctx context.Context, repo int64, path, principal string) (bool, error)
	// Unseals are a person's unseals in a repository that are unused and unexpired.
	Unseals(ctx context.Context, repo int64, principal string) ([]Unseal, error)
	// GrantApproval records that an approval's unseals were granted; false when they were.
	GrantApproval(ctx context.Context, repo int64, approval string) (bool, error)
	// DeleteUnseals deletes up to limit of a repository's unseals that expired before a time,
	// and returns how many.
	DeleteUnseals(ctx context.Context, repo int64, before time.Time, limit int) (int64, error)

	// ─── Audit ───

	// InsertAudit records a decision.
	InsertAudit(ctx context.Context, e AuditEntry) error
	// Audit is up to limit of a repository's decisions after an ID (0: from the first), oldest
	// first.
	Audit(ctx context.Context, repo, after int64, limit int) ([]AuditEntry, error)

	// ─── Repositories ───

	// Repositories are up to limit of the repositories with keys, after an ID, in order.
	Repositories(ctx context.Context, after int64, limit int) ([]int64, error)
	// DropRepository deletes up to limit of a repository's rows — versions first, then its
	// unseals, approvals, audit, and keys — and returns how many.
	DropRepository(ctx context.Context, repo int64, limit int) (int64, error)
}

// RepositoryKey is one version of a repository's key, as its root wrapped it.
type RepositoryKey struct {
	Repository int64
	Version    int64
	// Root names the root that wrapped it (keys.Opener).
	Root string
	// Wrapped is the key, wrapped by Root for its repository and version
	// (keys.RepositoryKeyAAD); nil once shredded.
	Wrapped   []byte
	CreatedAt time.Time
}

// VersionKey is where a version is in its repository's order.
type VersionKey struct {
	Path   string
	Digest secrets.Digest
}

// Version is one version of a secret: its value sealed under a data key of its own, the data key
// wrapped by its repository's key.
type Version struct {
	Repository int64
	Path       string
	Digest     secrets.Digest
	// KeyVersion is the repository key version that wrapped WrappedKey.
	KeyVersion int64
	// WrappedKey is its data key, wrapped (keys.DataKeyAAD); nil once destroyed.
	WrappedKey []byte
	// Sealed is its opening and value, sealed by the data key (keys.ValueAAD), when Store is
	// ok secrets' own database; nil in an external store, or once destroyed.
	Sealed []byte
	// Store is the store the sealed value is in: "default", or an external store's name.
	Store string
	// WrittenBy is the principal ID that wrote it.
	WrittenBy string
	WrittenAt time.Time
	// UnreferencedAt is when collection first found nothing referencing it; nil while something
	// does.
	UnreferencedAt *time.Time
	// DestroyedAt, and DestroyedBy, when it was destroyed.
	DestroyedAt *time.Time
	DestroyedBy string
}

// Destroyed reports whether the version was destroyed.
func (v Version) Destroyed() bool { return v.DestroyedAt != nil }

// Unseal lets a person read a path once, until it expires.
type Unseal struct {
	Repository int64
	ID         int64
	Path       string
	Principal  string // the person's stable ID
	ExpiresAt  time.Time
	UsedAt     *time.Time
}

// AuditEntry is one decision of ok secrets'.
type AuditEntry struct {
	Repository int64
	ID         int64 // data.NewID: ordered by time
	At         time.Time
	// Actor is the caller's principal ID; For, who it acted for: a person a role acted for, a
	// deploy's or a post job's identity, or "".
	Actor string
	For   string
	// Op is what was asked: write, read, unseal, deploy-read, run-read, check, destroy, rotate,
	// rewrap, purge.
	Op   string
	Path string
	// Digest is the version's digest's first 12 hex digits, or "".
	Digest string
	// Outcome is "ok", or why not.
	Outcome string
}
