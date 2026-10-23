// Package store is ok secrets' store (docs/secrets.md, "ok secrets"): every version of every
// secret, sealed under a data key of its own, wrapped by its repository's key, wrapped by a root
// that never leaves its key management service; the unseals that let a person read a path once;
// and the audit of every decision. Its rules are written once here, over a Backend each database
// implements.
package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/secrets/keys"
	"github.com/Cidan/ok/internal/secrets/secretstore"
	"github.com/Cidan/ok/internal/work"
	"github.com/Cidan/ok/internal/workflow"
)

var (
	// ErrNotFound is a version ok secrets never kept.
	ErrNotFound = errors.New("ok secrets keeps no such version")
	// ErrDestroyed is a version ok secrets destroyed.
	ErrDestroyed = errors.New("ok secrets destroyed this version")
	// ErrNotUnsealed is a person's read of a path they hold no unused unseal of.
	ErrNotUnsealed = errors.New("no unused unseal of this path: unseal it first (jj secret unseal)")
	// ErrPurged is a write to a repository being purged: its keys are gone.
	ErrPurged = errors.New("the repository's secrets are being purged")
)

// DefaultKeyCache is how long an unwrapped repository key is kept in memory.
const DefaultKeyCache = 10 * time.Minute

// CollectWork is each repository's daily collection of the versions nothing references
// (docs/secrets.md, "The SCM", References).
const CollectWork = "collect"

// Config is how a store keeps keys and values.
type Config struct {
	// Root is the installation's root, by its ID (keys.Opener): it wraps new repositories' keys,
	// unless RootOf names another.
	Root string
	// RootOf, when set, is the root an organization's own setting names for a repository's keys:
	// "" for none, the installation's then.
	RootOf func(ctx context.Context, repo int64) (string, error)
	// Opener opens the roots repository keys name.
	Opener *keys.Opener
	// Stores are the external stores repositories' settings may name.
	Stores secretstore.Stores
	// KeyCache is how long an unwrapped repository key is kept in memory: DefaultKeyCache unless
	// set.
	KeyCache time.Duration
	// RootCache is how long the root a repository's new keys are wrapped by is kept, once RootOf
	// answered: DefaultRootCache unless set.
	RootCache time.Duration
}

// DefaultRootCache is how long a repository's root is kept once its organization's setting was
// read: an organization's change takes effect within it.
const DefaultRootCache = time.Minute

// Store is ok secrets' store.
type Store struct {
	b   Backend
	cfg Config

	mu    sync.Mutex
	cache map[int64]*repoKeys
	roots map[int64]cachedRoot
}

// cachedRoot is a repository's root's ID, until a time.
type cachedRoot struct {
	id    string
	until time.Time
}

// repoKeys are what this replica holds of a repository's keys, until a time.
type repoKeys struct {
	until time.Time
	rows  []RepositoryKey
	keys  map[int64]keys.Key // unwrapped, by version
}

// New is a store over a backend.
func New(b Backend, cfg Config) (*Store, error) {
	if cfg.Opener == nil {
		cfg.Opener = &keys.Opener{}
	}
	if cfg.KeyCache <= 0 {
		cfg.KeyCache = DefaultKeyCache
	}
	if cfg.RootCache <= 0 {
		cfg.RootCache = DefaultRootCache
	}
	return &Store{b: b, cfg: cfg, cache: map[int64]*repoKeys{}, roots: map[int64]cachedRoot{}}, nil
}

// Work is the store's keyed work's store (internal/work).
func (s *Store) Work() work.Store { return s.b.Work() }

// ─── Writing and reading ───

// Write keeps a value as a new version at a path, written by writer (a principal ID), its sealed
// value in the store a name names (the default: ok secrets' own database). It draws the version's
// opening and data key, commits to the value at the path, seals the opening and value under the
// data key, and wraps the data key under the repository's current key — making its first, under
// its root, if it has none. It returns the version's digest and opening.
func (s *Store) Write(ctx context.Context, repo int64, path string, value []byte, writer, storeName string) (secrets.Digest, secrets.Opening, error) {
	var digest secrets.Digest
	var opening secrets.Opening
	if !secrets.ValidPath(path) {
		return digest, opening, fmt.Errorf("%q is not a secret's path", path)
	}
	if len(value) > secrets.MaxValue {
		return digest, opening, fmt.Errorf("a value of %d bytes: a secret holds at most %d", len(value), secrets.MaxValue)
	}
	ext, err := s.cfg.Stores.For(storeName)
	if err != nil {
		return digest, opening, err
	}
	if storeName == "" {
		storeName = workflow.DefaultSecretStore
	}
	if opening, err = secrets.NewOpening(); err != nil {
		return digest, opening, err
	}
	digest = secrets.Commit(repo, path, opening, value)
	plain := slices.Concat(opening[:], value)
	defer clear(plain)
	dk, err := keys.NewKey()
	if err != nil {
		return digest, opening, err
	}
	defer dk.Clear()
	sealed, err := keys.Seal(dk, plain, keys.ValueAAD(repo, path, digest))
	if err != nil {
		return digest, opening, err
	}
	ctx = move.WithRepository(ctx, repo)
	v := Version{Repository: repo, Path: path, Digest: digest, Store: storeName, WrittenBy: writer}
	if ext != nil {
		// Kept before its row: a value whose row never comes is ciphertext under a data key no
		// one kept, which opens with nothing.
		if err := ext.Put(ctx, secretstore.Key(repo, path, digest), sealed); err != nil {
			return digest, opening, fmt.Errorf("keeping the value in the %s store: %w", storeName, err)
		}
	} else {
		v.Sealed = sealed
	}
	for attempt := 0; ; attempt++ {
		ver, rk, err := s.currentKey(ctx, repo)
		if err != nil {
			return digest, opening, err
		}
		if v.WrappedKey, err = keys.Seal(rk, dk[:], keys.DataKeyAAD(repo, path, digest, ver)); err != nil {
			return digest, opening, err
		}
		v.KeyVersion = ver
		inserted, err := s.b.InsertVersion(ctx, v)
		if errors.Is(err, ErrNoKey) && attempt == 0 {
			// The key version this replica held was retired since (Rewrap): read them again.
			s.forget(repo)
			continue
		}
		if err != nil {
			return digest, opening, err
		}
		if !inserted {
			return digest, opening, fmt.Errorf("%s already has a version with digest %s", path, digest.Short())
		}
		return digest, opening, nil
	}
}

// Version is a version's row: who wrote it, and whether it was destroyed; never its value.
func (s *Store) Version(ctx context.Context, repo int64, path string, digest secrets.Digest) (Version, error) {
	v, ok, err := s.b.Version(ctx, repo, path, digest)
	if err != nil {
		return Version{}, err
	}
	if !ok {
		return Version{}, ErrNotFound
	}
	v.WrappedKey, v.Sealed = nil, nil
	return v, nil
}

// Kept reports, of each version, whether ok secrets keeps it: recorded at its path, and not
// destroyed.
func (s *Store) Kept(ctx context.Context, repo int64, versions []VersionKey) ([]bool, error) {
	out := make([]bool, len(versions))
	for i, k := range versions {
		v, ok, err := s.b.Version(ctx, repo, k.Path, k.Digest)
		if err != nil {
			return nil, err
		}
		out[i] = ok && !v.Destroyed()
	}
	return out, nil
}

// Open reads a version's opening and value, checked against its digest. Who may is the caller's
// to decide: a person's read is OpenUnsealed.
func (s *Store) Open(ctx context.Context, repo int64, path string, digest secrets.Digest) (secrets.Opening, []byte, error) {
	v, ok, err := s.b.Version(ctx, repo, path, digest)
	if err != nil {
		return secrets.Opening{}, nil, err
	}
	if !ok {
		return secrets.Opening{}, nil, ErrNotFound
	}
	return s.open(ctx, v)
}

// OpenUnsealed is a person's read of a version: in the transaction that reads its row, it uses
// one of their unseals of its path, which serves this read and no other.
func (s *Store) OpenUnsealed(ctx context.Context, repo int64, path string, digest secrets.Digest, principal string) (secrets.Opening, []byte, error) {
	var v Version
	ctx = move.WithRepository(ctx, repo)
	err := s.b.Txn(ctx, func(tx Ops) error {
		var ok bool
		var err error
		if v, ok, err = tx.Version(ctx, repo, path, digest); err != nil {
			return err
		}
		switch {
		case !ok:
			return ErrNotFound
		case v.Destroyed():
			return ErrDestroyed
		}
		used, err := tx.UseUnseal(ctx, repo, path, principal)
		if err != nil {
			return err
		}
		if !used {
			return ErrNotUnsealed
		}
		return nil
	})
	if err != nil {
		return secrets.Opening{}, nil, err
	}
	return s.open(ctx, v)
}

// open unwraps a version's data key under its repository key, opens its sealed value, and checks
// the opening and value against the digest.
func (s *Store) open(ctx context.Context, v Version) (secrets.Opening, []byte, error) {
	var opening secrets.Opening
	if v.Destroyed() {
		return opening, nil, ErrDestroyed
	}
	rk, err := s.key(ctx, v.Repository, v.KeyVersion)
	if err != nil {
		return opening, nil, err
	}
	raw, err := keys.Open(rk, v.WrappedKey, keys.DataKeyAAD(v.Repository, v.Path, v.Digest, v.KeyVersion))
	if err != nil {
		return opening, nil, fmt.Errorf("%s@%s's data key: %w", v.Path, v.Digest.Short(), err)
	}
	dk, err := keys.KeyOf(raw)
	clear(raw)
	if err != nil {
		return opening, nil, err
	}
	defer dk.Clear()
	sealed := v.Sealed
	if v.Store != workflow.DefaultSecretStore {
		ext, err := s.cfg.Stores.For(v.Store)
		if err != nil {
			return opening, nil, err
		}
		if sealed, err = ext.Get(ctx, secretstore.Key(v.Repository, v.Path, v.Digest)); err != nil {
			return opening, nil, fmt.Errorf("reading the value from the %s store: %w", v.Store, err)
		}
	}
	plain, err := keys.Open(dk, sealed, keys.ValueAAD(v.Repository, v.Path, v.Digest))
	if err != nil {
		return opening, nil, fmt.Errorf("%s@%s's value: %w", v.Path, v.Digest.Short(), err)
	}
	defer clear(plain)
	if len(plain) < len(opening) {
		return opening, nil, fmt.Errorf("%s@%s's value is shorter than its opening", v.Path, v.Digest.Short())
	}
	copy(opening[:], plain)
	value := slices.Clone(plain[len(opening):])
	if !secrets.Verify(v.Digest, v.Repository, v.Path, opening, value) {
		clear(value)
		return secrets.Opening{}, nil, fmt.Errorf("%s@%s's value does not match its digest", v.Path, v.Digest.Short())
	}
	return opening, value, nil
}

// Destroy destroys a version for good: its data key and sealed value forgotten, and its external
// store's copy deleted. A version destroyed already is passed over.
func (s *Store) Destroy(ctx context.Context, repo int64, path string, digest secrets.Digest, by string) (bool, error) {
	v, ok, err := s.b.Version(ctx, repo, path, digest)
	if err != nil || !ok {
		return false, err
	}
	destroyed, err := s.b.DestroyVersion(move.WithRepository(ctx, repo), repo, path, digest, by)
	if err != nil || !destroyed {
		return false, err
	}
	// The data key is gone: whatever the external store keeps opens with nothing, so its delete
	// failing loses nothing but space.
	if v.Store != workflow.DefaultSecretStore {
		ext, err := s.cfg.Stores.For(v.Store)
		if err != nil {
			return true, err
		}
		if err := ext.Delete(ctx, []string{secretstore.Key(repo, path, digest)}); err != nil {
			return true, fmt.Errorf("deleting the value from the %s store: %w", v.Store, err)
		}
	}
	return true, nil
}

// ─── Repository keys ───

// key is a repository's key version, unwrapped by its root.
func (s *Store) key(ctx context.Context, repo, version int64) (keys.Key, error) {
	c, err := s.keys(ctx, repo)
	if err != nil {
		return keys.Key{}, err
	}
	s.mu.Lock()
	k, ok := c.keys[version]
	s.mu.Unlock()
	if ok {
		return k, nil
	}
	i := slices.IndexFunc(c.rows, func(r RepositoryKey) bool { return r.Version == version })
	if i < 0 {
		// A version made since this replica read them: read them again, once.
		s.forget(repo)
		if c, err = s.keys(ctx, repo); err != nil {
			return keys.Key{}, err
		}
		if i = slices.IndexFunc(c.rows, func(r RepositoryKey) bool { return r.Version == version }); i < 0 {
			return keys.Key{}, fmt.Errorf("repository %d has no key version %d", repo, version)
		}
	}
	row := c.rows[i]
	if row.Wrapped == nil {
		return keys.Key{}, ErrPurged
	}
	root, err := s.cfg.Opener.Root(ctx, row.Root)
	if err != nil {
		return keys.Key{}, err
	}
	raw, err := root.Unwrap(ctx, row.Wrapped, keys.RepositoryKeyAAD(repo, version))
	if err != nil {
		return keys.Key{}, fmt.Errorf("repository %d's key version %d: %w", repo, version, err)
	}
	k, err = keys.KeyOf(raw)
	clear(raw)
	if err != nil {
		return keys.Key{}, err
	}
	s.mu.Lock()
	c.keys[version] = k
	s.mu.Unlock()
	return k, nil
}

// keys are what this replica holds of a repository's keys, read again once they expire.
func (s *Store) keys(ctx context.Context, repo int64) (*repoKeys, error) {
	s.mu.Lock()
	c, ok := s.cache[repo]
	if ok && time.Now().Before(c.until) {
		s.mu.Unlock()
		return c, nil
	}
	if ok {
		forgetKeys(c)
		delete(s.cache, repo)
	}
	s.mu.Unlock()
	rows, err := s.b.RepositoryKeys(ctx, repo)
	if err != nil {
		return nil, err
	}
	c = &repoKeys{until: time.Now().Add(s.cfg.KeyCache), rows: rows, keys: map[int64]keys.Key{}}
	s.mu.Lock()
	if have, ok := s.cache[repo]; ok {
		forgetKeys(have)
	}
	s.cache[repo] = c
	s.mu.Unlock()
	return c, nil
}

// forget drops what this replica holds of a repository's keys.
func (s *Store) forget(repo int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.cache[repo]; ok {
		forgetKeys(c)
		delete(s.cache, repo)
	}
}

func forgetKeys(c *repoKeys) {
	for v, k := range c.keys {
		k.Clear()
		delete(c.keys, v)
	}
}

// currentKey is the repository's newest key version, and the key: its first, made now, when it
// has none; and a fresh one when its root changed since the newest was made — so what is written
// from then on is sealed under the root it names now, and the daily pass rewraps the rest
// (FollowRoot).
func (s *Store) currentKey(ctx context.Context, repo int64) (int64, keys.Key, error) {
	for attempt := 0; attempt < 3; attempt++ {
		c, err := s.keys(ctx, repo)
		if err != nil {
			return 0, keys.Key{}, err
		}
		next := int64(1)
		if len(c.rows) > 0 {
			newest := c.rows[len(c.rows)-1]
			if newest.Wrapped == nil {
				return 0, keys.Key{}, ErrPurged
			}
			root, err := s.root(ctx, repo)
			if err != nil {
				return 0, keys.Key{}, err
			}
			if newest.Root == root.ID() {
				k, err := s.key(ctx, repo, newest.Version)
				return newest.Version, k, err
			}
			next = newest.Version + 1
		}
		// Made by this replica or another: either way, read them again.
		if _, err := s.newKey(ctx, repo, next); err != nil {
			return 0, keys.Key{}, err
		}
		s.forget(repo)
	}
	return 0, keys.Key{}, fmt.Errorf("repository %d's key was made under its root and is not there", repo)
}

// newKey makes a repository's key version, wrapped by the repository's root; false when another
// replica made that version first.
func (s *Store) newKey(ctx context.Context, repo, version int64) (bool, error) {
	root, err := s.root(ctx, repo)
	if err != nil {
		return false, err
	}
	k, err := keys.NewKey()
	if err != nil {
		return false, err
	}
	defer k.Clear()
	wrapped, err := root.Wrap(ctx, k[:], keys.RepositoryKeyAAD(repo, version))
	if err != nil {
		return false, err
	}
	return s.b.InsertRepositoryKey(move.WithRepository(ctx, repo), RepositoryKey{Repository: repo, Version: version, Root: root.ID(), Wrapped: wrapped})
}

// root is the root that wraps a repository's new keys: its organization's own, or the
// installation's. Its organization's setting is read again once RootCache passed.
func (s *Store) root(ctx context.Context, repo int64) (keys.Root, error) {
	id := s.cfg.Root
	if s.cfg.RootOf != nil {
		s.mu.Lock()
		c, ok := s.roots[repo]
		s.mu.Unlock()
		if !ok || !time.Now().Before(c.until) {
			own, err := s.cfg.RootOf(ctx, repo)
			if err != nil {
				return nil, fmt.Errorf("repository %d's organization's root: %w", repo, err)
			}
			c = cachedRoot{id: own, until: time.Now().Add(s.cfg.RootCache)}
			s.mu.Lock()
			if len(s.roots) > 100_000 {
				clear(s.roots)
			}
			s.roots[repo] = c
			s.mu.Unlock()
		}
		if c.id != "" {
			id = c.id
		}
	}
	if id == "" {
		return nil, errors.New("no root wraps repositories' keys (secrets.root)")
	}
	return s.cfg.Opener.Root(ctx, id)
}

// forgetRoot drops what this replica holds of a repository's root, so the next use reads its
// organization's setting: for what an administrator, or the daily pass, asks of it now.
func (s *Store) forgetRoot(repo int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.roots, repo)
}

// FollowRoot brings a repository's keys under its root as it is now — its organization's own, or
// the installation's — when any key version was wrapped by another: a fresh key under the current
// root if the newest was not, then a rewrap, which retires the rest. Nothing the old root unwraps
// stays in use. It reports whether it did anything.
func (s *Store) FollowRoot(ctx context.Context, repo int64) (bool, error) {
	ctx = move.WithRepository(ctx, repo)
	rows, err := s.b.RepositoryKeys(ctx, repo)
	if err != nil || len(rows) == 0 {
		return false, err
	}
	s.forgetRoot(repo)
	root, err := s.root(ctx, repo)
	if err != nil {
		return false, err
	}
	stale := false
	for _, r := range rows {
		if r.Wrapped == nil {
			return false, ErrPurged
		}
		stale = stale || r.Root != root.ID()
	}
	if !stale {
		return false, nil
	}
	if rows[len(rows)-1].Root != root.ID() {
		if _, err := s.Rotate(ctx, repo); err != nil {
			return false, err
		}
	}
	res, err := s.Rewrap(ctx, repo)
	if err != nil {
		return false, err
	}
	if len(res.Kept) > 0 {
		return true, fmt.Errorf("repository %d's key versions %v were written under during the rewrap: the next pass retires them", repo, res.Kept)
	}
	return true, nil
}

// Rotate makes a repository's next key version, which every value written after is sealed under;
// what the older versions sealed stays theirs until Rewrap. It returns the new version.
func (s *Store) Rotate(ctx context.Context, repo int64) (int64, error) {
	defer s.forget(repo)
	s.forgetRoot(repo)
	for {
		rows, err := s.b.RepositoryKeys(ctx, repo)
		if err != nil {
			return 0, err
		}
		next := int64(1)
		if len(rows) > 0 {
			if rows[len(rows)-1].Wrapped == nil {
				return 0, ErrPurged
			}
			next = rows[len(rows)-1].Version + 1
		}
		made, err := s.newKey(ctx, repo, next)
		if err != nil {
			return 0, err
		}
		if made {
			return next, nil
		}
	}
}

// RewrapResult is what a rewrap did.
type RewrapResult struct {
	// Versions are the versions whose data keys it wrapped again, under the newest key version.
	Versions int
	// Retired are the key versions it deleted, nothing using them; Kept, those it could not, a
	// version written under one meanwhile.
	Retired, Kept []int64
}

// Rewrap wraps every data key of a repository again under its newest key version, deletes the
// older versions nothing uses then, and wraps the newest again under the repository's root as it
// is now — its organization's own, or the installation's, at its current primary.
func (s *Store) Rewrap(ctx context.Context, repo int64) (RewrapResult, error) {
	var out RewrapResult
	ctx = move.WithRepository(ctx, repo)
	defer s.forget(repo)
	s.forget(repo)
	s.forgetRoot(repo)
	rows, err := s.b.RepositoryKeys(ctx, repo)
	if err != nil || len(rows) == 0 {
		return out, err
	}
	newest := rows[len(rows)-1]
	if newest.Wrapped == nil {
		return out, ErrPurged
	}
	nk, err := s.key(ctx, repo, newest.Version)
	if err != nil {
		return out, err
	}
	var after VersionKey
	for {
		vs, err := s.b.Versions(ctx, repo, after, 500)
		if err != nil {
			return out, err
		}
		for _, v := range vs {
			if v.KeyVersion == newest.Version {
				continue
			}
			var wrapped []byte
			if !v.Destroyed() {
				old, err := s.key(ctx, repo, v.KeyVersion)
				if err != nil {
					return out, err
				}
				raw, err := keys.Open(old, v.WrappedKey, keys.DataKeyAAD(repo, v.Path, v.Digest, v.KeyVersion))
				if err != nil {
					return out, fmt.Errorf("%s@%s's data key: %w", v.Path, v.Digest.Short(), err)
				}
				wrapped, err = keys.Seal(nk, raw, keys.DataKeyAAD(repo, v.Path, v.Digest, newest.Version))
				clear(raw)
				if err != nil {
					return out, err
				}
			}
			if err := s.b.RewrapVersion(ctx, repo, v.Path, v.Digest, newest.Version, wrapped); err != nil {
				return out, err
			}
			out.Versions++
		}
		if len(vs) < 500 {
			break
		}
		after = VersionKey{Path: vs[len(vs)-1].Path, Digest: vs[len(vs)-1].Digest}
	}
	for _, r := range rows[:len(rows)-1] {
		deleted, err := s.b.DeleteRepositoryKey(ctx, repo, r.Version)
		if err != nil {
			return out, err
		}
		if deleted {
			out.Retired = append(out.Retired, r.Version)
		} else {
			out.Kept = append(out.Kept, r.Version)
		}
	}
	root, err := s.root(ctx, repo)
	if err != nil {
		return out, err
	}
	wrapped, err := root.Wrap(ctx, nk[:], keys.RepositoryKeyAAD(repo, newest.Version))
	if err != nil {
		return out, err
	}
	return out, s.b.UpdateRepositoryKey(ctx, repo, newest.Version, root.ID(), wrapped)
}

// ─── Unseals ───

// DefaultUnsealTTL is how long an unseal waits for the read it lets a person make.
const DefaultUnsealTTL = 10 * time.Minute

// Unseal lets a person read each path once, for ttl. Unseals granted for an approval (a passkey's)
// are granted once however often it is asked: granted is false when they were already.
func (s *Store) Unseal(ctx context.Context, repo int64, paths []string, principal, approval string, ttl time.Duration) (granted bool, err error) {
	if ttl <= 0 {
		ttl = DefaultUnsealTTL
	}
	us := make([]Unseal, len(paths))
	for i, p := range paths {
		if !secrets.ValidPath(p) {
			return false, fmt.Errorf("%q is not a secret's path", p)
		}
		us[i] = Unseal{Repository: repo, ID: data.NewID(), Path: p, Principal: principal}
	}
	ctx = move.WithRepository(ctx, repo)
	err = s.b.Txn(ctx, func(tx Ops) error {
		granted = true
		if approval != "" {
			ok, err := tx.GrantApproval(ctx, repo, approval)
			if err != nil {
				return err
			}
			if !ok {
				granted = false
				return nil
			}
		}
		return tx.InsertUnseals(ctx, us, ttl)
	})
	return granted, err
}

// Unseals are a person's unused, unexpired unseals in a repository.
func (s *Store) Unseals(ctx context.Context, repo int64, principal string) ([]Unseal, error) {
	return s.b.Unseals(ctx, repo, principal)
}

// ─── Audit ───

// Audit records a decision.
func (s *Store) Audit(ctx context.Context, e AuditEntry) error {
	e.ID = data.NewID()
	return s.b.InsertAudit(move.WithRepository(ctx, e.Repository), e)
}

// AuditOf is up to limit of a repository's decisions after an ID, oldest first.
func (s *Store) AuditOf(ctx context.Context, repo, after int64, limit int) ([]AuditEntry, error) {
	return s.b.Audit(ctx, repo, after, limit)
}

// ─── Collection and purging ───

// DefaultGrace is how long a version nothing references is kept before it is destroyed: a
// rotation can be rolled back for a month.
const DefaultGrace = 30 * 24 * time.Hour

// CollectResult is what a collection did.
type CollectResult struct {
	// Marked are the versions it found unreferenced, Cleared those referenced again, and
	// Destroyed those unreferenced past the grace.
	Marked, Cleared, Destroyed int
	// Unseals are the expired unseals it deleted.
	Unseals int64
}

// Collect is a repository's collection, as of now: each version nothing references is marked so;
// one marked and referenced again is cleared; one marked for grace is destroyed. Unseals expired
// for a day are deleted.
func (s *Store) Collect(ctx context.Context, repo int64, referenced func(path string, digest secrets.Digest) bool, now time.Time, grace time.Duration, by string) (CollectResult, error) {
	var out CollectResult
	ctx = move.WithRepository(ctx, repo)
	var after VersionKey
	for {
		vs, err := s.b.Versions(ctx, repo, after, 500)
		if err != nil {
			return out, err
		}
		for _, v := range vs {
			if v.Destroyed() {
				continue
			}
			ref := referenced(v.Path, v.Digest)
			switch {
			case ref && v.UnreferencedAt != nil:
				if err := s.b.SetUnreferenced(ctx, repo, v.Path, v.Digest, nil); err != nil {
					return out, err
				}
				out.Cleared++
			case ref:
			case v.UnreferencedAt == nil:
				at := now
				if err := s.b.SetUnreferenced(ctx, repo, v.Path, v.Digest, &at); err != nil {
					return out, err
				}
				out.Marked++
			case now.Sub(*v.UnreferencedAt) >= grace:
				destroyed, err := s.Destroy(ctx, repo, v.Path, v.Digest, by)
				if err != nil {
					return out, err
				}
				if destroyed {
					out.Destroyed++
				}
			}
		}
		if len(vs) < 500 {
			break
		}
		after = VersionKey{Path: vs[len(vs)-1].Path, Digest: vs[len(vs)-1].Digest}
	}
	for {
		n, err := s.b.DeleteUnseals(ctx, repo, now.Add(-24*time.Hour), 1000)
		if err != nil {
			return out, err
		}
		out.Unseals += n
		if n < 1000 {
			return out, nil
		}
	}
}

// Purge deletes a repository's secrets for good: its keys first — committed before anything
// else, so whatever survives, a backup or an external store's copy, opens with nothing — then its
// external stores' copies, then its rows.
func (s *Store) Purge(ctx context.Context, repo int64) error {
	ctx = move.WithRepository(ctx, repo)
	if err := s.b.ShredRepositoryKeys(ctx, repo); err != nil {
		return err
	}
	s.forget(repo)
	var after VersionKey
	byStore := map[string][]string{}
	for {
		vs, err := s.b.Versions(ctx, repo, after, 500)
		if err != nil {
			return err
		}
		for _, v := range vs {
			if v.Store != workflow.DefaultSecretStore && !v.Destroyed() {
				byStore[v.Store] = append(byStore[v.Store], secretstore.Key(repo, v.Path, v.Digest))
			}
		}
		if len(vs) < 500 {
			break
		}
		after = VersionKey{Path: vs[len(vs)-1].Path, Digest: vs[len(vs)-1].Digest}
	}
	for name, ks := range byStore {
		ext, err := s.cfg.Stores.For(name)
		if err != nil {
			return err
		}
		if err := ext.Delete(ctx, ks); err != nil {
			return fmt.Errorf("deleting repository %d's values from the %s store: %w", repo, name, err)
		}
	}
	for {
		n, err := s.b.DropRepository(ctx, repo, 1000)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// Repositories are up to limit of the repositories with keys, after an ID.
func (s *Store) Repositories(ctx context.Context, after int64, limit int) ([]int64, error) {
	return s.b.Repositories(ctx, after, limit)
}

// ─── Moves ───

// Mover is ok secrets' side of moves between cells (docs/hosted.md, "Cells, as built"): its rows
// about the repositories — keys, versions, unseals, audit — as they are. Roots are an
// installation's or an organization's, never a cell's, so the destination opens them alike.
func (s *Store) Mover() *move.RepositoryRole {
	return &move.RepositoryRole{RoleName: cells.Secrets, Rows: s.b.Moves(), Arrived: func(_ context.Context, repos []int64) error {
		for _, r := range repos {
			s.forget(r)
		}
		return nil
	}}
}
