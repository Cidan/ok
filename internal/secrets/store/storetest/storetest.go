// Package storetest is the conformance suite of ok secrets' store backends: every backend
// (internal/secrets/store/postgres, spannerstore) passes it, through the store's rules.
package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/move/movetest"
	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/secrets/keys"
	"github.com/Cidan/ok/internal/secrets/keys/keystest"
	"github.com/Cidan/ok/internal/secrets/secretstore"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/workflow"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Run runs the suite: open makes an empty backend, each call a database of its own.
func Run(t *testing.T, open func(t *testing.T) store.Backend) {
	for _, tc := range []struct {
		name string
		fn   func(t *testing.T, e *env)
	}{
		{"WriteAndOpen", testWriteAndOpen},
		{"AtRest", testAtRest},
		{"Bound", testBound},
		{"Unseals", testUnseals},
		{"Approvals", testApprovals},
		{"RotateAndRewrap", testRotateAndRewrap},
		{"StaleReplica", testStaleReplica},
		{"RootOf", testRootOf},
		{"RootChange", testRootChange},
		{"Collect", testCollect},
		{"ExternalStore", testExternalStore},
		{"Transfer", testTransfer},
		{"Purge", testPurge},
		{"Audit", testAudit},
		{"Work", testWork},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, newEnv(t, open(t)))
		})
	}
	t.Run("Moves", func(t *testing.T) { Moves(t, open(t), open(t)) })
}

// env is a test's store over its backend, with a keyring root and an external store in memory.
type env struct {
	t     *testing.T
	ctx   context.Context
	b     store.Backend
	s     *store.Store
	cfg   store.Config
	ext   *secretstore.Memory
	other string // another root, an organization's
}

func newEnv(t *testing.T, b store.Backend) *env {
	ext := secretstore.NewMemory()
	cfg := store.Config{Root: keystest.Keyring(t, 2), Opener: &keys.Opener{}, Stores: secretstore.Stores{"mem": ext}}
	e := &env{t: t, ctx: context.Background(), b: b, cfg: cfg, ext: ext, other: keystest.Keyring(t, 1)}
	e.s = e.store(cfg)
	return e
}

// store is another replica's store over the same backend.
func (e *env) store(cfg store.Config) *store.Store {
	e.t.Helper()
	s, err := store.New(e.b, cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

const ada = "ada-id"

func (e *env) write(s *store.Store, repo int64, path, value string) secrets.Digest {
	e.t.Helper()
	d, o, err := s.Write(e.ctx, repo, path, []byte(value), ada, "")
	if err != nil {
		e.t.Fatalf("writing %s: %v", path, err)
	}
	if !secrets.Verify(d, repo, path, o, []byte(value)) {
		e.t.Fatalf("%s's digest does not commit to its value and opening", path)
	}
	return d
}

func (e *env) read(s *store.Store, repo int64, path string, d secrets.Digest) string {
	e.t.Helper()
	_, v, err := s.Open(e.ctx, repo, path, d)
	if err != nil {
		e.t.Fatalf("opening %s@%s: %v", path, d.Short(), err)
	}
	return string(v)
}

// A value written is read back as it was, with the opening it was written with; another write of
// the same value is another version, with another digest. The empty value is a value.
func testWriteAndOpen(t *testing.T, e *env) {
	d1, o1, err := e.s.Write(e.ctx, 7, "secret/prod/db-password", []byte("hunter2"), ada, "")
	if err != nil {
		t.Fatal(err)
	}
	o, v, err := e.s.Open(e.ctx, 7, "secret/prod/db-password", d1)
	if err != nil || string(v) != "hunter2" || o != o1 {
		t.Fatalf("opened %q, %v", v, err)
	}
	d2 := e.write(e.s, 7, "secret/prod/db-password", "hunter2")
	if d2 == d1 {
		t.Error("two writes of one value share a digest")
	}
	empty := e.write(e.s, 7, "secret/empty", "")
	if got := e.read(e.s, 7, "secret/empty", empty); got != "" {
		t.Errorf("the empty value read back as %q", got)
	}
	big := string(bytes.Repeat([]byte{0xa5}, secrets.MaxValue))
	if got := e.read(e.s, 7, "secret/big", e.write(e.s, 7, "secret/big", big)); got != big {
		t.Error("a value of the largest size did not read back")
	}
	if _, _, err := e.s.Write(e.ctx, 7, "secret/too-big", make([]byte, secrets.MaxValue+1), ada, ""); err == nil {
		t.Error("wrote a value past the largest")
	}
	if _, _, err := e.s.Write(e.ctx, 7, "secret/acl.yaml", []byte("x"), ada, ""); err == nil {
		t.Error("wrote a secret at an acl.yaml's path")
	}
	v1, err := e.s.Version(e.ctx, 7, "secret/prod/db-password", d1)
	if err != nil || v1.WrittenBy != ada || v1.KeyVersion != 1 || v1.WrappedKey != nil || v1.Sealed != nil || v1.Store != "default" {
		t.Errorf("the version's row: %+v, %v", v1, err)
	}
	kept, err := e.s.Kept(e.ctx, 7, []store.VersionKey{{Path: "secret/prod/db-password", Digest: d1}, {Path: "secret/other", Digest: d1}})
	if err != nil || !slices.Equal(kept, []bool{true, false}) {
		t.Errorf("kept %v, %v", kept, err)
	}
	if _, _, err := e.s.Open(e.ctx, 7, "secret/never", d1); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a version never kept: %v", err)
	}
}

// Nothing the database holds is the value, its opening, or a key: a version's row holds its value
// sealed and its data key wrapped, and a repository's key is wrapped by a root outside it.
func testAtRest(t *testing.T, e *env) {
	const value = "correct horse battery staple"
	d, o, err := e.s.Write(e.ctx, 7, "secret/token", []byte(value), ada, "")
	if err != nil {
		t.Fatal(err)
	}
	v, ok, err := e.b.Version(e.ctx, 7, "secret/token", d)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	for name, b := range map[string][]byte{"the sealed value": v.Sealed, "the wrapped data key": v.WrappedKey} {
		if len(b) == 0 || bytes.Contains(b, []byte(value)) || bytes.Contains(b, o[:]) {
			t.Errorf("%s holds the value or its opening", name)
		}
	}
	rows, err := e.b.RepositoryKeys(e.ctx, 7)
	if err != nil || len(rows) != 1 || rows[0].Root != e.cfg.Root || len(rows[0].Wrapped) == 0 {
		t.Fatalf("the repository's keys: %+v, %v", rows, err)
	}
}

// A version opens only as what it is: its sealed value or data key moved to another path's row,
// or another repository's, opens nothing.
func testBound(t *testing.T, e *env) {
	da := e.write(e.s, 7, "secret/a", "the value of a")
	e.write(e.s, 8, "secret/a", "another repository's")
	va, _, _ := e.b.Version(e.ctx, 7, "secret/a", da)
	for _, moved := range []store.Version{
		{Repository: 7, Path: "secret/b", Digest: da},
		{Repository: 8, Path: "secret/a", Digest: da},
	} {
		moved.KeyVersion, moved.WrappedKey, moved.Sealed, moved.Store, moved.WrittenBy = va.KeyVersion, va.WrappedKey, va.Sealed, va.Store, ada
		if ok, err := e.b.InsertVersion(move.WithRepository(e.ctx, moved.Repository), moved); err != nil || !ok {
			t.Fatalf("inserting a moved row: %v, %v", ok, err)
		}
		if _, _, err := e.s.Open(e.ctx, moved.Repository, moved.Path, moved.Digest); err == nil {
			t.Errorf("repository %d's %s opened a row moved from repository 7's secret/a", moved.Repository, moved.Path)
		}
	}
	if ok, err := e.b.InsertVersion(move.WithRepository(e.ctx, 7), va); err != nil || ok {
		t.Errorf("inserting a version again: %v, %v", ok, err)
	}
}

// A person reads a version only with an unseal of its path, once each: the read uses it. Unsealed
// for another path, or expired, nothing is read; a destroyed version uses no unseal.
func testUnseals(t *testing.T, e *env) {
	d := e.write(e.s, 7, "secret/prod/db", "hunter2")
	if _, _, err := e.s.OpenUnsealed(e.ctx, 7, "secret/prod/db", d, ada); !errors.Is(err, store.ErrNotUnsealed) {
		t.Fatalf("a read before unsealing: %v", err)
	}
	if granted, err := e.s.Unseal(e.ctx, 7, []string{"secret/prod/db", "secret/prod/db"}, ada, "", time.Minute); err != nil || !granted {
		t.Fatal(granted, err)
	}
	us, err := e.s.Unseals(e.ctx, 7, ada)
	if err != nil || len(us) != 2 || us[0].Path != "secret/prod/db" || us[0].Principal != ada || us[0].ExpiresAt.Before(time.Now()) {
		t.Fatalf("unseals %+v, %v", us, err)
	}
	for i := range 2 {
		if _, v, err := e.s.OpenUnsealed(e.ctx, 7, "secret/prod/db", d, ada); err != nil || string(v) != "hunter2" {
			t.Fatalf("read %d with an unseal: %q, %v", i, v, err)
		}
	}
	if _, _, err := e.s.OpenUnsealed(e.ctx, 7, "secret/prod/db", d, ada); !errors.Is(err, store.ErrNotUnsealed) {
		t.Errorf("a third read after two unseals: %v", err)
	}
	if us, err := e.s.Unseals(e.ctx, 7, ada); err != nil || len(us) != 0 {
		t.Errorf("unseals left: %+v, %v", us, err)
	}
	// Another person's unseal, another path's, another repository's: none is ada's here.
	other := e.write(e.s, 7, "secret/other", "x")
	for _, u := range []struct {
		repo      int64
		path, who string
	}{{7, "secret/prod/db", "bea-id"}, {7, "secret/other", ada}, {8, "secret/prod/db", ada}} {
		if _, err := e.s.Unseal(e.ctx, u.repo, []string{u.path}, u.who, "", time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := e.s.OpenUnsealed(e.ctx, 7, "secret/prod/db", d, ada); !errors.Is(err, store.ErrNotUnsealed) {
		t.Errorf("read with someone else's unseal, or another path's: %v", err)
	}
	if _, v, err := e.s.OpenUnsealed(e.ctx, 7, "secret/other", other, ada); err != nil || string(v) != "x" {
		t.Errorf("read of the path unsealed: %q, %v", v, err)
	}
	// Expired.
	if _, err := e.s.Unseal(e.ctx, 7, []string{"secret/prod/db"}, ada, "", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if _, _, err := e.s.OpenUnsealed(e.ctx, 7, "secret/prod/db", d, ada); !errors.Is(err, store.ErrNotUnsealed) {
		t.Errorf("read with an expired unseal: %v", err)
	}
	if res, err := e.s.Collect(e.ctx, 7, func(string, secrets.Digest) bool { return true }, time.Now().Add(48*time.Hour), store.DefaultGrace, "collect"); err != nil || res.Unseals == 0 {
		t.Errorf("collecting expired unseals: %+v, %v", res, err)
	}
	// A destroyed version uses no unseal.
	if _, err := e.s.Unseal(e.ctx, 7, []string{"secret/prod/db"}, ada, "", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.Destroy(e.ctx, 7, "secret/prod/db", d, ada); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.s.OpenUnsealed(e.ctx, 7, "secret/prod/db", d, ada); !errors.Is(err, store.ErrDestroyed) {
		t.Errorf("a read of a destroyed version: %v", err)
	}
	if us, _ := e.s.Unseals(e.ctx, 7, ada); len(us) != 1 {
		t.Errorf("a read of a destroyed version used its unseal: %+v", us)
	}
}

// Unseals for an approval are granted once, however often it is asked.
func testApprovals(t *testing.T, e *env) {
	d := e.write(e.s, 7, "secret/a", "v")
	for i, want := range []bool{true, false} {
		granted, err := e.s.Unseal(e.ctx, 7, []string{"secret/a"}, ada, "approval-1", time.Minute)
		if err != nil || granted != want {
			t.Fatalf("asking for approval-1's unseals, time %d: %v, %v", i, granted, err)
		}
	}
	if us, err := e.s.Unseals(e.ctx, 7, ada); err != nil || len(us) != 1 {
		t.Errorf("an approval asked twice granted %d unseals, %v", len(us), err)
	}
	if granted, err := e.s.Unseal(e.ctx, 8, []string{"secret/a"}, ada, "approval-1", time.Minute); err != nil || !granted {
		t.Errorf("the same approval's ID in another repository: %v, %v", granted, err)
	}
	if _, v, err := e.s.OpenUnsealed(e.ctx, 7, "secret/a", d, ada); err != nil || string(v) != "v" {
		t.Errorf("read with an approval's unseal: %q, %v", v, err)
	}
}

// A rotation's new key seals what is written after; what came before still opens under its own,
// until a rewrap wraps every data key under the newest and retires the rest.
func testRotateAndRewrap(t *testing.T, e *env) {
	before := e.write(e.s, 7, "secret/a", "before")
	destroyed := e.write(e.s, 7, "secret/gone", "destroyed")
	if _, err := e.s.Destroy(e.ctx, 7, "secret/gone", destroyed, ada); err != nil {
		t.Fatal(err)
	}
	if v, err := e.s.Rotate(e.ctx, 7); err != nil || v != 2 {
		t.Fatalf("rotating: %v, %v", v, err)
	}
	after := e.write(e.s, 7, "secret/b", "after")
	if v, _ := e.s.Version(e.ctx, 7, "secret/b", after); v.KeyVersion != 2 {
		t.Errorf("written after the rotation under key version %d", v.KeyVersion)
	}
	if v, _ := e.s.Version(e.ctx, 7, "secret/a", before); v.KeyVersion != 1 {
		t.Errorf("written before the rotation under key version %d", v.KeyVersion)
	}
	if got := e.read(e.s, 7, "secret/a", before); got != "before" {
		t.Errorf("read %q", got)
	}
	res, err := e.s.Rewrap(e.ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if res.Versions != 2 || !slices.Equal(res.Retired, []int64{1}) || len(res.Kept) != 0 {
		t.Errorf("rewrapped %+v", res)
	}
	rows, err := e.b.RepositoryKeys(e.ctx, 7)
	if err != nil || len(rows) != 1 || rows[0].Version != 2 {
		t.Fatalf("keys after the rewrap: %+v, %v", rows, err)
	}
	for _, s := range []*store.Store{e.s, e.store(e.cfg)} {
		if got := e.read(s, 7, "secret/a", before); got != "before" {
			t.Errorf("read %q after the rewrap", got)
		}
		if got := e.read(s, 7, "secret/b", after); got != "after" {
			t.Errorf("read %q after the rewrap", got)
		}
	}
	if _, _, err := e.s.Open(e.ctx, 7, "secret/gone", destroyed); !errors.Is(err, store.ErrDestroyed) {
		t.Errorf("a destroyed version after the rewrap: %v", err)
	}
	// A key version a version uses is never deleted.
	if deleted, err := e.b.DeleteRepositoryKey(move.WithRepository(e.ctx, 7), 7, 2); err != nil || deleted {
		t.Errorf("deleted a key version in use: %v, %v", deleted, err)
	}
}

// A replica holding a key version a rewrap retired writes under the newest, without losing the
// value: the database refuses a version under a key version it does not have.
func testStaleReplica(t *testing.T, e *env) {
	stale := e.store(e.cfg)
	e.write(stale, 7, "secret/a", "v1") // stale now holds key version 1 as the newest
	if _, err := e.s.Rotate(e.ctx, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.Rewrap(e.ctx, 7); err != nil {
		t.Fatal(err)
	}
	d := e.write(stale, 7, "secret/b", "written by the stale replica")
	if v, _ := e.s.Version(e.ctx, 7, "secret/b", d); v.KeyVersion != 2 {
		t.Errorf("the stale replica wrote under key version %d", v.KeyVersion)
	}
	if got := e.read(e.s, 7, "secret/b", d); got != "written by the stale replica" {
		t.Errorf("read %q", got)
	}
	// A key version another replica made since this one read them opens too.
	if _, err := e.s.Rotate(e.ctx, 7); err != nil {
		t.Fatal(err)
	}
	d3 := e.write(e.s, 7, "secret/c", "under version 3")
	if got := e.read(stale, 7, "secret/c", d3); got != "under version 3" {
		t.Errorf("read %q", got)
	}
}

// An organization's own root wraps its repositories' keys; a rewrap after it names one wraps
// the repository's key under it; and no other root opens them.
func testRootOf(t *testing.T, e *env) {
	byok := map[int64]string{}
	cfg := e.cfg
	cfg.RootOf = func(_ context.Context, repo int64) (string, error) { return byok[repo], nil }
	s := e.store(cfg)
	byok[9] = e.other
	d9 := e.write(s, 9, "secret/a", "the organization's")
	rows, _ := e.b.RepositoryKeys(e.ctx, 9)
	if len(rows) != 1 || rows[0].Root != e.other {
		t.Fatalf("repository 9's key, under %+v", rows)
	}
	d7 := e.write(s, 7, "secret/a", "the installation's")
	byok[7] = e.other
	if _, err := s.Rewrap(e.ctx, 7); err != nil {
		t.Fatal(err)
	}
	if rows, _ := e.b.RepositoryKeys(e.ctx, 7); len(rows) != 1 || rows[0].Root != e.other {
		t.Errorf("repository 7's key after its organization named a root: %+v", rows)
	}
	for repo, d := range map[int64]secrets.Digest{9: d9, 7: d7} {
		if _, _, err := e.store(e.cfg).Open(e.ctx, repo, "secret/a", d); err != nil {
			t.Errorf("repository %d, opened by a replica opening roots by their IDs: %v", repo, err)
		}
	}
	// Another keyring opens none of them.
	rows, _ = e.b.RepositoryKeys(e.ctx, 9)
	root, err := (&keys.Opener{}).Root(e.ctx, keystest.Keyring(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.Unwrap(e.ctx, rows[0].Wrapped, keys.RepositoryKeyAAD(9, 1)); err == nil {
		t.Error("another keyring unwrapped the repository's key")
	}
}

// Collection marks what nothing references, clears what is referenced again, and destroys what
// stayed unreferenced past the grace — its value gone for good, its row kept, its pointer no
// longer kept.
func testCollect(t *testing.T, e *env) {
	live := e.write(e.s, 7, "secret/live", "referenced")
	back := e.write(e.s, 7, "secret/back", "referenced again")
	gone := e.write(e.s, 7, "secret/gone", "unreferenced")
	refs := map[secrets.Digest]bool{live: true}
	referenced := func(_ string, d secrets.Digest) bool { return refs[d] }
	now := time.Now()
	res, err := e.s.Collect(e.ctx, 7, referenced, now, time.Hour, "collect")
	if err != nil || res.Marked != 2 || res.Destroyed != 0 {
		t.Fatalf("the first pass: %+v, %v", res, err)
	}
	refs[back] = true
	if res, err = e.s.Collect(e.ctx, 7, referenced, now.Add(30*time.Minute), time.Hour, "collect"); err != nil || res.Cleared != 1 || res.Destroyed != 0 {
		t.Fatalf("the second pass: %+v, %v", res, err)
	}
	if res, err = e.s.Collect(e.ctx, 7, referenced, now.Add(2*time.Hour), time.Hour, "collect"); err != nil || res.Destroyed != 1 {
		t.Fatalf("the third pass: %+v, %v", res, err)
	}
	if _, _, err := e.s.Open(e.ctx, 7, "secret/gone", gone); !errors.Is(err, store.ErrDestroyed) {
		t.Errorf("a destroyed version opened: %v", err)
	}
	v, ok, err := e.b.Version(e.ctx, 7, "secret/gone", gone)
	if err != nil || !ok || !v.Destroyed() || v.DestroyedBy != "collect" || v.Sealed != nil || v.WrappedKey != nil {
		t.Errorf("the destroyed version's row: %+v, %v, %v", v, ok, err)
	}
	if kept, _ := e.s.Kept(e.ctx, 7, []store.VersionKey{{Path: "secret/gone", Digest: gone}}); kept[0] {
		t.Error("a destroyed version is kept")
	}
	for path, d := range map[string]secrets.Digest{"secret/live": live, "secret/back": back} {
		if got := e.read(e.s, 7, path, d); got == "" {
			t.Errorf("%s did not survive", path)
		}
	}
}

// A repository whose settings name an external store keeps its sealed values there: ciphertext
// only, read back through ok secrets, deleted when its version is destroyed.
func testExternalStore(t *testing.T, e *env) {
	const value = "kept elsewhere"
	d, _, err := e.s.Write(e.ctx, 7, "secret/ext", []byte(value), ada, "mem")
	if err != nil {
		t.Fatal(err)
	}
	v, _, _ := e.b.Version(e.ctx, 7, "secret/ext", d)
	if v.Store != "mem" || v.Sealed != nil {
		t.Errorf("the version's row: store %q, sealed %d bytes", v.Store, len(v.Sealed))
	}
	held, err := e.ext.Get(e.ctx, secretstore.Key(7, "secret/ext", d))
	if err != nil || bytes.Contains(held, []byte(value)) {
		t.Fatalf("the external store holds %q, %v", held, err)
	}
	if got := e.read(e.s, 7, "secret/ext", d); got != value {
		t.Errorf("read %q", got)
	}
	if _, _, err := e.s.Write(e.ctx, 7, "secret/x", []byte("v"), ada, "unknown"); err == nil {
		t.Error("wrote to a store the installation does not configure")
	}
	if _, err := e.s.Destroy(e.ctx, 7, "secret/ext", d, ada); err != nil {
		t.Fatal(err)
	}
	if e.ext.Len() != 0 {
		t.Errorf("the external store holds %d values after its version was destroyed", e.ext.Len())
	}
}

// A purge shreds the repository's keys before anything else, then deletes its external values
// and its rows; another repository's are untouched.
func testPurge(t *testing.T, e *env) {
	d := e.write(e.s, 7, "secret/a", "purged")
	if _, _, err := e.s.Write(e.ctx, 7, "secret/ext", []byte("purged too"), ada, "mem"); err != nil {
		t.Fatal(err)
	}
	keep := e.write(e.s, 8, "secret/a", "kept")
	if _, err := e.s.Unseal(e.ctx, 7, []string{"secret/a"}, ada, "a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := e.s.Audit(e.ctx, store.AuditEntry{Repository: 7, Actor: ada, Op: "read", Path: "secret/a", Outcome: "ok"}); err != nil {
		t.Fatal(err)
	}
	// Shredding alone: nothing opens, and nothing is written.
	if err := e.b.ShredRepositoryKeys(move.WithRepository(e.ctx, 7), 7); err != nil {
		t.Fatal(err)
	}
	fresh := e.store(e.cfg)
	if _, _, err := fresh.Open(e.ctx, 7, "secret/a", d); !errors.Is(err, store.ErrPurged) {
		t.Errorf("opened a version of a shredded repository: %v", err)
	}
	if _, _, err := fresh.Write(e.ctx, 7, "secret/b", []byte("x"), ada, ""); !errors.Is(err, store.ErrPurged) {
		t.Errorf("wrote to a shredded repository: %v", err)
	}
	if err := e.s.Purge(e.ctx, 7); err != nil {
		t.Fatal(err)
	}
	if rows, _ := e.b.RepositoryKeys(e.ctx, 7); len(rows) != 0 {
		t.Errorf("keys left: %+v", rows)
	}
	if vs, _ := e.b.Versions(e.ctx, 7, store.VersionKey{}, 10); len(vs) != 0 {
		t.Errorf("versions left: %d", len(vs))
	}
	if a, _ := e.s.AuditOf(e.ctx, 7, 0, 10); len(a) != 0 {
		t.Errorf("audit left: %d", len(a))
	}
	if e.ext.Len() != 0 {
		t.Errorf("external values left: %d", e.ext.Len())
	}
	if repos, err := e.s.Repositories(e.ctx, 0, 10); err != nil || !slices.Equal(repos, []int64{8}) {
		t.Errorf("repositories after the purge: %v, %v", repos, err)
	}
	if got := e.read(e.s, 8, "secret/a", keep); got != "kept" {
		t.Errorf("another repository's read %q", got)
	}
}

// Decisions are kept in order, each with what it was about.
func testAudit(t *testing.T, e *env) {
	for i, op := range []string{"write", "unseal", "read"} {
		if err := e.s.Audit(e.ctx, store.AuditEntry{Repository: 7, Actor: ada, For: "", Op: op, Path: "secret/a", Digest: fmt.Sprint(i), Outcome: "ok"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := e.s.Audit(e.ctx, store.AuditEntry{Repository: 8, Actor: ada, Op: "read", Path: "secret/a", Outcome: "not a reader"}); err != nil {
		t.Fatal(err)
	}
	got, err := e.s.AuditOf(e.ctx, 7, 0, 10)
	if err != nil || len(got) != 3 {
		t.Fatalf("audit %+v, %v", got, err)
	}
	for i, op := range []string{"write", "unseal", "read"} {
		if got[i].Op != op || got[i].Actor != ada || got[i].Digest != fmt.Sprint(i) || got[i].At.IsZero() {
			t.Errorf("entry %d: %+v", i, got[i])
		}
	}
	if rest, err := e.s.AuditOf(e.ctx, 7, got[0].ID, 10); err != nil || len(rest) != 2 || rest[0].Op != "unseal" {
		t.Errorf("after the first: %+v, %v", rest, err)
	}
}

// The store's keyed work is marked, taken, and released.
func testWork(t *testing.T, e *env) {
	w := e.s.Work()
	if err := w.Mark(e.ctx, store.CollectWork, 7); err != nil {
		t.Fatal(err)
	}
	due, err := w.Due(e.ctx, store.CollectWork, 10)
	if err != nil || !slices.Equal(due, []int64{7}) {
		t.Fatalf("due %v, %v", due, err)
	}
	if ok, err := w.Take(e.ctx, store.CollectWork, 7, "me", time.Minute); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if again, err := w.Release(e.ctx, store.CollectWork, 7, "me"); err != nil || again {
		t.Fatal(again, err)
	}
}

// Moves moves a repository from one backend's ok secrets to another's — of either database — with
// its keys, versions, unseals, and audit as they are; the destination opens them with the same
// roots. Frozen, nothing about it is written; the source refuses it once it moved.
func Moves(t *testing.T, srcBackend, dstBackend store.Backend) {
	ctx := context.Background()
	src := newEnv(t, srcBackend)
	dst := newEnv(t, dstBackend)
	dst.cfg = src.cfg
	dst.s = dst.store(src.cfg)
	d := src.write(src.s, 7, "secret/a", "moved")
	if _, err := src.s.Rotate(ctx, 7); err != nil {
		t.Fatal(err)
	}
	d2 := src.write(src.s, 7, "secret/b", "moved under version 2")
	if _, err := src.s.Unseal(ctx, 7, []string{"secret/a"}, ada, "approval", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := src.s.Audit(ctx, store.AuditEntry{Repository: 7, Actor: ada, Op: "write", Path: "secret/a", Outcome: "ok"}); err != nil {
		t.Fatal(err)
	}
	stays := src.write(src.s, 8, "secret/a", "stays")
	scope := &okpb.MoveScope{NetworkId: 1, Repositories: []int64{7}, Role: cells.Secrets}
	srcMoves, dstMoves := movetest.Pair(t, src.s.Mover(), dst.s.Mover(), okpb.Caller_CALLER_SECRETS)
	movetest.Move(t, srcMoves, dstMoves, scope, movetest.Steps{Frozen: func() {
		if _, _, err := src.s.Write(ctx, 7, "secret/c", []byte("x"), ada, ""); !move.IsFrozen(err) {
			t.Errorf("a write while frozen: %v", err)
		}
		if _, err := src.s.Unseal(ctx, 7, []string{"secret/a"}, ada, "", time.Minute); !move.IsFrozen(err) {
			t.Errorf("an unseal while frozen: %v", err)
		}
		if got := src.read(src.s, 7, "secret/a", d); got != "moved" {
			t.Errorf("a read while frozen: %q", got)
		}
	}})
	for path, want := range map[string]string{"secret/a": "moved", "secret/b": "moved under version 2"} {
		dg := map[string]secrets.Digest{"secret/a": d, "secret/b": d2}[path]
		if got := dst.read(dst.store(src.cfg), 7, path, dg); got != want {
			t.Errorf("%s at the destination: %q", path, got)
		}
	}
	if _, v, err := dst.s.OpenUnsealed(ctx, 7, "secret/a", d, ada); err != nil || string(v) != "moved" {
		t.Errorf("an unseal moved: %q, %v", v, err)
	}
	if granted, err := dst.s.Unseal(ctx, 7, []string{"secret/a"}, ada, "approval", time.Minute); err != nil || granted {
		t.Errorf("the approval granted again at the destination: %v, %v", granted, err)
	}
	if a, _ := dst.s.AuditOf(ctx, 7, 0, 10); len(a) != 1 {
		t.Errorf("audit at the destination: %+v", a)
	}
	if dn := dst.write(dst.s, 7, "secret/c", "written there"); dst.read(dst.s, 7, "secret/c", dn) != "written there" {
		t.Error("a write at the destination did not read back")
	}
	if _, _, err := src.s.Write(ctx, 7, "secret/c", []byte("x"), ada, ""); err == nil {
		t.Error("the source wrote about a repository that moved")
	}
	if got := src.read(src.s, 8, "secret/a", stays); got != "stays" {
		t.Errorf("a repository that stayed: %q", got)
	}
}

// When a repository's root changes, what is written from then on is sealed under a fresh key the
// new root wraps, and FollowRoot brings the rest under it, retiring every key the old root wraps;
// every version still opens, on any replica; and changing back does the same.
func testRootChange(t *testing.T, e *env) {
	byok := map[int64]string{}
	cfg := e.cfg
	cfg.RootOf = func(_ context.Context, repo int64) (string, error) { return byok[repo], nil }
	cfg.RootCache = time.Nanosecond
	s := e.store(cfg)
	roots := func() []string {
		t.Helper()
		rows, err := e.b.RepositoryKeys(e.ctx, 7)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, r := range rows {
			out = append(out, fmt.Sprintf("%d %s", r.Version, r.Root))
		}
		return out
	}
	d1 := e.write(s, 7, "secret/a", "under the installation's")
	byok[7] = e.other
	d2 := e.write(s, 7, "secret/b", "under the organization's")
	if got, want := roots(), []string{"1 " + e.cfg.Root, "2 " + e.other}; !slices.Equal(got, want) {
		t.Fatalf("keys after the root changed: %v, want %v", got, want)
	}
	if moved, err := s.FollowRoot(e.ctx, 7); err != nil || !moved {
		t.Fatalf("following the root: %v, %v", moved, err)
	}
	if got, want := roots(), []string{"2 " + e.other}; !slices.Equal(got, want) {
		t.Errorf("keys after following it: %v, want %v", got, want)
	}
	if moved, err := s.FollowRoot(e.ctx, 7); err != nil || moved {
		t.Errorf("following it again: %v, %v", moved, err)
	}
	byok[7] = ""
	if moved, err := s.FollowRoot(e.ctx, 7); err != nil || !moved {
		t.Fatalf("following the installation's again: %v, %v", moved, err)
	}
	if got, want := roots(), []string{"3 " + e.cfg.Root}; !slices.Equal(got, want) {
		t.Errorf("keys back under the installation's: %v, want %v", got, want)
	}
	fresh := e.store(cfg)
	for path, d := range map[string]secrets.Digest{"secret/a": d1, "secret/b": d2} {
		if got := e.read(fresh, 7, path, d); got == "" {
			t.Errorf("%s did not open", path)
		}
	}
}

// A repository's versions transfer between stores: from ok secrets' own database into an external
// store, its rows then holding no sealed value, and back; every one opens throughout; a destroyed
// one stays where it was; a second transfer moves nothing; and a store the installation does not
// configure is refused.
func testTransfer(t *testing.T, e *env) {
	other := secretstore.NewMemory()
	cfg := e.cfg
	cfg.Stores = secretstore.Stores{"mem": e.ext, "other": other}
	s := e.store(cfg)
	d1 := e.write(s, 7, "secret/a", "in the database")
	d2, _, err := s.Write(e.ctx, 7, "secret/b", []byte("in mem"), ada, "mem")
	if err != nil {
		t.Fatal(err)
	}
	d3 := e.write(s, 7, "secret/c", "destroyed")
	if _, err := s.Destroy(e.ctx, 7, "secret/c", d3, ada); err != nil {
		t.Fatal(err)
	}
	e.write(s, 8, "secret/a", "another repository's")
	where := func(path string, d secrets.Digest) store.Version {
		t.Helper()
		v, ok, err := e.b.Version(e.ctx, 7, path, d)
		if err != nil || !ok {
			t.Fatalf("%s: %v, %v", path, ok, err)
		}
		return v
	}
	if n, err := s.Transfer(e.ctx, 7, "other"); err != nil || n != 2 {
		t.Fatalf("into other: %d, %v", n, err)
	}
	for path, d := range map[string]secrets.Digest{"secret/a": d1, "secret/b": d2} {
		if v := where(path, d); v.Store != "other" || v.Sealed != nil {
			t.Errorf("%s is in %s, sealed %d bytes", path, v.Store, len(v.Sealed))
		}
	}
	if other.Len() != 2 || e.ext.Len() != 0 {
		t.Errorf("other holds %d, mem %d", other.Len(), e.ext.Len())
	}
	if v := where("secret/c", d3); v.Store != workflow.DefaultSecretStore || !v.Destroyed() {
		t.Errorf("the destroyed version: %+v", v)
	}
	if e.read(s, 7, "secret/a", d1) != "in the database" || e.read(s, 7, "secret/b", d2) != "in mem" {
		t.Error("a transferred version read otherwise")
	}
	if n, err := s.Transfer(e.ctx, 7, "other"); err != nil || n != 0 {
		t.Errorf("again: %d, %v", n, err)
	}
	if n, err := s.Transfer(e.ctx, 7, ""); err != nil || n != 2 {
		t.Fatalf("back into the database: %d, %v", n, err)
	}
	if v := where("secret/b", d2); v.Store != workflow.DefaultSecretStore || len(v.Sealed) == 0 || other.Len() != 0 {
		t.Errorf("back: %s, %d bytes; other holds %d", v.Store, len(v.Sealed), other.Len())
	}
	if e.read(s, 7, "secret/b", d2) != "in mem" {
		t.Error("a version transferred back read otherwise")
	}
	if _, err := s.Transfer(e.ctx, 7, "vault"); !errors.Is(err, secretstore.ErrNoStore) {
		t.Errorf("a store not configured: %v", err)
	}
}
