package worker

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/checkout"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// fakeCheckout is a checkout at a directory that records whether it was closed.
type fakeCheckout struct {
	dir    string
	closed *[]string
}

func (f fakeCheckout) Dir() string { return f.dir }
func (f fakeCheckout) Sync(context.Context, int64, []byte, map[string][]byte) (checkout.Stats, error) {
	return checkout.Stats{}, nil
}
func (f fakeCheckout) Close() error { *f.closed = append(*f.closed, f.dir); return nil }

// A run's trust domain is its repository's pre or post; a copy is reused by its domain alone, the
// warmest first; a second run of a domain while the first runs gets a copy of its own; a worker
// started again takes up what it left; and past its most, the least recently used idle copies go,
// their checkouts closed, a busy one never.
func TestDomains(t *testing.T) {
	for phase, want := range map[automationpb.Origin_Phase]string{
		automationpb.Origin_PHASE_PRE: "7-pre", automationpb.Origin_PHASE_MANUAL: "7-pre",
		automationpb.Origin_PHASE_POST: "7-post", automationpb.Origin_PHASE_DEPLOY: "7-post",
	} {
		if got := domainOf(&automationpb.Run{RepositoryId: 7, Origin: &automationpb.Origin{Phase: phase}}); got != want {
			t.Errorf("a %s run's domain: %s, want %s", phase, got, want)
		}
	}
	root := t.TempDir()
	var closed []string
	newCheckout := func(dir string) (checkout.Checkout, error) {
		return fakeCheckout{dir: dir, closed: &closed}, os.MkdirAll(dir, 0o755)
	}
	d, err := newDomains(root, newCheckout, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	take := func(key string) *domainCopy {
		t.Helper()
		c, err := d.take(key)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	a := take("7-pre")
	b := take("7-pre") // while a runs
	if a.dir == b.dir {
		t.Fatal("two runs of a domain at once share a copy")
	}
	if err := os.WriteFile(filepath.Join(a.outputBase(), "warm"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := d.put(b); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := d.put(a); err != nil {
		t.Fatal(err)
	}
	if again := take("7-pre"); again.dir != a.dir {
		t.Errorf("the domain's next run took %s, want the warmest, %s", again.dir, a.dir)
	} else if err := d.put(again); err != nil {
		t.Fatal(err)
	}
	post := take("7-post")
	if post.dir == a.dir || post.dir == b.dir || filepath.Dir(post.dir) == filepath.Dir(a.dir) {
		t.Errorf("post took pre's copy: %s", post.dir)
	}

	// Started again, the worker takes up its copies.
	d2, err := newDomains(root, newCheckout, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(d2.copies) != 3 {
		t.Fatalf("copies taken up: %d, want 3", len(d2.copies))
	}

	// A fourth copy, of another repository: past the most, the least recently used idle copy
	// goes — b, not post, which runs.
	other := take("8-pre")
	if err := d.put(other); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(b.dir); !os.IsNotExist(err) {
		t.Errorf("the least recently used copy %s is still there: %v", b.dir, err)
	}
	if _, err := os.Stat(post.dir); err != nil {
		t.Errorf("a busy copy was evicted: %v", err)
	}
	if !slices.Contains(closed, filepath.Join(b.view(), "checkout")) || !slices.Contains(closed, filepath.Join(b.view(), "base")) {
		t.Errorf("the evicted copy's checkouts were not closed: %v", closed)
	}
	if _, err := os.Stat(filepath.Join(a.outputBase(), "warm")); err != nil {
		t.Errorf("the warm copy was evicted: %v", err)
	}
}

// removeTree removes what Bazel leaves read-only.
func TestRemoveTree(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	ro := filepath.Join(dir, "a", "b")
	if err := os.MkdirAll(ro, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ro, "f"), []byte("x"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(ro, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Dir(ro), 0o555); err != nil {
		t.Fatal(err)
	}
	if err := removeTree(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("%s is still there: %v", dir, err)
	}
}
