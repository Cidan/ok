package testobjects

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/blob"
)

// Latency is the delay OK_TEST_OBJECT_LATENCY names (20ms, say), added to every request a test's
// store makes: what an object store across a network costs a request, which a store on the test's
// own machine hides. Zero when it is unset.
func Latency(t testing.TB) time.Duration {
	t.Helper()
	v := os.Getenv("OK_TEST_OBJECT_LATENCY")
	if v == "" {
		return 0
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		t.Fatalf("testobjects: OK_TEST_OBJECT_LATENCY=%q is not a duration", v)
	}
	return d
}

// Slow is a store each of whose requests waits a latency first, counting them by kind.
type Slow struct {
	blob.Store
	latency time.Duration

	mu    sync.Mutex
	calls map[string]int
}

// NewSlow wraps s, each request waiting latency first.
func NewSlow(s blob.Store, latency time.Duration) *Slow {
	return &Slow{Store: s, latency: latency, calls: map[string]int{}}
}

func (s *Slow) wait(ctx context.Context, kind string) error {
	s.mu.Lock()
	s.calls[kind]++
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.latency):
		return nil
	}
}

// Count is how many requests of kind (put, get-range, download, stat, delete, list, copy) the
// store has made.
func (s *Slow) Count(kind string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[kind]
}

// Requests are how many requests of each kind the store has made, as "kind n" in kind order.
func (s *Slow) Requests() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	kinds := make([]string, 0, len(s.calls))
	for k := range s.calls {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	parts := make([]string, len(kinds))
	for i, k := range kinds {
		parts[i] = fmt.Sprintf("%s %d", k, s.calls[k])
	}
	return strings.Join(parts, ", ")
}

func (s *Slow) Put(ctx context.Context, key string, r io.Reader, size int64, opts ...blob.PutOption) error {
	if err := s.wait(ctx, "put"); err != nil {
		return err
	}
	return s.Store.Put(ctx, key, r, size, opts...)
}

func (s *Slow) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := s.wait(ctx, "get-range"); err != nil {
		return nil, err
	}
	return s.Store.GetRange(ctx, key, offset, length)
}

func (s *Slow) Download(ctx context.Context, key string, w io.Writer) (int64, error) {
	if err := s.wait(ctx, "download"); err != nil {
		return 0, err
	}
	return s.Store.Download(ctx, key, w)
}

func (s *Slow) Stat(ctx context.Context, key string) (blob.Object, error) {
	if err := s.wait(ctx, "stat"); err != nil {
		return blob.Object{}, err
	}
	return s.Store.Stat(ctx, key)
}

func (s *Slow) Delete(ctx context.Context, keys ...string) error {
	if err := s.wait(ctx, "delete"); err != nil {
		return err
	}
	return s.Store.Delete(ctx, keys...)
}

func (s *Slow) List(ctx context.Context, prefix string) ([]blob.Object, error) {
	if err := s.wait(ctx, "list"); err != nil {
		return nil, err
	}
	return s.Store.List(ctx, prefix)
}

func (s *Slow) ListPage(ctx context.Context, prefix, after string, limit int) ([]blob.Object, string, error) {
	if err := s.wait(ctx, "list"); err != nil {
		return nil, "", err
	}
	return s.Store.ListPage(ctx, prefix, after, limit)
}

func (s *Slow) Stage(ctx context.Context, tmp string) (blob.Staged, error) {
	if err := s.wait(ctx, "stage"); err != nil {
		return nil, err
	}
	st, err := s.Store.Stage(ctx, tmp)
	if err != nil {
		return nil, err
	}
	return &slowStaged{Staged: st, s: s}, nil
}

// slowStaged is a staged object whose commit waits the latency, as the copy that names it would.
type slowStaged struct {
	blob.Staged
	s *Slow
}

func (st *slowStaged) Commit(ctx context.Context, key string) error {
	if err := st.s.wait(ctx, "commit"); err != nil {
		st.Staged.Abort()
		return err
	}
	return st.Staged.Commit(ctx, key)
}

func (s *Slow) Copy(ctx context.Context, src, dst string) error {
	if err := s.wait(ctx, "copy"); err != nil {
		return err
	}
	return s.Store.Copy(ctx, src, dst)
}
