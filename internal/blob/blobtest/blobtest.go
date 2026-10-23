// Package blobtest is the conformance suite every blob.Store passes: run it against an
// implementation with Run, giving it a fresh, empty store for each test.
package blobtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/blob"
)

// Options says what a store can do beyond what every store must.
type Options struct {
	// Signs: the store signs URLs; with Fetch, the suite GETs them with Client (http.DefaultClient
	// if nil) — a fake that does not serve signed URLs leaves Fetch off.
	Signs  bool
	Fetch  bool
	Client *http.Client
	// Classes: the store accepts every blob.Class (a test server may refuse all but Standard).
	Classes bool
	// Nested: the store holds "a" and "a/b" side by side, as S3 and GCS do (a store over a POSIX
	// file system, like versitygw's, cannot). ok's own keys never need it.
	Nested bool
}

// Run runs the suite, each test on a new, empty store that open makes.
func Run(t *testing.T, open func(t *testing.T) blob.Store, o Options) {
	tests := []struct {
		name string
		fn   func(t *testing.T, s blob.Store)
	}{
		{"PutAndRead", testPutAndRead},
		{"Missing", testMissing},
		{"Replace", testReplace},
		{"IfAbsent", testIfAbsent},
		{"IfAbsentRace", testIfAbsentRace},
		{"ShortReader", testShortReader},
		{"Delete", testDelete},
		{"List", testList},
		{"ListPage", testListPage},
		{"Copy", testCopy},
		{"Empty", testEmpty},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, open(t)) })
	}
	t.Run("Nested", func(t *testing.T) {
		if !o.Nested {
			t.Skip(`the store cannot hold "a" and "a/b" side by side`)
		}
		testNested(t, open(t))
	})
	t.Run("Classes", func(t *testing.T) {
		if !o.Classes {
			t.Skip("the store refuses classes other than Standard")
		}
		testClasses(t, open(t))
	})
	t.Run("SignGet", func(t *testing.T) {
		s := open(t)
		if !o.Signs {
			if _, err := s.SignGet(context.Background(), "k", time.Minute); !errors.Is(err, blob.ErrUnsupported) {
				t.Fatalf("SignGet on a store that cannot sign = %v, want ErrUnsupported", err)
			}
			return
		}
		if !o.Fetch {
			put(t, s, "signed/x", []byte("x"))
			u, err := s.SignGet(context.Background(), "signed/x", time.Minute)
			if err != nil || u == "" {
				t.Fatalf("SignGet = %q, %v", u, err)
			}
			return
		}
		client := o.Client
		if client == nil {
			client = http.DefaultClient
		}
		testSignGet(t, s, client)
	})
}

func put(t *testing.T, s blob.Store, key string, data []byte, opts ...blob.PutOption) {
	t.Helper()
	if err := s.Put(context.Background(), key, bytes.NewReader(data), int64(len(data)), opts...); err != nil {
		t.Fatalf("Put %s: %v", key, err)
	}
}

func get(t *testing.T, s blob.Store, key string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := s.Download(context.Background(), key, &buf); err != nil {
		t.Fatalf("Download %s: %v", key, err)
	}
	return buf.Bytes()
}

// data is n bytes no two runs of which repeat for a while, so a wrong offset shows.
func data(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

func testPutAndRead(t *testing.T, s blob.Store) {
	ctx := context.Background()
	d := data(100_000)
	put(t, s, "a/b/c", d)
	if got, err := s.GetRange(ctx, "a/b/c", 12_345, 1_000); err != nil || !bytes.Equal(got, d[12_345:13_345]) {
		t.Fatalf("GetRange = %d bytes, %v", len(got), err)
	}
	if got, err := s.GetRange(ctx, "a/b/c", 99_000, 1_000); err != nil || !bytes.Equal(got, d[99_000:]) {
		t.Fatalf("GetRange to the end = %d bytes, %v", len(got), err)
	}
	if got, err := s.GetRange(ctx, "a/b/c", 5, 0); err != nil || len(got) != 0 {
		t.Fatalf("an empty GetRange = %q, %v", got, err)
	}
	if _, err := s.GetRange(ctx, "a/b/c", 99_500, 1_000); err == nil {
		t.Fatal("a GetRange past the end succeeded")
	}
	var buf bytes.Buffer
	if n, err := s.Download(ctx, "a/b/c", &buf); err != nil || n != int64(len(d)) || !bytes.Equal(buf.Bytes(), d) {
		t.Fatalf("Download = %d, %v", n, err)
	}
	o, err := s.Stat(ctx, "a/b/c")
	if err != nil || o.Key != "a/b/c" || o.Size != int64(len(d)) {
		t.Fatalf("Stat = %+v, %v", o, err)
	}
	if time.Since(o.Modified) > time.Hour || time.Until(o.Modified) > time.Hour {
		t.Fatalf("Stat's Modified = %v, not about now", o.Modified)
	}
	if s.Name() == "" {
		t.Fatal("Name is empty")
	}
}

func testMissing(t *testing.T, s blob.Store) {
	ctx := context.Background()
	if _, err := s.GetRange(ctx, "nope", 0, 10); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("GetRange of a missing key = %v", err)
	}
	if _, err := s.Download(ctx, "nope", io.Discard); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Download of a missing key = %v", err)
	}
	if _, err := s.Stat(ctx, "nope"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Stat of a missing key = %v", err)
	}
	if err := s.Delete(ctx, "nope"); err != nil {
		t.Fatalf("Delete of a missing key = %v", err)
	}
	if err := s.Copy(ctx, "nope", "dst"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Copy of a missing key = %v", err)
	}
}

func testReplace(t *testing.T, s blob.Store) {
	put(t, s, "k", []byte("first"))
	put(t, s, "k", []byte("second, longer"))
	if got := get(t, s, "k"); string(got) != "second, longer" {
		t.Fatalf("after a replace = %q", got)
	}
}

func testIfAbsent(t *testing.T, s blob.Store) {
	ctx := context.Background()
	put(t, s, "k", []byte("first"), blob.IfAbsent())
	err := s.Put(ctx, "k", strings.NewReader("second"), 6, blob.IfAbsent())
	if !errors.Is(err, blob.ErrExists) {
		t.Fatalf("a second Put IfAbsent = %v, want ErrExists", err)
	}
	if got := get(t, s, "k"); string(got) != "first" {
		t.Fatalf("a refused Put IfAbsent changed the object: %q", got)
	}
}

func testIfAbsentRace(t *testing.T, s blob.Store) {
	ctx := context.Background()
	const n = 8
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		won  []int
		errs []error
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := []byte(fmt.Sprintf("writer %d", i))
			err := s.Put(ctx, "race", bytes.NewReader(body), int64(len(body)), blob.IfAbsent())
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won = append(won, i)
			case !errors.Is(err, blob.ErrExists):
				errs = append(errs, err)
			}
		}(i)
	}
	wg.Wait()
	if len(errs) > 0 || len(won) != 1 {
		t.Fatalf("racing Puts IfAbsent: %d won, errors %v", len(won), errs)
	}
	if got := get(t, s, "race"); string(got) != fmt.Sprintf("writer %d", won[0]) {
		t.Fatalf("the object is %q, but writer %d won", got, won[0])
	}
}

func testShortReader(t *testing.T, s blob.Store) {
	ctx := context.Background()
	if err := s.Put(ctx, "short", strings.NewReader("abc"), 10); err == nil {
		t.Fatal("a Put whose reader held fewer bytes than its size succeeded")
	}
	if _, err := s.Stat(ctx, "short"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a failed Put left an object: %v", err)
	}
}

func testDelete(t *testing.T, s blob.Store) {
	ctx := context.Background()
	for _, k := range []string{"d/1", "d/2", "d/3", "keep"} {
		put(t, s, k, []byte(k))
	}
	if err := s.Delete(ctx, "d/1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "d/2", "d/3", "d/missing"); err != nil {
		t.Fatal(err)
	}
	objs, err := s.List(ctx, "")
	if err != nil || len(objs) != 1 || objs[0].Key != "keep" {
		t.Fatalf("after deletes, List = %v, %v", keys(objs), err)
	}
	if err := s.Delete(ctx); err != nil {
		t.Fatalf("Delete of no keys = %v", err)
	}
}

func keys(objs []blob.Object) []string {
	var out []string
	for _, o := range objs {
		out = append(out, o.Key)
	}
	return out
}

// listed is the store's keys in byte order: "a.b" (0x2e) before "a/b" (0x2f).
var listed = []string{"p/a.b", "p/a/b", "p/a/c/d", "p/b", "p0", "q/a"}

func testList(t *testing.T, s blob.Store) {
	ctx := context.Background()
	for i, k := range listed {
		put(t, s, k, data(i+1))
	}
	objs, err := s.List(ctx, "p/")
	want := []string{"p/a.b", "p/a/b", "p/a/c/d", "p/b"}
	if err != nil || strings.Join(keys(objs), ",") != strings.Join(want, ",") {
		t.Fatalf("List p/ = %v, %v; want %v", keys(objs), err, want)
	}
	for _, o := range objs {
		if o.Size == 0 || o.Modified.IsZero() {
			t.Fatalf("List's %s has size %d, modified %v", o.Key, o.Size, o.Modified)
		}
	}
	all, err := s.List(ctx, "")
	if err != nil || strings.Join(keys(all), ",") != strings.Join(listed, ",") {
		t.Fatalf("List all = %v, %v", keys(all), err)
	}
	if !sort.StringsAreSorted(keys(all)) {
		t.Fatal("List is out of order")
	}
	if objs, err := s.List(ctx, "p/a/"); err != nil || strings.Join(keys(objs), ",") != "p/a/b,p/a/c/d" {
		t.Fatalf("List p/a/ = %v, %v", keys(objs), err)
	}
	if objs, err := s.List(ctx, "p/a"); err != nil || len(objs) != 3 {
		t.Fatalf("List p/a (no slash) = %v, %v", keys(objs), err)
	}
	if objs, err := s.List(ctx, "zzz/"); err != nil || len(objs) != 0 {
		t.Fatalf("List of nothing = %v, %v", keys(objs), err)
	}
}

func testListPage(t *testing.T, s blob.Store) {
	ctx := context.Background()
	for i, k := range listed {
		put(t, s, k, data(i+1))
	}
	var got []string
	after := ""
	for pages := 0; ; pages++ {
		if pages > len(listed) {
			t.Fatal("ListPage does not end")
		}
		objs, next, err := s.ListPage(ctx, "", after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(objs) > 2 {
			t.Fatalf("a page of limit 2 held %d", len(objs))
		}
		got = append(got, keys(objs)...)
		if next == "" {
			break
		}
		if next != objs[len(objs)-1].Key {
			t.Fatalf("next = %q, the page's last key %q", next, objs[len(objs)-1].Key)
		}
		after = next
	}
	if strings.Join(got, ",") != strings.Join(listed, ",") {
		t.Fatalf("paged = %v, want %v", got, listed)
	}
	objs, next, err := s.ListPage(ctx, "p/", "p/a.b", 0)
	if err != nil || next != "" || strings.Join(keys(objs), ",") != "p/a/b,p/a/c/d,p/b" {
		t.Fatalf("ListPage after p/a.b = %v, %q, %v", keys(objs), next, err)
	}
	objs, next, err = s.ListPage(ctx, "p/", "p/b", 5)
	if err != nil || next != "" || len(objs) != 0 {
		t.Fatalf("ListPage after the last = %v, %q, %v", keys(objs), next, err)
	}
}

func testNested(t *testing.T, s blob.Store) {
	put(t, s, "n/a", []byte("outer"))
	put(t, s, "n/a/b", []byte("inner"))
	if got := get(t, s, "n/a"); string(got) != "outer" {
		t.Fatalf("n/a = %q", got)
	}
	objs, err := s.List(context.Background(), "n/")
	if err != nil || strings.Join(keys(objs), ",") != "n/a,n/a/b" {
		t.Fatalf("List n/ = %v, %v", keys(objs), err)
	}
}

func testCopy(t *testing.T, s blob.Store) {
	ctx := context.Background()
	d := data(70_000)
	put(t, s, "src", d)
	if err := s.Copy(ctx, "src", "dst/x"); err != nil {
		t.Fatal(err)
	}
	if got := get(t, s, "dst/x"); !bytes.Equal(got, d) {
		t.Fatalf("the copy holds %d bytes", len(got))
	}
	put(t, s, "src", []byte("changed"))
	if got := get(t, s, "dst/x"); !bytes.Equal(got, d) {
		t.Fatal("changing the source changed the copy")
	}
}

func testEmpty(t *testing.T, s blob.Store) {
	put(t, s, "empty", nil)
	if got := get(t, s, "empty"); len(got) != 0 {
		t.Fatalf("an empty object read as %d bytes", len(got))
	}
	if o, err := s.Stat(context.Background(), "empty"); err != nil || o.Size != 0 {
		t.Fatalf("Stat of an empty object = %+v, %v", o, err)
	}
}

func testClasses(t *testing.T, s blob.Store) {
	for _, c := range []blob.Class{blob.Standard, blob.Infrequent, blob.Archive} {
		key := fmt.Sprintf("class/%d", c)
		put(t, s, key, []byte(key), blob.WithClass(c))
		if got := get(t, s, key); string(got) != key {
			t.Fatalf("class %d read as %q", c, got)
		}
	}
}

func testSignGet(t *testing.T, s blob.Store, client *http.Client) {
	ctx := context.Background()
	d := data(50_000)
	put(t, s, "signed/x", d)
	u, err := s.SignGet(ctx, "signed/x", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	fetch := func(rng string) []byte {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			t.Fatal(err)
		}
		if rng != "" {
			req.Header.Set("Range", rng)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode/100 != 2 {
			t.Fatalf("GET of the signed URL: %s: %s", resp.Status, body)
		}
		return body
	}
	if got := fetch(""); !bytes.Equal(got, d) {
		t.Fatalf("the signed URL served %d bytes", len(got))
	}
	if got := fetch("bytes=1000-1999"); !bytes.Equal(got, d[1000:2000]) {
		t.Fatalf("a range of the signed URL served %d bytes", len(got))
	}
}
