package store

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/Cidan/ok/internal/scm/codesearch"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/work/worktest"
)

// Main's moves mark its index's build; a search before it is built asks for it; built, a search
// finds main's files; an index older than main answers, stale, its next build asked for — and
// not built again within codeIndexEvery of the last.
func TestCodeSearch(t *testing.T) {
	ctx := context.Background()
	e := newBrowseEnv(t)
	e.s.marksMade()
	if got := worktest.Ready(t, e.s.Work(), CodeSearchWork); !slices.Contains(got, e.repo) {
		t.Fatalf("main's moves did not mark its indexing: %v", got)
	}
	if _, _, _, err := e.s.SearchCode(ctx, e.repo, codesearch.Query{Pattern: "four"}); !errors.Is(err, ErrNoCodeIndex) {
		t.Fatalf("a search before the index: %v", err)
	}
	if err := e.s.BuildCodeIndex(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	found := func() ([]string, bool) {
		t.Helper()
		res, commit, stale, err := e.s.SearchCode(ctx, e.repo, codesearch.Query{Pattern: "four"})
		if err != nil {
			t.Fatal(err)
		}
		if !stale && commit != e.c3 {
			t.Errorf("the index is of %s, not main's %s", commit, e.c3)
		}
		var out []string
		for _, m := range res.Matches {
			out = append(out, m.Path+":"+strconv.Itoa(m.Line))
		}
		return out, stale
	}
	if got, stale := found(); stale || !slices.Equal(got, []string{"a/x.txt:4", "a/y.txt:4"}) {
		t.Errorf("main's matches: %v (stale %v)", got, stale)
	}

	// An index of an older commit stands in until the next is built: stale.
	b := codesearch.NewBuilder(e.c1)
	if err := e.s.Export(ctx, e.repo, e.c1, nil, false, func(x *ExportEntry, _ []byte) error {
		if x != nil && x.Value.Kind == encoding.TreeValueFile {
			data, err := e.s.ReadFile(ctx, e.repo, x.Value.ID)
			if err != nil {
				return err
			}
			b.Add(codesearch.File{Path: x.Path, ID: x.Value.ID, Size: x.Value.Size}, data)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := b.Index().Encode(&buf); err != nil {
		t.Fatal(err)
	}
	if err := e.s.blobs.Put(ctx, codeIndexKey(e.repo), bytes.NewReader(buf.Bytes()), int64(buf.Len())); err != nil {
		t.Fatal(err)
	}
	e.s.codeIndexes = newCache[*codeIndex]("code_indexes", DefaultCodeIndexCacheBytes)
	if _, stale := found(); !stale {
		t.Error("an index of an older commit is not stale")
	}
	// Built less than codeIndexEvery ago, it is not built again yet.
	if err := e.s.BuildCodeIndex(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	if commit, err := e.s.codeIndexCommit(ctx, codeIndexKey(e.repo)); err != nil || commit != e.c1 {
		t.Errorf("rebuilt within codeIndexEvery: %s, %v", commit, err)
	}
}
