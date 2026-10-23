package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/scm/codesearch"
	"github.com/Cidan/ok/internal/scm/encoding"
)

// Code search (docs/hosted.md, "ok control at scale, as built"): a trigram index of each repository's
// main (internal/scm/codesearch), built as keyed work (CodeSearchWork) when main moves — at most
// every codeIndexEvery — and kept in the bucket, one object of each repository's latest; replicas
// keep the indexes they search in memory.

// CodeSearchWork is the keyed work that indexes a repository's main.
const CodeSearchWork = "codesearch"

// codeIndexEvery is how often a repository's index may be built.
const codeIndexEvery = 30 * time.Second

// codeIndexCheck is how long a replica uses an index it holds before it looks for a newer one.
const codeIndexCheck = 10 * time.Second

// codeIndexBatch is how many files an index's build reads at once.
const codeIndexBatch = 256

// DefaultCodeIndexCacheBytes is the memory a replica keeps loaded indexes in.
const DefaultCodeIndexCacheBytes = 256 << 20

func codeIndexKey(repo int64) string { return "search/" + strconv.FormatInt(repo, 10) }

// codeIndex is an index a replica holds: what it is, the object it was read from, and when that was
// last checked against the bucket.
type codeIndex struct {
	ix       *codesearch.Index
	modified time.Time
	mu       sync.Mutex
	checked  time.Time
}

// ErrNoCodeIndex is a repository whose main is not indexed yet: its build has been asked for.
var ErrNoCodeIndex = errors.New("the repository's code is being indexed: search again in a minute")

// BuildCodeIndex indexes a repository's main, unless its index is of main already, or was built less
// than codeIndexEvery ago — when it is marked to be built then.
func (s *Store) BuildCodeIndex(ctx context.Context, repo int64) error {
	r, err := s.GetRepository(ctx, repo)
	if err != nil {
		return err
	}
	head := r.Head
	key := codeIndexKey(repo)
	if o, err := s.blobs.Stat(ctx, key); err == nil {
		if commit, err := s.codeIndexCommit(ctx, key); err == nil && commit == head {
			return nil
		}
		if wait := codeIndexEvery - time.Since(o.Modified); wait > 0 {
			return s.Work().MarkAt(ctx, CodeSearchWork, repo, time.Now().Add(wait))
		}
	} else if !errors.Is(err, blob.ErrNotFound) {
		return err
	}
	b := codesearch.NewBuilder(head)
	if head != encoding.RootCommitID {
		var files []codesearch.File
		if err := s.Export(ctx, repo, head, nil, false, func(e *ExportEntry, _ []byte) error {
			if e != nil && e.Value.Kind == encoding.TreeValueFile && e.Value.Size <= codesearch.MaxFile {
				files = append(files, codesearch.File{Path: e.Path, ID: e.Value.ID, Size: e.Value.Size})
			}
			return nil
		}); err != nil {
			return fmt.Errorf("listing main: %w", err)
		}
		for start := 0; start < len(files); start += codeIndexBatch {
			batch := files[start:min(start+codeIndexBatch, len(files))]
			data, err := s.readWhole(ctx, repo, batch)
			if err != nil {
				return err
			}
			for i, f := range batch {
				b.Add(f, data[i])
			}
		}
	}
	var buf bytes.Buffer
	if err := b.Index().Encode(&buf); err != nil {
		return err
	}
	return s.blobs.Put(ctx, key, bytes.NewReader(buf.Bytes()), int64(buf.Len()))
}

// readWhole reads files whole, in order: an empty file's nil.
func (s *Store) readWhole(ctx context.Context, repo int64, files []codesearch.File) ([][]byte, error) {
	out := make([][]byte, len(files))
	var ranges []FileRange
	var owner []int
	for i, f := range files {
		if f.Size > 0 {
			ranges = append(ranges, FileRange{FileID: f.ID})
			owner = append(owner, i)
		}
	}
	err := s.ReadFiles(ctx, repo, ranges, RawChunks, func(fr FileFrame) error {
		if fr.NotFound {
			return notFound("file %s", fr.FileID)
		}
		i := owner[fr.Range]
		out[i] = append(out[i], fr.Data...)
		return nil
	})
	return out, err
}

// codeIndexCommit is the commit an index object is of, from its head.
func (s *Store) codeIndexCommit(ctx context.Context, key string) (encoding.ID, error) {
	head, err := s.blobs.GetRange(ctx, key, 0, 5+encoding.IDLen)
	if err != nil {
		return encoding.ID{}, err
	}
	var id encoding.ID
	copy(id[:], head[5:])
	return id, nil
}

// loadCodeIndex is a repository's index: held, or read from the bucket, a newer one taken up within
// codeIndexCheck. A repository with none has its build marked, and is ErrNoCodeIndex.
func (s *Store) loadCodeIndex(ctx context.Context, repo int64) (*codesearch.Index, error) {
	key := codeIndexKey(repo)
	held, ok := s.codeIndexes.get(key)
	if ok {
		held.mu.Lock()
		fresh := time.Since(held.checked) < codeIndexCheck
		held.mu.Unlock()
		if fresh {
			return held.ix, nil
		}
	}
	o, err := s.blobs.Stat(ctx, key)
	if errors.Is(err, blob.ErrNotFound) {
		if err := s.Work().Mark(ctx, CodeSearchWork, repo); err != nil {
			return nil, err
		}
		return nil, ErrNoCodeIndex
	}
	if err != nil {
		return nil, err
	}
	if ok && o.Modified.Equal(held.modified) {
		held.mu.Lock()
		held.checked = time.Now()
		held.mu.Unlock()
		return held.ix, nil
	}
	var buf bytes.Buffer
	if _, err := s.blobs.Download(ctx, key, &buf); err != nil {
		return nil, err
	}
	ix, err := codesearch.Decode(buf.Bytes())
	if err != nil {
		return nil, err
	}
	s.codeIndexes.put(key, &codeIndex{ix: ix, modified: o.Modified, checked: time.Now()}, int(ix.Size()))
	return ix, nil
}

// SearchCode answers a query over a repository's latest index; stale says main has moved since,
// and its next index has been asked for.
func (s *Store) SearchCode(ctx context.Context, repo int64, q codesearch.Query) (res codesearch.Result, commit encoding.ID, stale bool, err error) {
	r, err := s.GetRepository(ctx, repo)
	if err != nil {
		return res, commit, false, err
	}
	ix, err := s.loadCodeIndex(ctx, repo)
	if err != nil {
		return res, commit, false, err
	}
	if stale = ix.Commit != r.Head; stale {
		if err := s.Work().Mark(ctx, CodeSearchWork, repo); err != nil {
			return res, commit, false, err
		}
	}
	res, err = ix.Search(ctx, q, func(ctx context.Context, ids [][32]byte) (map[[32]byte][]byte, error) {
		files := make([]codesearch.File, len(ids))
		for i, id := range ids {
			files[i] = codesearch.File{ID: id, Size: 1}
		}
		data, err := s.readWhole(ctx, repo, files)
		if err != nil {
			return nil, err
		}
		out := make(map[[32]byte][]byte, len(ids))
		for i, id := range ids {
			out[id] = data[i]
		}
		return out, nil
	})
	return res, ix.Commit, stale, err
}

// dropCodeIndex deletes a repository's index: it is deleted.
func (s *Store) dropCodeIndex(ctx context.Context, repo int64) error {
	if err := s.blobs.Delete(ctx, codeIndexKey(repo)); err != nil && !errors.Is(err, blob.ErrNotFound) {
		return err
	}
	return nil
}
