package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
)

// goFile is a small source file, like the many a repository holds: what a dictionary learns.
func goFile(i int) []byte {
	return []byte(fmt.Sprintf(`// Package p%[1]d does one thing.
package p%[1]d

import (
	"context"
	"errors"
	"fmt"
)

// ErrNotFound%[1]d is what Get%[1]d returns for a key it does not hold.
var ErrNotFound%[1]d = errors.New("p%[1]d: not found")

// Get%[1]d returns the value at key %[1]d, or ErrNotFound%[1]d.
func Get%[1]d(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("p%[1]d: an empty key: %%w", ErrNotFound%[1]d)
	}
	return fmt.Sprintf("%%s=%[2]d", key), nil
}
`, i, i*7919))
}

// A repository's GC trains a dictionary on its small files once it has enough, and compaction
// compresses its small chunks with it; every read form serves them — raw, stored with the
// dictionary's chunks sent raw, and stored as is — and a pass after that changes nothing.
func TestDictionaries(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	e.s.dictionaryMinSamples = 50
	b := newBuilder(t)
	contents := map[encoding.ID][]byte{}
	var entries []encoding.TreeEntry
	for i := range 200 {
		c := goFile(i)
		f := b.file(c)
		contents[f] = c
		entries = append(entries, fileEntry(fmt.Sprintf("p%03d.go", i), f))
	}
	commit := b.commit(b.tree(entries...), "many small files", encoding.RootCommitID)
	op := b.operation(b.view(commit), "snapshot", encoding.RootOperationID)
	mustPublish(t, e.s, e.repo, alice, b, &OpHeadsUpdate{New: op})

	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.DictionaryTrained == 0 || r.DictionarySamples < 50 || r.Dictionary != r.DictionaryTrained || r.ChunksRecompressed == 0 || r.BytesRecompressed <= 0 {
		t.Fatalf("no dictionary trained and used: %s", r)
	}
	var dictChunks int
	for _, k := range indexOf(t, e.s, e.repo).Sorted() {
		if k.Entry.Kind == encoding.KindChunk && pack.Codec(k.Entry.Loc.Codec) == pack.ZstdDict {
			dictChunks++
		}
	}
	if int64(dictChunks) != r.ChunksRecompressed {
		t.Fatalf("%d chunks name the dictionary; the pass compressed %d", dictChunks, r.ChunksRecompressed)
	}

	files := make([]FileRange, 0, len(contents))
	for f := range contents {
		files = append(files, FileRange{FileID: f})
	}
	// Raw, from a store that has read no dictionary yet.
	fresh := reopen(t, e.s)
	for f, c := range contents {
		if got := readAll(t, fresh, e.repo, f); !bytes.Equal(got, c) {
			t.Fatalf("file %s reads back wrong", f)
		}
	}
	// Stored, for a client that decodes no dictionary: those chunks come raw.
	got := map[encoding.ID][]byte{}
	if err := fresh.ReadFiles(ctx, e.repo, files, StoredChunks, func(fr FileFrame) error {
		if fr.Codec == pack.ZstdDict {
			return fmt.Errorf("a dictionary's chunk sent to a client that decodes none")
		}
		raw, err := pack.Decode(nil, fr.ChunkID, fr.Codec, fr.RawLength, fr.Data)
		if err != nil {
			return err
		}
		got[fr.FileID] = append(got[fr.FileID], raw...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for f, c := range contents {
		if !bytes.Equal(got[f], c) {
			t.Fatalf("file %s, stored without dictionaries, reads back wrong", f)
		}
	}
	// Stored as is: the client fetches the dictionary each frame names.
	decoders := map[uint32]*zstd.Decoder{}
	sent := 0
	if err := fresh.ReadFiles(ctx, e.repo, files, StoredChunksWithDictionaries, func(fr FileFrame) error {
		raw := fr.Data
		if fr.Codec == pack.ZstdDict {
			sent++
			id, err := pack.DictionaryID(fr.Data)
			if err != nil {
				return err
			}
			dec, ok := decoders[id]
			if !ok {
				body, err := fresh.Dictionary(ctx, e.repo, id)
				if err != nil {
					return err
				}
				if dec, err = zstd.NewReader(nil, zstd.WithDecoderDicts(body)); err != nil {
					return err
				}
				decoders[id] = dec
			}
			var err2 error
			if raw, err2 = dec.DecodeAll(fr.Data, nil); err2 != nil {
				return err2
			}
		} else if fr.Codec == pack.Zstd {
			var err error
			if raw, err = pack.Decode(nil, fr.ChunkID, fr.Codec, fr.RawLength, fr.Data); err != nil {
				return err
			}
		}
		if pack.ChunkID(raw) != fr.ChunkID {
			return fmt.Errorf("chunk %s decodes to other bytes", fr.ChunkID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if sent != dictChunks {
		t.Fatalf("%d chunks sent compressed with the dictionary, %d stored so", sent, dictChunks)
	}
	// A dictionary is read only through its repository.
	other := newRepo(t, e.s).ID
	if _, err := e.s.Dictionary(ctx, other, r.DictionaryTrained); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another repository's dictionary: %v", err)
	}
	if _, err := e.s.Dictionary(ctx, e.repo, 7); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an ID below the private range: %v", err)
	}

	// Settled: no new dictionary, nothing rewritten.
	again, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if again.DictionaryTrained != 0 || again.PacksWritten != 0 || again.ChunksRecompressed != 0 {
		t.Fatalf("a settled repository changed: %s", again)
	}
	// After DictionaryRetrain, a newer one joins it; the old one still decodes what names it.
	later, err := e.s.GC(ctx, e.repo, time.Now().Add(DictionaryRetrain+time.Hour), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if later.DictionaryTrained <= r.DictionaryTrained {
		t.Fatalf("no newer dictionary: %s", later)
	}
	if _, err := e.s.Dictionary(ctx, e.repo, r.DictionaryTrained); err != nil {
		t.Fatalf("the older dictionary: %v", err)
	}
	clean(t, reopen(t, e.s), e.repo)
}

// A repository with too few small files trains none, and compaction is as without dictionaries.
func TestNoDictionaryForFewFiles(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	commit := b.commit(b.tree(fileEntry("a.go", b.file(goFile(1))), fileEntry("b.go", b.file(goFile(2)))), "few", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, &OpHeadsUpdate{New: b.operation(b.view(commit), "snapshot", encoding.RootOperationID)})
	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.DictionaryTrained != 0 || r.ChunksRecompressed != 0 {
		t.Fatalf("a dictionary for two files: %s", r)
	}
	clean(t, e.s, e.repo)
}
