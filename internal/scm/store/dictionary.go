package store

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	builddict "github.com/klauspost/compress/dict"
	"github.com/klauspost/compress/zstd"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/serve/metrics"
)

// Dictionaries (docs/scm.md, "Dictionaries"): each repository's zstd dictionaries,
// trained on samples of its small files, which compaction compresses its small chunks with. Small
// data compresses badly alone — compression learns from what came before, and a 3 KB file has
// nothing before it — and a dictionary gives it a past: measured on the monorepo, at zstd's
// highest level, files to 4 KiB store 26% smaller, to 16 KiB 17%. A dictionary never changes and
// is never deleted; a newer one only joins it.
//
// A chunk's zstd frame names its dictionary by 32768 + the dictionary's row ID: RFC 8878 leaves
// IDs from 32768 below 2^31 to private use.

const (
	// dictionaryIDBase is a dictionary's frame ID less its row ID.
	dictionaryIDBase = 32768
	// dictionarySize bounds a trained dictionary: zstd's default.
	dictionarySize = 110 << 10
	// dictionarySamples is how many files a training reads, at most; one with fewer than
	// minDictionarySamples small files keeps none.
	dictionarySamples    = 8000
	minDictionarySamples = 1000
	// dictionaryFileMin and dictionaryFileMax bound the files sampled: too small to teach, or
	// big enough to compress well alone.
	dictionaryFileMin = 64
	dictionaryFileMax = 16 << 10
	// DictionaryChunkMax is the largest chunk compaction compresses with a dictionary: past it,
	// a chunk has its own past.
	DictionaryChunkMax = 64 << 10
	// DictionaryRetrain is how old a repository's newest dictionary grows before a pass trains
	// another.
	DictionaryRetrain = 90 * 24 * time.Hour
)

var dictionaryChunks = metrics.NewCounter("ok_scm_dictionary_chunks_total",
	"Chunks compaction compressed with a repository's dictionary, and those it tried that did not shrink.", "result")

// dictionaries holds decoders and encoders of the dictionaries this store has read.
type dictionaries struct {
	s        *Store
	mu       sync.Mutex
	decoders map[uint32]*zstd.Decoder
}

func newDictionaries(s *Store) *dictionaries {
	return &dictionaries{s: s, decoders: map[uint32]*zstd.Decoder{}}
}

// Decoder is a decoder holding the dictionary a frame names (pack.Dicts), read once.
func (d *dictionaries) Decoder(id uint32) (*zstd.Decoder, error) {
	d.mu.Lock()
	dec, ok := d.decoders[id]
	d.mu.Unlock()
	if ok {
		return dec, nil
	}
	if id < dictionaryIDBase {
		return nil, fmt.Errorf("dictionary %d: no dictionary has that ID", id)
	}
	var body []byte
	err := d.s.pool.QueryRow(context.Background(),
		`SELECT body FROM dictionary WHERE id = $1 AND length(body) > 0`, int64(id-dictionaryIDBase)).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("dictionary %d", id)
	}
	if err != nil {
		return nil, err
	}
	dec, err = zstd.NewReader(nil, zstd.WithDecoderDicts(body), zstd.WithDecoderConcurrency(0))
	if err != nil {
		return nil, fmt.Errorf("dictionary %d: %w", id, err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if have, ok := d.decoders[id]; ok {
		dec.Close()
		return have, nil
	}
	d.decoders[id] = dec
	return dec, nil
}

// Dictionary is a repository's dictionary as a zstd frame names it: what a client decoding the
// chunks compressed with it fetches. A dictionary holds pieces of the repository's files, so it is
// read only through its repository.
func (s *Store) Dictionary(ctx context.Context, repo int64, id uint32) ([]byte, error) {
	if id < dictionaryIDBase {
		return nil, notFound("dictionary %d", id)
	}
	var body []byte
	err := s.pool.QueryRow(ctx, `SELECT body FROM dictionary WHERE repository_id = $1 AND id = $2 AND length(body) > 0`,
		repo, int64(id-dictionaryIDBase)).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("dictionary %d", id)
	}
	return body, err
}

// newestDictionary is a repository's newest dictionary and when it was trained; a body of nil
// when it has none.
func (s *Store) newestDictionary(ctx context.Context, repo int64) (id uint32, body []byte, at time.Time, err error) {
	var row int64
	err = s.pool.QueryRow(ctx, `SELECT id, body, created_at FROM dictionary
		WHERE repository_id = $1 AND length(body) > 0 ORDER BY id DESC LIMIT 1`, repo).Scan(&row, &body, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, time.Time{}, nil
	}
	if err != nil {
		return 0, nil, time.Time{}, err
	}
	return uint32(row + dictionaryIDBase), body, at, nil
}

// dictionaryEncoder is an encoder with a repository's newest dictionary, at zstd's highest level:
// compaction's, which nothing waits on. Nil when the repository has none.
func (s *Store) dictionaryEncoder(ctx context.Context, repo int64) (*zstd.Encoder, uint32, error) {
	id, body, _, err := s.newestDictionary(ctx, repo)
	if err != nil || body == nil {
		return nil, 0, err
	}
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedBestCompression),
		zstd.WithEncoderDict(body),
		zstd.WithEncoderCRC(true),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, 0, fmt.Errorf("dictionary %d: %w", id, err)
	}
	return enc, id, nil
}

// trainDictionary trains a dictionary for the repository when it has none, or its newest is older
// than DictionaryRetrain: from up to dictionarySamples of its small files, chosen as the files
// whose IDs follow a random point — IDs are uniform, so they are a uniform sample, read through
// the primary key whatever the repository's size. It returns the dictionary's frame ID, 0 when it
// trained none.
func (s *Store) trainDictionary(ctx context.Context, repo int64, now time.Time) (uint32, int, error) {
	_, body, at, err := s.newestDictionary(ctx, repo)
	if err != nil {
		return 0, 0, err
	}
	if body != nil && now.Sub(at) < DictionaryRetrain {
		return 0, 0, nil
	}
	var start encoding.ID
	if _, err := rand.Read(start[:]); err != nil {
		return 0, 0, err
	}
	_, x, err := s.contentIndex(ctx, repo)
	if err != nil {
		return 0, 0, err
	}
	// The small files whose IDs follow the random point, wrapping around.
	all := x.Sorted()
	first := sort.Search(len(all), func(i int) bool { return string(all[i].ID[:]) >= string(start[:]) })
	var files []encoding.ID
	for i := range all {
		k := all[(first+i)%len(all)]
		if k.Entry.Kind == encoding.KindFile && k.Entry.Body && k.Entry.Size >= dictionaryFileMin && k.Entry.Size <= dictionaryFileMax {
			files = append(files, k.ID)
			if len(files) == dictionarySamples {
				break
			}
		}
	}
	least := minDictionarySamples
	if s.dictionaryMinSamples > 0 {
		least = s.dictionaryMinSamples
	}
	if len(files) < least {
		return 0, 0, nil
	}
	samples, err := s.sampleContents(ctx, repo, files)
	if err != nil {
		return 0, 0, err
	}
	if len(samples) < least {
		return 0, 0, nil
	}
	var row int64
	if err := s.pool.QueryRow(ctx, `INSERT INTO dictionary (repository_id, body, samples) VALUES ($1, '', $2) RETURNING id`,
		repo, len(samples)).Scan(&row); err != nil {
		return 0, 0, err
	}
	id := uint32(row + dictionaryIDBase)
	dict, err := builddict.BuildZstdDict(samples, builddict.Options{
		MaxDictSize:    dictionarySize,
		HashBytes:      6,
		ZstdDictID:     id,
		ZstdDictCompat: true,
		ZstdLevel:      zstd.SpeedBestCompression,
	})
	if err == nil {
		_, err = s.pool.Exec(ctx, `UPDATE dictionary SET body = $2 WHERE id = $1`, row, dict)
	}
	if err != nil {
		if _, derr := s.pool.Exec(context.WithoutCancel(ctx), `DELETE FROM dictionary WHERE id = $1`, row); derr != nil {
			return 0, 0, errors.Join(err, derr)
		}
		return 0, 0, fmt.Errorf("training dictionary %d: %w", id, err)
	}
	return id, len(samples), nil
}

// sampleContents is the contents of the files of one chunk among files: what a dictionary trains
// on.
func (s *Store) sampleContents(ctx context.Context, repo int64, files []encoding.ID) ([][]byte, error) {
	var out [][]byte
	for start := 0; start < len(files); start += manifestBatch {
		batch := files[start:min(start+manifestBatch, len(files))]
		ms, err := s.manifests(ctx, repo, batch)
		if err != nil {
			return nil, err
		}
		var chunks []encoding.ID
		for _, id := range batch {
			if m := ms[id]; m != nil && len(m.chunks) == 1 {
				chunks = append(chunks, m.chunks[0].id)
			}
		}
		raw, err := s.chunkBytes(ctx, repo, chunks)
		if err != nil {
			return nil, err
		}
		for _, c := range chunks {
			if b, ok := raw[c]; ok {
				out = append(out, b)
			}
		}
	}
	return out, nil
}
