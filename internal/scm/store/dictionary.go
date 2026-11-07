package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	builddict "github.com/klauspost/compress/dict"
	"github.com/klauspost/compress/zstd"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// Dictionaries (docs/scm.md, "Dictionaries"): each network's zstd dictionaries, trained on
// samples of its small files, which compaction compresses frames of many with and a small file read
// from one is sent with. Small data compresses badly alone — compression learns from what came
// before, and a 3 KB file has nothing before it — and a dictionary gives it a past: measured on the
// monorepo, at zstd's highest level, files to 4 KiB 26% smaller, to 16 KiB 17%. In a frame of many a
// file has its neighbours for a past, so a dictionary saves the frame little; it is what a file sent
// alone saves. A dictionary never changes and is never deleted; a newer one only joins it.
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
	// DictionaryChunkMax is the largest chunk sent alone with a dictionary: past it, a chunk has
	// its own past.
	DictionaryChunkMax = 64 << 10
	// DictionaryRetrain is how old a repository's newest dictionary grows before a pass trains
	// another.
	DictionaryRetrain = 90 * 24 * time.Hour
)

// dictionaries holds decoders and encoders of the dictionaries this store has read: a dictionary
// never changes, so neither goes stale.
type dictionaries struct {
	s        *Store
	mu       sync.Mutex
	decoders map[netDictionary]*zstd.Decoder
	encoders map[netDictionary]*zstd.Encoder
}

// netDictionary is a dictionary as a frame names it within its network: an ID is its network's.
type netDictionary struct {
	net backend.Network
	id  uint32
}

func newDictionaries(s *Store) *dictionaries {
	return &dictionaries{s: s, decoders: map[netDictionary]*zstd.Decoder{}, encoders: map[netDictionary]*zstd.Encoder{}}
}

// of decodes the frames of a network's packs (pack.Dicts): each names one of its network's
// dictionaries.
func (d *dictionaries) of(net backend.Network) pack.Dicts { return networkDicts{d, net} }

type networkDicts struct {
	d   *dictionaries
	net backend.Network
}

func (n networkDicts) Decoder(id uint32) (*zstd.Decoder, error) { return n.d.decoder(n.net, id) }

// decoder is a decoder holding a network's dictionary a frame names, read once.
func (d *dictionaries) decoder(net backend.Network, id uint32) (*zstd.Decoder, error) {
	k := netDictionary{net, id}
	d.mu.Lock()
	dec, ok := d.decoders[k]
	d.mu.Unlock()
	if ok {
		return dec, nil
	}
	if id < dictionaryIDBase {
		return nil, fmt.Errorf("dictionary %d: no dictionary has that ID", id)
	}
	body, _, ok, err := d.s.b.NetworkDictionary(context.Background(), net, int64(id-dictionaryIDBase))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, notFound("network %d's dictionary %d", net, id)
	}
	dec, err = zstd.NewReader(nil, zstd.WithDecoderDicts(body), zstd.WithDecoderConcurrency(0))
	if err != nil {
		return nil, fmt.Errorf("dictionary %d: %w", id, err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if have, ok := d.decoders[k]; ok {
		dec.Close()
		return have, nil
	}
	d.decoders[k] = dec
	return dec, nil
}

// encoder is an encoder holding a network's dictionary a frame names, read once: what a record of a
// frame compressed with it is compressed alone with, to be sent (sendable).
func (d *dictionaries) encoder(net backend.Network, id uint32) (*zstd.Encoder, error) {
	k := netDictionary{net, id}
	d.mu.Lock()
	enc, ok := d.encoders[k]
	d.mu.Unlock()
	if ok {
		return enc, nil
	}
	if id < dictionaryIDBase {
		return nil, fmt.Errorf("dictionary %d: no dictionary has that ID", id)
	}
	body, _, ok, err := d.s.b.NetworkDictionary(context.Background(), net, int64(id-dictionaryIDBase))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, notFound("network %d's dictionary %d", net, id)
	}
	enc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderDict(body),
		zstd.WithEncoderCRC(true), zstd.WithLowerEncoderMem(true))
	if err != nil {
		return nil, fmt.Errorf("dictionary %d: %w", id, err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if have, ok := d.encoders[k]; ok {
		enc.Close()
		return have, nil
	}
	d.encoders[k] = enc
	return enc, nil
}

// Dictionary is a repository's network's dictionary as a zstd frame names it: what a client
// decoding the chunks compressed with it fetches. A dictionary holds pieces of the files it was
// trained on, so it is read only through a repository that may read those (dictionaryReadable).
func (s *Store) Dictionary(ctx context.Context, repo int64, id uint32) ([]byte, error) {
	if id < dictionaryIDBase {
		return nil, notFound("dictionary %d", id)
	}
	net, err := s.network(ctx, repo)
	if err != nil {
		return nil, err
	}
	body, trainer, ok, err := s.ops(ctx).NetworkDictionary(ctx, net, int64(id-dictionaryIDBase))
	if err != nil {
		return nil, err
	}
	if ok {
		s.dictTrainers.Store(id, trainer)
		readable, err := s.dictionaryReadable(ctx, repo)
		if err != nil {
			return nil, err
		}
		if ok, err = readable(id); err != nil {
			return nil, err
		}
	}
	if !ok {
		return nil, notFound("dictionary %d", id)
	}
	return body, nil
}

// dictionaryReadable says which dictionaries a repository may read (docs/hosted.md, "Fork
// networks, as built", "Dictionaries"): those it trained, and of each other trainer those it was
// granted — no newer than its source could read when it last forked or synced, and so trained
// only on content it inherited.
func (s *Store) dictionaryReadable(ctx context.Context, repo int64) (func(uint32) (bool, error), error) {
	grants, err := s.ops(ctx).DictionaryGrants(ctx, repo)
	if err != nil {
		return nil, err
	}
	net, err := s.network(ctx, repo)
	if err != nil {
		return nil, err
	}
	return func(id uint32) (bool, error) {
		if id < dictionaryIDBase {
			return false, nil
		}
		row := int64(id - dictionaryIDBase)
		trainer, ok := s.dictTrainers.Load(id)
		if !ok {
			_, t, found, err := s.ops(ctx).NetworkDictionary(ctx, net, row)
			if err != nil || !found {
				return false, err
			}
			s.dictTrainers.Store(id, t)
			trainer = t
		}
		t := trainer.(int64)
		upTo, granted := grants[t]
		return t == repo || (granted && row <= upTo), nil
	}, nil
}

// readableDictionaries are, for each trainer, the newest dictionary a repository may read: what
// a fork of it, or a sync from it, grants.
func (s *Store) readableDictionaries(ctx context.Context, repo int64) (map[int64]int64, error) {
	grants, err := s.ops(ctx).DictionaryGrants(ctx, repo)
	if err != nil {
		return nil, err
	}
	own, ok, err := s.ops(ctx).NewestDictionaryOf(ctx, repo)
	if err != nil {
		return nil, err
	}
	if ok {
		grants[repo] = own
	}
	return grants, nil
}

// newestDictionary is a repository's network's newest dictionary and when it was trained; a body
// of nil when it has none.
func (s *Store) newestDictionary(ctx context.Context, repo int64) (id uint32, body []byte, at time.Time, err error) {
	net, err := s.network(ctx, repo)
	if err != nil {
		return 0, nil, time.Time{}, err
	}
	row, body, at, ok, err := s.ops(ctx).NewestDictionary(ctx, net)
	if err != nil || !ok {
		return 0, nil, time.Time{}, err
	}
	return uint32(row + dictionaryIDBase), body, at, nil
}

// trainDictionary trains a dictionary for the repository's network, if the repository is the one
// that trains them, when it has none or its newest is older than DictionaryRetrain: from up to
// dictionarySamples of the small files its main's head holds, chosen as the files whose IDs follow
// a random point, wrapping around — IDs are uniform, so they are a uniform sample. It returns the
// dictionary's frame ID, 0 when it trained none.
func (s *Store) trainDictionary(ctx context.Context, repo int64, now time.Time) (uint32, int, error) {
	net, err := s.network(ctx, repo)
	if err != nil {
		return 0, 0, err
	}
	// One repository of a network trains, and only from what its main holds: a dictionary holds
	// pieces of what it was trained on, and every reader its trainer grants it to may read that.
	// Pinned is no stand-in — what a workspace's settled operations reach is pinned (settle.go), a
	// person's files no CL ever held.
	trainer, err := s.ops(ctx).NetworkTrainer(ctx, net)
	if err != nil || trainer != repo {
		return 0, 0, err
	}
	_, body, at, err := s.newestDictionary(ctx, repo)
	if err != nil {
		return 0, 0, err
	}
	if body != nil && now.Sub(at) < DictionaryRetrain {
		return 0, 0, nil
	}
	held, err := s.mainSmallFiles(ctx, repo)
	if err != nil {
		return 0, 0, err
	}
	var start encoding.ID
	if _, err := rand.Read(start[:]); err != nil {
		return 0, 0, err
	}
	sortIDs(held)
	first, _ := slices.BinarySearchFunc(held, start, func(id, start encoding.ID) int { return bytes.Compare(id[:], start[:]) })
	files := make([]encoding.ID, 0, min(len(held), dictionarySamples))
	for i := 0; i < len(held) && len(files) < dictionarySamples; i++ {
		files = append(files, held[(first+i)%len(held)])
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
	if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		var err error
		row, err = tx.InsertDictionary(ctx, net, repo, len(samples))
		return err
	}); err != nil {
		return 0, 0, err
	}
	id := uint32(row + dictionaryIDBase)
	dict, err := builddict.BuildZstdDict(samples, builddict.Options{
		MaxDictSize:    dictionarySize,
		HashBytes:      6,
		ZstdDictID:     id,
		ZstdDictCompat: true,
		ZstdLevel:      zstd.SpeedBetterCompression, // dictionaryEncoder's, which the tables are fitted for
	})
	if err == nil {
		err = s.inTx(ctx, func(ctx context.Context, tx *txn) error { return tx.SetDictionaryBody(ctx, net, row, dict) })
	}
	if err != nil {
		if derr := s.b.DeleteDictionary(context.WithoutCancel(ctx), net, row); derr != nil {
			return 0, 0, errors.Join(err, derr)
		}
		return 0, 0, fmt.Errorf("training dictionary %d: %w", id, err)
	}
	return id, len(samples), nil
}

// mainSmallFiles are the distinct files of dictionaryFileMin to dictionaryFileMax bytes that a
// repository's main's head holds: its tree walked a level at a time, and the files' sizes read from
// the repository's content index a batch at a time.
func (s *Store) mainSmallFiles(ctx context.Context, repo int64) ([]encoding.ID, error) {
	head, _, ok, err := s.ops(ctx).Head(ctx, repo, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, notFound("repository %d", repo)
	}
	tree, err := s.resolvedTree(ctx, repo, head)
	if err != nil {
		return nil, err
	}
	seen := map[encoding.ID]bool{}
	var all []encoding.ID
	if err := diffTrees(treeReader{ctx: ctx, s: s, repo: repo}, encoding.EmptyTreeID, tree, "", func(ch leafChange) error {
		if v := ch.After; v != nil && v.Kind == encoding.TreeValueFile && !seen[v.ID] {
			seen[v.ID] = true
			all = append(all, v.ID)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	var out []encoding.ID
	for start := 0; start < len(all); start += mainFileBatch {
		_, entries, err := s.contentEntries(ctx, repoScope(repo), encoding.KindFile, all[start:min(start+mainFileBatch, len(all))])
		if err != nil {
			return nil, err
		}
		for id, e := range entries {
			if e.Size >= dictionaryFileMin && e.Size <= dictionaryFileMax {
				out = append(out, id)
			}
		}
	}
	return out, nil
}

// mainFileBatch bounds the files whose sizes one read of the content index asks for.
const mainFileBatch = 10_000

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
