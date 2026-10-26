package store

import (
	"context"
	"encoding/hex"
	"strconv"

	"github.com/klauspost/compress/zstd"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
)

// Deltas (docs/scm.md, "Deltas"): a whole file a client asks for naming a base — a version it holds,
// the file at the same path in the tree it had — goes as one frame of the delta from the base,
// compressed, where that is smaller than the file's chunks as stored. A delta is computed once on a
// replica, kept (Store.deltas), and so is the finding that none pays.

// DeltaMaxBytes bounds the files a delta is made of, and the bases it is made against.
const DeltaMaxBytes = 4 << 20

var deltaEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))

func deltaKey(repo int64, base, file encoding.ID) string {
	return strconv.FormatInt(repo, 10) + "/" + hex.EncodeToString(base[:]) + "/" + hex.EncodeToString(file[:])
}

// deltaFrames answers, of ranges, those it can with a delta: each a whole file at most DeltaMaxBytes
// naming a base the repository holds, of at most that too, whose compressed delta is smaller than
// the file's chunks as stored. It returns them by range; the rest are read as chunks. ms are the
// ranges' files' manifests.
func (s *Store) deltaFrames(ctx context.Context, repo int64, ranges []FileRange, ms map[encoding.ID]*manifest) (map[int]FileFrame, error) {
	type pair struct {
		i          int
		base, file encoding.ID
	}
	var want []pair
	for i, r := range ranges {
		m := ms[r.FileID]
		if r.Base == (encoding.ID{}) || r.Base == r.FileID || r.Offset != 0 || r.Length != 0 || m == nil || m.size == 0 || m.size > DeltaMaxBytes {
			continue
		}
		want = append(want, pair{i, r.Base, r.FileID})
	}
	if len(want) == 0 {
		return nil, nil
	}
	out := map[int]FileFrame{}
	frame := func(p pair, delta []byte) {
		out[p.i] = FileFrame{Range: p.i, FileID: p.file, Size: ms[p.file].size, Data: delta, RawLength: uint32(ms[p.file].size),
			Base: p.base, Last: true}
	}
	// What a replica found already; the rest are made from the files' and bases' bytes.
	var todo []pair
	for _, p := range want {
		d, ok := s.deltas.get(deltaKey(repo, p.base, p.file))
		switch {
		case !ok:
			todo = append(todo, p)
		case len(d) > 0:
			frame(p, d)
		}
	}
	if len(todo) == 0 {
		return out, nil
	}
	var bases []encoding.ID
	for _, p := range todo {
		bases = append(bases, p.base)
	}
	bms, err := s.manifests(ctx, repo, bases)
	if err != nil {
		return nil, err
	}
	var chunks []encoding.ID
	usable := todo[:0]
	for _, p := range todo {
		bm := bms[p.base]
		if bm == nil || bm.size > DeltaMaxBytes {
			continue // a base the repository does not hold, or too large: the file goes as chunks
		}
		usable = append(usable, p)
		for _, c := range bm.chunks {
			chunks = append(chunks, c.id)
		}
		for _, c := range ms[p.file].chunks {
			chunks = append(chunks, c.id)
		}
	}
	if len(usable) == 0 {
		return out, nil
	}
	// The chunks as stored are what a delta must beat; raw, they make each file.
	stored, err := s.storedChunks(ctx, repo, chunks)
	if err != nil {
		return nil, err
	}
	net, err := s.network(ctx, repo)
	if err != nil {
		return nil, err
	}
	raw := make(map[encoding.ID][]byte, len(stored))
	for id, c := range stored {
		if raw[id], err = pack.Decompress(s.dicts.of(net), id, c.codec, c.raw, c.data); err != nil {
			return nil, err
		}
	}
	whole := func(m *manifest) []byte {
		b := make([]byte, 0, m.size)
		for _, c := range m.chunks {
			b = append(b, raw[c.id]...)
		}
		return b
	}
	for _, p := range usable {
		m := ms[p.file]
		var storedLen int
		for _, c := range m.chunks {
			storedLen += len(stored[c.id].data)
		}
		d := deltaEncoder.EncodeAll(encoding.EncodeDelta(whole(bms[p.base]), whole(m)), nil)
		if len(d) >= storedLen {
			d = []byte{}
		}
		s.deltas.put(deltaKey(repo, p.base, p.file), d, len(d)+128)
		if len(d) > 0 {
			frame(p, d)
		}
	}
	return out, nil
}
