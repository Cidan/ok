package store

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// Landed CLs and main's log in segments (docs/hosted.md, "Segments, as built"). Nothing changes a
// CL once it has landed — no transition leaves StatusSubmitted, and an upload to it is refused —
// and main's log only grows, so compaction moves a landed CL, its patchsets, and the move of main
// that landed it out of the tail together. A read looks in the tail, then in the segments.
//
// segChangeLists' keys, by their first byte:
//
//	'n' number                  the CL (encodeSettled)
//	'c' change ID               its number
//	'p' commit ID, number       a patchset's commit
//	'a' author, NUL, number     its author
//	's' parent, number          a patchset uploaded stacked on CL parent
//
// segMainLog's:
//
//	'l' place                   the move (encodeMove)
//	'h' commit ID               the place main moved to it (uvarint)
//
// Numbers and places are big-endian, so keys of one prefix sort by them.

const (
	clRecord   = 'n'
	clByChange = 'c'
	clByCommit = 'p'
	clByAuthor = 'a'
	clByParent = 's'
	logMove    = 'l'
	logByHead  = 'h'
)

func be(n int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(n)) }

func clKey(kind byte, parts ...[]byte) []byte {
	k := []byte{kind}
	for _, p := range parts {
		k = append(k, p...)
	}
	return k
}

// trailingNumber is the number that ends a key.
func trailingNumber(k []byte) int64 { return int64(binary.BigEndian.Uint64(k[len(k)-8:])) }

// clPrefix is the part of a segChangeLists key a read names.
func clPrefix(k []byte) []byte {
	switch k[0] {
	case clByCommit:
		return k[:1+encoding.IDLen]
	case clByAuthor:
		return k[:bytes.IndexByte(k, 0)+1]
	case clByParent:
		return k[:9]
	case clByChange:
		return k
	}
	return k[:1]
}

// mainLogPrefix is the part of a segMainLog key a read names.
func mainLogPrefix(k []byte) []byte {
	if k[0] == logByHead {
		return k
	}
	return k[:1]
}

// settledRecords are a landed CL's records in segChangeLists, and its move's in segMainLog.
func settledRecords(sc backend.SettledChangeList) (cls, log []segment.Record, err error) {
	// Landed, so numbered, as is everything it was stacked on (sequence.go).
	provisional := sc.Number <= 0 || sc.Parent < 0
	for _, p := range sc.Patchsets {
		provisional = provisional || p.Parent < 0
	}
	if provisional {
		return nil, nil, fmt.Errorf("CL %d of ID %d is not numbered, nor is what it was stacked on", sc.Number, sc.ID)
	}
	n := be(sc.Number)
	if bytes.IndexByte([]byte(sc.Author), 0) >= 0 {
		return nil, nil, fmt.Errorf("CL %d's author holds a NUL: %q", sc.Number, sc.Author)
	}
	cls = append(cls,
		segment.Record{Key: clKey(clRecord, n), Value: encodeSettled(sc)},
		segment.Record{Key: clKey(clByChange, sc.ChangeID), Value: n},
		segment.Record{Key: clKey(clByAuthor, []byte(sc.Author), []byte{0}, n)})
	parents := map[int64]bool{}
	for _, p := range sc.Patchsets {
		cls = append(cls, segment.Record{Key: clKey(clByCommit, p.Commit[:], n)})
		if p.Parent != 0 && !parents[p.Parent] {
			parents[p.Parent] = true
			cls = append(cls, segment.Record{Key: clKey(clByParent, be(p.Parent), n)})
		}
	}
	h := sc.Landing
	log = append(log,
		segment.Record{Key: clKey(logMove, be(h.Seq)), Value: encodeMove(h, sc.LandedAt)},
		segment.Record{Key: clKey(logByHead, h.New[:]), Value: binary.AppendUvarint(nil, uint64(h.Seq))})
	return cls, log, nil
}

// recWriter writes a record's fields.
type recWriter []byte

func (w *recWriter) uint(v uint64)     { *w = binary.AppendUvarint(*w, v) }
func (w *recWriter) int(v int64)       { *w = binary.AppendVarint(*w, v) }
func (w *recWriter) bytes(b []byte)    { w.uint(uint64(len(b))); *w = append(*w, b...) }
func (w *recWriter) str(s string)      { w.bytes([]byte(s)) }
func (w *recWriter) id(id encoding.ID) { *w = append(*w, id[:]...) }
func (w *recWriter) time(t time.Time)  { w.int(t.UnixMicro()) }

// recReader reads them, remembering the first fault.
type recReader struct {
	b   []byte
	bad bool
}

func (r *recReader) uint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.bad = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *recReader) int() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.bad = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *recReader) bytes() []byte {
	l := r.uint()
	if r.bad || uint64(len(r.b)) < l {
		r.bad = true
		return nil
	}
	v := bytes.Clone(r.b[:l])
	r.b = r.b[l:]
	return v
}

func (r *recReader) str() string { return string(r.bytes()) }

// count reads how many of something follow, each at least a byte.
func (r *recReader) count() uint64 {
	n := r.uint()
	if n > uint64(len(r.b)) {
		r.bad = true
		return 0
	}
	return n
}

func (r *recReader) id() encoding.ID {
	if len(r.b) < encoding.IDLen {
		r.bad = true
		return encoding.ID{}
	}
	id := encoding.ID(r.b[:encoding.IDLen])
	r.b = r.b[encoding.IDLen:]
	return id
}

func (r *recReader) time() time.Time { return time.UnixMicro(r.int()) }

// recordVersion begins every record of these kinds.
const recordVersion = 2

// encodeSettled is a landed CL's record: its row, its patchsets, and its landing.
func encodeSettled(sc backend.SettledChangeList) []byte {
	w := recWriter{recordVersion}
	c := sc.ChangeList
	w.uint(uint64(c.ID))
	w.bytes(c.ChangeID)
	w.str(c.Author)
	w.uint(uint64(c.Status))
	w.uint(uint64(c.AbandonedFrom))
	w.uint(uint64(c.Parent))
	w.uint(uint64(c.CurrentPatchset))
	w.id(*c.Landed)
	w.str(c.SubmittedBy)
	w.str(c.Description)
	w.time(c.CreatedAt)
	w.time(c.UpdatedAt)
	w.uint(uint64(len(c.RollsBack)))
	for _, n := range c.RollsBack {
		w.uint(uint64(n))
	}
	for _, paths := range [][]string{c.RollbackPaths, c.RollbackLeft} {
		w.uint(uint64(len(paths)))
		for _, p := range paths {
			w.str(p)
		}
	}
	w.uint(uint64(len(sc.Patchsets)))
	for _, p := range sc.Patchsets {
		w.uint(uint64(p.Number))
		w.id(p.Commit)
		w.id(p.Base)
		w.uint(uint64(p.Parent))
		w.str(p.UploadedBy)
		w.time(p.UploadedAt)
	}
	w = append(w, encodeMove(sc.Landing, sc.LandedAt)...)
	return w
}

// decodeSettled reads CL number's record.
func decodeSettled(number int64, b []byte) (backend.SettledChangeList, error) {
	var sc backend.SettledChangeList
	if len(b) == 0 || b[0] != recordVersion {
		return sc, fmt.Errorf("CL %d's record: %w", number, segment.ErrCorrupt)
	}
	r := &recReader{b: b[1:]}
	c := &sc.ChangeList
	c.Number = number
	c.ID = int64(r.uint())
	c.ChangeID = r.bytes()
	c.Author = r.str()
	c.Status = int16(r.uint())
	c.AbandonedFrom = int16(r.uint())
	c.Parent = int64(r.uint())
	c.CurrentPatchset = int32(r.uint())
	landed := r.id()
	c.Landed = &landed
	c.SubmittedBy = r.str()
	c.Description = r.str()
	c.CreatedAt = r.time()
	c.UpdatedAt = r.time()
	for range r.count() {
		c.RollsBack = append(c.RollsBack, int64(r.uint()))
	}
	for _, paths := range []*[]string{&c.RollbackPaths, &c.RollbackLeft} {
		for range r.count() {
			*paths = append(*paths, r.str())
		}
	}
	for range r.count() {
		if r.bad {
			break
		}
		p := backend.Patchset{CL: c.ID, Number: int32(r.uint()), Commit: r.id(), Base: r.id(), Parent: int64(r.uint())}
		p.UploadedBy = r.str()
		p.UploadedAt = r.time()
		sc.Patchsets = append(sc.Patchsets, p)
	}
	sc.Landing, sc.LandedAt = readMove(r)
	if r.bad || len(r.b) != 0 {
		return sc, fmt.Errorf("CL %d's record: %w", number, segment.ErrCorrupt)
	}
	return sc, nil
}

// encodeMove is a move of main's record, its place in the key.
func encodeMove(h backend.HeadMove, at time.Time) []byte {
	var w recWriter
	w.uint(uint64(h.Seq))
	w.id(h.Old)
	w.id(h.New)
	w.uint(uint64(h.CL))
	w.uint(uint64(h.Patchset))
	w.str(h.By)
	w.time(at)
	return w
}

func readMove(r *recReader) (backend.HeadMove, time.Time) {
	h := backend.HeadMove{Seq: int64(r.uint()), Old: r.id(), New: r.id(), CL: int64(r.uint()), Patchset: int32(r.uint())}
	h.By = r.str()
	return h, r.time()
}

// settledChangeLists reads landed CLs numbered numbers from a repository's segments.
func (s *Store) settledChangeLists(ctx context.Context, repo int64, numbers []int64) (map[int64]backend.SettledChangeList, error) {
	out := map[int64]backend.SettledChangeList{}
	if len(numbers) == 0 {
		return out, nil
	}
	keys := make([][]byte, len(numbers))
	for i, n := range numbers {
		keys[i] = clKey(clRecord, be(n))
	}
	got, err := s.segmentGet(ctx, repoScope(repo), segChangeLists, keys)
	if err != nil {
		return nil, err
	}
	for _, n := range numbers {
		v, ok := got[string(clKey(clRecord, be(n)))]
		if !ok {
			continue
		}
		sc, err := decodeSettled(n, v)
		if err != nil {
			return nil, err
		}
		out[n] = sc
	}
	return out, nil
}

// settledByChange are the numbers of landed CLs of changes in a repository's segments.
func (s *Store) settledByChange(ctx context.Context, repo int64, changes [][]byte) ([]int64, error) {
	if len(changes) == 0 {
		return nil, nil
	}
	keys := make([][]byte, len(changes))
	for i, c := range changes {
		keys[i] = clKey(clByChange, c)
	}
	got, err := s.segmentGet(ctx, repoScope(repo), segChangeLists, keys)
	if err != nil {
		return nil, err
	}
	var out []int64
	for _, v := range got {
		if len(v) != 8 {
			return nil, fmt.Errorf("a change's CL in segments: %w", segment.ErrCorrupt)
		}
		out = append(out, int64(binary.BigEndian.Uint64(v)))
	}
	return out, nil
}

// settledByPrefix are the numbers ending the keys of a repository's segChangeLists that begin
// with one of prefixes.
func (s *Store) settledByPrefix(ctx context.Context, repo int64, prefixes [][]byte) ([]int64, error) {
	if len(prefixes) == 0 {
		return nil, nil
	}
	seen := map[int64]bool{}
	var out []int64
	err := s.segmentPrefixes(ctx, repoScope(repo), segChangeLists, prefixes, func(k, _ []byte) error {
		if n := trailingNumber(k); !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
		return nil
	})
	return out, err
}

// settledStackedOn are the (parent, CL) pairs of landed CLs with a patchset uploaded stacked on
// one of parents.
func (s *Store) settledStackedOn(ctx context.Context, repo int64, parents []int64) ([][2]int64, error) {
	if len(parents) == 0 {
		return nil, nil
	}
	prefixes := make([][]byte, len(parents))
	for i, p := range parents {
		prefixes[i] = clKey(clByParent, be(p))
	}
	var out [][2]int64
	err := s.segmentPrefixes(ctx, repoScope(repo), segChangeLists, prefixes, func(k, _ []byte) error {
		if len(k) == 17 {
			out = append(out, [2]int64{int64(binary.BigEndian.Uint64(k[1:9])), trailingNumber(k)})
		}
		return nil
	})
	return out, err
}

// settledNumbers are up to limit of the numbers of a repository's landed CLs in its segments,
// below before (0: any), by author ("": any), highest first.
func (s *Store) settledNumbers(ctx context.Context, repo, before int64, author string, limit int) ([]int64, error) {
	readers, err := s.segmentReaders(ctx, repoScope(repo), segChangeLists, true)
	if err != nil || len(readers) == 0 {
		return nil, err
	}
	prefix := []byte{clRecord}
	if author != "" {
		prefix = clKey(clByAuthor, []byte(author), []byte{0})
	}
	to := prefixEnd(prefix)
	if before > 0 {
		to = clKey(prefix[0], prefix[1:], be(before))
	}
	seen := map[int64]bool{}
	var out []int64
	err = scanSegments(ctx, readers, prefix, prefix, to, true, func(k, _ []byte) bool {
		if n := trailingNumber(k); !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
		return len(out) < limit
	})
	return out, err
}

// mainLog is up to limit of main's heads at places from to to, each with its place as its
// generation: the tail's, then the segments'.
func (s *Store) mainLog(ctx context.Context, repo, from, to int64, ascending bool, limit int) ([]backend.GenCommit, error) {
	tail, err := s.ops(ctx).MainLog(ctx, repo, from, to, ascending, limit)
	if err != nil {
		return nil, err
	}
	// Places are dense: a page the tail holds from its first place without a gap is whole.
	if len(tail) == limit {
		first, step := from, int64(1)
		if !ascending {
			first, step = to, -1
		}
		whole := true
		for i, g := range tail {
			whole = whole && g.Generation == first+int64(i)*step
		}
		if whole {
			return tail, nil
		}
	}
	readers, err := s.segmentReaders(ctx, repoScope(repo), segMainLog, true)
	if err != nil || len(readers) == 0 {
		return tail, err
	}
	have := make(map[int64]bool, len(tail))
	out := slices.Clone(tail)
	for _, g := range tail {
		have[g.Generation] = true
	}
	prefix := []byte{logMove}
	n := 0
	// A move's key is 'l' and its place: one byte more is just past place to.
	err = scanSegments(ctx, readers, prefix, clKey(logMove, be(from)), clKey(logMove, be(to), []byte{0}), !ascending, func(k, v []byte) bool {
		if len(k) == 9 {
			r := &recReader{b: v}
			h, _ := readMove(r)
			if !r.bad && !have[h.Seq] {
				have[h.Seq] = true
				out = append(out, backend.GenCommit{Commit: h.New, Generation: h.Seq})
			}
		}
		n++
		return n < limit
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(out, func(a, b backend.GenCommit) int {
		if ascending {
			return cmp.Compare(a.Generation, b.Generation)
		}
		return cmp.Compare(b.Generation, a.Generation)
	})
	return out[:min(limit, len(out))], nil
}

// headSeqOf is the place in main's history where main moved to commit, in the tail or the
// segments; ok is false when main never did.
func (s *Store) headSeqOf(ctx context.Context, q backend.Ops, repo int64, commit encoding.ID) (int64, bool, error) {
	seq, ok, err := q.HeadSeqOf(ctx, repo, commit)
	if err != nil || ok {
		return seq, ok, err
	}
	key := clKey(logByHead, commit[:])
	got, err := s.segmentGet(ctx, repoScope(repo), segMainLog, [][]byte{key})
	if err != nil {
		return 0, false, err
	}
	v, ok := got[string(key)]
	if !ok {
		return 0, false, nil
	}
	n, l := binary.Uvarint(v)
	if l <= 0 {
		return 0, false, fmt.Errorf("commit %s's place on main: %w", commit, segment.ErrCorrupt)
	}
	return int64(n), true, nil
}

// onMain reports whether a commit is on main: the root, or a commit some landing made main's
// head (main is linear, so that is all of it).
func (s *Store) onMain(ctx context.Context, q backend.Ops, repo int64, id encoding.ID) (bool, error) {
	if id == encoding.RootCommitID {
		return true, nil
	}
	_, ok, err := s.headSeqOf(ctx, q, repo, id)
	return ok, err
}
