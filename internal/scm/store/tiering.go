package store

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"slices"
	"time"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// Storage tiering (docs/scm.md, "Tiering"): a network's packs that hold only what is read rarely are
// kept in the object store's cold class (Config.ColdClass), the rest in its standard one. A chunk is
// cold when no repository of the network has it on main's head, and every repository that holds it
// holds it pinned — main's history, an upload — and has since before ColdAfter: what a checkout, a
// build, or a review reads is never cold, nor what a workspace published lately. A tree is never
// cold: every walk of a commit reads its trees. Reads are the same either way, only dearer from cold.
//
// Compaction writes cold chunks, hot chunks, and trees in packs of their own, the cold ones in the
// cold class, and splits so a pack at least half of whose live bytes turned cold — files that left
// main's head, beside some still on it. The daily collection of the network's first repository then
// moves each compacted pack all of whose records turned cold to the cold class, and each cold one a
// record of which turned hot again — a file came back — to the standard class (tierPacks). Each
// pack's class is its row's (backend.Pack.Class), so a pass reads no object to know where one is.

const (
	// DefaultColdAfter is how long a pinned chunk stays hot after it was put or pinned: the
	// minimum an infrequent class bills an object for (S3's STANDARD_IA, GCS's NEARLINE).
	DefaultColdAfter = 30 * 24 * time.Hour
	// coldMinBytes is the smallest pack tiering moves to the cold class: such classes bill an
	// object as at least 128 KiB, and a small pack is compaction's to merge first.
	coldMinBytes = 1 << 20
	// tierBatch bounds the packs one collection moves between classes; the next goes on.
	tierBatch = 10_000
)

// tierScan decides a network's records' tiers: what every repository of the network has on main's
// head, read once a collection — the dear part, a walk of each head — and each repository's index.
type tierScan struct {
	cutoff contentindex.Day
	// headChunks are the chunks of every repository's head's files, and of its large trees.
	headChunks map[encoding.ID]bool
	views      []*contentView
}

// newTierScan reads what deciding the network's tiers needs; nil when tiering is off.
func (s *Store) newTierScan(ctx context.Context, net backend.Network, now time.Time) (*tierScan, error) {
	if s.coldClass == blob.Standard {
		return nil, nil
	}
	repos, err := s.ops(ctx).NetworkRepositories(ctx, net)
	if err != nil {
		return nil, err
	}
	ts := &tierScan{cutoff: contentindex.DayOf(now.Add(-s.coldAfter)), headChunks: map[encoding.ID]bool{}}
	for _, repo := range repos {
		head, err := s.mainPaths(ctx, repo)
		if err != nil {
			return nil, err
		}
		rv, err := s.contentView(ctx, repoScope(repo))
		if err != nil {
			return nil, err
		}
		ts.views = append(ts.views, rv)
		if err := rv.each(ctx, func(id encoding.ID, e contentindex.Entry) bool {
			_, onHead := head[id]
			if (e.Kind == encoding.KindFile && onHead) || e.Chunked {
				for _, c := range e.Chunks {
					ts.headChunks[c.ID] = true
				}
			}
			return true
		}); err != nil {
			return nil, err
		}
	}
	return ts, nil
}

// packTier is a pack's live bytes, hot and cold.
type packTier struct{ hot, cold int64 }

// mixed reports whether compaction splits a pack: at least half of its live bytes are cold, beside
// hot ones, and its cold part is worth a pack of its own.
func (pt packTier) mixed() bool {
	return pt.hot > 0 && pt.cold >= coldMinBytes && pt.cold*2 >= pt.hot+pt.cold
}

// tiers is a scan's verdict: each pack's tiers, and the cold chunks of the packs it was asked about.
type tiers struct {
	packs  map[uint64]packTier
	chunks map[encoding.ID]bool
}

// scan decides each record the network's index (v) locates, merging it in ID order against each
// repository's index, as both are kept: no map of every chunk is made. The cold chunks in the packs
// of within are kept.
func (ts *tierScan) scan(ctx context.Context, v *contentView, within map[uint64]bool) (*tiers, error) {
	out := &tiers{packs: map[uint64]packTier{}, chunks: map[encoding.ID]bool{}}
	var scanErr error
	type held struct {
		id encoding.ID
		e  contentindex.Entry
		ok bool
	}
	pulls := make([]func() (encoding.ID, contentindex.Entry, bool), len(ts.views))
	cur := make([]held, len(ts.views))
	for i, rv := range ts.views {
		next, stop := iter.Pull2(rv.all(ctx, &scanErr))
		defer stop()
		pulls[i] = next
		cur[i].id, cur[i].e, cur[i].ok = next()
	}
	err := v.each(ctx, func(id encoding.ID, e contentindex.Entry) bool {
		if !e.Located() {
			return true
		}
		cold := e.Kind == encoding.KindChunk && !ts.headChunks[id]
		holders := 0
		for i := range cur {
			for cur[i].ok && bytes.Compare(cur[i].id[:], id[:]) < 0 {
				cur[i].id, cur[i].e, cur[i].ok = pulls[i]()
			}
			if !cur[i].ok || cur[i].id != id || cur[i].e.Kind != encoding.KindChunk {
				continue
			}
			holders++
			if he := cur[i].e; !he.Pinned || he.Day >= ts.cutoff {
				cold = false
			}
		}
		cold = cold && holders > 0
		pt := out.packs[e.Loc.Pack]
		if n := packLocation(e.Loc).Footprint(); cold {
			pt.cold += n
		} else {
			pt.hot += n
		}
		out.packs[e.Loc.Pack] = pt
		if cold && within[e.Loc.Pack] {
			out.chunks[id] = true
		}
		return true
	})
	if err == nil {
		err = scanErr
	}
	return out, err
}

// all is the view's entries in ID order, a scan's error left in err.
func (v *contentView) all(ctx context.Context, err *error) iter.Seq2[encoding.ID, contentindex.Entry] {
	return func(yield func(encoding.ID, contentindex.Entry) bool) {
		if e := v.each(ctx, yield); e != nil && *err == nil {
			*err = e
		}
	}
}

// tierPacks moves the network's compacted packs to the class their records now belong in, up to
// tierBatch of them: the cold class those of cold records alone, the standard class a cold one that
// holds a hot record. Only the collection of the network's first repository moves them, so the
// network is decided once a day; the others leave it.
func (s *Store) tierPacks(ctx context.Context, repo int64, ts *tierScan, opts GCOptions, r *GCReport) error {
	if ts == nil {
		return nil
	}
	net, netScope, err := s.networkOf(ctx, repo)
	if err != nil {
		return err
	}
	repos, err := s.ops(ctx).NetworkRepositories(ctx, net)
	if err != nil {
		return err
	}
	if len(repos) == 0 || slices.Min(repos) != repo {
		return nil
	}
	v, err := s.contentView(ctx, netScope)
	if err != nil {
		return err
	}
	t, err := ts.scan(ctx, v, nil)
	if err != nil {
		return err
	}
	live, err := s.ops(ctx).LivePacks(ctx, net)
	if err != nil {
		return err
	}
	moved := 0
	for _, p := range live {
		pt, located := t.packs[p.Num]
		if !p.Compacted || !located {
			continue
		}
		var want blob.Class
		switch {
		case pt.hot > 0:
			want = blob.Standard
		case p.Size >= coldMinBytes:
			want = s.coldClass
		default:
			continue
		}
		if blob.Class(p.Class) == want {
			continue
		}
		if moved == tierBatch {
			r.TierMore = true
			break
		}
		moved++
		if want == blob.Standard {
			r.PacksWarmed++
		} else {
			r.PacksCooled++
			r.BytesCooled += p.Size
		}
		if opts.DryRun {
			continue
		}
		// The object first: should the row's update fail, the next pass finds the row as it was and
		// moves the object again, already there.
		if err := s.blobs.SetClass(ctx, packKey(net, p.ID), want); err != nil {
			return fmt.Errorf("pack %s: %w", p.ID, err)
		}
		if err := s.ops(ctx).SetPackClass(ctx, net, p.ID, int16(want)); err != nil {
			return err
		}
	}
	return nil
}
