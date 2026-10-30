package queue

import (
	"slices"
	"sort"
)

// edge is a submission's conflict with one ahead of it in the queue: path when they touch a path
// in common (PathConflict) — blockers, of which at most one lands — and otherwise their changes
// interact (Conflict).
type edge struct {
	id   int64
	path bool
}

// conflictGraph is, for each of a queue's submissions, its conflicts with the submissions ahead of
// it, by their reference footprints, in queue order: the candidates found through an index of every
// footprint (footprintIndex), each decided by the pairwise checks. Checking every pair, a decision
// was quadratic in the queue: a herd's 2,000 submissions held each landing batch for minutes.
func conflictGraph(order []*Submission, refs []*Footprint) [][]edge {
	x := newFootprintIndex()
	for i, f := range refs {
		if f != nil {
			x.add(i, f)
		}
	}
	out := make([][]edge, len(order))
	seen := make([]int, len(order))
	for i, f := range refs {
		if f == nil {
			continue
		}
		var ahead []int
		x.candidates(f, func(j int) {
			if j < i && seen[j] != i+1 {
				seen[j] = i + 1
				ahead = append(ahead, j)
			}
		})
		slices.Sort(ahead)
		for _, j := range ahead {
			switch {
			case PathConflict(refs[j], f):
				out[i] = append(out[i], edge{id: order[j].ID, path: true})
			case Conflict(refs[j], f):
				out[i] = append(out[i], edge{id: order[j].ID})
			}
		}
	}
	return out
}

// meeting is what a passed attempt meets first among the commits that landed on main since it was
// tested.
type meeting int

const (
	landable    meeting = iota // nothing: it lands
	unread                     // a place on main the queue has not read yet
	foreign                    // a commit landed outside the queue, or with no footprint recorded
	interacting                // a commit the queue landed whose change interacts with its own
)

// mainHistory is main's history as a decision checks passed attempts against it, indexed: the
// places after From it lacks, the commits landed outside the queue, and the footprints of those
// the queue landed (footprintIndex), so that an attempt is checked against the commits it may
// interact with, not every one that landed since it was tested.
type mainHistory struct {
	from  int64
	head  int64 // the head the history was read to
	bySeq map[int64]MainCommit
	// gaps are the places after from, to head, the history lacks, as ranges in order; foreign the
	// places of commits landed outside the queue, in order.
	gaps    [][2]int64
	foreign []int64
	landed  *footprintIndex // the queue's commits' footprints, by place
	fps     map[int64]*Footprint
	read    int // how many of Main's commits it read
}

func newMainHistory(s Snapshot) *mainHistory {
	h := &mainHistory{from: s.From, head: s.HeadSeq, bySeq: map[int64]MainCommit{}, landed: newFootprintIndex(),
		fps: map[int64]*Footprint{}, read: len(s.Main)}
	// The latest commit at a place is the one a decision reads.
	for _, m := range s.Main {
		h.bySeq[m.Seq] = m
	}
	places := make([]int64, 0, len(h.bySeq))
	for seq := range h.bySeq {
		places = append(places, seq)
	}
	slices.Sort(places)
	last := h.from
	for _, seq := range places {
		h.hold(seq, s.Landings)
		if seq <= h.from || seq > h.head {
			continue
		}
		if seq > last+1 {
			h.gaps = append(h.gaps, [2]int64{last + 1, seq - 1})
		}
		last = seq
	}
	if h.head > last {
		h.gaps = append(h.gaps, [2]int64{last + 1, h.head})
	}
	return h
}

// hold reads the commit at a place: landed outside the queue, or by it, with its footprint.
func (h *mainHistory) hold(seq int64, landings map[int64]*Footprint) {
	m := h.bySeq[seq]
	fp := landings[m.Submission]
	if m.Submission == 0 || fp == nil {
		h.foreign = append(h.foreign, seq)
		return
	}
	h.fps[seq] = fp
	h.landed.add(int(seq), fp)
}

// extend reads the commits Main gained since the history last read it — a batch's landing's
// (Snapshot.AfterLanding), after the head it was read to. A commit at a place it holds already,
// the latest at a place being the one a decision reads, has it read the history anew.
func (h *mainHistory) extend(s Snapshot) {
	for _, m := range s.Main[h.read:] {
		if _, held := h.bySeq[m.Seq]; held {
			*h = *newMainHistory(s)
			return
		}
	}
	for _, m := range s.Main[h.read:] {
		h.bySeq[m.Seq] = m
		h.hold(m.Seq, s.Landings)
	}
	h.read = len(s.Main)
	slices.Sort(h.foreign)
}

// meets is the first commit a passed attempt meets, in main's order, after the place it was tested
// on and to head, that it cannot land after — a place not read, a commit landed outside the queue,
// or one whose change interacts with its own — skipping its chain's; landable when none.
func (h *mainHistory) meets(a *Attempt, head int64) (int64, meeting) {
	base := a.BaseSeq
	if base >= head {
		return 0, landable
	}
	first, met := int64(0), landable
	consider := func(seq int64, m meeting) {
		if seq > base && seq <= head && (met == landable || seq < first) {
			first, met = seq, m
		}
	}
	// Not read: at or before From, or a gap.
	if base+1 <= h.from {
		consider(base+1, unread)
	}
	if i := sort.Search(len(h.gaps), func(i int) bool { return h.gaps[i][1] > base }); i < len(h.gaps) {
		consider(max(h.gaps[i][0], base+1), unread)
	}
	inChain := func(seq int64) bool {
		sub := h.bySeq[seq].Submission
		return sub != 0 && slices.Contains(a.Chain, sub)
	}
	for i := sort.Search(len(h.foreign), func(i int) bool { return h.foreign[i] > base }); i < len(h.foreign); i++ {
		if seq := h.foreign[i]; !inChain(seq) {
			consider(seq, foreign)
			break
		}
	}
	if a.Footprint == nil {
		return first, met
	}
	var maybe []int64
	seen := map[int64]bool{}
	h.landed.candidates(a.Footprint, func(k int) {
		seq := int64(k)
		if seq > base && seq <= head && !seen[seq] && (met == landable || seq < first) {
			seen[seq] = true
			maybe = append(maybe, seq)
		}
	})
	slices.Sort(maybe)
	for _, seq := range maybe {
		if !inChain(seq) && Conflict(h.fps[seq], a.Footprint) {
			consider(seq, interacting)
			break
		}
	}
	return first, met
}
