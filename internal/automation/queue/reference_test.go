package queue

// The landing queue's decision as it was before conflictGraph and mainHistory: every submission
// checked against every earlier one, and a passed attempt against every commit since it was
// tested, deciding again after each landing of a batch. The equivalence tests hold Decide and
// Batch to it.

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// oldBatch is the landings to make at once: the first oldDecide says, then each next that deciding on
// the queue after the ones before it says, until one says anything but a landing, or max are
// taken.
func oldBatch(s Snapshot, max int) []Action {
	var out []Action
	for len(out) < max {
		acts := oldDecide(s)
		if len(acts) == 0 || acts[0].Kind != Land {
			break
		}
		out = append(out, acts[0])
		s = s.AfterLanding(acts[0].Submission, acts[0].Attempt)
	}
	return out
}

// oldDecide says what a repository's landing queue does next: landings (and adoptions) first, in
// queue order, then everything else. Landing moves main, so a caller acts on the first landing
// and asks again.
func oldDecide(s Snapshot) []Action {
	d := &oldDecider{s: s, active: map[int64]*Submission{}, leaving: map[int64]Outcome{}, pos: map[int64]int{},
		wanted: map[int64][]int64{}, known: map[int64]bool{}, ref: map[int64]*Footprint{}}
	if d.s.Depth <= 0 {
		d.s.Depth = DefaultDepth
	}
	for i := range s.Submissions {
		e := &s.Submissions[i]
		d.active[e.ID], d.pos[e.ID] = e, i
		d.ref[e.ID] = oldReference(e)
	}
	var lands, rest []Action
	for i := range s.Submissions {
		for _, a := range d.decide(i) {
			if a.Kind == Land || a.Kind == Adopt {
				lands = append(lands, a)
			} else {
				rest = append(rest, a)
			}
		}
	}
	return append(lands, rest...)
}

type oldDecider struct {
	s      Snapshot
	active map[int64]*Submission
	// leaving are the active submissions this decision ends: failed or canceled (Ended), or
	// adopted (Landed). Later submissions see them as ended already.
	leaving map[int64]Outcome
	pos     map[int64]int
	// wanted is each submission's chain: the earlier active submissions it conflicts with,
	// through each other, that can all land.
	wanted map[int64][]int64
	known  map[int64]bool // wanted is decided
	ref    map[int64]*Footprint
}

// reference is the footprint the queue groups a submission by: its latest attempt prepared on
// main alone that has been planned. Speculative attempts' footprints change with their chains,
// so grouping by them could chase itself; which attempt lands is checked against its own.
func oldReference(e *Submission) *Footprint {
	for i := len(e.Attempts) - 1; i >= 0; i-- {
		if a := e.Attempts[i]; len(a.Chain) == 0 && a.Footprint != nil {
			return a.Footprint
		}
	}
	return nil
}

// current is a submission's active attempt: the latest, when there are several.
func oldCurrent(e *Submission) *Attempt {
	for i := len(e.Attempts) - 1; i >= 0; i-- {
		if e.Attempts[i].Active {
			return &e.Attempts[i]
		}
	}
	return nil
}

func (d *oldDecider) decide(i int) []Action {
	e := &d.s.Submissions[i]
	cur := oldCurrent(e)
	note := func(format string, args ...any) Action {
		return Action{Kind: Note, Submission: e.ID, Reason: fmt.Sprintf(format, args...)}
	}

	// A CL that landed without the queue recording it: the queue landed it and lost the record
	// (it landed only on a passed attempt), or it landed some other way.
	if len(e.Landed) > 0 {
		if len(e.Landed) == len(e.CLs) && cur != nil && cur.State == Passed {
			d.leaving[e.ID] = Landed
			return []Action{{Kind: Adopt, Submission: e.ID, Attempt: cur.ID}}
		}
		d.leaving[e.ID] = Ended
		return []Action{{Kind: Cancel, Submission: e.ID,
			Reason: fmt.Sprintf("%s landed without the landing queue", oldClList(e.Landed))}}
	}
	if len(e.Attempts) == 0 || (cur == nil && d.ref[e.ID] == nil) {
		return []Action{{Kind: Create, Submission: e.ID, Chain: []int64{}}, note("planning post")}
	}
	if cur != nil && cur.State == Failed && d.realized(cur) {
		d.leaving[e.ID] = Ended
		return []Action{{Kind: Fail, Submission: e.ID, Reason: cur.Reason}}
	}
	ref := d.ref[e.ID]
	if ref == nil {
		return []Action{note("planning post")}
	}
	var earlier []*Submission
	for j := range d.s.Submissions[:i] {
		s := &d.s.Submissions[j]
		if _, gone := d.leaving[s.ID]; gone {
			continue
		}
		if !d.known[s.ID] {
			return []Action{note("waiting for %s ahead of it to be planned", d.name(s.ID))}
		}
		earlier = append(earlier, s)
	}

	// Its chain: the earlier submissions it conflicts with, and theirs, but those that change
	// a path it changes (blockers: at most one of them lands, and the earlier one is predicted
	// to) and any that change a path an earlier chain member changes.
	var blockers []int64
	chain := map[int64]bool{}
	for _, s := range earlier {
		switch {
		case PathConflict(d.ref[s.ID], ref):
			blockers = append(blockers, s.ID)
		case Conflict(d.ref[s.ID], ref):
			chain[s.ID] = true
			for _, t := range d.wanted[s.ID] {
				chain[t] = true
			}
		}
	}
	for _, b := range blockers {
		delete(chain, b)
	}
	var ordered []int64
	for id := range chain {
		ordered = append(ordered, id)
	}
	slices.SortFunc(ordered, func(a, b int64) int { return d.pos[a] - d.pos[b] })
	var wanted []int64
	for _, id := range ordered {
		if !slices.ContainsFunc(wanted, func(k int64) bool { return PathConflict(d.ref[id], d.ref[k]) }) {
			wanted = append(wanted, id)
		}
	}
	d.wanted[e.ID], d.known[e.ID] = wanted, true

	create := func(pre []Action) []Action {
		if len(wanted) > d.s.Depth {
			return append(pre, note("waiting: %d submissions ahead of it conflict with it, and the queue speculates on at most %d",
				len(wanted), d.s.Depth))
		}
		return append(pre, Action{Kind: Create, Submission: e.ID, Chain: wanted}, note("planning post%s", d.onTop(wanted)))
	}
	if cur == nil {
		return create(nil)
	}
	if reason := d.invalid(cur, wanted, blockers); reason != "" {
		return create([]Action{{Kind: Discard, Submission: e.ID, Attempt: cur.ID, Reason: reason}})
	}
	switch cur.State {
	case Planning:
		return []Action{note("planning post%s", d.onTop(cur.Chain))}
	case Planned:
		return []Action{{Kind: Release, Submission: e.ID, Attempt: cur.ID}, note("testing%s", d.onTop(cur.Chain))}
	case Testing:
		return []Action{note("testing%s", d.onTop(cur.Chain))}
	case Failed:
		waiting := d.pending(cur.Chain)
		return []Action{note("post failed%s: it fails if %s %s", d.onTop(cur.Chain), d.names(waiting),
			d.verb(waiting, "lands", "land"))}
	}
	// Passed.
	if waiting := d.pending(cur.Chain); len(waiting) > 0 {
		return []Action{note("passed; waiting for %s to land first", d.names(waiting))}
	}
	if len(blockers) > 0 {
		return []Action{note("passed; waiting for %s, which %s the same files, to finish", d.names(blockers),
			d.verb(blockers, "changes", "change"))}
	}
	have := map[int64]MainCommit{}
	for _, m := range d.s.Main {
		have[m.Seq] = m
	}
	for seq := cur.BaseSeq + 1; seq <= d.s.HeadSeq; seq++ {
		m, ok := have[seq]
		if !ok || seq <= d.s.From {
			return []Action{note("passed; reading what landed on main since it was tested")}
		}
		if slices.Contains(cur.Chain, m.Submission) && m.Submission != 0 {
			continue
		}
		landed := "a commit"
		if m.CL > 0 {
			landed = fmt.Sprintf("CL %d", m.CL)
		}
		fp := d.s.Landings[m.Submission]
		switch {
		case m.Submission == 0 || fp == nil:
			return create([]Action{{Kind: Discard, Submission: e.ID, Attempt: cur.ID,
				Reason: fmt.Sprintf("%s landed on main without the landing queue since it was tested", landed)}})
		case Conflict(fp, cur.Footprint):
			return create([]Action{{Kind: Discard, Submission: e.ID, Attempt: cur.ID,
				Reason: fmt.Sprintf("%s landed on main since it was tested, and their changes interact", landed)}})
		}
	}
	return []Action{{Kind: Land, Submission: e.ID, Attempt: cur.ID}}
}

// invalid says why an attempt no longer holds for main — a submission it speculates on ended,
// one it must include is not in its chain, or it speculates on one it cannot land with — or ""
// when it holds.
func (d *oldDecider) invalid(a *Attempt, wanted, blockers []int64) string {
	for _, c := range a.Chain {
		if o := d.outcome(c); o != 0 && o != Landed {
			return fmt.Sprintf("%s, which it was tested on, did not land", d.name(c))
		}
	}
	for _, w := range wanted {
		if !slices.Contains(a.Chain, w) {
			pronoun := "it"
			if d.plural(w) {
				pronoun = "them"
			}
			return fmt.Sprintf("%s, ahead of it, %s what it changes: it is tested on top of %s now", d.name(w),
				d.verb([]int64{w}, "changes", "change"), pronoun)
		}
	}
	for _, b := range blockers {
		if slices.Contains(a.Chain, b) {
			return fmt.Sprintf("%s changes the same files: it cannot land with it", d.name(b))
		}
	}
	return ""
}

// outcome is how a submission ended: 0 while it is active (and not ending now).
func (d *oldDecider) outcome(id int64) Outcome {
	if o, ok := d.leaving[id]; ok {
		return o
	}
	if d.active[id] != nil {
		return 0
	}
	if o, ok := d.s.Finished[id]; ok {
		return o
	}
	return Ended
}

// realized reports whether every submission an attempt speculates on has landed.
func (d *oldDecider) realized(a *Attempt) bool {
	for _, c := range a.Chain {
		if d.outcome(c) != Landed {
			return false
		}
	}
	return true
}

// pending are the submissions of a chain still active.
func (d *oldDecider) pending(chain []int64) []int64 {
	var out []int64
	for _, c := range chain {
		if d.outcome(c) == 0 {
			out = append(out, c)
		}
	}
	return out
}

// plural reports whether a submission is several CLs.
func (d *oldDecider) plural(id int64) bool {
	if s := d.active[id]; s != nil {
		return len(s.CLs) > 1
	}
	return len(d.s.FinishedCLs[id]) > 1
}

// verb picks the verb's form agreeing with submissions: one with one CL, or several.
func (d *oldDecider) verb(ids []int64, one, many string) string {
	if len(ids) == 1 && !d.plural(ids[0]) {
		return one
	}
	return many
}

// onTop words the submissions an attempt speculates on: "" when none.
func (d *oldDecider) onTop(chain []int64) string {
	if len(chain) == 0 {
		return ""
	}
	return " on top of " + d.names(chain)
}

// name words a submission by its CLs.
func (d *oldDecider) name(id int64) string {
	if s := d.active[id]; s != nil {
		return oldClList(s.CLs)
	}
	if cls := d.s.FinishedCLs[id]; len(cls) > 0 {
		return oldClList(cls)
	}
	return fmt.Sprintf("submission %d", id)
}

// names words several submissions.
func (d *oldDecider) names(ids []int64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = d.name(id)
	}
	if len(parts) > 1 {
		return strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1]
	}
	return strings.Join(parts, "")
}

// clList words CLs: "CL 12", "CLs 12 and 13", "CLs 12, 13, and 14".
func oldClList(cls []int64) string {
	n := make([]string, len(cls))
	for i, c := range cls {
		n[i] = strconv.FormatInt(c, 10)
	}
	switch len(n) {
	case 0:
		return "no CLs"
	case 1:
		return "CL " + n[0]
	case 2:
		return "CLs " + n[0] + " and " + n[1]
	}
	return "CLs " + strings.Join(n[:len(n)-1], ", ") + ", and " + n[len(n)-1]
}
