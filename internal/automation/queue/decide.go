// Package queue is ok automation's landing queue (docs/automation.md, "The landing
// queue"): it lands each submission once post has passed on a candidate that still holds for
// main. Decide is its rules, pure: from a repository's queue it says what to do next — prepare
// an attempt, release one to test, discard one that no longer holds, land, fail, or cancel. The
// Runner reads the queue, asks Decide, and does it, through the SCM and the store.
package queue

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// State is where an attempt stands.
type State int

const (
	Planning State = iota // its post plans are being planned
	Planned               // planned: its jobs wait for the queue to release them
	Testing               // its jobs run
	Passed                // every plan and job passed
	Failed                // a plan or a job did not pass, or the SCM refused a candidate
)

// Attempt is one of a submission's attempts: where it was prepared, what it speculates on, and
// where it stands.
type Attempt struct {
	ID      int64
	BaseSeq int64   // main's place when it was prepared
	Chain   []int64 // the earlier submissions it is tested on, in order
	// Active is false once it was discarded, landed, or failed.
	Active bool
	State  State
	Reason string // why it failed
	// Footprint is its change's, once its plans are planned: the union of its own CLs'.
	Footprint *Footprint
}

// Submission is an active submission.
type Submission struct {
	ID       int64
	CLs      []int64   // bottom first
	Attempts []Attempt // oldest first, every one
	// Landed are its CLs main has, though the queue has not recorded landing them.
	Landed []int64
}

// Outcome is how a submission that is no longer active ended.
type Outcome int

const (
	Landed Outcome = iota + 1
	Ended          // failed or canceled
)

// MainCommit is a commit main moved to.
type MainCommit struct {
	Seq        int64
	CL         int64 // 0 when automation has not seen which
	Submission int64 // the submission the queue landed it for; 0 when it did not
}

// Snapshot is a repository's landing queue.
type Snapshot struct {
	HeadSeq int64 // main's place now
	// Submissions are the active submissions, in queue order.
	Submissions []Submission
	// Finished are the submissions active attempts speculate on that are no longer active, and
	// FinishedCLs their CLs.
	Finished    map[int64]Outcome
	FinishedCLs map[int64][]int64
	// Main is main's history after From, as automation has seen it.
	From int64
	Main []MainCommit
	// Landings are the footprints Main's submissions landed with.
	Landings map[int64]*Footprint
	// Depth bounds how many earlier submissions an attempt speculates on.
	Depth int
}

// Kind is what an action does.
type Kind int

const (
	Create  Kind = iota + 1 // prepare an attempt on main's head, on Chain
	Release                 // queue an attempt's jobs
	Discard                 // end an attempt that no longer holds for main
	Land                    // land a submission on its passed attempt
	Adopt                   // record a landing main already has
	Fail                    // fail a submission
	Cancel                  // cancel a submission
	Note                    // say what a submission waits for
)

var kindNames = map[Kind]string{Create: "create", Release: "release", Discard: "discard", Land: "land", Adopt: "adopt",
	Fail: "fail", Cancel: "cancel", Note: "note"}

func (k Kind) String() string {
	if n, ok := kindNames[k]; ok {
		return n
	}
	return fmt.Sprintf("kind(%d)", int(k))
}

// Action is one thing the queue does.
type Action struct {
	Kind       Kind
	Submission int64
	Attempt    int64
	Chain      []int64
	Reason     string // Discard's, Fail's, and Cancel's reason; Note's note
}

// DefaultDepth is how many earlier submissions an attempt speculates on at most.
const DefaultDepth = 8

// AfterLanding is the queue as the next decision would read it once a submission lands on an
// attempt: main moved by its CLs, each recorded as the submission's; the submission finished,
// landed with the attempt's footprint. A batch's next landing is decided on it, so a batch lands
// exactly what deciding again after each landing would.
func (s Snapshot) AfterLanding(submission, attempt int64) Snapshot {
	out := s
	out.Submissions = nil
	out.Main = slices.Clone(s.Main)
	out.Finished = maps.Clone(s.Finished)
	out.FinishedCLs = maps.Clone(s.FinishedCLs)
	out.Landings = maps.Clone(s.Landings)
	if out.Finished == nil {
		out.Finished = map[int64]Outcome{}
	}
	if out.FinishedCLs == nil {
		out.FinishedCLs = map[int64][]int64{}
	}
	if out.Landings == nil {
		out.Landings = map[int64]*Footprint{}
	}
	for _, e := range s.Submissions {
		if e.ID != submission {
			out.Submissions = append(out.Submissions, e)
			continue
		}
		for _, c := range e.CLs {
			out.HeadSeq++
			out.Main = append(out.Main, MainCommit{Seq: out.HeadSeq, CL: c, Submission: e.ID})
		}
		out.Finished[e.ID], out.FinishedCLs[e.ID] = Landed, e.CLs
		for _, a := range e.Attempts {
			if a.ID == attempt && a.Footprint != nil {
				out.Landings[e.ID] = a.Footprint
			}
		}
	}
	return out
}

// Batch is the landings to make at once: the first Decide says, then each next that deciding on
// the queue after the ones before it says, until one says anything but a landing, or max are
// taken. It decides once, in queue order, going on after each landing from the submission behind
// it, on the queue after the landing: a landing moves main and ends its submission, which changes
// nothing ahead of it that a decision behind it reads, and lands nothing ahead of it — what waited
// there for something ahead of it still does, and main moving only adds to what an attempt there
// is checked against.
func Batch(s Snapshot, max int) []Action {
	d := newDecider(s)
	var out []Action
	for len(out) < max {
		a, ok := d.nextLanding()
		if !ok || a.Kind != Land {
			break
		}
		out = append(out, a)
		d.landed(a.Submission, a.Attempt)
	}
	return out
}

// LandingStep is what a step that lands does, of what Decide said on s (acts, a landing first): the
// batch it lands (Batch), and the rest, done beside the landing — every other submission's actions
// but an adoption, which waits for the next step. The batch's own submissions do nothing else.
func LandingStep(s Snapshot, acts []Action, max int) (batch, rest []Action) {
	batch = Batch(s, max)
	landing := map[int64]bool{}
	for _, a := range batch {
		landing[a.Submission] = true
	}
	for _, a := range acts {
		if a.Kind != Land && a.Kind != Adopt && !landing[a.Submission] {
			rest = append(rest, a)
		}
	}
	return batch, rest
}

// Decide says what a repository's landing queue does next: landings (and adoptions) first, in
// queue order, then everything else. Landing moves main, so a caller acts on the first landing
// and asks again.
func Decide(s Snapshot) []Action {
	d := newDecider(s)
	var lands, rest []Action
	for d.next < len(d.order) {
		for _, a := range d.decideNext() {
			if a.Kind == Land || a.Kind == Adopt {
				lands = append(lands, a)
			} else {
				rest = append(rest, a)
			}
		}
	}
	return append(lands, rest...)
}

type decider struct {
	s Snapshot
	// order is the queue's submissions as the decider was made on it, next the place of the next
	// to decide, and removed those a batch landed since (Batch).
	order   []*Submission
	next    int
	removed map[int64]bool
	active  map[int64]*Submission
	// leaving are the active submissions this decision ends: failed or canceled (Ended), or
	// adopted (Landed). Later submissions see them as ended already.
	leaving map[int64]Outcome
	pos     map[int64]int
	// wanted is each submission's chain: the earlier active submissions it conflicts with,
	// through each other, that can all land.
	wanted map[int64][]int64
	known  map[int64]bool // wanted is decided
	ref    map[int64]*Footprint
	// unplanned is the first submission decided that is neither known nor leaving, if any: every
	// one behind it waits for it.
	unplanned    int64
	hasUnplanned bool
	// conflicts are each submission's conflicts with those ahead of it (conflictGraph), and main
	// the history passed attempts are checked against, read the first time one is.
	conflicts [][]edge
	main      *mainHistory
}

func newDecider(s Snapshot) *decider {
	d := &decider{s: s, active: map[int64]*Submission{}, leaving: map[int64]Outcome{}, pos: map[int64]int{},
		wanted: map[int64][]int64{}, known: map[int64]bool{}, ref: map[int64]*Footprint{}, removed: map[int64]bool{}}
	if d.s.Depth <= 0 {
		d.s.Depth = DefaultDepth
	}
	refs := make([]*Footprint, len(s.Submissions))
	for i := range s.Submissions {
		e := &s.Submissions[i]
		d.order = append(d.order, e)
		d.active[e.ID], d.pos[e.ID] = e, i
		d.ref[e.ID] = reference(e)
		refs[i] = d.ref[e.ID]
	}
	d.conflicts = conflictGraph(d.order, refs)
	return d
}

// decideNext decides the next submission in queue order.
func (d *decider) decideNext() []Action {
	i := d.next
	d.next++
	e := d.order[i]
	acts := d.decide(i)
	if _, gone := d.leaving[e.ID]; !gone && !d.known[e.ID] && !d.hasUnplanned {
		d.unplanned, d.hasUnplanned = e.ID, true
	}
	return acts
}

// nextLanding decides on, in queue order from where it stopped, until a submission lands or is
// adopted, and returns that action; false when none is.
func (d *decider) nextLanding() (Action, bool) {
	for d.next < len(d.order) {
		for _, a := range d.decideNext() {
			if a.Kind == Land || a.Kind == Adopt {
				return a, true
			}
		}
	}
	return Action{}, false
}

// landed has the decider go on deciding on the queue after a submission lands on an attempt
// (Snapshot.AfterLanding).
func (d *decider) landed(submission, attempt int64) {
	d.s = d.s.AfterLanding(submission, attempt)
	d.removed[submission] = true
	delete(d.active, submission)
	if d.main != nil {
		d.main.extend(d.s)
	}
}

// reference is the footprint the queue groups a submission by: its latest attempt prepared on
// main alone that has been planned. Speculative attempts' footprints change with their chains,
// so grouping by them could chase itself; which attempt lands is checked against its own.
func reference(e *Submission) *Footprint {
	for i := len(e.Attempts) - 1; i >= 0; i-- {
		if a := e.Attempts[i]; len(a.Chain) == 0 && a.Footprint != nil {
			return a.Footprint
		}
	}
	return nil
}

// current is a submission's active attempt: the latest, when there are several.
func current(e *Submission) *Attempt {
	for i := len(e.Attempts) - 1; i >= 0; i-- {
		if e.Attempts[i].Active {
			return &e.Attempts[i]
		}
	}
	return nil
}

func (d *decider) decide(i int) []Action {
	e := d.order[i]
	cur := current(e)
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
			Reason: fmt.Sprintf("%s landed without the landing queue", clList(e.Landed))}}
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
	if d.hasUnplanned {
		return []Action{note("waiting for %s ahead of it to be planned", d.name(d.unplanned))}
	}

	// Its chain: the earlier submissions it conflicts with, and theirs, but those that change
	// a path it changes (blockers: at most one of them lands, and the earlier one is predicted
	// to) and any that change a path an earlier chain member changes.
	var blockers []int64
	chain := map[int64]bool{}
	for _, c := range d.conflicts[i] {
		if _, gone := d.leaving[c.id]; gone || d.removed[c.id] {
			continue
		}
		if c.path {
			blockers = append(blockers, c.id)
			continue
		}
		chain[c.id] = true
		for _, t := range d.wanted[c.id] {
			chain[t] = true
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
	// What landed on main since it was tested: the first commit it meets, in main's order, that it
	// cannot land after — one not read yet, one landed outside the queue, or one whose change
	// interacts with its own — but its chain's.
	if d.main == nil {
		d.main = newMainHistory(d.s)
	}
	seq, met := d.main.meets(cur, d.s.HeadSeq)
	switch met {
	case unread:
		return []Action{note("passed; reading what landed on main since it was tested")}
	case foreign, interacting:
		landed := "a commit"
		if m := d.main.bySeq[seq]; m.CL > 0 {
			landed = fmt.Sprintf("CL %d", m.CL)
		}
		reason := fmt.Sprintf("%s landed on main without the landing queue since it was tested", landed)
		if met == interacting {
			reason = fmt.Sprintf("%s landed on main since it was tested, and their changes interact", landed)
		}
		return create([]Action{{Kind: Discard, Submission: e.ID, Attempt: cur.ID, Reason: reason}})
	}
	return []Action{{Kind: Land, Submission: e.ID, Attempt: cur.ID}}
}

// invalid says why an attempt no longer holds for main — a submission it speculates on ended,
// one it must include is not in its chain, or it speculates on one it cannot land with — or ""
// when it holds.
func (d *decider) invalid(a *Attempt, wanted, blockers []int64) string {
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
func (d *decider) outcome(id int64) Outcome {
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
func (d *decider) realized(a *Attempt) bool {
	for _, c := range a.Chain {
		if d.outcome(c) != Landed {
			return false
		}
	}
	return true
}

// pending are the submissions of a chain still active.
func (d *decider) pending(chain []int64) []int64 {
	var out []int64
	for _, c := range chain {
		if d.outcome(c) == 0 {
			out = append(out, c)
		}
	}
	return out
}

// plural reports whether a submission is several CLs.
func (d *decider) plural(id int64) bool {
	if s := d.active[id]; s != nil {
		return len(s.CLs) > 1
	}
	return len(d.s.FinishedCLs[id]) > 1
}

// verb picks the verb's form agreeing with submissions: one with one CL, or several.
func (d *decider) verb(ids []int64, one, many string) string {
	if len(ids) == 1 && !d.plural(ids[0]) {
		return one
	}
	return many
}

// onTop words the submissions an attempt speculates on: "" when none.
func (d *decider) onTop(chain []int64) string {
	if len(chain) == 0 {
		return ""
	}
	return " on top of " + d.names(chain)
}

// name words a submission by its CLs.
func (d *decider) name(id int64) string {
	if s := d.active[id]; s != nil {
		return clList(s.CLs)
	}
	if cls := d.s.FinishedCLs[id]; len(cls) > 0 {
		return clList(cls)
	}
	return fmt.Sprintf("submission %d", id)
}

// names words several submissions.
func (d *decider) names(ids []int64) string {
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
func clList(cls []int64) string {
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
