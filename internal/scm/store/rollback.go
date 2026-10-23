package store

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// RollbackReason is why the SCM refuses a rollback.
type RollbackReason int

const (
	RollbackNotLanded   RollbackReason = 1 // the CL has not landed
	RollbackUnderway    RollbackReason = 2 // an open or draft rollback CL already rolls it back
	RollbackNothingLeft RollbackReason = 3 // main has none of its changes
	RollbackAllChanged  RollbackReason = 4 // main changed every path it changed since
)

// RollbackRejection is a rollback the SCM refuses.
type RollbackRejection struct {
	Number  int64
	Reason  RollbackReason
	Message string
	Paths   []string // RollbackAllChanged: the paths main changed since
}

func (r *RollbackRejection) Error() string { return "rollback refused: " + r.Message }

// RollbackResult is a rollback CL the SCM created, and the last event it wrote.
type RollbackResult struct {
	ChangeList *ChangeList
	Event      *scmpb.Event
}

// landedCommit is a CL's landing on main: the commit it landed as, its parent on main, and
// its place in main's history.
type landedCommit struct {
	number  int64
	seq     int64
	commit  encoding.ID
	parent  encoding.ID
	title   string
	creator string
}

// Rollback creates a CL that rolls back a landed CL (docs/scm.md, "Rollbacks"), with
// the CLs landed above it in its stack: each landing's inverse, from the top down, applied to
// main's head. A path main changed after the CLs landed is left as main has it, and named. The
// CL is actor's, open for review, with reviewers and the authors of the CLs it rolls back
// assigned; its description names what it rolls back and why.
func (s *Store) Rollback(ctx context.Context, repo, number int64, actor, reason string, reviewers []string) (*RollbackResult, error) {
	reason = strings.TrimSpace(reason)
	switch {
	case actor == "":
		return nil, invalid("a rollback is someone's")
	case reason == "":
		return nil, invalid("a rollback says why")
	}
	var out *RollbackResult
	err := s.inTx(ctx, func(tx *txn) error {
		out = nil
		if err := s.repositoryExists(ctx, tx, repo); err != nil {
			return err
		}
		landed, err := s.rollbackStack(ctx, tx, repo, number)
		if err != nil {
			return err
		}
		numbers := make([]int64, len(landed))
		for i, l := range landed {
			numbers[i] = l.number
		}
		// The CLs rolled back are locked, so two rollbacks of one CL are ordered and the
		// second finds the first.
		if _, err := lockChangeLists(ctx, tx, repo, numbers); err != nil {
			return err
		}
		underway, ok, err := tx.RollingBack(ctx, repo, numbers, []int16{int16(StatusDraft), int16(StatusOpen)})
		if err != nil {
			return err
		}
		if ok {
			return &RollbackRejection{Number: number, Reason: RollbackUnderway,
				Message: fmt.Sprintf("CL %d already rolls it back (review it, or abandon it and roll back again)", underway)}
		}

		head, _, _, err := tx.Head(ctx, repo, false)
		if err != nil {
			return err
		}
		lc := &landCommits{ctx: ctx, s: s, repo: repo, new: map[encoding.ID]*encoding.Commit{}}
		ts := s.pendingTrees(ctx, repo)
		tree, left, err := unapply(lc, ts, landed, head)
		if err != nil {
			return err
		}
		hTree, _, err := lc.tree(head)
		if err != nil {
			return err
		}
		var paths []string
		if err := diffTrees(ts, hTree, tree, "", func(c leafChange) error {
			paths = append(paths, c.Path)
			return nil
		}); err != nil {
			return err
		}
		if len(paths) == 0 {
			if len(left) > 0 {
				return &RollbackRejection{Number: number, Reason: RollbackAllChanged, Paths: left,
					Message: fmt.Sprintf("main changed every path %s changed after it landed: %s (roll it back by hand with jj, and upload that as a CL)",
						clNames(numbers), strings.Join(left, ", "))}
			}
			return &RollbackRejection{Number: number, Reason: RollbackNothingLeft,
				Message: fmt.Sprintf("main has none of %s's changes to roll back", clNames(numbers))}
		}

		var change encoding.ChangeID
		if _, err := rand.Read(change[:]); err != nil {
			return err
		}
		at := time.Now()
		commit := &encoding.Commit{
			Parents:        []encoding.ID{head},
			RootTree:       encoding.Merge[encoding.ID]{tree},
			ConflictLabels: encoding.Merge[string]{""},
			ChangeID:       change,
			Description:    rollbackDescription(landed, reason),
			Author:         s.actorSignature(actor, at),
			Committer:      s.actorSignature(actor, at),
		}
		id, err := encoding.IDOf(commit)
		if err != nil {
			return err
		}
		gen, err := generation(ctx, tx, repo, head)
		if err != nil {
			return err
		}
		if err := ts.insert(ctx, tx, []encoding.ID{tree}); err != nil {
			return err
		}
		if err := s.insertCommits(ctx, tx, repo, []*encoding.Commit{commit}, []int64{gen + 1}, actor); err != nil {
			return err
		}
		if err := s.pin(ctx, tx, repo, []ref{{table: tCommit, id: id}}); err != nil {
			if errors.Is(err, errMissing) {
				return fmt.Errorf("%w: %v", errRace, err)
			}
			return err
		}

		// The CL's number comes from the repository's counter, the last lock before the event
		// counter, as an upload's does.
		cl, err := tx.NextChangeListNumbers(ctx, repo, 1)
		if err != nil {
			return err
		}
		if left == nil {
			left = []string{}
		}
		if err := tx.InsertChangeList(ctx, repo, backend.ChangeList{Number: cl, ChangeID: change[:], Author: actor,
			Status: int16(StatusOpen), CurrentPatchset: 1, Description: commit.Description,
			RollsBack: numbers, RollbackPaths: paths, RollbackLeft: left}); err != nil {
			return err
		}
		if err := tx.InsertPatchset(ctx, repo, backend.Patchset{CL: cl, Number: 1, Commit: id, Base: head, UploadedBy: actor}); err != nil {
			return err
		}
		var assign []string
		for _, r := range reviewers {
			if r != actor && !slices.Contains(assign, r) {
				assign = append(assign, r)
			}
		}
		for _, l := range landed {
			if l.creator != actor && !slices.Contains(assign, l.creator) {
				assign = append(assign, l.creator)
			}
		}
		ev := &scmpb.Event{Kind: &scmpb.Event_Uploaded{Uploaded: &scmpb.ChangeListUploaded{
			Number: cl, Patchset: 1, Created: true, Uploader: actor, Reviewers: assign, Status: StatusOpen.proto(),
		}}}
		if err := appendEvents(ctx, tx, repo, []*scmpb.Event{ev}); err != nil {
			return err
		}
		created, err := readChangeLists(ctx, tx, repo, []int64{cl}, false)
		if err != nil {
			return err
		}
		out = &RollbackResult{ChangeList: created[cl], Event: ev}
		return nil
	})
	return out, err
}

// rollbackStack is what rolling back CL number rolls back, in the order it landed: the CL, and
// every landed CL above it in its stack — one with a patchset uploaded stacked on it, or on one
// above it. main never keeps a stack's top without its bottom.
func (s *Store) rollbackStack(ctx context.Context, tx *txn, repo, number int64) ([]landedCommit, error) {
	st, err := clStatus(ctx, tx, repo, number)
	if err != nil {
		return nil, err
	}
	if st != StatusSubmitted {
		return nil, &RollbackRejection{Number: number, Reason: RollbackNotLanded,
			Message: fmt.Sprintf("CL %d is %s: only a landed CL is rolled back", number, st)}
	}
	landings, err := tx.StackLandings(ctx, repo, number, int16(StatusSubmitted))
	if err != nil {
		return nil, err
	}
	out := make([]landedCommit, 0, len(landings))
	for _, l := range landings {
		title, _, _ := strings.Cut(strings.TrimSpace(l.Description), "\n")
		out = append(out, landedCommit{number: l.Number, seq: l.Seq, commit: l.Commit, parent: l.Parent, title: title, creator: l.Author})
	}
	if len(out) == 0 || out[0].number != number {
		return nil, fmt.Errorf("CL %d is submitted but main's history has no landing of it", number)
	}
	return out, nil
}

// unapply is main's head with the landings' changes undone, from the last landed down: each
// landing L, from its parent B on main, spliced backwards — every path it changed set back to
// B's value — onto what the landings above it left. A path main changed after the landing is
// left as the head has it, with everything under it, and named; the landings are undone again
// until the paths left are all they leave, so none is half rolled back.
func unapply(lc *landCommits, ts treeStore, landed []landedCommit, head encoding.ID) (encoding.ID, []string, error) {
	hTree, hResolved, err := lc.tree(head)
	if err != nil {
		return encoding.ID{}, nil, err
	}
	type step struct{ from, to encoding.ID }
	steps := make([]step, len(landed))
	for i, l := range landed {
		lTree, lResolved, err := lc.tree(l.commit)
		if err != nil {
			return encoding.ID{}, nil, err
		}
		bTree, bResolved, err := lc.tree(l.parent)
		if err != nil {
			return encoding.ID{}, nil, err
		}
		if !hResolved || !lResolved || !bResolved {
			return encoding.ID{}, nil, fmt.Errorf("main has a commit with unresolved conflicts")
		}
		steps[len(landed)-1-i] = step{from: lTree, to: bTree}
	}
	u := &unsplicer{ts: ts, keep: map[string]bool{}}
	for {
		u.left = nil
		tree := hTree
		for _, st := range steps {
			if tree, err = u.apply(st.from, st.to, tree, ""); err != nil {
				return encoding.ID{}, nil, err
			}
		}
		if len(u.left) == 0 {
			kept := make([]string, 0, len(u.keep))
			for p := range u.keep {
				kept = append(kept, p)
			}
			sort.Strings(kept)
			return tree, kept, nil
		}
		for _, p := range u.left {
			u.keep[p] = true
		}
	}
}

// unsplicer applies one landing's inverse: splice(P = the landed tree, C = its parent's, H)
// as landing does, but a path whose value on H is neither P's nor C's — main changed it since —
// is left as H has it and recorded rather than refusing the rest, and so is a path where the
// result would be both a file and a directory. keep are paths left from the start, each with
// everything under it.
type unsplicer struct {
	ts   treeStore
	keep map[string]bool
	left []string
}

// kept reports whether a path, or a directory above it, is left as it is.
func (u *unsplicer) kept(path string) bool {
	for p := path; ; {
		if u.keep[p] {
			return true
		}
		i := strings.LastIndexByte(p, '/')
		if i < 0 {
			return false
		}
		p = p[:i]
	}
}

// keepsUnder reports whether a path left as it is lies under dir ("" is the root).
func (u *unsplicer) keepsUnder(dir string) bool {
	if dir == "" {
		return len(u.keep) > 0
	}
	for p := range u.keep {
		if strings.HasPrefix(p, dir+"/") {
			return true
		}
	}
	return false
}

func (u *unsplicer) apply(p, c, h encoding.ID, dir string) (encoding.ID, error) {
	switch {
	case p == c:
		return h, nil
	case (h == p || h == c) && !u.keepsUnder(dir):
		return c, nil
	}
	tp, err := u.ts.readTree(p)
	if err != nil {
		return encoding.ID{}, err
	}
	tc, err := u.ts.readTree(c)
	if err != nil {
		return encoding.ID{}, err
	}
	th, err := u.ts.readTree(h)
	if err != nil {
		return encoding.ID{}, err
	}
	ep, ec, eh := entries(tp), entries(tc), entries(th)
	out := &encoding.Tree{}
	for _, name := range names(tp, tc, th) {
		vp, vc, vh := ep[name], ec[name], eh[name]
		path := joinPath(dir, name)
		var r *encoding.TreeValue
		switch {
		case u.kept(path), sameValue(vp, vc):
			r = vh
		case (sameValue(vh, vp) || sameValue(vh, vc)) && !u.keepsUnder(path):
			r = vc
		default:
			lp, sp := split(vp)
			lc, sc := split(vc)
			lh, sh := split(vh)
			leaf := lh
			if !sameValue(lp, lc) {
				if sameValue(lh, lp) || sameValue(lh, lc) {
					leaf = lc
				} else {
					u.left = append(u.left, path)
				}
			}
			sub, err := u.apply(sp, sc, sh, path)
			if err != nil {
				return encoding.ID{}, err
			}
			switch {
			case leaf != nil && sub != encoding.EmptyTreeID:
				u.left = append(u.left, path)
				r = vh
			case leaf != nil:
				r = leaf
			case sub != encoding.EmptyTreeID:
				r = &encoding.TreeValue{Kind: encoding.TreeValueTree, ID: sub}
			}
		}
		if r != nil {
			out.Entries = append(out.Entries, encoding.TreeEntry{Name: name, Value: *r})
		}
	}
	return u.ts.writeTree(out)
}

// rollbackDescription is a rollback commit's description: a title naming the CL rolled back
// and its title, the reason, and each landing it undoes.
func rollbackDescription(landed []landedCommit, reason string) string {
	var b strings.Builder
	first := landed[0]
	numbers := make([]int64, len(landed))
	for i, l := range landed {
		numbers[i] = l.number
	}
	fmt.Fprintf(&b, "Roll back %s: %s\n\n%s\n\n", clNames(numbers), first.title, reason)
	fmt.Fprintf(&b, "This rolls back CL %d, landed as %s", first.number, shortID(first.commit))
	if len(landed) > 1 {
		b.WriteString(", and the CLs landed above it in its stack:")
		for _, l := range landed[1:] {
			fmt.Fprintf(&b, "\n- CL %d, landed as %s: %s", l.number, shortID(l.commit), l.title)
		}
	} else {
		b.WriteString(".")
	}
	b.WriteString("\n")
	return b.String()
}

// clNames words CLs: "CL 12", "CLs 12 and 13", "CLs 12, 13, and 14".
func clNames(numbers []int64) string {
	n := make([]string, len(numbers))
	for i, x := range numbers {
		n[i] = fmt.Sprint(x)
	}
	switch len(n) {
	case 1:
		return "CL " + n[0]
	case 2:
		return "CLs " + n[0] + " and " + n[1]
	}
	return "CLs " + strings.Join(n[:len(n)-1], ", ") + ", and " + n[len(n)-1]
}

// shortID is a commit ID's first ten hex digits, as ok shows it.
func shortID(id encoding.ID) string {
	return id.String()[:10]
}
