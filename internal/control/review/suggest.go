package review

import (
	"context"
	"errors"
	"path"
	"slices"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/owners"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// The review dialog's bounds: how much of main's history is read for recent activity,
// how many directories of a large change it covers, and how many recent people and
// search results it offers.
const (
	recentCommits = 40
	recentMaxDirs = 50
	recentOffered = 6
	SearchOffered = 8
)

// Candidate is one person the review dialog offers.
type Candidate struct {
	Principal string
	Selected  bool // pre-selected: among the fewest people who can bring the CL to approval
	Assigned  bool // already reviewing; shown checked and not added again
}

// OwnerSetSuggestion is an owner set still needing Approval — an OWNERS set, or the ACL a part
// of a change to the groups-and-ACLs tree needs a member of — with the named owners whose Approval would
// count.
type OwnerSetSuggestion struct {
	Label      string
	Candidates []Candidate
}

// Suggestions are what the Request Review and Add Reviewers dialog offers, following
// Critique: the smallest set of reviewers that can approve every file, then the people who
// recently reviewed or edited the changed paths.
type Suggestions struct {
	OwnerSets []OwnerSetSuggestion // one per unsatisfied owner set that names owners
	Recent    []Candidate          // recent reviewers and editors of the changed paths
	NeedsLGTM bool                 // nobody assigned or pre-selected can give a counting LGTM
}

// suggest builds the dialog's suggestions from the CL's live evaluation, its changed
// files, and its reviewers. eligible says whether a principal can review the CL.
func (s *Service) suggest(ctx context.Context, cl db.ChangeList, st *Status, files []string, assignedRows []db.Reviewer) (*Suggestions, error) {
	assigned := map[string]bool{}
	for _, r := range assignedRows {
		assigned[r.Principal] = true
	}
	recent, err := s.recentPeople(ctx, cl.Repo, files)
	if err != nil {
		return nil, err
	}
	rank := map[string]int{}
	for i, p := range recent {
		rank[p] = i
	}
	eligible := func(p string) (bool, error) {
		if p == cl.Author {
			return false, nil
		}
		return s.people.IsActiveHuman(ctx, p)
	}

	out := &Suggestions{}
	var sets [][]string
	if st.OwnersErr == nil {
		for _, o := range st.Owners {
			if o.Satisfied {
				continue
			}
			var ids []string
			for _, owner := range o.Set.Owners {
				var named []string
				switch owner.Kind {
				case owners.User:
					if id, ok := st.owned.users[owner.Name]; ok {
						named = []string{id}
					}
				case owners.ACL:
					for m := range st.owned.acls[ACLName(owner)] {
						named = append(named, m)
					}
					slices.Sort(named)
				}
				for _, p := range named {
					if !grantCounts(o.Set, p, cl.Author, st.owned) || slices.Contains(ids, p) {
						continue
					}
					ok, err := eligible(p)
					if err != nil {
						return nil, err
					}
					if ok {
						ids = append(ids, p)
					}
				}
			}
			if len(ids) > 0 {
				sets = append(sets, ids)
				out.OwnerSets = append(out.OwnerSets, OwnerSetSuggestion{Label: OwnersLabel(o.Set)})
			}
		}
	}
	for _, r := range st.Identity {
		if r.Accepted || slices.ContainsFunc(out.OwnerSets, func(o OwnerSetSuggestion) bool { return o.Label == r.ACL }) {
			continue
		}
		var ids []string
		for _, name := range r.Eligible {
			p, err := people.IDOfName(ctx, name)
			if errors.Is(err, people.ErrUnknown) {
				continue
			}
			if err != nil {
				return nil, err
			}
			ok, err := eligible(p)
			if err != nil {
				return nil, err
			}
			if ok {
				ids = append(ids, p)
			}
		}
		if len(ids) > 0 {
			sets = append(sets, ids)
			out.OwnerSets = append(out.OwnerSets, OwnerSetSuggestion{Label: r.ACL})
		}
	}
	selected := pickCover(sets, assigned, func(p string) (int, string) {
		r, ok := rank[p]
		if !ok {
			r = len(rank)
		}
		return r, people.Handle(ctx, p)
	})
	inSets := map[string]bool{}
	for i, ids := range sets {
		for _, p := range ids {
			inSets[p] = true
			out.OwnerSets[i].Candidates = append(out.OwnerSets[i].Candidates, Candidate{Principal: p, Assigned: assigned[p], Selected: selected[p]})
		}
	}
	for _, p := range recent {
		if len(out.Recent) == recentOffered {
			break
		}
		if assigned[p] || inSets[p] {
			continue
		}
		ok, err := eligible(p)
		if err != nil {
			return nil, err
		}
		if ok {
			out.Recent = append(out.Recent, Candidate{Principal: p})
		}
	}
	// LGTM counts from anyone but the author, so any assigned or pre-selected reviewer can
	// give it.
	if !st.LGTMSatisfied && !st.SelfLGTMAllowed && len(assigned) == 0 && len(selected) == 0 {
		out.NeedsLGTM = true
		if len(out.Recent) > 0 {
			out.Recent[0].Selected = true
		}
	}
	return out, nil
}

// pickCover chooses the fewest people who cover every owner set not already covered by an
// assigned reviewer — a greedy set cover: repeatedly take whoever covers the most
// remaining sets, breaking ties by rank (recent activity on the changed paths first) and
// then name.
func pickCover(sets [][]string, assigned map[string]bool, order func(p string) (int, string)) map[string]bool {
	selected := map[string]bool{}
	remaining := make([]bool, len(sets))
	left := 0
	for i, ids := range sets {
		if !slices.ContainsFunc(ids, func(p string) bool { return assigned[p] }) {
			remaining[i] = true
			left++
		}
	}
	for left > 0 {
		counts := map[string]int{}
		var candidates []string
		for i, ids := range sets {
			if !remaining[i] {
				continue
			}
			for _, p := range ids {
				if counts[p] == 0 {
					candidates = append(candidates, p)
				}
				counts[p]++
			}
		}
		best := candidates[0]
		for _, p := range candidates[1:] {
			if counts[p] != counts[best] {
				if counts[p] > counts[best] {
					best = p
				}
				continue
			}
			ri, ni := order(p)
			rb, nb := order(best)
			if ri < rb || (ri == rb && ni < nb) {
				best = p
			}
		}
		selected[best] = true
		for i, ids := range sets {
			if remaining[i] && slices.Contains(ids, best) {
				remaining[i] = false
				left--
			}
		}
	}
	return selected
}

// recentPeople lists who recently worked on the changed paths on main, most recent first:
// for each recent commit on main touching their directories, the reviewers who granted a
// label on the CL that landed it, then its author (by their address).
func (s *Service) recentPeople(ctx context.Context, repo int64, files []string) ([]string, error) {
	prefixes, whole := recentPrefixes(files)
	req := &scmpb.LogRequest{RepositoryId: repo, PageSize: recentCommits}
	if !whole {
		req.PathPrefixes = prefixes
	}
	log, err := s.scm.Browse.Log(ctx, req)
	if err != nil {
		return nil, err
	}
	commits := log.GetCommits()
	ids := make([][]byte, len(commits))
	for i, c := range commits {
		ids[i] = c.GetId()
	}
	landed, err := s.db.LandedBy(ctx, repo, ids)
	if err != nil {
		return nil, err
	}
	var numbers []int64
	for _, n := range landed {
		numbers = append(numbers, n)
	}
	grants, err := s.db.GrantsOf(ctx, repo, numbers)
	if err != nil {
		return nil, err
	}
	reviewersOf := map[int64][]string{}
	for _, g := range grants {
		if g.Granted && !slices.Contains(reviewersOf[g.Number], g.Principal) {
			reviewersOf[g.Number] = append(reviewersOf[g.Number], g.Principal)
		}
	}
	var out []string
	seen := map[string]bool{}
	add := func(p string) {
		if p != "" && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	for _, c := range commits {
		if n, ok := landed[string(c.GetId())]; ok {
			for _, p := range reviewersOf[n] {
				add(p)
			}
		}
		add(people.FromEmail(ctx, c.GetAuthor().GetEmail()))
	}
	return out, nil
}

// recentPrefixes are the changed files' directories (whole when a change touches the
// repository root or too many directories to list, so all of main's history counts).
func recentPrefixes(files []string) (prefixes []string, whole bool) {
	for _, f := range files {
		d := path.Dir(f)
		if d == "." {
			return nil, true
		}
		if !slices.Contains(prefixes, d) {
			prefixes = append(prefixes, d)
		}
	}
	if len(prefixes) == 0 || len(prefixes) > recentMaxDirs {
		return nil, true
	}
	return prefixes, false
}
