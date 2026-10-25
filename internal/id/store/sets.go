package store

import (
	"context"
	"slices"
	"strings"

	"github.com/Cidan/ok/internal/identity"
)

// Sets, by their members (docs/identity.md, "Sets, by their members"): which sets a principal is
// in, who is in a set, and the sets a tree reaches, each read over the members' indexes a level of
// nesting at a time — never every set in the directory. A set holds names (the applied tree's
// sets, synced groups), principals by ID and child teams by name (teams), or what its team holds
// (an alias); a team holds its members only while they are active.

// SetsOf are every set p is in, directly or through others: a walk up from p, a statement for each
// level of nesting, whatever the directory's size.
func (s *Store) SetsOf(ctx context.Context, p Principal) (map[string]bool, error) {
	via, err := s.walkUp(ctx, p)
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(via))
	for n := range via {
		out[n] = true
	}
	return out, nil
}

// Chain is the shortest chain that puts p in set — the set, each set between, and p's name — and
// false when p is not in it.
func (s *Store) Chain(ctx context.Context, set string, p Principal) ([]string, bool, error) {
	via, err := s.walkUp(ctx, p)
	if _, ok := via[set]; err != nil || !ok {
		return nil, false, err
	}
	chain := []string{set}
	for n := set; n != p.Name; {
		n = via[n]
		chain = append(chain, n)
	}
	return chain, true, nil
}

// walkUp is every set p is in, each with what it holds on a shortest way down to p — p's name, for
// a set holding p itself: a breadth-first walk up the sets' indexes. A team holds p while p is
// active.
func (s *Store) walkUp(ctx context.Context, p Principal) (map[string]string, error) {
	via := map[string]string{}
	names := []string{p.Name}
	var ids []string
	if p.ID != "" && p.Active() {
		ids = []string{p.ID}
	}
	var teams []string
	for len(names)+len(ids)+len(teams) > 0 {
		hs, err := s.b.SetsHolding(ctx, names, ids, teams)
		if err != nil {
			return nil, err
		}
		var next []string
		for _, h := range hs {
			if _, seen := via[h.Set]; seen || h.Set == p.Name || len(h.Holds) == 0 {
				continue
			}
			held := h.Holds[0]
			if held == p.ID {
				held = p.Name
			}
			via[h.Set] = held
			next = append(next, h.Set)
		}
		names, ids, teams = next, nil, s.teamsAmong(next)
	}
	return via, nil
}

// setNamed reports whether a name may be a set's: anything but a person's or a robot's, which no
// set has (a name of no kind at all is taken for a set, and found to be none).
func (s *Store) setNamed(n string) bool {
	k, ok := s.names.KindOf(n)
	return !ok || (k != identity.Human && k != identity.Robot)
}

// teamsAmong are the teams' names among names.
func (s *Store) teamsAmong(names []string) []string {
	var out []string
	for _, n := range names {
		if k, ok := s.names.KindOf(n); ok && k == identity.TeamMember {
			out = append(out, n)
		}
	}
	return out
}

// Expand is every principal in set, by name, through the sets it holds, sorted — a team's members
// those that are active — and whether set is a set at all: one that stands, or a group the applied
// tree names that the system of record no longer holds, which holds no one. A name a set holds that
// is no set is taken for a principal, as the tree's model took it.
func (s *Store) Expand(ctx context.Context, set string) ([]string, bool, error) {
	recs, err := s.b.SetRecords(ctx, []string{set}, true)
	if err != nil {
		return nil, false, err
	}
	if len(recs) == 0 {
		if k, _ := s.names.KindOf(set); k != identity.GroupMember {
			return nil, false, nil
		}
		gone, err := s.treeNamed(ctx, []string{set})
		return nil, gone[set], err
	}
	principals := map[string]bool{}
	seen := map[string]bool{set: true}
	var ids, leaves []string
	for len(recs) > 0 {
		var next []string
		for _, r := range recs {
			switch r.Source {
			case SetFromTree, SetFromSynced:
				for _, m := range r.Members {
					if !seen[m] {
						seen[m] = true
						next = append(next, m)
					}
				}
			case SetFromTeam, SetFromAlias:
				ids = append(ids, r.Members...)
				for _, c := range r.Children {
					if !seen[c] {
						seen[c] = true
						next = append(next, c)
					}
				}
			}
		}
		// A person's or a robot's name is no set's; only the others are asked after.
		var sets []string
		for _, n := range next {
			if s.setNamed(n) {
				sets = append(sets, n)
			} else {
				principals[n] = true
			}
		}
		if len(sets) == 0 {
			break
		}
		if recs, err = s.b.SetRecords(ctx, sets, true); err != nil {
			return nil, false, err
		}
		found := make(map[string]bool, len(recs))
		for _, r := range recs {
			found[r.Name] = true
		}
		for _, n := range sets {
			switch k, _ := s.names.KindOf(n); {
			case found[n]:
			case k == identity.GroupMember:
				leaves = append(leaves, n)
			default:
				principals[n] = true
			}
		}
	}
	gone, err := s.treeNamed(ctx, leaves)
	if err != nil {
		return nil, false, err
	}
	for _, n := range leaves {
		if !gone[n] {
			principals[n] = true
		}
	}
	active, err := s.activeNames(ctx, ids)
	if err != nil {
		return nil, false, err
	}
	for _, n := range active {
		principals[n] = true
	}
	out := make([]string, 0, len(principals))
	for n := range principals {
		out = append(out, n)
	}
	slices.Sort(out)
	return out, true, nil
}

// treeNamed are those of groups the applied tree's sets name: groups the system of record no
// longer holds, which the tree's model holds as sets of no one.
func (s *Store) treeNamed(ctx context.Context, groups []string) (map[string]bool, error) {
	out := map[string]bool{}
	if len(groups) == 0 {
		return out, nil
	}
	hs, err := s.b.SetsHolding(ctx, groups, nil, nil)
	if err != nil {
		return nil, err
	}
	for _, h := range hs {
		if h.Source == SetFromTree {
			for _, n := range h.Holds {
				out[n] = true
			}
		}
	}
	return out, nil
}

// activeNames are the names of the active principals among ids, sorted.
func (s *Store) activeNames(ctx context.Context, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	slices.Sort(ids)
	ps, err := s.b.PrincipalsByID(ctx, slices.Compact(ids))
	if err != nil {
		return nil, err
	}
	var out []string
	for _, p := range ps {
		if p.Active() {
			out = append(out, p.Name)
		}
	}
	slices.Sort(out)
	return out, nil
}

// SetsReachable are the synced groups, teams, and aliases reachable from roots, by name, each with
// its members by name as a tree's model holds them (ReachableSets).
func (s *Store) SetsReachable(ctx context.Context, roots []string, tree bool) (map[string][]string, error) {
	sets, err := s.ReachableSets(ctx, roots, tree)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]string, len(sets))
	for _, r := range sets {
		out[r.Name] = r.Names
	}
	return out, nil
}

// ReachedSet is a set ReachableSets reached: as stored, and its members by name as a tree's model
// holds them — a team's active members and its child teams, an alias its team's.
type ReachedSet struct {
	SetRecord
	Names []string
}

// ReachableSets are the synced groups, teams, and aliases reachable from roots — those of roots,
// and those they hold, through every level — by name. tree says whether the applied tree's sets
// stand: true, a name it declares is its set, and not here; false, every set is as stored, as a
// tree yet to be applied sees them. The applied tree's own sets are never here.
func (s *Store) ReachableSets(ctx context.Context, roots []string, tree bool) ([]ReachedSet, error) {
	var out []ReachedSet
	seen := map[string]bool{}
	var level []string
	for _, n := range roots {
		if !seen[n] {
			seen[n] = true
			level = append(level, n)
		}
	}
	for len(level) > 0 {
		recs, err := s.b.SetRecords(ctx, level, tree)
		if err != nil {
			return nil, err
		}
		var next []string
		visit := func(n string) {
			if !seen[n] {
				seen[n] = true
				next = append(next, n)
			}
		}
		var teams []SetRecord
		for _, r := range recs {
			switch r.Source {
			case SetFromSynced:
				out = append(out, ReachedSet{r, r.Members})
				for _, m := range r.Members {
					if s.setNamed(m) {
						visit(m)
					}
				}
			case SetFromTeam, SetFromAlias:
				teams = append(teams, r)
				for _, c := range r.Children {
					visit(c)
				}
			}
		}
		named, err := s.teamMemberNames(ctx, teams)
		if err != nil {
			return nil, err
		}
		for _, r := range teams {
			out = append(out, ReachedSet{r, named[r.Name]})
		}
		level = next
	}
	slices.SortFunc(out, func(a, b ReachedSet) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// teamMemberNames are each team's (or alias's) members as a set of names: its active members' names
// and its child teams', sorted.
func (s *Store) teamMemberNames(ctx context.Context, teams []SetRecord) (map[string][]string, error) {
	out := make(map[string][]string, len(teams))
	if len(teams) == 0 {
		return out, nil
	}
	var ids []string
	for _, t := range teams {
		ids = append(ids, t.Members...)
	}
	slices.Sort(ids)
	ps, err := s.b.PrincipalsByID(ctx, slices.Compact(ids))
	if err != nil {
		return nil, err
	}
	name := make(map[string]string, len(ps))
	for _, p := range ps {
		if p.Active() {
			name[p.ID] = p.Name
		}
	}
	for _, t := range teams {
		ms := make([]string, 0, len(t.Members)+len(t.Children))
		for _, id := range t.Members {
			if n, ok := name[id]; ok {
				ms = append(ms, n)
			}
		}
		ms = append(ms, t.Children...)
		slices.Sort(ms)
		out[t.Name] = ms
	}
	return out, nil
}

// NamedSet is the set name names, with its members by name as the tree's model holds them, and
// false for none. An alias is the set of the team it stands for, named so.
func (s *Store) NamedSet(ctx context.Context, name string) (SetRecord, []string, bool, error) {
	recs, err := s.b.SetRecords(ctx, []string{name}, true)
	if err != nil || len(recs) == 0 {
		return SetRecord{}, nil, false, err
	}
	r := recs[0]
	switch r.Source {
	case SetFromTree, SetFromSynced:
		return r, r.Members, true, nil
	case SetFromAlias:
		r.Name, r.Source, r.Team = r.Team, SetFromTeam, ""
	}
	named, err := s.teamMemberNames(ctx, []SetRecord{r})
	return r, named[r.Name], err == nil, err
}

// TreeApplied reports whether a groups-and-ACLs tree has been applied.
func (s *Store) TreeApplied(ctx context.Context) (bool, error) {
	_, ok, err := s.b.TreeState(ctx)
	return ok, err
}

// Applied is which commit's tree is applied; zero before any.
func (s *Store) Applied(ctx context.Context) (Applied, error) {
	a, _, err := s.b.TreeState(ctx)
	return a, err
}
