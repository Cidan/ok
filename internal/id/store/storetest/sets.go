package storetest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	stdsync "sync"
	"testing"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/tenancy"
)

// The sets' walks (Store.SetsOf, Chain, Expand, NamedSet, SetsReachable) agree with the model the
// directory was once built into whole for each question — the applied tree with every synced group,
// team, and alias, walked in memory — over random directories as they change: people synced and
// suspended, nested synced groups, an organization's nested teams and their members, aliases, a tree
// applied and applied again, groups the system of record drops though the tree names them, and the
// names the directory's rules decide between: a synced group named as one of the tree's sets is the
// tree's, and one named as an alias is the alias.
func testSets(t *testing.T, b store.Backend) {
	for seed := range uint64(3) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { setsAgree(t, b, seed) })
	}
}

func setsAgree(t *testing.T, b store.Backend, seed uint64) {
	ctx := context.Background()
	s := store.New(b, names)
	rng := rand.New(rand.NewPCG(seed, 7))
	const people, groups, teams, rounds = 16, 7, 7, 8
	// Each seed's own names, as the seeds share the backend.
	label := func(format string, a ...any) string { return fmt.Sprintf("s%d", seed) + fmt.Sprintf(format, a...) }
	group := func(i int) string { return label("sg%d", i) + ".group.example.com" }
	snap := &directory.Snapshot{}
	for i := range people {
		snap.People = append(snap.People, person(label("-%d", i), label("p%02d", i), i == 0 || rng.IntN(5) > 0))
	}
	all := make([]directory.Group, groups)
	for i := range all {
		all[i] = directory.Group{ExternalID: label("-g%d", i), Name: label("sg%d", i)}
	}
	gone := map[int]bool{} // the groups the system of record dropped
	regroup := func(i int) {
		var ms []directory.Member
		for range 1 + rng.IntN(4) {
			if rng.IntN(3) == 0 {
				ms = append(ms, directory.Member{ExternalID: label("-g%d", rng.IntN(groups)), Group: true})
			} else {
				ms = append(ms, directory.Member{ExternalID: label("-%d", rng.IntN(people))})
			}
		}
		all[i].Members = ms
	}
	for i := range all {
		regroup(i)
	}
	src := &source{snap: snap}
	resync := func() {
		snap.Groups = nil
		for i, g := range all {
			if gone[i] {
				continue
			}
			var ms []directory.Member
			for _, m := range g.Members {
				if j, err := strconv.Atoi(m.ExternalID[len(label("-g")):]); m.Group && err == nil && gone[j] {
					continue
				}
				ms = append(ms, m)
			}
			g.Members = ms
			snap.Groups = append(snap.Groups, g)
		}
		sync(t, s, src)
	}
	resync()
	ps := make([]store.Principal, people)
	reread := func() {
		for i := range ps {
			p, err := s.Principal(ctx, label("p%02d", i)+".human.example.com")
			if err != nil {
				t.Fatal(err)
			}
			ps[i] = p
		}
	}
	reread()
	org, err := s.CreateOrganization(ctx, label("acme"), ps[0].ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}
	// The organization's members — who was active as it was made — are who its teams may hold.
	members := []store.Principal{ps[0]}
	for _, p := range ps[1:] {
		if !p.Active() {
			continue
		}
		if err := s.SetMember(ctx, org.ID, p.ID, tenancy.Member); err != nil {
			t.Fatal(err)
		}
		members = append(members, p)
	}
	var teamNames []string
	for i := range teams {
		parent := ""
		if i > 0 && rng.IntN(2) == 0 {
			parent = teamNames[rng.IntN(len(teamNames))]
		}
		tm, err := s.CreateTeam(ctx, org.ID, fmt.Sprintf("t%d", i), parent)
		if err != nil {
			t.Fatal(err)
		}
		teamNames = append(teamNames, tm.Name)
		for range rng.IntN(4) {
			if err := s.AddTeamMember(ctx, tm.Name, members[rng.IntN(len(members))].ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	aliases := []string{label("al0") + ".group.example.com", label("al1") + ".group.example.com"}
	for _, a := range aliases {
		if err := b.InsertAlias(ctx, a, teamNames[rng.IntN(teams)]); err != nil {
			t.Fatal(err)
		}
	}
	// Synced groups the system of record has not, which no sync makes, placed again after each one
	// drops them: one named as an alias, which is the alias, and — once the last tree is applied —
	// one named as a set of the tree, which is the tree's.
	var clashes []directory.SyncedGroup
	placed := map[string]bool{}
	place := func() {
		for _, g := range clashes {
			var err error
			if placed[g.ExternalID] {
				err = b.UpdateSyncedGroup(ctx, g)
			} else {
				err = b.InsertSyncedGroup(ctx, g)
			}
			if err != nil {
				t.Fatal(err)
			}
			placed[g.ExternalID] = true
		}
	}
	clashes = append(clashes, directory.SyncedGroup{ExternalID: label("-clash-alias"), Name: aliases[0], Members: []string{ps[1].Name}})

	folder := func(i int) string { return label("f%d", i) }
	applyTree := func(round int) {
		var standing []string
		for i := range groups {
			if !gone[i] {
				standing = append(standing, group(i))
			}
		}
		var people []string
		for _, p := range ps {
			people = append(people, p.Name)
		}
		pick := func(pool []string) string { return pool[rng.IntN(len(pool))] }
		const folders = 5
		members := func(i int, acl bool) []string {
			seen := map[string]bool{}
			var out []string
			for range 1 + rng.IntN(4) {
				var m string
				switch r := rng.IntN(6); {
				case r == 0 && len(standing) > 0:
					m = pick(standing)
				case r == 1:
					m = pick(teamNames)
				case r == 2:
					m = pick(aliases)
				case r == 3 && i+1 < folders:
					// A set declared after this one: none holds itself through others.
					j := i + 1 + rng.IntN(folders-i-1)
					m = folder(j) + ".group.example.com"
					if acl && rng.IntN(2) == 0 {
						m = folder(j) + ".acl.example.com"
					}
				default:
					m = pick(people)
				}
				if !seen[m] {
					seen[m] = true
					out = append(out, m)
				}
			}
			return out
		}
		sets := []identity.Set{{Kind: identity.ACL, Name: identity.Root, Members: members(-1, true)}}
		for i := range folders {
			f := folder(i)
			sets = append(sets, identity.Set{Kind: identity.Group, Name: f, Path: f, Members: members(i, false)},
				identity.Set{Kind: identity.ACL, Name: f, Path: f, Members: members(i, true)})
		}
		// Validated as the identity service validated a tree to apply: against every group as stored.
		tree, err := identity.New(names, sets, identity.WithSynced(oldModelOf(t, s).groups))
		if err != nil {
			t.Fatalf("round %d's tree: %v", round, err)
		}
		applied, err := s.Applied(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.ReplaceTree(ctx, applied, label("c%d", round), int64(seed)*100+int64(round+1), tree); err != nil {
			t.Fatalf("round %d: applying the tree: %v", round, err)
		}
	}

	for round := range rounds {
		switch {
		case round == 1, round > 1 && round < rounds-2 && rng.IntN(2) == 0:
			applyTree(round)
		case round == rounds-2:
			clashes = append(clashes, directory.SyncedGroup{ExternalID: label("-clash-tree"), Name: folder(0) + ".group.example.com",
				Members: []string{ps[2].Name, ps[3].Name}})
		}
		// What changes between rounds: who is active, synced groups' members, a group dropped
		// (once no tree is applied after: the tree may name it still), and teams' members.
		for range 3 {
			switch rng.IntN(4) {
			case 0:
				i := 1 + rng.IntN(people-1)
				snap.People[i].Active = !snap.People[i].Active
			case 1:
				regroup(rng.IntN(groups))
			case 2:
				if round >= rounds-3 {
					gone[rng.IntN(groups)] = true
				}
			case 3:
				tm, p := teamNames[rng.IntN(teams)], members[rng.IntN(len(members))]
				if rng.IntN(2) == 0 {
					if err := s.AddTeamMember(ctx, tm, p.ID); err != nil {
						t.Fatal(err)
					}
				} else if _, err := s.RemoveTeamMember(ctx, tm, p.ID); err != nil {
					t.Fatal(err)
				}
			}
		}
		resync()
		reread()
		place()

		old := oldModelOf(t, s)
		setNames := append(append([]string{}, teamNames...), aliases...)
		for i := range groups {
			setNames = append(setNames, group(i))
		}
		for n := range old.treeSets {
			setNames = append(setNames, n)
		}
		setNames = append(setNames, label("nosuch")+".group.example.com", label("nosuch")+".acl.example.com")
		slices.Sort(setNames)
		setNames = slices.Compact(setNames)
		where := fmt.Sprintf("seed %d, round %d (a tree applied: %v)", seed, round, old.tree != nil)

		for _, p := range ps {
			got, err := s.SetsOf(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			want := old.setsOf(p.Name)
			if !mapsEqual(got, want) {
				t.Fatalf("%s: %s (active %v) is in %v; the model says %v", where, p.Name, p.Active(), sortedSet(got), sortedSet(want))
			}
			// Each set it is in, and a few it may not be: in it by a shortest chain, as long as the
			// model's — which, with no tree, named the set and the member alone.
			candidates := sortedSet(want)
			for range 3 {
				candidates = append(candidates, setNames[rng.IntN(len(setNames))])
			}
			for _, set := range candidates {
				chain, ok, err := s.Chain(ctx, set, p)
				if err != nil {
					t.Fatal(err)
				}
				wantChain, wantOK := old.contains(set, p.Name)
				if ok != wantOK {
					t.Fatalf("%s: %s in %s: %v %v; the model says %v %v", where, p.Name, set, ok, chain, wantOK, wantChain)
				}
				if !ok {
					continue
				}
				if old.tree != nil && len(chain) != len(wantChain) {
					t.Fatalf("%s: %s in %s by %v; the model's shortest is %v", where, p.Name, set, chain, wantChain)
				}
				old.checkChain(t, where, set, chain, p)
			}
		}
		for _, set := range setNames {
			got, ok, err := s.Expand(ctx, set)
			if err != nil {
				t.Fatal(err)
			}
			want, wantOK := old.expand(set)
			if ok != wantOK || !slices.Equal(got, want) {
				t.Fatalf("%s: %s holds %v (a set: %v); the model says %v (%v)", where, set, got, ok, want, wantOK)
			}
			r, ms, found, err := s.NamedSet(ctx, set)
			if err != nil {
				t.Fatal(err)
			}
			wantName, wantMembers, wantFound := old.named(set)
			if found != wantFound || found && (r.Name != wantName || !slices.Equal(sorted(ms), sorted(wantMembers))) {
				t.Fatalf("%s: the set named %s is %s %v (found %v); the model says %s %v (%v)", where, set, r.Name, ms, found,
					wantName, wantMembers, wantFound)
			}
		}
		// What the applied tree reaches, as its model held it; and what every set as stored does.
		var roots []string
		for n, set := range old.treeSets {
			roots = append(append(roots, n), set.Members...)
		}
		got, err := s.SetsReachable(ctx, roots, true)
		if err != nil {
			t.Fatal(err)
		}
		if want := old.reachable(roots, old.treeSynced, true); !reachedEqual(got, want) {
			t.Fatalf("%s: the applied tree reaches %v; the model says %v", where, got, want)
		}
		if got, err = s.SetsReachable(ctx, setNames, false); err != nil {
			t.Fatal(err)
		}
		if want := old.reachable(setNames, old.groups, false); !reachedEqual(got, want) {
			t.Fatalf("%s: every set as stored reaches %v; the model says %v", where, got, want)
		}
	}
}

// oldModel is the directory as the identity service once built it for each question: the applied
// tree with every synced group, team, and alias, as the tree's model held them (treeSynced), and
// those alone, as stored (groups).
type oldModel struct {
	tree       *identity.Tree
	treeSets   map[string]identity.Set
	treeSynced map[string][]string
	groups     map[string][]string
	aliases    map[string]string // an alias whose team stands -> the team
}

func oldModelOf(t *testing.T, s *store.Store) oldModel {
	t.Helper()
	ctx := context.Background()
	groups, err := s.SyncedGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	teams, err := s.TeamSets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for n, ms := range teams {
		groups[n] = ms
	}
	aliases, err := s.Aliases(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m := oldModel{groups: groups, treeSets: map[string]identity.Set{}, aliases: map[string]string{}}
	for old, team := range aliases {
		if ms, ok := teams[team]; ok {
			groups[old] = ms
			m.aliases[old] = team
		}
	}
	sets, _, err := s.Tree(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) == 0 {
		return m
	}
	ts := map[string][]string{}
	for n, ms := range groups {
		ts[n] = ms
	}
	var model []identity.Set
	for _, set := range sets {
		model = append(model, set.Set)
		m.treeSets[names.OfSet(set.Set)] = set.Set
		for _, mem := range set.Members {
			if k, ok := names.KindOf(mem); ok && k == identity.GroupMember {
				if _, ok := ts[mem]; !ok {
					ts[mem] = nil // dropped by the system of record: no one
				}
			}
		}
	}
	for _, set := range sets {
		delete(ts, names.OfSet(set.Set))
	}
	if m.tree, err = identity.New(names, model, identity.WithSynced(ts)); err != nil {
		t.Fatalf("the applied tree: %v", err)
	}
	m.treeSynced = ts
	return m
}

func (m oldModel) setsOf(name string) map[string]bool {
	out := map[string]bool{}
	if m.tree != nil {
		for _, n := range m.tree.Memberships(name) {
			out[n] = true
		}
		return out
	}
	in := map[string][]string{}
	for n, ms := range m.groups {
		for _, mem := range ms {
			in[mem] = append(in[mem], n)
		}
	}
	queue := append([]string(nil), in[name]...)
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		if !out[n] {
			out[n] = true
			queue = append(queue, in[n]...)
		}
	}
	return out
}

func (m oldModel) expand(name string) ([]string, bool) {
	if m.tree != nil {
		if _, ok := m.tree.Set(name); ok || m.tree.Synced(name) {
			return m.tree.Expand(name), true
		}
	}
	if _, ok := m.groups[name]; !ok {
		return nil, false
	}
	seen := map[string]bool{}
	var out []string
	var walk func(string)
	walk = func(n string) {
		for _, mem := range m.groups[n] {
			if seen[mem] {
				continue
			}
			seen[mem] = true
			if _, ok := m.groups[mem]; ok {
				walk(mem)
			} else {
				out = append(out, mem)
			}
		}
	}
	walk(name)
	sort.Strings(out)
	return out, true
}

func (m oldModel) contains(set, name string) ([]string, bool) {
	if m.tree != nil {
		return m.tree.Contains(set, name)
	}
	if ms, _ := m.expand(set); slices.Contains(ms, name) {
		return []string{set, name}, true
	}
	return nil, false
}

// direct are a set's own members, as the model held them.
func (m oldModel) direct(set string) []string {
	if s, ok := m.treeSets[set]; ok {
		return s.Members
	}
	if m.tree != nil {
		return m.treeSynced[set]
	}
	return m.groups[set]
}

// checkChain fails unless chain runs from set to p, each set in it directly holding the next.
func (m oldModel) checkChain(t *testing.T, where, set string, chain []string, p store.Principal) {
	t.Helper()
	if chain[0] != set || chain[len(chain)-1] != p.Name {
		t.Fatalf("%s: the chain putting %s in %s is %v", where, p.Name, set, chain)
	}
	for i := 0; i+1 < len(chain); i++ {
		if ms := m.direct(chain[i]); !slices.Contains(ms, chain[i+1]) {
			t.Fatalf("%s: in the chain %v, %s does not hold %s (it holds %v)", where, chain, chain[i], chain[i+1], ms)
		}
	}
}

// named is the set a name names, with its members: one of the tree's sets; an alias, its team's —
// the alias, not a synced group named as it, which the membership the model walked holds too; or a
// synced group or team as stored.
func (m oldModel) named(name string) (string, []string, bool) {
	if s, ok := m.treeSets[name]; ok {
		return name, s.Members, true
	}
	if team, ok := m.aliases[name]; ok {
		return team, m.groups[team], true
	}
	ms, ok := m.groups[name]
	return name, ms, ok
}

// reachable is what sets the model holds of those roots reach, by name: none of the tree's own
// (with tree), nor the empty sets its model made of groups the system of record dropped.
func (m oldModel) reachable(roots []string, sets map[string][]string, tree bool) map[string][]string {
	out := map[string][]string{}
	seen := map[string]bool{}
	queue := append([]string(nil), roots...)
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		if seen[n] {
			continue
		}
		seen[n] = true
		if _, own := m.treeSets[n]; tree && own {
			continue
		}
		if ms, ok := sets[n]; ok && ms != nil {
			out[n] = ms
			queue = append(queue, ms...)
		}
	}
	return out
}

func mapsEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func reachedEqual(a, b map[string][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || !slices.Equal(sorted(v), sorted(w)) {
			return false
		}
	}
	return true
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func sorted(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}

// counting is a backend counting the reads of every set in the directory, and the walks' reads.
type counting struct {
	store.Backend
	mu    stdsync.Mutex
	calls map[string]int
}

func (c *counting) count(op string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[op]++
}

func (c *counting) reset() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.calls
	c.calls = map[string]int{}
	return out
}

func (c *counting) SyncedGroups(ctx context.Context) ([]directory.SyncedGroup, error) {
	c.count("SyncedGroups")
	return c.Backend.SyncedGroups(ctx)
}

func (c *counting) AllTeams(ctx context.Context) ([]store.Team, error) {
	c.count("AllTeams")
	return c.Backend.AllTeams(ctx)
}

func (c *counting) TreeSets(ctx context.Context) ([]store.StoredSet, error) {
	c.count("TreeSets")
	return c.Backend.TreeSets(ctx)
}

func (c *counting) Aliases(ctx context.Context) (map[string]string, error) {
	c.count("Aliases")
	return c.Backend.Aliases(ctx)
}

func (c *counting) SetsHolding(ctx context.Context, names, ids, teams []string) ([]store.Holding, error) {
	c.count("SetsHolding")
	return c.Backend.SetsHolding(ctx, names, ids, teams)
}

func (c *counting) SetRecords(ctx context.Context, names []string, tree bool) ([]store.SetRecord, error) {
	c.count("SetRecords")
	return c.Backend.SetRecords(ctx, names, tree)
}

// Which sets a person is in, who is in a set, and what a tree reaches cost a read a level of
// nesting, whatever the directory holds besides: a person three teams deep, in a synced group the
// tree's root ACL holds, beside hundreds of teams and synced groups they are in none of; no read of
// every set.
func testSetsAtScale(t *testing.T, b store.Backend) {
	ctx := context.Background()
	c := &counting{Backend: b, calls: map[string]int{}}
	s := store.New(c, names)
	const others = 300
	snap := &directory.Snapshot{People: []directory.Person{person("1", "ada", true), person("2", "bea", true)}}
	snap.Groups = append(snap.Groups, directory.Group{ExternalID: "ops", Name: "ops", Members: []directory.Member{{ExternalID: "1"}}})
	for i := range others {
		snap.Groups = append(snap.Groups, directory.Group{ExternalID: fmt.Sprintf("g%d", i), Name: fmt.Sprintf("other%d", i),
			Members: []directory.Member{{ExternalID: "2"}}})
	}
	sync(t, s, &source{snap: snap})
	ada, err := s.Principal(ctx, "ada.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	bea, _ := s.Principal(ctx, "bea.human.example.com")
	org, err := s.CreateOrganization(ctx, "acme", bea.ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetMember(ctx, org.ID, ada.ID, tenancy.Member); err != nil {
		t.Fatal(err)
	}
	// ada is in deep, in deeper, in deepest.
	parent := ""
	for _, n := range []string{"deepest", "deeper", "deep"} {
		tm, err := s.CreateTeam(ctx, org.ID, n, parent)
		if err != nil {
			t.Fatal(err)
		}
		parent = tm.Name
	}
	if err := s.AddTeamMember(ctx, parent, ada.ID); err != nil {
		t.Fatal(err)
	}
	for i := range others {
		tm, err := s.CreateTeam(ctx, org.ID, fmt.Sprintf("other%d", i), "")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.AddTeamMember(ctx, tm.Name, bea.ID); err != nil {
			t.Fatal(err)
		}
	}
	tree, err := identity.New(names, []identity.Set{
		{Kind: identity.ACL, Name: identity.Root, Members: []string{"ops.group.example.com", "deepest.acme.team.example.com"}},
	}, identity.WithSynced(map[string][]string{"ops.group.example.com": {ada.Name}, "deepest.acme.team.example.com": {"deeper.acme.team.example.com"}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceTree(ctx, store.Applied{}, "c1", 1, tree); err != nil {
		t.Fatal(err)
	}
	whole := func(what string, calls map[string]int) {
		t.Helper()
		for _, op := range []string{"SyncedGroups", "AllTeams", "TreeSets", "Aliases"} {
			if calls[op] > 0 {
				t.Errorf("%s read every set (%s): %v", what, op, calls)
			}
		}
	}

	c.reset()
	sets, err := s.SetsOf(ctx, ada)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"ops.group.example.com": true, "deep.acme.team.example.com": true, "deeper.acme.team.example.com": true,
		"deepest.acme.team.example.com": true, "admins.acl.example.com": true}
	if !mapsEqual(sets, want) {
		t.Fatalf("ada is in %v, want %v", sortedSet(sets), sortedSet(want))
	}
	calls := c.reset()
	whole("SetsOf", calls)
	// One read a level — ops and deep; admins.acl (holding ops) and deeper; deepest; then nothing
	// new — and no more.
	if calls["SetsHolding"] != 4 {
		t.Errorf("SetsOf read %d levels of sets, want 4: %v", calls["SetsHolding"], calls)
	}
	if chain, ok, err := s.Chain(ctx, "admins.acl.example.com", ada); err != nil || !ok || len(chain) != 3 {
		t.Errorf("ada in admins.acl: %v, %v, %v; want by ops", chain, ok, err)
	}
	whole("Chain", c.reset())
	if ms, ok, err := s.Expand(ctx, "admins.acl.example.com"); err != nil || !ok || !slices.Equal(ms, []string{ada.Name}) {
		t.Errorf("admins.acl holds %v, %v, %v; want ada", ms, ok, err)
	}
	whole("Expand", c.reset())
	if reached, err := s.SetsReachable(ctx, []string{"admins.acl.example.com", "ops.group.example.com", "deepest.acme.team.example.com"}, true); err != nil ||
		len(reached) != 4 {
		t.Errorf("the tree reaches %v, %v; want ops and the three deep teams", reached, err)
	}
	whole("SetsReachable", c.reset())
	if r, ms, ok, err := s.NamedSet(ctx, "deep.acme.team.example.com"); err != nil || !ok || r.Source != store.SetFromTeam ||
		!slices.Equal(ms, []string{ada.Name}) {
		t.Errorf("the team deep: %+v %v %v %v", r, ms, ok, err)
	}
	whole("NamedSet", c.reset())
}
