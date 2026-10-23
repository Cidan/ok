package directory

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/Cidan/ok/internal/identity"
)

// Status is where a principal stands.
type Status int

const (
	// Active principals act.
	Active Status = iota + 1
	// Suspended principals keep their name and history and do nothing: the system of record
	// deactivated them.
	Suspended
	// Deleted principals are tombstones: their names are retired for good.
	Deleted
)

func (s Status) String() string {
	switch s {
	case Active:
		return "active"
	case Suspended:
		return "suspended"
	case Deleted:
		return "deleted"
	}
	return fmt.Sprintf("Status(%d)", int(s))
}

// Principal is a principal the directory holds. The sync makes and keeps humans; a robot, which
// no system of record holds, has no external ID, and the sync never touches it.
type Principal struct {
	Name        string // ada.human.example.com
	ExternalID  string // the system of record's ID for a human
	Email       string
	DisplayName string
	Status      Status
}

// SyncedGroup is a group mirrored from the system of record.
type SyncedGroup struct {
	Name       string   // eng.group.example.com
	ExternalID string   // the system of record's ID for it
	Members    []string // full names, sorted
	Deleted    bool
}

// State is what the directory holds that the sync decides: its principals and synced groups —
// deleted ones kept, as tombstones — and every name retired, which nothing is given again.
type State struct {
	Principals map[string]Principal
	Groups     map[string]SyncedGroup
	Retired    map[string]bool
}

// NewState is an empty directory.
func NewState() *State {
	return &State{Principals: map[string]Principal{}, Groups: map[string]SyncedGroup{}, Retired: map[string]bool{}}
}

// Options are what an administrator decides for the sync.
type Options struct {
	// Handles sets a person's handle, by external ID, over what the source says: how a collision
	// is resolved.
	Handles map[string]string
	// GroupNames sets a group's name, by external ID, likewise.
	GroupNames map[string]string
	// Declared are the names the groups-and-ACLs tree declares: a synced group may not take one.
	Declared map[string]bool
	// HumanName and GroupName make a person's and a group's names of their handle and label: the
	// installation's, ada.human.<domain> and eng.group.<domain>, when nil; an enterprise
	// organization's people and teams otherwise.
	HumanName, GroupName func(string) string
}

// HumanChange is a human the plan makes or changes: Before is nil for a new one.
type HumanChange struct {
	Before *Principal
	After  Principal
}

// What lists what the change does: create, rename, suspend, reinstate, delete, and the fields it
// updates.
func (c HumanChange) What() []string {
	if c.Before == nil {
		if c.After.Status == Suspended {
			return []string{"create, suspended"}
		}
		return []string{"create"}
	}
	var out []string
	b, a := c.Before, c.After
	if a.Status == Deleted {
		return []string{"delete; the name is retired"}
	}
	if b.Name != a.Name {
		out = append(out, "rename from "+b.Name+"; the old name is retired")
	}
	switch {
	case b.Status == Active && a.Status == Suspended:
		out = append(out, "suspend")
	case b.Status == Suspended && a.Status == Active:
		out = append(out, "reinstate")
	}
	if b.Email != a.Email {
		out = append(out, fmt.Sprintf("email %q → %q", b.Email, a.Email))
	}
	if b.DisplayName != a.DisplayName {
		out = append(out, fmt.Sprintf("display name %q → %q", b.DisplayName, a.DisplayName))
	}
	return out
}

func (c HumanChange) String() string { return c.After.Name + ": " + strings.Join(c.What(), "; ") }

// GroupChange is a synced group the plan makes or changes: Before is nil for a new one.
type GroupChange struct {
	Before *SyncedGroup
	After  SyncedGroup
}

// What lists what the change does.
func (c GroupChange) What() []string {
	if c.Before == nil {
		return []string{"create, with members " + list(c.After.Members)}
	}
	if c.After.Deleted {
		return []string{"delete; the name is retired"}
	}
	var out []string
	if c.Before.Name != c.After.Name {
		out = append(out, "rename from "+c.Before.Name+"; the old name is retired")
	}
	if add := minus(c.After.Members, c.Before.Members); len(add) > 0 {
		out = append(out, "add "+strings.Join(add, ", "))
	}
	if rm := minus(c.Before.Members, c.After.Members); len(rm) > 0 {
		out = append(out, "remove "+strings.Join(rm, ", "))
	}
	return out
}

func (c GroupChange) String() string { return c.After.Name + ": " + strings.Join(c.What(), "; ") }

// Waiting is a person or group of the source the directory cannot take yet, and why: it waits,
// named on the administrators' page, until the source or an administrator's option resolves it.
type Waiting struct {
	ExternalID string
	Group      bool
	Reason     string
}

func (w Waiting) String() string {
	what := "the person"
	if w.Group {
		what = "the group"
	}
	return what + " " + w.ExternalID + " waits: " + w.Reason
}

// Plan is what the sync does to the directory: humans and groups made or changed, in name order,
// those that wait, and notes on what a group could not hold yet.
type Plan struct {
	Humans  []HumanChange
	Groups  []GroupChange
	Waiting []Waiting
	Notes   []string
}

// Empty reports whether the plan changes nothing.
func (p *Plan) Empty() bool { return len(p.Humans) == 0 && len(p.Groups) == 0 }

// Reconcile plans the directory's state to follow a snapshot of the system of record:
//
//   - a person the directory does not hold becomes a human, suspended if the source has them
//     inactive;
//   - a person who becomes inactive is suspended, and one who becomes active again reinstated;
//   - a person the source no longer holds is deleted: a tombstone, the name retired;
//   - a person whose handle changes keeps their principal under the new name, the old one retired;
//   - a name held by anyone, or retired, is given to no one else: a person or group wanting one
//     waits, as do several wanting one free name at once, and a deleted person the source holds
//     again (the sync brings no one back);
//   - groups follow likewise, their members those people and groups the directory holds, and no
//     group takes a name the tree declares.
//
// It is pure: the store applies the plan (Apply), a chunk at a time.
func Reconcile(names identity.Names, st *State, snap *Snapshot, opts Options) (*Plan, error) {
	if err := snap.check(); err != nil {
		return nil, err
	}
	plan := &Plan{}
	claimed := map[string]string{} // name -> the external ID the plan gives it to
	taken := func(name, id string) bool {
		if owner, ok := claimed[name]; ok && owner != id {
			return true
		}
		return st.Retired[name] || principalNamed(st, name, id) || groupNamed(st, name, id)
	}

	// People.
	byID := map[string]Principal{}
	for _, p := range st.Principals {
		if p.ExternalID != "" {
			byID[p.ExternalID] = p
		}
	}
	people := slices.Clone(snap.People)
	sort.Slice(people, func(i, j int) bool { return people[i].ExternalID < people[j].ExternalID })
	want := map[string]string{} // external ID -> the name they want
	wanted := map[string][]string{}
	for _, p := range people {
		handle := opts.Handles[p.ExternalID]
		if handle == "" {
			handle = HandleOf(p.Handle)
		}
		if !identity.ValidLabel(handle) {
			plan.Waiting = append(plan.Waiting, Waiting{ExternalID: p.ExternalID, Reason: fmt.Sprintf("no handle can be made of %q: set one", p.Handle)})
			continue
		}
		name := names.Human(handle)
		if opts.HumanName != nil {
			name = opts.HumanName(handle)
		}
		want[p.ExternalID] = name
		if cur, ok := byID[p.ExternalID]; !ok || cur.Name != name {
			wanted[name] = append(wanted[name], p.ExternalID)
		}
	}
	placed := map[string]string{} // external ID -> the name the person has after the plan
	for _, p := range people {
		name, ok := want[p.ExternalID]
		if !ok {
			continue
		}
		cur, known := byID[p.ExternalID]
		if known && cur.Status == Deleted {
			plan.Waiting = append(plan.Waiting, Waiting{ExternalID: p.ExternalID, Reason: cur.Name + " was deleted, and the sync brings no one back"})
			continue
		}
		after := Principal{Name: name, ExternalID: p.ExternalID, Email: p.Email, DisplayName: p.Name, Status: Active}
		if !p.Active {
			after.Status = Suspended
		}
		if known && cur.Name != name || !known {
			why := ""
			switch {
			case len(wanted[name]) > 1:
				why = fmt.Sprintf("%s is wanted by %s at once: set their handles", name, strings.Join(wanted[name], " and "))
			case taken(name, p.ExternalID):
				why = name + " is taken, or was, and a name is never given to another: set their handle"
			}
			if why != "" {
				plan.Waiting = append(plan.Waiting, Waiting{ExternalID: p.ExternalID, Reason: why})
				if !known {
					continue
				}
				after.Name = cur.Name // renamed once the name is free; changed meanwhile under their name
			}
		}
		claimed[after.Name] = p.ExternalID
		placed[p.ExternalID] = after.Name
		if known {
			before := cur
			if before != after {
				plan.Humans = append(plan.Humans, HumanChange{Before: &before, After: after})
			}
			continue
		}
		plan.Humans = append(plan.Humans, HumanChange{After: after})
	}
	inSnapshot := map[string]bool{}
	for _, p := range people {
		inSnapshot[p.ExternalID] = true
	}
	for _, id := range sortedKeys(byID) {
		cur := byID[id]
		if inSnapshot[id] || cur.Status == Deleted {
			continue
		}
		after := cur
		after.Status = Deleted
		before := cur
		plan.Humans = append(plan.Humans, HumanChange{Before: &before, After: after})
	}

	// Groups: their names first, then — every group's name known — their members.
	gByID := map[string]SyncedGroup{}
	for _, g := range st.Groups {
		gByID[g.ExternalID] = g
	}
	groups := slices.Clone(snap.Groups)
	sort.Slice(groups, func(i, j int) bool { return groups[i].ExternalID < groups[j].ExternalID })
	gWanted := map[string][]string{}
	gWant := map[string]string{}
	for _, g := range groups {
		label := opts.GroupNames[g.ExternalID]
		if label == "" {
			label = HandleOf(g.Name)
		}
		if !identity.ValidLabel(label) {
			plan.Waiting = append(plan.Waiting, Waiting{ExternalID: g.ExternalID, Group: true, Reason: fmt.Sprintf("no name can be made of %q: set one", g.Name)})
			continue
		}
		name := names.Of(identity.Group, label)
		if opts.GroupName != nil {
			name = opts.GroupName(label)
		}
		gWant[g.ExternalID] = name
		if cur, ok := gByID[g.ExternalID]; !ok || cur.Name != name {
			gWanted[name] = append(gWanted[name], g.ExternalID)
		}
	}
	gPlaced := map[string]string{}
	for _, g := range groups {
		name, ok := gWant[g.ExternalID]
		if !ok {
			continue
		}
		cur, known := gByID[g.ExternalID]
		if known && cur.Deleted {
			plan.Waiting = append(plan.Waiting, Waiting{ExternalID: g.ExternalID, Group: true, Reason: cur.Name + " was deleted, and the sync brings no group back"})
			continue
		}
		if !known || cur.Name != name {
			why := ""
			switch {
			case opts.Declared[name]:
				why = name + " is declared by the groups-and-ACLs tree: set the group's name"
			case len(gWanted[name]) > 1:
				why = fmt.Sprintf("%s is wanted by %s at once: set their names", name, strings.Join(gWanted[name], " and "))
			case taken(name, g.ExternalID):
				why = name + " is taken, or was, and a name is never given to another: set the group's name"
			}
			if why != "" {
				plan.Waiting = append(plan.Waiting, Waiting{ExternalID: g.ExternalID, Group: true, Reason: why})
				if !known {
					continue
				}
				name = cur.Name
			}
		}
		claimed[name] = g.ExternalID
		gPlaced[g.ExternalID] = name
	}
	for _, g := range groups {
		name, ok := gPlaced[g.ExternalID]
		if !ok {
			continue
		}
		var members []string
		for _, m := range g.Members {
			mn, ok := placed[m.ExternalID]
			if m.Group {
				mn, ok = gPlaced[m.ExternalID]
			}
			if !ok {
				plan.Notes = append(plan.Notes, fmt.Sprintf("%s: %s is not in the directory (it waits), so it is not a member yet", name, m.ExternalID))
				continue
			}
			members = append(members, mn)
		}
		sort.Strings(members)
		members = slices.Compact(members)
		after := SyncedGroup{Name: name, ExternalID: g.ExternalID, Members: members}
		if cur, known := gByID[g.ExternalID]; known {
			if cur.Name != after.Name || !slices.Equal(cur.Members, after.Members) {
				before := cur
				plan.Groups = append(plan.Groups, GroupChange{Before: &before, After: after})
			}
			continue
		}
		plan.Groups = append(plan.Groups, GroupChange{After: after})
	}
	inGroups := map[string]bool{}
	for _, g := range groups {
		inGroups[g.ExternalID] = true
	}
	for _, id := range sortedKeys(gByID) {
		cur := gByID[id]
		if inGroups[id] || cur.Deleted {
			continue
		}
		before, after := cur, cur
		after.Deleted = true
		plan.Groups = append(plan.Groups, GroupChange{Before: &before, After: after})
	}

	sort.Slice(plan.Humans, func(i, j int) bool { return plan.Humans[i].After.Name < plan.Humans[j].After.Name })
	sort.Slice(plan.Groups, func(i, j int) bool { return plan.Groups[i].After.Name < plan.Groups[j].After.Name })
	return plan, nil
}

// Apply carries a plan out on st: a renamed or deleted principal's or group's old name is retired.
func Apply(st *State, plan *Plan) {
	for _, c := range plan.Humans {
		if c.Before != nil && c.Before.Name != c.After.Name {
			delete(st.Principals, c.Before.Name)
			st.Retired[c.Before.Name] = true
		}
		st.Principals[c.After.Name] = c.After
		if c.After.Status == Deleted {
			st.Retired[c.After.Name] = true
		}
	}
	for _, c := range plan.Groups {
		if c.Before != nil && c.Before.Name != c.After.Name {
			delete(st.Groups, c.Before.Name)
			st.Retired[c.Before.Name] = true
		}
		st.Groups[c.After.Name] = c.After
		if c.After.Deleted {
			st.Retired[c.After.Name] = true
		}
	}
}

// HandleOf makes a source's handle a name's label: lower case, each run of anything but letters
// and digits a hyphen, at most 63 characters, no hyphen at either end — "Ada.Lovelace+eng" is
// ada-lovelace-eng. It is "" when nothing is left.
func HandleOf(raw string) string {
	var b strings.Builder
	hyphen := false
	for _, r := range strings.ToLower(raw) {
		if 'a' <= r && r <= 'z' || '0' <= r && r <= '9' {
			b.WriteRune(r)
			hyphen = false
			continue
		}
		if !hyphen && b.Len() > 0 {
			b.WriteByte('-')
			hyphen = true
		}
	}
	s := strings.TrimRight(b.String(), "-")
	if len(s) > 63 {
		s = strings.TrimRight(s[:63], "-")
	}
	return s
}

// principalNamed reports whether a principal other than the one with external ID id holds name.
func principalNamed(st *State, name, id string) bool {
	p, ok := st.Principals[name]
	return ok && (p.ExternalID == "" || p.ExternalID != id)
}

// groupNamed reports whether a synced group other than the one with external ID id holds name.
func groupNamed(st *State, name, id string) bool {
	g, ok := st.Groups[name]
	return ok && g.ExternalID != id
}

// minus is the elements of a not in b, sorted.
func minus(a, b []string) []string {
	in := map[string]bool{}
	for _, x := range b {
		in[x] = true
	}
	var out []string
	for _, x := range a {
		if !in[x] {
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}

func list(names []string) string {
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}
