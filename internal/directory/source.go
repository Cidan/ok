// Package directory is ok's directory of principals and the system of record it mirrors: who
// exists is the system of record's to say — Google Workspace, Microsoft Entra ID, LDAP, a SCIM
// provider, or a static file — and the directory follows it. A Source reads the system of record;
// the sync (Reconcile) turns what it read into a Plan over the directory's State: people become
// humans, deactivated ones are suspended, deleted ones are tombstoned and their names retired for
// good, renamed ones keep their principal under a new name, and the system of record's groups are
// mirrored as groups. A name is never given to two principals, ever.
package directory

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// Source reads a system of record. It is read-only toward the source.
type Source interface {
	// Snapshot reads everything the source holds now.
	Snapshot(ctx context.Context) (*Snapshot, error)
	// Changes reads what changed since cursor (a Snapshot's or a Delta's), where the source can
	// say; ErrNoDelta tells the sync to take a Snapshot instead.
	Changes(ctx context.Context, cursor string) (*Delta, error)
}

// ErrNoDelta is a Source's answer when it cannot say what changed since a cursor: it keeps no
// change log, or the cursor is too old.
var ErrNoDelta = errors.New("the system of record cannot say what changed since the cursor")

// Snapshot is everything a system of record holds, and the cursor to read its changes from.
type Snapshot struct {
	People []Person
	Groups []Group
	Cursor string
}

// Delta is what changed in a system of record since a cursor: people and groups added or
// changed (whole), and the external IDs of those removed.
type Delta struct {
	People        []Person
	Groups        []Group
	RemovedPeople []string
	RemovedGroups []string
	Cursor        string
}

// Person is a person as the system of record holds them.
type Person struct {
	// ExternalID is the source's immutable ID for them: never reused, never changed by a rename.
	ExternalID string
	// Handle is what their name is made from (alice of alice.human.<domain>), as the source has it:
	// an email's local part, a uid. The sync makes it a name's label (HandleOf).
	Handle string
	Email  string
	Name   string
	// Active is false for a person the source has suspended or deactivated.
	Active bool
}

// Group is a group as the system of record holds it.
type Group struct {
	ExternalID string
	// Name is what its name is made from (eng of eng.group.<domain>), as the source has it.
	Name    string
	Members []Member
}

// Member is a member of a source's group: a person or a group, by its external ID.
type Member struct {
	ExternalID string
	Group      bool
}

// Merge applies a delta to the snapshot it follows: what it adds or changes replaces what the
// snapshot had by external ID, and what it removes goes.
func Merge(s *Snapshot, d *Delta) *Snapshot {
	people := map[string]Person{}
	for _, p := range s.People {
		people[p.ExternalID] = p
	}
	for _, p := range d.People {
		people[p.ExternalID] = p
	}
	for _, id := range d.RemovedPeople {
		delete(people, id)
	}
	groups := map[string]Group{}
	for _, g := range s.Groups {
		groups[g.ExternalID] = g
	}
	for _, g := range d.Groups {
		groups[g.ExternalID] = g
	}
	for _, id := range d.RemovedGroups {
		delete(groups, id)
	}
	out := &Snapshot{Cursor: d.Cursor}
	for _, id := range sortedKeys(people) {
		out.People = append(out.People, people[id])
	}
	for _, id := range sortedKeys(groups) {
		out.Groups = append(out.Groups, groups[id])
	}
	return out
}

// check refuses a snapshot that names one person or group twice, or a group member that is
// neither a person nor a group of it.
func (s *Snapshot) check() error {
	var problems []string
	people := map[string]bool{}
	for _, p := range s.People {
		if p.ExternalID == "" {
			problems = append(problems, fmt.Sprintf("a person (%q) has no external ID", p.Handle))
		} else if people[p.ExternalID] {
			problems = append(problems, fmt.Sprintf("the person %s is listed twice", p.ExternalID))
		}
		people[p.ExternalID] = true
	}
	groups := map[string]bool{}
	for _, g := range s.Groups {
		if g.ExternalID == "" {
			problems = append(problems, fmt.Sprintf("a group (%q) has no external ID", g.Name))
		} else if groups[g.ExternalID] {
			problems = append(problems, fmt.Sprintf("the group %s is listed twice", g.ExternalID))
		}
		groups[g.ExternalID] = true
	}
	for _, g := range s.Groups {
		for _, m := range g.Members {
			if m.Group && !groups[m.ExternalID] || !m.Group && !people[m.ExternalID] {
				problems = append(problems, fmt.Sprintf("the group %s holds %s, which the source does not hold", g.ExternalID, m.ExternalID))
			}
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return &Error{Problems: problems}
	}
	return nil
}

// Error is everything wrong with what a source read, each problem a sentence.
type Error struct {
	Problems []string
}

func (e *Error) Error() string {
	if len(e.Problems) == 1 {
		return e.Problems[0]
	}
	s := fmt.Sprintf("%d problems:", len(e.Problems))
	for _, p := range e.Problems {
		s += "\n  " + p
	}
	return s
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
