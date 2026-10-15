package directory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/identity"
)

var names = identity.MustNames("example.com")

func person(id, handle string, active bool) Person {
	return Person{ExternalID: id, Handle: handle, Email: handle + "@example.com", Name: strings.ToUpper(handle), Active: active}
}

// step reconciles st to snap, applies the plan, and returns it.
func step(t *testing.T, st *State, snap *Snapshot, opts Options) *Plan {
	t.Helper()
	plan, err := Reconcile(names, st, snap, opts)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	Apply(st, plan)
	return plan
}

func describe(plan *Plan) []string {
	var out []string
	for _, c := range plan.Humans {
		out = append(out, c.String())
	}
	for _, c := range plan.Groups {
		out = append(out, c.String())
	}
	for _, w := range plan.Waiting {
		out = append(out, w.String())
	}
	out = append(out, plan.Notes...)
	return out
}

func wantPlan(t *testing.T, plan *Plan, want ...string) {
	t.Helper()
	if got := describe(plan); !reflect.DeepEqual(got, want) {
		t.Errorf("plan:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

func TestGenesis(t *testing.T) {
	st := NewState()
	plan := step(t, st, &Snapshot{
		People: []Person{person("1", "Ada.Lovelace", true), person("2", "grace", false)},
		Groups: []Group{
			{ExternalID: "g-eng", Name: "Eng", Members: []Member{{ExternalID: "1"}, {ExternalID: "g-infra", Group: true}}},
			{ExternalID: "g-infra", Name: "infra", Members: []Member{{ExternalID: "2"}}},
		},
	}, Options{})
	wantPlan(t, plan,
		"ada-lovelace.human.example.com: create",
		"grace.human.example.com: create, suspended",
		"eng.group.example.com: create, with members ada-lovelace.human.example.com, infra.group.example.com",
		"infra.group.example.com: create, with members grace.human.example.com",
	)
	if st.Principals["grace.human.example.com"].Status != Suspended {
		t.Errorf("grace is %v", st.Principals["grace.human.example.com"].Status)
	}
	if again := step(t, st, &Snapshot{
		People: []Person{person("1", "Ada.Lovelace", true), person("2", "grace", false)},
		Groups: []Group{
			{ExternalID: "g-eng", Name: "Eng", Members: []Member{{ExternalID: "1"}, {ExternalID: "g-infra", Group: true}}},
			{ExternalID: "g-infra", Name: "infra", Members: []Member{{ExternalID: "2"}}},
		},
	}, Options{}); !again.Empty() {
		t.Errorf("the same snapshot again planned %v", describe(again))
	}
}

func TestLifecycle(t *testing.T) {
	st := NewState()
	step(t, st, &Snapshot{People: []Person{person("1", "ada", true), person("2", "bea", true)}}, Options{})

	// Suspended, and renamed with a new email, at once.
	p := person("1", "ada-l", false)
	wantPlan(t, step(t, st, &Snapshot{People: []Person{p, person("2", "bea", true)}}, Options{}),
		`ada-l.human.example.com: rename from ada.human.example.com; the old name is retired; suspend; email "ada@example.com" → "ada-l@example.com"; display name "ADA" → "ADA-L"`)
	if !st.Retired["ada.human.example.com"] {
		t.Error("the old name is not retired")
	}

	// Reinstated.
	wantPlan(t, step(t, st, &Snapshot{People: []Person{person("1", "ada-l", true), person("2", "bea", true)}}, Options{}),
		"ada-l.human.example.com: reinstate")

	// Deleted from the source: a tombstone.
	wantPlan(t, step(t, st, &Snapshot{People: []Person{person("1", "ada-l", true)}}, Options{}),
		"bea.human.example.com: delete; the name is retired")
	if st.Principals["bea.human.example.com"].Status != Deleted || !st.Retired["bea.human.example.com"] {
		t.Errorf("bea is %+v, retired %v", st.Principals["bea.human.example.com"], st.Retired["bea.human.example.com"])
	}

	// The source holds bea again: the sync brings no one back.
	wantPlan(t, step(t, st, &Snapshot{People: []Person{person("1", "ada-l", true), person("2", "bea", true)}}, Options{}),
		"the person 2 waits: bea.human.example.com was deleted, and the sync brings no one back")

	// Someone new wants a retired name, then another's: they wait, until their handle is set.
	wantPlan(t, step(t, st, &Snapshot{People: []Person{person("1", "ada-l", true), person("3", "ada", true)}}, Options{}),
		"the person 3 waits: ada.human.example.com is taken, or was, and a name is never given to another: set their handle")
	wantPlan(t, step(t, st, &Snapshot{People: []Person{person("1", "ada-l", true), person("3", "ada-l", true)}}, Options{}),
		"the person 3 waits: ada-l.human.example.com is taken, or was, and a name is never given to another: set their handle")
	wantPlan(t, step(t, st, &Snapshot{People: []Person{person("1", "ada-l", true), person("3", "ada", true)}}, Options{Handles: map[string]string{"3": "ada-b"}}),
		"ada-b.human.example.com: create")
}

func TestBlockedRenameStillChanges(t *testing.T) {
	st := NewState()
	step(t, st, &Snapshot{People: []Person{person("1", "ada", true), person("2", "bea", true)}}, Options{})
	// ada wants bea's name, and is suspended meanwhile: she keeps hers, and is suspended.
	p := person("1", "bea", false)
	p.Email, p.Name = "ada@example.com", "ADA"
	wantPlan(t, step(t, st, &Snapshot{People: []Person{p, person("2", "bea", true)}}, Options{}),
		"ada.human.example.com: suspend",
		"the person 1 waits: bea.human.example.com is taken, or was, and a name is never given to another: set their handle")
}

func TestOneFreeNameWantedTwice(t *testing.T) {
	st := NewState()
	wantPlan(t, step(t, st, &Snapshot{People: []Person{person("1", "ada", true), person("2", "Ada", true)}}, Options{}),
		"the person 1 waits: ada.human.example.com is wanted by 1 and 2 at once: set their handles",
		"the person 2 waits: ada.human.example.com is wanted by 1 and 2 at once: set their handles")
	if len(st.Principals) != 0 {
		t.Errorf("someone was made: %v", st.Principals)
	}
}

func TestUnusableHandle(t *testing.T) {
	wantPlan(t, step(t, NewState(), &Snapshot{People: []Person{person("1", "...", true)}}, Options{}),
		`the person 1 waits: no handle can be made of "...": set one`)
}

func TestGroups(t *testing.T) {
	st := NewState()
	step(t, st, &Snapshot{
		People: []Person{person("1", "ada", true), person("2", "bea", true)},
		Groups: []Group{{ExternalID: "g1", Name: "eng", Members: []Member{{ExternalID: "1"}}}},
	}, Options{})

	// A member waits, so the group does not hold them yet; another group wants a name the tree
	// declares; the first is renamed.
	wantPlan(t, step(t, st, &Snapshot{
		People: []Person{person("1", "ada", true), person("2", "bea", true), person("3", "ada", true)},
		Groups: []Group{
			{ExternalID: "g1", Name: "engineering", Members: []Member{{ExternalID: "1"}, {ExternalID: "2"}, {ExternalID: "3"}}},
			{ExternalID: "g2", Name: "admins", Members: []Member{{ExternalID: "1"}}},
		},
	}, Options{Declared: map[string]bool{"admins.group.example.com": true}}),
		"engineering.group.example.com: rename from eng.group.example.com; the old name is retired; add bea.human.example.com",
		"the person 3 waits: ada.human.example.com is taken, or was, and a name is never given to another: set their handle",
		"the group g2 waits: admins.group.example.com is declared by the groups-and-ACLs tree: set the group's name",
		"engineering.group.example.com: 3 is not in the directory (it waits), so it is not a member yet",
	)

	// Gone from the source: deleted, its name retired; a person may not take a group's name
	// (they cannot: kinds name apart), and another group may not take it.
	wantPlan(t, step(t, st, &Snapshot{
		People: []Person{person("1", "ada", true), person("2", "bea", true)},
		Groups: []Group{{ExternalID: "g3", Name: "engineering"}},
	}, Options{}),
		"engineering.group.example.com: delete; the name is retired",
		"the group g3 waits: engineering.group.example.com is taken, or was, and a name is never given to another: set the group's name",
	)
}

func TestSnapshotRefused(t *testing.T) {
	_, err := Reconcile(names, NewState(), &Snapshot{
		People: []Person{person("1", "ada", true), person("1", "bea", true)},
		Groups: []Group{{ExternalID: "g", Name: "g", Members: []Member{{ExternalID: "9"}}}},
	}, Options{})
	var e *Error
	if !errors.As(err, &e) || len(e.Problems) != 2 {
		t.Fatalf("Reconcile = %v, want two problems", err)
	}
}

func TestHandleOf(t *testing.T) {
	for in, want := range map[string]string{
		"ada":                          "ada",
		"Ada.Lovelace+eng":             "ada-lovelace-eng",
		"__x__":                        "x",
		"…":                            "",
		strings.Repeat("a", 70):        strings.Repeat("a", 63),
		strings.Repeat("a", 62) + ".b": strings.Repeat("a", 62),
	} {
		if got := HandleOf(in); got != want {
			t.Errorf("HandleOf(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestStatic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "people.yaml")
	os.WriteFile(path, []byte(`
people:
  - id: "1001"
    email: Ada.Lovelace@example.com
    name: Ada Lovelace
  - id: "1002"
    handle: grace
    email: grace@example.com
    active: false
groups:
  - id: eng
    name: eng
    members: ["1001", infra]
  - id: infra
    name: infra
    members: ["1002"]
`), 0o644)
	src := Static{Path: path}
	snap, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []Person{
		{ExternalID: "1001", Handle: "Ada.Lovelace", Email: "Ada.Lovelace@example.com", Name: "Ada Lovelace", Active: true},
		{ExternalID: "1002", Handle: "grace", Email: "grace@example.com", Active: false},
	}
	if !reflect.DeepEqual(snap.People, want) {
		t.Errorf("people = %+v", snap.People)
	}
	if got := snap.Groups[0].Members; !reflect.DeepEqual(got, []Member{{ExternalID: "1001"}, {ExternalID: "infra", Group: true}}) {
		t.Errorf("eng's members = %+v", got)
	}
	if _, err := src.Changes(context.Background(), snap.Cursor); !errors.Is(err, ErrNoDelta) {
		t.Errorf("Changes = %v, want ErrNoDelta", err)
	}
	for _, bad := range []string{
		"people:\n  - id: a\n    wat: 1\n",
		"people:\n  - id: a\ngroups:\n  - id: a\n",
		"groups:\n  - id: g\n    members: [nobody]\n",
	} {
		os.WriteFile(path, []byte(bad), 0o644)
		if _, err := src.Snapshot(context.Background()); err == nil {
			t.Errorf("Snapshot accepted %q", bad)
		}
	}
}

// deltas is a source that says what changed since a cursor.
type deltas struct {
	snap   *Snapshot
	delta  *Delta
	asked  []string
	noMore bool
}

func (d *deltas) Snapshot(context.Context) (*Snapshot, error) {
	d.asked = append(d.asked, "snapshot")
	return d.snap, nil
}

func (d *deltas) Changes(_ context.Context, cursor string) (*Delta, error) {
	d.asked = append(d.asked, "changes since "+cursor)
	if d.noMore {
		return nil, ErrNoDelta
	}
	return d.delta, nil
}

func TestSyncReadsDeltas(t *testing.T) {
	src := &deltas{snap: &Snapshot{People: []Person{person("1", "ada", true), person("2", "bea", true)}, Cursor: "c1"}}
	store := NewMemory()
	ctx := context.Background()
	if _, err := Sync(ctx, names, src, store, Options{}); err != nil {
		t.Fatal(err)
	}
	src.delta = &Delta{People: []Person{person("3", "cal", true)}, RemovedPeople: []string{"2"}, Cursor: "c2"}
	plan, err := Sync(ctx, names, src, store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	wantPlan(t, plan, "bea.human.example.com: delete; the name is retired", "cal.human.example.com: create")
	src.noMore = true
	src.snap = &Snapshot{People: []Person{person("1", "ada", true), person("3", "cal", true)}, Cursor: "c3"}
	if plan, err = Sync(ctx, names, src, store, Options{}); err != nil || !plan.Empty() {
		t.Errorf("a snapshot matching the deltas planned %v, %v", describe(plan), err)
	}
	if want := []string{"snapshot", "changes since c1", "changes since c2", "snapshot"}; !reflect.DeepEqual(src.asked, want) {
		t.Errorf("the source was asked %v, want %v", src.asked, want)
	}
	st, mirror, _ := store.Load(ctx)
	if mirror.Cursor != "c3" || len(st.Principals) != 3 || !st.Retired["bea.human.example.com"] {
		t.Errorf("the store holds %+v, mirror %+v", st, mirror)
	}
}
