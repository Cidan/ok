package store

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// rollback asks for a rollback of number as bob, naming reviewers.
func (e *clEnv) rollback(number int64, reviewers ...string) (*RollbackResult, error) {
	return e.s.Rollback(context.Background(), e.repo, number, bob, "it broke the build", reviewers)
}

func (e *clEnv) mustRollback(number int64, reviewers ...string) *RollbackResult {
	e.t.Helper()
	r, err := e.rollback(number, reviewers...)
	if err != nil {
		e.t.Fatalf("rollback of CL %d: %v", number, err)
	}
	return r
}

// landChange commits files on main's head as a new change, uploads it, and lands it,
// returning its CL.
func (e *clEnv) landChange(desc string, files map[string]string) int64 {
	e.t.Helper()
	n := e.open(e.commit(alice, newChange(), e.head(), desc, files))
	e.mustLand(LandItem{Number: n, Patchset: 1})
	return n
}

func rollbackRejection(t *testing.T, err error, reason RollbackReason) *RollbackRejection {
	t.Helper()
	var rej *RollbackRejection
	if !errors.As(err, &rej) || rej.Reason != reason {
		t.Fatalf("got %v, want a rollback refused with reason %d", err, reason)
	}
	return rej
}

// A landed CL's rollback undoes every path it changed, but a path main changed since, which
// it names and leaves; it is a new open CL of whoever asked. Rolled back again while that
// rollback is open, it is refused; once that lands, rolling back the rollback restores it.
func TestRollback(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	e.landChange("one", map[string]string{"a": "1", "keep": "k"})
	two := e.landChange("two\n\nadds b and d/x", map[string]string{"a": "2", "b": "new", "keep": "k", "d/x": "x"})
	e.landChange("three", map[string]string{"a": "3", "b": "new", "keep": "k", "d/x": "x"})
	head := e.head()

	const carol = "carol.human.example.com"
	r := e.mustRollback(two, carol, bob, carol)
	cl := r.ChangeList
	ps := cl.Current()
	if cl.Author != bob || cl.Status != StatusOpen || cl.CurrentPatchset != 1 || ps.Base != head || ps.UploadedBy != bob ||
		cl.ParentNumber != 0 || cl.Rollback == nil {
		t.Fatalf("the rollback CL: %+v", cl)
	}
	if want := (Rollback{CLs: []int64{two}, Paths: []string{"b", "d/x"}, Left: []string{"a"}}); !reflect.DeepEqual(*cl.Rollback, want) {
		t.Errorf("rollback %+v, want %+v", *cl.Rollback, want)
	}
	if want := map[string]string{"a": "3", "keep": "k"}; !reflect.DeepEqual(e.flatAt(ps.Commit), want) {
		t.Errorf("rolled back tree %v, want %v", e.flatAt(ps.Commit), want)
	}
	landedTwo := e.cl(two).LandedCommit
	if want := "Roll back CL 2: two\n\nit broke the build\n\nThis rolls back CL 2, landed as " + landedTwo.String()[:10] + ".\n"; cl.Description != want {
		t.Errorf("description %q, want %q", cl.Description, want)
	}
	c, err := e.s.readCommit(ctx, e.repo, ps.Commit)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Parents, []encoding.ID{head}) || c.Author.Name != "bob" || c.Committer.Email != "bob@example.com" ||
		c.ChangeID != cl.ChangeID || c.ChangeID == (encoding.ChangeID{}) {
		t.Errorf("rollback commit %+v", c)
	}
	var pinned bool
	if err := poolOf(e.s).QueryRow(ctx, `SELECT pinned FROM commit WHERE repository_id = $1 AND id = $2`, e.repo, ps.Commit[:]).Scan(&pinned); err != nil || !pinned {
		t.Errorf("rollback commit pinned=%v (%v)", pinned, err)
	}
	evs, err := e.s.ReadEvents(ctx, e.repo, r.Event.GetRepositorySeq()-1, 0)
	if err != nil || len(evs) != 1 || evs[0].GetRepositorySeq() != r.Event.GetRepositorySeq() {
		t.Fatalf("events %v (%v)", evs, err)
	}
	up := evs[0].GetUploaded()
	if up.GetNumber() != cl.Number || !up.GetCreated() || up.GetUploader() != bob || !reflect.DeepEqual(up.GetReviewers(), []string{carol, alice}) ||
		up.GetStatus() != StatusOpen.proto() {
		t.Errorf("uploaded event %v", up)
	}

	// One rollback of a CL at a time.
	rej := rollbackRejection(t, func() error { _, err := e.rollback(two); return err }(), RollbackUnderway)
	if !strings.Contains(rej.Message, "CL 4 already rolls it back") {
		t.Errorf("underway: %s", rej.Message)
	}

	// Landed, the rollback leaves main without CL 2's changes but a; rolled back again, CL 2
	// has nothing left to roll back but the path main changed.
	e.mustLand(LandItem{Number: cl.Number, Patchset: 1})
	rej = rollbackRejection(t, func() error { _, err := e.rollback(two); return err }(), RollbackAllChanged)
	if !reflect.DeepEqual(rej.Paths, []string{"a"}) {
		t.Errorf("all changed: %+v", rej)
	}

	// Rolling back the rollback restores what it removed.
	again := e.mustRollback(cl.Number)
	if want := (Rollback{CLs: []int64{cl.Number}, Paths: []string{"b", "d/x"}, Left: []string{}}); !reflect.DeepEqual(*again.ChangeList.Rollback, want) {
		t.Errorf("rollback of the rollback %+v", *again.ChangeList.Rollback)
	}
	if want := map[string]string{"a": "3", "b": "new", "keep": "k", "d/x": "x"}; !reflect.DeepEqual(e.flatAt(again.ChangeList.Current().Commit), want) {
		t.Errorf("rolled back rollback %v", e.flatAt(again.ChangeList.Current().Commit))
	}
	if !strings.HasPrefix(again.ChangeList.Description, "Roll back CL 4: Roll back CL 2: two\n") {
		t.Errorf("its description %q", again.ChangeList.Description)
	}

	// Refused: a CL that has not landed, one that does not exist, and a rollback without a
	// reason.
	open := e.open(e.commit(alice, newChange(), e.head(), "open", map[string]string{"z": "z"}))
	rollbackRejection(t, func() error { _, err := e.rollback(open); return err }(), RollbackNotLanded)
	if _, err := e.rollback(999); !errors.Is(err, ErrNotFound) {
		t.Errorf("a CL that does not exist: %v", err)
	}
	if _, err := e.s.Rollback(ctx, e.repo, two, bob, " ", nil); !errors.Is(err, ErrInvalid) {
		t.Errorf("no reason: %v", err)
	}
}

// Rolling back a CL rolls back the CLs landed above it in its stack with it, topmost first, as
// one CL; a CL landed between them that is not in the stack stays.
func TestRollbackStack(t *testing.T) {
	e := newCLEnv(t)
	e.landChange("base", map[string]string{"f": "0"})
	a := e.commit(alice, newChange(), e.head(), "bottom", map[string]string{"f": "0", "s": "1"})
	b := e.commit(alice, newChange(), a, "top", map[string]string{"f": "0", "s": "2", "t": "2"})
	res := e.mustUpload(alice, Ready, a, b)
	na, nb := res[0].Number, res[1].Number
	e.mustLand(LandItem{Number: na, Patchset: 1})
	e.landChange("between", map[string]string{"f": "0", "s": "1", "u": "x"})
	e.mustLand(LandItem{Number: nb, Patchset: 1})

	r := e.mustRollback(na)
	if want := (Rollback{CLs: []int64{na, nb}, Paths: []string{"s", "t"}, Left: []string{}}); !reflect.DeepEqual(*r.ChangeList.Rollback, want) {
		t.Errorf("stack rollback %+v, want %+v", *r.ChangeList.Rollback, want)
	}
	if want := map[string]string{"f": "0", "u": "x"}; !reflect.DeepEqual(e.flatAt(r.ChangeList.Current().Commit), want) {
		t.Errorf("stack rolled back to %v", e.flatAt(r.ChangeList.Current().Commit))
	}
	for _, want := range []string{"Roll back CLs 2 and 3: bottom\n", "the CLs landed above it in its stack:\n- CL 3, landed as "} {
		if !strings.Contains(r.ChangeList.Description, want) {
			t.Errorf("description %q lacks %q", r.ChangeList.Description, want)
		}
	}
	// The top alone rolls back alone.
	e.transition(r.ChangeList.Number, Abandon)
	top := e.mustRollback(nb)
	if want := []int64{nb}; !reflect.DeepEqual(top.ChangeList.Rollback.CLs, want) {
		t.Errorf("the top's rollback %v", top.ChangeList.Rollback.CLs)
	}
	if want := map[string]string{"f": "0", "s": "1", "u": "x"}; !reflect.DeepEqual(e.flatAt(top.ChangeList.Current().Commit), want) {
		t.Errorf("the top rolled back to %v", e.flatAt(top.ChangeList.Current().Commit))
	}
}

// A path the rollback would make a file where main has since made a directory, or the
// reverse, is left whole, and named.
func TestRollbackFileAndDirectory(t *testing.T) {
	e := newCLEnv(t)
	e.landChange("one", map[string]string{"p": "file", "q": "0"})
	two := e.landChange("two", map[string]string{"q": "1"})
	e.landChange("three", map[string]string{"q": "1", "p/z": "z"})
	r := e.mustRollback(two)
	if want := (Rollback{CLs: []int64{two}, Paths: []string{"q"}, Left: []string{"p"}}); !reflect.DeepEqual(*r.ChangeList.Rollback, want) {
		t.Errorf("rollback %+v, want %+v", *r.ChangeList.Rollback, want)
	}
	if want := map[string]string{"q": "0", "p/z": "z"}; !reflect.DeepEqual(e.flatAt(r.ChangeList.Current().Commit), want) {
		t.Errorf("rolled back to %v", e.flatAt(r.ChangeList.Current().Commit))
	}
}
