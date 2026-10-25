package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// clReads are everything a reader learns of a repository's CLs and main's log.
type clReads struct {
	CLs     map[int64]ChangeList
	Pages   map[string][]int64
	Lookups []int64
	Logs    map[string]LogPage
}

// readCLs reads every CL numbered up to last, every page of every listing, every lookup of
// changes, and main's log whole, paged, and from a commit, with times in UTC.
func readCLs(t *testing.T, s *Store, repo, last int64, changes []encoding.ChangeID, from encoding.ID) clReads {
	t.Helper()
	ctx := context.Background()
	utc := func(c ChangeList) ChangeList {
		c.CreatedAt, c.UpdatedAt = c.CreatedAt.UTC(), c.UpdatedAt.UTC()
		pss := make([]Patchset, len(c.Patchsets))
		for i, p := range c.Patchsets {
			p.UploadedAt = p.UploadedAt.UTC()
			pss[i] = p
		}
		c.Patchsets = pss
		return c
	}
	r := clReads{CLs: map[int64]ChangeList{}, Pages: map[string][]int64{}, Logs: map[string]LogPage{}}
	for n := int64(1); n <= last; n++ {
		c, err := s.GetChangeList(ctx, repo, n)
		if err != nil {
			t.Fatalf("CL %d: %v", n, err)
		}
		r.CLs[n] = utc(c)
	}
	for _, l := range []struct {
		statuses []Status
		author   string
	}{{nil, ""}, {[]Status{StatusSubmitted}, ""}, {[]Status{StatusOpen, StatusAbandoned}, ""}, {nil, alice}, {nil, bob},
		{[]Status{StatusSubmitted}, alice}, {[]Status{StatusDraft}, ""}} {
		for _, size := range []int{1, 2, 100} {
			key := fmt.Sprint(l.statuses, l.author, size)
			var before int64
			for {
				page, next, err := s.ListChangeLists(ctx, repo, l.statuses, l.author, 0, size, before)
				if err != nil {
					t.Fatalf("listing %s: %v", key, err)
				}
				for _, c := range page {
					r.Pages[key] = append(r.Pages[key], c.Number)
				}
				if next == 0 {
					break
				}
				before = next
			}
		}
	}
	found, err := s.LookupChanges(ctx, repo, append(changes, newChange()))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range found {
		r.Lookups = append(r.Lookups, c.Number)
	}
	for _, l := range []struct {
		name string
		from *encoding.ID
		size int
	}{{"whole", nil, 100}, {"from", &from, 100}, {"pages", nil, 2}} {
		var before int64
		for i := 0; ; i++ {
			page, err := s.Log(ctx, repo, l.from, nil, l.size, before, 0)
			if err != nil {
				t.Fatalf("log %s: %v", l.name, err)
			}
			r.Logs[fmt.Sprint(l.name, i)] = page
			if page.Older == 0 {
				break
			}
			before = page.Older
		}
	}
	return r
}

// Landed CLs and main's log move out of the tail into segments, and read the same from a replica
// with nothing cached: every CL, every page of every listing, lookups by change, and main's log.
// What reads them to write — an upload of a landed change or stacked on a landed patchset, a
// rollback of a stack, a landing after — works as it does from the tail.
func TestLandedChangeListsInSegments(t *testing.T) {
	e := newCLEnv(t)
	e.landChange("base", map[string]string{"f": "0"})
	ca, cb := newChange(), newChange()
	a := e.commit(alice, ca, e.head(), "bottom", map[string]string{"f": "0", "s": "1"})
	b := e.commit(alice, cb, a, "top", map[string]string{"f": "0", "s": "2", "t": "2"})
	res := e.mustUpload(alice, Ready, a, b)
	na, nb := res[0].Number, res[1].Number
	e.mustLand(LandItem{Number: na, Patchset: 1})
	landedA := e.head()
	e.landChange("between", map[string]string{"f": "0", "s": "1", "u": "x"})
	e.mustLand(LandItem{Number: nb, Patchset: 1})
	co, cg := newChange(), newChange()
	open := e.mustUpload(bob, Ready, e.commit(bob, co, e.head(), "open", map[string]string{"f": "1"}))[0].Number
	gone := e.mustUpload(alice, WIP, e.commit(alice, cg, e.head(), "gone", map[string]string{"f": "2"}))[0].Number
	e.transition(gone, Abandon)
	last := max(open, gone)
	changes := []encoding.ChangeID{ca, cb, co, cg}

	before := readCLs(t, e.s, e.repo, last, changes, landedA)
	compactAll(t, e.s, e.repo)
	for table, want := range map[string]int{"changelist": 2, "patchset": 2, "head_log": 0} {
		if n := dbOf(e.s).count(t, table, `repository_id = @repo`, map[string]any{"repo": e.repo}); n != want {
			t.Errorf("after compaction the tail holds %d rows of %s, want %d", n, table, want)
		}
	}
	if after := readCLs(t, reopen(t, e.s), e.repo, last, changes, landedA); !reflect.DeepEqual(before, after) {
		t.Errorf("read from segments:\n%+v\nwant, from the tail:\n%+v", after, before)
	}

	// Writers that read landed CLs.
	_, err := e.upload(alice, Ready, nil, e.commit(alice, ca, e.head(), "again", map[string]string{"f": "3"}))
	var rej *UploadRejection
	if !errors.As(err, &rej) || rej.Reason != ReasonClosed || rej.Number != na {
		t.Errorf("an upload of a landed CL's change: %v, want refused as closed", err)
	}
	_, err = e.upload(alice, Ready, nil, e.commit(alice, newChange(), b, "on top", map[string]string{"f": "3"}))
	if !errors.As(err, &rej) || rej.Reason != ReasonStaleBase || rej.Number != nb {
		t.Errorf("an upload stacked on a landed patchset: %v, want refused as stale", err)
	}
	if _, _, err := e.s.Transition(context.Background(), e.repo, na, Abandon, alice, ""); err == nil {
		t.Error("a landed CL in segments was abandoned")
	}
	r := e.mustRollback(na)
	if want := []int64{na, nb}; !reflect.DeepEqual(r.ChangeList.Rollback.CLs, want) {
		t.Errorf("the stack's rollback, its CLs in segments: %v, want %v", r.ChangeList.Rollback.CLs, want)
	}
	if _, err := e.rollback(na); err == nil {
		t.Error("a second rollback of a CL in segments was not refused while the first is open")
	}
	n := e.landChange("after", map[string]string{"f": "4"})
	head := e.head()
	page, err := reopen(t, e.s).Log(context.Background(), e.repo, nil, nil, 3, 0, 0)
	if err != nil || len(page.Commits) != 3 || page.Commits[0].ID != head {
		t.Fatalf("main's log after a landing on segments: %+v, %v", page, err)
	}
	if c := e.cl(n); c.Status != StatusSubmitted {
		t.Errorf("CL %d landed after compaction is %s", n, c.Status)
	}
}

// A landed CL's record reads back as written, and a damaged one is refused.
func TestSettledRecord(t *testing.T) {
	landed := encoding.ID{9}
	at := time.UnixMicro(1_790_000_000_123_456)
	sc := backend.SettledChangeList{
		ChangeList: backend.ChangeList{ID: 4242, Number: 7, ChangeID: make([]byte, encoding.ChangeIDLen), Author: alice, Status: 3,
			Parent: 5, CurrentPatchset: 2, Landed: &landed, SubmittedBy: bob, Description: "a\nb", CreatedAt: at, UpdatedAt: at.Add(time.Second),
			RollsBack: []int64{3, 4}, RollbackPaths: []string{"x", "y/z"}, RollbackLeft: []string{"w"}},
		Patchsets: []backend.Patchset{
			{CL: 4242, Number: 1, Commit: encoding.ID{1}, Base: encoding.ID{2}, Parent: 5, UploadedBy: alice, UploadedAt: at},
			{CL: 4242, Number: 2, Commit: encoding.ID{3}, Base: encoding.ID{4}, UploadedBy: alice, UploadedAt: at.Add(time.Minute)},
		},
		Landing:  backend.HeadMove{Seq: 11, Old: encoding.ID{5}, New: landed, CL: 7, Patchset: 2, By: bob},
		LandedAt: at.Add(time.Hour),
	}
	b := encodeSettled(sc)
	got, err := decodeSettled(7, b)
	if err != nil || !reflect.DeepEqual(got, sc) {
		t.Fatalf("decoded %+v, %v\nwant %+v", got, err, sc)
	}
	for _, bad := range [][]byte{nil, {1}, {recordVersion}, b[:len(b)-1], append(append([]byte{}, b...), 0)} {
		if _, err := decodeSettled(7, bad); err == nil {
			t.Errorf("a damaged record of %d bytes decoded", len(bad))
		}
	}
}
