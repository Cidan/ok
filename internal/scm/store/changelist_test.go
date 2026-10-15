package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

const (
	alice = "alice.human.example.com"
	bob   = "bob.human.example.com"
)

// fakePeople knows every handle except those starting with "nobody".
type fakePeople struct{}

func (fakePeople) KnownHuman(_ context.Context, handle string) (bool, error) {
	return !strings.HasPrefix(handle, "nobody"), nil
}

func (fakePeople) KnownACL(_ context.Context, name string) (bool, error) {
	return name == "admins", nil
}

// flat adds a tree holding the flat paths (each a file with the given contents) and
// returns its ID.
func (b *builder) flat(files map[string]string) encoding.ID {
	sub := map[string]map[string]string{}
	var entries []encoding.TreeEntry
	for p, contents := range files {
		name, rest, nested := strings.Cut(p, "/")
		if !nested {
			entries = append(entries, fileEntry(name, b.file([]byte(contents))))
			continue
		}
		if sub[name] == nil {
			sub[name] = map[string]string{}
		}
		sub[name][rest] = contents
	}
	for name, f := range sub {
		entries = append(entries, treeEntry(name, b.flat(f)))
	}
	if len(entries) == 0 {
		return encoding.EmptyTreeID
	}
	return b.tree(entries...)
}

// newChange returns a random change ID, as jj makes them.
func newChange() encoding.ChangeID {
	var c encoding.ChangeID
	binary.LittleEndian.PutUint64(c[:8], rand.Uint64())
	binary.LittleEndian.PutUint64(c[8:], rand.Uint64())
	return c
}

// change adds a commit of change on parent with the flat files, and returns its ID.
func (b *builder) change(change encoding.ChangeID, parent encoding.ID, desc string, files map[string]string) encoding.ID {
	b.seq++
	sig := encoding.Signature{Name: "alice", Email: "alice@example.com", Time: encoding.Timestamp{Millis: 1_790_000_000_000 + int64(b.seq)}}
	return b.add(&encoding.Commit{
		Parents:        []encoding.ID{parent},
		RootTree:       encoding.Merge[encoding.ID]{b.flat(files)},
		ConflictLabels: encoding.Merge[string]{""},
		ChangeID:       change,
		Description:    desc,
		Author:         sig,
		Committer:      sig,
	})
}

// clEnv is one test's repository with a builder that publishes as it goes.
type clEnv struct {
	t    *testing.T
	s    *Store
	repo int64
}

func newCLEnv(t *testing.T) *clEnv {
	s := testStore(t)
	return &clEnv{t: t, s: s, repo: newRepo(t, s).ID}
}

// commit publishes a commit of change on parent with files, as principal.
func (e *clEnv) commit(principal string, change encoding.ChangeID, parent encoding.ID, desc string, files map[string]string) encoding.ID {
	e.t.Helper()
	b := newBuilder(e.t)
	id := b.change(change, parent, desc, files)
	mustPublish(e.t, e.s, e.repo, principal, b, nil)
	return id
}

func (e *clEnv) upload(principal string, readiness Readiness, reviewers []string, commits ...encoding.ID) ([]UploadResult, error) {
	return e.s.Upload(context.Background(), e.repo, principal, commits, readiness, reviewers, fakePeople{})
}

func (e *clEnv) mustUpload(principal string, readiness Readiness, commits ...encoding.ID) []UploadResult {
	e.t.Helper()
	res, err := e.upload(principal, readiness, nil, commits...)
	if err != nil {
		e.t.Fatalf("upload: %v", err)
	}
	return res
}

func (e *clEnv) cl(number int64) ChangeList {
	e.t.Helper()
	c, err := e.s.GetChangeList(context.Background(), e.repo, number)
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

func (e *clEnv) head() encoding.ID {
	e.t.Helper()
	r, err := e.s.GetRepository(context.Background(), e.repo)
	if err != nil {
		e.t.Fatal(err)
	}
	return r.Head
}

func (e *clEnv) transition(number int64, kind TransitionKind) ChangeList {
	e.t.Helper()
	c, _, err := e.s.Transition(context.Background(), e.repo, number, kind, alice, "")
	if err != nil {
		e.t.Fatalf("transition: %v", err)
	}
	return c
}

func (e *clEnv) land(expected encoding.ID, dryRun bool, items ...LandItem) ([]Landed, encoding.ID, error) {
	landed, head, _, err := e.s.Land(context.Background(), e.repo, items, expected, alice, dryRun)
	return landed, head, err
}

func (e *clEnv) mustLand(items ...LandItem) []Landed {
	e.t.Helper()
	landed, _, err := e.land(e.head(), false, items...)
	if err != nil {
		e.t.Fatalf("land: %v", err)
	}
	return landed
}

// open uploads a commit as alice and sends its CL for review.
func (e *clEnv) open(commit encoding.ID) int64 {
	e.t.Helper()
	return e.mustUpload(alice, Ready, commit)[0].Number
}

// flatAt reads a commit's tree as path → file contents.
func (e *clEnv) flatAt(commit encoding.ID) map[string]string {
	e.t.Helper()
	ctx := context.Background()
	c, err := e.s.readCommit(ctx, e.repo, commit)
	if err != nil {
		e.t.Fatal(err)
	}
	out := map[string]string{}
	if err := diffTrees(e.s.pendingTrees(ctx, e.repo), encoding.EmptyTreeID, c.RootTree[0], "", func(ch leafChange) error {
		data, err := e.s.readFile(ctx, e.repo, ch.After.ID)
		out[ch.Path] = string(data)
		return err
	}); err != nil {
		e.t.Fatal(err)
	}
	return out
}

func uploadReason(t *testing.T, err error) *UploadRejection {
	t.Helper()
	var rej *UploadRejection
	if !errors.As(err, &rej) {
		t.Fatalf("got %v, want an upload rejection", err)
	}
	return rej
}

func landReason(t *testing.T, err error) *LandRejection {
	t.Helper()
	var rej *LandRejection
	if !errors.As(err, &rej) {
		t.Fatalf("got %v, want a land rejection", err)
	}
	return rej
}

func TestUploadStackAndPatchsets(t *testing.T) {
	e := newCLEnv(t)
	a, b := newChange(), newChange()
	c1 := e.commit(alice, a, encoding.RootCommitID, "add x", map[string]string{"x": "1"})
	c2 := e.commit(alice, b, c1, "add y", map[string]string{"x": "1", "y": "1"})
	res := e.mustUpload(alice, ReadinessUnchanged, c1, c2)
	if res[0].Outcome != Created || res[1].Outcome != Created || res[0].Number+1 != res[1].Number {
		t.Fatalf("results %+v", res)
	}
	n1, n2 := res[0].Number, res[1].Number
	cl1, cl2 := e.cl(n1), e.cl(n2)
	if cl1.Status != StatusDraft || cl1.Author != alice || cl1.ParentNumber != 0 || cl1.Description != "add x" {
		t.Fatalf("CL 1: %+v", cl1)
	}
	if cl2.ParentNumber != n1 || cl2.Current().Commit != c2 || cl2.Current().Base != c1 {
		t.Fatalf("CL 2: %+v", cl2)
	}

	// Nothing new: refused.
	_, err := e.upload(alice, ReadinessUnchanged, nil, c1, c2)
	if rej := uploadReason(t, err); rej.Reason != ReasonNoNewChanges {
		t.Fatalf("re-upload: %v", rej)
	}

	// A new patchset of the top of the stack, sent for review.
	c2b := e.commit(alice, b, c1, "add y and z", map[string]string{"x": "1", "y": "1", "z": "1"})
	res = e.mustUpload(alice, Ready, c1, c2b)
	if res[0].Outcome != Unchanged || res[1].Outcome != NewPatchset || res[1].Patchset != 2 || res[1].Number != n2 {
		t.Fatalf("results %+v", res)
	}
	cl2 = e.cl(n2)
	if cl2.CurrentPatchset != 2 || cl2.Status != StatusOpen || cl2.Description != "add y and z" || len(cl2.Patchsets) != 2 {
		t.Fatalf("CL 2 after patchset 2: %+v", cl2)
	}
	if e.cl(n1).Status != StatusDraft {
		t.Fatal("ready applied to an unchanged CL")
	}
	// wip returns it to draft with a new patchset.
	c2c := e.commit(alice, b, c1, "add y and z, again", map[string]string{"x": "1", "y": "2", "z": "1"})
	e.mustUpload(alice, WIP, c1, c2c)
	if st := e.cl(n2).Status; st != StatusDraft {
		t.Fatalf("after wip: %s", st)
	}

	// Re-uploading an old patchset's commit is a new patchset.
	res = e.mustUpload(alice, ReadinessUnchanged, c1, c2)
	if res[1].Outcome != NewPatchset || res[1].Patchset != 4 {
		t.Fatalf("results %+v", res)
	}

	// Patchset commits and what they reach are pinned.
	for _, id := range []encoding.ID{c1, c2, c2b, c2c} {
		var pinned bool
		if err := e.s.pool.QueryRow(context.Background(), `SELECT pinned FROM commit WHERE repository_id = $1 AND id = $2`, e.repo, id[:]).Scan(&pinned); err != nil || !pinned {
			t.Fatalf("commit %s pinned=%v err=%v", id, pinned, err)
		}
	}
	var unpinned int
	if err := e.s.pool.QueryRow(context.Background(), `SELECT count(*) FROM object WHERE repository_id = $1 AND NOT pinned`, e.repo).Scan(&unpinned); err != nil {
		t.Fatal(err)
	}
	for _, k := range indexOf(t, e.s, e.repo).Sorted() {
		if !k.Entry.Pinned {
			unpinned++
		}
	}
	if unpinned != 0 {
		t.Fatalf("%d objects left unpinned", unpinned)
	}
}

func TestUploadRules(t *testing.T) {
	e := newCLEnv(t)

	// A CL of alice's, open, with two patchsets; one abandoned; one landed by splice.
	shared := newChange()
	s1 := e.commit(alice, shared, encoding.RootCommitID, "shared", map[string]string{"s": "1"})
	sharedCL := e.open(s1)
	s2 := e.commit(alice, shared, encoding.RootCommitID, "shared v2", map[string]string{"s": "2"})
	e.mustUpload(alice, ReadinessUnchanged, s2)

	gone := newChange()
	g1 := e.commit(alice, gone, encoding.RootCommitID, "gone", map[string]string{"g": "1"})
	goneCL := e.open(g1)
	e.transition(goneCL, Abandon)

	first := e.commit(alice, newChange(), encoding.RootCommitID, "first", map[string]string{"f": "1"})
	e.mustLand(LandItem{Number: e.open(first), Patchset: 1})
	spliced := newChange()
	sp1 := e.commit(alice, spliced, encoding.RootCommitID, "spliced", map[string]string{"q": "1"})
	splicedCL := e.open(sp1)
	e.mustLand(LandItem{Number: splicedCL, Patchset: 1})

	// Commits for each rule.
	b := newBuilder(t)
	tree := b.flat(map[string]string{"m": "1"})
	sig := encoding.Signature{Name: "a", Email: "a@example.com"}
	merge := b.add(&encoding.Commit{Parents: []encoding.ID{first, s2}, RootTree: encoding.Merge[encoding.ID]{tree}, ConflictLabels: encoding.Merge[string]{""}, ChangeID: newChange(), Description: "merge", Author: sig, Committer: sig})
	t2, t3 := b.flat(map[string]string{"m": "2"}), b.flat(map[string]string{"m": "3"})
	conflicted := b.add(&encoding.Commit{Parents: []encoding.ID{first}, RootTree: encoding.Merge[encoding.ID]{tree, t2, t3}, ConflictLabels: encoding.Merge[string]{"", "", ""}, ChangeID: newChange(), Description: "conflicted", Author: sig, Committer: sig})
	mustPublish(t, e.s, e.repo, alice, b, nil)
	undescribed := e.commit(alice, newChange(), first, "  \n", map[string]string{"f": "1", "u": "1"})
	empty := e.commit(alice, newChange(), first, "empty", map[string]string{"f": "1"})
	orphanParent := e.commit(alice, newChange(), first, "never uploaded", map[string]string{"f": "1", "o": "1"})
	orphan := e.commit(alice, newChange(), orphanParent, "on an orphan", map[string]string{"f": "1", "o": "1", "p": "1"})
	onStale := e.commit(alice, newChange(), s1, "on patchset 1", map[string]string{"s": "1", "t": "1"})
	onAbandoned := e.commit(alice, newChange(), g1, "on abandoned", map[string]string{"g": "1", "h": "1"})
	onLanded := e.commit(alice, newChange(), sp1, "on landed", map[string]string{"q": "1", "r": "1"})
	bobsPatchset := e.commit(bob, shared, encoding.RootCommitID, "bob's take", map[string]string{"s": "bob"})
	toAbandoned := e.commit(alice, gone, encoding.RootCommitID, "gone v2", map[string]string{"g": "2"})
	toLanded := e.commit(alice, spliced, encoding.RootCommitID, "spliced v2", map[string]string{"q": "2"})
	dupChange := newChange()
	dup1 := e.commit(alice, dupChange, first, "dup", map[string]string{"f": "1", "d": "1"})
	dup2 := e.commit(alice, dupChange, dup1, "dup again", map[string]string{"f": "1", "d": "2"})
	badOwners := e.commit(alice, newChange(), first, "owners", map[string]string{"f": "1", "dir/OWNERS": "user: nobody\nuser: carol\nbogus line\n", "dir/x": "x"})
	goodOwners := e.commit(alice, newChange(), first, "owners", map[string]string{"f": "1", "dir/OWNERS": "user: carol\nacl: admins\nper-file *.go=file: //team/OWNERS\n", "team/OWNERS": "user: dave\n", "dir/x": "x"})
	badACL := e.commit(alice, newChange(), first, "acl owners", map[string]string{"f": "1", "auth/OWNERS": "acl: eng\n", "auth/x": "x"})
	badWorkflow := e.commit(alice, newChange(), first, "workflow", map[string]string{"f": "1",
		"apps/.ok/workflow/ci.yaml": "include:\n  - //.ok/workflow/missing.yaml\npre:\n  build:\n    target: apps\n"})
	goodWorkflow := e.commit(alice, newChange(), first, "workflow", map[string]string{"f": "1",
		".ok/workflow/common.yaml":  "pre:\n  lint:\n    target: //tools/x:x\n",
		"apps/.ok/workflow/ci.yaml": "include:\n  - //.ok/workflow/common.yaml\npre:\n  build:\n    target: //apps/a:a\n    args: [\"{changed_files:**/*.go}\"]\n"})
	badSettings := e.commit(alice, newChange(), first, "settings", map[string]string{"f": "1", ".ok/repository.yaml": "identity: identity\n"})
	goodSettings := e.commit(alice, newChange(), first, "settings", map[string]string{"f": "1", ".ok/repository.yaml": "build: none\nidentity: identity\n"})
	ok := e.commit(alice, newChange(), first, "fine", map[string]string{"f": "1", "ok": "1"})

	for _, tc := range []struct {
		name      string
		who       string
		commits   []encoding.ID
		reviewers []string
		reason    UploadReason
		number    int64
		problem   string
	}{
		{"a merge commit", alice, []encoding.ID{merge}, nil, ReasonMergeCommit, 0, ""},
		{"a conflicted commit", alice, []encoding.ID{conflicted}, nil, ReasonConflicted, 0, ""},
		{"no description", alice, []encoding.ID{undescribed}, nil, ReasonEmptyDesc, 0, ""},
		{"an empty change", alice, []encoding.ID{empty}, nil, ReasonEmptyChange, 0, ""},
		{"a parent nobody uploaded", alice, []encoding.ID{orphan}, nil, ReasonUnknownBase, 0, ""},
		{"stacked on an older patchset", alice, []encoding.ID{onStale}, nil, ReasonStaleBase, sharedCL, ""},
		{"stacked on an abandoned CL", alice, []encoding.ID{onAbandoned}, nil, ReasonUnknownBase, goneCL, ""},
		{"stacked on a CL that landed by splice", alice, []encoding.ID{onLanded}, nil, ReasonStaleBase, splicedCL, ""},
		{"someone else's CL", bob, []encoding.ID{bobsPatchset}, nil, ReasonNotAuthor, sharedCL, ""},
		{"an abandoned CL", alice, []encoding.ID{toAbandoned}, nil, ReasonClosed, goneCL, ""},
		{"a landed CL", alice, []encoding.ID{toLanded}, nil, ReasonClosed, splicedCL, ""},
		{"one change twice", alice, []encoding.ID{dup1, dup2}, nil, ReasonDivergentChange, 0, ""},
		{"an invalid OWNERS file", alice, []encoding.ID{badOwners}, nil, ReasonInvalidOwners, 0, "dir/OWNERS:1: user: nobody names no person"},
		{"an OWNERS file naming no ACL", alice, []encoding.ID{badACL}, nil, ReasonInvalidOwners, 0, "auth/OWNERS:1: acl: eng names no ACL the groups-and-ACLs tree declares"},
		{"an invalid workflow file", alice, []encoding.ID{badWorkflow}, nil, ReasonInvalidWorkflow, 0, `apps/.ok/workflow/ci.yaml:2: include //.ok/workflow/missing.yaml does not exist`},
		{"repository settings that do not parse", alice, []encoding.ID{badSettings}, nil, ReasonInvalidWorkflow, 0, ".ok/repository.yaml: build is required: bazel, or none"},
		{"an unknown reviewer", alice, []encoding.ID{ok}, []string{"carol", "nobody-else@example.com"}, ReasonUnknownReviewer, 0, "nobody-else@example.com"},
		{"the first bad commit is the one reported", alice, []encoding.ID{ok, empty, merge}, nil, ReasonEmptyChange, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := e.upload(tc.who, ReadinessUnchanged, tc.reviewers, tc.commits...)
			rej := uploadReason(t, err)
			if rej.Reason != tc.reason || rej.Number != tc.number {
				t.Fatalf("got reason %d CL %d (%s), want reason %d CL %d", rej.Reason, rej.Number, rej.Message, tc.reason, tc.number)
			}
			if tc.problem != "" && !contains(rej.Problems, tc.problem) {
				t.Fatalf("problems %q lack %q", rej.Problems, tc.problem)
			}
		})
	}
	if _, err := e.upload(alice, ReadinessUnchanged, nil, encoding.Hash([]byte("never published"))); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an unpublished commit: %v", err)
	}
	if _, err := e.upload(alice, ReadinessUnchanged, []string{"carol", "dave@example.com"}, goodOwners); err != nil {
		t.Fatalf("a valid OWNERS file: %v", err)
	}
	if _, err := e.upload(alice, ReadinessUnchanged, nil, goodWorkflow); err != nil {
		t.Fatalf("a valid workflow file: %v", err)
	}
	if _, err := e.upload(alice, ReadinessUnchanged, nil, goodSettings); err != nil {
		t.Fatalf("valid repository settings: %v", err)
	}

	// Anyone may re-upload someone else's current patchset unchanged, and stack on it.
	mine := e.commit(bob, newChange(), s2, "bob on alice", map[string]string{"s": "2", "b": "1"})
	res, err := e.upload(bob, ReadinessUnchanged, nil, s2, mine)
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Outcome != Unchanged || res[1].Outcome != Created || e.cl(res[1].Number).ParentNumber != sharedCL || e.cl(res[1].Number).Author != bob {
		t.Fatalf("results %+v", res)
	}
	if c := e.cl(sharedCL); c.CurrentPatchset != 2 || c.Author != alice {
		t.Fatalf("alice's CL changed: %+v", c)
	}
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if strings.Contains(s, want) {
			return true
		}
	}
	return false
}

// TestUploadRace uploads one new change from many uploaders at once: exactly one creates
// the CL, and the rest find nothing new.
func TestUploadRace(t *testing.T) {
	e := newCLEnv(t)
	c := e.commit(alice, newChange(), encoding.RootCommitID, "raced", map[string]string{"r": "1"})
	var wg sync.WaitGroup
	outcomes := make([]error, 8)
	created := make([]bool, 8)
	for i := range outcomes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := e.upload(alice, ReadinessUnchanged, nil, c)
			outcomes[i] = err
			created[i] = err == nil && res[0].Outcome == Created
		}()
	}
	wg.Wait()
	n := 0
	for i, err := range outcomes {
		if created[i] {
			n++
			continue
		}
		if rej := uploadReason(t, err); rej.Reason != ReasonNoNewChanges {
			t.Fatalf("uploader %d: %v", i, err)
		}
	}
	if n != 1 {
		t.Fatalf("%d uploads created the CL", n)
	}
	cls, _, err := e.s.ListChangeLists(context.Background(), e.repo, nil, "", 0, 0)
	if err != nil || len(cls) != 1 {
		t.Fatalf("%d CLs (%v)", len(cls), err)
	}
}

func TestTransitions(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	c := e.commit(alice, newChange(), encoding.RootCommitID, "t", map[string]string{"t": "1"})
	n := e.mustUpload(alice, ReadinessUnchanged, c)[0].Number
	step := func(kind TransitionKind, want Status) {
		t.Helper()
		if got := e.transition(n, kind); got.Status != want {
			t.Fatalf("transition %d: %s, want %s", kind, got.Status, want)
		}
	}
	refuse := func(kind TransitionKind) {
		t.Helper()
		if _, _, err := e.s.Transition(ctx, e.repo, n, kind, alice, ""); !errors.Is(err, ErrPrecondition) {
			t.Fatalf("transition %d from %s: %v", kind, e.cl(n).Status, err)
		}
	}
	refuse(ReturnToDraft)
	refuse(Restore)
	step(RequestReview, StatusOpen)
	refuse(RequestReview)
	step(ReturnToDraft, StatusDraft)
	if _, _, err := e.s.Transition(ctx, e.repo, n, Abandon, alice, "not needed"); err != nil {
		t.Fatal(err)
	}
	if got := e.cl(n); got.Status != StatusAbandoned || got.AbandonedFrom != StatusDraft {
		t.Fatalf("abandoned: %+v", got)
	}
	refuse(Abandon)
	refuse(RequestReview)
	step(Restore, StatusDraft)
	step(RequestReview, StatusOpen)
	step(Abandon, StatusAbandoned)
	step(Restore, StatusOpen)
	if _, _, err := e.s.Transition(ctx, e.repo, n, RequestReview, alice, "why"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a reason on request review: %v", err)
	}
	if _, _, err := e.s.Transition(ctx, e.repo, 9999, Abandon, alice, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a missing CL: %v", err)
	}
	e.mustLand(LandItem{Number: n, Patchset: 1})
	refuse(Abandon)
	refuse(ReturnToDraft)
}

func TestLandFastForwardAndSplice(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	c1 := e.commit(alice, newChange(), encoding.RootCommitID, "one", map[string]string{"a/x": "1", "k": "k"})
	c2 := e.commit(alice, newChange(), encoding.RootCommitID, "two", map[string]string{"b/y": "2"})
	n1, n2 := e.open(c1), e.open(c2)

	// The first lands by fast-forward: the reviewed commit itself.
	landed := e.mustLand(LandItem{Number: n1, Patchset: 1})
	if landed[0].Commit != c1 || e.head() != c1 {
		t.Fatalf("fast-forward landed %s, head %s, want %s", landed[0].Commit, e.head(), c1)
	}
	cl1 := e.cl(n1)
	if cl1.Status != StatusSubmitted || *cl1.LandedCommit != c1 || cl1.SubmittedBy != alice {
		t.Fatalf("CL 1: %+v", cl1)
	}

	// The second lands as a splice onto the moved head.
	landed = e.mustLand(LandItem{Number: n2, Patchset: 1})
	l2 := landed[0].Commit
	if l2 == c2 || e.head() != l2 {
		t.Fatalf("splice landed %s, head %s", l2, e.head())
	}
	got, err := e.s.readCommit(ctx, e.repo, l2)
	if err != nil {
		t.Fatal(err)
	}
	orig, _ := e.s.readCommit(ctx, e.repo, c2)
	if !reflect.DeepEqual(got.Parents, []encoding.ID{c1}) || !reflect.DeepEqual(got.Predecessors, []encoding.ID{c2}) ||
		got.ChangeID != orig.ChangeID || got.Description != "two" || got.Author != orig.Author ||
		got.Committer.Name != "alice" || got.Committer.Email != "alice@example.com" || got.SecureSig != nil {
		t.Fatalf("spliced commit %+v", got)
	}
	if want := map[string]string{"a/x": "1", "k": "k", "b/y": "2"}; !reflect.DeepEqual(e.flatAt(l2), want) {
		t.Fatalf("landed tree %v", e.flatAt(l2))
	}

	// main's history, the paths index, and pins.
	var logged int
	if err := e.s.pool.QueryRow(ctx, `SELECT count(*) FROM head_log WHERE repository_id = $1`, e.repo).Scan(&logged); err != nil || logged != 2 {
		t.Fatalf("head_log has %d rows (%v)", logged, err)
	}
	if r, _ := e.s.GetRepository(ctx, e.repo); r.HeadSeq != 2 {
		t.Fatalf("head_seq %d", r.HeadSeq)
	}
	rows, err := e.s.pool.Query(ctx, `SELECT path FROM commit_path WHERE repository_id = $1 AND commit_id = $2 ORDER BY path`, e.repo, l2[:])
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for rows.Next() {
		var p string
		rows.Scan(&p)
		paths = append(paths, p)
	}
	rows.Close()
	if !reflect.DeepEqual(paths, []string{"b", "b/y"}) {
		t.Fatalf("indexed paths %v, want the changed file and its directory", paths)
	}
	var pinned bool
	if err := e.s.pool.QueryRow(ctx, `SELECT pinned FROM commit WHERE repository_id = $1 AND id = $2`, e.repo, l2[:]).Scan(&pinned); err != nil || !pinned {
		t.Fatalf("landed commit pinned=%v (%v)", pinned, err)
	}
	var unpinned int
	if err := e.s.pool.QueryRow(ctx, `SELECT count(*) FROM object WHERE repository_id = $1 AND NOT pinned`, e.repo).Scan(&unpinned); err != nil || unpinned != 0 {
		t.Fatalf("%d trees unpinned after landing (%v)", unpinned, err)
	}
	// Upload now knows both landed commits are on main.
	c3 := e.commit(alice, newChange(), l2, "three", map[string]string{"a/x": "1", "k": "k", "b/y": "2", "c": "3"})
	if _, err := e.upload(alice, ReadinessUnchanged, nil, c3); err != nil {
		t.Fatalf("upload on main's head: %v", err)
	}
}

func TestLandRefusals(t *testing.T) {
	e := newCLEnv(t)
	base := e.commit(alice, newChange(), encoding.RootCommitID, "base", map[string]string{"f": "0", "g": "0"})
	e.mustLand(LandItem{Number: e.open(base), Patchset: 1})
	h := e.head()

	draft := e.commit(alice, newChange(), h, "draft", map[string]string{"f": "0", "g": "0", "d": "1"})
	draftCL := e.mustUpload(alice, ReadinessUnchanged, draft)[0].Number
	stale := newChange()
	s1 := e.commit(alice, stale, h, "stale", map[string]string{"f": "0", "g": "0", "s": "1"})
	staleCL := e.open(s1)
	e.mustUpload(alice, ReadinessUnchanged, e.commit(alice, stale, h, "stale 2", map[string]string{"f": "0", "g": "0", "s": "2"}))
	one := e.commit(alice, newChange(), h, "f to 1", map[string]string{"f": "1", "g": "0"})
	oneCL := e.open(one)
	also := e.commit(alice, newChange(), h, "f to 2", map[string]string{"f": "2", "g": "1"})
	alsoCL := e.open(also)
	same := e.commit(alice, newChange(), h, "f to 1 as well", map[string]string{"f": "1", "g": "0"})
	sameCL := e.open(same)

	check := func(name string, expected encoding.ID, reason LandReason, paths []string, items ...LandItem) {
		t.Helper()
		for _, dry := range []bool{true, false} {
			before := e.head()
			_, _, err := e.land(expected, dry, items...)
			rej := landReason(t, err)
			if rej.Reason != reason || !reflect.DeepEqual(rej.Paths, paths) {
				t.Fatalf("%s (dry run %v): reason %d paths %v (%s), want %d %v", name, dry, rej.Reason, rej.Paths, rej.Message, reason, paths)
			}
			if e.head() != before {
				t.Fatalf("%s moved main", name)
			}
		}
	}
	check("a draft", h, LandNotOpen, nil, LandItem{Number: draftCL, Patchset: 1})
	check("an old patchset", h, LandStalePatchset, nil, LandItem{Number: staleCL, Patchset: 1})
	check("a moved head", encoding.RootCommitID, LandHeadMoved, nil, LandItem{Number: oneCL, Patchset: 1})
	e.mustLand(LandItem{Number: oneCL, Patchset: 1})
	check("a path conflict", e.head(), LandPathConflict, []string{"f"}, LandItem{Number: alsoCL, Patchset: 1})
	check("already on main", e.head(), LandAlreadyOnMain, nil, LandItem{Number: sameCL, Patchset: 1})
	if st := e.cl(alsoCL).Status; st != StatusOpen {
		t.Fatalf("a refused CL is %s", st)
	}

	// The conflicting CL, rebased onto main and re-uploaded (what jj sync and jj cl do),
	// lands.
	also2 := e.commit(alice, e.cl(alsoCL).ChangeID, e.head(), "f to 2", map[string]string{"f": "2", "g": "1"})
	e.mustUpload(alice, ReadinessUnchanged, also2)
	if landed := e.mustLand(LandItem{Number: alsoCL, Patchset: 2}); landed[0].Commit != also2 {
		t.Fatal("a patchset on main's head did not fast-forward")
	}
}

func TestLandStackAndDryRun(t *testing.T) {
	e := newCLEnv(t)
	other := e.commit(alice, newChange(), encoding.RootCommitID, "other", map[string]string{"o": "1"})
	otherCL := e.open(other)
	a := e.commit(alice, newChange(), encoding.RootCommitID, "a", map[string]string{"a": "1"})
	b := e.commit(alice, newChange(), a, "b", map[string]string{"a": "1", "b": "1"})
	res := e.mustUpload(alice, Ready, a, b)
	na, nb := res[0].Number, res[1].Number
	e.mustLand(LandItem{Number: otherCL, Patchset: 1})

	_, _, err := e.land(e.head(), false, LandItem{Number: nb, Patchset: 1})
	if rej := landReason(t, err); rej.Reason != LandParentNotSubmitted || rej.Number != nb {
		t.Fatalf("top of a stack alone: %+v", rej)
	}
	items := []LandItem{{Number: na, Patchset: 1}, {Number: nb, Patchset: 1}}
	dry, dryHead, err := e.land(e.head(), true, items...)
	if err != nil {
		t.Fatal(err)
	}
	if e.cl(na).Status != StatusOpen || e.head() != other {
		t.Fatal("a dry run wrote")
	}
	landed, head, err := e.land(e.head(), false, items...)
	if err != nil {
		t.Fatal(err)
	}
	if len(dry) != 2 || len(landed) != 2 || head != landed[1].Commit || e.head() != head || dryHead == (encoding.ID{}) {
		t.Fatalf("dry %v %s, landed %v %s", dry, dryHead, landed, head)
	}
	if want := map[string]string{"o": "1", "a": "1", "b": "1"}; !reflect.DeepEqual(e.flatAt(head), want) {
		t.Fatalf("stack landed %v", e.flatAt(head))
	}
	lb, _ := e.s.readCommit(context.Background(), e.repo, landed[1].Commit)
	if lb.Parents[0] != landed[0].Commit {
		t.Fatal("the stack's second commit is not on its first")
	}
	for _, n := range []int64{na, nb} {
		if e.cl(n).Status != StatusSubmitted {
			t.Fatalf("CL %d not submitted", n)
		}
	}
}

func TestCandidates(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	base := e.commit(alice, newChange(), encoding.RootCommitID, "base", map[string]string{"k": "0"})
	e.mustLand(LandItem{Number: e.open(base), Patchset: 1})
	h := e.head()
	a := e.commit(alice, newChange(), h, "a", map[string]string{"k": "0", "a": "1"})
	b := e.commit(alice, newChange(), a, "b", map[string]string{"k": "0", "a": "1", "b": "1"})
	res := e.mustUpload(alice, Ready, a, b)
	na, nb := res[0].Number, res[1].Number
	x := e.commit(alice, newChange(), h, "x", map[string]string{"k": "0", "x": "1"})
	nx := e.open(x)

	// On the patchset's own base the candidate is the patchset itself.
	ca, err := e.s.PrepareCandidate(ctx, e.repo, na, 1, h, alice)
	if err != nil || ca.Commit != a {
		t.Fatalf("candidate %+v (%v)", ca, err)
	}
	// Speculating: b on a's candidate.
	cb, err := e.s.PrepareCandidate(ctx, e.repo, nb, 1, ca.Commit, alice)
	if err != nil || cb.Commit != b {
		t.Fatalf("stacked candidate %+v (%v)", cb, err)
	}
	// b on main directly: its parent is neither submitted nor in the base.
	_, err = e.s.PrepareCandidate(ctx, e.repo, nb, 1, h, alice)
	if rej := landReason(t, err); rej.Reason != LandParentNotSubmitted {
		t.Fatalf("stacked candidate on main: %+v", rej)
	}
	// x onto a's candidate is a splice.
	cx, err := e.s.PrepareCandidate(ctx, e.repo, nx, 1, ca.Commit, alice)
	if err != nil || cx.Commit == x {
		t.Fatalf("spliced candidate %+v (%v)", cx, err)
	}
	if want := map[string]string{"k": "0", "a": "1", "x": "1"}; !reflect.DeepEqual(e.flatAt(cx.Commit), want) {
		t.Fatalf("candidate tree %v", e.flatAt(cx.Commit))
	}
	_, err = e.s.PrepareCandidate(ctx, e.repo, nx, 1, encoding.Hash([]byte("nothing")), alice)
	if rej := landReason(t, err); rej.Reason != LandUnknownBase {
		t.Fatalf("unknown base: %+v", rej)
	}

	// A candidate of another CL is refused.
	_, _, err = e.land(h, false, LandItem{Number: nx, Patchset: 1, Candidate: &ca.Commit})
	if rej := landReason(t, err); rej.Reason != LandCandidateMismatch {
		t.Fatalf("mismatched candidate: %+v", rej)
	}
	// a and b land as their candidates; x's candidate was based on a's, which is now
	// main's head, so it lands byte-identical.
	landed := e.mustLand(LandItem{Number: na, Patchset: 1, Candidate: &ca.Commit}, LandItem{Number: nb, Patchset: 1, Candidate: &cb.Commit})
	if landed[0].Commit != a || landed[1].Commit != b {
		t.Fatalf("landed %v", landed)
	}
	// x's candidate is on a, but main is now at b: it re-splices.
	landed = e.mustLand(LandItem{Number: nx, Patchset: 1, Candidate: &cx.Commit})
	if landed[0].Commit == cx.Commit {
		t.Fatal("a candidate on a stale base landed as is")
	}
	if want := map[string]string{"k": "0", "a": "1", "b": "1", "x": "1"}; !reflect.DeepEqual(e.flatAt(landed[0].Commit), want) {
		t.Fatalf("re-spliced tree %v", e.flatAt(landed[0].Commit))
	}
	var pending int
	if err := e.s.pool.QueryRow(ctx, `SELECT count(*) FROM candidate WHERE repository_id = $1`, e.repo).Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("%d candidates pending after their CLs landed (%v)", pending, err)
	}

	// Discard is idempotent.
	y := e.commit(alice, newChange(), e.head(), "y", map[string]string{"k": "0", "a": "1", "b": "1", "x": "1", "y": "1"})
	cy, err := e.s.PrepareCandidate(ctx, e.repo, e.open(y), 1, h, alice)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := e.s.DiscardCandidate(ctx, e.repo, cy.Commit); err != nil {
			t.Fatal(err)
		}
	}
}

func TestChangeListQueries(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	var changes []encoding.ChangeID
	for i := range 5 {
		ch := newChange()
		changes = append(changes, ch)
		who := alice
		if i%2 == 1 {
			who = bob
		}
		c := e.commit(who, ch, encoding.RootCommitID, fmt.Sprint(i), map[string]string{fmt.Sprint(i): "1"})
		e.mustUpload(who, ReadinessUnchanged, c)
	}
	e.transition(2, RequestReview)
	var got []int64
	for before := int64(0); ; {
		page, next, err := e.s.ListChangeLists(ctx, e.repo, nil, "", 2, before)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range page {
			got = append(got, c.Number)
		}
		if next == 0 {
			break
		}
		before = next
	}
	if !reflect.DeepEqual(got, []int64{5, 4, 3, 2, 1}) {
		t.Fatalf("pages %v", got)
	}
	bobs, _, _ := e.s.ListChangeLists(ctx, e.repo, nil, bob, 0, 0)
	open, _, _ := e.s.ListChangeLists(ctx, e.repo, []Status{StatusOpen}, "", 0, 0)
	if len(bobs) != 2 || len(open) != 1 || open[0].Number != 2 {
		t.Fatalf("bob's %d, open %d", len(bobs), len(open))
	}
	found, err := e.s.LookupChanges(ctx, e.repo, []encoding.ChangeID{changes[3], newChange(), changes[0]})
	if err != nil || len(found) != 2 || found[0].Number != 1 || found[1].Number != 4 {
		t.Fatalf("lookup %v (%v)", found, err)
	}
	if _, err := e.s.GetChangeList(ctx, e.repo, 99); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing CL: %v", err)
	}
}

func TestWatchEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e := newCLEnv(t)
	var start int64
	if err := e.s.pool.QueryRow(ctx, `SELECT coalesce((SELECT last FROM event_counter), 0)`).Scan(&start); err != nil {
		t.Fatal(err)
	}
	got := make(chan *scmpb.Event, 100)
	go e.s.WatchEvents(ctx, start, func(ev *scmpb.Event) error {
		if ev.RepositoryId == e.repo {
			got <- ev
		}
		return nil
	})
	time.Sleep(200 * time.Millisecond) // let the watcher catch up and wait
	a := e.commit(alice, newChange(), encoding.RootCommitID, "a", map[string]string{"a": "1"})
	b := e.commit(alice, newChange(), a, "b", map[string]string{"a": "1", "b": "1"})
	res := e.mustUpload(alice, ReadinessUnchanged, a, b)
	e.transition(res[0].Number, RequestReview)
	e.transition(res[1].Number, RequestReview)
	e.mustLand(LandItem{Number: res[0].Number, Patchset: 1}, LandItem{Number: res[1].Number, Patchset: 1})

	var kinds []string
	var last int64
	for len(kinds) < 8 {
		select {
		case ev := <-got:
			if ev.Seq <= last {
				t.Fatalf("event %d after %d", ev.Seq, last)
			}
			last = ev.Seq
			kinds = append(kinds, fmt.Sprintf("%T", ev.Kind)[len("*scmpb.Event_"):])
		case <-ctx.Done():
			t.Fatalf("saw only %v", kinds)
		}
	}
	want := []string{"Uploaded", "Uploaded", "Transitioned", "Transitioned", "Landed", "Landed", "HeadMoved", "HeadMoved"}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("events %v, want %v", kinds, want)
	}
}
