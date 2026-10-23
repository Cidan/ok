package service

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/scm/encoding"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

func uploadRejection(t *testing.T, err error) *scmpb.UploadRejection {
	t.Helper()
	wantCode(t, err, codes.FailedPrecondition)
	for _, d := range status.Convert(err).Details() {
		if r, ok := d.(*scmpb.UploadRejection); ok {
			return r
		}
	}
	t.Fatalf("%v carries no UploadRejection", err)
	return nil
}

func landRejection(t *testing.T, err error) *scmpb.LandRejection {
	t.Helper()
	wantCode(t, err, codes.FailedPrecondition)
	for _, d := range status.Convert(err).Details() {
		if r, ok := d.(*scmpb.LandRejection); ok {
			return r
		}
	}
	t.Fatalf("%v carries no LandRejection", err)
	return nil
}

func TestChangeListsService(t *testing.T) {
	c := testServer(t)
	anon := context.Background()
	alice, bob := human("alice.human.example.com"), human("bob.human.example.com")
	r := newRepo(t, c)
	repo := r.GetId()

	b := newObjects(t)
	tree := b.add(&encoding.Tree{Entries: []encoding.TreeEntry{{Name: "f", Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: b.file([]byte("one"))}}}})
	commit := b.commit(tree, "one", encoding.RootCommitID)
	if _, _, err := b.publish(alice, c, repo, nil); err != nil {
		t.Fatal(err)
	}
	upload := &scmpb.UploadRequest{RepositoryId: repo, CommitIds: [][]byte{commit[:]}, Readiness: scmpb.UploadRequest_READINESS_READY}

	_, err := c.cls.Upload(anon, upload)
	wantCode(t, err, codes.Unauthenticated)
	withNobody := &scmpb.UploadRequest{RepositoryId: repo, CommitIds: upload.CommitIds, Reviewers: []string{"carol", "nobody"}}
	_, err = c.cls.Upload(alice, withNobody)
	if rej := uploadRejection(t, err); rej.GetReason() != scmpb.UploadRejection_REASON_UNKNOWN_REVIEWER || len(rej.GetProblems()) != 1 || rej.GetProblems()[0] != "nobody" {
		t.Fatalf("unknown reviewer: %v", rej)
	}
	res, err := c.cls.Upload(alice, upload)
	if err != nil {
		t.Fatal(err)
	}
	got := res.GetResults()[0]
	if got.GetOutcome() != scmpb.UploadResponse_Result_OUTCOME_CREATED || got.GetNumber() != 1 || got.GetPatchset() != 1 || !bytes.Equal(got.GetCommitId(), commit[:]) {
		t.Fatalf("upload: %v", res)
	}
	_, err = c.cls.Upload(alice, upload)
	if rej := uploadRejection(t, err); rej.GetReason() != scmpb.UploadRejection_REASON_NO_NEW_CHANGES || !bytes.Equal(rej.GetCommitId(), commit[:]) {
		t.Fatalf("re-upload: %v", rej)
	}

	// Everyone reads CLs.
	cl, err := c.cls.GetChangeList(bob, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: 1})
	if err != nil || cl.GetStatus() != scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN || cl.GetAuthor() != "alice.human.example.com" ||
		len(cl.GetPatchsets()) != 1 || !bytes.Equal(cl.GetPatchsets()[0].GetBaseCommitId(), encoding.RootCommitID[:]) {
		t.Fatalf("get: %v (%v)", cl, err)
	}
	var change encoding.ChangeID
	copy(change[:], cl.GetChangeId())
	found, err := c.cls.LookupChanges(bob, &scmpb.LookupChangesRequest{RepositoryId: repo, ChangeIds: [][]byte{change[:], make([]byte, 16)}})
	if err != nil || len(found.GetChangeLists()) != 1 {
		t.Fatalf("lookup: %v (%v)", found, err)
	}
	list, err := c.cls.ListChangeLists(bob, &scmpb.ListChangeListsRequest{RepositoryId: repo, Statuses: []scmpb.ChangeListStatus{scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN}})
	if err != nil || len(list.GetChangeLists()) != 1 {
		t.Fatalf("list: %v (%v)", list, err)
	}
	_, err = c.cls.ListChangeLists(bob, &scmpb.ListChangeListsRequest{RepositoryId: repo, PageToken: "x"})
	wantCode(t, err, codes.InvalidArgument)

	// ok control transitions, naming the human it acts for (the mesh admits no one else to
	// Review: TestCallers).
	transition := &scmpb.TransitionRequest{RepositoryId: repo, Number: 1, Kind: scmpb.TransitionRequest_KIND_RETURN_TO_DRAFT, Actor: "alice.human.example.com"}
	_, err = c.review.Transition(control(), &scmpb.TransitionRequest{RepositoryId: repo, Number: 1, Kind: scmpb.TransitionRequest_KIND_RETURN_TO_DRAFT})
	wantCode(t, err, codes.InvalidArgument)
	if resp, err := c.review.Transition(control(), transition); err != nil || resp.GetChangeList().GetStatus() != scmpb.ChangeListStatus_CHANGE_LIST_STATUS_DRAFT || resp.GetRepositorySeq() == 0 {
		t.Fatalf("transition: %v (%v)", resp, err)
	}
	_, err = c.review.Transition(control(), transition)
	wantCode(t, err, codes.FailedPrecondition)

	land := &scmpb.LandRequest{RepositoryId: repo, Items: []*scmpb.LandRequest_Item{{Number: 1, Patchset: 1}}, Actor: "alice.human.example.com", ExpectedHead: encoding.RootCommitID[:]}
	_, err = c.landing.Land(automation(), land)
	if rej := landRejection(t, err); rej.GetReason() != scmpb.LandRejection_REASON_NOT_OPEN || rej.GetNumber() != 1 {
		t.Fatalf("landing a draft: %v", rej)
	}
	transition.Kind = scmpb.TransitionRequest_KIND_REQUEST_REVIEW
	if _, err := c.review.Transition(control(), transition); err != nil {
		t.Fatal(err)
	}
	cand, err := c.landing.PrepareCandidate(automation(), &scmpb.PrepareCandidateRequest{RepositoryId: repo, Number: 1, Patchset: 1, Base: encoding.RootCommitID[:], Actor: "alice.human.example.com"})
	if err != nil || !bytes.Equal(cand.GetCommitId(), commit[:]) || cand.GetPreparedBy() != "alice.human.example.com" || cand.GetPreparedAt() == nil {
		t.Fatalf("candidate: %v (%v)", cand, err)
	}
	// ok automation, which runs post and the landing queue, prepares, lists, and discards
	// candidates.
	again, err := c.landing.PrepareCandidate(automation(), &scmpb.PrepareCandidateRequest{RepositoryId: repo, Number: 1, Patchset: 1, Base: encoding.RootCommitID[:], Actor: "bob.human.example.com"})
	if err != nil || !bytes.Equal(again.GetCommitId(), commit[:]) || again.GetPreparedBy() != "alice.human.example.com" {
		t.Fatalf("preparing the same candidate again: %v (%v)", again, err)
	}
	listed, err := c.landing.ListCandidates(automation(), &scmpb.ListCandidatesRequest{RepositoryId: repo})
	if err != nil || len(listed.GetCandidates()) != 1 || !bytes.Equal(listed.GetCandidates()[0].GetCommitId(), commit[:]) || listed.GetCandidates()[0].GetNumber() != 1 {
		t.Fatalf("listing candidates: %v (%v)", listed, err)
	}
	_, err = c.landing.ListCandidates(automation(), &scmpb.ListCandidatesRequest{RepositoryId: repo + 1000})
	wantCode(t, err, codes.NotFound)
	dry, err := c.checks.DryRunLand(control(), land)
	if err != nil || !bytes.Equal(dry.GetHead(), commit[:]) {
		t.Fatalf("dry run: %v (%v)", dry, err)
	}
	if r, _ := c.repos.GetRepository(bob, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}}); r.GetHeadSeq() != 0 {
		t.Fatal("a dry run moved main")
	}
	land.Items[0].Candidate = cand.GetCommitId()
	landed, err := c.landing.Land(automation(), land)
	if err != nil || !bytes.Equal(landed.GetHead(), commit[:]) || len(landed.GetLanded()) != 1 || landed.GetRepositorySeq() == 0 {
		t.Fatalf("land: %v (%v)", landed, err)
	}
	_, err = c.landing.Land(automation(), land)
	if rej := landRejection(t, err); rej.GetReason() != scmpb.LandRejection_REASON_HEAD_MOVED {
		t.Fatalf("landing against a moved head: %v", rej)
	}
	if r, err := c.repos.GetRepository(bob, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}}); err != nil || !bytes.Equal(r.GetHead(), commit[:]) || r.GetHeadSeq() != 1 {
		t.Fatalf("repository after landing: %v (%v)", r, err)
	}
	// The landing dropped the CL's candidates.
	if listed, err := c.landing.ListCandidates(automation(), &scmpb.ListCandidatesRequest{RepositoryId: repo}); err != nil || len(listed.GetCandidates()) != 0 {
		t.Fatalf("candidates after landing: %v (%v)", listed, err)
	}
	if _, err := c.landing.DiscardCandidate(automation(), &scmpb.DiscardCandidateRequest{RepositoryId: repo, CommitId: commit[:]}); err != nil {
		t.Fatalf("discarding a candidate already gone: %v", err)
	}

	// ok control rolls back a landed CL for a person, as a new CL of theirs.
	rollback := &scmpb.RollbackRequest{RepositoryId: repo, Number: 1, Actor: "bob.human.example.com", Reason: "it broke the build",
		Reviewers: []string{"alice.human.example.com"}}
	_, err = c.review.Rollback(control(), &scmpb.RollbackRequest{RepositoryId: repo, Number: 1, Reason: "no actor"})
	wantCode(t, err, codes.InvalidArgument)
	rolled, err := c.review.Rollback(control(), rollback)
	if err != nil {
		t.Fatal(err)
	}
	rb := rolled.GetChangeList()
	if rb.GetNumber() != 2 || rb.GetAuthor() != "bob.human.example.com" || rb.GetStatus() != scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN ||
		rolled.GetRepositorySeq() == 0 || !strings.HasPrefix(rb.GetDescription(), "Roll back CL 1: one\n\nit broke the build\n") {
		t.Fatalf("rollback: %v", rolled)
	}
	if got, err := c.cls.GetChangeList(alice, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: 2}); err != nil ||
		!reflect.DeepEqual(got.GetRollback().GetChangeLists(), []int64{1}) || !reflect.DeepEqual(got.GetRollback().GetPaths(), []string{"f"}) ||
		len(got.GetRollback().GetLeft()) != 0 {
		t.Fatalf("the rollback CL: %v (%v)", got, err)
	}
	if got, _ := c.cls.GetChangeList(alice, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: 1}); got.GetRollback() != nil {
		t.Errorf("a CL that is not a rollback carries one: %v", got.GetRollback())
	}
	_, err = c.review.Rollback(control(), rollback)
	wantCode(t, err, codes.FailedPrecondition)
	var underway *scmpb.RollbackRejection
	for _, d := range status.Convert(err).Details() {
		if r, ok := d.(*scmpb.RollbackRejection); ok {
			underway = r
		}
	}
	if underway.GetReason() != scmpb.RollbackRejection_REASON_UNDERWAY || underway.GetNumber() != 1 {
		t.Errorf("a second rollback: %v", err)
	}

	// The SCM's events are ok control's, ok automation's, and ok's deployer's to read.
	for name, ctx := range map[string]context.Context{"ok automation": automation(), "a follower": follower()} {
		if _, err := c.events.Read(ctx, &scmpb.ReadEventsRequest{RepositoryId: repo, Limit: 1}); err != nil {
			t.Fatalf("%s reading the repository's log: %v", name, err)
		}
	}
	all, err := c.events.Read(control(), &scmpb.ReadEventsRequest{RepositoryId: repo})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, ev := range all.GetEvents() {
		switch ev.GetKind().(type) {
		case *scmpb.Event_RepositoryCreated:
			kinds = append(kinds, "created")
		case *scmpb.Event_Uploaded:
			kinds = append(kinds, "uploaded")
		case *scmpb.Event_Transitioned:
			kinds = append(kinds, "transitioned")
		case *scmpb.Event_Landed:
			kinds = append(kinds, "landed")
		case *scmpb.Event_HeadMoved:
			kinds = append(kinds, "head moved")
		}
	}
	if want := "created uploaded transitioned transitioned landed"; len(kinds) < 5 || join(kinds[:5]) != want {
		t.Fatalf("events %q, want them to begin %q", join(kinds), want)
	}

	// A repository's log, read from a place in it: the follower's way to fill a gap.
	log, err := c.events.Read(control(), &scmpb.ReadEventsRequest{RepositoryId: repo})
	if err != nil || len(log.GetEvents()) < 5 {
		t.Fatalf("reading the repository's log: %v, %v", log, err)
	}
	for i, ev := range log.GetEvents() {
		if ev.GetRepositoryId() != repo || ev.GetRepositorySeq() != int64(i+1) {
			t.Fatalf("event %d of the log: %v", i+1, ev)
		}
	}
	page, err := c.events.Read(control(), &scmpb.ReadEventsRequest{RepositoryId: repo, AfterSeq: 1, Limit: 2})
	if err != nil || len(page.GetEvents()) != 2 || page.GetEvents()[0].GetRepositorySeq() != 2 ||
		page.GetEvents()[0].GetKind() == nil {
		t.Fatalf("the page after 1: %v, %v", page, err)
	}
	_, err = c.events.Read(alice, &scmpb.ReadEventsRequest{RepositoryId: repo})
	wantCode(t, err, codes.PermissionDenied)
	_, err = c.events.Read(control(), &scmpb.ReadEventsRequest{RepositoryId: repo, Limit: -1})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.events.Read(control(), &scmpb.ReadEventsRequest{RepositoryId: 1 << 40})
	wantCode(t, err, codes.NotFound)
}

func join(s []string) string {
	out := ""
	for i, x := range s {
		if i > 0 {
			out += " "
		}
		out += x
	}
	return out
}
