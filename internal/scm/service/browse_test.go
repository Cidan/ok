package service

import (
	"bytes"
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/scm/encoding"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

func TestBrowseService(t *testing.T) {
	c := testServer(t)
	anon := context.Background()
	alice := human("alice.human.example.com")

	// ok control creates a repository for a human, who becomes its creator, through Review;
	// Repositories creates it for its caller and names no one else.
	r, err := c.review.CreateRepository(control(), &scmpb.CreateRepositoryRequest{Name: "browse-svc", Actor: "alice.human.example.com"})
	if err != nil || r.GetCreatedBy() != "alice.human.example.com" {
		t.Fatalf("control creating for alice: %v, %v", r, err)
	}
	_, err = c.review.CreateRepository(control(), &scmpb.CreateRepositoryRequest{Name: "browse-svc-2"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.repos.CreateRepository(alice, &scmpb.CreateRepositoryRequest{Name: "browse-svc-2", Actor: "bob.human.example.com"})
	wantCode(t, err, codes.InvalidArgument)
	repo := r.GetId()

	b := newObjects(t)
	contents := []byte("hello\nworld\n")
	tree := b.add(&encoding.Tree{Entries: []encoding.TreeEntry{{Name: "f", Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: b.file(contents)}}}})
	commit := b.commit(tree, "one", encoding.RootCommitID)
	if _, _, err := b.publish(alice, c, repo, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.cls.Upload(alice, &scmpb.UploadRequest{RepositoryId: repo, CommitIds: [][]byte{commit[:]}, Readiness: scmpb.UploadRequest_READINESS_READY}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.landing.Land(automation(), &scmpb.LandRequest{RepositoryId: repo, Items: []*scmpb.LandRequest_Item{{Number: 1, Patchset: 1}}, Actor: "alice.human.example.com", ExpectedHead: encoding.RootCommitID[:]}); err != nil {
		t.Fatal(err)
	}

	_, err = c.browse.Log(anon, &scmpb.LogRequest{RepositoryId: repo})
	wantCode(t, err, codes.Unauthenticated)
	log, err := c.browse.Log(alice, &scmpb.LogRequest{RepositoryId: repo})
	if err != nil || len(log.GetCommits()) != 1 || !bytes.Equal(log.GetCommits()[0].GetId(), commit[:]) || log.GetCommits()[0].GetAuthor().GetName() != "t" || log.GetNextPageToken() != "" {
		t.Fatalf("log: %v, %v", log, err)
	}
	for _, bad := range []string{"x", "o", "q5", "o-1", "n0"} {
		_, err = c.browse.Log(alice, &scmpb.LogRequest{RepositoryId: repo, PageToken: bad})
		wantCode(t, err, codes.InvalidArgument)
	}
	if older, err := c.browse.Log(alice, &scmpb.LogRequest{RepositoryId: repo, PageToken: "o1"}); err != nil || len(older.GetCommits()) != 0 {
		t.Fatalf("nothing is older than the first commit: %v, %v", older, err)
	}
	if newer, err := c.browse.Log(alice, &scmpb.LogRequest{RepositoryId: repo, PageToken: "n0x"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a malformed newer token: %v, %v", newer, err)
	}

	rt, err := c.browse.ReadTree(alice, &scmpb.ReadTreeRequest{RepositoryId: repo, CommitId: commit[:], LastCommits: true})
	if err != nil || len(rt.GetEntries()) != 1 || rt.GetEntries()[0].GetValue().GetSize() != uint64(len(contents)) || rt.GetEntries()[0].GetValue().GetType() != scmpb.EntryType_ENTRY_TYPE_FILE {
		t.Fatalf("read tree: %v, %v", rt, err)
	}
	if e := rt.GetEntries()[0]; !bytes.Equal(e.GetLastCommitId(), commit[:]) || len(rt.GetCommits()) != 1 || rt.GetCommits()[0].GetDescription() != "one" {
		t.Fatalf("read tree's last commits: %v", rt)
	}
	frames, err := drain(c.browse.ReadFileAt(alice, &scmpb.ReadFileAtRequest{RepositoryId: repo, CommitId: commit[:], Path: "f"}))
	if err != nil || frames[0].GetValue() == nil {
		t.Fatalf("read file: %v, %v", frames, err)
	}
	var got []byte
	for _, f := range frames {
		got = append(got, f.GetData()...)
	}
	if !bytes.Equal(got, contents) {
		t.Errorf("read file: %q", got)
	}
	_, err = drain(c.browse.ReadFileAt(alice, &scmpb.ReadFileAtRequest{RepositoryId: repo, CommitId: commit[:], Path: "nope"}))
	wantCode(t, err, codes.NotFound)

	dt, err := c.browse.DiffTrees(alice, &scmpb.DiffTreesRequest{RepositoryId: repo, ToTree: tree[:], DetectRenames: true})
	if err != nil || len(dt.GetChanges()) != 1 || dt.GetChanges()[0].GetKind() != scmpb.PathChange_KIND_ADDED {
		t.Fatalf("diff trees: %v, %v", dt, err)
	}
	fileID := rt.GetEntries()[0].GetValue().GetId()
	df, err := c.browse.DiffFiles(alice, &scmpb.DiffFilesRequest{RepositoryId: repo, Pairs: []*scmpb.DiffFilesRequest_Pair{{NewFile: fileID}}, ContextLines: 3})
	if err != nil || len(df.GetDiffs()) != 1 || len(df.GetDiffs()[0].GetHunks()) != 1 || string(df.GetDiffs()[0].GetHunks()[0].GetLines()[1].GetText()) != "world" {
		t.Fatalf("diff files: %v, %v", df, err)
	}
	if d := df.GetDiffs()[0]; d.GetAdditions() != 2 || d.GetDeletions() != 0 {
		t.Errorf("diff files' counts: %v", d)
	}
	st, err := c.browse.DiffFiles(alice, &scmpb.DiffFilesRequest{RepositoryId: repo, Pairs: []*scmpb.DiffFilesRequest_Pair{{NewFile: fileID}}, StatsOnly: true})
	if err != nil || len(st.GetDiffs()) != 1 || len(st.GetDiffs()[0].GetHunks()) != 0 || st.GetDiffs()[0].GetAdditions() != 2 {
		t.Fatalf("diff files, counts only: %v, %v", st, err)
	}
	an, err := c.browse.Annotate(alice, &scmpb.AnnotateRequest{RepositoryId: repo, CommitId: commit[:], Path: "f"})
	if err != nil || len(an.GetRanges()) != 1 || an.GetRanges()[0].GetLineCount() != 2 || !bytes.Equal(an.GetRanges()[0].GetCommitId(), commit[:]) {
		t.Fatalf("annotate: %v, %v", an, err)
	}
	anc, err := c.browse.IsAncestor(alice, &scmpb.IsAncestorRequest{RepositoryId: repo, Ancestor: encoding.RootCommitID[:], Descendant: commit[:]})
	if err != nil || !anc.GetIsAncestor() {
		t.Fatalf("is ancestor: %v, %v", anc, err)
	}
	ex, err := drain(c.browse.Export(alice, &scmpb.ExportRequest{RepositoryId: repo, CommitId: commit[:]}))
	if err != nil || len(ex) != 2 || ex[0].GetEntry().GetPath() != "f" || !bytes.Equal(ex[1].GetData(), contents) {
		t.Fatalf("export: %v, %v", ex, err)
	}
	ex, err = drain(c.browse.Export(alice, &scmpb.ExportRequest{RepositoryId: repo, CommitId: commit[:], EntriesOnly: true}))
	if err != nil || len(ex) != 1 || ex[0].GetEntry().GetPath() != "f" || ex[0].GetEntry().GetValue().GetSize() != uint64(len(contents)) {
		t.Fatalf("export, entries only: %v, %v", ex, err)
	}
	gc, err := c.browse.GetCommits(alice, &scmpb.GetCommitsRequest{RepositoryId: repo, CommitIds: [][]byte{commit[:]}})
	if err != nil || gc.GetCommits()[0].GetDescription() != "one" || gc.GetCommits()[0].GetConflicted() {
		t.Fatalf("get commits: %v, %v", gc, err)
	}
}
