package service

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/scm/encoding"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Workspaces over gRPC: a person attaches, checkpoints a published tree, and lists it; another
// person sees none of it; a robot attaches its own; an unpublished tree is refused; and an
// unidentified caller is refused outright.
func TestWorkspaces(t *testing.T) {
	c := testServer(t)
	repo := newRepo(t, c).GetId()
	alice, bob := human("alice.human.example.com"), human("bob.human.example.com")
	b := newObjects(t)
	tree := b.add(&encoding.Tree{Entries: []encoding.TreeEntry{{Name: "a", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: b.add(&encoding.Symlink{Target: "b"})}}}})
	if _, _, err := b.publish(alice, c, repo, nil); err != nil {
		t.Fatal(err)
	}
	_, err := c.ws.Attach(alice, &scmpb.AttachRequest{RepositoryId: repo, Workspace: "laptop-ada", Machine: "m1"})
	wantCode(t, err, codes.NotFound)
	for _, w := range []struct {
		who  context.Context
		name string
	}{{alice, "laptop-ada"}, {bob, "laptop-ada"}, {follower(), "deployer"}} {
		if _, err := c.ws.CreateWorkspace(w.who, &scmpb.CreateWorkspaceRequest{RepositoryId: repo, Workspace: w.name}); err != nil {
			t.Fatal(err)
		}
	}

	att, err := c.ws.Attach(alice, &scmpb.AttachRequest{RepositoryId: repo, Workspace: "laptop-ada", Machine: "m1"})
	if err != nil || !att.GetGranted() || att.GetLease().GetEpoch() != 1 {
		t.Fatalf("attach: %v %v", att, err)
	}
	cp, err := c.ws.RecordCheckpoint(alice, &scmpb.RecordCheckpointRequest{Lease: att.GetLease(), TreeIds: [][]byte{tree[:]}, BaseCommitId: encoding.RootCommitID[:]})
	if err != nil || cp.GetFork() || cp.GetId() == 0 {
		t.Fatalf("checkpoint: %v %v", cp, err)
	}
	renewed, err := c.ws.Renew(alice, &scmpb.RenewRequest{Lease: att.GetLease()})
	if err != nil || renewed.GetTakeoverBy() != "" {
		t.Fatalf("renew: %v %v", renewed, err)
	}
	list, err := c.ws.ListCheckpoints(alice, &scmpb.ListCheckpointsRequest{RepositoryId: repo, Workspace: "laptop-ada"})
	if err != nil || len(list.GetCheckpoints()) != 1 || list.GetCheckpoints()[0].GetId() != cp.GetId() {
		t.Fatalf("list: %v %v", list, err)
	}
	ws, err := c.ws.ListWorkspaces(alice, &scmpb.ListWorkspacesRequest{RepositoryId: repo})
	if err != nil || len(ws.GetWorkspaces()) != 1 || ws.GetWorkspaces()[0].GetHolder() != "m1" || ws.GetWorkspaces()[0].GetLatest().GetId() != cp.GetId() {
		t.Fatalf("workspaces: %v %v", ws, err)
	}

	// Bob sees none of it, and cannot act on alice's lease.
	if l, err := c.ws.ListCheckpoints(bob, &scmpb.ListCheckpointsRequest{RepositoryId: repo, Workspace: "laptop-ada"}); err != nil || len(l.GetCheckpoints()) != 0 {
		t.Errorf("bob listing alice's checkpoints: %v %v", l, err)
	}
	_, err = c.ws.GetCheckpoint(bob, &scmpb.GetCheckpointRequest{RepositoryId: repo, Id: cp.GetId()})
	wantCode(t, err, codes.NotFound)
	_, err = c.ws.Renew(bob, &scmpb.RenewRequest{Lease: att.GetLease()})
	wantCode(t, err, codes.FailedPrecondition)
	// Bob's workspace of the same name is his own.
	if a, err := c.ws.Attach(bob, &scmpb.AttachRequest{RepositoryId: repo, Workspace: "laptop-ada", Machine: "m9"}); err != nil || !a.GetGranted() {
		t.Errorf("bob attaching his own workspace: %v %v", a, err)
	}
	// A robot's workspaces are its own too.
	if a, err := c.ws.Attach(follower(), &scmpb.AttachRequest{RepositoryId: repo, Workspace: "deployer", Machine: "pod"}); err != nil || !a.GetGranted() {
		t.Errorf("a robot attaching: %v %v", a, err)
	}

	// An unpublished tree is refused, a malformed one invalid, an unknown caller unauthenticated.
	missing := encoding.ID{0x42}
	_, err = c.ws.RecordCheckpoint(alice, &scmpb.RecordCheckpointRequest{Lease: att.GetLease(), ParentId: cp.GetId(), TreeIds: [][]byte{missing[:]}, BaseCommitId: encoding.RootCommitID[:]})
	wantCode(t, err, codes.FailedPrecondition)
	_, err = c.ws.RecordCheckpoint(alice, &scmpb.RecordCheckpointRequest{Lease: att.GetLease(), TreeIds: [][]byte{{1, 2}}, BaseCommitId: encoding.RootCommitID[:]})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.ws.Attach(alice, &scmpb.AttachRequest{RepositoryId: repo, Workspace: "", Machine: "m1"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.ws.Attach(t.Context(), &scmpb.AttachRequest{RepositoryId: repo, Workspace: "w", Machine: "m1"})
	wantCode(t, err, codes.Unauthenticated)
	_, err = c.ws.Attach(alice, &scmpb.AttachRequest{RepositoryId: repo + 1000, Workspace: "w", Machine: "m1"})
	wantCode(t, err, codes.NotFound)

	// Releasing frees it for another machine.
	if _, err := c.ws.Release(alice, &scmpb.ReleaseRequest{Lease: renewed.GetLease()}); err != nil {
		t.Fatal(err)
	}
	if a, err := c.ws.Attach(alice, &scmpb.AttachRequest{RepositoryId: repo, Workspace: "laptop-ada", Machine: "m2"}); err != nil || !a.GetGranted() || a.GetLatest().GetId() != cp.GetId() {
		t.Errorf("attaching after the release: %v %v", a, err)
	}
}

// Each workspace's operation log over gRPC (docs/scm.md, "Decisions", 3): CreateWorkspace
// makes it, once; a workspace that was never made is NOT_FOUND; a publish naming another
// workspace is refused; and a request naming no workspace is INVALID_ARGUMENT.
func TestWorkspaceOperationLogs(t *testing.T) {
	c := testServer(t)
	repo := newRepo(t, c).GetId()
	alice := human("alice.human.example.com")

	made, err := c.ws.CreateWorkspace(alice, &scmpb.CreateWorkspaceRequest{RepositoryId: repo, Workspace: "laptop-ada"})
	if err != nil || made.GetWorkspace() != "laptop-ada" || !made.GetOwnLog() {
		t.Fatalf("CreateWorkspace: %v %v", made, err)
	}
	_, err = c.ws.CreateWorkspace(alice, &scmpb.CreateWorkspaceRequest{RepositoryId: repo, Workspace: "laptop-ada"})
	wantCode(t, err, codes.AlreadyExists)
	_, err = c.ws.CreateWorkspace(alice, &scmpb.CreateWorkspaceRequest{RepositoryId: repo})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.ws.CreateWorkspace(context.Background(), &scmpb.CreateWorkspaceRequest{RepositoryId: repo, Workspace: "x"})
	wantCode(t, err, codes.Unauthenticated)

	heads, err := c.store.GetOpHeads(alice, &scmpb.GetOpHeadsRequest{RepositoryId: repo, Workspace: "laptop-ada"})
	if err != nil || len(heads.GetOpIds()) != 1 {
		t.Fatalf("a new workspace's heads: %v %v", heads, err)
	}
	_, err = c.store.GetOpHeads(alice, &scmpb.GetOpHeadsRequest{RepositoryId: repo, Workspace: "never-made"})
	wantCode(t, err, codes.NotFound)

	b := newObjects(t)
	commit := b.commit(b.add(&encoding.Tree{}), "x\n", encoding.RootCommitID)
	view := b.add(&encoding.View{HeadIDs: []encoding.ID{commit}, WCCommitIDs: map[string]encoding.ID{"desk-ada": commit}})
	op := b.operation(view, encoding.RootOperationID)
	b.workspace = "laptop-ada"
	_, _, err = b.publish(alice, c, repo, &scmpb.OpHeadsUpdate{OldIds: [][]byte{encoding.RootOperationID[:]}, NewId: op[:]})
	wantCode(t, err, codes.InvalidArgument)

	list, err := c.ws.ListWorkspaces(alice, &scmpb.ListWorkspacesRequest{RepositoryId: repo})
	if err != nil || len(list.GetWorkspaces()) != 1 || !list.GetWorkspaces()[0].GetOwnLog() {
		t.Fatalf("ListWorkspaces: %v %v", list, err)
	}

	_, err = c.store.GetOpHeads(alice, &scmpb.GetOpHeadsRequest{RepositoryId: repo})
	wantCode(t, err, codes.InvalidArgument)
}
