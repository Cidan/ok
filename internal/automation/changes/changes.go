// Package changes reads what a commit changes from the SCM, as the job context and review count
// it: the files it adds, modifies, deletes, renames, or copies against a base.
package changes

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/workflow"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// renameSimilarity is how alike two files must be (percent) to count as a rename: ok
// control's, so a change's files are the ones its review shows.
const renameSimilarity = 50

// Files is what commit changes against base, renames and copies detected, each with its old
// path.
func Files(ctx context.Context, browse scmpb.BrowseClient, repo int64, base, commit []byte) ([]workflow.ChangedFile, error) {
	from, err := tree(ctx, browse, repo, base)
	if err != nil {
		return nil, err
	}
	to, err := tree(ctx, browse, repo, commit)
	if err != nil {
		return nil, err
	}
	resp, err := browse.DiffTrees(ctx, &scmpb.DiffTreesRequest{RepositoryId: repo, FromTree: from, ToTree: to,
		DetectRenames: true, RenameSimilarityPercent: renameSimilarity})
	if err != nil {
		return nil, err
	}
	files := []workflow.ChangedFile{}
	for _, c := range resp.GetChanges() {
		cf := workflow.ChangedFile{Path: c.GetPath()}
		switch c.GetKind() {
		case scmpb.PathChange_KIND_ADDED:
			cf.Status = workflow.Added
		case scmpb.PathChange_KIND_DELETED:
			cf.Status = workflow.Deleted
		case scmpb.PathChange_KIND_RENAMED:
			cf.Status, cf.OldPath = workflow.Renamed, c.GetOldPath()
		case scmpb.PathChange_KIND_COPIED:
			cf.Status, cf.OldPath = workflow.Copied, c.GetOldPath()
		default:
			cf.Status = workflow.Modified
		}
		files = append(files, cf)
	}
	return files, nil
}

// tree is a commit's root tree; the root commit's is the empty tree (nil).
func tree(ctx context.Context, browse scmpb.BrowseClient, repo int64, commit []byte) ([]byte, error) {
	if isRoot(commit) {
		return nil, nil
	}
	resp, err := browse.GetCommits(ctx, &scmpb.GetCommitsRequest{RepositoryId: repo, CommitIds: [][]byte{commit}})
	if err != nil {
		return nil, err
	}
	c := resp.GetCommits()[0]
	if c.GetConflicted() || len(c.GetRootTree()) != 1 {
		return nil, status.Errorf(codes.FailedPrecondition, "commit %x has conflicts", commit)
	}
	return c.GetRootTree()[0], nil
}

// isRoot reports whether a commit ID is the root commit's: all zeros.
func isRoot(id []byte) bool {
	for _, b := range id {
		if b != 0 {
			return false
		}
	}
	return len(id) == 32
}
