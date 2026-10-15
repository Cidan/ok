package engine

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// SCM reads main's history from the SCM: Main over its Browse service.
type SCM struct {
	Browse scmpb.BrowseClient
}

// Changed are the files that differ between two commits: both paths of a rename.
func (s SCM) Changed(ctx context.Context, repo int64, from, to []byte) ([]string, error) {
	resp, err := s.Browse.GetCommits(ctx, &scmpb.GetCommitsRequest{RepositoryId: repo, CommitIds: [][]byte{from, to}})
	if err != nil {
		return nil, err
	}
	trees := map[string][]byte{}
	for _, c := range resp.GetCommits() {
		if c.GetConflicted() || len(c.GetRootTree()) != 1 {
			return nil, fmt.Errorf("commit %x has conflicts", c.GetId())
		}
		trees[string(c.GetId())] = c.GetRootTree()[0]
	}
	fromTree, ok1 := trees[string(from)]
	toTree, ok2 := trees[string(to)]
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("the SCM did not return commits %x and %x", from, to)
	}
	if bytes.Equal(fromTree, toTree) {
		return nil, nil
	}
	diff, err := s.Browse.DiffTrees(ctx, &scmpb.DiffTreesRequest{RepositoryId: repo, FromTree: fromTree, ToTree: toTree})
	if err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for _, ch := range diff.GetChanges() {
		set[ch.GetPath()] = true
		if ch.GetOldPath() != "" {
			set[ch.GetOldPath()] = true
		}
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out, nil
}
