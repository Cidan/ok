package review

import (
	"context"
	"sort"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/scm"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Thread is an inline or file review thread: its root and replies. The anchor and
// resolution are the root's; replies inherit them.
type Thread struct {
	RootID   int64
	FilePath string
	Line     int32
	Side     db.Side
	Patchset int32
	Resolved bool
	Comments []db.Comment // root first, then replies oldest first
}

// BuildThreads groups a CL's file and inline comments into threads, sorted by file and
// line. CL-level comments are messages, which the change log shows.
func BuildThreads(comments []db.Comment) []Thread {
	var roots []db.Comment
	replies := map[int64][]db.Comment{}
	for _, c := range comments {
		if c.FilePath == "" {
			continue
		}
		if c.ParentID == 0 {
			roots = append(roots, c)
		} else {
			replies[c.ParentID] = append(replies[c.ParentID], c)
		}
	}
	threads := make([]Thread, 0, len(roots))
	for _, root := range roots {
		rs := replies[root.ID]
		sort.SliceStable(rs, func(i, j int) bool { return rs[i].CreatedAt.Before(rs[j].CreatedAt) })
		threads = append(threads, Thread{
			RootID:   root.ID,
			FilePath: root.FilePath,
			Line:     root.Line,
			Side:     root.Side,
			Patchset: root.Patchset,
			Resolved: root.Resolved,
			Comments: append([]db.Comment{root}, rs...),
		})
	}
	sort.SliceStable(threads, func(i, j int) bool {
		if threads[i].FilePath != threads[j].FilePath {
			return threads[i].FilePath < threads[j].FilePath
		}
		return threads[i].Line < threads[j].Line
	})
	return threads
}

// RenameSimilarity is the similarity at which diffs pair a deleted and an added file as a
// rename: jj's, and so the diff views'.
const RenameSimilarity = 50

// browseDiffer is porting's treeDiffer over the SCM's Browse, with the diff views' rename
// detection, so a thread follows a file exactly where the diff view says it went.
type browseDiffer struct {
	ctx  context.Context
	scm  *scm.Client
	repo int64
	// trees caches commits' root trees by hex ID.
	trees map[string][]byte
}

func (b *browseDiffer) tree(commit string) ([]byte, error) {
	if t, ok := b.trees[commit]; ok {
		return t, nil
	}
	id, ok := scm.ParseHex(commit)
	if !ok {
		return nil, errBadCommit(commit)
	}
	t, err := b.scm.Tree(b.ctx, b.repo, id)
	if err != nil {
		return nil, err
	}
	if b.trees == nil {
		b.trees = map[string][]byte{}
	}
	b.trees[commit] = t
	return t, nil
}

func (b *browseDiffer) changes(from, to string) ([]*scmpb.PathChange, error) {
	ft, err := b.tree(from)
	if err != nil {
		return nil, err
	}
	tt, err := b.tree(to)
	if err != nil {
		return nil, err
	}
	resp, err := b.scm.Browse.DiffTrees(b.ctx, &scmpb.DiffTreesRequest{
		RepositoryId: b.repo, FromTree: ft, ToTree: tt, DetectRenames: true, RenameSimilarityPercent: RenameSimilarity,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetChanges(), nil
}

// fileChange is how porting sees one path change: a copy is an addition (its source
// is unchanged), and a type change keeps its path.
func fileChange(c *scmpb.PathChange) FileChange {
	switch c.GetKind() {
	case scmpb.PathChange_KIND_ADDED, scmpb.PathChange_KIND_COPIED:
		return FileChange{NewPath: c.GetPath()}
	case scmpb.PathChange_KIND_DELETED:
		return FileChange{OldPath: c.GetPath()}
	case scmpb.PathChange_KIND_RENAMED:
		return FileChange{OldPath: c.GetOldPath(), NewPath: c.GetPath()}
	default:
		return FileChange{OldPath: c.GetPath(), NewPath: c.GetPath()}
	}
}

func (b *browseDiffer) nameStatus(from, to string) ([]FileChange, error) {
	changes, err := b.changes(from, to)
	if err != nil {
		return nil, err
	}
	out := make([]FileChange, 0, len(changes))
	for _, c := range changes {
		out = append(out, fileChange(c))
	}
	return out, nil
}

func (b *browseDiffer) patch(from, to string, paths []string) (map[string]patchFile, error) {
	changes, err := b.changes(from, to)
	if err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, p := range paths {
		want[p] = true
	}
	var pairs []*scmpb.DiffFilesRequest_Pair
	var keys []string
	for _, c := range changes {
		fc := fileChange(c)
		if fc.OldPath == "" || fc.NewPath == "" || !want[fc.OldPath] {
			continue
		}
		pair := &scmpb.DiffFilesRequest_Pair{}
		if v := c.GetBefore(); v.GetType() == scmpb.EntryType_ENTRY_TYPE_FILE {
			pair.OldFile = v.GetId()
		}
		if v := c.GetAfter(); v.GetType() == scmpb.EntryType_ENTRY_TYPE_FILE {
			pair.NewFile = v.GetId()
		}
		pairs = append(pairs, pair)
		keys = append(keys, fc.OldPath)
	}
	out := map[string]patchFile{}
	if len(pairs) == 0 {
		return out, nil
	}
	resp, err := b.scm.Browse.DiffFiles(b.ctx, &scmpb.DiffFilesRequest{RepositoryId: b.repo, Pairs: pairs, ContextLines: 0})
	if err != nil {
		return nil, err
	}
	for i, d := range resp.GetDiffs() {
		pf := patchFile{binary: d.GetBinary() || d.GetTooLarge()}
		for _, h := range d.GetHunks() {
			pf.hunks = append(pf.hunks, Hunk{
				OldStart: int64(h.GetOldStart()), OldLines: int64(h.GetOldLines()),
				NewStart: int64(h.GetNewStart()), NewLines: int64(h.GetNewLines()),
			})
		}
		out[keys[i]] = pf
	}
	return out, nil
}

type errBadCommit string

func (e errBadCommit) Error() string { return "not a commit ID: " + string(e) }
