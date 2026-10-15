package service

import (
	"context"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Browse serves code as structured values (docs/scm.md, "Browse") to any
// identified caller: ok control, automation, and people alike.
type Browse struct {
	scmpb.UnimplementedBrowseServer
	c *core
}

// maxFramePayload bounds the bytes one streamed frame carries.
const maxFramePayload = 1 << 20

func signatureOut(s encoding.Signature) *scmpb.Signature {
	return &scmpb.Signature{Name: s.Name, Email: s.Email, TimeMillis: s.Time.Millis, TzOffsetMinutes: s.Time.TZOffset}
}

func commitOut(c store.CommitInfo) *scmpb.Commit {
	out := &scmpb.Commit{
		Id:          c.ID[:],
		Parents:     idsOut(c.Commit.Parents),
		ChangeId:    c.Commit.ChangeID[:],
		Description: c.Commit.Description,
		Author:      signatureOut(c.Commit.Author),
		Committer:   signatureOut(c.Commit.Committer),
		RootTree:    idsOut(c.Commit.RootTree),
		Conflicted:  len(c.Commit.RootTree) > 1,
		Signed:      c.Commit.SecureSig != nil,
	}
	return out
}

func entryTypeOut(k encoding.TreeValueKind) scmpb.EntryType {
	switch k {
	case encoding.TreeValueFile:
		return scmpb.EntryType_ENTRY_TYPE_FILE
	case encoding.TreeValueSymlink:
		return scmpb.EntryType_ENTRY_TYPE_SYMLINK
	case encoding.TreeValueTree:
		return scmpb.EntryType_ENTRY_TYPE_TREE
	}
	return scmpb.EntryType_ENTRY_TYPE_UNSPECIFIED
}

func valueOut(v *store.BrowseValue) *scmpb.TreeValue {
	if v == nil {
		return nil
	}
	out := &scmpb.TreeValue{
		Type:          entryTypeOut(v.Kind),
		Id:            v.ID[:],
		Executable:    v.Executable,
		Size:          v.Size,
		SymlinkTarget: v.SymlinkTarget,
	}
	if v.CopyID != nil {
		out.CopyId = v.CopyID[:]
	}
	return out
}

// Log page tokens name a generation on main and a direction: "o<g>" pages to commits
// older than generation g, "n<g>" to newer ones.

// pageToken reads a Log page token.
func pageToken(t string) (before, after int64, err error) {
	if t == "" {
		return 0, 0, nil
	}
	n, perr := strconv.ParseInt(t[1:], 10, 64)
	if perr != nil || n <= 0 || (t[0] != 'o' && t[0] != 'n') {
		return 0, 0, status.Errorf(codes.InvalidArgument, "page_token %q is not one this service issued", t)
	}
	if t[0] == 'o' {
		return n, 0, nil
	}
	return 0, n, nil
}

func formatPageToken(direction byte, n int64) string {
	if n == 0 {
		return ""
	}
	return string(direction) + strconv.FormatInt(n, 10)
}

// optionalID reads an optional ID: empty is none.
func optionalID(b []byte, field string) (*encoding.ID, error) {
	if len(b) == 0 {
		return nil, nil
	}
	id, err := idIn(b, field)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func (s *Browse) GetCommits(ctx context.Context, req *scmpb.GetCommitsRequest) (*scmpb.GetCommitsResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if len(req.GetCommitIds()) > maxRefs {
		return nil, status.Errorf(codes.InvalidArgument, "commit_ids: %d (at most %d)", len(req.GetCommitIds()), maxRefs)
	}
	ids, err := idsIn(req.GetCommitIds(), "commit_ids")
	if err != nil {
		return nil, err
	}
	commits, err := s.c.store.GetCommits(ctx, req.GetRepositoryId(), ids)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.GetCommitsResponse{}
	for _, c := range commits {
		out.Commits = append(out.Commits, commitOut(c))
	}
	return out, nil
}

func (s *Browse) Log(ctx context.Context, req *scmpb.LogRequest) (*scmpb.LogResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	from, err := optionalID(req.GetFrom(), "from")
	if err != nil {
		return nil, err
	}
	before, after, err := pageToken(req.GetPageToken())
	if err != nil {
		return nil, err
	}
	page, err := s.c.store.Log(ctx, req.GetRepositoryId(), from, req.GetPathPrefixes(), int(req.GetPageSize()), before, after)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.LogResponse{NextPageToken: formatPageToken('o', page.Older), PrevPageToken: formatPageToken('n', page.Newer)}
	for _, c := range page.Commits {
		out.Commits = append(out.Commits, commitOut(c))
	}
	return out, nil
}

func (s *Browse) ReadTree(ctx context.Context, req *scmpb.ReadTreeRequest) (*scmpb.ReadTreeResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	commit, err := idIn(req.GetCommitId(), "commit_id")
	if err != nil {
		return nil, err
	}
	treeID, entries, err := s.c.store.ReadTree(ctx, req.GetRepositoryId(), commit, req.GetPath(), req.GetLastCommits())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.ReadTreeResponse{TreeId: treeID[:]}
	seen := map[encoding.ID]bool{}
	var last []encoding.ID
	for i := range entries {
		e := &scmpb.TreeEntry{Name: entries[i].Name, Value: valueOut(&entries[i].Value)}
		if c := entries[i].LastCommit; c != nil {
			e.LastCommitId = c[:]
			if !seen[*c] {
				seen[*c] = true
				last = append(last, *c)
			}
		}
		out.Entries = append(out.Entries, e)
	}
	if len(last) > 0 {
		commits, err := s.c.store.GetCommits(ctx, req.GetRepositoryId(), last)
		if err != nil {
			return nil, toStatus(err)
		}
		for _, c := range commits {
			out.Commits = append(out.Commits, commitOut(c))
		}
	}
	return out, nil
}

func (s *Browse) ReadFileAt(req *scmpb.ReadFileAtRequest, stream grpc.ServerStreamingServer[scmpb.ReadFileAtResponse]) error {
	ctx := stream.Context()
	if _, err := caller(ctx); err != nil {
		return err
	}
	commit, err := idIn(req.GetCommitId(), "commit_id")
	if err != nil {
		return err
	}
	err = s.c.store.ReadFileAt(ctx, req.GetRepositoryId(), commit, req.GetPath(), req.GetOffset(), req.GetLength(), func(v *store.BrowseValue, data []byte) error {
		first := true
		for first || len(data) > 0 {
			n := min(len(data), maxFramePayload)
			frame := &scmpb.ReadFileAtResponse{Data: data[:n]}
			if first {
				frame.Value = valueOut(v)
			}
			if err := stream.Send(frame); err != nil {
				return err
			}
			data, v, first = data[n:], nil, false
		}
		return nil
	})
	return toStatus(err)
}

func (s *Browse) ReadFilesAt(req *scmpb.ReadFilesAtRequest, stream grpc.ServerStreamingServer[scmpb.ReadFilesAtResponse]) error {
	ctx := stream.Context()
	if _, err := caller(ctx); err != nil {
		return err
	}
	commit, err := idIn(req.GetCommitId(), "commit_id")
	if err != nil {
		return err
	}
	if len(req.GetPaths()) > maxRefs {
		return status.Errorf(codes.InvalidArgument, "paths: %d (at most %d)", len(req.GetPaths()), maxRefs)
	}
	files, err := s.c.store.ReadFilesAt(ctx, req.GetRepositoryId(), commit, req.GetPaths(), req.GetLimit())
	if err != nil {
		return toStatus(err)
	}
	for _, f := range files {
		out := &scmpb.ReadFilesAtResponse{Path: f.Path, Data: f.Data}
		if f.Value != nil {
			out.Value = valueOut(f.Value)
		}
		if err := stream.Send(out); err != nil {
			return err
		}
	}
	return nil
}

func pathChangeKindOut(k store.PathChangeKind) scmpb.PathChange_Kind {
	switch k {
	case store.PathAdded:
		return scmpb.PathChange_KIND_ADDED
	case store.PathDeleted:
		return scmpb.PathChange_KIND_DELETED
	case store.PathModified:
		return scmpb.PathChange_KIND_MODIFIED
	case store.PathRenamed:
		return scmpb.PathChange_KIND_RENAMED
	case store.PathCopied:
		return scmpb.PathChange_KIND_COPIED
	case store.PathTypeChanged:
		return scmpb.PathChange_KIND_TYPE_CHANGED
	}
	return scmpb.PathChange_KIND_UNSPECIFIED
}

func (s *Browse) DiffTrees(ctx context.Context, req *scmpb.DiffTreesRequest) (*scmpb.DiffTreesResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	var from, to encoding.ID
	if f, err := optionalID(req.GetFromTree(), "from_tree"); err != nil {
		return nil, err
	} else if f != nil {
		from = *f
	}
	if t, err := optionalID(req.GetToTree(), "to_tree"); err != nil {
		return nil, err
	} else if t != nil {
		to = *t
	}
	changes, err := s.c.store.DiffTrees(ctx, req.GetRepositoryId(), from, to, req.GetPathPrefixes(), req.GetDetectRenames(), int(req.GetRenameSimilarityPercent()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.DiffTreesResponse{}
	for _, c := range changes {
		out.Changes = append(out.Changes, &scmpb.PathChange{
			Kind:    pathChangeKindOut(c.Kind),
			Path:    c.Path,
			OldPath: c.OldPath,
			Before:  valueOut(c.Before),
			After:   valueOut(c.After),
		})
	}
	return out, nil
}

// maxDiffPairs bounds the file pairs one DiffFiles call diffs.
const maxDiffPairs = 1000

func diffOpOut(op store.DiffOp) scmpb.DiffLine_Op {
	switch op {
	case store.DiffContext:
		return scmpb.DiffLine_OP_CONTEXT
	case store.DiffAdd:
		return scmpb.DiffLine_OP_ADD
	case store.DiffDelete:
		return scmpb.DiffLine_OP_DELETE
	}
	return scmpb.DiffLine_OP_UNSPECIFIED
}

func (s *Browse) DiffFiles(ctx context.Context, req *scmpb.DiffFilesRequest) (*scmpb.DiffFilesResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if len(req.GetPairs()) > maxDiffPairs {
		return nil, status.Errorf(codes.InvalidArgument, "pairs: %d (at most %d)", len(req.GetPairs()), maxDiffPairs)
	}
	pairs := make([]store.FilePair, len(req.GetPairs()))
	for i, p := range req.GetPairs() {
		old, err := optionalID(p.GetOldFile(), "old_file")
		if err != nil {
			return nil, err
		}
		cur, err := optionalID(p.GetNewFile(), "new_file")
		if err != nil {
			return nil, err
		}
		pairs[i] = store.FilePair{Old: old, New: cur}
	}
	diffs, err := s.c.store.DiffFiles(ctx, req.GetRepositoryId(), pairs, int(req.GetContextLines()), req.GetStatsOnly())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.DiffFilesResponse{}
	for _, d := range diffs {
		fd := &scmpb.FileDiff{Binary: d.Binary, TooLarge: d.TooLarge, Additions: int32(d.Additions), Deletions: int32(d.Deletions)}
		if req.GetStatsOnly() {
			out.Diffs = append(out.Diffs, fd)
			continue
		}
		for _, h := range d.Hunks {
			ph := &scmpb.Hunk{OldStart: int32(h.OldStart), OldLines: int32(h.OldLines), NewStart: int32(h.NewStart), NewLines: int32(h.NewLines)}
			for _, l := range h.Lines {
				ph.Lines = append(ph.Lines, &scmpb.DiffLine{Op: diffOpOut(l.Op), Text: l.Text, NoNewline: l.NoNewline})
			}
			fd.Hunks = append(fd.Hunks, ph)
		}
		out.Diffs = append(out.Diffs, fd)
	}
	return out, nil
}

func (s *Browse) Annotate(ctx context.Context, req *scmpb.AnnotateRequest) (*scmpb.AnnotateResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	commit, err := idIn(req.GetCommitId(), "commit_id")
	if err != nil {
		return nil, err
	}
	ranges, err := s.c.store.Annotate(ctx, req.GetRepositoryId(), commit, req.GetPath())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.AnnotateResponse{}
	for _, r := range ranges {
		out.Ranges = append(out.Ranges, &scmpb.AnnotateResponse_Range{FirstLine: int32(r.FirstLine), LineCount: int32(r.LineCount), CommitId: r.Commit[:]})
	}
	return out, nil
}

func (s *Browse) IsAncestor(ctx context.Context, req *scmpb.IsAncestorRequest) (*scmpb.IsAncestorResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	a, err := idIn(req.GetAncestor(), "ancestor")
	if err != nil {
		return nil, err
	}
	d, err := idIn(req.GetDescendant(), "descendant")
	if err != nil {
		return nil, err
	}
	ok, err := s.c.store.IsAncestor(ctx, req.GetRepositoryId(), a, d)
	if err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.IsAncestorResponse{IsAncestor: ok}, nil
}

func (s *Browse) Export(req *scmpb.ExportRequest, stream grpc.ServerStreamingServer[scmpb.ExportResponse]) error {
	ctx := stream.Context()
	if _, err := caller(ctx); err != nil {
		return err
	}
	commit, err := idIn(req.GetCommitId(), "commit_id")
	if err != nil {
		return err
	}
	err = s.c.store.Export(ctx, req.GetRepositoryId(), commit, req.GetPathPrefixes(), !req.GetEntriesOnly(), func(e *store.ExportEntry, data []byte) error {
		if e != nil {
			return stream.Send(&scmpb.ExportResponse{Frame: &scmpb.ExportResponse_Entry{Entry: &scmpb.ExportEntry{Path: e.Path, Value: valueOut(&e.Value)}}})
		}
		for len(data) > 0 {
			n := min(len(data), maxFramePayload)
			if err := stream.Send(&scmpb.ExportResponse{Frame: &scmpb.ExportResponse_Data{Data: data[:n]}}); err != nil {
				return err
			}
			data = data[n:]
		}
		return nil
	})
	return toStatus(err)
}
