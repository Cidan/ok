package review

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/workflow"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// maxIdentityFileSize bounds a set's declaration file read from the SCM.
const maxIdentityFileSize = 64 << 10

// IdentityReq is one approval a change to a repository's groups-and-ACLs tree needs
// (docs/identity.md, "Groups and ACLs"): a member of an ACL approving one part of the change.
// Where main has a tree, these replace OWNERS for its files.
type IdentityReq struct {
	// What is the part of the change: "create deploy.acl.example.com", "change identity/README.md".
	What string
	// Dir is the tree's folder in the repository, as main's .ok/repository.yaml names it.
	Dir string
	// ACL is the ACL whose member approves it, and ACLPath its folder in the tree ("" for the
	// root).
	ACL     string
	ACLPath string
	// Eligible are the principals whose Approval accepts it, given what the change's accepted
	// parts grant — by name, as the tree names them.
	Eligible []string
	Accepted bool
	// ApprovedBy are the eligible principals who granted Approval, by name; Implicit, that the
	// author is eligible, which accepts it by itself — as an author who is an owner satisfies
	// OWNERS.
	ApprovedBy []string
	Implicit   bool
}

// IdentityDirectory is what the review asks ok identity about a change to the tree, so that one
// it approves is one identity applies once it lands; and the groups the system of record syncs,
// which an ACL may hold.
type IdentityDirectory interface {
	IsPrincipal(ctx context.Context, name string) (bool, error)
	RetiredSetNames(ctx context.Context) (map[string]bool, error)
	SyncedGroups(ctx context.Context) (map[string][]string, error)
}

// TreeDir is the folder of ok's groups-and-ACLs tree at a commit of a repository, as its
// .ok/repository.yaml names it (identity:): "" for any repository but the tree's, or one that names
// no folder.
func (s *Service) TreeDir(ctx context.Context, repo int64, commit []byte) (string, error) {
	if s.treeRepository == "" {
		return "", nil
	}
	r, err := s.scm.Repositories.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}})
	if err != nil {
		return "", err
	}
	if r.GetName() != s.treeRepository {
		return "", nil
	}
	read, err := s.scm.ReadFilesAt(ctx, repo, commit, []string{workflow.SettingsFile}, 64<<10)
	if err != nil {
		return "", err
	}
	f := read[workflow.SettingsFile]
	present := f.Value != nil && f.Value.GetType() == scmpb.EntryType_ENTRY_TYPE_FILE
	settings, err := workflow.ParseSettings(f.Data, present)
	if err != nil {
		// A repository whose settings do not parse governs no tree: the settings are refused at
		// upload, so main's always parse.
		return "", nil
	}
	return settings.Identity, nil
}

// inTree reports whether a repository path is in the tree at dir.
func inTree(dir, p string) bool { return dir != "" && strings.HasPrefix(p, dir+"/") }

// splitTree separates the files in the tree at dir from the rest.
func splitTree(dir string, files []string) (tree, rest []string) {
	for _, f := range files {
		if inTree(dir, f) {
			tree = append(tree, f)
		} else {
			rest = append(rest, f)
		}
	}
	return tree, rest
}

// treeFiles reads the tree's files at a commit, by their paths from the tree's root (dir):
// every file's name, and the contents of its sets' declarations — the tree's entries in one call
// (Export), its declarations in another (readTreeFiles). Paths with a part starting with "." are
// no part of the tree. found is false where the commit has no tree.
func (s *Service) treeFiles(ctx context.Context, repo int64, commit []byte, dir string) (map[string][]byte, bool, error) {
	stream, err := s.scm.Browse.Export(ctx, &scmpb.ExportRequest{RepositoryId: repo, CommitId: commit, PathPrefixes: []string{dir}, EntriesOnly: true})
	if err != nil {
		return nil, false, err
	}
	files := map[string][]byte{}
	var paths []string
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, false, err
		}
		e := frame.GetEntry()
		if e == nil || !inTree(dir, e.GetPath()) {
			continue
		}
		rel := strings.TrimPrefix(e.GetPath(), dir+"/")
		if slices.ContainsFunc(strings.Split(rel, "/"), func(part string) bool { return strings.HasPrefix(part, ".") }) {
			continue
		}
		files[rel] = nil
		if e.GetValue().GetType() == scmpb.EntryType_ENTRY_TYPE_FILE && isDeclaration(rel) {
			paths = append(paths, e.GetPath())
		}
	}
	if len(files) == 0 {
		return nil, false, nil
	}
	read, err := s.readTreeFiles(ctx, repo, commit, paths)
	if err != nil {
		return nil, false, err
	}
	for p, f := range read {
		files[strings.TrimPrefix(p, dir+"/")] = f.data
	}
	return files, true, nil
}

// treeError is a tree that does not validate.
type treeError struct{ err error }

func (e treeError) Error() string { return e.err.Error() }

// isDeclaration reports whether a path in the tree is a set's declaration.
func isDeclaration(p string) bool {
	base := path.Base(p)
	return base == identity.ACL.File() || base == identity.Group.File()
}

// treeFile is one of the tree's files as read: whether it is there, and, for a set's
// declaration, its contents.
type treeFile struct {
	data  []byte
	found bool
}

// readTreeFiles reads some of the tree's files at a commit at once (ReadFilesAt), by path. A
// declaration larger than maxIdentityFileSize is a treeError.
func (s *Service) readTreeFiles(ctx context.Context, repo int64, commit []byte, paths []string) (map[string]treeFile, error) {
	read, err := s.scm.ReadFilesAt(ctx, repo, commit, paths, maxIdentityFileSize+1)
	if err != nil {
		return nil, err
	}
	out := make(map[string]treeFile, len(paths))
	for _, p := range paths {
		f := read[p]
		switch {
		case f.Value == nil:
			out[p] = treeFile{}
		case !isDeclaration(p):
			out[p] = treeFile{found: true}
		case f.Value.GetType() != scmpb.EntryType_ENTRY_TYPE_FILE:
			out[p] = treeFile{}
		case len(f.Data) > maxIdentityFileSize:
			return nil, treeError{fmt.Errorf("%s is larger than %d bytes", p, maxIdentityFileSize)}
		default:
			out[p] = treeFile{data: f.Data, found: true}
		}
	}
	return out, nil
}

// evaluateTree decides the approvals the change from before to after needs, as approvers —
// the principals who granted Approval, and the author — may give them: identity.Authorize for
// the sets, and a member of the root ACL, as it is before, for the tree's other files the change
// touches (its build.yaml, its documentation, its workflow).
func evaluateTree(dir string, before, after *identity.Tree, other []string, grants []grant, author string) []IdentityReq {
	granted := map[string]bool{}
	approvers := []string{author}
	for _, g := range grants {
		if g.label == db.LabelApproval {
			granted[g.principal] = true
			approvers = append(approvers, g.principal)
		}
	}
	folder := func(acl string) string {
		if s, ok := after.Set(acl); ok {
			return s.Path
		}
		s, _ := before.Set(acl)
		return s.Path
	}
	req := func(what, acl string, eligible []string, accepted bool) IdentityReq {
		r := IdentityReq{What: what, Dir: dir, ACL: acl, ACLPath: folder(acl), Eligible: eligible, Accepted: accepted}
		for _, e := range eligible {
			if granted[e] {
				r.ApprovedBy = append(r.ApprovedBy, e)
			}
		}
		r.Implicit = slices.Contains(eligible, author)
		return r
	}
	var out []IdentityReq
	for _, p := range identity.Authorize(before, after, approvers).Parts {
		out = append(out, req(p.What(), p.By, p.Eligible, p.Accepted))
	}
	if len(other) > 0 {
		root := before.Names().Of(identity.ACL, identity.Root)
		eligible := before.Expand(root)
		accepted := slices.ContainsFunc(approvers, func(a string) bool { return slices.Contains(eligible, a) })
		out = append(out, req("change "+strings.Join(other, ", "), root, eligible, accepted))
	}
	return out
}

// applies reports why ok identity would refuse to apply the tree the change makes, or nil: an ACL
// deleted that still owns something, a member that is no principal, or a set declared with a
// name identity retired.
func applies(ctx context.Context, dir IdentityDirectory, before, after *identity.Tree) error {
	var problems []string
	if err := identity.Check(before, after); err != nil {
		e, _ := identity.AsError(err)
		problems = append(problems, e.Problems...)
	}
	var checked []string
	for _, set := range after.Sets() {
		for _, m := range set.Members {
			if k, _ := after.Names().KindOf(m); (k != identity.Human && k != identity.Robot) || slices.Contains(checked, m) {
				continue
			}
			checked = append(checked, m)
			ok, err := dir.IsPrincipal(ctx, m)
			if err != nil {
				return fmt.Errorf("asking ok identity about %s: %w", m, err)
			}
			if !ok {
				problems = append(problems, fmt.Sprintf("%s (in %s) is no principal", m, after.Names().OfSet(set)))
			}
		}
	}
	retired, err := dir.RetiredSetNames(ctx)
	if err != nil {
		return fmt.Errorf("asking ok identity for the retired names: %w", err)
	}
	for _, c := range identity.Diff(before, after) {
		if c.Created && retired[c.Set] {
			problems = append(problems, c.Set+" was deleted, and its name is retired: a set's name is never declared again")
		}
	}
	if len(problems) > 0 {
		return &identity.Error{Problems: problems}
	}
	return nil
}

// evaluateIdentity decides a CL's the groups-and-ACLs tree approvals, for the tree's files among those it
// governs: the change from the tree on main (base) to that tree with those files as the CL has
// them — the CL's own change, whatever has landed since it was made. ok is false where main (or,
// for a rollback, the inverse change it began as) has no tree: the first tree is approved by
// OWNERS, like any file. An error that is no failure to read is why the change cannot be
// approved: it is no valid tree, or ok identity would refuse it.
func (s *Service) evaluateIdentity(ctx context.Context, cl db.ChangeList, dir string, base, commit []byte, treeFiles []string, grants []grant) (reqs []IdentityReq, ok bool, failed error, err error) {
	baseFiles, present, err := s.treeFiles(ctx, cl.Repo, base, dir)
	var te treeError
	switch {
	case errors.As(err, &te):
		return nil, true, fmt.Errorf("main's tree: %w", te.err), nil
	case err != nil:
		return nil, false, nil, err
	case !present:
		return nil, false, nil, nil
	}
	var synced map[string][]string
	if s.identityDir != nil {
		if synced, err = s.identityDir.SyncedGroups(ctx); err != nil {
			return nil, false, nil, fmt.Errorf("the groups ok identity syncs: %w", err)
		}
	}
	before, err := identity.LoadFiles(people.Names(), baseFiles, identity.WithSynced(synced))
	if err != nil {
		return nil, true, fmt.Errorf("main's tree is no valid tree: %w", err), nil
	}
	afterFiles := map[string][]byte{}
	for p, d := range baseFiles {
		afterFiles[p] = d
	}
	changed, err := s.readTreeFiles(ctx, cl.Repo, commit, treeFiles)
	switch {
	case errors.As(err, &te):
		return nil, true, fmt.Errorf("the change's tree: %w", te.err), nil
	case err != nil:
		return nil, false, nil, err
	}
	for _, f := range treeFiles {
		rel := strings.TrimPrefix(f, dir+"/")
		if c := changed[f]; c.found {
			afterFiles[rel] = c.data
		} else {
			delete(afterFiles, rel)
		}
	}
	if len(afterFiles) == 0 {
		return nil, true, errors.New("the change deletes the tree"), nil
	}
	after, err := identity.LoadFiles(people.Names(), afterFiles, identity.WithSynced(synced))
	if err != nil {
		return nil, true, fmt.Errorf("the change's tree is no valid tree: %w", err), nil
	}
	if s.identityDir != nil {
		if err := applies(ctx, s.identityDir, before, after); err != nil {
			return nil, true, fmt.Errorf("ok identity would not apply it: %w", err), nil
		}
	}
	var other []string
	for _, f := range treeFiles {
		if b := path.Base(f); b != identity.ACL.File() && b != identity.Group.File() {
			other = append(other, f)
		}
	}
	// The tree names principals by name: approvers are matched by theirs now.
	byName := make([]grant, len(grants))
	for i, g := range grants {
		byName[i] = grant{label: g.label, principal: people.Name(ctx, g.principal)}
	}
	return evaluateTree(dir, before, after, other, byName, people.Name(ctx, cl.Author)), true, nil, nil
}
