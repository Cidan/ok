package review

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/scm"
	"github.com/Cidan/ok/internal/owners"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Service is ok control's review process over its database and the SCM.
type Service struct {
	db     db.Store
	scm    *scm.Client
	auto   Automation
	people *people.Directory
	// identityDir is ok identity, which a change to the groups-and-ACLs tree must be one it applies.
	identityDir IdentityDirectory
	// treeRepository names the repository whose groups-and-ACLs tree is ok's.
	treeRepository string
	// feedWait bounds how long an action waits for the change feed to apply its own
	// transition or landing before the page renders.
	feedWait time.Duration
}

// New builds the review service. auto is ok automation: its verdict on each patchset's pre
// gates submit, and its landing queue lands what is submitted. dir is ok identity's directory:
// its people, its sets, and who administers ok — every repository, besides each one's creator.
// treeRepository names the repository whose groups-and-ACLs tree is ok's, if any.
func New(d db.Store, c *scm.Client, auto Automation, dir *people.Directory, treeRepository string) *Service {
	s := &Service{db: d, scm: c, auto: auto, people: dir, treeRepository: treeRepository, feedWait: 10 * time.Second}
	if dir != nil {
		s.identityDir = dir
	}
	return s
}

// UserError is a refusal meant for the person who asked: a permission, status, or
// request problem, shown to them as is.
type UserError struct{ Msg string }

func (e *UserError) Error() string { return e.Msg }

func refuse(format string, args ...any) error {
	return &UserError{Msg: fmt.Sprintf(format, args...)}
}

// IsUserError reports whether err is a refusal to show the person, not a failure.
func IsUserError(err error) bool {
	var u *UserError
	return errors.As(err, &u)
}

// ErrNotFound is a CL or comment that does not exist.
var ErrNotFound = errors.New("not found")

// IsAdmin reports whether principal administers the repository: its creator, or one of ok's
// administrators. When ok identity cannot say, they are not one: the answer gates actions.
func (s *Service) IsAdmin(ctx context.Context, repo *scmpb.Repository, principal string) bool {
	if principal == "" {
		return false
	}
	if principal == repo.GetCreatedBy() {
		return true
	}
	if s.people == nil {
		return false
	}
	ok, err := s.people.IsAdmin(ctx, principal)
	if err != nil {
		slog.Warn("review: asking ok identity whether someone administers ok", "principal", principal, "err", err)
		return false
	}
	return ok
}

// CanManage reports whether principal may drive a CL's lifecycle — Request Review,
// Return to Draft, Submit, Abandon, Restore: its author, or a repository admin.
func (s *Service) CanManage(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string) bool {
	return principal != "" && (principal == cl.Author || s.IsAdmin(ctx, repo, principal))
}

// ChangeList reads a CL's projection, and the SCM's CL with its patchsets.
func (s *Service) ChangeList(ctx context.Context, repo, number int64) (db.ChangeList, *scmpb.ChangeList, error) {
	c, err := s.scm.ChangeLists.GetChangeList(ctx, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: number})
	if status.Code(err) == codes.NotFound {
		return db.ChangeList{}, nil, ErrNotFound
	}
	if err != nil {
		return db.ChangeList{}, nil, err
	}
	// The SCM is authoritative: bring the projection up to it before reading.
	if _, err := s.db.UpsertChangeList(ctx, scm.ChangeListOut(c)); err != nil {
		return db.ChangeList{}, nil, err
	}
	p, ok, err := s.db.GetChangeList(ctx, repo, number)
	if err != nil {
		return db.ChangeList{}, nil, err
	}
	if !ok {
		return db.ChangeList{}, nil, ErrNotFound
	}
	return p, c, nil
}

// Evaluation is a CL's live review evaluation, with what it was computed from.
type Evaluation struct {
	Status *Status
	// Head is main's head, where OWNERS were read.
	Head []byte
	// Files are the paths the current patchset changes (a rename's both paths).
	Files []string
	// Governed are the files OWNERS govern — every file but those of main's groups-and-ACLs tree,
	// which ACLs govern — but for a rollback only those its
	// author edited by hand (handEdited).
	Governed []string
}

// maxOwnersFileSize caps how much of an OWNERS-config file is read.
const maxOwnersFileSize = 256 << 10

// ownersSource reads OWNERS-config files at a commit through Browse: the OWNERS along the
// governed files' directories in one call (ReadFilesAt, prefetch), and anything else the
// resolver asks for — an import's target — as it asks.
type ownersSource struct {
	ctx    context.Context
	scm    *scm.Client
	repo   int64
	commit []byte
	read   map[string]scm.FileAt // the prefetched paths; a nil Value is absent
}

// prefetch reads every OWNERS at or above the files' directories at once: the resolver walks
// each file's directories up to its nearest OWNERS, an absent one at a time.
func (o *ownersSource) prefetch(files []string) error {
	if scm.IsRoot(o.commit) || len(files) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var paths []string
	for _, f := range files {
		for d := path.Dir(f); ; d = path.Dir(d) {
			p := "OWNERS"
			if d != "." {
				p = d + "/OWNERS"
			}
			if seen[p] {
				break
			}
			seen[p] = true
			paths = append(paths, p)
			if d == "." {
				break
			}
		}
	}
	read, err := o.scm.ReadFilesAt(o.ctx, o.repo, o.commit, paths, maxOwnersFileSize+1)
	if err != nil {
		return err
	}
	o.read = read
	return nil
}

func (o *ownersSource) Read(p string) ([]byte, bool, error) {
	if scm.IsRoot(o.commit) {
		return nil, false, nil
	}
	if f, ok := o.read[p]; ok {
		return ownersFile(p, f.Value, f.Data, uint64(len(f.Data)) > maxOwnersFileSize)
	}
	data, v, truncated, err := o.scm.ReadFile(o.ctx, o.repo, o.commit, p, maxOwnersFileSize+1)
	if code := status.Code(err); code == codes.NotFound || code == codes.InvalidArgument {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return ownersFile(p, v, data, truncated)
}

// ownersFile is an OWNERS-config file as read: absent unless a file, and refused when larger
// than maxOwnersFileSize.
func ownersFile(p string, v *scmpb.TreeValue, data []byte, truncated bool) ([]byte, bool, error) {
	if v.GetType() != scmpb.EntryType_ENTRY_TYPE_FILE {
		return nil, false, nil
	}
	if truncated || len(data) > maxOwnersFileSize {
		return nil, false, fmt.Errorf("%s is larger than %d bytes", p, maxOwnersFileSize)
	}
	return data, true, nil
}

// changedFiles lists the paths a patchset changes against its base, a rename as both its
// paths (both need their owners' approval).
func (s *Service) changedFiles(ctx context.Context, repo int64, ps *scmpb.Patchset) ([]string, error) {
	baseTree, err := s.scm.Tree(ctx, repo, ps.GetBaseCommitId())
	if err != nil {
		return nil, err
	}
	tree, err := s.scm.Tree(ctx, repo, ps.GetCommitId())
	if err != nil {
		return nil, err
	}
	resp, err := s.scm.Browse.DiffTrees(ctx, &scmpb.DiffTreesRequest{RepositoryId: repo, FromTree: baseTree, ToTree: tree})
	if err != nil {
		return nil, err
	}
	var files []string
	for _, c := range resp.GetChanges() {
		files = append(files, c.GetPath())
		if c.GetKind() == scmpb.PathChange_KIND_RENAMED && c.GetOldPath() != "" {
			files = append(files, c.GetOldPath())
		}
	}
	return files, nil
}

// Evaluate computes a CL's review requirements for its current patchset. OWNERS are read
// at main's head — never from the CL, so a change cannot grant its own author ownership.
// Each changed file is governed by its nearest OWNERS; every governing owner set needs a
// counting Approval, LGTM needs a counting grant, and no review thread may be unresolved.
// Grants count whichever patchset they were made on. A rollback's review is expedited
// (docs/automation.md, "Rollbacks"): OWNERS govern only the files its author edited by
// hand, and its LGTM must come from someone else.
//
// Where main has a groups-and-ACLs tree, its files are governed by the ACLs that own what the change
// changes instead (docs/identity.md §9, evaluateIdentity), against the tree on main — or, for
// a rollback, the inverse change it began as: the rest of a rollback needs no Approval.
func (s *Service) Evaluate(ctx context.Context, cl db.ChangeList, c *scmpb.ChangeList, head []byte) (*Evaluation, error) {
	if head == nil {
		r, err := s.scm.Repositories.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: cl.Repo}})
		if err != nil {
			return nil, err
		}
		head = r.GetHead()
	}
	ps := scm.Patchset(c, c.GetCurrentPatchset())
	if ps == nil {
		return nil, fmt.Errorf("CL %d has no patchset %d", cl.Number, c.GetCurrentPatchset())
	}
	files, err := s.changedFiles(ctx, cl.Repo, ps)
	if err != nil {
		return nil, fmt.Errorf("the files CL %d changes: %w", cl.Number, err)
	}
	rollback := c.GetRollback() != nil
	governed := files
	if rollback {
		if governed, err = s.handEdited(ctx, cl.Repo, c, ps, files); err != nil {
			return nil, fmt.Errorf("the files rollback CL %d edits by hand: %w", cl.Number, err)
		}
	}
	rows, err := s.db.Grants(ctx, cl.Repo, cl.Number)
	if err != nil {
		return nil, err
	}
	unresolved, err := s.db.UnresolvedThreads(ctx, cl.Repo, cl.Number)
	if err != nil {
		return nil, err
	}
	grants := grantsOf(rows)
	var idReqs []IdentityReq
	var idErr error
	treeDir, err := s.TreeDir(ctx, cl.Repo, head)
	if err != nil {
		return nil, fmt.Errorf("main's repository settings: %w", err)
	}
	if treeFiles, rest := splitTree(treeDir, governed); len(treeFiles) > 0 {
		base := head
		if rollback {
			base = scm.Patchset(c, 1).GetCommitId()
		}
		reqs, ok, failed, err := s.evaluateIdentity(ctx, cl, treeDir, base, ps.GetCommitId(), treeFiles, grants)
		if err != nil {
			return nil, fmt.Errorf("CL %d's change to the groups-and-ACLs tree: %w", cl.Number, err)
		}
		if ok {
			idReqs, idErr, governed = reqs, failed, rest
		}
	}
	src := &ownersSource{ctx: ctx, scm: s.scm, repo: cl.Repo, commit: head}
	var sets []*owners.Set
	ownersErr := src.prefetch(governed)
	if ownersErr != nil {
		ownersErr = fmt.Errorf("read OWNERS: %w", ownersErr)
	} else {
		sets, ownersErr = owners.NewResolver(src).Resolve(governed)
	}
	var acls aclMembers
	if ownersErr == nil {
		acls, ownersErr = s.aclMembers(ctx, sets)
	}
	st := evaluate(sets, ownersErr, grants, cl.Author, unresolved, rollback, acls)
	st.Identity, st.IdentityErr = idReqs, idErr
	return &Evaluation{Status: st, Head: head, Files: files, Governed: governed}, nil
}

// handEdited are the files a rollback CL's current patchset changes to anything but what its
// first patchset — the inverse change the SCM computed — has there: its author's own edits,
// such as the paths the rollback left. The rest is the landed CLs' changes undone, which
// needs no Approval.
func (s *Service) handEdited(ctx context.Context, repo int64, c *scmpb.ChangeList, ps *scmpb.Patchset, files []string) ([]string, error) {
	first := scm.Patchset(c, 1)
	if first == nil {
		return nil, fmt.Errorf("CL %d has no first patchset", c.GetNumber())
	}
	if first.GetNumber() == ps.GetNumber() {
		return nil, nil
	}
	from, err := s.scm.Tree(ctx, repo, first.GetCommitId())
	if err != nil {
		return nil, err
	}
	to, err := s.scm.Tree(ctx, repo, ps.GetCommitId())
	if err != nil {
		return nil, err
	}
	resp, err := s.scm.Browse.DiffTrees(ctx, &scmpb.DiffTreesRequest{RepositoryId: repo, FromTree: from, ToTree: to})
	if err != nil {
		return nil, err
	}
	differs := map[string]bool{}
	for _, ch := range resp.GetChanges() {
		differs[ch.GetPath()] = true
		if ch.GetOldPath() != "" {
			differs[ch.GetOldPath()] = true
		}
	}
	var out []string
	for _, f := range files {
		if differs[f] {
			out = append(out, f)
		}
	}
	return out, nil
}

// Refresh re-derives a CL's approved projection from a live evaluation, recording when it
// is gained or lost. A closed CL's projection is left as it is.
func (s *Service) Refresh(ctx context.Context, repo, number int64) error {
	cl, c, err := s.ChangeList(ctx, repo, number)
	if err != nil {
		return err
	}
	if !cl.Status.Active() {
		return nil
	}
	ev, err := s.Evaluate(ctx, cl, c, nil)
	if err != nil {
		return err
	}
	ok := ev.Status.Satisfied()
	return s.db.Txn(ctx, func(tx db.Ops) error {
		changed, err := tx.SetApproved(ctx, repo, number, ok)
		if err != nil || !changed {
			return err
		}
		kind := db.EventApprovalLost
		if ok {
			kind = db.EventApproved
		}
		return tx.RecordEvent(ctx, repo, number, db.Event{Kind: kind, Patchset: cl.CurrentPatchset})
	})
}

// WaitForFeed waits until the change feed has applied a repository's event seq (its place in
// the repository's log), so a page rendered after an action shows it; after feedWait it gives
// up and the page renders from the projection the action already updated.
func (s *Service) WaitForFeed(ctx context.Context, repo, seq int64) error {
	if seq == 0 {
		return nil
	}
	deadline := time.Now().Add(s.feedWait)
	for {
		cur, err := s.db.FollowCursor(ctx, scmfeed.Topic, repo)
		if err != nil || cur >= seq || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// transition applies a lifecycle transition through the SCM on principal's behalf.
func (s *Service) transition(ctx context.Context, cl db.ChangeList, kind scmpb.TransitionRequest_Kind, principal, reason string) error {
	resp, err := s.scm.Review.Transition(ctx, &scmpb.TransitionRequest{
		RepositoryId: cl.Repo, Number: cl.Number, Kind: kind, Actor: principal, Reason: reason,
	})
	if status.Code(err) == codes.FailedPrecondition {
		return refuse("%s", upperFirst(status.Convert(err).Message()))
	}
	if err != nil {
		return err
	}
	if _, err := s.db.UpsertChangeList(ctx, scm.ChangeListOut(resp.GetChangeList())); err != nil {
		return err
	}
	return s.WaitForFeed(ctx, cl.Repo, resp.GetRepositorySeq())
}

func (s *Service) requireManage(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal, action string) error {
	if !s.CanManage(ctx, repo, cl, principal) {
		return refuse("Only the author or a repository admin can %s this changelist", action)
	}
	return nil
}

// checkReviewers refuses anyone who can't review the CL: its author, or anyone who is not
// an active person.
func (s *Service) checkReviewers(ctx context.Context, cl db.ChangeList, principals []string) ([]string, error) {
	var out []string
	for _, p := range principals {
		if slices.Contains(out, p) {
			continue
		}
		if p == cl.Author {
			return nil, refuse("The author of CL %d, %s, can't review it", cl.Number, people.Handle(p))
		}
		ok, err := s.people.IsActiveHuman(ctx, p)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, refuse("%s is not someone who can review changes", people.Handle(p))
		}
		out = append(out, p)
	}
	return out, nil
}

// assign adds reviewers inside a transaction, recording each new one.
func assign(ctx context.Context, tx db.Ops, cl db.ChangeList, by string, reviewers []string) error {
	for _, p := range reviewers {
		added, err := tx.AddReviewer(ctx, cl.Repo, cl.Number, p, by)
		if err != nil {
			return err
		}
		if added {
			if err := tx.RecordEvent(ctx, cl.Repo, cl.Number, db.Event{Actor: by, Kind: db.EventReviewerAdded, Subject: p, Patchset: cl.CurrentPatchset}); err != nil {
				return err
			}
		}
	}
	return nil
}

// postMessage posts a CL-level comment ("" posts nothing).
func postMessage(ctx context.Context, tx db.Ops, cl db.ChangeList, by, message string) error {
	message = strings.TrimSpace(message)
	if message == "" {
		return nil
	}
	_, err := tx.CreateComment(ctx, cl.Repo, db.Comment{Number: cl.Number, Patchset: cl.CurrentPatchset, Author: by, Body: message})
	return err
}

// RequestReview sends a draft for review: the SCM moves it to open, then the chosen
// reviewers are assigned and the optional message posted. Requesting review with no
// reviewers only leaves draft (the solo developer in an unowned repository).
func (s *Service) RequestReview(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string, reviewers []string, message string) error {
	if err := s.requireManage(ctx, repo, cl, principal, "request review on"); err != nil {
		return err
	}
	reviewers, err := s.checkReviewers(ctx, cl, reviewers)
	if err != nil {
		return err
	}
	if err := s.transition(ctx, cl, scmpb.TransitionRequest_KIND_REQUEST_REVIEW, principal, ""); err != nil {
		return err
	}
	if err := s.db.Txn(ctx, func(tx db.Ops) error {
		if err := assign(ctx, tx, cl, principal, reviewers); err != nil {
			return err
		}
		return postMessage(ctx, tx, cl, principal, message)
	}); err != nil {
		return err
	}
	return s.Refresh(ctx, cl.Repo, cl.Number)
}

// ReturnToDraft takes an open CL out of review. Its reviewers and grants stay.
func (s *Service) ReturnToDraft(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string) error {
	if err := s.requireManage(ctx, repo, cl, principal, "return to draft"); err != nil {
		return err
	}
	return s.transition(ctx, cl, scmpb.TransitionRequest_KIND_RETURN_TO_DRAFT, principal, "")
}

// Abandon closes an active CL without submitting it. Nothing is deleted, and Restore
// reopens it.
func (s *Service) Abandon(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal, reason string) error {
	if err := s.requireManage(ctx, repo, cl, principal, "abandon"); err != nil {
		return err
	}
	return s.transition(ctx, cl, scmpb.TransitionRequest_KIND_ABANDON, principal, strings.TrimSpace(reason))
}

// Restore reopens an abandoned CL in the status it was abandoned from, then re-derives
// its approval: OWNERS on main may have changed while it was closed.
func (s *Service) Restore(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string) error {
	if err := s.requireManage(ctx, repo, cl, principal, "restore"); err != nil {
		return err
	}
	if err := s.transition(ctx, cl, scmpb.TransitionRequest_KIND_RESTORE, principal, ""); err != nil {
		return err
	}
	return s.Refresh(ctx, cl.Repo, cl.Number)
}

// Rollback rolls back a submitted CL for principal (docs/automation.md, "Rollbacks"):
// the SCM creates the rollback CL — principal's, open for review, undoing the CL and those
// landed above it in its stack — with the CL's reviewers assigned, and the SCM adds the
// authors of the CLs it rolls back. It returns the rollback CL's number once the feed has
// applied its creation.
func (s *Service) Rollback(ctx context.Context, cl db.ChangeList, principal, reason string) (int64, error) {
	if !people.IsHuman(principal) {
		return 0, refuse("Only a person can roll back a change")
	}
	if cl.Status != db.StatusSubmitted {
		return 0, refuse("CL %d is %s: only a submitted change is rolled back", cl.Number, statusWord(cl.Status))
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return 0, refuse("Say why CL %d is rolled back", cl.Number)
	}
	rs, err := s.db.Reviewers(ctx, cl.Repo, cl.Number)
	if err != nil {
		return 0, err
	}
	var reviewers []string
	for _, r := range rs {
		if r.Principal != principal {
			reviewers = append(reviewers, r.Principal)
		}
	}
	resp, err := s.scm.Review.Rollback(ctx, &scmpb.RollbackRequest{
		RepositoryId: cl.Repo, Number: cl.Number, Actor: principal, Reason: reason, Reviewers: reviewers,
	})
	if status.Code(err) == codes.FailedPrecondition {
		return 0, refuse("%s", upperFirst(strings.TrimPrefix(status.Convert(err).Message(), "rollback refused: ")))
	}
	if err != nil {
		return 0, err
	}
	if _, err := s.db.UpsertChangeList(ctx, scm.ChangeListOut(resp.GetChangeList())); err != nil {
		return 0, err
	}
	if err := s.WaitForFeed(ctx, cl.Repo, resp.GetRepositorySeq()); err != nil {
		return 0, err
	}
	return resp.GetChangeList().GetNumber(), nil
}

// IsReviewer reports whether principal reviews the CL.
func (s *Service) IsReviewer(ctx context.Context, cl db.ChangeList, principal string) (bool, error) {
	rs, err := s.db.Reviewers(ctx, cl.Repo, cl.Number)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(rs, func(r db.Reviewer) bool { return r.Principal == principal }), nil
}

// lockActive locks a CL's projection row and refuses a closed CL.
func lockActive(ctx context.Context, tx db.Ops, cl db.ChangeList, what string) (db.ChangeList, error) {
	cur, ok, err := tx.LockChangeList(ctx, cl.Repo, cl.Number)
	if err != nil {
		return db.ChangeList{}, err
	}
	if !ok {
		return db.ChangeList{}, ErrNotFound
	}
	if !cur.Status.Active() {
		return db.ChangeList{}, refuse("CL %d is %s; %s", cl.Number, statusWord(cur.Status), what)
	}
	return cur, nil
}

func statusWord(s db.Status) string {
	switch s {
	case db.StatusDraft:
		return "a draft"
	case db.StatusOpen:
		return "open"
	case db.StatusSubmitted:
		return "submitted"
	case db.StatusAbandoned:
		return "abandoned"
	}
	return "unknown"
}

// AddReviewers assigns more reviewers to an active CL, posting the optional message. The
// author, a repository admin, and the CL's reviewers (who may hand a review on) can add
// reviewers.
func (s *Service) AddReviewers(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string, reviewers []string, message string) error {
	if !s.CanManage(ctx, repo, cl, principal) {
		ok, err := s.IsReviewer(ctx, cl, principal)
		if err != nil {
			return err
		}
		if !ok {
			return refuse("Only the author, a repository admin, or a reviewer can add reviewers to this changelist")
		}
	}
	reviewers, err := s.checkReviewers(ctx, cl, reviewers)
	if err != nil {
		return err
	}
	if len(reviewers) == 0 {
		return refuse("Choose at least one reviewer")
	}
	return s.db.Txn(ctx, func(tx db.Ops) error {
		cur, err := lockActive(ctx, tx, cl, "its reviewers can't change")
		if err != nil {
			return err
		}
		if err := assign(ctx, tx, cur, principal, reviewers); err != nil {
			return err
		}
		return postMessage(ctx, tx, cur, principal, message)
	})
}

// RemoveReviewer takes a reviewer off an active CL and revokes every label they grant, so
// the evaluation never counts someone no longer reviewing. The author and a repository
// admin can remove anyone; a reviewer can remove themself.
func (s *Service) RemoveReviewer(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal, reviewer string) error {
	if principal != reviewer {
		if err := s.requireManage(ctx, repo, cl, principal, "remove reviewers from"); err != nil {
			return err
		}
	}
	if err := s.db.Txn(ctx, func(tx db.Ops) error {
		cur, err := lockActive(ctx, tx, cl, "its reviewers can't change")
		if err != nil {
			return err
		}
		removed, err := tx.RemoveReviewer(ctx, cl.Repo, cl.Number, reviewer)
		if err != nil || !removed {
			return err
		}
		for _, l := range db.Labels {
			if _, err := tx.SetGrant(ctx, cl.Repo, cl.Number, reviewer, l, false, cur.CurrentPatchset); err != nil {
				return err
			}
		}
		return tx.RecordEvent(ctx, cl.Repo, cl.Number, db.Event{Actor: principal, Kind: db.EventReviewerRemoved, Subject: reviewer, Patchset: cur.CurrentPatchset})
	}); err != nil {
		return err
	}
	return s.Refresh(ctx, cl.Repo, cl.Number)
}

// Reply applies a reviewer's label grants and revocations and an optional comment to the
// CL's current patchset, atomically, then re-derives its approval. grants holds only the
// labels the reviewer's form carried; the rest are left as they are. Labels change only
// on an active CL; a comment may be left on any. Granting makes the granter a reviewer
// (never the author).
func (s *Service) Reply(ctx context.Context, cl db.ChangeList, principal string, grants map[db.Label]bool, comment string) error {
	if err := s.db.Txn(ctx, func(tx db.Ops) error {
		cur := cl
		if len(grants) > 0 {
			var err error
			if cur, err = lockActive(ctx, tx, cl, "it takes no more grants"); err != nil {
				return err
			}
		}
		for _, l := range db.Labels {
			g, ok := grants[l]
			if !ok {
				continue
			}
			changed, err := tx.SetGrant(ctx, cl.Repo, cl.Number, principal, l, g, cur.CurrentPatchset)
			if err != nil {
				return err
			}
			if !changed {
				continue
			}
			kind := db.EventLabelRevoked
			if g {
				kind = db.EventLabelGranted
			}
			if err := tx.RecordEvent(ctx, cl.Repo, cl.Number, db.Event{Actor: principal, Kind: kind, Label: l, Patchset: cur.CurrentPatchset}); err != nil {
				return err
			}
			if g && principal != cl.Author {
				if _, err := tx.AddReviewer(ctx, cl.Repo, cl.Number, principal, principal); err != nil {
					return err
				}
			}
		}
		return postMessage(ctx, tx, cur, principal, comment)
	}); err != nil {
		return err
	}
	return s.Refresh(ctx, cl.Repo, cl.Number)
}

// Comment posts a file or inline comment on a patchset, or a reply to a thread. A new
// thread opens unresolved, withdrawing the CL's approval until it is resolved; a reply
// inherits its thread's anchor.
func (s *Service) Comment(ctx context.Context, cl db.ChangeList, principal string, patchset int32, file string, line int32, side db.Side, parent int64, body string) error {
	body = strings.TrimSpace(body)
	if body == "" {
		return refuse("A comment needs some text")
	}
	if patchset < 1 || patchset > cl.CurrentPatchset {
		return refuse("CL %d has no patchset %d", cl.Number, patchset)
	}
	c := db.Comment{Number: cl.Number, Patchset: patchset, Author: principal, ParentID: parent, FilePath: file, Line: line, Side: side, Body: body}
	if parent != 0 {
		root, ok, err := s.db.GetComment(ctx, cl.Repo, cl.Number, parent)
		if err != nil {
			return err
		}
		if !ok || root.ParentID != 0 {
			return refuse("That thread is not on this changelist")
		}
		c.FilePath, c.Line, c.Side, c.Patchset = root.FilePath, root.Line, root.Side, root.Patchset
	} else if file == "" {
		return refuse("An inline comment names a file")
	} else if line != 0 && side != db.SideBase && side != db.SidePatchset {
		return refuse("An inline comment names a side of the diff")
	}
	if _, err := s.db.CreateComment(ctx, cl.Repo, c); err != nil {
		return err
	}
	if parent == 0 {
		return s.Refresh(ctx, cl.Repo, cl.Number)
	}
	return nil
}

// Resolve sets a thread's resolution, then re-derives the CL's approval.
func (s *Service) Resolve(ctx context.Context, cl db.ChangeList, root int64, resolved bool) error {
	c, ok, err := s.db.GetComment(ctx, cl.Repo, cl.Number, root)
	if err != nil {
		return err
	}
	if !ok || c.ParentID != 0 {
		return refuse("That thread is not on this changelist")
	}
	if _, err := s.db.SetResolved(ctx, cl.Repo, cl.Number, root, resolved); err != nil {
		return err
	}
	return s.Refresh(ctx, cl.Repo, cl.Number)
}

// DeleteComment deletes one of principal's own comments (a thread root takes its replies
// with it), then re-derives the CL's approval.
func (s *Service) DeleteComment(ctx context.Context, cl db.ChangeList, principal string, id int64) error {
	c, ok, err := s.db.GetComment(ctx, cl.Repo, cl.Number, id)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}
	if c.Author != principal {
		return refuse("Only its author can delete a comment")
	}
	if err := s.db.DeleteComment(ctx, cl.Repo, cl.Number, id); err != nil {
		return err
	}
	return s.Refresh(ctx, cl.Repo, cl.Number)
}

// Suggest builds the review dialog's suggestions for a CL from its evaluation.
func (s *Service) Suggest(ctx context.Context, cl db.ChangeList, ev *Evaluation) (*Suggestions, error) {
	rs, err := s.db.Reviewers(ctx, cl.Repo, cl.Number)
	if err != nil {
		return nil, err
	}
	return s.suggest(ctx, cl, ev.Status, ev.Files, rs)
}

// SearchReviewers finds people for the review dialog whose handle matches keyword,
// excluding the author and those in exclude.
func (s *Service) SearchReviewers(ctx context.Context, cl db.ChangeList, keyword string, exclude []string) ([]string, error) {
	skip := map[string]bool{cl.Author: true}
	for _, p := range exclude {
		skip[p] = true
	}
	return s.people.Search(ctx, keyword, skip, SearchOffered)
}

// PortThreads re-anchors the CL's unresolved threads from earlier patchsets onto the
// viewed one, by root ID.
func (s *Service) PortThreads(ctx context.Context, c *scmpb.ChangeList, viewed int32, threads []Thread) map[int64]PortedAnchor {
	var revs []revision
	var target revision
	for _, p := range c.GetPatchsets() {
		r := revision{Patchset: p.GetNumber(), Commit: scm.Hex(p.GetCommitId()), Base: scm.Hex(p.GetBaseCommitId())}
		revs = append(revs, r)
		if r.Patchset == viewed {
			target = r
		}
	}
	return portThreads(&browseDiffer{ctx: ctx, scm: s.scm, repo: c.GetRepositoryId()}, revs, target, threads)
}

// CreateRepository creates a repository on the SCM for principal, who becomes its
// creator.
func (s *Service) CreateRepository(ctx context.Context, name, principal string) (*scmpb.Repository, error) {
	r, err := s.scm.Review.CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: name, Actor: principal})
	switch status.Code(err) {
	case codes.OK:
		// The feed records it too; recording it now lists it before the feed catches up.
		return r, s.db.CreateRepository(ctx, db.Repository{ID: r.GetId(), Name: r.GetName(), CreatedBy: r.GetCreatedBy(), CreatedAt: r.GetCreatedAt().AsTime()})
	case codes.AlreadyExists:
		return nil, refuse("A repository named %s already exists", name)
	case codes.InvalidArgument:
		return nil, refuse("%s", upperFirst(status.Convert(err).Message()))
	}
	return nil, err
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// aclMembers resolves the members of every ACL the owner sets name, with ok identity. An ACL ok identity
// cannot answer for fails approval closed.
func (s *Service) aclMembers(ctx context.Context, sets []*owners.Set) (aclMembers, error) {
	out := aclMembers{}
	for _, set := range sets {
		for _, o := range set.Owners {
			if o.Kind != owners.ACL {
				continue
			}
			name := ACLName(o)
			if _, done := out[name]; done {
				continue
			}
			members, err := s.people.ACLMembers(ctx, name)
			if err != nil {
				return nil, fmt.Errorf("the members of %s: %w", name, err)
			}
			in := map[string]bool{}
			for _, m := range members {
				in[m] = true
			}
			out[name] = in
		}
	}
	return out, nil
}
