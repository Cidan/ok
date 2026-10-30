package service

import (
	"context"
	"errors"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/names"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store"
	"github.com/Cidan/ok/internal/serve/authn"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Directory is the part of ok identity's directory the SCM asks whether a handle names a person,
// or a name an ACL; identitypb.DirectoryClient is one.
type Directory interface {
	GetPrincipal(ctx context.Context, in *identitypb.GetPrincipalRequest, opts ...grpc.CallOption) (*identitypb.GetPrincipalResponse, error)
	GetSet(ctx context.Context, in *identitypb.GetSetRequest, opts ...grpc.CallOption) (*identitypb.GetSetResponse, error)
}

// people answers store.People from ok identity: a handle is known when <handle>.human.<domain>
// is a person who is not deleted.
type people struct {
	dir   Directory
	names identity.Names
}

func (p people) Human(ctx context.Context, handle string) (string, bool, error) {
	resp, err := p.dir.GetPrincipal(ctx, &identitypb.GetPrincipalRequest{Key: &identitypb.GetPrincipalRequest_Name{Name: p.names.Human(handle)}})
	if status.Code(err) == codes.NotFound {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	pr := resp.GetPrincipal()
	return pr.GetId(), pr.GetKind() == identitypb.Kind_KIND_HUMAN && pr.GetStatus() != identitypb.Status_STATUS_DELETED, nil
}

// KnownACL reports whether an OWNERS file's acl: names a set approval may come from: an ACL the
// groups-and-ACLs tree declares (eng), or an organization's team (acme/eng).
func (p people) KnownACL(ctx context.Context, name string) (bool, error) {
	full, ok := p.names.ParseSet(name)
	if !ok {
		return false, nil
	}
	resp, err := p.dir.GetSet(ctx, &identitypb.GetSetRequest{Name: full})
	if status.Code(err) == codes.NotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	set := resp.GetSet()
	return set.GetKind() == identitypb.SetKind_SET_KIND_ACL || set.GetSource() == identitypb.SetSource_SET_SOURCE_TEAM, nil
}

// ChangeLists serves uploads and reads of CLs, for people and ok's roles.
type ChangeLists struct {
	scmpb.UnimplementedChangeListsServer
	c *core
}

// Review serves ok control a CL's lifecycle and rollbacks, for the person it authorized.
type Review struct {
	scmpb.UnimplementedReviewServer
	c *core
}

// Landing serves ok automation's landing queue — candidates and landing — which moves main only
// once post passes, on behalf of the human ok control authorized.
type Landing struct {
	scmpb.UnimplementedLandingServer
	c *core
}

// LandingChecks dry-runs a landing, for ok control's Submit button and the landing queue.
type LandingChecks struct {
	scmpb.UnimplementedLandingChecksServer
	c *core
}

// changeListOut is a CL as the protocol carries it: its principals by stable ID, and by name as
// people see them now.
func (k *core) changeListOut(ctx context.Context, c store.ChangeList) (*scmpb.ChangeList, error) {
	author, err := k.names.Name(ctx, c.Author)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "naming CL %d's author: %v", c.Number, err)
	}
	submittedBy, err := k.names.Name(ctx, c.SubmittedBy)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "naming CL %d's submitter: %v", c.Number, err)
	}
	out := &scmpb.ChangeList{
		RepositoryId:    c.Repository,
		Number:          c.Number,
		ChangeId:        c.ChangeID[:],
		Author:          author,
		AuthorId:        c.Author,
		Status:          scmpb.ChangeListStatus(c.Status),
		AbandonedFrom:   scmpb.ChangeListStatus(c.AbandonedFrom),
		ParentNumber:    c.ParentNumber,
		CurrentPatchset: c.CurrentPatchset,
		SubmittedBy:     submittedBy,
		SubmittedById:   c.SubmittedBy,
		CreatedAt:       timestamppb.New(c.CreatedAt),
		UpdatedAt:       timestamppb.New(c.UpdatedAt),
		Description:     c.Description,
	}
	if c.LandedCommit != nil {
		out.LandedCommit = c.LandedCommit[:]
	}
	if c.Rollback != nil {
		out.Rollback = &scmpb.Rollback{ChangeLists: c.Rollback.CLs, Paths: c.Rollback.Paths, Left: c.Rollback.Left}
	}
	for _, ps := range c.Patchsets {
		uploadedBy, err := k.names.Name(ctx, ps.UploadedBy)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "naming CL %d's uploader: %v", c.Number, err)
		}
		out.Patchsets = append(out.Patchsets, &scmpb.Patchset{
			Number: ps.Number, CommitId: ps.Commit[:], BaseCommitId: ps.Base[:],
			UploadedBy: uploadedBy, UploadedById: ps.UploadedBy, UploadedAt: timestamppb.New(ps.UploadedAt),
		})
	}
	return out, nil
}

func (s *ChangeLists) Upload(ctx context.Context, req *scmpb.UploadRequest) (*scmpb.UploadResponse, error) {
	principal, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	commits, err := idsIn(req.GetCommitIds(), "commit_ids")
	if err != nil {
		return nil, err
	}
	var readiness store.Readiness
	switch req.GetReadiness() {
	case scmpb.UploadRequest_READINESS_UNCHANGED:
	case scmpb.UploadRequest_READINESS_READY:
		readiness = store.Ready
	case scmpb.UploadRequest_READINESS_WIP:
		readiness = store.WIP
	default:
		return nil, status.Errorf(codes.InvalidArgument, "readiness %d", req.GetReadiness())
	}
	if len(req.GetReviewers()) > maxReviewers {
		return nil, status.Errorf(codes.InvalidArgument, "reviewers: %d (at most %d)", len(req.GetReviewers()), maxReviewers)
	}
	results, err := s.c.store.Upload(ctx, req.GetRepositoryId(), principal, commits, readiness, req.GetReviewers(), s.c.people)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.UploadResponse{}
	for _, r := range results {
		out.Results = append(out.Results, &scmpb.UploadResponse_Result{
			CommitId: r.Commit[:], Number: r.Number, Patchset: r.Patchset,
			Outcome: scmpb.UploadResponse_Result_Outcome(r.Outcome),
		})
	}
	return out, nil
}

func (s *ChangeLists) GetChangeList(ctx context.Context, req *scmpb.GetChangeListRequest) (*scmpb.ChangeList, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	c, err := s.c.store.GetChangeList(ctx, req.GetRepositoryId(), req.GetNumber())
	if err != nil {
		return nil, toStatus(err)
	}
	return s.c.changeListOut(ctx, c)
}

func (s *ChangeLists) ListChangeLists(ctx context.Context, req *scmpb.ListChangeListsRequest) (*scmpb.ListChangeListsResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if req.GetPageSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "page_size is negative")
	}
	var before int64
	if t := req.GetPageToken(); t != "" {
		n, err := strconv.ParseInt(t, 10, 64)
		if err != nil || n <= 0 {
			return nil, status.Error(codes.InvalidArgument, "page_token is not one this service returned")
		}
		before = n
	}
	var statuses []store.Status
	for _, st := range req.GetStatuses() {
		if st < scmpb.ChangeListStatus_CHANGE_LIST_STATUS_DRAFT || st > scmpb.ChangeListStatus_CHANGE_LIST_STATUS_ABANDONED {
			return nil, status.Errorf(codes.InvalidArgument, "status %d", st)
		}
		statuses = append(statuses, store.Status(st))
	}
	var author string
	if req.GetAuthor() != "" {
		id, err := s.c.names.ID(ctx, req.GetAuthor())
		switch {
		case errors.Is(err, names.ErrUnknown):
			return &scmpb.ListChangeListsResponse{}, nil // no one by that name has CLs
		case err != nil:
			return nil, status.Errorf(codes.Unavailable, "resolving %s: %v", req.GetAuthor(), err)
		}
		author = id
	}
	if req.GetParentNumber() < 0 {
		return nil, status.Error(codes.InvalidArgument, "parent_number is negative")
	}
	cls, next, err := s.c.store.ListChangeLists(ctx, req.GetRepositoryId(), statuses, author, req.GetParentNumber(), int(req.GetPageSize()), before)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.ListChangeListsResponse{}
	if next != 0 {
		out.NextPageToken = strconv.FormatInt(next, 10)
	}
	for _, c := range cls {
		cl, err := s.c.changeListOut(ctx, c)
		if err != nil {
			return nil, err
		}
		out.ChangeLists = append(out.ChangeLists, cl)
	}
	return out, nil
}

func (s *ChangeLists) LookupChanges(ctx context.Context, req *scmpb.LookupChangesRequest) (*scmpb.LookupChangesResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if len(req.GetChangeIds()) > maxRefs {
		return nil, status.Errorf(codes.InvalidArgument, "change_ids: %d (at most %d)", len(req.GetChangeIds()), maxRefs)
	}
	changes := make([]encoding.ChangeID, len(req.GetChangeIds()))
	for i, b := range req.GetChangeIds() {
		if len(b) != encoding.ChangeIDLen {
			return nil, status.Errorf(codes.InvalidArgument, "change_ids: a change ID is %d bytes, not %d", encoding.ChangeIDLen, len(b))
		}
		changes[i] = encoding.ChangeID(b)
	}
	cls, err := s.c.store.LookupChanges(ctx, req.GetRepositoryId(), changes)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.LookupChangesResponse{}
	for _, c := range cls {
		cl, err := s.c.changeListOut(ctx, c)
		if err != nil {
			return nil, err
		}
		out.ChangeLists = append(out.ChangeLists, cl)
	}
	return out, nil
}

// CreateRepository creates a repository for the person actor_id names.
func (s *Review) CreateRepository(ctx context.Context, req *scmpb.CreateRepositoryRequest) (*scmpb.Repository, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if req.GetActorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "name the person ok control creates the repository for")
	}
	return s.c.create(ctx, req.GetActorId(), req.GetName())
}

// ForkRepository makes a fork for the person actor_id names.
func (s *Review) ForkRepository(ctx context.Context, req *scmpb.ForkRepositoryRequest) (*scmpb.Repository, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if req.GetActorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "name the person ok control forks the repository for")
	}
	return s.c.fork(ctx, req.GetActorId(), nil, req.GetParent(), req.GetName())
}

// SyncFork syncs a fork for the person actor_id names, whom ok control found may write to it.
func (s *Review) SyncFork(ctx context.Context, req *scmpb.SyncForkRequest) (*scmpb.SyncForkResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if req.GetActorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "name the person ok control syncs the fork for")
	}
	return s.c.syncFork(ctx, req.GetActorId(), nil, req.GetRepositoryId())
}

func (s *Review) Transition(ctx context.Context, req *scmpb.TransitionRequest) (*scmpb.TransitionResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if req.GetActorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "name the actor ok control authorized")
	}
	kind := store.TransitionKind(req.GetKind())
	if kind < store.RequestReview || kind > store.Restore {
		return nil, status.Errorf(codes.InvalidArgument, "transition %d", req.GetKind())
	}
	c, ev, err := s.c.store.Transition(ctx, req.GetRepositoryId(), req.GetNumber(), kind, req.GetActorId(), req.GetReason())
	if err != nil {
		return nil, toStatus(err)
	}
	cl, err := s.c.changeListOut(ctx, c)
	if err != nil {
		return nil, err
	}
	return &scmpb.TransitionResponse{ChangeList: cl, RepositorySeq: ev.GetRepositorySeq()}, nil
}

func (s *Review) Rollback(ctx context.Context, req *scmpb.RollbackRequest) (*scmpb.RollbackResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if req.GetActorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "name the actor ok control authorized")
	}
	if len(req.GetReviewerIds()) > maxReviewers {
		return nil, status.Errorf(codes.InvalidArgument, "reviewer_ids: %d (at most %d)", len(req.GetReviewerIds()), maxReviewers)
	}
	r, err := s.c.store.Rollback(ctx, req.GetRepositoryId(), req.GetNumber(), req.GetActorId(), req.GetReason(), req.GetReviewerIds())
	if err != nil {
		return nil, toStatus(err)
	}
	cl, err := s.c.changeListOut(ctx, *r.ChangeList)
	if err != nil {
		return nil, err
	}
	return &scmpb.RollbackResponse{ChangeList: cl, RepositorySeq: r.Event.GetRepositorySeq()}, nil
}

func (s *Landing) PrepareCandidate(ctx context.Context, req *scmpb.PrepareCandidateRequest) (*scmpb.Candidate, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if req.GetActorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "name the actor ok control authorized")
	}
	base, err := idIn(req.GetBase(), "base")
	if err != nil {
		return nil, err
	}
	c, err := s.c.store.PrepareCandidate(ctx, req.GetRepositoryId(), req.GetNumber(), req.GetPatchset(), base, req.GetActorId())
	if err != nil {
		return nil, toStatus(err)
	}
	return s.c.candidateOut(ctx, c)
}

func (k *core) candidateOut(ctx context.Context, c store.Candidate) (*scmpb.Candidate, error) {
	by, err := k.names.Name(ctx, c.PreparedBy)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "naming a candidate's preparer: %v", err)
	}
	return &scmpb.Candidate{CommitId: c.Commit[:], Base: c.Base[:], Number: c.Number, Patchset: c.Patchset,
		PreparedBy: by, PreparedById: c.PreparedBy, PreparedAt: timestamppb.New(c.PreparedAt)}, nil
}

func (s *Landing) ListCandidates(ctx context.Context, req *scmpb.ListCandidatesRequest) (*scmpb.ListCandidatesResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	cs, err := s.c.store.ListCandidates(ctx, req.GetRepositoryId())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.ListCandidatesResponse{}
	for _, c := range cs {
		cand, err := s.c.candidateOut(ctx, c)
		if err != nil {
			return nil, err
		}
		out.Candidates = append(out.Candidates, cand)
	}
	return out, nil
}

func (s *Landing) DiscardCandidate(ctx context.Context, req *scmpb.DiscardCandidateRequest) (*scmpb.DiscardCandidateResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	id, err := idIn(req.GetCommitId(), "commit_id")
	if err != nil {
		return nil, err
	}
	if err := s.c.store.DiscardCandidate(ctx, req.GetRepositoryId(), id); err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.DiscardCandidateResponse{}, nil
}

func (s *Landing) Land(ctx context.Context, req *scmpb.LandRequest) (*scmpb.LandResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	return s.c.land(ctx, req)
}

// DryRunLand is Land with dry_run set, whatever the request says.
func (s *LandingChecks) DryRunLand(ctx context.Context, req *scmpb.LandRequest) (*scmpb.LandResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	dry := proto.Clone(req).(*scmpb.LandRequest)
	dry.DryRun = true
	return s.c.land(ctx, dry)
}

// land validates and runs a landing, or its dry run.
func (s *core) land(ctx context.Context, req *scmpb.LandRequest) (*scmpb.LandResponse, error) {
	if req.GetActorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "name the submitter ok control authorized")
	}
	expected, err := idIn(req.GetExpectedHead(), "expected_head")
	if err != nil {
		return nil, err
	}
	if len(req.GetItems()) > maxLandItems {
		return nil, status.Errorf(codes.InvalidArgument, "items: %d (at most %d)", len(req.GetItems()), maxLandItems)
	}
	items := make([]store.LandItem, len(req.GetItems()))
	for i, it := range req.GetItems() {
		items[i] = store.LandItem{Number: it.GetNumber(), Patchset: it.GetPatchset(), Actor: it.GetActorId()}
		if len(it.GetCandidate()) > 0 {
			id, err := idIn(it.GetCandidate(), "items.candidate")
			if err != nil {
				return nil, err
			}
			items[i].Candidate = &id
		}
	}
	landed, head, last, err := s.store.Land(ctx, req.GetRepositoryId(), items, expected, req.GetActorId(), req.GetDryRun())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.LandResponse{Head: head[:], RepositorySeq: last.GetRepositorySeq()}
	for _, l := range landed {
		out.Landed = append(out.Landed, &scmpb.LandResponse_Landed{Number: l.Number, CommitId: l.Commit[:]})
	}
	return out, nil
}

// maxLandItems bounds the CLs one landing names, and maxReviewers the reviewers an upload or
// a rollback assigns.
const (
	maxLandItems = 1000
	maxReviewers = 100
)

// Events serves the change feed to ok control, ok automation, and ok's deployer.
type Events struct {
	scmpb.UnimplementedEventsServer
	c *core
}

// Follow records the calling role as one of the events' followers.
func (s *Events) Follow(ctx context.Context, _ *scmpb.FollowRequest) (*scmpb.FollowResponse, error) {
	c, ok := authn.FromContext(ctx)
	if !ok || c.Role == okpb.Caller_CALLER_UNSPECIFIED {
		return nil, status.Error(codes.PermissionDenied, "only ok's roles follow the SCM's events")
	}
	if err := s.c.store.AddFollower(ctx, follow.Name(c.Role)); err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.FollowResponse{}, nil
}

func (s *Events) Heads(ctx context.Context, req *scmpb.HeadsRequest) (*scmpb.HeadsResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if req.GetLimit() < 0 {
		return nil, status.Error(codes.InvalidArgument, "limit is negative")
	}
	heads, err := s.c.store.LogHeads(ctx, req.GetAfterRepositoryId(), int(req.GetLimit()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &scmpb.HeadsResponse{Heads: make([]*scmpb.HeadsResponse_Head, len(heads))}
	for i, h := range heads {
		out.Heads[i] = &scmpb.HeadsResponse_Head{RepositoryId: h.Repository, RepositorySeq: h.Seq}
	}
	return out, nil
}

func (s *Events) Read(ctx context.Context, req *scmpb.ReadEventsRequest) (*scmpb.ReadEventsResponse, error) {
	if _, err := caller(ctx); err != nil {
		return nil, err
	}
	if req.GetLimit() < 0 {
		return nil, status.Error(codes.InvalidArgument, "limit is negative")
	}
	evs, err := s.c.store.ReadEvents(ctx, req.GetRepositoryId(), req.GetAfterSeq(), int(req.GetLimit()))
	if err != nil {
		return nil, toStatus(err)
	}
	return &scmpb.ReadEventsResponse{Events: evs}, nil
}
