package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/owners"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/workflow"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// People answers whether a handle names a person: ok identity's directory.
type People interface {
	// Human is the stable ID of the person with a handle; ok is false when no such person stands.
	Human(ctx context.Context, handle string) (id string, ok bool, err error)
	// KnownACL reports whether <name>.acl.<domain> is an ACL (docs/identity.md).
	KnownACL(ctx context.Context, name string) (bool, error)
}

// Readiness is what an upload does to the status of the CLs it creates or updates.
type Readiness int

const (
	ReadinessUnchanged Readiness = 0
	// Ready sends drafts for review (draft → open); a new CL starts open.
	Ready Readiness = 1
	// WIP returns open CLs to draft.
	WIP Readiness = 2
)

// UploadReason is why an upload was refused. The values are the protocol's.
type UploadReason int

const (
	ReasonMergeCommit     UploadReason = 1
	ReasonConflicted      UploadReason = 2
	ReasonEmptyDesc       UploadReason = 3
	ReasonEmptyChange     UploadReason = 4
	ReasonUnknownBase     UploadReason = 5
	ReasonStaleBase       UploadReason = 6
	ReasonNotAuthor       UploadReason = 7
	ReasonClosed          UploadReason = 8
	ReasonDivergentChange UploadReason = 9
	ReasonInvalidOwners   UploadReason = 10
	ReasonUnknownReviewer UploadReason = 11
	ReasonNoNewChanges    UploadReason = 12
	ReasonInvalidWorkflow UploadReason = 13
	ReasonInvalidSecrets  UploadReason = 14
)

// UploadRejection refuses an upload: which commit, why, and what to run to fix it.
type UploadRejection struct {
	Commit   encoding.ID
	Reason   UploadReason
	Message  string
	Number   int64    // the CL concerned, when there is one
	Problems []string // every problem, for an invalid OWNERS or workflow file, or unknown reviewers
}

func (r *UploadRejection) Error() string { return "upload rejected: " + r.Message }

// UploadOutcome is what an upload did with one commit.
type UploadOutcome int

const (
	Created     UploadOutcome = 1
	NewPatchset UploadOutcome = 2
	Unchanged   UploadOutcome = 3
)

// UploadResult is one commit's outcome.
type UploadResult struct {
	Commit   encoding.ID
	Number   int64
	Patchset int32
	Outcome  UploadOutcome
}

// maxUpload bounds how many commits one upload carries.
const maxUpload = 1000

// maxConfigFile bounds the configuration files — OWNERS and workflow files — upload
// validation reads.
const maxConfigFile = 1 << 20

// errRace aborts a transaction that lost a race it can simply run again: a CL another
// upload created for the same change after this one looked, or an unreferenced object a
// landing or a candidate reuses that garbage collection swept after the transaction
// found it.
var errRace = errors.New("lost a race with a concurrent transaction")

// uploadCommit is one commit of an upload with what its static checks found.
type uploadCommit struct {
	id         encoding.ID
	c          *encoding.Commit
	parent     encoding.ID
	parentTree encoding.ID
	rule1      *UploadRejection // shape: one parent, resolved, described, not empty
	rule4      *UploadRejection // at most one commit per change
	rule5      *UploadRejection // valid configuration files, and //secret
}

// handleOf is a person's handle; other principals are returned whole.
// handleOf is a person's handle, by stable ID, for a message; their ID when they cannot be named.
func (s *Store) handleOf(ctx context.Context, id string) string {
	name, err := s.nameOf(ctx, id)
	if err != nil {
		return id
	}
	return strings.TrimSuffix(name, s.names.Human(""))
}

// Upload creates and updates CLs from a stack of stored commits, bottom first
// (docs/scm.md, "Upload"), on behalf of principal. It validates the whole stack
// before writing anything; one bad commit refuses the upload with an *UploadRejection.
// Otherwise one transaction creates the CLs and patchsets, stacks them, applies readiness,
// pins the new patchsets, and emits the events.
func (s *Store) Upload(ctx context.Context, repo int64, principal string, commits []encoding.ID, readiness Readiness, reviewers []string, people People) ([]UploadResult, error) {
	if len(commits) == 0 {
		return nil, invalid("an upload names at least one commit")
	}
	if len(commits) > maxUpload {
		return nil, invalid("an upload names at most %d commits, not %d", maxUpload, len(commits))
	}
	if err := s.repositoryExists(ctx, s.ops(ctx), repo); err != nil {
		return nil, err
	}
	staged, err := s.checkUpload(ctx, repo, principal, commits, people)
	if err != nil {
		return nil, err
	}
	assign, rej, err := s.resolveReviewers(ctx, reviewers, people)
	if err != nil {
		return nil, err
	}
	var results []UploadResult
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		var err error
		results, err = s.applyUpload(ctx, tx, repo, principal, staged, readiness, assign, rej)
		return err
	})
	return results, err
}

// checkUpload loads the commits and runs the checks that depend only on them, and on main as
// ok secrets reads it: rules 1, 4, and 5. The objects are immutable, so this needs no
// transaction.
func (s *Store) checkUpload(ctx context.Context, repo int64, principal string, commits []encoding.ID, people People) ([]*uploadCommit, error) {
	ts := s.pendingTrees(ctx, repo)
	seenCommit := map[encoding.ID]bool{}
	seenChange := map[encoding.ChangeID]encoding.ID{}
	known := &knownHumans{people: people, seen: map[string]bool{}}
	var out []*uploadCommit
	for _, id := range commits {
		if id == encoding.RootCommitID {
			return nil, invalid("the root commit cannot be uploaded")
		}
		if seenCommit[id] {
			return nil, invalid("commit %s is named twice", id)
		}
		seenCommit[id] = true
		c, err := s.readCommit(ctx, repo, id)
		if errors.Is(err, ErrNotFound) {
			return nil, notFound("commit %s is not on ok; publish it before uploading", id)
		}
		if err != nil {
			return nil, err
		}
		u := &uploadCommit{id: id, c: c}
		reject := func(reason UploadReason, format string, args ...any) *UploadRejection {
			return &UploadRejection{Commit: id, Reason: reason, Message: fmt.Sprintf(format, args...)}
		}
		if len(c.Parents) == 1 {
			u.parent = c.Parents[0]
			u.parentTree = encoding.EmptyTreeID
			if u.parent != encoding.RootCommitID {
				pc, err := s.readCommit(ctx, repo, u.parent)
				if err != nil {
					return nil, err
				}
				u.parentTree = pc.RootTree[0]
				if len(pc.RootTree) > 1 {
					u.parentTree = encoding.ID{} // conflicted: no tree to compare with
				}
			}
		}
		switch {
		case len(c.Parents) != 1:
			u.rule1 = reject(ReasonMergeCommit, "it is a merge commit; a CL is one change on one parent (rebase it onto a single parent)")
		case len(c.RootTree) != 1:
			u.rule1 = reject(ReasonConflicted, "it has unresolved conflicts (resolve them with jj resolve, then upload again)")
		case strings.TrimSpace(c.Description) == "":
			u.rule1 = reject(ReasonEmptyDesc, "it has no description (describe it with jj describe)")
		case c.RootTree[0] == u.parentTree:
			u.rule1 = reject(ReasonEmptyChange, "it changes nothing (abandon it with jj abandon, or upload its parent)")
		}
		if prev, ok := seenChange[c.ChangeID]; ok {
			u.rule4 = reject(ReasonDivergentChange, "change %s is uploaded twice, as %s and %s: the change is divergent (keep one with jj abandon)", c.ChangeID, prev, id)
		} else {
			seenChange[c.ChangeID] = id
		}
		if u.rule1 == nil {
			problems, err := s.validateOwnersEdits(ctx, repo, ts, u.parentTree, c.RootTree[0], known)
			if err != nil {
				return nil, err
			}
			if len(problems) > 0 {
				u.rule5 = reject(ReasonInvalidOwners, "the OWNERS files it edits are invalid (fix every problem listed, then upload again)")
				u.rule5.Problems = problems
			}
		}
		if u.rule1 == nil && u.rule5 == nil {
			problems, err := s.validateWorkflowEdits(ctx, repo, ts, u.parentTree, c.RootTree[0])
			if err != nil {
				return nil, err
			}
			if len(problems) > 0 {
				u.rule5 = reject(ReasonInvalidWorkflow, "the workflow files it edits are invalid (fix every problem listed, then upload again)")
				u.rule5.Problems = problems
			}
		}
		if u.rule1 == nil && u.rule5 == nil {
			problems, err := s.validateSecretEdits(ctx, repo, ts, u.parentTree, c.RootTree[0], principal, known)
			if err != nil {
				return nil, err
			}
			stanzas, err := s.validateSecretStanzas(ctx, repo, ts, u.parentTree, c.RootTree[0])
			if err != nil {
				return nil, err
			}
			if problems = append(problems, stanzas...); len(problems) > 0 {
				u.rule5 = reject(ReasonInvalidSecrets, "its //secret breaks the rules (fix every problem listed, then upload again; docs/secrets.md)")
				u.rule5.Problems = problems
			}
		}
		out = append(out, u)
	}
	return out, nil
}

// knownHumans asks People about each handle, and each ACL, once per upload.
type knownHumans struct {
	people People
	seen   map[string]bool
	ids    map[string]string // a known person's stable ID, by handle
	acls   map[string]bool
	sets   map[string]bool // by full name
	err    error
}

func (k *knownHumans) knownACL(ctx context.Context, name string) bool {
	if ok, done := k.acls[name]; done {
		return ok
	}
	if k.acls == nil {
		k.acls = map[string]bool{}
	}
	if !identity.ValidLabel(name) {
		k.acls[name] = false
		return false
	}
	ok, err := k.people.KnownACL(ctx, name)
	if err != nil {
		if k.err == nil {
			k.err = err
		}
		return false
	}
	k.acls[name] = ok
	return ok
}

func (k *knownHumans) known(ctx context.Context, handle string) bool {
	_, ok := k.id(ctx, handle)
	return ok
}

// id is the stable ID of the person with a handle — ada, or @ada as a hosted service writes it —
// and whether there is one.
func (k *knownHumans) id(ctx context.Context, handle string) (string, bool) {
	handle = strings.TrimPrefix(handle, "@")
	if ok, done := k.seen[handle]; done {
		return k.ids[handle], ok
	}
	if !identity.ValidLabel(handle) {
		k.seen[handle] = false
		return "", false
	}
	id, ok, err := k.people.Human(ctx, handle)
	if err != nil {
		if k.err == nil {
			k.err = err
		}
		return "", false
	}
	if k.ids == nil {
		k.ids = map[string]string{}
	}
	k.seen[handle], k.ids[handle] = ok, id
	return id, ok
}

// validateOwnersEdits validates every OWNERS-config file the change from parentTree to
// tree adds or edits (docs/owners.md, "Validation at upload"), against tree.
func (s *Store) validateOwnersEdits(ctx context.Context, repo int64, ts treeStore, parentTree, tree encoding.ID, known *knownHumans) ([]string, error) {
	from := parentTree
	if from == (encoding.ID{}) {
		from = encoding.EmptyTreeID
	}
	var edited []leafChange
	if err := diffTrees(ts, from, tree, "", func(ch leafChange) error {
		if ch.After != nil && ch.After.Kind == encoding.TreeValueFile && owners.IsOwnersFile(ch.Path) {
			edited = append(edited, ch)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	src := &treeSource{ctx: ctx, s: s, repo: repo, ts: ts, root: tree}
	var problems []string
	for _, ch := range edited {
		data, err := s.readSmallFile(ctx, repo, ch.After.ID, maxConfigFile)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", ch.Path, err))
			continue
		}
		for _, e := range owners.Validate(ch.Path, data, src, func(h string) bool { return known.known(ctx, h) }, func(a string) bool { return known.knownACL(ctx, a) }) {
			problems = append(problems, e.Error())
		}
		if known.err != nil {
			return nil, fmt.Errorf("check OWNERS users and ACLs: %w", known.err)
		}
	}
	return problems, nil
}

// validateWorkflowEdits validates every workflow file the change from parentTree to tree adds
// or edits (docs/automation.md, "Trust"), against tree: what can be checked without
// the build graph, which pre checks. The repository's settings (.ok/repository.yaml) must parse,
// so that main's always say where the groups-and-ACLs tree is.
func (s *Store) validateWorkflowEdits(ctx context.Context, repo int64, ts treeStore, parentTree, tree encoding.ID) ([]string, error) {
	from := parentTree
	if from == (encoding.ID{}) {
		from = encoding.EmptyTreeID
	}
	var edited []leafChange
	if err := diffTrees(ts, from, tree, "", func(ch leafChange) error {
		if ch.After != nil && (workflow.IsWorkflowFile(ch.Path) || ch.Path == workflow.SettingsFile) {
			edited = append(edited, ch)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	src := &treeSource{ctx: ctx, s: s, repo: repo, ts: ts, root: tree}
	var problems []string
	for _, ch := range edited {
		if ch.After.Kind != encoding.TreeValueFile {
			problems = append(problems, ch.Path+": a workflow file must be a regular file")
			continue
		}
		data, err := s.readSmallFile(ctx, repo, ch.After.ID, maxConfigFile)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", ch.Path, err))
			continue
		}
		if ch.Path == workflow.SettingsFile {
			if _, err := workflow.ParseSettings(data, true); err != nil {
				problems = append(problems, err.Error())
			}
			continue
		}
		for _, e := range workflow.Validate(ch.Path, data, src) {
			problems = append(problems, e.Error())
		}
	}
	return problems, nil
}

// treeSource serves files from a tree to the OWNERS resolver and the workflow validator.
type treeSource struct {
	ctx  context.Context
	s    *Store
	repo int64
	ts   treeStore
	root encoding.ID
}

func (t *treeSource) Read(path string) ([]byte, bool, error) {
	v, err := lookupPath(t.ts, t.root, path)
	if err != nil || v == nil || v.Kind != encoding.TreeValueFile {
		return nil, false, err
	}
	data, err := t.s.readSmallFile(t.ctx, t.repo, v.ID, maxConfigFile)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

// readSmallFile reads a whole file no larger than max bytes.
func (s *Store) readSmallFile(ctx context.Context, repo int64, id encoding.ID, max uint64) ([]byte, error) {
	size, _, found, err := s.fileChunks(ctx, repo, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, notFound("file %s", id)
	}
	if size > max {
		return nil, fmt.Errorf("%d bytes is larger than the %d this check reads", size, max)
	}
	return s.readFile(ctx, repo, id)
}

// resolveReviewers maps reviewers — handles, @handles, or <handle>@<email domain> — to principals' stable
// IDs, in order without repeats. Unknown ones make a rejection, which applyUpload reports after
// the per-commit rules.
func (s *Store) resolveReviewers(ctx context.Context, reviewers []string, people People) ([]string, *UploadRejection, error) {
	known := &knownHumans{people: people, seen: map[string]bool{}}
	var out, unknown []string
	seen := map[string]bool{}
	for _, r := range reviewers {
		handle := strings.TrimSuffix(strings.TrimSpace(r), "@"+s.email)
		id, ok := known.id(ctx, handle)
		if !ok {
			if known.err != nil {
				return nil, nil, fmt.Errorf("check reviewers: %w", known.err)
			}
			unknown = append(unknown, r)
			continue
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	if len(unknown) > 0 {
		return nil, &UploadRejection{
			Reason:   ReasonUnknownReviewer,
			Message:  "a reviewer is not a person ok knows (name reviewers by handle, @handle, or <handle>@" + s.email + ")",
			Problems: unknown,
		}, nil
	}
	return out, nil, nil
}

// plannedCL is what applyUpload does to one CL.
type plannedCL struct {
	u         *uploadCommit
	existing  *ChangeList // nil: created
	number    int64       // assigned at write time for a new CL
	patchset  int32
	status    Status
	parentIdx int   // index of the stack parent's commit in the upload, or -1
	parentCL  int64 // the stack parent's CL when it is not in the upload, or 0
}

func (s *Store) applyUpload(ctx context.Context, tx *txn, repo int64, principal string, staged []*uploadCommit, readiness Readiness, assign []string, reviewerRejection *UploadRejection) ([]UploadResult, error) {
	// Find and lock every CL the upload can touch — those of its changes, and those whose
	// patchsets are its commits' parents — in number order.
	changeKeys := make([][]byte, len(staged))
	parentKeys := make([][]byte, 0, len(staged))
	for i, u := range staged {
		changeKeys[i] = u.c.ChangeID[:]
		parentKeys = append(parentKeys, u.parent[:])
	}
	numbers, err := tx.UploadChangeLists(ctx, repo, changeKeys, parentKeys)
	if err != nil {
		return nil, err
	}
	// Landed CLs in segments: an upload of their changes, or stacked on their patchsets, is
	// refused below, as it is of those in the tail.
	ofChanges, err := s.settledByChange(ctx, repo, changeKeys)
	if err != nil {
		return nil, err
	}
	prefixes := make([][]byte, len(parentKeys))
	for i, p := range parentKeys {
		prefixes[i] = clKey(clByCommit, p)
	}
	ofParents, err := s.settledByPrefix(ctx, repo, prefixes)
	if err != nil {
		return nil, err
	}
	numbers = append(append(numbers, ofChanges...), ofParents...)
	locked, err := s.lockChangeLists(ctx, tx, repo, numbers)
	if err != nil {
		return nil, err
	}
	byChange := map[encoding.ChangeID]*ChangeList{}
	byPatchset := map[encoding.ID]*ChangeList{}
	for _, c := range locked {
		byChange[c.ChangeID] = c
		for _, ps := range c.Patchsets {
			byPatchset[ps.Commit] = c
		}
	}

	index := map[encoding.ID]int{}
	plans := make([]*plannedCL, len(staged))
	results := make([]UploadResult, len(staged))
	for i, u := range staged {
		reject := func(reason UploadReason, number int64, format string, args ...any) error {
			return &UploadRejection{Commit: u.id, Reason: reason, Number: number, Message: fmt.Sprintf(format, args...)}
		}
		if u.rule1 != nil {
			return nil, u.rule1
		}
		// Rule 2: the parent is known.
		p := &plannedCL{u: u, parentIdx: -1}
		if j, ok := index[u.parent]; ok {
			p.parentIdx = j
		} else if main, err := s.onMain(ctx, tx, repo, u.parent); err != nil {
			return nil, err
		} else if !main {
			c, ok := byPatchset[u.parent]
			switch {
			case !ok:
				return nil, reject(ReasonUnknownBase, 0, "its parent %s is neither on main nor a CL's current patchset (upload the parent with it, or rebase onto main with jj rebase -d 'trunk()')", u.parent)
			case c.Status == StatusSubmitted:
				return nil, reject(ReasonStaleBase, c.Number, "it is stacked on CL %d, which has landed (run jj sync to rebase it onto main)", c.Number)
			case c.Current().Commit != u.parent:
				return nil, reject(ReasonStaleBase, c.Number, "it is stacked on an older patchset of CL %d, which is now at patchset %d (run jj sync)", c.Number, c.CurrentPatchset)
			case c.Status == StatusAbandoned:
				return nil, reject(ReasonUnknownBase, c.Number, "it is stacked on CL %d, which is abandoned (restore CL %d, or rebase onto main with jj rebase -d 'trunk()')", c.Number, c.Number)
			}
			p.parentCL = c.Number
		}
		// Rule 3: the CL.
		c := byChange[u.c.ChangeID]
		switch {
		case c == nil:
			p.patchset = 1
			p.status = StatusDraft
			if readiness == Ready {
				p.status = StatusOpen
			}
			results[i] = UploadResult{Commit: u.id, Patchset: 1, Outcome: Created}
		case c.Current().Commit == u.id:
			p.existing = c
			results[i] = UploadResult{Commit: u.id, Number: c.Number, Patchset: c.CurrentPatchset, Outcome: Unchanged}
		case c.Author != principal:
			return nil, reject(ReasonNotAuthor, c.Number, "CL %d is %s's, and only its author uploads patchsets (to propose these edits yourself, jj duplicate the change and upload the copy)", c.Number, s.handleOf(ctx, c.Author))
		case c.Status == StatusSubmitted:
			return nil, reject(ReasonClosed, c.Number, "CL %d has landed and takes no new patchsets (run jj sync)", c.Number)
		case c.Status == StatusAbandoned:
			return nil, reject(ReasonClosed, c.Number, "CL %d is abandoned (restore it first, or drop the change with jj abandon)", c.Number)
		default:
			p.existing = c
			p.patchset = int32(len(c.Patchsets)) + 1
			p.status = c.Status
			switch {
			case readiness == Ready && c.Status == StatusDraft:
				p.status = StatusOpen
			case readiness == WIP && c.Status == StatusOpen:
				p.status = StatusDraft
			}
			results[i] = UploadResult{Commit: u.id, Number: c.Number, Patchset: p.patchset, Outcome: NewPatchset}
		}
		if u.rule4 != nil {
			return nil, u.rule4
		}
		if u.rule5 != nil {
			return nil, u.rule5
		}
		index[u.id] = i
		plans[i] = p
	}
	if reviewerRejection != nil {
		return nil, reviewerRejection
	}
	var roots []ref
	var created int64
	for i, p := range plans {
		switch results[i].Outcome {
		case Created:
			created++
			roots = append(roots, ref{table: tCommit, id: p.u.id})
		case NewPatchset:
			roots = append(roots, ref{table: tCommit, id: p.u.id})
		}
	}
	if len(roots) == 0 {
		last := staged[len(staged)-1]
		return nil, &UploadRejection{Commit: last.id, Reason: ReasonNoNewChanges, Number: results[len(results)-1].Number,
			Message: "no new changes: every change is already its CL's current patchset"}
	}
	if err := s.pin(ctx, tx, repo, roots); err != nil {
		return nil, err
	}

	// New CLs take their numbers from the repository's counter, the upload's last lock
	// before the event counter.
	if created > 0 {
		last, err := tx.NextChangeListNumbers(ctx, repo, created)
		if err != nil {
			return nil, err
		}
		next := last - created + 1
		for i, p := range plans {
			if results[i].Outcome == Created {
				p.number = next
				results[i].Number = next
				next++
			}
		}
	}
	for i, p := range plans {
		if p.existing != nil {
			p.number = p.existing.Number
		}
		if results[i].Outcome == Unchanged {
			continue
		}
		parent := p.parentCL
		if p.parentIdx >= 0 {
			parent = plans[p.parentIdx].number
		}
		var parentNumber *int64
		if parent != 0 {
			parentNumber = &parent
		}
		if results[i].Outcome == Created {
			err := tx.InsertChangeList(ctx, repo, backend.ChangeList{Number: p.number, ChangeID: p.u.c.ChangeID[:], Author: principal,
				Status: int16(p.status), Parent: parent, CurrentPatchset: 1, Description: p.u.c.Description})
			if errors.Is(err, backend.ErrDuplicate) {
				return nil, errRace
			}
			if err != nil {
				return nil, err
			}
		} else if err := tx.UpdateChangeList(ctx, repo, p.number, p.patchset, p.u.c.Description, parentNumber, int16(p.status)); err != nil {
			return nil, err
		}
		if err := tx.InsertPatchset(ctx, repo, backend.Patchset{CL: p.number, Number: p.patchset, Commit: p.u.id, Base: p.u.parent,
			Parent: parent, UploadedBy: principal}); err != nil {
			return nil, err
		}
	}

	var events []*scmpb.Event
	for i, p := range plans {
		if results[i].Outcome == Unchanged {
			continue
		}
		var to []string
		for _, r := range assign {
			author := principal
			if p.existing != nil {
				author = p.existing.Author
			}
			if r != author {
				to = append(to, r)
			}
		}
		events = append(events, &scmpb.Event{Kind: &scmpb.Event_Uploaded{Uploaded: &scmpb.ChangeListUploaded{
			Number: p.number, Patchset: p.patchset, Created: results[i].Outcome == Created,
			UploaderId: principal, ReviewerIds: to, Status: p.status.proto(),
		}}})
	}
	for i, p := range plans {
		if results[i].Outcome == NewPatchset && p.status != p.existing.Status {
			events = append(events, &scmpb.Event{Kind: &scmpb.Event_Transitioned{Transitioned: &scmpb.ChangeListTransitioned{
				Number: p.number, From: p.existing.Status.proto(), To: p.status.proto(), ActorId: principal,
			}}})
		}
	}
	if err := appendEvents(ctx, tx, repo, events); err != nil {
		return nil, err
	}
	return results, nil
}
