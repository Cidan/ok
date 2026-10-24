package load

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/scmtest"
	controlpb "github.com/Cidan/ok/proto/ok/control/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// A run with Config.SeedFiles writes as people in a monorepo do: each repository is seeded with a
// tree shaped like one, landed as a CL; a checkpoint or a publish changes files of a workspace's
// working copy — a few it keeps working on — so every directory on their paths is a new tree; a
// publish is a jj transaction in that workspace, carrying its operation and view and moving its
// operation log's head; and a CL changes files of main's tree, ones no other CL of the run
// changes, so CLs never conflict. Without SeedFiles, each write is a directory of its own.

// SeedFiles and SeedFileBytes are ok load run's seed by default: about a large monorepo's tree (a
// measured one held 67,591 files), in files of about its median size.
const (
	SeedFiles     = 67_000
	SeedFileBytes = 1500
)

// workingSet is how many of the seed's files a workspace works on.
const workingSet = 5

// clFiles is how many files a CL changes.
const clFiles = 3

// seedPaths are n files' paths in a tree grown as a monorepo's is: n/4.8 directories, each added
// under a parent drawn, from a few at random, with weight (1 + its directories)^1.4 — so a few are
// large and most small — and the files spread over them, a few at the root. A one-file change
// rewrites some 13–17 KB of trees, about 6 deep (the measured monorepo: 17 KB, 6.6 deep).
// The same n and seed give the same paths, so every pod of a run knows the seed it did not land.
func seedPaths(n int, seed uint64) []string {
	rng := mrand.New(mrand.NewPCG(seed, 0x6d6f6e6f))
	ndirs := max(1, int(float64(n)/4.8))
	parent, kids := make([]int, ndirs), make([]int, ndirs)
	parent[0] = -1
	weights := make([]float64, 24)
	cands := make([]int, 24)
	for i := 1; i < ndirs; i++ {
		total := 0.0
		for j := range cands {
			cands[j] = rng.IntN(i)
			weights[j] = pow14(1 + kids[cands[j]])
			total += weights[j]
		}
		pick, x := cands[0], rng.Float64()*total
		for j, w := range weights {
			if x < w {
				pick = cands[j]
				break
			}
			x -= w
		}
		parent[i] = pick
		kids[pick]++
	}
	names := make([]string, ndirs)
	for i := 1; i < ndirs; i++ {
		names[i] = fmt.Sprintf("dir%06x", i)
		if p := names[parent[i]]; p != "" {
			names[i] = p + "/" + names[i]
		}
	}
	out := make([]string, n)
	for i := range n {
		dir := 0
		if i >= 4 && ndirs > 1 {
			dir = 1 + rng.IntN(ndirs-1)
		}
		out[i] = fmt.Sprintf("file%06x.go", i)
		if names[dir] != "" {
			out[i] = names[dir] + "/" + out[i]
		}
	}
	return out
}

// pow14 is x^1.4.
func pow14(x int) float64 {
	f := float64(x)
	return f * root5(f*f) // x * x^0.4
}

// root5 is x^(1/5).
func root5(x float64) float64 {
	if x <= 0 {
		return 0
	}
	r := 1.0
	for range 40 {
		r -= (r*r*r*r*r - x) / (5 * r * r * r * r)
	}
	return r
}

// trees reads a repository's trees and commits by ID through the SCM and keeps them, as jj's store
// does, in two generations: past cacheLimit, the older one goes.
type trees struct {
	mu       sync.Mutex
	new, old map[encoding.ID]encoding.Object
}

const cacheLimit = 200_000

func newTrees() *trees {
	return &trees{new: map[encoding.ID]encoding.Object{}, old: map[encoding.ID]encoding.Object{}}
}

func (t *trees) put(id encoding.ID, o encoding.Object) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.new) >= cacheLimit {
		t.old, t.new = t.new, map[encoding.ID]encoding.Object{}
	}
	t.new[id] = o
}

// get is an object, read through p when it is not kept.
func (t *trees) get(ctx context.Context, p *Person, repo int64, kind encoding.Kind, id encoding.ID) (encoding.Object, error) {
	t.mu.Lock()
	o, ok := t.new[id]
	if !ok {
		if o, ok = t.old[id]; ok {
			t.new[id] = o
		}
	}
	t.mu.Unlock()
	if ok {
		return o, nil
	}
	stream, err := p.Store.ReadObjects(ctx, &scmpb.ReadObjectsRequest{RepositoryId: repo, Refs: []*scmpb.ObjectRef{{Kind: scmpb.ObjectKind(kind), Id: id[:]}}})
	if err != nil {
		return nil, err
	}
	res, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	obj := res.GetObject()
	if obj == nil {
		return nil, fmt.Errorf("the SCM has no %s %x", kind, id[:6])
	}
	o, err = encoding.Decode(append([]byte{byte(kind)}, obj.GetBody()...))
	if err != nil {
		return nil, err
	}
	t.put(id, o)
	return o, nil
}

// rootOf is a commit's tree.
func (t *trees) rootOf(ctx context.Context, p *Person, repo int64, commit []byte) (encoding.ID, error) {
	var id encoding.ID
	copy(id[:], commit)
	if id == encoding.RootCommitID {
		return encoding.EmptyTreeID, nil
	}
	o, err := t.get(ctx, p, repo, encoding.KindCommit, id)
	if err != nil {
		return encoding.ID{}, err
	}
	c, ok := o.(*encoding.Commit)
	if !ok || len(c.RootTree) != 1 {
		return encoding.ID{}, fmt.Errorf("commit %x has no single tree", id[:6])
	}
	return c.RootTree[0], nil
}

// edit stages in b the tree root with each path's file set to changes' — every directory on the
// paths a new tree, staged — and returns it.
func (t *trees) edit(ctx context.Context, p *Person, repo int64, b *scmtest.Objects, root encoding.ID, changes map[string]encoding.ID) (encoding.ID, error) {
	var entries []encoding.TreeEntry
	if root != encoding.EmptyTreeID && root != (encoding.ID{}) {
		o, err := t.get(ctx, p, repo, encoding.KindTree, root)
		if err != nil {
			return encoding.ID{}, err
		}
		entries = slices.Clone(o.(*encoding.Tree).Entries)
	}
	set := func(name string, v encoding.TreeValue) {
		i, found := slices.BinarySearchFunc(entries, name, func(e encoding.TreeEntry, n string) int { return strings.Compare(e.Name, n) })
		if found {
			entries[i].Value = v
		} else {
			entries = slices.Insert(entries, i, encoding.TreeEntry{Name: name, Value: v})
		}
	}
	sub := map[string]map[string]encoding.ID{}
	for path, f := range changes {
		first, rest, deeper := strings.Cut(path, "/")
		if !deeper {
			set(first, encoding.TreeValue{Kind: encoding.TreeValueFile, ID: f})
			continue
		}
		if sub[first] == nil {
			sub[first] = map[string]encoding.ID{}
		}
		sub[first][rest] = f
	}
	for name, ch := range sub {
		child := encoding.EmptyTreeID
		if i, found := slices.BinarySearchFunc(entries, name, func(e encoding.TreeEntry, n string) int { return strings.Compare(e.Name, n) }); found &&
			entries[i].Value.Kind == encoding.TreeValueTree {
			child = entries[i].Value.ID
		}
		id, err := t.edit(ctx, p, repo, b, child, ch)
		if err != nil {
			return encoding.ID{}, err
		}
		set(name, encoding.TreeValue{Kind: encoding.TreeValueTree, ID: id})
	}
	tree := &encoding.Tree{Entries: entries}
	id := b.Add(tree)
	t.put(id, tree)
	return id, nil
}

// fresh is size bytes of new content, in a file staged in b.
func fresh(b *scmtest.Objects, size int) encoding.ID {
	data := make([]byte, min(max(size, 1), maxFile))
	rand.Read(data)
	return b.File(data)
}

// seedBatch bounds the objects and files one of the seed's publishes sends: jj publishes in
// batches too.
const seedBatch = 4000

// seedRepo lands a repository's seeded tree, once: published a batch at a time, children before the
// trees naming them, then committed on main's root commit and landed as a CL of the run's first
// person, approved by another (themselves, when they are the run's one person), through the queue
// as any CL is. Another pod's run waits for it.
func (r *run) seedRepo(ctx context.Context, rs *repoState) error {
	p := r.people[0]
	head, err := r.freshHead(ctx, p, rs)
	if err != nil {
		return err
	}
	if !bytes.Equal(head, encoding.RootCommitID[:]) {
		return nil
	}
	if !r.cfg.SeedLands {
		for deadline := time.Now().Add(r.cfg.Drain + r.cfg.Duration); time.Now().Before(deadline); time.Sleep(2 * time.Second) {
			if head, err = r.freshHead(ctx, p, rs); err != nil || !bytes.Equal(head, encoding.RootCommitID[:]) {
				return err
			}
		}
		return fmt.Errorf("%s was never seeded", rs.Name)
	}
	if ref, err := r.seedUnderWay(ctx, p, rs); err != nil || ref != nil {
		if err != nil {
			return err
		}
		// A run stopped while it landed the seed: land that one.
		return r.landSeed(ctx, p, rs, ref)
	}
	b := scmtest.Builder()
	changes := make(map[string]encoding.ID, len(r.mono.paths))
	for _, path := range r.mono.paths {
		changes[path] = fresh(b, r.cfg.SeedFileBytes)
	}
	root, err := r.trees.edit(ctx, p, rs.ID, b, encoding.EmptyTreeID, changes)
	if err != nil {
		return err
	}
	if err := publishInBatches(ctx, p, rs.ID, b); err != nil {
		return fmt.Errorf("publishing %s's seed: %w", rs.Name, err)
	}
	cb := scmtest.Builder()
	c := commit(cb, root, seedDescription, encoding.RootCommitID[:])
	if _, _, err := cb.Publish(ctx, p.Store, rs.ID, nil); err != nil {
		return err
	}
	resp, err := p.Upload.Upload(ctx, &scmpb.UploadRequest{RepositoryId: rs.ID, CommitIds: [][]byte{c[:]}, Readiness: scmpb.UploadRequest_READINESS_READY})
	if err != nil {
		return err
	}
	if len(resp.GetResults()) != 1 {
		return fmt.Errorf("the seed's upload made %d CLs", len(resp.GetResults()))
	}
	return r.landSeed(ctx, p, rs, &controlpb.CLRef{Repository: rs.Name, Number: resp.GetResults()[0].GetNumber()})
}

// landSeed lands a seed CL: approved, submitted once its pre passed — already submitting, it is
// left so — and landed.
func (r *run) landSeed(ctx context.Context, p *Person, rs *repoState, ref *controlpb.CLRef) error {
	if _, err := r.reviewer(p).Control.Reply(ctx, &controlpb.ReplyRequest{Cl: ref, Grants: []*controlpb.ReplyRequest_Grant{
		{Label: controlpb.Label_LABEL_LGTM, Granted: true}, {Label: controlpb.Label_LABEL_APPROVAL, Granted: true}}}); err != nil {
		return err
	}
	stage, err := r.until(ctx, p, ref, controlpb.Stage_STAGE_SUBMITTABLE, controlpb.Stage_STAGE_PRE_FAILED)
	if err != nil || stage == controlpb.Stage_STAGE_PRE_FAILED {
		return errors.Join(err, fmt.Errorf("the seed of %s: its pre failed", rs.Name))
	}
	if stage == controlpb.Stage_STAGE_SUBMITTABLE {
		if _, err := p.Control.Submit(ctx, &controlpb.SubmitRequest{Cl: ref}); err != nil {
			return err
		}
	}
	return r.seedLanded(ctx, p, rs, ref)
}

// seedDescription is a seed CL's.
const seedDescription = "the load's seed: a monorepo's tree"

// seedUnderWay is a seed CL of an earlier run still open, if there is one.
func (r *run) seedUnderWay(ctx context.Context, p *Person, rs *repoState) (*controlpb.CLRef, error) {
	resp, err := p.Upload.ListChangeLists(ctx, &scmpb.ListChangeListsRequest{RepositoryId: rs.ID,
		Statuses: []scmpb.ChangeListStatus{scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN}})
	if err != nil {
		return nil, err
	}
	for _, c := range resp.GetChangeLists() {
		if strings.HasPrefix(c.GetDescription(), seedDescription) {
			return &controlpb.CLRef{Repository: rs.Name, Number: c.GetNumber()}, nil
		}
	}
	return nil, nil
}

// seedLanded waits for a seed CL, submitted, to land.
func (r *run) seedLanded(ctx context.Context, p *Person, rs *repoState, ref *controlpb.CLRef) error {
	if stage, err := r.until(ctx, p, ref, controlpb.Stage_STAGE_LANDED, controlpb.Stage_STAGE_POST_FAILED); err != nil || stage == controlpb.Stage_STAGE_POST_FAILED {
		return errors.Join(err, fmt.Errorf("the seed of %s did not land", rs.Name))
	}
	_, err := r.freshHead(ctx, p, rs)
	return err
}

// publishInBatches publishes what b staged, seedBatch objects and files at a time, in the order
// staged, files first: each batch names only what the ones before published or it holds.
func publishInBatches(ctx context.Context, p *Person, repo int64, b *scmtest.Objects) error {
	batch := scmtest.Builder()
	n := 0
	flush := func() error {
		if n == 0 {
			return nil
		}
		if _, _, err := batch.Publish(ctx, p.Store, repo, nil); err != nil {
			return err
		}
		batch, n = scmtest.Builder(), 0
		return nil
	}
	for id, data := range b.Files {
		batch.Files[id] = data
		if n++; n == seedBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	for _, o := range b.Order {
		batch.Order = append(batch.Order, o)
		if n++; n == seedBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// freshHead is a repository's main, read now.
func (r *run) freshHead(ctx context.Context, p *Person, rs *repoState) ([]byte, error) {
	got, err := p.Repositories.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: rs.ID}})
	if err != nil {
		return nil, err
	}
	rs.mu.Lock()
	rs.head, rs.headAt = got.GetHead(), time.Now()
	rs.mu.Unlock()
	return got.GetHead(), nil
}

// seeded are the seed's paths, and the CLs' share of them: each CL changes clFiles files no other
// CL of the run changes, from this pod's part of a shuffle of them, starting at a random point —
// so a run's CLs do not change the files an earlier run's still landing do.
type seeded struct {
	paths []string
	clMu  sync.Mutex
	clOf  []string
	next  int
}

func newSeeded(n int, pod, pods int) *seeded {
	s := &seeded{paths: seedPaths(n, 1)}
	order := slices.Clone(s.paths)
	rng := mrand.New(mrand.NewPCG(2, 0))
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	for i, path := range order {
		if i%max(pods, 1) == pod {
			s.clOf = append(s.clOf, path)
		}
	}
	if len(s.clOf) > 0 {
		s.next = mrand.IntN(len(s.clOf))
	}
	return s
}

// forCL are a CL's files: the next of this pod's share, from the start again once it is spent.
func (s *seeded) forCL() []string {
	s.clMu.Lock()
	defer s.clMu.Unlock()
	out := make([]string, 0, clFiles)
	for range clFiles {
		if len(s.clOf) == 0 {
			break
		}
		out = append(out, s.clOf[s.next%len(s.clOf)])
		s.next++
	}
	return out
}

// begin readies a workspace for a checkpoint or a publish: made, as jj clone makes one, and
// attached, as okfs attaches it, its working copy main's head when it began, and the files it
// works on picked. Its lock is held.
func (r *run) begin(ctx context.Context, ws *workspace) error {
	rs, p := ws.repo, ws.owner
	if ws.lease != nil {
		return nil
	}
	if _, err := p.Workspaces.CreateWorkspace(ctx, &scmpb.CreateWorkspaceRequest{RepositoryId: rs.ID, Workspace: ws.name}); err != nil &&
		status.Code(err) != codes.AlreadyExists {
		return err
	}
	got, err := p.Workspaces.Attach(ctx, &scmpb.AttachRequest{RepositoryId: rs.ID, Workspace: ws.name, Machine: "load", Takeover: true})
	if err != nil {
		return err
	}
	if !got.GetGranted() {
		return fmt.Errorf("the workspace is %s's", got.GetHolder())
	}
	ws.lease, ws.lastCheck = got.GetLease(), got.GetLatest().GetId()
	if r.mono == nil {
		return nil
	}
	if ws.tree == (encoding.ID{}) {
		head, err := r.mainHead(ctx, p, rs)
		if err != nil {
			return err
		}
		if ws.tree, err = r.trees.rootOf(ctx, p, rs.ID, head); err != nil {
			return err
		}
		ws.base = head
		for range workingSet {
			ws.paths = append(ws.paths, r.mono.paths[mrand.IntN(len(r.mono.paths))])
		}
	}
	heads, err := p.Store.GetOpHeads(ctx, &scmpb.GetOpHeadsRequest{RepositoryId: rs.ID, Workspace: ws.name})
	if err != nil {
		return err
	}
	ws.opHeads = heads.GetOpIds()
	return nil
}

// editWorkspace changes n of a workspace's files — size bytes over them, in as many as files of
// at most maxFile need — in its working copy, staging the new trees and files in b, and returns the
// working copy's new tree: the workspace's once the server holds it, as a client's outbox keeps
// what it has yet to send. Its lock is held.
func (r *run) editWorkspace(ctx context.Context, ws *workspace, b *scmtest.Objects, n, size int) (encoding.ID, error) {
	n = min(max(n, (size+maxFile-1)/maxFile), len(ws.paths))
	changes := map[string]encoding.ID{}
	for i, path := range mrand.Perm(len(ws.paths))[:n] {
		changes[ws.paths[path]] = fresh(b, size/n+min(1, i))
	}
	return r.trees.edit(ctx, ws.owner, ws.repo.ID, b, ws.tree, changes)
}

// publishTransaction is a jj transaction in a workspace: its working copy, a few of its files
// changed, committed as a new working-copy commit rewriting the last, with the operation and view
// that record it, its operation log's head moved to them. Its lock is held.
func (r *run) publishTransaction(ctx context.Context, ws *workspace) error {
	b := scmtest.Builder()
	b.Workspace = ws.name
	tree, err := r.editWorkspace(ctx, ws, b, 1+mrand.IntN(3), r.cfg.PublishBytes)
	if err != nil {
		return err
	}
	wc := commit(b, tree, "a load publish", ws.base)
	var base encoding.ID
	copy(base[:], ws.base)
	view := &encoding.View{HeadIDs: []encoding.ID{wc}, WCCommitIDs: map[string]encoding.ID{ws.name: wc}}
	if base != encoding.RootCommitID {
		view.HeadIDs = append(view.HeadIDs, base)
	}
	viewID := b.Add(view)
	preds := map[encoding.ID][]encoding.ID{wc: {}}
	if ws.wc != (encoding.ID{}) {
		preds[wc] = []encoding.ID{ws.wc}
	}
	name := ws.name
	parents := make([]encoding.ID, 0, len(ws.opHeads))
	for _, h := range ws.opHeads {
		var id encoding.ID
		copy(id[:], h)
		parents = append(parents, id)
	}
	now := encoding.Timestamp{Millis: time.Now().UnixMilli()}
	op := b.Add(&encoding.Operation{ViewID: viewID, Parents: parents, CommitPredecessors: preds,
		Metadata: encoding.OperationMetadata{Start: now, End: now, Description: "load publish", Hostname: "load", Username: "load", WorkspaceName: &name}})
	got, _, err := b.Publish(ctx, ws.owner.Store, ws.repo.ID, &scmpb.OpHeadsUpdate{OldIds: ws.opHeads, NewId: op[:]})
	if err != nil {
		ws.lease = nil // its operation heads, or its lease, may have moved: begin again
		return err
	}
	ws.tree, ws.wc, ws.opHeads = tree, wc, got
	return nil
}
