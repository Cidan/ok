package store

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/Cidan/ok/internal/membuf"
	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ChunkRef is one chunk of a staged file: its ID and length.
type ChunkRef struct {
	ID     encoding.ID
	Length uint32
}

// FileManifest describes a staged file: its ID, size, and chunks in file order.
// PathHint is the path it was written at, which orders chunks in packs.
type FileManifest struct {
	FileID   encoding.ID
	Size     uint64
	Chunks   []ChunkRef
	PathHint string
}

// OpHeadsUpdate is jj's update_op_heads(old, new), applied with a publish.
type OpHeadsUpdate struct {
	Old []encoding.ID
	New encoding.ID
}

// Publication is one Publish exchange (docs/scm.md, "Publish"): BeginPublish
// names what the client staged and learns what the server lacks, the client sends those
// objects and chunks, and Commit verifies everything and makes it visible atomically, or
// not at all. Close releases a publication that will not commit.
type Publication struct {
	s         *Store
	repo      int64
	net       backend.Network
	principal string
	workspace string
	opLog     int64

	staged   map[ObjectRef]bool
	held     map[ObjectRef]bool
	want     map[ObjectRef]bool
	received map[ObjectRef]encoding.Object
	bodies   map[ObjectRef][]byte

	files      map[encoding.ID]FileManifest // staged files the server lacks
	heldFiles  map[encoding.ID]bool
	chunkLen   map[encoding.ID]uint32
	wantChunks map[encoding.ID]bool
	// packs are the publish's packs, each held in memory as it is written — what it holds counted
	// against the store's publishMemory, memory — and sent to the bucket once it is full or the
	// publish commits. spooled are the chunks sent, and spooledTrees the trees, where they are.
	packs        []*spoolPack
	spooled      map[encoding.ID]spooledAt
	spooledTrees map[encoding.ID]spooledAt
	memory       int64
	// window is the bytes of a sent pack the publish read last: a file's chunks, read in order, cost
	// a request a window, not one each.
	window spoolWindow

	missingObjects []ObjectRef
	missingChunks  []encoding.ID
	closed         bool
}

func isVirtual(r ObjectRef) bool {
	switch r.Kind {
	case encoding.KindCommit:
		return r.ID == encoding.RootCommitID
	case encoding.KindTree:
		return r.ID == encoding.EmptyTreeID
	case encoding.KindOperation:
		return r.ID == encoding.RootOperationID
	case encoding.KindView:
		return r.ID == encoding.RootViewID
	}
	return false
}

// BeginPublish starts a publish of the objects and files a client staged, its operations and
// views for the operation log of the caller's workspace. It touches every staged object and file
// the server already holds, and reports the rest as missing (see Missing).
func (s *Store) BeginPublish(ctx context.Context, repo int64, principal, workspace string, objects []ObjectRef, files []FileManifest) (*Publication, error) {
	if err := s.repositoryExists(ctx, s.ops(ctx), repo); err != nil {
		return nil, err
	}
	if err := s.gate(ctx, s.ops(ctx)); err != nil {
		return nil, err
	}
	net, err := s.network(ctx, repo)
	if err != nil {
		return nil, err
	}
	p := &Publication{
		s: s, repo: repo, net: net, principal: principal, workspace: workspace,
		staged: map[ObjectRef]bool{}, held: map[ObjectRef]bool{}, want: map[ObjectRef]bool{},
		received: map[ObjectRef]encoding.Object{}, bodies: map[ObjectRef][]byte{},
		files: map[encoding.ID]FileManifest{}, heldFiles: map[encoding.ID]bool{},
		chunkLen: map[encoding.ID]uint32{}, wantChunks: map[encoding.ID]bool{},
		spooled: map[encoding.ID]spooledAt{}, spooledTrees: map[encoding.ID]spooledAt{},
	}
	for _, r := range objects {
		switch r.Kind {
		case encoding.KindSymlink, encoding.KindTree, encoding.KindCopyHistory, encoding.KindCommit,
			encoding.KindOperation, encoding.KindView:
		case encoding.KindFile:
			return nil, invalid("file %s is staged as a manifest, not an object", r.ID)
		default:
			return nil, invalid("cannot stage a %s object", r.Kind)
		}
		if isVirtual(r) {
			return nil, invalid("%s is virtual and never published", r)
		}
		p.staged[r] = true
	}
	manifests := map[encoding.ID]FileManifest{}
	for _, f := range files {
		if _, dup := manifests[f.FileID]; dup {
			return nil, invalid("file %s staged twice", f.FileID)
		}
		if f.Size > MaxFileSize {
			name := f.PathHint
			if name == "" {
				name = f.FileID.String()
			}
			return nil, invalid("%s is %d bytes: ok holds files up to 50 MiB (%d bytes)", name, f.Size, MaxFileSize)
		}
		var total uint64
		for _, c := range f.Chunks {
			if c.Length == 0 || c.Length > MaxChunkLen {
				return nil, invalid("file %s: chunk %s of %d bytes (1 to %d allowed)", f.FileID, c.ID, c.Length, MaxChunkLen)
			}
			if prev, ok := p.chunkLen[c.ID]; ok && prev != c.Length {
				return nil, invalid("chunk %s declared as both %d and %d bytes", c.ID, prev, c.Length)
			}
			p.chunkLen[c.ID] = c.Length
			total += uint64(c.Length)
		}
		if total != f.Size {
			return nil, invalid("file %s: chunks total %d bytes, manifest says %d", f.FileID, total, f.Size)
		}
		manifests[f.FileID] = f
	}

	// Objects are the repository's; operations and views are a workspace's. Only a publish
	// carrying them, or naming its workspace, reaches an operation log.
	carriesOps := false
	for r := range p.staged {
		carriesOps = carriesOps || r.Kind == encoding.KindOperation || r.Kind == encoding.KindView
	}
	if carriesOps || workspace != "" {
		if p.opLog, err = s.OpLog(ctx, repo, principal, workspace); err != nil {
			return nil, err
		}
	}
	if p.held, err = s.heldObjects(ctx, repo, p.opLog, p.staged); err != nil {
		return nil, err
	}
	fileIDs := make([]encoding.ID, 0, len(manifests))
	for id := range manifests {
		fileIDs = append(fileIDs, id)
	}
	if p.heldFiles, err = s.existing(ctx, s.ops(ctx), repo, tFile, 0, fileIDs); err != nil {
		return nil, err
	}

	// Touch what is held now — the objects, the files, and the chunks of the files it lacks —
	// so a sweep between this exchange and the commit cannot take it: in one transaction, a
	// second only for the chunks of a held file that vanished meanwhile. Anything that vanished
	// is requested like anything missing; a touch that reaches a missing chunk is rolled back
	// and run again without it.
	chunksOf := func(files []encoding.ID) []encoding.ID {
		var out []encoding.ID
		for _, id := range files {
			for _, c := range manifests[id].Chunks {
				out = append(out, c.ID)
			}
		}
		return out
	}
	var lacking []encoding.ID
	for id := range manifests {
		if !p.heldFiles[id] {
			lacking = append(lacking, id)
		}
	}
	present, err := s.existing(ctx, s.ops(ctx), repo, tChunk, 0, chunksOf(lacking))
	if err != nil {
		return nil, err
	}
	var roots []ref
	for r := range p.held {
		if rf := refOf(r.Kind, r.ID); rf.table != tOperation && rf.table != tView {
			roots = append(roots, rf)
		}
	}
	for id := range p.heldFiles {
		roots = append(roots, ref{table: tFile, id: id})
	}
	for id := range present {
		roots = append(roots, ref{table: tChunk, id: id})
	}
	vanished, err := s.touchHeld(ctx, repo, roots)
	if err != nil {
		return nil, err
	}
	var gone []encoding.ID
	for _, v := range vanished {
		switch v.table {
		case tFile:
			delete(p.heldFiles, v.id)
			gone = append(gone, v.id)
		case tChunk:
			delete(present, v.id)
		default:
			delete(p.held, v.objectRef())
		}
	}
	if len(gone) > 0 {
		more, err := s.existing(ctx, s.ops(ctx), repo, tChunk, 0, chunksOf(gone))
		if err != nil {
			return nil, err
		}
		var chunkRoots []ref
		for id := range more {
			if !present[id] {
				chunkRoots = append(chunkRoots, ref{table: tChunk, id: id})
			}
		}
		absent, err := s.touchHeld(ctx, repo, chunkRoots)
		if err != nil {
			return nil, err
		}
		for _, a := range absent {
			delete(more, a.id)
		}
		for id := range more {
			present[id] = true
		}
	}

	for r := range p.staged {
		if !p.held[r] {
			p.want[r] = true
			p.missingObjects = append(p.missingObjects, r)
		}
	}
	for id, f := range manifests {
		if p.heldFiles[id] {
			continue
		}
		p.files[id] = f
		for _, c := range f.Chunks {
			if !present[c.ID] && !p.wantChunks[c.ID] {
				p.wantChunks[c.ID] = true
				p.missingChunks = append(p.missingChunks, c.ID)
			}
		}
	}
	sort.Slice(p.missingObjects, func(i, j int) bool {
		a, b := p.missingObjects[i], p.missingObjects[j]
		return a.Kind < b.Kind || (a.Kind == b.Kind && string(a.ID[:]) < string(b.ID[:]))
	})
	sortIDs(p.missingChunks)
	return p, nil
}

// errTouchMissing rolls back a touch that reached a missing object.
var errTouchMissing = errors.New("touch reached a missing object")

// touchHeld touches everything reachable from roots, the objects a publish found held, and
// returns the roots that reach a missing object (a sweep took it). A touch commits only
// when every object it reached exists, so a fresh row never stands over a missing one: a
// root that reaches a missing object is dropped and the rest touched again.
func (s *Store) touchHeld(ctx context.Context, repo int64, roots []ref) ([]ref, error) {
	roots = append([]ref(nil), roots...)
	sort.Slice(roots, func(i, j int) bool { return string(roots[i].id[:]) < string(roots[j].id[:]) })
	var vanished []ref
	for len(roots) > 0 {
		var missing []ref
		err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			var err error
			if missing, err = s.touch(ctx, tx, repo, roots); err != nil {
				return err
			}
			if len(missing) > 0 {
				return errTouchMissing
			}
			return nil
		})
		if err == nil {
			break
		}
		if !errors.Is(err, errTouchMissing) {
			return nil, err
		}
		gone := map[ref]bool{}
		for _, m := range missing {
			gone[m] = true
		}
		vanished = append(vanished, missing...)
		kept := roots[:0]
		for _, r := range roots {
			if !gone[r] {
				kept = append(kept, r)
			}
		}
		roots = kept
	}
	return vanished, nil
}

// forward reports whether an update of the operation heads moves them forward: the new head is
// an operation of this exchange whose parents are every old head (jj's next operation, or a merge
// of concurrent ones). One it cannot tell is taken as not.
func (p *Publication) forward(heads *OpHeadsUpdate) bool {
	o, ok := p.received[ObjectRef{Kind: encoding.KindOperation, ID: heads.New}]
	if !ok {
		return false
	}
	op, ok := o.(*encoding.Operation)
	if !ok {
		return false
	}
	for _, old := range heads.Old {
		if old != heads.New && !slices.Contains(op.Parents, old) {
			return false
		}
	}
	return true
}

// Missing is what the client must send: the staged objects the server lacks, and the
// chunks of staged files it lacks.
func (p *Publication) Missing() ([]ObjectRef, []encoding.ID) {
	return p.missingObjects, p.missingChunks
}

// heldObjects returns which staged objects the server holds: repository objects in the
// repository, operations and views in the caller's log.
func (s *Store) heldObjects(ctx context.Context, repo, opLog int64, staged map[ObjectRef]bool) (map[ObjectRef]bool, error) {
	byKind := map[encoding.Kind][]encoding.ID{}
	for r := range staged {
		byKind[r.Kind] = append(byKind[r.Kind], r.ID)
	}
	held := map[ObjectRef]bool{}
	for kind, members := range byKind {
		var present map[encoding.ID]bool
		var err error
		switch kind {
		case encoding.KindOperation, encoding.KindView:
			present, err = s.existingInOpLog(ctx, s.ops(ctx), repo, opLog, kind, members)
		default:
			rf := refOf(kind, encoding.ID{})
			present, err = s.existing(ctx, s.ops(ctx), repo, rf.table, rf.kind, members)
		}
		if err != nil {
			return nil, err
		}
		for id := range present {
			held[ObjectRef{Kind: kind, ID: id}] = true
		}
	}
	return held, nil
}

func (s *Store) existingInOpLog(ctx context.Context, q backend.Ops, repo, opLog int64, kind encoding.Kind, members []encoding.ID) (map[encoding.ID]bool, error) {
	out := map[encoding.ID]bool{}
	if len(members) == 0 {
		return out, nil
	}
	bodies, err := s.opBodies(ctx, q, repo, opLog, kind, members)
	if err != nil {
		return nil, err
	}
	for id := range bodies {
		out[id] = true
	}
	return out, nil
}

// AddObject receives one missing object. Its body must decode canonically and hash to
// its ID.
func (p *Publication) AddObject(o EncodedObject) error {
	r := ObjectRef{Kind: o.Kind, ID: o.ID}
	if !p.want[r] {
		if _, got := p.received[r]; got {
			return invalid("%s sent twice", r)
		}
		return invalid("%s was not requested", r)
	}
	canonical := append([]byte{byte(o.Kind)}, o.Body...)
	obj, err := encoding.Decode(canonical)
	if err != nil {
		return invalid("%s: %v", r, err)
	}
	if got := encoding.Hash(canonical); got != o.ID {
		return invalid("%s: body hashes to %s", r, got)
	}
	if c, ok := obj.(*encoding.Commit); ok && len(c.Parents) == 0 {
		return invalid("%s has no parents; only the virtual root commit has none", r)
	}
	if err := p.isolated(r, obj); err != nil {
		return err
	}
	delete(p.want, r)
	p.received[r] = obj
	p.bodies[r] = o.Body
	return nil
}

// isolated refuses an operation or view that would reach past the workspace publishing it
// (docs/scm.md, "Decisions", 3): a view naming another workspace's working copy, or an
// operation run in another workspace.
func (p *Publication) isolated(r ObjectRef, obj encoding.Object) error {
	switch o := obj.(type) {
	case *encoding.View:
		for name := range o.WCCommitIDs {
			if name != p.workspace {
				return invalid("%s names workspace %q's working copy, in workspace %q's operation log: a workspace's operation log holds only its own working copy (jj clone makes another workspace)", r, name, p.workspace)
			}
		}
	case *encoding.Operation:
		if name := o.Metadata.WorkspaceName; name != nil && *name != p.workspace {
			return invalid("%s ran in workspace %q, in workspace %q's operation log: a workspace's operation log holds only its own operations", r, *name, p.workspace)
		}
	}
	return nil
}

// AddChunk receives one missing chunk, which must be its declared length and hash to
// its ID. It is spooled into the publish's pack.
func (p *Publication) AddChunk(ctx context.Context, id encoding.ID, data []byte) error {
	if !p.wantChunks[id] {
		if _, got := p.spooled[id]; got {
			return invalid("chunk %s sent twice", id)
		}
		return invalid("chunk %s was not requested", id)
	}
	if uint32(len(data)) != p.chunkLen[id] {
		return invalid("chunk %s is %d bytes, declared %d", id, len(data), p.chunkLen[id])
	}
	if got := pack.ChunkID(data); got != id {
		return invalid("chunk %s: bytes hash to %s", id, got)
	}
	at, err := p.spoolRecord(ctx, id, data)
	if err != nil {
		return err
	}
	delete(p.wantChunks, id)
	p.spooled[id] = at
	return nil
}

// spoolTree adds a tree's canonical body to the publish's pack.
func (p *Publication) spoolTree(ctx context.Context, id encoding.ID, body []byte) error {
	at, err := p.spoolRecord(ctx, id, body)
	if err != nil {
		return err
	}
	p.spooledTrees[id] = at
	return nil
}

// spoolPack is one pack of a publish: in memory until it is sent, then in the bucket as id.
type spoolPack struct {
	buf  membuf.Buffer
	pw   *pack.Writer
	sent bool
	id   encoding.ID
	size int64
}

// spooledAt is where a publish spooled a chunk or a tree: a record of its pack's.
type spooledAt struct {
	pack int
	loc  pack.Location
}

// spoolWindow is bytes of a sent pack, from off.
type spoolWindow struct {
	pack int
	off  int64
	data []byte
}

// sentWindow is the most a read of a sent pack's records fetches beyond what it was asked for.
const sentWindow = 4 << 20

// recordCost is the memory a record of n bytes takes in a pack being written: its stored bytes at
// most, its header, and its index entry.
func recordCost(n int) int64 { return int64(n) + pack.Overhead(1) - pack.Overhead(0) }

// spoolRecord adds a chunk's or a tree's record to the publish's pack in memory, holding what it
// costs of the store's publishMemory: past publishPackBytes, the pack is sent first, and another
// started; when publishes at once hold all of it, this publish sends what it holds and waits.
func (p *Publication) spoolRecord(ctx context.Context, id encoding.ID, data []byte) (spooledAt, error) {
	cost := min(recordCost(len(data)), p.s.publishMemoryBytes)
	if cur := p.current(); cur != nil && cur.buf.Len()+cost > p.s.publishPackBytes {
		if err := p.send(ctx); err != nil {
			return spooledAt{}, err
		}
	}
	if !p.s.publishMemory.TryAcquire(cost) {
		if err := p.send(ctx); err != nil {
			return spooledAt{}, err
		}
		if err := p.s.publishMemory.Acquire(ctx, cost); err != nil {
			return spooledAt{}, err
		}
	}
	p.memory += cost
	cur := p.current()
	if cur == nil {
		cur = &spoolPack{}
		cur.pw = pack.NewWriter(&cur.buf)
		p.packs = append(p.packs, cur)
	}
	loc, err := cur.pw.Add(id, data)
	if err != nil {
		return spooledAt{}, err
	}
	return spooledAt{pack: len(p.packs) - 1, loc: loc}, nil
}

// current is the pack being written: nil before the first, and once the last was sent.
func (p *Publication) current() *spoolPack {
	if len(p.packs) == 0 || p.packs[len(p.packs)-1].sent {
		return nil
	}
	return p.packs[len(p.packs)-1]
}

// send sends the pack being written to the bucket, if there is one, and lets its memory go. A pack
// sent by a publish that never commits is an orphan, which collection deletes (deletePacks).
func (p *Publication) send(ctx context.Context) error {
	cur := p.current()
	if cur == nil {
		return nil
	}
	id, size, err := cur.pw.Close()
	if err != nil {
		return err
	}
	if err := p.s.blobs.Put(ctx, packKey(p.net, id), cur.buf.Reader(), size); err != nil {
		return err
	}
	cur.sent, cur.id, cur.size, cur.pw = true, id, size, nil
	cur.buf.Reset()
	p.release()
	return nil
}

// release lets go of the memory the publish holds.
func (p *Publication) release() {
	if p.memory > 0 {
		p.s.publishMemory.Release(p.memory)
		p.memory = 0
	}
}

// Close releases the publication's spooled packs. Commit calls it.
func (p *Publication) Close() {
	if p.closed {
		return
	}
	p.closed = true
	for _, sp := range p.packs {
		sp.buf.Reset()
	}
	p.release()
}

// stored reads a spooled record's stored bytes: from memory while its pack is, else from the
// bucket, a window at a time.
func (p *Publication) stored(ctx context.Context, at spooledAt) ([]byte, error) {
	sp := p.packs[at.pack]
	off, n := at.loc.DataOffset(), int64(at.loc.StoredLength)
	if !sp.sent {
		out := make([]byte, n)
		if _, err := sp.buf.ReadAt(out, off); err != nil {
			return nil, err
		}
		return out, nil
	}
	w := p.window
	if w.data == nil || w.pack != at.pack || off < w.off || off+n > w.off+int64(len(w.data)) {
		data, err := p.s.blobs.GetRange(ctx, packKey(p.net, sp.id), off, min(max(n, sentWindow), sp.size-off))
		if err != nil {
			return nil, err
		}
		p.window = spoolWindow{pack: at.pack, off: off, data: data}
		w = p.window
	}
	return w.data[off-w.off : off-w.off+n], nil
}

// chunk returns a staged file's chunk: from the publish's packs if this publish sent it, else from
// storage.
func (p *Publication) chunk(ctx context.Context, c ChunkRef) ([]byte, error) {
	if at, ok := p.spooled[c.ID]; ok {
		stored, err := p.stored(ctx, at)
		if err != nil {
			return nil, err
		}
		return pack.Decode(p.s.dicts.of(p.net), c.ID, at.loc.Codec, at.loc.RawLength, stored)
	}
	data, err := p.s.chunkBytes(ctx, p.repo, []encoding.ID{c.ID})
	if err != nil {
		return nil, err
	}
	raw, ok := data[c.ID]
	if !ok {
		return nil, fmt.Errorf("chunk %s vanished", c.ID)
	}
	if uint32(len(raw)) != c.Length {
		return nil, invalid("chunk %s is %d bytes, declared %d", c.ID, len(raw), c.Length)
	}
	return raw, nil
}

// checkSecrets refuses a publish whose new commits hold a secret's value under //secret
// (checkSecretValues), reading their trees and files from the publish where it carries them.
func (p *Publication) checkSecrets(ctx context.Context) error {
	var roots []encoding.ID
	for r, o := range p.received {
		if c, ok := o.(*encoding.Commit); ok && r.Kind == encoding.KindCommit {
			roots = append(roots, c.RootTree...)
		}
	}
	if len(roots) == 0 {
		return nil
	}
	stored := p.s.storedFiles(ctx, p.repo)
	read := func(id encoding.ID) (uint64, func() ([]byte, error), error) {
		f, ok := p.files[id]
		if !ok {
			return stored(id)
		}
		return f.Size, func() ([]byte, error) {
			var out []byte
			for _, c := range f.Chunks {
				raw, err := p.chunk(ctx, c)
				if err != nil {
					return nil, err
				}
				out = append(out, raw...)
			}
			return out, nil
		}, nil
	}
	return p.s.checkSecretValues(ctx, p.repo, publishedTrees{p: p, stored: p.s.pendingTrees(ctx, p.repo)}, read, roots)
}

// publishedTrees reads the trees a publish carries, and the repository's others.
type publishedTrees struct {
	p      *Publication
	stored *pendingTrees
}

func (t publishedTrees) readTree(id encoding.ID) (*encoding.Tree, error) {
	if o, ok := t.p.received[ObjectRef{Kind: encoding.KindTree, ID: id}]; ok {
		if tree, ok := o.(*encoding.Tree); ok {
			return tree, nil
		}
	}
	return t.stored.readTree(id)
}

func (t publishedTrees) writeTree(*encoding.Tree) (encoding.ID, error) {
	return encoding.ID{}, errors.New("a publish's trees are read, never written")
}

// firstSpooled is where a new file's first chunk this publish sent lies: none, after every pack, for
// a file of chunks the repository held.
func (p *Publication) firstSpooled(file encoding.ID) spoolPlace {
	for _, c := range p.files[file].Chunks {
		if at, ok := p.spooled[c.ID]; ok {
			return spoolPlace{pack: at.pack, off: at.loc.Offset}
		}
	}
	return spoolPlace{pack: len(p.packs)}
}

// spoolPlace is a place in a publish's packs.
type spoolPlace struct {
	pack int
	off  int64
}

func (a spoolPlace) compare(b spoolPlace) int {
	if c := cmp.Compare(a.pack, b.pack); c != 0 {
		return c
	}
	return cmp.Compare(a.off, b.off)
}

// verifyFile checks that a new file's chunks, concatenated in manifest order, hash to
// its ID.
func (p *Publication) verifyFile(ctx context.Context, f FileManifest) error {
	h := encoding.NewHasher(encoding.KindFile)
	for _, c := range f.Chunks {
		raw, err := p.chunk(ctx, c)
		if err != nil {
			return err
		}
		h.Write(raw)
	}
	if got := h.Sum(); got != f.FileID {
		return invalid("file %s: chunks hash to %s", f.FileID, got)
	}
	return nil
}

// Commit verifies the publish and makes it visible in one transaction: its pack, chunks,
// files, objects, commits, operations, and views, the touch of everything pre-existing it
// references, and heads (if set) applied to the caller's operation log. It returns the
// resulting operation heads. References that resolve to nothing refuse the publish with
// a *Rejection; nothing it wrote becomes visible.
func (p *Publication) Commit(ctx context.Context, heads *OpHeadsUpdate) ([]encoding.ID, error) {
	defer p.Close()
	if len(p.want) > 0 {
		return nil, invalid("%d requested objects were not sent", len(p.want))
	}
	if len(p.wantChunks) > 0 {
		return nil, invalid("%d requested chunks were not sent", len(p.wantChunks))
	}
	fileIDs := make([]encoding.ID, 0, len(p.files))
	for id := range p.files {
		fileIDs = append(fileIDs, id)
	}
	sortIDs(fileIDs)
	// In the order their first chunks lie in the packs: a sent pack is read through once.
	verifying := slices.Clone(fileIDs)
	slices.SortStableFunc(verifying, func(a, b encoding.ID) int { return p.firstSpooled(a).compare(p.firstSpooled(b)) })
	for _, id := range verifying {
		if err := p.verifyFile(ctx, p.files[id]); err != nil {
			return nil, err
		}
	}
	if err := p.checkSecrets(ctx); err != nil {
		return nil, err
	}

	// Everything the new objects (and the heads update) reference must resolve: to an
	// object staged in this publish, or to one the repository (or the caller's log) holds.
	need := refSet{}
	for _, o := range p.received {
		closureRefs(need, o)
	}
	if heads != nil && heads.New != encoding.RootOperationID {
		need.add(ref{table: tOperation, id: heads.New})
	}
	resolved := func(r ref) bool {
		switch r.table {
		case tFile:
			_, isNew := p.files[r.id]
			return isNew || p.heldFiles[r.id]
		default:
			return p.staged[r.objectRef()]
		}
	}
	var repoRoots, opRefs []ref
	for _, r := range need.sorted() {
		switch {
		case resolved(r):
		case r.table == tOperation || r.table == tView:
			opRefs = append(opRefs, r)
		default:
			repoRoots = append(repoRoots, r)
		}
	}
	// Held staged objects and files are touched again under the transaction: re-sending
	// an old subgraph keeps it fresh, and a publish never commits against one that vanished.
	for r := range p.held {
		if rf := refOf(r.Kind, r.ID); rf.table != tOperation && rf.table != tView {
			repoRoots = append(repoRoots, rf)
		}
	}
	for id := range p.heldFiles {
		repoRoots = append(repoRoots, ref{table: tFile, id: id})
	}
	// So are the chunks new files reuse: a new file must never point at a chunk a sweep
	// takes. One that vanished since the exchange refuses the publish, and the client's
	// retry sends it.
	for _, id := range fileIDs {
		for _, c := range p.files[id].Chunks {
			if _, sent := p.spooled[c.ID]; !sent {
				repoRoots = append(repoRoots, ref{table: tChunk, id: c.ID})
			}
		}
	}

	// The trees sent go in the pack, after the chunks: the content index holds them, and reads
	// find them there, as it does the chunks.
	for _, r := range p.sortedReceived(func(k encoding.Kind) bool { return k == encoding.KindTree }) {
		if err := p.spoolTree(ctx, r.ID, p.bodies[r]); err != nil {
			return nil, err
		}
	}
	if err := p.send(ctx); err != nil {
		return nil, err
	}

	carriesOps := heads != nil
	for r := range p.staged {
		carriesOps = carriesOps || r.Kind == encoding.KindOperation || r.Kind == encoding.KindView
	}
	if carriesOps && p.opLog == 0 {
		return nil, unnamedWorkspace()
	}
	if p.s.beforeCommit != nil {
		p.s.beforeCommit()
	}
	var result []encoding.ID
	err := p.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		// The log's row lock orders this publish against collection and against the
		// caller's other publishes that move its operations.
		if carriesOps {
			if err := tx.LockOpLog(ctx, p.opLog); err != nil {
				return err
			}
		}
		missing, err := p.s.touch(ctx, tx, p.repo, repoRoots)
		if err != nil {
			return err
		}
		for _, kind := range []encoding.Kind{encoding.KindOperation, encoding.KindView} {
			var members []encoding.ID
			for _, r := range opRefs {
				if r.objectRef().Kind == kind {
					members = append(members, r.id)
				}
			}
			present, err := p.s.existingInOpLog(ctx, tx, p.repo, p.opLog, kind, members)
			if err != nil {
				return err
			}
			for _, id := range members {
				if !present[id] {
					missing = append(missing, refOf(kind, id))
				}
			}
		}
		if len(missing) > 0 {
			rej := &Rejection{Message: fmt.Sprintf("%d referenced objects are neither stored nor sent", len(missing))}
			for _, m := range missing {
				rej.Missing = append(rej.Missing, m.objectRef())
			}
			return rej
		}
		var stored []ref
		if len(p.packs) > 0 {
			held, err := p.insertPacks(ctx, tx)
			if err != nil {
				return err
			}
			stored = append(stored, held...)
		}
		found, err := p.insertFiles(ctx, tx, fileIDs)
		if err != nil {
			return err
		}
		stored = append(stored, found...)
		if found, err = p.insertObjects(ctx, tx); err != nil {
			return err
		}
		stored = append(stored, found...)
		if found, err = p.insertCommits(ctx, tx); err != nil {
			return err
		}
		stored = append(stored, found...)
		// What the inserts found already stored — sent again because part of its graph
		// was swept, or stored meanwhile by another publish — is touched now that its
		// graph is whole: a new object never references a stale one.
		if missing, err := p.s.touch(ctx, tx, p.repo, stored); err != nil {
			return err
		} else if len(missing) > 0 {
			rej := &Rejection{Message: fmt.Sprintf("%d referenced objects are neither stored nor sent", len(missing))}
			for _, m := range missing {
				rej.Missing = append(rej.Missing, m.objectRef())
			}
			return rej
		}
		if err := p.insertOperations(ctx, tx); err != nil {
			return err
		}
		if heads != nil {
			if err := updateOpHeads(ctx, tx, p.opLog, heads.Old, heads.New); err != nil {
				return err
			}
			// Heads that moved other than forward — jj op abandon — leave operations nothing
			// reaches: the log is collected again (settle.go).
			if !p.forward(heads) {
				if err := tx.DivergeOpLog(ctx, p.opLog); err != nil {
					return err
				}
			}
		}
		if p.opLog != 0 {
			result, err = tx.OpHeads(ctx, p.opLog)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	for r, body := range p.bodies {
		if r.Kind != encoding.KindOperation && r.Kind != encoding.KindView {
			p.s.objects.put(objectKey(p.repo, r), body, len(body))
		}
	}
	return result, nil
}

// packKey is a pack's object in the bucket, under its network's keys.
func packKey(net backend.Network, id encoding.ID) string { return packPrefix(net) + id.String() }

// packPrefix begins the key of every pack of a network.
func packPrefix(net backend.Network) string { return fmt.Sprintf("nets/%d/packs/", net) }

// insertPacks records the publish's packs and what they hold — the chunks sent, and the trees —
// each the repository lacks in the repository's content index, and each the network lacks a
// location of in the network's. It returns the trees the repository held already, to touch with
// the rest.
func (p *Publication) insertPacks(ctx context.Context, tx *txn) ([]ref, error) {
	lg, err := p.s.txIndex(ctx, tx, repoScope(p.repo))
	if err != nil {
		return nil, err
	}
	net, err := p.s.txIndex(ctx, tx, backend.NetworkScope(p.net))
	if err != nil {
		return nil, err
	}
	type record struct {
		id   encoding.ID
		kind encoding.Kind
		at   spooledAt
	}
	records := make([]record, 0, len(p.spooled)+len(p.spooledTrees))
	ids := make([]encoding.ID, 0, cap(records))
	for id, at := range p.spooled {
		records, ids = append(records, record{id, encoding.KindChunk, at}), append(ids, id)
	}
	for id, at := range p.spooledTrees {
		records, ids = append(records, record{id, encoding.KindTree, at}), append(ids, id)
	}
	slices.SortFunc(records, func(a, b record) int { return bytes.Compare(a.id[:], b.id[:]) })
	// A record another publish stored first keeps its entry, and one the network stores already
	// keeps its location; this pack's copy of either is dead bytes that compaction reclaims. A
	// location of what the repository does not hold is the network's collection's to drop once no
	// repository holds it (collectReleases): relied on, it is decided by under the network's lock,
	// as the collection decides — this publish then committed before the collection reads what
	// repositories hold, or finds the location gone and records its own.
	var fresh, unlocated []record
	var held []ref
	for {
		if err := lg.load(ctx, ids); err != nil {
			return nil, err
		}
		if err := net.load(ctx, ids); err != nil {
			return nil, err
		}
		fresh, unlocated, held = fresh[:0], unlocated[:0], held[:0]
		relied := false
		for _, r := range records {
			_, has, err := lg.has(ctx, r.kind, r.id)
			if err != nil {
				return nil, err
			}
			switch {
			case !has:
				fresh = append(fresh, r)
			case r.kind == encoding.KindTree:
				held = append(held, ref{table: tObject, kind: encoding.KindTree, id: r.id})
			}
			_, located, err := net.has(ctx, r.kind, r.id)
			if err != nil {
				return nil, err
			}
			if !located {
				unlocated = append(unlocated, r)
			} else if !has {
				relied = true
			}
		}
		if !relied || net.locked {
			break
		}
		if err := net.lock(ctx); err != nil {
			return nil, err
		}
	}
	nums := make([]uint64, len(p.packs))
	for i, sp := range p.packs {
		num, err := tx.PutPack(ctx, p.net, sp.id, sp.size)
		if err != nil {
			return nil, err
		}
		nums[i] = num
	}
	for _, r := range fresh {
		p.s.logIndex(tx, repoScope(p.repo), contentindex.Update{Op: contentindex.Put, ID: r.id, Entry: contentindex.Entry{
			Kind: r.kind, Day: lg.today,
		}})
	}
	for _, r := range unlocated {
		p.s.logIndex(tx, backend.NetworkScope(p.net), contentindex.Update{Op: contentindex.Put, ID: r.id, Entry: contentindex.Entry{
			Kind: r.kind, Day: net.today,
			Loc: contentindex.Location{Pack: nums[r.at.pack], Offset: r.at.loc.Offset, Stored: r.at.loc.StoredLength, Raw: r.at.loc.RawLength,
				Codec: uint8(r.at.loc.Codec)},
		}})
	}
	return held, nil
}

// notInserted returns, as references, the IDs in want that inserted lacks: rows the
// insert found already stored.
func notInserted(want, inserted []encoding.ID, mk func(encoding.ID) ref) []ref {
	got := make(map[encoding.ID]bool, len(inserted))
	for _, id := range inserted {
		got[id] = true
	}
	var out []ref
	for _, id := range want {
		if !got[id] {
			out = append(out, mk(id))
		}
	}
	return out
}

// insertFiles stores the new files and their chunk lists, and returns those it found
// already stored.
func (p *Publication) insertFiles(ctx context.Context, tx *txn, fileIDs []encoding.ID) ([]ref, error) {
	if len(fileIDs) == 0 {
		return nil, nil
	}
	lg, err := p.s.txIndex(ctx, tx, repoScope(p.repo))
	if err != nil {
		return nil, err
	}
	if err := lg.load(ctx, fileIDs); err != nil {
		return nil, err
	}
	var found []ref
	for _, id := range fileIDs {
		_, ok, err := lg.has(ctx, encoding.KindFile, id)
		if err != nil {
			return nil, err
		}
		if ok {
			found = append(found, ref{table: tFile, id: id})
			continue
		}
		f := p.files[id]
		chunks := make([]contentindex.FileChunk, len(f.Chunks))
		for i, c := range f.Chunks {
			chunks[i] = contentindex.FileChunk{ID: c.ID, Length: c.Length}
		}
		p.s.logIndex(tx, repoScope(p.repo), contentindex.Update{Op: contentindex.Put, ID: id, Entry: contentindex.Entry{
			Kind: encoding.KindFile, Day: lg.today, Size: f.Size, Chunks: chunks,
		}})
	}
	return found, nil
}

func (p *Publication) sortedReceived(match func(encoding.Kind) bool) []ObjectRef {
	var out []ObjectRef
	for r := range p.received {
		if match(r.Kind) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].ID[:]) < string(out[j].ID[:]) })
	return out
}

// insertObjects stores the new symlinks and copy histories, and returns those it
// found already stored.
func (p *Publication) insertObjects(ctx context.Context, tx *txn) ([]ref, error) {
	refs := p.sortedReceived(func(k encoding.Kind) bool {
		return k == encoding.KindSymlink || k == encoding.KindCopyHistory
	})
	if len(refs) == 0 {
		return nil, nil
	}
	idCol := make([]encoding.ID, len(refs))
	objects := make([]backend.Object, len(refs))
	for i, r := range refs {
		idCol[i] = r.ID
		objects[i] = backend.Object{ID: r.ID, Kind: r.Kind, Body: p.bodies[r]}
	}
	inserted, err := tx.InsertObjects(ctx, p.repo, objects)
	if err != nil {
		return nil, err
	}
	kindOf := make(map[encoding.ID]encoding.Kind, len(refs))
	for _, r := range refs {
		kindOf[r.ID] = r.Kind
	}
	found := notInserted(idCol, inserted, func(id encoding.ID) ref { return refOf(kindOf[id], id) })
	var copyCol, parentCol []encoding.ID
	for _, id := range inserted {
		c, ok := p.received[ObjectRef{Kind: encoding.KindCopyHistory, ID: id}].(*encoding.CopyHistory)
		if !ok {
			continue
		}
		for _, parent := range c.Parents {
			copyCol, parentCol = append(copyCol, id), append(parentCol, parent)
		}
	}
	if len(copyCol) == 0 {
		return found, nil
	}
	return found, tx.InsertCopyParents(ctx, p.repo, copyCol, parentCol)
}

// insertCommits stores the new commits and their parent edges, and returns those it found
// already stored.
func (p *Publication) insertCommits(ctx context.Context, tx *txn) ([]ref, error) {
	refs := p.sortedReceived(func(k encoding.Kind) bool { return k == encoding.KindCommit })
	if len(refs) == 0 {
		return nil, nil
	}
	// A commit's generation is one more than its highest parent's; the root's is 0.
	var outside []encoding.ID
	for _, r := range refs {
		for _, parent := range p.received[r].(*encoding.Commit).Parents {
			if _, isNew := p.received[ObjectRef{Kind: encoding.KindCommit, ID: parent}]; !isNew && parent != encoding.RootCommitID {
				outside = append(outside, parent)
			}
		}
	}
	gen := map[encoding.ID]int64{encoding.RootCommitID: 0}
	if len(outside) > 0 {
		got, err := p.s.commitGenerations(ctx, p.repo, outside)
		if err != nil {
			return nil, err
		}
		for id, g := range got {
			gen[id] = g
		}
	}
	var generation func(encoding.ID) int64
	generation = func(id encoding.ID) int64 {
		if g, ok := gen[id]; ok {
			return g
		}
		var g int64
		for _, parent := range p.received[ObjectRef{Kind: encoding.KindCommit, ID: id}].(*encoding.Commit).Parents {
			g = max(g, generation(parent)+1)
		}
		gen[id] = g
		return g
	}
	idCol := make([]encoding.ID, len(refs))
	commits := make([]backend.Commit, len(refs))
	for i, r := range refs {
		c := p.received[r].(*encoding.Commit)
		idCol[i] = r.ID
		commits[i] = backend.Commit{
			ID: r.ID, ChangeID: append([]byte(nil), c.ChangeID[:]...), Body: p.bodies[r],
			Generation: generation(r.ID), Conflicted: len(c.RootTree) > 1, CommittedAt: time.UnixMilli(c.Committer.Time.Millis),
		}
	}
	inserted, err := tx.InsertCommits(ctx, p.repo, commits, p.principal)
	if err != nil {
		return nil, err
	}
	found := notInserted(idCol, inserted, func(id encoding.ID) ref { return ref{table: tCommit, id: id} })
	var edges []backend.ParentEdge
	for _, id := range inserted {
		for i, parent := range p.received[ObjectRef{Kind: encoding.KindCommit, ID: id}].(*encoding.Commit).Parents {
			edges = append(edges, backend.ParentEdge{Commit: id, Ord: int16(i), Parent: parent})
		}
	}
	return found, tx.InsertCommitParents(ctx, p.repo, edges)
}

// insertOperations stores the new views and operations in the caller's log, and touches
// every staged one it already held: re-publishing an operation or view (undo and restore
// produce identical ones) keeps it from collection.
func (p *Publication) insertOperations(ctx context.Context, tx *txn) error {
	for _, kind := range []encoding.Kind{encoding.KindView, encoding.KindOperation} {
		refs := p.sortedReceived(func(k encoding.Kind) bool { return k == kind })
		var heldIDs []encoding.ID
		for r := range p.held {
			if r.Kind == kind {
				heldIDs = append(heldIDs, r.ID)
			}
		}
		sortIDs(heldIDs)
		if len(refs) > 0 {
			var err error
			if kind == encoding.KindOperation {
				ops := make([]backend.Operation, len(refs))
				for i, r := range refs {
					ops[i] = backend.Operation{ID: r.ID, View: p.received[r].(*encoding.Operation).ViewID, Body: p.bodies[r]}
				}
				err = tx.PutOperations(ctx, p.opLog, ops)
			} else {
				views := make([]backend.Object, len(refs))
				for i, r := range refs {
					views[i] = backend.Object{ID: r.ID, Kind: kind, Body: p.bodies[r]}
				}
				err = tx.PutViews(ctx, p.opLog, views)
			}
			if err != nil {
				return err
			}
		}
		if len(heldIDs) > 0 {
			if err := p.s.touchOps(ctx, tx, p.repo, p.opLog, kind, heldIDs); err != nil {
				return err
			}
		}
	}
	return nil
}
