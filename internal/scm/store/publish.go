package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
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
	spooled    map[encoding.ID]pack.Location
	spool      *os.File
	pw         *pack.Writer

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
	if err := s.repositoryExists(ctx, s.pool, repo); err != nil {
		return nil, err
	}
	p := &Publication{
		s: s, repo: repo, principal: principal, workspace: workspace,
		staged: map[ObjectRef]bool{}, held: map[ObjectRef]bool{}, want: map[ObjectRef]bool{},
		received: map[ObjectRef]encoding.Object{}, bodies: map[ObjectRef][]byte{},
		files: map[encoding.ID]FileManifest{}, heldFiles: map[encoding.ID]bool{},
		chunkLen: map[encoding.ID]uint32{}, wantChunks: map[encoding.ID]bool{},
		spooled: map[encoding.ID]pack.Location{},
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
	var err error
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
	if p.heldFiles, err = s.existing(ctx, s.pool, repo, tFile, 0, fileIDs); err != nil {
		return nil, err
	}

	// Touch what is held now, so a sweep between this exchange and the commit cannot
	// take it. Anything that vanished meanwhile is requested like any missing object.
	var roots []ref
	for r := range p.held {
		if rf := refOf(r.Kind, r.ID); rf.table != tOperation && rf.table != tView {
			roots = append(roots, rf)
		}
	}
	for id := range p.heldFiles {
		roots = append(roots, ref{table: tFile, id: id})
	}
	vanished, err := s.touchHeld(ctx, repo, roots)
	if err != nil {
		return nil, err
	}
	for _, v := range vanished {
		if v.table == tFile {
			delete(p.heldFiles, v.id)
		} else {
			delete(p.held, v.objectRef())
		}
	}

	for r := range p.staged {
		if !p.held[r] {
			p.want[r] = true
			p.missingObjects = append(p.missingObjects, r)
		}
	}
	var chunkIDs []encoding.ID
	for id, f := range manifests {
		if p.heldFiles[id] {
			continue
		}
		p.files[id] = f
		for _, c := range f.Chunks {
			chunkIDs = append(chunkIDs, c.ID)
		}
	}
	// Chunks the server holds are touched too, for the same reason; the rest are wanted.
	chunkRoots := make([]ref, len(chunkIDs))
	for i, id := range chunkIDs {
		chunkRoots[i] = ref{table: tChunk, id: id}
	}
	absent, err := s.touchHeld(ctx, repo, chunkRoots)
	if err != nil {
		return nil, err
	}
	present, err := s.existing(ctx, s.pool, repo, tChunk, 0, chunkIDs)
	if err != nil {
		return nil, err
	}
	for _, a := range absent {
		delete(present, a.id)
	}
	for _, id := range chunkIDs {
		if !present[id] && !p.wantChunks[id] {
			p.wantChunks[id] = true
			p.missingChunks = append(p.missingChunks, id)
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
		err := s.inTx(ctx, func(tx pgx.Tx) error {
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
			present, err = existingInOpLog(ctx, s.pool, opLog, kind, members)
		default:
			rf := refOf(kind, encoding.ID{})
			present, err = s.existing(ctx, s.pool, repo, rf.table, rf.kind, members)
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

func existingInOpLog(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, opLog int64, kind encoding.Kind, members []encoding.ID) (map[encoding.ID]bool, error) {
	out := map[encoding.ID]bool{}
	if len(members) == 0 {
		return out, nil
	}
	tableName := "operation"
	if kind == encoding.KindView {
		tableName = "view"
	}
	rows, err := q.Query(ctx, `SELECT id FROM `+tableName+` WHERE op_log_id = $1 AND id = ANY($2)`, opLog, ids(members))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		id, err := toID(b)
		if err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
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
func (p *Publication) AddChunk(id encoding.ID, data []byte) error {
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
	if p.pw == nil {
		f, err := os.CreateTemp("", "ok scm-pack-*")
		if err != nil {
			return err
		}
		p.spool, p.pw = f, pack.NewWriter(f)
	}
	loc, err := p.pw.Add(id, data)
	if err != nil {
		return err
	}
	delete(p.wantChunks, id)
	p.spooled[id] = loc
	return nil
}

// Close releases the publication's spooled pack. Commit calls it.
func (p *Publication) Close() {
	if p.closed {
		return
	}
	p.closed = true
	if p.spool != nil {
		p.spool.Close()
		os.Remove(p.spool.Name())
	}
}

// chunk returns a staged file's chunk: from the spool if this publish sent it, else from
// storage.
func (p *Publication) chunk(ctx context.Context, c ChunkRef) ([]byte, error) {
	if loc, ok := p.spooled[c.ID]; ok {
		stored := make([]byte, loc.StoredLength)
		if _, err := p.spool.ReadAt(stored, loc.DataOffset()); err != nil {
			return nil, err
		}
		return pack.Decode(p.s.dicts, c.ID, loc.Codec, loc.RawLength, stored)
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
	for _, id := range fileIDs {
		if err := p.verifyFile(ctx, p.files[id]); err != nil {
			return nil, err
		}
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

	var packID encoding.ID
	var packSize int64
	if p.pw != nil {
		var err error
		if packID, packSize, err = p.pw.Close(); err != nil {
			return nil, err
		}
		if _, err := p.spool.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		if err := p.s.blobs.Put(ctx, packKey(p.repo, packID), p.spool, packSize); err != nil {
			return nil, err
		}
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
	err := p.s.inTx(ctx, func(tx pgx.Tx) error {
		// The log's row lock orders this publish against collection and against the
		// caller's other publishes that move its operations.
		if carriesOps {
			if _, err := tx.Exec(ctx, `SELECT id FROM op_log WHERE id = $1 FOR UPDATE`, p.opLog); err != nil {
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
			present, err := existingInOpLog(ctx, tx, p.opLog, kind, members)
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
		if p.pw != nil {
			if err := p.insertPack(ctx, tx, packID, packSize); err != nil {
				return err
			}
		}
		var stored []ref
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
		}
		if p.opLog != 0 {
			result, err = opHeads(ctx, tx, p.opLog)
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

func packKey(repo int64, id encoding.ID) string {
	return fmt.Sprintf("repos/%d/packs/%s", repo, id)
}

func (p *Publication) insertPack(ctx context.Context, tx pgx.Tx, packID encoding.ID, size int64) error {
	lg, err := p.s.txIndex(ctx, tx, p.repo)
	if err != nil {
		return err
	}
	chunkIDs := make([]encoding.ID, 0, len(p.spooled))
	for id := range p.spooled {
		chunkIDs = append(chunkIDs, id)
	}
	sortIDs(chunkIDs)
	// A chunk another publish stored first keeps its entry; this pack's copy of it is dead
	// bytes that compaction reclaims.
	var fresh []encoding.ID
	for _, id := range chunkIDs {
		if _, ok := lg.has(encoding.KindChunk, id); !ok {
			fresh = append(fresh, id)
		}
	}
	var num uint64
	if err := tx.QueryRow(ctx, `INSERT INTO pack (repository_id, id, size) VALUES ($1, $2, $3)
		ON CONFLICT (repository_id, id) DO UPDATE SET size = EXCLUDED.size RETURNING num`,
		p.repo, packID[:], size).Scan(&num); err != nil {
		return err
	}
	for _, id := range fresh {
		loc := p.spooled[id]
		p.s.logIndex(tx, p.repo, contentindex.Update{Op: contentindex.Put, ID: id, Entry: contentindex.Entry{
			Kind: encoding.KindChunk, Day: lg.today,
			Loc: contentindex.Location{Pack: num, Offset: loc.Offset, Stored: loc.StoredLength, Raw: loc.RawLength, Codec: uint8(loc.Codec)},
		}})
	}
	return nil
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
func (p *Publication) insertFiles(ctx context.Context, tx pgx.Tx, fileIDs []encoding.ID) ([]ref, error) {
	if len(fileIDs) == 0 {
		return nil, nil
	}
	lg, err := p.s.txIndex(ctx, tx, p.repo)
	if err != nil {
		return nil, err
	}
	var found []ref
	for _, id := range fileIDs {
		if _, ok := lg.has(encoding.KindFile, id); ok {
			found = append(found, ref{table: tFile, id: id})
			continue
		}
		f := p.files[id]
		chunks := make([]contentindex.FileChunk, len(f.Chunks))
		for i, c := range f.Chunks {
			chunks[i] = contentindex.FileChunk{ID: c.ID, Length: c.Length}
		}
		p.s.logIndex(tx, p.repo, contentindex.Update{Op: contentindex.Put, ID: id, Entry: contentindex.Entry{
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

// insertObjects stores the new trees, symlinks, and copy histories, and returns those it
// found already stored.
func (p *Publication) insertObjects(ctx context.Context, tx pgx.Tx) ([]ref, error) {
	refs := p.sortedReceived(func(k encoding.Kind) bool {
		return k == encoding.KindSymlink || k == encoding.KindTree || k == encoding.KindCopyHistory
	})
	if len(refs) == 0 {
		return nil, nil
	}
	idCol := make([]encoding.ID, len(refs))
	kinds := make([]int16, len(refs))
	bodies := make([][]byte, len(refs))
	for i, r := range refs {
		idCol[i], kinds[i], bodies[i] = r.ID, int16(r.Kind), p.bodies[r]
	}
	inserted, err := returningIDs(ctx, tx, `INSERT INTO object (repository_id, id, kind, body)
		SELECT $1, u.id, u.kind, u.body FROM unnest($2::bytea[], $3::smallint[], $4::bytea[]) AS u(id, kind, body)
		ORDER BY u.id ON CONFLICT DO NOTHING RETURNING id`, p.repo, ids(idCol), kinds, bodies)
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
	_, err = tx.Exec(ctx, `INSERT INTO copy_parent (repository_id, copy_id, parent_id)
		SELECT $1, u.c, u.p FROM unnest($2::bytea[], $3::bytea[]) AS u(c, p) ON CONFLICT DO NOTHING`,
		p.repo, ids(copyCol), ids(parentCol))
	return found, err
}

// insertCommits stores the new commits and their parent edges, and returns those it found
// already stored.
func (p *Publication) insertCommits(ctx context.Context, tx pgx.Tx) ([]ref, error) {
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
		rows, err := tx.Query(ctx, `SELECT id, generation FROM commit WHERE repository_id = $1 AND id = ANY($2)`, p.repo, ids(outside))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var b []byte
			var g int64
			if err := rows.Scan(&b, &g); err != nil {
				rows.Close()
				return nil, err
			}
			id, err := toID(b)
			if err != nil {
				rows.Close()
				return nil, err
			}
			gen[id] = g
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
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
	n := len(refs)
	idCol := make([]encoding.ID, n)
	changeIDs := make([][]byte, n)
	bodies := make([][]byte, n)
	gens := make([]int64, n)
	conflicted := make([]bool, n)
	committed := make([]time.Time, n)
	for i, r := range refs {
		c := p.received[r].(*encoding.Commit)
		idCol[i], changeIDs[i], bodies[i] = r.ID, append([]byte(nil), c.ChangeID[:]...), p.bodies[r]
		gens[i], conflicted[i] = generation(r.ID), len(c.RootTree) > 1
		committed[i] = time.UnixMilli(c.Committer.Time.Millis)
	}
	inserted, err := returningIDs(ctx, tx, `INSERT INTO commit (repository_id, id, change_id, body, generation, conflicted, committed_at, written_by)
		SELECT $1, u.id, u.change, u.body, u.gen, u.conflicted, u.committed, $8
		FROM unnest($2::bytea[], $3::bytea[], $4::bytea[], $5::bigint[], $6::bool[], $7::timestamptz[]) AS u(id, change, body, gen, conflicted, committed)
		ORDER BY u.id ON CONFLICT DO NOTHING RETURNING id`,
		p.repo, ids(idCol), changeIDs, bodies, gens, conflicted, committed, p.principal)
	if err != nil {
		return nil, err
	}
	found := notInserted(idCol, inserted, func(id encoding.ID) ref { return ref{table: tCommit, id: id} })
	var commitCol, parentCol []encoding.ID
	var ords []int16
	for _, id := range inserted {
		for i, parent := range p.received[ObjectRef{Kind: encoding.KindCommit, ID: id}].(*encoding.Commit).Parents {
			commitCol, parentCol, ords = append(commitCol, id), append(parentCol, parent), append(ords, int16(i))
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO commit_parent (repository_id, commit_id, ord, parent_id)
		SELECT $1, u.c, u.o, u.p FROM unnest($2::bytea[], $3::smallint[], $4::bytea[]) AS u(c, o, p)`,
		p.repo, ids(commitCol), ords, ids(parentCol))
	return found, err
}

// insertOperations stores the new views and operations in the caller's log, and touches
// every staged one it already held: re-publishing an operation or view (undo and restore
// produce identical ones) keeps it from collection.
func (p *Publication) insertOperations(ctx context.Context, tx pgx.Tx) error {
	for _, kind := range []encoding.Kind{encoding.KindView, encoding.KindOperation} {
		refs := p.sortedReceived(func(k encoding.Kind) bool { return k == kind })
		var heldIDs []encoding.ID
		for r := range p.held {
			if r.Kind == kind {
				heldIDs = append(heldIDs, r.ID)
			}
		}
		sortIDs(heldIDs)
		tableName := "view"
		if kind == encoding.KindOperation {
			tableName = "operation"
		}
		if len(refs) > 0 {
			idCol := make([]encoding.ID, len(refs))
			viewCol := make([]encoding.ID, len(refs))
			bodies := make([][]byte, len(refs))
			for i, r := range refs {
				idCol[i], bodies[i] = r.ID, p.bodies[r]
				if op, ok := p.received[r].(*encoding.Operation); ok {
					viewCol[i] = op.ViewID
				}
			}
			var err error
			if kind == encoding.KindOperation {
				_, err = tx.Exec(ctx, `INSERT INTO operation (op_log_id, id, view_id, body)
					SELECT $1, u.id, u.view, u.body FROM unnest($2::bytea[], $3::bytea[], $4::bytea[]) AS u(id, view, body)
					ORDER BY u.id ON CONFLICT (op_log_id, id) DO UPDATE SET touched_at = now()`,
					p.opLog, ids(idCol), ids(viewCol), bodies)
			} else {
				_, err = tx.Exec(ctx, `INSERT INTO view (op_log_id, id, body)
					SELECT $1, u.id, u.body FROM unnest($2::bytea[], $3::bytea[]) AS u(id, body)
					ORDER BY u.id ON CONFLICT (op_log_id, id) DO UPDATE SET touched_at = now()`,
					p.opLog, ids(idCol), bodies)
			}
			if err != nil {
				return err
			}
		}
		if len(heldIDs) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE `+tableName+` SET touched_at = now() WHERE op_log_id = $1 AND id = ANY($2)`,
				p.opLog, ids(heldIDs)); err != nil {
				return err
			}
		}
	}
	return nil
}

func returningIDs(ctx context.Context, tx pgx.Tx, sql string, args ...any) ([]encoding.ID, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []encoding.ID
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		id, err := toID(b)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
