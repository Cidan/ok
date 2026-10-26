package checkout

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/layer"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// A commit's leaves come from its checkout (docs/scm.md, "Checkouts"): the SCM sends the layers
// this source does not hold — after a commit's first, usually one small one — and they are walked
// here, so listing a commit costs one request of what changed rather than its whole tree, and the
// files planning reads, which layers hold, need no request at all.

// layersBudget bounds the decoded layers a source keeps: a monorepo's checkout is tens of MiB.
const layersBudget = 1 << 30

// decodedLayer is a layer's records, by kind and ID.
type decodedLayer struct {
	id    ID
	bytes int
	objs  map[layerKey][]byte
}

type layerKey struct {
	kind encoding.Kind
	id   ID
}

// layerCache holds a source's decoded layers, by repository and ID, the newest used last.
type layerCache struct {
	mu     sync.Mutex
	layers map[int64]map[ID]*decodedLayer
	order  []repoLayer
	bytes  int
}

type repoLayer struct {
	repo int64
	id   ID
}

func (c *layerCache) held(repo int64) [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out [][]byte
	for id := range c.layers[repo] {
		out = append(out, id[:])
	}
	return out
}

func (c *layerCache) get(repo int64, id ID) *decodedLayer {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.layers[repo][id]
	if l != nil {
		c.touch(repo, id)
	}
	return l
}

func (c *layerCache) touch(repo int64, id ID) {
	if i := slices.Index(c.order, repoLayer{repo, id}); i >= 0 {
		c.order = append(slices.Delete(c.order, i, i+1), repoLayer{repo, id})
	}
}

func (c *layerCache) put(repo int64, l *decodedLayer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.layers == nil {
		c.layers = map[int64]map[ID]*decodedLayer{}
	}
	if c.layers[repo] == nil {
		c.layers[repo] = map[ID]*decodedLayer{}
	}
	if _, ok := c.layers[repo][l.id]; ok {
		c.touch(repo, l.id)
		return
	}
	c.layers[repo][l.id] = l
	c.order = append(c.order, repoLayer{repo, l.id})
	c.bytes += l.bytes
	for c.bytes > layersBudget && len(c.order) > 1 {
		old := c.order[0]
		c.order = c.order[1:]
		c.bytes -= c.layers[old.repo][old.id].bytes
		delete(c.layers[old.repo], old.id)
	}
}

// checkoutOf is a commit's checkout as layers this source holds, oldest first, reading those it
// lacks — with since's checkout, when the SCM answered what the commit changes from it.
type checkoutOf struct {
	tree   ID
	layers []*decodedLayer
	since  ID
}

// holding is what trees' whole checkouts a source holds, by repository: the layers holding each,
// the newest holdingMax trees.
type holding struct {
	mu    sync.Mutex
	trees map[int64]map[ID][]ID
	order map[int64][]ID
}

const holdingMax = 64

func (h *holding) record(repo int64, tree ID, layers []ID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.trees == nil {
		h.trees, h.order = map[int64]map[ID][]ID{}, map[int64][]ID{}
	}
	if h.trees[repo] == nil {
		h.trees[repo] = map[ID][]ID{}
	}
	if _, ok := h.trees[repo][tree]; !ok {
		h.order[repo] = append(h.order[repo], tree)
	}
	h.trees[repo][tree] = layers
	for len(h.order[repo]) > holdingMax {
		delete(h.trees[repo], h.order[repo][0])
		h.order[repo] = h.order[repo][1:]
	}
}

func (h *holding) layers(repo int64, tree ID) ([]ID, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	l, ok := h.trees[repo][tree]
	return l, ok
}

// holdsWhole reports whether every layer holding a tree's checkout is still held.
func (s *scm) holdsWhole(repo int64, tree ID) bool {
	ids, ok := s.full.layers(repo, tree)
	if !ok {
		return false
	}
	for _, id := range ids {
		if s.layers.get(repo, id) == nil {
			return false
		}
	}
	return true
}

func (k *checkoutOf) find(kind encoding.Kind, id ID) ([]byte, bool) {
	for i := len(k.layers) - 1; i >= 0; i-- {
		if b, ok := k.layers[i].objs[layerKey{kind, id}]; ok {
			return b, true
		}
	}
	return nil, false
}

// checkout is a commit's checkout — from a tree the source holds whole (have, zero for none), what
// the commit changes from it — and records the layers that now hold the commit's tree whole.
func (s *scm) checkout(ctx context.Context, repo int64, commit []byte, have ID) (*checkoutOf, error) {
	co, err := s.readCheckout(ctx, repo, commit, s.layers.held(repo), have)
	if errors.Is(err, errLayerGone) {
		// A layer named as held was let go as the others arrived: asked again, all are sent.
		co, err = s.readCheckout(ctx, repo, commit, nil, ID{})
	}
	if err != nil {
		return nil, err
	}
	var ids []ID
	if co.since != (ID{}) {
		below, ok := s.full.layers(repo, co.since)
		if !ok {
			return nil, fmt.Errorf("reading the commit's checkout: it completes tree %x's, which is not held", co.since[:])
		}
		ids = slices.Clone(below)
	}
	for _, l := range co.layers {
		ids = append(ids, l.id)
	}
	s.full.record(repo, co.tree, ids)
	return co, nil
}

// errLayerGone is a layer a checkout names that was neither held nor sent.
var errLayerGone = errors.New("a layer was neither held nor sent")

func (s *scm) readCheckout(ctx context.Context, repo int64, commit []byte, held [][]byte, have ID) (*checkoutOf, error) {
	req := &scmpb.ReadCheckoutRequest{RepositoryId: repo, CommitId: commit, HeldLayers: held}
	if have != (ID{}) {
		req.HaveTree = have[:]
	}
	stream, err := s.store.ReadCheckout(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("reading the commit's checkout: %w", interrupted(err))
	}
	var hdr *scmpb.CheckoutHeader
	pieces := map[ID][]byte{}
	for {
		f, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// The SCM refuses a commit it cannot present as one tree: it has conflicts.
			if status.Code(err) == codes.InvalidArgument {
				return nil, &CommitError{Message: status.Convert(err).Message()}
			}
			return nil, fmt.Errorf("reading the commit's checkout: %w", interrupted(err))
		}
		switch {
		case f.GetHeader() != nil:
			hdr = f.GetHeader()
		case f.GetLayer() != nil:
			p := f.GetLayer()
			var id ID
			if len(p.GetLayerId()) != len(id) {
				return nil, errors.New("reading the commit's checkout: a layer ID is not 32 bytes")
			}
			copy(id[:], p.GetLayerId())
			pieces[id] = append(pieces[id], p.GetData()...)
			if !p.GetLast() {
				continue
			}
			b := pieces[id]
			delete(pieces, id)
			if encoding.Hash(b) != encoding.ID(id) {
				return nil, fmt.Errorf("reading the commit's checkout: layer %x does not hash to its ID", id[:])
			}
			recs, err := layer.Decode(b)
			if err != nil {
				return nil, fmt.Errorf("reading the commit's checkout: layer %x: %w", id[:], err)
			}
			d := &decodedLayer{id: id, objs: make(map[layerKey][]byte, len(recs))}
			for _, r := range recs {
				d.objs[layerKey{r.Kind, ID(r.ID)}] = r.Body
				d.bytes += len(r.Body) + 64
			}
			s.layers.put(repo, d)
		}
	}
	if hdr == nil || len(hdr.GetTreeId()) != 32 {
		return nil, errors.New("reading the commit's checkout: the SCM sent no header")
	}
	out := &checkoutOf{}
	copy(out.tree[:], hdr.GetTreeId())
	if since := hdr.GetSinceTree(); len(since) > 0 {
		if have == (ID{}) || !bytes.Equal(since, have[:]) {
			return nil, errors.New("reading the commit's checkout: it completes a tree not named")
		}
		out.since = have
	}
	for _, b := range hdr.GetLayers() {
		var id ID
		copy(id[:], b)
		l := s.layers.get(repo, id)
		if l == nil {
			return nil, fmt.Errorf("reading the commit's checkout: layer %x: %w", id[:], errLayerGone)
		}
		out.layers = append(out.layers, l)
	}
	return out, nil
}

// find is a record the source's held layers of a repository hold: the checkout's own first.
func (s *scm) find(repo int64, co *checkoutOf, kind encoding.Kind, id ID) ([]byte, bool) {
	if b, ok := co.find(kind, id); ok {
		return b, true
	}
	s.layers.mu.Lock()
	defer s.layers.mu.Unlock()
	for _, l := range s.layers.layers[repo] {
		if b, ok := l.objs[layerKey{kind, id}]; ok {
			return b, true
		}
	}
	return nil, false
}

// treeOf is a tree the held layers hold, with its entries' sizes where its sizes record is held
// too; ok is false for a tree none holds.
func (s *scm) treeOf(repo int64, co *checkoutOf, id ID) (t *encoding.Tree, sizes []uint64, ok bool, err error) {
	if encoding.ID(id) == encoding.EmptyTreeID {
		return &encoding.Tree{}, nil, true, nil
	}
	body, ok := s.find(repo, co, encoding.KindTree, id)
	if !ok {
		return nil, nil, false, nil
	}
	o, err := encoding.Decode(append([]byte{byte(encoding.KindTree)}, body...))
	if err != nil {
		return nil, nil, false, fmt.Errorf("tree %x: %w", id[:], err)
	}
	t = o.(*encoding.Tree)
	if b, ok := s.find(repo, co, encoding.KindTreeSizes, id); ok {
		if ts, err := encoding.DecodeTreeSizes(b); err == nil && len(ts.Sizes) == len(t.Entries) {
			sizes = ts.Sizes
		}
	}
	return t, sizes, true, nil
}

// leaf is an entry's leaf as an Entry at p: a file or a symlink (whose target a held layer holds).
func (s *scm) leaf(repo int64, co *checkoutOf, p string, v encoding.TreeValue, size uint64) (Entry, error) {
	switch v.Kind {
	case encoding.TreeValueFile:
		return Entry{Path: p, Kind: File, ID: ID(v.ID), Executable: v.Executable, Size: int64(size)}, nil
	case encoding.TreeValueSymlink:
		body, ok := s.find(repo, co, encoding.KindSymlink, ID(v.ID))
		if !ok {
			return Entry{}, fmt.Errorf("its checkout lacks %s's symlink", p)
		}
		o, err := encoding.Decode(append([]byte{byte(encoding.KindSymlink)}, body...))
		if err != nil {
			return Entry{}, fmt.Errorf("%s: %w", p, err)
		}
		target := o.(*encoding.Symlink).Target
		return Entry{Path: p, Kind: Symlink, Target: target, Size: int64(len(target))}, nil
	}
	return Entry{}, fmt.Errorf("%s is a %d", p, v.Kind)
}

// walk calls each for every leaf under a tree the held layers hold, in path order.
func (s *scm) walk(repo int64, co *checkoutOf, id ID, dir string, each func(Entry) error) error {
	t, sizes, ok, err := s.treeOf(repo, co, id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("its checkout lacks tree %x", id[:])
	}
	for i, e := range t.Entries {
		p := joinPath(dir, e.Name)
		if e.Value.Kind == encoding.TreeValueTree {
			if err := s.walk(repo, co, ID(e.Value.ID), p, each); err != nil {
				return err
			}
			continue
		}
		var size uint64
		if sizes != nil {
			size = sizes[i]
		}
		l, err := s.leaf(repo, co, p, e.Value, size)
		if err != nil {
			return err
		}
		if err := each(l); err != nil {
			return err
		}
	}
	return nil
}

func joinPath(dir, name string) string {
	if dir == "" {
		return name
	}
	return dir + "/" + name
}

// Entries lists a commit's leaves from its checkout.
func (s *scm) Entries(ctx context.Context, repo int64, commit []byte) ([]Entry, error) {
	co, err := s.checkout(ctx, repo, commit, ID{})
	if err != nil {
		return nil, err
	}
	var out []Entry
	if err := s.walk(repo, co, co.tree, "", func(e Entry) error { out = append(out, e); return nil }); err != nil {
		return nil, fmt.Errorf("listing the commit: %w", err)
	}
	return out, nil
}

// errNoTree is a tree to compare from that no held layer holds.
var errNoTree = errors.New("a tree no held layer holds")

// Changes compares the commit's tree with from where the held layers hold both — after this
// source's last checkout of the repository, they do — walking only the subtrees whose IDs differ;
// else it lists the commit whole.
func (s *scm) Changes(ctx context.Context, repo int64, from ID, commit []byte) (ID, []Change, bool, error) {
	var have ID
	if from != (ID{}) && s.holdsWhole(repo, from) {
		have = from
	}
	co, err := s.checkout(ctx, repo, commit, have)
	if err != nil {
		return ID{}, nil, false, err
	}
	if from != (ID{}) {
		var out []Change
		err := s.diff(repo, co, from, co.tree, "", func(c Change) { out = append(out, c) })
		switch {
		case err == nil:
			return co.tree, out, false, nil
		case !errors.Is(err, errNoTree):
			return ID{}, nil, false, fmt.Errorf("comparing the commit: %w", err)
		}
	}
	var out []Change
	if err := s.walk(repo, co, co.tree, "", func(e Entry) error { out = append(out, Change{Path: e.Path, New: &e}); return nil }); err != nil {
		return ID{}, nil, false, fmt.Errorf("listing the commit: %w", err)
	}
	return co.tree, out, true, nil
}

// diff calls each for every leaf that differs between two trees at dir: a leaf old has and new
// does not (New nil), and a leaf new has that old has not, or has other.
func (s *scm) diff(repo int64, co *checkoutOf, old, new ID, dir string, each func(Change)) error {
	if old == new {
		return nil
	}
	ot, _, ok, err := s.treeOf(repo, co, old)
	if err != nil {
		return err
	}
	if !ok {
		return errNoTree
	}
	nt, sizes, ok, err := s.treeOf(repo, co, new)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("its checkout lacks tree %x", new[:])
	}
	gone := func(v encoding.TreeValue, p string) error {
		if v.Kind == encoding.TreeValueTree {
			return s.walkAll(repo, co, ID(v.ID), p, func(p string) { each(Change{Path: p}) })
		}
		each(Change{Path: p})
		return nil
	}
	added := func(v encoding.TreeValue, p string, size uint64) error {
		if v.Kind == encoding.TreeValueTree {
			return s.walk(repo, co, ID(v.ID), p, func(e Entry) error { each(Change{Path: e.Path, New: &e}); return nil })
		}
		l, err := s.leaf(repo, co, p, v, size)
		if err != nil {
			return err
		}
		each(Change{Path: p, New: &l})
		return nil
	}
	oi, ni := 0, 0
	for oi < len(ot.Entries) || ni < len(nt.Entries) {
		switch {
		case ni == len(nt.Entries) || oi < len(ot.Entries) && ot.Entries[oi].Name < nt.Entries[ni].Name:
			if err := gone(ot.Entries[oi].Value, joinPath(dir, ot.Entries[oi].Name)); err != nil {
				return err
			}
			oi++
		case oi == len(ot.Entries) || nt.Entries[ni].Name < ot.Entries[oi].Name:
			var size uint64
			if sizes != nil {
				size = sizes[ni]
			}
			if err := added(nt.Entries[ni].Value, joinPath(dir, nt.Entries[ni].Name), size); err != nil {
				return err
			}
			ni++
		default:
			o, n, p := ot.Entries[oi].Value, nt.Entries[ni].Value, joinPath(dir, nt.Entries[ni].Name)
			var size uint64
			if sizes != nil {
				size = sizes[ni]
			}
			switch {
			case o.Kind == encoding.TreeValueTree && n.Kind == encoding.TreeValueTree:
				if err := s.diff(repo, co, ID(o.ID), ID(n.ID), p, each); err != nil {
					return err
				}
			case o.Kind == n.Kind && o.ID == n.ID && o.Executable == n.Executable:
			default:
				// A tree replaced by a leaf goes leaf by leaf; a leaf replaced by a tree goes first.
				switch {
				case o.Kind == encoding.TreeValueTree:
					if err := gone(o, p); err != nil {
						return err
					}
				case n.Kind == encoding.TreeValueTree:
					each(Change{Path: p})
				}
				if err := added(n, p, size); err != nil {
					return err
				}
			}
			oi++
			ni++
		}
	}
	return nil
}

// walkAll calls each with the path of every leaf under a tree the held layers hold.
func (s *scm) walkAll(repo int64, co *checkoutOf, id ID, dir string, each func(string)) error {
	t, _, ok, err := s.treeOf(repo, co, id)
	if err != nil {
		return err
	}
	if !ok {
		return errNoTree
	}
	for _, e := range t.Entries {
		p := joinPath(dir, e.Name)
		if e.Value.Kind == encoding.TreeValueTree {
			if err := s.walkAll(repo, co, ID(e.Value.ID), p, each); err != nil {
				return err
			}
			continue
		}
		each(p)
	}
	return nil
}

// heldFile is a file a held layer holds whole: one planning reads.
func (s *scm) heldFile(repo int64, id ID) ([]byte, bool) {
	s.layers.mu.Lock()
	defer s.layers.mu.Unlock()
	for _, l := range s.layers.layers[repo] {
		if b, ok := l.objs[layerKey{encoding.KindFile, id}]; ok {
			return b, true
		}
	}
	return nil, false
}

// Read is a file to read and, when Base is set, a version of it held whole — BaseData — which the
// SCM may send it as a delta against.
type Read struct {
	ID       ID
	Base     ID
	BaseData []byte
}

func (s *scm) ReadFiles(ctx context.Context, repo int64, ids []ID, write func(ID, []byte, bool) error) error {
	reads := make([]Read, len(ids))
	for i, id := range ids {
		reads[i] = Read{ID: id}
	}
	return s.ReadFilesFrom(ctx, repo, reads, write)
}

// maxDeltaLength bounds the file a delta makes, as the SCM bounds what it makes deltas of.
const maxDeltaLength = 4 << 20

// deltaDecoder decompresses deltas.
var deltaDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(2*maxDeltaLength))

// ReadFilesFrom reads files: those a held layer holds from it, the rest from the SCM — each with a
// base as a delta against it, where the SCM finds that smaller.
func (s *scm) ReadFilesFrom(ctx context.Context, repo int64, reads []Read, write func(ID, []byte, bool) error) error {
	req := &scmpb.ReadFilesRequest{RepositoryId: repo, Stored: true, Dictionaries: true, Deltas: true}
	bases := map[ID][]byte{}
	for _, r := range reads {
		if b, ok := s.heldFile(repo, r.ID); ok {
			if err := write(r.ID, b, true); err != nil {
				return err
			}
			continue
		}
		fr := &scmpb.FileRange{FileId: r.ID[:]}
		if r.Base != (ID{}) && r.BaseData != nil {
			fr.BaseFileId = r.Base[:]
			bases[r.Base] = r.BaseData
		}
		req.Files = append(req.Files, fr)
	}
	if len(req.Files) == 0 {
		return nil
	}
	stream, err := s.store.ReadFiles(ctx, req)
	if err != nil {
		return fmt.Errorf("reading files: %w", interrupted(err))
	}
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading files: %w", interrupted(err))
		}
		var id ID
		if len(frame.GetFileId()) != len(id) {
			return fmt.Errorf("reading files: a %d-byte file ID", len(frame.GetFileId()))
		}
		copy(id[:], frame.GetFileId())
		if frame.GetNotFound() {
			return fmt.Errorf("reading files: the SCM does not have file %x", id[:])
		}
		var data []byte
		if frame.GetCodec() == scmpb.ChunkCodec_CHUNK_CODEC_DELTA {
			if data, err = applyDelta(frame, bases); err != nil {
				return fmt.Errorf("reading files: file %x: %w", id[:], err)
			}
		} else if data, err = chunkData(frame, func(id uint32) (*zstd.Decoder, error) { return s.dictionary(ctx, repo, id) }); err != nil {
			return fmt.Errorf("reading files: file %x: %w", id[:], err)
		}
		if err := write(id, data, frame.GetLast()); err != nil {
			return err
		}
	}
}

// applyDelta makes the file a delta frame carries from the base it names, and checks it against
// the file's ID.
func applyDelta(f *scmpb.FileData, bases map[ID][]byte) ([]byte, error) {
	var base ID
	copy(base[:], f.GetBaseFileId())
	b, ok := bases[base]
	if !ok {
		return nil, fmt.Errorf("a delta against %x, which was not asked for", base[:])
	}
	if f.GetRawLength() > maxDeltaLength {
		return nil, fmt.Errorf("a delta making %d bytes, over %d", f.GetRawLength(), maxDeltaLength)
	}
	delta, err := deltaDecoder.DecodeAll(f.GetData(), nil)
	if err != nil {
		return nil, fmt.Errorf("its delta: %w", err)
	}
	data, err := encoding.ApplyDelta(b, delta, uint64(f.GetRawLength()))
	if err != nil {
		return nil, err
	}
	h := encoding.NewHasher(encoding.KindFile)
	h.Write(data)
	if id := h.Sum(); !bytes.Equal(id[:], f.GetFileId()) {
		return nil, fmt.Errorf("the file its delta makes does not hash to its ID")
	}
	return data, nil
}
