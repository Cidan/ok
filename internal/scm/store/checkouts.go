package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/layer"
	"github.com/Cidan/ok/internal/serve/metrics"
)

// Checkouts (docs/scm.md, "Checkouts"): a commit's checkout — every tree under it with its sizes
// record, its symlinks and copy histories, and the files planning reads — derived once, kept in the
// bucket as layers, and read in one request. A checkout is its parent's layers and one more holding
// what the commit changed, the top layers merged so each is at least twice the one above it — the
// way git's commit-graph chains and LSM trees merge — and one layer of everything when the rest
// outweigh it; layers are immutable and named by their bytes' hash, so a client holding its last
// checkout's layers is sent only those it lacks, usually the small top one. Main's head is derived
// as main moves (CheckoutWork); any other commit — a patchset, a candidate — the first time a
// checkout of it is asked for.

// CheckoutWork is the keyed work that derives the checkout of a repository's main as it moves.
const CheckoutWork = "checkout"

const (
	// checkoutMaxLayers bounds a checkout's layers: past it, the top ones merge however small.
	checkoutMaxLayers = 16
	// checkoutDepth bounds the ancestors a derivation derives first, to stack its layer on theirs.
	checkoutDepth = 4
	// CheckoutRetention is how long a checkout no longer of main's head is kept.
	CheckoutRetention = 14 * 24 * time.Hour
	// checkoutLayerGrace is how long a layer no kept checkout names is kept: one a derivation has
	// written and not yet named.
	checkoutLayerGrace = 24 * time.Hour
	// plannedMaxBytes bounds the files planning reads a layer holds: a larger one is read as
	// any file is.
	plannedMaxBytes = 1 << 20
	// layerPiece bounds a message of ReadCheckout.
	layerPiece = 1 << 20
)

// checkoutsDerived times each checkout's derivation, by what it made: a layer stacked on its
// parent's, top layers merged too, none (its parent's tree), or one layer of the whole.
var checkoutsDerived = metrics.NewHistogram("ok_scm_checkouts_derived_seconds",
	"How long each checkout's derivation took, by what it made: a layer on its parent's (stacked), its top layers merged too (merged), none (same), or a whole one.",
	metrics.Seconds, "kind")

// PlanningReads are the globs of the files planning reads — the build graph Bazel loads, the
// repository's settings, its workflow files — which a checkout holds: `*` stays within a name, `**`
// crosses directories, as okfs's read-ahead globs.
var PlanningReads = []string{"MODULE.bazel", "MODULE.bazel.lock", ".bazelrc", ".bazelversion", "**/BUILD", "**/BUILD.bazel", "**/*.bzl",
	".ok/repository.yaml", "**/.ok/workflow/*.yaml"}

// LayerRef is a layer of a checkout: its ID, its size, and the tree it and the layers below it
// hold the checkout of.
type LayerRef struct {
	ID   encoding.ID
	Size int64
	Tree encoding.ID
}

// Checkout is a commit's checkout: its tree, resolved, and its layers, oldest first.
type Checkout struct {
	Tree   encoding.ID
	Layers []LayerRef
}

func checkoutsPrefix(repo int64) string { return "checkouts/" + strconv.FormatInt(repo, 10) + "/" }

func checkoutKey(repo int64, commit encoding.ID) string {
	return checkoutsPrefix(repo) + "c/" + hex.EncodeToString(commit[:])
}

func layerKey(repo int64, id encoding.ID) string {
	return checkoutsPrefix(repo) + "l/" + hex.EncodeToString(id[:])
}

// A checkout as kept: "OKCHKMAN", version 1, its tree, its layers' count (uvarint), and each
// layer's ID, tree, and size (uvarint).
const checkoutMagic = "OKCHKMAN"

func encodeCheckout(c Checkout) []byte {
	out := append([]byte(checkoutMagic), 1)
	out = append(out, c.Tree[:]...)
	out = binary.AppendUvarint(out, uint64(len(c.Layers)))
	for _, l := range c.Layers {
		out = append(out, l.ID[:]...)
		out = append(out, l.Tree[:]...)
		out = binary.AppendUvarint(out, uint64(l.Size))
	}
	return out
}

func decodeCheckout(b []byte) (Checkout, error) {
	var c Checkout
	if len(b) < len(checkoutMagic)+1+encoding.IDLen || string(b[:len(checkoutMagic)]) != checkoutMagic || b[len(checkoutMagic)] != 1 {
		return c, errors.New("not a version 1 checkout")
	}
	b = b[len(checkoutMagic)+1:]
	copy(c.Tree[:], b)
	b = b[encoding.IDLen:]
	n, k := binary.Uvarint(b)
	if k <= 0 || n > checkoutMaxLayers {
		return c, errors.New("a checkout's layers are not counted")
	}
	b = b[k:]
	for range n {
		if len(b) < 2*encoding.IDLen {
			return c, errors.New("a checkout's layers are truncated")
		}
		var l LayerRef
		copy(l.ID[:], b)
		copy(l.Tree[:], b[encoding.IDLen:])
		size, k := binary.Uvarint(b[2*encoding.IDLen:])
		if k <= 0 {
			return c, errors.New("a checkout's layers are truncated")
		}
		l.Size = int64(size)
		b = b[2*encoding.IDLen+k:]
		c.Layers = append(c.Layers, l)
	}
	if len(b) != 0 {
		return c, errors.New("bytes after a checkout's layers")
	}
	return c, nil
}

// singleflight runs one derivation of a key at a time on a replica: a caller asking for one under
// way waits for it and shares its answer.
type singleflight struct {
	mu    sync.Mutex
	calls map[string]*flight
}

type flight struct {
	done chan struct{}
	co   Checkout
	err  error
}

func (g *singleflight) do(key string, fn func() (Checkout, error)) (Checkout, error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = map[string]*flight{}
	}
	if f, ok := g.calls[key]; ok {
		g.mu.Unlock()
		<-f.done
		return f.co, f.err
	}
	f := &flight{done: make(chan struct{})}
	g.calls[key] = f
	g.mu.Unlock()
	f.co, f.err = fn()
	close(f.done)
	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	return f.co, f.err
}

// Checkout returns a commit's checkout, deriving it — and up to checkoutDepth of its ancestors,
// to stack on theirs — when it has none. A commit with conflicts has none.
func (s *Store) Checkout(ctx context.Context, repo int64, commit encoding.ID) (Checkout, error) {
	return s.checkoutAt(ctx, repo, commit, 0)
}

func (s *Store) checkoutAt(ctx context.Context, repo int64, commit encoding.ID, depth int) (Checkout, error) {
	if commit == encoding.RootCommitID {
		return Checkout{Tree: encoding.EmptyTreeID}, nil
	}
	if c, ok, err := s.storedCheckout(ctx, repo, commit); err != nil || ok {
		return c, err
	}
	return s.deriving.do(checkoutKey(repo, commit), func() (Checkout, error) {
		if c, ok, err := s.storedCheckout(ctx, repo, commit); err != nil || ok {
			return c, err
		}
		return s.deriveCheckout(ctx, repo, commit, depth)
	})
}

// storedCheckout reads a commit's checkout as kept: from memory, or the bucket.
func (s *Store) storedCheckout(ctx context.Context, repo int64, commit encoding.ID) (Checkout, bool, error) {
	key := checkoutKey(repo, commit)
	if c, ok := s.checkouts.get(key); ok {
		return c, true, nil
	}
	var buf bytes.Buffer
	if _, err := s.blobs.Download(ctx, key, &buf); errors.Is(err, blob.ErrNotFound) {
		return Checkout{}, false, nil
	} else if err != nil {
		return Checkout{}, false, err
	}
	c, err := decodeCheckout(buf.Bytes())
	if err != nil {
		return Checkout{}, false, fmt.Errorf("checkout %s: %w", key, err)
	}
	s.checkouts.put(key, c, buf.Len())
	return c, true, nil
}

// deriveCheckout derives a commit's checkout and keeps it: on its first parent's layers
// (stackLayers), or one layer of the whole.
func (s *Store) deriveCheckout(ctx context.Context, repo int64, commit encoding.ID, depth int) (Checkout, error) {
	start := time.Now()
	tree, err := s.resolvedTree(ctx, repo, commit)
	if err != nil {
		return Checkout{}, err
	}
	c, err := s.readCommit(ctx, repo, commit)
	if err != nil {
		return Checkout{}, err
	}
	var base *Checkout
	if len(c.Parents) > 0 && c.Parents[0] != encoding.RootCommitID {
		p := c.Parents[0]
		if pc, ok, err := s.storedCheckout(ctx, repo, p); err != nil {
			return Checkout{}, err
		} else if ok {
			base = &pc
		} else if depth < checkoutDepth {
			// A parent with no checkout of its own — a stack's, or one with conflicts — leaves this
			// one whole.
			if pc, err := s.checkoutAt(ctx, repo, p, depth+1); err == nil {
				base = &pc
			} else if ctx.Err() != nil {
				return Checkout{}, err
			}
		}
	}
	out := Checkout{Tree: tree}
	kind := "whole"
	switch {
	case base != nil && base.Tree == tree:
		out.Layers, kind = slices.Clone(base.Layers), "same"
	case base != nil && len(base.Layers) > 0:
		if out.Layers, kind, err = s.stackLayers(ctx, repo, tree, base.Layers); err != nil {
			return Checkout{}, err
		}
	case tree != encoding.EmptyTreeID:
		if out.Layers, err = s.wholeLayer(ctx, repo, tree); err != nil {
			return Checkout{}, err
		}
	}
	b := encodeCheckout(out)
	key := checkoutKey(repo, commit)
	if err := s.blobs.Put(ctx, key, bytes.NewReader(b), int64(len(b))); err != nil {
		return Checkout{}, err
	}
	s.checkouts.put(key, out, len(b))
	checkoutsDerived.With(kind).Since(start)
	return out, nil
}

// stackLayers are a checkout of tree on below, a checkout of another tree: below and a layer of
// what tree changes from below's top, then the top layers merged — each replaced, with the one under
// it, by one layer of what tree changes from the tree under both — until each is more than twice the
// one above it and there are at most checkoutMaxLayers. When a merge would take the whole layer at
// the bottom in, or the layers above it outweigh half of it, the checkout is one layer of the whole.
func (s *Store) stackLayers(ctx context.Context, repo int64, tree encoding.ID, below []LayerRef) ([]LayerRef, string, error) {
	layers := slices.Clone(below)
	from := func(k int) (LayerRef, error) {
		recs, err := s.layerRecords(ctx, repo, tree, layers[k].Tree)
		if err != nil {
			return LayerRef{}, err
		}
		return s.putLayer(ctx, repo, recs, tree)
	}
	top, err := from(len(layers) - 1)
	if err != nil {
		return nil, "", err
	}
	layers = append(layers, top)
	kind := "stacked"
	for n := len(layers); n > 1 && (layers[n-2].Size <= 2*layers[n-1].Size || n > checkoutMaxLayers); n = len(layers) {
		if n == 2 {
			whole, err := s.wholeLayer(ctx, repo, tree)
			return whole, "whole", err
		}
		merged, err := from(n - 3)
		if err != nil {
			return nil, "", err
		}
		layers = append(layers[:n-2], merged)
		kind = "merged"
	}
	var above int64
	for _, l := range layers[1:] {
		above += l.Size
	}
	if above > layers[0].Size/2 {
		whole, err := s.wholeLayer(ctx, repo, tree)
		return whole, "whole", err
	}
	return layers, kind, nil
}

// wholeLayer is a checkout of tree as one layer of everything.
func (s *Store) wholeLayer(ctx context.Context, repo int64, tree encoding.ID) ([]LayerRef, error) {
	recs, err := s.layerRecords(ctx, repo, tree, encoding.ID{})
	if err != nil {
		return nil, err
	}
	ref, err := s.putLayer(ctx, repo, recs, tree)
	if err != nil {
		return nil, err
	}
	return []LayerRef{ref}, nil
}

// putLayer writes a layer of records — of tree's checkout, with the layers below it — to the
// bucket, unless it is there, and keeps it.
func (s *Store) putLayer(ctx context.Context, repo int64, recs []encoding.LayerRecord, tree encoding.ID) (LayerRef, error) {
	b, id := layer.Encode(recs)
	key := layerKey(repo, id)
	if err := s.blobs.Put(ctx, key, bytes.NewReader(b), int64(len(b)), blob.IfAbsent()); err != nil && !errors.Is(err, blob.ErrExists) {
		return LayerRef{}, err
	}
	s.layers.put(key, b, len(b))
	return LayerRef{ID: id, Size: int64(len(b)), Tree: tree}, nil
}

// Layer returns a layer's bytes: from memory, or the bucket, checked against its ID.
func (s *Store) Layer(ctx context.Context, repo int64, id encoding.ID) ([]byte, error) {
	key := layerKey(repo, id)
	if b, ok := s.layers.get(key); ok {
		return b, nil
	}
	var buf bytes.Buffer
	if _, err := s.blobs.Download(ctx, key, &buf); err != nil {
		return nil, err
	}
	if encoding.Hash(buf.Bytes()) != id {
		return nil, fmt.Errorf("checkout layer %s does not hash to its ID", id)
	}
	s.layers.put(key, buf.Bytes(), buf.Len())
	return buf.Bytes(), nil
}

// sinceLayer is one layer of what tree changes from have — for a client holding have's checkout,
// made once on a replica and kept in memory by the pair — and false when have is no tree the
// repository holds.
func (s *Store) sinceLayer(ctx context.Context, repo int64, have, tree encoding.ID) ([]byte, encoding.ID, bool, error) {
	key := checkoutsPrefix(repo) + "since/" + hex.EncodeToString(have[:]) + "/" + hex.EncodeToString(tree[:])
	if b, ok := s.layers.get(key); ok {
		return b, encoding.Hash(b), true, nil
	}
	recs, err := s.layerRecords(ctx, repo, tree, have)
	if errors.Is(err, ErrNotFound) {
		return nil, encoding.ID{}, false, nil
	}
	if err != nil {
		return nil, encoding.ID{}, false, err
	}
	b, id := layer.Encode(recs)
	s.layers.put(key, b, len(b))
	checkoutsSince.Observe(float64(len(b)))
	return b, id, true, nil
}

// checkoutsSince sizes the layers made for clients moving from a tree they hold.
var checkoutsSince = metrics.NewHistogram("ok_scm_checkouts_since_bytes",
	"The bytes of each layer made for a client moving from a tree it holds: what the commit changes from it.", metrics.Bytes)

// ReadCheckout answers a commit's checkout: header with its tree and layers, then each layer
// held does not name, in pieces. Named a tree whose checkout the client holds (have), it answers
// one layer of what the commit's tree changes from it instead, the header naming have as what the
// layer completes — none at all for the same tree; have no tree of the repository's, it answers
// the checkout. A layer collection took since this replica read the checkout derives it again.
func (s *Store) ReadCheckout(ctx context.Context, repo int64, commit encoding.ID, held []encoding.ID, have encoding.ID,
	header func(co Checkout, since encoding.ID) error, piece func(id encoding.ID, data []byte, last bool) error) error {
	send := func(id encoding.ID, b []byte) error {
		for off := 0; off < len(b); off += layerPiece {
			end := min(off+layerPiece, len(b))
			if err := piece(id, b[off:end], end == len(b)); err != nil {
				return err
			}
		}
		return nil
	}
	if have != (encoding.ID{}) {
		tree, err := s.resolvedTree(ctx, repo, commit)
		if err != nil {
			return err
		}
		if tree == have {
			return header(Checkout{Tree: tree}, have)
		}
		// A kept checkout whose every layer the client holds costs nothing to send.
		if co, ok, err := s.storedCheckout(ctx, repo, commit); err != nil {
			return err
		} else if ok && !slices.ContainsFunc(co.Layers, func(l LayerRef) bool { return !slices.Contains(held, l.ID) }) {
			return header(co, encoding.ID{})
		}
		b, id, ok, err := s.sinceLayer(ctx, repo, have, tree)
		if err != nil {
			return err
		}
		if ok {
			if err := header(Checkout{Tree: tree, Layers: []LayerRef{{ID: id, Size: int64(len(b)), Tree: tree}}}, have); err != nil {
				return err
			}
			if slices.Contains(held, id) {
				return nil
			}
			return send(id, b)
		}
	}
	co, err := s.Checkout(ctx, repo, commit)
	if err != nil {
		return err
	}
	var bodies [][]byte
	for attempt := 0; ; attempt++ {
		bodies = bodies[:0]
		var missing error
		for _, l := range co.Layers {
			if slices.Contains(held, l.ID) {
				bodies = append(bodies, nil)
				continue
			}
			b, err := s.Layer(ctx, repo, l.ID)
			if errors.Is(err, blob.ErrNotFound) {
				missing = err
				break
			}
			if err != nil {
				return err
			}
			bodies = append(bodies, b)
		}
		if missing == nil {
			break
		}
		if attempt > 0 {
			return missing
		}
		if co, err = s.deriving.do(checkoutKey(repo, commit), func() (Checkout, error) {
			return s.deriveCheckout(ctx, repo, commit, checkoutDepth)
		}); err != nil {
			return err
		}
	}
	if err := header(co, encoding.ID{}); err != nil {
		return err
	}
	for i, l := range co.Layers {
		if err := send(l.ID, bodies[i]); err != nil {
			return err
		}
	}
	return nil
}

// DeriveMainCheckout derives the checkout of a repository's main head: CheckoutWork's.
func (s *Store) DeriveMainCheckout(ctx context.Context, repo int64) error {
	r, err := s.GetRepository(ctx, repo)
	if err != nil {
		return err
	}
	_, err = s.Checkout(ctx, repo, r.Head)
	return err
}

// layerRecords are the records of tree's checkout that base's does not hold at the same path — all
// of them, with no base: each tree whose ID differs from the base's tree there, with its sizes; the
// symlinks and copy histories those name that the base's tree there does not; and the files
// planning reads, of at most plannedMaxBytes, that are new where they are. Trees are read a level
// at a time.
func (s *Store) layerRecords(ctx context.Context, repo int64, tree, base encoding.ID) ([]encoding.LayerRecord, error) {
	type node struct {
		path       string
		id, baseID encoding.ID
	}
	var recs []encoding.LayerRecord
	held := map[encoding.ID]bool{}
	var files []encoding.ID
	wantFile := map[encoding.ID]bool{}
	var level []node
	if tree != base && tree != encoding.EmptyTreeID {
		level = []node{{id: tree, baseID: base}}
	}
	for len(level) > 0 {
		var ids []encoding.ID
		want := map[encoding.ID]bool{}
		for _, n := range level {
			for _, id := range []encoding.ID{n.id, n.baseID} {
				if id != (encoding.ID{}) && id != encoding.EmptyTreeID && !want[id] {
					want[id] = true
					ids = append(ids, id)
				}
			}
		}
		bodies := map[encoding.ID][]byte{}
		trees := map[encoding.ID]*encoding.Tree{encoding.EmptyTreeID: {}}
		for start := 0; start < len(ids); start += treeBatch {
			batch := ids[start:min(start+treeBatch, len(ids))]
			refs := make([]ObjectRef, len(batch))
			for i, id := range batch {
				refs[i] = ObjectRef{Kind: encoding.KindTree, ID: id}
			}
			res, err := s.ReadObjects(ctx, repo, refs)
			if err != nil {
				return nil, err
			}
			for _, r := range res {
				if !r.Found {
					return nil, notFound("%s", r.Ref)
				}
				o, err := decodeStored(encoding.KindTree, r.Ref.ID, r.Body)
				if err != nil {
					return nil, err
				}
				bodies[r.Ref.ID], trees[r.Ref.ID] = r.Body, o.(*encoding.Tree)
			}
		}
		var fresh []encoding.ID
		for _, n := range level {
			if !held[n.id] {
				held[n.id] = true
				fresh = append(fresh, n.id)
				recs = append(recs, encoding.LayerRecord{Kind: encoding.KindTree, ID: n.id, Body: bodies[n.id]})
			}
		}
		sizes, err := s.TreeSizes(ctx, repo, fresh)
		if err != nil {
			return nil, err
		}
		for _, id := range fresh {
			b, ok := sizes[id]
			if !ok {
				return nil, notFound("tree %s's sizes", id)
			}
			recs = append(recs, encoding.LayerRecord{Kind: encoding.KindTreeSizes, ID: id, Body: b})
		}
		var leaves []ObjectRef
		var next []node
		for _, n := range level {
			was := map[string]encoding.TreeValue{}
			if n.baseID != (encoding.ID{}) {
				for _, e := range trees[n.baseID].Entries {
					was[e.Name] = e.Value
				}
			}
			for _, e := range trees[n.id].Entries {
				p := joinPath(n.path, e.Name)
				old, inBase := was[e.Name]
				v := e.Value
				switch v.Kind {
				case encoding.TreeValueTree:
					if v.ID == encoding.EmptyTreeID || inBase && old.Kind == v.Kind && old.ID == v.ID {
						continue
					}
					var b encoding.ID
					if inBase && old.Kind == encoding.TreeValueTree {
						b = old.ID
					}
					next = append(next, node{path: p, id: v.ID, baseID: b})
				case encoding.TreeValueSymlink:
					if !(inBase && old.Kind == v.Kind && old.ID == v.ID) {
						leaves = append(leaves, ObjectRef{Kind: encoding.KindSymlink, ID: v.ID})
					}
				case encoding.TreeValueFile:
					if v.CopyID != nil && !(inBase && old.Kind == v.Kind && old.CopyID != nil && *old.CopyID == *v.CopyID) {
						leaves = append(leaves, ObjectRef{Kind: encoding.KindCopyHistory, ID: *v.CopyID})
					}
					if planned(p) && !(inBase && old.Kind == v.Kind && old.ID == v.ID) && !wantFile[v.ID] {
						wantFile[v.ID] = true
						files = append(files, v.ID)
					}
				}
			}
		}
		slices.SortFunc(leaves, func(a, b ObjectRef) int { return bytes.Compare(a.ID[:], b.ID[:]) })
		leaves = slices.Compact(leaves)
		if len(leaves) > 0 {
			res, err := s.ReadObjects(ctx, repo, leaves)
			if err != nil {
				return nil, err
			}
			for _, r := range res {
				if !r.Found {
					return nil, notFound("%s", r.Ref)
				}
				recs = append(recs, encoding.LayerRecord{Kind: r.Ref.Kind, ID: r.Ref.ID, Body: r.Body})
			}
		}
		level = next
	}
	if len(files) > 0 {
		planned, err := s.plannedFiles(ctx, repo, files)
		if err != nil {
			return nil, err
		}
		recs = append(recs, planned...)
	}
	return recs, nil
}

// plannedFiles reads files whole as layer records, but those over plannedMaxBytes.
func (s *Store) plannedFiles(ctx context.Context, repo int64, files []encoding.ID) ([]encoding.LayerRecord, error) {
	ms, err := s.manifests(ctx, repo, files)
	if err != nil {
		return nil, err
	}
	var ranges []FileRange
	for _, id := range files {
		if m := ms[id]; m != nil && m.size <= plannedMaxBytes {
			ranges = append(ranges, FileRange{FileID: id})
		}
	}
	data := make([][]byte, len(ranges))
	if err := s.ReadFiles(ctx, repo, ranges, RawChunks, func(f FileFrame) error {
		if f.NotFound {
			return notFound("file %s", f.FileID)
		}
		data[f.Range] = append(data[f.Range], f.Data...)
		return nil
	}); err != nil {
		return nil, err
	}
	out := make([]encoding.LayerRecord, len(ranges))
	for i, r := range ranges {
		out[i] = encoding.LayerRecord{Kind: encoding.KindFile, ID: r.FileID, Body: data[i]}
		if data[i] == nil {
			out[i].Body = []byte{}
		}
	}
	return out, nil
}

// planned reports whether planning reads a path (PlanningReads).
func planned(p string) bool {
	for _, g := range PlanningReads {
		if globMatch(g, p) {
			return true
		}
	}
	return false
}

// globMatch reports whether a repository-relative path matches a glob: `*` and the like within a
// name (path.Match), and `**` any number of directories, none included.
func globMatch(glob, p string) bool {
	return matchNames(strings.Split(glob, "/"), strings.Split(p, "/"))
}

func matchNames(glob, names []string) bool {
	for len(glob) > 0 {
		if glob[0] == "**" {
			for i := 0; i <= len(names); i++ {
				if matchNames(glob[1:], names[i:]) {
					return true
				}
			}
			return false
		}
		if len(names) == 0 {
			return false
		}
		if ok, _ := path.Match(glob[0], names[0]); !ok {
			return false
		}
		glob, names = glob[1:], names[1:]
	}
	return len(names) == 0
}

// dropCheckouts deletes every checkout of a repository, and their layers: it is deleted, or moved to
// another cell.
func (s *Store) dropCheckouts(ctx context.Context, repo int64) error {
	all, err := s.blobs.List(ctx, checkoutsPrefix(repo))
	if err != nil {
		return err
	}
	keys := make([]string, len(all))
	for i, o := range all {
		keys[i] = o.Key
	}
	return s.blobs.Delete(ctx, keys...)
}

// pruneCheckouts deletes the checkouts of a repository's commits older than CheckoutRetention but
// its main head's, then the layers no kept checkout names, older than checkoutLayerGrace. It returns
// how many of each it deleted (or, dry, would).
func (s *Store) pruneCheckouts(ctx context.Context, repo int64, now time.Time, dry bool) (checkouts, layers int, err error) {
	r, err := s.GetRepository(ctx, repo)
	if err != nil {
		return 0, 0, err
	}
	head := checkoutKey(repo, r.Head)
	kept, err := s.blobs.List(ctx, checkoutsPrefix(repo)+"c/")
	if err != nil {
		return 0, 0, err
	}
	named := map[string]bool{}
	var gone []string
	for _, o := range kept {
		if o.Key != head && now.Sub(o.Modified) > CheckoutRetention {
			gone = append(gone, o.Key)
			continue
		}
		var buf bytes.Buffer
		if _, err := s.blobs.Download(ctx, o.Key, &buf); errors.Is(err, blob.ErrNotFound) {
			continue
		} else if err != nil {
			return 0, 0, err
		}
		c, err := decodeCheckout(buf.Bytes())
		if err != nil {
			return 0, 0, fmt.Errorf("checkout %s: %w", o.Key, err)
		}
		for _, l := range c.Layers {
			named[layerKey(repo, l.ID)] = true
		}
	}
	all, err := s.blobs.List(ctx, checkoutsPrefix(repo)+"l/")
	if err != nil {
		return 0, 0, err
	}
	var unnamed []string
	for _, o := range all {
		if !named[o.Key] && now.Sub(o.Modified) > checkoutLayerGrace {
			unnamed = append(unnamed, o.Key)
		}
	}
	if !dry {
		if err := s.blobs.Delete(ctx, gone...); err != nil {
			return 0, 0, err
		}
		if err := s.blobs.Delete(ctx, unnamed...); err != nil {
			return len(gone), 0, err
		}
	}
	return len(gone), len(unnamed), nil
}
