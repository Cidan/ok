package store

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/graph"
	"github.com/Cidan/ok/internal/serve/metrics"
)

// The commit graph (docs/scm.md, "The commit graph"): what a client's index of a repository's
// commits needs of each — its ID, change ID, generation, committer's time, tree, and parents —
// without its body, so a clone indexes history without fetching every commit, and reads one's
// description, author, and the rest only when a command looks at it. Main's history is derived as
// it moves (GraphWork) into layers in the bucket, immutable, named by their bytes' hash, and merged
// geometrically, as checkouts' are: the newest is the smallest, each more than twice the one above
// it, so a client that indexed main once is sent only the layers since. One manifest per repository
// names main's layers and the head they reach; the rest of what a client asks for — main since that
// head, and every commit not on main its operation log references — is walked and sent as it is.

// GraphWork is the keyed work that derives the graph of a repository's main as it moves.
const GraphWork = "graph"

const (
	// graphMaxLayers bounds main's layers: past it, the top ones merge however small.
	graphMaxLayers = 16
	// graphLayerGrace is how long a layer the manifest no longer names is kept, for a reader that
	// read the manifest before it was replaced.
	graphLayerGrace = 24 * time.Hour
	// graphRecordsPiece is how many records ReadGraph sends a message.
	graphRecordsPiece = 4096
	// graphBodiesBatch is how many commits' bodies one read takes, to make their records.
	graphBodiesBatch = 256
)

// graphsDerived times each derivation of main's graph, by what it did: a layer on the last
// (stacked), the top layers merged too (merged), or nothing new (same).
var graphsDerived = metrics.NewHistogram("ok_scm_graphs_derived_seconds",
	"How long each derivation of main's commit graph took, by what it made: a layer on the last (stacked), the top layers merged too (merged), or none (same).",
	metrics.Seconds, "kind")

// GraphLayerRef is a layer of main's graph: its ID, its size, and how many commits it holds.
type GraphLayerRef struct {
	ID      encoding.ID
	Size    int64
	Commits int64
}

// MainGraph is main's graph as derived: the head it reaches, and its layers, oldest first, which
// together hold every commit reachable from it.
type MainGraph struct {
	Top    encoding.ID
	Layers []GraphLayerRef
}

func graphsPrefix(repo int64) string { return "graphs/" + strconv.FormatInt(repo, 10) + "/" }

func mainGraphKey(repo int64) string { return graphsPrefix(repo) + "main" }

func graphLayerKey(repo int64, id encoding.ID) string {
	return graphsPrefix(repo) + "l/" + hex.EncodeToString(id[:])
}

// Main's graph as kept: "OKGRAPHM", version 1, the head it reaches, its layers' count (uvarint),
// and each layer's ID, size (uvarint), and commits (uvarint).
const mainGraphMagic = "OKGRAPHM"

// mainGraphVersion is the manifest's version this release writes.
const mainGraphVersion = 1

// errNewerGraph is a manifest a later release wrote, naming layers this release cannot read: it is
// as if there were none, and this release derives main's graph again, as it writes it.
var errNewerGraph = errors.New("main's graph is a later release's")

func encodeMainGraph(g MainGraph) []byte {
	out := append([]byte(mainGraphMagic), mainGraphVersion)
	out = append(out, g.Top[:]...)
	out = binary.AppendUvarint(out, uint64(len(g.Layers)))
	for _, l := range g.Layers {
		out = append(out, l.ID[:]...)
		out = binary.AppendUvarint(out, uint64(l.Size))
		out = binary.AppendUvarint(out, uint64(l.Commits))
	}
	return out
}

func decodeMainGraph(b []byte) (MainGraph, error) {
	var g MainGraph
	if len(b) < len(mainGraphMagic)+1 || string(b[:len(mainGraphMagic)]) != mainGraphMagic {
		return g, errors.New("not a main graph")
	}
	if v := b[len(mainGraphMagic)]; v != mainGraphVersion {
		if v > mainGraphVersion {
			return g, errNewerGraph
		}
		return g, fmt.Errorf("a main graph of version %d", v)
	}
	if len(b) < len(mainGraphMagic)+1+encoding.IDLen {
		return g, errors.New("a main graph is truncated")
	}
	b = b[len(mainGraphMagic)+1:]
	copy(g.Top[:], b)
	b = b[encoding.IDLen:]
	n, k := binary.Uvarint(b)
	if k <= 0 || n > graphMaxLayers+1 {
		return g, errors.New("a main graph's layers are not counted")
	}
	b = b[k:]
	for range n {
		if len(b) < encoding.IDLen {
			return g, errors.New("a main graph's layers are truncated")
		}
		var l GraphLayerRef
		copy(l.ID[:], b)
		b = b[encoding.IDLen:]
		size, k := binary.Uvarint(b)
		if k <= 0 {
			return g, errors.New("a main graph's layers are truncated")
		}
		b = b[k:]
		commits, k := binary.Uvarint(b)
		if k <= 0 {
			return g, errors.New("a main graph's layers are truncated")
		}
		b = b[k:]
		l.Size, l.Commits = int64(size), int64(commits)
		g.Layers = append(g.Layers, l)
	}
	if len(b) != 0 {
		return g, errors.New("bytes after a main graph's layers")
	}
	return g, nil
}

// mainGraph reads main's graph as kept, from the bucket: it changes as main moves, so it is never
// held in memory. False when none was derived yet; errNewerGraph when a later release wrote it.
func (s *Store) mainGraph(ctx context.Context, repo int64) (MainGraph, bool, error) {
	var buf bytes.Buffer
	if _, err := s.blobs.Download(ctx, mainGraphKey(repo), &buf); errors.Is(err, blob.ErrNotFound) {
		return MainGraph{}, false, nil
	} else if err != nil {
		return MainGraph{}, false, err
	}
	g, err := decodeMainGraph(buf.Bytes())
	if errors.Is(err, errNewerGraph) {
		return MainGraph{}, false, err
	}
	if err != nil {
		return MainGraph{}, false, fmt.Errorf("%s: %w", mainGraphKey(repo), err)
	}
	return g, true, nil
}

// graphRecords are the records of every commit the repository holds that is reachable from want
// and not from have, in topological order: by generation, then ID. Each is made from its commit's
// body, read graphBodiesBatch at a time.
func (s *Store) graphRecords(ctx context.Context, repo int64, want, have []encoding.ID) ([]encoding.GraphRecord, error) {
	type node struct {
		id  encoding.ID
		gen int64
	}
	var nodes []node
	if err := s.walkWanted(ctx, repo, want, have, func(id encoding.ID, gen int64) (bool, error) {
		nodes = append(nodes, node{id, gen})
		return false, nil
	}); err != nil {
		return nil, err
	}
	slices.SortFunc(nodes, func(a, b node) int {
		if c := cmp.Compare(a.gen, b.gen); c != 0 {
			return c
		}
		return bytes.Compare(a.id[:], b.id[:])
	})
	out := make([]encoding.GraphRecord, 0, len(nodes))
	for start := 0; start < len(nodes); start += graphBodiesBatch {
		batch := nodes[start:min(start+graphBodiesBatch, len(nodes))]
		refs := make([]ObjectRef, len(batch))
		for i, n := range batch {
			refs[i] = ObjectRef{Kind: encoding.KindCommit, ID: n.id}
		}
		res, err := s.ReadObjects(ctx, repo, refs)
		if err != nil {
			return nil, err
		}
		for i, r := range res {
			if !r.Found {
				return nil, fmt.Errorf("commit %s: %w", r.Ref.ID, ErrNotFound)
			}
			o, err := decodeStored(encoding.KindCommit, r.Ref.ID, r.Body)
			if err != nil {
				return nil, err
			}
			c := o.(*encoding.Commit)
			out = append(out, encoding.GraphRecord{
				ID: r.Ref.ID, ChangeID: bytes.Clone(c.ChangeID[:]), Generation: uint64(batch[i].gen),
				Committed: c.Committer.Time, Tree: c.RootTree, Parents: c.Parents,
			})
		}
	}
	return out, nil
}

// putGraphLayer writes a layer of records to the bucket, unless it is there, and keeps its bytes.
func (s *Store) putGraphLayer(ctx context.Context, repo int64, recs []encoding.GraphRecord) (GraphLayerRef, error) {
	b, id := graph.Encode(recs)
	key := graphLayerKey(repo, id)
	if err := s.blobs.Put(ctx, key, bytes.NewReader(b), int64(len(b)), blob.IfAbsent()); err != nil && !errors.Is(err, blob.ErrExists) {
		return GraphLayerRef{}, err
	}
	s.layers.put(key, b, len(b))
	return GraphLayerRef{ID: id, Size: int64(len(b)), Commits: int64(len(recs))}, nil
}

// GraphLayer returns a layer of main's graph: from memory, or the bucket, checked against its ID.
func (s *Store) GraphLayer(ctx context.Context, repo int64, id encoding.ID) ([]byte, error) {
	key := graphLayerKey(repo, id)
	if b, ok := s.layers.get(key); ok {
		return b, nil
	}
	var buf bytes.Buffer
	if _, err := s.blobs.Download(ctx, key, &buf); err != nil {
		return nil, err
	}
	if encoding.Hash(buf.Bytes()) != id {
		return nil, fmt.Errorf("graph layer %s does not hash to its ID", id)
	}
	s.layers.put(key, buf.Bytes(), buf.Len())
	return buf.Bytes(), nil
}

// DeriveMainGraph derives the graph of a repository's main as far as its head: GraphWork's.
func (s *Store) DeriveMainGraph(ctx context.Context, repo int64) error {
	_, err := s.deriveMainGraph(ctx, repo, false)
	return err
}

// deriveMainGraph derives main's graph as far as its head and returns it as kept. The commits since
// the head the kept graph reaches become a layer on its layers, and the top layers merge — each pair
// into one of both's records — until each is more than twice the one above it and there are at most
// graphMaxLayers. Main's first graph, a graph a later release wrote, one with a layer gone, and one
// derived whole are one layer of all of main's history.
func (s *Store) deriveMainGraph(ctx context.Context, repo int64, whole bool) (MainGraph, error) {
	start := time.Now()
	r, err := s.GetRepository(ctx, repo)
	if err != nil {
		return MainGraph{}, err
	}
	if r.Head == encoding.RootCommitID {
		return MainGraph{}, nil
	}
	kept, ok, err := s.mainGraph(ctx, repo)
	if errors.Is(err, errNewerGraph) {
		ok, err = false, nil
	}
	if err != nil {
		return MainGraph{}, err
	}
	if whole {
		ok = false
	}
	if ok && kept.Top == r.Head {
		return kept, nil
	}
	var have []encoding.ID
	if ok {
		have = []encoding.ID{kept.Top}
	}
	recs, err := s.graphRecords(ctx, repo, []encoding.ID{r.Head}, have)
	if err != nil {
		return MainGraph{}, err
	}
	out := MainGraph{Top: r.Head}
	if ok {
		out.Layers = slices.Clone(kept.Layers)
	}
	kind := "same"
	if len(recs) > 0 {
		top, err := s.putGraphLayer(ctx, repo, recs)
		if err != nil {
			return MainGraph{}, err
		}
		out.Layers, kind = append(out.Layers, top), "stacked"
		for n := len(out.Layers); n > 1 && (out.Layers[n-2].Size <= 2*out.Layers[n-1].Size || n > graphMaxLayers); n = len(out.Layers) {
			merged, err := s.mergeGraphLayers(ctx, repo, out.Layers[n-2], out.Layers[n-1])
			if errors.Is(err, blob.ErrNotFound) && !whole {
				return s.deriveMainGraph(ctx, repo, true)
			}
			if err != nil {
				return MainGraph{}, err
			}
			out.Layers, kind = append(out.Layers[:n-2], merged), "merged"
		}
	}
	if !ok {
		kind = "whole"
	}
	b := encodeMainGraph(out)
	if err := s.blobs.Put(ctx, mainGraphKey(repo), bytes.NewReader(b), int64(len(b))); err != nil {
		return MainGraph{}, err
	}
	graphsDerived.With(kind).Since(start)
	return out, nil
}

// mergeGraphLayers writes one layer of lower's records and then upper's.
func (s *Store) mergeGraphLayers(ctx context.Context, repo int64, lower, upper GraphLayerRef) (GraphLayerRef, error) {
	var recs []encoding.GraphRecord
	for _, l := range []GraphLayerRef{lower, upper} {
		b, err := s.GraphLayer(ctx, repo, l.ID)
		if err != nil {
			return GraphLayerRef{}, err
		}
		got, err := graph.Decode(b)
		if err != nil {
			return GraphLayerRef{}, fmt.Errorf("graph layer %s: %w", l.ID, err)
		}
		recs = append(recs, got...)
	}
	return s.putGraphLayer(ctx, repo, recs)
}

// ReadGraph answers what a client indexing heads needs of the commit graph, knowing every commit
// reachable from have: a header naming main's layers and the head they reach (none, before main's
// graph is first derived), each of those layers held does not name, in pieces, and then the records
// of every commit reachable from heads and from neither have nor that head, in topological order,
// graphRecordsPiece a message. A head the repository does not hold is passed over: a client indexes
// what it has not published itself. Should the manifest be a later release's, or name a layer that
// is gone, main's graph is derived again first.
func (s *Store) ReadGraph(ctx context.Context, repo int64, heads, have, held []encoding.ID,
	header func(top encoding.ID, layers []GraphLayerRef) error, piece func(id encoding.ID, data []byte, last bool) error,
	records func(data []byte) error) error {
	kept, ok, err := s.mainGraph(ctx, repo)
	if errors.Is(err, errNewerGraph) {
		kept, err = s.deriveMainGraph(ctx, repo, true)
		ok = len(kept.Layers) > 0
	}
	if err != nil {
		return err
	}
	var bodies [][]byte
	for attempt := 0; ok; attempt++ {
		bodies = make([][]byte, len(kept.Layers))
		var missing error
		for i, l := range kept.Layers {
			if slices.Contains(held, l.ID) {
				continue
			}
			if bodies[i], err = s.GraphLayer(ctx, repo, l.ID); errors.Is(err, blob.ErrNotFound) {
				missing = err
				break
			} else if err != nil {
				return err
			}
		}
		if missing == nil {
			break
		}
		// A layer the manifest names is gone: main's graph is derived again, whole.
		if attempt > 0 {
			return missing
		}
		if kept, err = s.deriveMainGraph(ctx, repo, true); err != nil {
			return err
		}
		ok = len(kept.Layers) > 0
	}
	if !ok {
		if err := header(encoding.ID{}, nil); err != nil {
			return err
		}
	} else {
		if err := header(kept.Top, kept.Layers); err != nil {
			return err
		}
		for i, l := range kept.Layers {
			b := bodies[i]
			for off := 0; off < len(b); off += layerPiece {
				end := min(off+layerPiece, len(b))
				if err := piece(l.ID, b[off:end], end == len(b)); err != nil {
					return err
				}
			}
		}
		have = append(slices.Clone(have), kept.Top)
	}
	recs, err := s.graphRecords(ctx, repo, heads, have)
	if err != nil {
		return err
	}
	for start := 0; start < len(recs); start += graphRecordsPiece {
		if err := records(encoding.EncodeGraphRecords(recs[start:min(start+graphRecordsPiece, len(recs))])); err != nil {
			return err
		}
	}
	return nil
}

// dropGraphs deletes a repository's graph, and its layers: it is deleted, or moved to another cell.
func (s *Store) dropGraphs(ctx context.Context, repo int64) error {
	all, err := s.blobs.List(ctx, graphsPrefix(repo))
	if err != nil {
		return err
	}
	keys := make([]string, len(all))
	for i, o := range all {
		keys[i] = o.Key
	}
	return s.blobs.Delete(ctx, keys...)
}

// pruneGraphs deletes the layers of main's graph its manifest no longer names, older than
// graphLayerGrace. It returns how many it deleted (or, dry, would).
func (s *Store) pruneGraphs(ctx context.Context, repo int64, now time.Time, dry bool) (int, error) {
	kept, _, err := s.mainGraph(ctx, repo)
	if errors.Is(err, errNewerGraph) {
		// Its layers are a later release's, which this one cannot tell from the rest.
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	named := map[string]bool{}
	for _, l := range kept.Layers {
		named[graphLayerKey(repo, l.ID)] = true
	}
	all, err := s.blobs.List(ctx, graphsPrefix(repo)+"l/")
	if err != nil {
		return 0, err
	}
	var unnamed []string
	for _, o := range all {
		if !named[o.Key] && now.Sub(o.Modified) > graphLayerGrace {
			unnamed = append(unnamed, o.Key)
		}
	}
	if !dry {
		if err := s.blobs.Delete(ctx, unnamed...); err != nil {
			return 0, err
		}
	}
	return len(unnamed), nil
}
