package store

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/scm/store/backend"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Moving a network between cells (docs/hosted.md, "Cells, as built"): the SCM's side of a move —
// its rows and objects about a network, and the gate every write transaction about one reads.

// MovePurgeWork is a network that moved away, as keyed work due a day after: its rows and objects
// here are deleted then.
const MovePurgeWork = "move-purge"

// movedKept is how long a network that moved away is kept here before it is deleted.
const movedKept = 24 * time.Hour

// dropBatch bounds the rows one transaction of a moved or abandoned network's deletion deletes.
const dropBatch = 10_000

// gate refuses a write transaction about a network that moves: frozen, or moved away. ctx names
// what the transaction writes about (move.WithRepository, move.WithNetwork); one about a
// repository the SCM holds no network of yet — being made — is let through. A write that begins
// with an exchange checks it first through q, outside its transaction, so a moved repository is
// refused before the exchange — when the edge can still send the request to the cell that holds
// it — and its transaction checks it again.
func (s *Store) gate(ctx context.Context, tx backend.Ops) error {
	var net backend.Network
	repo, byRepo := move.RepositoryOf(ctx)
	switch n, ok := move.NetworkOf(ctx); {
	case ok:
		net = backend.Network(n)
	case byRepo:
		var err error
		if net, err = s.network(ctx, repo); errors.Is(err, ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
	default:
		return nil
	}
	state, cell, err := tx.NetworkMove(ctx, net)
	if err != nil {
		return err
	}
	switch state {
	case backend.MoveFrozen:
		return move.Frozen(int64(net))
	case backend.MoveMoved:
		if byRepo {
			return cells.Moved(repo, cell)
		}
		return fmt.Errorf("network %d moved to cell %s", net, cell)
	}
	return nil
}

// Mover is the SCM's side of moves (move.Role).
func (s *Store) Mover() move.Role { return mover{s} }

type mover struct{ s *Store }

func moving(sc move.Scope) backend.Moving {
	return backend.Moving{Network: backend.Network(sc.Network), Repositories: sc.Repositories}
}

func (mover) Name() string { return cells.SCM }

func (m mover) Tables(ctx context.Context) ([]string, error) { return m.s.b.MoveTables(ctx) }

func (m mover) Export(ctx context.Context, sc move.Scope, table string, after *okpb.Row, limit int) ([]*okpb.Row, error) {
	if table == "content_log" {
		return m.s.exportContentLog(ctx, sc, after, limit)
	}
	return m.s.b.ExportRows(ctx, moving(sc), table, after, limit)
}

// exportContentLog exports the network's content indexes' logs as the next replica would read
// them: of each index — the network's and each repository's — the rows its generation's fold did
// not read, in the order they apply, numbered 1, 2, … in that order. A row's order is otherwise
// its database's alone (Spanner's commit timestamps, PostgreSQL's stamps), which a move does not
// carry; numbered so, each database reads them in that order from the rows alone. The network is
// frozen, so every page reads the same rows.
func (s *Store) exportContentLog(ctx context.Context, sc move.Scope, after *okpb.Row, limit int) ([]*okpb.Row, error) {
	scopes := []backend.Scope{backend.NetworkScope(backend.Network(sc.Network))}
	for _, r := range sc.Repositories {
		scopes = append(scopes, repoScope(r))
	}
	// In the key's order: (scope, owner_id, id), repositories' scope 0 before the network's 1.
	slices.SortFunc(scopes, func(a, b backend.Scope) int {
		as, ao := scopeKey(a)
		bs, bo := scopeKey(b)
		return cmp.Or(cmp.Compare(as, bs), cmp.Compare(ao, bo))
	})
	var from [3]int64
	if after != nil {
		var ok bool
		if from, ok = contentLogKey(after); !ok {
			return nil, fmt.Errorf("a content log row's key: %v", after)
		}
	}
	at := time.Now()
	var out []*okpb.Row
	err := s.b.Snapshot(ctx, func(tx backend.Ops) error {
		out = nil
		for _, c := range scopes {
			scope, owner := scopeKey(c)
			if after != nil && (scope < from[0] || scope == from[0] && owner < from[1]) {
				continue
			}
			st, ok, err := tx.ContentIndexGeneration(ctx, c)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			logs, _, err := tx.ContentLogSince(ctx, c, st.Folded)
			if err != nil {
				return err
			}
			for i, l := range logs {
				id := int64(i + 1)
				if after != nil && scope == from[0] && owner == from[1] && id <= from[2] {
					continue
				}
				out = append(out, &okpb.Row{Columns: []*okpb.Column{
					{Name: "scope", Value: &okpb.Value{Kind: &okpb.Value_Int{Int: scope}}},
					{Name: "owner_id", Value: &okpb.Value{Kind: &okpb.Value_Int{Int: owner}}},
					{Name: "id", Value: &okpb.Value{Kind: &okpb.Value_Int{Int: id}}},
					{Name: "created_at", Value: &okpb.Value{Kind: &okpb.Value_Time{Time: timestamppb.New(at)}}},
					{Name: "body", Value: &okpb.Value{Kind: &okpb.Value_Data{Data: l.Body}}},
				}})
				if len(out) == limit {
					return nil
				}
			}
		}
		return nil
	})
	return out, err
}

// scopeKey is a content index's scope as its rows' key holds it: 0 a repository's, 1 a network's,
// and its owner.
func scopeKey(sc backend.Scope) (scope, owner int64) {
	if sc.Network {
		return 1, sc.ID
	}
	return 0, sc.ID
}

// contentLogKey is an exported content log row's key: its scope, owner, and number.
func contentLogKey(r *okpb.Row) ([3]int64, bool) {
	var k [3]int64
	found := 0
	for _, c := range r.GetColumns() {
		v, ok := c.GetValue().GetKind().(*okpb.Value_Int)
		if !ok {
			continue
		}
		switch c.GetName() {
		case "scope":
			k[0] = v.Int
		case "owner_id":
			k[1] = v.Int
		case "id":
			k[2] = v.Int
		default:
			continue
		}
		found++
	}
	return k, found == 3
}

func (m mover) Import(ctx context.Context, sc move.Scope, table string, rows []*okpb.Row) error {
	return m.s.b.ImportRows(ctx, moving(sc), table, rows)
}

// Imported moves the network's counters past the rows brought, and drops what this replica kept
// of it: an import writes under its keys. The network came frozen, as its row was.
func (m mover) Imported(ctx context.Context, sc move.Scope) error {
	if err := m.s.b.FinishImport(ctx, moving(sc)); err != nil {
		return err
	}
	m.s.forgetNetwork(sc)
	return nil
}

// Open lets the network be written here; its keyed work and its steps stayed behind, so what its
// tail holds is compacted, it is collected, the rows its writers left pending are sequenced, and
// its pending events are published, here.
func (m mover) Open(ctx context.Context, sc move.Scope) error {
	if err := m.s.b.SetNetworkMove(ctx, backend.Network(sc.Network), "", ""); err != nil {
		return err
	}
	jobs := m.s.Work().Jobs()
	for _, r := range sc.Repositories {
		for _, kind := range []string{SegmentWork, GCWork} {
			if err := m.s.Work().Mark(ctx, kind, r); err != nil {
				return err
			}
		}
		if m.s.b.SequencesAfterCommit() {
			if err := jobs.Step(ctx, sequenceKind, r, time.Time{}, binary.BigEndian.AppendUint64(nil, 0)); err != nil {
				return err
			}
		}
		if err := jobs.Step(ctx, relayKind, r, time.Time{}, relayBody(0, 0)); err != nil {
			return err
		}
	}
	return nil
}

func (m mover) Drop(ctx context.Context, sc move.Scope) error {
	// What this cell derived of the repositories, which the destination derives anew.
	for _, r := range sc.Repositories {
		if err := m.s.dropCodeIndex(ctx, r); err != nil {
			return err
		}
		if err := m.s.dropCheckouts(ctx, r); err != nil {
			return err
		}
	}
	for {
		n, err := m.s.b.DropNetworkRows(ctx, moving(sc), dropBatch)
		if err != nil {
			return err
		}
		if n < dropBatch {
			m.s.forgetNetwork(sc)
			return nil
		}
	}
}

// Freeze stops the network's writes, then sequences what its repositories' writers left pending:
// every writer's transaction read the network's gate, so none commits after the freeze, and the
// export that follows carries no pending row (sequence.go).
func (m mover) Freeze(ctx context.Context, sc move.Scope) error {
	if err := m.s.b.SetNetworkMove(ctx, backend.Network(sc.Network), backend.MoveFrozen, ""); err != nil {
		return err
	}
	for _, r := range sc.Repositories {
		if err := m.s.sequence(sequencing(), r, time.Now()); err != nil {
			return fmt.Errorf("sequencing repository %d: %w", r, err)
		}
	}
	return nil
}

func (m mover) Thaw(ctx context.Context, sc move.Scope) error {
	return m.s.b.SetNetworkMove(ctx, backend.Network(sc.Network), "", "")
}

// Finish records the network moved to cell — every write about it refused from here, naming the
// cell — and deletes it here a day later (MovePurgeWork).
func (m mover) Finish(ctx context.Context, sc move.Scope, cell string) error {
	if err := m.s.b.SetNetworkMove(ctx, backend.Network(sc.Network), backend.MoveMoved, cell); err != nil {
		return err
	}
	return m.s.Work().MarkAt(ctx, MovePurgeWork, sc.Network, time.Now().Add(movedKept))
}

func (m mover) Objects() move.Objects { return netObjects{m.s} }

// PurgeMoved deletes a network that moved away, its rows and objects: MovePurgeWork's, a day after
// it moved. One thawed since, or gone, is left.
func (s *Store) PurgeMoved(ctx context.Context, net int64) error {
	n := backend.Network(net)
	var state string
	if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		var err error
		state, _, err = tx.NetworkMove(ctx, n)
		return err
	}); err != nil {
		return err
	}
	if state != backend.MoveMoved {
		return nil // thawed, or gone already
	}
	repos, err := s.ops(ctx).NetworkRepositories(ctx, n)
	if err != nil {
		return err
	}
	sc := move.Scope{Network: net, Repositories: repos}
	if err := (netObjects{s}).Drop(ctx, sc); err != nil {
		return err
	}
	return mover{s}.Drop(ctx, sc)
}

// forgetNetwork drops what this replica keeps of a network and its repositories.
func (s *Store) forgetNetwork(sc move.Scope) {
	for _, r := range sc.Repositories {
		s.networks.Delete(r)
		s.forgetIndex(repoScope(r))
	}
	s.forgetIndex(backend.NetworkScope(backend.Network(sc.Network)))
}

// netObjects are a network's objects: under nets/<network>/, its packs and segments.
type netObjects struct{ s *Store }

func netPrefix(net int64) string { return fmt.Sprintf("nets/%d/", net) }

func (o netObjects) List(ctx context.Context, sc move.Scope, after string, limit int) ([]*okpb.MovedObject, error) {
	objs, _, err := o.s.blobs.ListPage(ctx, netPrefix(sc.Network), after, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*okpb.MovedObject, len(objs))
	for i, ob := range objs {
		out[i] = &okpb.MovedObject{Key: ob.Key, Size: ob.Size}
	}
	return out, nil
}

func (o netObjects) Open(ctx context.Context, sc move.Scope, key string) (io.ReadCloser, error) {
	if !strings.HasPrefix(key, netPrefix(sc.Network)) {
		return nil, invalid("%s is none of network %d's objects", key, sc.Network)
	}
	pr, pw := io.Pipe()
	go func() {
		_, err := o.s.blobs.Download(ctx, key, pw)
		pw.CloseWithError(err)
	}()
	return pr, nil
}

func (o netObjects) Has(ctx context.Context, key string) (bool, error) {
	_, err := o.s.blobs.Stat(ctx, key)
	if errors.Is(err, blob.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (o netObjects) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	return o.s.blobs.Put(ctx, key, r, size)
}

func (o netObjects) Drop(ctx context.Context, sc move.Scope) error {
	for {
		objs, _, err := o.s.blobs.ListPage(ctx, netPrefix(sc.Network), "", 1000)
		if err != nil || len(objs) == 0 {
			return err
		}
		keys := make([]string, len(objs))
		for i, ob := range objs {
			keys[i] = ob.Key
		}
		if err := o.s.blobs.Delete(ctx, keys...); err != nil {
			return err
		}
	}
}
