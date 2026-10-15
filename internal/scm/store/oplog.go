package store

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// Every operation log is one workspace's (docs/scm.md, "Decisions", 3): (repository,
// principal, workspace). A workspace's log is made with the workspace (CreateWorkspace) and is
// never made by a request naming it, so two clones can never come to share one; a request
// naming no workspace, or one the caller does not have, is refused.

// opLogID returns the operation log of the caller's workspace in repo, if it has one.
func opLogID(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, repo int64, principal, workspace string) (int64, bool, error) {
	var id int64
	err := q.QueryRow(ctx, `SELECT id FROM op_log WHERE repository_id = $1 AND principal = $2 AND workspace = $3`,
		repo, principal, workspace).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return id, err == nil, err
}

// logOf returns the operation log of the caller's workspace in repo: InvalidArgument for a
// request naming no workspace, and NotFound for a workspace the caller does not have.
func logOf(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, repo int64, principal, workspace string) (int64, error) {
	if workspace == "" {
		return 0, unnamedWorkspace()
	}
	id, ok, err := opLogID(ctx, q, repo, principal, workspace)
	if err == nil && !ok {
		err = noWorkspace(workspace)
	}
	return id, err
}

func unnamedWorkspace() error {
	return invalid("the request names no workspace: a clone made before each workspace had its own operation log, which no longer exists (jj clone makes a new workspace)")
}

func noWorkspace(workspace string) error {
	return notFound("you have no workspace %q in this repository (jj clone makes one)", workspace)
}

// ensureOpLog returns the operation log of the caller's workspace in repo, making it, with the
// root operation as its only head, when it does not exist.
func ensureOpLog(ctx context.Context, tx pgx.Tx, repo int64, principal, workspace string) (int64, bool, error) {
	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO op_log (repository_id, principal, workspace) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING id`,
		repo, principal, workspace).Scan(&id)
	switch {
	case err == nil:
		_, err = tx.Exec(ctx, `INSERT INTO op_head (op_log_id, op_id) VALUES ($1, $2)`, id, encoding.RootOperationID[:])
		return id, true, err
	case errors.Is(err, pgx.ErrNoRows):
		// It exists; ON CONFLICT waited for the transaction making it to commit.
		id, ok, err := opLogID(ctx, tx, repo, principal, workspace)
		if err == nil && !ok {
			err = errors.New("operation log vanished")
		}
		return id, false, err
	default:
		return 0, false, err
	}
}

// OpLog returns the operation log of the caller's workspace in repo (see logOf).
func (s *Store) OpLog(ctx context.Context, repo int64, principal, workspace string) (int64, error) {
	if err := s.repositoryExists(ctx, s.pool, repo); err != nil {
		return 0, err
	}
	return logOf(ctx, s.pool, repo, principal, workspace)
}

func opHeads(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, opLog int64) ([]encoding.ID, error) {
	rows, err := q.Query(ctx, `SELECT op_id FROM op_head WHERE op_log_id = $1 ORDER BY op_id`, opLog)
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

// GetOpHeads returns the operation heads of the caller's workspace: never empty, the root
// operation until the first publish.
func (s *Store) GetOpHeads(ctx context.Context, repo int64, principal, workspace string) ([]encoding.ID, error) {
	opLog, err := s.OpLog(ctx, repo, principal, workspace)
	if err != nil {
		return nil, err
	}
	return opHeads(ctx, s.pool, opLog)
}

// updateOpHeads applies jj's update_op_heads: add newID, then remove each old ID other
// than newID. Adding is idempotent and removing an ID that is not a head is a no-op, so
// the heads are never empty.
func updateOpHeads(ctx context.Context, tx pgx.Tx, opLog int64, old []encoding.ID, newID encoding.ID) error {
	if _, err := tx.Exec(ctx, `INSERT INTO op_head (op_log_id, op_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, opLog, newID[:]); err != nil {
		return err
	}
	var drop []encoding.ID
	for _, o := range old {
		if o != newID {
			drop = append(drop, o)
		}
	}
	if len(drop) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `DELETE FROM op_head WHERE op_log_id = $1 AND op_id = ANY($2)`, opLog, ids(drop))
	return err
}

// Resolution is the outcome of resolving an operation ID prefix.
type Resolution int

const (
	NoMatch Resolution = iota
	SingleMatch
	AmbiguousMatch
)

// ResolveOperationPrefix resolves a hex prefix to an operation in the caller's workspace's log,
// the root operation included, as jj's op store does. prefix holds the prefix's whole bytes;
// with oddNibble set, its last byte's high nibble is the prefix's final hex digit.
func (s *Store) ResolveOperationPrefix(ctx context.Context, repo int64, principal, workspace string, prefix []byte, oddNibble bool) (Resolution, encoding.ID, error) {
	if len(prefix) > encoding.IDLen || (oddNibble && len(prefix) == 0) {
		return NoMatch, encoding.ID{}, invalid("operation ID prefix of %d bytes", len(prefix))
	}
	lo := bytes.Clone(prefix)
	if oddNibble {
		lo[len(lo)-1] &= 0xf0
	}
	matches := func(id encoding.ID) bool {
		n := len(lo)
		if oddNibble {
			return bytes.Equal(id[:n-1], lo[:n-1]) && id[n-1]&0xf0 == lo[n-1]
		}
		return bytes.Equal(id[:n], lo)
	}
	opLog, err := logOf(ctx, s.pool, repo, principal, workspace)
	if err != nil {
		return NoMatch, encoding.ID{}, err
	}
	var found []encoding.ID
	if matches(encoding.RootOperationID) {
		found = append(found, encoding.RootOperationID)
	}
	// The IDs matching the prefix are the contiguous range [lo, hi).
	hi := bytes.Clone(lo)
	step := byte(1)
	if oddNibble {
		step = 0x10
	}
	carry := true
	for i := len(hi) - 1; i >= 0 && carry; i-- {
		sum := int(hi[i]) + int(step)
		hi[i], carry, step = byte(sum), sum > 0xff, 1
	}
	query := `SELECT id FROM operation WHERE op_log_id = $1 AND id >= $2 ORDER BY id LIMIT 2`
	args := []any{opLog, lo}
	if !carry && len(hi) > 0 {
		query = `SELECT id FROM operation WHERE op_log_id = $1 AND id >= $2 AND id < $3 ORDER BY id LIMIT 2`
		args = append(args, hi)
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return NoMatch, encoding.ID{}, err
	}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return NoMatch, encoding.ID{}, err
		}
		id, err := toID(b)
		if err != nil {
			rows.Close()
			return NoMatch, encoding.ID{}, err
		}
		found = append(found, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return NoMatch, encoding.ID{}, err
	}
	switch len(found) {
	case 0:
		return NoMatch, encoding.ID{}, nil
	case 1:
		return SingleMatch, found[0], nil
	}
	return AmbiguousMatch, encoding.ID{}, nil
}

// ReadOperations reads operations and views from the caller's workspace's operation log. A
// ref the log does not hold is reported absent.
func (s *Store) ReadOperations(ctx context.Context, repo int64, principal, workspace string, refs []ObjectRef) ([]ObjectResult, error) {
	for _, r := range refs {
		if r.Kind != encoding.KindOperation && r.Kind != encoding.KindView {
			return nil, invalid("ReadOperations reads operations and views, not a %s", r.Kind)
		}
	}
	opLog, err := logOf(ctx, s.pool, repo, principal, workspace)
	if err != nil {
		return nil, err
	}
	bodies := map[ObjectRef][]byte{}
	for _, t := range []struct {
		kind  encoding.Kind
		table string
	}{{encoding.KindOperation, "operation"}, {encoding.KindView, "view"}} {
		var want []encoding.ID
		for _, r := range refs {
			if r.Kind == t.kind {
				want = append(want, r.ID)
			}
		}
		if len(want) == 0 {
			continue
		}
		rows, err := s.pool.Query(ctx, `SELECT id, body FROM `+t.table+` WHERE op_log_id = $1 AND id = ANY($2)`, opLog, ids(want))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var idb, body []byte
			if err := rows.Scan(&idb, &body); err != nil {
				rows.Close()
				return nil, err
			}
			id, err := toID(idb)
			if err != nil {
				rows.Close()
				return nil, err
			}
			bodies[ObjectRef{Kind: t.kind, ID: id}] = body
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	out := make([]ObjectResult, len(refs))
	for i, r := range refs {
		body, found := bodies[r]
		out[i] = ObjectResult{Ref: r, Body: body, Found: found}
	}
	return out, nil
}

// prefetchOpsBatch is how many operations PrefetchOperations reads views for at a time.
const prefetchOpsBatch = 500

// PrefetchOperations streams the operations of the caller's workspace's log reachable from want — its heads
// when want is empty — and not from have, nearest first, each followed by its view the first
// time one names it, stopping after limit operations (0: no limit). A have the log does not hold
// is ignored. The log's operations are read with one query, as collection reads them: a log is
// one person's, and collection keeps it to what its heads reach.
func (s *Store) PrefetchOperations(ctx context.Context, repo int64, principal, workspace string, want, have []encoding.ID, limit int, emit func(EncodedObject) error) error {
	opLog, err := logOf(ctx, s.pool, repo, principal, workspace)
	if err != nil {
		return err
	}
	type op struct {
		body    []byte
		view    encoding.ID
		parents []encoding.ID
	}
	ops := map[encoding.ID]*op{}
	rows, err := s.pool.Query(ctx, `SELECT id, view_id, body FROM operation WHERE op_log_id = $1`, opLog)
	if err != nil {
		return err
	}
	for rows.Next() {
		var idb, viewb, body []byte
		if err := rows.Scan(&idb, &viewb, &body); err != nil {
			rows.Close()
			return err
		}
		id, err := toID(idb)
		if err != nil {
			rows.Close()
			return err
		}
		view, err := toID(viewb)
		if err != nil {
			rows.Close()
			return err
		}
		o, err := decodeStored(encoding.KindOperation, id, body)
		if err != nil {
			rows.Close()
			return err
		}
		ops[id] = &op{body: body, view: view, parents: o.(*encoding.Operation).Parents}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(want) == 0 {
		if want, err = opHeads(ctx, s.pool, opLog); err != nil {
			return err
		}
	}
	// held: everything have reaches, which the caller already holds.
	held := map[encoding.ID]bool{}
	for frontier := append([]encoding.ID(nil), have...); len(frontier) > 0; {
		id := frontier[len(frontier)-1]
		frontier = frontier[:len(frontier)-1]
		o, known := ops[id]
		if !known || held[id] {
			continue
		}
		held[id] = true
		frontier = append(frontier, o.parents...)
	}
	sentViews := map[encoding.ID]bool{}
	var batch []encoding.ID
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		var views []encoding.ID
		for _, id := range batch {
			if v := ops[id].view; !sentViews[v] {
				sentViews[v] = true
				views = append(views, v)
			}
		}
		bodies := map[encoding.ID][]byte{}
		if len(views) > 0 {
			rows, err := s.pool.Query(ctx, `SELECT id, body FROM view WHERE op_log_id = $1 AND id = ANY($2)`, opLog, ids(views))
			if err != nil {
				return err
			}
			for rows.Next() {
				var idb, body []byte
				if err := rows.Scan(&idb, &body); err != nil {
					rows.Close()
					return err
				}
				id, err := toID(idb)
				if err != nil {
					rows.Close()
					return err
				}
				bodies[id] = body
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
		}
		for _, id := range batch {
			if err := emit(EncodedObject{Kind: encoding.KindOperation, ID: id, Body: ops[id].body}); err != nil {
				return err
			}
			v := ops[id].view
			if body, ok := bodies[v]; ok {
				if err := emit(EncodedObject{Kind: encoding.KindView, ID: v, Body: body}); err != nil {
					return err
				}
				delete(bodies, v)
			}
		}
		batch = batch[:0]
		return nil
	}
	seen := map[encoding.ID]bool{}
	sent := 0
	for queue := append([]encoding.ID(nil), want...); len(queue) > 0 && (limit == 0 || sent < limit); {
		id := queue[0]
		queue = queue[1:]
		o, known := ops[id]
		if !known || seen[id] || held[id] {
			continue
		}
		seen[id] = true
		batch = append(batch, id)
		sent++
		queue = append(queue, o.parents...)
		if len(batch) == prefetchOpsBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// CollectOperations deletes the operations and views in the caller's workspace's log that are
// unreachable from both heads and every current op_head row, and that were last touched
// before keepNewer (server time). A head published concurrently is therefore never
// collected, whatever the caller passes.
func (s *Store) CollectOperations(ctx context.Context, repo int64, principal, workspace string, heads []encoding.ID, keepNewer time.Time) error {
	opLog, err := logOf(ctx, s.pool, repo, principal, workspace)
	if err != nil {
		return err
	}
	_, err = s.collectLog(ctx, opLog, heads, keepNewer, false)
	return err
}

// Collected counts what an operation-log collection deleted (or, dry, would delete).
type Collected struct {
	Operations, Views int64
}

// collectLog collects one operation log (see CollectOperations). Reachability over the
// whole log is computed from a snapshot without the log's row lock; then, under the lock
// every Publish carrying operations also takes, only what was published since is walked
// — from the current heads through operations the snapshot did not reach — so collection
// holds the lock for the new operations, not the log's history.
func (s *Store) collectLog(ctx context.Context, opLog int64, heads []encoding.ID, keepNewer time.Time, dryRun bool) (Collected, error) {
	var out Collected
	var old bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM operation WHERE op_log_id = $1 AND touched_at < $2)
		OR EXISTS (SELECT 1 FROM view WHERE op_log_id = $1 AND touched_at < $2)`, opLog, keepNewer).Scan(&old); err != nil || !old {
		return out, err
	}

	// The snapshot: every operation's parents, and which are old enough to collect.
	parents := map[encoding.ID][]encoding.ID{}
	stale := map[encoding.ID]bool{}
	rows, err := s.pool.Query(ctx, `SELECT id, body, touched_at < $2 FROM operation WHERE op_log_id = $1`, opLog, keepNewer)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var idb, body []byte
		var isOld bool
		if err := rows.Scan(&idb, &body, &isOld); err != nil {
			rows.Close()
			return out, err
		}
		id, err := toID(idb)
		if err != nil {
			rows.Close()
			return out, err
		}
		o, err := decodeStored(encoding.KindOperation, id, body)
		if err != nil {
			rows.Close()
			return out, err
		}
		parents[id] = o.(*encoding.Operation).Parents
		if isOld {
			stale[id] = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	current, err := opHeads(ctx, s.pool, opLog)
	if err != nil {
		return out, err
	}
	reachable := map[encoding.ID]bool{}
	// reach marks what frontier reaches through the snapshot and returns the operations
	// it reached that the snapshot does not hold.
	reach := func(frontier []encoding.ID) []encoding.ID {
		var unknown []encoding.ID
		for len(frontier) > 0 {
			id := frontier[len(frontier)-1]
			frontier = frontier[:len(frontier)-1]
			if id == encoding.RootOperationID || reachable[id] {
				continue
			}
			ps, known := parents[id]
			if !known {
				unknown = append(unknown, id)
				continue
			}
			reachable[id] = true
			frontier = append(frontier, ps...)
		}
		return unknown
	}
	reach(append(append([]encoding.ID(nil), heads...), current...))

	err = s.inTx(ctx, func(tx pgx.Tx) error {
		out = Collected{}
		if _, err := tx.Exec(ctx, `SELECT id FROM op_log WHERE id = $1 FOR UPDATE`, opLog); err != nil {
			return err
		}
		current, err := opHeads(ctx, tx, opLog)
		if err != nil {
			return err
		}
		// Operations published since the snapshot, and whatever of the snapshot they
		// make reachable again.
		fetched := map[encoding.ID]bool{}
		unknown := reach(append(append([]encoding.ID(nil), heads...), current...))
		for len(unknown) > 0 {
			var batch []encoding.ID
			for _, id := range unknown {
				if !fetched[id] {
					fetched[id] = true
					batch = append(batch, id)
				}
			}
			if len(batch) == 0 {
				break
			}
			rows, err := tx.Query(ctx, `SELECT id, body FROM operation WHERE op_log_id = $1 AND id = ANY($2)`, opLog, ids(batch))
			if err != nil {
				return err
			}
			var next []encoding.ID
			for rows.Next() {
				var idb, body []byte
				if err := rows.Scan(&idb, &body); err != nil {
					rows.Close()
					return err
				}
				id, err := toID(idb)
				if err != nil {
					rows.Close()
					return err
				}
				o, err := decodeStored(encoding.KindOperation, id, body)
				if err != nil {
					rows.Close()
					return err
				}
				parents[id] = o.(*encoding.Operation).Parents
				next = append(next, id)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
			unknown = reach(next)
		}
		var doomed []encoding.ID
		for id := range stale {
			if !reachable[id] {
				doomed = append(doomed, id)
			}
		}
		sortIDs(doomed)
		// touched_at is checked again: re-publishing an operation keeps it.
		if dryRun {
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM operation WHERE op_log_id = $1 AND id = ANY($2) AND touched_at < $3`,
				opLog, ids(doomed), keepNewer).Scan(&out.Operations); err != nil {
				return err
			}
			return tx.QueryRow(ctx, `SELECT count(*) FROM view v WHERE v.op_log_id = $1 AND v.touched_at < $2
				AND NOT EXISTS (SELECT 1 FROM operation o WHERE o.op_log_id = v.op_log_id AND o.view_id = v.id
				AND NOT (o.id = ANY($3) AND o.touched_at < $2))`, opLog, keepNewer, ids(doomed)).Scan(&out.Views)
		}
		tag, err := tx.Exec(ctx, `DELETE FROM operation WHERE op_log_id = $1 AND id = ANY($2) AND touched_at < $3`,
			opLog, ids(doomed), keepNewer)
		if err != nil {
			return err
		}
		out.Operations = tag.RowsAffected()
		tag, err = tx.Exec(ctx,
			`DELETE FROM view v WHERE v.op_log_id = $1 AND v.touched_at < $2
			   AND NOT EXISTS (SELECT 1 FROM operation o WHERE o.op_log_id = v.op_log_id AND o.view_id = v.id)`,
			opLog, keepNewer)
		if err != nil {
			return err
		}
		out.Views = tag.RowsAffected()
		return nil
	})
	return out, err
}
