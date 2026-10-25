package spannerstore

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/data/spannertxn"
	"github.com/Cidan/ok/internal/scm/store/backend"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// moveTables are the tables that hold a network's rows: all but its keyed work, its unfinished
// deletions' records, its counters — which an import sets from the rows it brings — and its
// staged content log's batches, whose rows an export reads only once committed.
var moveTables = []string{
	"network", "repository", "head_log", "object", "tree_sizes", "commit", "commit_parent", "copy_parent", "commit_path", "path_index",
	"pack", "content_index", "content_log", "dictionary", "dictionary_grant", "op_log", "operation", "view", "op_head",
	"op_tombstone", "changelist", "patchset", "candidate", "changelist_counter", "event", "event_pending", "repository_event_counter",
	"gc_pass", "workspace", "checkpoint", "segment", "chunk_release",
}

// moveRenames are the columns this schema names otherwise than PostgreSQL's.
var moveRenames = spannerdata.Renames{
	"network":    {"id": "network_id"},
	"repository": {"id": "repository_id"},
	"op_log":     {"id": "op_log_id"},
	"event":      {"at": "event_at"},
}

// moveSkip are the columns Spanner alone has, which a move does not carry: a staged log row's
// batch (an export reads only committed ones), a pending event's relay lease, a content index's
// log's commit times and fold's place — the SCM exports the log itself, in the order it applies
// (Store.exportContentLog), and the generation it carries folded none of it — and the tie of the
// transaction that wrote an event, which only its writer asks of it, at once, and a log row's shard,
// which only orders its writers.
var moveSkip = map[string]bool{"batch_id": true, "holder": true, "lease_until": true, "logged_at": true, "folded_through": true,
	"txn": true, "shard": true}

// moveWhere is the condition a table's rows about a network (@net) and its repositories (@repos)
// meet, by Spanner's names.
func moveWhere(table string) string {
	switch table {
	case "network":
		return "network_id = @net"
	case "repository":
		return "repository_id IN UNNEST(@repos)"
	case "operation", "view", "op_head", "op_tombstone":
		return "op_log_id IN (SELECT op_log_id FROM op_log WHERE repository_id IN UNNEST(@repos))"
	case "pack", "chunk_release", "dictionary", "segment":
		return "network_id = @net"
	case "content_index":
		return "((scope = 1 AND owner_id = @net) OR (scope = 0 AND owner_id IN UNNEST(@repos)))"
	case "content_log":
		return `((scope = 1 AND owner_id = @net) OR (scope = 0 AND owner_id IN UNNEST(@repos)))
			AND (batch_id IS NULL OR EXISTS (SELECT 1 FROM content_log_batch b
				WHERE b.scope = content_log.scope AND b.owner_id = content_log.owner_id AND b.batch_id = content_log.batch_id))`
	}
	return "repository_id IN UNNEST(@repos)"
}

func moveParams(m backend.Moving) map[string]any {
	repos := m.Repositories
	if repos == nil {
		repos = []int64{}
	}
	return map[string]any{"net": int64(m.Network), "repos": repos}
}

// moveCatalog is what the catalog says of the move's tables, read once.
type moveCatalog struct {
	once   sync.Once
	tables map[string]*spannerdata.Table
	order  []string
	err    error
}

var catalogs sync.Map // *spanner.Client -> *moveCatalog

func (b *Backend) catalog(ctx context.Context) (map[string]*spannerdata.Table, []string, error) {
	v, _ := catalogs.LoadOrStore(b.c, &moveCatalog{})
	c := v.(*moveCatalog)
	c.once.Do(func() {
		ro := b.c.ReadOnlyTransaction()
		defer ro.Close()
		if c.tables, c.err = spannerdata.Tables(ctx, ro, moveTables, moveRenames); c.err == nil {
			c.order, c.err = spannerdata.ParentsFirst(c.tables)
		}
	})
	return c.tables, c.order, c.err
}

func (b *Backend) MoveTables(ctx context.Context) ([]string, error) {
	_, order, err := b.catalog(ctx)
	return order, err
}

func (b *Backend) table(ctx context.Context, name string) (*spannerdata.Table, error) {
	tables, _, err := b.catalog(ctx)
	if err != nil {
		return nil, err
	}
	t, ok := tables[name]
	if !ok {
		return nil, fmt.Errorf("the SCM moves no table %s", name)
	}
	return t, nil
}

func (b *Backend) ExportRows(ctx context.Context, m backend.Moving, table string, after *okpb.Row, limit int) ([]*okpb.Row, error) {
	t, err := b.table(ctx, table)
	if err != nil {
		return nil, err
	}
	ro := b.c.Single()
	return spannerdata.ExportRows(ctx, ro, t, moveWhere(table), moveParams(m), after, limit, moveSkip)
}

func (b *Backend) ImportRows(ctx context.Context, m backend.Moving, table string, rows []*okpb.Row) error {
	t, err := b.table(ctx, table)
	if err != nil {
		return err
	}
	return spannerdata.ImportRows(ctx, b.c, t, rows, nil)
}

// moveCounters are the counters that number a network's rows, by the table and column whose
// largest value an import sets each to, and the rows' scope: a repository's (by owner_id, scope 0)
// or the network's.
var moveCounters = []struct {
	name, table, column, scope string
	repository                 bool
}{
	{packCounter, "pack", "num", "network_id = @owner", false},
	{ageCounter, "segment", "age", "network_id = @owner", false},
	{dictionaryCounter, "dictionary", "id", "network_id = @owner", false},
}

// FinishImport sets each counter of the network's past the largest value it numbered of the rows
// the import brought.
func (b *Backend) FinishImport(ctx context.Context, m backend.Moving) error {
	for _, c := range moveCounters {
		owners := []int64{int64(m.Network)}
		if c.repository {
			owners = m.Repositories
		}
		for _, owner := range owners {
			if _, err := spannertxn.ReadWrite(ctx, b.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
				p := map[string]any{"owner": owner, "name": c.name}
				var most spanner.NullInt64
				if _, err := spannerdata.First(ctx, tx, fmt.Sprintf(`SELECT MAX(%s) FROM %s WHERE %s`, c.column, c.table, c.scope), p, &most); err != nil {
					return err
				}
				if !most.Valid {
					return nil
				}
				p["most"] = most.Int64
				var last int64
				ok, err := spannerdata.First(ctx, tx, `SELECT last FROM counter WHERE name = @name AND scope_id = @owner`, p, &last)
				switch {
				case err != nil:
					return err
				case !ok:
					_, err = spannerdata.Update(ctx, tx, `INSERT INTO counter (name, scope_id, last) VALUES (@name, @owner, @most)`, p)
				case last < most.Int64:
					_, err = spannerdata.Update(ctx, tx, `UPDATE counter SET last = @most WHERE name = @name AND scope_id = @owner`, p)
				}
				return err
			}); err != nil {
				return fmt.Errorf("the counter %s of %d: %w", c.name, owner, err)
			}
		}
	}
	return nil
}

// DropNetworkRows deletes a network's rows, children first, then its counters, staged batches, and
// pending sequences (none, once its freeze sequenced it, but for a writer's that failed).
func (b *Backend) DropNetworkRows(ctx context.Context, m backend.Moving, limit int) (int64, error) {
	tables, order, err := b.catalog(ctx)
	if err != nil {
		return 0, err
	}
	p := moveParams(m)
	var n int64
	for _, table := range slices.Backward(order) {
		if n >= int64(limit) {
			return n, nil
		}
		d, err := spannerdata.DeleteRows(ctx, b.c, tables[table], moveWhere(table), p, limit-int(n))
		if err != nil {
			return n, fmt.Errorf("dropping %s: %w", table, err)
		}
		n += d
	}
	if n >= int64(limit) {
		return n, nil
	}
	p["repoLogs"], p["netLogs"], p["packs"], p["ages"], p["dictionaries"] = contentLogCounter(0), contentLogCounter(1), packCounter, ageCounter,
		dictionaryCounter
	_, err = spannertxn.ReadWrite(ctx, b.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		for _, t := range []tableRows{
			{"content_log_batch", `(scope = 1 AND owner_id = @net) OR (scope = 0 AND owner_id IN UNNEST(@repos))`},
			{"sequence_pending", `repository_id IN UNNEST(@repos)`},
			{"counter", `(name = @repoLogs AND scope_id IN UNNEST(@repos))
				OR ((name = @netLogs OR name = @packs OR name = @ages OR name = @dictionaries) AND scope_id = @net)`},
		} {
			d, err := deleteUpTo(ctx, tx, t.table, t.cond, p, int64(limit)-n)
			n += d
			if err != nil {
				return err
			}
		}
		return nil
	})
	return n, err
}

func (b *Backend) SetNetworkMove(ctx context.Context, net backend.Network, state, cell string) error {
	_, err := spannertxn.ReadWrite(ctx, b.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		n, err := spannerdata.Update(ctx, tx, `UPDATE network SET move_state = @state, moved_to = @cell,
			moved_at = IF(@state = 'moved', CURRENT_TIMESTAMP(), NULL) WHERE network_id = @net`,
			map[string]any{"net": int64(net), "state": state, "cell": cell})
		if err == nil && n == 0 {
			err = fmt.Errorf("no network %d", net)
		}
		return err
	})
	return err
}

// NetworkMove reads the network's row in the operation's transaction, which locks it.
func (x queries) NetworkMove(ctx context.Context, net backend.Network) (string, string, error) {
	var state, cell string
	_, err := x.one(ctx, `SELECT move_state, moved_to FROM network WHERE network_id = @net`, map[string]any{"net": int64(net)}, &state, &cell)
	return state, cell, err
}
