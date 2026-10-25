package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/scm/store/backend"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// moveTables are the tables that hold a network's rows: all but its keyed work, its unfinished
// deletions' records, and the locks, which stay with the cell.
var moveTables = []string{
	"network", "repository", "head_log", "object", "tree_sizes", "commit", "commit_parent", "copy_parent", "commit_path", "path_index",
	"pack", "content_index", "content_log", "dictionary", "dictionary_grant", "op_log", "operation", "view", "op_head",
	"op_tombstone", "changelist", "patchset", "candidate", "changelist_counter", "event", "event_pending", "repository_event_counter",
	"gc_pass", "workspace", "checkpoint", "segment", "chunk_release",
}

// moveWhere is the condition a table's rows about a network (@net) and its repositories (@repos)
// meet.
func moveWhere(table string) string {
	switch table {
	case "network":
		return "id = @net"
	case "repository":
		return "id = ANY(@repos)"
	case "operation", "view", "op_head", "op_tombstone":
		return "op_log_id IN (SELECT id FROM op_log WHERE repository_id = ANY(@repos))"
	case "pack", "chunk_release", "dictionary", "segment":
		return "network_id = @net"
	case "content_index", "content_log":
		return "((scope = 1 AND owner_id = @net) OR (scope = 0 AND owner_id = ANY(@repos)))"
	}
	return "repository_id = ANY(@repos)"
}

func moveArgs(m backend.Moving) pgx.NamedArgs {
	return pgx.NamedArgs{"net": int64(m.Network), "repos": m.Repositories}
}

// moveCatalog is what the catalog says of the move's tables, read once: it does not change while
// the process runs.
type moveCatalog struct {
	once   sync.Once
	tables map[string]*pgdata.Table
	order  []string
	err    error
}

var catalogs sync.Map // *pgxpool.Pool -> *moveCatalog

func (b *Backend) catalog(ctx context.Context) (map[string]*pgdata.Table, []string, error) {
	v, _ := catalogs.LoadOrStore(b.pool, &moveCatalog{})
	c := v.(*moveCatalog)
	c.once.Do(func() {
		if c.tables, c.err = pgdata.Tables(ctx, b.pool, moveTables); c.err == nil {
			c.order, c.err = pgdata.ParentsFirst(c.tables)
		}
	})
	return c.tables, c.order, c.err
}

func (b *Backend) MoveTables(ctx context.Context) ([]string, error) {
	_, order, err := b.catalog(ctx)
	return order, err
}

func (b *Backend) table(ctx context.Context, name string) (*pgdata.Table, error) {
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

// moveSkip are the columns PostgreSQL alone has, which a move does not carry: a content index's
// log's transaction IDs and stamps, and its fold's snapshot and stamps' sequence. The SCM exports
// the log itself, in the order it applies (Store.exportContentLog); where the rows arrive they
// take those columns' defaults, and read in that order by their numbers.
var moveSkip = map[string]bool{"xid": true, "stamp": true, "folded_through": true, "log_seq": true}

func (b *Backend) ExportRows(ctx context.Context, m backend.Moving, table string, after *okpb.Row, limit int) ([]*okpb.Row, error) {
	t, err := b.table(ctx, table)
	if err != nil {
		return nil, err
	}
	carried := *t
	carried.Columns = slices.DeleteFunc(slices.Clone(t.Columns), func(c pgdata.Column) bool { return moveSkip[c.Name] })
	return pgdata.ExportRows(ctx, b.pool, &carried, moveWhere(table), moveArgs(m), after, limit)
}

func (b *Backend) ImportRows(ctx context.Context, m backend.Moving, table string, rows []*okpb.Row) error {
	t, err := b.table(ctx, table)
	if err != nil {
		return err
	}
	return pgdata.ImportRows(ctx, b.pool, t, rows)
}

func (b *Backend) FinishImport(ctx context.Context, m backend.Moving) error {
	tables, order, err := b.catalog(ctx)
	if err != nil {
		return err
	}
	for _, name := range order {
		if err := pgdata.AdvanceSequences(ctx, b.pool, tables[name]); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backend) DropNetworkRows(ctx context.Context, m backend.Moving, limit int) (int64, error) {
	_, order, err := b.catalog(ctx)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, table := range slices.Backward(order) {
		if n >= int64(limit) {
			break
		}
		d, err := pgdata.DeleteRows(ctx, b.pool, table, moveWhere(table), moveArgs(m), limit-int(n))
		if err != nil {
			return n, fmt.Errorf("dropping %s: %w", table, err)
		}
		n += d
	}
	return n, nil
}

func (b *Backend) SetNetworkMove(ctx context.Context, net backend.Network, state, cell string) error {
	tag, err := b.pool.Exec(ctx, `UPDATE network SET move_state = $2, moved_to = $3, moved_at = CASE WHEN $2 = 'moved' THEN now() END
		WHERE id = $1`, net, state, cell)
	if err == nil && tag.RowsAffected() == 0 {
		err = fmt.Errorf("no network %d", net)
	}
	return err
}

func (x queries) NetworkMove(ctx context.Context, net backend.Network) (string, string, error) {
	var state, cell string
	err := x.q.QueryRow(ctx, `SELECT move_state, moved_to FROM network WHERE id = $1 FOR SHARE`, net).Scan(&state, &cell)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", nil
	}
	return state, cell, err
}
