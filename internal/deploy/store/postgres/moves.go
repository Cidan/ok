package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// moveTables are the tables of rows about repositories this cell holds, which a move copies
// (docs/hosted.md, "Cells, as built"): main's history, plans, services, deployments, and where the
// SCM's feed of each was applied to; and of the repository table, its head and what is planned
// (repositoryColumns) — its name is the directory's, which every cell's deployer keeps.
var moveTables = []string{"main_commit", "plan", "service", "service_observation", "deployment", "deployment_image",
	"deployment_migration", "deployment_event", "follow_cursor"}

var moveTableSet = func() map[string]bool {
	m := map[string]bool{}
	for _, t := range moveTables {
		m[t] = true
	}
	return m
}()

// moveWhere is the condition a table's rows about repositories (@repos) meet.
func moveWhere(table string) string {
	switch table {
	case "service_observation":
		return "service_id IN (SELECT id FROM service WHERE repository_id = ANY(@repos))"
	case "deployment_image", "deployment_migration", "deployment_event":
		return "deployment_id IN (SELECT id FROM deployment WHERE repository_id = ANY(@repos))"
	case "follow_cursor":
		return "topic = '" + scmfeed.Topic + "' AND key = ANY(@repos)"
	}
	return "repository_id = ANY(@repos)"
}

// repositoryColumns are the repository table's columns a move carries: this cell's of it.
var repositoryColumns = []string{"id", "head", "head_seq", "planned_head", "planned_seq"}

// moveRows are the deployer's rows about repositories: the tables' whole, and the repository's
// head and what is planned, first.
type moveRows struct {
	*move.PostgresRows
	pool pgdata.RowsQuerier
}

func (m *moveRows) Order(ctx context.Context) ([]string, error) {
	order, err := m.PostgresRows.Order(ctx)
	return append([]string{"repository"}, order...), err
}

func (m *moveRows) Export(ctx context.Context, repos []int64, table string, after *okpb.Row, limit int) ([]*okpb.Row, error) {
	if table != "repository" {
		return m.PostgresRows.Export(ctx, repos, table, after, limit)
	}
	tables, err := pgdata.Tables(ctx, m.pool, []string{"repository"})
	if err != nil {
		return nil, err
	}
	t := *tables["repository"]
	t.Columns = nil
	for _, name := range repositoryColumns {
		for _, c := range tables["repository"].Columns {
			if c.Name == name {
				t.Columns = append(t.Columns, c)
			}
		}
	}
	return pgdata.ExportRows(ctx, m.pool, &t, "id = ANY(@repos)", pgx.NamedArgs{"repos": repos}, after, limit)
}

// Import writes a repository's head and what is planned over this cell's, its name as the
// directory has named it here (or not yet: the directory's follower names it).
func (m *moveRows) Import(ctx context.Context, table string, rows []*okpb.Row) error {
	if table != "repository" {
		return m.PostgresRows.Import(ctx, table, rows)
	}
	for _, r := range rows {
		v := map[string]*okpb.Value{}
		for _, c := range r.GetColumns() {
			v[c.GetName()] = c.GetValue()
		}
		if _, err := m.pool.Exec(ctx, `INSERT INTO repository (id, name, head, head_seq, planned_head, planned_seq) VALUES ($1, '', $2, $3, $4, $5)
			ON CONFLICT (id) DO UPDATE SET head = EXCLUDED.head, head_seq = EXCLUDED.head_seq, planned_head = EXCLUDED.planned_head,
				planned_seq = EXCLUDED.planned_seq`,
			v["id"].GetInt(), bytesOrNil(v["head"]), v["head_seq"].GetInt(), bytesOrNil(v["planned_head"]), v["planned_seq"].GetInt()); err != nil {
			return fmt.Errorf("importing repository %d: %w", v["id"].GetInt(), err)
		}
	}
	return nil
}

func bytesOrNil(v *okpb.Value) []byte {
	if v == nil || v.GetNull() {
		return nil
	}
	return append([]byte{}, v.GetData()...)
}

// Drop deletes the repositories' rows, and forgets their heads and plans: their rows are the
// directory's names, which stay.
func (m *moveRows) Drop(ctx context.Context, repos []int64, limit int) (int64, error) {
	n, err := m.PostgresRows.Drop(ctx, repos, limit)
	if err != nil || n >= int64(limit) {
		return n, err
	}
	_, err = m.pool.Exec(ctx, `UPDATE repository SET head = NULL, head_seq = 0, planned_head = NULL, planned_seq = 0
		WHERE id = ANY($1) AND (head IS NOT NULL OR head_seq <> 0 OR planned_head IS NOT NULL OR planned_seq <> 0)`, repos)
	return n, err
}

// Moves are the backend's rows about repositories, for a move.
func (b *Backend) Moves() move.Rows { return b.moves }

func (x queries) Gate(ctx context.Context, repo int64) error {
	return move.GateRepository(ctx, x.q, repo)
}

func (x queries) GateState(ctx context.Context, repo int64) (string, error) {
	state, _, err := move.GateState(ctx, x.q, repo)
	return state, err
}

func (x queries) RunningDeployments(ctx context.Context, repos []int64) ([]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT id FROM deployment WHERE repository_id = ANY($1) AND status = 2 ORDER BY id`, repos)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}
