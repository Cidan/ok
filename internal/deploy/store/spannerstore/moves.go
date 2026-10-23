package spannerstore

import (
	"context"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/data/spannertxn"
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

// moveRenames are the columns this schema names otherwise than PostgreSQL's.
var moveRenames = spannerdata.Renames{
	"service":          {"id": "service_id"},
	"deployment":       {"id": "deployment_id"},
	"deployment_event": {"at": "event_at"},
}

// moveWhere is the condition a table's rows about repositories (@repos) meet, by Spanner's names.
func moveWhere(table string) string {
	switch table {
	case "service_observation":
		return "service_id IN (SELECT service_id FROM service WHERE repository_id IN UNNEST(@repos))"
	case "deployment_image", "deployment_migration", "deployment_event":
		return "deployment_id IN (SELECT deployment_id FROM deployment WHERE repository_id IN UNNEST(@repos))"
	case "follow_cursor":
		return "topic = '" + scmfeed.Topic + "' AND key IN UNNEST(@repos)"
	}
	return "repository_id IN UNNEST(@repos)"
}

// moveRows are the deployer's rows about repositories: the tables' whole, and the repository's
// head and what is planned, first — the parent its main's history and plans are interleaved in.
type moveRows struct {
	*move.SpannerRows
	client *spanner.Client
}

func (m *moveRows) Order(ctx context.Context) ([]string, error) {
	order, err := m.SpannerRows.Order(ctx)
	return append([]string{"repository"}, order...), err
}

func repoParams(repos []int64) map[string]any {
	if repos == nil {
		repos = []int64{}
	}
	return map[string]any{"repos": repos}
}

func (m *moveRows) Export(ctx context.Context, repos []int64, table string, after *okpb.Row, limit int) ([]*okpb.Row, error) {
	if table != "repository" {
		return m.SpannerRows.Export(ctx, repos, table, after, limit)
	}
	p := repoParams(repos)
	p["after"] = int64(-1 << 63)
	for _, c := range after.GetColumns() {
		if c.GetName() == "id" {
			p["after"] = c.GetValue().GetInt()
		}
	}
	p["limit"] = int64(limit)
	var out []*okpb.Row
	err := spannerdata.Each(ctx, m.client.Single(), `SELECT repository_id, head, head_seq, planned_head, planned_seq FROM repository
		WHERE repository_id IN UNNEST(@repos) AND repository_id > @after ORDER BY repository_id LIMIT @limit`, p, func(row *spanner.Row) error {
		var id, seq, plannedSeq int64
		var head, planned []byte
		if err := row.Columns(&id, &head, &seq, &planned, &plannedSeq); err != nil {
			return err
		}
		out = append(out, &okpb.Row{Columns: []*okpb.Column{
			{Name: "id", Value: &okpb.Value{Kind: &okpb.Value_Int{Int: id}}},
			{Name: "head", Value: dataValue(head)},
			{Name: "head_seq", Value: &okpb.Value{Kind: &okpb.Value_Int{Int: seq}}},
			{Name: "planned_head", Value: dataValue(planned)},
			{Name: "planned_seq", Value: &okpb.Value{Kind: &okpb.Value_Int{Int: plannedSeq}}},
		}})
		return nil
	})
	return out, err
}

func dataValue(b []byte) *okpb.Value {
	if b == nil {
		return &okpb.Value{Kind: &okpb.Value_Null{Null: true}}
	}
	return &okpb.Value{Kind: &okpb.Value_Data{Data: b}}
}

func bytesOrNil(v *okpb.Value) []byte {
	if v == nil || v.GetNull() {
		return nil
	}
	return append([]byte{}, v.GetData()...)
}

// Import writes a repository's head and what is planned over this cell's, its name as the
// directory has named it here (or not yet: the directory's follower names it).
func (m *moveRows) Import(ctx context.Context, table string, rows []*okpb.Row) error {
	if table != "repository" {
		return m.SpannerRows.Import(ctx, table, rows)
	}
	_, err := spannertxn.ReadWrite(ctx, m.client, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		for _, r := range rows {
			v := map[string]*okpb.Value{}
			for _, c := range r.GetColumns() {
				v[c.GetName()] = c.GetValue()
			}
			id := v["id"].GetInt()
			_, err := tx.ReadRow(ctx, "repository", spanner.Key{id}, []string{"repository_id"})
			cols := []string{"repository_id", "head", "head_seq", "planned_head", "planned_seq"}
			vals := []any{id, bytesOrNil(v["head"]), v["head_seq"].GetInt(), bytesOrNil(v["planned_head"]), v["planned_seq"].GetInt()}
			switch {
			case spanner.ErrCode(err) == codes.NotFound:
				if err := tx.BufferWrite([]*spanner.Mutation{spanner.Insert("repository", append(cols, "name"), append(vals, ""))}); err != nil {
					return err
				}
			case err != nil:
				return err
			default:
				if err := tx.BufferWrite([]*spanner.Mutation{spanner.Update("repository", cols, vals)}); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return err
}

// Drop deletes the repositories' rows, and forgets their heads and plans: their rows are the
// directory's names, which stay.
func (m *moveRows) Drop(ctx context.Context, repos []int64, limit int) (int64, error) {
	n, err := m.SpannerRows.Drop(ctx, repos, limit)
	if err != nil || n >= int64(limit) {
		return n, err
	}
	_, err = spannertxn.ReadWrite(ctx, m.client, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{SQL: `UPDATE repository SET head = NULL, head_seq = 0, planned_head = NULL, planned_seq = 0
			WHERE repository_id IN UNNEST(@repos)`, Params: repoParams(repos)})
		return err
	})
	return n, err
}

// Moves are the backend's rows about repositories, for a move.
func (b *Backend) Moves() move.Rows { return b.moves }

func (x queries) Gate(ctx context.Context, repo int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return move.SpannerGateRepository(ctx, tx, repo) })
}

func (x queries) GateState(ctx context.Context, repo int64) (string, error) {
	var state string
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		state, _, err = move.SpannerGateState(ctx, tx, repo)
		return err
	})
	return state, err
}

func (x queries) RunningDeployments(ctx context.Context, repos []int64) ([]int64, error) {
	var out []int64
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT deployment_id FROM deployment WHERE repository_id IN UNNEST(@repos) AND status = 2
			ORDER BY deployment_id`, repoParams(repos), func(row *spanner.Row) error {
			var id int64
			if err := row.Columns(&id); err != nil {
				return err
			}
			out = append(out, id)
			return nil
		})
	})
	return out, err
}
