package move

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/data/spannertxn"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Rows are a role's rows about repositories, and their gates: what RepositoryRole moves.
type Rows interface {
	// Order is the tables, parents first.
	Order(ctx context.Context) ([]string, error)
	Export(ctx context.Context, repos []int64, table string, after *okpb.Row, limit int) ([]*okpb.Row, error)
	Import(ctx context.Context, table string, rows []*okpb.Row) error
	// Finish moves what numbers rows past those brought, the repositories' gates closed until they
	// open here.
	Finish(ctx context.Context, repos []int64) error
	// Drop deletes up to limit of the repositories' rows, children first, then their gates, and
	// returns how many.
	Drop(ctx context.Context, repos []int64, limit int) (int64, error)
	// SetGates sets the repositories' gates, waiting for the writes under way about them.
	SetGates(ctx context.Context, repos []int64, state, cell string) error
	// MovedBefore are up to limit of the repositories that moved away before a time, after one, by
	// ID; MovedAt is when one moved away, if it did — its gate says it moved.
	MovedBefore(ctx context.Context, before time.Time, after int64, limit int) ([]int64, error)
	MovedAt(ctx context.Context, repo int64) (at time.Time, moved bool, err error)
}

// PostgresRows are a role's rows about repositories in PostgreSQL: Tables, each's rows about
// repositories (@repos) those Where says.
type PostgresRows struct {
	Pool   *pgxpool.Pool
	Tables []string
	Where  func(table string) string

	once   sync.Once
	tables map[string]*pgdata.Table
	order  []string
	err    error
}

func (p *PostgresRows) catalog(ctx context.Context) error {
	p.once.Do(func() {
		if p.tables, p.err = pgdata.Tables(ctx, p.Pool, p.Tables); p.err == nil {
			p.order, p.err = pgdata.ParentsFirst(p.tables)
		}
	})
	return p.err
}

func (p *PostgresRows) table(ctx context.Context, name string) (*pgdata.Table, error) {
	if err := p.catalog(ctx); err != nil {
		return nil, err
	}
	t, ok := p.tables[name]
	if !ok {
		return nil, fmt.Errorf("move: no table %s moves", name)
	}
	return t, nil
}

func (p *PostgresRows) Order(ctx context.Context) ([]string, error) {
	err := p.catalog(ctx)
	return p.order, err
}

func (p *PostgresRows) Export(ctx context.Context, repos []int64, table string, after *okpb.Row, limit int) ([]*okpb.Row, error) {
	t, err := p.table(ctx, table)
	if err != nil {
		return nil, err
	}
	return pgdata.ExportRows(ctx, p.Pool, t, p.Where(table), pgx.NamedArgs{"repos": repos}, after, limit)
}

func (p *PostgresRows) Import(ctx context.Context, table string, rows []*okpb.Row) error {
	t, err := p.table(ctx, table)
	if err != nil {
		return err
	}
	return pgdata.ImportRows(ctx, p.Pool, t, rows)
}

func (p *PostgresRows) Finish(ctx context.Context, repos []int64) error {
	if err := p.catalog(ctx); err != nil {
		return err
	}
	for _, name := range p.order {
		if err := pgdata.AdvanceSequences(ctx, p.Pool, p.tables[name]); err != nil {
			return err
		}
	}
	return SetGates(ctx, p.Pool, repos, StateFrozen, "")
}

func (p *PostgresRows) Drop(ctx context.Context, repos []int64, limit int) (int64, error) {
	if err := p.catalog(ctx); err != nil {
		return 0, err
	}
	args := pgx.NamedArgs{"repos": repos}
	var n int64
	for _, table := range slices.Backward(p.order) {
		if n >= int64(limit) {
			return n, nil
		}
		d, err := pgdata.DeleteRows(ctx, p.Pool, table, p.Where(table), args, limit-int(n))
		if err != nil {
			return n, fmt.Errorf("dropping %s: %w", table, err)
		}
		n += d
	}
	if n >= int64(limit) {
		return n, nil
	}
	d, err := pgdata.DeleteRows(ctx, p.Pool, "move_gate", "repository_id = ANY(@repos)", args, limit-int(n))
	return n + d, err
}

func (p *PostgresRows) SetGates(ctx context.Context, repos []int64, state, cell string) error {
	return SetGates(ctx, p.Pool, repos, state, cell)
}

func (p *PostgresRows) MovedBefore(ctx context.Context, before time.Time, after int64, limit int) ([]int64, error) {
	rows, err := p.Pool.Query(ctx, `SELECT repository_id FROM move_gate WHERE move_state = 'moved' AND moved_at < $1 AND repository_id > $2
		ORDER BY repository_id LIMIT $3`, before, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func (p *PostgresRows) MovedAt(ctx context.Context, repo int64) (time.Time, bool, error) {
	var at *time.Time
	err := p.Pool.QueryRow(ctx, `SELECT moved_at FROM move_gate WHERE repository_id = $1 AND move_state = 'moved'`, repo).Scan(&at)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && at == nil {
		return time.Time{}, false, nil
	}
	return *at, err == nil, err
}

// SpannerRows are a role's rows about repositories in Spanner: Tables, each's rows about
// repositories (@repos) those Where says, by Spanner's names; Renames the columns it names
// otherwise than PostgreSQL, Skip those it alone has, Fill what an import fills of them.
type SpannerRows struct {
	Client  *spanner.Client
	Tables  []string
	Where   func(table string) string
	Renames spannerdata.Renames
	Skip    map[string]bool
	Fill    func(table string, row map[string]*okpb.Value)

	once   sync.Once
	tables map[string]*spannerdata.Table
	order  []string
	err    error
}

func (s *SpannerRows) catalog(ctx context.Context) error {
	s.once.Do(func() {
		ro := s.Client.ReadOnlyTransaction()
		defer ro.Close()
		if s.tables, s.err = spannerdata.Tables(ctx, ro, s.Tables, s.Renames); s.err == nil {
			s.order, s.err = spannerdata.ParentsFirst(s.tables)
		}
	})
	return s.err
}

func (s *SpannerRows) table(ctx context.Context, name string) (*spannerdata.Table, error) {
	if err := s.catalog(ctx); err != nil {
		return nil, err
	}
	t, ok := s.tables[name]
	if !ok {
		return nil, fmt.Errorf("move: no table %s moves", name)
	}
	return t, nil
}

func (s *SpannerRows) Order(ctx context.Context) ([]string, error) {
	err := s.catalog(ctx)
	return s.order, err
}

func repoParams(repos []int64) map[string]any {
	if repos == nil {
		repos = []int64{}
	}
	return map[string]any{"repos": repos}
}

func (s *SpannerRows) Export(ctx context.Context, repos []int64, table string, after *okpb.Row, limit int) ([]*okpb.Row, error) {
	t, err := s.table(ctx, table)
	if err != nil {
		return nil, err
	}
	return spannerdata.ExportRows(ctx, s.Client.Single(), t, s.Where(table), repoParams(repos), after, limit, s.Skip)
}

func (s *SpannerRows) Import(ctx context.Context, table string, rows []*okpb.Row) error {
	t, err := s.table(ctx, table)
	if err != nil {
		return err
	}
	var fill func(map[string]*okpb.Value)
	if s.Fill != nil {
		fill = func(v map[string]*okpb.Value) { s.Fill(table, v) }
	}
	return spannerdata.ImportRows(ctx, s.Client, t, rows, fill)
}

func (s *SpannerRows) Finish(ctx context.Context, repos []int64) error {
	return s.SetGates(ctx, repos, StateFrozen, "")
}

func (s *SpannerRows) Drop(ctx context.Context, repos []int64, limit int) (int64, error) {
	if err := s.catalog(ctx); err != nil {
		return 0, err
	}
	p := repoParams(repos)
	var n int64
	for _, table := range slices.Backward(s.order) {
		if n >= int64(limit) {
			return n, nil
		}
		d, err := spannerdata.DeleteRows(ctx, s.Client, s.tables[table], s.Where(table), p, limit-int(n))
		if err != nil {
			return n, fmt.Errorf("dropping %s: %w", table, err)
		}
		n += d
	}
	if n >= int64(limit) {
		return n, nil
	}
	_, err := spannertxn.ReadWrite(ctx, s.Client, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var ks []spanner.KeySet
		for _, r := range repos {
			ks = append(ks, spanner.Key{r})
		}
		return tx.BufferWrite([]*spanner.Mutation{spanner.Delete("move_gate", spanner.KeySets(ks...))})
	})
	return n, err
}

func (s *SpannerRows) SetGates(ctx context.Context, repos []int64, state, cell string) error {
	_, err := spannertxn.ReadWrite(ctx, s.Client, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		// Read first, which waits for every write under way that read them.
		var ks []spanner.KeySet
		for _, r := range repos {
			ks = append(ks, spanner.Key{r})
		}
		if err := tx.Read(ctx, "move_gate", spanner.KeySets(ks...), []string{"repository_id"}).Do(func(*spanner.Row) error { return nil }); err != nil {
			return err
		}
		return SetSpannerGates(tx, repos, state, cell)
	})
	return err
}

func (s *SpannerRows) MovedAt(ctx context.Context, repo int64) (time.Time, bool, error) {
	var at spanner.NullTime
	var moved bool
	err := s.Client.Single().Query(ctx, spanner.Statement{SQL: `SELECT moved_at FROM move_gate WHERE repository_id = @repo AND move_state = 'moved'`,
		Params: map[string]any{"repo": repo}}).Do(func(r *spanner.Row) error {
		moved = true
		return r.Columns(&at)
	})
	return at.Time, moved && at.Valid, err
}

func (s *SpannerRows) MovedBefore(ctx context.Context, before time.Time, after int64, limit int) ([]int64, error) {
	var out []int64
	err := s.Client.Single().Query(ctx, spanner.Statement{SQL: `SELECT repository_id FROM move_gate
		WHERE move_state = 'moved' AND moved_at < @before AND repository_id > @after ORDER BY repository_id LIMIT @limit`,
		Params: map[string]any{"before": before, "after": after, "limit": int64(limit)}}).
		Do(func(r *spanner.Row) error {
			var id int64
			if err := r.Columns(&id); err != nil {
				return err
			}
			out = append(out, id)
			return nil
		})
	return out, err
}
