package move

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/cells"
)

// A repository's gate (docs/hosted.md, "Cells, as built"), for the roles that keep rows about
// repositories but no network: ok control, ok automation, the deployer. Each keeps a move_gate row
// per repository — its move state, and the cell it moved to — which every write transaction about
// a repository (named in ctx, WithRepository) reads first, in share mode; freezing writes it, so it
// waits for the writes under way, and none starts after.
//
// A role's write transactions read it in its backend's Txn (PostgresGate, SpannerGate), and its
// writes outside one run in one of their own that reads it first (GatedPool, Spanner's own
// transactions through spannerdata.Ops.Gate).

// Move states, as a gate holds them.
const (
	StateFrozen = "frozen"
	StateMoved  = "moved"
)

// GateDDL is a role's gate table on PostgreSQL, for its migrations.
const GateDDL = `CREATE TABLE move_gate (
    repository_id bigint PRIMARY KEY,
    move_state    text NOT NULL DEFAULT '' CHECK (move_state IN ('', 'frozen', 'moved')),
    moved_to      text NOT NULL DEFAULT '',
    moved_at      timestamptz
)`

// SpannerGateDDL is a role's gate table on Spanner.
const SpannerGateDDL = `CREATE TABLE move_gate (
    repository_id INT64       NOT NULL,
    move_state    STRING(MAX) NOT NULL,
    moved_to      STRING(MAX) NOT NULL,
    moved_at      TIMESTAMP,
) PRIMARY KEY (repository_id)`

// refusal is the gate's answer to a write about a repository in a state.
func refusal(repo int64, state, cell string) error {
	switch state {
	case StateFrozen:
		return Frozen(repo)
	case StateMoved:
		return cells.Moved(repo, cell)
	}
	return nil
}

// pgQuerier is what PostgresGate runs its statements on: a transaction.
type pgQuerier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PostgresGate reads, in tx, the gate of the repository ctx writes about, if any — making the row
// first, as PostgreSQL locks no row that is not there — and refuses the transaction if the
// repository's network is frozen or moved away.
func PostgresGate(ctx context.Context, tx pgQuerier) error {
	repo, ok := RepositoryOf(ctx)
	if !ok {
		return nil
	}
	return GateRepository(ctx, tx, repo)
}

// GateRepository reads, in tx, a repository's gate (GateState) and refuses the transaction if the
// repository's network is frozen or moved away: for a transaction that learns its repository from
// a row it locks. A CheckedTx is gated from then on.
func GateRepository(ctx context.Context, tx pgQuerier, repo int64) error {
	state, cell, err := GateState(ctx, tx, repo)
	if err != nil {
		return err
	}
	return refusal(repo, state, cell)
}

// GateState reads, in tx, a repository's gate — making the row first, as PostgreSQL locks no row
// that is not there — holding it in share mode until tx ends: its move state ("", frozen, moved),
// and the cell it moved to. For a transaction about many repositories that leaves the moving ones
// be. A CheckedTx is gated from then on.
func GateState(ctx context.Context, tx pgQuerier, repo int64) (state, cell string, err error) {
	if c, ok := tx.(*CheckedTx); ok {
		c.Gated = true
	}
	if _, err := tx.Exec(ctx, `INSERT INTO move_gate (repository_id) VALUES ($1) ON CONFLICT DO NOTHING`, repo); err != nil {
		return "", "", fmt.Errorf("the move gate of repository %d: %w", repo, err)
	}
	if err := tx.QueryRow(ctx, `SELECT move_state, moved_to FROM move_gate WHERE repository_id = $1 FOR SHARE`, repo).Scan(&state, &cell); err != nil {
		return "", "", fmt.Errorf("the move gate of repository %d: %w", repo, err)
	}
	return state, cell, nil
}

// SpannerGate reads, in tx, the gate of the repository ctx writes about, if any — which locks its
// key, there or not — and refuses the transaction if the repository's network is frozen or moved
// away.
func SpannerGate(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
	repo, ok := RepositoryOf(ctx)
	if !ok {
		return nil
	}
	return SpannerGateRepository(ctx, tx, repo)
}

// SpannerGateRepository reads, in tx, a repository's gate, which locks its key, and refuses the
// transaction if the repository's network is frozen or moved away.
func SpannerGateRepository(ctx context.Context, tx *spanner.ReadWriteTransaction, repo int64) error {
	state, cell, err := SpannerGateState(ctx, tx, repo)
	if err != nil {
		return err
	}
	return refusal(repo, state, cell)
}

// SpannerGateState reads, in tx, a repository's gate, which locks its key: its move state and the
// cell it moved to.
func SpannerGateState(ctx context.Context, tx *spanner.ReadWriteTransaction, repo int64) (state, cell string, err error) {
	row, err := tx.ReadRow(ctx, "move_gate", spanner.Key{repo}, []string{"move_state", "moved_to"})
	if spanner.ErrCode(err) == codes.NotFound {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("the move gate of repository %d: %w", repo, err)
	}
	err = row.Columns(&state, &cell)
	return state, cell, err
}

// SetGates sets the gates of repositories, PostgreSQL's: in one transaction, which waits for every
// write under way about them.
func SetGates(ctx context.Context, pool *pgxpool.Pool, repos []int64, state, cell string) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO move_gate (repository_id, move_state, moved_to, moved_at)
			SELECT r, $2, $3, CASE WHEN $2 = 'moved' THEN now() END FROM unnest($1::bigint[]) AS r
			ON CONFLICT (repository_id) DO UPDATE SET move_state = EXCLUDED.move_state, moved_to = EXCLUDED.moved_to, moved_at = EXCLUDED.moved_at`,
			repos, state, cell)
		return err
	})
}

// SetSpannerGates sets the gates of repositories, Spanner's, in tx.
func SetSpannerGates(tx *spanner.ReadWriteTransaction, repos []int64, state, cell string) error {
	var ms []*spanner.Mutation
	at := spanner.NullTime{}
	if state == StateMoved {
		at = spanner.NullTime{Time: time.Now().UTC(), Valid: true}
	}
	for _, r := range repos {
		ms = append(ms, spanner.InsertOrUpdate("move_gate", []string{"repository_id", "move_state", "moved_to", "moved_at"},
			[]any{r, state, cell, at}))
	}
	return tx.BufferWrite(ms)
}

// GatedPool is a role's pool, as its backend's operations run on it outside a transaction: a write
// about a repository named in ctx runs in a transaction of its own that reads the repository's
// gate first (PostgresGate). Reads, and writes about none, run on the pool as they are.
type GatedPool struct {
	Pool *pgxpool.Pool
	// Tables are the role's tables of rows about repositories: in strict mode (Strict), a write to
	// one about no repository named in ctx is refused, for the suites to show none is.
	Tables map[string]bool
}

// strict is whether writes about repositories must name theirs (OK_TEST_MOVE_STRICT): ok's suites
// run so, to show every write path does.
var strict = os.Getenv("OK_TEST_MOVE_STRICT") == "1"

// Strict reports whether writes about repositories must name theirs.
func Strict() bool { return strict }

// writeTarget is the table a statement writes, if it is a write: INSERT INTO t, UPDATE t, DELETE
// FROM t, after any WITH.
var writeTarget = regexp.MustCompile(`(?is)^\s*(?:WITH\b.*?\)\s*)?(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+"?([a-z_]+)"?`)

func isWrite(sql string) (string, bool) {
	m := writeTarget.FindStringSubmatch(sql)
	if m == nil {
		return "", false
	}
	return strings.ToLower(m[1]), true
}

// ErrUntagged is a write, in strict mode, to a table of rows about repositories by a path that
// named no repository.
var ErrUntagged = errors.New("move: a write about a repository that names none (move.WithRepository)")

// CheckTagged refuses, in strict mode, a write to one of tables about no repository named in ctx.
func CheckTagged(ctx context.Context, tables map[string]bool, sql string) error {
	if !strict {
		return nil
	}
	if t, ok := isWrite(sql); ok && tables[t] {
		if _, tagged := RepositoryOf(ctx); !tagged {
			if _, exempt := ctx.Value(untaggedKey{}).(bool); !exempt {
				return fmt.Errorf("%w: %s", ErrUntagged, strings.Join(strings.Fields(sql), " "))
			}
		}
	}
	return nil
}

type untaggedKey struct{}

// Untagged is ctx for a write that is about many repositories at once — a sweep, a purge — which
// strict mode lets through.
func Untagged(ctx context.Context) context.Context {
	return context.WithValue(ctx, untaggedKey{}, true)
}

func (g GatedPool) gated(ctx context.Context, sql string) (bool, error) {
	if err := CheckTagged(ctx, g.Tables, sql); err != nil {
		return false, err
	}
	_, tagged := RepositoryOf(ctx)
	_, write := isWrite(sql)
	return tagged && write, nil
}

func (g GatedPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	gated, err := g.gated(ctx, sql)
	if err != nil || !gated {
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		return g.Pool.Exec(ctx, sql, args...)
	}
	var tag pgconn.CommandTag
	err = pgx.BeginFunc(ctx, g.Pool, func(tx pgx.Tx) error {
		if err := PostgresGate(ctx, tx); err != nil {
			return err
		}
		var err error
		tag, err = tx.Exec(ctx, sql, args...)
		return err
	})
	return tag, err
}

func (g GatedPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	gated, err := g.gated(ctx, sql)
	if err != nil {
		return errRow{err}
	}
	if !gated {
		return g.Pool.QueryRow(ctx, sql, args...)
	}
	return gatedRow{g: g, ctx: ctx, sql: sql, args: args}
}

// gatedRow runs its write, and reads its row, in a gated transaction when scanned.
type gatedRow struct {
	g    GatedPool
	ctx  context.Context
	sql  string
	args []any
}

func (r gatedRow) Scan(dest ...any) error {
	return pgx.BeginFunc(r.ctx, r.g.Pool, func(tx pgx.Tx) error {
		if err := PostgresGate(r.ctx, tx); err != nil {
			return err
		}
		return tx.QueryRow(r.ctx, r.sql, r.args...).Scan(dest...)
	})
}

type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }

func (g GatedPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	gated, err := g.gated(ctx, sql)
	if err != nil {
		return nil, err
	}
	if !gated {
		return g.Pool.Query(ctx, sql, args...)
	}
	out := &bufferedRows{}
	err = pgx.BeginFunc(ctx, g.Pool, func(tx pgx.Tx) error {
		if err := PostgresGate(ctx, tx); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		out.types = tx.Conn().TypeMap()
		out.fields = rows.FieldDescriptions()
		out.rows = nil
		for rows.Next() {
			raw := rows.RawValues()
			row := make([][]byte, len(raw))
			for i, v := range raw {
				if v != nil {
					row[i] = append([]byte{}, v...)
				}
			}
			out.rows = append(out.rows, row)
		}
		out.tag = rows.CommandTag()
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (g GatedPool) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	var write bool
	for _, q := range b.QueuedQueries {
		gated, err := g.gated(ctx, q.SQL)
		if err != nil {
			return doneBatch{err}
		}
		write = write || gated
	}
	if !write {
		return g.Pool.SendBatch(ctx, b)
	}
	return doneBatch{pgx.BeginFunc(ctx, g.Pool, func(tx pgx.Tx) error {
		if err := PostgresGate(ctx, tx); err != nil {
			return err
		}
		return tx.SendBatch(ctx, b).Close()
	})}
}

// bufferedRows are a write's rows, read in its transaction before it committed.
type bufferedRows struct {
	types  *pgtype.Map
	fields []pgconn.FieldDescription
	rows   [][][]byte
	i      int
	tag    pgconn.CommandTag
}

func (r *bufferedRows) Close()                                       {}
func (r *bufferedRows) Err() error                                   { return nil }
func (r *bufferedRows) CommandTag() pgconn.CommandTag                { return r.tag }
func (r *bufferedRows) FieldDescriptions() []pgconn.FieldDescription { return r.fields }
func (r *bufferedRows) Conn() *pgx.Conn                              { return nil }
func (r *bufferedRows) TypeMap() *pgtype.Map                         { return r.types }
func (r *bufferedRows) RawValues() [][]byte                          { return r.rows[r.i-1] }

func (r *bufferedRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *bufferedRows) Scan(dest ...any) error {
	return pgx.ScanRow(r.types, r.fields, r.rows[r.i-1], dest...)
}

func (r *bufferedRows) Values() ([]any, error) {
	out := make([]any, len(r.fields))
	for i, fd := range r.fields {
		raw := r.rows[r.i-1][i]
		if raw == nil {
			continue
		}
		dt, ok := r.types.TypeForOID(fd.DataTypeOID)
		if !ok {
			out[i] = string(raw)
			continue
		}
		v, err := dt.Codec.DecodeValue(r.types, fd.DataTypeOID, fd.Format, raw)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// CheckedTx is a transaction's statements as a role's backend runs them: in strict mode, a write to
// one of Tables is refused unless ctx names its repository or the transaction read a repository's
// gate (GateRepository).
type CheckedTx struct {
	pgx.Tx
	Tables map[string]bool
	Gated  bool
}

func (t *CheckedTx) check(ctx context.Context, sql string) error {
	if t.Gated {
		return nil
	}
	return CheckTagged(ctx, t.Tables, sql)
}

func (t *CheckedTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := t.check(ctx, sql); err != nil {
		return pgconn.CommandTag{}, err
	}
	return t.Tx.Exec(ctx, sql, args...)
}

func (t *CheckedTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := t.check(ctx, sql); err != nil {
		return nil, err
	}
	return t.Tx.Query(ctx, sql, args...)
}

func (t *CheckedTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if err := t.check(ctx, sql); err != nil {
		return errRow{err}
	}
	return t.Tx.QueryRow(ctx, sql, args...)
}

func (t *CheckedTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	for _, q := range b.QueuedQueries {
		if err := t.check(ctx, q.SQL); err != nil {
			return doneBatch{err}
		}
	}
	return t.Tx.SendBatch(ctx, b)
}

// doneBatch is a batch run already, which Close reports.
type doneBatch struct{ err error }

func (b doneBatch) Exec() (pgconn.CommandTag, error) { return pgconn.CommandTag{}, b.errOr() }
func (b doneBatch) Query() (pgx.Rows, error)         { return nil, b.errOr() }
func (b doneBatch) QueryRow() pgx.Row                { return errRow{b.errOr()} }
func (b doneBatch) Close() error                     { return b.err }

func (b doneBatch) errOr() error {
	if b.err != nil {
		return b.err
	}
	return errors.New("move: a gated batch's results are its Close's alone")
}
