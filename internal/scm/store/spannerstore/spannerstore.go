// Package spannerstore is the SCM's database (internal/scm/store/backend) on Spanner, in a
// database of its own (docs/hosted.md, "The SCM on Spanner, as built"). Every write is DML, so a
// transaction reads what it wrote, as on PostgreSQL. A store's transaction runs once (Txn), and its
// statements take turns, as a PostgreSQL connection's do; an operation outside one is a transaction
// of its own, which the client runs again when Spanner aborts it.
package spannerstore

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/data/spannertxn"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are the SCM's schema on Spanner.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Backend is the SCM's database on Spanner.
type Backend struct{ queries }

// Work is the backend's keyed work's store.
func (b *Backend) Work() work.Store { return spannerdata.Work(b.c) }

func (b *Backend) Waits() wait.Store { return spannerdata.Waits(b.c) }

// defaultStageLogBytes are the bytes of content log a transaction writes itself before it stages
// the rest (InsertContentLog).
const defaultStageLogBytes = 32 << 20

var _ backend.Backend = (*Backend)(nil)

// Open brings the database client reaches to the SCM's schema, through admin, and returns a
// backend over it.
func Open(ctx context.Context, admin *database.DatabaseAdminClient, client *spanner.Client) (*Backend, error) {
	if _, err := spannerdata.Migrate(ctx, admin, client, Migrations); err != nil {
		return nil, err
	}
	return &Backend{queries{c: client, stageBytes: defaultStageLogBytes, packNums: &numberBlocks{}}}, nil
}

// Txn runs fn in a read-write transaction: a statement-based one, run again when Spanner aborts
// it — up to data.Attempts times, as the store runs PostgreSQL's — as the aborted one reset
// (ResetForRetry), which keeps its place in Spanner's wound-wait: begun anew, a long transaction
// a stream of shorter ones keeps aborting is younger at every attempt, and never commits (a
// repository's uploads beside its publishes). Its operations may be called from several
// goroutines at once: they take turns, as Spanner runs a transaction's statements in order.
func (b *Backend) Txn(ctx context.Context, fn func(backend.Ops) error) error {
	var stx *spanner.ReadWriteStmtBasedTransaction
	aborted := func(err error) bool { return spanner.ErrCode(err) == codes.Aborted }
	return data.Retry(ctx, aborted, func() (err error) {
		if stx != nil {
			next, rerr := stx.ResetForRetry(ctx)
			if rerr != nil {
				// An abort the transaction did not see — a nested transaction's: it is rolled
				// back, and the next begun anew.
				stx.Rollback(context.WithoutCancel(ctx))
				next = nil
			}
			stx = next
		}
		if stx == nil {
			if stx, err = spanner.NewReadWriteStmtBasedTransaction(ctx, b.c); err != nil {
				return err
			}
		}
		// Measured here, in Txn's own closure, so the site is the code that called Txn
		// (spannertxn's wrappers).
		t := &txn{tx: &stx.ReadWriteTransaction}
		done := spannertxn.Begun(t.tx)
		defer func() { done(err) }()
		work.Begin(b.c, t.tx)
		defer work.End(t.tx)
		// What ctx's lease guards is written only while its holder holds it (data.Fence).
		err = spannerdata.CheckFence(ctx, t.tx)
		if err == nil {
			err = fn(queries{c: b.c, tx: t, stageBytes: b.stageBytes, packNums: b.packNums})
		}
		if err == nil {
			// The jobs of the work it marked, before it commits.
			err = work.Flush(ctx, t.tx)
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		if err != nil {
			if spanner.ErrCode(err) != codes.Aborted {
				stx.Rollback(context.WithoutCancel(ctx))
			}
			return err
		}
		_, err = stx.Commit(ctx)
		return err
	})
}

// Retryable reports a transaction refused a pack number another pack holds, which a transaction
// begun anew numbers from a block reserved anew. One Spanner aborted Txn runs again itself.
func (b *Backend) Retryable(err error) bool {
	return errors.Is(err, errPackNumberTaken)
}

// Snapshot runs fn in a read-only transaction: every read in it at one moment.
func (b *Backend) Snapshot(ctx context.Context, fn func(backend.Ops) error) error {
	ro := b.c.ReadOnlyTransaction()
	defer ro.Close()
	return fn(queries{c: b.c, ro: ro, stageBytes: b.stageBytes, packNums: b.packNums})
}

func (b *Backend) Ping(ctx context.Context) error {
	return b.c.Single().Query(ctx, spanner.Statement{SQL: `SELECT 1`}).Do(func(*spanner.Row) error { return nil })
}

// relayLease is how long a relay holds the pending events it took before another may take them:
// publishing a page takes far less, and a relay that died has its events taken again once it
// lapses.
const relayLease = time.Minute

// RelayEvents reads a page of pending events no relay holds without a lock — a relay's locked scan
// of event_pending would hold off every sequencing pass that inserts into it — then takes those
// still unheld for a lease, in a transaction of its own that reads them by key; publishes them
// outside any transaction, as Spanner aborts a transaction left waiting; then deletes them from
// pending by key, or, if publishing failed, lets them go for another to take. A relay that dies
// after publishing has its events published again once its lease lapses, as on PostgreSQL one that
// publishes and dies before committing does.
func (b *Backend) RelayEvents(ctx context.Context, repo int64, limit int, publish func([]backend.Event) error) (int, error) {
	var seen []backend.Event
	ro := spannerdata.ReadOnly(b.c)
	err := spannerdata.Each(ctx, ro, `SELECT e.repository_id, e.repository_seq, e.body
		FROM event_pending p JOIN event e ON e.repository_id = p.repository_id AND e.repository_seq = p.repository_seq
		WHERE (@repo = 0 OR p.repository_id = @repo) AND (p.lease_until IS NULL OR p.lease_until < `+now+`)
		ORDER BY p.repository_id, p.repository_seq LIMIT @limit`, map[string]any{"repo": repo, "limit": int64(limit)}, func(row *spanner.Row) error {
		var e backend.Event
		if err := row.Columns(&e.Repository, &e.RepositorySeq, &e.Body); err != nil {
			return err
		}
		seen = append(seen, e)
		return nil
	})
	ro.Close()
	if err != nil || len(seen) == 0 {
		return 0, err
	}
	keys := make([]eventKey, len(seen))
	for i, e := range seen {
		keys[i] = eventKey{e.Repository, e.RepositorySeq}
	}
	holder := fmt.Sprintf("relay-%d", data.NewID())
	var evs []backend.Event
	var taken []eventKey
	_, err = spannertxn.ReadWrite(ctx, b.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		evs, taken = nil, nil
		free := map[eventKey]bool{}
		if err := spannerdata.Each(ctx, tx, `SELECT repository_id, repository_seq FROM event_pending
			WHERE STRUCT<r INT64, s INT64>(repository_id, repository_seq) IN UNNEST(@keys)
			  AND (lease_until IS NULL OR lease_until < `+now+`)`, map[string]any{"keys": keys}, func(row *spanner.Row) error {
			var k eventKey
			if err := row.Columns(&k.R, &k.S); err != nil {
				return err
			}
			free[k] = true
			return nil
		}); err != nil {
			return err
		}
		for i, e := range seen {
			if free[keys[i]] {
				evs, taken = append(evs, e), append(taken, keys[i])
			}
		}
		if len(taken) == 0 {
			return nil
		}
		_, err := spannerdata.Update(ctx, tx, `UPDATE event_pending SET holder = @holder,
				lease_until = TIMESTAMP_ADD(`+now+`, INTERVAL @lease SECOND)
			WHERE STRUCT<r INT64, s INT64>(repository_id, repository_seq) IN UNNEST(@keys)`,
			map[string]any{"holder": holder, "lease": int64(relayLease / time.Second), "keys": taken})
		return err
	})
	if err != nil || len(evs) == 0 {
		return 0, err
	}
	perr := publish(evs)
	if _, err := spannertxn.ReadWrite(context.WithoutCancel(ctx), b.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		if perr != nil {
			_, err := spannerdata.Update(ctx, tx, `UPDATE event_pending SET holder = NULL, lease_until = NULL
				WHERE STRUCT<r INT64, s INT64>(repository_id, repository_seq) IN UNNEST(@keys) AND holder = @holder`,
				map[string]any{"holder": holder, "keys": taken})
			return err
		}
		ks := make([]spanner.KeySet, len(taken))
		for i, k := range taken {
			ks[i] = spanner.Key{k.R, k.S}
		}
		return tx.BufferWrite([]*spanner.Mutation{spanner.Delete("event_pending", spanner.KeySets(ks...))})
	}); err != nil && perr == nil {
		return 0, fmt.Errorf("the published events, ending their lease: %w", err)
	}
	if perr != nil {
		return 0, perr
	}
	return len(evs), nil
}

// EventsRelayed reads the repository's counter, which the pass that places events holds until it
// commits, and its pending events, at one moment.
func (b *Backend) EventsRelayed(ctx context.Context, repo, first, last int64) (written, relayed bool, err error) {
	ro := b.c.Single()
	err = spannerdata.Each(ctx, ro, `SELECT IFNULL((SELECT last FROM repository_event_counter WHERE repository_id = @repo), 0) >= @last,
			NOT EXISTS (SELECT 1 FROM event_pending WHERE repository_id = @repo AND (@last = 0 OR repository_seq BETWEEN @first AND @last))`,
		map[string]any{"repo": repo, "first": first, "last": last}, func(row *spanner.Row) error {
			return row.Columns(&written, &relayed)
		})
	return written, relayed, err
}

// eventKey is an event's key, as a STRUCT parameter.
type eventKey struct {
	R int64 `spanner:"r"`
	S int64 `spanner:"s"`
}

// queries are the backend's operations: in a transaction (tx), a snapshot (ro), or each in a
// transaction of its own.
type queries struct {
	c  *spanner.Client
	tx *txn
	ro *spanner.ReadOnlyTransaction
	// stageBytes are the bytes of content log a transaction writes itself (InsertContentLog).
	stageBytes int
	// packNums are the pack numbers this replica reserved and has not given yet (packNum).
	packNums *numberBlocks
}

// txn is a store's transaction, its statements one at a time.
type txn struct {
	mu sync.Mutex
	tx *spanner.ReadWriteTransaction
	// logBytes are the bytes of content log it wrote itself; batch, the batch it stages the rest
	// under (0: none yet), its marker written for each scope in staged.
	logBytes int
	batch    int64
	staged   map[backend.Scope]bool
	// logID is the last number it gave a content log row of its (logID).
	logID int64
}

// write runs fn in the operation's transaction, alone in it, or in a read-write transaction of its
// own, which may run fn again.
func (x queries) write(ctx context.Context, fn func(tx *spanner.ReadWriteTransaction) error) error {
	if x.tx != nil {
		x.tx.mu.Lock()
		defer x.tx.mu.Unlock()
		return fn(x.tx.tx)
	}
	if x.ro != nil {
		return spannerdata.ErrReadOnly
	}
	_, err := spannertxn.ReadWrite(ctx, x.c, func(_ context.Context, tx *spanner.ReadWriteTransaction) error { return fn(tx) })
	return err
}

// read runs fn over the operation's transaction, alone in it; its snapshot; or a read-only
// transaction of its own: what fn reads, it reads at one moment.
func (x queries) read(fn func(r spannerdata.Reader) error) error {
	switch {
	case x.tx != nil:
		x.tx.mu.Lock()
		defer x.tx.mu.Unlock()
		return fn(x.tx.tx)
	case x.ro != nil:
		return fn(x.ro)
	}
	ro := spannerdata.ReadOnly(x.c)
	defer ro.Close()
	return fn(ro)
}

// unlocked runs fn over the operation's snapshot, or over a read-only transaction of its own,
// though the operation is in a read-write one: for the content index's generation, blocks, and log,
// which every writer appends to. On PostgreSQL those reads lock nothing (read committed); in a
// Spanner read-write transaction a read locks the range it scans, and every writer would conflict
// with every other. What holds a fold back from changing the index meanwhile is the transaction's
// share lock on the index's row (ShareContentIndex), as on PostgreSQL; and no transaction reads
// log rows it wrote, which it writes as it ends.
func (x queries) unlocked(fn func(r spannerdata.Reader) error) error {
	if x.ro != nil {
		return fn(x.ro)
	}
	ro := spannerdata.ReadOnly(x.c)
	defer ro.Close()
	return fn(ro)
}

// rowsPerStatement bounds the rows one statement writes, deletes, or names, and statementBytes
// the bytes of bodies one carries: Spanner refuses a statement of more than 80,000 mutations — each
// column a row writes, each index entry, and each row a cascade deletes — and the widest row the
// SCM writes, a commit's, counts 11; a request it refuses past its size.
const (
	rowsPerStatement = 5000
	statementBytes   = 4 << 20
)

// inChunks calls fn with params holding each run of the items in params' arrays — each the same
// length — of up to rowsPerStatement of them and, where size gives an item's bytes, of about
// statementBytes; an item larger runs alone. It calls fn not at all for none.
func inChunks(params map[string]any, arrays []string, size func(i int) int, fn func(p map[string]any) error) error {
	n := reflect.ValueOf(params[arrays[0]]).Len()
	for lo := 0; lo < n; {
		hi, bytes := lo, 0
		for hi < n && hi-lo < rowsPerStatement {
			if size != nil {
				if hi > lo && bytes+size(hi) > statementBytes {
					break
				}
				bytes += size(hi)
			}
			hi++
		}
		p := maps.Clone(params)
		for _, a := range arrays {
			p[a] = reflect.ValueOf(params[a]).Slice(lo, hi).Interface()
		}
		if err := fn(p); err != nil {
			return err
		}
		lo = hi
	}
	return nil
}

// updateIn runs a DML statement in tx over params' arrays a chunk at a time (inChunks), returning
// the rows it changed.
func updateIn(ctx context.Context, tx *spanner.ReadWriteTransaction, sql string, params map[string]any, arrays []string, size func(int) int) (int64, error) {
	var total int64
	err := inChunks(params, arrays, size, func(p map[string]any) error {
		n, err := spannerdata.Update(ctx, tx, sql, p)
		total += n
		return err
	})
	return total, err
}

// eachIn runs a query in r over params' arrays a chunk at a time, calling scan with each row: for
// a query whose rows need no order or limit across its chunks.
func eachIn(ctx context.Context, r spannerdata.Reader, sql string, params map[string]any, arrays []string, scan func(*spanner.Row) error) error {
	return inChunks(params, arrays, nil, func(p map[string]any) error { return spannerdata.Each(ctx, r, sql, p, scan) })
}

// execIn is exec over params' arrays a chunk at a time, in one transaction.
func (x queries) execIn(ctx context.Context, sql string, params map[string]any, arrays []string, size func(int) int) (int64, error) {
	var n int64
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		n, err = updateIn(ctx, tx, sql, params, arrays, size)
		return err
	})
	return n, err
}

// tableKeys are each table's key columns, typed, for paging through its rows (deleteAll).
var tableKeys = map[string]string{
	"tree_sizes":               "repository_id INT64, tree_id BYTES",
	"copy_parent":              "repository_id INT64, copy_id BYTES, parent_id BYTES",
	"event_pending":            "repository_id INT64, repository_seq INT64",
	"event":                    "repository_id INT64, repository_seq INT64",
	"work":                     "kind STRING, key INT64",
	"candidate":                "repository_id INT64, commit_id BYTES",
	"patchset":                 "repository_id INT64, cl_id INT64, number INT64",
	"changelist":               "repository_id INT64, id INT64",
	"sequence_pending":         "repository_id INT64, committed_at TIMESTAMP, txn INT64, place INT64",
	"head_log":                 "repository_id INT64, seq INT64",
	"commit_path":              "repository_id INT64, path STRING, generation INT64, commit_id BYTES",
	"path_index":               "repository_id INT64, generation INT64",
	"commit_parent":            "repository_id INT64, commit_id BYTES, ord INT64",
	"commit":                   "repository_id INT64, id BYTES",
	"object":                   "repository_id INT64, id BYTES",
	"operation":                "op_log_id INT64, id BYTES",
	"view":                     "op_log_id INT64, id BYTES",
	"op_head":                  "op_log_id INT64, op_id BYTES",
	"op_tombstone":             "op_log_id INT64, kind INT64, id BYTES",
	"op_log":                   "op_log_id INT64",
	"gc_pass":                  "repository_id INT64, started_at TIMESTAMP",
	"checkpoint":               "repository_id INT64, id INT64",
	"workspace":                "repository_id INT64, principal STRING, name STRING",
	"dictionary_grant":         "repository_id INT64, trainer_id INT64",
	"content_log":              "scope INT64, owner_id INT64, id INT64",
	"content_log_batch":        "scope INT64, owner_id INT64, batch_id INT64",
	"content_index":            "scope INT64, owner_id INT64",
	"segment":                  "scope INT64, owner_id INT64, kind INT64, id BYTES",
	"pack":                     "network_id INT64, id BYTES",
	"chunk_release":            "network_id INT64, chunk_id BYTES",
	"dictionary":               "network_id INT64, id INT64",
	"repository_deletion":      "repository_id INT64",
	"counter":                  "name STRING, scope_id INT64",
	"changelist_counter":       "repository_id INT64",
	"compaction_cursor":        "repository_id INT64",
	"sequence_pass":            "repository_id INT64",
	"repository_event_counter": "repository_id INT64",
}

// deleteAll deletes, in tx, table's rows where cond holds — cond naming the table d, as a subquery
// of its must where one of the subquery's tables or columns would shadow the table's name — a page
// of rowsPerStatement at a time, and returns how many it deleted.
func deleteAll(ctx context.Context, tx *spanner.ReadWriteTransaction, table, cond string, params map[string]any) (int64, error) {
	return deleteUpTo(ctx, tx, table, cond, params, -1)
}

// deleteUpTo is deleteAll, deleting at most max rows (negative: every one).
func deleteUpTo(ctx context.Context, tx *spanner.ReadWriteTransaction, table, cond string, params map[string]any, max int64) (int64, error) {
	typed := tableKeys[table]
	var cols []string
	for _, c := range strings.Split(typed, ", ") {
		cols = append(cols, strings.Fields(c)[0])
	}
	key := strings.Join(cols, ", ")
	sql := fmt.Sprintf(`DELETE FROM %[1]s WHERE STRUCT<%[2]s>(%[3]s) IN (SELECT AS STRUCT %[3]s FROM %[1]s AS d WHERE %[4]s LIMIT @page)`,
		table, typed, key, cond)
	p := maps.Clone(params)
	var total int64
	for {
		page := int64(rowsPerStatement)
		if max >= 0 {
			page = min(page, max-total)
		}
		if page == 0 {
			return total, nil
		}
		p["page"] = page
		n, err := spannerdata.Update(ctx, tx, sql, p)
		total += n
		if err != nil || n < page {
			return total, err
		}
	}
}

// deleteTail deletes, in tx, up to limit of the rows of tables where their conditions hold, in
// order, and returns how many.
func deleteTail(ctx context.Context, tx *spanner.ReadWriteTransaction, tables []tableRows, params map[string]any, limit int) (int64, error) {
	left := int64(limit)
	for _, t := range tables {
		if left == 0 {
			break
		}
		n, err := deleteUpTo(ctx, tx, t.table, t.cond, params, left)
		if err != nil {
			return int64(limit) - left, err
		}
		left -= n
	}
	return int64(limit) - left, nil
}

// tableRows are a table's rows where cond holds.
type tableRows struct{ table, cond string }

// rowsPerTransaction bounds the rows a transaction of an operation that runs in several deletes:
// Spanner holds a transaction to about 100,000 rows deleted.
const rowsPerTransaction = 10_000

// deleteInTransactions deletes tables' rows where their conditions hold: in the operation's
// transaction, if it is in one; otherwise in transactions of its own of up to rowsPerTransaction
// rows, until none is left. It returns how many of the last table's rows it deleted.
func (x queries) deleteInTransactions(ctx context.Context, tables []tableRows, params map[string]any) (int64, error) {
	var last int64
	for i, t := range tables {
		for {
			var n int64
			err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
				var err error
				limit := int64(rowsPerTransaction)
				if x.tx != nil {
					limit = -1
				}
				n, err = deleteUpTo(ctx, tx, t.table, t.cond, params, limit)
				return err
			})
			if err != nil {
				return 0, err
			}
			if i == len(tables)-1 {
				last += n
			}
			if x.tx != nil || n < rowsPerTransaction {
				break
			}
		}
	}
	return last, nil
}

// exec runs a DML statement, returning the rows it changed.
func (x queries) exec(ctx context.Context, sql string, params map[string]any) (int64, error) {
	var n int64
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		n, err = spannerdata.Update(ctx, tx, sql, params)
		return err
	})
	return n, err
}

// rows runs a query, calling scan with each row; reset runs first. Given arrays of params, it runs
// it over them a chunk at a time (eachIn).
func (x queries) rows(ctx context.Context, sql string, params map[string]any, reset func(), scan func(*spanner.Row) error, arrays ...string) error {
	return x.read(func(r spannerdata.Reader) error {
		reset()
		if len(arrays) > 0 {
			return eachIn(ctx, r, sql, params, arrays, scan)
		}
		return spannerdata.Each(ctx, r, sql, params, scan)
	})
}

// one runs a query, scanning its first row into dest; false when it has none.
func (x queries) one(ctx context.Context, sql string, params map[string]any, dest ...any) (bool, error) {
	var ok bool
	err := x.read(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, sql, params, dest...)
		return err
	})
	return ok, err
}

// lockOne is one in the operation's transaction, the query a FOR UPDATE one.
func (x queries) lockOne(ctx context.Context, sql string, params map[string]any, dest ...any) (bool, error) {
	var ok bool
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		ok, err = spannerdata.First(ctx, tx, sql, params, dest...)
		return err
	})
	return ok, err
}

// returning runs a DML statement with THEN RETURN, calling scan with each row it returns; reset
// runs first. Given arrays of params, it runs it over them a chunk at a time (inChunks), size
// giving an item's bytes where they are bodies.
func (x queries) returning(ctx context.Context, sql string, params map[string]any, reset func(), scan func(*spanner.Row) error,
	size func(int) int, arrays ...string) error {
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		reset()
		if len(arrays) > 0 {
			return inChunks(params, arrays, size, func(p map[string]any) error { return spannerdata.Each(ctx, tx, sql, p, scan) })
		}
		return spannerdata.Each(ctx, tx, sql, params, scan)
	})
}

// next takes n of a counter's values and returns the last, in a transaction of its own, as
// PostgreSQL's sequences and identity columns hand out values outside the transaction asking:
// so a transaction taking one holds no lock on it until it commits, and a value taken by one that
// does not commit is not taken again.
func (x queries) next(ctx context.Context, name string, scope, n int64) (int64, error) {
	var last int64
	_, err := spannertxn.ReadWrite(ctx, x.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		last = 0
		p := map[string]any{"name": name, "scope": scope}
		ok, err := spannerdata.First(ctx, tx, `SELECT last FROM counter WHERE name = @name AND scope_id = @scope`, p, &last)
		if err != nil {
			return err
		}
		last += n
		p["last"] = last
		sql := `INSERT INTO counter (name, scope_id, last) VALUES (@name, @scope, @last)`
		if ok {
			sql = `UPDATE counter SET last = @last WHERE name = @name AND scope_id = @scope`
		}
		_, err = spannerdata.Update(ctx, tx, sql, p)
		return err
	})
	return last, err
}

// now is the database's time to the microsecond, as PostgreSQL keeps a time: so a row's time
// reads the same from the tail as from a segment, which encodes microseconds.
const now = `TIMESTAMP_MICROS(UNIX_MICROS(CURRENT_TIMESTAMP()))`

// micros is a time a row keeps, to the microsecond, as now is.
func micros(t time.Time) time.Time { return t.Truncate(time.Microsecond) }

// The counters (counter): a network's packs and segments' ages, a content index's log rows (by its
// scope), and the dictionaries.
const (
	packCounter       = "pack"
	ageCounter        = "segment_age"
	dictionaryCounter = "dictionary"
)

// contentLogCounter is the counter of a content index's log rows: a repository's and a network's
// IDs are both random, and may be the same.
func contentLogCounter(scope int64) string { return fmt.Sprintf("content_log/%d", scope) }

// bodySize is the size of bodies' items, for inChunks.
func bodySize(bodies [][]byte) func(int) int { return func(i int) int { return len(bodies[i]) } }

// unique reports a duplicate in a unique index or of a key.
func unique(err error) bool { return spanner.ErrCode(err) == codes.AlreadyExists }

// ids are IDs as BYTES parameters.
func ids(in []encoding.ID) [][]byte {
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = in[i][:]
	}
	return out
}

func toID(b []byte) (encoding.ID, error) {
	if len(b) != encoding.IDLen {
		return encoding.ID{}, fmt.Errorf("stored ID of %d bytes", len(b))
	}
	return encoding.ID(b), nil
}

// distinct are ids, once each, in order.
func distinct(in []encoding.ID) []encoding.ID {
	seen := make(map[encoding.ID]bool, len(in))
	out := make([]encoding.ID, 0, len(in))
	for _, id := range in {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

func sortIDs(s []encoding.ID) {
	slices.SortFunc(s, func(a, b encoding.ID) int { return bytes.Compare(a[:], b[:]) })
}

func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// nullable is a nonzero number, or NULL.
func nullable[T int16 | int32 | int64](v T) spanner.NullInt64 {
	return spanner.NullInt64{Int64: int64(v), Valid: v != 0}
}

func nullString(s string) spanner.NullString { return spanner.NullString{StringVal: s, Valid: s != ""} }

// day is a DATE column's day as the store has it: midnight UTC, as PostgreSQL reads a date.
func day(d civil.Date) time.Time { return d.In(time.UTC) }

// dayBefore is the first day whose midnight (UTC) is not before t: a date is before t as
// PostgreSQL compares a date with a time — at its midnight — exactly when it is before this day.
func dayBefore(t time.Time) civil.Date {
	t = t.UTC()
	d := civil.DateOf(t)
	if !t.Equal(d.In(time.UTC)) {
		d = d.AddDays(1)
	}
	return d
}

// idList reads rows of one ID each, over arrays a chunk at a time (rows).
func (x queries) idList(ctx context.Context, sql string, params map[string]any, arrays ...string) ([]encoding.ID, error) {
	var out []encoding.ID
	err := x.rows(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		id, err := scanID(row)
		if err != nil {
			return err
		}
		out = append(out, id)
		return nil
	}, arrays...)
	return out, err
}

func scanID(row *spanner.Row) (encoding.ID, error) {
	var b []byte
	if err := row.Columns(&b); err != nil {
		return encoding.ID{}, err
	}
	return toID(b)
}

// unlockedIDSet is idSet, read unlocked (unlocked).
func (x queries) unlockedIDSet(ctx context.Context, sql string, params map[string]any, arrays ...string) (map[encoding.ID]bool, error) {
	out := map[encoding.ID]bool{}
	err := x.unlocked(func(r spannerdata.Reader) error {
		clear(out)
		return eachIn(ctx, r, sql, params, arrays, func(row *spanner.Row) error {
			id, err := scanID(row)
			out[id] = true
			return err
		})
	})
	return out, err
}

// idSet reads rows of one ID each, as a set.
func (x queries) idSet(ctx context.Context, sql string, params map[string]any, arrays ...string) (map[encoding.ID]bool, error) {
	list, err := x.idList(ctx, sql, params, arrays...)
	if err != nil {
		return nil, err
	}
	out := make(map[encoding.ID]bool, len(list))
	for _, id := range list {
		out[id] = true
	}
	return out, nil
}

// bodies reads rows of an ID and a body each.
func (x queries) bodies(ctx context.Context, sql string, params map[string]any, arrays ...string) (map[encoding.ID][]byte, error) {
	out := map[encoding.ID][]byte{}
	err := x.rows(ctx, sql, params, func() { clear(out) }, func(row *spanner.Row) error { return scanBody(row, out) }, arrays...)
	return out, err
}

func scanBody(row *spanner.Row, out map[encoding.ID][]byte) error {
	var idb, body []byte
	if err := row.Columns(&idb, &body); err != nil {
		return err
	}
	id, err := toID(idb)
	if err != nil {
		return err
	}
	out[id] = body
	return nil
}

// returningBodies runs a DML statement returning an ID and a body each, over arrays a chunk at a
// time (returning).
func (x queries) returningBodies(ctx context.Context, sql string, params map[string]any, arrays ...string) (map[encoding.ID][]byte, error) {
	out := map[encoding.ID][]byte{}
	err := x.returning(ctx, sql, params, func() { clear(out) }, func(row *spanner.Row) error { return scanBody(row, out) }, nil, arrays...)
	return out, err
}

// returningIDs runs a DML statement returning one ID a row, sorted, over arrays a chunk at a time,
// size giving an item's bytes (returning).
func (x queries) returningIDs(ctx context.Context, sql string, params map[string]any, size func(int) int, arrays ...string) ([]encoding.ID, error) {
	var out []encoding.ID
	err := x.returning(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		id, err := scanID(row)
		if err != nil {
			return err
		}
		out = append(out, id)
		return nil
	}, size, arrays...)
	sortIDs(out)
	return out, err
}

// objects reads rows of an ID and a body each, as objects of kind, in order.
func (x queries) objects(ctx context.Context, sql string, params map[string]any, kind encoding.Kind) ([]backend.Object, error) {
	var out []backend.Object
	err := x.rows(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		var idb, body []byte
		if err := row.Columns(&idb, &body); err != nil {
			return err
		}
		id, err := toID(idb)
		if err != nil {
			return err
		}
		out = append(out, backend.Object{ID: id, Kind: kind, Body: body})
		return nil
	})
	return out, err
}

// kindedObjects reads rows of an ID, a kind, and a body each, in order.
func (x queries) kindedObjects(ctx context.Context, sql string, params map[string]any, arrays ...string) ([]backend.Object, error) {
	var out []backend.Object
	err := x.rows(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		var idb, body []byte
		var kind int64
		if err := row.Columns(&idb, &kind, &body); err != nil {
			return err
		}
		id, err := toID(idb)
		if err != nil {
			return err
		}
		out = append(out, backend.Object{ID: id, Kind: encoding.Kind(kind), Body: body})
		return nil
	}, arrays...)
	return out, err
}

// pairs reads rows of two IDs each.
func (x queries) pairs(ctx context.Context, sql string, params map[string]any, arrays ...string) ([][2]encoding.ID, error) {
	var out [][2]encoding.ID
	err := x.rows(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		var ab, bb []byte
		if err := row.Columns(&ab, &bb); err != nil {
			return err
		}
		a, err := toID(ab)
		if err != nil {
			return err
		}
		b, err := toID(bb)
		if err != nil {
			return err
		}
		out = append(out, [2]encoding.ID{a, b})
		return nil
	}, arrays...)
	return out, err
}

// int64s reads rows of one number each.
func (x queries) int64s(ctx context.Context, sql string, params map[string]any) ([]int64, error) {
	var out []int64
	err := x.rows(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		var n int64
		if err := row.Columns(&n); err != nil {
			return err
		}
		out = append(out, n)
		return nil
	})
	return out, err
}

// count runs a query of one number, as an int64.
func (x queries) count(ctx context.Context, sql string, params map[string]any) (int64, error) {
	var n int64
	_, err := x.one(ctx, sql, params, &n)
	return n, err
}

// errNoRow is a row a read expected that is not there.
var errNoRow = errors.New("spannerstore: no row")
