package testdb

import (
	"bytes"
	"context"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/pgdb"
)

// NoNestedConns makes a pool fail its test when a goroutine that holds one of its connections —
// in a transaction, or reading a query's rows — acquires another of its connections. With every
// connection held so, each waiting for another, the pool deadlocks: under load, any such code can.
// A pool opened from this one's configuration (pgdb.Beside) is watched too, on its own: a
// goroutine holding one pool's connection may wait for another pool's.
func NoNestedConns(t testing.TB, cfg *pgxpool.Config) {
	cfg.ConnConfig.Tracer = &nesting{t: t, held: map[*pgx.Conn]holder{}, by: map[holder]int{}, seen: map[string]bool{}}
}

// holder is a goroutine holding connections of one pool.
type holder struct {
	pool *pgxpool.Pool
	g    uint64
}

type nesting struct {
	t    testing.TB
	mu   sync.Mutex
	held map[*pgx.Conn]holder // each held connection's pool and goroutine
	by   map[holder]int       // how many of a pool's connections each goroutine holds
	seen map[string]bool      // the stacks reported
}

func (n *nesting) TraceAcquireStart(ctx context.Context, pool *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	h := holder{pool, goroutine()}
	n.mu.Lock()
	holding := n.by[h]
	n.mu.Unlock()
	if holding == 0 {
		return ctx
	}
	stack := make([]byte, 64<<10)
	stack = stack[:runtime.Stack(stack, false)]
	n.mu.Lock()
	first := !n.seen[string(stack)]
	n.seen[string(stack)] = true
	n.mu.Unlock()
	if first {
		n.t.Errorf("testdb: a goroutine holding %d of a pool's connections acquires another:\n%s", holding, stack)
	}
	return ctx
}

func (n *nesting) TraceAcquireEnd(_ context.Context, pool *pgxpool.Pool, d pgxpool.TraceAcquireEndData) {
	if d.Err != nil {
		return
	}
	h := holder{pool, goroutine()}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.held[d.Conn] = h
	n.by[h]++
}

func (n *nesting) TraceRelease(_ *pgxpool.Pool, d pgxpool.TraceReleaseData) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if h, ok := n.held[d.Conn]; ok {
		delete(n.held, d.Conn)
		if n.by[h]--; n.by[h] == 0 {
			delete(n.by, h)
		}
	}
}

func (*nesting) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (*nesting) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// goroutine is the calling goroutine's ID, from its stack's first line.
func goroutine() uint64 {
	var b [64]byte
	line := b[:runtime.Stack(b[:], false)]
	line = bytes.TrimPrefix(line, []byte("goroutine "))
	id, _ := strconv.ParseUint(string(line[:bytes.IndexByte(line, ' ')]), 10, 64)
	return id
}

// Pool is a pool over a new database for this test alone (Postgres), closed when the test ends,
// that fails the test when a goroutine holding one of its connections asks for another.
func Pool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	NoNestedConns(t, cfg)
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

// SchemaPool is a pool of up to maxConns connections (0: the driver's default) over the database
// url names, its tables in schema (pgdb), closed when the test ends, that fails the test as Pool's
// does.
func SchemaPool(t testing.TB, url, schema string, maxConns int32) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	NoNestedConns(t, cfg)
	p, err := pgdb.OpenConfig(context.Background(), cfg, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}
