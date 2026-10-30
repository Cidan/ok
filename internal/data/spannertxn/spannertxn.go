// Package spannertxn begins ok's read-write transactions on Spanner, measured (docs/hosted.md,
// "Proof at scale, as built"): each one's time, its attempts — more than one is Spanner aborting
// it and running it again — and the statements it ran through spannerdata's helpers, by the code
// that began it; one past SlowTxn is logged.
package spannertxn

import (
	"context"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/work"
)

var (
	txnSeconds = metrics.NewHistogram("ok_spanner_txn_seconds",
		"Read-write transactions' time, from the first attempt's beginning to the commit, by where they began.", metrics.Seconds, "site")
	txnAttempts = metrics.NewCounter("ok_spanner_txn_attempts_total",
		"Read-write transactions' attempts, by where they began: past one a transaction, Spanner aborted and ran them again.", "site")
	txnStatements = metrics.NewCounter("ok_spanner_txn_statements_total",
		"Statements read-write transactions ran through spannerdata's helpers, every attempt's, by where they began.", "site")
	txnsDone = metrics.NewCounter("ok_spanner_txns_total", "Read-write transactions, by where they began and whether they committed.",
		"site", "outcome")
)

// SlowTxn is how long a transaction runs before it is logged.
const SlowTxn = 500 * time.Millisecond

// counting are the statement counts of the attempts under way, by their transactions.
var counting sync.Map // *spanner.ReadWriteTransaction → *atomic.Int64

// Counted counts a statement r runs, when r is an attempt under way.
func Counted(r any) {
	if tx, ok := r.(*spanner.ReadWriteTransaction); ok {
		if n, ok := counting.Load(tx); ok {
			n.(*atomic.Int64).Add(1)
		}
	}
}

// ReadWrite runs fn in a read-write transaction on c — again, each time Spanner aborts it —
// measuring it, and enqueueing the jobs of the work each attempt marks before it commits
// (work.Flush): every read-write transaction of ok's begins here.
func ReadWrite(ctx context.Context, c *spanner.Client, fn func(context.Context, *spanner.ReadWriteTransaction) error) (time.Time, error) {
	site := callSite()
	began := time.Now()
	var attempts, statements int64
	at, err := c.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		attempts++
		n := new(atomic.Int64)
		counting.Store(tx, n)
		defer func() {
			statements += n.Load()
			counting.Delete(tx)
		}()
		work.Begin(c, tx)
		defer work.End(tx)
		if err := fn(ctx, tx); err != nil {
			return err
		}
		return work.Flush(ctx, tx)
	})
	record(site, time.Since(began), attempts, statements, err)
	return at, err
}

// Begun measures a transaction its caller begins and commits itself, statement by statement: done
// records it, as one attempt — a caller that runs it again after Spanner aborts it begins another.
func Begun(tx *spanner.ReadWriteTransaction) (done func(err error)) {
	site := callSite()
	began := time.Now()
	n := new(atomic.Int64)
	counting.Store(tx, n)
	return func(err error) {
		counting.Delete(tx)
		record(site, time.Since(began), 1, n.Load(), err)
	}
}

func record(site string, took time.Duration, attempts, statements int64, err error) {
	txnSeconds.With(site).Observe(took.Seconds())
	txnAttempts.With(site).Add(uint64(attempts))
	txnStatements.With(site).Add(uint64(statements))
	outcome := "committed"
	switch {
	case spanner.ErrCode(err) == codes.Aborted:
		outcome = "aborted"
	case err != nil:
		outcome = "failed"
	}
	txnsDone.With(site, outcome).Inc()
	if took > SlowTxn {
		slog.Info("spanner: a slow transaction", "site", site, "seconds", took.Round(time.Millisecond).Seconds(), "attempts", attempts,
			"statements", statements, "err", err)
	}
}

// wrappers are the functions that begin transactions for others: a transaction's site is the first
// function past them.
var wrappers = []string{".Txn", ".txn", ".inTx", ".Write", ".Exec", ".Returning", ".ReadWrite", ".DeleteRows", ".RunTx", ".write", ".Retry",
	".Begun"}

// callSite names the code that began a transaction: the first caller outside this package and the
// wrappers, as a short function name.
func callSite() string {
	var pcs [16]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		name := f.Function
		inner := strings.Contains(name, "/internal/data/spannerdata.") || strings.Contains(name, "/internal/data/spannertxn.")
		for _, w := range wrappers {
			if strings.HasSuffix(name, w) || strings.Contains(name, w+".func") {
				inner = true
			}
		}
		if !inner && name != "" {
			return strings.TrimPrefix(name, "github.com/Cidan/ok/internal/")
		}
		if !more {
			return strings.TrimPrefix(name, "github.com/Cidan/ok/internal/")
		}
	}
}
