// Package pgbus is the bus (internal/bus) over PostgreSQL: a Stream and a Queue in the
// installation's database, in a schema of their own (bus), waking waiters with LISTEN/NOTIFY and
// polling besides, so a lost notification delays a waiter and never strands it.
package pgbus

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/migrate"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations is the bus schema's.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Schema is the PostgreSQL schema the bus keeps its tables in.
const Schema = "bus"

const (
	chanStream = "ok_bus_stream"
	chanQueue  = "ok_bus_queue"
	// parts is how many partitions each group's work is split into, shared among its members.
	parts = 16
)

// Options tunes the bus.
type Options struct {
	// MaxAttempts is how many leases a job gets before it is a dead letter. Default 10.
	MaxAttempts int
	// Poll bounds every wait, for a notification that was lost. Default one second.
	Poll time.Duration
	// Lease is how long a member holds a partition without renewing it. Default ten seconds.
	Lease time.Duration
	// Retention is how long an event is kept once every group has passed it, or at most, for a
	// topic no group reads. Defaults one hour and seven days.
	Retention    time.Duration
	MaxRetention time.Duration
}

func (o *Options) defaults() {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 10
	}
	if o.Poll <= 0 {
		o.Poll = time.Second
	}
	if o.Lease <= 0 {
		o.Lease = 10 * time.Second
	}
	if o.Retention <= 0 {
		o.Retention = time.Hour
	}
	if o.MaxRetention <= 0 {
		o.MaxRetention = 7 * 24 * time.Hour
	}
}

// Bus is a bus.Stream and a bus.Queue over one database.
type Bus struct {
	pool   *pgxpool.Pool
	opts   Options
	notify *notifier
	host   string
	n      atomic.Int64
	stop   context.CancelFunc
	done   chan struct{}
}

var (
	_ bus.Stream  = (*Bus)(nil)
	_ bus.Queue   = (*Bus)(nil)
	_ bus.Signals = (*Bus)(nil)
)

// Open brings the database pool reaches (its search path the bus schema, internal/pgdb) to the
// bus's schema, and listens for its notifications until Close.
func Open(ctx context.Context, pool *pgxpool.Pool, opts Options) (*Bus, error) {
	opts.defaults()
	if _, err := migrate.Apply(ctx, pool, Schema, Migrations); err != nil {
		return nil, fmt.Errorf("pgbus: %w", err)
	}
	host, _ := os.Hostname()
	lctx, stop := context.WithCancel(context.Background())
	b := &Bus{pool: pool, opts: opts, notify: newNotifier(), host: fmt.Sprintf("%s-%d-%x", host, os.Getpid(), randInt()&0xffffff), stop: stop, done: make(chan struct{})}
	go func() {
		defer close(b.done)
		b.listen(lctx)
	}()
	return b, nil
}

// Close stops listening.
func (b *Bus) Close() {
	b.stop()
	<-b.done
}

func randInt() int64 {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	return int64(binary.BigEndian.Uint64(buf[:]) >> 1)
}

// part is key's partition.
func part(key int64) int16 { return int16(((key % parts) + parts) % parts) }

// Publish numbers events under the counter's lock and stores them, waking each topic's members.
func (b *Bus) Publish(ctx context.Context, events ...bus.Event) error {
	if len(events) == 0 {
		return nil
	}
	return pgdata.Txn(ctx, b.pool, func(tx pgx.Tx) error {
		var last int64
		if err := tx.QueryRow(ctx, `UPDATE stream_counter SET last = last + $1 RETURNING last`, len(events)).Scan(&last); err != nil {
			return err
		}
		first := last - int64(len(events)) + 1
		topics, ps, ids, keys, seqs, bodies := make([]string, len(events)), make([]int16, len(events)),
			make([]int64, len(events)), make([]int64, len(events)), make([]int64, len(events)), make([][]byte, len(events))
		distinct := map[string]bool{}
		for i, e := range events {
			if e.Topic == "" {
				return errors.New("pgbus: an event with no topic")
			}
			body := e.Body
			if body == nil {
				body = []byte{}
			}
			topics[i], ps[i], ids[i], keys[i], seqs[i], bodies[i] = e.Topic, part(e.Key), first+int64(i), e.Key, e.Seq, body
			distinct[e.Topic] = true
		}
		if _, err := tx.Exec(ctx, `INSERT INTO stream_event (topic, part, id, key, seq, body)
			SELECT * FROM unnest($1::text[], $2::smallint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::bytea[])`,
			topics, ps, ids, keys, seqs, bodies); err != nil {
			return err
		}
		for t := range distinct {
			if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, chanStream, t); err != nil {
				return err
			}
		}
		return nil
	})
}

// CreateGroup makes group, its partitions starting after every event committed so far: the
// counter's lock waits out publishes in flight, whose events are before the group.
func (b *Bus) CreateGroup(ctx context.Context, topic, group string) error {
	return pgdata.Txn(ctx, b.pool, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM stream_group WHERE topic = $1 AND name = $2)`, topic, group).Scan(&exists); err != nil || exists {
			return err
		}
		var last int64
		if err := tx.QueryRow(ctx, `SELECT last FROM stream_counter FOR UPDATE`).Scan(&last); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `INSERT INTO stream_group (topic, name) VALUES ($1, $2) ON CONFLICT DO NOTHING`, topic, group)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO stream_part (topic, name, part, position)
			SELECT $1, $2, p, $3 FROM generate_series(0, $4 - 1) AS p`, topic, group, last, parts)
		return err
	})
}

// Subscribe delivers topic's events to h as one member of group: it renews its share of the
// group's partitions, delivers each partition's events in order, and redelivers what h failed.
func (b *Bus) Subscribe(ctx context.Context, topic, group string, h bus.Handler) error {
	if err := b.CreateGroup(ctx, topic, group); err != nil {
		return fmt.Errorf("pgbus: %w", err)
	}
	m := &member{b: b, topic: topic, group: group, id: fmt.Sprintf("%s-%d", b.host, b.n.Add(1)), h: h}
	defer m.leave()
	lastClean := time.Time{}
	for {
		woken := b.notify.watch(chanStream, topic)
		held, err := m.renew(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("pgbus: %w", err)
		}
		busy := false
		for _, p := range held {
			n, err := m.deliver(ctx, p)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("pgbus: %w", err)
			}
			busy = busy || n > 0
		}
		n, err := m.redeliver(ctx, held)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("pgbus: %w", err)
		}
		busy = busy || n > 0
		if time.Since(lastClean) > time.Minute {
			lastClean = time.Now()
			if err := b.clean(ctx, topic); err != nil && ctx.Err() == nil {
				slog.Warn("pgbus: trimming the stream", "topic", topic, "err", err)
			}
		}
		if busy {
			continue
		}
		if err := b.notify.wait(ctx, woken, b.opts.Poll); err != nil {
			return err
		}
	}
}

type member struct {
	b            *Bus
	topic, group string
	id           string
	h            bus.Handler
}

type heldPart struct {
	part     int16
	position int64
}

// renew records the member as live, keeps the partitions it holds, gives up those past its share
// of the live members', and takes free ones up to it.
func (m *member) renew(ctx context.Context) ([]heldPart, error) {
	b := m.b
	var held []heldPart
	err := pgdata.Txn(ctx, b.pool, func(tx pgx.Tx) error {
		held = held[:0]
		lease := b.opts.Lease.Seconds()
		if _, err := tx.Exec(ctx, `INSERT INTO stream_member (topic, name, member, seen_at) VALUES ($1, $2, $3, now())
			ON CONFLICT (topic, name, member) DO UPDATE SET seen_at = now()`, m.topic, m.group, m.id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM stream_member WHERE topic = $1 AND name = $2 AND seen_at < now() - make_interval(secs => $3)`,
			m.topic, m.group, 3*lease); err != nil {
			return err
		}
		var members int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM stream_member WHERE topic = $1 AND name = $2`, m.topic, m.group).Scan(&members); err != nil {
			return err
		}
		share := (parts + members - 1) / max(members, 1)
		rows, err := tx.Query(ctx, `UPDATE stream_part SET lease_until = now() + make_interval(secs => $4)
			WHERE topic = $1 AND name = $2 AND holder = $3 RETURNING part, position`, m.topic, m.group, m.id, lease)
		if err != nil {
			return err
		}
		mine, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (heldPart, error) {
			var p heldPart
			return p, r.Scan(&p.part, &p.position)
		})
		if err != nil {
			return err
		}
		if len(mine) > share {
			var give []int16
			for _, p := range mine[share:] {
				give = append(give, p.part)
			}
			if _, err := tx.Exec(ctx, `UPDATE stream_part SET holder = NULL, lease_until = NULL
				WHERE topic = $1 AND name = $2 AND holder = $3 AND part = ANY($4)`, m.topic, m.group, m.id, give); err != nil {
				return err
			}
			mine = mine[:share]
		}
		if len(mine) < share {
			rows, err := tx.Query(ctx, `UPDATE stream_part SET holder = $3, lease_until = now() + make_interval(secs => $4)
				WHERE (topic, name, part) IN (
					SELECT topic, name, part FROM stream_part
					WHERE topic = $1 AND name = $2 AND (holder IS NULL OR lease_until < now())
					ORDER BY part LIMIT $5 FOR UPDATE SKIP LOCKED)
				RETURNING part, position`, m.topic, m.group, m.id, lease, share-len(mine))
			if err != nil {
				return err
			}
			more, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (heldPart, error) {
				var p heldPart
				return p, r.Scan(&p.part, &p.position)
			})
			if err != nil {
				return err
			}
			mine = append(mine, more...)
		}
		held = mine
		return nil
	})
	return held, err
}

// batch is how many events a partition delivers between moves of its position.
const batch = 100

// deliver hands h the partition's events after its position, a batch at a time, recording a
// failure for redelivery, then moves the position past them — if the member still holds it.
func (m *member) deliver(ctx context.Context, p heldPart) (int, error) {
	b := m.b
	rows, err := b.pool.Query(ctx, `SELECT id, key, seq, body FROM stream_event
		WHERE topic = $1 AND part = $2 AND id > $3 ORDER BY id LIMIT $4`, m.topic, p.part, p.position, batch)
	if err != nil {
		return 0, err
	}
	type ev struct {
		id int64
		e  bus.Event
	}
	evs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (ev, error) {
		x := ev{e: bus.Event{Topic: m.topic}}
		return x, r.Scan(&x.id, &x.e.Key, &x.e.Seq, &x.e.Body)
	})
	if err != nil || len(evs) == 0 {
		return 0, err
	}
	for _, x := range evs {
		if herr := m.h(ctx, x.e); herr != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			if _, err := b.pool.Exec(ctx, `INSERT INTO stream_retry (topic, name, id, part, key, seq, body, attempts, not_before)
				VALUES ($1, $2, $3, $4, $5, $6, $7, 1, now() + make_interval(secs => $8)) ON CONFLICT DO NOTHING`,
				m.topic, m.group, x.id, p.part, x.e.Key, x.e.Seq, x.e.Body, backoff(1).Seconds()); err != nil {
				return 0, err
			}
		}
	}
	tag, err := b.pool.Exec(ctx, `UPDATE stream_part SET position = $5
		WHERE topic = $1 AND name = $2 AND part = $3 AND holder = $4 AND position < $5`,
		m.topic, m.group, p.part, m.id, evs[len(evs)-1].id)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() == 0 {
		return 0, nil // the partition moved to another member: it redelivers from its position
	}
	return len(evs), nil
}

// backoff is the pause before an event's attempt-th redelivery: a second, doubling, to five
// minutes.
func backoff(attempt int) time.Duration {
	d := time.Second << min(attempt-1, 9)
	return min(d, 5*time.Minute)
}

// redeliver hands h the failed events now due in the partitions the member holds.
func (m *member) redeliver(ctx context.Context, held []heldPart) (int, error) {
	if len(held) == 0 {
		return 0, nil
	}
	b := m.b
	ps := make([]int16, len(held))
	for i, p := range held {
		ps[i] = p.part
	}
	rows, err := b.pool.Query(ctx, `SELECT id, key, seq, body, attempts FROM stream_retry
		WHERE topic = $1 AND name = $2 AND part = ANY($3) AND not_before <= now() ORDER BY not_before LIMIT $4`,
		m.topic, m.group, ps, batch)
	if err != nil {
		return 0, err
	}
	type ev struct {
		id       int64
		attempts int
		e        bus.Event
	}
	evs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (ev, error) {
		x := ev{e: bus.Event{Topic: m.topic}}
		return x, r.Scan(&x.id, &x.e.Key, &x.e.Seq, &x.e.Body, &x.attempts)
	})
	if err != nil {
		return 0, err
	}
	for _, x := range evs {
		if herr := m.h(ctx, x.e); herr != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			if _, err := b.pool.Exec(ctx, `UPDATE stream_retry SET attempts = attempts + 1, not_before = now() + make_interval(secs => $4)
				WHERE topic = $1 AND name = $2 AND id = $3`, m.topic, m.group, x.id, backoff(x.attempts+1).Seconds()); err != nil {
				return 0, err
			}
			continue
		}
		if _, err := b.pool.Exec(ctx, `DELETE FROM stream_retry WHERE topic = $1 AND name = $2 AND id = $3`, m.topic, m.group, x.id); err != nil {
			return 0, err
		}
	}
	return len(evs), nil
}

// leave gives up the member's partitions and its place, so the others take them at once.
func (m *member) leave() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = m.b.pool.Exec(ctx, `UPDATE stream_part SET holder = NULL, lease_until = NULL WHERE topic = $1 AND name = $2 AND holder = $3`,
		m.topic, m.group, m.id)
	_, _ = m.b.pool.Exec(ctx, `DELETE FROM stream_member WHERE topic = $1 AND name = $2 AND member = $3`, m.topic, m.group, m.id)
}

// clean drops topic's events every group has passed, once Retention has passed too, and any
// older than MaxRetention.
func (b *Bus) clean(ctx context.Context, topic string) error {
	_, err := b.pool.Exec(ctx, `DELETE FROM stream_event e WHERE e.topic = $1 AND (
			e.at < now() - make_interval(secs => $3)
			OR (e.at < now() - make_interval(secs => $2)
				AND e.id <= (SELECT coalesce(min(p.position), 0) FROM stream_part p WHERE p.topic = $1 AND p.part = e.part)))`,
		topic, b.opts.Retention.Seconds(), b.opts.MaxRetention.Seconds())
	return err
}

// Enqueue adds jobs, dropping one whose queue holds a job with its ID.
func (b *Bus) Enqueue(ctx context.Context, jobs ...bus.Job) error {
	if len(jobs) == 0 {
		return nil
	}
	queues, ids, keys, bodies, notBefore := make([]string, len(jobs)), make([]string, len(jobs)),
		make([]int64, len(jobs)), make([][]byte, len(jobs)), make([]time.Time, len(jobs))
	distinct := map[string]bool{}
	now := time.Now()
	for i, j := range jobs {
		if j.Queue == "" || j.ID == "" {
			return errors.New("pgbus: a job needs a queue and an ID")
		}
		body := j.Body
		if body == nil {
			body = []byte{}
		}
		nb := j.NotBefore
		if nb.IsZero() {
			nb = now
		}
		queues[i], ids[i], keys[i], bodies[i], notBefore[i] = j.Queue, j.ID, j.Key, body, nb
		distinct[j.Queue] = true
	}
	return pgdata.Txn(ctx, b.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO queue_job (queue, id, key, body, not_before)
			SELECT * FROM unnest($1::text[], $2::text[], $3::bigint[], $4::bytea[], $5::timestamptz[])
			ON CONFLICT (queue, id) DO NOTHING`, queues, ids, keys, bodies, notBefore); err != nil {
			return err
		}
		for q := range distinct {
			if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, chanQueue, q); err != nil {
				return err
			}
		}
		return nil
	})
}

// Lease takes ready jobs of queue, waiting for one up to o.Wait; jobs that have had every attempt
// become dead letters instead.
func (b *Bus) Lease(ctx context.Context, queue string, o bus.LeaseOptions) ([]bus.Leased, error) {
	if o.Max < 1 {
		o.Max = 1
	}
	if o.Visibility <= 0 {
		return nil, errors.New("pgbus: a lease needs a visibility")
	}
	deadline := time.Now().Add(o.Wait)
	for {
		woken := b.notify.watch(chanQueue, queue)
		ls, err := b.lease(ctx, queue, o)
		if err != nil || len(ls) > 0 {
			return ls, err
		}
		left := time.Until(deadline)
		if left <= 0 {
			return nil, nil
		}
		if err := b.notify.wait(ctx, woken, min(left, b.opts.Poll)); err != nil {
			return nil, err
		}
	}
}

func (b *Bus) lease(ctx context.Context, queue string, o bus.LeaseOptions) ([]bus.Leased, error) {
	token := randInt()
	var out []bus.Leased
	err := pgdata.Txn(ctx, b.pool, func(tx pgx.Tx) error {
		out = out[:0]
		if _, err := tx.Exec(ctx, `WITH dead AS (
				DELETE FROM queue_job WHERE (queue, id) IN (
					SELECT queue, id FROM queue_job WHERE queue = $1 AND not_before <= now() AND attempts >= $2
					FOR UPDATE SKIP LOCKED)
				RETURNING queue, id, key, body, attempts)
			INSERT INTO queue_dead (queue, id, key, body, attempts) SELECT * FROM dead`, queue, b.opts.MaxAttempts); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `UPDATE queue_job j SET attempts = j.attempts + 1, token = $3,
				not_before = now() + make_interval(secs => $4)
			FROM (SELECT queue, id FROM queue_job WHERE queue = $1 AND not_before <= now() AND attempts < $5
				ORDER BY not_before LIMIT $2 FOR UPDATE SKIP LOCKED) picked
			WHERE j.queue = picked.queue AND j.id = picked.id
			RETURNING j.id, j.key, j.body, j.attempts`, queue, o.Max, token, o.Visibility.Seconds(), b.opts.MaxAttempts)
		if err != nil {
			return err
		}
		ls, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (bus.Leased, error) {
			l := &leased{b: b, token: token, job: bus.Job{Queue: queue}}
			return l, r.Scan(&l.job.ID, &l.job.Key, &l.job.Body, &l.attempt)
		})
		out = ls
		return err
	})
	return out, err
}

type leased struct {
	b       *Bus
	job     bus.Job
	token   int64
	attempt int
}

func (l *leased) Job() bus.Job { return l.job }
func (l *leased) Attempt() int { return l.attempt }

func (l *leased) Extend(ctx context.Context, d time.Duration) error {
	return l.exec(ctx, `UPDATE queue_job SET not_before = now() + make_interval(secs => $4)
		WHERE queue = $1 AND id = $2 AND token = $3`, d.Seconds())
}

func (l *leased) Ack(ctx context.Context) error {
	return l.exec(ctx, `DELETE FROM queue_job WHERE queue = $1 AND id = $2 AND token = $3`)
}

func (l *leased) Nack(ctx context.Context, delay time.Duration) error {
	if err := l.exec(ctx, `UPDATE queue_job SET not_before = now() + make_interval(secs => $4), token = NULL
		WHERE queue = $1 AND id = $2 AND token = $3`, delay.Seconds()); err != nil {
		return err
	}
	_, err := l.b.pool.Exec(ctx, `SELECT pg_notify($1, $2)`, chanQueue, l.job.Queue)
	return err
}

// exec runs a statement on the lease's job, whose first three arguments are its queue, ID, and
// token: no row means the lease ended.
func (l *leased) exec(ctx context.Context, sql string, args ...any) error {
	tag, err := l.b.pool.Exec(ctx, sql, append([]any{l.job.Queue, l.job.ID, l.token}, args...)...)
	if err != nil {
		return fmt.Errorf("pgbus: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return bus.ErrLeaseLost
	}
	return nil
}

// listen delivers the database's notifications to the bus's waiters until ctx ends, reconnecting
// after a failure.
func (b *Bus) listen(ctx context.Context) {
	for ctx.Err() == nil {
		err := b.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		slog.Warn("pgbus: listening for notifications", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (b *Bus) listenOnce(ctx context.Context) error {
	return pgdata.Listen(ctx, b.pool, []string{chanStream, chanQueue}, b.notify.fireAll, b.notify.fire)
}

// notifier wakes waiters on notifications, by channel and payload.
type notifier struct {
	mu      sync.Mutex
	waiters map[string]chan struct{}
}

func newNotifier() *notifier { return &notifier{waiters: map[string]chan struct{}{}} }

func (n *notifier) watch(channel, payload string) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	k := channel + "\x00" + payload
	ch, ok := n.waiters[k]
	if !ok {
		ch = make(chan struct{})
		n.waiters[k] = ch
	}
	return ch
}

func (n *notifier) fire(channel, payload string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	k := channel + "\x00" + payload
	if ch, ok := n.waiters[k]; ok {
		close(ch)
		delete(n.waiters, k)
	}
}

func (n *notifier) fireAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, ch := range n.waiters {
		close(ch)
		delete(n.waiters, k)
	}
}

// wait returns when ch is closed, max passes, or ctx ends (its error).
func (n *notifier) wait(ctx context.Context, ch <-chan struct{}, max time.Duration) error {
	t := time.NewTimer(max)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	case <-ch:
	}
	return nil
}
