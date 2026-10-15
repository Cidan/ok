package store

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"

	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// eventHub wakes event watchers when an event commits. One PostgreSQL connection per
// replica LISTENs on eventChannel, which every event-writing transaction notifies at
// commit; each notification closes the current channel and opens the next.
type eventHub struct {
	start sync.Once
	mu    sync.Mutex
	ch    chan struct{}
	// ctx ends the listening connection (Store.Close), and done closes once it has.
	ctx       context.Context
	stop      context.CancelFunc
	listening bool
	done      chan struct{}
}

func (h *eventHub) wait() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ch == nil {
		h.ch = make(chan struct{})
	}
	return h.ch
}

func (h *eventHub) signal() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ch != nil {
		close(h.ch)
	}
	h.ch = make(chan struct{})
}

// startListening starts the listening connection, once.
func (s *Store) startListening() {
	s.events.start.Do(func() {
		s.events.mu.Lock()
		s.events.listening = true
		s.events.mu.Unlock()
		go func() {
			defer close(s.events.done)
			s.listen()
		}()
	})
}

// Close stops the store's listening connection, if it started, and waits for it to end.
func (s *Store) Close() {
	s.events.stop()
	s.events.mu.Lock()
	listening := s.events.listening
	s.events.mu.Unlock()
	if listening {
		<-s.events.done
	}
}

// listen holds the LISTEN connection until the store closes, reconnecting after a
// failure: on eventChannel, for event watchers, and on contentChannel, for the content
// indexes. Every (re)connection signals and marks every index stale, so a watcher re-reads
// anything committed while the connection was down, and an index refreshes.
func (s *Store) listen() {
	ctx := s.events.ctx
	for backoff := time.Second; ctx.Err() == nil; {
		conn, err := pgx.ConnectConfig(ctx, s.pool.Config().ConnConfig.Copy())
		if err == nil {
			_, err = conn.Exec(ctx, `LISTEN `+eventChannel+`; LISTEN `+contentChannel)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("ok scm: event listener", slog.Any("err", err))
			if conn != nil {
				conn.Close(context.WithoutCancel(ctx))
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		s.events.signal()
		s.allIndexesChanged()
		for {
			var n *pgconn.Notification
			if n, err = conn.WaitForNotification(ctx); err != nil {
				break
			}
			if n.Channel == contentChannel {
				if repo, perr := strconv.ParseInt(n.Payload, 10, 64); perr == nil {
					s.indexChanged(repo)
				}
				continue
			}
			s.events.signal()
		}
		conn.Close(context.WithoutCancel(ctx))
		if ctx.Err() != nil {
			return
		}
		slog.Warn("ok scm: event listener lost its connection", slog.Any("err", err))
	}
}

// eventRepoll is how often a caught-up watcher re-reads without a notification, in case
// one was lost with the listening connection.
const eventRepoll = 30 * time.Second

// eventPage is how many events one read returns.
const eventPage = 1000

// WatchEvents streams every event after sequence number after, in sequence order, and
// keeps streaming new ones as they commit until ctx ends or emit fails. Sequence order is
// commit order, so a watcher never skips an event.
func (s *Store) WatchEvents(ctx context.Context, after int64, emit func(*scmpb.Event) error) error {
	s.startListening()
	for {
		wake := s.events.wait()
		n, err := s.readEvents(ctx, &after, emit)
		if err != nil {
			return err
		}
		if n == eventPage {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-time.After(eventRepoll):
		}
	}
}

func (s *Store) readEvents(ctx context.Context, after *int64, emit func(*scmpb.Event) error) (int, error) {
	rows, err := s.pool.Query(ctx, `SELECT seq, body FROM event WHERE seq > $1 ORDER BY seq LIMIT $2`, *after, eventPage)
	if err != nil {
		return 0, err
	}
	var evs []*scmpb.Event
	for rows.Next() {
		var seq int64
		var body []byte
		if err := rows.Scan(&seq, &body); err != nil {
			rows.Close()
			return 0, err
		}
		ev := &scmpb.Event{}
		if err := proto.Unmarshal(body, ev); err != nil {
			rows.Close()
			return 0, err
		}
		evs = append(evs, ev)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, ev := range evs {
		if err := emit(ev); err != nil {
			return 0, err
		}
		*after = ev.Seq
	}
	return len(evs), nil
}
