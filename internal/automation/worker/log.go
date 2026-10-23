package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/secrets"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// errLeaseLost is a run the worker no longer holds.
var errLeaseLost = errors.New("the run's lease is no longer this worker's")

const (
	// maxPending bounds the log held unsent, while ok automation cannot be reached.
	maxPending = 64 << 20
	// perCall is the most one AppendLog call sends, in requests of perRequest.
	perCall    = 8 << 20
	perRequest = 1 << 20
	// sendEvery is how often what was written is sent, or sooner once a request's worth is
	// pending. Each request the service takes is an object in its logs bucket.
	sendEvery = time.Second
)

// logStream is a run's log as the worker writes it: an io.Writer whose bytes are sent to
// ok automation in the background, each append at its offset, so a send that fails is
// simply sent again from what the service acknowledged.
type logStream struct {
	work   automationpb.WorkClient
	run    int64
	worker string

	mu      sync.Mutex
	pending []byte // written, not yet acknowledged
	acked   int64  // the log's size as the service last said: pending[0]'s offset
	dropped int64  // bytes dropped because pending was full
	full    bool   // the log reached the most a run keeps: nothing more is sent
	lost    bool   // the lease was lost
	wake    chan struct{}

	// hidden are values the log never holds — a post run's secrets, by path (docs/secrets.md,
	// "Readers that are not people") — each written as what names it; held is the raw tail of
	// what was written, kept back until the next write, as it may begin one.
	hidden map[string][]byte
	held   []byte
}

func newLogStream(work automationpb.WorkClient, run int64, worker string) *logStream {
	return &logStream{work: work, run: run, worker: worker, wake: make(chan struct{}, 1)}
}

func (l *logStream) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.full || l.lost {
		return len(p), nil
	}
	if l.hidden != nil {
		n := len(p)
		l.add(l.redact(p))
		return n, nil
	}
	return l.add(p), nil
}

// add appends what is written to what is pending, as much as there is room for.
func (l *logStream) add(p []byte) int {
	room := maxPending - len(l.pending)
	if room < len(p) {
		l.dropped += int64(len(p) - max(room, 0))
		p = p[:max(room, 0)]
	}
	l.pending = append(l.pending, p...)
	if len(l.pending) >= perRequest {
		select {
		case l.wake <- struct{}{}:
		default:
		}
	}
	return len(p)
}

// hide has the log never hold values, from now until reveal, which clears them: they are the log's
// until then.
func (l *logStream) hide(values map[string][]byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.hidden = values
}

// reveal writes what was held back, redacted, and clears and forgets the values.
func (l *logStream) reveal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.hidden == nil {
		return
	}
	if !l.full && !l.lost && len(l.held) > 0 {
		l.add(secrets.Redacted(l.held, l.hidden))
	}
	clear(l.held)
	for _, v := range l.hidden {
		clear(v)
	}
	l.hidden, l.held = nil, nil
}

// redact is what may be logged of what was held back and p, redacted: everything but the
// longest tail that may begin a value, which is held back, raw, for the next write to complete —
// and, where a whole value spans that point, from the value's start.
func (l *logStream) redact(p []byte) []byte {
	buf := append(l.held, p...)
	cut := len(buf)
	for _, v := range l.hidden {
		if len(v) < secrets.MinRedacted {
			continue
		}
		for k := min(len(v)-1, len(buf)); k > 0 && len(buf)-k < cut; k-- {
			if bytes.HasSuffix(buf, v[:k]) {
				cut = len(buf) - k
				break
			}
		}
	}
	for moved := true; moved; {
		moved = false
		for _, v := range l.hidden {
			if len(v) < secrets.MinRedacted {
				continue
			}
			// A whole occurrence starting before cut and ending after it.
			from, to := max(cut-len(v)+1, 0), min(cut+len(v)-1, len(buf))
			if from < to {
				if i := bytes.Index(buf[from:to], v); i >= 0 {
					cut, moved = from+i, true
				}
			}
		}
	}
	out := bytes.Clone(secrets.Redacted(buf[:cut], l.hidden))
	held := bytes.Clone(buf[cut:])
	clear(buf)
	l.held = held
	return out
}

// Printf writes one of the worker's own lines.
func (l *logStream) Printf(format string, args ...any) {
	fmt.Fprintf(l, "ok: "+format+"\n", args...)
}

// sendLoop sends what is written until ctx ends.
func (l *logStream) sendLoop(ctx context.Context) {
	t := time.NewTicker(sendEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-l.wake:
		}
		_ = l.send(ctx)
	}
}

// send sends up to perCall of what is pending, in one AppendLog call.
func (l *logStream) send(ctx context.Context) error {
	l.mu.Lock()
	if l.lost {
		l.mu.Unlock()
		return errLeaseLost
	}
	if l.full || len(l.pending) == 0 {
		l.mu.Unlock()
		return nil
	}
	data, offset := l.pending[:min(len(l.pending), perCall)], l.acked
	l.mu.Unlock()

	size, err := l.append(ctx, offset, data)
	l.mu.Lock()
	defer l.mu.Unlock()
	switch status.Code(err) {
	case codes.OK:
		if advance := min(size-l.acked, int64(len(l.pending))); advance > 0 {
			l.pending, l.acked = l.pending[advance:], size
		}
		return nil
	case codes.ResourceExhausted:
		l.full, l.pending = true, nil
		return nil
	case codes.FailedPrecondition:
		l.lost, l.pending = true, nil
		return errLeaseLost
	}
	return err
}

func (l *logStream) append(ctx context.Context, offset int64, data []byte) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	stream, err := l.work.AppendLog(ctx)
	if err != nil {
		return 0, err
	}
	for i := 0; i < len(data); i += perRequest {
		chunk := data[i:min(i+perRequest, len(data))]
		if err := stream.Send(&automationpb.AppendLogRequest{RunId: l.run, Worker: l.worker, Offset: offset + int64(i), Data: chunk}); err != nil {
			break // CloseAndRecv reports why
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return 0, err
	}
	return resp.GetSize(), nil
}

// flush sends everything written, retrying until ctx ends. It notes bytes dropped while the
// service could not be reached.
func (l *logStream) flush(ctx context.Context) error {
	l.mu.Lock()
	if l.dropped > 0 {
		note := fmt.Sprintf("ok: the worker dropped %d bytes of this log, which it could not send in time\n", l.dropped)
		l.dropped = 0
		l.mu.Unlock()
		l.Write([]byte(note))
	} else {
		l.mu.Unlock()
	}
	for {
		l.mu.Lock()
		before := l.acked
		l.mu.Unlock()
		err := l.send(ctx)
		if errors.Is(err, errLeaseLost) {
			return err
		}
		l.mu.Lock()
		done, moved := l.full || len(l.pending) == 0, l.acked > before
		l.mu.Unlock()
		if done && err == nil {
			return nil
		}
		if err == nil && moved {
			continue // more to send: at once
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("sending the log: %w", errors.Join(ctx.Err(), err))
		case <-time.After(sendEvery):
		}
	}
}
