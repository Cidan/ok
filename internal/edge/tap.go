package edge

import (
	"errors"
	"io"
	"sync"
)

// tap reads a request's body once and hands it to the attempts that forward it, one at a time,
// keeping what it read until an attempt is committed to — so a request a cell refused as another's
// (Moved) is sent again, whole, to the cell that holds its repository — or until the history grows
// past limit, after which it keeps only what the attempt has yet to read and no attempt starts
// again. It reads ahead of the attempt by at most limit, so a slow cell slows the client.
type tap struct {
	src   io.Reader
	limit int

	mu   sync.Mutex
	cond *sync.Cond
	data []byte // what it holds: the stream from offset base on
	base int64
	end  error // why src ended (io.EOF on its end); nil while it runs
	// keep: the history from offset 0 is held, and an attempt may start again.
	keep    bool
	active  *tapReader // the attempt reading now
	stopped bool       // the request is answered: pump reads no more
}

func newTap(src io.Reader, limit int) *tap {
	t := &tap{src: src, limit: limit, keep: true}
	t.cond = sync.NewCond(&t.mu)
	return t
}

// pump reads src until it ends, holding no more than limit unread.
func (t *tap) pump() {
	buf := make([]byte, 32<<10)
	for {
		t.mu.Lock()
		for t.unread() >= int64(t.limit) && t.end == nil && !t.stopped {
			t.cond.Wait()
		}
		stopped := t.stopped
		t.mu.Unlock()
		if stopped {
			return
		}
		n, err := t.src.Read(buf)
		t.mu.Lock()
		t.data = append(t.data, buf[:n]...)
		if t.keep && len(t.data) > t.limit {
			t.keep = false
		}
		t.trim()
		if err != nil {
			t.end = err
		}
		t.cond.Broadcast()
		t.mu.Unlock()
		if err != nil {
			return
		}
	}
}

// unread is how much it holds that the active attempt has yet to read. With t.mu held.
func (t *tap) unread() int64 {
	held := t.base + int64(len(t.data))
	if t.active == nil {
		return held - t.base
	}
	return held - t.active.off
}

// trim drops what the active attempt has read, once nothing more is kept for another. With t.mu
// held.
func (t *tap) trim() {
	if t.keep || t.active == nil {
		return
	}
	if drop := t.active.off - t.base; drop > 0 {
		t.data = append(t.data[:0:0], t.data[drop:]...)
		t.base = t.active.off
	}
}

// reader is a new attempt's body, from the stream's start; false once the start is no longer held.
func (t *tap) reader() (*tapReader, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.keep || t.base != 0 {
		return nil, false
	}
	if t.active != nil {
		t.active.closed = true
	}
	t.active = &tapReader{t: t}
	t.cond.Broadcast()
	return t.active, true
}

// replayable reports whether another attempt may start: the stream's start is still held.
func (t *tap) replayable() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.keep && t.base == 0
}

// stop ends pump's reading: the request is answered.
func (t *tap) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
	t.cond.Broadcast()
}

// commit is the attempt whose answer goes to the client: nothing more is kept for another.
func (t *tap) commit() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.keep = false
	t.trim()
	t.cond.Broadcast()
}

// tapReader is one attempt's body.
type tapReader struct {
	t      *tap
	off    int64
	closed bool
}

var errAbandoned = errors.New("edge: the attempt was abandoned for another")

func (r *tapReader) Read(p []byte) (int, error) {
	t := r.t
	t.mu.Lock()
	defer t.mu.Unlock()
	for !r.closed && r.off >= t.base+int64(len(t.data)) && t.end == nil {
		t.cond.Wait()
	}
	if r.closed {
		return 0, errAbandoned
	}
	if i := r.off - t.base; i < int64(len(t.data)) {
		n := copy(p, t.data[i:])
		r.off += int64(n)
		t.trim()
		t.cond.Broadcast()
		return n, nil
	}
	return 0, t.end
}

// Close abandons the attempt's body: the transport stops reading it.
func (r *tapReader) Close() error {
	r.t.mu.Lock()
	defer r.t.mu.Unlock()
	r.closed = true
	r.t.cond.Broadcast()
	return nil
}
