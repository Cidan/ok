package tunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// maxRequest bounds a request's body; maxBuffered what of an answer waits unread.
const (
	maxRequest  = 16 << 20
	maxBuffered = 64 << 20
)

// Transport sends HTTP requests to an agent's cluster's API server through a link, found anew for
// each request: the deployer's client-go's, over which it streams each answer, watches included.
type Transport struct {
	Link func(ctx context.Context) (Link, error)
}

var _ http.RoundTripper = (*Transport)(nil)

// RoundTrip sends a request, and returns its answer once its status comes; reading its body reads
// the agent's frames as they come, and closing it before its end cancels the request at the agent.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	var body []byte
	if req.Body != nil {
		b, err := io.ReadAll(io.LimitReader(req.Body, maxRequest+1))
		req.Body.Close()
		if err != nil {
			return nil, err
		}
		if len(b) > maxRequest {
			return nil, fmt.Errorf("tunnel: a request's body is over %d bytes", maxRequest)
		}
		body = b
	}
	link, err := t.Link(ctx)
	if err != nil {
		return nil, err
	}
	a := newAnswer(NewID(), link)
	a.stop = link.Watch(a.id, a.push)
	go a.watch(ctx)
	first := &deploypb.KubeRequestFrame{Id: a.id, Method: req.Method, Url: req.URL.RequestURI(), Headers: headersOut(req.Header)}
	for i := 0; ; i += frameBody {
		f := first
		if i > 0 {
			f = &deploypb.KubeRequestFrame{Id: a.id}
		}
		f.Body = body[min(i, len(body)):min(i+frameBody, len(body))]
		f.End = i+frameBody >= len(body)
		if err := link.Send(ctx, &deploypb.AgentDown{Kind: &deploypb.AgentDown_Kube{Kube: f}}); err != nil {
			a.Close()
			return nil, fmt.Errorf("tunnel: sending the request: %w", err)
		}
		if f.End {
			break
		}
	}
	head, err := a.head()
	if err != nil {
		a.Close()
		return nil, err
	}
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", head.GetStatus(), http.StatusText(int(head.GetStatus()))),
		StatusCode:    int(head.GetStatus()),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        headersIn(head.GetHeaders()),
		Body:          a,
		ContentLength: -1,
		Request:       req,
	}, nil
}

// answer is a request's answer as its frames come: its status, then its body.
type answer struct {
	id   int64
	link Link
	stop func()

	mu       sync.Mutex
	cond     *sync.Cond
	frames   []*deploypb.KubeResponseFrame
	buffered int
	cur      []byte
	headDone bool
	ended    bool  // the last frame was read
	err      error // why no more comes: io.EOF at its end
	closed   bool
	done     chan struct{}
	once     sync.Once
}

func newAnswer(id int64, link Link) *answer {
	a := &answer{id: id, link: link, done: make(chan struct{})}
	a.cond = sync.NewCond(&a.mu)
	return a
}

// push takes one of the agent's frames.
func (a *answer) push(up *deploypb.AgentUp) {
	k := up.GetKube()
	if k == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.err != nil {
		return
	}
	a.frames = append(a.frames, k)
	if a.buffered += len(k.GetBody()); a.buffered > maxBuffered {
		a.err = fmt.Errorf("tunnel: over %d bytes of the answer wait unread", maxBuffered)
		go a.cancel()
	}
	a.cond.Broadcast()
}

// watch ends the answer when the request's context or the link ends.
func (a *answer) watch(ctx context.Context) {
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-a.link.Done():
		err = ErrEnded
		if lerr := a.link.Err(); lerr != nil {
			err = fmt.Errorf("%w: %v", ErrEnded, lerr)
		}
	case <-a.done:
		return
	}
	a.mu.Lock()
	if a.err == nil {
		a.err = err
	}
	a.cond.Broadcast()
	a.mu.Unlock()
}

// head waits for the answer's first frame: its status and headers.
func (a *answer) head() (*deploypb.KubeResponseFrame, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for len(a.frames) == 0 && a.err == nil {
		a.cond.Wait()
	}
	if len(a.frames) == 0 {
		return nil, a.err
	}
	f := a.frames[0]
	if f.GetStatus() == 0 {
		msg := f.GetError()
		if msg == "" {
			msg = "the agent answered no status"
		}
		return nil, fmt.Errorf("tunnel: %s", msg)
	}
	a.headDone = true
	a.take()
	return f, nil
}

// take moves the next frame's body into cur, noting its end. Called holding mu.
func (a *answer) take() {
	f := a.frames[0]
	a.frames = a.frames[1:]
	a.buffered -= len(f.GetBody())
	a.cur = f.GetBody()
	if f.GetError() != "" && a.headDone && f.GetStatus() == 0 {
		a.err = fmt.Errorf("tunnel: the answer broke off: %s", f.GetError())
	}
	if f.GetEnd() {
		a.ended = true
	}
}

// Read reads the answer's body.
func (a *answer) Read(p []byte) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for {
		if len(a.cur) > 0 {
			n := copy(p, a.cur)
			a.cur = a.cur[n:]
			return n, nil
		}
		if a.ended {
			return 0, io.EOF
		}
		if len(a.frames) > 0 {
			a.take()
			if a.err != nil && !a.ended {
				return 0, a.err
			}
			continue
		}
		if a.err != nil {
			return 0, a.err
		}
		if a.closed {
			return 0, errors.New("tunnel: read after close")
		}
		a.cond.Wait()
	}
}

// Close stops reading the answer: one not read to its end is canceled at the agent.
func (a *answer) Close() error {
	a.once.Do(func() {
		a.mu.Lock()
		ended := a.ended
		a.closed = true
		a.frames, a.cur = nil, nil
		a.cond.Broadcast()
		a.mu.Unlock()
		close(a.done)
		if a.stop != nil {
			a.stop()
		}
		if !ended {
			a.cancel()
		}
	})
	return nil
}

func (a *answer) cancel() {
	ctx, cancelCtx := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCtx()
	_ = a.link.Send(ctx, cancel(a.id))
}
