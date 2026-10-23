package tunnel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/work"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// The lease on an agent's session, and how often its holder renews it.
const (
	DefaultLease = 30 * time.Second
	DefaultRenew = 10 * time.Second
)

// HubConfig is what a Hub works with.
type HubConfig struct {
	Store *store.Store
	// Address is where the other replicas reach this one's AgentRelay; Cell the cell it serves.
	Address string
	Cell    string
	// Account is the organization a robot is the deploy agent of; an error of codes.NotFound for none.
	Account func(ctx context.Context, robot string) (string, error)
	// Dial reaches another replica's AgentRelay, as the deployer.
	Dial func(ctx context.Context, addr string) (deploypb.AgentRelayClient, error)
	// Lease and Renew are the session's: DefaultLease and DefaultRenew unless set.
	Lease, Renew time.Duration
	Log          *slog.Logger
}

// Hub holds the agents' connections this replica took (Agents.Connect), serves the other replicas'
// requests to them (AgentRelay.Exchange), and relays this replica's requests to those another holds.
type Hub struct {
	deploypb.UnimplementedAgentsServer
	deploypb.UnimplementedAgentRelayServer

	cfg    HubConfig
	holder string
	base   context.Context
	stop   context.CancelFunc

	mu     sync.Mutex
	local  map[string]*session
	relays map[string]*relayLink
}

// NewHub makes a hub.
func NewHub(cfg HubConfig) *Hub {
	if cfg.Lease <= 0 {
		cfg.Lease = DefaultLease
	}
	if cfg.Renew <= 0 {
		cfg.Renew = DefaultRenew
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	base, stop := context.WithCancel(context.Background())
	return &Hub{cfg: cfg, holder: work.NewHolder(), base: base, stop: stop, local: map[string]*session{}, relays: map[string]*relayLink{}}
}

// Close ends the hub's relays; its sessions end with their calls.
func (h *Hub) Close() { h.stop() }

// Connected reports whether an account's agent is connected to this replica.
func (h *Hub) Connected(account string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.local[account] != nil
}

// Link is the link to an account's agent: its session here, or a relay to the replica holding it.
func (h *Hub) Link(ctx context.Context, account string) (Link, error) {
	h.mu.Lock()
	if s := h.local[account]; s != nil {
		h.mu.Unlock()
		return s, nil
	}
	h.mu.Unlock()
	row, ok, err := h.cfg.Store.AgentSession(ctx, account)
	if err != nil {
		return nil, err
	}
	if !ok || !row.LeaseUntil.After(time.Now()) || row.Holder == h.holder {
		return nil, ErrNoAgent
	}
	h.mu.Lock()
	if r := h.relays[account]; r != nil && r.addr == row.Address && !r.ended() {
		h.mu.Unlock()
		return r, nil
	}
	h.mu.Unlock()
	r, err := h.openRelay(account, row.Address)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	if old := h.relays[account]; old != nil {
		old.end(errors.New("replaced"))
	}
	h.relays[account] = r
	h.mu.Unlock()
	return r, nil
}

// errReplaced ends a session a newer connection of its agent replaced.
var errReplaced = status.Error(codes.Aborted, "a newer connection of this deploy agent replaced this one")

// Connect takes an organization's agent's connection: its designated robot alone.
func (h *Hub) Connect(stream deploypb.Agents_ConnectServer) error {
	ctx := stream.Context()
	c, ok := authn.FromContext(ctx)
	if !ok || c.Human || c.IsRole() {
		return status.Error(codes.PermissionDenied, "an organization's deploy agent connects here, as its robot")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetHello() == nil {
		return status.Error(codes.InvalidArgument, "an agent says hello first")
	}
	account, err := h.cfg.Account(ctx, c.ID)
	if status.Code(err) == codes.NotFound {
		return status.Errorf(codes.PermissionDenied, "%s is no organization's deploy agent", c.Principal)
	}
	if err != nil {
		return err
	}
	s := newSession(account, stream)
	now := time.Now()
	if err := h.cfg.Store.ClaimAgentSession(ctx, store.AgentSession{Account: account, Holder: h.holder, Address: h.cfg.Address,
		LeaseUntil: now.Add(h.cfg.Lease), ConnectedAt: now}); err != nil {
		return err
	}
	h.mu.Lock()
	old := h.local[account]
	h.local[account] = s
	h.mu.Unlock()
	if old != nil {
		old.end(errReplaced)
	}
	defer func() {
		h.mu.Lock()
		if h.local[account] == s {
			delete(h.local, account)
		}
		h.mu.Unlock()
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := h.cfg.Store.ReleaseAgentSession(rctx, account, h.holder); err != nil {
			h.cfg.Log.Warn("tunnel: releasing an agent's session", "account", account, "err", err)
		}
	}()
	if err := s.Send(ctx, &deploypb.AgentDown{Kind: &deploypb.AgentDown_Welcome{Welcome: &deploypb.AgentWelcome{AccountId: account,
		Cell: h.cfg.Cell}}}); err != nil {
		s.end(err)
		return err
	}
	h.cfg.Log.Info("tunnel: a deploy agent connected", "account", account, "agent", c.Principal)
	go h.renew(ctx, s)
	go func() {
		for {
			up, err := stream.Recv()
			if err != nil {
				s.end(err)
				return
			}
			if id := idOf(up); id != 0 {
				s.routes.dispatch(id, up)
			}
		}
	}()
	<-s.done
	h.cfg.Log.Info("tunnel: a deploy agent's connection ended", "account", account, "err", s.err)
	if errors.Is(s.err, errReplaced) {
		return errReplaced
	}
	return nil
}

// renew keeps a session's lease while it lasts, ending it once another replica holds it.
func (h *Hub) renew(ctx context.Context, s *session) {
	t := time.NewTicker(h.cfg.Renew)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		ok, err := h.cfg.Store.RenewAgentSession(ctx, s.account, h.holder, time.Now().Add(h.cfg.Lease))
		switch {
		case err != nil:
			h.cfg.Log.Warn("tunnel: renewing an agent's session", "account", s.account, "err", err)
		case !ok:
			s.end(errReplaced)
			return
		}
	}
}

// Exchange serves another replica's requests to an agent connected here.
func (h *Hub) Exchange(stream deploypb.AgentRelay_ExchangeServer) error {
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	h.mu.Lock()
	s := h.local[first.GetAccountId()]
	h.mu.Unlock()
	if s == nil {
		return status.Errorf(codes.Unavailable, "no deploy agent of %s is connected to this replica", first.GetAccountId())
	}
	out := newQueue()
	defer out.close()
	sendErr := make(chan error, 1)
	go func() {
		for {
			up, ok := out.pop()
			if !ok {
				return
			}
			if err := stream.Send(up); err != nil {
				sendErr <- err
				return
			}
		}
	}()
	var mu sync.Mutex
	open := map[int64]func(){}
	finish := func(id int64) {
		mu.Lock()
		stop := open[id]
		delete(open, id)
		mu.Unlock()
		if stop != nil {
			stop()
		}
	}
	defer func() {
		mu.Lock()
		ids := make([]int64, 0, len(open))
		for id, stop := range open {
			ids = append(ids, id)
			stop()
		}
		open = map[int64]func(){}
		mu.Unlock()
		for _, id := range ids {
			_ = s.Send(context.WithoutCancel(ctx), cancel(id))
		}
	}()
	forward := func(d *deploypb.AgentDown) error {
		if d == nil {
			return nil
		}
		id := int64(0)
		switch k := d.GetKind().(type) {
		case *deploypb.AgentDown_Kube:
			if k.Kube.GetMethod() != "" {
				id = k.Kube.GetId()
			}
		case *deploypb.AgentDown_Migration:
			id = k.Migration.GetId()
		case *deploypb.AgentDown_Cancel:
			finish(k.Cancel)
		case *deploypb.AgentDown_Welcome:
			return nil
		}
		if id != 0 {
			stop := s.Watch(id, func(up *deploypb.AgentUp) {
				out.put(up)
				if last(up) {
					go finish(id)
				}
			})
			mu.Lock()
			open[id] = stop
			mu.Unlock()
		}
		return s.Send(ctx, d)
	}
	if err := forward(first.GetDown()); err != nil {
		return err
	}
	recvErr := make(chan error, 1)
	go func() {
		for {
			m, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if err := forward(m.GetDown()); err != nil {
				recvErr <- err
				return
			}
		}
	}()
	select {
	case err := <-recvErr:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case err := <-sendErr:
		return err
	case <-s.done:
		return status.Error(codes.Unavailable, "the deploy agent's connection to this replica ended")
	case <-ctx.Done():
		return nil
	}
}

// last reports whether a frame is a request's last.
func last(up *deploypb.AgentUp) bool {
	switch k := up.GetKind().(type) {
	case *deploypb.AgentUp_Kube:
		return k.Kube.GetEnd() || k.Kube.GetError() != ""
	case *deploypb.AgentUp_Migration:
		return true
	}
	return false
}

// idOf is the request an agent's frame answers.
func idOf(up *deploypb.AgentUp) int64 {
	switch k := up.GetKind().(type) {
	case *deploypb.AgentUp_Kube:
		return k.Kube.GetId()
	case *deploypb.AgentUp_Migration:
		return k.Migration.GetId()
	}
	return 0
}

// openRelay opens a relay to the replica at addr holding an account's agent's session.
func (h *Hub) openRelay(account, addr string) (*relayLink, error) {
	ctx, cancel := context.WithCancel(h.base)
	client, err := h.cfg.Dial(ctx, addr)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("tunnel: reaching the replica holding %s's agent: %w", account, err)
	}
	stream, err := client.Exchange(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("tunnel: relaying to %s: %w", addr, err)
	}
	if err := stream.Send(&deploypb.RelayDown{AccountId: account}); err != nil {
		cancel()
		return nil, fmt.Errorf("tunnel: relaying to %s: %w", addr, err)
	}
	r := &relayLink{addr: addr, stream: stream, cancel: cancel, done: make(chan struct{})}
	r.routes.m = map[int64]func(*deploypb.AgentUp){}
	go func() {
		for {
			up, err := stream.Recv()
			if err != nil {
				r.end(err)
				return
			}
			if id := idOf(up); id != 0 {
				r.routes.dispatch(id, up)
			}
		}
	}()
	return r, nil
}

// routes hand an agent's frames to the requests that wait on them.
type routes struct {
	mu sync.Mutex
	m  map[int64]func(*deploypb.AgentUp)
}

func (r *routes) watch(id int64, f func(*deploypb.AgentUp)) func() {
	r.mu.Lock()
	r.m[id] = f
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(r.m, id)
		r.mu.Unlock()
	}
}

func (r *routes) dispatch(id int64, up *deploypb.AgentUp) {
	r.mu.Lock()
	f := r.m[id]
	r.mu.Unlock()
	if f != nil {
		f(up)
	}
}

// session is an agent's connection to this replica.
type session struct {
	account string
	stream  deploypb.Agents_ConnectServer
	sendMu  sync.Mutex
	routes  routes
	done    chan struct{}
	once    sync.Once
	err     error
}

var _ Link = (*session)(nil)

func newSession(account string, stream deploypb.Agents_ConnectServer) *session {
	return &session{account: account, stream: stream, routes: routes{m: map[int64]func(*deploypb.AgentUp){}}, done: make(chan struct{})}
}

func (s *session) Send(_ context.Context, d *deploypb.AgentDown) error {
	select {
	case <-s.done:
		return ErrEnded
	default:
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(d)
}

func (s *session) Watch(id int64, f func(*deploypb.AgentUp)) func() { return s.routes.watch(id, f) }
func (s *session) Done() <-chan struct{}                            { return s.done }
func (s *session) Err() error                                       { return s.err }

func (s *session) end(err error) {
	s.once.Do(func() {
		s.err = err
		close(s.done)
	})
}

// relayLink is a link to an agent through the replica holding its session.
type relayLink struct {
	addr   string
	stream deploypb.AgentRelay_ExchangeClient
	cancel context.CancelFunc
	sendMu sync.Mutex
	routes routes
	done   chan struct{}
	once   sync.Once
	err    error
}

var _ Link = (*relayLink)(nil)

func (r *relayLink) Send(_ context.Context, d *deploypb.AgentDown) error {
	select {
	case <-r.done:
		return ErrEnded
	default:
	}
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	return r.stream.Send(&deploypb.RelayDown{Down: d})
}

func (r *relayLink) Watch(id int64, f func(*deploypb.AgentUp)) func() { return r.routes.watch(id, f) }
func (r *relayLink) Done() <-chan struct{}                            { return r.done }
func (r *relayLink) Err() error                                       { return r.err }

func (r *relayLink) ended() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

func (r *relayLink) end(err error) {
	r.once.Do(func() {
		r.err = err
		close(r.done)
		r.cancel()
	})
}

// queue is frames waiting to be sent, unbounded: a slow reader of one request never holds up the
// connection's others.
type queue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []*deploypb.AgentUp
	closed bool
}

func newQueue() *queue {
	q := &queue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *queue) put(up *deploypb.AgentUp) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.items = append(q.items, up)
	q.cond.Signal()
}

func (q *queue) pop() (*deploypb.AgentUp, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return nil, false
	}
	up := q.items[0]
	q.items = q.items[1:]
	return up, true
}

func (q *queue) close() {
	q.mu.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
}
