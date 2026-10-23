// Package agent is an organization's deploy agent (docs/hosted.md, "Deploy agents, as built"): run
// in its cluster as one of its robots, it connects out to each cell's deployer that holds its
// repositories, and answers what the deployer asks — its cluster's API, which it calls with its own
// ServiceAccount, and migrations, which it runs with its own environment — so ok holds no
// credential to the cluster or its databases.
package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"k8s.io/client-go/rest"

	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// maxRequest bounds a request's body, as the deployer's does.
const maxRequest = 16 << 20

// frameBody bounds a frame's piece of an answer.
const frameBody = 256 << 10

// Agent answers deployers' requests.
type Agent struct {
	// Kube is its cluster, as its ServiceAccount.
	Kube *rest.Config
	// Migrate runs a deployment's migration; nil refuses them.
	Migrate func(ctx context.Context, req *deploypb.MigrationRequest) *deploypb.MigrationResult
	Version string
	Log     *slog.Logger

	once   sync.Once
	client *http.Client
	base   string
	err    error
}

func (a *Agent) init() error {
	a.once.Do(func() {
		if a.Log == nil {
			a.Log = slog.Default()
		}
		cfg := rest.CopyConfig(a.Kube)
		cfg.Timeout = 0 // a watch lasts as long as the deployer's
		if a.client, a.err = rest.HTTPClientFor(cfg); a.err != nil {
			return
		}
		a.base = strings.TrimSuffix(cfg.Host, "/")
		if !strings.Contains(a.base, "://") {
			a.base = "https://" + a.base
		}
	})
	return a.err
}

// Serve speaks one connection to a deployer: it says hello, is welcomed, then answers the
// deployer's requests until the connection ends, canceling those under way.
func (a *Agent) Serve(ctx context.Context, stream deploypb.Agents_ConnectClient) (*deploypb.AgentWelcome, error) {
	if err := a.init(); err != nil {
		return nil, fmt.Errorf("agent: its cluster: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := &conn{a: a, stream: stream, calls: map[int64]*call{}}
	if err := c.send(&deploypb.AgentUp{Kind: &deploypb.AgentUp_Hello{Hello: &deploypb.AgentHello{Version: a.Version}}}); err != nil {
		return nil, err
	}
	first, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	welcome := first.GetWelcome()
	if welcome == nil {
		return nil, errors.New("agent: the deployer did not welcome it")
	}
	a.Log.Info("agent: connected", "account", welcome.GetAccountId(), "cell", welcome.GetCell())
	defer c.cancelAll()
	for {
		d, err := stream.Recv()
		if err != nil {
			return welcome, err
		}
		switch k := d.GetKind().(type) {
		case *deploypb.AgentDown_Kube:
			c.kube(ctx, k.Kube)
		case *deploypb.AgentDown_Migration:
			c.migrate(ctx, k.Migration)
		case *deploypb.AgentDown_Cancel:
			c.cancel(k.Cancel)
		}
	}
}

// conn is one connection's requests.
type conn struct {
	a      *Agent
	stream deploypb.Agents_ConnectClient
	sendMu sync.Mutex

	mu    sync.Mutex
	calls map[int64]*call
}

// call is a request under way: its body as it comes, and how to stop it.
type call struct {
	method, url string
	headers     []*deploypb.Header
	body        bytes.Buffer
	refused     bool
	cancel      context.CancelFunc
}

func (c *conn) send(up *deploypb.AgentUp) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.stream.Send(up)
}

func (c *conn) kubeFrame(f *deploypb.KubeResponseFrame) {
	if err := c.send(&deploypb.AgentUp{Kind: &deploypb.AgentUp_Kube{Kube: f}}); err != nil {
		c.a.Log.Debug("agent: answering", "err", err)
	}
}

// kube takes a frame of a request to the cluster: the request starts once its body has come.
func (c *conn) kube(ctx context.Context, f *deploypb.KubeRequestFrame) {
	id := f.GetId()
	c.mu.Lock()
	cl := c.calls[id]
	if cl == nil {
		if f.GetMethod() == "" {
			c.mu.Unlock()
			return // the rest of one canceled
		}
		cl = &call{method: f.GetMethod(), url: f.GetUrl(), headers: f.GetHeaders()}
		if err := Allowed(cl.method, cl.url); err != nil {
			cl.refused = true
			c.a.Log.Warn("agent: refused a request", "method", cl.method, "url", cl.url, "err", err)
			go c.kubeFrame(&deploypb.KubeResponseFrame{Id: id, Error: err.Error()})
		}
		c.calls[id] = cl
	}
	if !cl.refused {
		if cl.body.Len()+len(f.GetBody()) > maxRequest {
			cl.refused = true
			go c.kubeFrame(&deploypb.KubeResponseFrame{Id: id, Error: "the request's body is too large"})
		} else {
			cl.body.Write(f.GetBody())
		}
	}
	if !f.GetEnd() {
		c.mu.Unlock()
		return
	}
	if cl.refused {
		delete(c.calls, id)
		c.mu.Unlock()
		return
	}
	cctx, cancel := context.WithCancel(ctx)
	cl.cancel = cancel
	c.mu.Unlock()
	go c.do(cctx, id, cl)
}

// do makes a request of the cluster's API server, streaming its answer back as it comes.
func (c *conn) do(ctx context.Context, id int64, cl *call) {
	defer c.finish(id)
	req, err := http.NewRequestWithContext(ctx, cl.method, c.a.base+cl.url, bytes.NewReader(cl.body.Bytes()))
	if err != nil {
		c.kubeFrame(&deploypb.KubeResponseFrame{Id: id, Error: err.Error()})
		return
	}
	for _, h := range cl.headers {
		for _, name := range forwarded {
			if strings.EqualFold(h.GetName(), name) {
				for _, v := range h.GetValues() {
					req.Header.Add(name, v)
				}
			}
		}
	}
	resp, err := c.a.client.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			c.kubeFrame(&deploypb.KubeResponseFrame{Id: id, Error: fmt.Sprintf("reaching the cluster: %v", err)})
		}
		return
	}
	defer resp.Body.Close()
	head := &deploypb.KubeResponseFrame{Id: id, Status: int32(resp.StatusCode)}
	for name, vs := range resp.Header {
		if !dropped[name] {
			head.Headers = append(head.Headers, &deploypb.Header{Name: name, Values: vs})
		}
	}
	c.kubeFrame(head)
	buf := make([]byte, frameBody)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			c.kubeFrame(&deploypb.KubeResponseFrame{Id: id, Body: append([]byte(nil), buf[:n]...)})
		}
		if errors.Is(err, io.EOF) {
			c.kubeFrame(&deploypb.KubeResponseFrame{Id: id, End: true})
			return
		}
		if err != nil {
			if ctx.Err() == nil {
				c.kubeFrame(&deploypb.KubeResponseFrame{Id: id, Error: fmt.Sprintf("reading the cluster's answer: %v", err)})
			}
			return
		}
	}
}

// migrate runs a migration, answering what it did.
func (c *conn) migrate(ctx context.Context, req *deploypb.MigrationRequest) {
	id := req.GetId()
	cctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.calls[id] = &call{cancel: cancel}
	c.mu.Unlock()
	go func() {
		defer c.finish(id)
		var res *deploypb.MigrationResult
		if c.a.Migrate == nil {
			res = &deploypb.MigrationResult{Error: "this agent runs no migrations"}
		} else {
			res = c.a.Migrate(cctx, req)
		}
		res.Id = id
		if err := c.send(&deploypb.AgentUp{Kind: &deploypb.AgentUp_Migration{Migration: res}}); err != nil {
			c.a.Log.Warn("agent: answering a migration", "err", err)
		}
	}()
}

func (c *conn) cancel(id int64) {
	c.mu.Lock()
	cl := c.calls[id]
	delete(c.calls, id)
	c.mu.Unlock()
	if cl != nil && cl.cancel != nil {
		cl.cancel()
	}
}

func (c *conn) finish(id int64) {
	c.mu.Lock()
	cl := c.calls[id]
	delete(c.calls, id)
	c.mu.Unlock()
	if cl != nil && cl.cancel != nil {
		cl.cancel()
	}
}

func (c *conn) cancelAll() {
	c.mu.Lock()
	calls := c.calls
	c.calls = map[int64]*call{}
	c.mu.Unlock()
	for _, cl := range calls {
		if cl.cancel != nil {
			cl.cancel()
		}
	}
}
