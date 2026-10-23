// Package tunnel carries the deployer's requests to organizations' deploy agents and their answers
// back (docs/hosted.md, "Deploy agents, as built"): a Link is an agent's connection — held by this
// replica, or relayed to the one that holds it — over which Transport sends client-go's requests to
// the agent's cluster, and Migrate a deployment's migrations.
package tunnel

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"net/http"

	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// Link carries frames to an agent, and its answers back.
type Link interface {
	// Send sends the agent a frame.
	Send(ctx context.Context, d *deploypb.AgentDown) error
	// Watch hands the agent's frames of a request to f, in order, until stop; f must not block.
	Watch(id int64, f func(*deploypb.AgentUp)) (stop func())
	// Done is closed once the link ends; Err says why.
	Done() <-chan struct{}
	Err() error
}

// ErrNoAgent is a request for an organization whose agent is not connected.
var ErrNoAgent = errors.New("the organization's deploy agent is not connected")

// ErrEnded is a request whose link ended before it was answered.
var ErrEnded = errors.New("the deploy agent's connection ended")

// NewID is a request's ID: random, positive, never 0.
func NewID() int64 {
	var b [8]byte
	for {
		rand.Read(b[:])
		if id := int64(binary.BigEndian.Uint64(b[:]) &^ (1 << 63)); id != 0 {
			return id
		}
	}
}

// frameBody bounds a frame's piece of a body, well under gRPC's message limit.
const frameBody = 256 << 10

// headersOut are an http.Header's frames.
func headersOut(h http.Header) []*deploypb.Header {
	out := make([]*deploypb.Header, 0, len(h))
	for name, vs := range h {
		out = append(out, &deploypb.Header{Name: name, Values: vs})
	}
	return out
}

// headersIn are frames' http.Header.
func headersIn(hs []*deploypb.Header) http.Header {
	out := http.Header{}
	for _, h := range hs {
		for _, v := range h.GetValues() {
			out.Add(h.GetName(), v)
		}
	}
	return out
}

// cancel is the frame that ends a request.
func cancel(id int64) *deploypb.AgentDown {
	return &deploypb.AgentDown{Kind: &deploypb.AgentDown_Cancel{Cancel: id}}
}
