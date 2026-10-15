package worker

import (
	"context"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/Cidan/ok/internal/buildsys/bazel"
	"github.com/Cidan/ok/internal/checkout"
)

// keptGraphs is how many commits' graphs a worker keeps: main's recent heads and the patchsets
// whose post compares with their pre.
const keptGraphs = 64

// baseDriver is d, as the slot's other checkout runs it: an output base of its own, so the
// change's Bazel server keeps its workspace.
func (cfg Config) baseDriver(d *bazel.Driver) *bazel.Driver {
	b := *d
	b.OutputBase = cfg.baseOutputBase
	return &b
}

// graphAt is the build graph at a commit: kept from an earlier query of it, or queried in co,
// which is moved to the commit first unless it is the slot's own checkout (which already holds
// the run's commit). log, when set, says which.
func graphAt(ctx context.Context, cfg Config, d *bazel.Driver, repo int64, commit []byte, co checkout.Checkout, log *logStream) (*bazel.Graph, error) {
	graphs := bazel.Graphs{Dir: cfg.Graphs, Keep: keptGraphs}
	key := strconv.FormatInt(repo, 10) + "-" + hex.EncodeToString(commit)
	if g, ok := graphs.Get(key); ok {
		if log != nil {
			log.Printf("the build graph at %.10x: kept from before (%d targets)", commit, g.Len())
		}
		return g, nil
	}
	if co != cfg.checkout {
		if log != nil {
			log.Printf("checking out %.10x to query its build graph", commit)
		}
		if _, err := co.Sync(ctx, repo, commit, nil); err != nil {
			return nil, err
		}
	}
	start := time.Now()
	g, keepErr, err := graphs.Query(ctx, d, co.Dir(), key)
	if err != nil {
		return nil, err
	}
	if log != nil {
		log.Printf("the build graph at %.10x: %d targets, queried in %s", commit, g.Len(), time.Since(start).Round(time.Millisecond))
		if keepErr != nil {
			log.Printf("note: keeping the graph: %v", keepErr)
		}
	}
	return g, nil
}
