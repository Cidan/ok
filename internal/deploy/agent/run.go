package agent

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// cellsEvery is how often a runner asks which cells it serves.
const cellsEvery = time.Minute

// Runner keeps an agent connected to each cell's deployer that holds its organization's
// repositories, out from the cluster.
type Runner struct {
	Agent *Agent
	// Cells are the cells to connect to: identity's DeployCells ("" for an installation of none).
	Cells func(ctx context.Context) ([]string, error)
	// Connect opens a connection to a cell's deployer.
	Connect func(ctx context.Context, cell string) (deploypb.Agents_ConnectClient, error)
	Log     *slog.Logger
	// Every is how often it asks which cells: cellsEvery unless set.
	Every time.Duration
}

// Run connects until ctx ends.
func (r *Runner) Run(ctx context.Context) error {
	if r.Log == nil {
		r.Log = slog.Default()
	}
	every := r.Every
	if every <= 0 {
		every = cellsEvery
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	running := map[string]context.CancelFunc{}
	defer func() {
		for _, stop := range running {
			stop()
		}
	}()
	for {
		cells, err := r.Cells(ctx)
		if err != nil && ctx.Err() == nil {
			r.Log.Warn("agent: asking which cells it serves", "err", err)
		}
		if err == nil {
			for cell, stop := range running {
				if !slices.Contains(cells, cell) {
					stop()
					delete(running, cell)
				}
			}
			for _, cell := range cells {
				if running[cell] != nil {
					continue
				}
				cctx, stop := context.WithCancel(ctx)
				running[cell] = stop
				wg.Add(1)
				go func() {
					defer wg.Done()
					r.keep(cctx, cell)
				}()
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(every):
		}
	}
}

// keep keeps a cell's connection, again after a backoff — a second, doubling to thirty — once it
// ends, the backoff starting over after one that lasted a minute.
func (r *Runner) keep(ctx context.Context, cell string) {
	backoff := time.Second
	for ctx.Err() == nil {
		began := time.Now()
		stream, err := r.Connect(ctx, cell)
		if err == nil {
			_, err = r.Agent.Serve(ctx, stream)
		}
		if ctx.Err() != nil {
			return
		}
		r.Log.Warn("agent: the connection to a cell's deployer ended", "cell", cell, "err", err)
		if time.Since(began) > time.Minute {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 30*time.Second)
	}
}
