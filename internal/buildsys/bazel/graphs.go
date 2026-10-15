package bazel

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// Graphs keeps commits' build graphs on disk, each the query's answer under a key of the caller's
// (a repository and commit), so a commit is queried once; beyond Keep, the least recently used are
// forgotten.
type Graphs struct {
	Dir  string
	Keep int
}

// Get is the graph kept under key, if one is.
func (g Graphs) Get(key string) (*Graph, bool) {
	file := g.file(key)
	f, err := os.Open(file)
	if err != nil {
		return nil, false
	}
	defer f.Close()
	graph, err := ParseGraph(f)
	if err != nil {
		return nil, false
	}
	now := time.Now()
	_ = os.Chtimes(file, now, now)
	return graph, true
}

// Query queries the graph of the checkout at dir and keeps it under key. A graph that cannot be
// kept is still returned, with the error that kept it from being kept as keepErr.
func (g Graphs) Query(ctx context.Context, d *Driver, dir, key string) (graph *Graph, keepErr, err error) {
	var answer bytes.Buffer
	if graph, err = d.GraphTo(ctx, dir, &answer); err != nil {
		return nil, nil, err
	}
	return graph, g.keep(key, answer.Bytes()), nil
}

func (g Graphs) file(key string) string { return filepath.Join(g.Dir, key+".jsonl") }

// keep writes an answer whole, then forgets the least recently used beyond Keep.
func (g Graphs) keep(key string, answer []byte) error {
	if err := os.MkdirAll(g.Dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(g.Dir, ".graph-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(answer); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), g.file(key)); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	entries, err := os.ReadDir(g.Dir)
	if err != nil {
		return err
	}
	type kept struct {
		name string
		used time.Time
	}
	var all []kept
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		if info, err := e.Info(); err == nil {
			all = append(all, kept{e.Name(), info.ModTime()})
		}
	}
	slices.SortFunc(all, func(a, b kept) int { return b.used.Compare(a.used) })
	for _, k := range all[min(len(all), max(g.Keep, 1)):] {
		os.Remove(filepath.Join(g.Dir, k.name))
	}
	return nil
}

// Empty is the graph of nothing: every target of another is new beside it.
func Empty() *Graph {
	g, _ := ParseGraph(strings.NewReader(""))
	return g
}
