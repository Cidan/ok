package bazel

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Graphs keeps what each query answered, and forgets the least recently used beyond Keep.
func TestGraphs(t *testing.T) {
	dir := t.TempDir()
	answer, err := filepath.Abs(filepath.Join("testdata", "sample", "base.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	fake := filepath.Join(t.TempDir(), "bazel")
	os.WriteFile(fake, []byte("#!/bin/sh\ncat "+answer+"\n"), 0o755)
	d := &Driver{Bazel: fake}
	g := Graphs{Dir: dir, Keep: 2}
	if _, ok := g.Get("1-a"); ok {
		t.Fatal("a graph kept before any was")
	}
	for i, key := range []string{"1-a", "1-b", "1-c"} {
		graph, keepErr, err := g.Query(context.Background(), d, t.TempDir(), key)
		if err != nil || keepErr != nil || graph.Len() == 0 {
			t.Fatalf("Query(%s) = %v, %v, %v", key, graph, keepErr, err)
		}
		// Kept in the past, each later than the last; a is used again after b.
		at := time.Now().Add(time.Duration(i-10) * time.Second)
		os.Chtimes(filepath.Join(dir, key+".jsonl"), at, at)
		if key == "1-b" {
			at = time.Now().Add(-time.Second)
			os.Chtimes(filepath.Join(dir, "1-a.jsonl"), at, at)
		}
	}
	for key, want := range map[string]bool{"1-a": true, "1-b": false, "1-c": true} {
		if graph, ok := g.Get(key); ok != want || (ok && graph.Len() == 0) {
			t.Errorf("Get(%s) = %v, want kept %v", key, ok, want)
		}
	}
}
