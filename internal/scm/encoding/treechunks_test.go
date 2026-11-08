package encoding

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
)

//go:embed testdata/tree_chunks.json
var treeChunksJSON []byte

// vectorTree is the tree a vector's rule makes: count entries named prefix and the index (five
// digits), each a file whose ID is the BLAKE3 of its name, executable every seventh.
func vectorTree(prefix string, count int) *Tree {
	t := &Tree{}
	for i := range count {
		name := fmt.Sprintf("%s%05d", prefix, i)
		t.Entries = append(t.Entries, TreeEntry{Name: name, Value: TreeValue{Kind: TreeValueFile, ID: Hash([]byte(name)), Executable: i%7 == 0}})
	}
	return t
}

func chunkIDOf(b []byte) ID {
	h := NewHasher(KindChunk)
	h.Write(b)
	return h.Sum()
}

func treeBody(t *testing.T, tree *Tree) ([]byte, ID) {
	t.Helper()
	canonical, err := Encode(tree)
	if err != nil {
		t.Fatal(err)
	}
	return canonical[1:], Hash(canonical)
}

// Each case's tree encodes to its body and cuts into exactly its chunks.
func TestTreeChunkVectors(t *testing.T) {
	var v struct {
		Comment []string `json:"comment"`
		Cases   []struct {
			Name    string `json:"name"`
			Prefix  string `json:"prefix"`
			Entries int    `json:"entries"`
			Tree    string `json:"tree"`
			Size    int    `json:"size"`
			Chunks  []struct {
				Length int    `json:"length"`
				ID     string `json:"id"`
			} `json:"chunks"`
		} `json:"cases"`
	}
	dec := json.NewDecoder(bytes.NewReader(treeChunksJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil || len(v.Cases) == 0 {
		t.Fatalf("parsing testdata/tree_chunks.json: %v", err)
	}
	for _, c := range v.Cases {
		t.Run(c.Name, func(t *testing.T) {
			body, id := treeBody(t, vectorTree(c.Prefix, c.Entries))
			if id.String() != c.Tree || len(body) != c.Size {
				t.Fatalf("tree %s of %d bytes, want %s of %d", id, len(body), c.Tree, c.Size)
			}
			chunks, err := ChunkTree(body)
			if err != nil {
				t.Fatal(err)
			}
			if len(chunks) != len(c.Chunks) {
				t.Fatalf("%d chunks, want %d", len(chunks), len(c.Chunks))
			}
			for i, ch := range chunks {
				if len(ch) != c.Chunks[i].Length || chunkIDOf(ch).String() != c.Chunks[i].ID {
					t.Errorf("chunk %d: %d bytes, %s; want %d, %s", i, len(ch), chunkIDOf(ch), c.Chunks[i].Length, c.Chunks[i].ID)
				}
			}
		})
	}
}

// A large tree's chunks join to its body: the entry count, then runs of whole entries, each at most
// a chunk's largest, whose entries are the tree's in order.
func TestChunkTreeJoins(t *testing.T) {
	tree := vectorTree("x", 30000)
	body, _ := treeBody(t, tree)
	chunks, err := ChunkTree(body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.Join(chunks, nil), body) {
		t.Fatal("the chunks do not join to the body")
	}
	if n, err := TreeCount(chunks[0]); err != nil || n != 30000 {
		t.Fatalf("count %d, %v", n, err)
	}
	var entries []TreeEntry
	for i, ch := range chunks[1:] {
		if len(ch) > treeLeafMax {
			t.Fatalf("leaf %d is %d bytes", i, len(ch))
		}
		got, err := TreeLeafEntries(ch)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, got...)
	}
	if !slices.EqualFunc(entries, tree.Entries, func(a, b TreeEntry) bool { return a.Name == b.Name && a.Value == b.Value }) {
		t.Fatal("the leaves' entries are not the tree's")
	}
}

// An edit of one entry changes one leaf; an entry added or removed changes the count and the leaves
// around it, not the rest: every other chunk is the same, so an edit stores and sends a few.
func TestChunkTreeEdits(t *testing.T) {
	tree := vectorTree("y", 20000)
	body, _ := treeBody(t, tree)
	before, err := ChunkTree(body)
	if err != nil {
		t.Fatal(err)
	}
	ids := func(chunks [][]byte) map[ID]bool {
		out := map[ID]bool{}
		for _, c := range chunks {
			out[chunkIDOf(c)] = true
		}
		return out
	}
	was := ids(before)
	changed := func(t *testing.T, edited *Tree) int {
		t.Helper()
		b, _ := treeBody(t, edited)
		after, err := ChunkTree(b)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for id := range ids(after) {
			if !was[id] {
				n++
			}
		}
		return n
	}
	value := &Tree{Entries: slices.Clone(tree.Entries)}
	value.Entries[12345].Value.ID = Hash([]byte("edited"))
	if n := changed(t, value); n != 1 {
		t.Errorf("an edited entry: %d new chunks, want 1", n)
	}
	added := &Tree{Entries: slices.Insert(slices.Clone(tree.Entries), 9001, TreeEntry{Name: "y09000a", Value: tree.Entries[0].Value})}
	if n := changed(t, added); n < 2 || n > 3 {
		t.Errorf("an added entry: %d new chunks, want the count and a leaf or two", n)
	}
	removed := &Tree{Entries: slices.Delete(slices.Clone(tree.Entries), 15000, 15001)}
	if n := changed(t, removed); n < 2 || n > 3 {
		t.Errorf("a removed entry: %d new chunks, want the count and a leaf or two", n)
	}
}

// A body with an entry longer than a chunk may be is not cut.
func TestChunkTreeRefusesAHugeEntry(t *testing.T) {
	tree := vectorTree("z", 3000)
	tree.Entries = append(tree.Entries, TreeEntry{Name: "zz" + string(bytes.Repeat([]byte("n"), treeLeafMax)), Value: tree.Entries[0].Value})
	body, _ := treeBody(t, tree)
	if _, err := ChunkTree(body); err == nil {
		t.Fatal("cut a tree with an entry of more than a chunk")
	}
}
