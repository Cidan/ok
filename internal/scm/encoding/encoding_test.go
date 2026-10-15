package encoding

import (
	"bytes"
	"testing"
)

// BLAKE3's official known answers (BLAKE3-team/BLAKE3 test_vectors.json, hash mode): the
// input of each length is the repeating sequence 0, 1, …, 250.
var blake3KnownAnswers = []struct {
	n    int
	hash string
}{
	{0, "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"},
	{1, "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213"},
	{1023, "10108970eeda3eb932baac1428c7a2163b0e924c9a9e25b35bba72b28f70bd11"},
	{1024, "42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7"},
	{1025, "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444"},
	{3072, "b98cb0ff3623be03326b373de6b9095218513e64f1ee2edd2525c7ad1e5cffd2"},
	{102400, "bc3e3d41a1146b069abffad3c0d44860cf664390afce4d9661f7902e7943e085"},
}

func knownAnswerInput(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func TestHashIsBLAKE3(t *testing.T) {
	for _, ka := range blake3KnownAnswers {
		if got := Hash(knownAnswerInput(ka.n)).String(); got != ka.hash {
			t.Errorf("Hash(%d bytes) = %s, want %s", ka.n, got, ka.hash)
		}
	}
}

func TestHasherMatchesHash(t *testing.T) {
	for _, n := range []int{0, 1, 1023, 1024, 1025, 128 << 10, 1<<20 + 7} {
		contents := knownAnswerInput(n)
		want, err := IDOf(&File{Contents: contents})
		if err != nil {
			t.Fatal(err)
		}
		for _, piece := range []int{1, 7, 1024, 64 << 10} {
			h := NewHasher(KindFile)
			for rest := contents; len(rest) > 0; {
				k := min(piece, len(rest))
				h.Write(rest[:k])
				rest = rest[k:]
			}
			if got := h.Sum(); got != want {
				t.Errorf("%d bytes in %d-byte pieces: got %s, want %s", n, piece, got, want)
			}
		}
	}
}

func TestKindSeparatesIDs(t *testing.T) {
	contents := []byte("same bytes")
	file, _ := IDOf(&File{Contents: contents})
	chunk, _ := IDOf(&Chunk{Contents: contents})
	symlink, _ := IDOf(&Symlink{Target: string(contents)})
	if file == chunk || file == symlink || chunk == symlink {
		t.Fatalf("kinds share an ID: file %s, chunk %s, symlink %s", file, chunk, symlink)
	}
}

func TestEmptyTreeID(t *testing.T) {
	got, err := IDOf(&Tree{})
	if err != nil {
		t.Fatal(err)
	}
	if got != EmptyTreeID {
		t.Fatalf("IDOf(empty tree) = %s, EmptyTreeID = %s", got, EmptyTreeID)
	}
}

func TestSignedDataOmitsOnlyTheSignature(t *testing.T) {
	c := &Commit{
		Parents:        []ID{RootCommitID},
		RootTree:       Merge[ID]{EmptyTreeID},
		ConflictLabels: Merge[string]{""},
		Description:    "signed\n",
		SecureSig:      []byte("sig"),
	}
	signed, err := Encode(c)
	if err != nil {
		t.Fatal(err)
	}
	data, err := SignedData(c)
	if err != nil {
		t.Fatal(err)
	}
	// The signed encoding is the unsigned one with its final absent-option byte replaced
	// by a present tag and the signature bytes.
	if want := append(bytes.Clone(data[:len(data)-1]), 1, 3, 0, 0, 0, 0, 0, 0, 0, 's', 'i', 'g'); !bytes.Equal(signed, want) {
		t.Fatalf("signed encoding\n got %x\nwant %x", signed, want)
	}
	if c.SecureSig == nil {
		t.Fatal("SignedData cleared the commit's signature")
	}
}

func TestSetAndMapOrderDoNotMatter(t *testing.T) {
	a, b := ID{1}, ID{2}
	v1 := &View{HeadIDs: []ID{a, b}, WCCommitIDs: map[string]ID{"x": a, "y": b}}
	v2 := &View{HeadIDs: []ID{b, a}, WCCommitIDs: map[string]ID{"y": b, "x": a}}
	e1, err := Encode(v1)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := Encode(v2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(e1, e2) {
		t.Fatalf("head order changed the encoding:\n%x\n%x", e1, e2)
	}
}

func TestEncodeRejects(t *testing.T) {
	file := TreeValue{Kind: TreeValueFile}
	for _, tc := range []struct {
		name string
		obj  Object
		want Code
	}{
		{"even merge", &Commit{RootTree: Merge[ID]{{}, {}}, ConflictLabels: Merge[string]{""}}, CodeMerge},
		{"empty merge", &Commit{RootTree: Merge[ID]{{}}, ConflictLabels: Merge[string]{}}, CodeMerge},
		{"even ref target", &View{LocalBookmarks: map[string]RefTarget{"b": {nil, nil}}}, CodeMerge},
		{"invalid UTF-8", &Commit{RootTree: Merge[ID]{{}}, ConflictLabels: Merge[string]{""}, Description: "\xff"}, CodeUTF8},
		{"invalid symlink target", &Symlink{Target: "\xc3("}, CodeUTF8},
		{"empty name", &Tree{Entries: []TreeEntry{{Name: "", Value: file}}}, CodeName},
		{"dot name", &Tree{Entries: []TreeEntry{{Name: ".", Value: file}}}, CodeName},
		{"dot-dot name", &Tree{Entries: []TreeEntry{{Name: "..", Value: file}}}, CodeName},
		{"slash name", &Tree{Entries: []TreeEntry{{Name: "a/b", Value: file}}}, CodeName},
		{"NUL name", &Tree{Entries: []TreeEntry{{Name: "a\x00", Value: file}}}, CodeName},
		{"unsorted entries", &Tree{Entries: []TreeEntry{{Name: "b", Value: file}, {Name: "a", Value: file}}}, CodeUnsorted},
		{"duplicate entries", &Tree{Entries: []TreeEntry{{Name: "a", Value: file}, {Name: "a", Value: file}}}, CodeDuplicate},
		{"submodule", &Tree{Entries: []TreeEntry{{Name: "m", Value: TreeValue{Kind: 3}}}}, CodeTag},
		{"empty path", &CopyHistory{CurrentPath: ""}, CodePath},
		{"absolute path", &CopyHistory{CurrentPath: "/a"}, CodePath},
		{"dot-dot path", &CopyHistory{CurrentPath: "a/../b"}, CodePath},
		{"duplicate heads", &View{HeadIDs: []ID{{1}, {1}}}, CodeDuplicate},
		{"remote ref state", &View{RemoteViews: map[string]RemoteView{"team": {
			Bookmarks: map[string]RemoteRef{"main": {Target: RefTarget{nil}, State: 2}},
		}}}, CodeState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := Encode(tc.obj)
			if got := CodeOf(err); got != tc.want {
				t.Fatalf("Encode = %x, %v; want code %q", b, err, tc.want)
			}
		})
	}
}

func TestDecodeErrorOffsets(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []byte
		want int
	}{
		{"kind", []byte{0x99}, 0},
		{"trailing", []byte{byte(KindTree), 0, 0, 0, 0, 0, 0, 0, 0, 0xaa}, 9},
		{"count", []byte{byte(KindTree), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, 1},
	} {
		_, err := Decode(tc.in)
		e, ok := err.(*Error)
		if !ok {
			t.Fatalf("%s: Decode error %v is not an *Error", tc.name, err)
		}
		if e.Offset != tc.want {
			t.Errorf("%s: offset %d, want %d (%v)", tc.name, e.Offset, tc.want, err)
		}
	}
}
