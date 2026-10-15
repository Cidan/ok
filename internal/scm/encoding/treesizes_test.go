package encoding

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"slices"
	"strconv"
	"testing"
)

//go:embed testdata/tree_sizes.json
var treeSizesJSON []byte

type treeSizesVectors struct {
	Comment []string `json:"comment"`
	Valid   []struct {
		Name  string   `json:"name"`
		Sizes []string `json:"sizes"`
		Bytes hexLines `json:"bytes"`
	} `json:"valid"`
	Invalid []struct {
		Name  string   `json:"name"`
		Error Code     `json:"error"`
		Bytes hexLines `json:"bytes"`
	} `json:"invalid"`
}

func loadTreeSizesVectors(t testing.TB) treeSizesVectors {
	t.Helper()
	var vf treeSizesVectors
	dec := json.NewDecoder(bytes.NewReader(treeSizesJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&vf); err != nil {
		t.Fatalf("parsing testdata/tree_sizes.json: %v", err)
	}
	if len(vf.Valid) == 0 || len(vf.Invalid) == 0 {
		t.Fatal("testdata/tree_sizes.json has no vectors")
	}
	return vf
}

// Every valid record encodes to its bytes and decodes back to its sizes.
func TestTreeSizesValidVectors(t *testing.T) {
	for _, v := range loadTreeSizesVectors(t).Valid {
		t.Run(v.Name, func(t *testing.T) {
			want := v.Bytes.decode(t)
			sizes := make([]uint64, len(v.Sizes))
			for i, s := range v.Sizes {
				n, err := strconv.ParseUint(s, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				sizes[i] = n
			}
			if got := EncodeTreeSizes(&TreeSizes{Sizes: sizes}); !bytes.Equal(got, want) {
				t.Fatalf("encoded %x, want %x", got, want)
			}
			got, err := DecodeTreeSizes(want)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got.Sizes, sizes) {
				t.Fatalf("decoded %v, want %v", got.Sizes, sizes)
			}
		})
	}
}

// Every invalid record is refused with its code.
func TestTreeSizesInvalidVectors(t *testing.T) {
	for _, v := range loadTreeSizesVectors(t).Invalid {
		t.Run(v.Name, func(t *testing.T) {
			_, err := DecodeTreeSizes(v.Bytes.decode(t))
			if got := CodeOf(err); got != v.Error {
				t.Fatalf("decoding: %v (code %q), want code %q", err, got, v.Error)
			}
		})
	}
}

// A tree sizes record is not an object: Decode refuses its kind byte.
func TestTreeSizesAreNoObject(t *testing.T) {
	_, err := Decode(EncodeTreeSizes(&TreeSizes{Sizes: []uint64{1}}))
	if CodeOf(err) != CodeKind {
		t.Fatalf("Decode of a tree sizes record: %v", err)
	}
}

// Any bytes decode to a record that encodes back to them, or are refused.
func FuzzDecodeTreeSizes(f *testing.F) {
	for _, v := range loadTreeSizesVectors(f).Valid {
		b, err := v.Bytes.bytes()
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := DecodeTreeSizes(b)
		if err != nil {
			return
		}
		if got := EncodeTreeSizes(s); !bytes.Equal(got, b) {
			t.Fatalf("%x decoded and encoded again is %x", b, got)
		}
	})
}
