package encoding

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"math/rand/v2"
	"strconv"
	"testing"
)

//go:embed testdata/delta.json
var deltaJSON []byte

type deltaVectors struct {
	Comment []string `json:"comment"`
	Valid   []struct {
		Name   string   `json:"name"`
		Base   hexLines `json:"base"`
		Delta  hexLines `json:"delta"`
		Result hexLines `json:"result"`
	} `json:"valid"`
	Invalid []struct {
		Name  string   `json:"name"`
		Base  hexLines `json:"base"`
		Delta hexLines `json:"delta"`
		Limit string   `json:"limit"`
	} `json:"invalid"`
}

func loadDeltaVectors(t *testing.T) deltaVectors {
	t.Helper()
	var v deltaVectors
	dec := json.NewDecoder(bytes.NewReader(deltaJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("parsing testdata/delta.json: %v", err)
	}
	if len(v.Valid) == 0 || len(v.Invalid) == 0 {
		t.Fatal("testdata/delta.json has no vectors")
	}
	return v
}

// Every valid vector's result encodes against its base to exactly its delta, which applies back to
// the result; every invalid delta is refused.
func TestDeltaVectors(t *testing.T) {
	v := loadDeltaVectors(t)
	for _, c := range v.Valid {
		t.Run(c.Name, func(t *testing.T) {
			base, delta, result := c.Base.decode(t), c.Delta.decode(t), c.Result.decode(t)
			if got := EncodeDelta(base, result); !bytes.Equal(got, delta) {
				t.Errorf("encoded %x, want %x", got, delta)
			}
			got, err := ApplyDelta(base, delta, uint64(len(result)))
			if err != nil || !bytes.Equal(got, result) {
				t.Errorf("applied: %q, %v; want %q", got, err, result)
			}
		})
	}
	for _, c := range v.Invalid {
		t.Run(c.Name, func(t *testing.T) {
			limit, err := strconv.ParseUint(c.Limit, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := ApplyDelta(c.Base.decode(t), c.Delta.decode(t), limit); err == nil {
				t.Errorf("applied, to %q", got)
			}
		})
	}
}

// Deltas of random edits apply back to their targets, and are a fraction of them: copies find what
// the base shares, wherever it moved.
func TestDeltaRoundTrips(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	word := func() []byte {
		b := make([]byte, 3+rng.IntN(8))
		for i := range b {
			b[i] = byte('a' + rng.IntN(26))
		}
		return append(b, ' ')
	}
	for i := range 200 {
		var base []byte
		for len(base) < 200+rng.IntN(20000) {
			base = append(base, word()...)
			if rng.IntN(12) == 0 {
				base = append(base, '\n')
			}
		}
		target := append([]byte{}, base...)
		for range 1 + rng.IntN(6) {
			at := rng.IntN(len(target) + 1)
			switch rng.IntN(3) {
			case 0: // insert
				target = append(target[:at], append(word(), target[at:]...)...)
			case 1: // delete
				end := min(len(target), at+rng.IntN(64))
				target = append(target[:at], target[end:]...)
			case 2: // move a block to the front
				end := min(len(target), at+rng.IntN(256))
				block := append([]byte{}, target[at:end]...)
				target = append(block, append(target[:at], target[end:]...)...)
			}
		}
		d := EncodeDelta(base, target)
		got, err := ApplyDelta(base, d, uint64(len(target)))
		if err != nil || !bytes.Equal(got, target) {
			t.Fatalf("%d: applied: %v (%d bytes of %d)", i, err, len(got), len(target))
		}
		if len(base) > 4000 && len(d) > len(target)/4 {
			t.Errorf("%d: a delta of %d bytes for a %d-byte file a few edits from its base", i, len(d), len(target))
		}
	}
}

// A delta never makes more than it says, nor reads outside its base, whatever its bytes.
func FuzzApplyDelta(f *testing.F) {
	f.Add([]byte("0123456789abcdef0123456789abcdef"), EncodeDelta([]byte("0123456789abcdef0123456789abcdef"), []byte("0123456789abcdefXX0123456789abcdef")))
	f.Fuzz(func(t *testing.T, base, delta []byte) {
		got, err := ApplyDelta(base, delta, 1<<16)
		if err == nil && len(got) > 1<<16 {
			t.Fatalf("made %d bytes past the limit", len(got))
		}
	})
}
